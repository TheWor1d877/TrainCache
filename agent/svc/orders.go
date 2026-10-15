package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// ringSuffix 是 sidecar ring 文件名后缀:文件 <pod-uid>.ring(design §6.2;
// 发现口径 = svc 目录轮询,decisions D22)。
const ringSuffix = ".ring"

// scanInterval 是发现目录轮询周期(D22:svc 1s 扫 *.ring,新文件构造 Reader)。
// 包内变量,白盒测试可改小。
var scanInterval = time.Second

// degradeTickInterval 是降级聚合判定周期(接线层,主对话裁决:1s)。
// 包内变量,白盒测试可改小。
var degradeTickInterval = time.Second

// orderStallThreshold 是订单源停滞阈值(§7.4:30s;与 collector.degradeStallThreshold 同口径,
// 严格大于才算停滞)。包内变量,白盒测试可改小。
var orderStallThreshold = 30 * time.Second

// OrderDiscovery 是 ordersource 读端的装配层(D22):轮询 dir 发现 <pod-uid>.ring,
// 为每个新文件构造 Reader 并把阶段切换转发给 onOrder;同时承担降级聚合判定(§7.4:
// 全部订单源停滞 → agent_degraded=1,降级期间不推送订单,任一源恢复推进自动续)。
// Run 阻塞,由 agentd 起 goroutine 运行。
type OrderDiscovery struct {
	dir     string
	m       *metrics.Registry
	onOrder func(ctx context.Context, podUID string, o ordersource.ConsumeOrder)

	// onReader 是"新 ring 发现的 reader"回调(装配注入:首个 ring → collector 懒绑定;
	// scan 内新 Reader 入表后、d.mu 之外调用,实现不得阻塞)。
	onReader func(ctx context.Context, podUID string, r *ordersource.Reader)

	// now 是聚合判定的时间源(测试注入:返回错位的"当前时间"模拟写入停滞;缺省 time.Now)。
	// 构造后只读(Run/转发 goroutine 并发读)。
	now func() time.Time

	// lastDegraded 是上一次聚合判定结果(Run goroutine 专属,仅用于状态转移日志)。
	lastDegraded bool

	mu      sync.Mutex
	readers map[string]*ordersource.Reader // pod-uid → Reader(发现即入表,不随文件消失删除)
	ctx     context.Context                // Run 注入的内部 ctx:转发 goroutine 与 Reader 订阅共用
	wg      sync.WaitGroup                 // 转发 goroutine 计数(Run 退出前等待)
}

// SetOnReader 注册新 ring 发现回调(装配期调用一次;回调在发现循环 goroutine 内调用)。
func (d *OrderDiscovery) SetOnReader(fn func(ctx context.Context, podUID string, r *ordersource.Reader)) {
	d.onReader = fn
}

// readerCount 返回已发现 reader 数(观测/降级判定用)。
func (d *OrderDiscovery) readerCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.readers)
}

// NewOrderDiscovery 构造发现器:dir = TRAINCACHE_ORDERS_DIR;m 收集格式错误计数(D23);
// onOrder 默认由 Assemble 接 HandleOrder(日志行 + 预取接线);在转发 goroutine 内串行调用,
// ctx = 发现循环 ctx(M3:预取 LoadPrefixes/OnStageSwitch 随发现循环取消)。
func NewOrderDiscovery(dir string, m *metrics.Registry, onOrder func(ctx context.Context, podUID string, o ordersource.ConsumeOrder)) *OrderDiscovery {
	return &OrderDiscovery{
		dir:     dir,
		m:       m,
		onOrder: onOrder,
		now:     time.Now,
		readers: make(map[string]*ordersource.Reader),
	}
}

// Run 阻塞运行发现循环(D22):MkdirAll(失败只 warn,不致命)→ 立即 scan 一次 →
// 每 scanInterval tick:scan + 以绝对值同步格式错误计数(D23);每 degradeTickInterval tick:
// 聚合降级判定(§7.4);ctx 取消 → Close 全部 Reader、等转发 goroutine 退出 →
// 返回 ctx.Err()(agentd 视 context.Canceled 为正常停止)。
func (d *OrderDiscovery) Run(ctx context.Context) error {
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()

	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		slog.Warn("orders: 创建发现目录失败(继续运行,等待目录出现)", "dir", d.dir, "err", err)
	}
	d.scan()
	d.aggregateDegrade() // 启动即判定一次(与 collector 的启动口径一致)

	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	degradeTicker := time.NewTicker(degradeTickInterval)
	defer degradeTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.closeReaders()
			d.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			d.scan()
			d.m.SyncOrdersourceFormatErrors(ordersource.FormatErrorTotals())
		case <-degradeTicker.C:
			d.aggregateDegrade()
		}
	}
}

// aggregateDegrade 执行一次聚合降级判定并写 gauge(§7.4;每 degradeTickInterval 一拍,
// 由 Run goroutine 调用):
//   - 存在 ≥1 已发现 reader 且全部停滞(Missing / 从未推进(LastAdvance 零值)/ 距今超过
//     orderStallThreshold)→ 降级 1;
//   - 任一 reader 仍在推进 → 0(恢复自动续);
//   - 无 reader(从未发现任何 ring)→ 不降级(0)。
//
// 每拍都写(不只跳变):以聚合结果覆盖 collector 内部的单 ring 判定——多 ring 且存活情况
// 不一致时两者可能相反,聚合口径权威;collector 只在状态跳变时写,故覆盖在 ≤1 拍内收敛。
func (d *OrderDiscovery) aggregateDegrade() bool {
	degraded := d.degradedNow()
	if d.m != nil {
		d.m.SetAgentDegraded(boolToRatio(degraded))
	}
	if d.lastDegraded != degraded {
		if degraded {
			slog.Warn("orders: 进入降级模式(全部订单源停滞/消失)",
				"readers", d.readerCount(), "stall_threshold", orderStallThreshold)
		} else {
			slog.Info("orders: 退出降级模式(订单源推进或尚无订单源)", "readers", d.readerCount())
		}
		d.lastDegraded = degraded
	}
	return degraded
}

// degradedNow 现算聚合降级(交付路径闸门用:不打点不记日志;判定现算,避免把"推进即恢复"
// 的那条订单被上一拍标志误丢)。
func (d *OrderDiscovery) degradedNow() bool {
	d.mu.Lock()
	readers := make([]*ordersource.Reader, 0, len(d.readers))
	for _, r := range d.readers {
		readers = append(readers, r)
	}
	d.mu.Unlock()
	return allStalled(readers, d.now())
}

// allStalled 判定聚合降级:全部 reader 停滞且至少一个(无 reader → false;单 reader 时
// 与 collector.degradedAt 同口径:Missing / LastAdvance 零值 / 严格超过停滞阈值)。
func allStalled(readers []*ordersource.Reader, now time.Time) bool {
	if len(readers) == 0 {
		return false
	}
	for _, r := range readers {
		if r.Missing() {
			continue
		}
		la := r.LastAdvance()
		if la.IsZero() || now.Sub(la) > orderStallThreshold {
			continue
		}
		return false // 该 reader 仍在推进
	}
	return true
}

// scan 扫目录发现新的 <pod-uid>.ring:新文件构造 Reader、入表并起转发 goroutine;
// 新 Reader 同时在 d.mu 之外交给 onReader(首个 ring → collector 绑定,见 wire.go)。
// 目录读失败只 warn(run 继续,下轮重试)。
func (d *OrderDiscovery) scan() {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		slog.Warn("orders: 扫描发现目录失败", "dir", d.dir, "err", err)
		return
	}
	d.mu.Lock()
	type discovered struct {
		podUID string
		r      *ordersource.Reader
	}
	var fresh []discovered
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ringSuffix) {
			continue
		}
		podUID := strings.TrimSuffix(e.Name(), ringSuffix)
		if podUID == "" {
			continue // 文件名恰为 ".ring":非合法 pod-uid,跳过
		}
		if _, ok := d.readers[podUID]; ok {
			continue
		}
		r := ordersource.NewReader(filepath.Join(d.dir, e.Name()), d.dir)
		d.readers[podUID] = r
		d.wg.Add(1)
		fresh = append(fresh, discovered{podUID: podUID, r: r})
		go d.forward(d.ctx, podUID, r)
	}
	ctx := d.ctx
	d.mu.Unlock()

	for _, f := range fresh {
		if d.onReader != nil {
			d.onReader(ctx, f.podUID, f.r)
		}
	}
}

// forward 把单个 ring 的阶段切换转发给 onOrder(每 Reader 一个 goroutine,随 ctx 取消退出):
// 先订阅,再补投 CurrentOrder 初始状态(约定 7:初始状态不重放,由 svc 补齐),
// 之后循环转发事件;两次投递之间按载荷字节去重,堵"先 CurrentOrder 后收事件"竞态双投的窄窗口。
//
// 降级闸门(§7.4):推送前现算聚合降级,降级期间不推送订单(恢复(任一 reader 推进)自动续,
// 因判定现算,不会丢"推进即恢复"的那条订单)。
func (d *OrderDiscovery) forward(ctx context.Context, podUID string, r *ordersource.Reader) {
	defer d.wg.Done()

	ch, err := r.SubscribeStageSwitch(ctx)
	if err != nil {
		if ctx.Err() == nil {
			// 订阅失败仅剩"Reader 已关闭"一种可能(唯一 Close 点在本循环退出路径);不重试:
			// 该 ring 停止转发,停滞判定在阈值后由聚合 tick 接管(不再遗留"M5 再处理")。
			slog.Warn("orders: 订阅阶段切换失败,该 ring 停止转发", "pod_uid", podUID, "err", err)
		}
		return
	}

	var lastDelivered []byte
	deliver := func(o ordersource.ConsumeOrder) {
		if d.degradedNow() {
			slog.Debug("orders: 降级期间订单不推送(等待订单源恢复推进)",
				"pod_uid", podUID, "dataset", o.Dataset, "stage_id", o.StageID)
			return
		}
		raw, err := json.Marshal(o)
		if err != nil {
			// 读端解析出的订单理论上必可序列化;防御性直投,不因去重失败丢单。
			slog.Warn("orders: ConsumeOrder 序列化失败,跳过去重直投", "pod_uid", podUID, "err", err)
			d.onOrder(ctx, podUID, o)
			return
		}
		if bytes.Equal(raw, lastDelivered) {
			return
		}
		lastDelivered = append(lastDelivered[:0], raw...)
		d.onOrder(ctx, podUID, o)
	}

	if o, ok := r.CurrentOrder(); ok {
		deliver(o)
	}
	for {
		select {
		case o := <-ch:
			deliver(o)
		case <-ctx.Done():
			return
		}
	}
}

// closeReaders 关闭全部已发现的 Reader(Close 幂等且阻塞至内部 poll goroutine 退出);
// 条目保留在 readers 中(不清理):Missing 的 reader 继续参与降级聚合判定(§7.4"文件消失
// → 降级"),且同一 pod-uid 的 ring 重建时原 Reader 自行恢复读写(新代际视为推进)。
func (d *OrderDiscovery) closeReaders() {
	d.mu.Lock()
	readers := make([]*ordersource.Reader, 0, len(d.readers))
	for _, r := range d.readers {
		readers = append(readers, r)
	}
	d.mu.Unlock()
	for _, r := range readers {
		r.Close()
	}
}

// LogOrder 是订单行级日志(高基数维度 path 不入日志聚合,阶段级字段足够定位;Q6)。
// msg 固定"收到 ConsumeOrder"——手工验收判据日志行(M2 判据,字段与措辞不得改)。
func LogOrder(podUID string, o ordersource.ConsumeOrder) {
	slog.Info("收到 ConsumeOrder",
		"pod_uid", podUID,
		"dataset", o.Dataset,
		"snapshot_id", o.SnapshotID,
		"stage_id", o.StageID,
		"chunks", len(o.Chunks),
	)
}

// HandleOrder 组装默认订单 handler(Assemble 装配;在发现转发 goroutine 内串行调用):
// 先打 M2 判据日志行(原样),再交预取管理器 ForOrder(前缀分片加载 → 首个订单 Start、
// 后续 OnStageSwitch)。预取接线失败只记日志——单条订单失败不应终止发现循环。
func HandleOrder(pm *PrefetchManager) func(ctx context.Context, podUID string, o ordersource.ConsumeOrder) {
	return func(ctx context.Context, podUID string, o ordersource.ConsumeOrder) {
		LogOrder(podUID, o)
		if pm == nil {
			return
		}
		if err := pm.ForOrder(ctx, podUID, o); err != nil && ctx.Err() == nil {
			slog.Warn("orders: 预取接线失败", "pod_uid", podUID, "dataset", o.Dataset, "stage_id", o.StageID, "err", err)
		}
	}
}
