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

// OrderDiscovery 是 ordersource 读端的装配层(D22):轮询 dir 发现 <pod-uid>.ring,
// 为每个新文件构造 Reader 并把阶段切换转发给 onOrder。Run 阻塞,由 agentd 起 goroutine 运行。
type OrderDiscovery struct {
	dir     string
	m       *metrics.Registry
	onOrder func(ctx context.Context, podUID string, o ordersource.ConsumeOrder)

	mu      sync.Mutex
	readers map[string]*ordersource.Reader // pod-uid → Reader(发现即入表,不随文件消失删除)
	ctx     context.Context                // Run 注入的内部 ctx:转发 goroutine 与 Reader 订阅共用
	wg      sync.WaitGroup                 // 转发 goroutine 计数(Run 退出前等待)
}

// NewOrderDiscovery 构造发现器:dir = TRAINCACHE_ORDERS_DIR;m 收集格式错误计数(D23);
// onOrder 默认由 Assemble 接 HandleOrder(日志行 + 预取接线);在转发 goroutine 内串行调用,
// ctx = 发现循环 ctx(M3:预取 LoadPrefixes/OnStageSwitch 随发现循环取消)。
func NewOrderDiscovery(dir string, m *metrics.Registry, onOrder func(ctx context.Context, podUID string, o ordersource.ConsumeOrder)) *OrderDiscovery {
	return &OrderDiscovery{
		dir:     dir,
		m:       m,
		onOrder: onOrder,
		readers: make(map[string]*ordersource.Reader),
	}
}

// Run 阻塞运行发现循环(D22):MkdirAll(失败只 warn,不致命)→ 立即 scan 一次 →
// 每 scanInterval tick:scan + 以绝对值同步格式错误计数(D23);ctx 取消 → Close 全部 Reader、
// 等转发 goroutine 退出 → 返回 ctx.Err()(agentd 视 context.Canceled 为正常停止)。
func (d *OrderDiscovery) Run(ctx context.Context) error {
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()

	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		slog.Warn("orders: 创建发现目录失败(继续运行,等待目录出现)", "dir", d.dir, "err", err)
	}
	d.scan()

	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.closeReaders()
			d.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			d.scan()
			d.m.SyncOrdersourceFormatErrors(ordersource.FormatErrorTotals())
		}
	}
}

// scan 扫目录发现新的 <pod-uid>.ring:新文件构造 Reader、入表并起转发 goroutine。
// 目录读失败只 warn(run 继续,下轮重试)。
func (d *OrderDiscovery) scan() {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		slog.Warn("orders: 扫描发现目录失败", "dir", d.dir, "err", err)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
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
		go d.forward(d.ctx, podUID, r)
	}
}

// forward 把单个 ring 的阶段切换转发给 onOrder(每 Reader 一个 goroutine,随 ctx 取消退出):
// 先订阅,再补投 CurrentOrder 初始状态(约定 7:初始状态不重放,由 svc 补齐),
// 之后循环转发事件;两次投递之间按载荷字节去重,堵"先 CurrentOrder 后收事件"竞态双投的窄窗口。
func (d *OrderDiscovery) forward(ctx context.Context, podUID string, r *ordersource.Reader) {
	defer d.wg.Done()

	ch, err := r.SubscribeStageSwitch(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("orders: 订阅阶段切换失败(M5 再处理)", "pod_uid", podUID, "err", err)
		}
		return
	}

	var lastDelivered []byte
	deliver := func(o ordersource.ConsumeOrder) {
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
// 条目保留在 readers 中供观测与 M5 重扫描沿用。
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
