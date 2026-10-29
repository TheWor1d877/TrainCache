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

// degradeTickInterval 是订单通道状态聚合周期(§7.4:1s)。
// 包内变量,白盒测试可改小。
var degradeTickInterval = time.Second

// orderStallThreshold 是订单源停滞阈值(§7.4:30s,严格大于才算停滞)。
// 包内变量,白盒测试可改小。
var orderStallThreshold = 30 * time.Second

// orderStartupGrace 是发现后的启动宽限(§7.4:5s 内不判 stalled)。
var orderStartupGrace = 5 * time.Second

// orderEndedTTL 是文件消失后回收 Reader 的等待时间(§7.4:5min)。
var orderEndedTTL = 5 * time.Minute

// orderStaleMtime 是发现时判定陈旧 ring 的 mtime 阈值(§7.4:超过 5min 不建 Reader)。
var orderStaleMtime = 5 * time.Minute

// 订单通道聚合模式(§7.4;不进标签)。
const (
	modeNone       = "none"
	modeHealthy    = "healthy"
	modePartial    = "partial"
	modeAllStalled = "all_stalled"
)

// sourceEntry 是一路订单源(identity = pod_uid + 所属 Reader 的 generation + dataset)。
type sourceEntry struct {
	podUID    string
	r         *ordersource.Reader
	cancel    context.CancelFunc // 该源转发 goroutine 的取消函数(回收时先取消再 Close)
	createdAt time.Time          // 发现时间(启动宽限用)

	missingSince time.Time // 首次观察到文件消失的时间(零值 = 文件在)
	state        string    // 上一拍状态(仅用于转移日志)
}

// OrderDiscovery 是 ordersource 读端的装配层(D22):轮询 dir 发现 <pod-uid>.ring,
// 为每个新文件构造 Reader 并把阶段切换转发给 onOrder;同时是订单通道状态的单写者
// (design §7.4):每拍按源算 discovered/healthy/stalled,end 满 5min 回收。
// Run 阻塞,由 agentd 起 goroutine 运行。
type OrderDiscovery struct {
	dir     string
	m       *metrics.Registry
	onOrder func(ctx context.Context, podUID string, o ordersource.ConsumeOrder)

	// now 是状态判定的时间源(测试注入:返回错位的"当前时间"模拟写入停滞;缺省 time.Now)。
	// 构造后只读(Run/转发 goroutine 并发读)。
	now func() time.Time

	// startedAt 是 Run 启动的真实时间(全局启动宽限用,不受注入时钟影响)。
	startedAt time.Time
	// mode 是上一拍聚合模式(Run goroutine 专属,仅用于转移日志)。
	mode string

	mu      sync.Mutex
	readers map[string]*sourceEntry // pod-uid → 订单源(发现即入表;回收后移除)
	stale   map[string]time.Time    // pod-uid → 已计过 stale_file 的 mtime
	ctx     context.Context         // Run 注入的内部 ctx:转发 goroutine 与 Reader 订阅共用
	wg      sync.WaitGroup          // 转发 goroutine 计数(Run 退出前等待)
}

// readerCount 返回当前订单源数(观测用)。
func (d *OrderDiscovery) readerCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.readers)
}

// NewOrderDiscovery 构造发现器:dir = TRAINCACHE_ORDERS_DIR;m 收集指标(D23/D63);
// onOrder 默认由 Assemble 接 HandleOrder(日志行 + 预取接线);在转发 goroutine 内串行调用,
// ctx = 发现循环 ctx(M3:预取 LoadPrefixes/OnStageSwitch 随发现循环取消)。
func NewOrderDiscovery(dir string, m *metrics.Registry, onOrder func(ctx context.Context, podUID string, o ordersource.ConsumeOrder)) *OrderDiscovery {
	return &OrderDiscovery{
		dir:     dir,
		m:       m,
		onOrder: onOrder,
		now:     time.Now,
		readers: make(map[string]*sourceEntry),
		stale:   make(map[string]time.Time),
	}
}

// Run 阻塞运行发现循环(D22):MkdirAll(失败只 warn,不致命)→ 立即 scan 一次 →
// 每 scanInterval tick:scan + 以绝对值同步格式错误计数(D23)与丢单计数(D63);
// 每 degradeTickInterval tick:聚合订单通道状态(§7.4);ctx 取消 → 回收全部 Reader、
// 等转发 goroutine 退出 → 返回 ctx.Err()(agentd 视 context.Canceled 为正常停止)。
func (d *OrderDiscovery) Run(ctx context.Context) error {
	d.mu.Lock()
	d.ctx = ctx
	d.mu.Unlock()
	d.startedAt = time.Now()

	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		slog.Warn("orders: 创建发现目录失败(继续运行,等待目录出现)", "dir", d.dir, "err", err)
	}
	d.scan()
	d.aggregate() // 启动即判定一次,空目录按 none/0 起步

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
			if d.m != nil {
				d.m.SyncOrdersourceFormatErrors(ordersource.FormatErrorTotals())
				resync, corrupt, invalid := ordersource.DroppedTotals()
				d.m.SyncOrdersDropped(resync, corrupt, invalid)
			}
		case <-degradeTicker.C:
			d.aggregate()
		}
	}
}

// aggregate 执行一拍订单通道状态聚合(D63/§7.4;单写者,仅 Run goroutine 调用):
// 按源算 discovered/healthy/stalled,消失满 orderEndedTTL 的源回收;写每源计数、
// ended counter 与 agent_degraded(只在 all_stalled 为 1)。
func (d *OrderDiscovery) aggregate() {
	now := d.now()

	var (
		discovered, healthy, stalled int
		ended                        []*sourceEntry
	)
	d.mu.Lock()
	for podUID, e := range d.readers {
		state := d.stateOfEntryLocked(e, now)
		e.state = state
		switch state {
		case metrics.OrderSourceDiscovered:
			discovered++
		case metrics.OrderSourceHealthy:
			healthy++
		case metrics.OrderSourceStalled:
			stalled++
		}
		if !e.missingSince.IsZero() && now.Sub(e.missingSince) >= orderEndedTTL {
			delete(d.readers, podUID)
			ended = append(ended, e)
		}
	}
	readers := len(d.readers)
	d.mu.Unlock()

	// 回收在锁外做:Close 会等 poll goroutine 退出,不能占着 d.mu。
	for _, e := range ended {
		if e.cancel != nil {
			e.cancel()
		}
		e.r.Close()
		if d.m != nil {
			d.m.IncOrdersourceEnded(metrics.OrderEndedMissingTTL)
		}
		slog.Info("orders: 订单源消失超时,回收 Reader",
			"pod_uid", e.podUID, "ttl", orderEndedTTL)
	}

	mode := aggregateMode(discovered, healthy, stalled)
	if d.m != nil {
		d.m.SetOrdersourceStates(discovered, healthy, stalled)
		d.m.SetAgentDegraded(boolToRatio(mode == modeAllStalled))
	}
	if mode != d.mode {
		switch mode {
		case modeAllStalled:
			slog.Warn("orders: 订单通道降级(全部订单源停滞)",
				"readers", readers, "mode", mode,
				"discovered", discovered, "healthy", healthy, "stalled", stalled,
				"stall_threshold", orderStallThreshold)
		case modePartial:
			slog.Warn("orders: 订单通道部分停滞",
				"readers", readers, "mode", mode, "healthy", healthy, "stalled", stalled)
		case modeNone:
			slog.Info("orders: 订单通道无活跃源",
				"readers", readers, "mode", mode, "discovered", discovered)
		default:
			slog.Info("orders: 订单通道恢复推进",
				"readers", readers, "mode", mode, "healthy", healthy)
		}
		d.mode = mode
	}
}

// stateOfEntryLocked 计算单个订单源的状态(调用方持 d.mu):
//   - 文件消失 → 启动宽限内 discovered,否则 stalled;
//   - 未读到有效头 / 从未推进 → 启动宽限内 discovered,否则 stalled;
//   - 最近一次推进在 orderStallThreshold 内 → healthy,否则 stalled。
//
// 启动宽限 = Run 启动 5s 内或该源发现 5s 内(§7.4:避免 Agent 重启瞬间闪降级)。
func (d *OrderDiscovery) stateOfEntryLocked(e *sourceEntry, now time.Time) string {
	grace := now.Sub(d.startedAt) < orderStartupGrace || now.Sub(e.createdAt) < orderStartupGrace
	if e.r.Missing() {
		if e.missingSince.IsZero() {
			e.missingSince = now
		}
		if grace {
			return metrics.OrderSourceDiscovered
		}
		return metrics.OrderSourceStalled
	}
	e.missingSince = time.Time{}
	if e.r.Generation() == 0 || e.r.LastAdvance().IsZero() {
		if grace {
			return metrics.OrderSourceDiscovered
		}
		return metrics.OrderSourceStalled
	}
	if now.Sub(e.r.LastAdvance()) > orderStallThreshold {
		return metrics.OrderSourceStalled
	}
	return metrics.OrderSourceHealthy
}

// aggregateMode 由各状态计数派生聚合模式(§7.4;不进标签):
//   - none:没有 healthy/stalled 的源;
//   - healthy:有活源、没有 stalled;
//   - partial:同时有 stalled 和非 stalled 的活源;
//   - all_stalled:至少一路 stalled,且没有 healthy/discovered。
func aggregateMode(discovered, healthy, stalled int) string {
	switch {
	case healthy > 0 && stalled == 0:
		return modeHealthy
	case stalled > 0 && healthy == 0 && discovered == 0:
		return modeAllStalled
	case stalled > 0:
		return modePartial
	default:
		return modeNone
	}
}

// scan 扫目录发现新的 <pod-uid>.ring:新文件构造 Reader、入表并起转发 goroutine;
// 目录读失败只 warn(run 继续,下轮重试)。mtime 超过 orderStaleMtime 的陈旧 ring
// 不建 Reader,每个 mtime 只计一次 ordersource_ended_total{reason="stale_file"}。
func (d *OrderDiscovery) scan() {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		slog.Warn("orders: 扫描发现目录失败", "dir", d.dir, "err", err)
		return
	}
	d.mu.Lock()
	type discoveredFile struct {
		podUID string
		path   string
		mtime  time.Time
	}
	var fresh []discoveredFile
	var stale []discoveredFile
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
		info, err := e.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(d.dir, e.Name())
		if time.Since(info.ModTime()) > orderStaleMtime {
			if last, ok := d.stale[podUID]; !ok || !last.Equal(info.ModTime()) {
				d.stale[podUID] = info.ModTime()
				stale = append(stale, discoveredFile{podUID: podUID, path: path, mtime: info.ModTime()})
			}
			continue
		}
		delete(d.stale, podUID)
		fresh = append(fresh, discoveredFile{podUID: podUID, path: path, mtime: info.ModTime()})
	}
	innerCtx := d.ctx
	for _, f := range fresh {
		fctx, cancel := context.WithCancel(innerCtx)
		r := ordersource.NewReader(f.path, d.dir)
		d.readers[f.podUID] = &sourceEntry{
			podUID:    f.podUID,
			r:         r,
			cancel:    cancel,
			createdAt: time.Now(),
			state:     metrics.OrderSourceDiscovered,
		}
		d.wg.Add(1)
		go d.forward(fctx, f.podUID, r)
	}
	d.mu.Unlock()

	for _, f := range stale {
		if d.m != nil {
			d.m.IncOrdersourceEnded(metrics.OrderEndedStaleFile)
		}
		slog.Warn("orders: 跳过陈旧 ring 文件(不建 Reader)",
			"pod_uid", f.podUID, "mtime", f.mtime, "stale_after", orderStaleMtime)
	}
}

// forward 把单个 ring 的阶段切换转发给 onOrder(每 Reader 一个 goroutine,随源 ctx 取消退出):
// 先订阅,再补投 CurrentOrder 初始状态(约定 7:初始状态不重放,由 svc 补齐),
// 之后循环转发事件;按 (generation, 载荷) 去重——新代际同内容订单重放一次(D63)。
//
// 订单是提示不是命令:降级不拦截投递,stalled 期间照样安装新订单。
func (d *OrderDiscovery) forward(ctx context.Context, podUID string, r *ordersource.Reader) {
	defer d.wg.Done()

	ch, err := r.SubscribeStageSwitch(ctx)
	if err != nil {
		if ctx.Err() == nil {
			// 订阅失败仅剩"Reader 已关闭"一种可能(唯一 Close 点在本循环退出路径或回收);
			// 不重试:该源停止转发,状态判定在阈值后由聚合 tick 接管。
			slog.Warn("orders: 订阅阶段切换失败,该 ring 停止转发", "pod_uid", podUID, "err", err)
		}
		return
	}

	var (
		lastGen   int64
		lastRaw   []byte
		delivered bool
	)
	deliver := func(o ordersource.ConsumeOrder) {
		gen := r.Generation()
		raw, err := json.Marshal(o)
		if err != nil {
			// 读端解析出的订单理论上必可序列化;防御性直投,不因去重失败丢单。
			slog.Warn("orders: ConsumeOrder 序列化失败,跳过去重直投", "pod_uid", podUID, "err", err)
			d.onOrder(ctx, podUID, o)
			return
		}
		if delivered && gen == lastGen && bytes.Equal(raw, lastRaw) {
			return
		}
		lastGen, delivered = gen, true
		lastRaw = append(lastRaw[:0], raw...)
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

// closeReaders 取消并关闭全部已发现的 Reader(Close 幂等且阻塞至内部 poll goroutine 退出)。
func (d *OrderDiscovery) closeReaders() {
	d.mu.Lock()
	entries := make([]*sourceEntry, 0, len(d.readers))
	for _, e := range d.readers {
		entries = append(entries, e)
	}
	d.mu.Unlock()
	for _, e := range entries {
		if e.cancel != nil {
			e.cancel()
		}
		e.r.Close()
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
