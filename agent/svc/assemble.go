package svc

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"traincache/agent/collector"
	"traincache/agent/evictor"
	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
	"traincache/agent/rescan"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// prefetchLookahead 是 PersistentPrefetcher 邻域预取提前量(D16:7.0 默认 8);
// 同时是淘汰硬保护窗口(§7.6:cursor 起 lookahead 块,"8 块为接线侧的约定值")。
const prefetchLookahead = 8

// Agent 是 svc 装配产物:持有配置与各子模块实例(design §3:svc 是装配层;D34 导出字段
// 供接线测试与探针注入)。collector 例外:它绑单个 ring,在首个 ring 被发现时才构造
// (§5.5;冻结构造签名不允许换绑),构造后经 Collector 字段发布(见 wire.go)。
type Agent struct {
	Cfg         Config
	Metrics     *metrics.Registry
	Orders      *OrderDiscovery
	Prefetch    *PrefetchManager     // per-dataset 预取资源(§7.3)
	Preheat     *PreheatTasks        // 预热任务状态机(§7.2)
	CS          *cas.Store           // 本地 CAS(D34)
	Fetcher     *fetch.Fetcher       // 回源取数(D34)
	ObjectStore objectstore.Store    // 对象存储(装配注入点:生产 MinIO / 测试 Mock)
	Rescan      *rescan.Rescanner    // 重扫描与坏块修复(M4/D35)
	Collector   *collector.Collector // 采集层(M5;首个 ring 发现时绑定,之前为 nil)
	Evictor     *evictor.Evictor     // 磁盘淘汰循环(M6/§7.6)
	HookSock    *HookSock            // 钩子收端(M5/§5.4)

	// collMu 保护 Collector/ring/ringPod 的懒绑定(collector 与其 ring 同时发布)。
	collMu  sync.Mutex
	ring    *ordersource.Reader // collector 所绑 ring(首个发现的;不换绑)
	ringPod string              // 所绑 ring 的 pod-uid(日志用)
}

// Assemble 构造并持有子模块实例(依赖方向 cmd → svc → 子模块 → core 单向无环,design §4):
// cas → objectstore(D26 MinIO 客户端)→ fetch → …(装配序见 assembleWith)。
func Assemble(cfg Config) (*Agent, error) {
	store, err := objectstore.NewMinIOStore(cfg.ObjstoreEndpoint, cfg.ObjstoreAccessKey,
		cfg.ObjstoreSecretKey, cfg.ObjstoreBucket, cfg.ObjstoreSecure)
	if err != nil {
		return nil, err
	}
	return assembleWith(store, cfg)
}

// assembleWith 是 Assemble 的可测内核(objectstore 注入:生产 MinIO,测试 objectstore.Mock)。
// 构造序(主对话裁决):cas → fetch → metrics(带 node 标签)→ PrefetchManager(+回调注入)
// → PreheatTasks → Rescan(catalogFor 复用 bundle)→ HookSock → Evictor → OrderDiscovery
// (+首 ring 回调);collector 由该回调在首个 ring 发现时构造并启动。
func assembleWith(obj objectstore.Store, cfg Config) (*Agent, error) {
	ag := &Agent{Cfg: cfg, ObjectStore: obj}

	cs, err := cas.New(cfg.CacheRoot, verifyLevel(cfg.Verify))
	if err != nil {
		return nil, err
	}
	ag.CS = cs

	ag.Fetcher = fetch.New(obj, cs, fetch.Options{}) // 零值回退 design §8 默认(1 GiB 在途/64 连接等)

	// 指标层带 node 标签(traincache_agent_up / agent_degraded;§5.8 node 为第二正交维度)。
	ag.Metrics = metrics.New(metrics.Config{
		DatasetWhitelist: cfg.DatasetWhitelist,
		Node:             cfg.NodeName.String(),
	})

	ag.Prefetch = NewPrefetchManager(obj, cs, ag.Fetcher, ag.Metrics)
	ag.Preheat = NewPreheatTasks(cfg, ag.Prefetch)
	// 读时坏块修复(D35)与块完成触达(D24/D28)接线(实现见 wire.go)。
	ag.Prefetch.SetBlockDoneHook(ag.onBlockDone)
	ag.Prefetch.SetCorruptRepairHook(ag.repairCorrupt)
	// 新订单到达 → 刷新订单内块 t_touch(§7.6 任务 4;实现见 instrument.go/wire.go)。
	ag.Prefetch.SetOrderTouchHook(ag.onOrderTouch)

	// 重扫描(M4/D35):catalogFor 与预取管理器共用同一 bundle(同一 dataset 一个 Catalog)。
	ag.Rescan = rescan.New(cs, ag.Fetcher, obj, ag.catalogFor, rescanMetrics{ag.Metrics})

	// 钩子收端(M5/§5.4;路径见 hookSockPath)+ 三型 handler(见 wire.go)。
	ag.HookSock = New(hookSockPath(cfg), ag.Metrics, ag.hookHandlers())

	// 淘汰循环(M6/§7.6):HeatSource = collector 快照(懒绑定适配),ProtectedSource =
	// 预取管理器(cursor 起 lookahead 8 块 + P0 在途),Sweep = 已知 dataset 逐个 core GC。
	ag.Evictor = evictor.New(cs, evictHeatSource{ag}, ag.Prefetch, ag.Metrics, evictor.Config{
		BlockDir:        cfg.CacheRoot,
		TriggerPercent:  cfg.Heat.DiskTriggerPercent,
		DeltaPercent:    cfg.Heat.DiskDeltaPercent,
		CriticalPercent: cfg.Heat.DiskCriticalPercent,
		Sweep:           ag.sweepDatasets,
	})

	// 发现循环由 agentd 在 signal ctx 下起 goroutine 运行(M2);handler = M2 日志行 + 预取
	// 接线(M3);首个 ring 发现 → collector 绑定(M5,见 wire.go bindCollector)。
	ag.Orders = NewOrderDiscovery(cfg.OrdersDir, ag.Metrics, HandleOrder(ag.Prefetch))
	ag.Orders.SetOnReader(ag.bindCollector)
	return ag, nil
}

// verifyLevel 把 svc 校验等级映射到 core cas 枚举(design §8:TRAINCACHE_VERIFY 透传)。
func verifyLevel(verify string) cas.VerifyLevel {
	if verify == VerifyXXH3SHA256 {
		return cas.VerifyXXH3SHA256
	}
	return cas.VerifyXXH3
}

// PrefetchManager 是 svc 的 per-dataset 预取资源管理器(design §3 装配层;§7.3):每个
// dataset 一个 Catalog 与 PersistentPrefetcher(共享 store/cas/fetcher/metrics,按
// 10.15 口径不再按 dataset 副本),mutex 串行化 bundle 表增改。三个入口:
// ForOrder(订单驱动)/ ForPreheat(预热 P0 入队)/ ForListSnapshot(条目枚举)。
type PrefetchManager struct {
	store   objectstore.Store
	cs      *cas.Store
	fetcher *fetch.Fetcher
	m       *metrics.Registry

	baseCtx context.Context // 预取器调度生命周期(Close 取消)
	cancel  context.CancelFunc

	mu      sync.Mutex
	bundles map[string]*datasetBundle

	// p0Done 是 P0 块完成分发(装配期经 SetP0Done 注册;回调在预取器调度协程内同步
	// 调用,故用 atomic 指针免锁读,回调路径不阻塞调度)。
	p0Done atomic.Pointer[p0DoneFunc]

	// blockDone/repair 是块完成触达与读时坏块修复钩子(装配期注入;同 p0Done 口径用
	// atomic 指针,回调路径(调度协程/工作协程)免锁读)。
	blockDone atomic.Pointer[blockDoneHook]
	repair    atomic.Pointer[corruptRepairHook]

	// orderTouch 是新 ConsumeOrder 到达时的 t_touch 刷新钩子(§7.6 任务 4;装配期经
	// SetOrderTouchHook 注入,ForOrder 调用链内原子读;见 instrument.go)。
	orderTouch atomic.Pointer[orderTouchHook]

	// p0Mu/p0Pending 记录 P0 在途块(ForPreheat 入队成功登记、完成回调移出;§7.6 硬保护)。
	p0Mu      sync.Mutex
	p0Pending map[p0Ref]int
}

// blockDoneHook 是块完成触达的 svc 侧签名(含 dataset 归属与该 dataset 的 Catalog,
// 供回调内解析指纹):err == nil = 块就绪/下载成功。在预取器调度协程内同步调用,
// 实现不得阻塞(见 D24/D28)。
type blockDoneHook func(dataset string, cat *index.Catalog, ref prefetch.ChunkRef, err error)

// corruptRepairHook 是读时坏块修复的 svc 侧签名(D35 冻结语义:Quarantine + RepairOne;
// 在工作协程内调用,可阻塞)。返回 nil 且重开成功 → 预取器按就绪继续。
type corruptRepairHook func(ctx context.Context, dataset string, cat *index.Catalog, ref prefetch.ChunkRef) error

// p0Ref 是 P0 在途保护表的键(块身份;与 D25 键同粒度)。
type p0Ref struct {
	dataset string
	path    string
	chunk   int64
}

// p0DoneFunc 是 P0 块完成回调的 svc 侧签名(含 dataset 归属;在预取器调度协程内同步
// 调用,实现不得阻塞)。
type p0DoneFunc func(dataset string, ref prefetch.ChunkRef, err error)

// NewPrefetchManager 构造管理器(导出构造函数:测试注入 objectstore.Mock + 临时 cas +
// 自定义 fetch.Options;Assemble 以 env 配置的 MinIO 实参调用)。m 允许为 nil(无指标场景)。
// 调用方应经 Close 释放调度协程(测试清理;agentd 进程退出即回收)。
func NewPrefetchManager(store objectstore.Store, cs *cas.Store, f *fetch.Fetcher, m *metrics.Registry) *PrefetchManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &PrefetchManager{
		store:     store,
		cs:        cs,
		fetcher:   f,
		m:         m,
		baseCtx:   ctx,
		cancel:    cancel,
		bundles:   make(map[string]*datasetBundle),
		p0Pending: make(map[p0Ref]int),
	}
}

// SetP0Done 注册 P0 块完成分发(装配期调用一次;传 nil 解除)。回调路径原子读。
func (m *PrefetchManager) SetP0Done(fn func(dataset string, ref prefetch.ChunkRef, err error)) {
	if fn == nil {
		m.p0Done.Store(nil)
		return
	}
	h := p0DoneFunc(fn)
	m.p0Done.Store(&h)
}

// SetBlockDoneHook 注册块完成触达钩子(装配期调用一次;传 nil 解除;实现不得阻塞)。
func (m *PrefetchManager) SetBlockDoneHook(fn blockDoneHook) {
	if fn == nil {
		m.blockDone.Store(nil)
		return
	}
	m.blockDone.Store(&fn)
}

// SetCorruptRepairHook 注册读时坏块修复钩子(装配期调用一次;传 nil 解除;可阻塞)。
func (m *PrefetchManager) SetCorruptRepairHook(fn corruptRepairHook) {
	if fn == nil {
		m.repair.Store(nil)
		return
	}
	m.repair.Store(&fn)
}

// dispatchBlockDone 把一次块完成转给已注册钩子(无注册则丢弃)。
func (m *PrefetchManager) dispatchBlockDone(dataset string, cat *index.Catalog, ref prefetch.ChunkRef, err error) {
	if h := m.blockDone.Load(); h != nil {
		(*h)(dataset, cat, ref, err)
	}
}

// dispatchRepair 把一次读时坏块的修复请求转给已注册钩子;无注册 → 报错(调用方走下载)。
func (m *PrefetchManager) dispatchRepair(ctx context.Context, dataset string, cat *index.Catalog, ref prefetch.ChunkRef) error {
	if h := m.repair.Load(); h != nil {
		return (*h)(ctx, dataset, cat, ref)
	}
	return errors.New("svc: 未装配坏块修复钩子")
}

// addP0Refs 登记 P0 在途块(§7.6 硬保护;ForPreheat 入队成功后调用)。
func (m *PrefetchManager) addP0Refs(dataset string, refs []prefetch.ChunkRef) {
	m.p0Mu.Lock()
	defer m.p0Mu.Unlock()
	for _, ref := range refs {
		m.p0Pending[p0Ref{dataset: dataset, path: ref.Path, chunk: ref.Chunk}]++
	}
}

// removeP0Ref 把一个 P0 块移出在途表(完成回调;调度协程内调用,锁内仅 map 操作)。
func (m *PrefetchManager) removeP0Ref(dataset string, ref prefetch.ChunkRef) {
	m.p0Mu.Lock()
	defer m.p0Mu.Unlock()
	k := p0Ref{dataset: dataset, path: ref.Path, chunk: ref.Chunk}
	if n := m.p0Pending[k]; n > 1 {
		m.p0Pending[k] = n - 1
		return
	}
	delete(m.p0Pending, k)
}

// p0Refs 取 P0 在途块快照。
func (m *PrefetchManager) p0Refs() []p0Ref {
	m.p0Mu.Lock()
	defer m.p0Mu.Unlock()
	out := make([]p0Ref, 0, len(m.p0Pending))
	for ref := range m.p0Pending {
		out = append(out, ref)
	}
	return out
}

// Close 取消管理器自有的调度 ctx(ForPreheat 启动的预取器随之退出;ForOrder 启动的
// 预取器生命周期归调用方 ctx)。
func (m *PrefetchManager) Close() { m.cancel() }

// dispatchP0 把一次 P0 块完成转给已注册分发(无注册则丢弃;仅 PreloadTasks 装配前窗口)。
func (m *PrefetchManager) dispatchP0(dataset string, ref prefetch.ChunkRef, err error) {
	if h := m.p0Done.Load(); h != nil {
		(*h)(dataset, ref, err)
	}
}

// datasetBundle 是单个 dataset 的预取资源(§7.3):Catalog(前缀分片加载/只读查询)与
// PersistentPrefetcher;started/stageSeen 记录调度启动与 dataset 绑定状态(bundle 锁保护;
// 阶段安装与 P0 入队在此锁内串行,保证 P0 抓取键的 dataset 绑定不早于首个订单交错)。
type datasetBundle struct {
	dataset    string
	catalog    *index.Catalog
	prefetcher *prefetch.PersistentPrefetcher

	mu        sync.Mutex
	started   bool // 预取器调度已启动(首个 ForOrder/ForPreheat)
	stageSeen bool // 已有 ConsumeOrder 绑定 dataset(否则 ForPreheat 以最小订单播种)

	// lastOrder 是最近一次成功安装的 ConsumeOrder(§7.6 硬保护窗口按它取 cursor 起
	// lookahead 块;protectedFPs 在 bundle 锁下读)。
	lastOrder ordersource.ConsumeOrder
	hasOrder  bool
}

// bundleFor 取/建 dataset 的 bundle(惰性:首个入口触发构造)。
func (m *PrefetchManager) bundleFor(dataset string) *datasetBundle {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.bundles[dataset]; ok {
		return b
	}
	cat := index.NewCatalog(m.store, dataset, index.Config{})
	pf := prefetch.New(cat, m.fetcher, m.cs, m.m, prefetch.Config{
		Lookahead: prefetchLookahead,
		OnP0Done: func(ref prefetch.ChunkRef, err error) {
			// 完成即移出 P0 在途保护表(在分发预热状态机之前,保持保护表新鲜)。
			m.removeP0Ref(dataset, ref)
			m.dispatchP0(dataset, ref, err)
		},
		OnDone: func(ref prefetch.ChunkRef, err error) { m.dispatchBlockDone(dataset, cat, ref, err) },
		OnCorrupt: func(ctx context.Context, ds string, ref prefetch.ChunkRef) error {
			if ds == "" {
				ds = dataset // 预取器按订单绑定 dataset;空值兜底用 bundle 归属
			}
			return m.dispatchRepair(ctx, ds, cat, ref)
		},
	})
	b := &datasetBundle{dataset: dataset, catalog: cat, prefetcher: pf}
	m.bundles[dataset] = b
	return b
}

// bundleNames 返回已有 bundle 的 dataset 名(升序稳定输出;状态 RPC/GC 枚举用)。
func (m *PrefetchManager) bundleNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.bundles))
	for name := range m.bundles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// lookupRef 在指定 dataset 的 Catalog 中查块(内存查询;映射未加载 → ErrNoCurrent / false)。
func (m *PrefetchManager) lookupRef(dataset string, ref ordersource.ChunkRef) (index.Entry, bool, error) {
	return m.bundleFor(dataset).catalog.Lookup(context.Background(), ref.Path, ref.Chunk)
}

// OnConsumeSideAdvance 把消费侧推进(钩子 advance 帧 / collector 校准重对齐回调)转给对应
// dataset 的预取器(§5.2;ref 不在当前订单内或尚无订单 → 预取器自身忽略并返回 nil)。
func (m *PrefetchManager) OnConsumeSideAdvance(ctx context.Context, dataset string, ref prefetch.ChunkRef) error {
	return m.bundleFor(dataset).prefetcher.OnConsumeSideAdvance(ctx, ref)
}

// ProtectedFPs 返回硬保护集合快照(§7.6;evictor.ProtectedSource,任何情况不淘汰):
//   - 各 dataset:当前订单 cursor 起 lookahead 8 个块(经索引解析指纹);
//   - 各 dataset:P0 在途块(ForPreheat 入队成功、完成回调移出)。
//
// 索引未命中/未加载映射的块跳过(无法解析指纹即不在淘汰候选表内)。纯读、不做 IO,
// 可被淘汰循环高频调用。
func (m *PrefetchManager) ProtectedFPs() []cas.Fingerprint {
	m.mu.Lock()
	bundles := make([]*datasetBundle, 0, len(m.bundles))
	for _, b := range m.bundles {
		bundles = append(bundles, b)
	}
	m.mu.Unlock()

	out := make([]cas.Fingerprint, 0, len(bundles)*(prefetchLookahead+1))
	byName := make(map[string]*datasetBundle, len(bundles))
	for _, b := range bundles {
		byName[b.dataset] = b
		out = append(out, b.protectedFPs()...)
	}
	for _, ref := range m.p0Refs() {
		b, ok := byName[ref.dataset]
		if !ok {
			continue
		}
		if e, found, err := b.catalog.Lookup(context.Background(), ref.path, ref.chunk); err == nil && found {
			out = append(out, e.Fingerprint)
		}
	}
	return out
}

// protectedFPs 返回该 dataset 的 cursor 起 lookahead 块指纹(§7.6 硬保护;bundle 锁下读
// 预取器状态与最近订单,索引查询为内存操作)。cursor 不在订单中(尚无游标)时从订单头起算。
func (b *datasetBundle) protectedFPs() []cas.Fingerprint {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.hasOrder || len(b.lastOrder.Chunks) == 0 {
		return nil
	}
	chunks := b.lastOrder.Chunks
	cursor := b.prefetcher.Status().Cursor
	start := 0
	for i := range chunks {
		if chunks[i] == cursor {
			start = i
			break
		}
	}
	end := start + prefetchLookahead
	if end > len(chunks) {
		end = len(chunks)
	}
	out := make([]cas.Fingerprint, 0, end-start)
	for _, ref := range chunks[start:end] {
		if e, ok, err := b.catalog.Lookup(context.Background(), ref.Path, ref.Chunk); err == nil && ok {
			out = append(out, e.Fingerprint)
		}
	}
	return out
}

// ForOrder 是订单入口(发现转发 goroutine 内串行调用;ctx = 发现循环 ctx):取/建 bundle →
// 只加载订单 chunks 涉及的前缀分片(LoadPrefixes,不整包下载;经 measureLoadPrefixes 打
// 索引线指标)→ 首个订单启动调度(Start;之后订单只走 OnStageSwitch)→ OnStageSwitch 安装
// 新阶段(旧阶段降级/在途/ETA 均在预取器内部处理,svc 只记日志)→ 新订单 t_touch 刷新
// (§7.6 任务 4;仅订单成功安装后)。返回索引加载或阶段切换错误(调用方记日志、不中断发现
// 循环)。
func (m *PrefetchManager) ForOrder(ctx context.Context, podUID string, o ordersource.ConsumeOrder) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := m.bundleFor(o.Dataset)
	if err := m.measureLoadPrefixes(ctx, o.Dataset, b.catalog, orderPaths(o.Chunks)); err != nil {
		return err
	}

	b.mu.Lock()
	if !b.started {
		b.started = true
		b.prefetcher.Start(ctx)
	}
	eta, err := b.prefetcher.OnStageSwitch(ctx, o)
	if err == nil {
		b.stageSeen = true
		b.lastOrder, b.hasOrder = o, len(o.Chunks) > 0
	}
	b.mu.Unlock()
	if err != nil {
		return err
	}
	// ④ 新 ConsumeOrder 到达:刷新订单内块 t_touch(§7.6 任务 4;消费窗口保护——订单
	// 安装成功后刷新,空 chunks 跳过;Lookup 未命中/collector 未绑定由钩子内跳过)。
	if len(o.Chunks) > 0 {
		m.dispatchOrderTouch(o.Dataset, b.catalog, o.Chunks)
	}
	slog.Info("orders: 预取阶段已切换",
		"pod_uid", podUID,
		"dataset", o.Dataset,
		"stage_id", o.StageID,
		"chunks", len(o.Chunks),
		"eta_seconds", eta.Seconds(),
	)
	return nil
}

// ForPreheat 是预热入队入口(SubmitPreheat 的 P0 入队):取/建 bundle → 未绑定 dataset 时
// 以最小订单(dataset-only)播种——P0 抓取键(D25)的 dataset 取最近一次 ConsumeOrder 的
// 绑定,预热先于订单到达时由播种补齐(真实首个订单随后覆盖;播种订单无 chunks,不影响
// 阶段语义;副作用:产生一次 0 值字段的 stage_switch_stall 样本)→ 按需启动调度(生命周期 =
// 管理器 ctx,先于订单到达也能推进)→ EnqueueP0(先于 Start 入队的块在 Start 后出队)。
// refs 为空时无操作。
func (m *PrefetchManager) ForPreheat(ctx context.Context, dataset string, refs []prefetch.ChunkRef) error {
	if len(refs) == 0 {
		return nil
	}
	b := m.bundleFor(dataset)
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.stageSeen {
		if _, err := b.prefetcher.OnStageSwitch(ctx, ordersource.ConsumeOrder{Dataset: dataset}); err != nil {
			return err
		}
		b.stageSeen = true
	}
	if !b.started {
		b.started = true
		b.prefetcher.Start(m.baseCtx)
	}
	if err := b.prefetcher.EnqueueP0(ctx, refs); err != nil {
		return err
	}
	// 入队成功即登记 P0 在途硬保护(§7.6;完成回调移出)。
	m.addP0Refs(dataset, refs)
	return nil
}

// ForListSnapshot 返回 dataset 当前快照的条目(ListSnapshot 查询入口):取/建 bundle
// (不加载前缀映射、不启动预取);Catalog.List 自身只下载相关分片(D29)。
func (m *PrefetchManager) ForListSnapshot(ctx context.Context, dataset, prefix string) ([]index.Entry, error) {
	return m.bundleFor(dataset).catalog.List(ctx, prefix)
}

// orderPaths 取订单 chunks 的 path 去重列表(首次出现序;LoadPrefixes 的输入)。
func orderPaths(chunks []ordersource.ChunkRef) []string {
	seen := make(map[string]struct{}, len(chunks))
	paths := make([]string, 0, len(chunks))
	for _, c := range chunks {
		if _, ok := seen[c.Path]; ok {
			continue
		}
		seen[c.Path] = struct{}{}
		paths = append(paths, c.Path)
	}
	return paths
}
