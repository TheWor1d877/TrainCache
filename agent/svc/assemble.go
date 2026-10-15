package svc

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// prefetchLookahead 是 PersistentPrefetcher 邻域预取提前量(D16:7.0 默认 8)。
const prefetchLookahead = 8

// Agent 是 svc 装配产物:持有配置与各子模块实例(design §3:svc 是装配层)。
// M1 只有 metrics 骨架;M2 增 ordersource 发现器(Orders);M3 增预取管理器(Prefetch)
// 与预热任务表(Preheat);collector/rescan 随各自里程碑接入。
type Agent struct {
	Cfg      Config
	Metrics  *metrics.Registry
	Orders   *OrderDiscovery
	Prefetch *PrefetchManager // per-dataset 预取资源(§7.3)
	Preheat  *PreheatTasks    // 预热任务状态机(§7.2)
}

// Assemble 构造并持有子模块实例(依赖方向 cmd → svc → 子模块 → core 单向无环,design §4):
// cas → objectstore(D26 MinIO 客户端)→ fetch → PrefetchManager → PreheatTasks → OrderDiscovery。
func Assemble(cfg Config) (*Agent, error) {
	ag := &Agent{
		Cfg:     cfg,
		Metrics: metrics.New(metrics.Config{DatasetWhitelist: cfg.DatasetWhitelist}),
	}
	cs, err := cas.New(cfg.CacheRoot, verifyLevel(cfg.Verify))
	if err != nil {
		return nil, err
	}
	store, err := objectstore.NewMinIOStore(cfg.ObjstoreEndpoint, cfg.ObjstoreAccessKey,
		cfg.ObjstoreSecretKey, cfg.ObjstoreBucket, cfg.ObjstoreSecure)
	if err != nil {
		return nil, err
	}
	fetcher := fetch.New(store, cs, fetch.Options{}) // 零值回退 design §8 默认(1 GiB 在途/64 连接等)
	ag.Prefetch = NewPrefetchManager(store, cs, fetcher, ag.Metrics)
	ag.Preheat = NewPreheatTasks(cfg, ag.Prefetch)
	// 发现循环由 agentd 在 signal ctx 下起 goroutine 运行(M2);handler = M2 日志行 + 预取接线(M3)。
	ag.Orders = NewOrderDiscovery(cfg.OrdersDir, ag.Metrics, HandleOrder(ag.Prefetch))
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
		store:   store,
		cs:      cs,
		fetcher: f,
		m:       m,
		baseCtx: ctx,
		cancel:  cancel,
		bundles: make(map[string]*datasetBundle),
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
		OnP0Done:  func(ref prefetch.ChunkRef, err error) { m.dispatchP0(dataset, ref, err) },
	})
	b := &datasetBundle{dataset: dataset, catalog: cat, prefetcher: pf}
	m.bundles[dataset] = b
	return b
}

// ForOrder 是订单入口(发现转发 goroutine 内串行调用;ctx = 发现循环 ctx):取/建 bundle →
// 只加载订单 chunks 涉及的前缀分片(LoadPrefixes,不整包下载)→ 首个订单启动调度(Start;
// 之后订单只走 OnStageSwitch)→ OnStageSwitch 安装新阶段(旧阶段降级/在途/ETA 均在预取器
// 内部处理,svc 只记日志)。返回索引加载或阶段切换错误(调用方记日志、不中断发现循环)。
func (m *PrefetchManager) ForOrder(ctx context.Context, podUID string, o ordersource.ConsumeOrder) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := m.bundleFor(o.Dataset)
	if err := b.catalog.LoadPrefixes(ctx, orderPaths(o.Chunks)); err != nil {
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
	}
	b.mu.Unlock()
	if err != nil {
		return err
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
	return b.prefetcher.EnqueueP0(ctx, refs)
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
