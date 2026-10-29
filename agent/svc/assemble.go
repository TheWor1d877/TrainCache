package svc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

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

// prefetchLookahead 是邻域预取提前量与淘汰硬保护窗口的默认值(D16:16;
// §7.6"cursor 起 lookahead 块")。保留为 const 供测试引用。
// lookahead 是运行时值,由 TRAINCACHE_PREFETCH_LOOKAHEAD 环境变量覆盖
// (装配期一次性设置,无并发写);生产代码统一使用 lookahead。
const prefetchLookahead = 16

var lookahead = prefetchLookahead

// envEvictPolicy / envPrefetchLookahead 是 M4 淘汰策略与预取提前量的环境变量名。
const (
	envEvictPolicy       = "TRAINCACHE_EVICT_POLICY"
	envPrefetchLookahead = "TRAINCACHE_PREFETCH_LOOKAHEAD"
)

// defaultEvictPolicy 是默认淘汰策略名(M4:D8 heat 档行为不变)。
const defaultEvictPolicy = "heat"

// selfOpenGuardTTL 是 Agent 自打开守卫的有效期(D49;覆盖 inotify 轮询与处理延迟)。
const selfOpenGuardTTL = 3 * time.Second

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
	ObjectStore objectstore.Store    // 对象存储(装配注入点:生产 S3 兼容后端 / 测试 Mock)
	Rescan      *rescan.Rescanner    // 重扫描与坏块修复(M4/D35)
	Collector   *collector.Collector // 采集层(M5;首个 ring 发现时绑定,之前为 nil)
	Evictor     *evictor.Evictor     // 磁盘淘汰循环(M6/§7.6)
	HookSock    *HookSock            // 钩子收端(M5/§5.4)
	// ReaderRegistry 是 R2 读者登记器(6.1 §5.10):声明本节点跟随的 snapID,
	// 供 GC 按读者确认删除。装配期构造,StartBackground 起心跳。
	ReaderRegistry *index.ReaderRegistry

	// selfOpen 是 Agent 自打开守卫(D49):prefetch/fetch/rescan/backfill/stall 归因的
	// 本地块读取先 Mark,collector 的 inotify 兜底以 SelfOpened 过滤。nil 安全。
	selfOpen *prefetch.SelfOpenGuard

	// M9 阶段/归因骨架(D46/D47;装配期构造,业务逻辑由 M9 填写)。
	phase *phaseTracker
	stall *stallAttributor

	// collMu 保护 Collector/ring/ringPod 的懒绑定(collector 与其 ring 同时发布)。
	collMu  sync.Mutex
	ring    *ordersource.Reader // collector 所绑 ring(首个发现的;不换绑)
	ringPod string              // 所绑 ring 的 pod-uid(日志用)
}

// Assemble 构造并持有子模块实例(依赖方向 cmd → svc → 子模块 → core 单向无环,design §4):
// cas → objectstore(D26 S3 兼容客户端)→ fetch → …(装配序见 assembleWith)。
func Assemble(cfg Config) (*Agent, error) {
	store, err := objectstore.NewS3Store(cfg.ObjstoreEndpoint, cfg.ObjstoreAccessKey,
		cfg.ObjstoreSecretKey, cfg.ObjstoreBucket, cfg.ObjstoreSecure)
	if err != nil {
		return nil, err
	}
	return assembleWith(store, cfg)
}

// assembleWith 是 Assemble 的可测内核(objectstore 注入:生产 S3 兼容后端,测试 objectstore.Mock)。
// 构造序(主对话裁决):cas → fetch → metrics(带 node 标签)→ PrefetchManager(+回调注入)
// → PreheatTasks → Rescan(catalogFor 复用 bundle)→ HookSock → Evictor → OrderDiscovery
// (+首 ring 回调);collector 由该回调在首个 ring 发现时构造并启动。
func assembleWith(obj objectstore.Store, cfg Config) (*Agent, error) {
	// M4:TRAINCACHE_PREFETCH_LOOKAHEAD 覆盖预取提前量(默认 16;装配期一次性设置)。
	if n, err := envPositiveInt(envPrefetchLookahead, lookahead); err == nil {
		lookahead = n
	} else {
		return nil, err
	}

	// M4:TRAINCACHE_EVICT_POLICY 选择淘汰策略(默认 heat;未知策略名拒绝启动)。
	policy, err := newEvictPolicyFromEnv()
	if err != nil {
		return nil, err
	}

	ag := &Agent{Cfg: cfg, ObjectStore: obj}

	cs, err := cas.New(cfg.CacheRoot, verifyLevel(cfg.Verify))
	if err != nil {
		return nil, err
	}
	ag.CS = cs

	// D49 self-open guard:fetch 在读回校验前登记指纹(通用钩子,core 不依赖 prefetch)。
	// 最早安装在 cas.Store 实例上(Open 前置观察者):本进程任何经 Store.Open 的读打开
	// (Commit 校验、fetch 读回、rescan/backfill 等)都登记为自打开;显式 Mark 点保留
	// (幂等无害)。必须在 fetch/prefetcher/rescan/backfill 构造之前完成。
	ag.selfOpen = prefetch.NewSelfOpenGuard(selfOpenGuardTTL)
	ag.Fetcher = fetch.New(obj, cs, fetch.Options{OnLocalOpen: ag.selfOpen.Mark}) // 其余零值回退 design §8 默认

	// 指标层带 node 标签(traincache_agent_up / agent_degraded;§5.8 node 为第二正交维度)。
	ag.Metrics = metrics.New(metrics.Config{
		DatasetWhitelist: cfg.DatasetWhitelist,
		Node:             cfg.NodeName.String(),
	})

	// M9/D46 阶段跟踪器(窗口 K 取 lookahead)。
	ag.phase = newPhaseTracker(lookahead)
	ag.Prefetch = NewPrefetchManager(obj, cs, ag.Fetcher, ag.Metrics)
	ag.Prefetch.SetSelfOpenGuard(ag.selfOpen) // D49:各 dataset 预取器的本地命中/读回打开
	// R1/R2(6.1 §5.9/§5.10):快照跟随周期 + 读者登记器(grace = 2*刷新周期,
	// 与 core ReaderGrace 默认口径一致)。Follow/心跳由 StartBackground 注入 ctx 后启动。
	interval := cfg.indexRefreshInterval()
	ag.ReaderRegistry = index.NewReaderRegistry(obj, cfg.NodeName.String(), 2*interval)
	ag.Prefetch.SetIndexFollow(interval, ag.ReaderRegistry)
	// R4(6.1 §5.12):条件写探测(默认关闭;不支持/失败只告警,不阻止启动)。
	if cfg.ObjstoreProbeConditionalWrite {
		probeCtx, cancel := context.WithTimeout(context.Background(), conditionalWriteProbeTimeout)
		probeErr := objectstore.ProbeConditionalWrite(probeCtx, obj)
		cancel()
		ag.Metrics.SetObjectstoreConditionalWriteSupported(probeErr == nil)
		if probeErr != nil {
			slog.Warn("对象存储条件写探测未通过(继续启动;提交冲突重试可能不可靠)",
				"err", probeErr)
		}
	}
	ag.Prefetch.phase = ag.phase
	// 块/事件 → dataset 归属:按 ring 订单/已知 bundle 解析,不要求索引命中(与
	// demand 同口径,钩子 resolveDataset;D60)。
	ag.phase.setDatasetResolver(func(ref ordersource.ChunkRef) (string, bool) {
		return ag.resolveDataset(ref)
	})
	// M9/D47 归因依赖:本地存在性 + 预取 pending/当前窗口(桩返回 false,逻辑由 M9 填写)。
	ag.stall = newStallAttributorWithThreshold(ag.stallLocalExists, ag.stallInflight,
		ag.stallCurrentWindow, cfg.ReadStallMinWaitMS)
	ag.Preheat = NewPreheatTasks(cfg, ag.Prefetch)
	// 读时坏块修复(D35)与块完成触达(D24/D28)接线(实现见 wire.go)。
	ag.Prefetch.SetBlockDoneHook(ag.onBlockDone)
	ag.Prefetch.SetCorruptRepairHook(ag.repairCorrupt)
	// 新订单到达 → 刷新订单内块 t_touch(§7.6 任务 4;实现见 instrument.go/wire.go)。
	ag.Prefetch.SetOrderTouchHook(ag.onOrderTouch)

	// 重扫描(M4/D35):catalogFor 与预取管理器共用同一 bundle(同一 dataset 一个 Catalog)。
	ag.Rescan = rescan.New(cs, ag.Fetcher, obj, ag.catalogFor, rescanMetrics{ag.Metrics})
	ag.Rescan.SetSelfOpenMarker(ag.selfOpen.Mark) // D49:扫描校验/修复读回
	// M8/M9 指标接线(singleflight 合并钩子、限速上限、修复队列初始深度;见 metricswiring.go)。
	ag.wireMetrics()

	// 钩子收端(M5/§5.4;路径见 hookSockPath)+ 三型 handler(见 wire.go)。
	ag.HookSock = New(hookSockPath(cfg), ag.Metrics, ag.hookHandlers())

	// 淘汰循环(M6/§7.6):HeatSource = collector 快照(懒绑定适配),ProtectedSource =
	// 预取管理器(cursor 起 lookahead 块 + P0 在途),Sweep = 已知 dataset 逐个 core GC。
	// M4:注入 Policy(heat/lru/lfu,由 TRAINCACHE_EVICT_POLICY 选择;D9 撤 W-TinyLFU 档)。
	ag.Evictor = evictor.New(cs, evictHeatSource{ag}, ag.Prefetch, ag.Metrics, policy, evictor.Config{
		BlockDir:           cfg.CacheRoot,
		TriggerPercent:     cfg.Heat.DiskTriggerPercent,
		DeltaPercent:       cfg.Heat.DiskDeltaPercent,
		CriticalPercent:    cfg.Heat.DiskCriticalPercent,
		Sweep:              ag.sweepDatasets,
		CacheCapacityBytes: cfg.CacheCapacityBytes,
	})

	// 统一注册 Store.Open 前置观察者(单槽 atomic.Pointer,必须合并两个回调):
	// selfOpen.Mark(D49 自打开守卫) + evictor.OnStoreOpen(字节维度累计,内部 seenFPs 去重)。
	// evictor.New 不再自动注册 SetOpenObserver,避免覆盖装配层的合并回调。
	cs.SetOpenObserver(func(fp cas.Fingerprint) {
		ag.selfOpen.Mark(fp)
		ag.Evictor.OnStoreOpen(fp)
	})
	// Store.Put 后置观察者:evictor.OnStorePut(字节维度累计,直接用 Put 返回的准确 size,
	// 与 OnStoreOpen 共享 seenFPs 去重表——Put 先于 Open,后续 Open 自动跳过)。
	cs.SetPutObserver(func(fp cas.Fingerprint, size int64) {
		ag.Evictor.OnStorePut(fp, size)
	})

	// 发现循环由 agentd 在 signal ctx 下起 goroutine 运行(M2);handler = M2 日志行 + 预取
	// 接线(M3);首个 ring 发现 → collector 绑定(M5,见 wire.go bindCollector)。
	ag.Orders = NewOrderDiscovery(cfg.OrdersDir, ag.Metrics, HandleOrder(ag.Prefetch))
	ag.Orders.SetOnReader(ag.bindCollector)
	return ag, nil
}

// newEvictPolicyFromEnv 从 TRAINCACHE_EVICT_POLICY 环境变量创建淘汰策略(M4)。
// 可选值:heat/lru/lfu,默认 heat;未知策略名返回错误(拒绝启动并打印合法清单;
// D9 撤 W-TinyLFU 档后合法清单收缩为三档)。
func newEvictPolicyFromEnv() (evictor.Policy, error) {
	name := envOr(envEvictPolicy, defaultEvictPolicy)
	switch name {
	case "heat":
		return evictor.NewHeatPolicy(), nil
	case "lru":
		return &evictor.LRUPolicy{}, nil
	case "lfu":
		return evictor.NewLFUPolicy(), nil
	default:
		return nil, fmt.Errorf("svc: TRAINCACHE_EVICT_POLICY=%q 非法(可选: heat / lru / lfu)", name)
	}
}

// markSelfOpen 在 Agent 自身读取本地块前登记指纹(D49;nil 安全)。
func (a *Agent) markSelfOpen(fp cas.Fingerprint) {
	if a.selfOpen != nil {
		a.selfOpen.Mark(fp)
	}
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
	store    objectstore.Store
	cs       *cas.Store
	fetcher  *fetch.Fetcher
	m        *metrics.Registry
	selfOpen *prefetch.SelfOpenGuard // D49 自打开守卫(SetSelfOpenGuard 注入;m.mu 保护)

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

	// phase 是 M9/D46 阶段跟踪器(装配期注入;各 dataset 的 OnStageSwitch 回调共用)。
	phase *phaseTracker

	// refreshInterval 是 Catalog.Follow 的刷新周期与心跳周期基数(6.1 §5.9;
	// 装配期注入,零值由 Catalog 自身回退默认 5s)。
	refreshInterval time.Duration
	// registry 是 R2 读者登记器(原子指针:装配期注入后仅供 OnSnapshotChange 回调读)。
	registry atomic.Pointer[index.ReaderRegistry]
	// lifecycleCtx 是 Follow/心跳的生命周期 ctx(Agent.StartBackground 注入;
	// m.mu 保护,未注入前不启动 Follow,避免 context.Background 泄漏)。
	lifecycleCtx context.Context
	// followStarts 记录每个 dataset 的 Follow 启动次数(仅测试断言"只启动一次";
	// m.mu 保护)。
	followStarts map[string]int
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
// 自定义 fetch.Options;Assemble 以 env 配置的对象存储实参调用)。m 允许为 nil(无指标场景)。
// 调用方应经 Close 释放调度协程(测试清理;agentd 进程退出即回收)。
func NewPrefetchManager(store objectstore.Store, cs *cas.Store, f *fetch.Fetcher, m *metrics.Registry) *PrefetchManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &PrefetchManager{
		store:        store,
		cs:           cs,
		fetcher:      f,
		m:            m,
		baseCtx:      ctx,
		cancel:       cancel,
		bundles:      make(map[string]*datasetBundle),
		p0Pending:    make(map[p0Ref]int),
		followStarts: make(map[string]int),
	}
}

// SetSelfOpenGuard 注入 Agent 自打开守卫(D49;装配期在首个 bundle 构造前调用一次)。
// 传 nil 解除。m.mu 串行化,bundleFor 在同一锁下读取。
func (m *PrefetchManager) SetSelfOpenGuard(g *prefetch.SelfOpenGuard) {
	m.mu.Lock()
	m.selfOpen = g
	m.mu.Unlock()
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
	// followStarted 表示该 dataset 的 Catalog.Follow 后台循环已启动(m.mu 保护;
	// 同一 dataset 只启动一次,6.1 §5.9)。
	followStarted bool

	// lastOrder 是最近一次成功安装的 ConsumeOrder(§7.6 硬保护窗口按它取 cursor 起
	// lookahead 块;protectedFPs 在 bundle 锁下读)。
	lastOrder ordersource.ConsumeOrder
	hasOrder  bool
	// followPrefixes 是当前订单的块路径前缀 hint(ForOrder 在首次索引加载前写入)。
	// Follow 首轮 tick 可能早于 lastOrder 安装,没有 hint 时会以空前缀走
	// RefreshManifest 并清空随后加载的映射;空订单写入非 nil 空切片显式清除。
	followPrefixes []string

	// fpGen/fpIndex 是按订单代际缓存的指纹反向索引(D49):键含 stageID、Chunk 数、
	// 首尾 ref,订单切换即重建。索引一经安装不再原地修改(重建换新 map),锁外查询安全。
	fpGen   fpIndexGen
	fpIndex map[cas.Fingerprint][]ordersource.ChunkRef
}

// fpIndexGen 是订单代际键(订单切换或块序列变化即变化)。
type fpIndexGen struct {
	stageID string
	n       int
	first   ordersource.ChunkRef
	last    ordersource.ChunkRef
}

// orderGeneration 返回订单的指纹索引代际键。
func orderGeneration(o ordersource.ConsumeOrder) fpIndexGen {
	g := fpIndexGen{stageID: o.StageID, n: len(o.Chunks)}
	if g.n > 0 {
		g.first, g.last = o.Chunks[0], o.Chunks[g.n-1]
	}
	return g
}

// bundleFor 取/建 dataset 的 bundle(惰性:首个入口触发构造)。initialPrefixes 非 nil
// 时随构造装入 followPrefixes——首次构造即携带订单前缀 hint,先于 Follow 启动
// (Follow 首轮检查不等定时器,D60);既有 bundle 忽略本参数,其前缀由
// orderPrefixes 跟随 followPrefixes/lastOrder 演进。无订单入口传 nil。
func (m *PrefetchManager) bundleFor(dataset string, initialPrefixes []string) *datasetBundle {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.bundles[dataset]; ok {
		return b
	}
	cat := index.NewCatalog(m.store, dataset, index.Config{
		RefreshInterval: m.refreshInterval,
		OnSnapshotChange: func(ds, snapID string) {
			m.onSnapshotChange(ds, snapID)
		},
		OnSnapshotError: func(ds string, err error) {
			m.onSnapshotError(ds, err)
		},
	})
	pf := prefetch.New(cat, m.fetcher, m.cs, m.m, prefetch.Config{
		Lookahead:     lookahead,
		SelfOpenGuard: m.selfOpen, // D49:本地命中/读回/ETA 探测打开前 Mark
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
		// M9/D46:每次 OnStageSwitch 通知 svc phaseTracker 迁移到 switch。
		OnStageSwitch: func(stageID string) {
			if m.phase != nil {
				m.phase.OnStageSwitchFor(dataset, stageID)
			}
		},
	})
	b := &datasetBundle{dataset: dataset, catalog: cat, prefetcher: pf}
	if initialPrefixes != nil {
		b.followPrefixes = initialPrefixes // 发布前装入,无并发读者
	}
	m.bundles[dataset] = b
	// R1(6.1 §5.9/D60):hint 在 b 发布前装入(创建与 m.mu 同段持有,Follow 的
	// prefixesFn 用 b.mu,无并发窗口);生命周期 ctx 未注入时 startFollowLocked
	// 空转,由 StartBackground 对既有 bundle 补启动。
	m.startFollowLocked(b)
	return b
}

// existingBundle 返回已存在的 dataset bundle(不触发构造;归因等只读路径用)。
func (m *PrefetchManager) existingBundle(dataset string) (*datasetBundle, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.bundles[dataset]
	return b, ok
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
	return m.bundleFor(dataset, nil).catalog.Lookup(context.Background(), ref.Path, ref.Chunk)
}

// lookupFingerprint 按指纹反查当前订单中的候选块引用(D49;inotify OPEN 兜底用)。
// Catalog 无按指纹反查接口,故惰性遍历 lastOrder.Chunks 逐个 Lookup 建整张反向索引,
// 同一订单代际内查询 O(1);无订单返回 nil。索引构建在 bundle 锁外进行(先快照订单、
// 锁外 Catalog 查询、再校验代际安装),并发下允许重复构建但无数据竞争。
func (b *datasetBundle) lookupFingerprint(ctx context.Context, fp cas.Fingerprint) []ordersource.ChunkRef {
	b.mu.Lock()
	if !b.hasOrder || len(b.lastOrder.Chunks) == 0 {
		b.mu.Unlock()
		return nil
	}
	order := b.lastOrder
	gen := orderGeneration(order)
	if b.fpIndex != nil && b.fpGen == gen {
		out := b.fpIndex[fp]
		b.mu.Unlock()
		return out
	}
	b.mu.Unlock()

	idx := make(map[cas.Fingerprint][]ordersource.ChunkRef, len(order.Chunks))
	for _, ref := range order.Chunks {
		e, ok, err := b.catalog.Lookup(ctx, ref.Path, ref.Chunk)
		if err != nil || !ok {
			continue
		}
		idx[e.Fingerprint] = append(idx[e.Fingerprint], ref)
	}

	b.mu.Lock()
	if b.hasOrder && orderGeneration(b.lastOrder) == gen {
		b.fpGen, b.fpIndex = gen, idx
	}
	b.mu.Unlock()
	return idx[fp]
}

// OnConsumeSideAdvance 把消费侧推进(钩子 advance 帧 / collector 校准重对齐回调 /
// inotify 兜底反查)转给对应 dataset 的预取器(§5.2),直接透传底层"是否真正推进"
// (重复/落后事件返回 false 且无副作用;ref 不在当前订单内或尚无阶段同样 false)。
func (m *PrefetchManager) OnConsumeSideAdvance(ctx context.Context, dataset string, ref prefetch.ChunkRef) bool {
	return m.bundleFor(dataset, nil).prefetcher.OnConsumeSideAdvance(ctx, ref)
}

// ProtectedFPs 返回硬保护集合快照(§7.6;evictor.ProtectedSource,任何情况不淘汰):
//   - 各 dataset:当前订单 cursor 起 lookahead 16 个块(经索引解析指纹);
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

	out := make([]cas.Fingerprint, 0, len(bundles)*(lookahead+1))
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

// refWindowState 判断 ref 是否属于该 dataset 最近订单的当前消费窗口(D46 命中 source
// 判据):known=false 表示无订单可归属;inWindow=true 表示位点落在 cursor 起 lookahead
// 范围内。bundle 锁下读;Status() 内部锁与 bundle 锁同序(b.mu → p.mu),无反向持有。
func (b *datasetBundle) refWindowState(ref ordersource.ChunkRef) (known, inWindow bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.hasOrder || len(b.lastOrder.Chunks) == 0 {
		return false, false
	}
	chunks := b.lastOrder.Chunks
	cursor := -1
	cur := b.prefetcher.Status().Cursor
	for i := range chunks {
		if chunks[i] == cur {
			cursor = i
			break
		}
	}
	if cursor < 0 {
		cursor = 0 // 尚无游标:从订单头起算(与 protectedFPs 同口径)
	}
	for i := range chunks {
		if chunks[i] == ref {
			return true, i >= cursor && i < cursor+lookahead
		}
	}
	return true, false
}

// protectedFPs 返回该 dataset 的保护窗口指纹(§7.6 硬保护;bundle 锁下读预取器状态与
// 最近订单,索引查询为内存操作)。窗口基准 = max(消费游标 Cursor, 预取前沿
// PrefetchFrontier)在订单中的位置:保证已预取块不因消费尚未推进而提前解锁;任一字段
// 找不到时回退到能找到的那个,都找不到则从订单头起算。
func (b *datasetBundle) protectedFPs() []cas.Fingerprint {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.hasOrder || len(b.lastOrder.Chunks) == 0 {
		return nil
	}
	chunks := b.lastOrder.Chunks
	st := b.prefetcher.Status()
	ci, fi := -1, -1
	for i := range chunks {
		if ci < 0 && chunks[i] == st.Cursor {
			ci = i
		}
		if fi < 0 && chunks[i] == st.PrefetchFrontier {
			fi = i
		}
	}
	start := 0
	switch {
	case ci >= 0 && fi >= 0:
		start = ci
		if fi > start {
			start = fi
		}
	case ci >= 0:
		start = ci
	case fi >= 0:
		start = fi
	}
	end := start + lookahead
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

// ForOrder 是订单入口(发现转发 goroutine 内串行调用;ctx = 发现循环 ctx):取/建 bundle
// (订单前缀 hint 随构造装入,先于 Follow 首轮检查,D60)→ 只加载订单 chunks 涉及的前缀
// 分片(LoadPrefixes,不整包下载;经 measureLoadPrefixes 打索引线指标)→ 首个订单启动
// 调度(Start;之后订单只走 OnStageSwitch)→ OnStageSwitch 安装新阶段(旧阶段降级/在途/
// ETA 均在预取器内部处理,svc 只记日志)→ 新订单 t_touch 刷新(§7.6 任务 4;仅订单成功
// 安装后)。返回索引加载或阶段切换错误(调用方记日志、不中断发现循环)。
func (m *PrefetchManager) ForOrder(ctx context.Context, podUID string, o ordersource.ConsumeOrder) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 订单前缀 hint 必须在首次索引加载前写入:Follow 启动即执行首轮检查,读到空前缀
	// 就会走 RefreshManifest 清空映射(D60;bundle 内 hint 装入先于 Follow 启动)。
	// 空订单写非 nil 空切片,显式清除上一单前缀且不回退。
	prefixes := orderPaths(o.Chunks)
	b := m.bundleFor(o.Dataset, prefixes)
	if err := m.measureLoadPrefixes(ctx, o.Dataset, b.catalog, prefixes); err != nil {
		return err
	}

	// M9/D46:启动/切换窗口 K 取本订单的 ConsumeOrder.PrefetchChunks(与预取器同口径
	// 截断到订单块数),按 dataset 记录;未注入 K 的 dataset 回退缺省 lookahead。
	// 必须在 OnStageSwitch 前写入:预取器的 OnStageSwitch 回调会同步迁移 phase 状态。
	if m.phase != nil {
		k := o.PrefetchChunks
		if k < 0 {
			k = 0
		}
		if k > len(o.Chunks) {
			k = len(o.Chunks)
		}
		m.phase.OnOrderForDataset(o.Dataset, k)
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

// PreheatScope 结一个显式预热的下载作用域(D62):派生自预取管理器自身的 ctx,所以 Agent
// 关停连带取消,而任务自己能在 CancelPreheat 时提前把尚未完成的块从 core fetch 的准入
// 排队里叫醒。调用方须在任务出表时调用返回的 cancel,否则 ctx 泄漏。
func (m *PrefetchManager) PreheatScope() (context.Context, context.CancelFunc) {
	return context.WithCancel(m.baseCtx)
}

// ForPreheat 是预热入队入口(SubmitPreheat 的 P0 入队):取/建 bundle → 未绑定 dataset 时
// 以最小订单(dataset-only)播种——P0 抓取键(D25)的 dataset 取最近一次 ConsumeOrder 的
// 绑定,预热先于订单到达时由播种补齐(真实首个订单随后覆盖;播种订单无 chunks,不影响
// 阶段语义;副作用:产生一次 0 值字段的 stage_switch_eta 样本)→ 按需启动调度(生命周期 =
// 管理器 ctx,先于订单到达也能推进)→ EnqueueP0(先于 Start 入队的块在 Start 后出队)。
// refs 为空时无操作。
//
// ctx 是本次请求的生存期(取消则拒绝入队);work 是这批块的下载作用域,取消 work 即撤回
// 尚未完成的块(D62),须来自 PreheatScope。阶段绑定与在途保护按 work 走。
func (m *PrefetchManager) ForPreheat(ctx, work context.Context, dataset string, refs []prefetch.ChunkRef) error {
	if len(refs) == 0 {
		return nil
	}
	b := m.bundleFor(dataset, nil)
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.stageSeen {
		if _, err := b.prefetcher.OnStageSwitch(work, ordersource.ConsumeOrder{Dataset: dataset}); err != nil {
			return err
		}
		b.stageSeen = true
	}
	if !b.started {
		b.started = true
		b.prefetcher.Start(m.baseCtx)
	}
	if err := b.prefetcher.EnqueueP0Scoped(ctx, work, refs); err != nil {
		return err
	}
	// 入队成功即登记 P0 在途硬保护(§7.6;完成回调移出)。
	m.addP0Refs(dataset, refs)
	return nil
}

// ForListSnapshot 返回 dataset 当前快照的条目(ListSnapshot 查询入口):取/建 bundle
// (不加载前缀映射、不启动预取);Catalog.List 自身只下载相关分片(D29)。
func (m *PrefetchManager) ForListSnapshot(ctx context.Context, dataset, prefix string) ([]index.Entry, error) {
	return m.bundleFor(dataset, nil).catalog.List(ctx, prefix)
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
