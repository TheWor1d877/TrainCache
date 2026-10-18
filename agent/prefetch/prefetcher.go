// Package prefetch 实现持久预取器 PersistentPrefetcher(design §5.2、§7.3;decisions D16/D25)。
//
// 结构:四个严格优先级队列(P0>P1>P2>P3)+ 单调度协程串行出队,每块一个抓取 goroutine
// (并发受 core fetch 字节信号量统一约束);Start 后维持 cursor+lookahead(默认 8)的 P2
// 邻域窗口,每完成一块推进 cursor 并上报 /metrics;阶段切换时旧阶段未出队块降级 P3、
// 新阶段开头 K 块以 P1 入队,返回"新窗口就绪"ETA(剩余未就绪字节 ÷ EMA 吞吐,α=0.3)。
// P0 为显式预热入队(EnqueueP0;D27):不参与阶段簿记,每块完成经 Config.OnP0Done 回调
// (在调度协程内同步调用,仅做记账,不得阻塞)。
//
// 完成回调与坏块回源(M4;D28):Config.OnDone 覆盖所有优先级队列的块完成(与 OnP0Done
// 同触发点);Config.OnCorrupt 在本地命中判定遇 cas.Open 返回 ErrCorrupt 时回源修复
// (Quarantine+RepairOne 的包装由接线阶段注入;修复成功且重新 Open 成功 → 就绪跳过下载,
// 否则按未命中走下载路径——下载会覆盖坏块)。
//
// 类型 ChunkRef/ConsumeOrder 是 ordersource(M2 冻结)的类型别名,prefetch 只取类型、
// 不依赖读端;下载与簿记契约见主对话裁决(D25 对象键;不 Acquire/不 Pin——引用是写快照
// 方的簿记,Agent 只拉副本)。
package prefetch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/index"
)

// ChunkRef 是块引用(ordersource 冻结类型别名;design §5.1/§5.3)。
type ChunkRef = ordersource.ChunkRef

// ConsumeOrder 是本阶段的消费顺序(ordersource 冻结类型别名;design §5.3/§6.1)。
type ConsumeOrder = ordersource.ConsumeOrder

// 队列优先级:数值小者先出队(design §5.2 严格优先级 P0>P1>P2>P3)。
const (
	priP0         = iota // P0:显式预热任务(EnqueueP0;D27——不参与阶段降级与游标簿记)
	priP1                // P1:阶段切换 K 启动窗口
	priP2                // P2:邻域持续预取
	priP3                // P3:旧阶段降级块
	numPriorities = 4
)

const (
	// defaultLookahead 是邻域预取提前量默认值(D16)。
	defaultLookahead = 8
	// throughputAlpha 是吞吐 EMA 平滑系数(D16:α=0.3)。
	throughputAlpha = 0.3
	// throughputWindow 是"最近 30s"吞吐窗口(design §5.2)的落地口径:相邻完成的间隔
	// 超过它视为空窗,历史 EMA 失效,从本次完成重新积累。
	throughputWindow = 30 * time.Second
	// hitWindowSize 是 Status.HitRate 的消费样本滑动窗口大小(design §5.2 未定死,取 100)。
	hitWindowSize = 100
	// resultBufSize 是抓取结果回投通道(调度协程)的缓冲:借以平滑"出队速率 ≫ 完成
	// 速率"时的投递,抓取协程在满时等待调度协程排空(不丢结果、不改优先级语义)。
	resultBufSize = 64
)

// Config 是预取器配置(design §5.2 冻结;D27 增 OnP0Done,D28 增 OnDone/OnCorrupt)。
type Config struct {
	Lookahead int // 邻域预取提前量,默认 8(≤0 取默认;D16)
	// SelfOpenGuard 记录 Agent 自身打开本地块的动作(D49 self-open guard):本地命中、
	// 下载后读回校验、ETA 探测等打开点在 Open 前 Mark,collector 的 inotify 兜底据此
	// 跳过对应 OPEN(避免把 Agent 自身读取误判为训练消费)。nil = 不记录。
	SelfOpenGuard *SelfOpenGuard
	// OnP0Done 是 P0 块完成回调(仅 P0 触发;D27):每块完成(本地就绪或下载成功 = nil,
	// 抓取失败 = 具体原因)调用一次,在调度协程内同步调用——实现只做轻量记账,不得阻塞;
	// nil 表示无回调。
	OnP0Done func(ref ChunkRef, err error)
	// OnDone 是所有优先级队列块完成回调(D28):每块完成(本地就绪/下载成功 = nil,下载
	// 失败 = 具体原因)触发一次,与 OnP0Done 同触发点(同一调度协程内同步调用,不得阻塞;
	// P0 块两回调并存,先 OnP0Done 后 OnDone);nil 表示无回调。collector 的 t_touch
	// "预取写入"来源。
	OnDone func(ref ChunkRef, err error)
	// OnCorrupt 是本地坏块回源回调(主对话追加冻结,并入 D28 语义):本地命中判定遇
	// cas.Open 返回 ErrCorrupt 时调用(在工作协程内,可阻塞;内部应为 Quarantine+RepairOne
	// 组合,接线阶段注入 rescan 包装)。返回 nil 且随后按索引指纹重新 Open 成功 → 视为就绪
	// (跳过下载);nil 回调 / 返回错误 / 重开失败 → 按未命中走下载路径(下载覆盖坏块)。
	// dataset 取任务归属(Catalog 绑定的数据集);nil 表示不回源修复。
	OnCorrupt func(ctx context.Context, dataset string, ref ChunkRef) error
	// OnStageSwitch 是阶段切换通知回调(M9/D46 接线钩子):OnStageSwitch 安装新阶段后
	// 调用一次(stageID = 新 ConsumeOrder.StageID);供 svc phaseTracker 迁移到 switch。
	// 在调度/调用方 goroutine 内同步调用,不得阻塞;nil 表示无回调。
	OnStageSwitch func(stageID string)
}

// withDefaults 返回补齐默认值的配置(≤0 回退 defaultLookahead)。
func (c Config) withDefaults() Config {
	if c.Lookahead <= 0 {
		c.Lookahead = defaultLookahead
	}
	return c
}

// Status 是预取器状态快照(design §5.2 冻结)。
type Status struct {
	Stage            string   // 当前阶段标识
	Cursor           ChunkRef // 纯消费位点:仅由 OnConsumeSideAdvance 推进
	PrefetchFrontier ChunkRef // 预取前沿:连续已完成就绪的最远块
	QueueDepth       [4]int   // P0..P3 各队列长度
	HitRate          float64  // 预取命中率(窗口内消费时块已就绪的比例),供自适应 K 调参
}

// stageState 是单个阶段的预取视图与统计。阶段切换后旧对象由在途 task 继续引用,
// 完成时按块归属记账(不回写当前阶段游标与指标;design §5.2"在途不取消")。
type stageState struct {
	dataset string
	stageID string
	refs    []ordersource.ChunkRef
	pos     map[ordersource.ChunkRef]int // ref → 位置(重复 ref 取首次出现)

	k       int // 启动窗口 K = clamp(prefetch_chunks, 0, len(refs))
	kDone   int // K 窗口中已处理块数;达 k 后开放 P2 邻域窗口(§7.3"K 块就绪后剩余块转 P2")
	cursor  int // 调度游标 = max(消费游标, 预取前沿)(-1 = 尚未推进)
	schedTo int // 邻域窗口水位:已入队过的最高位置(-1 = 未入队;单调防重复入队)

	// D48 双游标口径(design §5.2):consume 仅由 advance 钩子推进;front 为已完成就绪
	// 的最远块。lead_blocks = front − consume(负值 = 消费领先预取)。
	consume int
	front   int

	ready []bool // 位置已就绪(本地命中或下载完成;HitRate 判定口径)

	hitWin    [hitWindowSize]bool // 消费命中样本滑动窗口(环形)
	hitNext   int
	hitFilled int
}

// newStage 构造阶段状态:K 截断到 [0, len(chunks)];cursor/水位从 -1 起(尚无推进/入队)。
func newStage(o ordersource.ConsumeOrder) *stageState {
	st := &stageState{
		dataset: o.Dataset,
		stageID: o.StageID,
		refs:    o.Chunks,
		pos:     make(map[ordersource.ChunkRef]int, len(o.Chunks)),
		cursor:  -1,
		schedTo: -1,
		consume: -1,
		front:   -1,
		ready:   make([]bool, len(o.Chunks)),
	}
	for i, r := range o.Chunks {
		if _, dup := st.pos[r]; !dup {
			st.pos[r] = i
		}
	}
	st.k = o.PrefetchChunks
	if st.k < 0 {
		st.k = 0
	}
	if st.k > len(st.refs) {
		st.k = len(st.refs)
	}
	return st
}

// addHit 记一条消费就绪样本(环形窗口)。
func (st *stageState) addHit(hit bool) {
	st.hitWin[st.hitNext] = hit
	st.hitNext = (st.hitNext + 1) % hitWindowSize
	if st.hitFilled < hitWindowSize {
		st.hitFilled++
	}
}

// hitRate 返回窗口内就绪样本占比;无样本返回 0。
func (st *stageState) hitRate() float64 {
	if st.hitFilled == 0 {
		return 0
	}
	n := 0
	for i := 0; i < st.hitFilled; i++ {
		if st.hitWin[i] {
			n++
		}
	}
	return float64(n) / float64(st.hitFilled)
}

// leadLocked 返回 D48 预取领先块数 = 预取前沿 − 消费游标(调用方持 p.mu)。
// consume/front 均以位置表示,-1 = 尚无；两者都未推进时为 0。
func (st *stageState) leadLocked() int64 {
	return int64(st.front - st.consume)
}

// task 是一次入队项:块在其归属阶段中的位置(P0 显式预热块无归属阶段,st == nil)。
type task struct {
	st  *stageState // 归属阶段(P0 为 nil:不参与阶段降级与游标簿记)
	idx int         // 阶段内位置(P0 无意义:-1)
	ref ordersource.ChunkRef
	// dataset 是抓取键(D25)所用的 dataset:阶段任务 = 所属阶段的 dataset,P0 = 入队
	// 时刻最近一次 ConsumeOrder 的 dataset 快照(见 EnqueueP0)。
	dataset string
	p0      bool // P0 显式预热块:完成经 Config.OnP0Done 回调
	// reason 是回源打点 reason(D48):prefetch|demand|repair|other;空值按 prefetch
	// (P0–P3 常规预取)。Backfill 的 demand 入口显式置 FetchReasonDemand。
	reason string

	// pri/start 是 M9/D48 任务耗时打点口径:pri = 最终生效优先级(去重提升不重置
	// start;骨架阶段由 enqueueLocked 记录)。
	pri   int
	start time.Time
}

// taskResult 是一次抓取结果,回投调度协程:簿记(finishTask)与 P0 完成回调统一在调度
// 协程内串行执行(D27 回调"在调度协程内同步调用")。
type taskResult struct {
	t     *task
	size  int64
	ready bool  // 就绪(本地命中或下载成功;true 计吞吐)
	err   error // 失败原因(P0 回调的 err;就绪为 nil)
}

// PersistentPrefetcher 是持久预取器(design §5.2)。
type PersistentPrefetcher struct {
	cat *index.Catalog
	f   *fetch.Fetcher
	cs  *cas.Store
	m   *metrics.Registry
	cfg Config

	// 白盒测试钩子(非导出;生产路径为 nil / time.Now,行为与导出签名不受影响):
	// now 提供吞吐采样时间源;grab 替换单块抓取实现(默认 grabBlock 走契约路径)。
	now  func() time.Time
	grab func(ctx context.Context, dataset string, ref ChunkRef) (size int64, ready bool)

	mu      sync.Mutex
	started bool                   // Start 已调用(幂等)
	runDone chan struct{}          // 调度协程退出信号(Start 内创建;测试等待用)
	stage   *stageState            // 当前阶段(nil = 尚无订单)
	dataset string                 // 最近一次 OnStageSwitch 的 dataset(P0 入队快照用;mu 保护)
	queues  [numPriorities][]*task // P0..P3 FIFO 分片数组
	pending map[pendingKey]*task   // D45 去重表:覆盖 P0–P3 队列与运行中 task(mu 保护)
	wake    chan struct{}          // 调度协程唤醒信号(缓冲 1,入队时非阻塞投递)
	results chan taskResult        // 抓取结果回投调度协程通道(New 内创建,非阻塞投递)

	// 吞吐 EMA(ETA 用;mu 保护):bytes/s,0 = 无历史。
	ema   float64
	emaAt time.Time // 上次吞吐样本时刻

	// fetchInflightMu/fetchInflight 维护回源在途计数(traincache_fetch_inflight_bytes
	// 的本次口径 = 当前在途 Fetch 调用数,±1;见 reportFetchStart/End)。独立于调度锁 mu:
	// 下载路径不与调度/簿记锁交错;锁内"计数递增 + Set"保证并发下载下 gauge 终值恒定
	// (不会出现后写 Set 覆盖为旧值)。
	fetchInflightMu sync.Mutex
	fetchInflight   int64
}

// New 构造持久预取器(design §5.2 冻结签名)。cat/f/cs/m 分别提供索引查找、下载编排、
// 本地块存取与指标注册(m 允许为 nil,用于无指标场景);cfg 零值字段回退默认。
// 构造后经 OnStageSwitch 注入消费顺序、Start 启动调度;Start 前入队的任务只在 Start 后出队。
func New(cat *index.Catalog, f *fetch.Fetcher, cs *cas.Store,
	m *metrics.Registry, cfg Config) *PersistentPrefetcher {
	return &PersistentPrefetcher{
		cat:     cat,
		f:       f,
		cs:      cs,
		m:       m,
		cfg:     cfg.withDefaults(),
		now:     time.Now,
		pending: make(map[pendingKey]*task),
		wake:    make(chan struct{}, 1),
		results: make(chan taskResult, resultBufSize),
	}
}

// Start 开始持续预取:从 cursor 起按当前 ConsumeOrder 邻域预取,每完成一块推进 cursor
// 并上报 /metrics。可被显式启动预取(P0)与阶段切换重预取(P1)打断:在途请求不取消,
// 新请求按更高优先级入队。
//
// 实现口径(design §5.2 未明文处):Start 启动后台调度协程后立即返回,ctx 取消即退出;
// 重复调用无副作用(幂等,首个 ctx 有效)。尚无订单时先启动调度,订单到达后自然生效。
func (p *PersistentPrefetcher) Start(ctx context.Context) {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return
	}
	p.started = true
	p.runDone = make(chan struct{})
	p.ensureWindowLocked()
	p.mu.Unlock()
	go p.run(ctx)
}

// OnConsumeSideAdvance DataLoader 钩子每消费完一块调用,推进消费位点;预取提前量 =
// consume + lookahead(默认 8)。
//
// 返回是否真正推进了消费位点:ref 不在当前订单内、ctx 已取消、尚无阶段,或 idx <= consume
// (重复/回退的消费事件)→ false,且不产生任何副作用(包括不计入 HitRate 窗口)。真正推进时
// 置 consume = idx(单调不回退)、记一条 HitRate 样本、上报 lead_blocks,并按需推进调度游标
// 与邻域窗口,返回 true。
func (p *PersistentPrefetcher) OnConsumeSideAdvance(ctx context.Context, ref ChunkRef) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	var (
		report  bool
		leads   int64
		chunk   int64
		depths  [numPriorities]int
		dataset string
		hitRate float64
	)
	p.mu.Lock()
	st := p.stage
	if st == nil {
		p.mu.Unlock()
		return false
	}
	idx, ok := st.pos[ref]
	if !ok {
		p.mu.Unlock()
		return false
	}
	// 去重:已消费过的位点不推进、不计命中样本(重复 inotify 事件不得污染窗口)。
	if idx <= st.consume {
		p.mu.Unlock()
		return false
	}
	// D48:消费游标只由 advance 钩子推进(与调度游标解耦)。
	st.consume = idx
	st.addHit(st.ready[idx])
	dataset, hitRate = st.dataset, st.hitRate()
	leads = st.leadLocked()
	if idx > st.cursor {
		st.cursor = idx
		p.ensureWindowLocked()
		report = true
		chunk = st.refs[st.cursor].Chunk
		for i := range p.queues {
			depths[i] = len(p.queues[i])
		}
	}
	p.mu.Unlock()

	if p.m != nil {
		p.m.SetPrefetchHitRatio(dataset, hitRate)
	}
	if report {
		p.reportCursor(chunk, depths)
	}
	p.reportLeadBlocks(dataset, leads)
	return true
}

// OnStageSwitch 阶段切换:旧阶段 cursor 之后未消费块降级 P3(在途请求不取消);
// 新阶段开头 K 块(K = 新 ConsumeOrder.prefetch_chunks)作为 P1 高优先级入队,剩余块转
// P2 邻域预取;返回"切换时刻到新窗口就绪"的预估时间,供 Agent 上报
// traincache_stage_switch_eta_seconds。
//
// 实现口径:降级作用于"已入队未出队"的旧阶段非 P0 任务(P0 = 显式预热,不参与降级);
// 剩余块在 K 块全部处理完后进入 P2 邻域窗口(§7.3);ETA = 新窗口 K 块中 Lookup+Open
// 未命中的块 Size 之和 ÷ 吞吐 EMA(α=0.3,"最近 30s"口径见 observeThroughputLocked),
// 无吞吐历史时返回 0;每次切换都以 eta.Seconds() 上报 traincache_stage_switch_eta_seconds。
func (p *PersistentPrefetcher) OnStageSwitch(ctx context.Context, next ConsumeOrder) (eta time.Duration, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// 1) 旧阶段队列中未出队的非 P0 任务整体降级 P3(在途任务不在队列中,天然不受影响)。
	p.mu.Lock()
	var demoted []*task
	for pri := priP1; pri <= priP3; pri++ {
		demoted = append(demoted, p.queues[pri]...)
		p.queues[pri] = nil
	}

	// 2) 新阶段:K 块以 P1 入队;K == 0 时邻域窗口立即开放(ensureWindowLocked)。
	st := newStage(next)
	p.stage = st
	p.dataset = next.Dataset // P0 入队快照的 dataset 来源(仅更新,不影响旧阶段在途)
	for i := 0; i < st.k; i++ {
		p.enqueueLocked(&task{st: st, idx: i, ref: st.refs[i], dataset: st.dataset}, priP1)
	}
	st.schedTo = st.k - 1
	for _, t := range demoted {
		p.enqueueLocked(t, priP3)
	}
	p.ensureWindowLocked()
	ema := p.ema
	dataset := st.dataset
	lead := st.leadLocked() // 新阶段双游标均为 -1 → 0
	p.mu.Unlock()

	// M9/D46:阶段切换通知(phaseTracker 迁移;在锁外调用,回调可安全读 Status)。
	if p.cfg.OnStageSwitch != nil {
		p.cfg.OnStageSwitch(next.StageID)
	}

	// 3) ETA 在锁外计算(块 Open 校验是 IO;按切换时刻的 EMA 快照)。
	eta = p.estimateETA(ctx, st, ema)
	p.reportStageSwitchStall(dataset, eta)
	p.reportLeadBlocks(dataset, lead)
	return eta, nil
}

// EnqueueP0 显式预热入队(D27):refs 按入参顺序以 P0 严格最高优先级入队,在途请求不
// 取消;每块完成(本地就绪或下载成败)经 Config.OnP0Done 回调一次(若有)。空列表
// 返回 nil;ctx 已取消返回 ctx.Err() 且不入队。
//
// 实现口径:P0 块不参与阶段簿记(不推进游标/邻域窗口),完成时仍照常更新队列深度 gauge。
// 抓取键(D25)的 dataset 取入队时刻最近一次 ConsumeOrder 的 dataset(OnStageSwitch 绑定;
// 尚无订单则为空——此时未本地命中的块无法构造下载键,以错误回调)。
// Start 前入队的块在 Start 后出队处理(与 OnStageSwitch 同口径)。
func (p *PersistentPrefetcher) EnqueueP0(ctx context.Context, refs []ChunkRef) error {
	return p.enqueueP0(ctx, refs, metrics.FetchReasonPrefetch)
}

// EnqueueP0Demand 是 demand(按需补块)的 P0 入队入口:语义与 EnqueueP0 完全一致
// (严格最高优先级、在途不取消、完成仍经 OnP0Done),仅把回源 reason 标为 demand。
// 复用同一队列与 pending 去重表,不改变冻结的 pending 语义。
func (p *PersistentPrefetcher) EnqueueP0Demand(ctx context.Context, refs []ChunkRef) error {
	return p.enqueueP0(ctx, refs, metrics.FetchReasonDemand)
}

func (p *PersistentPrefetcher) enqueueP0(ctx context.Context, refs []ChunkRef, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	p.mu.Lock()
	dataset := p.dataset
	for _, r := range refs {
		p.enqueueLocked(&task{idx: -1, ref: r, dataset: dataset, p0: true, reason: reason}, priP0)
	}
	p.mu.Unlock()
	return nil
}

// fetchReason 返回本 task 的回源 reason(D48;空值 = 常规预取)。
func (t *task) fetchReason() string {
	if t.reason == "" {
		return metrics.FetchReasonPrefetch
	}
	return t.reason
}

// Status 返回当前状态快照(design §5.2 冻结):阶段标识、纯消费位点(consume 所在块)、
// 预取前沿(front 所在块)、四队列深度与 HitRate(最近窗口内消费时块已就绪的比例;无样本为 0)。
// 不暴露调度游标(cursor)。纯读、并发安全。
func (p *PersistentPrefetcher) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	var s Status
	for i := range p.queues {
		s.QueueDepth[i] = len(p.queues[i])
	}
	st := p.stage
	if st == nil {
		return s
	}
	s.Stage = st.stageID
	if st.consume >= 0 {
		s.Cursor = st.refs[st.consume]
	}
	if st.front >= 0 {
		s.PrefetchFrontier = st.refs[st.front]
	}
	s.HitRate = st.hitRate()
	return s
}

// reportCursor 上报游标与四队列深度(M3 打点;design §5.8 family)。
func (p *PersistentPrefetcher) reportCursor(chunk int64, depths [numPriorities]int) {
	if p.m == nil {
		return
	}
	p.m.SetPrefetchCursor(chunk)
	p.reportQueueDepth(depths)
}

// reportQueueDepth 上报四队列深度(P0 完成不推进游标,但深度 gauge 照常更新)。
func (p *PersistentPrefetcher) reportQueueDepth(depths [numPriorities]int) {
	if p.m == nil {
		return
	}
	for i, d := range depths {
		p.m.SetPrefetchQueueDepth(i, d)
	}
}

// reportStageSwitchStall 上报一次阶段切换停滞(ETA 秒数;D46 起带当前 dataset 标签)。
func (p *PersistentPrefetcher) reportStageSwitchStall(dataset string, eta time.Duration) {
	if p.m == nil {
		return
	}
	p.m.ObserveStageSwitchStall(eta.Seconds(), dataset)
}

// reportFetchStart / reportFetchEnd 维护并上报回源在途请求数(回源线②:traincache_fetch_
// inflight_bytes;本次口径 = 当前在途 Fetch 调用数,±1——family 名冻结为 bytes,gauge 语义
// 见 DECISIONS 回传)。m == nil 时整体跳过(计数与 gauge 同源,m==nil 时两者都不维护,
// 不会漂移);锁内连计数带 Set,多个下载 goroutine 并发时终值恒为 0。
func (p *PersistentPrefetcher) reportFetchStart(dataset string) {
	if p.m == nil {
		return
	}
	p.fetchInflightMu.Lock()
	defer p.fetchInflightMu.Unlock()
	p.fetchInflight++
	p.m.SetFetchInflightBytes(dataset, p.fetchInflight)
}

func (p *PersistentPrefetcher) reportFetchEnd(dataset string) {
	if p.m == nil {
		return
	}
	p.fetchInflightMu.Lock()
	defer p.fetchInflightMu.Unlock()
	p.fetchInflight--
	p.m.SetFetchInflightBytes(dataset, p.fetchInflight)
}

// markSelfOpen 在 Agent 自身读取本地块前登记指纹(D49 self-open guard;nil 安全)。
func (p *PersistentPrefetcher) markSelfOpen(fp cas.Fingerprint) {
	if p.cfg.SelfOpenGuard != nil {
		p.cfg.SelfOpenGuard.Mark(fp)
	}
}

// reportFetchSuccess 上报一次成功回源(回源线②):fetch_bytes_total{dataset,success,reason}
// += 实际交付字节 + fetch_requests_total{dataset,success,reason} +1(reason 由 task 携带:
// 常规预取 prefetch、按需补块 demand;repair 由 rescan.RepairOne 自记)。字节口径 = Fetch 返回指纹
// 的 cas.Open().Size()(Fetch 已读回校验,Size 即真实块数据字节);Open 失败(理论上
// 不应发生,防御)回退 fallbackSize(索引 Size / P0 的 expectSize,可能为 0 → 不计字节)。
func (p *PersistentPrefetcher) reportFetchSuccess(dataset string, fp cas.Fingerprint, fallbackSize int64, reason string) {
	if p.m == nil {
		return
	}
	n := fallbackSize
	p.markSelfOpen(fp) // D49:读回校验打开前登记
	if br, err := p.cs.Open(fp); err == nil {
		n = br.Size()
		_ = br.Close()
	}
	p.m.AddFetchBytes(dataset, n, reason)
	p.m.IncFetchRequests(dataset, metrics.ResultSuccess, reason)
}

// reportFetchError 上报一次失败回源(回源线②):fetch_requests_total{dataset,error,reason} +1
// (失败不产生字节,不触碰 fetch_bytes)。
func (p *PersistentPrefetcher) reportFetchError(dataset, reason string) {
	if p.m == nil {
		return
	}
	p.m.IncFetchRequests(dataset, metrics.ResultError, reason)
}

// estimateETA 计算新窗口(K 块)就绪 ETA(D16):剩余未就绪字节 ÷ 吞吐 EMA(ema 为切换
// 时刻快照)。就绪判定与抓取一致:Catalog.Lookup 命中且 cas.Open 成功;Lookup 未命中
// (或索引尚无快照)的块既无 Size 也不产生下载,不计入字节。无吞吐历史(ema<=0)或全部
// 就绪时返回 0(口径:不上报无依据的估计)。
func (p *PersistentPrefetcher) estimateETA(ctx context.Context, st *stageState, ema float64) time.Duration {
	if ema <= 0 {
		return 0
	}
	var remaining int64
	for i := 0; i < st.k; i++ {
		entry, ok, err := p.cat.Lookup(ctx, st.refs[i].Path, st.refs[i].Chunk)
		if err != nil || !ok {
			continue
		}
		p.markSelfOpen(entry.Fingerprint) // D49:ETA 就绪探测打开
		br, err := p.cs.Open(entry.Fingerprint)
		if err == nil {
			_ = br.Close()
			continue
		}
		remaining += entry.Size
	}
	if remaining <= 0 {
		return 0
	}
	return time.Duration(float64(remaining) / ema * float64(time.Second))
}

// ensureWindowLocked 补齐当前阶段的 P2 邻域窗口(design §5.2/§7.3):起点 =
// max(已入队水位, cursor)+1,终点 = min(cursor+lookahead, len-1);K 启动窗口未全部处理完
// (kDone < k)时不开放邻域(§7.3"K 块就绪后剩余块转 P2")。幂等:水位单调不回退,
// cursor 跳跃(advance 大幅前进)时途经位置视为已消费、不再入队。
func (p *PersistentPrefetcher) ensureWindowLocked() {
	st := p.stage
	if st == nil || st.kDone < st.k {
		return
	}
	start := st.schedTo
	if st.cursor > start {
		start = st.cursor
	}
	end := st.cursor + p.cfg.Lookahead
	if end > len(st.refs)-1 {
		end = len(st.refs) - 1
	}
	for i := start + 1; i <= end; i++ {
		p.enqueueLocked(&task{st: st, idx: i, ref: st.refs[i], dataset: st.dataset}, priP2)
	}
	if end > st.schedTo {
		st.schedTo = end
	}
}

// enqueueLocked 经 D45 pending 去重表入队并唤醒调度协程(wake 缓冲 1;已有待处理信号
// 时不再投递)。action 语义:"enqueue" = 新任务已追加;"promote"/"ignore" = 去重表已
// 处理(骨架阶段 enqueueDedupLocked 恒返回 "enqueue",保持既有行为)。
func (p *PersistentPrefetcher) enqueueLocked(t *task, pri int) {
	// 去重提升不重置起点(D48);骨架阶段这里只记录首次入队参数。
	if t.start.IsZero() {
		t.start = p.now()
	}
	t.pri = pri
	if action := p.enqueueDedupLocked(t, pri); action != "enqueue" {
		return
	}
	p.queues[pri] = append(p.queues[pri], t)
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// popTaskLocked 按严格优先级(P0>P1>P2>P3)出队,同级 FIFO;全空返回 (nil, 0)。
// 调度协程串行调用(design §5.2"调度协程串行出队")。
func (p *PersistentPrefetcher) popTaskLocked() (*task, int) {
	for pri := priP0; pri < numPriorities; pri++ {
		if q := p.queues[pri]; len(q) > 0 {
			p.queues[pri] = q[1:]
			return q[0], pri
		}
	}
	return nil, 0
}

// run 是调度协程:串行出队,每块一个抓取 goroutine 并发执行(并发受 core fetch 的
// 字节/任务/主机信号量统一约束,design §5.2 内部约定);抓取结果经 results 回投,
// 记账(finishTask)与 P0 完成回调在调度协程内串行执行(D27)。ctx 取消即退出。
func (p *PersistentPrefetcher) run(ctx context.Context) {
	defer close(p.runDone)
	for {
		if ctx.Err() != nil {
			return
		}
		// 优先排空已完成结果(记账/回调不受出队空窗影响),无结果再出队/等待。
		select {
		case res := <-p.results:
			p.finishTask(res)
			continue
		default:
		}
		p.mu.Lock()
		t, _ := p.popTaskLocked()
		p.mu.Unlock()
		if t == nil {
			select {
			case <-ctx.Done():
				return
			case res := <-p.results:
				p.finishTask(res)
			case <-p.wake:
			}
			continue
		}
		go p.process(ctx, t)
	}
}

// process 在独立 goroutine 内抓取单块(IO 与调度解耦),结果经 results 回投调度协程。
// grab 为白盒测试钩子(非导出,签名保持既有测试不变),nil 时走默认契约路径 grabBlock。
func (p *PersistentPrefetcher) process(ctx context.Context, t *task) {
	grab := p.grab
	var (
		size  int64
		ready bool
		err   error
	)
	if grab != nil {
		size, ready = grab(ctx, t.dataset, t.ref)
		if !ready {
			err = errors.New("prefetch: 块未就绪(白盒抓取钩子)")
		}
	} else {
		size, ready, err = p.grabBlock(ctx, t)
	}
	// D45:task 进入终态(成功或失败终止)后移除去重表(失败重试由 core fetch 在任务内部
	// 保留;身份安全清理,不误删跨阶段替换后的新 task 键)。
	p.mu.Lock()
	p.clearPendingTaskLocked(t)
	p.mu.Unlock()
	select {
	case p.results <- taskResult{t: t, size: size, ready: ready, err: err}:
	case <-ctx.Done():
		// 调度协程已随 ctx 退出:结果丢弃(不再记账、不回调)。
	}
}

// grabBlock 是默认抓取路径(design §5.2 内部约定;D25 对象键):Catalog.Lookup 命中 →
// cas.Open 成功 = 本地命中(不产生下载,直接视为就绪,计入吞吐);Open 返回 ErrNotFound →
// fetch.Fetch 下载(key = blockKey,expectSize = Entry.Size)。下载后不调 Acquire/Pin
// (簿记契约:引用是写快照方的职责,Agent 只拉副本)。失败原因作完成回调的 err。
//
// 坏块(M4;D28):Open 返回 ErrCorrupt 时,若 Config.OnCorrupt 非 nil 则调用它(内部为
// Quarantine+RepairOne 包装,在工作协程内执行,可阻塞);返回 nil 且按索引指纹重新 Open
// 成功 → 视为就绪(跳过下载);nil 回调 / 修复失败 / 重开失败 → 按未命中走下载路径
// (下载会覆盖坏块)。其他 Open 错误(IO 等)维持原口径:仅返回错误、不下载。
//
// 索引未命中/映射未加载(ErrNoCurrent)分两类口径:P0(显式预热)仍下载——预热先于订单
// 到达时映射未加载(且预热不切换映射),键由 (dataset, path, chunk) 直接构造,expectSize
// 未知按 0 记账(仅少占字节额度,不影响正确性);阶段任务(P1/P2/P3)不抓取——其映射由
// LoadPrefixes 背书,未命中即订单引用了不存在的块。
//
// 回源线打点(②):仅真实下载路径打点(本地命中/修复就绪跳过不打点)——下载前
// reportFetchStart(在途 +1)、下载后 reportFetchEnd(在途 -1),成功计
// fetch_bytes+fetch_requests{success,reason},失败计 fetch_requests{error,reason}
// (reason 取 task:常规预取 prefetch、按需补块 demand;见 reportFetch* 注释)。
func (p *PersistentPrefetcher) grabBlock(ctx context.Context, t *task) (size int64, ready bool, err error) {
	ref := t.ref
	var entrySize int64
	entry, ok, err := p.cat.Lookup(ctx, ref.Path, ref.Chunk)
	switch {
	case err != nil:
		if !t.p0 {
			return 0, false, err
		}
		// P0:映射未加载不拦下载(D25 键无需索引)。
	case ok:
		entrySize = entry.Size
		p.markSelfOpen(entry.Fingerprint) // D49:本地命中探测打开
		br, oerr := p.cs.Open(entry.Fingerprint)
		if oerr == nil {
			_ = br.Close()
			return entry.Size, true, nil
		}
		switch {
		case errors.Is(oerr, cas.ErrCorrupt):
			// 坏块回源(D28):修复成功且重开成功 → 就绪;否则落到下载路径(覆盖坏块)。
			if p.cfg.OnCorrupt != nil {
				if cerr := p.cfg.OnCorrupt(ctx, t.dataset, ref); cerr == nil {
					p.markSelfOpen(entry.Fingerprint) // D49:修复后重开
					if br2, oerr2 := p.cs.Open(entry.Fingerprint); oerr2 == nil {
						_ = br2.Close()
						return entry.Size, true, nil
					}
				}
			}
		case !errors.Is(oerr, cas.ErrNotFound):
			return entry.Size, false, oerr
		}
	default: // 索引无此条目
		if !t.p0 {
			return 0, false, fmt.Errorf("prefetch: 块 %s#%d 不在当前索引", ref.Path, ref.Chunk)
		}
	}

	if t.dataset == "" {
		// 尚无 ConsumeOrder 绑定 dataset(D25 键需要 dataset):只在本地命中时就绪。
		return entrySize, false, errors.New("prefetch: dataset 未绑定(尚无 ConsumeOrder),无法构造下载键")
	}
	reason := t.fetchReason()
	p.reportFetchStart(t.dataset)
	fp, ferr := p.f.Fetch(ctx, string(blockKey(t.dataset, ref)), entrySize)
	p.reportFetchEnd(t.dataset)
	if ferr != nil {
		p.reportFetchError(t.dataset, reason)
		return entrySize, false, ferr
	}
	p.reportFetchSuccess(t.dataset, fp, entrySize, reason)
	return entrySize, true, nil
}

// finishTask 处理一次抓取结果并推进簿记(调度协程内串行调用):t.st 为块归属阶段(阶段
// 切换后在途完成仍记旧阶段,不干扰新阶段游标);仅当前阶段的完成触发邻域窗口补齐与
// /metrics 上报(SetPrefetchCursor + 四队列 SetPrefetchQueueDepth,冻结口径:每块完成推进
// cursor 时上报)。完成回调(D27/D28):P0 块经 Config.OnP0Done(仅 P0),所有块经
// Config.OnDone(与 OnP0Done 同触发点,先 OnP0Done 后 OnDone),err = 抓取失败原因
// (就绪为 nil);回调在调度协程内同步调用且不持内部锁(回调内可安全调 Status 等只读
// 方法),实现不得阻塞。
func (p *PersistentPrefetcher) finishTask(res taskResult) {
	t := res.t
	// D48 任务耗时:进入终态时 observe 一次(优先取 task 最终生效优先级;骨架阶段
	// 计时源已接通,时长取真实起点)。
	if p.m != nil && !t.start.IsZero() {
		p.m.ObservePrefetchTaskDuration(t.dataset, priorityLabel(t.pri), p.now().Sub(t.start).Seconds())
	}
	var (
		report bool
		lead   bool
		leads  int64
		chunk  int64
		depths [numPriorities]int
	)
	p.mu.Lock()
	if res.ready {
		if !t.p0 {
			t.st.ready[t.idx] = true
			if t.idx > t.st.front {
				t.st.front = t.idx // D48:预取前沿 = 已完成就绪的最远块
			}
		}
		// 吞吐口径含本地命中与回源下载:每完成一块按其 Size 计入(design §5.2 ETA)。
		p.observeThroughputLocked(res.size, p.now())
	}
	if t.p0 {
		for i := range p.queues {
			depths[i] = len(p.queues[i])
		}
		p.mu.Unlock()
		p.reportQueueDepth(depths)
		if p.cfg.OnP0Done != nil {
			p.cfg.OnP0Done(t.ref, res.err)
		}
		p.notifyDone(t.ref, res.err)
		return
	}
	st := t.st
	if t.idx < st.k {
		st.kDone++
	}
	if t.idx > st.cursor {
		st.cursor = t.idx
	}
	if st == p.stage {
		p.ensureWindowLocked()
		report = true
		chunk = st.refs[st.cursor].Chunk
		for i := range p.queues {
			depths[i] = len(p.queues[i])
		}
		// D48 预取领先块数:仅当前阶段的就绪完成推进前沿(旧阶段在途完成不回写)。
		if res.ready {
			lead = true
			leads = st.leadLocked()
		}
	}
	p.mu.Unlock()

	if report {
		p.reportCursor(chunk, depths)
	}
	if lead {
		p.reportLeadBlocks(st.dataset, leads)
	}
	p.notifyDone(t.ref, res.err)
}

// priorityLabel 把内部优先级 0..3 映射为指标标签 P0..P3(D48;越界回退 P0)。
func priorityLabel(pri int) string {
	if pri < 0 || pri >= numPriorities {
		return "P0"
	}
	return [...]string{"P0", "P1", "P2", "P3"}[pri]
}

// reportLeadBlocks 上报预取前沿与消费游标的块数差(D48)。
func (p *PersistentPrefetcher) reportLeadBlocks(dataset string, n int64) {
	if p.m == nil {
		return
	}
	p.m.SetPrefetchLeadBlocks(dataset, n)
}

// notifyDone 触发通用完成回调(D28;调度协程内同步调用,不持内部锁,不得阻塞)。
func (p *PersistentPrefetcher) notifyDone(ref ChunkRef, err error) {
	if p.cfg.OnDone != nil {
		p.cfg.OnDone(ref, err)
	}
}

// observeThroughputLocked 以"块完成"为样本更新吞吐 EMA(bytes/s;α=0.3,D16)。速率样本 =
// 本次块字节 ÷ 距上次完成的间隔;首个完成只记时刻(尚无速率可言,等待下一次完成);间隔
// 超过 throughputWindow 视为空窗,丢弃历史重新积累。EMA 保持 0 = 无历史(ETA 返回 0)。
func (p *PersistentPrefetcher) observeThroughputLocked(size int64, at time.Time) {
	switch {
	case size <= 0:
		return
	case p.emaAt.IsZero() || at.Sub(p.emaAt) > throughputWindow:
		p.ema = 0
		p.emaAt = at
	case at.Sub(p.emaAt) <= 0:
		p.emaAt = at // 时钟未前进(测试假钟/极端并发):不给速率样本,避免除零
	default:
		rate := float64(size) / at.Sub(p.emaAt).Seconds()
		if p.ema == 0 {
			p.ema = rate
		} else {
			p.ema = throughputAlpha*rate + (1-throughputAlpha)*p.ema
		}
		p.emaAt = at
	}
}
