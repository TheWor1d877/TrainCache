// Package prefetch 实现持久预取器 PersistentPrefetcher(design §5.2、§7.3;decisions D16/D25)。
//
// 结构:四个严格优先级队列(P0>P1>P2>P3)+ 单调度协程串行出队,每块一个抓取 goroutine
// (并发受 core fetch 字节信号量统一约束);Start 后维持 cursor+lookahead(默认 8)的 P2
// 邻域窗口,每完成一块推进 cursor 并上报 /metrics;阶段切换时旧阶段未出队块降级 P3、
// 新阶段开头 K 块以 P1 入队,返回"新窗口就绪"ETA(剩余未就绪字节 ÷ EMA 吞吐,α=0.3)。
// P0 为显式预热入队(EnqueueP0;D27):不参与阶段簿记,每块完成经 Config.OnP0Done 回调
// (在调度协程内同步调用,仅做记账,不得阻塞)。
//
// 类型 ChunkRef/ConsumeOrder 是 ordersource(M2 冻结)的类型别名,prefetch 只取类型、
// 不依赖读端;下载与簿记契约见主对话裁决(D25 对象键;不 Acquire/不 Pin——引用是写快照
// 方的簿记,Agent 只拉副本)。
package prefetch

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

// Config 是预取器配置(design §5.2 冻结;D27 增 OnP0Done)。
type Config struct {
	Lookahead int // 邻域预取提前量,默认 8(≤0 取默认;D16)
	// OnP0Done 是 P0 块完成回调(仅 P0 触发;D27):每块完成(本地就绪或下载成功 = nil,
	// 抓取失败 = 具体原因)调用一次,在调度协程内同步调用——实现只做轻量记账,不得阻塞;
	// nil 表示无回调。
	OnP0Done func(ref ChunkRef, err error)
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
	Stage      string   // 当前阶段标识
	Cursor     ChunkRef // 当前消费位点
	QueueDepth [4]int   // P0..P3 各队列长度
	HitRate    float64  // 预取命中率(窗口内消费时块已就绪的比例),供自适应 K 调参
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
	cursor  int // 已推进到的位置(-1 = 尚未推进)
	schedTo int // 邻域窗口水位:已入队过的最高位置(-1 = 未入队;单调防重复入队)

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

// task 是一次入队项:块在其归属阶段中的位置(P0 显式预热块无归属阶段,st == nil)。
type task struct {
	st  *stageState // 归属阶段(P0 为 nil:不参与阶段降级与游标簿记)
	idx int         // 阶段内位置(P0 无意义:-1)
	ref ordersource.ChunkRef
	// dataset 是抓取键(D25)所用的 dataset:阶段任务 = 所属阶段的 dataset,P0 = 入队
	// 时刻最近一次 ConsumeOrder 的 dataset 快照(见 EnqueueP0)。
	dataset string
	p0      bool // P0 显式预热块:完成经 Config.OnP0Done 回调
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
	wake    chan struct{}          // 调度协程唤醒信号(缓冲 1,入队时非阻塞投递)
	results chan taskResult        // 抓取结果回投调度协程通道(New 内创建,非阻塞投递)

	// 吞吐 EMA(ETA 用;mu 保护):bytes/s,0 = 无历史。
	ema   float64
	emaAt time.Time // 上次吞吐样本时刻
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

// OnConsumeSideAdvance DataLoader 钩子每消费完一块调用,推进 cursor;预取提前量 =
// cursor + lookahead(默认 8)。
//
// 实现口径:ref 在当前订单内 → cursor 置为该 ref 的位置(重复 ref 取首次出现;单调不回退,
// 已被预取推进超过时保持原位),并记一条 HitRate 样本(消费时刻该块是否已就绪);ref 不在
// 订单内(或尚无订单)→ 忽略并返回 nil。推进后按需补齐邻域窗口。
func (p *PersistentPrefetcher) OnConsumeSideAdvance(ctx context.Context, ref ChunkRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var (
		report bool
		chunk  int64
		depths [numPriorities]int
	)
	p.mu.Lock()
	st := p.stage
	if st == nil {
		p.mu.Unlock()
		return nil
	}
	idx, ok := st.pos[ref]
	if !ok {
		p.mu.Unlock()
		return nil
	}
	st.addHit(st.ready[idx])
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

	if report {
		p.reportCursor(chunk, depths)
	}
	return nil
}

// OnStageSwitch 阶段切换:旧阶段 cursor 之后未消费块降级 P3(在途请求不取消);
// 新阶段开头 K 块(K = 新 ConsumeOrder.prefetch_chunks)作为 P1 高优先级入队,剩余块转
// P2 邻域预取;返回"切换时刻到新窗口就绪"的预估时间,供 Agent 上报
// traincache_stage_switch_stall_seconds。
//
// 实现口径:降级作用于"已入队未出队"的旧阶段非 P0 任务(P0 = 显式预热,不参与降级);
// 剩余块在 K 块全部处理完后进入 P2 邻域窗口(§7.3);ETA = 新窗口 K 块中 Lookup+Open
// 未命中的块 Size 之和 ÷ 吞吐 EMA(α=0.3,"最近 30s"口径见 observeThroughputLocked),
// 无吞吐历史时返回 0;每次切换都以 eta.Seconds() 上报 traincache_stage_switch_stall_seconds。
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
	p.mu.Unlock()

	// 3) ETA 在锁外计算(块 Open 校验是 IO;按切换时刻的 EMA 快照)。
	eta = p.estimateETA(ctx, st, ema)
	p.reportStageSwitchStall(eta)
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	p.mu.Lock()
	dataset := p.dataset
	for _, r := range refs {
		p.enqueueLocked(&task{idx: -1, ref: r, dataset: dataset, p0: true}, priP0)
	}
	p.mu.Unlock()
	return nil
}

// Status 返回当前状态快照(design §5.2 冻结):阶段标识、消费位点(cursor 所在块)、
// 四队列深度与 HitRate(最近窗口内消费时块已就绪的比例;无样本为 0)。纯读、并发安全。
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
	if st.cursor >= 0 {
		s.Cursor = st.refs[st.cursor]
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

// reportStageSwitchStall 上报一次阶段切换停滞(ETA 秒数)。
func (p *PersistentPrefetcher) reportStageSwitchStall(eta time.Duration) {
	if p.m == nil {
		return
	}
	p.m.ObserveStageSwitchStall(eta.Seconds())
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

// enqueueLocked 入队并唤醒调度协程(wake 缓冲 1;已有待处理信号时不再投递)。
func (p *PersistentPrefetcher) enqueueLocked(t *task, pri int) {
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
	select {
	case p.results <- taskResult{t: t, size: size, ready: ready, err: err}:
	case <-ctx.Done():
		// 调度协程已随 ctx 退出:结果丢弃(不再记账、不回调)。
	}
}

// grabBlock 是默认抓取路径(design §5.2 内部约定;D25 对象键):Catalog.Lookup 命中 →
// cas.Open 成功 = 本地命中(不产生下载,直接视为就绪,计入吞吐);Open 返回 ErrNotFound →
// fetch.Fetch 下载(key = blockKey,expectSize = Entry.Size);其他 Open 错误 → 不抓取
// (重扫描/坏块修复属 M4)。下载后不调 Acquire/Pin(簿记契约:引用是写快照方的职责,
// Agent 只拉副本)。失败原因作 P0 完成回调的 err(阶段任务路径无回调,仅按 ready 记账)。
//
// 索引未命中/映射未加载(ErrNoCurrent)分两类口径:P0(显式预热)仍下载——预热先于订单
// 到达时映射未加载(且预热不切换映射),键由 (dataset, path, chunk) 直接构造,expectSize
// 未知按 0 记账(仅少占字节额度,不影响正确性);阶段任务(P1/P2/P3)不抓取——其映射由
// LoadPrefixes 背书,未命中即订单引用了不存在的块。
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
		br, oerr := p.cs.Open(entry.Fingerprint)
		if oerr == nil {
			_ = br.Close()
			return entry.Size, true, nil
		}
		if !errors.Is(oerr, cas.ErrNotFound) {
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
	if _, err := p.f.Fetch(ctx, blockKey(t.dataset, ref), entrySize); err != nil {
		return entrySize, false, err
	}
	return entrySize, true, nil
}

// blockKey 是数据块源站对象键(D25):<dataset>/blocks/<path>#<chunk>。
// path 约定不含 '#';chunk 为 int64 十进制。
func blockKey(dataset string, ref ChunkRef) string {
	return dataset + "/blocks/" + ref.Path + "#" + strconv.FormatInt(ref.Chunk, 10)
}

// finishTask 处理一次抓取结果并推进簿记(调度协程内串行调用):t.st 为块归属阶段(阶段
// 切换后在途完成仍记旧阶段,不干扰新阶段游标);仅当前阶段的完成触发邻域窗口补齐与
// /metrics 上报(SetPrefetchCursor + 四队列 SetPrefetchQueueDepth,冻结口径:每块完成推进
// cursor 时上报)。P0 块不参与阶段簿记:每块完成经 Config.OnP0Done 回调一次(err = 抓取
// 失败原因),并照常更新四队列深度 gauge;回调在调度协程内同步调用且不持内部锁(回调
// 内可安全调 Status 等只读方法),实现不得阻塞。
func (p *PersistentPrefetcher) finishTask(res taskResult) {
	t := res.t
	var (
		report bool
		chunk  int64
		depths [numPriorities]int
	)
	p.mu.Lock()
	if res.ready {
		if !t.p0 {
			t.st.ready[t.idx] = true
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
	}
	p.mu.Unlock()

	if report {
		p.reportCursor(chunk, depths)
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
