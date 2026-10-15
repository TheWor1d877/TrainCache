package prefetch

// 本文件是 prefetch 的白盒/集成测试(7.2 §5 任务 2 场景):契约路径用真实 core 组合
// (objectstore.Mock + cas.Store + fetch.Fetcher + index.Catalog)验证,时序敏感场景经
// 非导出钩子(grab/now)注入逐块放行闸与假时钟,断言不依赖 goroutine 调度顺序;
// 等待一律带 deadline 轮询,不用裸 sleep。

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/agent/metrics"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// testTimeout 是轮询/等待类断言的统一 deadline。
const testTimeout = 3 * time.Second

// testDataset 是所有用例使用的数据集名(源站键与索引共用)。
const testDataset = "ds"

// stageSwitchStallName 与实现侧 family 名一致(独立字面量,防改名后测试跟着漂移)。
const stageSwitchStallName = "traincache_stage_switch_stall_seconds"

// waitFor 带 deadline 轮询 cond(不用裸 sleep 断言;poll 间隔 2ms)。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("超时未满足: %s(deadline %s)", what, testTimeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// countingStore 包装 objectstore.Mock,记录 Get 调用的对象键(断言"本地命中零下载"
// 与 D25 键布局用)。
type countingStore struct {
	*objectstore.Mock
	mu   sync.Mutex
	keys []string
}

func newCountingStore() *countingStore { return &countingStore{Mock: objectstore.NewMock()} }

func (s *countingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.Mock.Get(ctx, key, w)
}

func (s *countingStore) getKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.keys)
}

// blockGets 返回源站 Get 中属于数据块(D25 键 <dataset>/blocks/...)的键;
// 索引元数据(manifest/shard/CURRENT)的读取是 Catalog 正常行为,不计入"下载"。
func blockGets(s *countingStore) []string {
	var out []string
	for _, k := range s.getKeys() {
		if strings.HasPrefix(k, testDataset+"/blocks/") {
			out = append(out, k)
		}
	}
	return out
}

var _ objectstore.Store = (*countingStore)(nil)

// expectedBlockKey 按 D25 字面量构造数据块源站对象键(<dataset>/blocks/<path>#<chunk>),
// 独立于实现,防键布局漂移。
func expectedBlockKey(dataset string, ref ChunkRef) string {
	return dataset + "/blocks/" + ref.Path + "#" + strconv.FormatInt(ref.Chunk, 10)
}

// blockSpec 描述一个测试块:内容(决定指纹/大小)+ 本地与源站可达性组合。
type blockSpec struct {
	path    string
	chunk   int64
	content string
	local   bool // true = 提交后留在本地 CAS(本地命中);false = Evict(需下载)
	source  bool // true = 内容放入源站对象(D25 键),供下载路径使用
}

// testEnv 是测试装置:计数 Mock 源站 + 临时目录 CAS + fetch.Fetcher + 已 Refresh 的
// Catalog + metrics 注册表。
type testEnv struct {
	store *countingStore
	cs    *cas.Store
	fetch *fetch.Fetcher
	cat   *index.Catalog
	reg   *metrics.Registry
	fps   map[ChunkRef]cas.Fingerprint // ref → 指纹(断言本地落盘用)
}

// newTestEnv 构造测试装置:逐块 cas.Put(Commit 验证要求块已先行落盘)→ Commit →
// Catalog.Refresh;再按 specs 放置源站对象 / Evict 本地副本。无 specs 时 Catalog 保持
// 空快照(Lookup 返回 ErrNoCurrent)。
func newTestEnv(t *testing.T, specs []blockSpec) *testEnv {
	t.Helper()
	ctx := context.Background()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	env := &testEnv{
		store: newCountingStore(),
		cs:    cs,
		reg:   metrics.New(metrics.Config{}),
		fps:   make(map[ChunkRef]cas.Fingerprint),
	}
	env.fetch = fetch.New(env.store, cs, fetch.Options{
		MaxInflightBytes: 1 << 20,
		MaxTasks:         4,
		MaxConnsPerHost:  2,
		MaxRetries:       1,
	})
	env.cat = index.NewCatalog(env.store, testDataset, index.Config{})
	if len(specs) == 0 {
		return env
	}

	entries := make([]index.Entry, 0, len(specs))
	for _, sp := range specs {
		fp, n, err := cs.Put(ctx, strings.NewReader(sp.content))
		if err != nil {
			t.Fatalf("cas.Put(%s#%d): %v", sp.path, sp.chunk, err)
		}
		entries = append(entries, index.Entry{Path: sp.path, Chunk: sp.chunk, Size: n, Fingerprint: fp})
		env.fps[ChunkRef{Path: sp.path, Chunk: sp.chunk}] = fp
	}
	w := index.NewWriter(env.store, cs, index.Config{})
	if _, err := w.Commit(ctx, testDataset, entries); err != nil {
		t.Fatalf("index Commit: %v", err)
	}
	if err := env.cat.Refresh(ctx); err != nil {
		t.Fatalf("catalog Refresh: %v", err)
	}
	for _, sp := range specs {
		ref := ChunkRef{Path: sp.path, Chunk: sp.chunk}
		if sp.source {
			if _, err := env.store.Put(ctx, expectedBlockKey(testDataset, ref), strings.NewReader(sp.content), int64(len(sp.content))); err != nil {
				t.Fatalf("源站 Put(%s): %v", ref.Path, err)
			}
		}
		if !sp.local {
			if err := cs.Evict(env.fps[ref]); err != nil {
				t.Fatalf("cas.Evict(%s#%d): %v", ref.Path, ref.Chunk, err)
			}
		}
	}
	return env
}

func (env *testEnv) newPrefetcher(cfg Config) *PersistentPrefetcher {
	return New(env.cat, env.fetch, env.cs, env.reg, cfg)
}

// orderOf 组装测试消费顺序(字段语义见 design §6.1)。
func orderOf(stageID string, refs []ChunkRef, k int) ConsumeOrder {
	return ConsumeOrder{
		Dataset:        testDataset,
		SnapshotID:     "snap-test",
		StageID:        stageID,
		BatchSize:      32,
		PrefetchChunks: k,
		Chunks:         refs,
	}
}

// refsNamed 生成 n 个块引用:path = prefix-NN.tar,chunk = 0。
func refsNamed(prefix string, n int) []ChunkRef {
	refs := make([]ChunkRef, 0, n)
	for i := 0; i < n; i++ {
		refs = append(refs, ChunkRef{Path: fmt.Sprintf("%s-%02d.tar", prefix, i)})
	}
	return refs
}

// startPrefetcher 启动预取器调度;t.Cleanup 兜底取消 ctx 并等待调度协程退出(不泄漏)。
func startPrefetcher(t *testing.T, p *PersistentPrefetcher, ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	p.Start(ctx)
	t.Cleanup(func() {
		cancel()
		<-p.runDone
	})
}

// gates 是逐块放行闸(白盒测试钩子):每块一个闸,测试按块控制"抓取"何时完成,
// 断言不依赖 goroutine 调度顺序;releaseAll 后对所有块(含其后才出现的)永不阻塞。
type gates struct {
	mu       sync.Mutex
	open     bool
	released map[ChunkRef]bool
	ch       map[ChunkRef]chan struct{}
	started  atomic.Int64 // 进入 wait 的抓取数
}

func newGates() *gates {
	return &gates{
		released: make(map[ChunkRef]bool),
		ch:       make(map[ChunkRef]chan struct{}),
	}
}

// wait 阻塞至该块被 release(或 releaseAll);进入即计入 started。
func (g *gates) wait(ref ChunkRef) {
	g.started.Add(1)
	g.mu.Lock()
	if g.open || g.released[ref] {
		g.mu.Unlock()
		return
	}
	ch, ok := g.ch[ref]
	if !ok {
		ch = make(chan struct{})
		g.ch[ref] = ch
	}
	g.mu.Unlock()
	<-ch
}

// release 放行单块(幂等)。
func (g *gates) release(ref ChunkRef) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.open || g.released[ref] {
		return
	}
	g.released[ref] = true
	if ch, ok := g.ch[ref]; ok {
		close(ch)
		delete(g.ch, ref)
	}
}

// releaseAll 放行所有块(含尚未出现的:open 后 wait 不再阻塞)。
func (g *gates) releaseAll() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = true
	for ref, ch := range g.ch {
		close(ch)
		delete(g.ch, ref)
	}
}

// pending 返回当前阻塞在闸上的块集合。
func (g *gates) pending() map[ChunkRef]bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[ChunkRef]bool, len(g.ch))
	for ref := range g.ch {
		out[ref] = true
	}
	return out
}

// waitPending 等待在途(已出队、阻塞在闸上)的块集合恰为 want。
func waitPending(t *testing.T, g *gates, want ...ChunkRef) {
	t.Helper()
	waitFor(t, fmt.Sprintf("在途块集合 = %v", want), func() bool {
		got := g.pending()
		if len(got) != len(want) {
			return false
		}
		for _, r := range want {
			if !got[r] {
				return false
			}
		}
		return true
	})
}

// fakeClock 是注入的时间源(白盒钩子):测试精确控制吞吐 EMA 样本间隔。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// gatherStall 读出阶段切换停滞 histogram 的样本数与和(断言 OnStageSwitch 打点接通;
// 读取方式同 metrics 测试,不 import client_model)。
func gatherStall(t *testing.T, r *metrics.Registry) (count uint64, sum float64) {
	t.Helper()
	families, err := r.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != stageSwitchStallName {
			continue
		}
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			count += h.GetSampleCount()
			sum += h.GetSampleSum()
		}
	}
	return count, sum
}

// ---------------- 队列结构与配置 ----------------

// TestQueuePriorityPreemption:严格优先级 P0>P1>P2>P3,P1 入队后先于 P2 出队;同级 FIFO。
func TestQueuePriorityPreemption(t *testing.T) {
	p := New(nil, nil, nil, nil, Config{})

	p.enqueueLocked(&task{}, priP2)
	p.enqueueLocked(&task{}, priP3)
	p.enqueueLocked(&task{}, priP1)
	p.enqueueLocked(&task{}, priP0)
	p.enqueueLocked(&task{}, priP2)

	want := []int{priP0, priP1, priP2, priP2, priP3}
	for i, wp := range want {
		gotTask, gotPri := p.popTaskLocked()
		if gotTask == nil {
			t.Fatalf("第 %d 次出队为空,仍期望优先级 %d", i, wp)
		}
		if gotPri != wp {
			t.Fatalf("第 %d 次出队优先级 = %d, want %d(严格优先级出队)", i, gotPri, wp)
		}
	}
	if gotTask, _ := p.popTaskLocked(); gotTask != nil {
		t.Fatal("队列已空,出队应为 nil")
	}
}

// TestConfigLookaheadDefault:Lookahead ≤ 0 取默认 8(D16)。
func TestConfigLookaheadDefault(t *testing.T) {
	if got := New(nil, nil, nil, nil, Config{}).cfg.Lookahead; got != 8 {
		t.Errorf("零值 Lookahead = %d, want 8", got)
	}
	if got := New(nil, nil, nil, nil, Config{Lookahead: -3}).cfg.Lookahead; got != 8 {
		t.Errorf("负 Lookahead = %d, want 8(≤0 取默认)", got)
	}
	if got := New(nil, nil, nil, nil, Config{Lookahead: 3}).cfg.Lookahead; got != 3 {
		t.Errorf("Lookahead = %d, want 3", got)
	}
}

// TestStageSwitchEnqueuesKWindowAsP1:OnStageSwitch 后新阶段开头 K 块以 P1 入队
// (队列深度 P1 = K,K 超过订单长度时截断)。
func TestStageSwitchEnqueuesKWindowAsP1(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{Lookahead: 2})

	eta, err := p.OnStageSwitch(ctx, orderOf("epoch-1", refsNamed("a", 5), 3))
	if err != nil || eta != 0 {
		t.Fatalf("OnStageSwitch = (%v, %v), want (0, nil;无吞吐历史)", eta, err)
	}
	if got := p.Status().QueueDepth; got != [4]int{0, 3, 0, 0} {
		t.Fatalf("K=3 入队后队列深度 = %v, want [0 3 0 0](P1=K)", got)
	}

	// K 超过订单长度:截断为 len(chunks)。
	p.OnStageSwitch(ctx, orderOf("epoch-2", refsNamed("b", 2), 5))
	if got := p.Status().QueueDepth; got != [4]int{0, 2, 0, 3} {
		t.Fatalf("K=5/len=2 后队列深度 = %v, want [0 2 0 3](P1 截断为 2,旧 P1 降级 P3)", got)
	}
}

// TestOnStageSwitchDemotesOldQueueToP3:旧阶段队列中未出队的块降级 P3(保序),
// 新阶段仅 K 块 P1;K=0 时邻域窗口立即开放。
func TestOnStageSwitchDemotesOldQueueToP3(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{Lookahead: 2})

	old := orderOf("epoch-1", refsNamed("a", 4), 0) // K=0 → P2 邻域窗口 {0,1}
	if _, err := p.OnStageSwitch(ctx, old); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	if got := p.Status().QueueDepth; got != [4]int{0, 0, 2, 0} {
		t.Fatalf("K=0 后队列深度 = %v, want [0 0 2 0](邻域窗口)", got)
	}

	if _, err := p.OnStageSwitch(ctx, orderOf("epoch-2", refsNamed("b", 6), 1)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	if got := p.Status().QueueDepth; got != [4]int{0, 1, 0, 2} {
		t.Fatalf("切换后队列深度 = %v, want [0 1 0 2](旧 2 块降级 P3)", got)
	}

	// 白盒:降级的是旧阶段的块(无调度协程运行,直接读队列无竞态)。
	p.mu.Lock()
	demoted := slices.Clone(p.queues[priP3])
	p.mu.Unlock()
	for i, wantRef := range old.Chunks[:2] {
		if demoted[i].ref != wantRef || demoted[i].st.dataset != testDataset {
			t.Fatalf("P3[%d] = %+v, want 旧阶段块 %+v", i, demoted[i].ref, wantRef)
		}
	}
}

// ---------------- Status / advance ----------------

// TestStatusFieldsAndAdvance:Cursor 初始零值;advance 置为 ref 位置且单调不回退;
// ref 不在订单内忽略返回 nil;推进后邻域窗口前进;样本计入口径一致。
func TestStatusFieldsAndAdvance(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{Lookahead: 2})

	order := orderOf("epoch-2", refsNamed("a", 4), 0)
	if _, err := p.OnStageSwitch(ctx, order); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	s := p.Status()
	if s.Stage != "epoch-2" {
		t.Errorf("Stage = %q, want epoch-2", s.Stage)
	}
	if s.Cursor != (ChunkRef{}) {
		t.Errorf("未推进时 Cursor = %+v, want 零值", s.Cursor)
	}
	if s.QueueDepth != [4]int{0, 0, 2, 0} {
		t.Errorf("QueueDepth = %v, want [0 0 2 0]", s.QueueDepth)
	}
	if s.HitRate != 0 {
		t.Errorf("无样本 HitRate = %v, want 0", s.HitRate)
	}

	// advance 置 cursor 为该 ref 位置(窗口随之补齐到 {2,3})。
	if err := p.OnConsumeSideAdvance(ctx, order.Chunks[1]); err != nil {
		t.Fatalf("OnConsumeSideAdvance: %v", err)
	}
	if got := p.Status(); got.Cursor != order.Chunks[1] || got.QueueDepth != [4]int{0, 0, 4, 0} {
		t.Fatalf("advance 后 Status = %+v, want Cursor=%+v 深度 [0 0 4 0]", got, order.Chunks[1])
	}

	// 单调不回退:落后的 ref 不再回置 cursor。
	if err := p.OnConsumeSideAdvance(ctx, order.Chunks[0]); err != nil {
		t.Fatalf("OnConsumeSideAdvance: %v", err)
	}
	if got := p.Status().Cursor; got != order.Chunks[1] {
		t.Fatalf("回退 advance 后 Cursor = %+v, want 保持 %+v", got, order.Chunks[1])
	}

	// ref 不在订单内:忽略,返回 nil,不计样本。
	if err := p.OnConsumeSideAdvance(ctx, ChunkRef{Path: "zzz.tar", Chunk: 9}); err != nil {
		t.Fatalf("订单外 ref advance 应返回 nil,实际 %v", err)
	}
	if got := p.Status().Cursor; got != order.Chunks[1] {
		t.Fatalf("订单外 ref 不应改变 Cursor,实际 %+v", got)
	}

	// HitRate 样本:两次订单内 advance(块均未就绪),订单外不计样本。
	p.mu.Lock()
	filled := p.stage.hitFilled
	p.mu.Unlock()
	if filled != 2 {
		t.Fatalf("命中样本数 = %d, want 2(未就绪 2 次;订单外不计)", filled)
	}
	if got := p.Status().HitRate; got != 0 {
		t.Fatalf("未就绪样本 HitRate = %v, want 0", got)
	}
}

// TestHitRateCountsReadyOnConsume:消费时块已就绪计命中、未就绪计未命中;
// K 块全部处理完后剩余块按 cursor 起转 P2 邻域窗口(§7.3)。
func TestHitRateCountsReadyOnConsume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{Lookahead: 2})
	g := newGates()
	p.grab = func(ctx context.Context, _ string, ref ChunkRef) (int64, bool) {
		g.wait(ref)
		return 100, true
	}

	order := orderOf("epoch-3", refsNamed("a", 5), 2)
	if _, err := p.OnStageSwitch(ctx, order); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)
	waitPending(t, g, order.Chunks[0], order.Chunks[1])

	// 块 0 完成(已就绪),再消费块 0 → 命中样本 true。
	g.release(order.Chunks[0])
	waitFor(t, "块 0 完成推进 cursor", func() bool { return p.Status().Cursor == order.Chunks[0] })
	if err := p.OnConsumeSideAdvance(ctx, order.Chunks[0]); err != nil {
		t.Fatalf("OnConsumeSideAdvance: %v", err)
	}
	// 块 2 未就绪时被消费 → 命中样本 false;cursor 推进到块 2。
	if err := p.OnConsumeSideAdvance(ctx, order.Chunks[2]); err != nil {
		t.Fatalf("OnConsumeSideAdvance: %v", err)
	}
	s := p.Status()
	if s.Stage != "epoch-3" || s.Cursor != order.Chunks[2] || s.HitRate != 0.5 {
		t.Fatalf("Status = %+v, want Stage=epoch-3 Cursor=%+v HitRate=0.5", s, order.Chunks[2])
	}

	// K 窗口最后一块完成 → 剩余块从 cursor 起转 P2(cursor=2 → 窗口 {3,4})。
	g.release(order.Chunks[1])
	waitPending(t, g, order.Chunks[3], order.Chunks[4])
	g.releaseAll()
	waitFor(t, "全阶段完成推进 cursor 到末块", func() bool { return p.Status().Cursor == order.Chunks[4] })
	if got := p.Status().HitRate; got != 0.5 {
		t.Fatalf("全部完成后 HitRate = %v, want 保持 0.5(无新样本)", got)
	}
}

// ---------------- 邻域窗口 ----------------

// TestStartNeighborhoodWindowWalksCursor:Start 后仅入队 cursor+1..cursor+lookahead 范围
// 的块(P2),完成一块推进 cursor、窗口按块滑入下一个(逐块断言,集合口径避免依赖调度序)。
func TestStartNeighborhoodWindowWalksCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{Lookahead: 2})
	g := newGates()
	p.grab = func(ctx context.Context, _ string, ref ChunkRef) (int64, bool) {
		g.wait(ref)
		return 100, true
	}

	order := orderOf("epoch-1", refsNamed("a", 6), 0) // K=0 → 邻域窗口即全部预取来源
	if _, err := p.OnStageSwitch(ctx, order); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	// 窗口 = (cursor=-1, -1+lookahead] = 前 2 块;范围外未入队。
	if got := p.Status().QueueDepth; got != [4]int{0, 0, 2, 0} {
		t.Fatalf("初始邻域窗口深度 = %v, want [0 0 2 0]", got)
	}

	startPrefetcher(t, p, ctx, cancel)
	waitPending(t, g, order.Chunks[0], order.Chunks[1])
	g.release(order.Chunks[0]) // 完成块 0 → cursor=0 → 窗口滑入块 2
	waitPending(t, g, order.Chunks[1], order.Chunks[2])
	g.release(order.Chunks[1]) // → 滑入块 3
	waitPending(t, g, order.Chunks[2], order.Chunks[3])
	g.release(order.Chunks[2]) // → 滑入块 4
	waitPending(t, g, order.Chunks[3], order.Chunks[4])
	g.release(order.Chunks[3]) // → 滑入块 5(末尾截断)
	waitPending(t, g, order.Chunks[4], order.Chunks[5])
	g.release(order.Chunks[4])
	g.release(order.Chunks[5])
	waitFor(t, "全部完成且游标到末块", func() bool { return p.Status().Cursor == order.Chunks[5] })
	if got := p.Status().QueueDepth; got != [4]int{0, 0, 0, 0} {
		t.Fatalf("走完全程后队列深度 = %v, want 全 0", got)
	}
}

// ---------------- 在途不取消 ----------------

// TestStageSwitchKeepsInflightRunning:OnStageSwitch 时在途下载不取消、继续完成;
// 新阶段 K 块以 P1 入队并正常处理。
func TestStageSwitchKeepsInflightRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{Lookahead: 1})
	g := newGates()
	var live atomic.Int64
	p.grab = func(ctx context.Context, _ string, ref ChunkRef) (int64, bool) {
		g.wait(ref)
		if ctx.Err() == nil {
			live.Add(1)
		}
		return 50, true
	}

	order1 := orderOf("epoch-1", refsNamed("a", 3), 3)
	if _, err := p.OnStageSwitch(ctx, order1); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)
	waitPending(t, g, order1.Chunks...) // 旧阶段 3 块全部在途(阻塞在闸上)

	order2 := orderOf("epoch-2", refsNamed("b", 2), 1)
	if _, err := p.OnStageSwitch(ctx, order2); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	// 切换不得取消运行 ctx(在途请求不取消);新阶段 P1 块也开始在途。
	if ctx.Err() != nil {
		t.Fatalf("阶段切换不应取消运行 ctx: %v", ctx.Err())
	}
	waitPending(t, g, append(slices.Clone(order1.Chunks), order2.Chunks[0])...)

	g.releaseAll()
	waitFor(t, "旧在途完成且新阶段游标到 K 后窗口末块", func() bool {
		return p.Status().Cursor == order2.Chunks[1]
	})
	if got := live.Load(); got != 5 {
		t.Fatalf("未取消完成的抓取数 = %d, want 5(旧 3 + 新 2)", got)
	}
}

// ---------------- ETA ----------------

// TestStageSwitchETA:已知吞吐(假时钟精确控制完成间隔)与 K 块字节 → 断言公式
// (剩余未就绪字节 ÷ EMA);同时断言 ObserveStageSwitchStall 打点接通。
func TestStageSwitchETA(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, []blockSpec{
		// 暖机阶段:两块本地块(仅经注入 grab 完成,给吞吐样本)。
		{path: "w0.tar", chunk: 0, content: strings.Repeat("x", 100), local: true},
		{path: "w1.tar", chunk: 0, content: strings.Repeat("y", 200), local: true},
		// 新窗口 K 块:只存在于索引(Evict 后本地无副本、源站也不放)→ 全部计入未就绪字节。
		{path: "s0.tar", chunk: 0, content: strings.Repeat("a", 1000)},
		{path: "s1.tar", chunk: 0, content: strings.Repeat("b", 1000)},
		{path: "s2.tar", chunk: 0, content: strings.Repeat("c", 1000)},
	})
	p := env.newPrefetcher(Config{Lookahead: 1})
	clk := &fakeClock{t: time.Unix(1700000000, 0)}
	p.now = clk.Now
	g := newGates()
	sizes := map[string]int64{"w0.tar": 100, "w1.tar": 200}
	p.grab = func(ctx context.Context, _ string, ref ChunkRef) (int64, bool) {
		g.wait(ref)
		return sizes[ref.Path], true
	}

	warm := orderOf("warm-1", []ChunkRef{{Path: "w0.tar"}, {Path: "w1.tar"}}, 2)
	if eta, err := p.OnStageSwitch(ctx, warm); err != nil || eta != 0 {
		t.Fatalf("无吞吐历史 OnStageSwitch = (%v, %v), want (0, nil)", eta, err)
	}
	startPrefetcher(t, p, ctx, cancel)
	waitPending(t, g, warm.Chunks...)

	g.release(warm.Chunks[0]) // t0 完成(首样本只记时刻)
	waitFor(t, "w0 完成", func() bool { return p.Status().Cursor == warm.Chunks[0] })
	clk.advance(time.Second)
	g.release(warm.Chunks[1]) // t0+1s 完成 200 字节 → EMA = 200 B/s
	waitFor(t, "w1 完成", func() bool { return p.Status().Cursor == warm.Chunks[1] })

	next := orderOf("epoch-2", []ChunkRef{{Path: "s0.tar"}, {Path: "s1.tar"}, {Path: "s2.tar"}}, 3)
	eta, err := p.OnStageSwitch(ctx, next)
	if err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	if want := 15 * time.Second; eta != want {
		t.Fatalf("ETA = %v, want %v(3000 未就绪字节 ÷ 200 B/s)", eta, want)
	}
	if count, sum := gatherStall(t, env.reg); count != 2 || sum != 15 {
		t.Fatalf("stall histogram = (count %d, sum %v), want (2, 15;两次切换 0 与 15s)", count, sum)
	}

	// 收尾:放行新阶段全部块(其 Size 为 0 的完成不产生吞吐样本)。
	g.releaseAll()
	waitFor(t, "新阶段全部完成", func() bool { return p.Status().Cursor == next.Chunks[2] })
}

// ---------------- 契约路径(真实 core 组合) ----------------

// TestLocalHitSkipsDownload:Lookup 命中且 cas.Open 成功 → 不产生任何下载(源站 Get 计数 0)。
func TestLocalHitSkipsDownload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, []blockSpec{
		{path: "l0.tar", chunk: 0, content: "block-l0", local: true},
		{path: "l1.tar", chunk: 1, content: "block-l1", local: true},
	})
	p := env.newPrefetcher(Config{Lookahead: 2})

	order := orderOf("epoch-1", []ChunkRef{{Path: "l0.tar"}, {Path: "l1.tar", Chunk: 1}}, 2)
	if _, err := p.OnStageSwitch(ctx, order); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)
	waitFor(t, "两块处理完成(cursor 推进到末块)", func() bool {
		return p.Status().Cursor == order.Chunks[1]
	})

	if got := blockGets(env.store); len(got) != 0 {
		t.Fatalf("本地命中不应产生数据块下载,实际块 Get 键 = %v", got)
	}
	for _, ref := range order.Chunks {
		br, err := env.cs.Open(env.fps[ref])
		if err != nil {
			t.Fatalf("本地块 %s#%d 应仍可打开: %v", ref.Path, ref.Chunk, err)
		}
		_ = br.Close()
	}
}

// TestDownloadOnLocalMissUsesD25Key:Open ErrNotFound → 经 fetch.Fetch 下载,
// 对象键为 D25 布局 <dataset>/blocks/<path>#<chunk>;下载后块落本地。
func TestDownloadOnLocalMissUsesD25Key(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, []blockSpec{
		{path: "r0.tar", chunk: 3, content: "remote-block-0", source: true},
		{path: "r1.tar", chunk: 0, content: "remote-block-1", source: true},
	})
	p := env.newPrefetcher(Config{Lookahead: 2})

	order := orderOf("epoch-1", []ChunkRef{{Path: "r0.tar", Chunk: 3}, {Path: "r1.tar"}}, 2)
	if _, err := p.OnStageSwitch(ctx, order); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)

	waitFor(t, "两块下载完成且落本地", func() bool {
		if p.Status().Cursor != order.Chunks[1] {
			return false
		}
		if len(blockGets(env.store)) != 2 {
			return false
		}
		for _, ref := range order.Chunks {
			br, err := env.cs.Open(env.fps[ref])
			if err != nil {
				return false
			}
			_ = br.Close()
		}
		return true
	})

	got := blockGets(env.store)
	want := []string{
		expectedBlockKey(testDataset, order.Chunks[0]),
		expectedBlockKey(testDataset, order.Chunks[1]),
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("源站 Get 键 = %v, want %v(D25 键布局)", got, want)
	}
}

// ---------------- 并发与边界 ----------------

// TestConcurrentAdvancesAndStageSwitch:多协程并发 advance/Status/阶段切换与完成回调
// 交错,-race 下的锁正确性(验收:并发 -race 场景)。
func TestConcurrentAdvancesAndStageSwitch(t *testing.T) {
	const n = 24
	refs := refsNamed("c", n)
	specs := make([]blockSpec, 0, n)
	for i, r := range refs {
		specs = append(specs, blockSpec{path: r.Path, chunk: r.Chunk, content: fmt.Sprintf("concurrent-block-%02d", i), local: true})
	}
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, specs)
	p := env.newPrefetcher(Config{Lookahead: 4})

	if _, err := p.OnStageSwitch(ctx, orderOf("epoch-1", refs, 4)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				for j := 0; j < len(refs); j++ {
					idx := (j*7 + seed*5 + i*3) % len(refs)
					_ = p.OnConsumeSideAdvance(ctx, refs[idx])
				}
			}
		}(w)
	}

	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		for i := 0; i < 200; i++ {
			s := p.Status()
			if s.HitRate < 0 || s.HitRate > 1 {
				t.Errorf("HitRate 越界: %v", s.HitRate)
				return
			}
		}
	}()

	if _, err := p.OnStageSwitch(ctx, orderOf("epoch-2", refs, 2)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	wg.Wait()
	<-statusDone

	waitFor(t, "新阶段全部块就绪且队列清空", func() bool {
		s := p.Status()
		return s.Cursor == refs[n-1] && s.QueueDepth == [4]int{}
	})
	if got := p.Status().Stage; got != "epoch-2" {
		t.Fatalf("Stage = %q, want epoch-2", got)
	}
}

// TestContextCanceled:ctx 已取消时 OnStageSwitch / OnConsumeSideAdvance 返回取消错误。
func TestContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{})

	if _, err := p.OnStageSwitch(ctx, orderOf("s", refsNamed("a", 1), 0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("OnStageSwitch err = %v, want context.Canceled", err)
	}
	if err := p.OnConsumeSideAdvance(ctx, ChunkRef{Path: "a-00.tar"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("OnConsumeSideAdvance err = %v, want context.Canceled", err)
	}
}

// TestNilMetricsRegistrySafe:metrics 注册表为 nil 时打点路径静默跳过,不 panic。
func TestNilMetricsRegistrySafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, []blockSpec{
		{path: "n0.tar", chunk: 0, content: "nil-metrics-block", local: true},
	})
	p := New(env.cat, env.fetch, env.cs, nil, Config{Lookahead: 1})

	order := orderOf("epoch-1", []ChunkRef{{Path: "n0.tar"}}, 1)
	if _, err := p.OnStageSwitch(ctx, order); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)
	waitFor(t, "块完成推进 cursor", func() bool { return p.Status().Cursor == order.Chunks[0] })
}

// ---------------- D27:显式预热入队(P0) ----------------

// TestEnqueueP0PriorityPreemptsP1AndP2:P0 严格最高优先级——先 P0(按入参顺序)再 P1(K
// 启动窗口)再 P2(邻域窗口);空列表返回 nil 且队列不变(白盒出队断言,不依赖调度时序)。
func TestEnqueueP0PriorityPreemptsP1AndP2(t *testing.T) {
	ctx := context.Background()

	// 子场景 1:K=2 → 前两块以 P1 入队;P0 应整体先出队。
	t.Run("P0>P1", func(t *testing.T) {
		env := newTestEnv(t, nil)
		p := env.newPrefetcher(Config{Lookahead: 2})
		order := orderOf("epoch-1", refsNamed("a", 5), 2)
		if _, err := p.OnStageSwitch(ctx, order); err != nil {
			t.Fatalf("OnStageSwitch: %v", err)
		}
		if got := p.Status().QueueDepth; got != [4]int{0, 2, 0, 0} {
			t.Fatalf("P1 队列深度 = %v, want [0 2 0 0]", got)
		}

		p0 := []ChunkRef{{Path: "p0-first.tar"}, {Path: "p0-second.tar"}}
		if err := p.EnqueueP0(ctx, p0); err != nil {
			t.Fatalf("EnqueueP0: %v", err)
		}
		if got := p.Status().QueueDepth; got != [4]int{2, 2, 0, 0} {
			t.Fatalf("P0 入队后队列深度 = %v, want [2 2 0 0]", got)
		}

		var got []ChunkRef
		for {
			tk, _ := p.popTaskLocked()
			if tk == nil {
				break
			}
			if wantP0 := len(got) < len(p0); tk.p0 != wantP0 {
				t.Fatalf("第 %d 个出队项 p0 = %v, want %v(P0 应前置)", len(got), tk.p0, wantP0)
			}
			got = append(got, tk.ref)
		}
		want := slices.Concat(p0, order.Chunks[:2])
		if !slices.Equal(got, want) {
			t.Fatalf("出队序 = %v, want %v(P0 按入参序优先于 P1)", got, want)
		}
	})

	// 子场景 2:K=0 → 邻域窗口以 P2 入队;P0 仍整体先出队。
	t.Run("P0>P2", func(t *testing.T) {
		env := newTestEnv(t, nil)
		p := env.newPrefetcher(Config{Lookahead: 2})
		order := orderOf("epoch-1", refsNamed("a", 5), 0)
		if _, err := p.OnStageSwitch(ctx, order); err != nil {
			t.Fatalf("OnStageSwitch: %v", err)
		}
		if got := p.Status().QueueDepth; got != [4]int{0, 0, 2, 0} {
			t.Fatalf("P2 队列深度 = %v, want [0 0 2 0]", got)
		}

		p0 := []ChunkRef{{Path: "p0-first.tar"}}
		if err := p.EnqueueP0(ctx, p0); err != nil {
			t.Fatalf("EnqueueP0: %v", err)
		}

		var got []ChunkRef
		for {
			tk, _ := p.popTaskLocked()
			if tk == nil {
				break
			}
			got = append(got, tk.ref)
		}
		want := slices.Concat(p0, order.Chunks[:2])
		if !slices.Equal(got, want) {
			t.Fatalf("出队序 = %v, want %v(P0 优先于 P2)", got, want)
		}
	})
}

// TestEnqueueP0EmptyAndCanceled:空列表返回 nil 且不入队;ctx 已取消返回 context.Canceled
// 且不入队(D27 冻结语义)。
func TestEnqueueP0EmptyAndCanceled(t *testing.T) {
	env := newTestEnv(t, nil)
	p := env.newPrefetcher(Config{})

	if err := p.EnqueueP0(context.Background(), nil); err != nil {
		t.Fatalf("空列表 EnqueueP0 err = %v, want nil", err)
	}
	if err := p.EnqueueP0(context.Background(), []ChunkRef{}); err != nil {
		t.Fatalf("空切片 EnqueueP0 err = %v, want nil", err)
	}
	if got := p.Status().QueueDepth; got != [4]int{} {
		t.Fatalf("空列表入队后队列深度 = %v, want 全 0", got)
	}

	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.EnqueueP0(cctx, []ChunkRef{{Path: "x.tar"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消 ctx EnqueueP0 err = %v, want context.Canceled", err)
	}
	if got := p.Status().QueueDepth; got != [4]int{} {
		t.Fatalf("取消 ctx 不应入队,队列深度 = %v, want 全 0", got)
	}
}

// TestEnqueueP0BlockCallbacks:P0 每块完成回调一次——本地命中(nil)、回源下载成功(nil)、
// 下载失败(非 nil)三类逐块触发;回调在调度协程内串行且不持内部锁(回调内可安全调用
// Status;回调自身阻塞会拖慢调度,用例中 done 有缓冲、回调保持轻量)。dataset 绑定经
// 最小订单播种(与 svc"预热先于订单"的装配路径一致)。
func TestEnqueueP0BlockCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, []blockSpec{
		{path: "hit.tar", chunk: 0, content: "local-hit-block", local: true},
		{path: "dl.tar", chunk: 0, content: "remote-block-content", source: true},
		{path: "dead.tar", chunk: 0, content: "no-source-block"}, // 索引有、本地无、源站无 → 下载失败
	})

	var p *PersistentPrefetcher
	var (
		mu     sync.Mutex
		got    = make(map[ChunkRef]error)
		calls  int
		inCall atomic.Int64
		maxCon atomic.Int64
	)
	done := make(chan struct{}, 3)
	p = env.newPrefetcher(Config{Lookahead: 1, OnP0Done: func(ref ChunkRef, err error) {
		if cur := inCall.Add(1); cur > maxCon.Load() {
			maxCon.Store(cur)
		}
		_ = p.Status() // 回调内可安全查询状态(回调不持预取器内部锁)
		mu.Lock()
		got[ref] = err
		calls++
		mu.Unlock()
		inCall.Add(-1)
		done <- struct{}{}
	}})

	// dataset 播种:最小订单(无 chunks、K=0)只绑定 dataset,不入队任何阶段任务。
	if _, err := p.OnStageSwitch(ctx, ConsumeOrder{Dataset: testDataset, StageID: "preheat-seed"}); err != nil {
		t.Fatalf("播种 OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)

	refs := []ChunkRef{{Path: "hit.tar"}, {Path: "dl.tar"}, {Path: "dead.tar"}}
	if err := p.EnqueueP0(ctx, refs); err != nil {
		t.Fatalf("EnqueueP0: %v", err)
	}
	for i := range refs {
		select {
		case <-done:
		case <-time.After(testTimeout):
			t.Fatalf("等待第 %d 个 P0 完成回调超时", i+1)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != len(refs) {
		t.Fatalf("回调次数 = %d, want %d(每块一次)", calls, len(refs))
	}
	if err := got[refs[0]]; err != nil {
		t.Errorf("本地命中块回调 err = %v, want nil", err)
	}
	if err := got[refs[1]]; err != nil {
		t.Errorf("下载成功块回调 err = %v, want nil", err)
	}
	if err := got[refs[2]]; err == nil {
		t.Error("无源块回调 err = nil, want 下载失败原因")
	}
	if got := maxCon.Load(); got != 1 {
		t.Errorf("回调最大并发 = %d, want 1(调度协程内串行调用)", got)
	}

	// 下载成功块已落本地;下载路径键为 D25 布局;本地命中块零下载。
	br, err := env.cs.Open(env.fps[refs[1]])
	if err != nil {
		t.Fatalf("下载后本地 Open(%s): %v", refs[1].Path, err)
	}
	_ = br.Close()
	keys := blockGets(env.store)
	if !slices.Contains(keys, expectedBlockKey(testDataset, refs[1])) {
		t.Errorf("源站 Get 键 = %v, want 含 %s", keys, expectedBlockKey(testDataset, refs[1]))
	}
	if slices.Contains(keys, expectedBlockKey(testDataset, refs[0])) {
		t.Errorf("本地命中块不应产生下载: %v", keys)
	}
}

// TestEnqueueP0UnboundDatasetFails:尚无 ConsumeOrder 绑定 dataset 时(D25 键缺 dataset),
// 未本地命中的 P0 块以错误回调,不产生任何下载;本地命中块仍就绪(nil)。
func TestEnqueueP0UnboundDatasetFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, []blockSpec{
		{path: "local.tar", chunk: 0, content: "unbound-local", local: true},
		{path: "remote.tar", chunk: 0, content: "unbound-remote", source: true},
	})
	var (
		mu  sync.Mutex
		got = make(map[ChunkRef]error)
	)
	done := make(chan struct{}, 2)
	p := env.newPrefetcher(Config{OnP0Done: func(ref ChunkRef, err error) {
		mu.Lock()
		got[ref] = err
		mu.Unlock()
		done <- struct{}{}
	}})
	startPrefetcher(t, p, ctx, cancel)

	refs := []ChunkRef{{Path: "local.tar"}, {Path: "remote.tar"}}
	if err := p.EnqueueP0(ctx, refs); err != nil {
		t.Fatalf("EnqueueP0: %v", err)
	}
	for range refs {
		select {
		case <-done:
		case <-time.After(testTimeout):
			t.Fatal("等待 P0 回调超时")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if err := got[refs[0]]; err != nil {
		t.Errorf("本地命中块回调 err = %v, want nil(dataset 未绑定不影响本地命中)", err)
	}
	if err := got[refs[1]]; err == nil {
		t.Error("未绑定 dataset 的未命中块回调 err = nil, want 构造下载键失败")
	}
	if keys := blockGets(env.store); len(keys) != 0 {
		t.Errorf("dataset 未绑定不应产生数据块下载: %v", keys)
	}
}

// TestEnqueueP0FetchesWithoutMapping:P0(显式预热)在索引映射未加载时(ErrNoCurrent;
// "预热先于订单"的装配——预热不切换映射)仍按 D25 键下载;阶段任务口径相反(未命中的
// 块不抓取,由既有用例覆盖)。
func TestEnqueueP0FetchesWithoutMapping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, []blockSpec{
		{path: "r-000.tar", chunk: 0, content: "p0-no-mapping", source: true}, // 本地无、源站有
	})
	// 全新 Catalog(不 Refresh):映射为空,Lookup 返回 ErrNoCurrent。
	cat := index.NewCatalog(env.store, testDataset, index.Config{})
	done := make(chan error, 1)
	p := New(cat, env.fetch, env.cs, env.reg, Config{
		OnP0Done: func(_ ChunkRef, err error) { done <- err },
	})
	if _, err := p.OnStageSwitch(ctx, ConsumeOrder{Dataset: testDataset, StageID: "preheat-seed"}); err != nil {
		t.Fatalf("播种 OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)

	ref := ChunkRef{Path: "r-000.tar"}
	if err := p.EnqueueP0(ctx, []ChunkRef{ref}); err != nil {
		t.Fatalf("EnqueueP0: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("映射未加载时 P0 应仍可下载,回调 err = %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("等待 P0 回调超时")
	}

	// 下载成功:块已落本地,源站键为 D25 布局。
	br, err := env.cs.Open(env.fps[ref])
	if err != nil {
		t.Fatalf("下载后本地 Open: %v", err)
	}
	_ = br.Close()
	if keys := blockGets(env.store); !slices.Contains(keys, expectedBlockKey(testDataset, ref)) {
		t.Errorf("源站 Get 键 = %v, want 含 %s", keys, expectedBlockKey(testDataset, ref))
	}
}
