package prefetch

// pending_test.go:D45 跨队列去重(M8)——优先级提升、运行中忽略、终态清理、
// 同块只下载一次、跨阶段归属迁移、InFlight/当前窗口判据。

import (
	"context"
	"sync/atomic"
	"testing"
)

// queueDepthsLocked 读取四队列深度(调用方持 p.mu;Status 会重复加锁,白盒断言用)。
func queueDepthsLocked(p *PersistentPrefetcher) [4]int {
	var d [4]int
	for i := range p.queues {
		d[i] = len(p.queues[i])
	}
	return d
}

// TestPendingP2ThenP0Promotes:同块先 P2 后 P0(均未开始)→ 提升到 P0,不新建 task,
// 队列深度按去重后计数;P0 归属由提升后的 task 继承(OnP0Done 语义)。
func TestPendingP2ThenP0Promotes(t *testing.T) {
	p := New(nil, nil, nil, nil, Config{Lookahead: 2})
	ref := ChunkRef{Path: "b-00.tar"}

	p.mu.Lock()
	p.enqueueLocked(&task{idx: 0, ref: ref, dataset: testDataset}, priP2)
	p.enqueueLocked(&task{idx: -1, ref: ref, dataset: testDataset, p0: true}, priP0)
	depth := queueDepthsLocked(p)
	key := blockKey(testDataset, ref)
	registered := p.pending[key]
	tk, pri := p.popTaskLocked()
	second, _ := p.popTaskLocked()
	pendingLen := len(p.pending)
	p.mu.Unlock()

	if depth != [4]int{1, 0, 0, 0} {
		t.Fatalf("去重后队列深度 = %v, want [1 0 0 0](P2 提升到 P0)", depth)
	}
	if tk == nil || pri != priP0 {
		t.Fatalf("出队 = (%v, %d), want (task, P0)", tk, pri)
	}
	if !tk.p0 {
		t.Error("P0 请求去重到已有 task 后应继承 p0 归属(OnP0Done 语义)")
	}
	if registered != tk {
		t.Error("pending 表应指向被提升的同一 task(不新建 task)")
	}
	if second != nil || pendingLen != 1 {
		t.Fatalf("应只有一个 task/pending 条目,实际 second=%v pending=%d", second, pendingLen)
	}
}

// TestPendingP0ThenP2KeepsPriority:先 P0 后 P2 → 保持 P0(不降级),只登记一个 task。
func TestPendingP0ThenP2KeepsPriority(t *testing.T) {
	p := New(nil, nil, nil, nil, Config{Lookahead: 2})
	ref := ChunkRef{Path: "b-01.tar"}

	p.mu.Lock()
	p.enqueueLocked(&task{idx: -1, ref: ref, dataset: testDataset, p0: true}, priP0)
	p.enqueueLocked(&task{idx: 0, ref: ref, dataset: testDataset}, priP2)
	depth := queueDepthsLocked(p)
	tk, pri := p.popTaskLocked()
	second, _ := p.popTaskLocked()
	p.mu.Unlock()

	if depth != [4]int{1, 0, 0, 0} {
		t.Fatalf("队列深度 = %v, want [1 0 0 0](P2 不降级 P0)", depth)
	}
	if tk == nil || pri != priP0 || !tk.p0 {
		t.Fatalf("出队 = (%v, %d, p0=%v), want P0", tk, pri, tk != nil && tk.p0)
	}
	if second != nil {
		t.Fatal("同块不应出现第二个 task")
	}
}

// TestPendingCrossStageRehome:阶段切换后旧阶段已入队未开始块,被新阶段同块请求提升时
// 迁移归属到新阶段(游标/就绪簿记不落在旧阶段),且不新建 task、不重复入队。
func TestPendingCrossStageRehome(t *testing.T) {
	ctx := context.Background()
	p := New(nil, nil, nil, nil, Config{Lookahead: 4})
	refs := refsNamed("x", 4)

	// 旧阶段 K=0 → 邻域窗口 4 块全部入 P2。
	if _, err := p.OnStageSwitch(ctx, orderOf("old", refs, 0)); err != nil {
		t.Fatalf("OnStageSwitch(old): %v", err)
	}
	// 新阶段同块:K=0,旧块降级 P3 后邻域窗口再以 P2 请求 → 就地迁移 + 提升。
	if _, err := p.OnStageSwitch(ctx, orderOf("new", refs, 0)); err != nil {
		t.Fatalf("OnStageSwitch(new): %v", err)
	}

	p.mu.Lock()
	depth := queueDepthsLocked(p)
	pendingLen := len(p.pending)
	var got []*task
	for {
		tk, _ := p.popTaskLocked()
		if tk == nil {
			break
		}
		got = append(got, tk)
	}
	stage := p.stage
	p.mu.Unlock()

	if depth != [4]int{0, 0, 4, 0} {
		t.Fatalf("队列深度 = %v, want [0 0 4 0](旧 P3 提升到新阶段 P2)", depth)
	}
	if len(got) != len(refs) || pendingLen != len(refs) {
		t.Fatalf("task/pending 数 = %d/%d, want %d(不新建、不重复)", len(got), pendingLen, len(refs))
	}
	for _, tk := range got {
		if tk.st != stage {
			t.Errorf("块 %s 归属仍为旧阶段(应迁移到新阶段)", tk.ref.Path)
		}
		if want := stage.pos[tk.ref]; tk.idx != want {
			t.Errorf("块 %s idx = %d, want %d", tk.ref.Path, tk.idx, want)
		}
	}
}

// TestPendingRunningDuplicateIgnored:同块在运行(已出队未终态)时重复入队被忽略,
// 不产生第二次完成回调;终态后键清理,可再次入队。
func TestPendingRunningDuplicateIgnored(t *testing.T) {
	env := newTestEnv(t, nil)
	ref := ChunkRef{Path: "run.tar"}
	var (
		onDone atomic.Int64
		onP0   atomic.Int64
	)
	p := env.newPrefetcher(Config{
		Lookahead: 0,
		OnDone:    func(ChunkRef, error) { onDone.Add(1) },
		OnP0Done:  func(ChunkRef, error) { onP0.Add(1) },
	})
	g := newGates()
	p.grab = func(_ context.Context, _ string, r ChunkRef) (int64, bool) {
		g.wait(r)
		return 10, true
	}

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := p.OnStageSwitch(ctx, orderOf("epoch", []ChunkRef{ref}, 1)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)
	waitPending(t, g, ref)

	if !p.InFlight(testDataset, ref) {
		t.Fatal("运行中的块 InFlight 应为 true")
	}
	if err := p.EnqueueP0(ctx, []ChunkRef{ref}); err != nil {
		t.Fatalf("EnqueueP0: %v", err)
	}
	if depth := p.Status().QueueDepth; depth != [4]int{} {
		t.Fatalf("运行中重复入队后队列深度 = %v, want 全 0(忽略)", depth)
	}

	g.releaseAll()
	// 去重键在 process 结束时即清理,OnDone 在其后由调度协程串行触发;
	// 仅等 InFlight=false 会在回调前返回,须同时等回调计数。
	waitFor(t, "块完成回调且去重键清理", func() bool {
		return onDone.Load() >= 1 && !p.InFlight(testDataset, ref)
	})
	if got := onDone.Load(); got != 1 {
		t.Fatalf("OnDone 次数 = %d, want 1(运行中重复入队不重复回调)", got)
	}
	if got := onP0.Load(); got != 0 {
		t.Fatalf("OnP0Done 次数 = %d, want 0(重复入队被忽略,未接受为 P0 task)", got)
	}

	p.mu.Lock()
	action := p.enqueueDedupLocked(&task{idx: -1, ref: ref, dataset: testDataset, p0: true}, priP0)
	p.mu.Unlock()
	if action != "enqueue" {
		t.Fatalf("终态后同块入队 action = %q, want enqueue(键已清理)", action)
	}
}

// TestPendingFailureRetryRetainsThenClears:失败任务的去重键在其运行/回源重试期间保留
// (重复入队忽略);进入终态后清理。成功任务同样在终态清理。
func TestPendingFailureRetryRetainsThenClears(t *testing.T) {
	cases := []struct {
		name  string
		ready bool
	}{
		{"failure", false},
		{"success", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t, nil)
			ref := ChunkRef{Path: "term.tar"}
			var calls atomic.Int64
			p := env.newPrefetcher(Config{
				Lookahead: 0,
				OnDone:    func(ChunkRef, error) { calls.Add(1) },
			})
			g := newGates()
			p.grab = func(_ context.Context, _ string, r ChunkRef) (int64, bool) {
				g.wait(r)
				return 7, tc.ready
			}

			ctx, cancel := context.WithCancel(context.Background())
			if _, err := p.OnStageSwitch(ctx, orderOf("epoch", []ChunkRef{ref}, 1)); err != nil {
				t.Fatalf("OnStageSwitch: %v", err)
			}
			startPrefetcher(t, p, ctx, cancel)
			waitPending(t, g, ref)

			// 运行(含 Fetch 内部重试)期间:键保留 → 重复入队忽略。
			if err := p.EnqueueP0(ctx, []ChunkRef{ref}); err != nil {
				t.Fatalf("EnqueueP0: %v", err)
			}
			if !p.InFlight(testDataset, ref) {
				t.Fatal("运行/重试期间键应保留(InFlight=true)")
			}

			g.releaseAll()
			// 同 TestPendingRunningDuplicateIgnored:终态清理(process)先于 OnDone
			// (调度协程),等待条件需同时覆盖两者。
			waitFor(t, "终态清理去重键且完成回调", func() bool {
				return calls.Load() >= 1 && !p.InFlight(testDataset, ref)
			})
			if got := calls.Load(); got != 1 {
				t.Fatalf("OnDone 次数 = %d, want 1", got)
			}

			p.mu.Lock()
			action := p.enqueueDedupLocked(&task{idx: -1, ref: ref, dataset: testDataset, p0: true}, priP0)
			p.mu.Unlock()
			if action != "enqueue" {
				t.Fatalf("终态后入队 action = %q, want enqueue", action)
			}
		})
	}
}

// TestPendingDownloadOnceAcrossP2AndP0:同一块先以 P2/P1 入队、再以 P0 请求 → 提升后只
// 下载一次、OnDone/OnP0Done 各恰一次、队列深度按去重后计数。
func TestPendingDownloadOnceAcrossP2AndP0(t *testing.T) {
	ref := ChunkRef{Path: "once.tar"}
	env := newTestEnv(t, []blockSpec{{path: ref.Path, content: "once-block", source: true}})

	var (
		onDone atomic.Int64
		onP0   atomic.Int64
	)
	p := env.newPrefetcher(Config{
		Lookahead: 0,
		OnDone:    func(ChunkRef, error) { onDone.Add(1) },
		OnP0Done:  func(ChunkRef, error) { onP0.Add(1) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	// K=1:块先以 P1 入队(未开始),随后 EnqueueP0 将其提升到 P0。
	if _, err := p.OnStageSwitch(ctx, orderOf("epoch", []ChunkRef{ref}, 1)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	if err := p.EnqueueP0(ctx, []ChunkRef{ref}); err != nil {
		t.Fatalf("EnqueueP0: %v", err)
	}
	if depth := p.Status().QueueDepth; depth != [4]int{1, 0, 0, 0} {
		t.Fatalf("提升后队列深度 = %v, want [1 0 0 0]", depth)
	}

	startPrefetcher(t, p, ctx, cancel)
	waitFor(t, "块完成回调", func() bool { return onDone.Load() == 1 && onP0.Load() == 1 })

	if got := blockGets(env.store); len(got) != 1 {
		t.Fatalf("数据块下载次数 = %d(%v), want 1(去重后一次)", len(got), got)
	}
	if p.InFlight(testDataset, ref) {
		t.Fatal("终态后 InFlight 应为 false")
	}
}

// TestPendingInFlightAndCurrentWindow:InFlight 仅运行态为 true;IsCurrentWindow 按当前
// 阶段/游标/lookahead(启动窗口 K 未处理完时含整个 K 窗口)判定。
func TestPendingInFlightAndCurrentWindow(t *testing.T) {
	ctx := context.Background()
	p := New(nil, nil, nil, nil, Config{Lookahead: 2})
	refs := refsNamed("w", 5)
	if _, err := p.OnStageSwitch(ctx, orderOf("epoch", refs, 2)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}

	if !p.IsCurrentWindow(testDataset, refs[0]) || !p.IsCurrentWindow(testDataset, refs[1]) {
		t.Error("启动窗口 K 内块应属于当前窗口")
	}
	if p.IsCurrentWindow(testDataset, refs[3]) {
		t.Error("K 窗口与邻域外块不应属于当前窗口")
	}
	if p.IsCurrentWindow("other", refs[0]) {
		t.Error("非本 dataset 不应属于当前窗口")
	}
	if p.IsCurrentWindow(testDataset, ChunkRef{Path: "unknown.tar"}) {
		t.Error("订单外块不应属于当前窗口")
	}

	// 已入队未开始 → 不算在途;出队后(模拟运行)算在途。
	if p.InFlight(testDataset, refs[0]) {
		t.Error("仅入队未开始的块不应算在途")
	}
	p.mu.Lock()
	tk, _ := p.popTaskLocked()
	p.mu.Unlock()
	if tk == nil {
		t.Fatal("应能出队 K 窗口首块")
	}
	if !p.InFlight(testDataset, refs[0]) {
		t.Error("已出队未终态的块应算在途")
	}
	if p.InFlight(testDataset, refs[1]) {
		t.Error("仍在队列中的块不应算在途")
	}

	// cursor 推进到 0 后,窗口为 [0,2];块 3 仍在窗口外。
	if !p.OnConsumeSideAdvance(ctx, refs[0]) {
		t.Fatal("OnConsumeSideAdvance 应返回 true")
	}
	if !p.IsCurrentWindow(testDataset, refs[2]) {
		t.Error("cursor 邻域窗口内块应属于当前窗口")
	}
	if p.IsCurrentWindow(testDataset, refs[3]) {
		t.Error("cursor+lookahead 之外块不应属于当前窗口")
	}
}
