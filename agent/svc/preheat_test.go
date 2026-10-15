package svc

// 本文件是 svc 侧的测试装置与预热任务状态机用例(§7.2):真实 core 组合(objectstore.Mock +
// 临时 CAS + fetch.Fetcher + index.Writer/NewCatalog)装配 PrefetchManager 与 PreheatTasks,
// 不依赖 env 与外部服务;等待一律带 deadline 轮询,不用裸 sleep 断言。

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/agent/prefetch"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// testDataset 是 svc 用例使用的 dataset 名(与 server_test 的 testConfig 节点名配套)。
const testDataset = "ds"

// testTimeout 是轮询/等待类断言的统一 deadline。
const testTimeout = 5 * time.Second

// svcFixture 是 svc 单测装置:Mock 源站 + 临时 CAS + fetch.Fetcher + PrefetchManager +
// PreheatTasks(与 Assemble 的装配同构,仅把 MinIO 换为 Mock、把 CacheRoot 换为临时目录)。
type svcFixture struct {
	cfg   Config
	store objectstore.Store
	cs    *cas.Store
	pm    *PrefetchManager
	tasks *PreheatTasks
}

// svcBlockSpec 描述一个测试块:内容(决定指纹/大小)+ 本地与源站可达性组合
// (语义同 prefetch 测试的 blockSpec)。
type svcBlockSpec struct {
	path    string
	chunk   int64
	content string
	local   bool // true = Commit 后留在本地 CAS(本地命中);false = Evict(需下载)
	source  bool // true = 内容放源站(D25 键),供下载路径使用
}

// newSvcFixture 构造装置:t.Cleanup 关闭管理器调度 ctx(不泄漏调度协程)。
func newSvcFixture(t *testing.T) *svcFixture {
	t.Helper()
	return newSvcFixtureWithStore(t, objectstore.NewMock())
}

// newSvcFixtureWithStore 同上,但注入定制的源站(包装 Mock 模拟特定时序)。
func newSvcFixtureWithStore(t *testing.T, store objectstore.Store) *svcFixture {
	t.Helper()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	f := fetch.New(store, cs, fetch.Options{
		MaxInflightBytes: 1 << 20,
		MaxTasks:         4,
		MaxConnsPerHost:  2,
		MaxRetries:       1,
	})
	pm := NewPrefetchManager(store, cs, f, metrics.New(metrics.Config{}))
	t.Cleanup(pm.Close)
	cfg := Config{
		NodeName:  nodeid.ID("worker-01"),
		CacheRoot: t.TempDir(),
		Verify:    VerifyXXH3,
	}
	return &svcFixture{
		cfg:   cfg,
		store: store,
		cs:    cs,
		pm:    pm,
		tasks: NewPreheatTasks(cfg, pm),
	}
}

// preloadMapping 预加载 bundle 的索引映射(白盒;模拟"订单已到达"后的 Catalog 状态:
// 预热本身不切换映射,未预加载时 P0 块一律走下载,预加载后本地命中块零下载)。
func (fx *svcFixture) preloadMapping(t *testing.T, dataset string) {
	t.Helper()
	if err := fx.pm.bundleFor(dataset).catalog.LoadPrefixes(context.Background(), []string{""}); err != nil {
		t.Fatalf("预加载索引映射: %v", err)
	}
}

// agent 返回与装置组件同构的 Agent(供 NewServer 装配)。
func (fx *svcFixture) agent() *Agent {
	return &Agent{
		Cfg:      fx.cfg,
		Metrics:  metrics.New(metrics.Config{}),
		Prefetch: fx.pm,
		Preheat:  fx.tasks,
	}
}

// blockKeyLiteral 按 D25 字面量构造块对象键(独立于实现,防键布局漂移)。
func blockKeyLiteral(dataset string, ref prefetch.ChunkRef) string {
	return dataset + "/blocks/" + ref.Path + "#" + strconv.FormatInt(ref.Chunk, 10)
}

// commitDataset 落索引:逐块 cas.Put → index.Writer.Commit → 按 specs 放源站对象 / Evict
// 本地副本;返回 ref → 指纹(断言本地落盘用)。
func (fx *svcFixture) commitDataset(t *testing.T, dataset string, specs []svcBlockSpec) map[prefetch.ChunkRef]cas.Fingerprint {
	t.Helper()
	ctx := context.Background()
	fps := make(map[prefetch.ChunkRef]cas.Fingerprint, len(specs))
	entries := make([]index.Entry, 0, len(specs))
	for _, sp := range specs {
		fp, n, err := fx.cs.Put(ctx, strings.NewReader(sp.content))
		if err != nil {
			t.Fatalf("cas.Put(%s#%d): %v", sp.path, sp.chunk, err)
		}
		ref := prefetch.ChunkRef{Path: sp.path, Chunk: sp.chunk}
		fps[ref] = fp
		entries = append(entries, index.Entry{Path: sp.path, Chunk: sp.chunk, Size: n, Fingerprint: fp})
	}
	if _, err := index.NewWriter(fx.store, fx.cs, index.Config{}).Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("index Commit(%s): %v", dataset, err)
	}
	for _, sp := range specs {
		ref := prefetch.ChunkRef{Path: sp.path, Chunk: sp.chunk}
		if sp.source {
			if _, err := fx.store.Put(ctx, blockKeyLiteral(dataset, ref), strings.NewReader(sp.content), int64(len(sp.content))); err != nil {
				t.Fatalf("源站 Put(%s#%d): %v", sp.path, sp.chunk, err)
			}
		}
		if !sp.local {
			if err := fx.cs.Evict(fps[ref]); err != nil {
				t.Fatalf("cas.Evict(%s#%d): %v", sp.path, sp.chunk, err)
			}
		}
	}
	return fps
}

// validSubmitReq 返回通过全部校验的 SubmitPreheat 请求(调用方按需改字段)。
func validSubmitReq() *agentpb.SubmitPreheatRequest {
	return &agentpb.SubmitPreheatRequest{
		TaskId:   "task-1",
		Node:     "worker-01",
		Dataset:  testDataset,
		Prefixes: []string{""},
	}
}

// waitTaskState 轮询直到任务到达 want 状态(deadline testTimeout)。
func waitTaskState(t *testing.T, tasks *PreheatTasks, taskID string, want agentpb.PreheatStatus_State) *agentpb.PreheatStatus {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		st, err := tasks.Status(context.Background(), taskID)
		if err != nil {
			t.Fatalf("Status(%s): %v", taskID, err)
		}
		if st.GetState() == want {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("任务 %s 超时未到状态 %v,当前 state=%v completed=%d failed=%d total=%d",
				taskID, want, st.GetState(), st.GetCompletedBlocks(), st.GetFailedBlocks(), st.GetTotalBlocks())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestSubmitPreheatValidation:校验顺序固定(node → task_id → dataset → snapshot_id →
// prefixes/chunks),全部 InvalidArgument 且不建任务。
func TestSubmitPreheatValidation(t *testing.T) {
	fx := newSvcFixture(t)
	tests := []struct {
		name   string
		mutate func(*agentpb.SubmitPreheatRequest)
	}{
		{"node 空", func(r *agentpb.SubmitPreheatRequest) { r.Node = "" }},
		{"node 非本节点", func(r *agentpb.SubmitPreheatRequest) { r.Node = "other-node" }},
		{"task_id 空", func(r *agentpb.SubmitPreheatRequest) { r.TaskId = "" }},
		{"dataset 空", func(r *agentpb.SubmitPreheatRequest) { r.Dataset = "" }},
		{"snapshot_id 非空", func(r *agentpb.SubmitPreheatRequest) { r.SnapshotId = "snap-1" }},
		{"prefixes 与 chunks 同空", func(r *agentpb.SubmitPreheatRequest) { r.Prefixes = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validSubmitReq()
			tt.mutate(req)
			_, err := fx.tasks.Submit(context.Background(), req)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument(err=%v)", got, err)
			}
			if st, err := fx.tasks.Status(context.Background(), req.GetTaskId()); err == nil || st != nil {
				t.Fatalf("校验失败不应建任务:Status err=%v st=%v", err, st)
			}
		})
	}
}

// TestSubmitPreheatIdempotentAndConflict:同 task_id 同内容(含顺序)→ 幂等 accepted=true;
// 同 task_id 不同内容 → AlreadyExists。
func TestSubmitPreheatIdempotentAndConflict(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "a-000.tar", chunk: 0, content: "idem-a0", local: true},
		{path: "a-001.tar", chunk: 0, content: "idem-a1", local: true},
	})
	fx.preloadMapping(t, testDataset) // 本地命中块零下载(映射已加载)

	ctx := context.Background()
	req := validSubmitReq()
	resp1, err := fx.tasks.Submit(ctx, req)
	if err != nil {
		t.Fatalf("首次 Submit: %v", err)
	}
	if !resp1.GetAccepted() || resp1.GetTaskId() != req.GetTaskId() {
		t.Fatalf("响应 = %+v, want accepted=true task_id=%s", resp1, req.GetTaskId())
	}
	waitTaskState(t, fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_DONE)

	resp2, err := fx.tasks.Submit(ctx, validSubmitReq()) // 同内容重放
	if err != nil {
		t.Fatalf("幂等 Submit: %v", err)
	}
	if !resp2.GetAccepted() || resp2.GetTaskId() != req.GetTaskId() {
		t.Fatalf("幂等响应 = %+v, want accepted=true 同 task_id", resp2)
	}
	// 幂等不重置进度:仍是 DONE。
	st, err := fx.tasks.Status(ctx, req.GetTaskId())
	if err != nil || st.GetState() != agentpb.PreheatStatus_DONE {
		t.Fatalf("幂等提交后状态 = %v(err=%v), want DONE", st, err)
	}

	diff := validSubmitReq()
	diff.Prefixes = []string{"b-"} // 内容不同
	if _, err := fx.tasks.Submit(ctx, diff); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("不同内容重试 code = %v, want AlreadyExists(err=%v)", status.Code(err), err)
	}
}

// TestSubmitPreheatDoneProgress:本地命中块逐块完成 → DONE;字段逐项映射(total/completed/
// failed/node/dataset/snapshot_id/paths/时间戳)。
func TestSubmitPreheatDoneProgress(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "a-000.tar", chunk: 0, content: "done-a0", local: true},
		{path: "a-001.tar", chunk: 0, content: "done-a1", local: true},
		{path: "a-002.tar", chunk: 1, content: "done-a2", local: true},
	})
	fx.preloadMapping(t, testDataset) // 三块均本地命中(零下载)

	req := validSubmitReq()
	req.Prefixes = []string{"a-"}
	resp, err := fx.tasks.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("响应 = %+v, want accepted=true", resp)
	}

	st := waitTaskState(t, fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_DONE)
	if st.GetTotalBlocks() != 3 || st.GetCompletedBlocks() != 3 || st.GetFailedBlocks() != 0 {
		t.Errorf("进度 = total %d / completed %d / failed %d, want 3/3/0",
			st.GetTotalBlocks(), st.GetCompletedBlocks(), st.GetFailedBlocks())
	}
	if st.GetNode() != "worker-01" || st.GetDataset() != testDataset || st.GetSnapshotId() != "" {
		t.Errorf("标识 = node %q dataset %q snapshot %q, want worker-01/ds/空",
			st.GetNode(), st.GetDataset(), st.GetSnapshotId())
	}
	if len(st.GetPaths()) != 1 || st.GetPaths()[0] != "a-" {
		t.Errorf("paths = %v, want [a-](请求 prefixes 原样)", st.GetPaths())
	}
	if st.GetStartedAt() == nil || st.GetUpdatedAt() == nil {
		t.Error("started_at/updated_at 不应为空")
	}
	if st.GetUpdatedAt().AsTime().Before(st.GetStartedAt().AsTime()) {
		t.Error("updated_at 应不早于 started_at")
	}
}

// TestSubmitPreheatFailedBlockIsError:任一块下载失败 → 任务 ERROR,failed/completed 计数
// 准确(completed + failed == total)。
func TestSubmitPreheatFailedBlockIsError(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "ok-000.tar", chunk: 0, content: "fail-ok", local: true},
		{path: "bad-000.tar", chunk: 0, content: "fail-bad"}, // 本地无、源站无 → 下载失败
	})
	fx.preloadMapping(t, testDataset) // ok 块本地命中免下载;bad 块走下载且失败

	req := validSubmitReq()
	if _, err := fx.tasks.Submit(context.Background(), req); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st := waitTaskState(t, fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_ERROR)
	if st.GetFailedBlocks() != 1 || st.GetCompletedBlocks() != 1 {
		t.Errorf("进度 = completed %d / failed %d, want 1/1", st.GetCompletedBlocks(), st.GetFailedBlocks())
	}
	if st.GetCompletedBlocks()+st.GetFailedBlocks() != st.GetTotalBlocks() {
		t.Errorf("completed+failed = %d, want total %d", st.GetCompletedBlocks()+st.GetFailedBlocks(), st.GetTotalBlocks())
	}
}

// TestSubmitPreheatChunkSelection:chunks 非空——prefixes 空时以 chunks 为准;prefixes 非空
// 时求交,交集为空立即 DONE(total = 0)。
func TestSubmitPreheatChunkSelection(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "a-000.tar", chunk: 0, content: "sel-a0", local: true},
		{path: "a-001.tar", chunk: 0, content: "sel-a1", local: true},
		{path: "b-000.tar", chunk: 0, content: "sel-b0", local: true},
	})
	fx.preloadMapping(t, testDataset)

	t.Run("仅 chunks", func(t *testing.T) {
		req := validSubmitReq()
		req.TaskId = "task-chunks-only"
		req.Prefixes = nil
		req.Chunks = []*agentpb.ChunkRef{{Path: "b-000.tar", Chunk: 0}, {Path: "b-000.tar", Chunk: 0}} // 重复去重
		if _, err := fx.tasks.Submit(context.Background(), req); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		st := waitTaskState(t, fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_DONE)
		if st.GetTotalBlocks() != 1 || st.GetCompletedBlocks() != 1 {
			t.Errorf("total/completed = %d/%d, want 1/1(去重后单块)", st.GetTotalBlocks(), st.GetCompletedBlocks())
		}
	})

	t.Run("prefixes 交 chunks", func(t *testing.T) {
		req := validSubmitReq()
		req.TaskId = "task-intersect"
		req.Prefixes = []string{"a-"}
		req.Chunks = []*agentpb.ChunkRef{
			{Path: "a-000.tar", Chunk: 0},
			{Path: "a-001.tar", Chunk: 0},
			{Path: "b-000.tar", Chunk: 0}, // 不在 a- 前缀内 → 剔除
		}
		if _, err := fx.tasks.Submit(context.Background(), req); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		st := waitTaskState(t, fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_DONE)
		if st.GetTotalBlocks() != 2 || st.GetCompletedBlocks() != 2 {
			t.Errorf("total/completed = %d/%d, want 2/2(交集)", st.GetTotalBlocks(), st.GetCompletedBlocks())
		}
	})

	t.Run("交集为空立即 DONE", func(t *testing.T) {
		req := validSubmitReq()
		req.TaskId = "task-empty-intersect"
		req.Prefixes = []string{"a-"}
		req.Chunks = []*agentpb.ChunkRef{{Path: "zzz-000.tar", Chunk: 0}}
		resp, err := fx.tasks.Submit(context.Background(), req)
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("响应 = %+v, want accepted=true", resp)
		}
		st, err := fx.tasks.Status(context.Background(), req.GetTaskId())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.GetState() != agentpb.PreheatStatus_DONE || st.GetTotalBlocks() != 0 {
			t.Errorf("state/total = %v/%d, want DONE/0(交集为空立即结束)",
				st.GetState(), st.GetTotalBlocks())
		}
	})

	t.Run("无 CURRENT 视作空", func(t *testing.T) {
		req := validSubmitReq()
		req.TaskId = "task-no-current"
		req.Dataset = "no-such-dataset"
		resp, err := fx.tasks.Submit(context.Background(), req)
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("响应 = %+v, want accepted=true", resp)
		}
		st, err := fx.tasks.Status(context.Background(), req.GetTaskId())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.GetState() != agentpb.PreheatStatus_DONE || st.GetTotalBlocks() != 0 {
			t.Errorf("state/total = %v/%d, want DONE/0(ErrNoCurrent 视作空列表)",
				st.GetState(), st.GetTotalBlocks())
		}
	})
}

// TestSubmitPreheatDownloadWithoutOrder:预热先于订单到达 —— 未绑定 dataset 时播种最小订单,
// 未命中块走 D25 键下载并完成 DONE(本地副本落盘)。
func TestSubmitPreheatDownloadWithoutOrder(t *testing.T) {
	fx := newSvcFixture(t)
	fps := fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "r-000.tar", chunk: 0, content: "preheat-remote", source: true},
	})

	req := validSubmitReq()
	if _, err := fx.tasks.Submit(context.Background(), req); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st := waitTaskState(t, fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_DONE)
	if st.GetCompletedBlocks() != 1 || st.GetFailedBlocks() != 0 {
		t.Errorf("进度 = completed %d / failed %d, want 1/0", st.GetCompletedBlocks(), st.GetFailedBlocks())
	}

	// 下载路径确实用了 D25 键,且块已落本地。
	keys, err := fx.store.List(context.Background(), testDataset+"/blocks/")
	if err != nil {
		t.Fatalf("store.List: %v", err)
	}
	if len(keys) != 1 || keys[0] != blockKeyLiteral(testDataset, prefetch.ChunkRef{Path: "r-000.tar"}) {
		t.Errorf("源站键 = %v, want [%s]", keys, blockKeyLiteral(testDataset, prefetch.ChunkRef{Path: "r-000.tar"}))
	}
	br, err := fx.cs.Open(fps[prefetch.ChunkRef{Path: "r-000.tar"}])
	if err != nil {
		t.Fatalf("下载后本地 Open: %v", err)
	}
	_ = br.Close()
}

// TestPreheatStatusNotFound:task_id 缺失(空或不存在)→ NotFound。
func TestPreheatStatusNotFound(t *testing.T) {
	fx := newSvcFixture(t)
	for _, id := range []string{"", "no-such-task"} {
		_, err := fx.tasks.Status(context.Background(), id)
		if got := status.Code(err); got != codes.NotFound {
			t.Errorf("Status(%q) code = %v, want NotFound(err=%v)", id, got, err)
		}
	}
}

// TestPreheatRunningCount:QUEUED/RUNNING 计入,DONE/ERROR 不计(GetNodeStatus.running_tasks)。
func TestPreheatRunningCount(t *testing.T) {
	fx := newSvcFixture(t)
	if got := fx.tasks.RunningCount(); got != 0 {
		t.Fatalf("空表 RunningCount = %d, want 0", got)
	}

	// 白盒登记三个任务:QUEUED / RUNNING / DONE。
	mk := func(id string, state agentpb.PreheatStatus_State) *preheatTask {
		tk := newPreheatTask(id, "worker-01", preheatRequest{dataset: testDataset})
		tk.state.Store(int32(state))
		return tk
	}
	fx.tasks.mu.Lock()
	fx.tasks.tasks["t-q"] = mk("t-q", agentpb.PreheatStatus_QUEUED)
	fx.tasks.tasks["t-r"] = mk("t-r", agentpb.PreheatStatus_RUNNING)
	fx.tasks.tasks["t-d"] = mk("t-d", agentpb.PreheatStatus_DONE)
	fx.tasks.mu.Unlock()

	if got := fx.tasks.RunningCount(); got != 2 {
		t.Errorf("RunningCount = %d, want 2(QUEUED+RUNNING)", got)
	}
}

// TestPreheatCallbackPathLockFree:回调路径不取表锁(持表锁时直接触发 onP0Done 立即返回并
// 完成计数;若回调取表锁会自锁,经超时兜底发现)。同 ref 两次回调按 pending 计数各记一次。
func TestPreheatCallbackPathLockFree(t *testing.T) {
	fx := newSvcFixture(t)
	ref := prefetch.ChunkRef{Path: "lockfree.tar", Chunk: 0}
	tk := newPreheatTask("t-lockfree", "worker-01", preheatRequest{dataset: testDataset})
	fx.tasks.mu.Lock()
	fx.tasks.tasks[tk.taskID] = tk
	fx.tasks.mu.Unlock()
	fx.tasks.register(tk, []prefetch.ChunkRef{ref, ref}) // 同 ref 两次入队 → pending = 2

	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.tasks.mu.Lock() // 持表锁:回调若取表锁将自锁
		defer fx.tasks.mu.Unlock()
		fx.tasks.onP0Done(testDataset, ref, nil)
		fx.tasks.onP0Done(testDataset, ref, nil)
	}()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("回调路径疑似取了表锁(持锁调用未返回)")
	}

	st, err := fx.tasks.Status(context.Background(), tk.taskID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetState() != agentpb.PreheatStatus_DONE || st.GetCompletedBlocks() != 2 || st.GetFailedBlocks() != 0 {
		t.Errorf("状态 = %v(completed %d / failed %d), want DONE(2/0)",
			st.GetState(), st.GetCompletedBlocks(), st.GetFailedBlocks())
	}
}

// TestPreheatClaimRejectsUnknownRef:重复/未知 ref 的回调不重复计数(claim 返回 false)。
func TestPreheatClaimRejectsUnknownRef(t *testing.T) {
	tk := newPreheatTask("t-claim", "worker-01", preheatRequest{dataset: testDataset})
	ref := prefetch.ChunkRef{Path: "x.tar", Chunk: 0}
	tk.pending = map[prefetch.ChunkRef]int{ref: 1}
	tk.total.Store(1)

	if !tk.claim(ref, nil) {
		t.Fatal("首次 claim 应认领")
	}
	if tk.claim(ref, nil) {
		t.Fatal("重复 claim 不应认领(回调数已耗尽)")
	}
	if got := tk.completed.Load(); got != 1 {
		t.Errorf("completed = %d, want 1(重复回调不计数)", got)
	}
	if tk.stateNow() != agentpb.PreheatStatus_DONE {
		t.Errorf("state = %v, want DONE", tk.stateNow())
	}
}

// TestPreheatSubmitCanceledCtx:Submit 入口 ctx 已取消 → Canceled,且不建任务(与
// prefetch 各方法同口径)。
func TestPreheatSubmitCanceledCtx(t *testing.T) {
	fx := newSvcFixture(t)
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := validSubmitReq()
	req.Prefixes = nil
	req.Chunks = []*agentpb.ChunkRef{{Path: "x-000.tar", Chunk: 0}} // 免 List,直达解析
	_, err := fx.tasks.Submit(cctx, req)
	if got := status.Code(err); got != codes.Canceled {
		t.Fatalf("code = %v, want Canceled(err=%v)", got, err)
	}
	if _, err := fx.tasks.Status(context.Background(), req.GetTaskId()); status.Code(err) != codes.NotFound {
		t.Errorf("取消的提交不应建任务:Status err = %v, want NotFound", err)
	}
}

// cancelOnShardGet 包装 Mock:在首次读取索引分片后取消 ctx(模拟"块列表解析成功、P0 入队
// 前 ctx 已取消"的窗口;分片键含 "/shard-",属冻结索引格式)。arm 之前取消钩子不生效
// (Commit 自身也会读回分片)。
type cancelOnShardGet struct {
	*objectstore.Mock
	mu     sync.Mutex
	cancel context.CancelFunc
	done   bool
}

// arm 装载取消钩子(需在提交索引之后调用)。
func (s *cancelOnShardGet) arm(cancel context.CancelFunc) {
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
}

func (s *cancelOnShardGet) Get(ctx context.Context, key string, w io.Writer) error {
	err := s.Mock.Get(ctx, key, w) // 先让本次读取成功
	s.mu.Lock()
	if !s.done && s.cancel != nil && strings.Contains(key, "/shard-") {
		s.done = true
		s.cancel()
	}
	s.mu.Unlock()
	return err
}

// TestPreheatEnqueueFailureIsError:解析成功后的 P0 入队失败(ctx 在解析后取消)→ 任务受理
// 但状态 ERROR(入队失败不回滚受理,状态查询可见)。
func TestPreheatEnqueueFailureIsError(t *testing.T) {
	store := &cancelOnShardGet{Mock: objectstore.NewMock()}
	fx := newSvcFixtureWithStore(t, store)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "e-000.tar", chunk: 0, content: "enq-fail", source: true},
	})

	ctx, cancel := context.WithCancel(context.Background())
	store.arm(cancel)
	t.Cleanup(cancel)

	req := validSubmitReq()
	resp, err := fx.tasks.Submit(ctx, req)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("响应 = %+v, want accepted=true(入队失败不回滚受理)", resp)
	}
	st, err := fx.tasks.Status(context.Background(), req.GetTaskId())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetState() != agentpb.PreheatStatus_ERROR {
		t.Errorf("state = %v, want ERROR(入队失败)", st.GetState())
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("前置条件:解析后 ctx 应已被取消(存储钩子未生效)")
	}
}
