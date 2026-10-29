package svc

// M12 log assertions for docs/9.1 plan §4.2. These tests drive the
// production code paths (preheat lifecycle + evictor round + agentd startup)
// and assert the structured JSON records (message + required fields), so
// renaming a message or dropping a field fails here.
//
// Slog usage: production code uses the global slog.Info/Warn/Error. We
// install a JSON-handler logger via slog.SetDefault at the top of each test
// so every slog call lands in our capture buffer (same pattern as
// controller/internal/controller/warmup/m12_logs_assert_test.go).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/evictor"
	"traincache/agent/nodeid"
	"traincache/agent/prefetch"
	"traincache/core/cas"
)

// ————————————————————————————————————————————————————
// slog 捕获 helper（直接抄 controller m12 模式）
// ————————————————————————————————————————————————————

func m12CaptureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)
	return &buf
}

func m12LogRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("invalid log line %q: %v", raw, err)
		}
		out = append(out, rec)
	}
	return out
}

func m12FindLog(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for _, rec := range m12LogRecords(t, buf) {
		if rec["msg"] == msg {
			return rec
		}
	}
	t.Fatalf("log %q not found in:\n%s", msg, buf.String())
	return nil
}

func m12RequireFields(t *testing.T, rec map[string]any, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if _, ok := rec[field]; !ok {
			t.Fatalf("log %q missing field %q: %#v", rec["msg"], field, rec)
		}
	}
}

func m12RequireLevel(t *testing.T, rec map[string]any, want string) {
	t.Helper()
	if rec["level"] != want {
		t.Fatalf("log %q level = %v, want %s", rec["msg"], rec["level"], want)
	}
}

// m12FindAllLogs 返回所有 msg 精确匹配的记录。
func m12FindAllLogs(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, rec := range m12LogRecords(t, buf) {
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// ————————————————————————————————————————————————————
// preheat 共用 fixture
// ————————————————————————————————————————————————————

// m12PreheatCfg 返回 preheat 测试需要的最小 Config（pm=nil 让 Submit 立即返回）。
func m12PreheatCfg() Config {
	return Config{NodeName: nodeid.ID("worker-01")}
}

// m12ValidSubmitReq 返回通过全部校验的 SubmitPreheat 请求。
func m12ValidSubmitReq(gen uint64) *agentpb.SubmitPreheatRequest {
	return &agentpb.SubmitPreheatRequest{
		TaskId:     "task-m12",
		Node:       "worker-01",
		Dataset:    testDataset,
		Prefixes:   []string{""},
		Generation: gen,
	}
}

// ————————————————————————————————————————————————————
// 1. Submit 生命周期 → "任务结束" Info
// ————————————————————————————————————————————————————

// TestM12PreheatSubmitLifecycleLogs: 手工登记任务 + 置位完成计数 → finish()
// 触发 "preheat: 任务结束" Info，7 字段齐全。
func TestM12PreheatSubmitLifecycleLogs(t *testing.T) {
	buf := m12CaptureLogs(t)

	tasks := NewPreheatTasks(m12PreheatCfg(), nil)

	// 白盒：手工登记一个任务，带 3 个块引用，completed=total。
	tk := newPreheatTask("task-end", "worker-01", preheatRequest{dataset: testDataset})
	refs := []prefetch.ChunkRef{
		{Path: "a-000.tar", Chunk: 0},
		{Path: "a-001.tar", Chunk: 0},
		{Path: "a-002.tar", Chunk: 1},
	}
	tasks.mu.Lock()
	tasks.tasks[tk.taskID] = tk
	tasks.mu.Unlock()
	tasks.register(tk, refs)

	// 标记所有块已完成（completed + failed == total），直接调 finish() 触发日志。
	tk.completed.Store(3)
	tk.total.Store(3)
	tk.finish()

	rec := m12FindLog(t, buf, "preheat: 任务结束")
	m12RequireLevel(t, rec, "INFO")
	m12RequireFields(t, rec,
		"task_id", "dataset", "state",
		"total_blocks", "completed_blocks", "failed_blocks",
	)
	if rec["task_id"] != "task-end" {
		t.Errorf("task_id = %v, want task-end", rec["task_id"])
	}
	if rec["dataset"] != testDataset {
		t.Errorf("dataset = %v, want %s", rec["dataset"], testDataset)
	}
	if rec["state"] != "DONE" {
		t.Errorf("state = %v, want DONE", rec["state"])
	}
	if got := rec["total_blocks"].(float64); got != 3 {
		t.Errorf("total_blocks = %v, want 3", got)
	}
	if got := rec["completed_blocks"].(float64); got != 3 {
		t.Errorf("completed_blocks = %v, want 3", got)
	}
	if got := rec["failed_blocks"].(float64); got != 0 {
		t.Errorf("failed_blocks = %v, want 0", got)
	}
}

// ————————————————————————————————————————————————————
// 2. 陈旧 generation 三处 Warn
// ————————————————————————————————————————————————————

// TestM12PreheatRejectStaleGenerationLogs: 先后三次陈旧 generation 触发点
// （Submit 重复 / Update 低代 / Cancel 低代）各自打一条 Warn，四字段齐全。
func TestM12PreheatRejectStaleGenerationLogs(t *testing.T) {
	buf := m12CaptureLogs(t)

	tasks := NewPreheatTasks(m12PreheatCfg(), nil)
	ctx := context.Background()

	// 先受理 generation=1（pm=nil → markError → state=ERROR；generation 推进到 1）。
	req1 := m12ValidSubmitReq(1)
	req1.TaskId = "task-stale"
	if _, err := tasks.Submit(ctx, req1); err != nil {
		t.Fatalf("Submit gen=1: %v", err)
	}

	// 2a. 再 Submit 同一 task_id 但 generation=0 → 被拒绝（SubmitPreheat）。
	req0 := m12ValidSubmitReq(0)
	req0.TaskId = "task-stale"
	if _, err := tasks.Submit(ctx, req0); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Submit gen=0 code = %v, want FailedPrecondition(err=%v)", status.Code(err), err)
	}

	// 2b. Update generation=0 → 被拒绝（UpdatePreheat）。
	upd := &agentpb.UpdatePreheatRequest{
		TaskId:     "task-stale",
		Node:       "worker-01",
		Generation: 0,
		Chunks:     []*agentpb.ChunkRef{{Path: "x.tar", Chunk: 0}},
	}
	if _, err := tasks.Update(ctx, upd); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Update gen=0 code = %v, want FailedPrecondition(err=%v)", status.Code(err), err)
	}

	// 2c. Cancel generation=0 → 被拒绝（CancelPreheat）。
	cnl := &agentpb.CancelPreheatRequest{
		TaskId:     "task-stale",
		Node:       "worker-01",
		Generation: 0,
	}
	if _, err := tasks.Cancel(ctx, cnl); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Cancel gen=0 code = %v, want FailedPrecondition(err=%v)", status.Code(err), err)
	}

	// 断言三条 "拒绝陈旧 generation" Warn 各出现一次，rpc 字段正确。
	recs := m12FindAllLogs(t, buf, "preheat: 拒绝陈旧 generation")
	if len(recs) != 3 {
		t.Fatalf("陈旧 generation Warn 条数 = %d, want 3\n%s", len(recs), buf.String())
	}

	// 收集出现过的 rpc 值。
	rpcs := make(map[string]bool)
	for _, rec := range recs {
		m12RequireLevel(t, rec, "WARN")
		m12RequireFields(t, rec, "rpc", "task_id", "generation", "current_generation")
		rpcs[rec["rpc"].(string)] = true
		if rec["task_id"] != "task-stale" {
			t.Errorf("task_id = %v, want task-stale", rec["task_id"])
		}
	}
	for _, want := range []string{"SubmitPreheat", "UpdatePreheat", "CancelPreheat"} {
		if !rpcs[want] {
			t.Errorf("缺失 rpc=%s 的拒绝日志; got rpcs=%v", want, rpcs)
		}
	}
}

// ————————————————————————————————————————————————————
// 3. Update 窗口全量更新 Info
// ————————————————————————————————————————————————————

// TestM12PreheatUpdateWindowLogs: Submit 受理后 Update 新 generation + chunks
// → "任务窗口全量更新" Info，四字段齐全。
func TestM12PreheatUpdateWindowLogs(t *testing.T) {
	buf := m12CaptureLogs(t)

	tasks := NewPreheatTasks(m12PreheatCfg(), nil)
	ctx := context.Background()

	req := m12ValidSubmitReq(1)
	req.TaskId = "task-update"
	if _, err := tasks.Submit(ctx, req); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	upd := &agentpb.UpdatePreheatRequest{
		TaskId:     "task-update",
		Node:       "worker-01",
		Generation: 2,
		Chunks: []*agentpb.ChunkRef{
			{Path: "new-00.tar", Chunk: 0},
			{Path: "new-01.tar", Chunk: 0},
			{Path: "new-02.tar", Chunk: 1},
		},
	}
	if _, err := tasks.Update(ctx, upd); err != nil {
		t.Fatalf("Update: %v", err)
	}

	rec := m12FindLog(t, buf, "preheat: 任务窗口全量更新")
	m12RequireLevel(t, rec, "INFO")
	m12RequireFields(t, rec, "task_id", "generation", "chunks", "seq")
	if rec["task_id"] != "task-update" {
		t.Errorf("task_id = %v, want task-update", rec["task_id"])
	}
	if got := rec["generation"].(float64); got != 2 {
		t.Errorf("generation = %v, want 2", got)
	}
	if got := rec["chunks"].(float64); got != 3 {
		t.Errorf("chunks = %v, want 3", got)
	}
}

// ————————————————————————————————————————————————————
// 4. Cancel 存在 + 不存在 两种路径
// ————————————————————————————————————————————————————

// TestM12PreheatCancelLogs: Cancel 存在的任务 → "任务取消(在途 P0 不取消)" Info；
// Cancel 不存在的 task_id → "取消不存在的任务(幂等空操作)" Info。
func TestM12PreheatCancelLogs(t *testing.T) {
	buf := m12CaptureLogs(t)

	tasks := NewPreheatTasks(m12PreheatCfg(), nil)
	ctx := context.Background()

	// 先受理一个任务。
	req := m12ValidSubmitReq(1)
	req.TaskId = "task-cancel-exist"
	if _, err := tasks.Submit(ctx, req); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// 4a. Cancel 存在的任务。
	_, err := tasks.Cancel(ctx, &agentpb.CancelPreheatRequest{
		TaskId:     "task-cancel-exist",
		Node:       "worker-01",
		Generation: 1,
	})
	if err != nil {
		t.Fatalf("Cancel 存在: %v", err)
	}
	recCancel := m12FindLog(t, buf, "preheat: 任务取消(撤回未完成的下载,D62)")
	m12RequireLevel(t, recCancel, "INFO")
	m12RequireFields(t, recCancel, "task_id", "generation", "state", "withdrawn_blocks")
	if recCancel["task_id"] != "task-cancel-exist" {
		t.Errorf("task_id = %v, want task-cancel-exist", recCancel["task_id"])
	}

	// 4b. Cancel 不存在的任务（幂等空操作）。
	_, err = tasks.Cancel(ctx, &agentpb.CancelPreheatRequest{
		TaskId:     "task-cancel-nonexist",
		Node:       "worker-01",
		Generation: 1,
	})
	if err != nil {
		t.Fatalf("Cancel 不存在: %v (应幂等)", err)
	}
	recNoop := m12FindLog(t, buf, "preheat: 取消不存在的任务(幂等空操作)")
	m12RequireLevel(t, recNoop, "INFO")
	m12RequireFields(t, recNoop, "task_id", "generation")
	if recNoop["task_id"] != "task-cancel-nonexist" {
		t.Errorf("task_id = %v, want task-cancel-nonexist", recNoop["task_id"])
	}
}

// ————————————————————————————————————————————————————
// 5. Cancel 后再 Submit → "已取消任务重新提交视为新任务"
// ————————————————————————————————————————————————————

// TestM12PreheatCancelResubmitLogs: 先 Cancel 一个任务，再 Submit 同一 task_id
// → 命中 "已取消任务重新提交视为新任务" Info。
func TestM12PreheatCancelResubmitLogs(t *testing.T) {
	buf := m12CaptureLogs(t)

	tasks := NewPreheatTasks(m12PreheatCfg(), nil)
	ctx := context.Background()

	req := m12ValidSubmitReq(1)
	req.TaskId = "task-resubmit"
	if _, err := tasks.Submit(ctx, req); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Cancel 它。
	if _, err := tasks.Cancel(ctx, &agentpb.CancelPreheatRequest{
		TaskId:     "task-resubmit",
		Node:       "worker-01",
		Generation: 1,
	}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// 同 task_id + 同 generation 再 Submit → 视为新任务。
	req2 := m12ValidSubmitReq(1)
	req2.TaskId = "task-resubmit"
	if _, err := tasks.Submit(ctx, req2); err != nil {
		t.Fatalf("Submit after Cancel: %v", err)
	}

	rec := m12FindLog(t, buf, "preheat: 已取消任务重新提交视为新任务")
	m12RequireLevel(t, rec, "INFO")
	m12RequireFields(t, rec, "task_id", "generation")
	if rec["task_id"] != "task-resubmit" {
		t.Errorf("task_id = %v, want task-resubmit", rec["task_id"])
	}
}

// ————————————————————————————————————————————————————
// evictor 共用 fixture（黑盒，import evictor 包）
// ————————————————————————————————————————————————————

// m12FakeUsage 是注入的利用率来源：每次调用按序取 values 中的比例。
type m12FakeUsage struct {
	values []float64
	total  uint64
	idx    int
}

func (f *m12FakeUsage) use() (uint64, uint64, error) {
	if len(f.values) == 0 {
		return uint64(0.85 * float64(f.total)), f.total, nil
	}
	v := f.values[min(f.idx, len(f.values)-1)]
	f.idx++
	return uint64(v * float64(f.total)), f.total, nil
}

// m12FakeHeat 是注入的热度来源：固定返回 entries 快照。
type m12FakeHeat struct {
	entries []evictor.EvictionEntry
}

func (f *m12FakeHeat) ListForEviction() []evictor.EvictionEntry {
	return f.entries
}

// m12EvictFixture 是 evictor 测试最小 fixture。
type m12EvictFixture struct {
	cs    *cas.Store
	heat  *m12FakeHeat
	usage *m12FakeUsage
	ev    *evictor.Evictor
}

func newM12EvictFixture(t *testing.T, usageValues ...float64) *m12EvictFixture {
	t.Helper()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	fx := &m12EvictFixture{
		cs:    cs,
		heat:  &m12FakeHeat{},
		usage: &m12FakeUsage{values: usageValues, total: 10000},
	}
	fx.ev = evictor.New(cs, fx.heat, nil, nil, nil, evictor.Config{
		Dataset: "ds",
		Usage:   fx.usage.use,
	})
	return fx
}

// putEvictBlock 在 CAS 落一个块，返回指纹。
func putEvictBlock(t *testing.T, cs *cas.Store, content string) evictor.EvictionEntry {
	t.Helper()
	fp, _, err := cs.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	return evictor.EvictionEntry{
		FP:        fp,
		Heat:      0.1,
		LastTouch: time.Now(),
	}
}

// ————————————————————————————————————————————————————
// 6. RunOnce → "淘汰轮结束" Info + 6 字段
// ————————————————————————————————————————————————————

// TestM12EvictorRoundEndLog: util 超触发线 (85%) → RunOnce → 触发淘汰 →
// "evictor: 淘汰轮结束" Info，6 字段齐全。
func TestM12EvictorRoundEndLog(t *testing.T) {
	buf := m12CaptureLogs(t)

	// 利用率 0.85 触发；淘汰一块后降到 0.74 ≤ k-Δ=0.75，停止。
	fx := newM12EvictFixture(t, 0.85, 0.74)

	e1 := putEvictBlock(t, fx.cs, "m12-round-block-1")
	e2 := putEvictBlock(t, fx.cs, "m12-round-block-2")
	e3 := putEvictBlock(t, fx.cs, "m12-round-block-3")
	fx.heat.entries = []evictor.EvictionEntry{e1, e2, e3}

	fx.ev.RunOnce(context.Background())

	rec := m12FindLog(t, buf, "evictor: 淘汰轮结束")
	m12RequireLevel(t, rec, "INFO")
	m12RequireFields(t, rec,
		"dataset", "result", "evicted_blocks", "protected_skipped",
		"disk_util", "bytes",
	)
	if rec["dataset"] != "ds" {
		t.Errorf("dataset = %v, want ds", rec["dataset"])
	}
	if rec["result"] != "normal" {
		t.Errorf("result = %v, want normal", rec["result"])
	}
	if got := rec["evicted_blocks"].(float64); got < 1 {
		t.Errorf("evicted_blocks = %v, want >= 1", got)
	}
}

// ————————————————————————————————————————————————————
// 7. RunOnce → 危险线 Error + 7 字段
// ————————————————————————————————————————————————————

// TestM12EvictorDangerLineLog: util 超 M (95%) → RunOnce 先打
// "evictor: 超危险线,激进淘汰" Error，再打 "淘汰轮结束" Info。
func TestM12EvictorDangerLineLog(t *testing.T) {
	buf := m12CaptureLogs(t)

	// 0.95 超 M(90%) → danger；淘汰 3 块后降到 0.70 ≤ k-2Δ=0.70，停止。
	fx := newM12EvictFixture(t, 0.95, 0.85, 0.78, 0.70)

	fx.heat.entries = []evictor.EvictionEntry{
		putEvictBlock(t, fx.cs, "m12-danger-block-1"),
		putEvictBlock(t, fx.cs, "m12-danger-block-2"),
		putEvictBlock(t, fx.cs, "m12-danger-block-3"),
	}

	fx.ev.RunOnce(context.Background())

	// 先断言危险线 Error 日志。
	danger := m12FindLog(t, buf, "evictor: 超危险线,激进淘汰")
	m12RequireLevel(t, danger, "ERROR")
	m12RequireFields(t, danger,
		"dataset", "disk_util", "disk_trigger",
		"bytes", "bytes_trigger", "capacity_bytes",
	)
	if danger["dataset"] != "ds" {
		t.Errorf("dataset = %v, want ds", danger["dataset"])
	}

	// 再断言 "淘汰轮结束" 存在且 result=danger。
	round := m12FindLog(t, buf, "evictor: 淘汰轮结束")
	m12RequireLevel(t, round, "INFO")
	m12RequireFields(t, round,
		"dataset", "result", "evicted_blocks", "protected_skipped",
		"disk_util", "bytes",
	)
	if round["result"] != "danger" {
		t.Errorf("result = %v, want danger", round["result"])
	}
}

// ————————————————————————————————————————————————————
// 8. HookSock 就绪 Info + 三种帧错误 Warn（hooksock.go slog 全覆）
// ————————————————————————————————————————————————————

// m12threadSafeBuf 是 bytes.Buffer 的并发安全包装:HookSock readLoop(goroutine)
// 与主线程同时 Write/Read 时无 race(Go 默认 bytes.Buffer 不是并发安全的)。
type m12threadSafeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *m12threadSafeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *m12threadSafeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// m12CaptureLogsTS 同 m12CaptureLogs,但返回线程安全 buffer(hooksock 并发写场景专用)。
func m12CaptureLogsTS(t *testing.T) *m12threadSafeBuf {
	t.Helper()
	var buf m12threadSafeBuf
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)
	return &buf
}

// m12FindLogTS 从线程安全 buffer 中查找指定 msg 的 JSON 日志行。
func m12FindLogTS(t *testing.T, buf *m12threadSafeBuf, msg string) map[string]any {
	t.Helper()
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("invalid log line %q: %v", raw, err)
		}
		if rec["msg"] == msg {
			return rec
		}
	}
	t.Fatalf("log %q not found in:\n%s", msg, buf.String())
	return nil
}

// m12WaitForLogTS 带 deadline 轮询等待指定 msg 出现在线程安全 buffer 里。
// buf 读写均加锁,goroutine 写 / 主线程读无 race。
func m12WaitForLogTS(t *testing.T, buf *m12threadSafeBuf, msg string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if raw == "" {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(raw), &rec); err == nil && rec["msg"] == msg {
				return rec
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待日志 %q 超时(%v),buf=%s", msg, timeout, buf.String())
	return nil
}

// TestM12HookSockReadyAndFrameErrors:启动收端 → 断言 "hooksock: 钩子收端就绪" Info;
// 依次直写三种坏帧（超 4 KiB / 非法 JSON / 未知类型）→ 断言对应 Warn,path 字段等于
// 临时套接字路径,size/err/type 字段齐全。
func TestM12HookSockReadyAndFrameErrors(t *testing.T) {
	buf := m12CaptureLogsTS(t)

	path := hookPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(path, nil, HookHandlers{}) // m=nil 跳过 metrics 计数;handlers 全 nil 仅测日志
	if err := s.Start(ctx); err != nil {
		t.Fatalf("HookSock.Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = s.Close()
	})

	// 就绪 Info 在 Start 内同步打印(在 readLoop goroutine 启动之前),Start 返回即已落 buf。
	ready := m12FindLogTS(t, buf, "hooksock: 钩子收端就绪")
	m12RequireLevel(t, ready, "INFO")
	m12RequireFields(t, ready, "path")
	if ready["path"] != path {
		t.Errorf("ready log path = %v, want %s", ready["path"], path)
	}

	c := dialHook(t, path)

	// 8a. 超 4 KiB 帧:整报丢弃,带 size 字段。
	base := `{"type":"advance","path":"p","chunk":1}`
	oversize := base + strings.Repeat(" ", 5*1024-len(base)) // 5 KiB
	sendHook(t, c, []byte(oversize))
	recSize := m12WaitForLogTS(t, buf, "hooksock: 帧超过 4 KiB,丢弃", hookTestTimeout)
	m12RequireLevel(t, recSize, "WARN")
	m12RequireFields(t, recSize, "path", "size")
	if recSize["path"] != path {
		t.Errorf("oversize log path = %v, want %s", recSize["path"], path)
	}
	if got := recSize["size"].(float64); got <= float64(hookMaxFrame) {
		t.Errorf("oversize log size = %v, want > hookMaxFrame(%d)", got, hookMaxFrame)
	}

	// 8b. 非法 JSON:带 err 字段。
	sendHook(t, c, []byte("not a json frame"))
	recParse := m12WaitForLogTS(t, buf, "hooksock: 帧 JSON 非法,丢弃", hookTestTimeout)
	m12RequireLevel(t, recParse, "WARN")
	m12RequireFields(t, recParse, "path", "err")
	if recParse["path"] != path {
		t.Errorf("parse log path = %v, want %s", recParse["path"], path)
	}
	if recParse["err"] == "" {
		t.Error("parse log err 字段为空")
	}

	// 8c. 未知帧类型:带 type 字段。
	sendHook(t, c, []byte(`{"type":"totally-unknown","path":"p","chunk":9}`))
	recType := m12WaitForLogTS(t, buf, "hooksock: 未知帧类型,丢弃", hookTestTimeout)
	m12RequireLevel(t, recType, "WARN")
	m12RequireFields(t, recType, "path", "type")
	if recType["path"] != path {
		t.Errorf("unknown-type log path = %v, want %s", recType["path"], path)
	}
	if recType["type"] != "totally-unknown" {
		t.Errorf("unknown-type log type = %v, want totally-unknown", recType["type"])
	}

	// 8d. 读帧失败(收端退出):读循环遇到非 ErrClosed 的读错误时打 Warn,带
	// path 与 err 字段。用读超时制造该错误,不依赖对端关闭。
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		t.Fatal("HookSock conn 未建立")
	}
	if err := conn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("设置读超时: %v", err)
	}
	recFail := m12WaitForLogTS(t, buf, "hooksock: 读帧失败,收端退出", hookTestTimeout)
	m12RequireLevel(t, recFail, "WARN")
	m12RequireFields(t, recFail, "path", "err")
	if recFail["path"] != path {
		t.Errorf("read-failure log path = %v, want %s", recFail["path"], path)
	}
}

// ————————————————————————————————————————————————————
// 测试内省：避免 io 未使用告警（helper io.EOF 用于签名参考）
// ————————————————————————————————————————————————————

var _ = io.EOF
