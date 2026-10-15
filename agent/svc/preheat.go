package svc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/prefetch"
	"traincache/core/index"
)

// PreheatTasks 是预热任务状态机(design §7.2;D7:任务引用 =(dataset, snapshot_id,
// prefixes, chunks);状态 QUEUED→RUNNING→DONE/ERROR)。task_id 幂等:内容摘要相同幂等
// 返回现有任务,不同返回 AlreadyExists。块进度经 prefetch.Config.OnP0Done 逐块回调
// (回调在预取器调度协程内同步;本表回调路径只做 atomic 计数与无锁查表,不取表锁,不阻塞)。
// 任务记录不做 TTL 清理(留 M6/Q13)。
type PreheatTasks struct {
	cfg Config
	pm  *PrefetchManager

	mu    sync.Mutex
	tasks map[string]*preheatTask

	// byDataset 是 dataset → 任务追加表(回调按 dataset 定位任务的只读快照;登记任务时
	// 以 COW 整体替换,回调路径无锁读)。
	byDataset atomic.Pointer[map[string][]*preheatTask]
}

// preheatRequest 是任务内容摘要(D7 任务引用四元组;task_id 幂等/冲突判据)。
type preheatRequest struct {
	dataset    string
	snapshotID string
	prefixes   []string
	chunks     []prefetch.ChunkRef
}

// matches 判断两次请求内容是否一致(元素顺序参与判定:顺序不同视为不同任务)。
func (a preheatRequest) matches(b preheatRequest) bool {
	return a.dataset == b.dataset && a.snapshotID == b.snapshotID &&
		slices.Equal(a.prefixes, b.prefixes) && slices.Equal(a.chunks, b.chunks)
}

// preheatTask 是一个预热任务(§7.2 状态机;字段与 §5.1 PreheatStatus 一一对应)。
type preheatTask struct {
	taskID string
	req    preheatRequest
	node   string

	// pending 是期望的 P0 完成回调数(按 ref;重复 ref 计多次)。登记(register)后仅
	// 预取器调度协程读写:同一 bundle 的 P0 完成回调在该协程内串行,无并发。
	pending map[prefetch.ChunkRef]int

	total     atomic.Int64 // 块总数(登记时定盘;Status 读取无锁)
	completed atomic.Int64
	failed    atomic.Int64
	state     atomic.Int32 // agentpb.PreheatStatus_State
	updatedAt atomic.Int64 // UnixNano(每次计数/状态变更刷新)

	startedAt time.Time // 任务创建时刻(只读)
}

// newPreheatTask 构造 QUEUED 任务(clone 摘要切片防调用方别名)。
func newPreheatTask(taskID, node string, req preheatRequest) *preheatTask {
	t := &preheatTask{
		taskID:    taskID,
		node:      node,
		req:       req,
		startedAt: time.Now(),
	}
	t.touch()
	return t
}

// stateNow 返回当前状态(atomic 读)。
func (t *preheatTask) stateNow() agentpb.PreheatStatus_State {
	return agentpb.PreheatStatus_State(t.state.Load())
}

// touch 刷新 updatedAt。
func (t *preheatTask) touch() { t.updatedAt.Store(time.Now().UnixNano()) }

// claim 记一个 P0 块的完成(调度协程内串行调用):该 ref 尚有剩余回调数才记账
// (err != nil 计 failed,否则计 completed),全部到齐(total == completed + failed)
// 转终态 DONE(failed == 0)/ERROR。返回是否由本任务认领(get 即扣减)。
func (t *preheatTask) claim(ref prefetch.ChunkRef, err error) bool {
	if n := t.pending[ref]; n > 0 {
		t.pending[ref] = n - 1
	} else {
		return false
	}
	if err != nil {
		t.failed.Add(1)
	} else {
		t.completed.Add(1)
	}
	t.touch()
	if t.completed.Load()+t.failed.Load() >= t.total.Load() {
		t.finish()
	}
	return true
}

// finish 终态迁移:DONE(failed == 0)/ERROR;CAS 防与 markRunning/markError 竞争,
// 已终态则幂等不动。
func (t *preheatTask) finish() {
	target := agentpb.PreheatStatus_DONE
	if t.failed.Load() > 0 {
		target = agentpb.PreheatStatus_ERROR
	}
	for {
		cur := agentpb.PreheatStatus_State(t.state.Load())
		if cur == agentpb.PreheatStatus_DONE || cur == agentpb.PreheatStatus_ERROR {
			return
		}
		if t.state.CompareAndSwap(int32(cur), int32(target)) {
			t.touch()
			slog.Info("preheat: 任务结束",
				"task_id", t.taskID, "dataset", t.req.dataset, "state", target.String(),
				"total_blocks", t.total.Load(), "completed_blocks", t.completed.Load(),
				"failed_blocks", t.failed.Load())
			return
		}
	}
}

// markRunning QUEUED→RUNNING(块可能先于 EnqueueP0 返回完成并已置终态,CAS 不改写终态)。
func (t *preheatTask) markRunning() {
	if t.state.CompareAndSwap(int32(agentpb.PreheatStatus_QUEUED), int32(agentpb.PreheatStatus_RUNNING)) {
		t.touch()
	}
}

// markError 置 ERROR(入队失败等非块级错误;幂等,不改写已到终态的任务)。
func (t *preheatTask) markError(reason string) {
	for {
		cur := t.state.Load()
		if cur == int32(agentpb.PreheatStatus_DONE) || cur == int32(agentpb.PreheatStatus_ERROR) {
			return
		}
		if t.state.CompareAndSwap(cur, int32(agentpb.PreheatStatus_ERROR)) {
			t.touch()
			slog.Warn("preheat: 任务失败", "task_id", t.taskID, "dataset", t.req.dataset, "reason", reason)
			return
		}
	}
}

// NewPreheatTasks 构造任务表并把它注册为 pm 的 P0 完成分发(一个 Agent 一套;测试可用
// nil 管理器——此时 Submit 入队直接报错,Status 仍可查)。
func NewPreheatTasks(cfg Config, pm *PrefetchManager) *PreheatTasks {
	t := &PreheatTasks{cfg: cfg, pm: pm, tasks: make(map[string]*preheatTask)}
	empty := make(map[string][]*preheatTask)
	t.byDataset.Store(&empty)
	if pm != nil {
		pm.SetP0Done(t.onP0Done)
	}
	return t
}

// onP0Done 是预取器 P0 完成回调(调度协程内同步调用,不得阻塞):按 dataset 无锁取任务
// 快照,把本次完成记到第一个有剩余回调数的任务(同 ref 多任务按登记序认领);只做
// atomic 计数与 slog,不取表锁。
func (t *PreheatTasks) onP0Done(dataset string, ref prefetch.ChunkRef, err error) {
	idx := t.byDataset.Load()
	if idx == nil {
		return
	}
	for _, tk := range (*idx)[dataset] {
		if tk.claim(ref, err) {
			return
		}
	}
	// 无匹配任务(任务不在本表或回调重复):静默忽略,不影响调度。
}

// Submit 处理一次 SubmitPreheat(§7.2 流程):校验(node → task_id → dataset → snapshot_id →
// prefixes/chunks,顺序固定)→ task_id 幂等/AlreadyExists → 解析块列表(catalog.List,
// 不切换内存映射)→ 先登记进度表后 EnqueueP0(防块完成回调早于登记)→ RUNNING;
// 空列表立即 DONE(completed = 0);入队失败置 ERROR(任务仍可查)。返回 accepted=true
// 的受理响应(即使任务已立即失败,状态经 GetPreheatStatus 查)。
func (t *PreheatTasks) Submit(ctx context.Context, req *agentpb.SubmitPreheatRequest) (*agentpb.SubmitPreheatResponse, error) {
	// 1) 校验。
	if node := req.GetNode(); node == "" || node != t.cfg.NodeName.String() {
		return nil, status.Errorf(codes.InvalidArgument,
			"node %q 必填且须等于本节点 %q(design §5.1)", node, t.cfg.NodeName.String())
	}
	taskID := req.GetTaskId()
	if taskID == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id 必填(task 幂等键)")
	}
	dataset := req.GetDataset()
	if dataset == "" {
		return nil, status.Error(codes.InvalidArgument, "dataset 必填")
	}
	if req.GetSnapshotId() != "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"snapshot_id %q 不支持(M3 仅支持 CURRENT,须留空)", req.GetSnapshotId())
	}
	prefixes := req.GetPrefixes()
	chunks := req.GetChunks()
	if len(prefixes) == 0 && len(chunks) == 0 {
		return nil, status.Error(codes.InvalidArgument, "prefixes 与 chunks 不能同时为空")
	}

	summary := preheatRequest{
		dataset:    dataset,
		snapshotID: req.GetSnapshotId(), // 校验后必为空;仍入摘要保判据完整
		prefixes:   slices.Clone(prefixes),
		chunks:     chunkRefsFromProto(chunks),
	}

	// 2) task_id 幂等/冲突(建表与查重同锁)。
	t.mu.Lock()
	if ex, ok := t.tasks[taskID]; ok {
		t.mu.Unlock()
		if ex.req.matches(summary) {
			return &agentpb.SubmitPreheatResponse{
				Accepted: true,
				TaskId:   taskID,
				Message:  fmt.Sprintf("任务已存在(幂等),状态 %s", ex.stateNow()),
			}, nil
		}
		return nil, status.Errorf(codes.AlreadyExists,
			"task_id %q 已存在且内容不同(D7 任务引用)", taskID)
	}
	tk := newPreheatTask(taskID, t.cfg.NodeName.String(), summary)
	t.tasks[taskID] = tk
	t.mu.Unlock()

	// 3) 无预取管理器的装配(仅测试):受理后立即 ERROR,状态仍可查。
	if t.pm == nil {
		t.register(tk, nil)
		tk.markError("预取管理器未装配")
		return &agentpb.SubmitPreheatResponse{
			Accepted: true,
			TaskId:   taskID,
			Message:  fmt.Sprintf("任务已受理(状态 %s)", tk.stateNow()),
		}, nil
	}

	// 4) 解析块列表(只读查询,不切换 catalog 内存映射;ErrNoCurrent 视作空列表)。
	refs, err := t.resolveRefs(ctx, dataset, prefixes, chunks)
	if err != nil {
		t.drop(taskID, tk) // 解析失败:撤销登记,客户端可原样重试
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, status.FromContextError(ctxErr).Err()
		}
		return nil, status.Errorf(codes.Internal, "预热块列表解析失败: %v", err)
	}

	// 5) 先登记进度表(含回调认领表)后入队:回调可能先于 EnqueueP0 返回。
	t.register(tk, refs)
	if len(refs) == 0 {
		tk.finish() // 空列表/交集为空:立即 DONE(completed = 0;裁决口径)
		return &agentpb.SubmitPreheatResponse{
			Accepted: true,
			TaskId:   taskID,
			Message:  fmt.Sprintf("任务已受理(状态 %s,无匹配块)", tk.stateNow()),
		}, nil
	}

	// 6) P0 入队;失败置 ERROR(任务不撤销:状态查询可见失败原因)。
	if err := t.pm.ForPreheat(ctx, dataset, refs); err != nil {
		tk.markError(fmt.Sprintf("P0 入队失败: %v", err))
	} else {
		tk.markRunning()
	}
	return &agentpb.SubmitPreheatResponse{
		Accepted: true,
		TaskId:   taskID,
		Message:  fmt.Sprintf("任务已受理(状态 %s)", tk.stateNow()),
	}, nil
}

// Status 返回任务状态(§5.1 PreheatStatus 字段逐一映射);task_id 缺失(空或不存在)
// → NotFound。纯读(计数走 atomic,不取表锁以外路径)。
func (t *PreheatTasks) Status(ctx context.Context, taskID string) (*agentpb.PreheatStatus, error) {
	t.mu.Lock()
	tk, ok := t.tasks[taskID]
	t.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task_id %q 不存在", taskID)
	}
	return &agentpb.PreheatStatus{
		TaskId:          tk.taskID,
		Node:            tk.node,
		Dataset:         tk.req.dataset,
		SnapshotId:      tk.req.snapshotID,
		Paths:           slices.Clone(tk.req.prefixes),
		State:           tk.stateNow(),
		TotalBlocks:     tk.total.Load(),
		CompletedBlocks: tk.completed.Load(),
		FailedBlocks:    tk.failed.Load(),
		StartedAt:       timestamppb.New(tk.startedAt),
		UpdatedAt:       timestamppb.New(time.Unix(0, tk.updatedAt.Load())),
	}, nil
}

// RunningCount 返回运行中任务数(QUEUED/RUNNING;GetNodeStatus.running_tasks)。
func (t *PreheatTasks) RunningCount() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var n int64
	for _, tk := range t.tasks {
		switch tk.stateNow() {
		case agentpb.PreheatStatus_QUEUED, agentpb.PreheatStatus_RUNNING:
			n++
		}
	}
	return n
}

// register 填写块列表并登记进 dataset 索引(先登记后入队):total 与回调认领表由本次
// 入队定盘;byDataset 以 COW 快照整体替换,回调路径无锁读。
func (t *PreheatTasks) register(tk *preheatTask, refs []prefetch.ChunkRef) {
	pending := make(map[prefetch.ChunkRef]int, len(refs))
	for _, r := range refs {
		pending[r]++
	}
	tk.pending = pending
	tk.total.Store(int64(len(refs)))
	tk.touch()

	t.mu.Lock()
	defer t.mu.Unlock()
	old := t.byDataset.Load()
	nw := make(map[string][]*preheatTask, len(*old)+1)
	for k, v := range *old {
		nw[k] = v
	}
	nw[tk.req.dataset] = append(slices.Clone(nw[tk.req.dataset]), tk)
	t.byDataset.Store(&nw)
}

// drop 撤销登记(块列表解析失败:不留半成品任务,客户端可原样重试同一 task_id)。
func (t *PreheatTasks) drop(taskID string, tk *preheatTask) {
	t.mu.Lock()
	if cur, ok := t.tasks[taskID]; ok && cur == tk {
		delete(t.tasks, taskID)
	}
	t.mu.Unlock()
}

// resolveRefs 解析预热块列表(只经 Catalog.List 只读查询,不切换内存映射):
//   - prefixes:含空串 → 只调一次 List("")(全量,覆盖其余前缀);否则逐个 List(prefix);
//     结果按 (path, chunk) 去重合并;
//   - chunks 非空:prefixes 为空 → 以 chunks 为准(去重保序);否则与 prefix 结果求交
//     (交集为空 → 空列表,调用方立即 DONE);
//   - ErrNoCurrent(该 dataset 尚无 CURRENT 快照)→ 视作空列表(与 ListSnapshot 同口径)。
func (t *PreheatTasks) resolveRefs(ctx context.Context, dataset string, prefixes []string, chunks []*agentpb.ChunkRef) ([]prefetch.ChunkRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.pm == nil {
		return nil, errors.New("预取管理器未装配") // 防御:Submit 已在解析前拦截
	}
	cat := t.pm.bundleFor(dataset).catalog

	var (
		listed    []prefetch.ChunkRef
		listedSet map[prefetch.ChunkRef]struct{}
	)
	if len(prefixes) > 0 {
		listedSet = make(map[prefetch.ChunkRef]struct{})
		ps := prefixes
		if slices.Contains(prefixes, "") {
			ps = []string{""} // 空串 = 全量(字面语义同 Catalog.LoadPrefixes)
		}
		for _, p := range ps {
			entries, err := cat.List(ctx, p)
			if err != nil {
				if errors.Is(err, index.ErrNoCurrent) {
					return nil, nil
				}
				return nil, err
			}
			for _, e := range entries {
				r := prefetch.ChunkRef{Path: e.Path, Chunk: e.Chunk}
				if _, dup := listedSet[r]; dup {
					continue
				}
				listedSet[r] = struct{}{}
				listed = append(listed, r)
			}
		}
	}
	if len(chunks) == 0 {
		return listed, nil
	}

	out := make([]prefetch.ChunkRef, 0, len(chunks))
	seen := make(map[prefetch.ChunkRef]struct{}, len(chunks))
	for _, c := range chunks {
		r := prefetch.ChunkRef{Path: c.GetPath(), Chunk: c.GetChunk()}
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		if listedSet != nil {
			if _, ok := listedSet[r]; !ok {
				continue // 与 prefixes 求交:未列出即剔除
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// chunkRefsFromProto 把 proto chunks 转成保序 ChunkRef 列表(幂等摘要用;索引结果映射
// 另走 resolveRefs)。
func chunkRefsFromProto(chunks []*agentpb.ChunkRef) []prefetch.ChunkRef {
	out := make([]prefetch.ChunkRef, 0, len(chunks))
	for _, c := range chunks {
		out = append(out, prefetch.ChunkRef{Path: c.GetPath(), Chunk: c.GetChunk()})
	}
	return out
}
