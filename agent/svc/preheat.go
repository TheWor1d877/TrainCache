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
// 任务记录 TTL = taskTTL(Q13 结案):Submit/Status/RunningCount 任一入口先惰性清理过期
// 任务,不另起清理协程(见 taskTTL 与 expireTasks)。
type PreheatTasks struct {
	cfg Config
	pm  *PrefetchManager

	mu    sync.Mutex
	tasks map[string]*preheatTask

	// byDataset 是 dataset → 任务追加表(回调按 dataset 定位任务的只读快照;登记任务时
	// 以 COW 整体替换,回调路径无锁读)。
	byDataset atomic.Pointer[map[string][]*preheatTask]
}

// taskTTL 是任务记录存活时长(Q13 结案:24h;入口惰性清理,不另起协程)。判定口径:
// now − updatedAt > taskTTL——终态(DONE/ERROR)任务的 updatedAt 即完成时刻(即"完成
// 超 24h 清理");非终态(QUEUED/RUNNING)任务的 updatedAt 即最近一次进度/状态刷新时刻,
// 停滞超 24h 视作死任务一并清理(两种状态同口径,TLL 自最近进度起算而非创建时刻;
// DECISIONS 回传)。
const taskTTL = 24 * time.Hour

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
// 8.x 增量:generation/seq/ready_bitmap 及取消标记;位序 = chunks 切片顺序。
// 并发约定:chunks/bitmap/pending/priority 由 mu 保护(Update 会全量替换,须与调度
// 协程的 claim 互斥);generation/canceled/seq 用 atomic,供 Submit 在表锁内只读。
type preheatTask struct {
	taskID string
	req    preheatRequest
	node   string

	// pending 是期望的 P0 完成回调数(按 ref;重复 ref 计多次)。register 后由调度
	// 协程的 claim 与本表 Update 路径在 mu 下读写。
	pending map[prefetch.ChunkRef]int

	mu       sync.Mutex
	chunks   []prefetch.ChunkRef // 当前窗口有序块(位序 = 本切片顺序)
	bitmap   []byte              // 就绪位图,长度 ceil(len(chunks)/8)
	priority agentpb.PreheatPriority

	generation atomic.Uint64 // 已接受的任务代际(单调不减)
	seq        atomic.Uint64 // 位图变更序号(置位/重置时递增)
	canceled   atomic.Bool   // Cancel 标记(不取消在途 P0)

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
	t.seq.Store(1) // 新任务 = 一次位图重置(空 → 请求 chunks)
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
// 8.x:成功完成时把该 ref 在 chunks 中的全部位置置位(重复 ref 一次回调覆盖多位),
// 有新位置置位则 seq 递增;失败位保持 0。
func (t *preheatTask) claim(ref prefetch.ChunkRef, err error) bool {
	t.mu.Lock()
	n := t.pending[ref]
	if n <= 0 {
		t.mu.Unlock()
		return false
	}
	t.pending[ref] = n - 1
	if err != nil {
		t.failed.Add(1)
	} else {
		t.completed.Add(1)
		var changed uint64
		for i, c := range t.chunks {
			if c == ref && setBitmapBit(t.bitmap, i) {
				changed++
			}
		}
		if changed > 0 {
			t.seq.Add(changed) // 任一位置位都使 seq 递增(A.2)
		}
	}
	t.mu.Unlock()
	t.touch()
	if t.completed.Load()+t.failed.Load() >= t.total.Load() {
		t.finish()
	}
	return true
}

// bitmapLen 返回 chunks 数 n 对应的位图字节数(ceil(n/8);D56)。
func bitmapLen(n int) int { return (n + 7) / 8 }

// setBitmapBit 把位图第 i 位置 1(位 i = 字节 i/8 的第 i%8 位,LittleEndian 位序);
// 越界或已置位返回 false。
func setBitmapBit(bitmap []byte, i int) bool {
	if i < 0 || i/8 >= len(bitmap) {
		return false
	}
	mask := byte(1 << uint(i%8))
	if bitmap[i/8]&mask != 0 {
		return false
	}
	bitmap[i/8] |= mask
	return true
}

// advanceGeneration 单调推进已接受代际(低于当前值不动作;CAS 防并发回退)。
func (t *preheatTask) advanceGeneration(gen uint64) {
	for {
		cur := t.generation.Load()
		if gen <= cur {
			return
		}
		if t.generation.CompareAndSwap(cur, gen) {
			t.touch()
			return
		}
	}
}

// finish 终态迁移:DONE(failed == 0)/ERROR;CAS 防与 markRunning/markError 竞争,
// 已终态则幂等不动。持任务锁复核计数:Update 重置窗口后旧回调不得把新窗口误置终态。
func (t *preheatTask) finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.completed.Load()+t.failed.Load() < t.total.Load() {
		return
	}
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
	t.expireTasks() // 入口惰性清理过期任务(Q13;先清后判,过期 task_id 可复用)

	// 1) 校验。
	if err := t.checkNode(req.GetNode()); err != nil {
		return nil, err
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
	gen := req.GetGeneration()

	summary := preheatRequest{
		dataset:    dataset,
		snapshotID: req.GetSnapshotId(), // 校验后必为空;仍入摘要保判据完整
		prefixes:   slices.Clone(prefixes),
		chunks:     chunkRefsFromProto(chunks),
	}

	// 2) task_id 幂等/冲突/代际(建表与查重同锁;generation/canceled 走 atomic,
	//    表锁内不取任务锁,避免与 register 反向加锁)。
	t.mu.Lock()
	if ex, ok := t.tasks[taskID]; ok {
		curGen := ex.generation.Load()
		if gen < curGen {
			t.mu.Unlock()
			slog.Warn("preheat: 拒绝陈旧 generation",
				"rpc", "SubmitPreheat", "task_id", taskID, "generation", gen, "current_generation", curGen)
			return nil, status.Errorf(codes.FailedPrecondition,
				"generation %d 低于 Agent 当前代际 %d", gen, curGen)
		}
		if !ex.canceled.Load() {
			t.mu.Unlock()
			if ex.req.matches(summary) {
				ex.advanceGeneration(gen) // 同内容幂等重放:仅推进代际,不重置进度
				return &agentpb.SubmitPreheatResponse{
					Accepted: true,
					TaskId:   taskID,
					Message:  fmt.Sprintf("任务已存在(幂等),状态 %s", ex.stateNow()),
				}, nil
			}
			return nil, status.Errorf(codes.AlreadyExists,
				"task_id %q 已存在且内容不同(D7 任务引用)", taskID)
		}
		// 已取消任务:同名同代际(或更高)再 Submit 视为新任务(A.2)。
		delete(t.tasks, taskID)
		t.removeDatasetLocked(ex)
		slog.Info("preheat: 已取消任务重新提交视为新任务",
			"task_id", taskID, "generation", gen)
	}
	tk := newPreheatTask(taskID, t.cfg.NodeName.String(), summary)
	tk.generation.Store(gen)
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
	//    ordered 保留请求 chunks 顺序(含重复项,定义位序);enq 去重保序(下载侧)。
	ordered, enq, err := t.resolveRefs(ctx, dataset, prefixes, chunks)
	if err != nil {
		t.drop(taskID, tk) // 解析失败:撤销登记,客户端可原样重试
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, status.FromContextError(ctxErr).Err()
		}
		return nil, status.Errorf(codes.Internal, "预热块列表解析失败: %v", err)
	}

	// 5) 先登记进度表(含回调认领表)后入队:回调可能先于 EnqueueP0 返回。
	t.registerSubmit(tk, ordered, enq)
	if len(enq) == 0 {
		tk.finish() // 空列表/交集为空:立即 DONE(completed = 0;裁决口径)
		return &agentpb.SubmitPreheatResponse{
			Accepted: true,
			TaskId:   taskID,
			Message:  fmt.Sprintf("任务已受理(状态 %s,无匹配块)", tk.stateNow()),
		}, nil
	}

	// 6) P0 入队;失败置 ERROR(任务不撤销:状态查询可见失败原因)。
	if err := t.pm.ForPreheat(ctx, dataset, enq); err != nil {
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

// Update 处理 UpdatePreheat(8.x D55/D56):校验 → 代际 → 全量替换 chunks(位序=本次
// 请求顺序,旧位图重置、seq++)→ 唯一 ref 入 P0;已落盘块不清理,在途请求不取消。
// 空 chunks 视为清空窗口(总数为 0,立即 DONE)。返回 accepted 表示窗口已替换;
// 入队失败置 ERROR,状态经 GetPreheatStatus 可见。
func (t *PreheatTasks) Update(ctx context.Context, req *agentpb.UpdatePreheatRequest) (*agentpb.UpdatePreheatResponse, error) {
	t.expireTasks()
	if err := t.checkNode(req.GetNode()); err != nil {
		return nil, err
	}
	taskID := req.GetTaskId()
	if taskID == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id 必填")
	}
	ordered, enq, err := validateWindowChunks(req.GetChunks())
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	tk, ok := t.tasks[taskID]
	t.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task_id %q 不存在", taskID)
	}

	curGen := tk.generation.Load()
	if req.GetGeneration() < curGen {
		slog.Warn("preheat: 拒绝陈旧 generation",
			"rpc", "UpdatePreheat", "task_id", taskID,
			"generation", req.GetGeneration(), "current_generation", curGen)
		return nil, status.Errorf(codes.FailedPrecondition,
			"generation %d 低于 Agent 当前代际 %d", req.GetGeneration(), curGen)
	}
	if tk.canceled.Load() {
		return nil, status.Errorf(codes.FailedPrecondition, "task_id %q 已取消,不能更新", taskID)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, status.FromContextError(ctxErr).Err()
	}
	tk.advanceGeneration(req.GetGeneration())
	tk.resetWindow(ordered, enq, req.GetPriority())
	slog.Info("preheat: 任务窗口全量更新",
		"task_id", taskID, "generation", tk.generation.Load(), "chunks", len(ordered),
		"seq", tk.seq.Load())

	if len(enq) == 0 {
		tk.finish() // 空列表:清空窗口后立即 DONE
		return &agentpb.UpdatePreheatResponse{
			Accepted: true,
			Message:  fmt.Sprintf("任务已更新(状态 %s,窗口已清空)", tk.stateNow()),
		}, nil
	}
	if t.pm == nil {
		tk.markError("预取管理器未装配")
	} else if err := t.pm.ForPreheat(ctx, tk.req.dataset, enq); err != nil {
		tk.markError(fmt.Sprintf("P0 入队失败: %v", err))
	} else {
		tk.markRunning()
	}
	return &agentpb.UpdatePreheatResponse{
		Accepted: true,
		Message:  fmt.Sprintf("任务已更新(状态 %s)", tk.stateNow()),
	}, nil
}

// Cancel 处理 CancelPreheat(8.x D55):仅标记取消并停止调度新块,不取消在途 P0;
// 幂等(重复 Cancel/任务不存在均 accepted=true)。低 generation → FailedPrecondition。
func (t *PreheatTasks) Cancel(ctx context.Context, req *agentpb.CancelPreheatRequest) (*agentpb.CancelPreheatResponse, error) {
	t.expireTasks()
	if err := t.checkNode(req.GetNode()); err != nil {
		return nil, err
	}
	taskID := req.GetTaskId()
	if taskID == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id 必填")
	}

	t.mu.Lock()
	tk, ok := t.tasks[taskID]
	t.mu.Unlock()
	if !ok {
		slog.Info("preheat: 取消不存在的任务(幂等空操作)", "task_id", taskID,
			"generation", req.GetGeneration())
		return &agentpb.CancelPreheatResponse{Accepted: true}, nil
	}
	curGen := tk.generation.Load()
	if req.GetGeneration() < curGen {
		slog.Warn("preheat: 拒绝陈旧 generation",
			"rpc", "CancelPreheat", "task_id", taskID,
			"generation", req.GetGeneration(), "current_generation", curGen)
		return nil, status.Errorf(codes.FailedPrecondition,
			"generation %d 低于 Agent 当前代际 %d", req.GetGeneration(), curGen)
	}
	tk.advanceGeneration(req.GetGeneration())
	tk.canceled.Store(true)
	slog.Info("preheat: 任务取消(在途 P0 不取消)",
		"task_id", taskID, "generation", tk.generation.Load(), "state", tk.stateNow().String())
	return &agentpb.CancelPreheatResponse{Accepted: true}, nil
}

// checkNode 校验请求 node 必填且等于本节点(design §5.1;InvalidArgument)。
func (t *PreheatTasks) checkNode(node string) error {
	if node == "" || node != t.cfg.NodeName.String() {
		return status.Errorf(codes.InvalidArgument,
			"node %q 必填且须等于本节点 %q(design §5.1)", node, t.cfg.NodeName.String())
	}
	return nil
}

// validateWindowChunks 校验 Update 的 chunks 形状并返回:
//   - ordered:原样保序(含重复)的位序列表,位图长度 = ceil(len(ordered)/8);
//   - enq:去重保序的入队列表(同一 ref 只下载一次)。
//
// path 为空或 chunk 为负视为请求形状非法 → InvalidArgument(Q15)。
func validateWindowChunks(chunks []*agentpb.ChunkRef) (ordered, enq []prefetch.ChunkRef, err error) {
	ordered = make([]prefetch.ChunkRef, 0, len(chunks))
	enq = make([]prefetch.ChunkRef, 0, len(chunks))
	seen := make(map[prefetch.ChunkRef]struct{}, len(chunks))
	for i, c := range chunks {
		if c == nil || c.GetPath() == "" {
			return nil, nil, status.Errorf(codes.InvalidArgument, "chunks[%d].path 必填", i)
		}
		if c.GetChunk() < 0 {
			return nil, nil, status.Errorf(codes.InvalidArgument,
				"chunks[%d].chunk = %d 非法(须 >= 0)", i, c.GetChunk())
		}
		r := prefetch.ChunkRef{Path: c.GetPath(), Chunk: c.GetChunk()}
		ordered = append(ordered, r)
		if _, dup := seen[r]; !dup {
			seen[r] = struct{}{}
			enq = append(enq, r)
		}
	}
	return ordered, enq, nil
}

// Status 返回任务状态(§5.1 PreheatStatus 字段逐一映射);task_id 缺失(空、不存在或
// 已过 taskTTL 被入口清理)→ NotFound。计数走 atomic;入口在表锁内顺带惰性清理过期任务。
func (t *PreheatTasks) Status(ctx context.Context, taskID string) (*agentpb.PreheatStatus, error) {
	t.mu.Lock()
	t.expireLocked(time.Now()) // 入口惰性清理过期任务(Q13)
	tk, ok := t.tasks[taskID]
	t.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "task_id %q 不存在", taskID)
	}
	tk.mu.Lock()
	chunkCount := len(tk.chunks)
	readyBitmap := slices.Clone(tk.bitmap)
	tk.mu.Unlock()
	// Q15:位图长度/位序形状非法按请求形状错误返回 InvalidArgument(正常路径不会触发;
	// 防御内部状态损坏,避免把不可解码的位图交给 Controller)。
	if len(readyBitmap) != bitmapLen(chunkCount) {
		slog.Error("preheat: 位图长度与 chunks 数不匹配",
			"task_id", taskID, "bitmap_bytes", len(readyBitmap), "chunks", chunkCount)
		return nil, status.Errorf(codes.InvalidArgument,
			"ready_bitmap 长度 %d 与 chunks 数 %d 不匹配(应为 %d)",
			len(readyBitmap), chunkCount, bitmapLen(chunkCount))
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
		Canceled:        tk.canceled.Load(),
		StartedAt:       timestamppb.New(tk.startedAt),
		UpdatedAt:       timestamppb.New(time.Unix(0, tk.updatedAt.Load())),
		Generation:      tk.generation.Load(),
		Seq:             tk.seq.Load(),
		ReadyBitmap:     readyBitmap,
	}, nil
}

// RunningCount 返回运行中任务数(QUEUED/RUNNING;GetNodeStatus.running_tasks);入口
// 顺带惰性清理过期任务(Q13)。
func (t *PreheatTasks) RunningCount() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(time.Now())
	var n int64
	for _, tk := range t.tasks {
		switch tk.stateNow() {
		case agentpb.PreheatStatus_QUEUED, agentpb.PreheatStatus_RUNNING:
			n++
		}
	}
	return n
}

// expireTasks 惰性清理过期任务(Submit 入口调用;取表锁一次)。
func (t *PreheatTasks) expireTasks() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireLocked(time.Now())
}

// expireLocked 删除过期任务并同步剔除 byDataset 快照(t.mu 已持)。byDataset 剔除防
// 回调路径认领已出表任务(死任务残留 pending 会吞掉后续任务的同 ref 回调)与孤儿指针
// 无界累积;重建新切片(COW),不原地改写旧快照(调度协程可能正在读)。
func (t *PreheatTasks) expireLocked(now time.Time) {
	if len(t.tasks) == 0 {
		return
	}
	var expired map[*preheatTask]struct{}
	for id, tk := range t.tasks {
		if now.Sub(time.Unix(0, tk.updatedAt.Load())) > taskTTL {
			if expired == nil {
				expired = make(map[*preheatTask]struct{})
			}
			expired[tk] = struct{}{}
			delete(t.tasks, id)
			slog.Info("preheat: 任务记录过 TTL 清理", "task_id", id, "dataset", tk.req.dataset,
				"state", tk.stateNow().String(), "ttl", taskTTL)
		}
	}
	if expired == nil {
		return
	}
	old := t.byDataset.Load()
	if old == nil {
		return
	}
	nw := make(map[string][]*preheatTask, len(*old))
	for ds, list := range *old {
		kept := make([]*preheatTask, 0, len(list))
		for _, tk := range list {
			if _, dead := expired[tk]; !dead {
				kept = append(kept, tk)
			}
		}
		nw[ds] = kept
	}
	t.byDataset.Store(&nw)
}

// register 填写块列表并登记进 dataset 索引(先登记后入队):total 与回调认领表由本次
// 入队定盘;byDataset 以 COW 快照整体替换,回调路径无锁读。按 refs 原样计数(重复
// ref 计多次),供白盒用例直接构造认领表;生产 Submit 走 registerSubmit。
func (t *PreheatTasks) register(tk *preheatTask, refs []prefetch.ChunkRef) {
	pending := make(map[prefetch.ChunkRef]int, len(refs))
	for _, r := range refs {
		pending[r]++
	}
	t.installWindow(tk, refs, pending, int64(len(refs)))
}

// registerSubmit 是 Submit 的生产登记:ordered 保留请求 chunks 顺序(含重复项,定义
// 位序),enq 为去重后的下载/回调认领列表;total 按唯一块计,重复位置由同一次成功
// 回调一并置位(与 Update 一致)。
func (t *PreheatTasks) registerSubmit(tk *preheatTask, ordered, enq []prefetch.ChunkRef) {
	pending := make(map[prefetch.ChunkRef]int, len(enq))
	for _, r := range enq {
		pending[r]++
	}
	t.installWindow(tk, ordered, pending, int64(len(enq)))
}

// installWindow 写入 chunks/位图/回调认领表并把任务追加进 dataset 索引。
func (t *PreheatTasks) installWindow(tk *preheatTask, ordered []prefetch.ChunkRef, pending map[prefetch.ChunkRef]int, total int64) {
	tk.mu.Lock()
	tk.pending = pending
	tk.chunks = slices.Clone(ordered) // 位序 = 请求块顺序(含重复项)
	tk.bitmap = make([]byte, bitmapLen(len(ordered)))
	tk.total.Store(total)
	tk.mu.Unlock()
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

// resetWindow 全量替换窗口(Update;调用方已校验代际/取消状态):
// chunks 原样保序定义位序,enq 为去重后的入队列表;旧位图失效并重置,seq 递增,
// 进度计数归零、状态回到 QUEUED。已落盘块不清理(由预取器本地命中直接回调置位)。
func (t *preheatTask) resetWindow(chunks, enq []prefetch.ChunkRef, priority agentpb.PreheatPriority) {
	t.mu.Lock()
	t.chunks = slices.Clone(chunks)
	t.bitmap = make([]byte, bitmapLen(len(chunks)))
	pending := make(map[prefetch.ChunkRef]int, len(enq))
	for _, r := range enq {
		pending[r]++
	}
	t.pending = pending
	t.priority = priority
	t.total.Store(int64(len(enq)))
	t.completed.Store(0)
	t.failed.Store(0)
	t.seq.Add(1) // 位图重置 → seq 递增(D56)
	t.state.Store(int32(agentpb.PreheatStatus_QUEUED))
	t.mu.Unlock()
	t.touch()
}

// removeDatasetLocked 从 byDataset 快照剔除任务(调用方持表锁;COW 整体替换,
// 回调路径无锁读)。用于 Cancel 后同名同代际重建新任务。
func (t *PreheatTasks) removeDatasetLocked(tk *preheatTask) {
	old := t.byDataset.Load()
	if old == nil {
		return
	}
	nw := make(map[string][]*preheatTask, len(*old))
	for ds, list := range *old {
		kept := make([]*preheatTask, 0, len(list))
		for _, x := range list {
			if x != tk {
				kept = append(kept, x)
			}
		}
		nw[ds] = kept
	}
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
//   - chunks 非空:prefixes 为空 → 以 chunks 为准;否则与 prefix 结果求交
//     (交集为空 → 空列表,调用方立即 DONE);
//   - ordered 保留请求 chunks 顺序(含重复项,定义位图位序);enq 去重保序,供下载/回调
//     认领(与 Update 同口径,D56);
//   - ErrNoCurrent(该 dataset 尚无 CURRENT 快照)→ 视作空列表(与 ListSnapshot 同口径)。
func (t *PreheatTasks) resolveRefs(ctx context.Context, dataset string, prefixes []string, chunks []*agentpb.ChunkRef) (ordered, enq []prefetch.ChunkRef, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if t.pm == nil {
		return nil, nil, errors.New("预取管理器未装配") // 防御:Submit 已在解析前拦截
	}
	cat := t.pm.bundleFor(dataset, nil).catalog

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
					return nil, nil, nil
				}
				return nil, nil, err
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
		return listed, listed, nil // prefixes 路径已去重,位序=目录顺序
	}

	ordered = make([]prefetch.ChunkRef, 0, len(chunks))
	enq = make([]prefetch.ChunkRef, 0, len(chunks))
	seen := make(map[prefetch.ChunkRef]struct{}, len(chunks))
	for _, c := range chunks {
		r := prefetch.ChunkRef{Path: c.GetPath(), Chunk: c.GetChunk()}
		if listedSet != nil {
			if _, ok := listedSet[r]; !ok {
				continue // 与 prefixes 求交:未列出即剔除
			}
		}
		ordered = append(ordered, r) // 保留重复项与请求顺序(位序定义)
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		enq = append(enq, r)
	}
	return ordered, enq, nil
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
