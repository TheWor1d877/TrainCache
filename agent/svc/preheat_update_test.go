package svc

// 本文件覆盖 8.x M3 Agent 增量(7.1 附录 A / 7.1 §12.1):
// generation 单调与陈旧拒绝、Update 全量替换与位序、Cancel 幂等与重建、
// GetPreheatStatus 新字段、seq 单调、位图形状校验、ReportBlocks 仍 Unimplemented。
// 等待一律有界轮询(waitTaskState),不依赖裸 sleep 做时序断言。

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/prefetch"
)

// m3Chunk 构造 proto ChunkRef。
func m3Chunk(path string, chunk int64) *agentpb.ChunkRef {
	return &agentpb.ChunkRef{Path: path, Chunk: chunk}
}

// m3Ref 构造内部 ChunkRef。
func m3Ref(path string, chunk int64) prefetch.ChunkRef {
	return prefetch.ChunkRef{Path: path, Chunk: chunk}
}

// m3Submit 构造显式 chunks 的 Submit 请求(8.x:prefixes 为空,位序即 chunks 顺序)。
func m3Submit(taskID string, gen uint64, chunks ...*agentpb.ChunkRef) *agentpb.SubmitPreheatRequest {
	return &agentpb.SubmitPreheatRequest{
		TaskId:     taskID,
		Node:       "worker-01",
		Dataset:    testDataset,
		Chunks:     chunks,
		Generation: gen,
	}
}

// m3Update 构造 Update 请求。
func m3Update(taskID string, gen uint64, chunks ...*agentpb.ChunkRef) *agentpb.UpdatePreheatRequest {
	return &agentpb.UpdatePreheatRequest{
		TaskId:     taskID,
		Node:       "worker-01",
		Generation: gen,
		Chunks:     chunks,
	}
}

// m3Cancel 构造 Cancel 请求。
func m3Cancel(taskID string, gen uint64) *agentpb.CancelPreheatRequest {
	return &agentpb.CancelPreheatRequest{TaskId: taskID, Node: "worker-01", Generation: gen}
}

// m3Bit 返回位图第 i 位。
func m3Bit(bitmap []byte, i int) bool {
	if i < 0 || i/8 >= len(bitmap) {
		return false
	}
	return bitmap[i/8]&(1<<uint(i%8)) != 0
}

// TestPreheatStaleGenerationRejected:低于已接受代际的 Submit/Update/Cancel 均返回
// FailedPrecondition;各字段号与位序按冻结契约。
func TestPreheatStaleGenerationRejected(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "a-000.tar", chunk: 0, content: "gen-a0", local: true},
	})
	fx.preloadMapping(t, testDataset) // 本地命中判定需要已加载索引映射
	ctx := context.Background()

	req := m3Submit("t-gen", 5, m3Chunk("a-000.tar", 0))
	if _, err := fx.tasks.Submit(ctx, req); err != nil {
		t.Fatalf("Submit(gen=5): %v", err)
	}
	waitTaskState(t, fx.tasks, "t-gen", agentpb.PreheatStatus_DONE)

	// 同内容、较低代际的 Submit 必须拒绝(而非幂等)。
	if _, err := fx.tasks.Submit(ctx, m3Submit("t-gen", 4, m3Chunk("a-000.tar", 0))); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("陈旧 Submit code = %v, want FailedPrecondition(err=%v)", status.Code(err), err)
	}
	// generation=0(未提供)在已接受代际 > 0 时同样视为陈旧。
	if _, err := fx.tasks.Submit(ctx, m3Submit("t-gen", 0, m3Chunk("a-000.tar", 0))); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("generation=0 Submit code = %v, want FailedPrecondition", status.Code(err))
	}
	if _, err := fx.tasks.Update(ctx, m3Update("t-gen", 4, m3Chunk("a-000.tar", 0))); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("陈旧 Update code = %v, want FailedPrecondition(err=%v)", status.Code(err), err)
	}
	if _, err := fx.tasks.Cancel(ctx, m3Cancel("t-gen", 4)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("陈旧 Cancel code = %v, want FailedPrecondition(err=%v)", status.Code(err), err)
	}

	// 同代际 Update 接受,并推进到更高代际。
	resp, err := fx.tasks.Update(ctx, m3Update("t-gen", 6, m3Chunk("a-000.tar", 0)))
	if err != nil || !resp.GetAccepted() {
		t.Fatalf("Update(gen=6) = %+v, err=%v, want accepted", resp, err)
	}
	st := waitTaskState(t, fx.tasks, "t-gen", agentpb.PreheatStatus_DONE)
	if st.GetGeneration() != 6 {
		t.Errorf("generation = %d, want 6(回显已接受值)", st.GetGeneration())
	}
	if _, err := fx.tasks.Cancel(ctx, m3Cancel("t-gen", 5)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Update 后陈旧 Cancel code = %v, want FailedPrecondition", status.Code(err))
	}
}

// TestPreheatUpdateReplacesWindowAndBitOrder:Update 全量替换 chunks、位序按本次请求
// 顺序重定义、旧位图重置;失败位保持 0;已落盘块不清理。
func TestPreheatUpdateReplacesWindowAndBitOrder(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "a-000.tar", chunk: 0, content: "upd-a", local: true},
		{path: "b-000.tar", chunk: 0, content: "upd-b", local: true},
	})
	fx.preloadMapping(t, testDataset)
	ctx := context.Background()

	// 初始窗口 [a,b]:两块本地命中 → 位图 bit0/bit1 置位。
	if _, err := fx.tasks.Submit(ctx, m3Submit("t-upd", 1,
		m3Chunk("a-000.tar", 0), m3Chunk("b-000.tar", 0))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st := waitTaskState(t, fx.tasks, "t-upd", agentpb.PreheatStatus_DONE)
	if st.GetSeq() == 0 {
		t.Fatal("seq 应从 1 起单调递增")
	}
	if len(st.GetReadyBitmap()) != 1 || !m3Bit(st.GetReadyBitmap(), 0) || !m3Bit(st.GetReadyBitmap(), 1) {
		t.Fatalf("初始位图 = %v, want bit0/bit1 置位", st.GetReadyBitmap())
	}

	// 全量替换为 [missing, a]:旧的 a@0 位失效,a 的新位序为 1;missing 失败位保持 0。
	upd, err := fx.tasks.Update(ctx, m3Update("t-upd", 2,
		m3Chunk("missing-000.tar", 0), m3Chunk("a-000.tar", 0)))
	if err != nil || !upd.GetAccepted() {
		t.Fatalf("Update = %+v, err=%v, want accepted", upd, err)
	}
	st = waitTaskState(t, fx.tasks, "t-upd", agentpb.PreheatStatus_ERROR)
	if len(st.GetReadyBitmap()) != 1 {
		t.Fatalf("位图长度 = %d, want ceil(2/8)=1", len(st.GetReadyBitmap()))
	}
	if m3Bit(st.GetReadyBitmap(), 0) {
		t.Error("bit0 对应 missing,失败位必须保持 0")
	}
	if !m3Bit(st.GetReadyBitmap(), 1) {
		t.Error("bit1 对应已落盘的 a-000.tar,应为 1(位序=本次 chunks 顺序)")
	}
	if st.GetTotalBlocks() != 2 || st.GetCompletedBlocks() != 1 || st.GetFailedBlocks() != 1 {
		t.Errorf("进度 = total %d/completed %d/failed %d, want 2/1/1",
			st.GetTotalBlocks(), st.GetCompletedBlocks(), st.GetFailedBlocks())
	}

	// 空 chunks = 清空窗口:立即 DONE,位图长度 0。
	empty, err := fx.tasks.Update(ctx, m3Update("t-upd", 3))
	if err != nil || !empty.GetAccepted() {
		t.Fatalf("空窗口 Update = %+v, err=%v", empty, err)
	}
	st = waitTaskState(t, fx.tasks, "t-upd", agentpb.PreheatStatus_DONE)
	if len(st.GetReadyBitmap()) != 0 || st.GetTotalBlocks() != 0 {
		t.Errorf("清空后 = bitmap %v total %d, want 空位图/0", st.GetReadyBitmap(), st.GetTotalBlocks())
	}
}

// TestSubmitPreservesDuplicateBitOrder:Submit 与 Update 同口径——位序=请求 chunks
// 顺序(含重复项),位图长度 ceil(len(chunks)/8);下载侧去重,一次成功回调置位该 ref
// 的全部位置。
func TestSubmitPreservesDuplicateBitOrder(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "dup-000.tar", chunk: 0, content: "dup-0", local: true},
	})
	fx.preloadMapping(t, testDataset)

	if _, err := fx.tasks.Submit(context.Background(),
		m3Submit("t-dup", 1, m3Chunk("dup-000.tar", 0), m3Chunk("dup-000.tar", 0))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st := waitTaskState(t, fx.tasks, "t-dup", agentpb.PreheatStatus_DONE)
	if len(st.GetReadyBitmap()) != 1 { // ceil(2/8)=1
		t.Fatalf("位图长度 = %d, want ceil(2/8)=1", len(st.GetReadyBitmap()))
	}
	if !m3Bit(st.GetReadyBitmap(), 0) || !m3Bit(st.GetReadyBitmap(), 1) {
		t.Fatalf("位图 = %v, want bit0/bit1 均置位(重复 ref 一次回调覆盖两位)", st.GetReadyBitmap())
	}
	// 下载侧仍去重:total/completed 按唯一块计。
	if st.GetTotalBlocks() != 1 || st.GetCompletedBlocks() != 1 {
		t.Errorf("total/completed = %d/%d, want 1/1(下载去重)",
			st.GetTotalBlocks(), st.GetCompletedBlocks())
	}
}

// TestPreheatBitmapSetAfterDownload:非本地块下载成功(落盘且校验通过)后置位;
// 位图长度按请求 chunks 数推导。
func TestPreheatBitmapSetAfterDownload(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "d-000.tar", chunk: 0, content: "dl-d0", source: true}, // 本地无、源站有
	})
	fx.preloadMapping(t, testDataset)

	if _, err := fx.tasks.Submit(context.Background(),
		m3Submit("t-download", 1, m3Chunk("d-000.tar", 0))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st := waitTaskState(t, fx.tasks, "t-download", agentpb.PreheatStatus_DONE)
	if len(st.GetReadyBitmap()) != 1 || !m3Bit(st.GetReadyBitmap(), 0) {
		t.Fatalf("下载成功后位图 = %v, want bit0 置位", st.GetReadyBitmap())
	}
	if st.GetSeq() != 2 {
		t.Errorf("seq = %d, want 2(初始 1 + 置位一次)", st.GetSeq())
	}
}

// TestPreheatSeqMonotonic:每次置位/位图重置 seq 严格递增;无变化轮询 seq 不变。
func TestPreheatSeqMonotonic(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "s-000.tar", chunk: 0, content: "seq-s0", local: true},
		{path: "s-001.tar", chunk: 0, content: "seq-s1", local: true},
	})
	fx.preloadMapping(t, testDataset)
	ctx := context.Background()

	if _, err := fx.tasks.Submit(ctx, m3Submit("t-seq", 1,
		m3Chunk("s-000.tar", 0), m3Chunk("s-001.tar", 0))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	st := waitTaskState(t, fx.tasks, "t-seq", agentpb.PreheatStatus_DONE)
	afterSubmit := st.GetSeq()
	// 新任务 seq=1,两块各置位一次 → 3(严格递增,非零)。
	if afterSubmit != 3 {
		t.Errorf("两位置位后 seq = %d, want 3(重置 1 + 两次置位)", afterSubmit)
	}
	st2, err := fx.tasks.Status(ctx, "t-seq")
	if err != nil || st2.GetSeq() != afterSubmit {
		t.Fatalf("无变化轮询 seq = %d(err=%v), want %d 不变", st2.GetSeq(), err, afterSubmit)
	}

	// Update 重置位图(seq++)后本地命中置位(seq++)。
	if _, err := fx.tasks.Update(ctx, m3Update("t-seq", 2, m3Chunk("s-000.tar", 0))); err != nil {
		t.Fatalf("Update: %v", err)
	}
	st = waitTaskState(t, fx.tasks, "t-seq", agentpb.PreheatStatus_DONE)
	if st.GetSeq() <= afterSubmit {
		t.Errorf("Update 后 seq = %d, want > %d(重置与置位均递增)", st.GetSeq(), afterSubmit)
	}
	st2, err = fx.tasks.Status(ctx, "t-seq")
	if err != nil || st2.GetSeq() != st.GetSeq() {
		t.Fatalf("无变化轮询 seq = %d(err=%v), want %d 不变", st2.GetSeq(), err, st.GetSeq())
	}
}

// TestPreheatCancelIdempotentAndResubmitNewTask:Cancel 幂等;取消后 Status 报
// canceled=true;同代际 Submit 视为新任务(seq 重新从 1 计、canceled 清零);取消后 Update 拒绝。
func TestPreheatCancelIdempotentAndResubmitNewTask(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "c-000.tar", chunk: 0, content: "cancel-c0", local: true},
		{path: "c-001.tar", chunk: 0, content: "cancel-c1", local: true},
	})
	fx.preloadMapping(t, testDataset)
	ctx := context.Background()

	if _, err := fx.tasks.Submit(ctx, m3Submit("t-cancel", 1,
		m3Chunk("c-000.tar", 0), m3Chunk("c-001.tar", 0))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	old := waitTaskState(t, fx.tasks, "t-cancel", agentpb.PreheatStatus_DONE)
	if old.GetSeq() < 2 {
		t.Fatalf("旧任务 seq = %d, want >= 2", old.GetSeq())
	}

	for i := 0; i < 2; i++ {
		resp, err := fx.tasks.Cancel(ctx, m3Cancel("t-cancel", 1))
		if err != nil || !resp.GetAccepted() {
			t.Fatalf("第 %d 次 Cancel = %+v, err=%v, want accepted(幂等)", i+1, resp, err)
		}
	}
	// 不存在的任务 Cancel 也幂等接受。
	if resp, err := fx.tasks.Cancel(ctx, m3Cancel("t-absent", 1)); err != nil || !resp.GetAccepted() {
		t.Fatalf("不存在任务 Cancel = %+v, err=%v, want accepted", resp, err)
	}
	// 取消只标记不删除,所以同名任务必须查得到 canceled:Controller 的台账靠它
	// 区分"节点上还有这个任务"和"已经取消"。
	if st, err := fx.tasks.Status(ctx, "t-cancel"); err != nil || !st.GetCanceled() {
		t.Fatalf("取消后 Status = canceled %v(err=%v), want true", st.GetCanceled(), err)
	}
	if _, err := fx.tasks.Update(ctx, m3Update("t-cancel", 1, m3Chunk("c-000.tar", 0))); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("取消后 Update code = %v, want FailedPrecondition", status.Code(err))
	}

	// 同 task_id、同 generation 再 Submit → 新任务:seq 从 1 重新开始。
	resp, err := fx.tasks.Submit(ctx, m3Submit("t-cancel", 1,
		m3Chunk("c-000.tar", 0), m3Chunk("c-001.tar", 0)))
	if err != nil || !resp.GetAccepted() {
		t.Fatalf("取消后重 Submit = %+v, err=%v, want accepted", resp, err)
	}
	fresh := waitTaskState(t, fx.tasks, "t-cancel", agentpb.PreheatStatus_DONE)
	if fresh.GetCanceled() {
		t.Error("重建任务 canceled = true, want false(取消标记属于旧任务)")
	}
	if fresh.GetSeq() != 3 {
		t.Errorf("重建任务 seq = %d, want 3(新任务从 1 起:1+2 次置位)", fresh.GetSeq())
	}
	if !fresh.GetStartedAt().AsTime().After(old.GetStartedAt().AsTime()) &&
		!fresh.GetStartedAt().AsTime().Equal(old.GetStartedAt().AsTime()) {
		t.Error("重建任务 started_at 应不早于旧任务")
	}
}

// TestPreheatBitmapShapeInvalid:位图长度与 chunks 数不匹配时 Status 返回 InvalidArgument;
// Update 的 chunks 形状非法同样 InvalidArgument(Q15)。
func TestPreheatBitmapShapeInvalid(t *testing.T) {
	fx := newSvcFixture(t)

	tk := newPreheatTask("t-bad-bitmap", "worker-01", preheatRequest{dataset: testDataset})
	tk.mu.Lock()
	tk.chunks = []prefetch.ChunkRef{m3Ref("x.tar", 0), m3Ref("y.tar", 0)}
	tk.bitmap = nil // 2 块应需 1 字节
	tk.mu.Unlock()
	fx.tasks.mu.Lock()
	fx.tasks.tasks[tk.taskID] = tk
	fx.tasks.mu.Unlock()

	if _, err := fx.tasks.Status(context.Background(), tk.taskID); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("位图形状非法 code = %v, want InvalidArgument(err=%v)", status.Code(err), err)
	}

	for name, chunks := range map[string][]*agentpb.ChunkRef{
		"空 path":  {m3Chunk("", 0)},
		"负 chunk": {m3Chunk("x.tar", -1)},
		"nil 元素":  {nil},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fx.tasks.Update(context.Background(), m3Update("t-any", 1, chunks...)); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument(err=%v)", status.Code(err), err)
			}
		})
	}
}

// TestPreheatProtoFrozenFieldNumbers:冻结字段号与新增 RPC 的 proto 描述符断言。
func TestPreheatProtoFrozenFieldNumbers(t *testing.T) {
	fd := agentpb.File_agent_proto
	for _, tt := range []struct {
		msg   string
		field string
		want  int32
	}{
		{"SubmitPreheatRequest", "generation", 8},
		{"PreheatStatus", "generation", 12},
		{"PreheatStatus", "seq", 13},
		{"PreheatStatus", "ready_bitmap", 14},
	} {
		m := fd.Messages().ByName(protoreflect.Name(tt.msg))
		if m == nil {
			t.Fatalf("消息 %s 不存在", tt.msg)
		}
		f := m.Fields().ByName(protoreflect.Name(tt.field))
		if f == nil || int32(f.Number()) != tt.want {
			t.Fatalf("%s.%s 字段号 = %v, want %d", tt.msg, tt.field, f, tt.want)
		}
	}
	svc := fd.Services().ByName("PreheatService")
	if svc == nil {
		t.Fatal("PreheatService 不存在")
	}
	for _, name := range []string{"SubmitPreheat", "GetPreheatStatus", "UpdatePreheat", "CancelPreheat"} {
		if svc.Methods().ByName(protoreflect.Name(name)) == nil {
			t.Errorf("PreheatService 缺少 RPC %s", name)
		}
	}
}

// TestReportBlocksStillUnimplemented:8.x 不使用推送模型,ReportBlocks 保持 Unimplemented。
func TestReportBlocksStillUnimplemented(t *testing.T) {
	s := NewServer(Config{}, nil)
	if err := s.ReportBlocks(nil); status.Code(err) != codes.Unimplemented {
		t.Fatalf("ReportBlocks code = %v, want Unimplemented(err=%v)", status.Code(err), err)
	}
}
