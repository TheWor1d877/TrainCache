package agentpb_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentpb "traincache/agent/api/agentpb"
)

// roundTrip 校验 proto 序列化往返保真(Marshal → Unmarshal → proto.Equal)。
func roundTrip(t *testing.T, in, out proto.Message) {
	t.Helper()
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := proto.Unmarshal(b, out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Errorf("往返不相等:\n in = %v\nout = %v", in, out)
	}
}

func TestNodeStatusRoundTrip(t *testing.T) {
	in := &agentpb.NodeStatus{
		Node:         "worker-01",
		AgentOk:      true,
		CacheRoot:    "/var/cache/traincache",
		CachedBytes:  1 << 30,
		FreeBytes:    42 << 30,
		RunningTasks: 3,
		Rescan: &agentpb.RescanStatus{
			Phase:       agentpb.RescanStatus_FULL_SCAN,
			Sampled:     123,
			Verified:    120,
			Quarantined: 2,
			RepairQueue: 5,
		},
		Catalog: []*agentpb.DatasetCatalog{
			{Dataset: "b-track", SnapshotId: "snap-20261014", Shards: 128, Entries: 4096},
			{Dataset: "a-track", SnapshotId: "snap-20261013", Shards: 64, Entries: 2048},
		},
	}
	roundTrip(t, in, &agentpb.NodeStatus{})
}

func TestPreheatStatusRoundTrip(t *testing.T) {
	in := &agentpb.PreheatStatus{
		TaskId:          "task-1",
		Node:            "worker-01",
		Dataset:         "b-track",
		SnapshotId:      "snap-20261014",
		Paths:           []string{"/train-data", "/train-extra"},
		State:           agentpb.PreheatStatus_RUNNING,
		TotalBlocks:     100,
		CompletedBlocks: 40,
		FailedBlocks:    1,
		StartedAt:       timestamppb.New(time.Unix(1763000000, 123456789)),
		UpdatedAt:       timestamppb.New(time.Unix(1763000060, 987654321)),
	}
	out := &agentpb.PreheatStatus{}
	roundTrip(t, in, out)

	if !out.GetStartedAt().AsTime().Equal(in.GetStartedAt().AsTime()) {
		t.Errorf("started_at AsTime 不相等:got %v, want %v", out.GetStartedAt().AsTime(), in.GetStartedAt().AsTime())
	}
	if !out.GetUpdatedAt().AsTime().Equal(in.GetUpdatedAt().AsTime()) {
		t.Errorf("updated_at AsTime 不相等:got %v, want %v", out.GetUpdatedAt().AsTime(), in.GetUpdatedAt().AsTime())
	}
}

func TestSubmitPreheatRequestRoundTrip(t *testing.T) {
	in := &agentpb.SubmitPreheatRequest{
		TaskId:     "task-1",
		Node:       "worker-01",
		Dataset:    "b-track",
		SnapshotId: "snap-20261014",
		Prefixes:   []string{"/train-data"},
		Chunks: []*agentpb.ChunkRef{
			{Path: "shard-000.tar", Chunk: 3},
			{Path: "shard-007.tar", Chunk: 0},
		},
		Priority: agentpb.PreheatPriority_P3,
	}
	roundTrip(t, in, &agentpb.SubmitPreheatRequest{})
}

func TestBlockReportRoundTrip(t *testing.T) {
	in := &agentpb.BlockReport{
		Node:   "worker-01",
		TaskId: "task-1",
		Ref:    &agentpb.ChunkRef{Path: "shard-000.tar", Chunk: 1},
		Fp:     "00112233445566778899aabbccddeeff",
		Bytes:  12345,
		Result: agentpb.BlockReport_REFETCHED,
	}
	roundTrip(t, in, &agentpb.BlockReport{})
}

// TestEnumZeroValues 断言各枚举零值与 §5.1 冻结内容一致(零值语义被 readiness 等依赖)。
func TestEnumZeroValues(t *testing.T) {
	if got := agentpb.PreheatPriority(0); got != agentpb.PreheatPriority_P0 {
		t.Errorf("PreheatPriority 零值 = %v, want P0(默认 P0)", got)
	}
	if got := agentpb.PreheatStatus_State(0); got != agentpb.PreheatStatus_QUEUED {
		t.Errorf("task state 零值 = %v, want QUEUED", got)
	}
	if got := agentpb.RescanStatus_Phase(0); got != agentpb.RescanStatus_SAMPLING {
		t.Errorf("RescanStatus.phase 零值 = %v, want SAMPLING", got)
	}
	if got := agentpb.BlockReport_Result(0); got != agentpb.BlockReport_OK {
		t.Errorf("BlockReport.result 零值 = %v, want OK", got)
	}
	if got := agentpb.NodeStatusItem(0); got != agentpb.NodeStatusItem_UNSPECIFIED {
		t.Errorf("NodeStatusItem 零值 = %v, want UNSPECIFIED", got)
	}
}

// TestEnumOrder 断言枚举值编号与 §5.1 冻结顺序逐一对应。
func TestEnumOrder(t *testing.T) {
	priorities := []agentpb.PreheatPriority{
		agentpb.PreheatPriority_P0, agentpb.PreheatPriority_P1,
		agentpb.PreheatPriority_P2, agentpb.PreheatPriority_P3,
	}
	for i, v := range priorities {
		if int32(v) != int32(i) {
			t.Errorf("PreheatPriority[%d] = %d, want %d", i, v, i)
		}
	}

	states := []agentpb.PreheatStatus_State{
		agentpb.PreheatStatus_QUEUED, agentpb.PreheatStatus_RUNNING,
		agentpb.PreheatStatus_DONE, agentpb.PreheatStatus_ERROR,
	}
	for i, v := range states {
		if int32(v) != int32(i) {
			t.Errorf("task state[%d] = %d, want %d", i, v, i)
		}
	}

	phases := []agentpb.RescanStatus_Phase{
		agentpb.RescanStatus_SAMPLING, agentpb.RescanStatus_FULL_SCAN, agentpb.RescanStatus_DONE,
	}
	for i, v := range phases {
		if int32(v) != int32(i) {
			t.Errorf("RescanStatus.phase[%d] = %d, want %d", i, v, i)
		}
	}

	results := []agentpb.BlockReport_Result{
		agentpb.BlockReport_OK, agentpb.BlockReport_CORRUPT, agentpb.BlockReport_REFETCHED,
	}
	for i, v := range results {
		if int32(v) != int32(i) {
			t.Errorf("BlockReport.result[%d] = %d, want %d", i, v, i)
		}
	}

	items := []agentpb.NodeStatusItem{
		agentpb.NodeStatusItem_UNSPECIFIED, agentpb.NodeStatusItem_AGENT,
		agentpb.NodeStatusItem_CATALOG, agentpb.NodeStatusItem_SPACE,
		agentpb.NodeStatusItem_TASKS, agentpb.NodeStatusItem_RESCAN,
	}
	for i, v := range items {
		if int32(v) != int32(i) {
			t.Errorf("NodeStatusItem[%d] = %d, want %d", i, v, i)
		}
	}
}

// TestServiceDescNames 断言服务全限定名与 §5.1 冻结内容一致。
func TestServiceDescNames(t *testing.T) {
	tests := []struct {
		got  string
		want string
	}{
		{agentpb.PreheatService_ServiceDesc.ServiceName, "traincache.agent.v1.PreheatService"},
		{agentpb.NodeService_ServiceDesc.ServiceName, "traincache.agent.v1.NodeService"},
		{agentpb.ReportService_ServiceDesc.ServiceName, "traincache.agent.v1.ReportService"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("ServiceDesc.ServiceName = %q, want %q", tt.got, tt.want)
		}
	}
}
