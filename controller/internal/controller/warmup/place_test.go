package warmup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/gwaa"
)

type placementCall struct {
	kind   PlacementActionKind
	node   string
	taskID string
}

type fakePlacementRPC struct {
	mu    sync.Mutex
	calls []placementCall
	fail  map[string]error
}

func (f *fakePlacementRPC) record(kind PlacementActionKind, node, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, placementCall{kind: kind, node: node, taskID: taskID})
	return f.fail[node]
}

func (f *fakePlacementRPC) Submit(_ context.Context, node string, req *agentpb.SubmitPreheatRequest) error {
	return f.record(ActionSubmit, node, req.GetTaskId())
}

func (f *fakePlacementRPC) Update(_ context.Context, node string, req *agentpb.UpdatePreheatRequest) error {
	return f.record(ActionUpdate, node, req.GetTaskId())
}

func (f *fakePlacementRPC) Cancel(_ context.Context, node string, req *agentpb.CancelPreheatRequest) error {
	return f.record(ActionCancel, node, req.GetTaskId())
}

func (f *fakePlacementRPC) snapshot() []placementCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]placementCall(nil), f.calls...)
}

func testChunks(paths ...string) []*agentpb.ChunkRef {
	out := make([]*agentpb.ChunkRef, len(paths))
	for i, path := range paths {
		out[i] = &agentpb.ChunkRef{Path: path, Chunk: int64(i)}
	}
	return out
}

func testPlacementDataSet() *traincachev1alpha1.DataSet {
	return &traincachev1alpha1.DataSet{
		Spec:   traincachev1alpha1.DataSetSpec{DatasetName: "dataset"},
		Status: traincachev1alpha1.DataSetStatus{SnapshotID: "snap"},
	}
}

func testPlacementInput() PlacementApplyInput {
	task := testTask("task", traincachev1alpha1.TargetRef{})
	task.Generation = 7
	return PlacementApplyInput{
		Task:    task,
		DataSet: testPlacementDataSet(),
		Windows: [][]*agentpb.ChunkRef{
			testChunks("a"),
			testChunks("b"),
			testChunks("c"),
			testChunks("d"),
			// Pod 4's window grew to 9 chunks while the Agent still reports the
			// 1-byte bitmap it accepted for the previous (<=8 chunk) window.
			testChunks("new", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"),
		},
		Previous: map[PreheatPair]ExistingPreheat{
			{PodIndex: 0, Node: "n0"}: {Generation: 7, BitmapBytes: 1}, // unchanged
			{PodIndex: 1, Node: "n1"}: {Generation: 7, BitmapBytes: 1}, // moved to n3
			{PodIndex: 2, Node: "n2"}: {Generation: 7, BitmapBytes: 1}, // unassigned
			{PodIndex: 4, Node: "n0"}: {Generation: 7, BitmapBytes: 1}, // same pair, window grew -> Update
		},
		Placement: gwaa.Output{
			Assignment: map[int32][]string{
				0: {"n0"},
				1: {"n3"},
				3: {"n0"},
				4: {"n0"},
			},
			UnassignedPods: []int32{2},
		},
		PodNames: map[int32]string{0: "p-0", 1: "p-1", 2: "p-2", 3: "p-3", 4: "p-4"},
	}
}

func TestPlacementActionsOnlyDifferences(t *testing.T) {
	actions, err := PlacementActions(testPlacementInput())
	if err != nil {
		t.Fatalf("PlacementActions: %v", err)
	}
	want := []PlacementAction{
		{Kind: ActionSubmit, PodIndex: 1, Node: "n3"},
		{Kind: ActionSubmit, PodIndex: 3, Node: "n0"},
		{Kind: ActionUpdate, PodIndex: 4, Node: "n0"},
		{Kind: ActionCancel, PodIndex: 1, Node: "n1"},
		{Kind: ActionCancel, PodIndex: 2, Node: "n2"},
	}
	if len(actions) != len(want) {
		t.Fatalf("actions = %v, want %v", actions, want)
	}
	for i := range want {
		if actions[i].Kind != want[i].Kind || actions[i].PodIndex != want[i].PodIndex || actions[i].Node != want[i].Node {
			t.Fatalf("action %d = %+v, want %+v", i, actions[i], want[i])
		}
	}
	if actions[0].Chunks == nil || len(actions[0].Chunks) != 1 || actions[0].Chunks[0].GetPath() != "b" {
		t.Fatalf("submit chunks = %v, want pod 1 window", actions[0].Chunks)
	}
}

func TestPlacementActionsUnknownGenerationForcesUpdate(t *testing.T) {
	in := testPlacementInput()
	in.Placement.Assignment = map[int32][]string{0: {"n0"}}
	in.Previous = map[PreheatPair]ExistingPreheat{
		{PodIndex: 0, Node: "n0"}: {
			Generation:  0,
			BitmapBytes: bitmapBytes(len(in.Windows[0])),
		},
	}
	actions, err := PlacementActions(in)
	if err != nil {
		t.Fatalf("PlacementActions: %v", err)
	}
	if len(actions) != 1 || actions[0].Kind != ActionUpdate ||
		actions[0].PodIndex != 0 || actions[0].Node != "n0" {
		t.Fatalf("actions = %+v, want one Update for pod 0 on n0", actions)
	}
}

func TestApplyPlacementOnlyDiffRPC(t *testing.T) {
	rpc := &fakePlacementRPC{}
	in := testPlacementInput()
	result, err := ApplyPlacement(context.Background(), rpc, nil, in)
	if err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	if result.Submitted != 2 || result.Updated != 1 || result.Cancelled != 2 {
		t.Fatalf("counts submit=%d update=%d cancel=%d", result.Submitted, result.Updated, result.Cancelled)
	}
	if !reflect.DeepEqual(result.Unassigned, []int32{2}) {
		t.Fatalf("unassigned = %v, want [2]", result.Unassigned)
	}
	calls := rpc.snapshot()
	if len(calls) != 5 {
		t.Fatalf("RPC calls = %d, want 5: %+v", len(calls), calls)
	}
	for _, call := range calls {
		if call.kind == ActionSubmit && call.taskID != "train/task/p1" && call.taskID != "train/task/p3" {
			t.Fatalf("unexpected submit task id %q", call.taskID)
		}
	}

	// A second pass with the same desired state must not issue any RPC.
	previous := map[PreheatPair]ExistingPreheat{}
	for index, nodes := range in.Placement.Assignment {
		for _, node := range nodes {
			previous[PreheatPair{PodIndex: index, Node: node}] = ExistingPreheat{
				Generation:  uint64(in.Task.Generation),
				BitmapBytes: bitmapBytes(len(in.Windows[index])),
			}
		}
	}
	in.Previous = previous
	rpc = &fakePlacementRPC{}
	result, err = ApplyPlacement(context.Background(), rpc, nil, in)
	if err != nil {
		t.Fatalf("second ApplyPlacement: %v", err)
	}
	if len(result.Actions) != 0 || len(rpc.snapshot()) != 0 {
		t.Fatalf("idempotent pass issued actions: %v", rpc.snapshot())
	}
}

// The 7.x Agent treats an empty snapshot_id as CURRENT and rejects a concrete
// one, so ApplyPlacement's Submit branch must drop DataSet.Status.SnapshotID.
func TestApplyPlacementSubmitClearsSnapshotID(t *testing.T) {
	in := testPlacementInput()
	in.DataSet.Status.SnapshotID = "snap-1"
	in.Previous = map[PreheatPair]ExistingPreheat{}
	in.Placement = gwaa.Output{Assignment: map[int32][]string{0: {"n0"}}}

	pool := &fakeAgentPool{}
	result, err := ApplyPlacement(context.Background(), pool, nil, in)
	if err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	if result.Submitted != 1 {
		t.Fatalf("submitted = %d, want 1", result.Submitted)
	}
	records := pool.snapshot()
	if len(records) != 1 || records[0].node != "n0" {
		t.Fatalf("submit records = %+v, want one on n0", records)
	}
	req := records[0].req
	if req.GetSnapshotId() != "" {
		t.Fatalf("snapshot_id = %q, want empty (CURRENT)", req.GetSnapshotId())
	}
	if req.GetDataset() != "dataset" || req.GetTaskId() != "train/task/p0" {
		t.Fatalf("request = %+v, want dataset/task id preserved", req)
	}
}

func TestApplyPlacementFailureClassification(t *testing.T) {
	in := testPlacementInput()
	in.Previous = map[PreheatPair]ExistingPreheat{}
	in.Placement = gwaa.Output{
		Assignment: map[int32][]string{0: {"n0"}, 1: {"n0"}, 2: {"n1"}},
	}
	rpc := &fakePlacementRPC{fail: map[string]error{"n0": agentclient.ErrPermanent}}
	result, err := ApplyPlacement(context.Background(), rpc, nil, in)
	if err != nil {
		t.Fatalf("permanent failure should not be returned as a retry error: %v", err)
	}
	if _, ok := result.Permanent["n0"]; !ok {
		t.Fatalf("permanent map = %v, want n0", result.Permanent)
	}
	if result.Submitted != 1 {
		t.Fatalf("submitted = %d, want 1 (n1 only)", result.Submitted)
	}
	calls := rpc.snapshot()
	if len(calls) != 2 {
		t.Fatalf("calls = %+v, want one n0 failure and one n1 success", calls)
	}

	rpc = &fakePlacementRPC{fail: map[string]error{"n0": errors.New("transport down")}}
	result, err = ApplyPlacement(context.Background(), rpc, nil, in)
	if !errors.Is(err, agentclient.ErrAgentUnavailable) {
		t.Fatalf("error = %v, want ErrAgentUnavailable", err)
	}
	if _, ok := result.Unavailable["n0"]; !ok {
		t.Fatalf("unavailable map = %v, want n0", result.Unavailable)
	}
}

func TestApplyPlacementLogsFallback(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	in := testPlacementInput()
	in.Previous = map[PreheatPair]ExistingPreheat{}
	in.Placement = gwaa.Output{
		Assignment:     map[int32][]string{0: {"n0"}},
		UnassignedPods: []int32{1, 2, 3, 4},
		Rounds:         4,
		FellBack:       true,
	}
	if _, err := ApplyPlacement(context.Background(), &fakePlacementRPC{}, log, in); err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var foundWarn, foundChange bool
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		msg, _ := record["msg"].(string)
		switch msg {
		case "gwaa assignment not converged, keeping first greedy result":
			foundWarn = record["level"] == "WARN" &&
				record["reason"] == "round_limit_exceeded" &&
				record["rounds"] == float64(4) &&
				record["warmup_task"] == "train/task"
		case "gwaa assignment changed":
			foundChange = record["rounds"] == float64(4) && record["assign"] == "p0=n0"
		}
	}
	if !foundWarn {
		t.Fatalf("missing fallback Warn log: %s", buf.String())
	}
	if !foundChange {
		t.Fatalf("missing assignment change log: %s", buf.String())
	}
}

func TestPlacementActionsInvalidInput(t *testing.T) {
	in := testPlacementInput()
	in.Task = nil
	if _, err := PlacementActions(in); err == nil {
		t.Fatal("PlacementActions accepted a nil Task")
	}

	in = testPlacementInput()
	in.Placement.Err = gwaa.ErrInvalidInput
	if _, err := PlacementActions(in); err == nil {
		t.Fatal("PlacementActions ignored Placement.Err")
	}

	in = testPlacementInput()
	in.Placement.Assignment = map[int32][]string{9: {"n0"}}
	if _, err := PlacementActions(in); err == nil {
		t.Fatal("PlacementActions accepted an out-of-range window")
	}

	in = testPlacementInput()
	in.DataSet = nil
	if _, err := ApplyPlacement(context.Background(), &fakePlacementRPC{}, nil, in); err == nil {
		t.Fatal("ApplyPlacement accepted a nil DataSet for Submit")
	}
}

func TestBitmapBytes(t *testing.T) {
	if got := bitmapBytes(0); got != 0 {
		t.Fatalf("bitmapBytes(0) = %d, want 0", got)
	}
	for n := 1; n <= 8; n++ {
		if got := bitmapBytes(n); got != 1 {
			t.Fatalf("bitmapBytes(%d) = %d, want 1", n, got)
		}
	}
	if got := bitmapBytes(9); got != 2 {
		t.Fatalf("bitmapBytes(9) = %d, want 2", got)
	}
}
