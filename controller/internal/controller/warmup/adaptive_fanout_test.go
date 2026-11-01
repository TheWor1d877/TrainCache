package warmup

// Adaptive fanout acceptance tests (docs/7.2 D39):
// the controller passes both bounds to GWAA, then drives status, score
// annotations, gate inputs and logs from the actual Assignment instead of the
// fanout maximum.

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"testing"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// m14PlacementTask builds a one-Pod task whose ReadinessTable row is the cache
// distribution on the candidate columns.
func m14PlacementTask(k int32, row []int32, nodes []string, fanout, fanoutMin int32) *traincachev1alpha1.WarmupTask {
	task := fixtureTask(1, k, [][]int32{{0, 1}})
	task.Spec.PreheatFanout = fanout
	task.Spec.PreheatFanoutMin = fanoutMin
	task.Status.CandidateNodes = append([]string(nil), nodes...)
	task.Status.ReadinessTable = [][]int32{append([]int32(nil), row...)}
	return task
}

// m14Window returns k ChunkRefs so score writing has a window to decode.
func m14Window(k int32) []*agentpb.ChunkRef {
	chunks := make([]*agentpb.ChunkRef, k)
	for i := range chunks {
		chunks[i] = &agentpb.ChunkRef{Path: "s.tar", Chunk: int64(i)}
	}
	return chunks
}

func m14Nodes(count int) []string {
	out := make([]string, count)
	for i := range out {
		out[i] = "n" + string(rune('1'+i))
	}
	return out
}

func m14AssignmentNodes(assignment map[int32][]string, pod int32) map[string]struct{} {
	out := make(map[string]struct{}, len(assignment[pod]))
	for _, node := range assignment[pod] {
		out[node] = struct{}{}
	}
	return out
}

// TestAdaptiveTargetFromDistribution covers:
// 12,1,0,0,0 with K=16 keeps only the node above the 0.5*best line unless the
// minimum forces more; min=3 also sets ForcedByMin. Status and the score
// annotation follow the actual pair count.
func TestAdaptiveTargetFromDistribution(t *testing.T) {
	nodes := m14Nodes(5)
	cases := []struct {
		name   string
		min    int32
		want   int
		forced bool
		low    bool
	}{
		{name: "min1", min: 1, want: 1},
		{name: "min2", min: 2, want: 2, forced: true},
		{name: "min3", min: 3, want: 3, forced: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := m14PlacementTask(16, []int32{12, 1, 0, 0, 0}, nodes, 3, tc.min)
			r := &Reconciler{}
			placement := r.computePlacement(task, nodes, nil, nil)
			if placement.Err != nil {
				t.Fatalf("computePlacement: %v", placement.Err)
			}
			if got := len(placement.TargetSet[0]); got != tc.want {
				t.Fatalf("target count = %d, want %d: %#v", got, tc.want, placement.TargetSet)
			}
			if got := len(placement.Assignment[0]); got != tc.want {
				t.Fatalf("assignment count = %d, want %d: %#v", got, tc.want, placement.Assignment)
			}
			decision := placement.Decisions[0]
			if decision.TargetFanout != int32(tc.want) {
				t.Fatalf("decision target = %d, want %d", decision.TargetFanout, tc.want)
			}
			if decision.ForcedByMin != tc.forced {
				t.Fatalf("ForcedByMin = %v, want %v", decision.ForcedByMin, tc.forced)
			}
			if decision.LowSignal != tc.low {
				t.Fatalf("LowSignal = %v, want %v", decision.LowSignal, tc.low)
			}

			applyAssignment(task, placement, nil, map[int32]string{0: "trainer-0"}, nodes)
			if got := len(podEntries(task, 0)); got != tc.want {
				t.Fatalf("status entries = %d, want actual assignment count %d", got, tc.want)
			}
			assigned := m14AssignmentNodes(placement.Assignment, 0)
			if len(assigned) != tc.want {
				t.Fatalf("assigned node set = %v, want %d nodes", assigned, tc.want)
			}

			// The Pod score snapshot covers exactly the assigned nodes, not P*max.
			snapshot := podScoreSnapshot(task, slog.New(slog.NewTextHandler(io.Discard, nil)),
				[][]*agentpb.ChunkRef{m14Window(16)}, 0, "trainer-0")
			if len(snapshot) != tc.want {
				t.Fatalf("score snapshot = %v, want %d assigned nodes", snapshot, tc.want)
			}
			for node := range snapshot {
				if _, ok := assigned[node]; !ok {
					t.Fatalf("score snapshot covers an unassigned node %s: %v", node, snapshot)
				}
			}
			for node := range assigned {
				if _, ok := snapshot[node]; !ok {
					t.Fatalf("score snapshot missing assigned node %s: %v", node, snapshot)
				}
			}
		})
	}
}

// TestLowSignalTakesUpperBound covers: with K=16 the
// low-signal line is ceil(0.25*16)=4, so 1,1,0,0,0 takes max=3 even at min=1.
func TestLowSignalTakesUpperBound(t *testing.T) {
	nodes := m14Nodes(5)
	task := m14PlacementTask(16, []int32{1, 1, 0, 0, 0}, nodes, 3, 1)
	r := &Reconciler{}
	placement := r.computePlacement(task, nodes, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}
	decision := placement.Decisions[0]
	if !decision.LowSignal {
		t.Fatalf("LowSignal = false, want true: %#v", decision)
	}
	if got := len(placement.Assignment[0]); got != 3 {
		t.Fatalf("assignment count = %d, want max 3: %#v", got, placement.Assignment)
	}
}

// TestRetentionLineKeepsTwo covers: best=11>4 and the
// 0.5*best=5.5 line keeps 11 and 7, so the target is 2.
func TestRetentionLineKeepsTwo(t *testing.T) {
	nodes := m14Nodes(5)
	task := m14PlacementTask(16, []int32{11, 7, 3, 0, 0}, nodes, 3, 1)
	r := &Reconciler{}
	placement := r.computePlacement(task, nodes, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}
	if got := len(placement.Assignment[0]); got != 2 {
		t.Fatalf("assignment count = %d, want 2: %#v", got, placement.Assignment)
	}
	if placement.Decisions[0].LowSignal {
		t.Fatalf("LowSignal = true, want false")
	}
}

// TestFanoutOneKeepsSingleNode covers: fanout=1,min=1 keeps the
// single-node behaviour for status and annotations.
func TestFanoutOneKeepsSingleNode(t *testing.T) {
	nodes := m14Nodes(4)
	task := m14PlacementTask(16, []int32{12, 1, 0, 0}, nodes, 1, 1)
	r := &Reconciler{}
	placement := r.computePlacement(task, nodes, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}
	if got := len(placement.Assignment[0]); got != 1 {
		t.Fatalf("assignment count = %d, want 1", got)
	}
	applyAssignment(task, placement, nil, map[int32]string{0: "trainer-0"}, nodes)
	if got := len(podEntries(task, 0)); got != 1 {
		t.Fatalf("status entries = %d, want 1", got)
	}
}

// TestMinAboveFeasibleSetsMinUnmet covers: min=3 with only
// two feasible nodes caps the target at 2 and reports MinUnmet in the placement
// log together with the rest of the fanout decision fields.
func TestMinAboveFeasibleSetsMinUnmet(t *testing.T) {
	nodes := m14Nodes(2)
	task := m14PlacementTask(16, []int32{12, 1}, nodes, 3, 3)
	r := &Reconciler{}
	placement := r.computePlacement(task, nodes, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}
	decision := placement.Decisions[0]
	if !decision.MinUnmet {
		t.Fatalf("MinUnmet = false, want true: %#v", decision)
	}
	if got := len(placement.Assignment[0]); got != 2 {
		t.Fatalf("assignment count = %d, want feasible 2: %#v", got, placement.Assignment)
	}

	buf, log := m12JSONLog(t)
	rpc := &fakePlacementRPC{}
	_, err := ApplyPlacement(context.Background(), rpc, log, PlacementApplyInput{
		Task:      task,
		DataSet:   testPlacementDataSet(),
		Windows:   [][]*agentpb.ChunkRef{m14Window(16)},
		Previous:  map[PreheatPair]ExistingPreheat{},
		Placement: placement,
		PodNames:  map[int32]string{0: "trainer-0"},
		Fanout:    3,
		FanoutMin: 3,
	})
	if err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	changed := m12FindLog(t, buf, "gwaa assignment changed")
	m12RequireFields(t, changed, "fanout_min", "fanout_max", "fanout_target", "fanout_actual",
		"low_signal", "forced_by_min", "min_unmet", "fallback_out_of_target", "prune_reason")
	if changed["min_unmet"] != true {
		t.Fatalf("log min_unmet = %v, want true: %#v", changed["min_unmet"], changed)
	}
	if changed["fanout_min"] != float64(3) || changed["fanout_max"] != float64(3) {
		t.Fatalf("log fanout bounds = %v/%v, want 3/3", changed["fanout_min"], changed["fanout_max"])
	}
	if changed["fanout_target"] != "p0=2" || changed["fanout_actual"] != "p0=2" {
		t.Fatalf("log target/actual = %v/%v, want p0=2/p0=2: %#v",
			changed["fanout_target"], changed["fanout_actual"], changed)
	}
}

// TestOutOfTargetFallback covers the documented multi-Pod case: identical
// distributions and allowMultiPodPerNode=false leave one Pod on the single
// fallback node outside its deterministic target set.
func TestOutOfTargetFallback(t *testing.T) {
	nodes := m14Nodes(3)
	task := fixtureTask(2, 16, [][]int32{{0, 1}, {0, 1}})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 1
	task.Status.CandidateNodes = append([]string(nil), nodes...)
	task.Status.ReadinessTable = [][]int32{{12, 1, 0}, {12, 1, 0}}
	r := &Reconciler{Rand: rand.New(rand.NewSource(7))}
	placement := r.computePlacement(task, nodes, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}

	fallbacks := 0
	fallbackNode := ""
	fallbackPod := int32(-1)
	for pod := int32(0); pod < 2; pod++ {
		if got := len(placement.Assignment[pod]); got != 1 {
			t.Fatalf("pod %d assignment count = %d, want 1: %#v", pod, got, placement.Assignment)
		}
		decision := placement.Decisions[pod]
		if decision.FallbackOutOfTarget {
			fallbacks++
			fallbackPod = pod
			fallbackNode = placement.Assignment[pod][0]
		} else if len(placement.TargetSet[pod]) != 1 {
			t.Fatalf("pod %d target count = %d, want 1", pod, len(placement.TargetSet[pod]))
		}
	}
	if fallbacks != 1 {
		t.Fatalf("fallback pods = %d, want exactly 1: %#v", fallbacks, placement.Decisions)
	}
	if fallbackPod < 0 || fallbackNode == "" {
		t.Fatalf("fallback Pod/node not recorded: %#v", placement)
	}
	for _, target := range placement.TargetSet[fallbackPod] {
		if target == fallbackNode {
			t.Fatalf("fallback node %s is inside target set %v", fallbackNode, placement.TargetSet[fallbackPod])
		}
	}

	buf, log := m12JSONLog(t)
	rpc := &fakePlacementRPC{}
	_, err := ApplyPlacement(context.Background(), rpc, log, PlacementApplyInput{
		Task:      task,
		DataSet:   testPlacementDataSet(),
		Windows:   [][]*agentpb.ChunkRef{m14Window(16), m14Window(16)},
		Previous:  map[PreheatPair]ExistingPreheat{},
		Placement: placement,
		PodNames:  map[int32]string{0: "trainer-0", 1: "trainer-1"},
		Fanout:    2,
		FanoutMin: 1,
	})
	if err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	changed := m12FindLog(t, buf, "gwaa assignment changed")
	if changed["fallback_out_of_target"] != true {
		t.Fatalf("log fallback_out_of_target = %v, want true: %#v",
			changed["fallback_out_of_target"], changed)
	}
	if changed["fanout_actual"] != "p0=1,p1=1" {
		t.Fatalf("log fanout_actual = %v, want p0=1,p1=1", changed["fanout_actual"])
	}
}

// TestMinUnmetFollowsActualAssignment covers the follow-up contract: both
// Pods plan a two-node target from the same 11,7,3 distribution, but
// allowMultiPodPerNode=false forces one of them down to a single node after
// conflict adjudication and fallback. MinUnmet, the placement log, status
// entries and score annotations all follow the actual Assignment.
func TestMinUnmetFollowsActualAssignment(t *testing.T) {
	nodes := m14Nodes(3)
	task := fixtureTask(2, 16, [][]int32{{0, 1}, {0, 1}})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 2
	task.Status.CandidateNodes = append([]string(nil), nodes...)
	task.Status.ReadinessTable = [][]int32{{11, 7, 3}, {11, 7, 3}}

	// The conflict coin flips decide whether the loser ends with one target
	// node or an out-of-target fallback. Pick the first seed that exercises the
	// fallback path so the log assertion below is meaningful.
	var chosenSeed int64 = -1
	var lowPod int32
	for seed := int64(0); seed < 64 && chosenSeed < 0; seed++ {
		r := &Reconciler{Rand: rand.New(rand.NewSource(seed))}
		probe := r.computePlacement(task, nodes, nil, nil)
		if probe.Err != nil {
			t.Fatalf("computePlacement: %v", probe.Err)
		}
		for pod := int32(0); pod < 2; pod++ {
			if len(probe.Assignment[pod]) == 1 && probe.Decisions[pod].FallbackOutOfTarget {
				chosenSeed, lowPod = seed, pod
				break
			}
		}
	}
	if chosenSeed < 0 {
		t.Fatal("no seed produced a single-node out-of-target fallback")
	}
	r := &Reconciler{Rand: rand.New(rand.NewSource(chosenSeed))}
	placement := r.computePlacement(task, nodes, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}

	highPod := int32(1 - lowPod)
	for pod := int32(0); pod < 2; pod++ {
		if got := len(placement.TargetSet[pod]); got != 2 {
			t.Fatalf("pod %d target count = %d, want 2", pod, got)
		}
	}
	if got := len(placement.Assignment[lowPod]); got != 1 {
		t.Fatalf("low pod %d actual = %d, want 1: %#v", lowPod, got, placement.Assignment)
	}
	if !placement.Decisions[lowPod].MinUnmet {
		t.Fatalf("low pod %d MinUnmet = false: %#v", lowPod, placement.Decisions[lowPod])
	}
	if got := len(placement.Assignment[highPod]); got != 2 {
		t.Fatalf("high pod %d actual = %d, want 2: %#v", highPod, got, placement.Assignment)
	}
	if placement.Decisions[highPod].MinUnmet {
		t.Fatalf("high pod %d MinUnmet = true: %#v", highPod, placement.Decisions[highPod])
	}

	podNames := map[int32]string{0: "trainer-0", 1: "trainer-1"}
	applyAssignment(task, placement, nil, podNames, nodes)
	if got := len(podEntries(task, lowPod)); got != 1 {
		t.Fatalf("low pod %d status entries = %d, want 1", lowPod, got)
	}
	if got := len(podEntries(task, highPod)); got != 2 {
		t.Fatalf("high pod %d status entries = %d, want 2", highPod, got)
	}

	// The Pod score snapshot covers exactly its assigned nodes: one for the low Pod.
	snapshot := podScoreSnapshot(task, slog.New(slog.NewTextHandler(io.Discard, nil)),
		[][]*agentpb.ChunkRef{m14Window(16), m14Window(16)}, lowPod, podNames[lowPod])
	if len(snapshot) != 1 {
		t.Fatalf("low pod %d score snapshot = %v, want its actual 1 node", lowPod, snapshot)
	}
	if _, ok := snapshot[placement.Assignment[lowPod][0]]; !ok {
		t.Fatalf("low pod %d score snapshot = %v, want assigned node %s",
			lowPod, snapshot, placement.Assignment[lowPod][0])
	}

	buf, log := m12JSONLog(t)
	rpc := &fakePlacementRPC{}
	_, err := ApplyPlacement(context.Background(), rpc, log, PlacementApplyInput{
		Task:      task,
		DataSet:   testPlacementDataSet(),
		Windows:   [][]*agentpb.ChunkRef{m14Window(16), m14Window(16)},
		Previous:  map[PreheatPair]ExistingPreheat{},
		Placement: placement,
		PodNames:  podNames,
		Fanout:    2,
		FanoutMin: 2,
	})
	if err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	changed := m12FindLog(t, buf, "gwaa assignment changed")
	m12RequireFields(t, changed, "fanout_min", "fanout_max", "fanout_target", "fanout_actual",
		"low_signal", "forced_by_min", "min_unmet", "fallback_out_of_target", "prune_reason")
	if changed["min_unmet"] != true {
		t.Fatalf("log min_unmet = %v, want true: %#v", changed["min_unmet"], changed)
	}
	if changed["fallback_out_of_target"] != true {
		t.Fatalf("log fallback_out_of_target = %v, want true: %#v",
			changed["fallback_out_of_target"], changed)
	}
	if changed["fanout_min"] != float64(2) || changed["fanout_max"] != float64(2) {
		t.Fatalf("log fanout bounds = %v/%v, want 2/2", changed["fanout_min"], changed["fanout_max"])
	}
	if changed["fanout_target"] != "p0=2,p1=2" {
		t.Fatalf("log fanout_target = %v, want p0=2,p1=2", changed["fanout_target"])
	}
	wantActual := "p0=1,p1=2"
	if lowPod == 1 {
		wantActual = "p0=2,p1=1"
	}
	if changed["fanout_actual"] != wantActual {
		t.Fatalf("log fanout_actual = %v, want %s", changed["fanout_actual"], wantActual)
	}
}

// TestValidateTaskFanoutBounds covers the defensive spec checks: negative
// bounds are rejected and an effective minimum above the maximum is rejected.
func TestValidateTaskFanoutBounds(t *testing.T) {
	base := fileTask([][]string{{"a.tar"}}, 2)
	cases := []struct {
		name      string
		fanout    int32
		fanoutMin int32
		want      bool
	}{
		{name: "defaults", fanout: 0, fanoutMin: 0},
		{name: "valid", fanout: 3, fanoutMin: 2},
		{name: "negative_max", fanout: -1, fanoutMin: 1, want: true},
		{name: "negative_min", fanout: 2, fanoutMin: -1, want: true},
		{name: "min_gt_max", fanout: 2, fanoutMin: 3, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := base.DeepCopy()
			task.Spec.PreheatFanout = tc.fanout
			task.Spec.PreheatFanoutMin = tc.fanoutMin
			reasons := ValidateTask(task)
			if got := len(reasons) > 0; got != tc.want {
				t.Fatalf("reasons = %v, want invalid=%v", reasons, tc.want)
			}
			if tc.want && !strings.Contains(strings.Join(reasons, ";"), "preheatFanout") {
				t.Fatalf("reasons = %v, want a preheatFanout message", reasons)
			}
		})
	}
}

// TestMinAboveMaxClampWarning covers the controller-side defensive clamp
// that runs when an object bypassed the CRD CEL rule.
func TestMinAboveMaxClampWarning(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Spec.PreheatFanout = 1
	task.Spec.PreheatFanoutMin = 3
	buf, log := m12JSONLog(t)
	c := fixtureClient(t, task)
	r := &Reconciler{Client: c, Agents: &fakeAgentPool{}, Log: log}

	_, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rec := m12FindLog(t, buf, "preheat fanout minimum exceeds maximum; clamping to max")
	m12RequireFields(t, rec, "fanout_min", "fanout_max", "requested_fanout_min", "reason")
	if rec["fanout_min"] != float64(1) || rec["fanout_max"] != float64(1) {
		t.Fatalf("clamped bounds = %v/%v, want 1/1", rec["fanout_min"], rec["fanout_max"])
	}
	if rec["requested_fanout_min"] != float64(3) {
		t.Fatalf("requested min = %v, want 3", rec["requested_fanout_min"])
	}
}
