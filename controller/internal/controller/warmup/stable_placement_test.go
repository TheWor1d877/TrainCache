package warmup

// Regression for the cancel loop (docs/TODO.md A): the "already submitted"
// side of the differential pass used to be guessed from
// status.podNodeReadiness, so the wide-preheat broadcast stayed "initial"
// forever. With several candidate nodes every reconcile re-broadcast Submit to
// all of them and cancelled the ones GWAA had just dropped, which made each
// Agent re-download its window from chunk 0. The ledger is now what the Agents
// report about the task_id, so one broadcast is enough.

import (
	"reflect"
	"testing"
	"time"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/gwaa"
)

func TestReconcileWideFanoutRunsOnceWithSeveralCandidates(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1, 2, 3}})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 2
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2", "n3"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds,
		fixtureNode("n1"), fixtureNode("n2"), fixtureNode("n3"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureAgentPod("n3"),
		fixtureTargetPod(0))
	now := time.Unix(300, 0)
	r := &Reconciler{Client: c, Agents: pool, Now: func() time.Time { return now }}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	assigned := assignedNodes(getTask(t, c), 0)
	if len(assigned) != 2 {
		t.Fatalf("assigned nodes = %v, want the fan-out of 2", assigned)
	}
	// Round 1 is the broadcast: one Submit per candidate, plus one Cancel for
	// the candidate GWAA dropped.
	if pool.submitCalls != 3 {
		t.Fatalf("round 1 submits = %d, want 3 (one per candidate)", pool.submitCalls)
	}
	if pool.cancelCalls != 1 {
		t.Fatalf("round 1 cancels = %d, want 1 (the dropped candidate)", pool.cancelCalls)
	}
	if contains(assigned, pool.cancelled[0]) {
		t.Fatalf("round 1 cancelled an assigned node: %v assigned, cancelled %v", assigned, pool.cancelled)
	}

	for round := 2; round <= 3; round++ {
		if err := reconcile(t, r); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if got := assignedNodes(getTask(t, c), 0); !reflect.DeepEqual(got, assigned) {
			t.Fatalf("round %d assignment = %v, want %v", round, got, assigned)
		}
	}

	// Every Agent reported holding its task, so nothing was re-submitted and
	// no further Cancel was issued.
	if pool.submitCalls != 3 {
		t.Fatalf("submits = %d after three rounds, want 3 (broadcast only)", pool.submitCalls)
	}
	if pool.cancelCalls != 1 {
		t.Fatalf("cancels = %d after three rounds, want 1 (%v)", pool.cancelCalls, pool.cancelled)
	}
	if phase := getTask(t, c).Status.Phase; phase != traincachev1alpha1.WarmupPhasePreheating {
		t.Fatalf("phase = %q, want %q", phase, traincachev1alpha1.WarmupPhasePreheating)
	}
}

// TestSubmittedLedgerOnlyCountsHeldTasks pins the three states the Agents can
// answer in: held (in the ledger, so placement leaves it alone), answered but
// cancelled (not held, so a re-selection Submits again instead of silently
// skipping), and no answer at all (in no bucket this round).
func TestSubmittedLedgerOnlyCountsHeldTasks(t *testing.T) {
	task := testTask("task", traincachev1alpha1.TargetRef{})
	task.Generation = 7
	windows := [][]*agentpb.ChunkRef{testChunks("a", "b")}
	candidates := []string{"n1", "n2", "n3"}
	observations := map[readinessKey]AgentTaskObservation{
		{podIndex: 0, node: "n1"}: {Held: true, Generation: 7, WindowBlocks: 2},
		// Cancel marks a task without deleting it; the Agent still answers, but
		// it is not holding anything (docs/6.3 D55).
		{podIndex: 0, node: "n2"}: {Generation: 7, WindowBlocks: 2},
		// n3 answered nothing, so it is absent from the map.
	}

	ledger := submittedLedger(task, windows, candidates, observations, false, newSubmitResult())
	if len(ledger) != 1 {
		t.Fatalf("ledger = %#v, want only the held pair", ledger)
	}
	if got := ledger[PreheatPair{PodIndex: 0, Node: "n1"}]; got != (ExistingPreheat{Generation: 7, WindowBlocks: 2}) {
		t.Fatalf("held pair = %#v, want generation 7 and one bitmap byte", got)
	}
	if !anyTaskHeld(observations) {
		t.Fatal("anyTaskHeld = false, want true: the wide fan-out already ran")
	}
	if anyTaskHeld(map[readinessKey]AgentTaskObservation{{podIndex: 0, node: "n2"}: {Generation: 7, WindowBlocks: 2}}) {
		t.Fatal("a cancelled task alone must not count as held, or the broadcast never runs again")
	}

	// GWAA now wants the cancelled pair. It must be submitted afresh, and the
	// pair that fell out of the assignment cancelled.
	actions, err := PlacementActions(PlacementApplyInput{
		Task:      task,
		DataSet:   testPlacementDataSet(),
		Windows:   windows,
		Previous:  ledger,
		Placement: gwaa.Output{Assignment: map[int32][]string{0: {"n2"}}},
	})
	if err != nil {
		t.Fatalf("PlacementActions: %v", err)
	}
	want := []PlacementAction{
		{Kind: ActionSubmit, PodIndex: 0, Node: "n2", Chunks: windows[0]},
		{Kind: ActionCancel, PodIndex: 0, Node: "n1"},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("actions = %v, want %v", actions, want)
	}
}

func contains(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

// TestReconcileKeepsIncumbentWhileCandidatesAdvance is the D45 gate: the wide
// preheat leaves a task on every candidate, their counts advance together, and
// a strict c_ij argmax handed the slot to whichever node landed a block first.
// One hand costs a Submit plus a Cancel, and the resubmitted node restarts its
// window, so the loop never released (docs/TODO.md A, 4 GiB for a 1 GiB task).
func TestReconcileKeepsIncumbentWhileCandidatesAdvance(t *testing.T) {
	task := fixtureTask(1, 4, [][]int32{{0, 1, 2, 3}})
	task.Spec.PreheatFanout = 1
	task.Spec.PreheatFanoutMin = 1
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2", "n3"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds,
		fixtureNode("n1"), fixtureNode("n2"), fixtureNode("n3"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureAgentPod("n3"),
		fixtureTargetPod(0))
	now := time.Unix(600, 0)
	r := &Reconciler{Client: c, Agents: pool, Now: func() time.Time { return now }}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	assigned := assignedNodes(getTask(t, c), 0)
	if len(assigned) != 1 {
		t.Fatalf("assigned nodes = %v, want 1", assigned)
	}
	incumbent := assigned[0]
	dropped := otherNode(incumbent, []string{"n1", "n2", "n3"})
	if pool.submitCalls != 3 || pool.cancelCalls != 2 {
		t.Fatalf("round 1: submits=%d cancels=%d, want 3 and 2", pool.submitCalls, pool.cancelCalls)
	}

	taskID := TaskID(getTask(t, c), 0)
	// The rival that was cancelled in round 1 keeps the in-flight block it was
	// already fetching, so by round 2 it is one block ahead of the incumbent.
	for round := 2; round <= 4; round++ {
		pool.advance(incumbent, taskID, uint64(round), 3)
		pool.advance(dropped, taskID, uint64(round), 4)
		if err := reconcile(t, r); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if got := assignedNodes(getTask(t, c), 0); !reflect.DeepEqual(got, []string{incumbent}) {
			t.Fatalf("round %d assignment = %v, want [%s]: it is inside the retention line", round, got, incumbent)
		}
		if got := otherColumn(getTask(t, c).Status.ReadinessTable, incumbent, []string{"n1", "n2", "n3"}); got != 3 {
			t.Fatalf("round %d incumbent column = %d, want 3 (the table keeps following the incumbent)", round, got)
		}
	}
	if pool.submitCalls != 3 {
		t.Fatalf("submits = %d after four rounds, want 3 (broadcast only)", pool.submitCalls)
	}
	if pool.cancelCalls != 2 {
		t.Fatalf("cancels = %d after four rounds, want 2 (%v)", pool.cancelCalls, pool.cancelled)
	}
	table := getTask(t, c).Status.ReadinessTable
	if len(table) != 1 || len(table[0]) != 3 {
		t.Fatalf("readiness table = %v, want one row of three candidate columns", table)
	}
	if got := otherColumn(table, dropped, []string{"n1", "n2", "n3"}); got != 4 {
		t.Fatalf("cancelled candidate column = %d, want 4: it stays the best score, it just no longer decides the slot", got)
	}
}

// otherNode picks the first candidate that is not the given node.
func otherNode(excluded string, nodes []string) string {
	for _, node := range nodes {
		if node != excluded {
			return node
		}
	}
	return ""
}

// otherColumn reads the readiness column of the candidate that is not the given
// one; candidate order is Status.CandidateNodes, ascending by name.
func otherColumn(table [][]int32, node string, candidates []string) int32 {
	if len(table) == 0 {
		return -1
	}
	for i, candidate := range candidates {
		if candidate == node && i < len(table[0]) {
			return table[0][i]
		}
	}
	return -1
}

// TestResubmitAcceptsRestartedSeq is the D46 gate: a legitimate hand re-creates
// the task on the new node, and the Agent restarts its seq counter at 1. The
// old per-pair high-water mark used to discard every later report, so the gate
// kept reading the frozen value (31 of 128) while the node finished the window.
func TestResubmitAcceptsRestartedSeq(t *testing.T) {
	task := fixtureTask(1, 8, [][]int32{{0, 1, 2, 3, 4, 5, 6, 7}})
	task.Spec.PreheatFanout = 1
	task.Spec.PreheatFanoutMin = 1
	ds := fixtureDataSet(8, singleShardLayout(8), []string{"n1", "n2", "n3"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds,
		fixtureNode("n1"), fixtureNode("n2"), fixtureNode("n3"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureAgentPod("n3"),
		fixtureTargetPod(0))
	now := time.Unix(900, 0)
	r := &Reconciler{Client: c, Agents: pool, Now: func() time.Time { return now }}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	incumbent := assignedNodes(getTask(t, c), 0)[0]
	taskID := TaskID(getTask(t, c), 0)

	// The incumbent falls out of the retention line, so the hand is legitimate.
	pool.advance(incumbent, taskID, 2, 1)
	rival := otherNode(incumbent, []string{"n1", "n2", "n3"})
	pool.advance(rival, taskID, 9, 4)
	if err := reconcile(t, r); err != nil {
		t.Fatalf("round 2: %v", err)
	}
	assigned := assignedNodes(getTask(t, c), 0)
	if !reflect.DeepEqual(assigned, []string{rival}) {
		t.Fatalf("assignment = %v, want [%s] to move out of the band", assigned, rival)
	}
	if pool.submitCalls != 4 {
		t.Fatalf("submits = %d, want 4 (the hand re-submits the task)", pool.submitCalls)
	}
	// The re-submitted task restarts at seq 1 with the blocks it already holds,
	// then advances to seq 2 - below the seq 9 the old mark still remembers.
	pool.advance(rival, taskID, 1, 4)
	pool.advance(rival, taskID, 2, 5)

	if err := reconcile(t, r); err != nil {
		t.Fatalf("round 3: %v", err)
	}
	table := getTask(t, c).Status.ReadinessTable
	column := 0
	for i, node := range []string{"n1", "n2", "n3"} {
		if node == rival {
			column = i
		}
	}
	if len(table) != 1 || table[0][column] != 5 {
		t.Fatalf("readiness table = %v, want the new node's column %d at 5: its seq=2 report must not be dropped as stale", table, column)
	}
	if got := assignedNodes(getTask(t, c), 0); !reflect.DeepEqual(got, []string{rival}) {
		t.Fatalf("assignment = %v, want [%s] and no further hand", got, rival)
	}
	if pool.submitCalls != 4 || pool.cancelCalls != 3 {
		t.Fatalf("round 3: submits=%d cancels=%d, want 4 and 3", pool.submitCalls, pool.cancelCalls)
	}
}
