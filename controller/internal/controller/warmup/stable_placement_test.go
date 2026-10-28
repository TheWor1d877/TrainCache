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
		{podIndex: 0, node: "n1"}: {Held: true, Generation: 7, BitmapBytes: 1},
		// Cancel marks a task without deleting it; the Agent still answers, but
		// it is not holding anything (docs/6.3 D55).
		{podIndex: 0, node: "n2"}: {Generation: 7, BitmapBytes: 1},
		// n3 answered nothing, so it is absent from the map.
	}

	ledger := submittedLedger(task, windows, candidates, observations, false, newSubmitResult())
	if len(ledger) != 1 {
		t.Fatalf("ledger = %#v, want only the held pair", ledger)
	}
	if got := ledger[PreheatPair{PodIndex: 0, Node: "n1"}]; got != (ExistingPreheat{Generation: 7, BitmapBytes: 1}) {
		t.Fatalf("held pair = %#v, want generation 7 and one bitmap byte", got)
	}
	if !anyTaskHeld(observations) {
		t.Fatal("anyTaskHeld = false, want true: the wide fan-out already ran")
	}
	if anyTaskHeld(map[readinessKey]AgentTaskObservation{{podIndex: 0, node: "n2"}: {Generation: 7, BitmapBytes: 1}}) {
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
