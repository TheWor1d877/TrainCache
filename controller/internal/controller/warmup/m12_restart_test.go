package warmup

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// m12RestartPool records the differential RPCs issued after a restart. Progress
// returns an empty payload, so the status written before the restart must be
// carried over unchanged.
type m12RestartPool struct {
	mu            sync.Mutex
	submitCalls   int
	updateCalls   int
	cancelCalls   int
	progressCalls int
}

func (p *m12RestartPool) Submit(context.Context, string, *agentpb.SubmitPreheatRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submitCalls++
	return nil
}

func (p *m12RestartPool) Update(context.Context, string, *agentpb.UpdatePreheatRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updateCalls++
	return nil
}

func (p *m12RestartPool) Cancel(context.Context, string, *agentpb.CancelPreheatRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelCalls++
	return nil
}

func (p *m12RestartPool) Progress(context.Context, string, string, uint64) (*agentpb.PreheatStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.progressCalls++
	return nil, nil
}

// TestM12RestartRebuildsStateFromStatus simulates a controller process restart:
// a brand-new Reconciler (empty in-memory tracker) reconciles a WarmupTask that
// already carries a P×N table and per-Pod GWAA assignments. It must continue
// from the CRD status, not fan out Submit again, and must preserve the
// assigned-node readiness facts.
func TestM12RestartRebuildsStateFromStatus(t *testing.T) {
	task := fixtureTask(2, 4, [][]int32{{0, 1, 2, 3}, {4, 5, 6, 7}})
	task.Generation = 3
	started := metav1.NewTime(time.Unix(100, 0))
	task.Status = traincachev1alpha1.WarmupTaskStatus{
		ObservedGeneration: 2,
		Phase:              traincachev1alpha1.WarmupPhasePreheating,
		ConsumeOrderSource: traincachev1alpha1.ConsumeOrderSourceSpec,
		StartedAt:          &started,
		CandidateNodes:     []string{"n1", "n2"},
		ReadinessTable:     [][]int32{{2, 1}, {1, 2}},
		PodNodeReadiness: []traincachev1alpha1.PodNodeReadiness{
			{PodIndex: 0, PodName: "trainer-0", NodeName: "n1", ReadyInWindow: 2, RequiredReady: 3, State: traincachev1alpha1.PreheatStatePreheating},
			{PodIndex: 1, PodName: "trainer-1", NodeName: "n2", ReadyInWindow: 2, RequiredReady: 3, State: traincachev1alpha1.PreheatStatePreheating},
		},
	}

	ds := fixtureDataSet(8, singleShardLayout(8), []string{"n1", "n2"})
	pool := &m12RestartPool{}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{
		Client: c,
		Agents: pool,
		Now:    func() time.Time { return time.Unix(200, 0) },
	}

	// A restarted process has no rate history.
	if got := r.progress.samples("uid-1", 0); len(got) != 0 {
		t.Fatalf("fresh Reconciler history = %v, want empty", got)
	}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile after restart: %v", err)
	}

	pool.mu.Lock()
	submitCalls, updateCalls, cancelCalls, progressCalls := pool.submitCalls, pool.updateCalls, pool.cancelCalls, pool.progressCalls
	pool.mu.Unlock()

	if submitCalls != 0 {
		t.Fatalf("Submit calls = %d after restart, want 0 (status already has an assignment)", submitCalls)
	}
	if updateCalls != 2 {
		t.Fatalf("Update calls = %d, want 2 for the two assigned pairs", updateCalls)
	}
	if cancelCalls != 0 {
		t.Fatalf("Cancel calls = %d, want 0 (assignment unchanged)", cancelCalls)
	}
	if progressCalls == 0 {
		t.Fatal("Progress was not polled after restart")
	}
	if result.RequeueAfter == 0 {
		t.Fatal("gate cycle stopped without release; want requeue while Hold")
	}

	updated := getTask(t, c)
	if updated.Status.ObservedGeneration != task.Generation {
		t.Fatalf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, task.Generation)
	}
	if !reflect.DeepEqual(updated.Status.ReadinessTable, task.Status.ReadinessTable) {
		t.Fatalf("readinessTable after restart = %v, want %v", updated.Status.ReadinessTable, task.Status.ReadinessTable)
	}
	if len(updated.Status.PodNodeReadiness) != len(task.Status.PodNodeReadiness) {
		t.Fatalf("podNodeReadiness entries = %d, want %d", len(updated.Status.PodNodeReadiness), len(task.Status.PodNodeReadiness))
	}
	for i, before := range task.Status.PodNodeReadiness {
		after := updated.Status.PodNodeReadiness[i]
		if after.PodIndex != before.PodIndex ||
			after.NodeName != before.NodeName || after.ReadyInWindow != before.ReadyInWindow {
			t.Fatalf("entry[%d] after restart = %#v, want facts from %#v", i, after, before)
		}
	}
	if updated.Status.StartedAt == nil || !updated.Status.StartedAt.Equal(&started) {
		t.Fatalf("startedAt after restart = %v, want %v", updated.Status.StartedAt, started)
	}
	if got := r.progress.samples("uid-1", 0); len(got) != 0 {
		t.Fatalf("rate history after restart = %v, want empty (memory is not restored)", got)
	}
}
