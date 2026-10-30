package warmup

// D44 regression coverage: the status patch that commits a gate decision
// re-enqueues the task, and the duplicate pass can start from an informer copy
// that still shows the pre-release status. isTerminal cannot see that, so the
// in-process release memo must keep the pass from re-drawing the assignment,
// cancelling the submits the release just made and rewriting the score
// annotations.

import (
	"context"
	"reflect"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// staleTaskClient serves one fixed WarmupTask copy on Get, the way a cache that
// has not observed the release patch does, and counts writes to Nodes and Pods.
type staleTaskClient struct {
	client.Client
	stale        *traincachev1alpha1.WarmupTask
	objectWrites int
}

func (c *staleTaskClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if task, ok := obj.(*traincachev1alpha1.WarmupTask); ok {
		c.stale.DeepCopyInto(task)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *staleTaskClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	switch obj.(type) {
	case *corev1.Node, *corev1.Pod:
		c.objectWrites++
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *staleTaskClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	switch obj.(type) {
	case *corev1.Node, *corev1.Pod:
		c.objectWrites++
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// rpcCountingPool adds the Cancel and Update calls fakeAgentPool swallows, so a
// pass that issues no Submit is still measurable.
type rpcCountingPool struct {
	*fakeProgressPool
	mu         sync.Mutex
	cancels    int
	updates    int
	progresses int
}

func (p *rpcCountingPool) Cancel(ctx context.Context, node string, req *agentpb.CancelPreheatRequest) error {
	p.mu.Lock()
	p.cancels++
	p.mu.Unlock()
	return p.fakeProgressPool.Cancel(ctx, node, req)
}

func (p *rpcCountingPool) Update(ctx context.Context, node string, req *agentpb.UpdatePreheatRequest) error {
	p.mu.Lock()
	p.updates++
	p.mu.Unlock()
	return p.fakeProgressPool.Update(ctx, node, req)
}

func (p *rpcCountingPool) Progress(ctx context.Context, node, taskID string, generation uint64) (*agentpb.PreheatStatus, error) {
	p.mu.Lock()
	p.progresses++
	p.mu.Unlock()
	return p.fakeProgressPool.Progress(ctx, node, taskID, generation)
}

func (p *rpcCountingPool) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot()) + p.cancels + p.updates + p.progresses
}

// scriptReleaseProgress makes every (Pod, Node) pair report a full bitmap, so a
// pass that reaches placement and polling commits its own gate decision.
func scriptReleaseProgress(pool *fakeProgressPool) {
	for _, node := range []string{"n1", "n2"} {
		for _, taskID := range []string{"train/t1/p0", "train/t1/p1"} {
			pool.set(taskID, node, doneResponse(3, fullBitmap(2)))
		}
	}
}

// newCountingPool returns a scripted agent pool that can prove a pass actually
// ran: without progress it would only ever Hold and write no annotation.
func newCountingPool() *rpcCountingPool {
	pool := newFakeProgressPool()
	scriptReleaseProgress(pool)
	return &rpcCountingPool{fakeProgressPool: pool}
}

// readinessPairs maps each Pod to its preheat Node, ignoring the summary rows
// written for Pods that claimed no Node.
func readinessPairs(task *traincachev1alpha1.WarmupTask) map[int32]string {
	out := make(map[int32]string, len(task.Status.PodNodeReadiness))
	for _, entry := range task.Status.PodNodeReadiness {
		if entry.NodeName != "" {
			out[entry.PodIndex] = entry.NodeName
		}
	}
	return out
}

// releaseFixture drives one task to Released and returns its client, the
// reconciler that got there and a pristine pre-release copy of the task.
func releaseFixture(t *testing.T) (client.Client, *Reconciler, func() *traincachev1alpha1.WarmupTask) {
	t.Helper()
	task := fixtureTask(2, 2, [][]int32{{0, 1}, {2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2"})
	pool := newFakeProgressPool()
	scriptReleaseProgress(pool)
	c := fixtureClient(t, task, ds,
		fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		gatedPod(0), gatedPod(1))
	r := &Reconciler{Client: c, Agents: pool}
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("release Reconcile: %v", err)
	}
	released := getTask(t, c)
	if released.Status.Phase != traincachev1alpha1.WarmupPhaseReleased {
		t.Fatalf("setup did not release: phase=%q", released.Status.Phase)
	}
	if got := readinessPairs(released); len(got) != 2 {
		t.Fatalf("released pairs = %v, want one per Pod", got)
	}
	// A cached copy from before the release patch: same generation, no status.
	preRelease := func() *traincachev1alpha1.WarmupTask {
		return fixtureTask(2, 2, [][]int32{{0, 1}, {2, 3}})
	}
	return c, r, preRelease
}

func TestTerminalFreezeBlocksStaleDuplicatePass(t *testing.T) {
	c, r, preRelease := releaseFixture(t)
	before := readinessPairs(getTask(t, c))

	pool := newCountingPool()
	stale := &staleTaskClient{Client: c, stale: preRelease()}
	r.Client = stale
	r.Agents = pool
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("frozen Reconcile: %v", err)
	}
	if got := pool.calls(); got != 0 {
		t.Fatalf("duplicate pass issued %d placement RPCs, want 0", got)
	}
	if stale.objectWrites != 0 {
		t.Fatalf("duplicate pass wrote %d Node/Pod objects, want 0 score annotations", stale.objectWrites)
	}
	after := getTask(t, c)
	if got := readinessPairs(after); !reflect.DeepEqual(before, got) {
		t.Fatalf("release pairs rewritten by the duplicate pass: before=%v after=%v", before, got)
	}
	if after.Status.Phase != traincachev1alpha1.WarmupPhaseReleased {
		t.Fatalf("phase after the duplicate pass = %q, want Released", after.Status.Phase)
	}
}

// The premise of the freeze test: a pass that reads the pre-release copy does
// re-run the whole placement pipeline when this process has not recorded the
// gate decision. Without that, the assertions above would pass for an
// unreachable fixture.
func TestStaleDuplicatePassRedrawsWithoutReleaseMemo(t *testing.T) {
	c, _, preRelease := releaseFixture(t)

	stale := &staleTaskClient{Client: c, stale: preRelease()}
	pool := newCountingPool()
	// A separate Reconciler owns a fresh pollTracker, so no generation is
	// remembered as released and isTerminal is the only guard left.
	r := &Reconciler{Client: stale, Agents: pool}
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("duplicate Reconcile: %v", err)
	}
	if pool.calls() == 0 {
		t.Fatal("stale duplicate pass issued no RPC; the fixture never reaches placement")
	}
	// The Pods are already ungated, so the re-drawn pass has nothing to patch:
	// D49 freezes each Pod's annotation once its gate is removed.
	if stale.objectWrites != 0 {
		t.Fatalf("stale duplicate pass rewrote %d Node/Pod objects, want 0", stale.objectWrites)
	}
}
