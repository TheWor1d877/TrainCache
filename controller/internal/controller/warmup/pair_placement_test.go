package warmup

// Fanout acceptance tests (docs/7.2 D38):
// per-Pod top-K placement, pair-level RPC diff, P x K status, existential
// gate, P x K score annotation, landing deviation and fanout=1 regression.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/gate"
	"traincache/controller/internal/gwaa"
	"traincache/controller/internal/metrics"
	"traincache/controller/internal/scorefmt"
)

// m13Seeds are the seeds that previously exposed the GWAA short-assignment
// branch (0, 3 and 7). They are used to assert the fixed fill-up behaviour
// instead of pinning a known-good seed.
var m13Seeds = []int64{0, 3, 7}

func fanoutTask(t *testing.T, podCount, k, fanout int32, orders [][]int32) *traincachev1alpha1.WarmupTask {
	t.Helper()
	task := fixtureTask(podCount, k, orders)
	task.Spec.PreheatFanout = fanout
	return task
}

func podEntries(task *traincachev1alpha1.WarmupTask, pod int32) []traincachev1alpha1.PodNodeReadiness {
	var out []traincachev1alpha1.PodNodeReadiness
	for _, entry := range task.Status.PodNodeReadiness {
		if entry.PodIndex == pod {
			out = append(out, entry)
		}
	}
	return out
}

// TestStatusHasOneEntryPerPodNodePair covers: status has
// exactly P x K entries, each Pod has K distinct preheat nodes and every
// entry's window count matches the ReadinessTable cell for its pair.
func TestStatusHasOneEntryPerPodNodePair(t *testing.T) {
	for _, seed := range m13Seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			statusPairsForSeed(t, seed)
		})
	}
}

func statusPairsForSeed(t *testing.T, seed int64) {
	t.Helper()
	task := fanoutTask(t, 2, 2, 2, [][]int32{{0, 1}, {2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2", "n3", "n4"})
	pool := newFakeProgressPool()
	for _, node := range []string{"n1", "n2", "n3", "n4"} {
		for _, pod := range []string{"train/t1/p0", "train/t1/p1"} {
			pool.set(pod, node, progressResponse{status: bitmapResponse(3, 1, 2, []byte{0b00000001})})
		}
	}
	c := fixtureClient(t, task, ds,
		fixtureNode("n1"), fixtureNode("n2"), fixtureNode("n3"), fixtureNode("n4"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureAgentPod("n3"), fixtureAgentPod("n4"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool, Rand: rand.New(rand.NewSource(seed))}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	// The adaptive target no longer pins the per-Pod count to fanout: the adaptive target can be
	// smaller, and identical distributions with allowMultiPodPerNode=false can
	// leave one Pod with a single out-of-target fallback. The invariant is one
	// entry per actual (Pod, node) pair within [min, max].
	if got := len(updated.Status.PodNodeReadiness); got < 2 || got > 4 {
		t.Fatalf("status entries = %d, want within [P*min, P*max] = [2,4]: %#v", got, updated.Status.PodNodeReadiness)
	}
	columns := map[string]int{}
	for i, node := range updated.Status.CandidateNodes {
		columns[node] = i
	}
	for pod := int32(0); pod < 2; pod++ {
		entries := podEntries(updated, pod)
		if len(entries) < 1 || len(entries) > 2 {
			t.Fatalf("pod %d entries = %d, want within [min,max] = [1,2]", pod, len(entries))
		}
		seen := map[string]struct{}{}
		for _, entry := range entries {
			if entry.NodeName == "" {
				t.Fatalf("pod %d has an empty NodeName: %#v", pod, entry)
			}
			if _, dup := seen[entry.NodeName]; dup {
				t.Fatalf("pod %d repeats node %q", pod, entry.NodeName)
			}
			seen[entry.NodeName] = struct{}{}
			if entry.RequiredReady != 1 {
				t.Fatalf("pod %d requiredReady = %d, want floor(0.8*2)=1", pod, entry.RequiredReady)
			}
			column, ok := columns[entry.NodeName]
			if !ok {
				t.Fatalf("entry node %q is not a candidate", entry.NodeName)
			}
			if want := updated.Status.ReadinessTable[pod][column]; entry.ReadyInWindow != want {
				t.Fatalf("pod %d node %s ready = %d, want table %d",
					pod, entry.NodeName, entry.ReadyInWindow, want)
			}
		}
	}
}

// TestGateReleasesWhenAnyWarmNodeReachesThreshold covers: one
// ready preheat node is enough for the Pod; all nodes below threshold hold.
func TestGateReleasesWhenAnyWarmNodeReachesThreshold(t *testing.T) {
	now := time.Unix(100, 0)
	r := &Reconciler{Now: func() time.Time { return now }}

	build := func(ready1, ready2 int32) *traincachev1alpha1.WarmupTask {
		task := fanoutTask(t, 1, 2, 2, [][]int32{{0, 1}})
		task.Spec.Gate.Threshold = 0.8
		task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
			{PodIndex: 0, NodeName: "n1", ReadyInWindow: ready1},
			{PodIndex: 0, NodeName: "n2", ReadyInWindow: ready2},
		}
		return task
	}

	released := r.evaluateGate(build(2, 0))
	if released.Decision.State != gate.Released || released.Decision.Reason != gate.ReasonAllPodsReady {
		t.Fatalf("decision = %s/%s, want Released/all_pods_ready",
			released.Decision.State, released.Decision.Reason)
	}
	if len(released.Pods) != 1 || len(released.Pods[0].AssignedNodes) != 2 {
		t.Fatalf("assigned nodes = %#v, want both warm nodes", released.Pods)
	}
	if hits := gateHitNodes(released); hits != "p0=n1" {
		t.Fatalf("hit nodes = %q, want p0=n1", hits)
	}

	held := r.evaluateGate(build(0, 0))
	if held.Decision.State != gate.Hold {
		t.Fatalf("decision = %s, want Hold when no warm node reaches required", held.Decision.State)
	}
}

// TestScoreAnnotationCoversEveryPair covers criteria ④: each Pod's
// annotation lists exactly its own assigned nodes, and the union is P x K.
func TestScoreAnnotationCoversEveryPair(t *testing.T) {
	task := fanoutTask(t, 2, 2, 2, [][]int32{{0, 1}, {2, 3}})
	task.Spec.Score = traincachev1alpha1.ScorePolicy{HalfLife: 1, Cutoff: 2}
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{PodIndex: 0, NodeName: "n1", ReadyBlockBitmap: fullBitmap(2)},
		{PodIndex: 0, NodeName: "n2", ReadyBlockBitmap: fullBitmap(2)},
		{PodIndex: 1, NodeName: "n2", ReadyBlockBitmap: fullBitmap(2)},
		{PodIndex: 1, NodeName: "n3", ReadyBlockBitmap: fullBitmap(2)},
	}
	windows := [][]*agentpb.ChunkRef{testChunks("a", "b"), testChunks("a", "b")}
	c := scoreClient(t, gatedPod(0), gatedPod(1))
	r := &Reconciler{Client: c, Now: func() time.Time { return scoreTestNow }}
	podNames := map[int32]string{0: "trainer-0", 1: "trainer-1"}

	removed, skipped, err := r.removeGates(context.Background(),
		slog.New(slog.NewTextHandler(io.Discard, nil)), task, windows, podNames)
	if err != nil || removed != 2 || skipped != 0 {
		t.Fatalf("removeGates() = (%d, %d, %v), want (2, 0, nil)", removed, skipped, err)
	}

	want := map[int32][]string{0: {"n1", "n2"}, 1: {"n2", "n3"}}
	pairs := 0
	for index, nodes := range want {
		value := readPodScore(t, c, client.ObjectKey{Namespace: task.Namespace, Name: podNames[index]}, scorefmt.DefaultAnnotation)
		if value.Task != task.Namespace+"/"+task.Name || value.G != task.Generation {
			t.Fatalf("pod %d task/generation = %q/%d, want %s/%d",
				index, value.Task, value.G, task.Namespace+"/"+task.Name, task.Generation)
		}
		if len(value.S) != len(nodes) {
			t.Fatalf("pod %d annotation S = %v, want nodes %v", index, value.S, nodes)
		}
		for _, node := range nodes {
			if _, ok := value.S[node]; !ok {
				t.Fatalf("pod %d annotation missing node %s: %v", index, node, value.S)
			}
			pairs++
		}
	}
	if pairs != 4 {
		t.Fatalf("annotation pairs = %d, want P x K = 4", pairs)
	}
}

// TestPairDiffSubmitsUpdatesCancels covers: only changed pairs
// issue RPCs; removed pairs are cancelled and unchanged pairs are untouched.
func TestPairDiffSubmitsUpdatesCancels(t *testing.T) {
	task := testTask("task", traincachev1alpha1.TargetRef{})
	task.Generation = 7
	in := PlacementApplyInput{
		Task:    task,
		DataSet: testPlacementDataSet(),
		Windows: [][]*agentpb.ChunkRef{
			testChunks("a"),
			testChunks("b"),
			testChunks("c"),
		},
		Previous: map[PreheatPair]ExistingPreheat{
			// same pair, window shrank past the reported bitmap -> Update
			{PodIndex: 0, Node: "n0"}: {Generation: 7, WindowBlocks: 2},
			{PodIndex: 0, Node: "n1"}: {Generation: 7, WindowBlocks: 1}, // removed -> Cancel
			{PodIndex: 2, Node: "n0"}: {Generation: 7, WindowBlocks: 1}, // removed -> Cancel
		},
		Placement: gwaa.Output{
			Assignment: map[int32][]string{
				0: {"n0", "n2"},
				1: {"n0"},
			},
			PreheatSet: map[string][]int32{
				"n0": {0, 1},
				"n2": {0},
			},
			UnassignedPods: []int32{2},
		},
		Fanout:   2,
		PodNames: map[int32]string{0: "p-0", 1: "p-1", 2: "p-2"},
	}

	rpc := &fakePlacementRPC{}
	result, err := ApplyPlacement(context.Background(), rpc, nil, in)
	if err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	want := []placementCall{
		{kind: ActionUpdate, node: "n0", taskID: "train/task/p0"},
		{kind: ActionSubmit, node: "n2", taskID: "train/task/p0"},
		{kind: ActionSubmit, node: "n0", taskID: "train/task/p1"},
		{kind: ActionCancel, node: "n1", taskID: "train/task/p0"},
		{kind: ActionCancel, node: "n0", taskID: "train/task/p2"},
	}
	calls := rpc.snapshot()
	if len(calls) != len(want) {
		t.Fatalf("RPC calls = %#v, want %#v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call[%d] = %#v, want %#v", i, calls[i], want[i])
		}
	}
	if result.Submitted != 2 || result.Updated != 1 || result.Cancelled != 2 {
		t.Fatalf("counts submit=%d update=%d cancel=%d", result.Submitted, result.Updated, result.Cancelled)
	}
}

type m13DeviationPool struct {
	submitCalls int
	cancelCalls int
}

func (p *m13DeviationPool) Submit(context.Context, string, *agentpb.SubmitPreheatRequest) error {
	p.submitCalls++
	return nil
}

func (p *m13DeviationPool) Update(context.Context, string, *agentpb.UpdatePreheatRequest) error {
	return nil
}

func (p *m13DeviationPool) Cancel(context.Context, string, *agentpb.CancelPreheatRequest) error {
	p.cancelCalls++
	return nil
}

func (p *m13DeviationPool) Progress(context.Context, string, string, uint64) (*agentpb.PreheatStatus, error) {
	return nil, nil
}

// TestDeviationInsideWarmSetOnlyBackfills covers criterion ⑦ (inside): a
// landing on one of the Pod's warm nodes only records ActualNode.
func TestDeviationInsideWarmSetOnlyBackfills(t *testing.T) {
	task := fanoutTask(t, 1, 1, 2, [][]int32{{0}})
	task.Status.Phase = traincachev1alpha1.WarmupPhaseReleased
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{PodIndex: 0, NodeName: "n1"},
		{PodIndex: 0, NodeName: "n2"},
	}
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1", "n2"})
	pod := fixtureTargetPod(0)
	pod.Spec.NodeName = "n2"
	pool := &m13DeviationPool{}
	recorder := record.NewFakeRecorder(4)
	c := fixtureClient(t, task, ds, pod)
	r := &Reconciler{Client: c, Agents: pool, Recorder: recorder, Now: func() time.Time { return time.Unix(100, 0) }}

	r.reconcileDeviation(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), task)

	if pool.submitCalls != 0 || pool.cancelCalls != 0 {
		t.Fatalf("in-set landing issued RPCs: submit=%d cancel=%d", pool.submitCalls, pool.cancelCalls)
	}
	for _, entry := range task.Status.PodNodeReadiness {
		if entry.ActualNode != "n2" {
			t.Fatalf("entry %+v actualNode = %q, want n2", entry, entry.ActualNode)
		}
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected event for in-set landing: %q", event)
	default:
	}
}

// TestDeviationOutsideWarmSetSubmitsActualNode covers criterion ⑦
// (outside): record NodeDeviation, submit the actual node and never cancel the
// original warm nodes.
func TestDeviationOutsideWarmSetSubmitsActualNode(t *testing.T) {
	task := fanoutTask(t, 1, 1, 2, [][]int32{{0}})
	task.Status.Phase = traincachev1alpha1.WarmupPhaseReleased
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{PodIndex: 0, NodeName: "n1"},
		{PodIndex: 0, NodeName: "n2"},
	}
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1", "n2", "n3"})
	pod := fixtureTargetPod(0)
	pod.Spec.NodeName = "n3"
	pool := &m13DeviationPool{}
	recorder := record.NewFakeRecorder(4)
	c := fixtureClient(t, task, ds, pod)
	r := &Reconciler{Client: c, Agents: pool, Recorder: recorder, Now: func() time.Time { return time.Unix(100, 0) }}

	r.reconcileDeviation(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), task)

	if pool.submitCalls != 1 {
		t.Fatalf("submit calls = %d, want 1 on the actual node", pool.submitCalls)
	}
	if pool.cancelCalls != 0 {
		t.Fatalf("cancel calls = %d, want 0 (warm nodes stay)", pool.cancelCalls)
	}
	for _, entry := range task.Status.PodNodeReadiness {
		if entry.ActualNode != "n3" {
			t.Fatalf("entry %+v actualNode = %q, want n3", entry, entry.ActualNode)
		}
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, EventNodeDeviation) {
			t.Fatalf("event = %q, want %s", event, EventNodeDeviation)
		}
	default:
		t.Fatalf("no %s event recorded", EventNodeDeviation)
	}
}

// TestFanoutOneKeepsSingleEntry covers: fanout=1 keeps one status
// entry per Pod and one annotation pair per Pod.
func TestFanoutOneKeepsSingleEntry(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{{0, 1}, {0, 1}})
	task.Spec.PreheatFanout = 1
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2"})
	pool := newFakeProgressPool()
	for _, node := range []string{"n1", "n2"} {
		for _, pod := range []string{"train/t1/p0", "train/t1/p1"} {
			pool.set(pod, node, doneResponse(3, fullBitmap(2)))
		}
	}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), gatedPod(0), gatedPod(1))
	r := &Reconciler{Client: c, Agents: pool, Now: func() time.Time { return time.Unix(100, 0) }}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want released", result.RequeueAfter)
	}
	updated := readyTask(t, c)
	if updated.Status.Phase != traincachev1alpha1.WarmupPhaseReleased {
		t.Fatalf("phase = %q, want Released", updated.Status.Phase)
	}
	if got := len(updated.Status.PodNodeReadiness); got != 2 {
		t.Fatalf("status entries = %d, want one per Pod (2)", got)
	}
	pairs := 0
	for index, name := range []string{"trainer-0", "trainer-1"} {
		value := readPodScore(t, c, client.ObjectKey{Namespace: "train", Name: name}, scorefmt.DefaultAnnotation)
		if len(value.S) != 1 {
			t.Fatalf("pod %d annotation S = %v, want fanout=1 (one node)", index, value.S)
		}
		pairs += len(value.S)
	}
	if pairs != 2 {
		t.Fatalf("fanout=1 annotation pairs = %d, want P x K = 2", pairs)
	}
}

// TestLogFields asserts the D21 log points: fanout, per-Pod warm-node
// counts, top-K assignment and status entry count.
func TestLogFields(t *testing.T) {
	for _, seed := range m13Seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			logFieldsForSeed(t, seed)
		})
	}
}

func logFieldsForSeed(t *testing.T, seed int64) {
	t.Helper()
	task := fanoutTask(t, 2, 2, 2, [][]int32{{0, 1}, {2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2", "n3", "n4"})
	pool := newFakeProgressPool()
	var buf bytes.Buffer
	log := m12JSONLogInto(&buf)
	c := fixtureClient(t, task, ds,
		fixtureNode("n1"), fixtureNode("n2"), fixtureNode("n3"), fixtureNode("n4"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureAgentPod("n3"), fixtureAgentPod("n4"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool, Log: log, PollInterval: time.Second,
		Rand: rand.New(rand.NewSource(seed))}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	wantCounts := statusWarmNodeCounts(updated)
	wantEntries := float64(len(updated.Status.PodNodeReadiness))

	applied := m12FindLog(t, &buf, "gwaa placement applied")
	for _, field := range []string{"fanout", "assign", "warm_node_count", "status_entries"} {
		if _, ok := applied[field]; !ok {
			t.Fatalf("placement log missing %q: %#v", field, applied)
		}
	}
	if applied["fanout"] != float64(2) || applied["status_entries"] != wantEntries {
		t.Fatalf("placement log fanout/status_entries = %v/%v, want 2/%.0f",
			applied["fanout"], applied["status_entries"], wantEntries)
	}
	if counts, _ := applied["warm_node_count"].(string); counts != wantCounts {
		t.Fatalf("warm_node_count = %q, want %q", counts, wantCounts)
	}
	// assign 是一 Pod 一个键的结构化对象,节点列表与 status 里的配对一致。
	assign := m12AssignNodes(t, applied)
	if len(assign) != int(task.Spec.PodCount) {
		t.Fatalf("assign = %v, want %d Pod entries", assign, task.Spec.PodCount)
	}
	for pod := int32(0); pod < task.Spec.PodCount; pod++ {
		entries := podEntries(updated, pod)
		if got := assign[fmt.Sprintf("p%d", pod)]; len(got) != len(entries) {
			t.Fatalf("assign[p%d] = %v, want %d nodes", pod, got, len(entries))
		}
		want := make(map[string]struct{}, len(entries))
		for _, entry := range entries {
			want[entry.NodeName] = struct{}{}
		}
		for _, node := range assign[fmt.Sprintf("p%d", pod)] {
			if _, ok := want[node]; !ok {
				t.Fatalf("assign[p%d] holds %q, not in status %v", pod, node, entries)
			}
		}
	}

	evaluated := m12FindLog(t, &buf, "gate evaluated")
	for _, field := range []string{"fanout", "warm_node_count", "status_entries", "hit_nodes"} {
		if _, ok := evaluated[field]; !ok {
			t.Fatalf("gate log missing %q: %#v", field, evaluated)
		}
	}
	if evaluated["fanout"] != float64(2) || evaluated["status_entries"] != wantEntries {
		t.Fatalf("gate log fanout/status_entries = %v/%v, want 2/%.0f",
			evaluated["fanout"], evaluated["status_entries"], wantEntries)
	}
	if counts, _ := evaluated["warm_node_count"].(string); counts != wantCounts {
		t.Fatalf("gate warm_node_count = %q, want %q", counts, wantCounts)
	}
}

// statusWarmNodeCounts renders the actual per-Pod preheat node counts from the
// persisted status, in the same format the controller logs.
func statusWarmNodeCounts(task *traincachev1alpha1.WarmupTask) string {
	parts := make([]string, 0, task.Spec.PodCount)
	for pod := int32(0); pod < task.Spec.PodCount; pod++ {
		parts = append(parts, fmt.Sprintf("p%d=%d", pod, len(podEntries(task, pod))))
	}
	return strings.Join(parts, ",")
}

// m12JSONLogInto returns the shared JSON test logger writing into buf.
func m12JSONLogInto(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestPodsPerNodeCountsPairs covers: the metric observes one
// sample per node of the pair set, so its sum is P x K.
func TestPodsPerNodeCountsPairs(t *testing.T) {
	for _, seed := range m13Seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			podsPerNodePairsForSeed(t, seed)
		})
	}
}

func podsPerNodePairsForSeed(t *testing.T, seed int64) {
	t.Helper()
	m := metrics.New()
	r := &Reconciler{Metrics: m, Now: func() time.Time { return time.Unix(100, 0) },
		Rand: rand.New(rand.NewSource(seed))}
	task := fanoutTask(t, 2, 1, 2, [][]int32{{0}, {1}})

	placement := r.computePlacement(task, []string{"n1", "n2", "n3", "n4"}, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}
	pairs := 0
	for _, pods := range placement.PreheatSet {
		pairs += len(pods)
	}
	actual := 0
	for pod := int32(0); pod < 2; pod++ {
		got := len(placement.Assignment[pod])
		// The adaptive target chooses the actual count: 1 represents the single
		// out-of-target fallback when both Pods share a target set.
		if got < 1 || got > 2 {
			t.Fatalf("pod %d warm nodes = %d, want within [min,max] = [1,2]: %#v",
				pod, got, placement.Assignment)
		}
		actual += got
	}
	if pairs != actual {
		t.Fatalf("preheat pairs = %d, want actual assignment pairs = %d: %#v",
			pairs, actual, placement.Assignment)
	}
	if got := emitHistogramCount(t, m, "traincache_controller_pods_per_node", nil); got != uint64(len(placement.PreheatSet)) {
		t.Fatalf("pods_per_node observations = %d, want %d", got, len(placement.PreheatSet))
	}
	if got := emitHistogramSum(t, m, "traincache_controller_pods_per_node", nil); got != float64(pairs) {
		t.Fatalf("pods_per_node sum = %v, want pair count %d", got, pairs)
	}
}
