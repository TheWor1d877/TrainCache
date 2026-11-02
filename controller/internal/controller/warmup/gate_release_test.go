package warmup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/scorefmt"
)

func gatedPod(index int32) *corev1.Pod {
	pod := fixtureTargetPod(index)
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: DefaultGateName}}
	return pod
}

func hasWarmupGate(pod *corev1.Pod) bool {
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == DefaultGateName {
			return true
		}
	}
	return false
}

func fullBitmap(chunks int) []byte {
	out := make([]byte, (chunks+7)/8)
	for i := 0; i < chunks; i++ {
		out[i/8] |= 1 << (uint(i) % 8)
	}
	return out
}

func doneResponse(generation uint64, bitmap []byte) progressResponse {
	status := &agentpb.PreheatStatus{
		Generation:  generation,
		Seq:         1,
		ReadyBitmap: bitmap,
		State:       agentpb.PreheatStatus_DONE,
	}
	return progressResponse{status: status}
}

// TestReleasePodPatchFailureKeepsGateAndAnnotation asserts the per-Pod atomic
// patch: when the Pod patch fails, the gate stays and no score annotation is
// written, so the Scheduler never sees half a release (D49).
func TestReleasePodPatchFailureKeepsGateAndAnnotation(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", doneResponse(3, fullBitmap(2)))
	base := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), gatedPod(0))
	c := &podPatchFailClient{Client: base, pod: "trainer-0"}
	c.remaining.Store(100)
	r := &Reconciler{Client: c, Agents: pool}

	if _, err := reconcileResult(t, r); err == nil {
		t.Fatal("Reconcile succeeded, want the Pod patch failure")
	}

	pod := &corev1.Pod{}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-0"}, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if !hasWarmupGate(pod) {
		t.Fatal("gate was removed although the Pod patch failed")
	}
	if _, ok := pod.Annotations[scorefmt.DefaultAnnotation]; ok {
		t.Fatalf("score annotation %q written although the Pod patch failed", scorefmt.DefaultAnnotation)
	}
	updated := getTask(t, base)
	if updated.Status.Phase == traincachev1alpha1.WarmupPhaseReleased {
		t.Fatal("task was released although the Pod patch failed")
	}
	if updated.Status.GateDecision.State != "" {
		t.Fatalf("gate decision = %q, want empty", updated.Status.GateDecision.State)
	}
}

// podPatchFailClient fails the first Patch of one Pod.
type podPatchFailClient struct {
	client.Client
	pod       string
	remaining atomic.Int32
	patches   atomic.Int32
}

func (c *podPatchFailClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if p, ok := obj.(*corev1.Pod); ok && p.Name == c.pod {
		c.patches.Add(1)
		if c.remaining.Add(-1) >= 0 {
			return errors.New("patch failed")
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestReleaseGateRemovalIsIdempotentAfterPartialFailure(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{{0, 1}, {2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2"})
	pool := newFakeProgressPool()
	for _, node := range []string{"n1", "n2"} {
		for _, pod := range []string{"train/t1/p0", "train/t1/p1"} {
			pool.set(pod, node, doneResponse(3, fullBitmap(2)))
		}
	}
	base := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), gatedPod(0), gatedPod(1))
	c := &podPatchFailClient{Client: base, pod: "trainer-0"}
	c.remaining.Store(1)
	r := &Reconciler{Client: c, Agents: pool}

	if _, err := reconcileResult(t, r); err == nil {
		t.Fatal("first Reconcile succeeded, want partial removal error")
	}
	if phase := getTask(t, base).Status.Phase; phase == traincachev1alpha1.WarmupPhaseReleased {
		t.Fatalf("phase = %q after partial failure, want not Released", phase)
	}
	pod0 := &corev1.Pod{}
	pod1 := &corev1.Pod{}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-0"}, pod0); err != nil {
		t.Fatalf("get pod 0: %v", err)
	}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-1"}, pod1); err != nil {
		t.Fatalf("get pod 1: %v", err)
	}
	if !hasWarmupGate(pod0) {
		t.Fatal("failed Pod gate was removed")
	}
	if hasWarmupGate(pod1) {
		t.Fatal("successful Pod gate was not removed")
	}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("retry Reconcile: %v", err)
	}
	updated := getTask(t, base)
	if updated.Status.Phase != traincachev1alpha1.WarmupPhaseReleased {
		t.Fatalf("phase = %q, want Released after retry", updated.Status.Phase)
	}
	for _, name := range []string{"trainer-0", "trainer-1"} {
		pod := &corev1.Pod{}
		if err := base.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: name}, pod); err != nil {
			t.Fatalf("get pod %s: %v", name, err)
		}
		if hasWarmupGate(pod) {
			t.Fatalf("pod %s still has the warmup gate", name)
		}
	}
	if updated.Status.GateDecision.State != traincachev1alpha1.GateStateReleased {
		t.Fatalf("gate decision = %q, want Released", updated.Status.GateDecision.State)
	}
}

func TestReleaseTMaxDegradesAndRequeuesNothing(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Spec.Gate.TMax = metav1.Duration{Duration: time.Minute}
	started := metav1.NewTime(time.Unix(100, 0).Add(-2 * time.Minute))
	task.Status.StartedAt = &started
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool() // no progress: readiness stays zero
	base := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), gatedPod(0))
	r := &Reconciler{Client: base, Agents: pool, Now: func() time.Time { return time.Unix(100, 0) }}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0 after Degraded", result.RequeueAfter)
	}
	updated := getTask(t, base)
	if updated.Status.Phase != traincachev1alpha1.WarmupPhaseDegraded {
		t.Fatalf("phase = %q, want Degraded", updated.Status.Phase)
	}
	if updated.Status.GateDecision.State != traincachev1alpha1.GateStateDegraded {
		t.Fatalf("gate decision = %q, want Degraded", updated.Status.GateDecision.State)
	}
	pod := &corev1.Pod{}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-0"}, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if hasWarmupGate(pod) {
		t.Fatal("degraded release did not remove the gate")
	}
}

func TestGateCycleRequeuesAfterTerminalPoll(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Spec.Gate.Threshold = 1
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	// Agent reports Ready (terminal) but only one block is ready, below K.
	pool.set("train/t1/p0", "n1", doneResponse(3, []byte{0b01}))
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	base := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), gatedPod(0))
	r := &Reconciler{Client: base, Agents: pool, Log: log, PollInterval: 2 * time.Second}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("RequeueAfter = %v, want the independent gate interval 2s", result.RequeueAfter)
	}
	updated := getTask(t, base)
	if updated.Status.Phase != traincachev1alpha1.WarmupPhaseGating {
		t.Fatalf("phase = %q, want Gating", updated.Status.Phase)
	}
	if updated.Status.GateDecision.State != "" {
		t.Fatalf("gate decision = %q, want undecided", updated.Status.GateDecision.State)
	}
	pod := &corev1.Pod{}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-0"}, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if !hasWarmupGate(pod) {
		t.Fatal("Hold removed the gate")
	}

	var decision map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if record["msg"] == "gate evaluated" {
			decision = record
		}
	}
	if decision == nil {
		t.Fatalf("missing gate evaluated log: %s", buf.String())
	}
	for _, key := range []string{"warmup_task", "state", "reason", "required", "gap", "slope", "rates"} {
		if _, ok := decision[key]; !ok {
			t.Fatalf("gate log missing %q: %#v", key, decision)
		}
	}
	if decision["state"] != "Hold" || decision["reason"] != "hold" {
		t.Fatalf("gate log state/reason = %v/%v", decision["state"], decision["reason"])
	}
}

func TestRemoveGatesUsesConfiguredAnnotationKey(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{PodIndex: 0, NodeName: "n1", ReadyBlockBitmap: fullBitmap(2)},
	}
	c := scoreClient(t, gatedPod(0))
	r := &Reconciler{
		Client:          c,
		Now:             func() time.Time { return scoreTestNow },
		ScoreAnnotation: "example.io/node-scores",
	}
	podNames := map[int32]string{0: "trainer-0"}
	if _, _, err := r.removeGates(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		task, [][]*agentpb.ChunkRef{testChunks("a", "b")}, podNames); err != nil {
		t.Fatalf("removeGates: %v", err)
	}

	value := readPodScore(t, c, types.NamespacedName{Namespace: "train", Name: "trainer-0"}, "example.io/node-scores")
	if _, ok := value.S["n1"]; !ok {
		t.Fatalf("S = %v, want the assigned node", value.S)
	}

	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-0"}, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if _, ok := pod.Annotations[scorefmt.DefaultAnnotation]; ok {
		t.Fatalf("default annotation %q written, want only the configured key", scorefmt.DefaultAnnotation)
	}
}

func TestPlacementGenerationChangeSendsUpdate(t *testing.T) {
	in := testPlacementInput()
	in.Placement.Assignment = map[int32][]string{0: {"n0"}}
	in.Previous = map[PreheatPair]ExistingPreheat{
		{PodIndex: 0, Node: "n0"}: {
			Generation:   uint64(in.Task.Generation) - 1,
			WindowBlocks: uint32(len(in.Windows[0])),
		},
	}
	actions, err := PlacementActions(in)
	if err != nil {
		t.Fatalf("PlacementActions: %v", err)
	}
	if len(actions) != 1 || actions[0].Kind != ActionUpdate {
		t.Fatalf("actions = %+v, want one Update for the generation change", actions)
	}

	in.Previous[PreheatPair{PodIndex: 0, Node: "n0"}] = ExistingPreheat{
		Generation:   uint64(in.Task.Generation),
		WindowBlocks: uint32(len(in.Windows[0])),
	}
	actions, err = PlacementActions(in)
	if err != nil {
		t.Fatalf("PlacementActions: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("actions = %+v, want none when generation and bitmap match", actions)
	}
}

func TestComputePlacementStopsFailedNodes(t *testing.T) {
	task := fixtureTask(1, 2, nil)
	task.Status.ReadinessTable = [][]int32{{0, 2}}
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{PodIndex: 0, NodeName: "n1", State: traincachev1alpha1.PreheatStateFailed},
	}
	r := &Reconciler{}
	placement := r.computePlacement(task, []string{"n1", "n2"}, map[string]error{
		"n1": fmt.Errorf("%w: down", agentclient.ErrAgentUnavailable),
	}, nil)
	if placement.Err != nil {
		t.Fatalf("Compute: %v", placement.Err)
	}
	if got := placement.Assignment[0]; len(got) != 1 || got[0] != "n2" {
		t.Fatalf("assignment = %v, want [n2] (n1 stopped)", got)
	}
}

func TestGwaaAssignmentChangeLogHasReason(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	in := testPlacementInput()
	in.Previous = map[PreheatPair]ExistingPreheat{}
	rpc := &fakePlacementRPC{}
	if _, err := ApplyPlacement(context.Background(), rpc, log, in); err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if record["msg"] == "gwaa assignment changed" {
			if _, ok := record["reason"]; !ok {
				t.Fatalf("assignment change log missing reason: %#v", record)
			}
			return
		}
	}
	t.Fatalf("missing gwaa assignment changed log: %s", buf.String())
}
