package warmup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
)

type progressCall struct {
	taskID     string
	node       string
	generation uint64
}

type progressResponse struct {
	status *agentpb.PreheatStatus
	err    error
}

// fakeProgressPool reuses the M4 fake for Submit; Progress serves a scripted
// response per (task_id, node) and repeats the last entry once exhausted.
type fakeProgressPool struct {
	*fakeAgentPool
	mu    sync.Mutex
	byKey map[string][]progressResponse
	calls []progressCall
}

func newFakeProgressPool() *fakeProgressPool {
	return &fakeProgressPool{fakeAgentPool: &fakeAgentPool{}, byKey: map[string][]progressResponse{}}
}

func progressKeyOf(taskID, node string) string { return taskID + "@" + node }

func (p *fakeProgressPool) set(taskID, node string, responses ...progressResponse) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byKey[progressKeyOf(taskID, node)] = responses
}

func (p *fakeProgressPool) Progress(_ context.Context, node, taskID string, generation uint64) (*agentpb.PreheatStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, progressCall{taskID: taskID, node: node, generation: generation})
	responses := p.byKey[progressKeyOf(taskID, node)]
	if len(responses) == 0 {
		return nil, nil
	}
	response := responses[0]
	if len(responses) > 1 {
		p.byKey[progressKeyOf(taskID, node)] = responses[1:]
	}
	return response.status, response.err
}

func bitmapResponse(generation, seq uint64, bitmap []byte) *agentpb.PreheatStatus {
	return &agentpb.PreheatStatus{
		Generation:      generation,
		Seq:             seq,
		ReadyBitmap:     bitmap,
		State:           agentpb.PreheatStatus_RUNNING,
		CompletedBlocks: 0,
	}
}

func readyTask(t *testing.T, c client.Client) *traincachev1alpha1.WarmupTask {
	t.Helper()
	return getTask(t, c)
}

func reconcileResult(t *testing.T, r *Reconciler) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "train", Name: "t1"},
	})
}

func readinessEntry(task *traincachev1alpha1.WarmupTask, pod int32, node string) *traincachev1alpha1.PodNodeReadiness {
	for i := range task.Status.PodNodeReadiness {
		entry := &task.Status.PodNodeReadiness[i]
		if entry.PodIndex == pod && (entry.AssignedNode == node || entry.NodeName == node) {
			return entry
		}
	}
	return nil
}

func TestPollUpdatesReadinessAndRequeues(t *testing.T) {
	task := fixtureTask(1, 8, [][]int32{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}})
	task.Spec.Gate.Threshold = 0.8
	ds := fixtureDataSet(10, singleShardLayout(10), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0b00000111})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool, PollInterval: 3 * time.Second}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 3*time.Second {
		t.Fatalf("RequeueAfter = %v, want 3s", result.RequeueAfter)
	}

	updated := readyTask(t, c)
	if got := updated.Status.ReadinessTable[0][0]; got != 3 {
		t.Fatalf("table[0][0] = %d, want 3", got)
	}
	entry := readinessEntry(updated, 0, "n1")
	if entry == nil {
		t.Fatal("missing PodNodeReadiness for p0/n1")
	}
	if entry.StartupWindowReady != 3 || entry.ReadyInWindow != 3 {
		t.Fatalf("window ready = %d/%d, want 3", entry.StartupWindowReady, entry.ReadyInWindow)
	}
	if entry.StartupWindowReadyRate != 3.0/8.0 {
		t.Fatalf("rate = %v, want 0.375", entry.StartupWindowReadyRate)
	}
	if entry.RequiredReady != 6 {
		t.Fatalf("required = %d, want 6", entry.RequiredReady)
	}
	if entry.State != traincachev1alpha1.PreheatStatePreheating {
		t.Fatalf("state = %q, want Preheating", entry.State)
	}
	if entry.LastReportTime == nil {
		t.Fatal("LastReportTime is nil")
	}
	if len(entry.ReadyBlockBitmap) != 1 || entry.ReadyBlockBitmap[0] != 0b00000111 {
		t.Fatalf("bitmap = %v", entry.ReadyBlockBitmap)
	}
	if updated.Status.LastUpdated == nil {
		t.Fatal("LastUpdated is nil")
	}
}

func TestPollDropsOldGeneration(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1, 2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(2, 9, []byte{0xFF})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := &Reconciler{Client: c, Agents: pool, Log: log}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	if got := updated.Status.ReadinessTable[0][0]; got != 0 {
		t.Fatalf("table[0][0] = %d, want 0 for stale generation", got)
	}
	entry := readinessEntry(updated, 0, "n1")
	if entry == nil || len(entry.ReadyBlockBitmap) != 0 {
		t.Fatalf("entry = %#v, want untouched", entry)
	}
	if !strings.Contains(buf.String(), "drop progress: generation mismatch") {
		t.Fatalf("missing generation-drop log: %s", buf.String())
	}
}

func TestPollDropsStaleSeq(t *testing.T) {
	task := fixtureTask(1, 4, [][]int32{{0, 1, 2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1",
		progressResponse{status: bitmapResponse(3, 5, []byte{0b00000011})},
		progressResponse{status: bitmapResponse(3, 4, []byte{0xFF})},
	)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := &Reconciler{Client: c, Agents: pool, Log: log}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	updated := readyTask(t, c)
	if got := updated.Status.ReadinessTable[0][0]; got != 2 {
		t.Fatalf("table[0][0] = %d, want 2 (stale seq must not win)", got)
	}
	if !strings.Contains(buf.String(), "drop progress: stale seq") {
		t.Fatalf("missing stale-seq log: %s", buf.String())
	}
}

// patchCountingClient records status patches so idempotent replays can be
// asserted without relying on fake-client resourceVersion semantics.
type patchCountingClient struct {
	client.Client
	patches int
}

func (c *patchCountingClient) Status() client.SubResourceWriter {
	return &patchCountingStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type patchCountingStatusWriter struct {
	client.SubResourceWriter
	parent *patchCountingClient
}

func (w *patchCountingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	w.parent.patches++
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func TestPollDuplicateSeqIsIdempotent(t *testing.T) {
	task := fixtureTask(1, 4, [][]int32{{0, 1, 2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 7, []byte{0b00001111})})
	base := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	counting := &patchCountingClient{Client: base}
	r := &Reconciler{Client: counting, Agents: pool}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	firstPatches := counting.patches
	before := readyTask(t, base)

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if counting.patches != firstPatches {
		t.Fatalf("status patches = %d after replay, want %d", counting.patches, firstPatches)
	}
	after := readyTask(t, base)
	before.ResourceVersion, after.ResourceVersion = "", ""
	if !reflect.DeepEqual(before.Status, after.Status) {
		t.Fatalf("status changed on duplicate seq")
	}
}

func TestPollBitmapLengthMismatchDoesNotUpdate(t *testing.T) {
	// K=9 asks for 9 chunks, so 1 bitmap byte is malformed (want 2).
	task := fixtureTask(1, 9, [][]int32{{0, 1, 2, 3, 4, 5, 6, 7, 8}})
	ds := fixtureDataSet(9, singleShardLayout(9), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := &Reconciler{Client: c, Agents: pool, Log: log}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	if got := updated.Status.ReadinessTable[0][0]; got != 0 {
		t.Fatalf("table[0][0] = %d, want 0", got)
	}
	entry := readinessEntry(updated, 0, "n1")
	if entry == nil || len(entry.ReadyBlockBitmap) != 0 || entry.State != traincachev1alpha1.PreheatStatePreheating {
		t.Fatalf("entry = %#v, want unchanged Preheating", entry)
	}
	if !strings.Contains(buf.String(), "invalid ready bitmap") {
		t.Fatalf("missing bitmap parse log: %s", buf.String())
	}
}

func TestPollThreeUnavailableFailures(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{err: fmt.Errorf("%w: down", agentclient.ErrAgentUnavailable)})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := &Reconciler{Client: c, Agents: pool, Log: log, PollInterval: time.Second}

	for i := 0; i < 2; i++ {
		result, err := reconcileResult(t, r)
		if err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
		if result.RequeueAfter == 0 {
			t.Fatalf("Reconcile %d stopped requeueing before the threshold", i+1)
		}
		entry := readinessByPod(readyTask(t, c), 0)
		if entry == nil || entry.State != traincachev1alpha1.PreheatStatePreheating {
			t.Fatalf("attempt %d state = %#v, want Preheating", i+1, entry)
		}
	}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("third Reconcile: %v", err)
	}
	entry := readinessByPod(readyTask(t, c), 0)
	if entry == nil || entry.State != traincachev1alpha1.PreheatStateFailed {
		t.Fatalf("state = %#v, want Failed after 3 failures", entry)
	}
	// M7 keeps an independent gate cycle alive after a terminal poll: the task
	// has not been released and TMax has not elapsed.
	if result.RequeueAfter != time.Second {
		t.Fatalf("gate cycle RequeueAfter = %v, want 1s", result.RequeueAfter)
	}
	if !strings.Contains(buf.String(), "preheat progress degraded") {
		t.Fatalf("missing degradation log: %s", buf.String())
	}
}

func TestPollThreeUnavailableWithSubmitFailure(t *testing.T) {
	// M4 still returns the submit error, but polling must count the failures
	// independently and mark the pair Failed after three consecutive rounds.
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	pool.fail = map[string]error{"n1": fmt.Errorf("%w: submit down", agentclient.ErrAgentUnavailable)}
	pool.set("train/t1/p0", "n1", progressResponse{err: fmt.Errorf("%w: progress down", agentclient.ErrAgentUnavailable)})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	for i := 0; i < unavailableFailureThreshold; i++ {
		result, err := reconcileResult(t, r)
		if i == 0 && !errors.Is(err, agentclient.ErrAgentUnavailable) {
			t.Fatalf("initial Reconcile error = %v, want ErrAgentUnavailable", err)
		}
		if result.RequeueAfter == 0 {
			t.Fatalf("Reconcile %d stopped the gate cycle without release", i+1)
		}
	}
	entry := readinessByPod(readyTask(t, c), 0)
	if entry == nil || entry.State != traincachev1alpha1.PreheatStateFailed {
		t.Fatalf("state = %#v, want Failed after 3 failures", entry)
	}
}

func TestPollCompletionStopsRequeue(t *testing.T) {
	task := fixtureTask(1, 4, [][]int32{{0, 1, 2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1"})
	pool := newFakeProgressPool()
	done := bitmapResponse(3, 1, []byte{0xFF})
	done.State = agentpb.PreheatStatus_DONE
	pool.set("train/t1/p0", "n1", progressResponse{status: done})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool, PollInterval: time.Second}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0 for Ready", result.RequeueAfter)
	}
	entry := readinessEntry(readyTask(t, c), 0, "n1")
	if entry == nil || entry.State != traincachev1alpha1.PreheatStateReady {
		t.Fatalf("state = %#v, want Ready", entry)
	}
	if entry.StartupWindowReady != 4 || entry.ReadyInWindow != 4 {
		t.Fatalf("window ready = %d, want 4", entry.StartupWindowReady)
	}
}

func TestPollReadinessTableShape(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{{0, 1, 2}, {3, 4, 5}})
	task.Spec.Gate.Threshold = 0.5
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n2", "n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0b00000001})})
	pool.set("train/t1/p0", "n2", progressResponse{status: bitmapResponse(3, 1, []byte{0b00000011})})
	pool.set("train/t1/p1", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0b00000011})})
	pool.set("train/t1/p1", "n2", progressResponse{status: bitmapResponse(3, 1, []byte{0b00000000})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	if got := strings.Join(updated.Status.CandidateNodes, ","); got != "n1,n2" {
		t.Fatalf("candidate order = %q, want n1,n2", got)
	}
	want := [][]int32{{1, 2}, {2, 0}}
	if len(updated.Status.ReadinessTable) != len(want) {
		t.Fatalf("table rows = %d, want %d", len(updated.Status.ReadinessTable), len(want))
	}
	for pod := range want {
		for node := range want[pod] {
			if got := updated.Status.ReadinessTable[pod][node]; got != want[pod][node] {
				t.Fatalf("table[%d][%d] = %d, want %d", pod, node, got, want[pod][node])
			}
		}
	}
	if len(updated.Status.PodNodeReadiness) != 2 {
		t.Fatalf("PodNodeReadiness entries = %d, want one per Pod (2)", len(updated.Status.PodNodeReadiness))
	}
	for pod := range updated.Status.PodNodeReadiness {
		got := updated.Status.PodNodeReadiness[pod]
		if got.PodIndex != int32(pod) {
			t.Fatalf("entry[%d].podIndex = %d, want %d", pod, got.PodIndex, pod)
		}
		column := -1
		for i, node := range updated.Status.CandidateNodes {
			if node == got.AssignedNode {
				column = i
				break
			}
		}
		if column < 0 {
			t.Fatalf("entry[%d] assignedNode = %q, not in candidateNodes", pod, got.AssignedNode)
		}
		if got.ReadyInWindow != want[pod][column] {
			t.Fatalf("entry[%d].readyInWindow = %d, want assigned-node table value %d",
				pod, got.ReadyInWindow, want[pod][column])
		}
	}
}

func TestProgressPassesGeneration(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(pool.calls) != 1 {
		t.Fatalf("progress calls = %d, want 1", len(pool.calls))
	}
	if pool.calls[0].generation != 3 || pool.calls[0].taskID != "train/t1/p0" || pool.calls[0].node != "n1" {
		t.Fatalf("progress call = %#v", pool.calls[0])
	}
}

func TestRateHistoryMinIntervalAndRestart(t *testing.T) {
	r := &Reconciler{}
	t0 := time.Unix(1000, 0)
	minInterval := 10 * time.Second
	r.progress.recordRate("uid-1", 0, t0, 0.1, minInterval)
	r.progress.recordRate("uid-1", 0, t0.Add(5*time.Second), 0.2, minInterval)  // too soon
	r.progress.recordRate("uid-1", 0, t0.Add(10*time.Second), 0.3, minInterval) // accepted
	r.progress.recordRate("uid-1", 0, t0.Add(15*time.Second), 0.4, minInterval) // too soon
	r.progress.recordRate("uid-1", 0, t0.Add(20*time.Second), 0.5, minInterval) // accepted

	samples := r.progress.samples("uid-1", 0)
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want 2: %#v", len(samples), samples)
	}
	if samples[0].Rate != 0.3 || !samples[0].At.Equal(t0.Add(10*time.Second)) {
		t.Fatalf("first sample = %#v", samples[0])
	}
	if samples[1].Rate != 0.5 || !samples[1].At.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("second sample = %#v", samples[1])
	}

	restarted := &Reconciler{}
	if got := restarted.progress.samples("uid-1", 0); len(got) != 0 {
		t.Fatalf("restarted history = %#v, want empty", got)
	}
}

func TestPollRateHistoryUsesBestCandidate(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1, 2, 3}})
	task.Spec.Gate.MinInterval.Duration = time.Minute
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0b00000001})})
	pool.set("train/t1/p0", "n2", progressResponse{status: bitmapResponse(3, 1, []byte{0b00000011})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	samples := r.progress.samples(string(task.UID), 0)
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if samples[0].Rate != 1.0 {
		t.Fatalf("rate = %v, want 1.0 from the best candidate", samples[0].Rate)
	}
}

func TestPollNilProgressIsPending(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool() // no scripted response -> nil, nil
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool, PollInterval: 2 * time.Second}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("RequeueAfter = %v, want 2s", result.RequeueAfter)
	}
}

func TestPollLogsRequiredFields(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 2, []byte{0xFF})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})).
		With("request_id", "rid-1", "warmup_task", "train/t1")
	r := &Reconciler{Client: c, Agents: pool, Log: log}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		if entry["msg"] == "progress updated" {
			found = entry
		}
	}
	if found == nil {
		t.Fatalf("missing progress updated log: %s", buf.String())
	}
	for _, key := range []string{"request_id", "warmup_task", "task_id", "pod", "node", "generation", "seq", "duration_ms"} {
		if _, ok := found[key]; !ok {
			t.Fatalf("progress log missing %q: %#v", key, found)
		}
	}
}

func TestPollPermanentErrorDoesNotFail(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{err: fmt.Errorf("%w: bad", agentclient.ErrPermanent)})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("permanent progress error should keep polling, result = %#v", result)
	}
	entry := readinessEntry(readyTask(t, c), 0, "n1")
	if entry == nil || entry.State == traincachev1alpha1.PreheatStateFailed {
		t.Fatalf("entry = %#v, want not Failed", entry)
	}
}
