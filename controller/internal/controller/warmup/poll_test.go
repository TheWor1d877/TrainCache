package warmup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/gwaa"
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
		if entry.PodIndex == pod && entry.NodeName == node {
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
			if node == got.NodeName {
				column = i
				break
			}
		}
		if column < 0 {
			t.Fatalf("entry[%d] nodeName = %q, not in candidateNodes", pod, got.NodeName)
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

// --- MinUnmet end-to-end chain tests ---

// TestPollMinUnmetFromGWAA 构造 conflict 场景（2 Pod, 3 node, max=2, min=2,
// 两 Pod 分布相同），GWAA 返回 placement.Decisions[pod].MinUnmet=true。
// applyAssignment → buildPodDetails 置位 → 断言 status 里每条 PodNodeReadiness
// 的 MinUnmet 都 true，且同一 Pod 的各条值相同。
func TestPollMinUnmetFromGWAA(t *testing.T) {
	// 2 Pod × K=16，3 节点，PreheatFanout=2，PreheatFanoutMin=2，
	// 每节点只能放 1 Pod（默认 AllowMultiPodPerNode=false）。
	// 两 Pod 在三节点上的 ready 分布相同 {n0:11, n1:7, n2:3}。
	task := fixtureTask(2, 16, [][]int32{nil, nil})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 2
	ds := fixtureDataSet(32, singleShardLayout(32), []string{"n0", "n1", "n2"})
	pool := newFakeProgressPool()
	// 两个 Pod 在三个节点上都有进度，让 GWAA 看到 conflict 分布。
	pool.set("train/t1/p0", "n0", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p0", "n2", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p1", "n0", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p1", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p1", "n2", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	c := fixtureClient(t, task, ds,
		fixtureNode("n0"), fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n0"), fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool, PollInterval: time.Second}

	// 第一轮 Reconcile 会提交 Pod 并 Poll。Gate 未满足则处于 Preheating/Gating。
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	updated := readyTask(t, c)

	if len(updated.Status.PodNodeReadiness) == 0 {
		t.Fatal("no PodNodeReadiness entries produced")
	}

	// 统计每个 Pod 下有多少条 MinUnmet=true 的记录，且同一 Pod 各条值相同。
	for pod := int32(0); pod < 2; pod++ {
		var (
			values         []bool
			nodeCount      int
			anyMinUnmet    bool
			anyNotMinUnmet bool
		)
		for _, entry := range updated.Status.PodNodeReadiness {
			if entry.PodIndex != pod {
				continue
			}
			nodeCount++
			values = append(values, entry.MinUnmet)
			if entry.MinUnmet {
				anyMinUnmet = true
			} else {
				anyNotMinUnmet = true
			}
		}
		if nodeCount == 0 {
			t.Fatalf("pod %d has no PodNodeReadiness entries", pod)
		}
		// D40 冻结约定：同一 Pod 各条记录 MinUnmet 相同。
		if anyMinUnmet && anyNotMinUnmet {
			t.Fatalf("pod %d entries disagree on MinUnmet: %v", pod, values)
		}
	}

	// 在这个 conflict 场景中（两 Pod 争三节点，min=2, max=2），
	// 必有 Pod 拿不到 2 个节点 → MinUnmet=true 应该出现。
	hasTrue := false
	for _, entry := range updated.Status.PodNodeReadiness {
		if entry.MinUnmet {
			hasTrue = true
			break
		}
	}
	if !hasTrue {
		t.Fatalf("conflict scenario should produce at least one MinUnmet=true: %#v",
			updated.Status.PodNodeReadiness)
	}
}

// TestPollMinUnmetCarryFromPrevious 直接测 buildPodDetails 的 carryReadinessFact 路径：
// 上一次 status 已有 MinUnmet=true 的记录，本次调用 minUnmet=nil 且没有新 fact，
// 走 carryReadinessFact 分支时 MinUnmet 应该保留。
func TestPollMinUnmetCarryFromPrevious(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1, 2, 3}})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 2
	// 预置 status：模拟上一次 GWAA 已经写好的 MinUnmet=true。
	now := metav1.NewTime(time.Now())
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{
			PodIndex:       0,
			PodName:        "trainer-0",
			NodeName:       "n1",
			ActualNode:     "trainer-0",
			RequiredReady:  2,
			ReadyInWindow:  2,
			State:          traincachev1alpha1.PreheatStatePreheating,
			LastReportTime: &now,
			MinUnmet:       true,
		},
		{
			PodIndex:       0,
			PodName:        "trainer-0",
			NodeName:       "n2",
			ActualNode:     "trainer-0",
			RequiredReady:  2,
			ReadyInWindow:  1,
			State:          traincachev1alpha1.PreheatStatePreheating,
			LastReportTime: &now,
			MinUnmet:       true,
		},
	}

	podNames := map[int32]string{0: "trainer-0"}
	candidates := []string{"n0", "n1", "n2", "n3"}
	// assignment=nil + 保持现有 status 里的 assigned nodes
	// facts=nil + minUnmet=nil → 两条 entry 都走 carryReadinessFact 分支
	buildPodDetails(task, nil, podNames, candidates, nil, nil)

	if len(task.Status.PodNodeReadiness) != 2 {
		t.Fatalf("entries = %d, want 2", len(task.Status.PodNodeReadiness))
	}
	for _, entry := range task.Status.PodNodeReadiness {
		if entry.NodeName == "" {
			continue
		}
		if !entry.MinUnmet {
			t.Fatalf("entry pod=%d node=%q MinUnmet=false, want true (carry from previous)",
				entry.PodIndex, entry.NodeName)
		}
	}
}

// TestPollMinUnmetCarryLostOnFreshFact 对比路径：当 minUnmet=nil 但有新 fact
// （hasFact && fact.hasData）时，走 applyReadinessFact 分支不会保留 MinUnmet
// （这是 Poll 中间态的行为，会被后续 applyAssignment 覆盖回来）。
func TestPollMinUnmetCarryLostOnFreshFact(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1, 2, 3}})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 2
	now := metav1.NewTime(time.Now())
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{
			PodIndex:       0,
			PodName:        "trainer-0",
			NodeName:       "n1",
			ActualNode:     "trainer-0",
			RequiredReady:  2,
			ReadyInWindow:  2,
			State:          traincachev1alpha1.PreheatStatePreheating,
			LastReportTime: &now,
			MinUnmet:       true, // 预置 true
		},
	}

	// 有新 fact + hasData=true → applyReadinessFact 分支
	facts := map[readinessKey]readinessFact{
		{podIndex: 0, node: "n1"}: {
			ready:   2,
			state:   traincachev1alpha1.PreheatStatePreheating,
			hasData: true,
		},
	}
	podNames := map[int32]string{0: "trainer-0"}
	candidates := []string{"n1", "n2"}
	buildPodDetails(task, facts, podNames, candidates, nil, nil)

	// applyReadinessFact 不设置 MinUnmet，而 minUnmet map 也是 nil → 零值 false。
	entry := readinessEntry(task, 0, "n1")
	if entry == nil {
		t.Fatal("entry for n1 missing")
	}
	if entry.MinUnmet {
		t.Fatalf("fresh fact path: MinUnmet=true, want false (zero fill when minUnmet map is nil)")
	}
	// 但如果随后 applyAssignment 提供了正确的 minUnmet map，会再覆盖回来。
}

// TestPollMinUnmetFalseHappyPath 正常场景（足够节点、min=2, max=3、
// 缓存分布均匀），Assignment >= min，断言 MinUnmet=false。
func TestPollMinUnmetFalseHappyPath(t *testing.T) {
	// 1 Pod, K=8, 4 节点，PreheatFanout=3, PreheatFanoutMin=2。
	// 足够多的节点 → GWAA 能拿到 >= min 个节点 → MinUnmet=false。
	task := fixtureTask(1, 8, [][]int32{{0, 1, 2, 3, 4, 5, 6, 7}})
	task.Spec.PreheatFanout = 3
	task.Spec.PreheatFanoutMin = 2
	ds := fixtureDataSet(8, singleShardLayout(8), []string{"n0", "n1", "n2", "n3"})
	pool := newFakeProgressPool()
	for _, node := range []string{"n0", "n1", "n2", "n3"} {
		pool.set("train/t1/p0", node, progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	}
	c := fixtureClient(t, task, ds,
		fixtureNode("n0"), fixtureNode("n1"), fixtureNode("n2"), fixtureNode("n3"),
		fixtureAgentPod("n0"), fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureAgentPod("n3"),
		fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool, PollInterval: time.Second}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)

	for _, entry := range updated.Status.PodNodeReadiness {
		if entry.MinUnmet {
			t.Fatalf("happy path entry pod=%d node=%q MinUnmet=true, want false: %#v",
				entry.PodIndex, entry.NodeName, entry)
		}
	}
	// 同时确认 Assignment 数量 ≥ min=2。
	count := 0
	for _, entry := range updated.Status.PodNodeReadiness {
		if entry.NodeName != "" {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("assigned node count = %d, want >= 2 (fanoutMin)", count)
	}
}

// TestGWAAConflictMinUnmetEndToEnd 手动跑整条链路：
// gwaa.Compute (conflict) → placement.Decisions[*].MinUnmet=true
// → applyAssignment 转 map → buildPodDetails 置位。
// 验证完整链路通。
func TestGWAAConflictMinUnmetEndToEnd(t *testing.T) {
	import_rand := rand.New(rand.NewSource(3))
	placement := gwaa.Compute(gwaa.Input{
		Pods: []gwaa.PodState{
			{Index: 0, K: 16, ReadyCountByNode: map[string]int32{"n0": 11, "n1": 7, "n2": 3}},
			{Index: 1, K: 16, ReadyCountByNode: map[string]int32{"n0": 11, "n1": 7, "n2": 3}},
		},
		Nodes: []gwaa.NodeState{
			{Name: "n0"}, {Name: "n1"}, {Name: "n2"},
		},
		Rand:                 import_rand,
		Fanout:               2,
		FanoutMin:            2,
		AllowMultiPodPerNode: false,
	})
	if placement.Err != nil {
		t.Fatalf("GWAA Compute error: %v", placement.Err)
	}

	// 验证 placement.Decisions 与 Assignment 一致（GWAA 已保证，但显式断言）。
	minUnmetMap := make(map[int32]bool, len(placement.Decisions))
	for idx, dec := range placement.Decisions {
		minUnmetMap[idx] = dec.MinUnmet
		if got := int32(len(placement.Assignment[idx])); dec.MinUnmet {
			if got >= 2 {
				t.Fatalf("pod %d MinUnmet=true but assignment has %d >= min=2: decisions=%#v assignment=%v",
					idx, got, placement.Decisions, placement.Assignment)
			}
		} else {
			if got < 2 {
				t.Fatalf("pod %d MinUnmet=false but assignment has %d < min=2: decisions=%#v assignment=%v",
					idx, got, placement.Decisions, placement.Assignment)
			}
		}
	}

	// 必有至少一个 Pod MinUnmet=true（2 Pod × min=2 = 4 claim，只有 3 节点可放）。
	hasTrue := false
	for _, v := range minUnmetMap {
		if v {
			hasTrue = true
			break
		}
	}
	if !hasTrue {
		t.Fatalf("conflict scenario expected at least one MinUnmet=true: %#v", placement.Decisions)
	}

	// 把 minUnmet map + placement.Assignment 喂进 buildPodDetails。
	task := fixtureTask(2, 16, [][]int32{nil, nil})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 2
	podNames := map[int32]string{0: "trainer-0", 1: "trainer-1"}
	candidates := []string{"n0", "n1", "n2"}
	buildPodDetails(task, nil, podNames, candidates, placement.Assignment, minUnmetMap)

	if len(task.Status.PodNodeReadiness) == 0 {
		t.Fatal("buildPodDetails produced no entries")
	}

	// 对每个 Pod：所有 entry 的 MinUnmet 相同，且等于 GWAA 决策。
	for pod := int32(0); pod < 2; pod++ {
		want := minUnmetMap[pod]
		values := make([]bool, 0)
		for _, entry := range task.Status.PodNodeReadiness {
			if entry.PodIndex != pod {
				continue
			}
			values = append(values, entry.MinUnmet)
			if entry.MinUnmet != want {
				t.Fatalf("pod %d node=%q MinUnmet=%v, want %v (GWAA decisions=%#v)",
					pod, entry.NodeName, entry.MinUnmet, want, placement.Decisions)
			}
		}
		if len(values) == 0 {
			t.Fatalf("pod %d has no entries", pod)
		}
	}
}

// TestMinUnmetInCRDStatusPatch 跑完整 Reconcile（controller-runtime fake client），
// 然后 Get 回 WarmupTask，断言 MinUnmet 字段真实存在于 status.PodNodeReadiness 中。
func TestMinUnmetInCRDStatusPatch(t *testing.T) {
	task := fixtureTask(2, 16, [][]int32{nil, nil})
	task.Spec.PreheatFanout = 2
	task.Spec.PreheatFanoutMin = 2
	ds := fixtureDataSet(32, singleShardLayout(32), []string{"n0", "n1", "n2"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n0", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p0", "n2", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p1", "n0", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p1", "n1", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	pool.set("train/t1/p1", "n2", progressResponse{status: bitmapResponse(3, 1, []byte{0xFF})})
	c := fixtureClient(t, task, ds,
		fixtureNode("n0"), fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n0"), fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool, PollInterval: time.Second}

	// Reconcile → computePlacement (GWAA) → applyAssignment → buildPodDetails → status patch。
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// 用同一个 fake client 读回 —— 等价于 Kubernetes 持久化后重新 Get。
	var fetched traincachev1alpha1.WarmupTask
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, &fetched); err != nil {
		t.Fatalf("get patched task: %v", err)
	}

	if len(fetched.Status.PodNodeReadiness) == 0 {
		t.Fatal("PodNodeReadiness is empty after Reconcile")
	}

	// 1) MinUnmet 字段已被 GWAA 设置，出现在 status 中。
	hasTrue := false
	for i, entry := range fetched.Status.PodNodeReadiness {
		t.Logf("entry[%d] pod=%d node=%q minUnmet=%v state=%s",
			i, entry.PodIndex, entry.NodeName, entry.MinUnmet, entry.State)
		if entry.MinUnmet {
			hasTrue = true
		}
	}
	if !hasTrue {
		t.Fatalf("MinUnmet=true expected in conflict scenario, got none: %#v",
			fetched.Status.PodNodeReadiness)
	}

	// 2) 同一 Pod 各条 MinUnmet 值相同（D40 冻结约定）。
	byPod := make(map[int32][]bool)
	for _, entry := range fetched.Status.PodNodeReadiness {
		byPod[entry.PodIndex] = append(byPod[entry.PodIndex], entry.MinUnmet)
	}
	for pod, values := range byPod {
		first := values[0]
		for i, v := range values {
			if v != first {
				t.Fatalf("pod %d entries disagree at index %d: %v", pod, i, values)
			}
		}
	}
}
