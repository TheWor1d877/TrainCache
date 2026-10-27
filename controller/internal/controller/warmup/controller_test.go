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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
)

type submitted struct {
	node string
	req  *agentpb.SubmitPreheatRequest
}

type fakeAgentPool struct {
	mu          sync.Mutex
	records     []submitted
	submitCalls int
	fail        map[string]error
}

func (p *fakeAgentPool) Submit(_ context.Context, node string, req *agentpb.SubmitPreheatRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submitCalls++
	if err := p.fail[node]; err != nil {
		return err
	}
	p.records = append(p.records, submitted{node: node, req: req})
	return nil
}

func (p *fakeAgentPool) Update(context.Context, string, *agentpb.UpdatePreheatRequest) error {
	return nil
}

func (p *fakeAgentPool) Cancel(context.Context, string, *agentpb.CancelPreheatRequest) error {
	return nil
}

func (p *fakeAgentPool) Progress(context.Context, string, string, uint64) (*agentpb.PreheatStatus, error) {
	return nil, nil
}

func (p *fakeAgentPool) snapshot() []submitted {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]submitted(nil), p.records...)
}

func fixtureDataSet(total int32, layout []traincachev1alpha1.BlockLayout, accessible []string) *traincachev1alpha1.DataSet {
	return &traincachev1alpha1.DataSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ds1", Namespace: "train"},
		Spec: traincachev1alpha1.DataSetSpec{
			DatasetName: "dataset",
		},
		Status: traincachev1alpha1.DataSetStatus{
			SnapshotID:      "snap-1",
			TotalChunks:     total,
			BlockLayout:     layout,
			AccessibleNodes: accessible,
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, Reason: "NodesResolved",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
}

func fixtureTask(podCount, k int32, orders [][]int32) *traincachev1alpha1.WarmupTask {
	details := make([]traincachev1alpha1.PodDetail, podCount)
	for i := int32(0); i < podCount; i++ {
		details[i] = traincachev1alpha1.PodDetail{Index: i, K: k}
		if int(i) < len(orders) {
			details[i].InitialConsumeFiles = chunkFiles(orders[i])
		}
	}
	return &traincachev1alpha1.WarmupTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "t1",
			Namespace:  "train",
			UID:        "uid-1",
			Generation: 3,
		},
		Spec: traincachev1alpha1.WarmupTaskSpec{
			DataSetRef: corev1.LocalObjectReference{Name: "ds1"},
			TargetRef:  traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"},
			PodCount:   podCount,
			PodDetails: details,
			Score:      traincachev1alpha1.ScorePolicy{Cutoff: 0},
			Gate: traincachev1alpha1.GatePolicy{
				Threshold:   0.8,
				TMax:        metav1.Duration{Duration: time.Hour},
				MinInterval: metav1.Duration{Duration: time.Second},
			},
		},
	}
}

func fixtureNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func fixtureAgentPod(node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agent-" + node,
			Namespace: "traincache-system",
			Labels:    map[string]string{"app": "traincache-agentd"},
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      "10.0.0.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func fixtureTargetPod(index int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("trainer-%d", index),
			Namespace: "train",
			Labels: map[string]string{
				"batch.kubernetes.io/job-name": "trainer",
				JobCompletionIndexLabel:        fmt.Sprint(index),
			},
		},
	}
}

func fixtureClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}, &traincachev1alpha1.DataSet{}).
		WithObjects(objs...).
		Build()
}

func reconcile(t *testing.T, r *Reconciler) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "train", Name: "t1"},
	})
	return err
}

func getTask(t *testing.T, c client.Client) *traincachev1alpha1.WarmupTask {
	t.Helper()
	task := &traincachev1alpha1.WarmupTask{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, task); err != nil {
		t.Fatalf("get task: %v", err)
	}
	return task
}

// singleChunkShard names the one-chunk shard that carries a global chunk, so
// tests can express an explicit per-chunk consume list as file names.
func singleChunkShard(chunk int32) string {
	return fmt.Sprintf("s%03d.tar", chunk)
}

// chunkFiles expands chunk numbers into the matching shard names.
func chunkFiles(chunks []int32) []string {
	out := make([]string, len(chunks))
	for i, chunk := range chunks {
		out[i] = singleChunkShard(chunk)
	}
	return out
}

// singleShardLayout keeps its historical name but returns one single-chunk
// shard per global chunk. The name predates initialConsumeFiles (D43); callers
// use it only to build a layout for a fixed chunk count.
func singleShardLayout(total int32) []traincachev1alpha1.BlockLayout {
	out := make([]traincachev1alpha1.BlockLayout, 0, total)
	for chunk := int32(0); chunk < total; chunk++ {
		out = append(out, traincachev1alpha1.BlockLayout{Shard: singleChunkShard(chunk), StartChunk: chunk, Count: 1})
	}
	return out
}

func TestReconcileSubmitsPodWindowsToCandidates(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1", "n2"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool, Now: func() time.Time { return time.Unix(100, 0) }}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	records := pool.snapshot()
	if len(records) != 4 {
		t.Fatalf("submits = %d, want 4", len(records))
	}
	byKey := map[string]submitted{}
	for _, record := range records {
		byKey[record.req.TaskId+"@"+record.node] = record
	}
	for _, taskID := range []string{"train/t1/p0", "train/t1/p1"} {
		for _, node := range []string{"n1", "n2"} {
			record, ok := byKey[taskID+"@"+node]
			if !ok {
				t.Fatalf("missing submit %s on %s: %v", taskID, node, byKey)
			}
			// The 7.x Agent only accepts an empty snapshot_id (= CURRENT).
			if record.req.Generation != 3 || record.req.Dataset != "dataset" || record.req.SnapshotId != "" {
				t.Fatalf("request = %#v", record.req)
			}
			if len(record.req.Chunks) != 2 {
				t.Fatalf("%s chunks = %d, want K=2", taskID, len(record.req.Chunks))
			}
		}
	}

	updated := getTask(t, c)
	if updated.Status.Phase != traincachev1alpha1.WarmupPhasePreheating {
		t.Fatalf("phase = %q", updated.Status.Phase)
	}
	if got := strings.Join(updated.Status.CandidateNodes, ","); got != "n1,n2" {
		t.Fatalf("candidate nodes = %q", got)
	}
	if updated.Status.ConsumeOrderSource != traincachev1alpha1.ConsumeOrderSourceSpec {
		t.Fatalf("consume order source = %q", updated.Status.ConsumeOrderSource)
	}
	if updated.Status.StartedAt == nil || !updated.Status.StartedAt.Time.Equal(time.Unix(100, 0)) {
		t.Fatalf("startedAt = %v", updated.Status.StartedAt)
	}
}

func TestReconcileSingleNodeUnavailableDoesNotBlockOthers(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1", "n2"})
	pool := &fakeAgentPool{fail: map[string]error{
		"n2": fmt.Errorf("%w: connection refused", agentclient.ErrAgentUnavailable),
	}}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool}

	err := reconcile(t, r)
	if !errors.Is(err, agentclient.ErrAgentUnavailable) {
		t.Fatalf("Reconcile error = %v, want ErrAgentUnavailable", err)
	}
	records := pool.snapshot()
	if len(records) != 2 {
		t.Fatalf("submits = %d, want 2 from n1", len(records))
	}
	for _, record := range records {
		if record.node != "n1" {
			t.Fatalf("submitted to failed node: %#v", record)
		}
	}
	updated := getTask(t, c)
	if got := strings.Join(updated.Status.CandidateNodes, ","); got != "n1,n2" {
		t.Fatalf("candidate nodes = %q", got)
	}
	if updated.Status.Phase != traincachev1alpha1.WarmupPhasePreheating {
		t.Fatalf("phase = %q", updated.Status.Phase)
	}
}

func TestReconcileIsIdempotent(t *testing.T) {
	task := fixtureTask(2, 1, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	now := time.Unix(200, 0)
	r := &Reconciler{Client: c, Agents: pool, Now: func() time.Time { return now }}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	first := getTask(t, c)
	if err := reconcile(t, r); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	second := getTask(t, c)

	// M6 makes later rounds differential: the initial M4 fan-out runs once and
	// the replay issues no duplicate Submit.
	if pool.submitCalls != 2 {
		t.Fatalf("submit calls = %d, want 2 (initial fan-out only)", pool.submitCalls)
	}
	unique := map[string]struct{}{}
	for _, record := range pool.snapshot() {
		unique[record.req.TaskId+"@"+record.node] = struct{}{}
	}
	if len(unique) != 2 {
		t.Fatalf("unique agent tasks = %d, want 2 idempotency keys", len(unique))
	}
	if first.Status.StartedAt == nil || second.Status.StartedAt == nil ||
		!first.Status.StartedAt.Time.Equal(second.Status.StartedAt.Time) {
		t.Fatalf("startedAt changed: %v -> %v", first.Status.StartedAt, second.Status.StartedAt)
	}
	first.ResourceVersion, second.ResourceVersion = "", ""
	if !reflect.DeepEqual(first.Status, second.Status) {
		t.Fatalf("status changed across replay:\nfirst=%#v\nsecond=%#v", first.Status, second.Status)
	}
}

func TestReconcileInvalidPodIndex(t *testing.T) {
	task := fixtureTask(2, 1, [][]int32{{0, 1}, {2, 3}})
	task.Spec.PodDetails[1].Index = 0
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds)
	r := &Reconciler{Client: c, Agents: pool}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pool.submitCalls != 0 {
		t.Fatalf("submits = %d, want 0", pool.submitCalls)
	}
	updated := getTask(t, c)
	if cond := metaCondition(updated, ConditionInvalidSpec); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("InvalidSpec condition = %#v", cond)
	}
}

func TestReconcilePodLimit(t *testing.T) {
	podCount := int32(maxPods + 1)
	task := fixtureTask(podCount, 1, nil)
	for i := range task.Spec.PodDetails {
		task.Spec.PodDetails[i].InitialConsumeFiles = []string{singleChunkShard(int32(i))}
	}
	ds := fixtureDataSet(podCount, singleShardLayout(podCount), []string{"n1"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds)
	r := &Reconciler{Client: c, Agents: pool}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pool.submitCalls != 0 {
		t.Fatalf("submits = %d, want 0", pool.submitCalls)
	}
	if cond := metaCondition(getTask(t, c), ConditionInvalidSpec); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("InvalidSpec condition = %#v", cond)
	}
}

func TestReconcileCandidateLimit(t *testing.T) {
	task := fixtureTask(1, 1, [][]int32{{0}})
	accessible := make([]string, 0, maxCandidateNodes+1)
	objs := []client.Object{task, fixtureDataSet(1, singleShardLayout(1), nil)}
	for i := 0; i <= maxCandidateNodes; i++ {
		node := fmt.Sprintf("n%03d", i)
		accessible = append(accessible, node)
		objs = append(objs, fixtureNode(node), fixtureAgentPod(node))
	}
	ds := objs[1].(*traincachev1alpha1.DataSet)
	ds.Status.AccessibleNodes = accessible
	pool := &fakeAgentPool{}
	c := fixtureClient(t, objs...)
	r := &Reconciler{Client: c, Agents: pool}

	err := reconcile(t, r)
	if !errors.Is(err, ErrTooManyCandidates) {
		t.Fatalf("Reconcile error = %v, want ErrTooManyCandidates", err)
	}
	if pool.submitCalls != 0 {
		t.Fatalf("submits = %d, want 0", pool.submitCalls)
	}
	cond := metaCondition(getTask(t, c), ConditionNoCandidates)
	if cond == nil || cond.Reason != reasonTooManyCandidates {
		t.Fatalf("NoCandidates condition = %#v", cond)
	}
}

func TestReconcileNoCandidates(t *testing.T) {
	task := fixtureTask(1, 1, [][]int32{{0}})
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	err := reconcile(t, r)
	if !errors.Is(err, ErrNoCandidates) {
		t.Fatalf("Reconcile error = %v, want ErrNoCandidates", err)
	}
	if pool.submitCalls != 0 {
		t.Fatalf("submits = %d, want 0", pool.submitCalls)
	}
	if cond := metaCondition(getTask(t, c), ConditionNoCandidates); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("NoCandidates condition = %#v", cond)
	}
}

func TestReconcileDataSetNotReady(t *testing.T) {
	task := fixtureTask(1, 1, [][]int32{{0}})
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1"})
	ds.Status.Conditions = nil
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	err := reconcile(t, r)
	if !errors.Is(err, ErrDataSetNotReady) {
		t.Fatalf("Reconcile error = %v, want ErrDataSetNotReady", err)
	}
	if pool.submitCalls != 0 {
		t.Fatalf("submits = %d, want 0", pool.submitCalls)
	}
	if cond := metaCondition(getTask(t, c), ConditionDataSetNotReady); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("DataSetNotReady condition = %#v", cond)
	}
}

func TestReconcileDefaultsConsumeOrder(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{nil, {2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1"})
	pool := &fakeAgentPool{}
	recorder := record.NewFakeRecorder(8)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool, Recorder: recorder}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := getTask(t, c)
	if updated.Status.ConsumeOrderSource != traincachev1alpha1.ConsumeOrderSourceDefault {
		t.Fatalf("consume order source = %q", updated.Status.ConsumeOrderSource)
	}
	found := false
	for len(recorder.Events) > 0 {
		event := <-recorder.Events
		if strings.Contains(event, EventConsumeOrderDefaulted) {
			found = true
		}
	}
	if !found {
		t.Fatal("ConsumeOrderDefaulted event not recorded")
	}

	want := map[string][]string{
		"train/t1/p0": {singleChunkShard(0), singleChunkShard(1)},
		"train/t1/p1": {singleChunkShard(2), singleChunkShard(3)},
	}
	for _, record := range pool.snapshot() {
		got := make([]string, 0, len(record.req.Chunks))
		for _, chunk := range record.req.Chunks {
			got = append(got, chunk.Path)
		}
		if fmt.Sprint(got) != fmt.Sprint(want[record.req.TaskId]) {
			t.Fatalf("%s chunks = %v, want %v", record.req.TaskId, got, want[record.req.TaskId])
		}
	}
}

func TestReconcileDefaultsKAndRequeues(t *testing.T) {
	task := fixtureTask(2, 0, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1"})
	ds.Spec.ChunkSize = 8 << 20
	recorder := record.NewFakeRecorder(8)
	c := fixtureClient(t, task, ds)
	r := &Reconciler{
		Client:             c,
		Agents:             &fakeAgentPool{},
		Recorder:           recorder,
		CacheCapacityBytes: 1 << 30,
		StartupWindowBytes: 512 << 20,
		MaxFillFraction:    0.125,
	}

	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("result = %#v, want Requeue after writing K", result)
	}

	updated := getTask(t, c)
	for i := range updated.Spec.PodDetails {
		// W_i=3, K_window=64, K_cache=8 -> K_i=3.
		if got := updated.Spec.PodDetails[i].K; got != 3 {
			t.Fatalf("pod %d K = %d, want 3", i, got)
		}
	}
	if updated.Status.ConsumeOrderSource != traincachev1alpha1.ConsumeOrderSourceSpec {
		t.Fatalf("consume order source = %q", updated.Status.ConsumeOrderSource)
	}
	found := false
	for len(recorder.Events) > 0 {
		if strings.Contains(<-recorder.Events, EventKDefaulted) {
			found = true
		}
	}
	if !found {
		t.Fatal("KDefaulted event not recorded")
	}
}

func TestReconcileRejectsExplicitKAboveWindow(t *testing.T) {
	task := fixtureTask(2, 9, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1"})
	ds.Spec.ChunkSize = 8 << 20
	c := fixtureClient(t, task, ds)
	r := &Reconciler{Client: c, Agents: &fakeAgentPool{}}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	cond := metaCondition(getTask(t, c), ConditionInvalidSpec)
	if cond == nil || cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "exceeds") {
		t.Fatalf("InvalidSpec condition = %#v", cond)
	}
}

func TestReconcilePodSetMismatch(t *testing.T) {
	task := fixtureTask(2, 1, [][]int32{{0, 1}, {2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pool.submitCalls != 2 {
		t.Fatalf("submits = %d, want 2", pool.submitCalls)
	}
	if cond := metaCondition(getTask(t, c), ConditionPodSetMismatch); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("PodSetMismatch condition = %#v", cond)
	}
}

func TestReconcilePermanentRejectionSetsCondition(t *testing.T) {
	task := fixtureTask(1, 1, [][]int32{{0}})
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1", "n2"})
	pool := &fakeAgentPool{fail: map[string]error{
		"n2": fmt.Errorf("%w: stale generation", agentclient.ErrPermanent),
	}}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pool.submitCalls != 2 {
		t.Fatalf("submit calls = %d, want 2 (one per node)", pool.submitCalls)
	}
	cond := metaCondition(getTask(t, c), ConditionPreheatRejected)
	if cond == nil || cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "n2") {
		t.Fatalf("PreheatRejected condition = %#v", cond)
	}
}

func TestReconcileTotalBelowPods(t *testing.T) {
	task := fixtureTask(2, 1, [][]int32{nil, nil})
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1"})
	pool := &fakeAgentPool{}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{Client: c, Agents: pool}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pool.submitCalls != 0 {
		t.Fatalf("submits = %d, want 0", pool.submitCalls)
	}
	if cond := metaCondition(getTask(t, c), ConditionInvalidSpec); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("InvalidSpec condition = %#v", cond)
	}
}

func TestReconcileLogsRequiredFields(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1", "n2"})
	pool := &fakeAgentPool{fail: map[string]error{
		"n2": fmt.Errorf("%w: down", agentclient.ErrAgentUnavailable),
	}}
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := &Reconciler{Client: c, Agents: pool, Log: log}

	if err := reconcile(t, r); !errors.Is(err, agentclient.ErrAgentUnavailable) {
		t.Fatalf("Reconcile error = %v", err)
	}

	var messages []string
	fields := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		messages = append(messages, fmt.Sprint(entry["msg"]))
		for _, key := range []string{"consume_order_source", "task_id", "node", "duration_ms", "err"} {
			if _, ok := entry[key]; ok {
				fields[key] = true
			}
		}
	}
	joined := strings.Join(messages, "\n")
	for _, want := range []string{"consume order resolved", "candidate filtering", "agent submit", "agent submit failed", "warmup reconcile finished"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing log %q in:\n%s", want, joined)
		}
	}
	for _, key := range []string{"consume_order_source", "task_id", "node", "duration_ms", "err"} {
		if !fields[key] {
			t.Fatalf("log field %q missing", key)
		}
	}
}

func metaCondition(task *traincachev1alpha1.WarmupTask, conditionType string) *metav1.Condition {
	for i := range task.Status.Conditions {
		if task.Status.Conditions[i].Type == conditionType {
			return &task.Status.Conditions[i]
		}
	}
	return nil
}
