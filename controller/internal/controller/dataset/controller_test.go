package dataset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/storage"
	"traincache/core/index"
	"traincache/core/objectstore"
)

func TestReconcileInvalidSpecSetsConditionWithoutError(t *testing.T) {
	ds := newDataSet()
	ds.Spec.ChunkSize = 0
	env := newTestEnv(t, []client.Object{ds}, nil, nil)

	_, err := env.reconciler.Reconcile(context.Background(), requestFor(ds))
	if err != nil {
		t.Fatalf("Reconcile invalid spec returned error: %v", err)
	}

	got := env.getDataSet(t)
	if cond := findCondition(t, got, ConditionInvalidSpec); cond.Status != metav1.ConditionFalse {
		t.Fatalf("InvalidSpec status = %q, want False", cond.Status)
	}
	if cond := findCondition(t, got, ConditionReady); cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready status = %q, want False", cond.Status)
	}
	if got.Status.ObservedGeneration != got.Generation {
		t.Fatalf("ObservedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
	}

	line := findLogLine(t, env.logLines(t), "dataset spec invalid")
	if line["level"] != "WARN" || line["dataset"] != "ds" || line["reason"] != reasonInvalidSpec {
		t.Fatalf("validation log fields = %#v", line)
	}
}

func TestReconcileSnapshotNotFound(t *testing.T) {
	ds := newDataSet()
	env := newTestEnv(t, []client.Object{ds}, &fakeFactory{store: objectstore.NewMock()}, nil)

	_, err := env.reconciler.Reconcile(context.Background(), requestFor(ds))
	if err != nil {
		t.Fatalf("Reconcile snapshot missing returned error: %v", err)
	}
	got := env.getDataSet(t)
	if cond := findCondition(t, got, ConditionSnapshotNotFound); cond.Status != metav1.ConditionFalse {
		t.Fatalf("SnapshotNotFound status = %q, want False", cond.Status)
	}
	findLogLine(t, env.logLines(t), "dataset snapshot not found")
}

func TestReconcileInvalidSnapshotSetsConditionWithoutError(t *testing.T) {
	ds := newDataSet()
	store := objectstore.NewMock()
	current := "not-a-valid-current"
	if _, err := store.Put(context.Background(), index.CurrentKey("ds"), strings.NewReader(current), int64(len(current))); err != nil {
		t.Fatalf("put CURRENT: %v", err)
	}
	env := newTestEnv(t, []client.Object{ds}, &fakeFactory{store: store}, nil)

	result, err := env.reconciler.Reconcile(context.Background(), requestFor(ds))
	if err != nil {
		t.Fatalf("Reconcile invalid snapshot returned error: %v", err)
	}
	if result != (ctrl.Result{}) {
		t.Fatalf("Reconcile result = %#v, want empty", result)
	}

	got := env.getDataSet(t)
	if cond := findCondition(t, got, ConditionSnapshotNotFound); cond.Status != metav1.ConditionFalse || cond.Reason != reasonSnapshotInvalid {
		t.Fatalf("SnapshotNotFound = %#v, want False/%s", cond, reasonSnapshotInvalid)
	}
	if cond := findCondition(t, got, ConditionReady); cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready status = %q, want False", cond.Status)
	}
	if cond := findCondition(t, got, ConditionStorageUnreachable); cond.Status != metav1.ConditionTrue {
		t.Fatalf("StorageUnreachable status = %q, want True", cond.Status)
	}

	line := findLogLine(t, env.logLines(t), "dataset snapshot invalid")
	if line["level"] != "WARN" || line["reason"] != reasonSnapshotInvalid {
		t.Fatalf("invalid snapshot log fields = %#v", line)
	}
	if _, ok := line["snapshot"]; !ok {
		t.Fatalf("invalid snapshot log missing snapshot field: %#v", line)
	}
	if _, ok := line["err"]; !ok {
		t.Fatalf("invalid snapshot log missing err field: %#v", line)
	}

	select {
	case event := <-env.events.Events:
		if !strings.Contains(event, reasonSnapshotInvalid) {
			t.Fatalf("event = %q, want reason %s", event, reasonSnapshotInvalid)
		}
	default:
		t.Fatal("no InvalidSnapshot event recorded")
	}
}

func TestReconcileStorageUnavailableReturnsError(t *testing.T) {
	ds := newDataSet()
	env := newTestEnv(t, []client.Object{ds}, &fakeFactory{err: storage.ErrSecretNotFound}, nil)

	_, err := env.reconciler.Reconcile(context.Background(), requestFor(ds))
	if !errors.Is(err, storage.ErrSecretNotFound) {
		t.Fatalf("Reconcile err = %v, want ErrSecretNotFound", err)
	}
	got := env.getDataSet(t)
	if cond := findCondition(t, got, ConditionStorageUnreachable); cond.Status != metav1.ConditionFalse {
		t.Fatalf("StorageUnreachable status = %q, want False", cond.Status)
	}
}

func TestReconcileFreezesLayoutAndResolvesNodes(t *testing.T) {
	ds := newDataSet()
	ds.Spec.Shards = []traincachev1alpha1.ShardSpec{
		{Name: "a", Count: 2, LastChunkSize: 100},
		{Name: "b", Count: 3, LastChunkSize: 40},
		{Name: "c", Count: 2, LastChunkSize: 40},
	}
	env := newTestEnv(t, []client.Object{
		ds,
		node("node-b", map[string]string{AccessibleNodeLabelKey: "true"}),
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true"}),
		node("node-c", map[string]string{AccessibleNodeLabelKey: "false"}),
	}, &fakeFactory{store: snapshotStore(t, "ds", "snap-1")}, nil)

	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := env.getDataSet(t)

	if got.Status.SnapshotID != "snap-1" || got.Status.TotalChunks != 7 {
		t.Fatalf("status snapshot/total = (%q,%d), want (snap-1,7)", got.Status.SnapshotID, got.Status.TotalChunks)
	}
	wantStarts := []int32{0, 2, 5}
	if len(got.Status.BlockLayout) != len(wantStarts) {
		t.Fatalf("BlockLayout = %#v", got.Status.BlockLayout)
	}
	for i, start := range wantStarts {
		if got.Status.BlockLayout[i].StartChunk != start {
			t.Fatalf("BlockLayout[%d].StartChunk = %d, want %d", i, got.Status.BlockLayout[i].StartChunk, start)
		}
	}
	if want := []string{"node-a", "node-b"}; !reflect.DeepEqual(got.Status.AccessibleNodes, want) {
		t.Fatalf("AccessibleNodes = %v, want %v", got.Status.AccessibleNodes, want)
	}
	for _, typ := range []string{ConditionReady, ConditionInvalidSpec, ConditionSnapshotNotFound, ConditionStorageUnreachable} {
		if cond := findCondition(t, got, typ); cond.Status != metav1.ConditionTrue {
			t.Fatalf("%s status = %q, want True", typ, cond.Status)
		}
	}
	if meta.FindStatusCondition(got.Status.Conditions, ConditionNoAccessibleNodes) != nil {
		t.Fatalf("stale NoAccessibleNodes condition: %#v", got.Status.Conditions)
	}

	layoutLog := findLogLine(t, env.logLines(t), "dataset block layout frozen")
	if layoutLog["total_chunks"] != float64(7) || layoutLog["shards"] != float64(3) {
		t.Fatalf("layout log = %#v", layoutLog)
	}
	nodesLog := findLogLine(t, env.logLines(t), "dataset accessible nodes resolved")
	if nodesLog["count"] != float64(2) {
		t.Fatalf("nodes log = %#v", nodesLog)
	}
}

func TestReconcileNarrowsAccessibleNodes(t *testing.T) {
	ds := newDataSet()
	ds.Spec.AccessibleNodeSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"zone": "a"}}
	env := newTestEnv(t, []client.Object{
		ds,
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true", "zone": "a"}),
		node("node-b", map[string]string{AccessibleNodeLabelKey: "true", "zone": "b"}),
	}, &fakeFactory{store: snapshotStore(t, "ds", "snap-1")}, nil)

	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := env.getDataSet(t)
	if want := []string{"node-a"}; !reflect.DeepEqual(got.Status.AccessibleNodes, want) {
		t.Fatalf("AccessibleNodes = %v, want %v", got.Status.AccessibleNodes, want)
	}
}

func TestReconcileNoAccessibleNodesRequiresBackoff(t *testing.T) {
	ds := newDataSet()
	env := newTestEnv(t, []client.Object{ds}, &fakeFactory{store: snapshotStore(t, "ds", "snap-1")}, nil)

	_, err := env.reconciler.Reconcile(context.Background(), requestFor(ds))
	if !errors.Is(err, ErrNoAccessibleNodes) {
		t.Fatalf("Reconcile err = %v, want ErrNoAccessibleNodes", err)
	}
	got := env.getDataSet(t)
	if cond := findCondition(t, got, ConditionNoAccessibleNodes); cond.Status != metav1.ConditionFalse {
		t.Fatalf("NoAccessibleNodes status = %q, want False", cond.Status)
	}
	if cond := findCondition(t, got, ConditionReady); cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready status = %q, want False", cond.Status)
	}
	if got.Status.TotalChunks != 3 {
		t.Fatalf("TotalChunks = %d, want 3", got.Status.TotalChunks)
	}

	select {
	case event := <-env.events.Events:
		if !strings.Contains(event, reasonNoAccessibleNodes) {
			t.Fatalf("event = %q, want reason %s", event, reasonNoAccessibleNodes)
		}
	default:
		t.Fatal("no NoAccessibleNodes event recorded")
	}
}

func TestReconcileNeverWritesNodes(t *testing.T) {
	ds := newDataSet()
	var mutated bool
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			checkNodeMutation(t, obj, &mutated)
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			checkNodeMutation(t, obj, &mutated)
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			checkNodeMutation(t, obj, &mutated)
			return c.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			checkNodeMutation(t, obj, &mutated)
			return c.Delete(ctx, obj, opts...)
		},
		DeleteAllOf: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteAllOfOption) error {
			checkNodeMutation(t, obj, &mutated)
			return c.DeleteAllOf(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			checkNodeMutation(t, obj, &mutated)
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	env := newTestEnv(t, []client.Object{
		ds,
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true"}),
	}, &fakeFactory{store: snapshotStore(t, "ds", "snap-1")}, &funcs)

	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if mutated {
		t.Fatal("controller issued a write against a Node")
	}
}

func TestReconcileLogsRequestLifecycle(t *testing.T) {
	ds := newDataSet()
	env := newTestEnv(t, []client.Object{
		ds,
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true"}),
	}, &fakeFactory{store: snapshotStore(t, "ds", "snap-1")}, nil)

	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	lines := env.logLines(t)
	start := findLogLine(t, lines, "dataset reconcile start")
	finish := findLogLine(t, lines, "dataset reconcile finished")
	for _, line := range []map[string]any{start, finish} {
		if line["request_id"] == "" || line["dataset"] != "ds" || line["namespace"] != "default" {
			t.Fatalf("lifecycle log fields = %#v", line)
		}
		if line["generation"] != float64(ds.Generation) {
			t.Fatalf("generation field = %#v, want %d", line["generation"], ds.Generation)
		}
	}
	if _, ok := finish["duration_ms"]; !ok {
		t.Fatalf("finish log missing duration_ms: %#v", finish)
	}
}

type testEnv struct {
	reconciler *Reconciler
	client     client.Client
	logs       *bytes.Buffer
	events     *record.FakeRecorder
}

func newTestEnv(t *testing.T, objects []client.Object, factory storage.Factory, intercept *interceptor.Funcs) *testEnv {
	t.Helper()
	builder := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithStatusSubresource(&traincachev1alpha1.DataSet{}).
		WithObjects(objects...)
	if intercept != nil {
		builder = builder.WithInterceptorFuncs(*intercept)
	}
	c := builder.Build()

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	recorder := record.NewFakeRecorder(10)
	return &testEnv{
		reconciler: &Reconciler{
			Client:       c,
			StoreFactory: factory,
			Recorder:     recorder,
			Log:          log,
			MaxChunks:    1000,
			Now:          func() time.Time { return time.Unix(1_700_000_000, 0) },
		},
		client: c,
		logs:   &buf,
		events: recorder,
	}
}

func (e *testEnv) getDataSet(t *testing.T) *traincachev1alpha1.DataSet {
	t.Helper()
	var ds traincachev1alpha1.DataSet
	if err := e.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ds"}, &ds); err != nil {
		t.Fatalf("get DataSet: %v", err)
	}
	return &ds
}

func (e *testEnv) logLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(e.logs.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("invalid log line %q: %v", raw, err)
		}
		out = append(out, line)
	}
	return out
}

type fakeFactory struct {
	store objectstore.Store
	err   error
}

func (f *fakeFactory) Store(context.Context, client.Reader, string, corev1.LocalObjectReference, string, string) (objectstore.Store, error) {
	return f.store, f.err
}

func (f *fakeFactory) Close() error { return nil }

func newDataSet() *traincachev1alpha1.DataSet {
	return &traincachev1alpha1.DataSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "ds",
			UID:        types.UID("uid-1"),
			Generation: 2,
		},
		Spec: validSpec(),
	}
}

func requestFor(ds *traincachev1alpha1.DataSet) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ds.Namespace, Name: ds.Name}}
}

func snapshotStore(t *testing.T, dataset, snapID string) objectstore.Store {
	t.Helper()
	store := objectstore.NewMock()
	manifest, err := json.Marshal(index.Manifest{
		SnapshotID: snapID,
		CreatedAt:  time.Unix(1_700_000_000, 0),
		Shards:     []index.ShardRef{{Key: "dataset/ds/snapshots/x/shard-0011223344556677.idx", Entries: 1}},
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Put(ctx, index.CurrentKey(dataset), strings.NewReader(snapID+" 0011223344556677"), int64(len(snapID)+17)); err != nil {
		t.Fatalf("put CURRENT: %v", err)
	}
	if _, err := store.Put(ctx, index.ManifestKey(dataset, snapID), bytes.NewReader(manifest), int64(len(manifest))); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	return store
}

func findCondition(t *testing.T, ds *traincachev1alpha1.DataSet, conditionType string) metav1.Condition {
	t.Helper()
	cond := meta.FindStatusCondition(ds.Status.Conditions, conditionType)
	if cond == nil {
		t.Fatalf("condition %s missing: %#v", conditionType, ds.Status.Conditions)
	}
	return *cond
}

func findLogLine(t *testing.T, lines []map[string]any, msg string) map[string]any {
	t.Helper()
	for _, line := range lines {
		if line["msg"] == msg {
			return line
		}
	}
	t.Fatalf("log %q not found in %#v", msg, lines)
	return nil
}

func checkNodeMutation(t *testing.T, obj client.Object, mutated *bool) {
	t.Helper()
	if _, ok := obj.(*corev1.Node); ok {
		*mutated = true
		t.Errorf("controller wrote Node %q", obj.GetName())
	}
}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	if err := traincachev1alpha1.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}
