package warmup

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/scorefmt"
)

// failureClient builds a fake client whose API calls are routed through the
// given interceptor funcs, so a failure can be injected at one exact call site.
func failureClient(t *testing.T, objs []client.Object, funcs interceptor.Funcs) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}, &traincachev1alpha1.DataSet{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
}

// rawClient returns the store behind an intercepted client, so a test can read
// the persisted object while the interceptor still fails the controller's own
// reads.
func rawClient(c client.Client) client.WithWatch {
	if unwrapper, ok := c.(interface{ Unwrap() client.WithWatch }); ok {
		return unwrapper.Unwrap()
	}
	return nil
}

// podPatchCounter records every Pod patch the controller issued.
type podPatchCounter struct {
	mu    sync.Mutex
	calls map[string]int
}

func (c *podPatchCounter) record(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[name]++
}

func (c *podPatchCounter) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[name]
}

// TestReconcile_ScoreAnnotationWriteFails_KeepsGate pins the failure promise of
// docs/7.1-design.md §7.5: when the Pod patch that carries the score
// annotation fails, the Pod keeps its scheduling gate, its annotations stay
// untouched, the reconcile reports the error instead of silently succeeding,
// and a Pod that was already released is not rolled back.
func TestReconcile_ScoreAnnotationWriteFails_KeepsGate(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "forbidden",
			err: apierrors.NewForbidden(
				schema.GroupResource{Resource: "pods"}, "trainer-1",
				errors.New("rbac denied")),
		},
		{
			name: "too many requests",
			err:  apierrors.NewTooManyRequests("apiserver throttled the request", 1),
		},
		{
			name: "conflict",
			err: apierrors.NewConflict(
				schema.GroupResource{Resource: "pods"}, "trainer-1",
				errors.New("the object has been modified")),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := fixtureTask(2, 2, [][]int32{{0, 1}, {2, 3}})
			ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2"})
			pool := newFakeProgressPool()
			for _, node := range []string{"n1", "n2"} {
				for _, pod := range []string{"train/t1/p0", "train/t1/p1"} {
					pool.set(pod, node, doneResponse(3, fullBitmap(2)))
				}
			}
			kept := gatedPod(1)
			kept.Annotations = map[string]string{"keep": "me"}
			counter := &podPatchCounter{}
			funcs := interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if pod, ok := obj.(*corev1.Pod); ok {
						counter.record(pod.Name)
						if pod.Name == "trainer-1" {
							return tc.err
						}
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			}
			c := failureClient(t, []client.Object{
				task, ds, fixtureNode("n1"), fixtureNode("n2"),
				fixtureAgentPod("n1"), fixtureAgentPod("n2"),
				gatedPod(0), kept,
			}, funcs)
			r := &Reconciler{Client: c, Agents: pool, Now: func() time.Time { return time.Unix(100, 0) }}

			if _, err := reconcileResult(t, r); err == nil {
				t.Fatal("Reconcile succeeded, want the Pod patch failure")
			}

			failed := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-1"}, failed); err != nil {
				t.Fatalf("get pod trainer-1: %v", err)
			}
			if !hasWarmupGate(failed) {
				t.Fatal("the failed Pod lost its scheduling gate")
			}
			if _, ok := failed.Annotations[scorefmt.DefaultAnnotation]; ok {
				t.Fatalf("score annotation %q was written although the patch failed", scorefmt.DefaultAnnotation)
			}
			if !reflect.DeepEqual(failed.Annotations, map[string]string{"keep": "me"}) {
				t.Fatalf("pod annotations = %v, want the unchanged original map", failed.Annotations)
			}

			updated := getTask(t, c)
			if updated.Status.Phase == traincachev1alpha1.WarmupPhaseReleased {
				t.Fatal("task was released although the score annotation write failed")
			}
			if updated.Status.GateDecision.State != "" {
				t.Fatalf("gate decision = %q, want empty", updated.Status.GateDecision.State)
			}

			// The Pod whose patch succeeded was released; it must stay released.
			released := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-0"}, released); err != nil {
				t.Fatalf("get pod trainer-0: %v", err)
			}
			if hasWarmupGate(released) {
				t.Fatal("the released Pod kept its gate")
			}
			score, ok := released.Annotations[scorefmt.DefaultAnnotation]
			if !ok {
				t.Fatalf("released Pod misses the score annotation %q", scorefmt.DefaultAnnotation)
			}

			// The next reconcile retries only the remaining gate.
			if _, err := reconcileResult(t, r); err == nil {
				t.Fatal("second Reconcile succeeded, want the Pod patch failure")
			}
			again := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "trainer-0"}, again); err != nil {
				t.Fatalf("get pod trainer-0 again: %v", err)
			}
			if hasWarmupGate(again) {
				t.Fatal("the released Pod was rolled back to a gated Pod")
			}
			if got := again.Annotations[scorefmt.DefaultAnnotation]; got != score {
				t.Fatalf("released Pod score annotation = %q, want the frozen %q", got, score)
			}
			if counter.count("trainer-0") != 1 {
				t.Fatalf("patches on the released Pod = %d, want 1 (the released Pod is skipped)", counter.count("trainer-0"))
			}
			if counter.count("trainer-1") != 2 {
				t.Fatalf("patches on the failed Pod = %d, want 2 (one per reconcile)", counter.count("trainer-1"))
			}
		})
	}
}

// TestReconcile_TaskGetFails_NoConditionWritten pins the outage rule for
// reads: while the API server is unreachable the controller writes no
// Condition and keeps the previous phase of the WarmupTask.
func TestReconcile_TaskGetFails_NoConditionWritten(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Status.Phase = traincachev1alpha1.WarmupPhaseGating
	task.Status.ObservedGeneration = task.Generation
	task.Status.Conditions = []metav1.Condition{{
		Type:               ConditionFirstReady,
		Status:             metav1.ConditionTrue,
		Reason:             reasonFirstReady,
		Message:            "pod 0 reached required ready blocks on node n1",
		LastTransitionTime: metav1.NewTime(time.Unix(100, 0)),
	}}
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*traincachev1alpha1.WarmupTask); ok {
				return apierrors.NewServiceUnavailable("apiserver unreachable")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	c := failureClient(t, []client.Object{task, ds, fixtureNode("n1"), fixtureAgentPod("n1")}, funcs)
	r := &Reconciler{Client: c, Agents: newFakeProgressPool(), Now: func() time.Time { return time.Unix(100, 0) }}

	if _, err := reconcileResult(t, r); err == nil {
		t.Fatal("Reconcile succeeded, want the read failure")
	}

	stored := &traincachev1alpha1.WarmupTask{}
	if err := rawClient(c).Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, stored); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if stored.Status.Phase != traincachev1alpha1.WarmupPhaseGating {
		t.Fatalf("phase = %q, want the previous %q", stored.Status.Phase, traincachev1alpha1.WarmupPhaseGating)
	}
	if !reflect.DeepEqual(stored.Status.Conditions, task.Status.Conditions) {
		t.Fatalf("conditions = %#v, want the unchanged %#v", stored.Status.Conditions, task.Status.Conditions)
	}
}

// TestReconcile_NodeListFails_NoConditionWritten_RecomputesAfterRecovery pins
// the outage rule for list reads: the failure path writes no Condition, keeps
// the previous phase, and the next successful reconcile recomputes the
// candidate set from the fresh list.
func TestReconcile_NodeListFails_NoConditionWritten_RecomputesAfterRecovery(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Status.Phase = traincachev1alpha1.WarmupPhaseGating
	task.Status.ObservedGeneration = task.Generation
	task.Status.Conditions = []metav1.Condition{{
		Type:               ConditionFirstReady,
		Status:             metav1.ConditionTrue,
		Reason:             reasonFirstReady,
		Message:            "pod 0 reached required ready blocks on node n1",
		LastTransitionTime: metav1.NewTime(time.Unix(100, 0)),
	}}
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1", "n2"})
	before := task.Status.DeepCopy()

	var outage bool
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.NodeList); ok && outage {
				return apierrors.NewServiceUnavailable("apiserver unreachable")
			}
			return c.List(ctx, list, opts...)
		},
	}
	c := failureClient(t, []client.Object{
		task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0),
	}, funcs)
	r := &Reconciler{Client: c, Agents: &fakeAgentPool{}, Now: func() time.Time { return time.Unix(100, 0) }}

	outage = true
	if _, err := reconcileResult(t, r); err == nil {
		t.Fatal("Reconcile succeeded, want the list failure")
	}
	stored := &traincachev1alpha1.WarmupTask{}
	if err := rawClient(c).Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, stored); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if stored.Status.Phase != traincachev1alpha1.WarmupPhaseGating {
		t.Fatalf("phase = %q, want the previous %q", stored.Status.Phase, traincachev1alpha1.WarmupPhaseGating)
	}
	if !reflect.DeepEqual(stored.Status.Conditions, before.Conditions) {
		t.Fatalf("conditions = %#v, want the unchanged %#v", stored.Status.Conditions, before.Conditions)
	}
	storedDataSet := &traincachev1alpha1.DataSet{}
	if err := rawClient(c).Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "ds1"}, storedDataSet); err != nil {
		t.Fatalf("get dataset: %v", err)
	}
	if len(storedDataSet.Status.Conditions) != len(ds.Status.Conditions) {
		t.Fatalf("DataSet conditions = %#v, want the unchanged %#v", storedDataSet.Status.Conditions, ds.Status.Conditions)
	}

	// The API server is reachable again and a second Node appeared.
	outage = false
	if err := c.Create(context.Background(), fixtureNode("n2")); err != nil {
		t.Fatalf("create node n2: %v", err)
	}
	if err := c.Create(context.Background(), fixtureAgentPod("n2")); err != nil {
		t.Fatalf("create agent pod n2: %v", err)
	}
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile after recovery: %v", err)
	}
	recovered := getTask(t, c)
	if got := fmt.Sprint(recovered.Status.CandidateNodes); got != "[n1 n2]" {
		t.Fatalf("candidate nodes = %v, want the fresh list [n1 n2]", recovered.Status.CandidateNodes)
	}
}

// TestReconcile_StatusWriteFails_KeepsSpecPatch asserts what the code does
// when the status subresource write fails after the spec side work succeeded:
// the spec patch stays, the status is lost and the failure surfaces as the
// reconcile error.
func TestReconcile_StatusWriteFails_KeepsSpecPatch(t *testing.T) {
	task := fixtureTask(2, 0, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1"})
	ds.Spec.ChunkSize = 8 << 20
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if sub == "status" {
				return apierrors.NewConflict(
					schema.GroupResource{Resource: "warmuptasks"}, obj.GetName(),
					errors.New("the object has been modified"))
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
	c := failureClient(t, []client.Object{task, ds}, funcs)
	r := &Reconciler{
		Client:             c,
		Agents:             &fakeAgentPool{},
		CacheCapacityBytes: 1 << 30,
		StartupWindowBytes: 512 << 20,
		MaxFillFraction:    0.125,
		Now:                func() time.Time { return time.Unix(100, 0) },
	}

	result, err := reconcileResult(t, r)
	if err == nil {
		t.Fatal("Reconcile succeeded, want the status write failure")
	}
	if !result.Requeue {
		t.Fatalf("result = %#v, want a requeue next to the error", result)
	}

	stored := &traincachev1alpha1.WarmupTask{}
	if err := rawClient(c).Get(context.Background(), types.NamespacedName{Namespace: "train", Name: "t1"}, stored); err != nil {
		t.Fatalf("get task: %v", err)
	}
	for i := range stored.Spec.PodDetails {
		if stored.Spec.PodDetails[i].K == 0 {
			t.Fatalf("pod %d K = 0, want the spec side patch to survive", i)
		}
	}
	if stored.Status.Phase != "" {
		t.Fatalf("phase = %q, want the lost status write to leave it empty", stored.Status.Phase)
	}
	if len(stored.Status.Conditions) != 0 {
		t.Fatalf("conditions = %#v, want none", stored.Status.Conditions)
	}
}
