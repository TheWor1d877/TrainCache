package dataset

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// rawDataSetClient returns the store behind an intercepted client, so a test
// can read the persisted object while the interceptor still fails the
// controller's own reads.
func rawDataSetClient(c client.Client) client.WithWatch {
	if unwrapper, ok := c.(interface{ Unwrap() client.WithWatch }); ok {
		return unwrapper.Unwrap()
	}
	return nil
}

// TestReconcile_GetFails_NoConditionWritten pins the outage rule for reads:
// while the API server is unreachable the DataSet controller writes no
// Condition, keeps the previous status, and the next successful reconcile
// recomputes the status from the current store.
func TestReconcile_GetFails_NoConditionWritten(t *testing.T) {
	ds := newDataSet()
	ds.Status.TotalChunks = 7
	ds.Status.Conditions = []metav1.Condition{{
		Type:               ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             reasonNodesResolved,
		Message:            "snapshot frozen and nodes resolved",
		LastTransitionTime: metav1.NewTime(time.Unix(1_700_000_000, 0)),
	}}
	before := ds.Status.DeepCopy()

	var outage bool
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*traincachev1alpha1.DataSet); ok && outage {
				return apierrors.NewServiceUnavailable("apiserver unreachable")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	env := newTestEnv(t, []client.Object{
		ds,
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true"}),
	}, &fakeFactory{store: snapshotStore(t, "ds", "snap-1")}, &funcs)

	outage = true
	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err == nil {
		t.Fatal("Reconcile succeeded, want the read failure")
	}

	stored := &traincachev1alpha1.DataSet{}
	if err := rawDataSetClient(env.client).Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "ds"}, stored); err != nil {
		t.Fatalf("get DataSet: %v", err)
	}
	if !reflect.DeepEqual(stored.Status.Conditions, before.Conditions) {
		t.Fatalf("conditions = %#v, want the unchanged %#v", stored.Status.Conditions, before.Conditions)
	}
	if stored.Status.TotalChunks != before.TotalChunks {
		t.Fatalf("TotalChunks = %d, want the previous %d", stored.Status.TotalChunks, before.TotalChunks)
	}

	// The API server is reachable again: the reconcile recomputes the status.
	outage = false
	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err != nil {
		t.Fatalf("Reconcile after recovery: %v", err)
	}
	recovered := env.getDataSet(t)
	if recovered.Status.TotalChunks != 3 {
		t.Fatalf("TotalChunks = %d, want the recomputed 3", recovered.Status.TotalChunks)
	}
	if recovered.Status.SnapshotID != "snap-1" {
		t.Fatalf("SnapshotID = %q, want snap-1", recovered.Status.SnapshotID)
	}
	if cond := meta.FindStatusCondition(recovered.Status.Conditions, ConditionReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %#v, want True", cond)
	}
}

// TestReconcile_StatusUpdateFails_KeepsPreviousStatus asserts what the code
// does when the status subresource write fails after the layout and node
// resolution succeeded: the error surfaces, nothing is persisted and the
// DataSet keeps its previous status.
func TestReconcile_StatusUpdateFails_KeepsPreviousStatus(t *testing.T) {
	ds := newDataSet()
	ds.Status.TotalChunks = 7
	var failStatus bool
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "status" && failStatus {
				return apierrors.NewConflict(
					schema.GroupResource{Resource: "datasets"}, obj.GetName(),
					errors.New("the object has been modified"))
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	env := newTestEnv(t, []client.Object{
		ds,
		node("node-a", map[string]string{AccessibleNodeLabelKey: "true"}),
	}, &fakeFactory{store: snapshotStore(t, "ds", "snap-1")}, &funcs)

	failStatus = true
	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err == nil {
		t.Fatal("Reconcile succeeded, want the status write failure")
	}
	failStatus = false

	stored := &traincachev1alpha1.DataSet{}
	if err := rawDataSetClient(env.client).Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "ds"}, stored); err != nil {
		t.Fatalf("get DataSet: %v", err)
	}
	if stored.Status.TotalChunks != 7 {
		t.Fatalf("TotalChunks = %d, want the previous 7", stored.Status.TotalChunks)
	}
	if stored.Status.SnapshotID != "" {
		t.Fatalf("SnapshotID = %q, want the previous empty value", stored.Status.SnapshotID)
	}
	if len(stored.Status.Conditions) != 0 {
		t.Fatalf("conditions = %#v, want none", stored.Status.Conditions)
	}

	// The next reconcile writes the status once the write works again.
	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err != nil {
		t.Fatalf("Reconcile after recovery: %v", err)
	}
	recovered := env.getDataSet(t)
	if recovered.Status.TotalChunks != 3 {
		t.Fatalf("TotalChunks = %d, want the recomputed 3", recovered.Status.TotalChunks)
	}
}
