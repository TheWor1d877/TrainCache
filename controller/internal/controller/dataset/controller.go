// Package dataset reconciles DataSet objects: spec validation, snapshot
// resolution, frozen block layout, accessible-node discovery and status
// conditions (docs/7.1 design.md §5.2 and §7.1).
package dataset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/logging"
	"traincache/controller/internal/metrics"
	"traincache/controller/internal/storage"
)

// Condition types written to DataSet.status.
const (
	ConditionInvalidSpec        = "InvalidSpec"
	ConditionStorageUnreachable = "StorageUnreachable"
	ConditionSnapshotNotFound   = "SnapshotNotFound"
	ConditionNoAccessibleNodes  = "NoAccessibleNodes"
	ConditionReady              = "Ready"
)

// Condition reasons.
const (
	reasonInvalidSpec        = "InvalidSpec"
	reasonSpecValid          = "SpecValid"
	reasonStorageUnreachable = "StorageUnreachable"
	reasonStorageReachable   = "StorageReachable"
	reasonSnapshotNotFound   = "SnapshotNotFound"
	reasonSnapshotInvalid    = "InvalidSnapshot"
	reasonSnapshotResolved   = "SnapshotResolved"
	reasonNoAccessibleNodes  = "NoAccessibleNodes"
	reasonNodesResolved      = "NodesResolved"
)

const defaultMaxChunks int64 = 10_000_000

// ErrNoAccessibleNodes requests controller-runtime backoff: Node labels may be
// added later by a human (D30), so the reconcile is retried.
var ErrNoAccessibleNodes = errors.New("dataset: no accessible nodes")

// Reconciler reconciles DataSet objects. Reconciles are serialized by
// MaxConcurrentReconciles=1 (D24).
type Reconciler struct {
	client.Client
	StoreFactory storage.Factory
	Recorder     record.EventRecorder
	Log          *slog.Logger
	Metrics      *metrics.Metrics
	MaxChunks    int64
	Now          func() time.Time
}

// SetupWithManager registers the DataSet reconciler with the manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&traincachev1alpha1.DataSet{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// Reconcile implements the DataSet state machine.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	start := r.now()
	// Registered before the first Get so every return path, including
	// NotFound and DeletionTimestamp, is observed exactly once.
	defer func() {
		r.Metrics.ObserveReconcile(metrics.ControllerDataset, retErr, result.RequeueAfter, r.now().Sub(start))
	}()

	ds := &traincachev1alpha1.DataSet{}
	if err := r.Get(ctx, req.NamespacedName, ds); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !ds.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	requestID := fmt.Sprintf("%s/%d/%d", ds.UID, ds.Generation, start.UnixNano())
	log := r.logger(ctx).With(
		"dataset", ds.Name,
		"namespace", ds.Namespace,
		"generation", ds.Generation,
		"request_id", requestID,
	)
	log.Info("dataset reconcile start")
	defer func() {
		fields := []any{"duration_ms", r.now().Sub(start).Milliseconds()}
		if retErr != nil {
			fields = append(fields, "err", retErr)
		}
		log.Info("dataset reconcile finished", fields...)
	}()

	if reasons := ValidateSpec(ds.Spec, r.maxChunks()); len(reasons) > 0 {
		message := strings.Join(reasons, "; ")
		log.Warn("dataset spec invalid", "reason", reasonInvalidSpec, "err", message)
		r.event(ds, corev1.EventTypeWarning, reasonInvalidSpec, message)
		r.setCondition(ds, ConditionInvalidSpec, metav1.ConditionFalse, reasonInvalidSpec, message)
		r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonInvalidSpec, message)
		return ctrl.Result{}, r.writeStatus(ctx, ds, log)
	}
	r.setCondition(ds, ConditionInvalidSpec, metav1.ConditionTrue, reasonSpecValid, "spec is valid")

	if r.StoreFactory == nil {
		err := fmt.Errorf("%w: storage factory is not configured", storage.ErrStoreUnavailable)
		return ctrl.Result{}, r.storageFailure(ctx, ds, log, err)
	}
	store, err := r.StoreFactory.Store(ctx, r.Client, ds.Namespace, ds.Spec.SecretRef, ds.Spec.Endpoint, ds.Spec.Bucket)
	if err != nil {
		log.Warn("dataset storage unavailable", "reason", reasonStorageUnreachable, "err", err)
		r.event(ds, corev1.EventTypeWarning, reasonStorageUnreachable, err.Error())
		r.setCondition(ds, ConditionStorageUnreachable, metav1.ConditionFalse, reasonStorageUnreachable, err.Error())
		r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonStorageUnreachable, err.Error())
		if serr := r.writeStatus(ctx, ds, log); serr != nil {
			log.Error("dataset status update failed", "err", serr)
		}
		return ctrl.Result{}, err
	}
	r.setCondition(ds, ConditionStorageUnreachable, metav1.ConditionTrue, reasonStorageReachable, "object store is reachable")

	snap, err := storage.ResolveSnapshot(ctx, store, ds.Spec.DatasetName, ds.Spec.SnapshotID)
	if err != nil {
		if errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Warn("dataset snapshot not found",
				"snapshot", ds.Spec.SnapshotID, "reason", reasonSnapshotNotFound, "err", err)
			r.event(ds, corev1.EventTypeWarning, reasonSnapshotNotFound, err.Error())
			r.setCondition(ds, ConditionSnapshotNotFound, metav1.ConditionFalse, reasonSnapshotNotFound, err.Error())
			r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonSnapshotNotFound, err.Error())
			return ctrl.Result{}, r.writeStatus(ctx, ds, log)
		}
		if errors.Is(err, storage.ErrInvalidSnapshot) {
			// Invalid snapshot metadata is permanent: retrying cannot fix it
			// (docs/7.1 design.md §5.2 error semantics).
			log.Warn("dataset snapshot invalid",
				"snapshot", ds.Spec.SnapshotID, "reason", reasonSnapshotInvalid, "err", err)
			r.event(ds, corev1.EventTypeWarning, reasonSnapshotInvalid, err.Error())
			r.setCondition(ds, ConditionSnapshotNotFound, metav1.ConditionFalse, reasonSnapshotInvalid, err.Error())
			r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonSnapshotInvalid, err.Error())
			return ctrl.Result{}, r.writeStatus(ctx, ds, log)
		}
		log.Warn("dataset snapshot validation failed",
			"snapshot", ds.Spec.SnapshotID, "reason", reasonStorageUnreachable, "err", err)
		r.event(ds, corev1.EventTypeWarning, reasonStorageUnreachable, err.Error())
		r.setCondition(ds, ConditionStorageUnreachable, metav1.ConditionFalse, reasonStorageUnreachable, err.Error())
		r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonStorageUnreachable, err.Error())
		if serr := r.writeStatus(ctx, ds, log); serr != nil {
			log.Error("dataset status update failed", "err", serr)
		}
		return ctrl.Result{}, err
	}
	r.setCondition(ds, ConditionSnapshotNotFound, metav1.ConditionTrue, reasonSnapshotResolved, "snapshot resolved")
	log.Info("dataset snapshot resolved", "snapshot", snap.ID, "manifest_shards", len(snap.Manifest.Shards))

	totalChunks, blockLayout, err := BuildLayout(ds.Spec.Shards)
	if err != nil {
		log.Warn("dataset block layout invalid", "reason", reasonInvalidSpec, "err", err)
		r.event(ds, corev1.EventTypeWarning, reasonInvalidSpec, err.Error())
		r.setCondition(ds, ConditionInvalidSpec, metav1.ConditionFalse, reasonInvalidSpec, err.Error())
		r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonInvalidSpec, err.Error())
		return ctrl.Result{}, r.writeStatus(ctx, ds, log)
	}
	log.Info("dataset block layout frozen", "shards", len(blockLayout), "total_chunks", totalChunks)

	nodes, err := ResolveAccessibleNodes(ctx, r.Client, ds.Spec.AccessibleNodeSelector)
	if err != nil {
		log.Error("dataset accessible nodes resolution failed", "reason", reasonNoAccessibleNodes, "err", err)
		return ctrl.Result{}, err
	}
	log.Info("dataset accessible nodes resolved", "count", len(nodes))

	ds.Status.SnapshotID = snap.ID
	ds.Status.TotalChunks = totalChunks
	ds.Status.BlockLayout = blockLayout
	ds.Status.AccessibleNodes = nodes

	if len(nodes) == 0 {
		message := "no Nodes carry " + AccessibleNodeLabelKey + "=" + AccessibleNodeLabelValue
		log.Warn("dataset has no accessible nodes", "reason", reasonNoAccessibleNodes)
		r.event(ds, corev1.EventTypeWarning, reasonNoAccessibleNodes, message)
		r.setCondition(ds, ConditionNoAccessibleNodes, metav1.ConditionFalse, reasonNoAccessibleNodes, message)
		r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonNoAccessibleNodes, message)
		if serr := r.writeStatus(ctx, ds, log); serr != nil {
			log.Error("dataset status update failed", "err", serr)
		}
		return ctrl.Result{}, ErrNoAccessibleNodes
	}

	meta.RemoveStatusCondition(&ds.Status.Conditions, ConditionNoAccessibleNodes)
	r.setCondition(ds, ConditionReady, metav1.ConditionTrue, reasonNodesResolved, "snapshot frozen and nodes resolved")
	if err := r.writeStatus(ctx, ds, log); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) storageFailure(ctx context.Context, ds *traincachev1alpha1.DataSet, log *slog.Logger, err error) error {
	log.Warn("dataset storage unavailable", "reason", reasonStorageUnreachable, "err", err)
	r.event(ds, corev1.EventTypeWarning, reasonStorageUnreachable, err.Error())
	r.setCondition(ds, ConditionStorageUnreachable, metav1.ConditionFalse, reasonStorageUnreachable, err.Error())
	r.setCondition(ds, ConditionReady, metav1.ConditionFalse, reasonStorageUnreachable, err.Error())
	if serr := r.writeStatus(ctx, ds, log); serr != nil {
		log.Error("dataset status update failed", "err", serr)
	}
	return err
}

func (r *Reconciler) writeStatus(ctx context.Context, ds *traincachev1alpha1.DataSet, log *slog.Logger) error {
	ds.Status.ObservedGeneration = ds.Generation
	if err := r.Status().Update(ctx, ds); err != nil {
		log.Error("dataset status update failed", "err", err)
		return err
	}
	return nil
}

func (r *Reconciler) setCondition(ds *traincachev1alpha1.DataSet, conditionType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&ds.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: ds.Generation,
		LastTransitionTime: metav1.NewTime(r.now()),
	})
}

func (r *Reconciler) event(ds *traincachev1alpha1.DataSet, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(ds, eventType, reason, message)
	}
}

func (r *Reconciler) logger(ctx context.Context) *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return logging.From(ctx)
}

func (r *Reconciler) maxChunks() int64 {
	if r.MaxChunks > 0 {
		return r.MaxChunks
	}
	return defaultMaxChunks
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
