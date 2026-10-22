// Package warmup implements the WarmupTask reconcile loop for M4: candidate
// filtering, consume-order resolution, window expansion and preheat submission
// (docs/8.1 design.md §5.3, §5.4, §7.2). Polling, readiness tables, GWAA,
// gating and scoring belong to M5-M8 and are intentionally absent.
package warmup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/gate"
	"traincache/controller/internal/logging"
	"traincache/controller/internal/metrics"
)

// Condition types written by the M4 reconciler. Negative-named conditions use
// ConditionFalse to mean "the problem is present"; they are removed on success,
// mirroring the DataSet controller (docs/8.1 §5.2/§7.2).
const (
	ConditionDataSetNotReady   = "DataSetNotReady"
	ConditionInvalidSpec       = "InvalidSpec"
	ConditionPodSetMismatch    = "PodSetMismatch"
	ConditionNoAccessibleNodes = "NoAccessibleNodes"
	ConditionNoCandidates      = "NoCandidates"
	ConditionPreheatRejected   = "PreheatRejected"
)

// Condition and event reasons.
const (
	reasonDataSetNotReady   = "DataSetNotReady"
	reasonDataSetNotFound   = "DataSetNotFound"
	reasonInvalidSpec       = "InvalidSpec"
	reasonPodSetMismatch    = "PodSetMismatch"
	reasonNoAccessibleNodes = "NoAccessibleNodes"
	reasonNoCandidates      = "NoCandidates"
	reasonTooManyCandidates = "TooManyCandidates"
	reasonAgentRejected     = "AgentRejected"
)

// EventConsumeOrderDefaulted is emitted when any explicit consume order is
// invalid and the whole group falls back to the default split (D29).
const EventConsumeOrderDefaulted = "ConsumeOrderDefaulted"

// Retry sentinels request controller-runtime backoff because Node/Pod/DataSet
// state may change (docs/8.1 §7.9).
var (
	ErrDataSetNotReady   = errors.New("warmup: DataSet is not ready")
	ErrNoCandidates      = errors.New("warmup: no candidate nodes")
	ErrTooManyCandidates = errors.New("warmup: candidate node limit exceeded")
)

// Reconciler reconciles WarmupTask objects. MaxConcurrentReconciles=1 keeps
// reconciles for one task serial (D24).
type Reconciler struct {
	client.Client
	Agents       agentclient.Pool
	Now          func() time.Time
	Recorder     record.EventRecorder
	Log          *slog.Logger
	Metrics      *metrics.Metrics
	PollInterval time.Duration

	// ScoreAnnotationPrefix is the configured Node annotation prefix; it must
	// match the Scheduler's CONTROLLER_SCORE_ANNOTATION (D27).
	ScoreAnnotationPrefix string
	// GateName is the scheduling gate removed on release.
	GateName string
	// Rand breaks GWAA ties. nil keeps gwaa's fixed-seed determinism; the
	// manager injects a time-seeded source (D32).
	Rand *rand.Rand

	// progress holds seq high-water marks, consecutive failure counters and
	// rateHistory. It is intentionally in-memory only (D24); a restart starts
	// with empty history.
	progress pollTracker
}

// SetupWithManager registers the WarmupTask reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&traincachev1alpha1.WarmupTask{}).
		// M11: released Pods are watched so a spec.nodeName change can be
		// compared with the GWAA assignment (docs/8.1 §7.8).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapPodToTasks)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// Reconcile implements docs/8.1 §7.2 steps 1-6 for M4.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	start := r.now()
	// Registered before the first Get so every return path, including
	// NotFound and DeletionTimestamp, is observed exactly once.
	defer func() {
		r.Metrics.ObserveReconcile(metrics.ControllerWarmup, retErr, result.RequeueAfter, r.now().Sub(start))
	}()

	task := &traincachev1alpha1.WarmupTask{}
	if err := r.Get(ctx, req.NamespacedName, task); err != nil {
		if apierrors.IsNotFound(err) {
			r.Metrics.ForgetGateTask(req.NamespacedName.String())
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !task.DeletionTimestamp.IsZero() {
		r.Metrics.ForgetGateTask(TaskKey(task))
		return ctrl.Result{}, nil
	}

	base := task.DeepCopy()
	taskKey := TaskKey(task)
	requestID := fmt.Sprintf("%s/%d/%d", task.UID, task.Generation, start.UnixNano())
	fanout, fanoutMin, clamped := normalizeFanoutRange(task.Spec.PreheatFanout, task.Spec.PreheatFanoutMin)
	log := r.logger(ctx).With(
		"warmup_task", taskKey,
		"namespace", task.Namespace,
		"generation", task.Generation,
		"request_id", requestID,
		"fanout", fanout,
		"fanout_min", fanoutMin,
		"fanout_max", fanout,
	)
	if clamped {
		// The CRD CEL rule rejects this, so reaching here means admission was
		// bypassed. Clamp defensively and let ValidateTask flag the spec.
		log.Warn("preheat fanout minimum exceeds maximum; clamping to max",
			"reason", "min_gt_max",
			"requested_fanout_min", normalizeFanoutMin(task.Spec.PreheatFanoutMin),
			"fanout_min", fanoutMin,
			"fanout_max", fanout,
		)
	}
	log.Info("warmup reconcile start")
	defer func() {
		task.Status.ObservedGeneration = task.Generation
		if err := r.patchStatus(ctx, base, task); err != nil {
			log.Error("warmup status patch failed", "err", err)
			if retErr == nil {
				retErr = err
			}
		}
		fields := []any{"duration_ms", r.now().Sub(start).Milliseconds()}
		if retErr != nil {
			fields = append(fields, "err", retErr)
		}
		log.Info("warmup reconcile finished", fields...)
	}()

	// M11 fallback (docs/8.1 §7.8): a released Pod may land on a Node other
	// than its GWAA assignment. This must run for terminal tasks too, so it
	// stays before the isTerminal short-circuit.
	r.reconcileDeviation(ctx, log, task)

	if isTerminal(task) {
		r.Metrics.ForgetGateTask(TaskKey(task))
		log.Debug("warmup task already terminal; skipped",
			"state", task.Status.GateDecision.State,
			"reason", task.Status.GateDecision.Reason,
		)
		return ctrl.Result{}, nil
	}

	if reasons := ValidateTask(task); len(reasons) > 0 {
		message := strings.Join(reasons, "; ")
		log.Warn("warmup spec invalid", "reason", reasonInvalidSpec, "err", message)
		if r.setCondition(task, ConditionInvalidSpec, metav1.ConditionFalse, reasonInvalidSpec, message) {
			r.event(task, corev1.EventTypeWarning, reasonInvalidSpec, message)
		}
		return ctrl.Result{}, nil
	}
	meta.RemoveStatusCondition(&task.Status.Conditions, ConditionInvalidSpec)

	ds := &traincachev1alpha1.DataSet{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.DataSetRef.Name}, ds); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get DataSet %s/%s: %w", task.Namespace, task.Spec.DataSetRef.Name, err)
		}
		message := fmt.Sprintf("DataSet %s/%s not found", task.Namespace, task.Spec.DataSetRef.Name)
		log.Warn("dataset not found", "reason", reasonDataSetNotFound, "err", err)
		if r.setCondition(task, ConditionDataSetNotReady, metav1.ConditionFalse, reasonDataSetNotFound, message) {
			r.event(task, corev1.EventTypeWarning, reasonDataSetNotFound, message)
		}
		return ctrl.Result{}, ErrDataSetNotReady
	}
	if !dataSetReady(ds) || ds.Status.TotalChunks <= 0 || len(ds.Status.BlockLayout) == 0 {
		message := fmt.Sprintf("DataSet %s is not Ready", task.Spec.DataSetRef.Name)
		log.Warn("dataset not ready", "reason", reasonDataSetNotReady, "err", message)
		if r.setCondition(task, ConditionDataSetNotReady, metav1.ConditionFalse, reasonDataSetNotReady, message) {
			r.event(task, corev1.EventTypeWarning, reasonDataSetNotReady, message)
		}
		return ctrl.Result{}, ErrDataSetNotReady
	}
	meta.RemoveStatusCondition(&task.Status.Conditions, ConditionDataSetNotReady)

	accessible := sets.New[string](ds.Status.AccessibleNodes...)
	if accessible.Len() == 0 {
		message := fmt.Sprintf("DataSet %s has no accessible nodes", task.Spec.DataSetRef.Name)
		log.Warn("dataset has no accessible nodes", "reason", reasonNoAccessibleNodes)
		if r.setCondition(task, ConditionNoAccessibleNodes, metav1.ConditionFalse, reasonNoAccessibleNodes, message) {
			r.event(task, corev1.EventTypeWarning, reasonNoAccessibleNodes, message)
		}
		return ctrl.Result{}, nil
	}
	meta.RemoveStatusCondition(&task.Status.Conditions, ConditionNoAccessibleNodes)

	orders, source, err := ResolveConsumeOrders(task, ds.Status.TotalChunks)
	if err != nil {
		message := err.Error()
		log.Warn("warmup consume order invalid", "reason", reasonInvalidSpec, "err", err)
		if r.setCondition(task, ConditionInvalidSpec, metav1.ConditionFalse, reasonInvalidSpec, message) {
			r.event(task, corev1.EventTypeWarning, reasonInvalidSpec, message)
		}
		return ctrl.Result{}, nil
	}
	log.Info("consume order resolved",
		"consume_order_source", source,
		"reason", consumeOrderReason(source),
	)
	if source == traincachev1alpha1.ConsumeOrderSourceDefault &&
		task.Status.ConsumeOrderSource != traincachev1alpha1.ConsumeOrderSourceDefault {
		r.event(task, corev1.EventTypeNormal, EventConsumeOrderDefaulted,
			"one or more initialConsumeOrder values are invalid; default split applied")
	}
	task.Status.ConsumeOrderSource = source

	podNames, matchedPods, err := r.targetPodNames(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	if int32(matchedPods) != task.Spec.PodCount {
		message := fmt.Sprintf("target Pod set has %d Pods, spec.podCount is %d", matchedPods, task.Spec.PodCount)
		log.Warn("target Pod set mismatch", "reason", reasonPodSetMismatch, "pods", matchedPods)
		if r.setCondition(task, ConditionPodSetMismatch, metav1.ConditionFalse, reasonPodSetMismatch, message) {
			r.event(task, corev1.EventTypeWarning, reasonPodSetMismatch, message)
		}
	} else {
		meta.RemoveStatusCondition(&task.Status.Conditions, ConditionPodSetMismatch)
	}

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return ctrl.Result{}, fmt.Errorf("list Nodes: %w", err)
	}
	agentPods, err := agentclient.ListAgentPods(ctx, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list agent Pods: %w", err)
	}
	agentReady := agentclient.ReadyAgentNodes(agentPods)

	filtered, err := Filter(FilterInput{
		Nodes:      nodeList.Items,
		Accessible: accessible,
		Pods:       task.Spec.PodDetails,
		Policy:     task.Spec.Candidate,
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	candidates := candidatesWithAgent(filtered, agentReady)
	r.Metrics.ObserveCandidateNodes(datasetKey(task), len(candidates))
	log.Info("candidate filtering",
		"nodes", len(nodeList.Items),
		"accessible", accessible.Len(),
		"agent_ready", agentReady.Len(),
		"candidates", len(candidates),
		"reason", "ready+accessible+taint+resources+agent",
	)

	if len(candidates) == 0 {
		message := fmt.Sprintf("no candidate Nodes (%d Nodes, %d accessible, %d with Ready Agent)",
			len(nodeList.Items), accessible.Len(), agentReady.Len())
		log.Warn("no candidate nodes", "reason", reasonNoCandidates)
		if r.setCondition(task, ConditionNoCandidates, metav1.ConditionFalse, reasonNoCandidates, message) {
			r.event(task, corev1.EventTypeWarning, reasonNoCandidates, message)
		}
		return ctrl.Result{}, ErrNoCandidates
	}
	if len(candidates) > maxCandidateNodes {
		message := fmt.Sprintf("candidate count %d exceeds limit %d", len(candidates), maxCandidateNodes)
		log.Warn("too many candidate nodes", "reason", reasonTooManyCandidates, "candidates", len(candidates))
		if r.setCondition(task, ConditionNoCandidates, metav1.ConditionFalse, reasonTooManyCandidates, message) {
			r.event(task, corev1.EventTypeWarning, reasonTooManyCandidates, message)
		}
		return ctrl.Result{}, fmt.Errorf("%w: %d > %d", ErrTooManyCandidates, len(candidates), maxCandidateNodes)
	}
	meta.RemoveStatusCondition(&task.Status.Conditions, ConditionNoCandidates)

	if r.Agents == nil {
		return ctrl.Result{}, fmt.Errorf("warmup: agent pool is not configured")
	}
	windows, err := PodWindows(task, ds.Status.BlockLayout, orders)
	if err != nil {
		return ctrl.Result{}, err
	}
	// M4 submits every candidate until GWAA has produced an assignment. From
	// then on only the diff RPCs run, so a cancelled preheat task is not
	// recreated on every reconcile.
	initialFanout := task.Status.StartedAt == nil || !hasAssignment(task)
	submission := newSubmitResult()
	var submitErr error
	if initialFanout {
		submission, submitErr = submitToCandidates(ctx, r.Agents, r.Metrics, log, task, ds, windows, candidates, podNames)
		if len(submission.Permanent) > 0 {
			rejected := sortedErrorKeys(submission.Permanent)
			message := fmt.Sprintf("Agent rejected preheat on %s", strings.Join(rejected, ", "))
			log.Error("agent permanently rejected preheat", "reason", reasonAgentRejected, "nodes", strings.Join(rejected, ","))
			if r.setCondition(task, ConditionPreheatRejected, metav1.ConditionFalse, reasonAgentRejected, message) {
				r.event(task, corev1.EventTypeWarning, reasonAgentRejected, message)
			}
		} else {
			meta.RemoveStatusCondition(&task.Status.Conditions, ConditionPreheatRejected)
		}
		if submitErr != nil {
			log.Warn("preheat submission incomplete",
				"err", submitErr,
				"submitted", submission.Submitted,
				"unavailable", len(submission.Unavailable),
			)
		}
	}

	task.Status.CandidateNodes = append([]string(nil), candidates...)
	if task.Status.StartedAt == nil {
		now := metav1.NewTime(r.now())
		task.Status.StartedAt = &now
	}
	if task.Status.Phase != traincachev1alpha1.WarmupPhaseGating {
		task.Status.Phase = traincachev1alpha1.WarmupPhasePreheating
	}

	// M5: poll every (pod,node) fact. Polling failures degrade individual
	// pairs; they never replace the submission error/backoff.
	outcome := pollOutcome{}
	var facts map[readinessKey]readinessFact
	if len(candidates) > 0 && len(windows) > 0 {
		outcome, facts = r.pollReadiness(ctx, log, task, windows, candidates, podNames)
	}

	// M6: recompute GWAA after each poll and issue only the RPC differences.
	// M7: evaluate the gate on the assigned node's readiness.
	if len(candidates) > 0 && len(windows) > 0 {
		stopped := submitFailures(submission)
		placement := r.computePlacement(task, candidates, stopped)
		if placement.Err != nil {
			log.Error("gwaa computation failed", "err", placement.Err)
			if submitErr == nil {
				submitErr = placement.Err
			}
		} else {
			applied, applyErr := ApplyPlacement(ctx, r.Agents, log, PlacementApplyInput{
				Task:      task,
				DataSet:   ds,
				Windows:   windows,
				Previous:  previousPreheat(task, windows, candidates, initialFanout),
				Placement: placement,
				PodNames:  podNames,
				Fanout:    fanout,
				FanoutMin: fanoutMin,
				Metrics:   r.Metrics,
			})
			if applyErr != nil {
				log.Warn("placement apply incomplete",
					"err", applyErr,
					"submitted", applied.Submitted,
					"updated", applied.Updated,
					"cancelled", applied.Cancelled,
				)
				if submitErr == nil {
					submitErr = applyErr
				}
			}
			applyAssignment(task, placement, facts, podNames, candidates)
			fields := []any{
				"fanout", fanout,
				"fanout_min", fanoutMin,
				"fanout_max", fanout,
				"assign", formatAssignment(placement.Assignment),
				"warm_node_count", formatWarmNodeCounts(placement.Assignment),
				"status_entries", len(task.Status.PodNodeReadiness),
				"unassigned", len(placement.UnassignedPods),
			}
			fields = append(fields, summarizeFanout(placement, applied.Actions).fields()...)
			log.Info("gwaa placement applied", fields...)
		}

		evaluation := r.evaluateGate(task)
		logGateDecision(log, taskKey, evaluation)
		if evaluation.Decision.State == gate.Hold {
			r.Metrics.ObserveGateTask(taskKey, metrics.GateHold, r.now())
			if !outcome.Pending {
				task.Status.Phase = traincachev1alpha1.WarmupPhaseGating
			}
			// M7 independent gate cycle: a terminal poll (all Ready/Failed)
			// still requeues until release or TMax.
			return ctrl.Result{RequeueAfter: r.pollRequeueAfter()}, submitErr
		}
		released, releaseErr := r.releaseGate(ctx, log, task, windows, podNames, evaluation)
		if releaseErr == nil {
			// The task only leaves Hold once the score/gate writes succeeded.
			r.Metrics.ObserveGateTask(taskKey, gateMetricState(evaluation.Decision.State), r.now())
		}
		return released, releaseErr
	}
	return ctrl.Result{}, submitErr
}

// submitFailures turns the initial M4 per-node failures into the GWAA stopped
// node set.
func submitFailures(submission submitResult) map[string]error {
	failed := make(map[string]error, len(submission.Unavailable)+len(submission.Permanent))
	for node, err := range submission.Unavailable {
		failed[node] = err
	}
	for node, err := range submission.Permanent {
		failed[node] = err
	}
	return failed
}

func (r *Reconciler) targetPodNames(ctx context.Context, task *traincachev1alpha1.WarmupTask) (map[int32]string, int, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(task.Namespace)); err != nil {
		return nil, 0, fmt.Errorf("list target Pods: %w", err)
	}
	names := make(map[int32]string, task.Spec.PodCount)
	matched := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !TargetMatchesPod(task, pod) {
			continue
		}
		matched++
		index, err := ResolvePodIndex(pod)
		if err != nil {
			continue
		}
		if _, err := BindPodIndex(task, index); err != nil {
			continue
		}
		names[index] = pod.Name
	}
	return names, matched, nil
}

func (r *Reconciler) patchStatus(ctx context.Context, base, task *traincachev1alpha1.WarmupTask) error {
	if equality.Semantic.DeepEqual(base.Status, task.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, task, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patch WarmupTask status: %w", err)
	}
	return nil
}

// setCondition applies a condition and reports whether the visible state
// changed, so callers only emit an event on transitions.
func (r *Reconciler) setCondition(task *traincachev1alpha1.WarmupTask, conditionType string, status metav1.ConditionStatus, reason, message string) bool {
	before := meta.FindStatusCondition(task.Status.Conditions, conditionType)
	meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: task.Generation,
		LastTransitionTime: metav1.NewTime(r.now()),
	})
	after := meta.FindStatusCondition(task.Status.Conditions, conditionType)
	return before == nil || before.Status != after.Status || before.Reason != after.Reason || before.Message != after.Message
}

func (r *Reconciler) event(task *traincachev1alpha1.WarmupTask, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(task, eventType, reason, message)
	}
}

func (r *Reconciler) logger(ctx context.Context) *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return logging.From(ctx)
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// datasetKey is the bounded metric label for a DataSet: "<namespace>/<name>".
// The WarmupTask namespace is also the DataSet namespace (the controller reads
// the DataSet from task.Namespace), so this value cannot collide across
// namespaces.
func datasetKey(task *traincachev1alpha1.WarmupTask) string {
	if task == nil {
		return ""
	}
	return task.Namespace + "/" + task.Spec.DataSetRef.Name
}

// gateMetricState maps the gate package verdict to the frozen metric enum.
func gateMetricState(state gate.GateState) metrics.GateState {
	switch state {
	case gate.Released:
		return metrics.GateReleased
	case gate.Degraded:
		return metrics.GateDegraded
	default:
		return metrics.GateHold
	}
}

func dataSetReady(ds *traincachev1alpha1.DataSet) bool {
	if ds == nil {
		return false
	}
	cond := meta.FindStatusCondition(ds.Status.Conditions, "Ready")
	return cond != nil && cond.Status == metav1.ConditionTrue
}

// candidatesWithAgent intersects the frozen Filter result with Nodes that host
// a Ready Agent Pod, preserving the ascending order.
func candidatesWithAgent(filtered []string, ready sets.Set[string]) []string {
	out := make([]string, 0, len(filtered))
	for _, name := range filtered {
		if ready.Has(name) {
			out = append(out, name)
		}
	}
	return out
}

func consumeOrderReason(source traincachev1alpha1.ConsumeOrderSource) string {
	if source == traincachev1alpha1.ConsumeOrderSourceDefault {
		return "invalid, incomplete or missing initialConsumeOrder; default split applied"
	}
	return "explicit initialConsumeOrder"
}

func sortedErrorKeys(m map[string]error) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
