// Package warmup contains the WarmupTask control-plane logic. This file owns
// the M9 Pod association contract shared with the mutating webhook and the
// GateInjectionDegraded condition (docs/7.1 design.md §5.7, docs/7.3 D8-D10).
package warmup

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/metrics"
)

// Pod association and injection keys frozen by docs/7.1 §5.7/§6.3 and
// docs/7.3 D8-D10.
const (
	WarmupEnabledLabel      = "traincache.io/warmup"
	WarmupEnabledValue      = "enabled"
	AnnotationWarmupTask    = "traincache.io/warmup-task"
	AnnotationPodIndex      = "traincache.io/pod-index"
	DefaultGateName         = "traincache.io/warmup"
	JobCompletionIndexLabel = "batch.kubernetes.io/job-completion-index"

	// ConditionGateInjectionDegraded is written to WarmupTask.status when a
	// selected Pod could not be injected or bound to its task.
	ConditionGateInjectionDegraded = "GateInjectionDegraded"
	ReasonGateInjectionDegraded    = "GateInjectionDegraded"

	// EventWarmupTaskMappingFailed is recorded on the Pod when the ordinal
	// cannot be resolved or does not bind to a PodDetail (D10).
	EventWarmupTaskMappingFailed = "WarmupTaskMappingFailed"
)

// TaskKey returns the canonical "<namespace>/<name>" task reference used in
// Pod annotations and score keys.
func TaskKey(task *traincachev1alpha1.WarmupTask) string {
	if task == nil {
		return ""
	}
	return task.Namespace + "/" + task.Name
}

// TaskReady implements the "Ready" filter from docs/7.1 §5.7. A task that
// already exposes a Ready condition uses it; otherwise terminal phases
// (Released/Failed/Degraded) are treated as not ready and every other phase
// (including a freshly created task with empty status) is ready.
func TaskReady(task *traincachev1alpha1.WarmupTask) bool {
	if task == nil {
		return false
	}
	if cond := meta.FindStatusCondition(task.Status.Conditions, "Ready"); cond != nil {
		return cond.Status == metav1.ConditionTrue
	}
	switch task.Status.Phase {
	case traincachev1alpha1.WarmupPhaseReleased,
		traincachev1alpha1.WarmupPhaseFailed,
		traincachev1alpha1.WarmupPhaseDegraded:
		return false
	default:
		return true
	}
}

// TargetMatchesPod reports whether task.Spec.TargetRef selects pod. Job Pods
// are matched through owner references and the standard Job name labels;
// StatefulSet Pods through owner references or the "<name>-<ordinal>" form;
// standalone Pods by name. podLabelSelector, when present, must also match.
func TargetMatchesPod(task *traincachev1alpha1.WarmupTask, pod *corev1.Pod) bool {
	if task == nil || pod == nil || task.Spec.TargetRef.Name == "" {
		return false
	}
	ref := task.Spec.TargetRef

	if ref.PodLabelSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(ref.PodLabelSelector)
		if err != nil || !selector.Matches(labels.Set(pod.Labels)) {
			return false
		}
	}

	switch {
	case strings.EqualFold(ref.Kind, "Pod"):
		return pod.Name == ref.Name && apiVersionMatches(ref.APIVersion, pod.APIVersion)

	case strings.EqualFold(ref.Kind, "Job"):
		for _, owner := range pod.OwnerReferences {
			if strings.EqualFold(owner.Kind, "Job") && owner.Name == ref.Name &&
				apiVersionMatches(ref.APIVersion, owner.APIVersion) {
				return true
			}
		}
		if !apiVersionMatches(ref.APIVersion, "batch/v1") {
			return false
		}
		for _, key := range []string{"batch.kubernetes.io/job-name", "job-name"} {
			if pod.Labels[key] == ref.Name {
				return true
			}
		}
		return false

	case strings.EqualFold(ref.Kind, "StatefulSet"):
		for _, owner := range pod.OwnerReferences {
			if strings.EqualFold(owner.Kind, "StatefulSet") && owner.Name == ref.Name &&
				apiVersionMatches(ref.APIVersion, owner.APIVersion) {
				return true
			}
		}
		if !apiVersionMatches(ref.APIVersion, "apps/v1") {
			return false
		}
		prefix := ref.Name + "-"
		if strings.HasPrefix(pod.Name, prefix) {
			if _, err := strconv.ParseInt(strings.TrimPrefix(pod.Name, prefix), 10, 32); err == nil {
				return true
			}
		}
		return false
	}
	return false
}

func apiVersionMatches(want, got string) bool {
	return want == "" || got == "" || want == got
}

// MatchingTasks returns every task that targets pod, ordered by name for
// deterministic selection and degraded-condition targeting.
func MatchingTasks(tasks []traincachev1alpha1.WarmupTask, pod *corev1.Pod) []traincachev1alpha1.WarmupTask {
	matches := make([]traincachev1alpha1.WarmupTask, 0, 1)
	for i := range tasks {
		if TargetMatchesPod(&tasks[i], pod) {
			matches = append(matches, tasks[i])
		}
	}
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].Name < matches[j].Name })
	return matches
}

// ReadyTaskForPod returns the deterministic ready task targeting pod, or nil.
func ReadyTaskForPod(tasks []traincachev1alpha1.WarmupTask, pod *corev1.Pod) *traincachev1alpha1.WarmupTask {
	matches := MatchingTasks(tasks, pod)
	for i := range matches {
		if TaskReady(&matches[i]) {
			return &matches[i]
		}
	}
	return nil
}

// ResolvePodIndex implements D10: the indexed-Job label wins; otherwise the
// last "-<ordinal>" segment of the Pod name is parsed. A present but malformed
// label is an error and does not silently fall back to the Pod name.
func ResolvePodIndex(pod *corev1.Pod) (int32, error) {
	if pod == nil {
		return 0, fmt.Errorf("pod is nil")
	}
	if raw, ok := pod.Labels[JobCompletionIndexLabel]; ok {
		if strings.TrimSpace(raw) == "" {
			return 0, fmt.Errorf("label %s is empty", JobCompletionIndexLabel)
		}
		index, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || index < 0 {
			return 0, fmt.Errorf("label %s=%q is not a non-negative int32", JobCompletionIndexLabel, raw)
		}
		return int32(index), nil
	}

	sep := strings.LastIndex(pod.Name, "-")
	if sep < 0 || sep == len(pod.Name)-1 {
		return 0, fmt.Errorf("pod %q has no StatefulSet ordinal suffix", pod.Name)
	}
	raw := pod.Name[sep+1:]
	index, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || index < 0 {
		return 0, fmt.Errorf("pod %q ordinal suffix %q is not a non-negative int32", pod.Name, raw)
	}
	return int32(index), nil
}

// BindPodIndex verifies that index is inside 0..PodCount-1 and maps to a
// declared PodDetail (D8). Without a PodDetail the Pod cannot be bound to a
// readiness row or score key.
func BindPodIndex(task *traincachev1alpha1.WarmupTask, index int32) (traincachev1alpha1.PodDetail, error) {
	if task == nil {
		return traincachev1alpha1.PodDetail{}, fmt.Errorf("task is nil")
	}
	if index < 0 || index >= task.Spec.PodCount {
		return traincachev1alpha1.PodDetail{}, fmt.Errorf("pod index %d is outside 0..%d", index, task.Spec.PodCount-1)
	}
	for i := range task.Spec.PodDetails {
		if task.Spec.PodDetails[i].Index == index {
			return task.Spec.PodDetails[i], nil
		}
	}
	return traincachev1alpha1.PodDetail{}, fmt.Errorf("pod index %d has no PodDetail in task %s", index, TaskKey(task))
}

// ApplyInjection adds the scheduling gate and the two Pod annotations. It is
// idempotent: an already-present gate is not appended twice and matching
// annotations are left untouched. It does not touch nodeAffinity, resources or
// containers (docs/7.1 §5.7) and returns whether anything changed.
func ApplyInjection(pod *corev1.Pod, task *traincachev1alpha1.WarmupTask, index int32, gateName string) bool {
	if pod == nil || task == nil {
		return false
	}
	if gateName == "" {
		gateName = DefaultGateName
	}
	changed := false

	found := false
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == gateName {
			found = true
			break
		}
	}
	if !found {
		pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: gateName})
		changed = true
	}

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	if taskKey := TaskKey(task); pod.Annotations[AnnotationWarmupTask] != taskKey {
		pod.Annotations[AnnotationWarmupTask] = taskKey
		changed = true
	}
	if podIndex := strconv.FormatInt(int64(index), 10); pod.Annotations[AnnotationPodIndex] != podIndex {
		pod.Annotations[AnnotationPodIndex] = podIndex
		changed = true
	}
	return changed
}

// MarkGateInjectionDegraded sets GateInjectionDegraded=True on the task. It is
// idempotent and uses a status patch so it cannot clobber concurrent status
// writers. A nil task is a no-op because there is nowhere to record the
// condition.
func MarkGateInjectionDegraded(ctx context.Context, c client.Client, logger *slog.Logger, task *traincachev1alpha1.WarmupTask, podName, cause string) error {
	if task == nil {
		return nil
	}
	message := fmt.Sprintf("pod %s was not gated: %s", podName, cause)
	if existing := meta.FindStatusCondition(task.Status.Conditions, ConditionGateInjectionDegraded); existing != nil &&
		existing.Status == metav1.ConditionTrue &&
		existing.Message == message &&
		existing.ObservedGeneration == task.Generation {
		return nil
	}
	if c == nil {
		return fmt.Errorf("warmup: cannot record %s: client is nil", ConditionGateInjectionDegraded)
	}

	base := task.DeepCopy()
	meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:               ConditionGateInjectionDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonGateInjectionDegraded,
		Message:            message,
		ObservedGeneration: task.Generation,
	})
	if equality.Semantic.DeepEqual(base.Status, task.Status) {
		return nil
	}
	if err := c.Status().Patch(ctx, task, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("warmup: patch %s condition: %w", ConditionGateInjectionDegraded, err)
	}
	if logger != nil {
		logger.Warn("gate injection degraded",
			"warmup_task", TaskKey(task),
			"pod", podName,
			"reason", ReasonGateInjectionDegraded,
			"state", ConditionGateInjectionDegraded,
		)
	}
	return nil
}

// DegradedReconciler watches selected Pods and records
// GateInjectionDegraded on the matching WarmupTask when the webhook did not
// inject the task annotation (docs/7.1 §5.7 and §7.9).
//
// Wiring note: controller-manager registers this reconciler once the warmup
// manager owns Pod watches; SetupWithManager is provided for that caller.
type DegradedReconciler struct {
	client.Client
	Log      *slog.Logger
	Recorder record.EventRecorder
	Now      func() time.Time
	Metrics  *metrics.Metrics
}

// SetupWithManager registers the degraded-condition reconciler for Pods
// carrying the warmup=enabled selector label.
func (r *DegradedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			pod, ok := obj.(*corev1.Pod)
			return ok && pod.Labels[WarmupEnabledLabel] == WarmupEnabledValue
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// Reconcile detects a selected Pod with no warmup-task annotation and writes
// the degraded condition to its matching task. It never mutates the Pod and
// never blocks Pod admission.
func (r *DegradedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	start := r.now()
	defer func() {
		r.Metrics.ObserveReconcile(metrics.ControllerWarmupDegraded, retErr, result.RequeueAfter, r.now().Sub(start))
	}()

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	requestID := fmt.Sprintf("%s/%d/%d", pod.UID, pod.Generation, start.UnixNano())
	log := r.logger().With(
		"request_id", requestID,
		"pod", pod.Name,
		"namespace", pod.Namespace,
	)

	if pod.Labels[WarmupEnabledLabel] != WarmupEnabledValue {
		return ctrl.Result{}, nil
	}
	if pod.Annotations[AnnotationWarmupTask] != "" {
		log.Debug("gate injection present; degraded check skipped")
		return ctrl.Result{}, nil
	}

	list := &traincachev1alpha1.WarmupTaskList{}
	if err := r.List(ctx, list, client.InNamespace(pod.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("list warmup tasks: %w", err)
	}
	matches := MatchingTasks(list.Items, pod)
	if len(matches) == 0 {
		log.Debug("no matching WarmupTask; no degraded target",
			"reason", "WarmupTaskNotFound",
		)
		return ctrl.Result{}, nil
	}

	task := &matches[0]
	cause := fmt.Sprintf("pod has %s=%s but no %s annotation", WarmupEnabledLabel, WarmupEnabledValue, AnnotationWarmupTask)
	if err := MarkGateInjectionDegraded(ctx, r.Client, log, task, pod.Name, cause); err != nil {
		return ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(task, corev1.EventTypeWarning, ConditionGateInjectionDegraded,
			"pod %s/%s was not injected by the webhook", pod.Namespace, pod.Name)
	}
	log.Warn("gate injection degraded condition recorded",
		"warmup_task", TaskKey(task),
		"reason", ReasonGateInjectionDegraded,
		"duration_ms", r.now().Sub(start).Milliseconds(),
	)
	return ctrl.Result{}, nil
}

func (r *DegradedReconciler) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

func (r *DegradedReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
