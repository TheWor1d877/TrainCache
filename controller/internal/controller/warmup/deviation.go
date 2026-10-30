package warmup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	reconcilepkg "sigs.k8s.io/controller-runtime/pkg/reconcile"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	corelog "traincache/core/logging"
)

// EventNodeDeviation is recorded on the WarmupTask when a released Pod lands
// on a Node other than its GWAA assignment (docs/7.1 §7.8).
const EventNodeDeviation = "NodeDeviation"

// mapPodToTasks enqueues every WarmupTask whose TargetRef selects the Pod.
// Pods are watched so a released Pod's spec.nodeName change triggers the
// deviation check without polling after the task became terminal.
func (r *Reconciler) mapPodToTasks(ctx context.Context, obj client.Object) []reconcilepkg.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	var tasks traincachev1alpha1.WarmupTaskList
	if err := r.List(ctx, &tasks, client.InNamespace(pod.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcilepkg.Request, 0, 1)
	for i := range tasks.Items {
		task := &tasks.Items[i]
		if !TargetMatchesPod(task, pod) {
			continue
		}
		requests = append(requests, reconcilepkg.Request{NamespacedName: types.NamespacedName{
			Namespace: task.Namespace,
			Name:      task.Name,
		}})
	}
	return requests
}

// reconcileDeviation implements the fallback from docs/7.1 §7.8. For every
// released Pod whose spec.nodeName differs from its GWAA assignment it writes
// PodNodeReadiness.ActualNode, emits a NodeDeviation event and submits the
// same startup window on the actual Node at P0. The original assignment is
// not cancelled and the Pod is not re-gated.
func (r *Reconciler) reconcileDeviation(ctx context.Context, log *slog.Logger, task *traincachev1alpha1.WarmupTask) {
	if task == nil || (task.Status.Phase != traincachev1alpha1.WarmupPhaseReleased &&
		task.Status.Phase != traincachev1alpha1.WarmupPhaseDegraded) {
		return
	}
	if r.Agents == nil {
		return
	}

	pods, err := r.targetPodMap(ctx, task)
	if err != nil {
		log.Warn("deviation check skipped: list target Pods failed", "err", err)
		return
	}

	ds := &traincachev1alpha1.DataSet{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.DataSetRef.Name}, ds); err != nil {
		log.Warn("deviation check skipped: get DataSet failed", "err", err)
		return
	}
	orders, _, err := ResolveConsumeOrders(task, ds.Status.TotalChunks, ds.Status.BlockLayout)
	if err != nil {
		log.Warn("deviation check skipped: consume order invalid", "err", err)
		return
	}
	windows, err := PodWindows(task, ds.Status.BlockLayout, orders)
	if err != nil {
		log.Warn("deviation check skipped: window expansion failed", "err", err)
		return
	}

	for index, pod := range pods {
		actual := pod.Spec.NodeName
		if actual == "" {
			continue
		}
		nodes := assignedNodes(task, index)
		if len(nodes) == 0 {
			continue
		}
		// Landing inside the Pod's warm-node set is not a deviation: only
		// backfill ActualNode on every entry (docs/7.1 §7.8).
		if containsString(nodes, actual) {
			setActualNode(task, index, actual)
			continue
		}
		if setActualNode(task, index, actual) {
			r.event(task, corev1.EventTypeWarning, EventNodeDeviation,
				fmt.Sprintf("pod %s landed on node %s, GWAA preheated %s", pod.Name, actual, strings.Join(nodes, ",")))
			log.Warn("node deviation detected",
				"pod", pod.Name,
				"pod_index", index,
				"warm_nodes", strings.Join(nodes, ","),
				"actual_node", actual,
			)
		}
		if int(index) >= len(windows) {
			continue
		}
		req := &agentpb.SubmitPreheatRequest{
			TaskId:     TaskID(task, index),
			Node:       actual,
			Dataset:    ds.Spec.DatasetName,
			Chunks:     windows[index],
			Priority:   agentpb.PreheatPriority_P0,
			Generation: uint64(task.Generation),
		}
		callCtx := corelog.WithRequestMeta(ctx, "", req.TaskId, req.Generation)
		if err := r.Agents.Submit(callCtx, actual, req); err != nil {
			log.Warn("deviation preheat submit failed",
				"pod", pod.Name,
				"pod_index", index,
				"node", actual,
				"err", err,
			)
		}
	}
}

// containsString reports whether values contains target.
func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// setActualNode writes actual to every PodNodeReadiness entry of the Pod and
// reports whether the value changed. All entries of one Pod must agree
// (docs/7.1 §6.1).
func setActualNode(task *traincachev1alpha1.WarmupTask, pod int32, actual string) bool {
	changed := false
	for i := range task.Status.PodNodeReadiness {
		entry := &task.Status.PodNodeReadiness[i]
		if entry.PodIndex != pod {
			continue
		}
		if entry.ActualNode != actual {
			entry.ActualNode = actual
			changed = true
		}
	}
	return changed
}

// targetPodMap lists target Pods keyed by their resolved Pod index. A Pod
// index that cannot be bound to a PodDetail is ignored, matching
// targetPodNames.
func (r *Reconciler) targetPodMap(ctx context.Context, task *traincachev1alpha1.WarmupTask) (map[int32]*corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(task.Namespace)); err != nil {
		return nil, fmt.Errorf("list target Pods: %w", err)
	}
	pods := make(map[int32]*corev1.Pod, task.Spec.PodCount)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !TargetMatchesPod(task, pod) {
			continue
		}
		index, err := ResolvePodIndex(pod)
		if err != nil {
			continue
		}
		if _, err := BindPodIndex(task, index); err != nil {
			continue
		}
		pods[index] = pod
	}
	return pods, nil
}
