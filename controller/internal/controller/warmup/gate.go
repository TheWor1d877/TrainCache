package warmup

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/gate"
	"traincache/controller/internal/gwaa"
	"traincache/controller/internal/nodescore"
)

const (
	// ConditionFirstReady records the first instant at which any Pod reached
	// its integer threshold; condition ② starts from this timestamp (D15).
	ConditionFirstReady = "FirstReady"
	// ConditionGateReleased and ConditionGateDegraded record the terminal gate
	// verdict.
	ConditionGateReleased = "GateReleased"
	ConditionGateDegraded = "GateDegraded"

	// EventGateReleased and EventGateDegraded are emitted when the gate opens.
	EventGateReleased = "GateReleased"
	EventGateDegraded = "GateDegraded"

	reasonFirstReady = "FirstPodReady"
)

// isTerminal reports whether the task already made its gate decision at the
// current generation. A generation change reopens the loop.
func isTerminal(task *traincachev1alpha1.WarmupTask) bool {
	if task == nil || task.Status.ObservedGeneration != task.Generation {
		return false
	}
	switch task.Status.Phase {
	case traincachev1alpha1.WarmupPhaseReleased, traincachev1alpha1.WarmupPhaseDegraded:
		return true
	}
	return false
}

// computePlacement runs GWAA over the current readiness table. Submitted
// failures are treated as stopped nodes so a Pod is not assigned to a node
// whose initial Submit already failed. previous is this round's ledger: pairs
// still running at the current generation are the hysteresis input (docs/7.1
// §5.5, D45) that keeps an incumbent from being replaced just because another
// node landed one block first.
func (r *Reconciler) computePlacement(
	task *traincachev1alpha1.WarmupTask,
	candidates []string,
	submitFailed map[string]error,
	previous map[PreheatPair]ExistingPreheat,
) gwaa.Output {
	pods := make([]gwaa.PodState, 0, task.Spec.PodCount)
	for index := int32(0); index < task.Spec.PodCount; index++ {
		detail, ok := podDetail(task, index)
		if !ok {
			continue
		}
		ready := make(map[string]int32, len(candidates))
		for column, node := range candidates {
			ready[node] = readinessAt(task, index, column)
		}
		pods = append(pods, gwaa.PodState{
			Index:            index,
			K:                detail.K,
			ReadyCountByNode: ready,
		})
	}

	nodes := make([]gwaa.NodeState, 0, len(candidates))
	for _, node := range candidates {
		nodes = append(nodes, gwaa.NodeState{
			Name:    node,
			Stopped: r.nodeStopped(task, node, submitFailed),
		})
	}
	fanoutMax, fanoutMin, _ := normalizeFanoutRange(task.Spec.PreheatFanout, task.Spec.PreheatFanoutMin)
	held := make(map[int32][]string, len(pods))
	for pair, existing := range previous {
		if existing.Generation != uint64(task.Generation) {
			continue
		}
		held[pair.PodIndex] = append(held[pair.PodIndex], pair.Node)
	}
	placement := gwaa.Compute(gwaa.Input{
		Pods:      pods,
		Nodes:     nodes,
		Held:      held,
		Rand:      r.gwaaRand(task),
		Fanout:    fanoutMax,
		FanoutMin: fanoutMin,
	})
	r.Metrics.ObservePlacement(datasetKey(task), placement.PreheatSet, len(placement.UnassignedPods), placement.FellBack)
	return placement
}

// gwaaRand returns the tie-breaking source for one placement computation. The
// seed comes from the task UID and generation, so every poll of the same
// generation draws the same shuffle order: at cold start all c_ij are equal, and
// a per-reconcile random stream would flip the assignment each round and cancel
// the previous submits (D32). Different tasks and spec updates still get an
// independent order. Reconciler.Rand overrides it in tests.
func (r *Reconciler) gwaaRand(task *traincachev1alpha1.WarmupTask) *rand.Rand {
	if r.Rand != nil {
		return r.Rand
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(task.UID))
	_, _ = fmt.Fprintf(hash, "/%d", task.Generation)
	return rand.New(rand.NewSource(int64(hash.Sum64())))
}

// readinessAt returns table[pod][column] defensively.
func readinessAt(task *traincachev1alpha1.WarmupTask, pod int32, column int) int32 {
	if task == nil || int(pod) < 0 || int(pod) >= len(task.Status.ReadinessTable) {
		return 0
	}
	row := task.Status.ReadinessTable[pod]
	if column < 0 || column >= len(row) {
		return 0
	}
	return row[column]
}

// nodeStopped implements the "已停止节点集合" input from docs/7.1 §7.4. A node
// is stopped when its initial Submit failed, every Pod's poll on it crossed the
// consecutive-failure threshold, or every readiness fact collected on it
// reports Failed. The in-memory counter covers the pre-assignment fan-out (the
// P×N facts are no longer persisted); the status scan covers assigned facts.
func (r *Reconciler) nodeStopped(task *traincachev1alpha1.WarmupTask, node string, submitFailed map[string]error) bool {
	if _, failed := submitFailed[node]; failed {
		return true
	}
	if r != nil && r.progress.nodeFailed(string(task.UID), node, uint64(task.Generation), int(task.Spec.PodCount)) {
		return true
	}
	found := false
	for i := range task.Status.PodNodeReadiness {
		entry := &task.Status.PodNodeReadiness[i]
		if entry.NodeName != node {
			continue
		}
		found = true
		if entry.State != traincachev1alpha1.PreheatStateFailed {
			return false
		}
	}
	return found
}

// assignedNodes returns the current GWAA preheat node set for a Pod, in
// assignment (c_ij descending) order. A Pod with no feasible node has only a
// summary entry with an empty NodeName, so the result is empty.
func assignedNodes(task *traincachev1alpha1.WarmupTask, pod int32) []string {
	var nodes []string
	for i := range task.Status.PodNodeReadiness {
		entry := &task.Status.PodNodeReadiness[i]
		if entry.PodIndex == pod && entry.NodeName != "" {
			nodes = append(nodes, entry.NodeName)
		}
	}
	return nodes
}

// readinessFor returns the readiness fact for (pod,node), or nil. Status keeps
// one entry per (Pod, preheat node) pair (M13), so NodeName is the identity.
func readinessFor(task *traincachev1alpha1.WarmupTask, pod int32, node string) *traincachev1alpha1.PodNodeReadiness {
	for i := range task.Status.PodNodeReadiness {
		entry := &task.Status.PodNodeReadiness[i]
		if entry.PodIndex == pod && entry.NodeName == node {
			return entry
		}
	}
	return nil
}

// applyAssignment writes the GWAA result into status.PodNodeReadiness as one
// entry per PodDetail.Index. A changed assignment replaces the entry's
// assigned-node fact with this round's observation (or clears it when none is
// available), so stale data from the previous node is never relabeled.
// MinUnmet is stamped onto every entry from placement.Decisions (D40).
func applyAssignment(
	task *traincachev1alpha1.WarmupTask,
	placement gwaa.Output,
	facts map[readinessKey]readinessFact,
	podNames map[int32]string,
	candidates []string,
) {
	minUnmet := make(map[int32]bool, len(placement.Decisions))
	for idx, dec := range placement.Decisions {
		minUnmet[idx] = dec.MinUnmet
	}
	buildPodDetails(task, facts, podNames, candidates, placement.Assignment, minUnmet)
}

// submittedLedger is the "already submitted" side of the placement diff: every
// task an Agent confirmed it holds this round, plus every task this round's
// wide-preheat broadcast just accepted (that poll ran before the broadcast). A
// pair whose Agent gave no answer at all is absent, so it triggers neither
// Submit nor Cancel until the Controller can see it.
func submittedLedger(
	task *traincachev1alpha1.WarmupTask,
	windows [][]*agentpb.ChunkRef,
	candidates []string,
	observations map[readinessKey]AgentTaskObservation,
	initial bool,
	submission submitResult,
) map[PreheatPair]ExistingPreheat {
	previous := make(map[PreheatPair]ExistingPreheat, len(observations)+len(candidates)*len(windows))
	for key, observation := range observations {
		if !observation.Held {
			continue
		}
		previous[PreheatPair{PodIndex: key.podIndex, Node: key.node}] = ExistingPreheat{
			Generation:   observation.Generation,
			WindowBlocks: observation.WindowBlocks,
		}
	}
	if !initial {
		return previous
	}
	for _, node := range candidates {
		if _, failed := submission.Unavailable[node]; failed {
			continue
		}
		if _, rejected := submission.Permanent[node]; rejected {
			continue
		}
		for pod, window := range windows {
			pair := PreheatPair{PodIndex: int32(pod), Node: node}
			if _, ok := previous[pair]; ok {
				continue
			}
			previous[pair] = ExistingPreheat{
				Generation:   uint64(task.Generation),
				WindowBlocks: uint32(len(window)),
			}
		}
	}
	return previous
}

// anyTaskHeld reports whether some Agent already holds a task for this Warmup
// Task. It is the only signal that the wide-preheat broadcast has run: status
// fields can lag or be dropped, an Agent's own answer cannot.
func anyTaskHeld(observations map[readinessKey]AgentTaskObservation) bool {
	for _, observation := range observations {
		if observation.Held {
			return true
		}
	}
	return false
}

// gateEvaluation bundles the pure decision with the assembled input for logs.
type gateEvaluation struct {
	Decision      gate.Decision
	Pods          []gate.PodGateState
	Fanout        int32
	FanoutMin     int32
	StatusEntries int
}

// evaluateGate assembles PodGateState from status and invokes gate.Evaluate.
func (r *Reconciler) evaluateGate(task *traincachev1alpha1.WarmupTask) gateEvaluation {
	now := r.now()
	pods := make([]gate.PodGateState, 0, task.Spec.PodCount)
	for index := int32(0); index < task.Spec.PodCount; index++ {
		detail, ok := podDetail(task, index)
		if !ok {
			continue
		}
		k := detail.K
		nodes := assignedNodes(task, index)
		readyByNode := make(map[string]int32, len(nodes))
		rate := 0.0
		for _, node := range nodes {
			ready := int32(0)
			if entry := readinessFor(task, index, node); entry != nil {
				ready = entry.ReadyInWindow
			}
			readyByNode[node] = ready
			if k > 0 {
				if nodeRate := float64(ready) / float64(k); nodeRate > rate {
					rate = nodeRate
				}
			}
		}
		prevRate, prevAt, seen := rateSample(r.progress.samples(string(task.UID), index), now)
		pods = append(pods, gate.PodGateState{
			Index:           index,
			AssignedNodes:   nodes,
			ReadyByNode:     readyByNode,
			K:               k,
			Rate:            rate,
			PrevRate:        prevRate,
			PrevAt:          prevAt,
			FirstSampleSeen: seen,
		})
	}

	firstReady := r.ensureFirstReady(task, pods, now)
	policy := task.Spec.Gate
	started := time.Time{}
	if task.Status.StartedAt != nil {
		started = task.Status.StartedAt.Time
	}
	return gateEvaluation{
		Decision: gate.Evaluate(gate.Input{
			Now: now,
			Policy: gate.GatePolicy{
				Threshold:   policy.Threshold,
				GapMin:      policy.GapMin,
				SlopeMin:    policy.SlopeMin,
				TMax:        policy.TMax.Duration,
				MinInterval: policy.MinInterval.Duration,
			},
			Pods:         pods,
			FirstReadyAt: firstReady,
			StartedAt:    started,
		}),
		Pods:          pods,
		Fanout:        normalizeFanout(task.Spec.PreheatFanout),
		FanoutMin:     normalizeFanoutMin(task.Spec.PreheatFanoutMin),
		StatusEntries: len(task.Status.PodNodeReadiness),
	}
}

// ensureFirstReady freezes the first time any Pod reached its integer
// threshold. Once the condition exists it is never updated (D15).
func (r *Reconciler) ensureFirstReady(task *traincachev1alpha1.WarmupTask, pods []gate.PodGateState, now time.Time) time.Time {
	if existing := meta.FindStatusCondition(task.Status.Conditions, ConditionFirstReady); existing != nil {
		return existing.LastTransitionTime.Time
	}
	threshold := task.Spec.Gate.Threshold
	for _, pod := range pods {
		required := requiredReady(threshold, pod.K)
		for _, node := range pod.AssignedNodes {
			if pod.ReadyByNode[node] < required {
				continue
			}
			meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
				Type:               ConditionFirstReady,
				Status:             metav1.ConditionTrue,
				Reason:             reasonFirstReady,
				Message:            fmt.Sprintf("pod %d reached required ready blocks on node %s", pod.Index, node),
				ObservedGeneration: task.Generation,
				LastTransitionTime: metav1.NewTime(now),
			})
			return now
		}
	}
	return time.Time{}
}

// rateSample picks the previous sample. The poll has just recorded `now` when
// the interval allowed it; otherwise the last stored sample is the baseline.
func rateSample(history []RateSample, now time.Time) (float64, time.Time, bool) {
	if len(history) == 0 {
		return 0, time.Time{}, false
	}
	if last := history[len(history)-1]; last.At.Before(now) {
		return last.Rate, last.At, true
	}
	if len(history) >= 2 {
		previous := history[len(history)-2]
		return previous.Rate, previous.At, true
	}
	return 0, time.Time{}, false
}

// logGateDecision records the mandatory M7/M13 fields on every evaluation:
// fanout, per-Pod warm-node counts, the preheat nodes that met the integer
// threshold and the status entry count (docs/7.1 §6.5).
func logGateDecision(log *slog.Logger, taskKey string, evaluation gateEvaluation) {
	n := len(evaluation.Decision.Rates)
	required := make([]int32, n)
	gaps := make([]float64, n)
	slopes := make([]float64, n)
	rates := make([]string, n)
	for i, rate := range evaluation.Decision.Rates {
		required[i] = rate.Required
		gaps[i] = rate.Gap
		slopes[i] = rate.Slope
		rates[i] = fmt.Sprintf("p%d:ready=%d/%d:rate=%.4f",
			rate.PodIndex, rate.Ready, rate.Required, rate.Rate)
	}
	log.Info("gate evaluated",
		"warmup_task", taskKey,
		"state", string(evaluation.Decision.State),
		"reason", evaluation.Decision.Reason,
		"fanout", evaluation.Fanout,
		"fanout_min", evaluation.FanoutMin,
		"fanout_max", evaluation.Fanout,
		"fanout_actual", gateWarmNodeCounts(evaluation.Pods),
		"warm_node_count", gateWarmNodeCounts(evaluation.Pods),
		"status_entries", evaluation.StatusEntries,
		"hit_nodes", gateHitNodes(evaluation),
		"required", required,
		"gap", gaps,
		"slope", slopes,
		"rates", rates,
	)
}

// gateWarmNodeCounts renders each Pod's preheat node count for logs.
func gateWarmNodeCounts(pods []gate.PodGateState) string {
	if len(pods) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(pods))
	for _, pod := range pods {
		parts = append(parts, fmt.Sprintf("p%d=%d", pod.Index, len(pod.AssignedNodes)))
	}
	return strings.Join(parts, ",")
}

// gateHitNodes lists, per Pod, the preheat nodes whose window ready count
// reached the integer threshold. Pods with no hit are omitted.
func gateHitNodes(evaluation gateEvaluation) string {
	rates := evaluation.Decision.Rates
	hits := make([]string, 0, len(evaluation.Pods))
	for i := range evaluation.Pods {
		if i >= len(rates) {
			break
		}
		pod := evaluation.Pods[i]
		var nodes []string
		for _, node := range pod.AssignedNodes {
			if pod.ReadyByNode[node] >= rates[i].Required {
				nodes = append(nodes, node)
			}
		}
		if len(nodes) > 0 {
			hits = append(hits, fmt.Sprintf("p%d=%s", pod.Index, strings.Join(nodes, "+")))
		}
	}
	if len(hits) == 0 {
		return "none"
	}
	return strings.Join(hits, ",")
}

// releaseGate performs the frozen release order from docs/7.1 §7.5:
// scores+gates in one Pod patch, then status. A failed Pod patch leaves that
// Pod gated and annotation-free; already released Pods are not rolled back.
func (r *Reconciler) releaseGate(
	ctx context.Context,
	log *slog.Logger,
	task *traincachev1alpha1.WarmupTask,
	windows [][]*agentpb.ChunkRef,
	podNames map[int32]string,
	evaluation gateEvaluation,
) (ctrl.Result, error) {
	decision := evaluation.Decision
	taskKey := TaskKey(task)

	removed, skipped, err := r.removeGates(ctx, log, task, windows, podNames)
	if err != nil {
		log.Warn("gate removal incomplete",
			"warmup_task", taskKey,
			"state", string(decision.State),
			"reason", decision.Reason,
			"removed", removed,
			"skipped", skipped,
			"err", err,
		)
		return ctrl.Result{}, err
	}

	now := metav1.NewTime(r.now())
	task.Status.GateDecision = traincachev1alpha1.GateDecision{
		State:     traincachev1alpha1.GateState(decision.State),
		Reason:    decision.Reason,
		DecidedAt: &now,
		PodRates:  podRates(decision.Rates),
	}
	switch decision.State {
	case gate.Released:
		task.Status.Phase = traincachev1alpha1.WarmupPhaseReleased
		meta.RemoveStatusCondition(&task.Status.Conditions, ConditionGateDegraded)
		r.setCondition(task, ConditionGateReleased, metav1.ConditionTrue, EventGateReleased,
			fmt.Sprintf("gate released: %s", decision.Reason))
		r.event(task, corev1.EventTypeNormal, EventGateReleased,
			fmt.Sprintf("warmup gate released: %s", decision.Reason))
		log.Info("gate released",
			"warmup_task", taskKey,
			"state", string(decision.State),
			"reason", decision.Reason,
			"removed", removed,
			"skipped", skipped,
		)
	case gate.Degraded:
		task.Status.Phase = traincachev1alpha1.WarmupPhaseDegraded
		meta.RemoveStatusCondition(&task.Status.Conditions, ConditionGateReleased)
		r.setCondition(task, ConditionGateDegraded, metav1.ConditionTrue, EventGateDegraded,
			fmt.Sprintf("gate degraded: %s", decision.Reason))
		r.event(task, corev1.EventTypeWarning, EventGateDegraded,
			fmt.Sprintf("warmup gate degraded: %s", decision.Reason))
		log.Warn("gate degraded",
			"warmup_task", taskKey,
			"state", string(decision.State),
			"reason", decision.Reason,
			"removed", removed,
			"skipped", skipped,
		)
	}
	return ctrl.Result{}, nil
}

func podRates(rates []gate.PodRate) []traincachev1alpha1.PodRate {
	out := make([]traincachev1alpha1.PodRate, len(rates))
	for i, rate := range rates {
		out[i] = traincachev1alpha1.PodRate{
			PodIndex: rate.PodIndex,
			Rate:     rate.Rate,
			Gap:      rate.Gap,
			Slope:    rate.Slope,
		}
	}
	return out
}

// scoreBits decodes the (pod,node) bitmap. Missing or malformed bitmaps score
// as zeros; release must not be blocked by a missing observation.
func scoreBits(task *traincachev1alpha1.WarmupTask, pod int32, node string, requested, k int32) []bool {
	entry := readinessFor(task, pod, node)
	if entry == nil || len(entry.ReadyBlockBitmap) == 0 {
		return nil
	}
	decoded, err := nodescore.DecodeReadiness(entry.ReadyBlockBitmap, requested, k)
	if err != nil {
		return nil
	}
	return decoded.Bits
}

// removeGates patches each selected Pod once: it drops the warmup scheduling
// gate and writes this Pod's node→score snapshot in the same object update
// (D49). The scheduler therefore only sees a schedulable Pod that already
// carries its scores. Pods whose gate is already absent are skipped and their
// annotation is left frozen; a failure on one Pod does not undo the others, and
// the next reconcile retries only the remaining gates.
func (r *Reconciler) removeGates(
	ctx context.Context,
	log *slog.Logger,
	task *traincachev1alpha1.WarmupTask,
	windows [][]*agentpb.ChunkRef,
	podNames map[int32]string,
) (removed, skipped int, err error) {
	gateName := r.GateName
	if gateName == "" {
		gateName = DefaultGateName
	}
	annotationKey := ScoreAnnotationKey(r.ScoreAnnotation)
	ref := TaskRef{Namespace: task.Namespace, Name: task.Name, Generation: task.Generation}

	var errs []error
	for index := int32(0); index < task.Spec.PodCount; index++ {
		name := podNames[index]
		if name == "" {
			skipped++
			continue
		}
		pod := &corev1.Pod{}
		if getErr := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: name}, pod); getErr != nil {
			if apierrors.IsNotFound(getErr) {
				skipped++
				continue
			}
			errs = append(errs, fmt.Errorf("get pod %s: %w", name, getErr))
			continue
		}

		found := false
		for _, schedulingGate := range pod.Spec.SchedulingGates {
			if schedulingGate.Name == gateName {
				found = true
				break
			}
		}
		if !found {
			skipped++
			continue
		}

		nodeScores := podScoreSnapshot(task, log, windows, index, name)
		raw, encodeErr := EncodePodScores(ref, nodeScores, r.now())
		if encodeErr != nil {
			errs = append(errs, fmt.Errorf("encode scores for pod %s: %w", name, encodeErr))
			continue
		}

		base := pod.DeepCopy()
		gates := pod.Spec.SchedulingGates[:0]
		for _, schedulingGate := range pod.Spec.SchedulingGates {
			if schedulingGate.Name != gateName {
				gates = append(gates, schedulingGate)
			}
		}
		pod.Spec.SchedulingGates = gates
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[annotationKey] = raw
		if patchErr := r.Patch(ctx, pod, client.MergeFrom(base)); patchErr != nil {
			errs = append(errs, fmt.Errorf("release pod %s: %w", name, patchErr))
			log.Warn("gate removal failed",
				"warmup_task", TaskKey(task),
				"pod", name,
				"pod_index", index,
				"node", strings.Join(assignedNodes(task, index), ","),
				"annotation_key", annotationKey,
				"err", patchErr,
			)
			continue
		}
		removed++
		log.Info("score annotation written",
			"warmup_task", TaskKey(task),
			"pod", name,
			"pod_index", index,
			"annotation_key", annotationKey,
			"generation", task.Generation,
			"entries", len(nodeScores),
		)
		log.Info("gate removed",
			"warmup_task", TaskKey(task),
			"pod", name,
			"pod_index", index,
			"node", strings.Join(assignedNodes(task, index), ","),
		)
	}
	if len(errs) > 0 {
		return removed, skipped, errors.Join(errs...)
	}
	return removed, skipped, nil
}

// podScoreSnapshot computes S(node) for one Pod on each of its assigned preheat
// nodes. Detail logging stays at Debug; the stored snapshot keeps the integer
// score only.
func podScoreSnapshot(
	task *traincachev1alpha1.WarmupTask,
	log *slog.Logger,
	windows [][]*agentpb.ChunkRef,
	index int32,
	podName string,
) map[string]int32 {
	detail, ok := podDetail(task, index)
	nodeScores := map[string]int32{}
	if !ok {
		return nodeScores
	}
	requested := int32(0)
	if int(index) < len(windows) {
		requested = int32(len(windows[index]))
	}
	for _, node := range assignedNodes(task, index) {
		bits := scoreBits(task, index, node, requested, detail.K)
		score := nodescore.Compute(bits, nodescore.Params{
			K: detail.K,
			H: task.Spec.Score.HalfLife,
			C: task.Spec.Score.Cutoff,
		})
		nodeScores[node] = score.Total
		log.Debug("pod score detail",
			"warmup_task", TaskKey(task),
			"pod", podName,
			"pod_index", index,
			"node", node,
			"k", detail.K,
			"h", task.Spec.Score.HalfLife,
			"c", task.Spec.Score.Cutoff,
			"score", score.Total,
		)
	}
	return nodeScores
}
