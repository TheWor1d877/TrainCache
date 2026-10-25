package warmup

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/metrics"
	"traincache/controller/internal/nodescore"
)

const (
	// defaultPollInterval and maxPollInterval bound the requeue delay even
	// when Reconciler.PollInterval is unset or misconfigured.
	defaultPollInterval = 2 * time.Second
	maxPollInterval     = 5 * time.Minute

	// unavailableFailureThreshold is the D24/§7.3 consecutive
	// ErrAgentUnavailable count after which the (pod,node) pair is Failed.
	unavailableFailureThreshold = 3

	maxRateSamples = 2
)

// RateSample is one in-memory slope observation for a Pod. rateHistory is
// never persisted (D24) and is empty after a controller restart.
type RateSample struct {
	At   time.Time
	Rate float64
}

type progressKey struct {
	taskUID  string
	podIndex int32
	node     string
}

type progressMark struct {
	generation uint64
	seq        uint64
	seen       bool
}

type failureKey struct {
	taskUID    string
	podIndex   int32
	node       string
	generation uint64
}

// pollTracker owns the only mutable in-memory polling state: the accepted
// generation/seq high-water mark, consecutive failure counters and rateHistory.
type pollTracker struct {
	mu          sync.Mutex
	marks       map[progressKey]progressMark
	failures    map[failureKey]int
	rateHistory map[string][]RateSample
}

// check reports whether a response is fresh. A repeated seq is treated as
// "no change" (plan M5 criterion ④). The mark is only recorded after the
// payload decodes, so a malformed response does not poison the high-water mark.
func (t *pollTracker) check(generation, seq uint64, key progressKey) (bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if mark, ok := t.marks[key]; ok && mark.seen {
		switch {
		case generation < mark.generation:
			return false, "old generation"
		case generation == mark.generation && seq <= mark.seq:
			return false, "stale seq"
		}
	}
	return true, ""
}

func (t *pollTracker) record(key progressKey, generation, seq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.marks == nil {
		t.marks = map[progressKey]progressMark{}
	}
	t.marks[key] = progressMark{generation: generation, seq: seq, seen: true}
}

func (t *pollTracker) fail(key failureKey) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failures == nil {
		t.failures = map[failureKey]int{}
	}
	t.failures[key]++
	return t.failures[key]
}

func (t *pollTracker) clearFailures(key failureKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.failures, key)
}

// nodeFailed reports whether every Pod crossed the consecutive
// ErrAgentUnavailable threshold on node in the current generation. It restores
// the pre-M12 "stopped node" signal that no longer has a persisted per-pair
// State.
func (t *pollTracker) nodeFailed(taskUID, node string, generation uint64, pods int) bool {
	if pods <= 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for pod := 0; pod < pods; pod++ {
		key := failureKey{
			taskUID:    taskUID,
			podIndex:   int32(pod),
			node:       node,
			generation: generation,
		}
		if t.failures[key] < unavailableFailureThreshold {
			return false
		}
	}
	return true
}

// recordRate keeps at most the two most recent samples and drops a sample that
// arrives inside MinInterval of the previous one (D24, condition ② input).
func (t *pollTracker) recordRate(taskUID string, podIndex int32, at time.Time, rate float64, minInterval time.Duration) {
	if minInterval < 0 {
		minInterval = 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rateHistory == nil {
		t.rateHistory = map[string][]RateSample{}
	}
	key := rateHistoryKey(taskUID, podIndex)
	history := t.rateHistory[key]
	if len(history) > 0 && at.Sub(history[len(history)-1].At) < minInterval {
		return
	}
	history = append(history, RateSample{At: at, Rate: rate})
	if len(history) > maxRateSamples {
		history = history[len(history)-maxRateSamples:]
	}
	t.rateHistory[key] = history
}

func (t *pollTracker) samples(taskUID string, podIndex int32) []RateSample {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]RateSample(nil), t.rateHistory[rateHistoryKey(taskUID, podIndex)]...)
}

func rateHistoryKey(taskUID string, podIndex int32) string {
	return taskUID + "/p" + strconv.FormatInt(int64(podIndex), 10)
}

type pollOutcome struct {
	// Pending is true while at least one (pod,node) pair has not reached a
	// terminal Ready/Failed state.
	Pending bool
}

// readinessFact is one poll round's observation for a (Pod, candidate node).
// It is never persisted: M12/S15 keeps only the assigned node's fact per Pod
// in status, while candidate-node counts live in ReadinessTable.
type readinessFact struct {
	ready          int32
	bitmap         []byte
	rate           float64
	state          traincachev1alpha1.PreheatState
	lastReportTime *metav1.Time
	hasData        bool
}

// pollReadiness polls every submitted (pod,node) pair, updates the P×N
// readiness table, the per-Pod assigned-node facts and rateHistory. It never
// returns an error: RPC failures degrade individual pairs and must not stop
// the other polls (docs/7.1 §7.3). The returned facts are this round's
// in-memory observations; applyAssignment uses them to refresh the per-Pod
// details after GWAA has (re)assigned nodes.
func (r *Reconciler) pollReadiness(
	ctx context.Context,
	log *slog.Logger,
	task *traincachev1alpha1.WarmupTask,
	windows [][]*agentpb.ChunkRef,
	candidates []string,
	podNames map[int32]string,
) (pollOutcome, map[readinessKey]readinessFact) {
	if log == nil {
		log = slog.Default()
	}
	pods := int(task.Spec.PodCount)
	generation := uint64(task.Generation)
	facts := make(map[readinessKey]readinessFact)
	table := remapReadinessTable(task, pods, candidates)

	reported := make([]bool, pods)
	for pod := 0; pod < pods; pod++ {
		k := int32(1)
		if detail, ok := podDetail(task, int32(pod)); ok {
			k = detail.K
		}
		chunkCount := int32(0)
		if pod < len(windows) {
			chunkCount = int32(len(windows[pod]))
		}
		taskID := TaskID(task, int32(pod))
		podName := podNames[int32(pod)]

		for nodeIndex, node := range candidates {
			pairLog := log.With(
				"task_id", taskID,
				"pod", podName,
				"pod_index", pod,
				"node", node,
				"generation", generation,
			)
			attemptKey := failureKey{
				taskUID:    string(task.UID),
				podIndex:   int32(pod),
				node:       node,
				generation: generation,
			}

			start := r.now()
			resp, err := r.Agents.Progress(ctx, node, taskID, generation)
			duration := r.now().Sub(start).Milliseconds()
			if err != nil {
				if errors.Is(err, agentclient.ErrAgentUnavailable) {
					r.Metrics.ObserveAgentPoll(metrics.PollUnavailable)
					attempt := r.progress.fail(attemptKey)
					pairLog.Warn("agent progress unavailable",
						"attempt", attempt,
						"duration_ms", duration,
						"err", err,
					)
					if attempt >= unavailableFailureThreshold {
						facts[readinessKey{podIndex: int32(pod), node: node}] = readinessFact{
							state: traincachev1alpha1.PreheatStateFailed,
						}
						pairLog.Warn("preheat progress degraded",
							"attempt", attempt,
							"state", string(traincachev1alpha1.PreheatStateFailed),
							"duration_ms", duration,
						)
					}
				} else {
					r.Metrics.ObserveAgentPoll(metrics.PollError)
					pairLog.Warn("agent progress failed",
						"duration_ms", duration,
						"err", err,
					)
				}
				continue
			}
			r.progress.clearFailures(attemptKey)
			if resp == nil {
				// Tests and defensive callers may return an empty payload.
				r.Metrics.ObserveAgentPoll(metrics.PollError)
				continue
			}

			if resp.GetGeneration() != generation {
				r.Metrics.ObserveAgentPoll(metrics.PollStale)
				pairLog.Debug("drop progress: generation mismatch",
					"response_generation", resp.GetGeneration(),
					"seq", resp.GetSeq(),
					"duration_ms", duration,
				)
				continue
			}

			markKey := progressKey{
				taskUID:  string(task.UID),
				podIndex: int32(pod),
				node:     node,
			}
			fresh, reason := r.progress.check(generation, resp.GetSeq(), markKey)
			reported[pod] = true
			if !fresh {
				r.Metrics.ObserveAgentPoll(metrics.PollStale)
				pairLog.Debug("drop progress: "+reason,
					"seq", resp.GetSeq(),
					"duration_ms", duration,
				)
				continue
			}

			decoded, decodeErr := nodescore.DecodeReadiness(resp.GetReadyBitmap(), chunkCount, k)
			if decodeErr != nil {
				// A malformed bitmap must not change the table (criterion ②).
				r.Metrics.ObserveAgentPoll(metrics.PollError)
				pairLog.Warn("drop progress: invalid ready bitmap",
					"seq", resp.GetSeq(),
					"duration_ms", duration,
					"err", decodeErr,
				)
				continue
			}

			r.progress.record(markKey, generation, resp.GetSeq())
			r.Metrics.ObserveAgentPoll(metrics.PollOK)
			fact := readinessFact{
				ready:          decoded.ReadyInWindow,
				bitmap:         append([]byte(nil), resp.GetReadyBitmap()...),
				rate:           windowReadyRate(decoded.ReadyInWindow, k),
				state:          preheatState(resp),
				lastReportTime: reportTime(resp, r.now()),
				hasData:        true,
			}
			facts[readinessKey{podIndex: int32(pod), node: node}] = fact
			table[pod][nodeIndex] = decoded.ReadyInWindow

			pairLog.Debug("progress updated",
				"seq", resp.GetSeq(),
				"ready", decoded.ReadyInWindow,
				"ready_total", decoded.ReadyTotal,
				"duration_ms", duration,
			)
		}
	}

	// Sample one rate per Pod. After GWAA (M6/M13) has recorded the Pod's
	// preheat node set the sample is the maximum over that set (D14); before
	// assignment it follows the best reported candidate. Samples stay in
	// memory only.
	minInterval := task.Spec.Gate.MinInterval.Duration
	now := r.now()
	for pod := 0; pod < pods; pod++ {
		if !reported[pod] {
			continue
		}
		nodes := assignedNodes(task, int32(pod))
		best := 0.0
		found := false
		if len(nodes) > 0 {
			for _, node := range nodes {
				if fact, ok := facts[readinessKey{podIndex: int32(pod), node: node}]; ok && fact.hasData {
					if !found || fact.rate > best {
						best, found = fact.rate, true
					}
					continue
				}
				if prev := readinessFor(task, int32(pod), node); prev != nil && prev.LastReportTime != nil {
					if !found || prev.StartupWindowReadyRate > best {
						best, found = prev.StartupWindowReadyRate, true
					}
				}
			}
		} else {
			for _, node := range candidates {
				fact, ok := facts[readinessKey{podIndex: int32(pod), node: node}]
				if !ok || !fact.hasData {
					continue
				}
				if !found || fact.rate > best {
					best, found = fact.rate, true
				}
			}
		}
		if found {
			r.progress.recordRate(string(task.UID), int32(pod), now, best, minInterval)
		}
	}

	before := task.Status.DeepCopy()
	task.Status.ReadinessTable = table
	buildPodDetails(task, facts, podNames, candidates, nil, nil)
	if !equality.Semantic.DeepEqual(before, &task.Status) {
		lastUpdated := metav1.NewTime(now)
		task.Status.LastUpdated = &lastUpdated
	}

	return pollOutcome{Pending: pollPending(task, facts, candidates, pods)}, facts
}

// remapReadinessTable starts from the published table and remaps its columns
// by node name. Keeping the last known counts for candidate columns that were
// not observed this round preserves the pre-M12 table semantics without
// persisting P×N per-node details.
func remapReadinessTable(task *traincachev1alpha1.WarmupTask, pods int, candidates []string) [][]int32 {
	oldColumns := make(map[string]int, len(task.Status.CandidateNodes))
	for column, node := range task.Status.CandidateNodes {
		oldColumns[node] = column
	}
	table := make([][]int32, pods)
	for pod := 0; pod < pods; pod++ {
		table[pod] = make([]int32, len(candidates))
		for column, node := range candidates {
			oldColumn, ok := oldColumns[node]
			if !ok || pod >= len(task.Status.ReadinessTable) {
				continue
			}
			oldRow := task.Status.ReadinessTable[pod]
			if oldColumn >= 0 && oldColumn < len(oldRow) {
				table[pod][column] = oldRow[oldColumn]
			}
		}
	}
	return table
}

// pollPending reports whether any (Pod, candidate node) pair is still
// non-terminal. A fresh observation is authoritative; without one, a fully
// ready table cell counts as terminal and anything else keeps polling.
func pollPending(task *traincachev1alpha1.WarmupTask, facts map[readinessKey]readinessFact, candidates []string, pods int) bool {
	for pod := 0; pod < pods; pod++ {
		k := int32(1)
		if detail, ok := podDetail(task, int32(pod)); ok {
			k = detail.K
		}
		for column, node := range candidates {
			if fact, ok := facts[readinessKey{podIndex: int32(pod), node: node}]; ok {
				if fact.state != traincachev1alpha1.PreheatStateReady &&
					fact.state != traincachev1alpha1.PreheatStateFailed {
					return true
				}
				continue
			}
			if pod < len(task.Status.ReadinessTable) &&
				column < len(task.Status.ReadinessTable[pod]) &&
				task.Status.ReadinessTable[pod][column] >= k {
				continue
			}
			return true
		}
	}
	return false
}

// buildPodDetails writes one PodNodeReadiness per (Pod, preheat node) pair
// (M13). assignment == nil keeps the node set currently recorded in status.
// minUnmet is a per-Pod map produced by GWAA; when non-nil, every entry of
// the same Pod carries the same MinUnmet flag (D40). nil keeps the previous
// value via carryReadinessFact.
// Each entry's fact is this round's observation for the pair; without a fresh
// observation the previous fact is preserved when the pair did not change. A
// Pod with no feasible preheat node gets exactly one summary entry with an
// empty NodeName and the Pod-level aggregate State. ActualNode is Pod-wide, so
// every entry of the same Pod carries the same value.
func buildPodDetails(
	task *traincachev1alpha1.WarmupTask,
	facts map[readinessKey]readinessFact,
	podNames map[int32]string,
	candidates []string,
	assignment map[int32][]string,
	minUnmet map[int32]bool,
) {
	previous := make(map[readinessKey]traincachev1alpha1.PodNodeReadiness, len(task.Status.PodNodeReadiness))
	actual := make(map[int32]string, task.Spec.PodCount)
	for _, entry := range task.Status.PodNodeReadiness {
		if entry.NodeName != "" {
			previous[readinessKey{podIndex: entry.PodIndex, node: entry.NodeName}] = entry
		}
		if entry.ActualNode != "" && actual[entry.PodIndex] == "" {
			actual[entry.PodIndex] = entry.ActualNode
		}
	}

	capacity := len(task.Spec.PodDetails) * int(normalizeFanout(task.Spec.PreheatFanout))
	entries := make([]traincachev1alpha1.PodNodeReadiness, 0, capacity)
	for index := int32(0); index < task.Spec.PodCount; index++ {
		detail, ok := podDetail(task, index)
		if !ok {
			continue
		}
		podMinUnmet := false
		if minUnmet != nil {
			podMinUnmet = minUnmet[index]
		}
		nodes := assignedNodes(task, index)
		if assignment != nil {
			nodes = assignment[index]
		}
		nodes = uniqueStrings(nodes)
		if len(nodes) == 0 {
			entries = append(entries, traincachev1alpha1.PodNodeReadiness{
				PodIndex:      index,
				PodName:       podNames[index],
				ActualNode:    actual[index],
				RequiredReady: requiredReady(task.Spec.Gate.Threshold, detail.K),
				State:         aggregatePodState(facts, index, candidates),
				MinUnmet:      podMinUnmet,
			})
			continue
		}
		for _, node := range nodes {
			prev, hadPrev := previous[readinessKey{podIndex: index, node: node}]
			entry := traincachev1alpha1.PodNodeReadiness{
				PodIndex:      index,
				PodName:       podNames[index],
				NodeName:      node,
				ActualNode:    actual[index],
				RequiredReady: requiredReady(task.Spec.Gate.Threshold, detail.K),
			}
			fact, hasFact := facts[readinessKey{podIndex: index, node: node}]
			switch {
			case hasFact && fact.hasData:
				applyReadinessFact(&entry, fact)
			case hasFact:
				entry.State = fact.state
			case hadPrev:
				carryReadinessFact(&entry, prev)
			default:
				entry.State = traincachev1alpha1.PreheatStatePreheating
			}
			if minUnmet != nil {
				entry.MinUnmet = podMinUnmet
			}
			entries = append(entries, entry)
		}
	}
	task.Status.PodNodeReadiness = entries
}

// uniqueStrings removes empty and duplicate node names while preserving the
// top-K order.
func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, dup := seen[value]; dup {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func applyReadinessFact(entry *traincachev1alpha1.PodNodeReadiness, fact readinessFact) {
	entry.ReadyBlockBitmap = append([]byte(nil), fact.bitmap...)
	entry.StartupWindowReady = fact.ready
	entry.ReadyInWindow = fact.ready
	entry.StartupWindowReadyRate = fact.rate
	entry.LastReportTime = fact.lastReportTime
	entry.State = fact.state
}

func carryReadinessFact(entry *traincachev1alpha1.PodNodeReadiness, prev traincachev1alpha1.PodNodeReadiness) {
	entry.ReadyBlockBitmap = append([]byte(nil), prev.ReadyBlockBitmap...)
	entry.StartupWindowReady = prev.StartupWindowReady
	entry.ReadyInWindow = prev.ReadyInWindow
	entry.StartupWindowReadyRate = prev.StartupWindowReadyRate
	entry.GlobalDecayScore = prev.GlobalDecayScore
	entry.State = prev.State
	entry.LastReportTime = prev.LastReportTime
	entry.MinUnmet = prev.MinUnmet
}

// aggregatePodState summarizes an unassigned Pod: Ready when any candidate is
// Ready, Failed only when every candidate is Failed, otherwise Preheating.
func aggregatePodState(facts map[readinessKey]readinessFact, pod int32, candidates []string) traincachev1alpha1.PreheatState {
	seen := false
	anyReady := false
	allFailed := len(candidates) > 0
	for _, node := range candidates {
		fact, ok := facts[readinessKey{podIndex: pod, node: node}]
		if !ok {
			allFailed = false
			continue
		}
		seen = true
		if fact.state == traincachev1alpha1.PreheatStateReady {
			anyReady = true
		}
		if fact.state != traincachev1alpha1.PreheatStateFailed {
			allFailed = false
		}
	}
	if seen && allFailed {
		return traincachev1alpha1.PreheatStateFailed
	}
	if anyReady {
		return traincachev1alpha1.PreheatStateReady
	}
	return traincachev1alpha1.PreheatStatePreheating
}

// readinessByPod returns the first PodNodeReadiness entry whose PodIndex
// matches pod. After M13 a Pod normally has one entry per preheat node, so this
// helper does not identify a specific node; it is currently only used by tests
// that expect a single-entry Pod. Production code uses readinessFor to select
// a (Pod, node) pair.
func readinessByPod(task *traincachev1alpha1.WarmupTask, pod int32) *traincachev1alpha1.PodNodeReadiness {
	for i := range task.Status.PodNodeReadiness {
		if task.Status.PodNodeReadiness[i].PodIndex == pod {
			return &task.Status.PodNodeReadiness[i]
		}
	}
	return nil
}

// pollRequeueAfter returns the bounded polling delay. Terminal tasks stop
// requeueing so the controller does not spin.
func (r *Reconciler) pollRequeueAfter() time.Duration {
	interval := r.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	if interval > maxPollInterval {
		interval = maxPollInterval
	}
	return interval
}
