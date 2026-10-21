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

// pollReadiness polls every submitted (pod,node) pair, updates the P×N
// readiness table and PodNodeReadiness, and records rateHistory. It never
// returns an error: RPC failures degrade individual pairs and must not stop
// the other polls (docs/8.1 §7.3).
func (r *Reconciler) pollReadiness(
	ctx context.Context,
	log *slog.Logger,
	task *traincachev1alpha1.WarmupTask,
	windows [][]*agentpb.ChunkRef,
	candidates []string,
	podNames map[int32]string,
) pollOutcome {
	if log == nil {
		log = slog.Default()
	}
	pods := int(task.Spec.PodCount)
	generation := uint64(task.Generation)

	existing := make(map[readinessKey]traincachev1alpha1.PodNodeReadiness, len(task.Status.PodNodeReadiness))
	for _, entry := range task.Status.PodNodeReadiness {
		existing[readinessKey{podIndex: entry.PodIndex, node: entry.NodeName}] = entry
	}

	entries := make([]traincachev1alpha1.PodNodeReadiness, 0, pods*len(candidates))
	slots := make([][]int, pods)
	for pod := 0; pod < pods; pod++ {
		k := int32(1)
		if detail, ok := podDetail(task, int32(pod)); ok {
			k = detail.K
		}
		slots[pod] = make([]int, len(candidates))
		for nodeIndex, node := range candidates {
			entry, ok := existing[readinessKey{podIndex: int32(pod), node: node}]
			if !ok {
				entry = traincachev1alpha1.PodNodeReadiness{PodIndex: int32(pod), NodeName: node}
			}
			entry.PodIndex = int32(pod)
			entry.PodName = podNames[int32(pod)]
			entry.NodeName = node
			if entry.State == "" {
				entry.State = traincachev1alpha1.PreheatStatePreheating
			}
			entry.RequiredReady = requiredReady(task.Spec.Gate.Threshold, k)
			entries = append(entries, entry)
			slots[pod][nodeIndex] = len(entries) - 1
		}
	}

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
			entry := &entries[slots[pod][nodeIndex]]
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
					attempt := r.progress.fail(attemptKey)
					pairLog.Warn("agent progress unavailable",
						"attempt", attempt,
						"duration_ms", duration,
						"err", err,
					)
					if attempt >= unavailableFailureThreshold {
						entry.State = traincachev1alpha1.PreheatStateFailed
						pairLog.Warn("preheat progress degraded",
							"attempt", attempt,
							"state", string(traincachev1alpha1.PreheatStateFailed),
							"duration_ms", duration,
						)
					}
				} else {
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
				continue
			}

			if resp.GetGeneration() != generation {
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
				pairLog.Debug("drop progress: "+reason,
					"seq", resp.GetSeq(),
					"duration_ms", duration,
				)
				continue
			}

			decoded, decodeErr := nodescore.DecodeReadiness(resp.GetReadyBitmap(), chunkCount, k)
			if decodeErr != nil {
				// A malformed bitmap must not change the table (criterion ②).
				pairLog.Warn("drop progress: invalid ready bitmap",
					"seq", resp.GetSeq(),
					"duration_ms", duration,
					"err", decodeErr,
				)
				continue
			}

			r.progress.record(markKey, generation, resp.GetSeq())
			entry.ReadyBlockBitmap = append([]byte(nil), resp.GetReadyBitmap()...)
			entry.StartupWindowReady = decoded.ReadyInWindow
			entry.ReadyInWindow = decoded.ReadyInWindow
			entry.StartupWindowReadyRate = windowReadyRate(decoded.ReadyInWindow, k)
			entry.State = preheatState(resp)
			entry.LastReportTime = reportTime(resp, r.now())

			pairLog.Debug("progress updated",
				"seq", resp.GetSeq(),
				"ready", decoded.ReadyInWindow,
				"ready_total", decoded.ReadyTotal,
				"duration_ms", duration,
			)
		}
	}

	// Sample one rate per Pod. After GWAA (M6) has recorded AssignedNode the
	// sample follows that node (D14); before assignment it follows the best
	// reported candidate. Samples stay in memory only.
	minInterval := task.Spec.Gate.MinInterval.Duration
	now := r.now()
	for pod := 0; pod < pods; pod++ {
		if !reported[pod] {
			continue
		}
		assigned := ""
		for _, index := range slots[pod] {
			if entries[index].AssignedNode != "" {
				assigned = entries[index].AssignedNode
				break
			}
		}
		best := 0.0
		found := false
		for _, index := range slots[pod] {
			entry := entries[index]
			if entry.LastReportTime == nil {
				continue
			}
			if assigned != "" && entry.NodeName != assigned {
				continue
			}
			if !found || entry.StartupWindowReadyRate > best {
				best = entry.StartupWindowReadyRate
				found = true
			}
		}
		if found {
			r.progress.recordRate(string(task.UID), int32(pod), now, best, minInterval)
		}
	}

	table := make([][]int32, pods)
	for pod := 0; pod < pods; pod++ {
		table[pod] = make([]int32, len(candidates))
		for nodeIndex := range candidates {
			table[pod][nodeIndex] = entries[slots[pod][nodeIndex]].ReadyInWindow
		}
	}

	before := task.Status.DeepCopy()
	task.Status.ReadinessTable = table
	task.Status.PodNodeReadiness = entries
	if !equality.Semantic.DeepEqual(before, &task.Status) {
		lastUpdated := metav1.NewTime(now)
		task.Status.LastUpdated = &lastUpdated
	}

	pending := false
	for i := range entries {
		if entries[i].State != traincachev1alpha1.PreheatStateReady &&
			entries[i].State != traincachev1alpha1.PreheatStateFailed {
			pending = true
			break
		}
	}
	return pollOutcome{Pending: pending && len(entries) > 0}
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
