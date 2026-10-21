// Package metrics owns the control plane's independent Prometheus registry and
// the bounded-label metric families frozen by docs/8.1 design.md §9.5.
//
// The controller must not share the process-global default registry. New
// builds the registry owned by the controller-manager (controller families)
// and NewScheduler builds the one owned by the scheduler process (scheduler
// family only); both return an independent registry.
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Registry is a controller-owned Prometheus registry. It exposes the
// Registerer and Gatherer views of the same underlying registry so callers can
// register families or expose them without depending on a concrete type.
type Registry struct {
	prometheus.Registerer
	prometheus.Gatherer
}

// Controller is the fixed controller label enum. Values match the controller
// names registered in cmd/controller-manager/main.go.
type Controller string

const (
	ControllerDataset        Controller = "dataset"
	ControllerWarmup         Controller = "warmup"
	ControllerWarmupDegraded Controller = "warmup-degraded"
)

// ReconcileResult is the fixed reconcile outcome enum. A nil error with a
// positive RequeueAfter is a requeue; an error always wins.
type ReconcileResult string

const (
	ReconcileSuccess ReconcileResult = "success"
	ReconcileError   ReconcileResult = "error"
	ReconcileRequeue ReconcileResult = "requeue"
)

// PreheatOp is the fixed Agent preheat RPC operation enum.
type PreheatOp string

const (
	PreheatSubmit PreheatOp = "submit"
	PreheatUpdate PreheatOp = "update"
	PreheatCancel PreheatOp = "cancel"
)

// RPCResult is the fixed preheat RPC outcome enum. It mirrors the existing
// ErrPermanent / ErrAgentUnavailable split.
type RPCResult string

const (
	RPCOK          RPCResult = "ok"
	RPCPermanent   RPCResult = "permanent"
	RPCUnavailable RPCResult = "unavailable"
)

// GateState is the gate_tasks label enum. It is lower-case on purpose; the
// status GateState in the gate package is a separate, capitalized type.
type GateState string

const (
	GateHold     GateState = "hold"
	GateReleased GateState = "released"
	GateDegraded GateState = "degraded"
)

// PollResult is the fixed GetPreheatStatus poll outcome enum. stale covers a
// response dropped because its generation or seq went backwards.
type PollResult string

const (
	PollOK          PollResult = "ok"
	PollUnavailable PollResult = "unavailable"
	PollStale       PollResult = "stale"
	PollError       PollResult = "error"
)

// ScoreResult is the fixed scheduler Evaluate branch enum.
type ScoreResult string

const (
	ScoreOK              ScoreResult = "ok"
	ScoreNoTaskRef       ScoreResult = "no_task_ref"
	ScoreInvalidTaskRef  ScoreResult = "invalid_task_ref"
	ScoreNoPodIndex      ScoreResult = "no_pod_index"
	ScoreInvalidPodIndex ScoreResult = "invalid_pod_index"
	ScoreNoAnnotation    ScoreResult = "no_annotation"
	ScoreInvalidValue    ScoreResult = "invalid_value"
	ScoreTaskMismatch    ScoreResult = "task_mismatch"
	ScoreStaleGeneration ScoreResult = "stale_generation"
	ScoreExpired         ScoreResult = "expired"
	ScoreNoPodEntry      ScoreResult = "no_pod_entry"
)

// gateEntry is the in-memory state used to keep gate_tasks balanced. The task
// identity is a map key, never a label.
type gateEntry struct {
	state     GateState
	holdSince time.Time
}

// Metrics bundles the independent registry with the typed metric families.
//
// Every emit method is nil-safe so tests and callers that do not need metrics
// can leave Reconciler.Metrics unset.
type Metrics struct {
	*Registry

	reconcileTotal    *prometheus.CounterVec
	reconcileDuration *prometheus.HistogramVec
	candidateNodes    *prometheus.HistogramVec
	preheatRPC        *prometheus.CounterVec
	gateTasks         *prometheus.GaugeVec
	gateHoldDuration  prometheus.Histogram
	agentPoll         *prometheus.CounterVec
	podsPerNode       prometheus.Histogram
	gwaaUnassigned    *prometheus.GaugeVec
	gwaaFallback      prometheus.Counter
	schedulerScore    *prometheus.CounterVec

	gateMu     sync.Mutex
	gateStates map[string]gateEntry
}

// New returns the controller-manager registry with the ten controller
// families registered. Every call creates an independent registry; the
// process-global registry is never touched.
func New() *Metrics {
	return newMetrics(false)
}

// NewScheduler returns an independent registry with only the scheduler
// process family registered. The scheduler binary runs in its own process and
// must not expose the controller-only families as empty series.
func NewScheduler() *Metrics {
	return newMetrics(true)
}

func newMetrics(schedulerOnly bool) *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		Registry: &Registry{Registerer: registry, Gatherer: registry},
		reconcileTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "traincache_controller_reconcile_total",
			Help: "Number of reconciles by controller and result.",
		}, []string{"controller", "result"}),
		reconcileDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "traincache_controller_reconcile_duration_seconds",
			Help:    "Reconcile duration in seconds by controller.",
			Buckets: prometheus.DefBuckets,
		}, []string{"controller"}),
		candidateNodes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "traincache_controller_candidate_nodes",
			Help:    "Number of candidate Nodes observed per WarmupTask reconcile.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256},
		}, []string{"dataset"}),
		preheatRPC: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "traincache_controller_preheat_rpc_total",
			Help: "Number of Agent preheat RPCs by operation and result.",
		}, []string{"op", "result"}),
		gateTasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "traincache_controller_gate_tasks",
			Help: "WarmupTasks currently in each gate state.",
		}, []string{"state"}),
		gateHoldDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "traincache_controller_gate_hold_duration_seconds",
			Help:    "Time a WarmupTask spent in the Hold gate state.",
			Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200},
		}),
		agentPoll: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "traincache_controller_agent_poll_total",
			Help: "Number of Agent GetPreheatStatus polls by result.",
		}, []string{"result"}),
		podsPerNode: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "traincache_controller_pods_per_node",
			Help:    "Number of Pods assigned to each node by GWAA.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
		}),
		gwaaUnassigned: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "traincache_controller_gwaa_unassigned_pods",
			Help: "Pods GWAA could not assign for a DataSet.",
		}, []string{"dataset"}),
		gwaaFallback: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "traincache_controller_gwaa_fallback_total",
			Help: "Number of GWAA computations that fell back to the first greedy result.",
		}),
		schedulerScore: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "traincache_scheduler_score_total",
			Help: "Number of scheduler score evaluations by result.",
		}, []string{"result"}),
		gateStates: map[string]gateEntry{},
	}

	if schedulerOnly {
		registry.MustRegister(m.schedulerScore)
		return m
	}
	registry.MustRegister(
		m.reconcileTotal,
		m.reconcileDuration,
		m.candidateNodes,
		m.preheatRPC,
		m.gateTasks,
		m.gateHoldDuration,
		m.agentPoll,
		m.podsPerNode,
		m.gwaaUnassigned,
		m.gwaaFallback,
	)
	return m
}

// ObserveReconcile records one reconcile outcome and its duration. Result
// priority: an error wins, then a positive requeue, otherwise success.
func (m *Metrics) ObserveReconcile(controller Controller, err error, requeueAfter, duration time.Duration) {
	if m == nil || !validController(controller) {
		return
	}
	result := ReconcileSuccess
	switch {
	case err != nil:
		result = ReconcileError
	case requeueAfter > 0:
		result = ReconcileRequeue
	}
	seconds := duration.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	m.reconcileTotal.WithLabelValues(string(controller), string(result)).Inc()
	m.reconcileDuration.WithLabelValues(string(controller)).Observe(seconds)
}

// ObserveCandidateNodes records len(candidates) for one reconcile. dataset is
// the DataSet key "<namespace>/<name>".
func (m *Metrics) ObserveCandidateNodes(dataset string, count int) {
	if m == nil || dataset == "" || count < 0 {
		return
	}
	m.candidateNodes.WithLabelValues(dataset).Observe(float64(count))
}

// ObservePreheatRPC records one Agent preheat RPC outcome.
func (m *Metrics) ObservePreheatRPC(op PreheatOp, result RPCResult) {
	if m == nil || !validPreheatOp(op) || !validRPCResult(result) {
		return
	}
	m.preheatRPC.WithLabelValues(string(op), string(result)).Inc()
}

// ObserveGateTask transitions the gate_tasks gauge for one task. The first
// evaluation inserts the task; later evaluations move it between states.
// Leaving Hold observes gate_hold_duration_seconds exactly once. A repeated
// state is a no-op, so the gauge cannot drift.
func (m *Metrics) ObserveGateTask(key string, state GateState, now time.Time) {
	if m == nil || key == "" || !validGateState(state) {
		return
	}
	m.gateMu.Lock()
	defer m.gateMu.Unlock()
	if m.gateStates == nil {
		m.gateStates = map[string]gateEntry{}
	}

	prev, ok := m.gateStates[key]
	if ok && prev.state == state {
		return
	}
	if ok {
		m.gateTasks.WithLabelValues(string(prev.state)).Dec()
	}

	entry := gateEntry{state: state}
	switch {
	case state == GateHold:
		entry.holdSince = now
	case ok && prev.state == GateHold && !prev.holdSince.IsZero():
		held := now.Sub(prev.holdSince)
		if held < 0 {
			held = 0
		}
		m.gateHoldDuration.Observe(held.Seconds())
	}

	m.gateStates[key] = entry
	m.gateTasks.WithLabelValues(string(state)).Inc()
}

// ForgetGateTask removes a task from gate_tasks. It must be called when the
// task is deleted or reaches a terminal phase.
func (m *Metrics) ForgetGateTask(key string) {
	if m == nil || key == "" {
		return
	}
	m.gateMu.Lock()
	defer m.gateMu.Unlock()
	prev, ok := m.gateStates[key]
	if !ok {
		return
	}
	m.gateTasks.WithLabelValues(string(prev.state)).Dec()
	delete(m.gateStates, key)
}

// ObserveAgentPoll records one GetPreheatStatus poll outcome.
func (m *Metrics) ObserveAgentPoll(result PollResult) {
	if m == nil || !validPollResult(result) {
		return
	}
	m.agentPoll.WithLabelValues(string(result)).Inc()
}

// ObservePlacement records the metrics derived from one GWAA computation:
// pods_per_node for every node in PreheatSet, the dataset's unassigned-pod
// gauge, and the non-convergence fallback counter.
func (m *Metrics) ObservePlacement(dataset string, podsPerNode map[string][]int32, unassigned int, fellBack bool) {
	if m == nil {
		return
	}
	for _, pods := range podsPerNode {
		m.podsPerNode.Observe(float64(len(pods)))
	}
	if dataset != "" {
		m.gwaaUnassigned.WithLabelValues(dataset).Set(float64(unassigned))
	}
	if fellBack {
		m.gwaaFallback.Inc()
	}
}

// ObserveSchedulerScore records one scheduler Evaluate outcome.
func (m *Metrics) ObserveSchedulerScore(result ScoreResult) {
	if m == nil || !validScoreResult(result) {
		return
	}
	m.schedulerScore.WithLabelValues(string(result)).Inc()
}

func validController(value Controller) bool {
	switch value {
	case ControllerDataset, ControllerWarmup, ControllerWarmupDegraded:
		return true
	}
	return false
}

func validPreheatOp(value PreheatOp) bool {
	switch value {
	case PreheatSubmit, PreheatUpdate, PreheatCancel:
		return true
	}
	return false
}

func validRPCResult(value RPCResult) bool {
	switch value {
	case RPCOK, RPCPermanent, RPCUnavailable:
		return true
	}
	return false
}

func validGateState(value GateState) bool {
	switch value {
	case GateHold, GateReleased, GateDegraded:
		return true
	}
	return false
}

func validPollResult(value PollResult) bool {
	switch value {
	case PollOK, PollUnavailable, PollStale, PollError:
		return true
	}
	return false
}

func validScoreResult(value ScoreResult) bool {
	switch value {
	case ScoreOK, ScoreNoTaskRef, ScoreInvalidTaskRef, ScoreNoPodIndex,
		ScoreInvalidPodIndex, ScoreNoAnnotation, ScoreInvalidValue,
		ScoreTaskMismatch, ScoreStaleGeneration, ScoreExpired, ScoreNoPodEntry:
		return true
	}
	return false
}
