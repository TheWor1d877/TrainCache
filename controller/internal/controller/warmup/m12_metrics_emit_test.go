package warmup

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	agentpb "traincache/agent/api/agentpb"
	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/metrics"
)

// TestReconcileNotFoundEmitsReconcileMetrics proves the entry/exit defer runs
// before the first Get and also clears a stale gate_tasks entry.
func TestReconcileNotFoundEmitsReconcileMetrics(t *testing.T) {
	m := metrics.New()
	m.ObserveGateTask("train/missing", metrics.GateHold, time.Unix(100, 0))
	r := &Reconciler{Client: fixtureClient(t), Metrics: m}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "train", Name: "missing"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := emitCounter(t, m, "traincache_controller_reconcile_total", map[string]string{
		"controller": "warmup", "result": "success",
	}); got != 1 {
		t.Errorf("warmup success count = %v, want 1", got)
	}
	if got := emitHistogramCount(t, m, "traincache_controller_reconcile_duration_seconds",
		map[string]string{"controller": "warmup"}); got != 1 {
		t.Errorf("warmup duration count = %d, want 1", got)
	}
	if got := emitGauge(t, m, "traincache_controller_gate_tasks",
		map[string]string{"state": "hold"}); got != 0 {
		t.Errorf("gate_tasks{hold} = %v, want 0 after NotFound", got)
	}
}

// TestTerminalTaskForgetsGateGauge proves a terminal task is removed exactly
// once, so the gauge cannot drift.
func TestTerminalTaskForgetsGateGauge(t *testing.T) {
	m := metrics.New()
	m.ObserveGateTask("train/t1", metrics.GateHold, time.Unix(100, 0))

	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Status.Phase = traincachev1alpha1.WarmupPhaseReleased
	task.Status.ObservedGeneration = task.Generation
	c := fixtureClient(t, task)
	r := &Reconciler{Client: c, Metrics: m}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := emitGauge(t, m, "traincache_controller_gate_tasks",
		map[string]string{"state": "hold"}); got != 0 {
		t.Errorf("gate_tasks{hold} = %v, want 0 after terminal", got)
	}
}

// TestSubmitCandidatesEmitsPreheatRPCMetrics covers the successful and
// permanently-rejected Submit paths.
func TestSubmitCandidatesEmitsPreheatRPCMetrics(t *testing.T) {
	m := metrics.New()
	pool := &fakeAgentPool{fail: map[string]error{"n1": agentclient.ErrPermanent}}
	task := fixtureTask(2, 2, [][]int32{{0, 1}, {2, 3}})
	ds := fixtureDataSet(4, singleShardLayout(4), []string{"n1", "n2"})
	windows := [][]*agentpb.ChunkRef{{}, {}}
	podNames := map[int32]string{0: "trainer-0", 1: "trainer-1"}

	// A permanent rejection is recorded per node and does not surface as an
	// error; only ErrAgentUnavailable sets Unavailable and returns an error.
	if _, err := submitToCandidates(context.Background(), pool, m, nil, task, ds, windows,
		[]string{"n1", "n2"}, podNames); err != nil {
		t.Fatalf("submitToCandidates: %v", err)
	}

	cases := map[string]float64{
		"ok":          2, // both Pods on n2
		"permanent":   1, // n1 rejected immediately
		"unavailable": 0,
	}
	for result, want := range cases {
		if got := emitCounter(t, m, "traincache_controller_preheat_rpc_total", map[string]string{
			"op": "submit", "result": result,
		}); got != want {
			t.Errorf("preheat_rpc{submit,%s} = %v, want %v", result, got, want)
		}
	}
}

// TestApplyPlacementEmitsPreheatRPCMetrics covers Submit, Update and Cancel.
func TestApplyPlacementEmitsPreheatRPCMetrics(t *testing.T) {
	m := metrics.New()
	in := testPlacementInput()
	in.Metrics = m

	if _, err := ApplyPlacement(context.Background(), &fakePlacementRPC{}, nil, in); err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}

	cases := []struct {
		op   string
		want float64
	}{
		{"submit", 2},
		{"update", 1},
		{"cancel", 2},
	}
	for _, tc := range cases {
		if got := emitCounter(t, m, "traincache_controller_preheat_rpc_total", map[string]string{
			"op": tc.op, "result": "ok",
		}); got != tc.want {
			t.Errorf("preheat_rpc{%s,ok} = %v, want %v", tc.op, got, tc.want)
		}
	}
}

// TestComputePlacementEmitsGWAAMetrics proves every GWAA computation feeds
// pods_per_node, gwaa_unassigned_pods and gwaa_fallback_total.
func TestComputePlacementEmitsGWAAMetrics(t *testing.T) {
	m := metrics.New()
	r := &Reconciler{Metrics: m, Now: func() time.Time { return time.Unix(100, 0) }}
	task := fixtureTask(2, 2, [][]int32{{0, 1}, {2, 3}})

	placement := r.computePlacement(task, []string{"n1", "n2"}, nil, nil)
	if placement.Err != nil {
		t.Fatalf("computePlacement: %v", placement.Err)
	}

	if got, want := emitHistogramCount(t, m, "traincache_controller_pods_per_node", nil), uint64(len(placement.PreheatSet)); got != want {
		t.Errorf("pods_per_node observations = %d, want %d", got, want)
	}
	if got, want := emitHistogramSum(t, m, "traincache_controller_pods_per_node", nil), float64(len(placement.Assignment)); got != want {
		t.Errorf("pods_per_node sum = %v, want %v assigned Pods", got, want)
	}
	if got, want := emitGauge(t, m, "traincache_controller_gwaa_unassigned_pods",
		map[string]string{"dataset": "train/ds1"}), float64(len(placement.UnassignedPods)); got != want {
		t.Errorf("gwaa_unassigned_pods = %v, want %v", got, want)
	}
	wantFallback := 0.0
	if placement.FellBack {
		wantFallback = 1
	}
	if got := emitCounter(t, m, "traincache_controller_gwaa_fallback_total", nil); got != wantFallback {
		t.Errorf("gwaa_fallback_total = %v, want %v", got, wantFallback)
	}
}

func emitCounter(t *testing.T, m *metrics.Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := emitMetric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetCounter().GetValue()
}

func emitGauge(t *testing.T, m *metrics.Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := emitMetric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetGauge().GetValue()
}

func emitHistogramCount(t *testing.T, m *metrics.Metrics, family string, labels map[string]string) uint64 {
	t.Helper()
	metric, ok := emitMetric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}

func emitHistogramSum(t *testing.T, m *metrics.Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := emitMetric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetHistogram().GetSampleSum()
}

func emitMetric(m *metrics.Metrics, family string, labels map[string]string) (*dto.Metric, bool) {
	families, err := m.Gatherer.Gather()
	if err != nil {
		return nil, false
	}
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			if emitLabelsMatch(metric, labels) {
				return metric, true
			}
		}
	}
	return nil, false
}

func emitLabelsMatch(metric *dto.Metric, want map[string]string) bool {
	if len(metric.GetLabel()) != len(want) {
		return false
	}
	for _, label := range metric.GetLabel() {
		value, ok := want[label.GetName()]
		if !ok || value != label.GetValue() {
			return false
		}
	}
	return true
}
