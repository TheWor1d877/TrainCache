package metrics

import (
	"errors"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestNewReturnsIndependentRegistry(t *testing.T) {
	m := New()
	if m == nil || m.Registry == nil {
		t.Fatal("New() returned a nil registry")
	}
	if m.Registerer == nil || m.Gatherer == nil {
		t.Fatalf("registry views are nil: registerer=%v gatherer=%v", m.Registerer, m.Gatherer)
	}

	// The controller registry must already own its families; an unobserved
	// unlabeled histogram/counter still gathers with a zero sample.
	families := m12Gather(t, m)
	for _, want := range []string{
		"traincache_controller_gate_hold_duration_seconds",
		"traincache_controller_pods_per_node",
		"traincache_controller_gwaa_fallback_total",
	} {
		if _, ok := m12FindFamily(families, want); !ok {
			t.Errorf("family %q is not registered", want)
		}
	}
	if _, ok := m12FindFamily(families, "traincache_scheduler_score_total"); ok {
		t.Error("controller registry unexpectedly contains the scheduler family")
	}
}

func TestNewCreatesIndependentRegistries(t *testing.T) {
	first := New()
	second := New()
	if first == second {
		t.Fatal("New() returned the same registry twice")
	}
	if first.Registerer == second.Registerer {
		t.Fatal("separate New() calls share a Registerer")
	}
	if first.Gatherer == second.Gatherer {
		t.Fatal("separate New() calls share a Gatherer")
	}

	first.ObserveGateTask("train/t1", GateHold, time.Unix(100, 0))
	if _, ok := m12FindFamily(m12Gather(t, first), "traincache_controller_gate_tasks"); !ok {
		t.Fatal("first registry did not record its own gate task")
	}
	if _, ok := m12FindFamily(m12Gather(t, second), "traincache_controller_gate_tasks"); ok {
		t.Fatal("second registry observed the first registry's state")
	}
}

func TestObserveReconcileResultPriority(t *testing.T) {
	m := New()
	second := time.Second
	m.ObserveReconcile(ControllerDataset, errors.New("boom"), 5*time.Second, second)
	m.ObserveReconcile(ControllerWarmup, nil, 5*time.Second, second)
	m.ObserveReconcile(ControllerWarmupDegraded, nil, 0, second)

	cases := []struct {
		controller Controller
		result     ReconcileResult
	}{
		{ControllerDataset, ReconcileError},
		{ControllerWarmup, ReconcileRequeue},
		{ControllerWarmupDegraded, ReconcileSuccess},
	}
	for _, tc := range cases {
		if got := m12CounterValue(t, m, "traincache_controller_reconcile_total",
			map[string]string{"controller": string(tc.controller), "result": string(tc.result)}); got != 1 {
			t.Errorf("reconcile_total{%s,%s} = %v, want 1", tc.controller, tc.result, got)
		}
	}
	if _, ok := m12Metric(m, "traincache_controller_reconcile_total",
		map[string]string{"controller": "dataset", "result": "success"}); ok {
		t.Error("dataset error was also counted as success")
	}
	// Every controller observed one duration sample.
	for _, controller := range []Controller{ControllerDataset, ControllerWarmup, ControllerWarmupDegraded} {
		if got := m12HistogramCount(t, m, "traincache_controller_reconcile_duration_seconds",
			map[string]string{"controller": string(controller)}); got != 1 {
			t.Errorf("reconcile duration count{%s} = %d, want 1", controller, got)
		}
	}
}

func TestGateTaskGaugeAndHoldDurationStayBalanced(t *testing.T) {
	m := New()
	now := time.Unix(1000, 0)

	m.ObserveGateTask("train/a", GateHold, now)
	m.ObserveGateTask("train/a", GateHold, now.Add(time.Second)) // repeated state is a no-op
	m.ObserveGateTask("train/b", GateReleased, now)              // first evaluation outside Hold
	assertGauge(t, m, GateHold, 1)
	assertGauge(t, m, GateReleased, 1)

	m.ObserveGateTask("train/a", GateReleased, now.Add(10*time.Second))
	assertGauge(t, m, GateHold, 0)
	assertGauge(t, m, GateReleased, 2)
	if got := m12HistogramCount(t, m, "traincache_controller_gate_hold_duration_seconds", nil); got != 1 {
		t.Fatalf("hold duration count = %d, want 1", got)
	}
	if got := m12HistogramSum(t, m, "traincache_controller_gate_hold_duration_seconds", nil); got != 10 {
		t.Fatalf("hold duration sum = %v, want 10", got)
	}

	m.ForgetGateTask("train/a")
	assertGauge(t, m, GateReleased, 1)
	m.ForgetGateTask("train/b")
	assertGauge(t, m, GateReleased, 0)
	m.ForgetGateTask("train/missing") // idempotent

	m.ObserveGateTask("train/c", GateHold, now)
	m.ObserveGateTask("train/c", GateDegraded, now.Add(5*time.Second))
	assertGauge(t, m, GateHold, 0)
	assertGauge(t, m, GateDegraded, 1)
	if got := m12HistogramCount(t, m, "traincache_controller_gate_hold_duration_seconds", nil); got != 2 {
		t.Fatalf("hold duration count = %d, want 2", got)
	}
	if got := m12HistogramSum(t, m, "traincache_controller_gate_hold_duration_seconds", nil); got != 15 {
		t.Fatalf("hold duration sum = %v, want 15", got)
	}
}

func TestObservePlacementMetrics(t *testing.T) {
	m := New()
	m.ObservePlacement("train/ds1", map[string][]int32{"n1": {0}, "n2": {1, 2}}, 3, true)

	if got := m12HistogramCount(t, m, "traincache_controller_pods_per_node", nil); got != 2 {
		t.Errorf("pods_per_node count = %d, want 2 observations", got)
	}
	if got := m12HistogramSum(t, m, "traincache_controller_pods_per_node", nil); got != 3 {
		t.Errorf("pods_per_node sum = %v, want 3 assigned pods", got)
	}
	if got := m12GaugeValue(t, m, "traincache_controller_gwaa_unassigned_pods",
		map[string]string{"dataset": "train/ds1"}); got != 3 {
		t.Errorf("gwaa_unassigned_pods = %v, want 3", got)
	}
	if got := m12CounterValue(t, m, "traincache_controller_gwaa_fallback_total", nil); got != 1 {
		t.Errorf("gwaa_fallback_total = %v, want 1", got)
	}
}

func assertGauge(t *testing.T, m *Metrics, state GateState, want float64) {
	t.Helper()
	if got := m12GaugeValue(t, m, "traincache_controller_gate_tasks", map[string]string{"state": string(state)}); got != want {
		t.Fatalf("gate_tasks{state=%s} = %v, want %v", state, got, want)
	}
}

func m12CounterValue(t *testing.T, m *Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := m12Metric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetCounter().GetValue()
}

func m12GaugeValue(t *testing.T, m *Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := m12Metric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetGauge().GetValue()
}

func m12HistogramCount(t *testing.T, m *Metrics, family string, labels map[string]string) uint64 {
	t.Helper()
	metric, ok := m12Metric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}

func m12HistogramSum(t *testing.T, m *Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := m12Metric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetHistogram().GetSampleSum()
}

func m12Metric(m *Metrics, family string, labels map[string]string) (*dto.Metric, bool) {
	families := m12GatherNoFail(m)
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			if m12LabelsMatch(metric, labels) {
				return metric, true
			}
		}
	}
	return nil, false
}

func m12GatherNoFail(m *Metrics) []*dto.MetricFamily {
	families, err := m.Gatherer.Gather()
	if err != nil {
		return nil
	}
	return families
}

func m12LabelsMatch(metric *dto.Metric, want map[string]string) bool {
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
