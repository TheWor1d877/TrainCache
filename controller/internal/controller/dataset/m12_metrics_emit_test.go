package dataset

import (
	"context"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"traincache/controller/internal/metrics"
)

// TestReconcileNotFoundEmitsReconcileMetrics proves the entry/exit defer runs
// before the first Get, so NotFound is observed too.
func TestReconcileNotFoundEmitsReconcileMetrics(t *testing.T) {
	m := metrics.New()
	r := &Reconciler{
		Client:  fake.NewClientBuilder().WithScheme(testScheme()).Build(),
		Metrics: m,
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "train", Name: "missing"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := datasetCounter(t, m, "traincache_controller_reconcile_total", map[string]string{
		"controller": "dataset", "result": "success",
	}); got != 1 {
		t.Errorf("dataset success count = %v, want 1", got)
	}
	if got := datasetHistogramCount(t, m, "traincache_controller_reconcile_duration_seconds",
		map[string]string{"controller": "dataset"}); got != 1 {
		t.Errorf("dataset duration count = %d, want 1", got)
	}
}

func datasetCounter(t *testing.T, m *metrics.Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	metric, ok := datasetMetric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetCounter().GetValue()
}

func datasetHistogramCount(t *testing.T, m *metrics.Metrics, family string, labels map[string]string) uint64 {
	t.Helper()
	metric, ok := datasetMetric(m, family, labels)
	if !ok {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}

func datasetMetric(m *metrics.Metrics, family string, labels map[string]string) (*dto.Metric, bool) {
	families, err := m.Gatherer.Gather()
	if err != nil {
		return nil, false
	}
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			if datasetLabelsMatch(metric, labels) {
				return metric, true
			}
		}
	}
	return nil, false
}

func datasetLabelsMatch(metric *dto.Metric, want map[string]string) bool {
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
