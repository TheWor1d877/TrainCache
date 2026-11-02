package scheduler

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"traincache/controller/internal/metrics"
	"traincache/controller/internal/scorefmt"
)

// TestScoreNodeEmitsSchedulerScoreResults covers a representative branch per
// result enum. The full enum set is frozen by the metrics inventory test.
func TestScoreNodeEmitsSchedulerScoreResults(t *testing.T) {
	registry := metrics.NewScheduler()
	p := newTestPlugin(t, Options{Metrics: registry})

	ok := freshValue("default/train-1", 7, map[string]int32{"node-1": 87})
	p.scoreNode(testPod("train-1", &ok), testNode("node-1"), testNow) // ok

	expiredValue := freshValue("default/train-1", 7, map[string]int32{"node-1": 87})
	expiredValue.T = testNow.Add(-10 * time.Minute)
	p.scoreNode(testPod("train-1", &expiredValue), testNode("node-1"), testNow) // expired

	p.scoreNode(testPod("train-1", nil), testNode("node-1"), testNow) // no_annotation

	missingEntry := freshValue("default/train-1", 7, map[string]int32{"node-2": 87})
	p.scoreNode(testPod("train-1", &missingEntry), testNode("node-1"), testNow) // no_node_entry

	invalidTaskRef := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: testNamespace,
		Name:      "train-1",
		Annotations: map[string]string{
			AnnotationWarmupTask: "bogus",
		},
	}}
	p.scoreNode(invalidTaskRef, testNode("node-1"), testNow) // invalid_task_ref

	for _, result := range []string{"ok", "expired", "no_annotation", "no_node_entry", "invalid_task_ref"} {
		if got := schedulerCounter(t, registry, result); got != 1 {
			t.Errorf("scheduler_score_total{result=%q} = %v, want 1", result, got)
		}
	}
}

func mustEncode(t *testing.T, value scorefmt.Value) string {
	t.Helper()
	raw, err := scorefmt.EncodeValue(value)
	if err != nil {
		t.Fatalf("EncodeValue: %v", err)
	}
	return raw
}

func schedulerCounter(t *testing.T, m *metrics.Metrics, result string) float64 {
	t.Helper()
	families, err := m.Gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "traincache_scheduler_score_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "result" && label.GetValue() == result {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
