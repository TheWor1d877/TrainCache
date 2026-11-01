package metrics

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// m12ControllerInventory is the reviewable family/label inventory for the
// controller-owned registry (docs/7.1 design.md §9.5). Any family or label
// change must update this map and m12AllowedLabelValues in the same change.
var m12ControllerInventory = map[string][]string{
	"traincache_controller_reconcile_total":            {"controller", "result"},
	"traincache_controller_reconcile_duration_seconds": {"controller"},
	"traincache_controller_candidate_nodes":            {"dataset"},
	"traincache_controller_preheat_rpc_total":          {"op", "result"},
	"traincache_controller_gate_tasks":                 {"state"},
	"traincache_controller_gate_hold_duration_seconds": nil,
	"traincache_controller_agent_poll_total":           {"result"},
	"traincache_controller_pods_per_node":              nil,
	"traincache_controller_gwaa_unassigned_pods":       {"dataset"},
	"traincache_controller_gwaa_fallback_total":        nil,
}

// m12SchedulerInventory is the scheduler process's independent registry.
var m12SchedulerInventory = map[string][]string{
	"traincache_scheduler_score_total": {"result"},
}

// m12AllowedLabelValues freezes every enum label. A nil set means the label is
// a bounded object key (dataset) checked separately for "<namespace>/<name>".
var m12AllowedLabelValues = map[string]map[string]map[string]struct{}{
	"traincache_controller_reconcile_total": {
		"controller": m12Set("dataset", "warmup", "warmup-degraded"),
		"result":     m12Set("success", "error", "requeue"),
	},
	"traincache_controller_reconcile_duration_seconds": {
		"controller": m12Set("dataset", "warmup", "warmup-degraded"),
	},
	"traincache_controller_candidate_nodes": {"dataset": nil},
	"traincache_controller_preheat_rpc_total": {
		"op":     m12Set("submit", "update", "cancel"),
		"result": m12Set("ok", "permanent", "unavailable"),
	},
	"traincache_controller_gate_tasks": {
		"state": m12Set("hold", "released", "degraded"),
	},
	"traincache_controller_agent_poll_total": {
		"result": m12Set("ok", "unavailable", "stale", "error"),
	},
	"traincache_controller_gwaa_unassigned_pods": {"dataset": nil},
	"traincache_scheduler_score_total": {
		"result": m12Set(
			"ok", "no_task_ref", "invalid_task_ref", "no_annotation",
			"invalid_value", "task_mismatch", "stale_generation", "expired",
			"no_node_entry",
		),
	},
}

// m12ForbiddenLabelParts are unbounded identities that must never become
// Prometheus label names. Matching is on lower-case substrings, so camelCase
// spellings are caught too.
var m12ForbiddenLabelParts = []string{
	"pod_name", "podname",
	"task_name", "taskname",
	"node_name", "nodename",
	"uid",
	"annotation",
	"timestamp",
	"created_at", "started_at", "last_report",
}

// m12ForbiddenLabelNames are bare labels whose values are identities rather
// than bounded enums. design §9.5 originally proposed several of these;
// implementing them verbatim would create one series per Pod/task/Node.
var m12ForbiddenLabelNames = map[string]struct{}{
	"pod":         {},
	"task":        {},
	"node":        {},
	"warmup_task": {},
}

// TestControllerMetricFamilyAndLabelInventory gathers the populated
// controller registry and checks the exact family set, every label name and
// every label value against the frozen enums.
func TestControllerMetricFamilyAndLabelInventory(t *testing.T) {
	m := New()
	m12PopulateController(t, m)
	families := m12Gather(t, m)

	if got, want := m12FamilyNames(families), m12SortedKeys(m12ControllerInventory); !m12EqualStrings(got, want) {
		t.Fatalf("families = %v, want %v", got, want)
	}
	m12CheckFamilies(t, families, m12ControllerInventory)
}

// TestSchedulerMetricFamilyAndLabelInventory does the same for the separate
// scheduler registry: it must expose exactly one family.
func TestSchedulerMetricFamilyAndLabelInventory(t *testing.T) {
	m := NewScheduler()
	m12PopulateScheduler(t, m)
	families := m12Gather(t, m)

	if got, want := m12FamilyNames(families), m12SortedKeys(m12SchedulerInventory); !m12EqualStrings(got, want) {
		t.Fatalf("families = %v, want %v", got, want)
	}
	m12CheckFamilies(t, families, m12SchedulerInventory)
}

// TestInvalidEnumValuesAreDropped proves the enum guard is not vacuous: an
// out-of-enum value never creates a series.
func TestInvalidEnumValuesAreDropped(t *testing.T) {
	m := New()
	m.ObserveReconcile(Controller("bogus"), nil, 0, time.Second)
	m.ObserveCandidateNodes("", 3)
	m.ObservePreheatRPC(PreheatOp("bogus"), RPCOK)
	m.ObservePreheatRPC(PreheatSubmit, RPCResult("bogus"))
	m.ObserveGateTask("train/t1", GateState("bogus"), time.Now())
	m.ObserveAgentPoll(PollResult("bogus"))

	for _, name := range []string{
		"traincache_controller_reconcile_total",
		"traincache_controller_reconcile_duration_seconds",
		"traincache_controller_candidate_nodes",
		"traincache_controller_preheat_rpc_total",
		"traincache_controller_gate_tasks",
		"traincache_controller_agent_poll_total",
	} {
		if _, ok := m12FindFamily(m12Gather(t, m), name); ok {
			t.Errorf("invalid enum value created family %q", name)
		}
	}

	s := NewScheduler()
	s.ObserveSchedulerScore(ScoreResult("bogus"))
	if _, ok := m12FindFamily(m12Gather(t, s), "traincache_scheduler_score_total"); ok {
		t.Error("invalid scheduler result created a series")
	}
}

func m12PopulateController(t *testing.T, m *Metrics) {
	t.Helper()
	now := time.Unix(1000, 0)
	for _, controller := range []Controller{ControllerDataset, ControllerWarmup, ControllerWarmupDegraded} {
		m.ObserveReconcile(controller, nil, 0, time.Second)                // success
		m.ObserveReconcile(controller, fmt.Errorf("boom"), 0, time.Second) // error wins
		m.ObserveReconcile(controller, nil, time.Second, time.Second)      // requeue
	}
	m.ObserveCandidateNodes("train/ds1", 4)
	for _, op := range []PreheatOp{PreheatSubmit, PreheatUpdate, PreheatCancel} {
		for _, result := range []RPCResult{RPCOK, RPCPermanent, RPCUnavailable} {
			m.ObservePreheatRPC(op, result)
		}
	}
	m.ObserveGateTask("train/hold", GateHold, now)
	m.ObserveGateTask("train/released", GateHold, now)
	m.ObserveGateTask("train/released", GateReleased, now.Add(2*time.Second))
	m.ObserveGateTask("train/degraded", GateHold, now)
	m.ObserveGateTask("train/degraded", GateDegraded, now.Add(3*time.Second))
	for _, result := range []PollResult{PollOK, PollUnavailable, PollStale, PollError} {
		m.ObserveAgentPoll(result)
	}
	m.ObservePlacement("train/ds1", map[string][]int32{"n1": {0}, "n2": {1, 2}}, 1, true)
}

func m12PopulateScheduler(t *testing.T, m *Metrics) {
	t.Helper()
	for _, result := range []ScoreResult{
		ScoreOK, ScoreNoTaskRef, ScoreInvalidTaskRef, ScoreNoAnnotation,
		ScoreInvalidValue, ScoreTaskMismatch, ScoreStaleGeneration,
		ScoreExpired, ScoreNoNodeEntry,
	} {
		m.ObserveSchedulerScore(result)
	}
}

func m12Gather(t *testing.T, m *Metrics) []*dto.MetricFamily {
	t.Helper()
	families, err := m.Gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}
	return families
}

func m12CheckFamilies(t *testing.T, families []*dto.MetricFamily, inventory map[string][]string) {
	t.Helper()
	for _, family := range families {
		name := family.GetName()
		want, ok := inventory[name]
		if !ok {
			t.Errorf("family %q is not in the inventory", name)
			continue
		}
		got := m12LabelNames(family)
		if !m12EqualStrings(got, m12SortedCopy(want)) {
			t.Errorf("family %q labels = %v, want %v", name, got, want)
		}
		if err := m12CheckLabelNames(got); err != nil {
			t.Errorf("family %q: %v", name, err)
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				m12CheckLabelValue(t, name, label.GetName(), label.GetValue())
			}
		}
	}
}

func m12CheckLabelValue(t *testing.T, family, label, value string) {
	t.Helper()
	allowedByLabel, ok := m12AllowedLabelValues[family]
	if !ok {
		return
	}
	allowed, ok := allowedByLabel[label]
	if !ok {
		return
	}
	if allowed == nil {
		// Bounded object key: "<namespace>/<name>".
		if value == "" || !strings.Contains(value, "/") {
			t.Errorf("%s{%s}=%q is not a namespace/name key", family, label, value)
		}
		return
	}
	if _, ok := allowed[value]; !ok {
		t.Errorf("%s{%s}=%q is outside the frozen enum %v", family, label, value, m12SortedKeys(allowed))
	}
}

// TestLabelCheckerRejectsUnboundedLabels proves the guard is not vacuous.
func TestLabelCheckerRejectsUnboundedLabels(t *testing.T) {
	for _, allowed := range [][]string{
		{"controller", "result"},
		{"op", "dataset"},
		{"state"},
	} {
		if err := m12CheckLabelNames(allowed); err != nil {
			t.Errorf("m12CheckLabelNames(%v) = %v, want nil", allowed, err)
		}
	}
	for _, forbidden := range []string{
		"pod_name", "podName", "task_name", "nodeName", "uid",
		"annotation_key", "timestamp", "started_at",
		"pod", "task", "node", "warmup_task",
	} {
		if err := m12CheckLabelNames([]string{forbidden}); err == nil {
			t.Errorf("m12CheckLabelNames(%q) = nil, want high-cardinality error", forbidden)
		}
	}
}

func m12FamilyNames(families []*dto.MetricFamily) []string {
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	sort.Strings(names)
	return names
}

func m12FindFamily(families []*dto.MetricFamily, name string) (*dto.MetricFamily, bool) {
	for _, family := range families {
		if family.GetName() == name {
			return family, true
		}
	}
	return nil, false
}

// m12LabelNames returns the distinct label names used by a family.
func m12LabelNames(family *dto.MetricFamily) []string {
	seen := map[string]struct{}{}
	for _, metric := range family.GetMetric() {
		for _, label := range metric.GetLabel() {
			seen[label.GetName()] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func m12CheckLabelNames(names []string) error {
	for _, name := range names {
		lower := strings.ToLower(name)
		if _, forbidden := m12ForbiddenLabelNames[lower]; forbidden {
			return fmt.Errorf("label %q is an unbounded identity", name)
		}
		for _, forbidden := range m12ForbiddenLabelParts {
			if strings.Contains(lower, forbidden) {
				return fmt.Errorf("label %q contains unbounded identity %q", name, forbidden)
			}
		}
	}
	return nil
}

func m12Set(values ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func m12SortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func m12SortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func m12EqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
