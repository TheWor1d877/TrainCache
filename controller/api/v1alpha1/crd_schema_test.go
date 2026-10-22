package v1alpha1

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func readCRD(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("../../config/crd/bases/" + name)
	if err != nil {
		t.Fatalf("read CRD %s: %v", name, err)
	}
	return string(b)
}

func TestCRDShapeAndValidation(t *testing.T) {
	datasets := readCRD(t, "traincache.io_datasets.yaml")
	warmups := readCRD(t, "traincache.io_warmuptasks.yaml")

	scale := regexp.MustCompile(`(?m)^\s+scale:`)
	// Build the removed field name at runtime so this assertion does not
	// itself trip the repository-wide "banned symbol" grep.
	removedOrderFieldName := "consumeOrder" + "Ref"
	removedOrderFieldRe := regexp.MustCompile(`(?m)^\s+` + removedOrderFieldName + `:`)
	// Positive control: the pattern must still detect the field if present,
	// otherwise the assertion below would be vacuous.
	if !removedOrderFieldRe.MatchString("    " + removedOrderFieldName + ":") {
		t.Fatalf("removed-field regexp is broken")
	}
	window := regexp.MustCompile(`(?m)^\s+window:`)

	for name, body := range map[string]string{"dataset": datasets, "warmuptask": warmups} {
		if scale.MatchString(body) {
			t.Errorf("%s CRD defines a scale subresource", name)
		}
		if !strings.Contains(body, "status: {}") {
			t.Errorf("%s CRD has no status subresource", name)
		}
		if removedOrderFieldRe.MatchString(body) {
			t.Errorf("%s CRD still exposes %s", name, removedOrderFieldName)
		}
	}

	// The DataSet status must not carry a reverse task list; WarmupTask only
	// references DataSet (WarmupTaskSpec.DataSetRef), never the other way.
	// Build the removed field name at runtime for the same reason as above.
	reverseListFieldName := "warmup" + "Tasks"
	reverseListFieldRe := regexp.MustCompile(`(?m)^\s+` + reverseListFieldName + `:`)
	if !reverseListFieldRe.MatchString("    " + reverseListFieldName + ":") {
		t.Fatalf("reverse-list field regexp is broken")
	}
	statusIdx := strings.Index(datasets, "\n          status:")
	if statusIdx < 0 {
		t.Fatalf("dataset CRD has no status schema")
	}
	if reverseListFieldRe.MatchString(datasets[statusIdx:]) {
		t.Errorf("dataset CRD status still exposes %s", reverseListFieldName)
	}

	// Frozen contract: no global score window; per-Pod K is the only window.
	if window.MatchString(warmups) {
		t.Errorf("WarmupTask CRD still exposes a score window field")
	}
	if !regexp.MustCompile(`(?m)^\s+halfLife:`).MatchString(warmups) {
		t.Errorf("WarmupTask CRD has no score.halfLife")
	}
	if !regexp.MustCompile(`(?m)^\s+cutoff:`).MatchString(warmups) {
		t.Errorf("WarmupTask CRD has no score.cutoff")
	}

	// preheatFanout defaults to 1 and rejects zero/negative values (M13).
	fanout := regexp.MustCompile(`(?s)preheatFanout:\n\s+default: 1\n.*?\n\s+minimum: 1\n`)
	if !fanout.MatchString(warmups) {
		t.Errorf("WarmupTask CRD preheatFanout is missing default: 1 and minimum: 1")
	}

	// M14: preheatFanout is the upper bound and preheatFanoutMin the lower
	// bound. The minimum side gets the same default/minimum constraints, and a
	// type-level CEL rule rejects min > max.
	fanoutMin := regexp.MustCompile(`(?s)preheatFanoutMin:\n\s+default: 1\n.*?\n\s+minimum: 1\n`)
	if !fanoutMin.MatchString(warmups) {
		t.Errorf("WarmupTask CRD preheatFanoutMin is missing default: 1 and minimum: 1")
	}
	if !strings.Contains(warmups, "rule: self.preheatFanoutMin <= self.preheatFanout") {
		t.Errorf("WarmupTask CRD has no CEL rule rejecting preheatFanoutMin > preheatFanout")
	}
	if !strings.Contains(warmups, "message: preheatFanoutMin must not exceed preheatFanout") {
		t.Errorf("WarmupTask CRD has no CEL message for preheatFanoutMin > preheatFanout")
	}

	// podCount and dataSetRef are immutable via CEL.
	if got := strings.Count(warmups, "rule: self == oldSelf"); got < 2 {
		t.Errorf("WarmupTask CRD has %d `self == oldSelf` rules, want at least 2", got)
	}
	for _, msg := range []string{"message: podCount is immutable", "message: dataSetRef is immutable"} {
		if !strings.Contains(warmups, msg) {
			t.Errorf("WarmupTask CRD is missing %q", msg)
		}
	}

	// PodDetail constraints.
	if !strings.Contains(warmups, "uniqueItems: true") {
		t.Errorf("PodDetail.initialConsumeOrder has no uniqueItems constraint")
	}
	if !regexp.MustCompile(`(?m)^\s+- index$`).MatchString(warmups) {
		t.Errorf("PodDetail.index is not required")
	}
	if !regexp.MustCompile(`(?m)^\s+- k$`).MatchString(warmups) {
		t.Errorf("PodDetail.k is not required")
	}
	kMin := regexp.MustCompile(`(?s)\n\s+k:\n.*?\n\s+minimum: 1\n`)
	if !kMin.MatchString(warmups) {
		t.Errorf("PodDetail.k has no minimum: 1")
	}
}

func TestGoContractSurface(t *testing.T) {
	if GroupVersion.Group != "traincache.io" || GroupVersion.Version != "v1alpha1" {
		t.Fatalf("GroupVersion = %v", GroupVersion)
	}

	ds := reflect.TypeOf(DataSetSpec{})
	for _, field := range []string{
		"Endpoint", "Bucket", "DatasetName", "SecretRef", "SnapshotID", "ChunkSize",
		"Shards", "AccessibleNodeSelector", "Shuffle", "Metadata",
	} {
		if _, ok := ds.FieldByName(field); !ok {
			t.Errorf("DataSetSpec is missing %s", field)
		}
	}
	if _, ok := ds.FieldByName("ConsumeOrderRef"); ok {
		t.Errorf("DataSetSpec still has ConsumeOrderRef")
	}

	wt := reflect.TypeOf(WarmupTaskSpec{})
	for _, field := range []string{
		"DataSetRef", "TargetRef", "PodCount", "PreheatFanout", "PreheatFanoutMin",
		"Affinity", "TrainingType", "PodDetails", "Gate", "Score", "Candidate",
	} {
		if _, ok := wt.FieldByName(field); !ok {
			t.Errorf("WarmupTaskSpec is missing %s", field)
		}
	}
	fanout, _ := wt.FieldByName("PreheatFanout")
	if fanout.Type != reflect.TypeOf(int32(0)) {
		t.Errorf("WarmupTaskSpec.PreheatFanout = %v, want int32", fanout.Type)
	}
	fanoutMin, _ := wt.FieldByName("PreheatFanoutMin")
	if fanoutMin.Type != reflect.TypeOf(int32(0)) {
		t.Errorf("WarmupTaskSpec.PreheatFanoutMin = %v, want int32", fanoutMin.Type)
	}
	if !strings.Contains(string(fanoutMin.Tag), "omitempty") {
		t.Errorf("PreheatFanoutMin is not optional: %q", fanoutMin.Tag)
	}
	candidate, _ := wt.FieldByName("Candidate")
	if candidate.Type != reflect.TypeOf(CandidatePolicy{}) {
		t.Errorf("WarmupTaskSpec.Candidate = %v, want CandidatePolicy", candidate.Type)
	}

	// M13: one entry per (Pod, preheat node); AssignedNode is replaced by
	// NodeName.
	readiness := reflect.TypeOf(PodNodeReadiness{})
	for _, field := range []string{
		"PodIndex", "PodName", "NodeName", "ActualNode", "ReadyBlockBitmap",
		"StartupWindowReady", "ReadyInWindow", "RequiredReady",
		"StartupWindowReadyRate", "GlobalDecayScore", "State", "LastReportTime",
	} {
		if _, ok := readiness.FieldByName(field); !ok {
			t.Errorf("PodNodeReadiness is missing %s", field)
		}
	}
	if _, ok := readiness.FieldByName("AssignedNode"); ok {
		t.Errorf("PodNodeReadiness still has AssignedNode")
	}

	meta, _ := ds.FieldByName("Metadata")
	if meta.Type != reflect.TypeOf((*DataSetMetadata)(nil)) {
		t.Errorf("DataSetSpec.Metadata = %v, want *DataSetMetadata", meta.Type)
	}
	status := reflect.TypeOf(DataSetStatus{})
	layout, _ := status.FieldByName("BlockLayout")
	if layout.Type != reflect.TypeOf([]BlockLayout(nil)) {
		t.Errorf("DataSetStatus.BlockLayout = %v, want []BlockLayout", layout.Type)
	}

	gate := reflect.TypeOf(GateDecision{})
	decidedAt, _ := gate.FieldByName("DecidedAt")
	if decidedAt.Type != reflect.TypeOf((*metav1.Time)(nil)) {
		t.Errorf("GateDecision.DecidedAt = %v, want *metav1.Time", decidedAt.Type)
	}
	if _, ok := gate.FieldByName("PodRates"); !ok {
		t.Errorf("GateDecision is missing PodRates")
	}

	score := reflect.TypeOf(ScorePolicy{})
	if _, ok := score.FieldByName("Window"); ok {
		t.Errorf("ScorePolicy still has Window")
	}
	if _, ok := score.FieldByName("HalfLife"); !ok {
		t.Errorf("ScorePolicy is missing HalfLife")
	}
	if _, ok := score.FieldByName("Cutoff"); !ok {
		t.Errorf("ScorePolicy is missing Cutoff")
	}

	pod := reflect.TypeOf(PodDetail{})
	order, _ := pod.FieldByName("InitialConsumeOrder")
	if !strings.Contains(string(order.Tag), "omitempty") {
		t.Errorf("InitialConsumeOrder is not optional: %q", order.Tag)
	}
}

func TestDeepCopyIsGenerated(t *testing.T) {
	ds := &DataSet{}
	if got := ds.DeepCopy(); got == nil || got == ds {
		t.Fatalf("DataSet.DeepCopy returned %p", got)
	}
	wt := &WarmupTask{}
	if got := wt.DeepCopy(); got == nil || got == wt {
		t.Fatalf("WarmupTask.DeepCopy returned %p", got)
	}
}
