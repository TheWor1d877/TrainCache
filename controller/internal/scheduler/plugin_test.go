package scheduler

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedframework "k8s.io/kube-scheduler/framework"

	"traincache/controller/internal/scorefmt"
)

const testNamespace = "default"

var testNow = time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)

// testPod builds a training Pod with the webhook's warmup-task annotation and,
// when value is non-nil, the controller-written score annotation.
func testPod(name string, value *scorefmt.Value) *corev1.Pod {
	annotations := map[string]string{
		AnnotationWarmupTask: testNamespace + "/" + name,
	}
	if value != nil {
		raw, err := scorefmt.EncodeValue(*value)
		if err != nil {
			panic(err)
		}
		annotations[scorefmt.DefaultAnnotation] = raw
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   testNamespace,
			Name:        name,
			Annotations: annotations,
		},
	}
}

func testNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func freshValue(task string, generation int64, scores map[string]int32) scorefmt.Value {
	return scorefmt.Value{V: 1, Task: task, G: generation, T: testNow.Add(-time.Minute), S: scores}
}

func newTestPlugin(t *testing.T, opts Options) *Plugin {
	t.Helper()
	if opts.Now == nil {
		opts.Now = func() time.Time { return testNow }
	}
	p, err := NewWithOptions(opts)
	if err != nil {
		t.Fatalf("NewWithOptions() error: %v", err)
	}
	return p
}

func TestEvaluateReturnsScore(t *testing.T) {
	value := freshValue("default/train-1", 7, map[string]int32{"node-1": 87, "node-2": 12})
	pod := testPod("train-1", &value)
	node := testNode("node-1")

	before := pod.DeepCopy()
	score, key, stale, err := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)

	if err != nil || stale {
		t.Fatalf("evaluate() = (%d, %q, %v, %v), want a fresh score", score, key, stale, err)
	}
	if score != 87 {
		t.Fatalf("score = %d, want 87", score)
	}
	if key != scorefmt.DefaultAnnotation {
		t.Fatalf("key = %q, want %q", key, scorefmt.DefaultAnnotation)
	}
	if !reflect.DeepEqual(before, pod) {
		t.Fatal("evaluate() mutated the Pod")
	}
}

func TestEvaluateZeroScore(t *testing.T) {
	value := freshValue("default/train-1", 3, map[string]int32{"node-1": 0})
	pod := testPod("train-1", &value)

	score, _, stale, err := newTestPlugin(t, Options{}).evaluate(pod, testNode("node-1"), testNow)
	if err != nil || stale || score != 0 {
		t.Fatalf("evaluate() = (%d, %v, %v), want fresh 0", score, stale, err)
	}
}

func TestEvaluateMissingInputs(t *testing.T) {
	value := freshValue("default/train-1", 2, map[string]int32{"node-2": 50})
	noTaskRef := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "train-1"}}
	tests := []struct {
		name string
		pod  *corev1.Pod
		node *corev1.Node
	}{
		{"nil pod", nil, testNode("node-1")},
		{"nil node", testPod("train-1", &value), nil},
		{"no warmup-task annotation", noTaskRef, testNode("node-1")},
		{"missing score annotation", testPod("train-1", nil), testNode("node-1")},
		{"missing node entry", testPod("train-1", &value), testNode("node-1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score, _, stale, err := newTestPlugin(t, Options{}).evaluate(tt.pod, tt.node, testNow)
			if score != 0 || !stale || err != nil {
				t.Fatalf("evaluate() = (%d, stale=%v, err=%v), want (0, true, nil)", score, stale, err)
			}
		})
	}
}

func TestEvaluateTaskFieldMismatch(t *testing.T) {
	value := freshValue("default/other", 2, map[string]int32{"node-1": 90})
	pod := testPod("train-1", &value)

	score, _, stale, evalErr := newTestPlugin(t, Options{}).evaluate(pod, testNode("node-1"), testNow)
	if score != 0 || !stale || evalErr == nil {
		t.Fatalf("evaluate() = (%d, stale=%v, err=%v), want task mismatch -> 0", score, stale, evalErr)
	}
}

func TestEvaluateExpiredTTL(t *testing.T) {
	value := freshValue("default/train-1", 2, map[string]int32{"node-1": 90})
	value.T = testNow.Add(-10 * time.Minute)
	pod := testPod("train-1", &value)

	p := newTestPlugin(t, Options{TTL: 5 * time.Minute})
	score, _, stale, err := p.evaluate(pod, testNode("node-1"), testNow)
	if score != 0 || !stale || err != nil {
		t.Fatalf("evaluate() = (%d, stale=%v, err=%v), want expired -> 0", score, stale, err)
	}
}

func TestEvaluateStaleGeneration(t *testing.T) {
	for _, generation := range []int64{0, -3} {
		value := freshValue("default/train-1", generation, map[string]int32{"node-1": 90})
		pod := testPod("train-1", &value)

		score, _, stale, err := newTestPlugin(t, Options{}).evaluate(pod, testNode("node-1"), testNow)
		if score != 0 || !stale || err == nil {
			t.Fatalf("generation %d: evaluate() = (%d, stale=%v, err=%v), want stale -> 0", generation, score, stale, err)
		}
	}
}

func TestEvaluateInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		ref  string
		bad  bool
	}{
		{"bad json", `{"v":1,"task":`, "default/train-1", true},
		{"unsupported version", `{"v":2,"task":"default/train-1","g":1,"t":"2026-10-20T07:59:00Z","s":{"node-1":50}}`, "default/train-1", true},
		{"score out of range", `{"v":1,"task":"default/train-1","g":1,"t":"2026-10-20T07:59:00Z","s":{"node-1":101}}`, "default/train-1", true},
		{"bad task ref", `{"v":1,"task":"default/train-1","g":1,"t":"2026-10-20T07:59:00Z","s":{"node-1":50}}`, "bad/task/ref", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := testPod("train-1", nil)
			pod.Annotations[AnnotationWarmupTask] = tt.ref
			pod.Annotations[scorefmt.DefaultAnnotation] = tt.raw

			score, _, stale, err := newTestPlugin(t, Options{}).evaluate(pod, testNode("node-1"), testNow)
			if score != 0 || !stale {
				t.Fatalf("evaluate() = (%d, stale=%v, err=%v), want 0/stale", score, stale, err)
			}
			if tt.bad && err == nil {
				t.Fatal("evaluate() = nil error, want a malformed-value error")
			}
		})
	}
}

func TestNormalizeScore(t *testing.T) {
	tests := []struct {
		name string
		in   []int64
		want []int64
	}{
		{"mixed", []int64{10, 50, 100}, []int64{10, 50, 100}},
		{"all zero", []int64{0, 0, 0}, []int64{0, 0, 0}},
		{"empty", nil, []int64{}},
		{"all tie", []int64{40, 40, 40}, []int64{100, 100, 100}},
		{"rounding", []int64{1, 2, 3}, []int64{33, 67, 100}},
		{"negative clamped", []int64{-5, 10}, []int64{0, 100}},
	}
	p := newTestPlugin(t, Options{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scores := make(schedframework.NodeScoreList, len(tt.in))
			for i, v := range tt.in {
				scores[i] = schedframework.NodeScore{Name: "node-" + strconv.Itoa(i), Score: v}
			}
			if status := p.NormalizeScore(context.Background(), nil, &corev1.Pod{}, scores); status != nil && !status.IsSuccess() {
				t.Fatalf("NormalizeScore() status = %v", status)
			}
			got := make([]int64, len(scores))
			for i := range scores {
				got[i] = scores[i].Score
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("NormalizeScore(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestScoreNilInputs(t *testing.T) {
	p := newTestPlugin(t, Options{})
	score, status := p.Score(context.Background(), nil, nil, nil)
	if score != 0 || status != nil {
		t.Fatalf("Score(nil) = (%d, %v), want (0, nil)", score, status)
	}
}

func TestFactoryUsesOptions(t *testing.T) {
	custom := "example.io/custom-scores"
	plugin, err := Factory(Options{Annotation: custom, TTL: time.Minute, Now: func() time.Time { return testNow }})(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Factory() error: %v", err)
	}
	p, ok := plugin.(*Plugin)
	if !ok {
		t.Fatalf("Factory() returned %T, want *Plugin", plugin)
	}
	if p.Name() != Name || p.ScoreExtensions() != p {
		t.Fatal("plugin does not implement the score contract")
	}

	value := freshValue("default/train-1", 2, map[string]int32{"node-1": 50})
	pod := testPod("train-1", nil)
	raw, err := scorefmt.EncodeValue(value)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[custom] = raw

	score, key, stale, err := p.evaluate(pod, testNode("node-1"), testNow)
	if err != nil || stale || score != 50 {
		t.Fatalf("evaluate() = (%d, %q, stale=%v, err=%v), want custom annotation score 50", score, key, stale, err)
	}
	if key != custom {
		t.Fatalf("key = %q, want %q", key, custom)
	}
}

func TestNewWithOptionsRejectsNegativeTTL(t *testing.T) {
	if _, err := NewWithOptions(Options{TTL: -time.Second}); err == nil {
		t.Fatal("NewWithOptions() accepted a negative TTL")
	}
}

func TestScoreLogsWarnOnParseFailure(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := newTestPlugin(t, Options{Logger: log})
	pod := testPod("train-1", nil)
	pod.Annotations[scorefmt.DefaultAnnotation] = `{broken`

	if score := p.scoreNode(pod, testNode("node-1"), testNow); score != 0 {
		t.Fatalf("scoreNode() = %d, want 0", score)
	}
	if got := buf.String(); !strings.Contains(got, `"level":"WARN"`) {
		t.Fatalf("no WARN log emitted: %s", got)
	}
}
