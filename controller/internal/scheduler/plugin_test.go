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

func testPod(name string, index int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      name,
			Annotations: map[string]string{
				AnnotationWarmupTask: testNamespace + "/" + name,
				AnnotationPodIndex:   strconv.FormatInt(int64(index), 10),
			},
		},
	}
}

func testNode(values ...scorefmt.Value) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-1",
			Annotations: map[string]string{},
		},
	}
	for _, v := range values {
		raw, err := scorefmt.EncodeValue(v)
		if err != nil {
			panic(err)
		}
		namespace, name, ok := splitTaskRef(v.Task)
		if !ok {
			panic("invalid task in test value: " + v.Task)
		}
		node.Annotations[scorefmt.Key(namespace, name)] = raw
	}
	return node
}

func freshValue(task string, generation int64, scores map[string]int32) scorefmt.Value {
	return scorefmt.Value{V: 1, Task: task, G: generation, T: testNow.Add(-time.Minute), P: scores}
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
	pod := testPod("train-1", 2)
	node := testNode(freshValue("default/train-1", 7, map[string]int32{"0": 12, "2": 87}))

	before := node.DeepCopy()
	score, key, stale, err := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)

	if err != nil || stale {
		t.Fatalf("evaluate() = (%d, %q, %v, %v), want a fresh score", score, key, stale, err)
	}
	if score != 87 {
		t.Fatalf("score = %d, want 87", score)
	}
	// Cross-package contract check: the annotation key must be exactly the one
	// the controller writer (scorefmt) produces.
	if want := scorefmt.KeyWithPrefix(scorefmt.DefaultPrefix, testNamespace, "train-1"); key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	if !reflect.DeepEqual(before, node) {
		t.Fatal("evaluate() mutated the Node")
	}
	if !reflect.DeepEqual(pod.Annotations, testPod("train-1", 2).Annotations) {
		t.Fatal("evaluate() mutated the Pod")
	}
}

func TestEvaluateZeroScore(t *testing.T) {
	pod := testPod("train-1", 0)
	node := testNode(freshValue("default/train-1", 3, map[string]int32{"0": 0}))

	score, _, stale, err := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)
	if err != nil || stale || score != 0 {
		t.Fatalf("evaluate() = (%d, %v, %v), want fresh 0", score, stale, err)
	}
}

func TestEvaluateMissingInputs(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		node *corev1.Node
	}{
		{"nil pod", nil, testNode()},
		{"nil node", testPod("train-1", 0), nil},
		{"no warmup-task annotation", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "train-1"}}, testNode()},
		{"no pod-index annotation", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationWarmupTask: "default/train-1"}}}, testNode()},
		{"missing node annotation", testPod("train-1", 0), testNode()},
		{"missing pod entry", testPod("train-1", 4), testNode(freshValue("default/train-1", 2, map[string]int32{"0": 50}))},
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

func TestEvaluateKeyMismatch(t *testing.T) {
	// The Node carries a valid annotation, but for another task.
	pod := testPod("train-1", 0)
	node := testNode(freshValue("default/train-2", 2, map[string]int32{"0": 90}))

	score, key, stale, err := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)
	if score != 0 || !stale || err != nil {
		t.Fatalf("evaluate() = (%d, %q, stale=%v, err=%v), want missing key -> 0", score, key, stale, err)
	}
	if key == "" || !strings.Contains(key, "train-1") {
		t.Fatalf("key = %q, want the pod task key", key)
	}
}

func TestEvaluateTaskFieldMismatch(t *testing.T) {
	pod := testPod("train-1", 0)
	// Same key slot but the payload claims another task.
	node := testNode(freshValue("default/train-2", 2, map[string]int32{"0": 90}))

	// Rewrite the payload under the pod's key to exercise the task check.
	raw, err := scorefmt.EncodeValue(freshValue("default/other", 2, map[string]int32{"0": 90}))
	if err != nil {
		t.Fatal(err)
	}
	node.Annotations[scorefmt.KeyWithPrefix(scorefmt.DefaultPrefix, testNamespace, "train-1")] = raw

	score, _, stale, evalErr := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)
	if score != 0 || !stale || evalErr == nil {
		t.Fatalf("evaluate() = (%d, stale=%v, err=%v), want task mismatch -> 0", score, stale, evalErr)
	}
}

func TestEvaluateExpiredTTL(t *testing.T) {
	pod := testPod("train-1", 0)
	value := freshValue("default/train-1", 2, map[string]int32{"0": 90})
	value.T = testNow.Add(-10 * time.Minute)
	node := testNode(value)

	p := newTestPlugin(t, Options{TTL: 5 * time.Minute})
	score, _, stale, err := p.evaluate(pod, node, testNow)
	if score != 0 || !stale || err != nil {
		t.Fatalf("evaluate() = (%d, stale=%v, err=%v), want expired -> 0", score, stale, err)
	}
}

func TestEvaluateStaleGeneration(t *testing.T) {
	for _, generation := range []int64{0, -3} {
		pod := testPod("train-1", 0)
		node := testNode(freshValue("default/train-1", generation, map[string]int32{"0": 90}))

		score, _, stale, err := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)
		if score != 0 || !stale || err == nil {
			t.Fatalf("generation %d: evaluate() = (%d, stale=%v, err=%v), want stale -> 0", generation, score, stale, err)
		}
	}
}

func TestEvaluateInvalidValues(t *testing.T) {
	taskKey := scorefmt.KeyWithPrefix(scorefmt.DefaultPrefix, testNamespace, "train-1")
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{"bad json", map[string]string{taskKey: `{"v":1,"task":`}, true},
		{"unsupported version", map[string]string{taskKey: `{"v":2,"task":"default/train-1","g":1,"t":"2026-10-20T07:59:00Z","p":{"0":50}}`}, true},
		{"score out of range", map[string]string{taskKey: `{"v":1,"task":"default/train-1","g":1,"t":"2026-10-20T07:59:00Z","p":{"0":101}}`}, true},
		{"bad task ref", map[string]string{taskKey: `{"v":1,"task":"default","g":1,"t":"2026-10-20T07:59:00Z","p":{"0":50}}`}, true},
	}
	for _, tt := range tests {
		for _, ref := range []string{"default/train-1", "bad/task/ref", "/train-1", "default/"} {
			t.Run(tt.name+"_"+ref, func(t *testing.T) {
				pod := testPod("train-1", 0)
				pod.Annotations[AnnotationWarmupTask] = ref
				pod.Annotations[AnnotationPodIndex] = "0"
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", Annotations: tt.annotations}}

				score, _, stale, err := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)
				if score != 0 || !stale {
					t.Fatalf("evaluate() = (%d, stale=%v, err=%v), want 0/stale", score, stale, err)
				}
				if ref == "default/train-1" && (err != nil) != tt.wantErr {
					t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	}
}

func TestEvaluateInvalidPodIndex(t *testing.T) {
	for _, raw := range []string{"abc", "-1", "1.5", ""} {
		pod := testPod("train-1", 0)
		pod.Annotations[AnnotationPodIndex] = raw
		node := testNode(freshValue("default/train-1", 2, map[string]int32{"0": 50}))

		score, _, stale, err := newTestPlugin(t, Options{}).evaluate(pod, node, testNow)
		if score != 0 || !stale || err == nil {
			t.Fatalf("pod-index %q: evaluate() = (%d, stale=%v, err=%v), want 0", raw, score, stale, err)
		}
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
	prefix := "example.io/custom"
	plugin, err := Factory(Options{Prefix: prefix, TTL: time.Minute, Now: func() time.Time { return testNow }})(context.Background(), nil, nil)
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

	pod := testPod("train-1", 0)
	node := testNode(freshValue("default/train-1", 2, map[string]int32{"0": 50}))
	raw, err := scorefmt.EncodeValue(freshValue("default/train-1", 2, map[string]int32{"0": 50}))
	if err != nil {
		t.Fatal(err)
	}
	node.Annotations = map[string]string{scorefmt.KeyWithPrefix(prefix, testNamespace, "train-1"): raw}

	score, key, stale, err := p.evaluate(pod, node, testNow)
	if err != nil || stale || score != 50 {
		t.Fatalf("evaluate() = (%d, %q, stale=%v, err=%v), want custom prefix score 50", score, key, stale, err)
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
	taskKey := scorefmt.KeyWithPrefix(scorefmt.DefaultPrefix, testNamespace, "train-1")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", Annotations: map[string]string{taskKey: `{broken`}}}

	if score := p.scoreNode(testPod("train-1", 0), node, testNow); score != 0 {
		t.Fatalf("scoreNode() = %d, want 0", score)
	}
	if got := buf.String(); !strings.Contains(got, `"level":"WARN"`) {
		t.Fatalf("no WARN log emitted: %s", got)
	}
}
