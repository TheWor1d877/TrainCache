package warmup

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"traincache/controller/internal/scorefmt"
)

var scoreTestNow = time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)

func scoreTestTaskRef(name string) TaskRef {
	return TaskRef{Namespace: "train", Name: name, Generation: 3}
}

func scoreNode(name string, annotations map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}}
}

func scoreClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build()
}

func readPodScore(t *testing.T, c client.Client, podKey types.NamespacedName, annotationKey string) scorefmt.Value {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), podKey, pod); err != nil {
		t.Fatalf("get pod %s: %v", podKey, err)
	}
	raw, ok := pod.Annotations[annotationKey]
	if !ok {
		t.Fatalf("annotation %q missing on pod %s", annotationKey, podKey)
	}
	value, err := scorefmt.DecodeValue(raw)
	if err != nil {
		t.Fatalf("decode annotation %q: %v", annotationKey, err)
	}
	return value
}

func TestScoreAnnotationKey(t *testing.T) {
	if got := ScoreAnnotationKey(""); got != scorefmt.DefaultAnnotation {
		t.Fatalf("ScoreAnnotationKey(\"\") = %q, want %q", got, scorefmt.DefaultAnnotation)
	}
	if got := ScoreAnnotationKey("example.io/node-scores"); got != "example.io/node-scores" {
		t.Fatalf("ScoreAnnotationKey(custom) = %q, want the configured key", got)
	}
}

func TestEncodePodScoresRoundTrip(t *testing.T) {
	task := scoreTestTaskRef("trainer")
	raw, err := EncodePodScores(task, map[string]int32{"node-a": 87, "node-b": 73}, scoreTestNow)
	if err != nil {
		t.Fatalf("EncodePodScores: %v", err)
	}
	value, err := scorefmt.DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if value.V != 1 {
		t.Fatalf("V = %d, want 1", value.V)
	}
	if value.Task != "train/trainer" {
		t.Fatalf("Task = %q, want train/trainer", value.Task)
	}
	if value.G != task.Generation {
		t.Fatalf("G = %d, want %d", value.G, task.Generation)
	}
	if !value.T.Equal(scoreTestNow) {
		t.Fatalf("T = %v, want %v", value.T, scoreTestNow)
	}
	if len(value.S) != 2 || value.S["node-a"] != 87 || value.S["node-b"] != 73 {
		t.Fatalf("S = %v, want map[node-a:87 node-b:73]", value.S)
	}
}

func TestEncodePodScoresEmptySnapshot(t *testing.T) {
	raw, err := EncodePodScores(scoreTestTaskRef("trainer"), map[string]int32{}, scoreTestNow)
	if err != nil {
		t.Fatalf("EncodePodScores: %v", err)
	}
	value, err := scorefmt.DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if len(value.S) != 0 {
		t.Fatalf("S = %v, want an empty node map", value.S)
	}
}

func TestEncodePodScoresValidates(t *testing.T) {
	cases := []struct {
		name   string
		task   TaskRef
		scores map[string]int32
	}{
		{"empty namespace", TaskRef{Name: "trainer", Generation: 3}, map[string]int32{"node-a": 50}},
		{"empty name", TaskRef{Namespace: "train", Generation: 3}, map[string]int32{"node-a": 50}},
		{"empty node", scoreTestTaskRef("trainer"), map[string]int32{"": 50}},
		{"score above max", scoreTestTaskRef("trainer"), map[string]int32{"node-a": 101}},
		{"score below min", scoreTestTaskRef("trainer"), map[string]int32{"node-a": -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := EncodePodScores(tc.task, tc.scores, scoreTestNow); err == nil {
				t.Fatal("EncodePodScores succeeded, want validation error")
			}
		})
	}
}
