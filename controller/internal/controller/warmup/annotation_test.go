package warmup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"traincache/controller/internal/logging"
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

func readScoreValue(t *testing.T, c client.Client, nodeName, key string) scorefmt.Value {
	t.Helper()
	node := &corev1.Node{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: nodeName}, node); err != nil {
		t.Fatalf("get node: %v", err)
	}
	raw, ok := node.Annotations[key]
	if !ok {
		t.Fatalf("annotation %q missing on node %q", key, nodeName)
	}
	value, err := scorefmt.DecodeValue(raw)
	if err != nil {
		t.Fatalf("decode annotation %q: %v", key, err)
	}
	return value
}

func scoreTestEntry(index, score int32) ScoreEntry {
	return ScoreEntry{PodIndex: index, K: 4, H: 2, C: 8, Score: score}
}

func TestScoreWriterRoundTrip(t *testing.T) {
	c := scoreClient(t, scoreNode("node-a", nil))
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}
	task := scoreTestTaskRef("trainer")

	if err := w.Write(context.Background(), task, "node-a",
		[]ScoreEntry{scoreTestEntry(0, 87), scoreTestEntry(1, 73)}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	key := scorefmt.Key(task.Namespace, task.Name)
	value := readScoreValue(t, c, "node-a", key)
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
	if len(value.P) != 2 || value.P["0"] != 87 || value.P["1"] != 73 {
		t.Fatalf("P = %v, want map[0:87 1:73]", value.P)
	}
}

func TestScoreWriterKeyLength(t *testing.T) {
	longName := strings.Repeat("a", 60)
	key := scorefmt.Key("namespace", longName)
	namePart := key[strings.LastIndex(key, "/")+1:]
	if len(namePart) > 52 {
		t.Fatalf("key name part %q has length %d, want <= 52", namePart, len(namePart))
	}
}

func TestScoreWriterPreservesOtherKeysAndPodEntries(t *testing.T) {
	ownKey := scorefmt.Key("train", "trainer")
	otherKey := scorefmt.Key("train", "other-task")
	otherValue, err := scorefmt.EncodeValue(scorefmt.Value{
		V: 1, Task: "train/other-task", G: 9, T: scoreTestNow, P: map[string]int32{"0": 42},
	})
	if err != nil {
		t.Fatalf("encode other value: %v", err)
	}
	ownOld, err := scorefmt.EncodeValue(scorefmt.Value{
		V: 1, Task: "train/trainer", G: 2, T: scoreTestNow, P: map[string]int32{"0": 10, "9": 99},
	})
	if err != nil {
		t.Fatalf("encode own value: %v", err)
	}

	node := scoreNode("node-a", map[string]string{
		ownKey:   ownOld,
		otherKey: otherValue,
		"foo":    "bar",
	})
	c := scoreClient(t, node)
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}
	if err := w.Write(context.Background(), scoreTestTaskRef("trainer"), "node-a",
		[]ScoreEntry{scoreTestEntry(0, 50), scoreTestEntry(1, 60)}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	gotNode := &corev1.Node{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "node-a"}, gotNode); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if gotNode.Annotations["foo"] != "bar" {
		t.Fatalf("unrelated annotation was modified: %q", gotNode.Annotations["foo"])
	}
	if gotNode.Annotations[otherKey] != otherValue {
		t.Fatalf("other task annotation was modified")
	}

	own := readScoreValue(t, c, "node-a", ownKey)
	if own.G != 3 {
		t.Fatalf("G = %d, want 3", own.G)
	}
	if own.P["0"] != 50 || own.P["1"] != 60 {
		t.Fatalf("P = %v, want updated entries for pods 0 and 1", own.P)
	}
	if own.P["9"] != 99 {
		t.Fatalf("old pod entry was dropped: P = %v", own.P)
	}
}

func TestScoreWriterReplacesCorruptValue(t *testing.T) {
	key := scorefmt.Key("train", "trainer")
	c := scoreClient(t, scoreNode("node-a", map[string]string{key: "{not-json"}))
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}
	if err := w.Write(context.Background(), scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(2, 70)}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	value := readScoreValue(t, c, "node-a", key)
	if len(value.P) != 1 || value.P["2"] != 70 {
		t.Fatalf("P = %v, want map[2:70]", value.P)
	}
}

// scoreBarrierClient makes every writer's first Get observe the same pre-write
// state, so the subsequent writes deterministically conflict at least once
// with the fake client's resourceVersion checking.
type scoreBarrierClient struct {
	client.Client
	once    *sync.Once
	barrier *sync.WaitGroup
}

func (c *scoreBarrierClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	c.once.Do(c.barrier.Done)
	c.barrier.Wait()
	return nil
}

func runScoreWritesConcurrently(t *testing.T, writes ...func() error) {
	t.Helper()
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, len(writes))
	for _, write := range writes {
		wg.Add(1)
		go func(write func() error) {
			defer wg.Done()
			<-start
			if err := write(); err != nil {
				errs <- err
			}
		}(write)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write: %v", err)
	}
}

func TestScoreWriterConcurrentTasksKeepBothKeys(t *testing.T) {
	c := scoreClient(t, scoreNode("node-a", nil))
	var barrier sync.WaitGroup
	barrier.Add(2)
	newWriter := func() *ScoreWriter {
		return &ScoreWriter{
			Client: &scoreBarrierClient{Client: c, once: &sync.Once{}, barrier: &barrier},
			Now:    func() time.Time { return scoreTestNow },
		}
	}
	wA, wB := newWriter(), newWriter()
	taskA, taskB := scoreTestTaskRef("task-a"), scoreTestTaskRef("task-b")

	runScoreWritesConcurrently(t,
		func() error {
			return wA.Write(context.Background(), taskA, "node-a", []ScoreEntry{scoreTestEntry(0, 10)})
		},
		func() error {
			return wB.Write(context.Background(), taskB, "node-a", []ScoreEntry{scoreTestEntry(1, 20)})
		},
	)

	valueA := readScoreValue(t, c, "node-a", scorefmt.Key(taskA.Namespace, taskA.Name))
	valueB := readScoreValue(t, c, "node-a", scorefmt.Key(taskB.Namespace, taskB.Name))
	if len(valueA.P) != 1 || valueA.P["0"] != 10 {
		t.Fatalf("task A P = %v, want map[0:10]", valueA.P)
	}
	if len(valueB.P) != 1 || valueB.P["1"] != 20 {
		t.Fatalf("task B P = %v, want map[1:20]", valueB.P)
	}
}

func TestScoreWriterConcurrentSameTaskMergesPods(t *testing.T) {
	c := scoreClient(t, scoreNode("node-a", nil))
	var barrier sync.WaitGroup
	barrier.Add(2)
	newWriter := func() *ScoreWriter {
		return &ScoreWriter{
			Client: &scoreBarrierClient{Client: c, once: &sync.Once{}, barrier: &barrier},
			Now:    func() time.Time { return scoreTestNow },
		}
	}
	wA, wB := newWriter(), newWriter()
	task := scoreTestTaskRef("trainer")

	runScoreWritesConcurrently(t,
		func() error {
			return wA.Write(context.Background(), task, "node-a", []ScoreEntry{scoreTestEntry(0, 11)})
		},
		func() error {
			return wB.Write(context.Background(), task, "node-a", []ScoreEntry{scoreTestEntry(1, 22)})
		},
	)

	value := readScoreValue(t, c, "node-a", scorefmt.Key(task.Namespace, task.Name))
	if len(value.P) != 2 || value.P["0"] != 11 || value.P["1"] != 22 {
		t.Fatalf("P = %v, want both concurrent pod entries", value.P)
	}
}

// scoreConflictClient rejects every Update with a resourceVersion conflict.
type scoreConflictClient struct {
	client.Client
	updates atomic.Int32
}

func (c *scoreConflictClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updates.Add(1)
	return apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, obj.GetName(), errors.New("conflict"))
}

func TestScoreWriterConflictExhausted(t *testing.T) {
	base := scoreClient(t, scoreNode("node-a", nil))
	c := &scoreConflictClient{Client: base}
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}

	err := w.Write(context.Background(), scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(0, 50)})
	if !errors.Is(err, ErrScoreConflict) {
		t.Fatalf("Write error = %v, want ErrScoreConflict", err)
	}
	if got := c.updates.Load(); got != scoreWriteAttempts {
		t.Fatalf("Update calls = %d, want %d", got, scoreWriteAttempts)
	}
}

// scoreFlakyClient fails the first failures Updates and then delegates.
type scoreFlakyClient struct {
	client.Client
	remaining atomic.Int32
	updates   atomic.Int32
}

func (c *scoreFlakyClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updates.Add(1)
	if c.remaining.Add(-1) >= 0 {
		return apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, obj.GetName(), errors.New("conflict"))
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestScoreWriterConflictRetrySucceeds(t *testing.T) {
	base := scoreClient(t, scoreNode("node-a", nil))
	c := &scoreFlakyClient{Client: base}
	c.remaining.Store(2)
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}

	if err := w.Write(context.Background(), scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(0, 50)}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := c.updates.Load(); got != 3 {
		t.Fatalf("Update calls = %d, want 3", got)
	}
}

func TestScoreWriterValidatesInput(t *testing.T) {
	c := scoreClient(t, scoreNode("node-a", nil))
	w := &ScoreWriter{Client: c}
	cases := []struct {
		name    string
		task    TaskRef
		node    string
		entries []ScoreEntry
	}{
		{"empty namespace", TaskRef{Name: "trainer"}, "node-a", []ScoreEntry{scoreTestEntry(0, 1)}},
		{"empty name", TaskRef{Namespace: "train"}, "node-a", []ScoreEntry{scoreTestEntry(0, 1)}},
		{"empty node", scoreTestTaskRef("trainer"), "", []ScoreEntry{scoreTestEntry(0, 1)}},
		{"negative pod", scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(-1, 1)}},
		{"score too high", scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(0, 101)}},
		{"score negative", scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(0, -1)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := w.Write(context.Background(), tc.task, tc.node, tc.entries); err == nil {
				t.Fatal("Write succeeded, want validation error")
			}
		})
	}
}

func TestScoreWriterLogsDetailsAndResult(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := logging.Into(context.Background(), logger)

	c := scoreClient(t, scoreNode("node-a", nil))
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}
	if err := w.Write(ctx, scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(0, 87)}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var debug, info map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		switch record["level"] {
		case "DEBUG":
			if record["msg"] == "pod score detail" {
				debug = record
			}
		case "INFO":
			if record["msg"] == "score annotation written" {
				info = record
			}
		}
	}
	if debug == nil {
		t.Fatalf("missing pod score detail debug record: %s", buf.String())
	}
	for _, field := range []string{"pod", "node", "k", "h", "c", "score"} {
		if _, ok := debug[field]; !ok {
			t.Fatalf("debug record missing field %q: %v", field, debug)
		}
	}
	if info == nil {
		t.Fatalf("missing annotation written info record: %s", buf.String())
	}
	for _, field := range []string{"node", "annotation_key", "generation", "pods"} {
		if _, ok := info[field]; !ok {
			t.Fatalf("info record missing field %q: %v", field, info)
		}
	}
}
