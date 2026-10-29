package warmup

// M12 log assertions for docs/8.1 design.md §4.1 points 13/14/17: Node
// annotation write conflict/exhaustion and pod score detail; released Pod
// landing off its GWAA assignment (Warn log paired with NodeDeviation event).

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/logging"
)

// §4.1 第 13/14 点: pod score detail (Debug), conflict retry (Debug) and the
// successful annotation write (Info) all carry node/key/generation context.
func TestM12ScoreAnnotationLogFields(t *testing.T) {
	buf, log := m12JSONLog(t)
	ctx := logging.Into(context.Background(), log)
	c := &scoreFlakyClient{Client: scoreClient(t, scoreNode("node-a", nil))}
	c.remaining.Store(2)
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}

	if err := w.Write(ctx, scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(0, 42)}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	detail := m12FindLog(t, buf, "pod score detail")
	m12RequireLevel(t, detail, "DEBUG")
	m12RequireFields(t, detail, "component", "warmup_task", "pod", "node", "k", "h", "c", "score")

	retry := m12FindLog(t, buf, "score annotation conflict, retrying")
	m12RequireLevel(t, retry, "DEBUG")
	m12RequireFields(t, retry, "component", "warmup_task", "node", "annotation_key", "generation", "attempt")

	written := m12FindLog(t, buf, "score annotation written")
	m12RequireLevel(t, written, "INFO")
	m12RequireFields(t, written, "component", "warmup_task", "node", "annotation_key", "generation", "entries", "pods")
}

// §4.1 第 13 点: exhausting scoreWriteAttempts logs ErrScoreConflict at Error.
func TestM12ScoreAnnotationConflictExhaustedLogFields(t *testing.T) {
	buf, log := m12JSONLog(t)
	ctx := logging.Into(context.Background(), log)
	c := &scoreConflictClient{Client: scoreClient(t, scoreNode("node-a", nil))}
	w := &ScoreWriter{Client: c, Now: func() time.Time { return scoreTestNow }}

	err := w.Write(ctx, scoreTestTaskRef("trainer"), "node-a", []ScoreEntry{scoreTestEntry(0, 42)})
	if !errors.Is(err, ErrScoreConflict) {
		t.Fatalf("Write error = %v, want ErrScoreConflict", err)
	}

	rec := m12FindLog(t, buf, "score annotation conflict exhausted")
	m12RequireLevel(t, rec, "ERROR")
	m12RequireFields(t, rec, "component", "warmup_task", "node", "annotation_key", "generation", "attempts", "err")
}

// §4.1 第 5 点: GWAA 分配结果按 reconcile 打出 fanout/assign/unassigned.
func TestM12GwaaPlacementAppliedLogFields(t *testing.T) {
	task := fixtureTask(2, 2, [][]int32{{0, 1, 2}, {3, 4, 5}})
	ds := fixtureDataSet(6, singleShardLayout(6), []string{"n1", "n2"})
	buf, log := m12JSONLog(t)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureNode("n2"),
		fixtureAgentPod("n1"), fixtureAgentPod("n2"),
		fixtureTargetPod(0), fixtureTargetPod(1))
	r := &Reconciler{
		Client: c,
		Agents: &fakeAgentPool{},
		Log:    log,
		Now:    func() time.Time { return time.Unix(100, 0) },
	}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	rec := m12FindLog(t, buf, "gwaa placement applied")
	m12RequireLevel(t, rec, "INFO")
	m12RequireFields(t, rec, "component", "warmup_task",
		"fanout", "fanout_min", "fanout_max",
		"assign", "warm_node_count", "status_entries", "unassigned")
	// assign 是 Pod→节点列表的结构化对象:每个 Pod 单独取得到,节点名各自成元素。
	assign := m12AssignNodes(t, rec)
	if len(assign) != 2 {
		t.Fatalf("assign = %v, want one entry per Pod", assign)
	}
	known := map[string]struct{}{"n1": {}, "n2": {}}
	for _, pod := range []string{"p0", "p1"} {
		if len(assign[pod]) == 0 {
			t.Fatalf("assign[%s] = %v, want at least one node", pod, assign[pod])
		}
		for _, node := range assign[pod] {
			if _, ok := known[node]; !ok {
				t.Fatalf("assign[%s] holds %q, want a candidate node", pod, node)
			}
		}
	}
}

// §4.1 第 17 点: a released Pod on a node outside its assignment records the
// Warn log and the NodeDeviation event as one pair.
func TestM12DeviationWarnAndEventPaired(t *testing.T) {
	task := fixtureTask(1, 1, [][]int32{{0}})
	task.Status.Phase = traincachev1alpha1.WarmupPhaseReleased
	task.Status.PodNodeReadiness = []traincachev1alpha1.PodNodeReadiness{
		{PodIndex: 0, NodeName: "n1"},
	}
	ds := fixtureDataSet(1, singleShardLayout(1), []string{"n1", "n2"})
	pod := fixtureTargetPod(0)
	pod.Spec.NodeName = "n2"
	recorder := record.NewFakeRecorder(4)
	buf, log := m12JSONLog(t)
	c := fixtureClient(t, task, ds, pod)
	r := &Reconciler{
		Client:   c,
		Agents:   &fakeAgentPool{},
		Recorder: recorder,
		Log:      log,
		Now:      func() time.Time { return time.Unix(100, 0) },
	}
	taskLog := log.With("warmup_task", task.Namespace+"/"+task.Name)

	r.reconcileDeviation(context.Background(), taskLog, task)

	rec := m12FindLog(t, buf, "node deviation detected")
	m12RequireLevel(t, rec, "WARN")
	m12RequireFields(t, rec, "component", "warmup_task", "pod", "pod_index", "warm_nodes", "actual_node")
	if rec["actual_node"] != "n2" || rec["warm_nodes"] != "n1" {
		t.Fatalf("deviation log = %#v, want actual_node=n2 warm_nodes=n1", rec)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, EventNodeDeviation) {
			t.Fatalf("event = %q, want %s", event, EventNodeDeviation)
		}
		if !strings.Contains(event, "n2") || !strings.Contains(event, "n1") {
			t.Fatalf("event = %q, want actual and assigned nodes", event)
		}
	default:
		t.Fatalf("no %s event recorded", EventNodeDeviation)
	}
}
