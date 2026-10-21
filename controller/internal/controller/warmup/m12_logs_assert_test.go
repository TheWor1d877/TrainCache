package warmup

// M12 log assertions for docs/8.1 design.md §9.3. These tests drive the
// production code paths and assert the structured JSON records (message +
// required fields), so renaming a message or dropping a field fails here.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/gwaa"
)

func m12JSONLog(t *testing.T) (*bytes.Buffer, *slog.Logger) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})).
		With("component", "controller-manager")
	return &buf, log
}

func m12LogRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("invalid log line %q: %v", raw, err)
		}
		out = append(out, rec)
	}
	return out
}

func m12FindLog(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for _, rec := range m12LogRecords(t, buf) {
		if rec["msg"] == msg {
			return rec
		}
	}
	t.Fatalf("log %q not found in:\n%s", msg, buf.String())
	return nil
}

func m12RequireFields(t *testing.T, rec map[string]any, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if _, ok := rec[field]; !ok {
			t.Fatalf("log %q missing field %q: %#v", rec["msg"], field, rec)
		}
	}
}

func m12RequireLevel(t *testing.T, rec map[string]any, want string) {
	t.Helper()
	if rec["level"] != want {
		t.Fatalf("log %q level = %v, want %s", rec["msg"], rec["level"], want)
	}
}

// §9.3 每个 Reconcile 入口/出口 + 候选筛选结果 + 消费顺序解析.
func TestM12WarmupReconcileLifecycleAndSummaryLogs(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	buf, log := m12JSONLog(t)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: &fakeAgentPool{}, Log: log, Now: func() time.Time { return time.Unix(100, 0) }}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	start := m12FindLog(t, buf, "warmup reconcile start")
	m12RequireLevel(t, start, "INFO")
	m12RequireFields(t, start, "component", "warmup_task", "namespace", "generation", "request_id")

	finish := m12FindLog(t, buf, "warmup reconcile finished")
	m12RequireLevel(t, finish, "INFO")
	m12RequireFields(t, finish, "component", "warmup_task", "generation", "request_id", "duration_ms")

	candidates := m12FindLog(t, buf, "candidate filtering")
	m12RequireLevel(t, candidates, "INFO")
	m12RequireFields(t, candidates, "warmup_task", "reason", "nodes", "accessible", "agent_ready", "candidates")

	consume := m12FindLog(t, buf, "consume order resolved")
	m12RequireLevel(t, consume, "INFO")
	m12RequireFields(t, consume, "warmup_task", "consume_order_source", "reason")
}

// §9.3 spec 校验失败.
func TestM12WarmupSpecInvalidLogFields(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	task.Spec.PodDetails[0].K = 0 // fails ValidateTask
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	buf, log := m12JSONLog(t)
	c := fixtureClient(t, task, ds)
	r := &Reconciler{Client: c, Agents: &fakeAgentPool{}, Log: log}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "train", Name: "t1"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	rec := m12FindLog(t, buf, "warmup spec invalid")
	m12RequireLevel(t, rec, "WARN")
	m12RequireFields(t, rec, "component", "warmup_task", "generation", "request_id", "reason", "err")
}

// §9.3 消费顺序默认切分：日志 + ConsumeOrderDefaulted 事件.
func TestM12ConsumeOrderDefaultSplitLogAndEvent(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{99}}) // out of range => default split
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	buf, log := m12JSONLog(t)
	recorder := record.NewFakeRecorder(4)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: &fakeAgentPool{}, Log: log, Recorder: recorder}

	if err := reconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	rec := m12FindLog(t, buf, "consume order resolved")
	m12RequireFields(t, rec, "warmup_task", "consume_order_source", "reason")
	if rec["consume_order_source"] != string(traincachev1alpha1.ConsumeOrderSourceDefault) {
		t.Fatalf("consume_order_source = %v, want %s", rec["consume_order_source"], traincachev1alpha1.ConsumeOrderSourceDefault)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, EventConsumeOrderDefaulted) {
			t.Fatalf("event = %q, want %s", event, EventConsumeOrderDefaulted)
		}
	default:
		t.Fatal("no ConsumeOrderDefaulted event recorded")
	}
}

// §9.3 进度轮询结果变化.
func TestM12ProgressPollLogFields(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 2, []byte{0xFF})})
	buf, log := m12JSONLog(t)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	r := &Reconciler{Client: c, Agents: pool, Log: log}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	rec := m12FindLog(t, buf, "progress updated")
	m12RequireLevel(t, rec, "DEBUG")
	m12RequireFields(t, rec, "task_id", "node", "generation", "seq", "ready")
}

// §9.3 gRPC Submit/Update/Cancel.
func TestM12AgentPlacementRPCLogs(t *testing.T) {
	buf, log := m12JSONLog(t)
	if _, err := ApplyPlacement(context.Background(), &fakePlacementRPC{}, log, testPlacementInput()); err != nil {
		t.Fatalf("ApplyPlacement: %v", err)
	}

	for _, msg := range []string{"agent submit", "agent update", "agent cancel"} {
		rec := m12FindLog(t, buf, msg)
		m12RequireLevel(t, rec, "INFO")
		m12RequireFields(t, rec, "warmup_task", "task_id", "pod", "node", "generation")
	}
}

// §9.3 gRPC 失败/重试: each operation logs attempt/err at Warn.
func TestM12AgentPlacementFailureLogs(t *testing.T) {
	down := fmt.Errorf("%w: down", agentclient.ErrAgentUnavailable)
	cases := []struct {
		name string
		in   func() PlacementApplyInput
		msg  string
	}{
		{
			name: "submit",
			in: func() PlacementApplyInput {
				in := testPlacementInput()
				in.Previous = map[PreheatPair]ExistingPreheat{}
				in.Placement = gwaa.Output{Assignment: map[int32]string{0: "n0"}}
				return in
			},
			msg: "agent submit failed",
		},
		{
			name: "update",
			in: func() PlacementApplyInput {
				in := testPlacementInput()
				in.Previous = map[PreheatPair]ExistingPreheat{
					{PodIndex: 0, Node: "n0"}: {Chunks: testChunks("old"), Generation: 7},
				}
				in.Placement = gwaa.Output{Assignment: map[int32]string{0: "n0"}}
				return in
			},
			msg: "agent update failed",
		},
		{
			name: "cancel",
			in: func() PlacementApplyInput {
				in := testPlacementInput()
				in.Previous = map[PreheatPair]ExistingPreheat{
					{PodIndex: 0, Node: "n0"}: {Chunks: testChunks("a"), Generation: 7},
				}
				in.Placement = gwaa.Output{}
				return in
			},
			msg: "agent cancel failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, log := m12JSONLog(t)
			rpc := &fakePlacementRPC{fail: map[string]error{"n0": down}}
			if _, err := ApplyPlacement(context.Background(), rpc, log, tc.in()); err == nil {
				t.Fatal("ApplyPlacement succeeded, want unavailable error")
			}
			rec := m12FindLog(t, buf, tc.msg)
			m12RequireLevel(t, rec, "WARN")
			m12RequireFields(t, rec, "warmup_task", "task_id", "pod", "node", "generation", "attempt", "err")
		})
	}
}

// §9.3 Gate 注入/摘除: release path emits gate released + per-Pod gate removed.
func TestM12GateReleaseAndRemovalLogs(t *testing.T) {
	task := fixtureTask(1, 2, [][]int32{{0, 1}})
	ds := fixtureDataSet(2, singleShardLayout(2), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", doneResponse(3, fullBitmap(2)))
	buf, log := m12JSONLog(t)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), gatedPod(0))
	r := &Reconciler{Client: c, Agents: pool, Log: log, PollInterval: 2 * time.Second}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	evaluated := m12FindLog(t, buf, "gate evaluated")
	m12RequireLevel(t, evaluated, "INFO")
	m12RequireFields(t, evaluated, "warmup_task", "state", "reason", "rates", "required")

	released := m12FindLog(t, buf, "gate released")
	m12RequireLevel(t, released, "INFO")
	m12RequireFields(t, released, "warmup_task", "state", "reason", "removed", "skipped")

	removed := m12FindLog(t, buf, "gate removed")
	m12RequireLevel(t, removed, "INFO")
	m12RequireFields(t, removed, "warmup_task", "pod", "pod_index", "node", "request_id")
	if removed["node"] != "n1" {
		t.Fatalf("gate removed node = %v, want n1", removed["node"])
	}
}

// §9.3 降级/故障恢复.
func TestM12DegradedConditionLogs(t *testing.T) {
	task := testTask("t1", traincachev1alpha1.TargetRef{Kind: "Job", Name: "trainer"})
	pod := selectedPod("trainer-0")
	pod.Labels["batch.kubernetes.io/job-name"] = "trainer"
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&traincachev1alpha1.WarmupTask{}).
		WithObjects(task, pod).
		Build()
	buf, log := m12JSONLog(t)
	r := &DegradedReconciler{Client: c, Log: log}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "train", Name: "trainer-0"}}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	degraded := m12FindLog(t, buf, "gate injection degraded")
	m12RequireLevel(t, degraded, "WARN")
	m12RequireFields(t, degraded, "component", "warmup_task", "pod", "reason", "state")

	recorded := m12FindLog(t, buf, "gate injection degraded condition recorded")
	m12RequireLevel(t, recorded, "WARN")
	m12RequireFields(t, recorded, "component", "warmup_task", "reason", "duration_ms", "request_id")
}
