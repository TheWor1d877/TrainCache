package warmup

import (
	"testing"
	"time"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
)

// poll_bitmap_recovery_test.go pins the malformed-bitmap behaviour from
// docs/7.3 §4: the entry keeps its previous state (never Failed), the table is
// untouched, and the next poll round with a valid bitmap updates readiness.
func TestPollBitmapMismatchKeepsPollingAndRecovers(t *testing.T) {
	// K=9 asks for 9 chunks, so a 1-byte bitmap is malformed (want 2).
	task := fixtureTask(1, 9, [][]int32{{0, 1, 2, 3, 4, 5, 6, 7, 8}})
	ds := fixtureDataSet(9, singleShardLayout(9), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1",
		progressResponse{status: bitmapResponse(3, 1, 9, []byte{0xFF})},
		progressResponse{status: bitmapResponse(3, 2, 9, []byte{0xFF, 0x01})},
	)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	buf, log := m12JSONLog(t)
	r := &Reconciler{Client: c, Agents: pool, Log: log, PollInterval: 3 * time.Second}

	// 第一轮:长度不符 → 丢弃,不改表,不置 Failed。
	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	if result.RequeueAfter != 3*time.Second {
		t.Fatalf("RequeueAfter = %v, want 3s", result.RequeueAfter)
	}
	updated := readyTask(t, c)
	if got := updated.Status.ReadinessTable[0][0]; got != 0 {
		t.Fatalf("table[0][0] = %d, want 0 after malformed bitmap", got)
	}
	entry := readinessEntry(updated, 0, "n1")
	if entry == nil {
		t.Fatal("missing PodNodeReadiness for p0/n1")
	}
	if entry.State != traincachev1alpha1.PreheatStatePreheating {
		t.Fatalf("state = %q, want Preheating", entry.State)
	}
	if len(entry.ReadyBlockBitmap) != 0 {
		t.Fatalf("bitmap = %v, want empty", entry.ReadyBlockBitmap)
	}
	for _, e := range updated.Status.PodNodeReadiness {
		if e.State == traincachev1alpha1.PreheatStateFailed {
			t.Fatalf("malformed bitmap set state Failed for pod %d node %s", e.PodIndex, e.NodeName)
		}
	}
	rec := m12FindLog(t, buf, "drop progress: invalid ready bitmap")
	m12RequireLevel(t, rec, "WARN")
	m12RequireFields(t, rec, "task_id", "pod", "pod_index", "node", "generation", "seq", "duration_ms", "err")
	if got, _ := rec["seq"].(float64); got != 1 {
		t.Errorf("log seq = %v, want 1", rec["seq"])
	}

	// 第二轮:合法位图 → 表就绪数前进(下一轮重试由 RequeueAfter 保证)。
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}
	updated = readyTask(t, c)
	if got := updated.Status.ReadinessTable[0][0]; got != 9 {
		t.Fatalf("table[0][0] = %d, want 9 after recovery", got)
	}
	entry = readinessEntry(updated, 0, "n1")
	if entry == nil {
		t.Fatal("missing PodNodeReadiness for p0/n1 after recovery")
	}
	if entry.State != traincachev1alpha1.PreheatStatePreheating {
		t.Fatalf("state = %q, want Preheating", entry.State)
	}
	if len(entry.ReadyBlockBitmap) != 2 || entry.ReadyBlockBitmap[1] != 0x01 {
		t.Fatalf("bitmap = %v, want 2 bytes ending 0x01", entry.ReadyBlockBitmap)
	}
}
