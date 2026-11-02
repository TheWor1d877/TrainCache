package svc

import (
	"os"
	"testing"

	"traincache/agent/ordersource"
)

// backfill_logs_test.go asserts the corrupt-block isolation log record keeps
// its structured fields (docs/8.1 §4.2).
func TestBackfillQuarantineLogFields(t *testing.T) {
	buf := m12CaptureLogsTS(t)

	ag, cfg, _ := newBackfillAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "bad-000.tar", content: "bad-content", source: true})
	fp := fps["bad-000.tar#0"]
	preloadBackfillMapping(t, ag, wiringDataset)
	if err := os.WriteFile(wiringBlockFile(cfg, fp), []byte("corrupted-not-matching"), 0o644); err != nil {
		t.Fatalf("破坏本地块: %v", err)
	}

	// 不装配重扫描修复器:坏块只能隔离,这条 Warn 是本用例的断言目标。
	ag.Rescan = nil
	ag.handleReadMiss(ordersource.ChunkRef{Path: "bad-000.tar"}, "corrupt")

	rec := m12WaitForLogTS(t, buf, "backfill: 未装配重扫描修复器,坏块仅隔离", hookTestTimeout)
	m12RequireLevel(t, rec, "WARN")
	m12RequireFields(t, rec, "dataset", "path", "chunk")
	if rec["dataset"] != wiringDataset {
		t.Errorf("dataset = %v, want %s", rec["dataset"], wiringDataset)
	}
	if rec["path"] != "bad-000.tar" {
		t.Errorf("path = %v, want bad-000.tar", rec["path"])
	}
	if got, ok := rec["chunk"].(float64); !ok || got != 0 {
		t.Errorf("chunk = %v, want 0", rec["chunk"])
	}

	qs, err := ag.CS.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined: %v", err)
	}
	if len(qs) != 1 {
		t.Errorf("quarantined blocks = %d, want 1", len(qs))
	}
}
