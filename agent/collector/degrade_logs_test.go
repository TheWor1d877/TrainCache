package collector

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// degrade_logs_test.go asserts the two degrade-state log records survive with
// their actual structured fields (docs/9.1 §4.2, docs/9.4 §3 item 4).

func captureCollectorLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func collectorLogRecord(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("invalid log line %q: %v", raw, err)
		}
		if rec["msg"] == msg {
			return rec
		}
	}
	t.Fatalf("log %q not found in:\n%s", msg, buf.String())
	return nil
}

func TestDegradeLogsCarryFields(t *testing.T) {
	const (
		warnMsg = "collector: 进入降级模式(sidecar ring 停滞/文件消失),以 inotify 兜底"
		infoMsg = "collector: 退出降级模式(sidecar ring 恢复推进)"
	)
	buf := captureCollectorLogs(t)

	w, ring := ringAndReader(t)
	env := newTestEnv(t, ring, CollectorConfig{})
	env.c.clock = time.Now
	env.c.stallThreshold = 40 * time.Millisecond

	time.Sleep(60 * time.Millisecond)
	env.c.checkDegrade()
	if !env.c.Degraded() {
		t.Fatal("stall did not enter degraded state")
	}

	rec := collectorLogRecord(t, buf, warnMsg)
	if rec["level"] != "WARN" {
		t.Errorf("degrade log level = %v, want WARN", rec["level"])
	}
	if rec["block_dir"] != env.dir {
		t.Errorf("block_dir = %v, want %s", rec["block_dir"], env.dir)
	}
	if got, ok := rec["stall_threshold"].(float64); !ok || got != float64(int64(40*time.Millisecond)) {
		t.Errorf("stall_threshold = %v, want %d", rec["stall_threshold"], int64(40*time.Millisecond))
	}

	la := ring.LastAdvance()
	w.writeOrder(testOrder("b-track", "epoch-1", 2, 3))
	mustEventually(t, "advance visible", func() bool { return ring.LastAdvance().After(la) })
	env.c.checkDegrade()
	if env.c.Degraded() {
		t.Fatal("advance did not leave degraded state")
	}

	rec = collectorLogRecord(t, buf, infoMsg)
	if rec["level"] != "INFO" {
		t.Errorf("recovery log level = %v, want INFO", rec["level"])
	}
	if rec["block_dir"] != env.dir {
		t.Errorf("block_dir = %v, want %s", rec["block_dir"], env.dir)
	}
}
