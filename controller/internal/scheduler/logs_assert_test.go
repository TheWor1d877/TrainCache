package scheduler

// Log assertions for docs/7.1 design.md §9.3 (Scheduler 打分).

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"traincache/controller/internal/scorefmt"
)

func m12SchedulerLog(t *testing.T) (*bytes.Buffer, *slog.Logger) {
	t.Helper()
	var buf bytes.Buffer
	return &buf, slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func m12SchedulerFind(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
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

func TestSchedulerScoreLogFields(t *testing.T) {
	buf, log := m12SchedulerLog(t)
	p := newTestPlugin(t, Options{Logger: log})
	value := freshValue("default/train-1", 7, map[string]int32{"node-1": 87})

	if score := p.scoreNode(testPod("train-1", &value), testNode("node-1"), testNow); score != 87 {
		t.Fatalf("scoreNode() = %d, want 87", score)
	}

	rec := m12SchedulerFind(t, buf, "scheduler score")
	if rec["level"] != "DEBUG" {
		t.Fatalf("level = %v, want DEBUG", rec["level"])
	}
	for _, field := range []string{"pod", "node", "annotation_key", "score", "stale"} {
		if _, ok := rec[field]; !ok {
			t.Fatalf("scheduler score log missing %q: %#v", field, rec)
		}
	}
	if rec["pod"] != "default/train-1" || rec["node"] != "node-1" {
		t.Fatalf("pod/node = %v/%v", rec["pod"], rec["node"])
	}
	if rec["annotation_key"] != scorefmt.DefaultAnnotation {
		t.Fatalf("annotation_key = %v, want %q", rec["annotation_key"], scorefmt.DefaultAnnotation)
	}
	if rec["score"] != float64(87) || rec["stale"] != false {
		t.Fatalf("score/stale = %v/%v, want 87/false", rec["score"], rec["stale"])
	}
}

func TestSchedulerScoreRejectedLogFields(t *testing.T) {
	buf, log := m12SchedulerLog(t)
	p := newTestPlugin(t, Options{Logger: log})
	pod := testPod("train-1", nil)
	pod.Annotations[scorefmt.DefaultAnnotation] = `{broken`

	if score := p.scoreNode(pod, testNode("node-1"), testNow); score != 0 {
		t.Fatalf("scoreNode() = %d, want 0", score)
	}

	rec := m12SchedulerFind(t, buf, "scheduler score rejected")
	if rec["level"] != "WARN" {
		t.Fatalf("level = %v, want WARN", rec["level"])
	}
	for _, field := range []string{"pod", "node", "annotation_key", "err"} {
		if _, ok := rec[field]; !ok {
			t.Fatalf("scheduler rejected log missing %q: %#v", field, rec)
		}
	}
	if rec["annotation_key"] != scorefmt.DefaultAnnotation {
		t.Fatalf("annotation_key = %v, want %q", rec["annotation_key"], scorefmt.DefaultAnnotation)
	}
}
