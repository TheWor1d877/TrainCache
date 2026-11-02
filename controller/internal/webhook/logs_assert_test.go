package webhook

// Log assertions for docs/7.1 design.md §9.3 (Webhook 拒绝/跳过).

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"traincache/controller/internal/controller/warmup"
)

func m12WebhookFind(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
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

func m12WebhookAssertSkipped(t *testing.T, buf *bytes.Buffer, wantReason string) {
	t.Helper()
	rec := m12WebhookFind(t, buf, "pod injection skipped")
	if rec["level"] != "WARN" {
		t.Fatalf("level = %v, want WARN", rec["level"])
	}
	for _, field := range []string{"pod", "namespace", "reason"} {
		if _, ok := rec[field]; !ok {
			t.Fatalf("skip log missing %q: %#v", field, rec)
		}
	}
	if rec["reason"] != wantReason {
		t.Fatalf("reason = %v, want %s", rec["reason"], wantReason)
	}
	if rec["namespace"] != "train" {
		t.Fatalf("namespace = %v, want train", rec["namespace"])
	}
}

func TestWebhookSkipLogFields(t *testing.T) {
	t.Run("unselected pod", func(t *testing.T) {
		m, _ := newMutator(t, trainTask())
		buf := &bytes.Buffer{}
		m.Log = slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		pod := selectedJobPod("0")
		delete(pod.Labels, warmup.WarmupEnabledLabel)

		if resp := m.Handle(context.Background(), admissionRequest(t, pod)); !resp.Allowed {
			t.Fatalf("response not allowed: %#v", resp.Result)
		}
		m12WebhookAssertSkipped(t, buf, "missing warmup=enabled label")
	})

	t.Run("no matching task", func(t *testing.T) {
		m, _ := newMutator(t)
		buf := &bytes.Buffer{}
		m.Log = slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		if resp := m.Handle(context.Background(), admissionRequest(t, selectedJobPod("0"))); !resp.Allowed {
			t.Fatalf("response not allowed: %#v", resp.Result)
		}
		m12WebhookAssertSkipped(t, buf, "no matching WarmupTask")
	})
}
