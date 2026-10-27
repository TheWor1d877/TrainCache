package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func newTestLogger(buf *bytes.Buffer, withAttrs ...any) *slog.Logger {
	handler := NewRedactingHandler(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return slog.New(handler).With(withAttrs...)
}

func decodeSingle(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("output is not one JSON object: %v: %q", err, buf.String())
	}
	return got
}

func TestRedactingHandlerRedactsMessageAndError(t *testing.T) {
	const (
		accessKey = "HANDLER-ACCESS-KEY-SENTINEL"
		secretKey = "HANDLER-SECRET-KEY-SENTINEL"
		signature = "HANDLER-SIGNATURE-SENTINEL"
	)
	var buf bytes.Buffer
	log := newTestLogger(&buf)
	err := errors.New("s3 get failed: accessKey=" + accessKey + " secretKey=" + secretKey +
		" url=https://user:pass@minio:9000/bench-b/key?X-Amz-Signature=" + signature)

	log.Error("预热失败 accessKey="+accessKey, "err", err)

	out := buf.String()
	for _, leak := range []string{accessKey, secretKey, signature, "user:pass@"} {
		if strings.Contains(out, leak) {
			t.Fatalf("log leaks %q: %s", leak, out)
		}
	}
	got := decodeSingle(t, &buf)
	if got["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", got["level"])
	}
	if !strings.Contains(got["msg"].(string), "***") {
		t.Fatalf("message not redacted: %v", got["msg"])
	}
	if !strings.Contains(got["err"].(string), "***") {
		t.Fatalf("err not redacted: %v", got["err"])
	}
	if !strings.Contains(got["err"].(string), "minio:9000") {
		t.Fatalf("redacted err lost host: %v", got["err"])
	}
}

func TestRedactingHandlerRedactsWithAttrsAndGroups(t *testing.T) {
	const (
		secretKey = "WITH-ATTRS-SECRET-SENTINEL"
		token     = "GROUP-TOKEN-SENTINEL"
	)
	var buf bytes.Buffer
	log := newTestLogger(&buf, "secretKey", secretKey)

	log.Info("connected", slog.Group("creds", slog.String("token", token)))

	out := buf.String()
	for _, leak := range []string{secretKey, token} {
		if strings.Contains(out, leak) {
			t.Fatalf("log leaks %q: %s", leak, out)
		}
	}
	got := decodeSingle(t, &buf)
	group, ok := got["creds"].(map[string]any)
	if !ok {
		t.Fatalf("group missing: %#v", got)
	}
	if group["token"] != "***" {
		t.Fatalf("group token = %v, want ***", group["token"])
	}
}

func TestRedactingHandlerLeavesOrdinaryValues(t *testing.T) {
	var buf bytes.Buffer
	log := newTestLogger(&buf)
	log.Info("starting", "addr", ":9100", "dataset", "compare-data-8m", "reason", "snapshot ready")

	got := decodeSingle(t, &buf)
	if got["msg"] != "starting" || got["addr"] != ":9100" || got["dataset"] != "compare-data-8m" {
		t.Fatalf("ordinary values changed: %#v", got)
	}
}
