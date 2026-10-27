package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// captureOutput replaces the package output hook for the duration of a test.
func captureOutput(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := output
	output = &buf
	t.Cleanup(func() { output = old })
	return &buf
}

func decodeOne(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("output is not one JSON object: %v: %q", err, buf.String())
	}
	return got
}

func TestNewEmitsJSONWithComponentVersion(t *testing.T) {
	buf := captureOutput(t)

	log := New("agentd", "v1.2.3", "debug")
	log.Info("starting", "addr", ":9100")

	got := decodeOne(t, buf)
	if got["component"] != "agentd" {
		t.Fatalf("component = %v, want agentd", got["component"])
	}
	if got["version"] != "v1.2.3" {
		t.Fatalf("version = %v, want v1.2.3", got["version"])
	}
	if got["log_schema_version"] != float64(1) {
		t.Fatalf("log_schema_version = %v (%T), want 1", got["log_schema_version"], got["log_schema_version"])
	}
	if got["addr"] != ":9100" {
		t.Fatalf("addr = %v, want :9100", got["addr"])
	}
	if got["msg"] != "starting" {
		t.Fatalf("msg = %v, want starting", got["msg"])
	}
}

// TestNewRedactsSentinelCredentials 用哨兵凭据验证 New 的 handler 已接脱敏（8.1 §3.4）。
func TestNewRedactsSentinelCredentials(t *testing.T) {
	const (
		accessKey = "AGENT-ACCESS-KEY-SENTINEL"
		secretKey = "AGENT-SECRET-KEY-SENTINEL"
		signature = "AGENT-SIGNATURE-SENTINEL"
	)
	buf := captureOutput(t)

	log := New("agentd", "v1.2.3", "info")
	log.Error("object store request failed",
		"err", errors.New("accessKey="+accessKey+" secretKey="+secretKey),
		"url", "https://user:pass@minio:9000/bench-b/key?X-Amz-Signature="+signature,
	)

	out := buf.String()
	for _, leak := range []string{accessKey, secretKey, signature, "user:pass@"} {
		if strings.Contains(out, leak) {
			t.Fatalf("log leaks %q: %s", leak, out)
		}
	}
	got := decodeOne(t, buf)
	if got["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", got["level"])
	}
}

func TestNewSetsSlogDefault(t *testing.T) {
	buf := captureOutput(t)
	oldDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldDefault) })

	log := New("agentd", "v2.0.0", "info")
	if slog.Default() != log {
		t.Fatalf("slog.Default() = %p, want logger returned by New (%p)", slog.Default(), log)
	}

	slog.Info("via default", "addr", ":9100")

	got := decodeOne(t, buf)
	if got["component"] != "agentd" {
		t.Fatalf("component = %v, want agentd", got["component"])
	}
	if got["version"] != "v2.0.0" {
		t.Fatalf("version = %v, want v2.0.0", got["version"])
	}
	if got["addr"] != ":9100" {
		t.Fatalf("addr = %v, want :9100", got["addr"])
	}
}

func TestNewLevelCaseInsensitive(t *testing.T) {
	for _, level := range []string{"WARN", " warn ", "WaRn"} {
		t.Run(level, func(t *testing.T) {
			buf := captureOutput(t)

			log := New("agentd", "test", level)
			log.Info("should be filtered")
			log.Warn("kept")

			lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
			if len(lines) != 1 {
				t.Fatalf("got %d lines, want 1 filtered warn line: %q", len(lines), buf.String())
			}
		})
	}
}

func TestNewUnknownLevelDegradesToInfo(t *testing.T) {
	buf := captureOutput(t)

	log := New("agentd", "test", "verbose")
	log.Info("kept")

	got := decodeOne(t, buf)
	if got["msg"] != "kept" {
		t.Fatalf("msg = %v, want kept at info level", got["msg"])
	}
}

func TestIntoFrom(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	ctx := Into(context.Background(), log)
	if got := From(ctx); got != log {
		t.Fatalf("From(Into(ctx)) = %p, want %p", got, log)
	}
}

func TestFromFallsBackToDefault(t *testing.T) {
	if got := From(context.Background()); got != slog.Default() {
		t.Fatalf("From(empty) = %p, want slog.Default()", got)
	}
	if got := From(nil); got != slog.Default() {
		t.Fatalf("From(nil) = %p, want slog.Default()", got)
	}

	ctx := Into(context.Background(), nil)
	if got := From(ctx); got != slog.Default() {
		t.Fatalf("From(Into(ctx, nil)) = %p, want slog.Default()", got)
	}
}
