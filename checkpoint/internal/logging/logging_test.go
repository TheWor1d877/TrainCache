package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

// captureOutput 在用例期间替换包级输出，返回捕获缓冲。
func captureOutput(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := output
	output = &buf
	t.Cleanup(func() { output = old })
	return &buf
}

// decodeOne 解析单行 JSON 输出。
func decodeOne(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("输出不是单个 JSON 对象: %v: %q", err, buf.String())
	}
	return got
}

func TestNewEmitsJSONWithComponentVersion(t *testing.T) {
	buf := captureOutput(t)
	oldDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldDefault) })

	log := New("checkpointd", "v0.4.0", "debug")
	log.Info("starting")

	got := decodeOne(t, buf)
	if got["component"] != "checkpointd" {
		t.Fatalf("component = %v, want checkpointd", got["component"])
	}
	if got["version"] != "v0.4.0" {
		t.Fatalf("version = %v, want v0.4.0", got["version"])
	}
	if got["log_schema_version"] != float64(1) {
		t.Fatalf("log_schema_version = %v, want 1", got["log_schema_version"])
	}
	if slog.Default() != log {
		t.Fatalf("slog.Default() 未指向 New 返回的 logger")
	}
}

// TestNewRedactsAccessKey 验证处理器已接上 core/logging 的脱敏层。
func TestNewRedactsAccessKey(t *testing.T) {
	buf := captureOutput(t)
	oldDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldDefault) })

	log := New("checkpointd", "dev", "info")
	log.Info("backend", "accessKey", "AKIAIOSFODNN7EXAMPLE")

	got := decodeOne(t, buf)
	if got["accessKey"] == "AKIAIOSFODNN7EXAMPLE" {
		t.Fatalf("accessKey 未脱敏: %v", got["accessKey"])
	}
}
