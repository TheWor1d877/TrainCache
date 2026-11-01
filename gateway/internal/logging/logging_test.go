package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
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

	log := New("gateway", "v0.3.0", "debug")
	log.Info("starting", "listen", ":9413")

	got := decodeOne(t, buf)
	if got["component"] != "gateway" {
		t.Fatalf("component = %v, want gateway", got["component"])
	}
	if got["version"] != "v0.3.0" {
		t.Fatalf("version = %v, want v0.3.0", got["version"])
	}
	if got["log_schema_version"] != float64(1) {
		t.Fatalf("log_schema_version = %v (%T), want 1", got["log_schema_version"], got["log_schema_version"])
	}
	if got["listen"] != ":9413" {
		t.Fatalf("listen = %v, want :9413", got["listen"])
	}
	if slog.Default() != log {
		t.Fatalf("slog.Default() = %p, want New 返回的 logger (%p)", slog.Default(), log)
	}
}

// TestNewRedactsAccessKey 用真实会被 core/logging 脱敏的键（accessKey）验证
// New 的 handler 已接上脱敏层。
func TestNewRedactsAccessKey(t *testing.T) {
	const accessKey = "AKIAIOSFODNN7EXAMPLE"
	buf := captureOutput(t)

	log := New("gateway", "v0.3.0", "info")
	log.Error("源站请求失败", "err", errors.New("s3 get failed: accessKey="+accessKey))

	out := buf.String()
	if strings.Contains(out, accessKey) {
		t.Fatalf("日志泄漏凭据 %q: %s", accessKey, out)
	}
	if !strings.Contains(out, "***") {
		t.Fatalf("日志未出现脱敏哨兵: %s", out)
	}
	got := decodeOne(t, buf)
	if got["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", got["level"])
	}
	if !strings.Contains(got["err"].(string), "***") {
		t.Fatalf("err 未脱敏: %v", got["err"])
	}
}

// TestNewUnknownLevelDegradesToInfo 覆盖非法级别退化（design 10.1 §5.3）。
func TestNewUnknownLevelDegradesToInfo(t *testing.T) {
	buf := captureOutput(t)

	log := New("gateway", "dev", "TRACE")
	log.Debug("filtered")
	log.Info("kept")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1: %q", len(lines), buf.String())
	}
	if got := decodeOne(t, buf); got["msg"] != "kept" {
		t.Fatalf("msg = %v, want kept", got["msg"])
	}
}
