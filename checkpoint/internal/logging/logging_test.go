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

// decodeLines 按行解析 JSON 输出。
func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var got map[string]any
		if err := json.Unmarshal(line, &got); err != nil {
			t.Fatalf("第 %d 行不是 JSON 对象: %v: %q", len(lines)+1, err, line)
		}
		lines = append(lines, got)
	}
	return lines
}

// TestJobOnEveryLine 锁住 D26 的注入方式：job 作为 logger 公共字段挂一次，
// 之后每一行都带上它，调用点不需要各写一遍。
func TestJobOnEveryLine(t *testing.T) {
	t.Setenv("JOB", "train-job-7f9c")
	buf := captureOutput(t)
	oldDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldDefault) })

	log := New("checkpointd", "dev", "debug")
	log.Info("第一条")
	log.Warn("第二条")
	slog.Default().Error("第三条")

	lines := decodeLines(t, buf)
	if len(lines) != 3 {
		t.Fatalf("行数 = %d, want 3: %q", len(lines), buf.String())
	}
	for i, line := range lines {
		if line["job"] != "train-job-7f9c" {
			t.Fatalf("第 %d 行 job = %v, want train-job-7f9c", i+1, line["job"])
		}
	}
}

// TestEmptyJobFieldIsPresent 锁住 D26 的另一半：JOB 未设置或为空串时
// job 仍以空串出现，日志一行都不能少——字段存在性是按 Job 名对账的前提。
func TestEmptyJobFieldIsPresent(t *testing.T) {
	for name, value := range map[string]string{"未设置": "", "空串": "   "} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("JOB", value)
			buf := captureOutput(t)
			oldDefault := slog.Default()
			t.Cleanup(func() { slog.SetDefault(oldDefault) })

			log := New("checkpointd", "dev", "debug")
			log.Info("启动")
			log.Warn("退出")

			lines := decodeLines(t, buf)
			if len(lines) != 2 {
				t.Fatalf("行数 = %d, want 2: %q", len(lines), buf.String())
			}
			for i, line := range lines {
				raw, ok := line["job"]
				if !ok {
					t.Fatalf("第 %d 行缺 job 字段: %v", i+1, line)
				}
				if raw != "" {
					t.Fatalf("第 %d 行 job = %v, want 空串", i+1, raw)
				}
			}
		})
	}
}
