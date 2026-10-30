package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"traincache/controller/internal/config"
)

func TestLifecycleLogsStructuredFields(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil)).With("component", component, "version", version)
	cfg := &config.Config{HealthAddr: ":8081", Namespace: "default", ScoreAnnotation: "traincache.io/node-scores"}

	logStartup(log, cfg)
	logShutdown(log, cfg)

	lines := decodeJSONLines(t, &buf)
	if len(lines) != 3 {
		t.Fatalf("got %d log lines, want 3: %q", len(lines), buf.String())
	}
	for _, line := range lines {
		assertField(t, line, "component", component)
		assertField(t, line, "version", version)
		if line["addr"] == nil || line["addr"] == "" {
			t.Fatalf("line has no addr: %#v", line)
		}
	}
	assertField(t, lines[0], "msg", "config loaded")
	assertField(t, lines[1], "msg", "starting")
	assertField(t, lines[2], "msg", "shutting down")
}

func decodeJSONLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("invalid JSON log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func assertField(t *testing.T, m map[string]any, key, want string) {
	t.Helper()
	if got, _ := m[key].(string); got != want {
		t.Fatalf("%s = %q, want %q (line %#v)", key, got, want, m)
	}
}

// TestStartDebugServerDisabled:DebugAddr 为空时端点不启动,调用立刻返回。
func TestStartDebugServerDisabled(t *testing.T) {
	done := make(chan struct{})
	go func() {
		startDebugServer(context.Background(), &config.Config{}, slog.Default())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("startDebugServer blocked with empty DebugAddr; the endpoint must stay off")
	}
}
