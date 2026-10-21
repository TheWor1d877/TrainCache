package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"traincache/controller/internal/config"
)

func TestLifecycleLogsStructuredFields(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil)).With("component", component, "version", version)
	cfg := &config.Config{WebhookAddr: ":9443", HealthAddr: ":8081", Namespace: "default"}

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

func TestParseListenAddr(t *testing.T) {
	cases := []struct {
		addr     string
		wantHost string
		wantPort int
		wantErr  bool
	}{
		{addr: ":9443", wantHost: "", wantPort: 9443},
		{addr: "127.0.0.1:19443", wantHost: "127.0.0.1", wantPort: 19443},
		{addr: "9443", wantErr: true},
		{addr: ":0", wantErr: true},
		{addr: ":99999", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.addr, func(t *testing.T) {
			host, port, err := parseListenAddr(tc.addr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseListenAddr(%q) = (%q, %d), want error", tc.addr, host, port)
				}
				return
			}
			if err != nil || host != tc.wantHost || port != tc.wantPort {
				t.Fatalf("parseListenAddr(%q) = (%q, %d, %v), want (%q, %d)",
					tc.addr, host, port, err, tc.wantHost, tc.wantPort)
			}
		})
	}
}

func TestManagerOptionsUseWebhookAndHealthAddresses(t *testing.T) {
	cfg := &config.Config{WebhookAddr: ":19443", HealthAddr: ":18081", MetricsAddr: ":19412"}
	opts, err := managerOptions(cfg, newScheme())
	if err != nil {
		t.Fatalf("managerOptions: %v", err)
	}
	if opts.HealthProbeBindAddress != ":18081" {
		t.Fatalf("HealthProbeBindAddress = %q", opts.HealthProbeBindAddress)
	}
	if opts.WebhookServer == nil {
		t.Fatal("WebhookServer is nil")
	}
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
