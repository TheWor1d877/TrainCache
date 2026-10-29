package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// resetLevel 把进程级级别恢复到 info,避免用例之间互相影响。
func resetLevel(t *testing.T) {
	t.Helper()
	Level().Set(slog.LevelInfo)
	t.Cleanup(func() { Level().Set(slog.LevelInfo) })
}

// captureDefault 把 slog.Default 临时指向缓冲区,用于断言审计日志。
func captureDefault(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
		ok   bool
	}{
		{"debug", slog.LevelDebug, true},
		{"DEBUG", slog.LevelDebug, true},
		{" info ", slog.LevelInfo, true},
		{"Warn", slog.LevelWarn, true},
		{"error", slog.LevelError, true},
		{"", slog.LevelInfo, false},
		{"verbose", slog.LevelInfo, false},
	}
	for _, tc := range cases {
		got, ok := ParseLevel(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ParseLevel(%q) = (%v, %v), want (%v, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestLevelHandler(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		body       string
		wantStatus int
		wantLevel  string
		wantAudit  bool
		wantOld    string
		wantNew    string
	}{
		{name: "get current", method: http.MethodGet, wantStatus: http.StatusOK, wantLevel: "info"},
		{name: "put debug", method: http.MethodPut, body: `{"level":"debug"}`, wantStatus: http.StatusOK, wantLevel: "debug", wantAudit: true, wantOld: "info", wantNew: "debug"},
		{name: "put upper case warn", method: http.MethodPut, body: `{"level":"WARN"}`, wantStatus: http.StatusOK, wantLevel: "warn", wantAudit: true, wantOld: "info", wantNew: "warn"},
		{name: "put unknown level", method: http.MethodPut, body: `{"level":"verbose"}`, wantStatus: http.StatusBadRequest, wantLevel: "info"},
		{name: "put empty level", method: http.MethodPut, body: `{"level":""}`, wantStatus: http.StatusBadRequest, wantLevel: "info"},
		{name: "put malformed body", method: http.MethodPut, body: `not json`, wantStatus: http.StatusBadRequest, wantLevel: "info"},
		{name: "post rejected", method: http.MethodPost, wantStatus: http.StatusMethodNotAllowed, wantLevel: "info"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetLevel(t)
			buf := captureDefault(t)
			h := NewLevelHandler("controller-manager")

			var body *strings.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			} else {
				body = strings.NewReader("")
			}
			req := httptest.NewRequest(tc.method, LevelPath, body)
			req.RemoteAddr = "127.0.0.1:54321"
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if got := levelName(Level().Level()); got != tc.wantLevel {
				t.Fatalf("level = %q, want %q", got, tc.wantLevel)
			}
			if tc.wantStatus == http.StatusOK {
				var resp struct {
					Level string `json:"level"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("response is not JSON: %v: %q", err, rec.Body.String())
				}
				if resp.Level != tc.wantLevel {
					t.Fatalf("response level = %q, want %q", resp.Level, tc.wantLevel)
				}
			}
			if tc.wantStatus == http.StatusMethodNotAllowed && rec.Header().Get("Allow") != "GET, PUT" {
				t.Fatalf("Allow = %q, want %q", rec.Header().Get("Allow"), "GET, PUT")
			}
			assertAudit(t, buf, tc.wantAudit, tc.wantOld, tc.wantNew)
		})
	}
}

func assertAudit(t *testing.T, buf *bytes.Buffer, want bool, old, new string) {
	t.Helper()
	text := strings.TrimSpace(buf.String())
	if !want {
		if text != "" {
			t.Fatalf("unexpected audit line: %q", text)
		}
		return
	}
	if text == "" {
		t.Fatal("missing audit line")
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(text), &got); err != nil {
		t.Fatalf("audit line is not JSON: %v: %q", err, text)
	}
	wants := map[string]string{
		"msg":         "log level changed",
		"component":   "controller-manager",
		"old":         old,
		"new":         new,
		"remote_addr": "127.0.0.1:54321",
		"level":       "INFO",
	}
	for k, v := range wants {
		if got[k] != v {
			t.Errorf("audit field %s = %v, want %q", k, got[k], v)
		}
	}
}

func TestLevelChangeAffectsExistingLogger(t *testing.T) {
	resetLevel(t)
	captureDefault(t)

	var buf bytes.Buffer
	log := NewRedactingHandler(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: Level()}))
	logger := slog.New(log)

	logger.Debug("before")
	if strings.Contains(buf.String(), "before") {
		t.Fatalf("debug line emitted at info level: %q", buf.String())
	}

	h := NewLevelHandler("agentd")
	req := httptest.NewRequest(http.MethodPut, LevelPath, strings.NewReader(`{"level":"debug"}`))
	h.ServeHTTP(httptest.NewRecorder(), req)

	buf.Reset()
	logger.Debug("after")
	if !strings.Contains(buf.String(), "after") {
		t.Fatalf("debug line missing after level change: %q", buf.String())
	}
}

func TestServeLevelDisabledReturnsImmediately(t *testing.T) {
	done := make(chan struct{})
	go func() {
		ServeLevel(context.Background(), "", "agentd", slog.Default())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ServeLevel with empty addr blocked; the endpoint must be off")
	}
}
