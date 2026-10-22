package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewEmitsJSONWithComponentVersion(t *testing.T) {
	var buf bytes.Buffer
	old := output
	output = &buf
	t.Cleanup(func() { output = old })

	log := New("controller-manager", "v1.2.3", "debug")
	log.Info("starting", "addr", ":8081")

	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("output is not one JSON object: %v: %q", err, buf.String())
	}
	if got["component"] != "controller-manager" {
		t.Fatalf("component = %v, want controller-manager", got["component"])
	}
	if got["version"] != "v1.2.3" {
		t.Fatalf("version = %v, want v1.2.3", got["version"])
	}
	if got["log_schema_version"] != float64(1) {
		t.Fatalf("log_schema_version = %v (%T), want 1", got["log_schema_version"], got["log_schema_version"])
	}
	if got["addr"] != ":8081" {
		t.Fatalf("addr = %v, want :8081", got["addr"])
	}
	if got["msg"] != "starting" {
		t.Fatalf("msg = %v, want starting", got["msg"])
	}
}

func TestNewRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	old := output
	output = &buf
	t.Cleanup(func() { output = old })

	log := New("webhook", "test", "warn")
	log.Info("should be filtered")
	log.Warn("kept")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1 filtered warn line: %q", len(lines), buf.String())
	}
}

func TestNewSetsSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	oldOutput := output
	output = &buf
	oldDefault := slog.Default()
	t.Cleanup(func() {
		output = oldOutput
		slog.SetDefault(oldDefault)
	})

	log := New("controller-manager", "v2.0.0", "info")
	if slog.Default() != log {
		t.Fatalf("slog.Default() = %p, want logger returned by New (%p)", slog.Default(), log)
	}

	slog.Info("via default", "addr", ":8081")

	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("default output is not one JSON object: %v: %q", err, buf.String())
	}
	if got["component"] != "controller-manager" {
		t.Fatalf("component = %v, want controller-manager", got["component"])
	}
	if got["version"] != "v2.0.0" {
		t.Fatalf("version = %v, want v2.0.0", got["version"])
	}
	if got["addr"] != ":8081" {
		t.Fatalf("addr = %v, want :8081", got["addr"])
	}
}

func TestIntoFrom(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	ctx := Into(context.Background(), log)
	if got := From(ctx); got != log {
		t.Fatalf("From(Into(ctx)) = %p, want %p", got, log)
	}
	if got := From(context.Background()); got != slog.Default() {
		t.Fatalf("From(empty) = %p, want slog.Default()", got)
	}
}
