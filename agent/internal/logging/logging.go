// Package logging initializes the agent structured logger.
//
// It intentionally exposes only New, Into and From. The handler is the
// standard library slog JSONHandler; no third-party logging dependency is
// allowed.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// output is the JSON destination. Tests replace it to capture structured
// output; production always writes to stderr like agentd.
var output io.Writer = os.Stderr

// logSchemaVersion is the public log field schema version (docs/9.1 §3.1).
// It only increases; never change the meaning of an existing value.
const logSchemaVersion = 1

// New builds a JSON logger bound to component and version, and installs it
// as slog.Default so that packages logging through the default logger emit
// the same structured JSON.
//
// level accepts debug, info, warn or error (case-insensitive). An unknown
// level degrades to info so that logging can never prevent startup.
func New(component, version, level string) *slog.Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{Level: parseLevel(level)})
	logger := slog.New(handler).With(
		slog.String("component", component),
		slog.String("version", version),
		slog.Int("log_schema_version", logSchemaVersion),
	)
	slog.SetDefault(logger)
	return logger
}

// Into stores l in ctx for From.
func Into(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// From returns the logger stored by Into, or slog.Default if absent.
func From(ctx context.Context) *slog.Logger {
	if ctx != nil {
		if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
			return l
		}
	}
	return slog.Default()
}

type loggerKey struct{}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
