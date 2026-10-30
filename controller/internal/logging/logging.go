// Package logging initializes the control-plane structured logger.
//
// It intentionally exposes only New, Into and From. The handler is the
// standard library slog JSONHandler; no third-party logging dependency is
// allowed (D20).
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync/atomic"

	corelog "traincache/core/logging"
)

// output is the JSON destination. Tests replace it to capture structured
// output; production always writes to stderr like agentd.
var output io.Writer = os.Stderr

// logSchemaVersion is the public log field schema version (docs/8.1 §3.1).
// It only increases; never change the meaning of an existing value.
const logSchemaVersion = 1

// processSampler 保存 New 构造的进程采样器,供 metrics Collector 在 scrape 时
// 读取丢弃计数。只在启动期写入;用 atomic.Pointer 保证抓取侧无数据竞争。
var processSampler atomic.Pointer[corelog.Sampler]

// Sampler 返回 New 构造的进程采样器;尚未初始化时返回 nil。
func Sampler() *corelog.Sampler {
	return processSampler.Load()
}

// New builds a JSON logger bound to component and version, and installs it
// as slog.Default so that packages logging through the default logger emit
// the same structured JSON.
//
// level accepts debug, info, warn or error (case-insensitive). Config
// validation rejects invalid values before New is called; an unknown level
// degrades to info so that logging can never prevent startup.
func New(component, version, level string) *slog.Logger {
	// 级别存在 corelog 的进程级 LevelVar 上：启动时写入 env 值，运行中由
	// /debug/loglevel 修改，所有已构造的 handler 立即跟随。
	lvl, _ := corelog.ParseLevel(level)
	corelog.Level().Set(lvl)
	// 采样限流包在脱敏外层:先判断是否值得写,再格式化与脱敏(8.1 §3.3)。
	sampler := corelog.NewControlSampler(component)
	processSampler.Store(sampler)
	// 脱敏包在 JSON handler 外层：消息、字符串与 error 属性写出前统一处理，
	// 调用点不需要逐个调用 Redact（D32）。
	handler := corelog.NewSamplingHandler(
		corelog.NewRedactingHandler(
			slog.NewJSONHandler(output, &slog.HandlerOptions{Level: corelog.Level()}),
		),
		sampler,
	)
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
