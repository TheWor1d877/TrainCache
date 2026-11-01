// Package logging 初始化网关的结构化日志（design 10.1 §7.1；10.2 D19）。
//
// 与 agent/internal/logging 同构：只做设级别、装处理器、注入
// component/version/log_schema_version 三件事；脱敏、采样与动态级别全部复用
// core/logging，不在此重写。
package logging

import (
	"io"
	"log/slog"
	"os"

	corelog "traincache/core/logging"
)

// output 是 JSON 输出目标。测试替换它捕获结构化输出；生产与 agentd 一样写 stderr。
var output io.Writer = os.Stderr

// logSchemaVersion 是公开日志字段的 schema 版本（docs/8.1 §3.1）；只增不改。
const logSchemaVersion = 1

// New 构造绑定 component 与 version 的 JSON logger，并设为 slog.Default，
// 使各包通过默认 logger 写出的日志带同样字段。
//
// level 接受 debug/info/warn/error（大小写不敏感）；未知值退化为 info，
// 日志配置永远不阻断启动。
func New(component, version, level string) *slog.Logger {
	// 级别存在 corelog 的进程级 LevelVar 上：启动时写入 env 值，运行中由
	// /debug/loglevel 修改，所有已构造的 handler 立即跟随。
	lvl, _ := corelog.ParseLevel(level)
	corelog.Level().Set(lvl)

	// 采样包在脱敏外层：先判断是否值得写，再格式化与脱敏（8.1 §3.3）。
	// 网关是数据面，沿用 agent 的 200 行/秒、突发 400 额度。
	sampler := corelog.NewAgentSampler(component)
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
