// Package logging 初始化 checkpointd 的结构化日志（design 10.1「可观测出口」；docs/8.1 §3）。
//
// 与 gateway/internal/logging 同构：只做设级别、装处理器、注入
// component/version/log_schema_version 三件事；脱敏与采样全部复用
// core/logging，不在此重写。
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"traincache/checkpoint/internal/obs"
	corelog "traincache/core/logging"
)

// output 是 JSON 输出目标。测试替换它捕获结构化输出；生产写 stderr，
// 与 agentd、gatewayd 保持一致。
var output io.Writer = os.Stderr

// envJob 由准入注入（docs/10.2 D26），取值是 owner Job 名；裸 Pod 时为空串。
const envJob = "JOB"

// logSchemaVersion 是公开日志字段的 schema 版本（docs/8.1 §3.1）；只增不改。
const logSchemaVersion = 1

// New 构造绑定 component、job 与 version 的 JSON logger，并设为 slog.Default。
// level 取 debug/info/warn/error（大小写不敏感），未知值退化为 info；
// 启动阶段的级别合法性由 config 负责，这里不阻断启动。
func New(component, version, level string) *slog.Logger {
	lvl, _ := corelog.ParseLevel(level)
	corelog.Level().Set(lvl)

	// 采样包在脱敏外层：先判断是否值得写，再格式化与脱敏（8.1 §3.3）。
	sampler := corelog.NewAgentSampler(component)
	handler := corelog.NewSamplingHandler(
		corelog.NewRedactingHandler(
			slog.NewJSONHandler(output, &slog.HandlerOptions{Level: corelog.Level()}),
		),
		sampler,
	)
	// job 按 logger 属性一次性挂上，全部日志行才能带上它（D26）；
	// 空串也要挂，字段存在性是日志侧按 Job 名对账的前提。
	// 去空白与 sidecar 的 envString 同口径，两处读到的 Job 才是同一个值。
	logger := slog.New(handler).With(
		slog.String("component", component),
		obs.Job(strings.TrimSpace(os.Getenv(envJob))),
		slog.String("version", version),
		slog.Int("log_schema_version", logSchemaVersion),
	)
	slog.SetDefault(logger)
	return logger
}
