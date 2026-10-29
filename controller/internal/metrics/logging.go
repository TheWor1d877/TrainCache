package metrics

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	corelog "traincache/core/logging"
)

// logDroppedName is the log sampling/limiting drop counter family (docs/8.1 §3.3).
const logDroppedName = "traincache_log_dropped_total"

// logDroppedLevels 是 level 标签的固定取值,顺序与 Sampler 的四档计数一致。
var logDroppedLevels = [4]string{"DEBUG", "INFO", "WARN", "ERROR"}

var logDroppedLevelValues = [4]slog.Level{
	slog.LevelDebug,
	slog.LevelInfo,
	slog.LevelWarn,
	slog.LevelError,
}

// LogDroppedCollector 在 scrape 时读取 Sampler 的 atomic 丢弃计数,
// 暴露 traincache_log_dropped_total{component,level}。
type LogDroppedCollector struct {
	sampler *corelog.Sampler
	desc    *prometheus.Desc
}

// NewLogDroppedCollector 构造采集器;sampler 为 nil 时 Collect 不输出任何序列。
func NewLogDroppedCollector(sampler *corelog.Sampler) *LogDroppedCollector {
	return &LogDroppedCollector{
		sampler: sampler,
		desc: prometheus.NewDesc(
			logDroppedName,
			"被采样、去重或令牌桶丢弃的日志条数(component,level;8.1 §3.3)",
			[]string{"component", "level"},
			nil,
		),
	}
}

func (c *LogDroppedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *LogDroppedCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil || c.sampler == nil {
		return
	}
	component := c.sampler.Component()
	for i, level := range logDroppedLevelValues {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.CounterValue,
			float64(c.sampler.Dropped(component, level)),
			component,
			logDroppedLevels[i],
		)
	}
}
