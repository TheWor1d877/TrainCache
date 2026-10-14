package svc

import "traincache/agent/metrics"

// Agent 是 svc 装配产物:持有配置与各子模块实例(design §3:svc 是装配层)。
// M1 只有 metrics 骨架;M2 起增 ordersource 发现器(Orders),prefetch/collector/rescan
// 随各自里程碑接入。
type Agent struct {
	Cfg     Config
	Metrics *metrics.Registry
	Orders  *OrderDiscovery
}

// Assemble 构造并持有子模块实例(依赖方向 cmd → svc → 子模块 → core 单向无环,design §4)。
func Assemble(cfg Config) (*Agent, error) {
	ag := &Agent{
		Cfg:     cfg,
		Metrics: metrics.New(metrics.Config{DatasetWhitelist: cfg.DatasetWhitelist}),
	}
	// 发现循环由 agentd 在 signal ctx 下起 goroutine 运行(M2);默认 handler = 行级日志。
	ag.Orders = NewOrderDiscovery(cfg.OrdersDir, ag.Metrics, LogOrder)
	return ag, nil
}
