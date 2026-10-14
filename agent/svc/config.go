// Package svc 是服务化装配层:读配置、起 gRPC 服务、持有子模块实例、接探针
// (design §4:cmd → svc → 子模块 → core 单向无环)。
package svc

import (
	"fmt"
	"os"
	"strings"
	"time"

	"traincache/agent/nodeid"
)

// 校验等级取值(design §8:TRAINCACHE_VERIFY 透传 core verify_level)。
const (
	VerifyXXH3       = "xxh3"        // 默认:只验 xxh3
	VerifyXXH3SHA256 = "xxh3+sha256" // 新写块时同时算 sha256,读时双重校验
)

// 默认值(design §8;D5:gRPC 9410 / metrics 9411 非特权端口)。
const (
	defaultGRPCAddr    = ":9410"
	defaultMetricsAddr = ":9411"
	// defaultCacheRoot 是 CAS 根目录默认值。本里程碑无对应 env:design §8 未列,
	// 由 DaemonSet hostPath(/var/cache/traincache)固定(§8 部署要点)。
	defaultCacheRoot = "/var/cache/traincache"
	// defaultOrdersDir 是 ordersource ring 发现目录默认值(design §6.2 路径;
	// 两侧 Pod 同路径挂载 hostPath /run/traincache,decisions D22)。
	defaultOrdersDir = "/run/traincache/orders"
)

// HeatConfig 是热度与磁盘触发线参数(D14:N=24h、M=30min、T=24h(因子 0.5)、k=80%、Δ=5%、M=90%)。
// 本里程碑只落结构与默认值;TRAINCACHE_HEAT_* 的 env 解析延后 M5/M6。
type HeatConfig struct {
	DecayAge            time.Duration // N:超过该年龄的记录直接衰减热度,默认 24h
	DumpInterval        time.Duration // M:每该间隔 dump 一次 heatmap.json,默认 30min
	DecayInterval       time.Duration // T:每隔该间隔全体乘衰减因子,默认 24h
	DecayFactor         float64       // 衰减因子,默认 0.5
	DiskTriggerPercent  float64       // k:磁盘利用率触发线(%),默认 80
	DiskDeltaPercent    float64       // Δ:触发后回落幅度(%):Sweep 至 k-Δ,默认 5
	DiskCriticalPercent float64       // M:激进 Sweep + 告警触发线(%),默认 90
}

// Config 是 Agent 运行配置(design §8;D5/D8/D14)。
type Config struct {
	NodeName         nodeid.ID // NODE_NAME(D8:必填、权威节点标识)
	GRPCAddr         string    // TRAINCACHE_GRPC_ADDR,默认 :9410(D5)
	MetricsAddr      string    // TRAINCACHE_METRICS_ADDR,默认 :9411(D5;M1 未起服务,M6 落地)
	Verify           string    // TRAINCACHE_VERIFY:xxh3 | xxh3+sha256,默认 xxh3
	DatasetWhitelist []string  // TRAINCACHE_DATASET_WHITELIST,逗号分隔(§5.8 基数控制)
	CacheRoot        string    // CAS 根目录,默认 /var/cache/traincache(本里程碑无 env)
	OrdersDir        string    // TRAINCACHE_ORDERS_DIR,默认 /run/traincache/orders(§6.2 ring 发现目录)
	Heat             HeatConfig
}

// Load 从环境变量加载配置并校验(design §8):
// NODE_NAME 必填非空(D8);TRAINCACHE_VERIFY 仅接受 xxh3 / xxh3+sha256。
func Load() (*Config, error) {
	node, err := nodeid.FromEnv()
	if err != nil {
		return nil, err
	}

	verify := strings.TrimSpace(os.Getenv("TRAINCACHE_VERIFY"))
	if verify == "" {
		verify = VerifyXXH3
	}
	if verify != VerifyXXH3 && verify != VerifyXXH3SHA256 {
		return nil, fmt.Errorf("svc: TRAINCACHE_VERIFY=%q 非法(仅支持 %q / %q)", verify, VerifyXXH3, VerifyXXH3SHA256)
	}

	return &Config{
		NodeName:         node,
		GRPCAddr:         envOr("TRAINCACHE_GRPC_ADDR", defaultGRPCAddr),
		MetricsAddr:      envOr("TRAINCACHE_METRICS_ADDR", defaultMetricsAddr),
		Verify:           verify,
		DatasetWhitelist: splitList(os.Getenv("TRAINCACHE_DATASET_WHITELIST")),
		CacheRoot:        defaultCacheRoot,
		OrdersDir:        envOr("TRAINCACHE_ORDERS_DIR", defaultOrdersDir),
		Heat:             defaultHeatConfig(), // D14 默认值;env 覆盖延后 M5/M6
	}, nil
}

// defaultHeatConfig 返回 D14 默认六值。
func defaultHeatConfig() HeatConfig {
	return HeatConfig{
		DecayAge:            24 * time.Hour,
		DumpInterval:        30 * time.Minute,
		DecayInterval:       24 * time.Hour,
		DecayFactor:         0.5,
		DiskTriggerPercent:  80,
		DiskDeltaPercent:    5,
		DiskCriticalPercent: 90,
	}
}

// envOr 返回环境变量值;未设置或全空白时返回默认值。
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// splitList 逗号切分、TrimSpace、丢空项(TRAINCACHE_DATASET_WHITELIST 解析口径)。
func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
