// Package svc 是服务化装配层:读配置、起 gRPC 服务、持有子模块实例、接探针
// (design §4:cmd → svc → 子模块 → core 单向无环)。
package svc

import (
	"fmt"
	"os"
	"strconv"
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
	// defaultCacheRoot 是 CAS 根目录默认值。DaemonSet 部署仍用默认值(design §8 部署要点:
	// hostPath /var/cache/traincache 固定);TRAINCACHE_CACHE_ROOT env(7.3 D38)仅覆盖本机
	// 进程级演练与非标准缓存根场景,未设置时回退此默认。
	defaultCacheRoot = "/var/cache/traincache"
	// defaultOrdersDir 是 ordersource ring 发现目录默认值(design §6.2 路径;
	// 两侧 Pod 同路径挂载 hostPath /run/traincache,decisions D22)。
	defaultOrdersDir = "/run/traincache/orders"
	// defaultObjstoreEndpoint 是对象存储端点默认值(D26:进程内 MinIO 边车约定)。
	defaultObjstoreEndpoint = "127.0.0.1:9000"
	// defaultObjstoreBucket 是对象存储桶默认值(D26;v1 单桶)。
	defaultObjstoreBucket = "traincache"

	// envReadStallMinWaitMS 是 read_stall 入直方图最小等待的配置项(D47/Q15;毫秒)。
	envReadStallMinWaitMS = "TRAINCACHE_READ_STALL_MIN_WAIT_MS"

	// defaultIndexRefreshInterval 是前缀感知快照跟随的默认刷新周期(7.1 §5.9)。
	defaultIndexRefreshInterval = 5 * time.Second
	// envIndexRefreshInterval 覆盖快照跟随刷新周期(time.ParseDuration)。
	envIndexRefreshInterval = "TRAINCACHE_INDEX_REFRESH_INTERVAL"
	// envObjstoreProbeConditionalWrite 控制装配期条件写探测(默认关闭;7.1 §5.12)。
	envObjstoreProbeConditionalWrite = "TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE"
)

// HeatConfig 是热度与磁盘触发线参数(D14:N=24h、M=30min、T=24h(因子 0.5)、k=80%、Δ=5%、M=90%;
// D30 增 β=0.15、T_R=6h、校准窗口 100/阈值 0.8,并冻结 11 个 TRAINCACHE_HEAT_* env)。
type HeatConfig struct {
	DecayAge            time.Duration // N:超过该年龄的记录直接衰减热度,默认 24h
	DumpInterval        time.Duration // M:每该间隔 dump 一次 heatmap.json,默认 30min
	DecayInterval       time.Duration // T:每隔该间隔全体乘衰减因子,默认 24h
	DecayFactor         float64       // 衰减因子,默认 0.5((0,1])
	HeatOpenBeta        float64       // β:单次打开热度增量系数(§5.5),默认 0.15((0,1])
	HeatTouchHalfLife   time.Duration // T_R:新鲜度 R 半衰期(§5.5),默认 6h
	CalibrateWindow     int           // 交叉校准滑动窗口(次打开),默认 100(>0)
	CalibrateThreshold  float64       // 匹配率低于该值触发重对齐,默认 0.8((0,1])
	DiskTriggerPercent  float64       // k:磁盘利用率触发线(%),默认 80(0..100)
	DiskDeltaPercent    float64       // Δ:触发后回落幅度(%):Sweep 至 k-Δ,默认 5(0..100)
	DiskCriticalPercent float64       // M:激进 Sweep + 告警触发线(%),默认 90(0..100)
}

// TRAINCACHE_HEAT_* env 名(D30 冻结 11 个;duration 走 time.ParseDuration,百分比 0..100)。
const (
	envHeatDecayAge            = "TRAINCACHE_HEAT_DECAY_AGE"
	envHeatDumpInterval        = "TRAINCACHE_HEAT_DUMP_INTERVAL"
	envHeatDecayInterval       = "TRAINCACHE_HEAT_DECAY_INTERVAL"
	envHeatDecayFactor         = "TRAINCACHE_HEAT_DECAY_FACTOR"
	envHeatOpenBeta            = "TRAINCACHE_HEAT_OPEN_BETA"
	envHeatTouchHalfLife       = "TRAINCACHE_HEAT_TOUCH_HALF_LIFE"
	envHeatCalibrateWindow     = "TRAINCACHE_HEAT_CALIBRATE_WINDOW"
	envHeatCalibrateThreshold  = "TRAINCACHE_HEAT_CALIBRATE_THRESHOLD"
	envHeatDiskTriggerPercent  = "TRAINCACHE_HEAT_DISK_TRIGGER_PERCENT"
	envHeatDiskDeltaPercent    = "TRAINCACHE_HEAT_DISK_DELTA_PERCENT"
	envHeatDiskCriticalPercent = "TRAINCACHE_HEAT_DISK_CRITICAL_PERCENT"
)

// Config 是 Agent 运行配置(design §8;D5/D8/D14/D26)。
type Config struct {
	NodeName         nodeid.ID // NODE_NAME(D8:必填、权威节点标识)
	GRPCAddr         string    // TRAINCACHE_GRPC_ADDR,默认 :9410(D5)
	MetricsAddr      string    // TRAINCACHE_METRICS_ADDR,默认 :9411(D5;M1 未起服务,M6 落地)
	Verify           string    // TRAINCACHE_VERIFY:xxh3 | xxh3+sha256,默认 xxh3
	DatasetWhitelist []string  // TRAINCACHE_DATASET_WHITELIST,逗号分隔(§5.8 基数控制)
	CacheRoot        string    // TRAINCACHE_CACHE_ROOT,默认 /var/cache/traincache(7.3 D38:本机演练/非标准根)
	OrdersDir        string    // TRAINCACHE_ORDERS_DIR,默认 /run/traincache/orders(§6.2 ring 发现目录)
	// 对象存储接入(D26;经 core objectstore.NewMinIOStore 构造)。
	ObjstoreEndpoint  string // TRAINCACHE_OBJSTORE_ENDPOINT,默认 127.0.0.1:9000
	ObjstoreAccessKey string // TRAINCACHE_OBJSTORE_ACCESS_KEY,默认空(匿名/无鉴权)
	ObjstoreSecretKey string // TRAINCACHE_OBJSTORE_SECRET_KEY,默认空
	ObjstoreBucket    string // TRAINCACHE_OBJSTORE_BUCKET,默认 traincache(v1 单桶)
	ObjstoreSecure    bool   // TRAINCACHE_OBJSTORE_SECURE,默认 false("1"/"true"/"yes" 为真)
	Heat              HeatConfig
	// ReadStallMinWaitMS 是 read_stall 入直方图的最小等待毫秒(D47/Q15;默认 100,
	// TRAINCACHE_READ_STALL_MIN_WAIT_MS 覆盖,需 >0)。
	ReadStallMinWaitMS int64
	// IndexRefreshInterval 是快照跟随刷新周期(7.1 §5.9;默认 5s,
	// TRAINCACHE_INDEX_REFRESH_INTERVAL 覆盖)。
	IndexRefreshInterval time.Duration
	// ObjstoreProbeConditionalWrite 开启装配期条件写探测(7.1 §5.12;默认 false,
	// TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE 覆盖,口径同 envBool)。
	ObjstoreProbeConditionalWrite bool
}

// indexRefreshInterval 返回生效的快照跟随刷新周期(未配置/非正 → 默认 5s;
// Config 直接构造的测试与零值场景同样适用)。
func (c Config) indexRefreshInterval() time.Duration {
	if c.IndexRefreshInterval > 0 {
		return c.IndexRefreshInterval
	}
	return defaultIndexRefreshInterval
}

// Load 从环境变量加载配置并校验(design §8):
// NODE_NAME 必填非空(D8);TRAINCACHE_VERIFY 仅接受 xxh3 / xxh3+sha256;
// TRAINCACHE_HEAT_* 非法取值报错(D30:duration 走 time.ParseDuration,百分比 0..100)。
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

	heat, err := loadHeatConfig()
	if err != nil {
		return nil, err
	}
	readStallMinWaitMS, err := envPositiveInt64(envReadStallMinWaitMS, stallMinWaitMS)
	if err != nil {
		return nil, err
	}
	indexRefreshInterval, err := envDuration(envIndexRefreshInterval, defaultIndexRefreshInterval)
	if err != nil {
		return nil, err
	}

	return &Config{
		NodeName:                      node,
		GRPCAddr:                      envOr("TRAINCACHE_GRPC_ADDR", defaultGRPCAddr),
		MetricsAddr:                   envOr("TRAINCACHE_METRICS_ADDR", defaultMetricsAddr),
		Verify:                        verify,
		DatasetWhitelist:              splitList(os.Getenv("TRAINCACHE_DATASET_WHITELIST")),
		CacheRoot:                     envOr("TRAINCACHE_CACHE_ROOT", defaultCacheRoot),
		OrdersDir:                     envOr("TRAINCACHE_ORDERS_DIR", defaultOrdersDir),
		ObjstoreEndpoint:              envOr("TRAINCACHE_OBJSTORE_ENDPOINT", defaultObjstoreEndpoint),
		ObjstoreAccessKey:             envOr("TRAINCACHE_OBJSTORE_ACCESS_KEY", ""),
		ObjstoreSecretKey:             envOr("TRAINCACHE_OBJSTORE_SECRET_KEY", ""),
		ObjstoreBucket:                envOr("TRAINCACHE_OBJSTORE_BUCKET", defaultObjstoreBucket),
		ObjstoreSecure:                envBool("TRAINCACHE_OBJSTORE_SECURE"),
		Heat:                          heat,
		ReadStallMinWaitMS:            readStallMinWaitMS,
		IndexRefreshInterval:          indexRefreshInterval,
		ObjstoreProbeConditionalWrite: envBool(envObjstoreProbeConditionalWrite),
	}, nil
}

// defaultHeatConfig 返回 D14/D30 默认十一值。
func defaultHeatConfig() HeatConfig {
	return HeatConfig{
		DecayAge:            24 * time.Hour,
		DumpInterval:        30 * time.Minute,
		DecayInterval:       24 * time.Hour,
		DecayFactor:         0.5,
		HeatOpenBeta:        0.15,
		HeatTouchHalfLife:   6 * time.Hour,
		CalibrateWindow:     100,
		CalibrateThreshold:  0.8,
		DiskTriggerPercent:  80,
		DiskDeltaPercent:    5,
		DiskCriticalPercent: 90,
	}
}

// loadHeatConfig 以默认十一值为基底逐项读取 TRAINCACHE_HEAT_* env(D30):
// 未设置/全空白 → 默认;非法取值 → 错误(与 TRAINCACHE_VERIFY 同风格,启动即拦)。
func loadHeatConfig() (HeatConfig, error) {
	cfg := defaultHeatConfig()
	var err error
	setD := func(dst *time.Duration, key string) bool {
		*dst, err = envDuration(key, *dst)
		return err == nil
	}
	setR := func(dst *float64, key string) bool {
		*dst, err = envRatio(key, *dst)
		return err == nil
	}
	setP := func(dst *float64, key string) bool {
		*dst, err = envPercent(key, *dst)
		return err == nil
	}

	if !setD(&cfg.DecayAge, envHeatDecayAge) ||
		!setD(&cfg.DumpInterval, envHeatDumpInterval) ||
		!setD(&cfg.DecayInterval, envHeatDecayInterval) ||
		!setR(&cfg.DecayFactor, envHeatDecayFactor) ||
		!setR(&cfg.HeatOpenBeta, envHeatOpenBeta) ||
		!setD(&cfg.HeatTouchHalfLife, envHeatTouchHalfLife) ||
		!setR(&cfg.CalibrateThreshold, envHeatCalibrateThreshold) ||
		!setP(&cfg.DiskTriggerPercent, envHeatDiskTriggerPercent) ||
		!setP(&cfg.DiskDeltaPercent, envHeatDiskDeltaPercent) ||
		!setP(&cfg.DiskCriticalPercent, envHeatDiskCriticalPercent) {
		return cfg, err
	}
	if cfg.CalibrateWindow, err = envPositiveInt(envHeatCalibrateWindow, cfg.CalibrateWindow); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// envOr 返回环境变量值;未设置或全空白时返回默认值。
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// envBool 解析布尔 env(D26 口径:TrimSpace 后 "1"/"true"/"yes",大小写不敏感,为真;
// 未设置或其余取值一律为假,不报错——布尔 env 的错误容忍度高于端口/等级类)。
func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes":
		return true
	}
	return false
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

// envDuration 解析 duration env(time.ParseDuration;如 24h / 30m / 1m30s)。
// 未设置或全空白 → 默认值;解析失败或 ≤0 → 错误。
func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("svc: %s=%q 非法(需 time.ParseDuration 格式,如 24h/30m): %w", key, v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("svc: %s=%q 非法(需 > 0)", key, v)
	}
	return d, nil
}

// envRatio 解析比例 env((0,1] 区间,如衰减因子/β/校准阈值)。
// 未设置或全空白 → 默认值;非法取值(含 NaN / 区间外)→ 错误。
func envRatio(key string, def float64) (float64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("svc: %s=%q 非法(需数值): %w", key, v, err)
	}
	if !(f > 0 && f <= 1) { // NaN 比较恒 false → 同样落入错误分支
		return 0, fmt.Errorf("svc: %s=%v 非法(需 0 < x ≤ 1)", key, f)
	}
	return f, nil
}

// envPercent 解析百分比 env(0..100,含端点;D30:k/Δ/M 三个触发线)。
// 未设置或全空白 → 默认值;非法取值(含 NaN / 区间外)→ 错误。
func envPercent(key string, def float64) (float64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("svc: %s=%q 非法(需数值): %w", key, v, err)
	}
	if !(f >= 0 && f <= 100) { // NaN 比较恒 false → 同样落入错误分支
		return 0, fmt.Errorf("svc: %s=%v 非法(需 0..100)", key, f)
	}
	return f, nil
}

// envPositiveInt64 解析正整数 env(如 read_stall 最小等待毫秒)。
// 未设置或全空白 → 默认值;非法取值或 ≤0 → 错误。
func envPositiveInt64(key string, def int64) (int64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("svc: %s=%q 非法(需整数): %w", key, v, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("svc: %s=%d 非法(需 > 0)", key, n)
	}
	return n, nil
}

// envPositiveInt 解析正整数 env(如校准滑动窗口)。
// 未设置或全空白 → 默认值;非法取值或 ≤0 → 错误。
func envPositiveInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("svc: %s=%q 非法(需整数): %w", key, v, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("svc: %s=%d 非法(需 > 0)", key, n)
	}
	return n, nil
}
