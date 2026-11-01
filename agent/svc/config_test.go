package svc

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// setBaseEnv 清空本里程碑涉及的全部 env(隔离宿主环境),用例再按需覆盖。
func setBaseEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"NODE_NAME",
		"TRAINCACHE_GRPC_ADDR",
		"TRAINCACHE_METRICS_ADDR",
		"TRAINCACHE_VERIFY",
		"TRAINCACHE_DATASET_WHITELIST",
		"TRAINCACHE_CACHE_ROOT",
		"TRAINCACHE_ORDERS_DIR",
		"TRAINCACHE_OBJSTORE_ENDPOINT",
		"TRAINCACHE_OBJSTORE_ACCESS_KEY",
		"TRAINCACHE_OBJSTORE_SECRET_KEY",
		"TRAINCACHE_OBJSTORE_BUCKET",
		"TRAINCACHE_OBJSTORE_SECURE",
		"TRAINCACHE_HEAT_DECAY_AGE",
		"TRAINCACHE_HEAT_DUMP_INTERVAL",
		"TRAINCACHE_HEAT_DECAY_INTERVAL",
		"TRAINCACHE_HEAT_DECAY_FACTOR",
		"TRAINCACHE_HEAT_OPEN_BETA",
		"TRAINCACHE_HEAT_TOUCH_HALF_LIFE",
		"TRAINCACHE_HEAT_CALIBRATE_WINDOW",
		"TRAINCACHE_HEAT_CALIBRATE_THRESHOLD",
		"TRAINCACHE_HEAT_DISK_TRIGGER_PERCENT",
		"TRAINCACHE_HEAT_DISK_DELTA_PERCENT",
		"TRAINCACHE_HEAT_DISK_CRITICAL_PERCENT",
		"TRAINCACHE_READ_STALL_MIN_WAIT_MS",
		"TRAINCACHE_READ_STALL_WINDOW_MS",
		"TRAINCACHE_INDEX_REFRESH_INTERVAL",
		"TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE",
		"TRAINCACHE_GATEWAY_ENDPOINT",
		"TRAINCACHE_GATEWAY_SECURE",
	} {
		t.Setenv(k, "")
	}
	// TRAINCACHE_DEBUG_ADDR 区分"未设置"与"显式空串",不能设成空串,直接清除。
	unsetEnv(t, "TRAINCACHE_DEBUG_ADDR")
}

// unsetEnv 删除环境变量,并在用例结束时恢复原值。
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	old, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

// wantDefaultHeat 是 D14/D30 默认十一值:N=24h、M=30min、T=24h(因子 0.5)、β=0.15、
// T_R=6h、校准窗口 100/阈值 0.8、k=80、Δ=5、M=90。
func wantDefaultHeat() HeatConfig {
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

func TestLoadDefaults(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-01")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.NodeName.String() != "worker-01" {
		t.Errorf("NodeName = %q, want worker-01", cfg.NodeName)
	}
	if cfg.GRPCAddr != ":9410" {
		t.Errorf("GRPCAddr = %q, want :9410(D5)", cfg.GRPCAddr)
	}
	if cfg.MetricsAddr != ":9411" {
		t.Errorf("MetricsAddr = %q, want :9411(D5)", cfg.MetricsAddr)
	}
	if cfg.Verify != "xxh3" {
		t.Errorf("Verify = %q, want xxh3(design §8 默认)", cfg.Verify)
	}
	if len(cfg.DatasetWhitelist) != 0 {
		t.Errorf("DatasetWhitelist = %q, want 空", cfg.DatasetWhitelist)
	}
	if cfg.CacheRoot != "/var/cache/traincache" {
		t.Errorf("CacheRoot = %q, want /var/cache/traincache(§8;TRAINCACHE_CACHE_ROOT 未设,D38)", cfg.CacheRoot)
	}
	if cfg.OrdersDir != "/run/traincache/orders" {
		t.Errorf("OrdersDir = %q, want /run/traincache/orders(§6.2)", cfg.OrdersDir)
	}
	if cfg.ObjstoreEndpoint != "127.0.0.1:9000" {
		t.Errorf("ObjstoreEndpoint = %q, want 127.0.0.1:9000(D26)", cfg.ObjstoreEndpoint)
	}
	if cfg.ObjstoreAccessKey != "" || cfg.ObjstoreSecretKey != "" {
		t.Errorf("Objstore 凭据默认值 = (%q, %q), want 均为空(D26)", cfg.ObjstoreAccessKey, cfg.ObjstoreSecretKey)
	}
	if cfg.ObjstoreBucket != "traincache" {
		t.Errorf("ObjstoreBucket = %q, want traincache(D26)", cfg.ObjstoreBucket)
	}
	if cfg.ObjstoreSecure {
		t.Error("ObjstoreSecure 默认值 = true, want false(D26)")
	}
	if want := wantDefaultHeat(); cfg.Heat != want {
		t.Errorf("Heat 默认值 = %+v, want %+v(D14)", cfg.Heat, want)
	}
	if cfg.ReadStallMinWaitMS != stallMinWaitMS {
		t.Errorf("ReadStallMinWaitMS = %d, want %d(D47 默认)", cfg.ReadStallMinWaitMS, stallMinWaitMS)
	}
	if cfg.ReadStallWindowMS != defaultReadStallWindowMS {
		t.Errorf("ReadStallWindowMS = %d, want %d(C3 默认)", cfg.ReadStallWindowMS, defaultReadStallWindowMS)
	}
	if cfg.IndexRefreshInterval != defaultIndexRefreshInterval {
		t.Errorf("IndexRefreshInterval = %v, want %v(6.1 §5.9 默认)", cfg.IndexRefreshInterval, defaultIndexRefreshInterval)
	}
	if cfg.ObjstoreProbeConditionalWrite {
		t.Error("ObjstoreProbeConditionalWrite 默认值 = true, want false(6.1 §5.12)")
	}
	if cfg.GatewayEndpoint != "" {
		t.Errorf("GatewayEndpoint 默认值 = %q, want 空(10.1 §5.3:全部直连)", cfg.GatewayEndpoint)
	}
	if cfg.GatewaySecure {
		t.Error("GatewaySecure 默认值 = true, want false(10.1 §5.3)")
	}
	if cfg.DebugAddr != defaultDebugAddr {
		t.Errorf("DebugAddr = %q, want %q", cfg.DebugAddr, defaultDebugAddr)
	}
}

// TestLoadDebugAddr 覆盖 /debug/loglevel 地址的默认、显式与关闭三种取值。
func TestLoadDebugAddr(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		set     bool
		want    string
		wantErr bool
	}{
		{name: "default when unset", want: defaultDebugAddr},
		{name: "explicit", set: true, value: "127.0.0.1:16060", want: "127.0.0.1:16060"},
		{name: "empty disables", set: true, value: "", want: ""},
		{name: "not loopback", set: true, value: "0.0.0.0:6060", wantErr: true},
		{name: "not host port", set: true, value: "6060", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("NODE_NAME", "worker-01")
			if tc.set {
				t.Setenv("TRAINCACHE_DEBUG_ADDR", tc.value)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Load() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.DebugAddr != tc.want {
				t.Fatalf("DebugAddr = %q, want %q", cfg.DebugAddr, tc.want)
			}
		})
	}
}

// TestLoadIndexRefreshAndProbeEnv:新增两个配置项的环境变量解析(6.1 §5.9/§5.12)。
func TestLoadIndexRefreshAndProbeEnv(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-01")
	t.Setenv("TRAINCACHE_INDEX_REFRESH_INTERVAL", "750ms")
	t.Setenv("TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE", "yes")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.IndexRefreshInterval != 750*time.Millisecond {
		t.Errorf("IndexRefreshInterval = %v, want 750ms", cfg.IndexRefreshInterval)
	}
	if !cfg.ObjstoreProbeConditionalWrite {
		t.Error("ObjstoreProbeConditionalWrite = false, want true(envBool yes)")
	}
}

func TestLoadEnvOverride(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-02")
	t.Setenv("TRAINCACHE_GRPC_ADDR", "127.0.0.1:19410")
	t.Setenv("TRAINCACHE_METRICS_ADDR", "127.0.0.1:19411")
	t.Setenv("TRAINCACHE_VERIFY", "xxh3+sha256")
	t.Setenv("TRAINCACHE_DATASET_WHITELIST", "b-track,a-track")
	t.Setenv("TRAINCACHE_CACHE_ROOT", "/tmp/tc-cache-root")
	t.Setenv("TRAINCACHE_ORDERS_DIR", "/tmp/tc-orders")
	t.Setenv("TRAINCACHE_OBJSTORE_ENDPOINT", "minio.internal:9001")
	t.Setenv("TRAINCACHE_OBJSTORE_ACCESS_KEY", "ak-test")
	t.Setenv("TRAINCACHE_OBJSTORE_SECRET_KEY", "sk-test")
	t.Setenv("TRAINCACHE_OBJSTORE_BUCKET", "tc-blocks")
	t.Setenv("TRAINCACHE_OBJSTORE_SECURE", "1")
	t.Setenv("TRAINCACHE_GATEWAY_ENDPOINT", "gateway.traincache.svc:9413")
	t.Setenv("TRAINCACHE_GATEWAY_SECURE", "yes")
	t.Setenv("TRAINCACHE_HEAT_DECAY_AGE", "48h")
	t.Setenv("TRAINCACHE_HEAT_DUMP_INTERVAL", "1m")
	t.Setenv("TRAINCACHE_HEAT_DECAY_INTERVAL", "12h")
	t.Setenv("TRAINCACHE_HEAT_DECAY_FACTOR", "0.25")
	t.Setenv("TRAINCACHE_HEAT_OPEN_BETA", "0.3")
	t.Setenv("TRAINCACHE_HEAT_TOUCH_HALF_LIFE", "90m")
	t.Setenv("TRAINCACHE_HEAT_CALIBRATE_WINDOW", "50")
	t.Setenv("TRAINCACHE_HEAT_CALIBRATE_THRESHOLD", "0.65")
	t.Setenv("TRAINCACHE_HEAT_DISK_TRIGGER_PERCENT", "70")
	t.Setenv("TRAINCACHE_HEAT_DISK_DELTA_PERCENT", "10")
	t.Setenv("TRAINCACHE_HEAT_DISK_CRITICAL_PERCENT", "95")
	t.Setenv("TRAINCACHE_READ_STALL_MIN_WAIT_MS", "50")
	t.Setenv("TRAINCACHE_READ_STALL_WINDOW_MS", "2500")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.NodeName.String() != "worker-02" {
		t.Errorf("NodeName = %q, want worker-02", cfg.NodeName)
	}
	if cfg.GRPCAddr != "127.0.0.1:19410" {
		t.Errorf("GRPCAddr = %q, want 127.0.0.1:19410", cfg.GRPCAddr)
	}
	if cfg.MetricsAddr != "127.0.0.1:19411" {
		t.Errorf("MetricsAddr = %q, want 127.0.0.1:19411", cfg.MetricsAddr)
	}
	if cfg.Verify != "xxh3+sha256" {
		t.Errorf("Verify = %q, want xxh3+sha256", cfg.Verify)
	}
	if !slices.Equal(cfg.DatasetWhitelist, []string{"b-track", "a-track"}) {
		t.Errorf("DatasetWhitelist = %q, want [b-track a-track]", cfg.DatasetWhitelist)
	}
	if cfg.CacheRoot != "/tmp/tc-cache-root" {
		t.Errorf("CacheRoot = %q, want /tmp/tc-cache-root(TRAINCACHE_CACHE_ROOT 覆盖,D38)", cfg.CacheRoot)
	}
	if cfg.OrdersDir != "/tmp/tc-orders" {
		t.Errorf("OrdersDir = %q, want /tmp/tc-orders(TRAINCACHE_ORDERS_DIR 覆盖)", cfg.OrdersDir)
	}
	if cfg.ObjstoreEndpoint != "minio.internal:9001" {
		t.Errorf("ObjstoreEndpoint = %q, want minio.internal:9001(D26)", cfg.ObjstoreEndpoint)
	}
	if cfg.ObjstoreAccessKey != "ak-test" || cfg.ObjstoreSecretKey != "sk-test" {
		t.Errorf("Objstore 凭据 = (%q, %q), want (ak-test, sk-test)", cfg.ObjstoreAccessKey, cfg.ObjstoreSecretKey)
	}
	if cfg.ObjstoreBucket != "tc-blocks" {
		t.Errorf("ObjstoreBucket = %q, want tc-blocks", cfg.ObjstoreBucket)
	}
	if !cfg.ObjstoreSecure {
		t.Error("ObjstoreSecure = false, want true(TRAINCACHE_OBJSTORE_SECURE=1)")
	}
	if cfg.GatewayEndpoint != "gateway.traincache.svc:9413" {
		t.Errorf("GatewayEndpoint = %q, want gateway.traincache.svc:9413(10.1 §5.3)", cfg.GatewayEndpoint)
	}
	if !cfg.GatewaySecure {
		t.Error("GatewaySecure = false, want true(TRAINCACHE_GATEWAY_SECURE=yes)")
	}
	wantHeat := HeatConfig{
		DecayAge:            48 * time.Hour,
		DumpInterval:        time.Minute,
		DecayInterval:       12 * time.Hour,
		DecayFactor:         0.25,
		HeatOpenBeta:        0.3,
		HeatTouchHalfLife:   90 * time.Minute,
		CalibrateWindow:     50,
		CalibrateThreshold:  0.65,
		DiskTriggerPercent:  70,
		DiskDeltaPercent:    10,
		DiskCriticalPercent: 95,
	}
	if cfg.Heat != wantHeat {
		t.Errorf("Heat = %+v, want %+v(TRAINCACHE_HEAT_* 覆盖,D30)", cfg.Heat, wantHeat)
	}
	if cfg.ReadStallMinWaitMS != 50 {
		t.Errorf("ReadStallMinWaitMS = %d, want 50(TRAINCACHE_READ_STALL_MIN_WAIT_MS 覆盖)", cfg.ReadStallMinWaitMS)
	}
	if cfg.ReadStallWindowMS != 2500 {
		t.Errorf("ReadStallWindowMS = %d, want 2500(TRAINCACHE_READ_STALL_WINDOW_MS 覆盖)", cfg.ReadStallWindowMS)
	}
}

// TestLoadReadStallWindowMS:C3 关联窗口三种情况——未设置取默认 5000ms、
// 非法取值(非整数/非正)回退默认且不阻断启动、显式配置生效。
func TestLoadReadStallWindowMS(t *testing.T) {
	cases := []struct {
		name  string
		value string
		set   bool
		want  int64
	}{
		{name: "默认", want: defaultReadStallWindowMS},
		{name: "非法非整数", set: true, value: "abc", want: defaultReadStallWindowMS},
		{name: "非法非正", set: true, value: "0", want: defaultReadStallWindowMS},
		{name: "显式配置", set: true, value: "1500", want: 1500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("NODE_NAME", "worker-01")
			if tc.set {
				t.Setenv("TRAINCACHE_READ_STALL_WINDOW_MS", tc.value)
			}
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v(非法窗口不应阻断启动)", err)
			}
			if cfg.ReadStallWindowMS != tc.want {
				t.Fatalf("ReadStallWindowMS = %d, want %d", cfg.ReadStallWindowMS, tc.want)
			}
		})
	}
}

// TestLoadObjstoreSecureBool:TRAINCACHE_OBJSTORE_SECURE 真值口径(D26:"1"/"true"/"yes",
// 大小写不敏感);其余取值(含未设置、垃圾值)一律 false。
func TestLoadObjstoreSecureBool(t *testing.T) {
	truthy := []string{"1", "true", "TRUE", "Yes", " yes "}
	for _, v := range truthy {
		t.Run("truthy="+v, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("NODE_NAME", "worker-01")
			t.Setenv("TRAINCACHE_OBJSTORE_SECURE", v)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !cfg.ObjstoreSecure {
				t.Errorf("ObjstoreSecure = false, want true(env=%q)", v)
			}
		})
	}
	falsy := []string{"0", "false", "no", "on", "junk"}
	for _, v := range falsy {
		t.Run("falsy="+v, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("NODE_NAME", "worker-01")
			t.Setenv("TRAINCACHE_OBJSTORE_SECURE", v)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.ObjstoreSecure {
				t.Errorf("ObjstoreSecure = true, want false(env=%q)", v)
			}
		})
	}
}

// TestLoadGatewayEnv:网关入口与独立 TLS 开关(10.1 §5.3 / 10.2 D15/D16)。
// 缺省为空/false;TRAINCACHE_GATEWAY_SECURE 按 envBool 口径(1/true/yes);
// 两个 secure 互相独立;地址不做 scheme 校验(非法值不阻断启动)。
func TestLoadGatewayEnv(t *testing.T) {
	t.Run("默认关闭", func(t *testing.T) {
		setBaseEnv(t)
		t.Setenv("NODE_NAME", "worker-01")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.GatewayEndpoint != "" || cfg.GatewaySecure {
			t.Fatalf("GatewayEndpoint=%q GatewaySecure=%v, want 空/false", cfg.GatewayEndpoint, cfg.GatewaySecure)
		}
	})

	for _, v := range []string{"1", "true", "TRUE", "yes", " Yes "} {
		t.Run("secure="+v, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("NODE_NAME", "worker-01")
			t.Setenv("TRAINCACHE_GATEWAY_SECURE", v)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !cfg.GatewaySecure {
				t.Errorf("GatewaySecure = false, want true(env=%q)", v)
			}
		})
	}

	// 两个 secure 互相独立：只开源站 TLS 不影响网关，反之亦然。
	t.Run("objstore secure 不影响 gateway", func(t *testing.T) {
		setBaseEnv(t)
		t.Setenv("NODE_NAME", "worker-01")
		t.Setenv("TRAINCACHE_OBJSTORE_SECURE", "1")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !cfg.ObjstoreSecure || cfg.GatewaySecure {
			t.Fatalf("ObjstoreSecure=%v GatewaySecure=%v, want true/false(D16)", cfg.ObjstoreSecure, cfg.GatewaySecure)
		}
	})
	t.Run("gateway secure 不影响 objstore", func(t *testing.T) {
		setBaseEnv(t)
		t.Setenv("NODE_NAME", "worker-01")
		t.Setenv("TRAINCACHE_GATEWAY_SECURE", "1")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.ObjstoreSecure || !cfg.GatewaySecure {
			t.Fatalf("ObjstoreSecure=%v GatewaySecure=%v, want false/true(D16)", cfg.ObjstoreSecure, cfg.GatewaySecure)
		}
	})

	// 地址不做 scheme 校验：任意非空串都接受(启用与否由装配层判断)。
	t.Run("地址不校验 scheme", func(t *testing.T) {
		setBaseEnv(t)
		t.Setenv("NODE_NAME", "worker-01")
		t.Setenv("TRAINCACHE_GATEWAY_ENDPOINT", "http://gateway:9413")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v(地址不应做 scheme 校验)", err)
		}
		if cfg.GatewayEndpoint != "http://gateway:9413" {
			t.Errorf("GatewayEndpoint = %q, want 原样保留", cfg.GatewayEndpoint)
		}
	})
}

// TestLoadOrdersDirDefault:未设 TRAINCACHE_ORDERS_DIR → §6.2 默认发现目录。
func TestLoadOrdersDirDefault(t *testing.T) {
	setBaseEnv(t)
	os.Unsetenv("TRAINCACHE_ORDERS_DIR") // t.Setenv 的 cleanup 会恢复原值
	t.Setenv("NODE_NAME", "worker-01")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OrdersDir != "/run/traincache/orders" {
		t.Errorf("OrdersDir = %q, want /run/traincache/orders(未设 env 取默认)", cfg.OrdersDir)
	}
}

// TestLoadCacheRootBlankFallsBack：TRAINCACHE_CACHE_ROOT 全空白 → 回退默认
// /var/cache/traincache(与 OrdersDir 等字符串 env 同一 envOr 解析口径;D38)。
func TestLoadCacheRootBlankFallsBack(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-01")
	t.Setenv("TRAINCACHE_CACHE_ROOT", "   ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.CacheRoot != "/var/cache/traincache" {
		t.Errorf("CacheRoot = %q, want /var/cache/traincache(全空白回退默认,D38)", cfg.CacheRoot)
	}
}

func TestLoadWhitelistTrimAndDropEmpty(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-01")
	t.Setenv("TRAINCACHE_DATASET_WHITELIST", "b-track, a-track,,")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !slices.Equal(cfg.DatasetWhitelist, []string{"b-track", "a-track"}) {
		t.Errorf("DatasetWhitelist = %q, want [b-track a-track]", cfg.DatasetWhitelist)
	}
}

func TestLoadNodeNameMissing(t *testing.T) {
	setBaseEnv(t)
	os.Unsetenv("NODE_NAME") // t.Setenv 的 cleanup 会恢复原值

	if _, err := Load(); err == nil {
		t.Fatal("NODE_NAME 缺失时 Load 应报错")
	}
}

func TestLoadNodeNameEmpty(t *testing.T) {
	setBaseEnv(t) // NODE_NAME=""

	if _, err := Load(); err == nil {
		t.Fatal(`NODE_NAME="" 时 Load 应报错`)
	}
}

func TestLoadVerifyInvalid(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-01")
	t.Setenv("TRAINCACHE_VERIFY", "md5")

	_, err := Load()
	if err == nil {
		t.Fatal("TRAINCACHE_VERIFY=md5 时 Load 应报错")
	}
	if !strings.Contains(err.Error(), "TRAINCACHE_VERIFY") {
		t.Errorf("错误信息应指明 TRAINCACHE_VERIFY,实际: %v", err)
	}
}

// TestLoadHeatEnvInvalid:TRAINCACHE_HEAT_* 非法取值启动即报错(D30:duration 走
// time.ParseDuration,percent 0..100;比值类按 (0,1]、窗口 >0 校验),错误信息指明 env 名。
func TestLoadHeatEnvInvalid(t *testing.T) {
	cases := []struct{ key, val string }{
		{"TRAINCACHE_HEAT_DECAY_AGE", "24"},       // 缺单位
		{"TRAINCACHE_HEAT_DECAY_AGE", "0s"},       // ≤0
		{"TRAINCACHE_HEAT_DUMP_INTERVAL", "-1m"},  // 负
		{"TRAINCACHE_HEAT_DECAY_INTERVAL", "abc"}, // 非 duration
		{"TRAINCACHE_HEAT_DECAY_FACTOR", "0"},     // 比值下界外
		{"TRAINCACHE_HEAT_DECAY_FACTOR", "1.5"},   // 比值上界外
		{"TRAINCACHE_HEAT_OPEN_BETA", "-0.1"},
		{"TRAINCACHE_HEAT_OPEN_BETA", "NaN"},
		{"TRAINCACHE_HEAT_TOUCH_HALF_LIFE", "6"}, // 缺单位
		{"TRAINCACHE_HEAT_CALIBRATE_WINDOW", "0"},
		{"TRAINCACHE_HEAT_CALIBRATE_WINDOW", "1.5"},
		{"TRAINCACHE_HEAT_CALIBRATE_THRESHOLD", "1.01"},
		{"TRAINCACHE_HEAT_DISK_TRIGGER_PERCENT", "101"},
		{"TRAINCACHE_HEAT_DISK_DELTA_PERCENT", "-1"},
		{"TRAINCACHE_HEAT_DISK_CRITICAL_PERCENT", "junk"},
		{"TRAINCACHE_READ_STALL_MIN_WAIT_MS", "0"},
		{"TRAINCACHE_READ_STALL_MIN_WAIT_MS", "-1"},
		{"TRAINCACHE_READ_STALL_MIN_WAIT_MS", "1.5"},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.val, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("NODE_NAME", "worker-01")
			t.Setenv(tc.key, tc.val)
			_, err := Load()
			if err == nil {
				t.Fatalf("%s=%q 时 Load 应报错", tc.key, tc.val)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("错误信息应指明 %s,实际: %v", tc.key, err)
			}
		})
	}
}

// TestLoadHeatEnvBoundary:TRAINCACHE_HEAT_* 边界合法值受理(percent 0/100、比值 1、窗口 1)。
func TestLoadHeatEnvBoundary(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-01")
	t.Setenv("TRAINCACHE_HEAT_DECAY_FACTOR", "1")
	t.Setenv("TRAINCACHE_HEAT_OPEN_BETA", "1")
	t.Setenv("TRAINCACHE_HEAT_CALIBRATE_THRESHOLD", "1")
	t.Setenv("TRAINCACHE_HEAT_CALIBRATE_WINDOW", "1")
	t.Setenv("TRAINCACHE_HEAT_DISK_TRIGGER_PERCENT", "0")
	t.Setenv("TRAINCACHE_HEAT_DISK_DELTA_PERCENT", "100")
	t.Setenv("TRAINCACHE_HEAT_DISK_CRITICAL_PERCENT", "100")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("边界值 Load 应成功: %v", err)
	}
	if cfg.Heat.DecayFactor != 1 || cfg.Heat.HeatOpenBeta != 1 ||
		cfg.Heat.CalibrateThreshold != 1 || cfg.Heat.CalibrateWindow != 1 {
		t.Errorf("比值/窗口边界值解析错误: %+v", cfg.Heat)
	}
	if cfg.Heat.DiskTriggerPercent != 0 || cfg.Heat.DiskDeltaPercent != 100 || cfg.Heat.DiskCriticalPercent != 100 {
		t.Errorf("百分比边界值解析错误: %+v", cfg.Heat)
	}
}
