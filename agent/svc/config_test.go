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
		"TRAINCACHE_ORDERS_DIR",
		"TRAINCACHE_OBJSTORE_ENDPOINT",
		"TRAINCACHE_OBJSTORE_ACCESS_KEY",
		"TRAINCACHE_OBJSTORE_SECRET_KEY",
		"TRAINCACHE_OBJSTORE_BUCKET",
		"TRAINCACHE_OBJSTORE_SECURE",
	} {
		t.Setenv(k, "")
	}
}

// wantDefaultHeat 是 D14 默认六值:N=24h、M=30min、T=24h(因子 0.5)、k=80、Δ=5、M=90。
func wantDefaultHeat() HeatConfig {
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
		t.Errorf("CacheRoot = %q, want /var/cache/traincache(§8)", cfg.CacheRoot)
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
}

func TestLoadEnvOverride(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("NODE_NAME", "worker-02")
	t.Setenv("TRAINCACHE_GRPC_ADDR", "127.0.0.1:19410")
	t.Setenv("TRAINCACHE_METRICS_ADDR", "127.0.0.1:19411")
	t.Setenv("TRAINCACHE_VERIFY", "xxh3+sha256")
	t.Setenv("TRAINCACHE_DATASET_WHITELIST", "b-track,a-track")
	t.Setenv("TRAINCACHE_ORDERS_DIR", "/tmp/tc-orders")
	t.Setenv("TRAINCACHE_OBJSTORE_ENDPOINT", "minio.internal:9001")
	t.Setenv("TRAINCACHE_OBJSTORE_ACCESS_KEY", "ak-test")
	t.Setenv("TRAINCACHE_OBJSTORE_SECRET_KEY", "sk-test")
	t.Setenv("TRAINCACHE_OBJSTORE_BUCKET", "tc-blocks")
	t.Setenv("TRAINCACHE_OBJSTORE_SECURE", "1")

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
	// TRAINCACHE_HEAT_* 解析延后 M5/M6:Heat 恒为 D14 默认值。
	if want := wantDefaultHeat(); cfg.Heat != want {
		t.Errorf("Heat = %+v, want 默认值 %+v(env 解析延后 M5/M6)", cfg.Heat, want)
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
