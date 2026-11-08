// Package config 读取并校验 checkpointd 的运行配置（design 10.1「部署与配置」）。
//
// 配置一次性读取，不做热加载；非法值返回错误，进程以退出码 2 结束，
// 训练容器保持不启动（design 10.1「关键流程」）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// 默认值逐字来自 design 10.1「部署与配置」。
const (
	defaultLogLevel        = "info"
	defaultSaveDeadline    = 15 * time.Minute
	defaultRestoreDeadline = 10 * time.Minute
	defaultPollInterval    = 200 * time.Millisecond
	defaultScanInterval    = 20 * time.Second
	defaultScanStable      = 3
)

// env 名（design 10.1「部署与配置」冻结）。
const (
	envLogLevel        = "TRAINCACHE_CHECKPOINT_LOG_LEVEL"
	envSaveDeadline    = "TRAINCACHE_CHECKPOINT_SAVE_DEADLINE"
	envRestoreDeadline = "TRAINCACHE_CHECKPOINT_RESTORE_DEADLINE"
	envPollInterval    = "TRAINCACHE_CHECKPOINT_POLL_INTERVAL"
	envScanInterval    = "TRAINCACHE_CHECKPOINT_SCAN_INTERVAL"
	envScanStable      = "TRAINCACHE_CHECKPOINT_SCAN_STABLE"
)

// Config 是 checkpointd 进程的完整配置（design 10.1「部署与配置」）。
type Config struct {
	LogLevel        string        // TRAINCACHE_CHECKPOINT_LOG_LEVEL，默认 info；非法值报错
	SaveDeadline    time.Duration // TRAINCACHE_CHECKPOINT_SAVE_DEADLINE，默认 15m
	RestoreDeadline time.Duration // TRAINCACHE_CHECKPOINT_RESTORE_DEADLINE，默认 10m
	PollInterval    time.Duration // TRAINCACHE_CHECKPOINT_POLL_INTERVAL，默认 200ms
	ScanInterval    time.Duration // TRAINCACHE_CHECKPOINT_SCAN_INTERVAL，默认 20s
	ScanStable      int           // TRAINCACHE_CHECKPOINT_SCAN_STABLE，默认 3，需 ≥2
}

// Load 从环境变量加载配置并校验：时长必须为正，ScanStable 需 ≥2，
// 日志级别限定 debug/info/warn/error，未知值视为启动错误。
func Load() (*Config, error) {
	level, err := loadLogLevel()
	if err != nil {
		return nil, err
	}
	save, err := envPositiveDuration(envSaveDeadline, defaultSaveDeadline)
	if err != nil {
		return nil, err
	}
	restore, err := envPositiveDuration(envRestoreDeadline, defaultRestoreDeadline)
	if err != nil {
		return nil, err
	}
	poll, err := envPositiveDuration(envPollInterval, defaultPollInterval)
	if err != nil {
		return nil, err
	}
	scan, err := envPositiveDuration(envScanInterval, defaultScanInterval)
	if err != nil {
		return nil, err
	}
	stable, err := envIntAtLeast(envScanStable, defaultScanStable, 2)
	if err != nil {
		return nil, err
	}
	return &Config{
		LogLevel:        level,
		SaveDeadline:    save,
		RestoreDeadline: restore,
		PollInterval:    poll,
		ScanInterval:    scan,
		ScanStable:      stable,
	}, nil
}

// loadLogLevel 解析日志级别。未设置时取 info，未知值报错而不是静默退化，
// 与 gateway 的宽松口径不同，理由见 design 10.1「关键流程」。
func loadLogLevel() (string, error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(envLogLevel)))
	if v == "" {
		return defaultLogLevel, nil
	}
	switch v {
	case "debug", "info", "warn", "error":
		return v, nil
	}
	return "", fmt.Errorf("config: %s=%q 非法(需 debug/info/warn/error)", envLogLevel, v)
}

// envPositiveDuration 解析正整数时长。未设置或全空白取默认值；
// 无法解析或 ≤0 返回错误。
func envPositiveDuration(key string, def time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q 非法(需时长): %w", key, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("config: %s=%q 非法(需 > 0)", key, raw)
	}
	return d, nil
}

// envIntAtLeast 解析不小于 min 的整数 env。未设置取默认值，非法或越界报错。
func envIntAtLeast(key string, def, min int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q 非法(需整数): %w", key, raw, err)
	}
	if n < min {
		return 0, fmt.Errorf("config: %s=%d 非法(需 ≥ %d)", key, n, min)
	}
	return n, nil
}
