package config

import (
	"strings"
	"testing"
	"time"
)

// setEnv 清空六个键后按需覆盖，避免宿主机环境污染默认值用例。
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for _, k := range []string{
		envLogLevel, envSaveDeadline, envRestoreDeadline,
		envPollInterval, envScanInterval, envScanStable,
	} {
		t.Setenv(k, "")
	}
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t, nil)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("LogLevel = %q, want info", cfg.LogLevel)
	}
	if cfg.SaveDeadline != 15*time.Minute || cfg.RestoreDeadline != 10*time.Minute {
		t.Fatalf("deadlines = %v/%v, want 15m/10m", cfg.SaveDeadline, cfg.RestoreDeadline)
	}
	if cfg.PollInterval != 200*time.Millisecond || cfg.ScanInterval != 20*time.Second {
		t.Fatalf("intervals = %v/%v, want 200ms/20s", cfg.PollInterval, cfg.ScanInterval)
	}
	if cfg.ScanStable != 3 {
		t.Fatalf("ScanStable = %d, want 3", cfg.ScanStable)
	}
}

func TestLoadOverrides(t *testing.T) {
	setEnv(t, map[string]string{
		envLogLevel:        "DEBUG",
		envSaveDeadline:    "1m30s",
		envRestoreDeadline: "2m",
		envPollInterval:    "50ms",
		envScanInterval:    "5s",
		envScanStable:      "4",
	})
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("LogLevel = %q, want debug", cfg.LogLevel)
	}
	if cfg.SaveDeadline != 90*time.Second {
		t.Fatalf("SaveDeadline = %v, want 1m30s", cfg.SaveDeadline)
	}
	if cfg.PollInterval != 50*time.Millisecond || cfg.ScanStable != 4 {
		t.Fatalf("PollInterval/ScanStable = %v/%d", cfg.PollInterval, cfg.ScanStable)
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"level", map[string]string{envLogLevel: "trace"}, envLogLevel},
		{"duration syntax", map[string]string{envSaveDeadline: "15"}, envSaveDeadline},
		{"duration zero", map[string]string{envRestoreDeadline: "0s"}, envRestoreDeadline},
		{"duration negative", map[string]string{envPollInterval: "-1s"}, envPollInterval},
		{"scan stable", map[string]string{envScanStable: "1"}, envScanStable},
		{"scan stable syntax", map[string]string{envScanStable: "x"}, envScanStable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			_, err := Load()
			if err == nil {
				t.Fatal("Load() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to mention %s", err, tc.want)
			}
		})
	}
}
