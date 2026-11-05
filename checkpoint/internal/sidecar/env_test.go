package sidecar

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var allEnvKeys = []string{
	envTaskID, envRank, envWorldSize, envTaskDir, envNVMEDir, envStateDir,
	envProbeFile, envPlugin, envPluginPath, envBackend, envRequire,
	envRetention, envCheckpointStep, envJob, envRestoreGiveUpAfter,
}

func clearSidecarEnv(t *testing.T) {
	t.Helper()
	for _, key := range allEnvKeys {
		key := key
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
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv(envTaskID, "task-a")
	t.Setenv(envRank, "0")
	t.Setenv(envWorldSize, "1")
	t.Setenv(envTaskDir, filepath.Join(t.TempDir(), "work", "task-a"))
}

func TestFromEnvDefaults(t *testing.T) {
	clearSidecarEnv(t)
	setRequiredEnv(t)

	opts, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if opts.TaskID != "task-a" || opts.Rank != 0 || opts.WorldSize != 1 {
		t.Fatalf("基础字段 = %+v", opts)
	}
	if opts.NVMEDir != "" {
		t.Fatalf("NVMEDir = %q, want empty", opts.NVMEDir)
	}
	if opts.StateDir != DefaultStateDir || opts.ProbeFile != DefaultProbeFile {
		t.Fatalf("StateDir=%q ProbeFile=%q", opts.StateDir, opts.ProbeFile)
	}
	if opts.Plugin != DefaultPlugin || opts.PluginPath != filepath.Join(defaultPluginRoot, DefaultPlugin, DefaultPlugin) {
		t.Fatalf("Plugin=%q PluginPath=%q", opts.Plugin, opts.PluginPath)
	}
	if opts.SocketPath != DefaultSocketPath {
		t.Fatalf("SocketPath = %q", opts.SocketPath)
	}
	if string(opts.Backend) != "{}" {
		t.Fatalf("Backend = %q, want {}", opts.Backend)
	}
	if opts.RequireCheckpoint {
		t.Fatal("RequireCheckpoint 默认应为 false")
	}
	if opts.NVMERetention != 1 {
		t.Fatalf("NVMERetention = %d, want 1", opts.NVMERetention)
	}
	if opts.CheckpointStep != nil {
		t.Fatalf("CheckpointStep = %v, want nil", *opts.CheckpointStep)
	}
	// 裸 Pod 没有 owner Job：JOB 未注入时 Job 是空串而不是缺省值，也不报错。
	if opts.Job != "" {
		t.Fatalf("Job = %q, want 空串", opts.Job)
	}
	if opts.RestoreGiveUpAfter != DefaultRestoreGiveUpAfter {
		t.Fatalf("RestoreGiveUpAfter = %s, want %s", opts.RestoreGiveUpAfter, DefaultRestoreGiveUpAfter)
	}
}

func TestFromEnvRequiredVariables(t *testing.T) {
	required := []string{envTaskID, envRank, envWorldSize, envTaskDir}
	for _, missing := range required {
		missing := missing
		t.Run(missing, func(t *testing.T) {
			clearSidecarEnv(t)
			setRequiredEnv(t)
			if err := os.Unsetenv(missing); err != nil {
				t.Fatal(err)
			}
			_, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("err = %v, want 包含 %s", err, missing)
			}
		})
	}
}

func TestFromEnvInvalidValues(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*testing.T)
		want  string
	}{
		{"rank not int", func(t *testing.T) { t.Setenv(envRank, "x") }, envRank},
		{"rank exceeds world", func(t *testing.T) { t.Setenv(envRank, "2") }, envRank},
		{"world zero", func(t *testing.T) { t.Setenv(envWorldSize, "0") }, envWorldSize},
		{"retention zero", func(t *testing.T) { t.Setenv(envRetention, "0") }, envRetention},
		{"retention not int", func(t *testing.T) { t.Setenv(envRetention, "abc") }, envRetention},
		{"require not bool", func(t *testing.T) { t.Setenv(envRequire, "maybe") }, envRequire},
		{"backend invalid", func(t *testing.T) { t.Setenv(envBackend, "{") }, envBackend},
		{"backend null", func(t *testing.T) { t.Setenv(envBackend, "null") }, envBackend},
		{"backend array", func(t *testing.T) { t.Setenv(envBackend, "[1]") }, envBackend},
		{"step negative", func(t *testing.T) { t.Setenv(envCheckpointStep, "-1") }, envCheckpointStep},
		{"step not int", func(t *testing.T) { t.Setenv(envCheckpointStep, "x") }, envCheckpointStep},
		{"task traversal", func(t *testing.T) { t.Setenv(envTaskID, "../escape") }, envTaskID},
		{"plugin traversal", func(t *testing.T) { t.Setenv(envPlugin, "../escape") }, envPlugin},
		{"give up not duration", func(t *testing.T) { t.Setenv(envRestoreGiveUpAfter, "abc") }, envRestoreGiveUpAfter},
		{"give up zero", func(t *testing.T) { t.Setenv(envRestoreGiveUpAfter, "0s") }, envRestoreGiveUpAfter},
		{"give up negative", func(t *testing.T) { t.Setenv(envRestoreGiveUpAfter, "-5m") }, envRestoreGiveUpAfter},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			clearSidecarEnv(t)
			setRequiredEnv(t)
			tc.setup(t)
			_, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want 包含 %s", err, tc.want)
			}
		})
	}
}

func TestFromEnvOptionalOverrides(t *testing.T) {
	clearSidecarEnv(t)
	setRequiredEnv(t)
	backend := fmt.Sprintf("{%q:%q}", "root", "/cache")
	t.Setenv(envNVMEDir, "/nvme")
	t.Setenv(envStateDir, "/state")
	t.Setenv(envProbeFile, "/probe/ready")
	t.Setenv(envPlugin, "local")
	t.Setenv(envPluginPath, "/opt/traincache/plugins/local/local")
	t.Setenv(envBackend, backend)
	t.Setenv(envRequire, "true")
	t.Setenv(envRetention, "3")
	t.Setenv(envCheckpointStep, "42")
	t.Setenv(envJob, "train-job-7f9c")
	t.Setenv(envRestoreGiveUpAfter, "90m")

	opts, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if opts.NVMEDir != "/nvme" || opts.StateDir != "/state" || opts.ProbeFile != "/probe/ready" {
		t.Fatalf("路径字段 = %+v", opts)
	}
	if opts.Plugin != "local" || opts.PluginPath != "/opt/traincache/plugins/local/local" {
		t.Fatalf("Plugin=%q PluginPath=%q", opts.Plugin, opts.PluginPath)
	}
	if string(opts.Backend) != backend || !json.Valid(opts.Backend) {
		t.Fatalf("Backend = %q, want %q", opts.Backend, backend)
	}
	if !opts.RequireCheckpoint || opts.NVMERetention != 3 {
		t.Fatalf("Require=%v Retention=%d", opts.RequireCheckpoint, opts.NVMERetention)
	}
	if opts.CheckpointStep == nil || *opts.CheckpointStep != 42 {
		t.Fatalf("CheckpointStep = %v", opts.CheckpointStep)
	}
	if opts.Job != "train-job-7f9c" {
		t.Fatalf("Job = %q, want train-job-7f9c", opts.Job)
	}
	if opts.RestoreGiveUpAfter != 90*time.Minute {
		t.Fatalf("RestoreGiveUpAfter = %s, want 90m", opts.RestoreGiveUpAfter)
	}
}

func TestEnvLoader(t *testing.T) {
	clearSidecarEnv(t)
	setRequiredEnv(t)
	var l Loader = EnvLoader{}
	opts, err := l.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if opts.TaskID != "task-a" {
		t.Fatalf("TaskID = %q", opts.TaskID)
	}
}
