package sidecar

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// 环境变量名（design 10.1 §4.2、§5.4 与任务约定冻结）。
const (
	envTaskID         = "TASK_ID"
	envRank           = "RANK"
	envWorldSize      = "WORLD_SIZE"
	envTaskDir        = "TRAINCACHE_CHECKPOINT_DIR"
	envNVMEDir        = "NVME_DIR"
	envStateDir       = "STATE_DIR"
	envProbeFile      = "PROBE_FILE"
	envPlugin         = "PLUGIN"
	envPluginPath     = "PLUGIN_PATH"
	envSocketPath     = "SOCKET_PATH"
	envBackend        = "BACKEND"
	envRequire        = "REQUIRE_CHECKPOINT"
	envRetention      = "NVME_RETENTION"
	envCheckpointStep = "CHECKPOINT_STEP"
)

// FromEnv 从环境变量组装 Options。错误信息带变量名，供启动日志直接定位。
func FromEnv() (Options, error) {
	taskID, err := requiredEnv(envTaskID)
	if err != nil {
		return Options{}, err
	}
	if err := validateTaskID(taskID); err != nil {
		return Options{}, err
	}
	rank, err := envInt(envRank, 0, true, 0)
	if err != nil {
		return Options{}, err
	}
	worldSize, err := envInt(envWorldSize, 0, true, 1)
	if err != nil {
		return Options{}, err
	}
	if rank >= worldSize {
		return Options{}, fmt.Errorf("sidecar: %s=%d 需小于 %s=%d", envRank, rank, envWorldSize, worldSize)
	}
	taskDir, err := requiredEnv(envTaskDir)
	if err != nil {
		return Options{}, err
	}

	opts := Options{
		TaskID:    taskID,
		Rank:      rank,
		WorldSize: worldSize,
		TaskDir:   taskDir,
		StateDir:  envString(envStateDir, DefaultStateDir),
		ProbeFile: envString(envProbeFile, DefaultProbeFile),
		Plugin:    envString(envPlugin, DefaultPlugin),
	}
	opts.NVMEDir = envString(envNVMEDir, "")
	if err := validatePluginName(opts.Plugin); err != nil {
		return Options{}, err
	}
	opts.PluginPath = envString(envPluginPath, filepath.Join(defaultPluginRoot, opts.Plugin, opts.Plugin))
	opts.SocketPath = envString(envSocketPath, DefaultSocketPath)

	if opts.Backend, err = envBackendJSON(); err != nil {
		return Options{}, err
	}
	if opts.RequireCheckpoint, err = envBool(envRequire, false); err != nil {
		return Options{}, err
	}
	if opts.NVMERetention, err = envInt(envRetention, defaultRetention, false, 1); err != nil {
		return Options{}, err
	}
	if raw := strings.TrimSpace(os.Getenv(envCheckpointStep)); raw != "" {
		step, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Options{}, fmt.Errorf("sidecar: %s=%q 非法(需非负整数): %w", envCheckpointStep, raw, err)
		}
		if step < 0 {
			return Options{}, fmt.Errorf("sidecar: %s=%d 非法(需 ≥ 0)", envCheckpointStep, step)
		}
		opts.CheckpointStep = &step
	}
	return opts, nil
}

func requiredEnv(name string) (string, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return "", fmt.Errorf("sidecar: 环境变量 %s 必填", name)
	}
	return v, nil
}

func envString(name, def string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	return v
}

func envInt(name string, def int, required bool, min int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		if required {
			return 0, fmt.Errorf("sidecar: 环境变量 %s 必填", name)
		}
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("sidecar: %s=%q 非法(需整数): %w", name, raw, err)
	}
	if n < min {
		return 0, fmt.Errorf("sidecar: %s=%d 非法(需 ≥ %d)", name, n, min)
	}
	return n, nil
}

func envBool(name string, def bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("sidecar: %s=%q 非法(需 true/false): %w", name, raw, err)
	}
	return v, nil
}

func envBackendJSON() (json.RawMessage, error) {
	raw := strings.TrimSpace(os.Getenv(envBackend))
	if raw == "" {
		return json.RawMessage("{}"), nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, fmt.Errorf("sidecar: %s 非法(需 JSON 对象): %w", envBackend, err)
	}
	if obj == nil {
		return nil, fmt.Errorf("sidecar: %s 非法(需 JSON 对象)", envBackend)
	}
	return json.RawMessage(raw), nil
}

func validateTaskID(id string) error {
	if id == "." || id == ".." {
		return fmt.Errorf("sidecar: %s=%q 非法", envTaskID, id)
	}
	if strings.ContainsAny(id, "/\\\x00") {
		return fmt.Errorf("sidecar: %s=%q 不能包含路径分隔符或 NUL", envTaskID, id)
	}
	return nil
}

func validatePluginName(name string) error {
	if name == "." || name == ".." {
		return fmt.Errorf("sidecar: %s=%q 非法", envPlugin, name)
	}
	if strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("sidecar: %s=%q 不能包含路径分隔符或 NUL", envPlugin, name)
	}
	return nil
}
