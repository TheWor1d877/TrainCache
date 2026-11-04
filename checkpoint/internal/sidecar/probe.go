package sidecar

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"traincache/checkpoint/internal/obs"
)

// Probe 维护探针标记文件。状态只允许 restoring -> ready，failed 可从任意
// 非 failed 状态进入并作为终态；重复设置同一状态不写文件也不写日志。
type Probe struct {
	path string
	log  *slog.Logger

	mu    sync.Mutex
	state obs.State
}

// NewProbe 构造探针读写器。
func NewProbe(path string, logger *slog.Logger) *Probe {
	if logger == nil {
		logger = slog.Default()
	}
	return &Probe{path: path, log: logger}
}

// State 返回当前进程内的探针状态，测试与排障使用。
func (p *Probe) State() obs.State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// Set 原子写入探针状态（临时文件 fsync 后 rename）。
func (p *Probe) Set(state obs.State) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if state == p.state {
		// 重复设置同一状态不记日志；仅当文件被外部删除或改写时补写。
		if got, err := ReadProbe(p.path); err == nil && got == string(state) {
			return nil
		}
		return writeProbeFile(p.path, state)
	}
	if !validProbeTransition(p.state, state) {
		return fmt.Errorf("sidecar: 探针状态不能从 %q 迁移到 %q", p.state, state)
	}
	if err := writeProbeFile(p.path, state); err != nil {
		return err
	}
	prev := p.state
	p.state = state
	// 消息带上状态：core 日志按 (component,event,task_id,reason) 去重 60s，
	// 同名消息会把下一次切换吞掉。
	p.log.Info("探针状态切换: "+string(state),
		obs.StateAttr(state), slog.String("prev", string(prev)))
	return nil
}

func validProbeTransition(from, to obs.State) bool {
	switch to {
	case obs.StateRestoring:
		return from == ""
	case obs.StateReady:
		return from == obs.StateRestoring
	case obs.StateFailed:
		return from == obs.StateRestoring || from == obs.StateReady
	default:
		return false
	}
}

func writeProbeFile(path string, state obs.State) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("sidecar: 创建探针目录 %s: %w", dir, err)
	}
	f, err := os.CreateTemp(dir, ".probe-*")
	if err != nil {
		return fmt.Errorf("sidecar: 创建探针临时文件: %w", err)
	}
	temp := f.Name()
	defer func() { _ = os.Remove(temp) }()

	if _, err := f.WriteString(string(state)); err != nil {
		_ = f.Close()
		return fmt.Errorf("sidecar: 写探针 %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sidecar: fsync 探针 %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("sidecar: 关闭探针 %s: %w", path, err)
	}
	if err := os.Rename(temp, path); err != nil {
		return fmt.Errorf("sidecar: rename 探针 %s: %w", path, err)
	}
	return syncDir(dir)
}

// ReadProbe 读取探针文件内容。文件不存在或读失败返回错误，调用方按失败处理。
func ReadProbe(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
