package sidecar

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"traincache/checkpoint/internal/obs"
)

func TestProbeLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe", "ready")
	log, buf := newTestLogger()
	p := NewProbe(path, log)

	if err := p.Set(obs.StateRestoring); err != nil {
		t.Fatalf("Set(restoring): %v", err)
	}
	if got, err := ReadProbe(path); err != nil || got != string(obs.StateRestoring) {
		t.Fatalf("ReadProbe = %q, %v", got, err)
	}
	if err := p.Set(obs.StateReady); err != nil {
		t.Fatalf("Set(ready): %v", err)
	}
	if got, err := ReadProbe(path); err != nil || got != string(obs.StateReady) {
		t.Fatalf("ReadProbe = %q, %v", got, err)
	}
	if err := p.Set(obs.StateReady); err != nil {
		t.Fatalf("重复 Set(ready): %v", err)
	}
	if p.State() != obs.StateReady {
		t.Fatalf("State = %q, want ready", p.State())
	}
	if got := strings.Count(buf.String(), "探针状态切换"); got != 2 {
		t.Fatalf("探针切换日志条数 = %d, want 2\n%s", got, buf.String())
	}
	leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".probe-*"))
	if len(leftovers) != 0 {
		t.Fatalf("残留临时文件: %v", leftovers)
	}
}

func TestProbeRejectsInvalidTransition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready")
	log, _ := newTestLogger()
	p := NewProbe(path, log)
	if err := p.Set(obs.StateReady); err == nil {
		t.Fatal("空状态不能直接迁移到 ready")
	}
	if err := p.Set(obs.StateRestoring); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(obs.StateReady); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(obs.StateRestoring); err == nil {
		t.Fatal("ready 不能回退到 restoring")
	}
	if got, _ := ReadProbe(path); got != string(obs.StateReady) {
		t.Fatalf("非法迁移不应改写文件, got %q", got)
	}
}

func TestProbeFailedIsTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready")
	log, buf := newTestLogger()
	p := NewProbe(path, log)
	if err := p.Set(obs.StateRestoring); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(obs.StateFailed); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(obs.StateFailed); err != nil {
		t.Fatalf("重复 failed 应幂等: %v", err)
	}
	if err := p.Set(obs.StateReady); err == nil {
		t.Fatal("failed 后不能迁移到 ready")
	}
	if got := strings.Count(buf.String(), "探针状态切换"); got != 2 {
		t.Fatalf("日志条数 = %d, want 2\n%s", got, buf.String())
	}
}

func TestReadProbeMissing(t *testing.T) {
	if _, err := ReadProbe(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("不存在的探针文件应返回错误")
	}
	if _, err := os.Stat(filepath.Join(t.TempDir(), "missing")); !os.IsNotExist(err) {
		t.Fatal("测试前提不成立")
	}
}
