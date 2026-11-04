package sidecar

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"traincache/checkpoint/internal/plugin"
)

func TestRestoreCacheHitCreatesSymlink(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	cacheDir := writeCachedStep(t, opts.NVMEDir, opts.TaskID, 7, "cached")
	var restoreCalls atomic.Int32
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error) {
				return plugin.ResolveResult{Found: true, Step: 7, Complete: true, Epoch: 2}, nil
			},
			restore: func(context.Context, plugin.RestoreParams) (plugin.TransferResult, error) {
				restoreCalls.Add(1)
				return plugin.TransferResult{}, nil
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	if err := s.registerCache(); err != nil {
		t.Fatalf("registerCache: %v", err)
	}
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	if err := s.restoreOnce(context.Background(), st); err != nil {
		t.Fatalf("restoreOnce: %v", err)
	}
	if restoreCalls.Load() != 0 {
		t.Fatal("缓存命中不应调用插件 restore")
	}
	link := filepath.Join(opts.TaskDir, "step-7")
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("软链不存在: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("恢复目标不是软链")
	}
	got, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	want, _ := filepath.Abs(cacheDir)
	if got != want {
		t.Fatalf("软链目标 = %q, want %q", got, want)
	}
	s.mu.Lock()
	_, completed := s.completed[7]
	_, linked := s.links[7]
	s.mu.Unlock()
	if !completed || !linked {
		t.Fatalf("completed=%v linked=%v, want true/true", completed, linked)
	}
}

func TestRestoreMissCallsPluginAndBackfills(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	var gotRestore plugin.RestoreParams
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error) {
				return plugin.ResolveResult{Found: true, Step: 3, Complete: true, Epoch: 1}, nil
			},
			restore: func(_ context.Context, p plugin.RestoreParams) (plugin.TransferResult, error) {
				gotRestore = p
				if err := os.MkdirAll(p.TargetPath, 0o755); err != nil {
					return plugin.TransferResult{}, err
				}
				if err := os.WriteFile(filepath.Join(p.TargetPath, "model.bin"), []byte("abcdef"), 0o644); err != nil {
					return plugin.TransferResult{}, err
				}
				return plugin.TransferResult{Bytes: 6, Files: 1}, nil
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	if err := s.restoreOnce(context.Background(), st); err != nil {
		t.Fatalf("restoreOnce: %v", err)
	}
	wantTarget := filepath.Join(opts.TaskDir, "step-3")
	if gotRestore.TargetPath != wantTarget || gotRestore.Step != 3 {
		t.Fatalf("restore 参数 = %+v, want target %s", gotRestore, wantTarget)
	}
	if !pathExists(filepath.Join(wantTarget, "model.bin")) {
		t.Fatal("插件恢复的目标文件不存在")
	}
	s.bg.Wait()
	cacheDir := filepath.Join(opts.NVMEDir, opts.TaskID, "step-3")
	if !pathExists(filepath.Join(cacheDir, completeMarker)) {
		t.Fatalf("恢复后异步回填没有写 .complete: %s", cacheDir)
	}
}

func TestRestoreCorruptSwitchesStep(t *testing.T) {
	opts := testOptions(t)
	var restoreCalls atomic.Int32
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(_ context.Context, p plugin.ResolveParams) (plugin.ResolveResult, error) {
				step := int64(10)
				if p.Policy.Mode == "step" {
					step = p.Policy.Step
				}
				return plugin.ResolveResult{Found: true, Step: step, Complete: true}, nil
			},
			restore: func(_ context.Context, p plugin.RestoreParams) (plugin.TransferResult, error) {
				restoreCalls.Add(1)
				if p.Step == 10 {
					return plugin.TransferResult{}, &plugin.Error{Code: plugin.CodeCorrupt, Message: "bad manifest"}
				}
				if err := os.MkdirAll(p.TargetPath, 0o755); err != nil {
					return plugin.TransferResult{}, err
				}
				return plugin.TransferResult{Bytes: 1, Files: 1}, nil
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	if err := s.restoreOnce(context.Background(), st); err == nil {
		t.Fatal("CORRUPT 首次应返回错误")
	}
	if st.switches != 1 || st.policy.Mode != "step" || st.policy.Step != 9 {
		t.Fatalf("state = %+v, want switches=1 step=9", st)
	}
	if err := s.restoreOnce(context.Background(), st); err != nil {
		t.Fatalf("换 step 后应成功: %v", err)
	}
	if !pathExists(filepath.Join(opts.TaskDir, "step-9")) {
		t.Fatal("没有恢复到 step-9")
	}
	if restoreCalls.Load() != 2 {
		t.Fatalf("restore 调用次数 = %d, want 2", restoreCalls.Load())
	}
}

func TestRestoreCorruptSwitchesAtMostTwice(t *testing.T) {
	opts := testOptions(t)
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(_ context.Context, p plugin.ResolveParams) (plugin.ResolveResult, error) {
				step := int64(10)
				if p.Policy.Mode == "step" {
					step = p.Policy.Step
				}
				return plugin.ResolveResult{Found: true, Step: step, Complete: true}, nil
			},
			restore: func(context.Context, plugin.RestoreParams) (plugin.TransferResult, error) {
				return plugin.TransferResult{}, &plugin.Error{Code: plugin.CodeCorrupt, Message: "bad"}
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	for i := 0; i < 3; i++ {
		if err := s.restoreOnce(context.Background(), st); err == nil {
			t.Fatalf("第 %d 次应失败", i+1)
		}
	}
	if st.switches != 2 {
		t.Fatalf("switch 次数 = %d, want 2", st.switches)
	}
	if st.policy.Step != 8 {
		t.Fatalf("最终 step = %d, want 8", st.policy.Step)
	}
}

func TestRestoreRequireCheckpoint(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	opts.RequireCheckpoint = true
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{resolve: func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error) {
			return plugin.ResolveResult{Found: false}, nil
		}}
	}}
	s := newTestSidecar(t, opts, f)
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	err := s.restoreOnce(context.Background(), st)
	if err == nil || !errors.Is(err, errNoCheckpoint) {
		t.Fatalf("require=true 应返回 errNoCheckpoint, got %v", err)
	}

	opts.RequireCheckpoint = false
	opts.Logger = log
	s2 := newTestSidecar(t, opts, f)
	if err := s2.restoreOnce(context.Background(), &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}); err != nil {
		t.Fatalf("require=false 应直接成功: %v", err)
	}
	if !strings.Contains(buf.String(), "result=skipped") {
		t.Fatalf("缺少 skipped 日志:\n%s", buf.String())
	}
}

func TestRestoreRetriesThenSucceeds(t *testing.T) {
	opts := testOptions(t)
	var calls atomic.Int32
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error) {
				return plugin.ResolveResult{Found: true, Step: 1, Complete: true}, nil
			},
			restore: func(_ context.Context, p plugin.RestoreParams) (plugin.TransferResult, error) {
				if calls.Add(1) < 3 {
					return plugin.TransferResult{}, &plugin.Error{Code: plugin.CodeUnavailable, Message: "busy"}
				}
				if err := os.MkdirAll(p.TargetPath, 0o755); err != nil {
					return plugin.TransferResult{}, err
				}
				return plugin.TransferResult{Bytes: 1, Files: 1}, nil
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	if err := s.restore(context.Background()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if f.count() != 3 {
		t.Fatalf("插件启动次数 = %d, want 3", f.count())
	}
}

func TestRestoreSlowModeKeepsRunningUntilCancel(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	opts.RestoreDeadline = 10 * time.Millisecond
	opts.RestoreRetryIntervals = []time.Duration{time.Millisecond}
	opts.RestoreSlowInterval = 10 * time.Millisecond
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error) {
				return plugin.ResolveResult{Found: true, Step: 1, Complete: true}, nil
			},
			restore: func(context.Context, plugin.RestoreParams) (plugin.TransferResult, error) {
				return plugin.TransferResult{}, &plugin.Error{Code: plugin.CodeUnavailable, Message: "down"}
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.restore(ctx) }()

	time.Sleep(40 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("恢复慢速重试不应退出, err=%v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel 后 restore 返回 %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel 后 restore 未退出")
	}
	if strings.Count(buf.String(), "恢复失败") < 2 {
		t.Fatalf("慢速重试应每轮记 Error:\n%s", buf.String())
	}
}

func TestRunRestoresAndStopsOnCancel(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	var saveCalls atomic.Int32
	resolveRelease := make(chan struct{})
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(ctx context.Context, _ plugin.ResolveParams) (plugin.ResolveResult, error) {
				select {
				case <-resolveRelease:
					return plugin.ResolveResult{Found: false}, nil
				case <-ctx.Done():
					return plugin.ResolveResult{}, ctx.Err()
				}
			},
			save: func(context.Context, plugin.SaveParams) (plugin.TransferResult, error) {
				saveCalls.Add(1)
				return plugin.TransferResult{Bytes: 3, Files: 1}, nil
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	waitFor(t, 2*time.Second, "探针 restoring", func() bool {
		v, err := ReadProbe(opts.ProbeFile)
		return err == nil && v == "restoring"
	})
	if v, _ := ReadProbe(opts.ProbeFile); v == "ready" {
		t.Fatal("恢复完成前探针不应 ready")
	}
	close(resolveRelease)
	waitFor(t, 2*time.Second, "探针 ready", func() bool {
		v, err := ReadProbe(opts.ProbeFile)
		return err == nil && v == "ready"
	})
	writeStep(t, opts.TaskDir, 1, "abc", true)
	waitFor(t, 2*time.Second, "保存成功后删除工作卷副本", func() bool {
		return !pathExists(filepath.Join(opts.TaskDir, "step-1"))
	})
	if v, _ := ReadProbe(opts.ProbeFile); v != "ready" {
		t.Fatalf("保存期间探针 = %q, want ready", v)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run 返回 %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未在取消后退出")
	}
	if v, err := ReadProbe(opts.ProbeFile); err != nil || v != "failed" {
		t.Fatalf("退出后探针 = %q, %v; want failed", v, err)
	}
	if saveCalls.Load() != 1 {
		t.Fatalf("插件保存次数 = %d, want 1", saveCalls.Load())
	}
}
