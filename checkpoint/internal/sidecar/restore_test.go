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

// 放弃期限之前，慢速重试段保持不退出：现场还在，探针保持非 ready，
// 排障的人还能进去看状态与网络（design 10.2 D14）。
func TestRestoreSlowModeKeepsRunningUntilCancel(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	opts.RestoreDeadline = 10 * time.Millisecond
	opts.RestoreRetryIntervals = []time.Duration{time.Millisecond}
	opts.RestoreSlowInterval = 10 * time.Millisecond
	opts.RestoreGiveUpAfter = 30 * time.Second
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

	time.Sleep(60 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("未到放弃期限时慢速重试不应退出, err=%v", err)
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
	if strings.Count(buf.String(), "等待下一轮慢速重试") < 2 {
		t.Fatalf("慢速重试应每轮记 Error:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), "restore_give_up") {
		t.Fatalf("未到放弃期限不应记放弃日志:\n%s", buf.String())
	}
}

// 到放弃期限仍未成功时放弃并带错误返回：cmd 据此以非零码退出，把失败
// 交给 Job 记账，而不是让 Pod 停在 Init 占着 GPU（design 10.2 D14）。
func TestRestoreGivesUpAndExits(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	opts.RestoreDeadline = 10 * time.Millisecond
	opts.RestoreRetryIntervals = []time.Duration{time.Millisecond}
	opts.RestoreSlowInterval = 5 * time.Millisecond
	opts.RestoreGiveUpAfter = 60 * time.Millisecond
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
	err := s.restore(context.Background())
	if !errors.Is(err, ErrRestoreGiveUp) {
		t.Fatalf("restore = %v, want ErrRestoreGiveUp", err)
	}
	text := buf.String()
	if !strings.Contains(text, "reason=restore_give_up") {
		t.Fatalf("放弃日志缺 reason=restore_give_up:\n%s", text)
	}
	if !strings.Contains(text, "task_id=task") || !strings.Contains(text, "retry=") || !strings.Contains(text, "err=") {
		t.Fatalf("放弃日志缺 task_id/retry/err 字段:\n%s", text)
	}
	// 放弃前先走过慢速重试段：每轮一条 ERROR 是可观测出口的前提。
	if strings.Count(text, "等待下一轮慢速重试") < 1 {
		t.Fatalf("放弃前应至少有一轮慢速重试 ERROR:\n%s", text)
	}
	if f.count() < 2 {
		t.Fatalf("插件启动次数 = %d, want ≥2", f.count())
	}
}

// Run 把放弃错误原样交给进程入口，同时收尾把探针置 failed：
// 这是 D14 唯一的失败出口，不能被 shutdown 的收尾逻辑吞掉。
func TestRunReturnsGiveUpError(t *testing.T) {
	opts := testOptions(t)
	opts.RestoreDeadline = 10 * time.Millisecond
	opts.RestoreRetryIntervals = []time.Duration{time.Millisecond}
	opts.RestoreSlowInterval = 5 * time.Millisecond
	opts.RestoreGiveUpAfter = 60 * time.Millisecond
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
	if err := s.Run(context.Background()); !errors.Is(err, ErrRestoreGiveUp) {
		t.Fatalf("Run = %v, want ErrRestoreGiveUp", err)
	}
	if v, err := ReadProbe(opts.ProbeFile); err != nil || v != "failed" {
		t.Fatalf("放弃后探针 = %q (%v), want failed", v, err)
	}
}

// 放弃期限不大于恢复期限时慢速段长度为零，恢复一失败就退出，等于绕过
// D14 的「保留现场」，所以判配置非法而不是静默取大。
func TestNewRejectsGiveUpNotAfterRestoreDeadline(t *testing.T) {
	for _, give := range []time.Duration{500 * time.Millisecond, time.Second} {
		give := give
		t.Run(give.String(), func(t *testing.T) {
			opts := testOptions(t)
			opts.RestoreDeadline = time.Second
			opts.RestoreGiveUpAfter = give
			if _, err := New(opts, &fakeFactory{}); err == nil {
				t.Fatalf("RestoreGiveUpAfter=%s 应判非法", give)
			} else if !strings.Contains(err.Error(), "需大于") {
				t.Fatalf("err = %v, want 包含「需大于」", err)
			}
		})
	}
}

// 多余 rank 由插件枚举带回，WARN 在 checkpointd 侧记：两个插件的 stderr
// 都不进日志，只有这里能保证行为与日志字段一致（design 10.2 D25）。
func TestRestoreWarnsOnRankBeyondWorldSize(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error) {
				return plugin.ResolveResult{Found: false, MaxExtraRank: 3}, nil
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	if err := s.restoreOnce(context.Background(), st); err != nil {
		t.Fatalf("require=false 时应跳过恢复: %v", err)
	}
	text := buf.String()
	if !strings.Contains(text, "reason=rank_beyond_world_size") {
		t.Fatalf("缺 rank_beyond_world_size 的 WARN:\n%s", text)
	}
	if !strings.Contains(text, "max_rank=3") || !strings.Contains(text, "world_size=1") {
		t.Fatalf("WARN 缺最大 rank 或 world_size:\n%s", text)
	}
}

// 完整 step 上没有多余份时不记 WARN：多余份编号 0 是「没有」，不能因为
// 字段存在就报警。
func TestRestoreNoWarnWithoutExtraRank(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{
			resolve: func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error) {
				return plugin.ResolveResult{Found: false}, nil
			},
		}
	}}
	s := newTestSidecar(t, opts, f)
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	if err := s.restoreOnce(context.Background(), st); err != nil {
		t.Fatalf("restoreOnce: %v", err)
	}
	if strings.Contains(buf.String(), "rank_beyond_world_size") {
		t.Fatalf("没有多余份却记了 WARN:\n%s", buf.String())
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
