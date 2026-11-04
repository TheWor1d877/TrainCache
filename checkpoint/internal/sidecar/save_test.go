package sidecar

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/checkpoint/internal/plugin"
)

// 以下 helper 由 sidecar 包的多个测试文件共用。

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf
}

type fakePlugin struct {
	resolve func(context.Context, plugin.ResolveParams) (plugin.ResolveResult, error)
	save    func(context.Context, plugin.SaveParams) (plugin.TransferResult, error)
	restore func(context.Context, plugin.RestoreParams) (plugin.TransferResult, error)
	closed  atomic.Int32
}

func (p *fakePlugin) Resolve(ctx context.Context, params plugin.ResolveParams) (plugin.ResolveResult, error) {
	if p.resolve != nil {
		return p.resolve(ctx, params)
	}
	return plugin.ResolveResult{}, nil
}

func (p *fakePlugin) Save(ctx context.Context, params plugin.SaveParams) (plugin.TransferResult, error) {
	if p.save != nil {
		return p.save(ctx, params)
	}
	return plugin.TransferResult{}, nil
}

func (p *fakePlugin) Restore(ctx context.Context, params plugin.RestoreParams) (plugin.TransferResult, error) {
	if p.restore != nil {
		return p.restore(ctx, params)
	}
	return plugin.TransferResult{}, nil
}

func (p *fakePlugin) Close() error {
	p.closed.Add(1)
	return nil
}

type fakeFactory struct {
	mu       sync.Mutex
	starts   int
	startErr error
	build    func() *fakePlugin
	progress func(done, total int64)
}

func (f *fakeFactory) Start(ctx context.Context, onProgress func(done, total int64)) (Plugin, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return nil, f.startErr
	}
	f.starts++
	f.progress = onProgress
	p := &fakePlugin{}
	if f.build != nil {
		p = f.build()
	}
	return p, nil
}

func (f *fakeFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts
}

func testOptions(t *testing.T) Options {
	t.Helper()
	root := t.TempDir()
	log, _ := newTestLogger()
	return Options{
		TaskID:                "task",
		Rank:                  0,
		WorldSize:             1,
		TaskDir:               filepath.Join(root, "work", "task"),
		StateDir:              filepath.Join(root, "state"),
		ProbeFile:             filepath.Join(root, "probe", "ready"),
		SocketPath:            filepath.Join(root, "plugin.sock"),
		PollInterval:          5 * time.Millisecond,
		ScanInterval:          5 * time.Millisecond,
		ScanStable:            3,
		SaveDeadline:          time.Second,
		RestoreDeadline:       time.Second,
		SaveRetryIntervals:    []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond},
		RestoreRetryIntervals: []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond},
		RestoreSlowInterval:   5 * time.Millisecond,
		NVMERetention:         1,
		Logger:                log,
	}
}

func newTestSidecar(t *testing.T, opts Options, factory Factory) *Sidecar {
	t.Helper()
	if err := os.MkdirAll(opts.TaskDir, 0o755); err != nil {
		t.Fatalf("创建测试工作目录: %v", err)
	}
	s, err := New(opts, factory)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.accepting = true
	return s
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

func writeStep(t *testing.T, taskDir string, step int64, body string, done bool) string {
	t.Helper()
	dir := filepath.Join(taskDir, fmt.Sprintf("step-%d", step))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("创建 step 目录: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "model.bin"), []byte(body), 0o644); err != nil {
		t.Fatalf("写数据文件: %v", err)
	}
	if done {
		if err := os.WriteFile(filepath.Join(dir, doneMarker), nil, 0o644); err != nil {
			t.Fatalf("写 .done: %v", err)
		}
	}
	return dir
}

func writeCachedStep(t *testing.T, nvmeRoot, taskID string, step int64, body string) string {
	t.Helper()
	dir := filepath.Join(nvmeRoot, taskID, fmt.Sprintf("step-%d", step))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("创建缓存目录: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "model.bin"), []byte(body), 0o644); err != nil {
		t.Fatalf("写缓存文件: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, completeMarker), nil, 0o644); err != nil {
		t.Fatalf("写 .complete: %v", err)
	}
	return dir
}

func TestPollDoneEnqueuesAndSaves(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	var gotSave plugin.SaveParams
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{save: func(_ context.Context, p plugin.SaveParams) (plugin.TransferResult, error) {
			gotSave = p
			return plugin.TransferResult{Bytes: 8, Files: 1}, nil
		}}
	}}
	s := newTestSidecar(t, opts, f)
	dir := writeStep(t, opts.TaskDir, 10, "12345678", true)

	s.pollOnce()
	s.mu.Lock()
	queued := len(s.queue)
	s.mu.Unlock()
	if queued != 1 {
		t.Fatalf("poll 后队列长度 = %d, want 1", queued)
	}

	job, ok := s.nextJob()
	if !ok || job.step != 10 {
		t.Fatalf("nextJob = %+v, %v", job, ok)
	}
	if err := s.processSave(context.Background(), job.step, job.path); err != nil {
		t.Fatalf("processSave: %v", err)
	}
	s.finishJob(job.step)

	if gotSave.Step != 10 || gotSave.LocalPath != dir {
		t.Fatalf("save 参数 = %+v", gotSave)
	}
	if gotSave.StatePath != s.statePath(10) {
		t.Fatalf("state_path = %q, want %q", gotSave.StatePath, s.statePath(10))
	}
	if f.count() != 1 {
		t.Fatalf("插件启动次数 = %d, want 1", f.count())
	}
	if pathExists(dir) {
		t.Fatalf("保存成功后工作卷 step 目录仍存在")
	}
	cacheDir := filepath.Join(opts.NVMEDir, opts.TaskID, "step-10")
	if !pathExists(filepath.Join(cacheDir, completeMarker)) {
		t.Fatalf("缓存目录没有 .complete: %s", cacheDir)
	}
	if !pathExists(filepath.Join(cacheDir, "model.bin")) {
		t.Fatalf("缓存目录缺少数据文件")
	}
	s.mu.Lock()
	_, cached := s.cache[10]
	_, completed := s.completed[10]
	s.mu.Unlock()
	if !cached || !completed {
		t.Fatalf("cache=%v completed=%v, want true/true", cached, completed)
	}
}

func TestScanStableRequiresThreeObservations(t *testing.T) {
	opts := testOptions(t)
	opts.ScanStable = 3
	s := newTestSidecar(t, opts, &fakeFactory{})
	writeStep(t, opts.TaskDir, 1, "abc", false)

	s.scanOnce()
	s.scanOnce()
	if got := len(s.queue); got != 0 {
		t.Fatalf("两次扫描后队列长度 = %d, want 0", got)
	}
	s.scanOnce()
	if got := len(s.queue); got != 1 {
		t.Fatalf("三次稳定后队列长度 = %d, want 1", got)
	}
}

func TestDoneStepWaitsForChangingBytes(t *testing.T) {
	opts := testOptions(t)
	s := newTestSidecar(t, opts, &fakeFactory{})
	writeStep(t, opts.TaskDir, 1, "abcd", true)

	s.lastPoll[1] = 3 // 模拟上一轮字节数不同
	s.pollOnce()
	if got := len(s.queue); got != 0 {
		t.Fatalf("字节数变化时不应入队, 队列长度 = %d", got)
	}
	s.pollOnce()
	if got := len(s.queue); got != 1 {
		t.Fatalf("连续两轮字节数一致后应入队, 队列长度 = %d", got)
	}
}

func TestSaveRetriesTransientError(t *testing.T) {
	opts := testOptions(t)
	var calls atomic.Int32
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{save: func(context.Context, plugin.SaveParams) (plugin.TransferResult, error) {
			if calls.Add(1) < 3 {
				return plugin.TransferResult{}, &plugin.Error{Code: plugin.CodeUnavailable, Message: "busy"}
			}
			return plugin.TransferResult{Bytes: 3, Files: 1}, nil
		}}
	}}
	s := newTestSidecar(t, opts, f)
	dir := writeStep(t, opts.TaskDir, 1, "abc", true)
	if err := s.processSave(context.Background(), 1, dir); err != nil {
		t.Fatalf("processSave: %v", err)
	}
	if f.count() != 3 {
		t.Fatalf("插件启动次数 = %d, want 3", f.count())
	}
}

func TestSavePermanentErrorStopsImmediately(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{save: func(context.Context, plugin.SaveParams) (plugin.TransferResult, error) {
			return plugin.TransferResult{}, &plugin.Error{Code: plugin.CodeAuth, Message: "denied"}
		}}
	}}
	s := newTestSidecar(t, opts, f)
	dir := writeStep(t, opts.TaskDir, 2, "abc", true)

	err := s.processSave(context.Background(), 2, dir)
	if err == nil {
		t.Fatal("AUTH 应立即失败")
	}
	if f.count() != 1 {
		t.Fatalf("插件启动次数 = %d, want 1", f.count())
	}
	if !pathExists(dir) {
		t.Fatal("失败后应保留工作目录")
	}
	s.mu.Lock()
	accepting := s.accepting
	failed := s.failed
	s.mu.Unlock()
	if accepting || !failed {
		t.Fatalf("accepting=%v failed=%v, want false/true", accepting, failed)
	}
	logs := buf.String()
	if !strings.Contains(logs, "err_class=") || !strings.Contains(logs, "保存最终失败") {
		t.Fatalf("缺少 err/err_class 或最终失败日志:\n%s", logs)
	}
}

func TestSaveDeadlineStopsRetrying(t *testing.T) {
	opts := testOptions(t)
	opts.SaveDeadline = 20 * time.Millisecond
	opts.SaveRetryIntervals = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{save: func(context.Context, plugin.SaveParams) (plugin.TransferResult, error) {
			return plugin.TransferResult{}, &plugin.Error{Code: plugin.CodeUnavailable, Message: "down"}
		}}
	}}
	s := newTestSidecar(t, opts, f)
	dir := writeStep(t, opts.TaskDir, 3, "abc", true)

	start := time.Now()
	err := s.processSave(context.Background(), 3, dir)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("超过总时限应失败")
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("总时限未生效, 耗时 %s", elapsed)
	}
	if !pathExists(dir) {
		t.Fatal("失败后应保留工作目录")
	}
}

func TestProgressIsDebugSampled(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{save: func(_ context.Context, _ plugin.SaveParams) (plugin.TransferResult, error) {
			return plugin.TransferResult{Bytes: 100, Files: 1}, nil
		}}
	}}
	s := newTestSidecar(t, opts, f)
	progress := s.progressFunc(4)
	for i := 1; i <= 100; i++ {
		progress(int64(i), 250)
	}
	progress(250, 250)
	logs := buf.String()
	if got := strings.Count(logs, "保存进度"); got != 2 {
		t.Fatalf("进度日志条数 = %d, want 2\n%s", got, logs)
	}
	if !strings.Contains(logs, "level=DEBUG") {
		t.Fatalf("进度日志应为 Debug:\n%s", logs)
	}
}

func TestEmptyStepSkipped(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	s := newTestSidecar(t, opts, &fakeFactory{})
	dir := filepath.Join(opts.TaskDir, "step-1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, doneMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	s.pollOnce()
	if got := len(s.queue); got != 0 {
		t.Fatalf("空 step 不应入队, 队列长度 = %d", got)
	}
	if !strings.Contains(buf.String(), "reason=empty_dir") {
		t.Fatalf("缺少空目录跳过日志:\n%s", buf.String())
	}
}
