package sidecar

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"traincache/checkpoint/internal/plugin"
)

func TestQueueFullSkips(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	s := newTestSidecar(t, opts, &fakeFactory{})

	for step := int64(1); step <= queueLimit; step++ {
		if !s.enqueue(step, "step") {
			t.Fatalf("第 %d 个入队失败", step)
		}
	}
	if s.enqueue(5, "step") {
		t.Fatal("队满时应拒绝入队")
	}
	s.mu.Lock()
	got := len(s.queue)
	s.mu.Unlock()
	if got != queueLimit {
		t.Fatalf("队列长度 = %d, want %d", got, queueLimit)
	}
	if !strings.Contains(buf.String(), "queue_full") {
		t.Fatalf("缺少 queue_full 日志:\n%s", buf.String())
	}
}

func TestQueueOrderAndDedup(t *testing.T) {
	opts := testOptions(t)
	s := newTestSidecar(t, opts, &fakeFactory{})
	if !s.enqueue(30, "step-30") || !s.enqueue(10, "step-10") || !s.enqueue(20, "step-20") {
		t.Fatal("入队失败")
	}
	if s.enqueue(10, "step-10") {
		t.Fatal("重复 step 不应再次入队")
	}
	for _, want := range []int64{10, 20, 30} {
		job, ok := s.nextJob()
		if !ok || job.step != want {
			t.Fatalf("nextJob = %+v, %v; want step %d", job, ok, want)
		}
		s.finishJob(job.step)
	}
	if _, ok := s.nextJob(); ok {
		t.Fatal("队列应已清空")
	}
}

func TestQueueSkipsCompletedAndCached(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	s := newTestSidecar(t, opts, &fakeFactory{})
	s.mu.Lock()
	s.completed[1] = true
	s.cache[2] = t.TempDir()
	s.mu.Unlock()

	if s.enqueue(1, "step-1") || s.enqueue(2, "step-2") {
		t.Fatal("已完成或已缓存的 step 不应入队")
	}
	logs := buf.String()
	if !strings.Contains(logs, "reason=completed") || !strings.Contains(logs, "reason=cached") {
		t.Fatalf("缺少跳过原因日志:\n%s", logs)
	}
}

func TestSaveLoopSerialByAscendingStep(t *testing.T) {
	opts := testOptions(t)
	started := make(chan int64, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	f := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{save: func(ctx context.Context, p plugin.SaveParams) (plugin.TransferResult, error) {
			started <- p.Step
			select {
			case <-release:
			case <-ctx.Done():
				return plugin.TransferResult{}, ctx.Err()
			}
			return plugin.TransferResult{Bytes: 1, Files: 1}, nil
		}}
	}}
	s := newTestSidecar(t, opts, f)
	s.enqueue(2, "step-2")
	s.enqueue(1, "step-1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.saveLoop(ctx)
		close(done)
	}()

	select {
	case step := <-started:
		if step != 1 {
			t.Fatalf("首个上传 step = %d, want 1", step)
		}
	case <-time.After(time.Second):
		t.Fatal("等待首个上传超时")
	}
	select {
	case step := <-started:
		t.Fatalf("第一个 step 未完成就开始了 step %d", step)
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case step := <-started:
		if step != 2 {
			t.Fatalf("第二个上传 step = %d, want 2", step)
		}
	case <-time.After(time.Second):
		t.Fatal("等待第二个上传超时")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("saveLoop 未退出")
	}
}
