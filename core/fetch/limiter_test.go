package fetch

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestOptionsDefaults：Options 零值（及负值）字段按 design.md §8 取默认值。
func TestOptionsDefaults(t *testing.T) {
	got := Options{}.withDefaults()
	if got.MaxInflightBytes != defaultMaxInflightBytes || got.MaxConnsPerHost != defaultMaxConnsPerHost ||
		got.MaxTasks != defaultMaxTasks || got.MaxRetries != defaultMaxRetries {
		t.Fatalf("Options{} -> %+v, want defaults (%d/%d/%d/%d)",
			got, defaultMaxInflightBytes, defaultMaxConnsPerHost, defaultMaxTasks, defaultMaxRetries)
	}
	got = Options{MaxInflightBytes: -1, MaxTasks: 2, MaxRetries: 1}.withDefaults()
	if got.MaxInflightBytes != defaultMaxInflightBytes || got.MaxConnsPerHost != defaultMaxConnsPerHost ||
		got.MaxTasks != 2 || got.MaxRetries != 1 {
		t.Fatalf("partial Options -> %+v, want defaults only for unset fields", got)
	}
}

// TestLimiterByteSemaphoreCtxCancel：字节额度耗尽时 Acquire 阻塞，ctx 取消立即返回且不占额度。
func TestLimiterByteSemaphoreCtxCancel(t *testing.T) {
	sem := newByteSem(100)
	held, err := sem.acquire(context.Background(), 100)
	if err != nil || held != 100 {
		t.Fatalf("acquire(100) = (%d, %v), want (100, nil)", held, err)
	}
	if got := sem.inflight(); got != 100 {
		t.Fatalf("inflight = %d, want 100", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := sem.acquire(ctx, 1) // 额度已满，必阻塞
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked acquire err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after ctx cancel")
	}
	if got := sem.inflight(); got != 100 {
		t.Fatalf("inflight after canceled acquire = %d, want 100（取消不占额度）", got)
	}

	sem.release(held)
	if got := sem.inflight(); got != 0 {
		t.Fatalf("inflight after release = %d, want 0", got)
	}
	got, err := sem.acquire(context.Background(), 10)
	if err != nil || got != 10 {
		t.Fatalf("acquire after release = (%d, %v), want (10, nil)", got, err)
	}
}

// TestLimiterByteSemaphoreOversizedRequest：n > cap 时按 cap 记账（独占全部预算）。
func TestLimiterByteSemaphoreOversizedRequest(t *testing.T) {
	sem := newByteSem(10)
	held, err := sem.acquire(context.Background(), 64)
	if err != nil || held != 10 {
		t.Fatalf("acquire(64) with cap 10 = (%d, %v), want (10, nil)", held, err)
	}
	// 预算已被独占：普通 acquire 必须阻塞（用短超时验证）。
	tctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := sem.acquire(tctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire while oversized request holds budget = %v, want DeadlineExceeded", err)
	}
	sem.release(held)
	if _, err := sem.acquire(context.Background(), 10); err != nil {
		t.Fatalf("acquire after oversized release: %v", err)
	}
}

// TestLimiterTaskSemaphore：任务信号量上限与 ctx 取消。
func TestLimiterTaskSemaphore(t *testing.T) {
	sem := newCountSem(2)
	ctx := context.Background()
	if err := sem.acquire(ctx); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if err := sem.acquire(ctx); err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	tctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := sem.acquire(tctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire beyond max = %v, want DeadlineExceeded", err)
	}
	sem.release()
	if err := sem.acquire(ctx); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if got := sem.current(); got != 2 {
		t.Fatalf("current = %d, want 2", got)
	}
}

// TestLimiterHostSemaphore：同 host 并发受 MaxConnsPerHost 限制，不同 host 相互独立。
func TestLimiterHostSemaphore(t *testing.T) {
	sem := newHostSem(1)
	ctx := context.Background()
	if err := sem.acquire(ctx, "a"); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	tctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := sem.acquire(tctx, "a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire on same host = %v, want DeadlineExceeded", err)
	}
	if err := sem.acquire(ctx, "b"); err != nil {
		t.Fatalf("acquire other host: %v", err)
	}
	sem.release("a")
	sem.release("b")
	if err := sem.acquire(ctx, "a"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}

	if got := hostOf("dataset/ds/snapshots/1"); got != "dataset" {
		t.Fatalf("hostOf = %q, want %q", got, "dataset")
	}
	if got := hostOf("plain-key"); got != "plain-key" {
		t.Fatalf("hostOf = %q, want %q", got, "plain-key")
	}
}
