package fetch

import (
	"context"
	"errors"
	"sync"
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

// TestLimiterByteSemaphoreConcurrentAcquireAndCancel 并发压力场景：10 个 goroutine
// 同时 Acquire，cap=100、每个请求 20 → 前 5 个成功、后 5 个因额度耗尽阻塞。
// ctx cancel 触发后：
//   - 后 5 个阻塞者全部返回 ctx.Err()（且未占任何额度）
//   - 成功的前 5 个持有 permit，bytesAvailable 归零
//   - 释放后剩余 permit 全部归位、inflight 归零。
//
// 现有 limiter_test.go 仅顺序单测，缺竞争 + 混合取消的覆盖。
func TestLimiterByteSemaphoreConcurrentAcquireAndCancel(t *testing.T) {
	const nGoroutines = 10
	const cap int64 = 100
	const per int64 = 20 // 5 * 20 = 100，正好用完 cap

	sem := newByteSem(cap)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		wgDone    sync.WaitGroup
		wgRelease sync.WaitGroup
		start     = make(chan struct{})
		release   = make(chan struct{})
		errs      = make([]error, nGoroutines)
		held      = make([]int64, nGoroutines)
	)

	for i := 0; i < nGoroutines; i++ {
		wgDone.Add(1)
		go func(i int) {
			defer wgDone.Done()
			<-start
			got, err := sem.acquire(ctx, per)
			if err == nil {
				held[i] = got
				wgRelease.Add(1)
				go func() {
					defer wgRelease.Done()
					<-release
					sem.release(got)
				}()
			} else {
				errs[i] = err
			}
		}(i)
	}

	close(start)
	// 等待前 5 个拿到额度、后 5 个进入 select 等待状态。
	time.Sleep(50 * time.Millisecond)

	cancel()
	wgDone.Wait()

	// 断言：恰好 5 个成功 acquire，每个持有完整 20 字节。
	var successCount int
	for i := 0; i < nGoroutines; i++ {
		if errs[i] == nil {
			successCount++
			if held[i] != per {
				t.Fatalf("goroutine %d 持有 = %d，期望 %d", i, held[i], per)
			}
		}
	}
	if successCount != 5 {
		t.Fatalf("成功 acquire 的 goroutine 数 = %d，期望 5", successCount)
	}

	// 断言：剩余 5 个阻塞者全部因 ctx cancel 返回 context.Canceled，
	// 且它们未占任何额度（held 保持 0）。
	for i := 0; i < nGoroutines; i++ {
		if held[i] == 0 {
			if !errors.Is(errs[i], context.Canceled) {
				t.Fatalf("goroutine %d 阻塞 acquire 返回 = %v，期望 context.Canceled", i, errs[i])
			}
		}
	}

	// 断言：ctx cancel 后 inflight 恰好等于 cap（前 5 个持有的额度尚未释放），
	// 即剩余 permit 为零、bytesAvailable 归零。
	if got := sem.inflight(); got != cap {
		t.Fatalf("ctx cancel 后 inflight = %d，期望 %d（已占满全部额度）", got, cap)
	}

	// 释放前 5 个持有的 permit，全部归位 → inflight 归零。
	close(release)
	wgRelease.Wait()
	if got := sem.inflight(); got != 0 {
		t.Fatalf("全部 release 后 inflight = %d，期望 0", got)
	}
}
