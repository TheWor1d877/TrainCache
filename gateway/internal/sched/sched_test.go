package sched

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// acquire 是测试里 Acquire 的简写。
func acquire(t *testing.T, s *Scheduler, dataset string) func() {
	t.Helper()
	rel, err := s.Acquire(context.Background(), dataset)
	if err != nil {
		t.Fatalf("Acquire(%q): %v", dataset, err)
	}
	return rel
}

// TestSingleDatasetFillsLimit 覆盖单个 dataset 能用满 L，且在满额后排队。
func TestSingleDatasetFillsLimit(t *testing.T) {
	const l = 4
	s := New(l)

	var rels []func()
	for i := 0; i < l; i++ {
		rels = append(rels, acquire(t, s, "a"))
	}
	if got := s.inflightTotal(); got != l {
		t.Fatalf("inflight = %d, want %d", got, l)
	}

	granted := make(chan func(), 1)
	go func() {
		rel, err := s.Acquire(context.Background(), "a")
		if err != nil {
			t.Errorf("第 %d 个 Acquire: %v", l+1, err)
			close(granted)
			return
		}
		granted <- rel
	}()

	select {
	case <-granted:
		t.Fatal("满额后不应立即放行")
	case <-time.After(30 * time.Millisecond):
	}

	rels[0]()
	select {
	case rel := <-granted:
		if rel == nil {
			t.Fatal("释放后应放行")
		}
		rel()
	case <-time.After(time.Second):
		t.Fatal("释放槽后未放行等待者")
	}
	for _, rel := range rels[1:] {
		rel()
	}
	if got := s.inflightTotal(); got != 0 {
		t.Fatalf("全部释放后 inflight = %d, want 0", got)
	}
}

// TestThreeDatasetsEachGetsOne 覆盖三个 dataset 并发时每个至少得到 1。
func TestThreeDatasetsEachGetsOne(t *testing.T) {
	s := New(3)
	rels := []func(){acquire(t, s, "a"), acquire(t, s, "b"), acquire(t, s, "c")}
	if got := s.inflightTotal(); got != 3 {
		t.Fatalf("inflight = %d, want 3", got)
	}

	// 第四个请求必须排队（全局已满）。
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Acquire(ctx, "a"); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("满额 Acquire = %v, want ErrWaitTimeout", err)
	}
	for _, rel := range rels {
		rel()
	}
}

// TestGlobalLimitNeverExceeded 覆盖并发申请/释放下 sum(inflight) <= L 恒成立。
func TestGlobalLimitNeverExceeded(t *testing.T) {
	const (
		l       = 4
		workers = 40
	)
	s := New(l)

	var over atomic.Int64
	stop := make(chan struct{})
	var poll sync.WaitGroup
	poll.Add(1)
	go func() {
		defer poll.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n := s.inflightTotal(); n > l {
				over.Store(int64(n))
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dataset := []string{"a", "b", "c"}[i%3]
			rel, err := s.Acquire(context.Background(), dataset)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			rel()
		}(i)
	}
	wg.Wait()
	close(stop)
	poll.Wait()
	if n := over.Load(); n != 0 {
		t.Fatalf("观测到 inflight = %d > L = %d", n, l)
	}
	if got := s.inflightTotal(); got != 0 {
		t.Fatalf("结束时 inflight = %d, want 0", got)
	}
}

// TestWaitTimeout 覆盖排队超时返回 ErrWaitTimeout。
func TestWaitTimeout(t *testing.T) {
	s := New(1)
	rel := acquire(t, s, "a")
	defer rel()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.Acquire(ctx, "b")
	if !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("Acquire = %v, want ErrWaitTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("等待 %v，超时限制未生效", elapsed)
	}
	if got := s.waitingTotal(); got != 0 {
		t.Fatalf("超时后 waiting = %d, want 0", got)
	}
}

// TestQueueFull 覆盖等待队列上限 64。
func TestQueueFull(t *testing.T) {
	s := New(1)
	held := acquire(t, s, "held")
	defer held()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < queueLimit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := s.Acquire(ctx, "waiting")
			if err != nil {
				return
			}
			rel()
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for s.waitingTotal() < queueLimit && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.waitingTotal(); got != queueLimit {
		t.Fatalf("waiting = %d, want %d", got, queueLimit)
	}

	if _, err := s.Acquire(ctx, "overflow"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("队列满 Acquire = %v, want ErrQueueFull", err)
	}

	cancel()
	wg.Wait()
}

// TestIdleShareTransferred 覆盖空闲份额自动让给仍在排队的 dataset。
func TestIdleShareTransferred(t *testing.T) {
	s := New(2)
	relA := acquire(t, s, "a")
	relB := acquire(t, s, "b")
	if got := s.inflightTotal(); got != 2 {
		t.Fatalf("inflight = %d, want 2", got)
	}

	granted := make(chan func(), 1)
	go func() {
		rel, err := s.Acquire(context.Background(), "a")
		if err != nil {
			t.Errorf("Acquire a: %v", err)
			close(granted)
			return
		}
		granted <- rel
	}()

	select {
	case <-granted:
		t.Fatal("全局满额且 a 已达份额时不应放行")
	case <-time.After(30 * time.Millisecond):
	}

	// b 释放后，它让出的空闲份额应转给排队的 a。
	relB()
	select {
	case rel := <-granted:
		if rel == nil {
			t.Fatal("份额转移后应放行")
		}
		rel()
	case <-time.After(time.Second):
		t.Fatal("b 释放后未把空闲份额让给 a")
	}
	relA()
}
