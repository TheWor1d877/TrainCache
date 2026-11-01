package svc

// gateway_state_test.go：降级状态机的退避、探测互斥与代际丢弃（10.1 §4.3/§6.4，
// 10.2 D5）；用假时钟表驱动，不 sleep。

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock 是可手动推进的单调时钟（测试用）。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

// TestSourceStateInitial：启动时 disabled → mode=0；enabled → mode=1、nextProbe 零值。
func TestSourceStateInitial(t *testing.T) {
	off := newSourceState(false)
	if off.mode != 0 || !off.nextProbe.IsZero() {
		t.Errorf("disabled 初始状态 = mode %d nextProbe %v, want 0/零值", off.mode, off.nextProbe)
	}
	on := newSourceState(true)
	if on.mode != 1 || !on.nextProbe.IsZero() {
		t.Errorf("enabled 初始状态 = mode %d nextProbe %v, want 1/零值", on.mode, on.nextProbe)
	}
}

// TestSourceStateBackoffSequence：10s→20s→40s→80s→120s→120s；退避期间直连，
// 到达 nextProbe 时只放行一个探测。
func TestSourceStateBackoffSequence(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	clock := newFakeClock(base)
	s := newSourceState(true)
	s.setClock(clock.Now)

	// mode=1 的首次失败：10s 起点。
	use, gen := s.route()
	if !use {
		t.Fatal("enabled 启动后首次 Get 应走网关")
	}
	s.reportFailure(gen, "unavailable")
	if s.backoff != gatewayInitialBackoff {
		t.Fatalf("首次退避 = %v, want %v", s.backoff, gatewayInitialBackoff)
	}
	if want := base.Add(gatewayInitialBackoff); !s.nextProbe.Equal(want) {
		t.Fatalf("nextProbe = %v, want %v", s.nextProbe, want)
	}

	wantSequence := []time.Duration{
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
		120 * time.Second,
		120 * time.Second,
	}
	for i, want := range wantSequence {
		// 退避窗口内（nextProbe - 1s）必须直连。
		clock.Set(s.nextProbe.Add(-time.Second))
		if use, _ := s.route(); use {
			t.Fatalf("第 %d 次：退避窗口内不应探测", i+1)
		}

		// 到达 nextProbe 时抢到探测权。
		clock.Set(s.nextProbe)
		use, gen = s.route()
		if !use {
			t.Fatalf("第 %d 次：到达 nextProbe 应抢到探测权", i+1)
		}
		now := clock.Now()
		s.reportFailure(gen, "unavailable")
		if s.backoff != want {
			t.Fatalf("第 %d 次退避 = %v, want %v", i+1, s.backoff, want)
		}
		if !s.nextProbe.Equal(now.Add(want)) {
			t.Fatalf("第 %d 次 nextProbe = %v, want %v", i+1, s.nextProbe, now.Add(want))
		}
		if s.mode != 0 {
			t.Fatalf("第 %d 次失败后 mode = %d, want 0", i+1, s.mode)
		}
	}
}

// TestSourceStateRecoveryClearsBackoff：探测成功恢复 mode=1、backoff=0、nextProbe 清零。
func TestSourceStateRecoveryClearsBackoff(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	clock := newFakeClock(base)
	s := newSourceState(true)
	s.setClock(clock.Now)

	var modes []int
	s.setObservers(func(m int) { modes = append(modes, m) }, nil)

	// 连续两次失败，退避到 20s。
	_, gen := s.route()
	s.reportFailure(gen, "unavailable")
	clock.Set(s.nextProbe)
	_, gen = s.route()
	s.reportFailure(gen, "unavailable")
	if s.backoff != 20*time.Second {
		t.Fatalf("退避 = %v, want 20s", s.backoff)
	}

	// 到达下次探测时刻，探测成功。
	clock.Set(s.nextProbe)
	_, gen = s.route()
	s.reportSuccess(gen)

	if s.mode != 1 {
		t.Errorf("恢复后 mode = %d, want 1", s.mode)
	}
	if s.backoff != 0 {
		t.Errorf("恢复后 backoff = %v, want 0", s.backoff)
	}
	if !s.nextProbe.IsZero() {
		t.Errorf("恢复后 nextProbe = %v, want 零值", s.nextProbe)
	}
	if len(modes) != 3 || modes[0] != 0 || modes[1] != 0 || modes[2] != 1 {
		t.Errorf("mode 通知 = %v, want [0 0 1]", modes)
	}
	if use, _ := s.route(); !use {
		t.Error("恢复后 Get 应走网关")
	}
}

// TestSourceStateConcurrentProbe：now >= nextProbe 时 100 个并发请求只允许一个走网关。
func TestSourceStateConcurrentProbe(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	clock := newFakeClock(base)
	s := newSourceState(false) // mode=0，nextProbe 零值，立即可探测
	s.setClock(clock.Now)

	const n = 100
	var gateway, direct atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			<-start
			if use, _ := s.route(); use {
				gateway.Add(1)
			} else {
				direct.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := gateway.Load(); got != 1 {
		t.Fatalf("走网关的请求数 = %d, want 1（同一时刻最多一个探测）", got)
	}
	if got := direct.Load(); got != n-1 {
		t.Fatalf("直连请求数 = %d, want %d", got, n-1)
	}
}

// TestSourceStateGenerationDiscard：探测期间另一失败推进 gen 后，旧成功结果必须丢弃。
func TestSourceStateGenerationDiscard(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	clock := newFakeClock(base)
	s := newSourceState(false) // mode=0，立即可探测
	s.setClock(clock.Now)

	use, probeGen := s.route()
	if !use {
		t.Fatal("应立即抢到探测权")
	}

	// 探测返回前，另一个失败结果先落地：mode=0、gen 推进、退避 10s。
	s.reportFailure(probeGen, "unavailable")
	genAfterFailure := s.gen
	backoffAfterFailure := s.backoff
	nextAfterFailure := s.nextProbe
	if s.mode != 0 || s.backoff != gatewayInitialBackoff {
		t.Fatalf("失败迁移 = mode %d backoff %v", s.mode, s.backoff)
	}

	// 旧探测成功结果：代际不匹配，不得把 mode 拉回 1，也不得清零退避。
	s.reportSuccess(probeGen)
	if s.mode != 0 {
		t.Errorf("旧成功结果把 mode 拉回 %d, want 0", s.mode)
	}
	if s.gen != genAfterFailure || s.backoff != backoffAfterFailure || !s.nextProbe.Equal(nextAfterFailure) {
		t.Errorf("旧成功结果改动了状态: gen=%d backoff=%v nextProbe=%v", s.gen, s.backoff, s.nextProbe)
	}

	// 旧失败结果同样按代际丢弃：退避不应再次翻倍。
	s.reportFailure(probeGen, "unavailable")
	if s.backoff != gatewayInitialBackoff {
		t.Errorf("旧失败结果改动了退避: %v, want %v", s.backoff, gatewayInitialBackoff)
	}
}

// TestSourceStateNilObservers：未接线时迁移不 panic。
func TestSourceStateNilObservers(t *testing.T) {
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	s := newSourceState(true)
	s.setClock(clock.Now)
	s.setObservers(nil, nil)

	_, gen := s.route()
	s.reportFailure(gen, "unavailable")
	clock.Set(s.nextProbe)
	_, gen = s.route()
	s.reportSuccess(gen)
	if s.mode != 1 {
		t.Fatalf("mode = %d, want 1", s.mode)
	}
}
