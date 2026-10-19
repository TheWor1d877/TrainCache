package prefetch

import (
	"sync"
	"testing"
	"time"

	"traincache/core/cas"
)

// guardClock 是可推进的假时钟(并发安全)。
type guardClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *guardClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *guardClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func testGuardFP(b byte) cas.Fingerprint {
	var fp cas.Fingerprint
	for i := range fp {
		fp[i] = b
	}
	return fp
}

// TestSelfOpenGuardTTL:Mark 后 TTL 内为 true,过期后为 false,重 Mark 重新生效。
func TestSelfOpenGuardTTL(t *testing.T) {
	clk := &guardClock{now: time.Unix(1_700_000_000, 0)}
	g := newSelfOpenGuardWithClock(3*time.Second, clk.Now)
	fp := testGuardFP(0x01)

	if g.SelfOpened(fp) {
		t.Fatal("未 Mark 时 SelfOpened = true, want false")
	}
	g.Mark(fp)
	if !g.SelfOpened(fp) {
		t.Fatal("Mark 后 TTL 内 SelfOpened = false, want true")
	}
	clk.Advance(2 * time.Second)
	if !g.SelfOpened(fp) {
		t.Fatal("TTL 内(2s)SelfOpened = false, want true")
	}
	clk.Advance(2 * time.Second)
	if g.SelfOpened(fp) {
		t.Fatal("TTL 过期(4s)SelfOpened = true, want false")
	}
	g.Mark(fp)
	clk.Advance(time.Second)
	if !g.SelfOpened(fp) {
		t.Fatal("重新 Mark 后 SelfOpened = false, want true")
	}
}

// TestSelfOpenGuardZeroAndNil:零值 TTL 回退默认;nil 接收者 Mark no-op、SelfOpened false。
func TestSelfOpenGuardZeroAndNil(t *testing.T) {
	var nilGuard *SelfOpenGuard
	nilGuard.Mark(testGuardFP(0x02)) // 不 panic
	if nilGuard.SelfOpened(testGuardFP(0x02)) {
		t.Fatal("nil 接收者 SelfOpened = true, want false")
	}

	clk := &guardClock{now: time.Unix(1_700_000_000, 0)}
	g := newSelfOpenGuardWithClock(0, clk.Now)
	fp := testGuardFP(0x03)
	g.Mark(fp)
	clk.Advance(defaultSelfOpenTTL - time.Millisecond)
	if !g.SelfOpened(fp) {
		t.Fatal("零值 TTL 未回退默认:TTL 内 SelfOpened = false")
	}

	// 结构体零值(未注入时钟/上限):Mark/SelfOpened 可用且按默认 TTL 生效。
	var zero SelfOpenGuard
	zero.Mark(fp)
	if !zero.SelfOpened(fp) {
		t.Fatal("SelfOpenGuard 零值不可用:SelfOpened = false")
	}
}

// TestSelfOpenGuardBounded:条目达到上限时先清过期;仍满则淘汰最旧条目,内存有界。
func TestSelfOpenGuardBounded(t *testing.T) {
	clk := &guardClock{now: time.Unix(1_700_000_000, 0)}
	g := newSelfOpenGuardWithClock(time.Minute, clk.Now)
	g.maxEntries = 4

	for i := byte(0); i < 4; i++ {
		g.Mark(testGuardFP(i))
		clk.Advance(time.Second)
	}
	// 全部未过期:插入第 5 条淘汰最旧(0x00)。
	g.Mark(testGuardFP(0xff))
	if g.SelfOpened(testGuardFP(0x00)) {
		t.Fatal("满额插入后最旧条目仍在, want 被淘汰")
	}
	if !g.SelfOpened(testGuardFP(0xff)) {
		t.Fatal("满额插入后新条目不可见")
	}
	if got := len(g.entries); got > g.maxEntries {
		t.Fatalf("条目数 = %d, want ≤ %d", got, g.maxEntries)
	}

	// TTL 过期后插入:先清过期,不触发最旧淘汰。
	clk.Advance(2 * time.Minute)
	g.Mark(testGuardFP(0x11))
	if got := len(g.entries); got != 1 {
		t.Fatalf("过期清理后条目数 = %d, want 1(仅新条目)", got)
	}
}

// TestSelfOpenGuardConcurrent:并发 Mark/SelfOpened 无数据竞争(-race 验证)。
func TestSelfOpenGuardConcurrent(t *testing.T) {
	g := NewSelfOpenGuard(time.Second)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				fp := testGuardFP(byte(i))
				g.Mark(fp)
				_ = g.SelfOpened(fp)
			}
		}(w)
	}
	wg.Wait()
}
