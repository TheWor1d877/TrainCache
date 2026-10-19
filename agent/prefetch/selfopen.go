package prefetch

import (
	"sync"
	"time"

	"traincache/core/cas"
)

const (
	// defaultSelfOpenTTL 是 TTL ≤0(含零值)时的回退值;装配处显式传 3s。
	defaultSelfOpenTTL = 3 * time.Second
	// selfOpenMaxEntries 是 SelfOpenGuard 的条目上限:达到上限先清过期,仍满则淘汰
	// 最旧一条。按 3s TTL 与 Agent 自身打开速率,8192 足以覆盖在途窗口且内存有界。
	selfOpenMaxEntries = 8192
)

// SelfOpenGuard 记录 Agent 自身最近打开过的本地块指纹,供 collector 的 inotify 兜底
// 过滤(D49 self-open guard):Mark 后 TTL 内 SelfOpened 返回 true,过期自动失效。
//
// 并发安全;惰性清理(仅在 Mark 触发条目上限时清过期);零值/nil 安全——nil 接收者
// Mark 为 no-op、SelfOpened 返回 false。TTL ≤0 的零值按 defaultSelfOpenTTL 处理。
type SelfOpenGuard struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	now        func() time.Time
	entries    map[cas.Fingerprint]time.Time
}

// NewSelfOpenGuard 构造守卫(ttl 为条目有效期;装配处传 3*time.Second)。
func NewSelfOpenGuard(ttl time.Duration) *SelfOpenGuard {
	return newSelfOpenGuardWithClock(ttl, time.Now)
}

// newSelfOpenGuardWithClock 是注入时钟的内部构造(测试用)。
func newSelfOpenGuardWithClock(ttl time.Duration, now func() time.Time) *SelfOpenGuard {
	if ttl <= 0 {
		ttl = defaultSelfOpenTTL
	}
	if now == nil {
		now = time.Now
	}
	return &SelfOpenGuard{
		ttl:        ttl,
		maxEntries: selfOpenMaxEntries,
		now:        now,
		entries:    make(map[cas.Fingerprint]time.Time),
	}
}

// Mark 记录 fp 的本次自打开时刻(覆盖旧值)。nil 接收者为 no-op。
func (g *SelfOpenGuard) Mark(fp cas.Fingerprint) {
	if g == nil {
		return
	}
	now := g.currentTime()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.entries == nil {
		g.entries = make(map[cas.Fingerprint]time.Time)
	}
	limit := g.entryLimit()
	if _, exists := g.entries[fp]; !exists && len(g.entries) >= limit {
		g.purgeExpiredLocked(now)
		if len(g.entries) >= limit {
			g.evictOldestLocked()
		}
	}
	g.entries[fp] = now
}

// SelfOpened 报告 fp 是否在 TTL 内被 Mark 过。nil 接收者返回 false。
func (g *SelfOpenGuard) SelfOpened(fp cas.Fingerprint) bool {
	if g == nil {
		return false
	}
	now := g.currentTime()
	g.mu.Lock()
	defer g.mu.Unlock()
	at, ok := g.entries[fp]
	if !ok {
		return false
	}
	if now.Sub(at) >= g.expiry() {
		delete(g.entries, fp)
		return false
	}
	return true
}

// currentTime 返回时钟(零值未注入时钟时回退 time.Now)。
func (g *SelfOpenGuard) currentTime() time.Time {
	if g.now == nil {
		return time.Now()
	}
	return g.now()
}

// expiry 返回生效 TTL(零值 ≤0 回退 defaultSelfOpenTTL)。
func (g *SelfOpenGuard) expiry() time.Duration {
	if g.ttl <= 0 {
		return defaultSelfOpenTTL
	}
	return g.ttl
}

// entryLimit 返回生效条目上限(零值 ≤0 回退 selfOpenMaxEntries)。
func (g *SelfOpenGuard) entryLimit() int {
	if g.maxEntries <= 0 {
		return selfOpenMaxEntries
	}
	return g.maxEntries
}

// purgeExpiredLocked 删除所有已过期条目(调用方持锁)。
func (g *SelfOpenGuard) purgeExpiredLocked(now time.Time) {
	ttl := g.expiry()
	for fp, at := range g.entries {
		if now.Sub(at) >= ttl {
			delete(g.entries, fp)
		}
	}
}

// evictOldestLocked 淘汰时间戳最旧的一条(调用方持锁;仅在上限兜底时调用)。
func (g *SelfOpenGuard) evictOldestLocked() {
	var (
		oldestFP cas.Fingerprint
		oldestAt time.Time
		found    bool
	)
	for fp, at := range g.entries {
		if !found || at.Before(oldestAt) {
			oldestFP, oldestAt, found = fp, at, true
		}
	}
	if found {
		delete(g.entries, oldestFP)
	}
}
