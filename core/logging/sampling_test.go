package logging

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// captureHandler 记录通过的日志条数,用于断言采样/限流效果。
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *captureHandler) last() slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records[len(h.records)-1]
}

// newTestSampler 返回固定时钟(可改)的采样器,默认随机源返回 0。
func newTestSampler(component string, rate, burst, infoRate, debugRate float64) (*Sampler, *time.Time) {
	now := time.Unix(1700000000, 0)
	s := newSampler(component, rate, burst, infoRate, debugRate)
	s.now = func() time.Time { return now }
	s.rand = func() float64 { return 0 }
	s.last = now
	s.tokens = burst
	return s, &now
}

func recordInt64(r slog.Record, key string) (int64, bool) {
	var value int64
	var ok bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value = a.Value.Int64()
			ok = true
			return false
		}
		return true
	})
	return value, ok
}

// TestSamplerInfoAndDebugRate 固定随机源,断言 Info/Debug 各自按采样率放行。
func TestSamplerInfoAndDebugRate(t *testing.T) {
	s, _ := newTestSampler("test", 1000, 1000, 0.5, 0.5)
	s.rand = func() float64 { return 0.49 }
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	log.Info("info pass")
	log.Debug("debug pass")
	if got := capture.count(); got != 2 {
		t.Fatalf("随机值低于采样率时应放行,got %d 条, want 2", got)
	}

	s.rand = func() float64 { return 0.5 }
	log.Info("info drop")
	log.Debug("debug drop")
	if got := capture.count(); got != 2 {
		t.Fatalf("随机值不低于采样率时应丢弃,got %d 条, want 2", got)
	}
	if got := s.Dropped("test", slog.LevelInfo); got != 1 {
		t.Fatalf("Info 丢弃计数 = %d, want 1", got)
	}
	if got := s.Dropped("test", slog.LevelDebug); got != 1 {
		t.Fatalf("Debug 丢弃计数 = %d, want 1", got)
	}
	if got := s.Dropped("other", slog.LevelInfo); got != 0 {
		t.Fatalf("其他 component 的丢弃计数 = %d, want 0", got)
	}
}

// TestSamplerTokenBucketBurstAndRate 固定时钟,断言突发额度与按秒补充速率。
func TestSamplerTokenBucketBurstAndRate(t *testing.T) {
	s, now := newTestSampler("test", 2, 3, 1, 1)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	for i := 0; i < 5; i++ {
		log.Info(fmt.Sprintf("burst %d", i))
	}
	if got := capture.count(); got != 3 {
		t.Fatalf("突发额度应为 3,got %d 条, want 3", got)
	}
	if got := s.Dropped("test", slog.LevelInfo); got != 2 {
		t.Fatalf("令牌耗尽后的 Info 丢弃计数 = %d, want 2", got)
	}

	*now = now.Add(500 * time.Millisecond)
	log.Info("after 500ms")
	if got := capture.count(); got != 4 {
		t.Fatalf("500ms 应补 1 个令牌,got %d 条, want 4", got)
	}
	log.Info("no token")
	if got := capture.count(); got != 4 {
		t.Fatalf("无令牌时应丢弃,got %d 条, want 4", got)
	}
	if got := s.Dropped("test", slog.LevelInfo); got != 3 {
		t.Fatalf("Info 丢弃计数 = %d, want 3", got)
	}
}

// TestSamplerDedupWindow 断言相同字段 60s 内只出一条,下一窗口带回 suppressed。
func TestSamplerDedupWindow(t *testing.T) {
	s, now := newTestSampler("test", 1000, 1000, 1, 1)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With(
		"component", "test",
		"node", "n1",
		"task_id", "t1",
		"reason", "same",
	)

	log.Info("duplicated")
	if got := capture.count(); got != 1 {
		t.Fatalf("窗口首条应放行,got %d 条, want 1", got)
	}
	if _, ok := recordInt64(capture.last(), "suppressed"); ok {
		t.Fatal("窗口首条不应带 suppressed")
	}

	*now = now.Add(59 * time.Second)
	log.Info("duplicated")
	if got := capture.count(); got != 1 {
		t.Fatalf("窗口内重复应被抑制,got %d 条, want 1", got)
	}
	if got := s.Dropped("test", slog.LevelInfo); got != 1 {
		t.Fatalf("去重丢弃计数 = %d, want 1", got)
	}

	*now = now.Add(time.Second)
	log.Info("duplicated")
	if got := capture.count(); got != 2 {
		t.Fatalf("窗口到期后应再放行一条,got %d 条, want 2", got)
	}
	suppressed, ok := recordInt64(capture.last(), "suppressed")
	if !ok || suppressed != 1 {
		t.Fatalf("下一窗口 suppressed = %d (ok=%v), want 1", suppressed, ok)
	}

	// 任一去重维度变化即为不同键,不受窗口抑制。
	log.Info("duplicated", "reason", "different")
	if got := capture.count(); got != 3 {
		t.Fatalf("reason 变化应视为新键,got %d 条, want 3", got)
	}
}

// TestSamplerZeroRateKeepsWarnError 断言采样率为 0 且令牌桶为空时 Warn/Error 零丢失。
func TestSamplerZeroRateKeepsWarnError(t *testing.T) {
	s, _ := newTestSampler("test", 0, 0, 0, 0)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	log.Info("info dropped")
	for i := 0; i < 3; i++ {
		log.Warn("warn kept")
	}
	for i := 0; i < 2; i++ {
		log.Error("error kept")
	}

	if got := capture.count(); got != 5 {
		t.Fatalf("Warn/Error 应全部放行,got %d 条, want 5", got)
	}
	if got := s.Dropped("test", slog.LevelWarn); got != 0 {
		t.Fatalf("Warn 丢弃计数 = %d, want 0", got)
	}
	if got := s.Dropped("test", slog.LevelError); got != 0 {
		t.Fatalf("Error 丢弃计数 = %d, want 0", got)
	}
	if got := s.Dropped("test", slog.LevelInfo); got != 1 {
		t.Fatalf("Info 丢弃计数 = %d, want 1", got)
	}
}

// TestSamplerEnvRates 断言 env 采样率生效,非法值退默认且不阻断构造。
func TestSamplerEnvRates(t *testing.T) {
	t.Setenv(infoSampleRateEnv, "0.25")
	t.Setenv(debugSampleRateEnv, "0.5")
	s := NewAgentSampler("agentd")
	if s.infoRate != 0.25 || s.debugRate != 0.5 {
		t.Fatalf("env 采样率 = (%v,%v), want (0.25,0.5)", s.infoRate, s.debugRate)
	}
	if s.rate != agentRate || s.burst != agentBurst {
		t.Fatalf("Agent 令牌桶 = (%v,%v), want (%v,%v)", s.rate, s.burst, agentRate, agentBurst)
	}

	for _, bad := range []string{"bad", "-1", "1.5", "NaN"} {
		t.Setenv(infoSampleRateEnv, bad)
		t.Setenv(debugSampleRateEnv, bad)
		s := NewControlSampler("controller-manager")
		if s.infoRate != defaultInfoRate || s.debugRate != defaultDebugRate {
			t.Fatalf("非法值 %q 应退默认,got (%v,%v)", bad, s.infoRate, s.debugRate)
		}
		if s.rate != controlRate || s.burst != controlBurst {
			t.Fatalf("控制面令牌桶 = (%v,%v), want (%v,%v)", s.rate, s.burst, controlRate, controlBurst)
		}
	}
}
