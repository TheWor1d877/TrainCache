package obs

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// recHandler 把每条日志的字段送进 channel，便于按窗口断言。
type recHandler struct {
	ch chan map[string]any
}

func (h *recHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recHandler) Handle(_ context.Context, r slog.Record) error {
	m := map[string]any{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	select {
	case h.ch <- m:
	default:
	}
	return nil
}

func (h *recHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recHandler) WithGroup(string) slog.Handler      { return h }

func newTestLogger(interval time.Duration) (*Logger, *recHandler) {
	h := &recHandler{ch: make(chan map[string]any, 32)}
	return New(interval, slog.New(h)), h
}

// TestSummaryFields 覆盖九个字段与 datasets 计数。
func TestSummaryFields(t *testing.T) {
	l, h := newTestLogger(300 * time.Millisecond)
	defer l.Stop()

	l.Get("ds-a")
	l.Get("ds-b")
	l.Merged("ds-a")
	l.Owner("ds-a")
	l.Passthrough()
	l.Kicked()
	l.Rejected()
	l.Bytes(123)

	select {
	case m := <-h.ch:
		if m["msg"] != SummaryMsg {
			t.Fatalf("msg = %v, want %q", m["msg"], SummaryMsg)
		}
		want := map[string]int64{
			"gets":        2,
			"merged":      1,
			"owners":      1,
			"passthrough": 1,
			"kicked":      1,
			"rejected":    1,
			"bytes":       123,
			"datasets":    2,
		}
		for k, v := range want {
			got, ok := m[k].(int64)
			if !ok {
				t.Fatalf("字段 %s 类型 = %T, want int64", k, m[k])
			}
			if got != v {
				t.Fatalf("字段 %s = %d, want %d", k, got, v)
			}
		}
		if _, ok := m["window_s"]; !ok {
			t.Fatal("缺少 window_s 字段")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未输出 60s 汇总行")
	}
}

// TestWindowBoundaryNoLoss 覆盖窗口切换：每个 Get 恰好计数一次，不重不漏。
func TestWindowBoundaryNoLoss(t *testing.T) {
	l, h := newTestLogger(30 * time.Millisecond)
	defer l.Stop()

	l.Get("a")
	first := waitRecord(t, h)
	if got := first["gets"].(int64); got != 1 {
		t.Fatalf("第一窗口 gets = %d, want 1", got)
	}

	l.Get("b")
	second := waitRecord(t, h)
	if got := second["gets"].(int64); got != 1 {
		t.Fatalf("第二窗口 gets = %d, want 1（边界处不重不漏）", got)
	}
	if got := second["datasets"].(int64); got != 1 {
		t.Fatalf("第二窗口 datasets = %d, want 1", got)
	}
}

func waitRecord(t *testing.T, h *recHandler) map[string]any {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case m := <-h.ch:
			if m["msg"] == SummaryMsg {
				return m
			}
		case <-deadline:
			t.Fatal("等待汇总行超时")
		}
	}
}
