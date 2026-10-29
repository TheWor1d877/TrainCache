package logging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

// TestClassify 断言通用分类只认超时与取消,其余 io 兜底。
func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline", fmt.Errorf("read chunk: %w", context.DeadlineExceeded), ErrClassTimeout},
		{"canceled", context.Canceled, ErrClassUnavailable},
		{"io", errors.New("disk full"), ErrClassIO},
		{"nil", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.err); got != c.want {
				t.Fatalf("Classify(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

func recordValue(r slog.Record, key string) (slog.Value, bool) {
	var value slog.Value
	var ok bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value, ok = a.Value.Resolve(), true
			return false
		}
		return true
	})
	return value, ok
}

func recordStrings(r slog.Record, key string) []string {
	var values []string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			values = append(values, a.Value.Resolve().String())
		}
		return true
	})
	return values
}

// TestSamplingHandlerSampleFields 断言每条放行记录都带 sampled/sample_rate,
// 且 Info/Debug 的取值随级别采样率变化。
func TestSamplingHandlerSampleFields(t *testing.T) {
	s, _ := newTestSampler("test", 1000, 1000, 0.25, 0.5)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	log.Warn("warn kept")
	warn := capture.last()
	if v, ok := recordValue(warn, "sampled"); !ok || v.Bool() {
		t.Fatalf("Warn sampled = %v (ok=%v), want false", v, ok)
	}
	if v, ok := recordValue(warn, "sample_rate"); !ok || v.Float64() != 1.0 {
		t.Fatalf("Warn sample_rate = %v (ok=%v), want 1.0", v, ok)
	}

	log.Info("info sampled")
	info := capture.last()
	if v, ok := recordValue(info, "sampled"); !ok || !v.Bool() {
		t.Fatalf("Info sampled = %v (ok=%v), want true", v, ok)
	}
	if v, ok := recordValue(info, "sample_rate"); !ok || v.Float64() != 0.25 {
		t.Fatalf("Info sample_rate = %v (ok=%v), want 0.25", v, ok)
	}

	log.Debug("debug sampled")
	debug := capture.last()
	if v, ok := recordValue(debug, "sampled"); !ok || !v.Bool() {
		t.Fatalf("Debug sampled = %v (ok=%v), want true", v, ok)
	}
	if v, ok := recordValue(debug, "sample_rate"); !ok || v.Float64() != 0.5 {
		t.Fatalf("Debug sample_rate = %v (ok=%v), want 0.5", v, ok)
	}
}

// TestSamplingHandlerFullRateNotSampled 断言采样率为 1 时 Info 也标 sampled=false。
func TestSamplingHandlerFullRateNotSampled(t *testing.T) {
	s, _ := newTestSampler("test", 1000, 1000, 1, 1)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	log.Info("info full rate")
	info := capture.last()
	if v, ok := recordValue(info, "sampled"); !ok || v.Bool() {
		t.Fatalf("采样率 1 时 Info sampled = %v (ok=%v), want false", v, ok)
	}
	if v, ok := recordValue(info, "sample_rate"); !ok || v.Float64() != 1.0 {
		t.Fatalf("采样率 1 时 Info sample_rate = %v (ok=%v), want 1.0", v, ok)
	}
}

// TestSamplingHandlerErrClassPair 断言有 err 必有 err_class,显式分类不被覆盖。
func TestSamplingHandlerErrClassPair(t *testing.T) {
	s, _ := newTestSampler("test", 1000, 1000, 1, 1)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	log.Warn("deadline", "err", fmt.Errorf("open: %w", context.DeadlineExceeded))
	if got := recordStrings(capture.last(), "err_class"); len(got) != 1 || got[0] != ErrClassTimeout {
		t.Fatalf("err_class = %v, want [%s]", got, ErrClassTimeout)
	}

	log.Warn("explicit", "err", errors.New("verify failed"), "err_class", ErrClassCorrupt)
	if got := recordStrings(capture.last(), "err_class"); len(got) != 1 || got[0] != ErrClassCorrupt {
		t.Fatalf("显式 err_class 应保留且不重复,got %v", got)
	}

	log.Warn("text err", "err", "bad spec")
	if got := recordStrings(capture.last(), "err_class"); len(got) != 1 || got[0] != ErrClassIO {
		t.Fatalf("字符串 err 的 err_class = %v, want [%s]", got, ErrClassIO)
	}

	log.Info("no err")
	if got := recordStrings(capture.last(), "err_class"); len(got) != 0 {
		t.Fatalf("无 err 不应出现 err_class,got %v", got)
	}
}

// TestSamplingHandlerZeroRateWarnFields 断言采样率为 0 时 Warn/Error 零丢失且
// 仍带 sampled/sample_rate。
func TestSamplingHandlerZeroRateWarnFields(t *testing.T) {
	s, _ := newTestSampler("test", 0, 0, 0, 0)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	log.Info("info dropped")
	log.Warn("warn kept")
	log.Error("error kept")
	if got := capture.count(); got != 2 {
		t.Fatalf("rate=0 时 Warn/Error 应全部放行,got %d 条, want 2", got)
	}
	for _, r := range capture.records {
		if v, ok := recordValue(r, "sampled"); !ok || v.Bool() {
			t.Fatalf("%s 的 sampled = %v (ok=%v), want false", r.Level, v, ok)
		}
		if v, ok := recordValue(r, "sample_rate"); !ok || v.Float64() != 1.0 {
			t.Fatalf("%s 的 sample_rate = %v (ok=%v), want 1.0", r.Level, v, ok)
		}
	}
}

// TestSamplingHandlerSuppressedWithSampleFields 断言去重的 suppressed 计数与
// 采样字段共存。
func TestSamplingHandlerSuppressedWithSampleFields(t *testing.T) {
	s, now := newTestSampler("test", 1000, 1000, 0.5, 1)
	capture := &captureHandler{}
	log := slog.New(NewSamplingHandler(capture, s)).With("component", "test")

	log.Info("dup")
	log.Info("dup")
	if got := capture.count(); got != 1 {
		t.Fatalf("窗口内重复应被抑制,got %d 条, want 1", got)
	}
	*now = now.Add(61 * time.Second)
	log.Info("dup")
	last := capture.last()
	if v, ok := recordValue(last, "suppressed"); !ok || v.Int64() != 1 {
		t.Fatalf("suppressed = %v (ok=%v), want 1", v, ok)
	}
	if v, ok := recordValue(last, "sampled"); !ok || !v.Bool() {
		t.Fatalf("带 suppressed 的记录 sampled = %v (ok=%v), want true", v, ok)
	}
}
