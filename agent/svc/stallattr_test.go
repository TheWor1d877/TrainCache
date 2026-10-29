package svc

import (
	"testing"
	"time"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// stallattr_test.go:M9/D47 read_stall 归因(100ms 阈值、可配关联窗口、四类 cause)。

// fixedStallNow 构造固定时间源。
func fixedStallNow(at time.Time) func() time.Time { return func() time.Time { return at } }

var stallTestNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// TestStallThreshold:wait <100ms 不记录;=100ms 记录(q15 固定阈值)。
func TestStallThreshold(t *testing.T) {
	a := newStallAttributor(
		func(ordersource.ChunkRef) (string, bool) { return "ds", false },
		func(string, ordersource.ChunkRef) bool { return false },
		func(string, ordersource.ChunkRef) bool { return false },
	)
	a.now = fixedStallNow(stallTestNow)
	ref := phaseRef("p", 1)

	if _, record := a.Attribute(ref, 99, stallTestNow); record {
		t.Error("wait=99ms 不应记录")
	}
	for _, wait := range []int64{100, 101, 5000} {
		cause, record := a.Attribute(ref, wait, stallTestNow)
		if !record {
			t.Errorf("wait=%dms 应记录", wait)
		}
		if cause != metrics.CausePrefetchLag {
			t.Errorf("wait=%dms cause = %q, want prefetch_lag", wait, cause)
		}
	}
}

// TestStallThresholdConfigurable:自定义阈值 50ms 时 60ms 记录、40ms 不记录
// (D47/Q15:100ms 仅为默认值,装配期可注入)。
func TestStallThresholdConfigurable(t *testing.T) {
	a := newStallAttributorWithThreshold(
		func(ordersource.ChunkRef) (string, bool) { return "ds", false },
		func(string, ordersource.ChunkRef) bool { return false },
		func(string, ordersource.ChunkRef) bool { return false },
		50,
		defaultReadStallWindowMS,
	)
	a.now = fixedStallNow(stallTestNow)
	ref := phaseRef("p", 1)

	if _, record := a.Attribute(ref, 40, stallTestNow); record {
		t.Error("阈值 50ms 下 wait=40ms 不应记录")
	}
	if _, record := a.Attribute(ref, 60, stallTestNow); !record {
		t.Error("阈值 50ms 下 wait=60ms 应记录")
	}
}

// TestStallCorrelationWindow:默认窗口 5000ms 表示帧 ts 前后各 5000ms;
// ts 为零值(解析失败)或 |now-ts|>5000ms → unknown 且记录;恰为 ±5000ms 仍在窗内。
func TestStallCorrelationWindow(t *testing.T) {
	a := newStallAttributor(
		func(ordersource.ChunkRef) (string, bool) { return "ds", true },
		func(string, ordersource.ChunkRef) bool { return false },
		func(string, ordersource.ChunkRef) bool { return false },
	)
	a.now = fixedStallNow(stallTestNow)
	ref := phaseRef("p", 1)

	half := time.Duration(defaultReadStallWindowMS) * time.Millisecond
	for _, ts := range []time.Time{
		{},
		stallTestNow.Add(half + time.Nanosecond),
		stallTestNow.Add(-half - time.Nanosecond),
	} {
		cause, record := a.Attribute(ref, 150, ts)
		if !record || cause != metrics.CauseUnknown {
			t.Errorf("ts=%v → (%q, record=%v), want (unknown,true)", ts, cause, record)
		}
	}
	for _, ts := range []time.Time{
		stallTestNow.Add(half),
		stallTestNow.Add(-half),
	} {
		cause, record := a.Attribute(ref, 150, ts)
		if !record || cause != metrics.CauseLocalSlow {
			t.Errorf("ts=%v → (%q, record=%v), want (local_slow,true)", ts, cause, record)
		}
	}
}

// TestStallCorrelationWindowConfigurable:显式窗口 1000ms 时边界为帧 ts 前后各 1000ms
// (C3:窗口可配,恰在边界内可关联,越界 1ns 记 unknown)。
func TestStallCorrelationWindowConfigurable(t *testing.T) {
	a := newStallAttributorWithThreshold(
		func(ordersource.ChunkRef) (string, bool) { return "ds", true },
		func(string, ordersource.ChunkRef) bool { return false },
		func(string, ordersource.ChunkRef) bool { return false },
		stallMinWaitMS,
		1000,
	)
	a.now = fixedStallNow(stallTestNow)
	ref := phaseRef("p", 1)

	for _, ts := range []time.Time{
		stallTestNow.Add(1000 * time.Millisecond),
		stallTestNow.Add(-1000 * time.Millisecond),
	} {
		cause, record := a.Attribute(ref, 150, ts)
		if !record || cause != metrics.CauseLocalSlow {
			t.Errorf("窗口边界 ts=%v → (%q, record=%v), want (local_slow,true)", ts, cause, record)
		}
	}
	for _, ts := range []time.Time{
		stallTestNow.Add(1000*time.Millisecond + time.Nanosecond),
		stallTestNow.Add(-1000*time.Millisecond - time.Nanosecond),
	} {
		cause, record := a.Attribute(ref, 150, ts)
		if !record || cause != metrics.CauseUnknown {
			t.Errorf("窗口外 ts=%v → (%q, record=%v), want (unknown,true)", ts, cause, record)
		}
	}
}

// TestStallCauses:四类归因按 D47 优先级。
func TestStallCauses(t *testing.T) {
	cases := []struct {
		name    string
		exists  func(ordersource.ChunkRef) (string, bool)
		current func(string, ordersource.ChunkRef) bool
		flight  func(string, ordersource.ChunkRef) bool
		want    string
	}{
		{
			name:    "本地存在且非当前窗口 → local_slow",
			exists:  func(ordersource.ChunkRef) (string, bool) { return "ds", true },
			current: func(string, ordersource.ChunkRef) bool { return false },
			flight:  func(string, ordersource.ChunkRef) bool { return false },
			want:    metrics.CauseLocalSlow,
		},
		{
			name:    "未本地且 fetch 在途 → origin_slow",
			exists:  func(ordersource.ChunkRef) (string, bool) { return "ds", false },
			current: func(string, ordersource.ChunkRef) bool { return false },
			flight:  func(string, ordersource.ChunkRef) bool { return true },
			want:    metrics.CauseOriginSlow,
		},
		{
			name:    "本地当前窗口但在途 → origin_slow",
			exists:  func(ordersource.ChunkRef) (string, bool) { return "ds", true },
			current: func(string, ordersource.ChunkRef) bool { return true },
			flight:  func(string, ordersource.ChunkRef) bool { return true },
			want:    metrics.CauseOriginSlow,
		},
		{
			name:    "未本地且未在途 → prefetch_lag",
			exists:  func(ordersource.ChunkRef) (string, bool) { return "ds", false },
			current: func(string, ordersource.ChunkRef) bool { return false },
			flight:  func(string, ordersource.ChunkRef) bool { return false },
			want:    metrics.CausePrefetchLag,
		},
		{
			name:    "本地当前窗口且不在途 → unknown",
			exists:  func(ordersource.ChunkRef) (string, bool) { return "ds", true },
			current: func(string, ordersource.ChunkRef) bool { return true },
			flight:  func(string, ordersource.ChunkRef) bool { return false },
			want:    metrics.CauseUnknown,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := newStallAttributor(c.exists, c.flight, c.current)
			a.now = fixedStallNow(stallTestNow)
			cause, record := a.Attribute(phaseRef("p", 1), 120, stallTestNow)
			if !record {
				t.Fatal("应记录")
			}
			if cause != c.want {
				t.Errorf("cause = %q, want %q", cause, c.want)
			}
		})
	}
}

// TestStallDatasetUnresolved:exists 取不到 dataset(回调 nil / 返回空)→ unknown。
func TestStallDatasetUnresolved(t *testing.T) {
	now := fixedStallNow(stallTestNow)
	ref := phaseRef("p", 1)

	a := newStallAttributor(nil, nil, nil)
	a.now = now
	if cause, record := a.Attribute(ref, 120, stallTestNow); !record || cause != metrics.CauseUnknown {
		t.Errorf("无 exists 回调 → (%q, %v), want (unknown,true)", cause, record)
	}

	a = newStallAttributor(
		func(ordersource.ChunkRef) (string, bool) { return "", false },
		func(string, ordersource.ChunkRef) bool { return false },
		func(string, ordersource.ChunkRef) bool { return false },
	)
	a.now = now
	if cause, record := a.Attribute(ref, 120, stallTestNow); !record || cause != metrics.CauseUnknown {
		t.Errorf("dataset 为空 → (%q, %v), want (unknown,true)", cause, record)
	}
}

// TestStallCallbacksReceiveDataset:归因依赖收到 exists 解析出的 dataset。
func TestStallCallbacksReceiveDataset(t *testing.T) {
	var gotDataset string
	var gotInflight, gotWindow bool
	a := newStallAttributor(
		func(ordersource.ChunkRef) (string, bool) { return "ds-42", false },
		func(ds string, _ ordersource.ChunkRef) bool { gotDataset = ds; gotInflight = true; return true },
		func(ds string, _ ordersource.ChunkRef) bool { gotWindow = true; return false },
	)
	a.now = fixedStallNow(stallTestNow)
	if cause, _ := a.Attribute(phaseRef("p", 1), 100, stallTestNow); cause != metrics.CauseOriginSlow {
		t.Fatalf("cause = %q, want origin_slow", cause)
	}
	if gotDataset != "ds-42" || !gotInflight || !gotWindow {
		t.Errorf("回调 dataset=%q inflight=%v window=%v, want ds-42/true/true", gotDataset, gotInflight, gotWindow)
	}
}
