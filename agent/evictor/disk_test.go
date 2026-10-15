package evictor

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

// TestStatfsUsage:默认 statfs 实现——临时目录取到 total > 0 且 used ≤ total;
// 不存在路径返回错误(不 panic)。
func TestStatfsUsage(t *testing.T) {
	u := statfsUsage(t.TempDir())
	used, total, err := u()
	if err != nil {
		t.Fatalf("statfsUsage: %v", err)
	}
	if total == 0 {
		t.Error("total = 0, want > 0")
	}
	if used > total {
		t.Errorf("used = %d > total = %d(want used ≤ total)", used, total)
	}

	u2 := statfsUsage(filepath.Join(t.TempDir(), "no-such-dir"))
	if _, _, err := u2(); err == nil {
		t.Error("不存在目录应返回错误")
	}
}

// TestRatioZeroTotal:利用率除零防御(total == 0 → 0)。
func TestRatioZeroTotal(t *testing.T) {
	if got := ratio(0, 0); got != 0 {
		t.Errorf("ratio(0, 0) = %v, want 0", got)
	}
	if got := ratio(1, 0); got != 0 {
		t.Errorf("ratio(1, 0) = %v, want 0", got)
	}
	if got := ratio(85, 100); got != 0.85 {
		t.Errorf("ratio(85, 100) = %v, want 0.85", got)
	}
}

// TestFreshnessHalfLife:新鲜度 R = 0.5^(Δt/6h) 的半衰期语义(D24):每过一个 6h
// 减半;LastTouch 为零值 → 下溢为 0。
func TestFreshnessHalfLife(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		ago  time.Duration
		want float64
	}{
		{0, 1},
		{6 * time.Hour, 0.5},
		{12 * time.Hour, 0.25},
		{24 * time.Hour, 0.0625},
	} {
		if got := freshness(now, now.Add(-tc.ago)); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("freshness(Δt=%v) = %v, want %v", tc.ago, got, tc.want)
		}
	}
	if got := freshness(now, time.Time{}); got != 0 {
		t.Errorf("freshness(零值 LastTouch) = %v, want 0", got)
	}
}
