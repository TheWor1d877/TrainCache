package collector

import (
	"testing"
	"time"
)

// hitrate_test.go:滚动窗口(分钟桶环形)与阶段聚合(§5.5)。

// TestHitRateRollingWindow:窗口内 命中/(命中+未命中);超出窗口的桶滚出。
func TestHitRateRollingWindow(t *testing.T) {
	h := newHitRate(3 * time.Minute)
	h.observe(baseTime, "", true)
	h.observe(baseTime, "", false)
	if got := h.rate(); got != 0.5 {
		t.Errorf("窗口内 1 命中 1 未命中 rate = %v, want 0.5", got)
	}

	// 4 分钟后旧桶滚出(last-m0 = 4 ≥ 3)。
	h.observe(baseTime.Add(4*time.Minute), "", true)
	if got := h.rate(); got != 1 {
		t.Errorf("旧桶滚出后 rate = %v, want 1", got)
	}
	// 同分钟再记一次未命中。
	h.observe(baseTime.Add(4*time.Minute+30*time.Second), "", false)
	if got := h.rate(); got != 0.5 {
		t.Errorf("同分钟追加未命中后 rate = %v, want 0.5", got)
	}
}

// TestHitRateWindowBoundary:距最近观测恰为窗口长度的桶不计入(≥ 判定)。
func TestHitRateWindowBoundary(t *testing.T) {
	h := newHitRate(3 * time.Minute)
	h.observe(baseTime, "", true)
	h.observe(baseTime.Add(3*time.Minute), "", false) // last-m0 = 3 ≥ 3:旧桶出窗
	if got := h.rate(); got != 0 {
		t.Errorf("恰在窗外的样本 rate = %v, want 0", got)
	}
}

// TestHitRateEmptyZero:无样本 → 0;window ≤0 取包内默认。
func TestHitRateEmptyZero(t *testing.T) {
	h := newHitRate(0)
	if got := h.rate(); got != 0 {
		t.Errorf("空窗口 rate = %v, want 0", got)
	}
	if got := h.stageRate("epoch-1"); got != 0 {
		t.Errorf("空阶段 stageRate = %v, want 0", got)
	}
}

// TestHitRateStageAggregation:阶段聚合为进程内累计(不随窗口滚动);空阶段不入聚合。
func TestHitRateStageAggregation(t *testing.T) {
	h := newHitRate(time.Minute)
	h.observe(baseTime, "epoch-1", true)
	h.observe(baseTime, "epoch-1", true)
	h.observe(baseTime, "epoch-1", false)
	h.observe(baseTime, "epoch-2", false)
	h.observe(baseTime, "", true) // 空阶段:仅入窗口

	if got := h.rate(); got != 0.6 {
		t.Errorf("窗口 rate = %v, want 0.6(3 命中 2 未命中,含 epoch-2 的空阶段样本)", got)
	}
	if got, want := h.stageRate("epoch-1"), 2.0/3.0; got != want {
		t.Errorf("stageRate(epoch-1) = %v, want %v", got, want)
	}
	if got := h.stageRate("epoch-2"); got != 0 {
		t.Errorf("stageRate(epoch-2) = %v, want 0", got)
	}
	if got := h.stageRate(""); got != 0 {
		t.Errorf("空阶段 stageRate = %v, want 0(不入聚合)", got)
	}

	// 一小时后再观测:窗口样本已滚出,阶段聚合保留。
	h.observe(baseTime.Add(time.Hour), "epoch-1", true)
	if got := h.rate(); got != 1 {
		t.Errorf("滚动后窗口 rate = %v, want 1", got)
	}
	if got, want := h.stageRate("epoch-1"), 3.0/4.0; got != want {
		t.Errorf("滚动后 stageRate(epoch-1) = %v, want %v", got, want)
	}
}
