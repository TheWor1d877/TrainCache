package collector

import (
	"testing"

	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// calibrate_test.go:交叉校准(§5.5/D15)——滑动窗口内实际打开顺序与订单期望顺序的
// 匹配率 < 阈值 → 触发 cursor 重对齐(回调收到 dataset 与实际 ref)。

// refChunk 构造与 testOrder 同 Path 的块引用(ChunkRef 可比较,Path+Chunk 全等才算命中)。
func refChunk(c int64) ordersource.ChunkRef {
	return ordersource.ChunkRef{Path: "shard-000.tar", Chunk: c}
}

// TestCalibrateSequentialMatchNoTrigger:按期望顺序逐块打开,匹配率 100% → 不触发。
func TestCalibrateSequentialMatchNoTrigger(t *testing.T) {
	k := newCalibrator(10, 0.8)
	order := testOrder("b-track", "epoch-1", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	for i := int64(0); i < 10; i++ {
		trig, _ := k.observe(order, refChunk(i))
		if trig {
			t.Fatalf("第 %d 次顺序匹配即触发重对齐(不应触发)", i+1)
		}
	}
	if got := k.count(); got != 0 {
		t.Errorf("触发次数 = %d, want 0", got)
	}
}

// TestCalibrateMismatchTriggers:窗口填满后匹配率 0 < 0.8 → 触发,回调收到实际位点。
func TestCalibrateMismatchTriggers(t *testing.T) {
	k := newCalibrator(10, 0.8)
	order := testOrder("b-track", "epoch-1", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	triggered, chunk := false, int64(0)
	for i := 0; i < 10; i++ {
		triggered, chunk = k.observe(order, refChunk(5)) // 期望 0..9,实际全开 5:全不匹配
		if triggered {
			break
		}
	}
	if !triggered {
		t.Fatal("窗口填满且匹配率 0,未触发重对齐")
	}
	if chunk != 5 {
		t.Errorf("触发回调位点 = %d, want 5(实际打开的 chunk)", chunk)
	}
	if got := k.count(); got != 1 {
		t.Errorf("触发次数 = %d, want 1", got)
	}
}

// TestCalibrateExactThresholdNoTrigger:匹配率恰为阈值(8/10 = 0.8)不触发;
// 滑动一次后跌破(7/10)→ 触发——同时覆盖窗口覆写语义。
func TestCalibrateExactThresholdNoTrigger(t *testing.T) {
	k := newCalibrator(10, 0.8)
	order := testOrder("b-track", "epoch-1", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	for i := int64(0); i < 8; i++ { // 8 匹配
		if trig, _ := k.observe(order, refChunk(i)); trig {
			t.Fatalf("第 %d 次观测提前触发", i+1)
		}
	}
	for i := 0; i < 2; i++ { // 2 不匹配(位点回退重定位,不计匹配)
		if trig, _ := k.observe(order, refChunk(3)); trig {
			t.Fatalf("匹配率恰为阈值 0.8 时触发(应不触发),第 %d 次", 9+i)
		}
	}
	if got := k.count(); got != 0 {
		t.Fatalf("恰好 0.8 时触发次数 = %d, want 0", got)
	}
	// 第 11 次:窗口覆写最旧的匹配 → 7/10 = 0.7 < 0.8 → 触发。
	trig, chunk := k.observe(order, refChunk(3))
	if !trig {
		t.Fatal("滑动后匹配率 0.7 未触发")
	}
	if chunk != 3 {
		t.Errorf("触发位点 = %d, want 3", chunk)
	}
}

// TestCalibrateStageChangeRealigns:阶段切换后 chunk 从头开始——重定位后继续匹配,
// 单次错位不足以把整窗拖到阈值之下(不误触发)。
func TestCalibrateStageChangeRealigns(t *testing.T) {
	k := newCalibrator(10, 0.8)
	old := testOrder("b-track", "epoch-1", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	for i := int64(0); i < 5; i++ {
		if trig, _ := k.observe(old, refChunk(i)); trig {
			t.Fatalf("旧阶段内提前触发(%d)", i+1)
		}
	}
	next := testOrder("b-track", "epoch-2", 0, 1, 2, 3, 4)
	for i := int64(0); i < 5; i++ {
		if trig, _ := k.observe(next, refChunk(i)); trig {
			t.Fatalf("阶段切换后误触发(第 %d 次)", i+1)
		}
	}
	if got := k.count(); got != 0 {
		t.Errorf("阶段切换误触发次数 = %d, want 0(重定位后 9/10 ≥ 0.8)", got)
	}
}

// TestCollectorCalibrationTriggersAdvanceCallback:接线验证——Collector.Open 经 provider
// 取订单期望顺序,匹配率跌破阈值时以已解析的 dataset 与本次 ref 调重对齐回调。
func TestCollectorCalibrationTriggersAdvanceCallback(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{CalibrateWindow: 4, CalibrateThreshold: 0.8})
	order := testOrder("b-track", "epoch-1", 0, 1, 2, 3)
	env.c.SetOrderContextProvider(func(ref ordersource.ChunkRef, fp cas.Fingerprint) (string, string, ordersource.ConsumeOrder, bool) {
		return order.Dataset, order.StageID, order, true
	})

	type advance struct {
		dataset string
		ref     ordersource.ChunkRef
	}
	var got []advance
	env.c.SetAdvanceCallback(func(dataset string, ref ordersource.ChunkRef) {
		got = append(got, advance{dataset: dataset, ref: ref})
	})
	for i := 0; i < 4; i++ { // 期望 0..3,实际全开 chunk 2:全不匹配
		env.c.Open(testFP(byte(0xe0+i)), refChunk(2))
	}
	if len(got) != 1 || got[0].dataset != "b-track" || got[0].ref != refChunk(2) {
		t.Errorf("重对齐回调收到 %+v, want [{b-track %v}](校准重对齐到实际位点)", got, refChunk(2))
	}
	if n := env.c.cal.count(); n != 1 {
		t.Errorf("校准触发次数 = %d, want 1", n)
	}
}
