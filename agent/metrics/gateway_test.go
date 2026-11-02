package metrics_test

import (
	"maps"
	"testing"

	"traincache/agent/metrics"
)

// TestSourceModeGauge:traincache_source_mode 构造预置 0(直连);SetSourceMode(1) 置 1;
// 集合外取值(7、-1)忽略该次调用,值不变(与现有"集合外取值忽略"同风格)。
func TestSourceModeGauge(t *testing.T) {
	r := metrics.New(metrics.Config{Node: "node-1"})

	if got := metric(t, snapshotFamilies(t, r), sourceModeName, labels()).value; got != 0 {
		t.Fatalf("构造预置 source_mode = %v, want 0(直连)", got)
	}

	r.SetSourceMode(1)
	if got := metric(t, snapshotFamilies(t, r), sourceModeName, labels()).value; got != 1 {
		t.Errorf("SetSourceMode(1) 后 = %v, want 1(网关)", got)
	}

	r.SetSourceMode(7)
	if got := metric(t, snapshotFamilies(t, r), sourceModeName, labels()).value; got != 1 {
		t.Errorf("SetSourceMode(7) 后 = %v, want 1 不变(非法取值忽略)", got)
	}

	r.SetSourceMode(-1)
	if got := metric(t, snapshotFamilies(t, r), sourceModeName, labels()).value; got != 1 {
		t.Errorf("SetSourceMode(-1) 后 = %v, want 1 不变(非法取值忽略)", got)
	}

	r.SetSourceMode(0)
	if got := metric(t, snapshotFamilies(t, r), sourceModeName, labels()).value; got != 0 {
		t.Errorf("SetSourceMode(0) 后 = %v, want 0(直连)", got)
	}
}

// TestGatewayFallbackCounter:构造预置 reason∈{unavailable,timeout,transport} 三个子指标为 0;
// IncGatewayFallback 按 reason 计数;集合外 reason 忽略,不新增子指标。
func TestGatewayFallbackCounter(t *testing.T) {
	r := metrics.New(metrics.Config{Node: "node-1"})

	snap := snapshotFamilies(t, r)
	f := family(t, snap, gatewayFallbackName)
	if f.typ != "COUNTER" {
		t.Errorf("family %q 类型 = %q, want COUNTER", gatewayFallbackName, f.typ)
	}
	// 构造预置:标签集合恰为 {reason},取值集合恰为三个冻结 reason 且全为 0。
	got := make(map[string]float64, len(f.metrics))
	for _, m := range f.metrics {
		if len(m.labels) != 1 {
			t.Errorf("子指标标签 = %v, want 仅 reason", m.labels)
		}
		got[m.labels["reason"]] = m.value
	}
	if want := map[string]float64{"unavailable": 0, "timeout": 0, "transport": 0}; !maps.Equal(got, want) {
		t.Fatalf("构造预置 = %v, want %v", got, want)
	}

	r.IncGatewayFallback(metrics.GatewayReasonTimeout)
	r.IncGatewayFallback(metrics.GatewayReasonTimeout)
	r.IncGatewayFallback(metrics.GatewayReasonUnavailable)

	snap = snapshotFamilies(t, r)
	for reason, want := range map[string]float64{"unavailable": 1, "timeout": 2, "transport": 0} {
		if v := metric(t, snap, gatewayFallbackName, labels("reason", reason)).value; v != want {
			t.Errorf("reason=%q = %v, want %v", reason, v, want)
		}
	}

	// 集合外 reason(含空串)忽略:不新增子指标、不 panic。
	r.IncGatewayFallback("bogus")
	r.IncGatewayFallback("")
	f = family(t, snapshotFamilies(t, r), gatewayFallbackName)
	if len(f.metrics) != 3 {
		t.Errorf("集合外 reason 不应新增子指标,实际 %d 个(%v)", len(f.metrics), labelSetsString(f))
	}
	// reason 子指标集合恰为三个冻结取值。
	reasons := make(map[string]bool, len(f.metrics))
	for _, m := range f.metrics {
		reasons[m.labels["reason"]] = true
	}
	if want := valueSet(
		metrics.GatewayReasonUnavailable,
		metrics.GatewayReasonTimeout,
		metrics.GatewayReasonTransport,
	); !maps.Equal(reasons, want) {
		t.Errorf("reason 子指标集合 = %v, want %v", reasons, want)
	}
}
