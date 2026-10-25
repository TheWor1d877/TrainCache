package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
)

// capacityBytesName 是 traincache_cache_capacity_bytes_total（与 families.go 冻结名一致）。
const capacityBytesName = "traincache_cache_capacity_bytes_total"

// TestTraincacheCapacityBytesGauge:SetCapacityBytes 按 source 独立设置 gauge 值;构造预置
// 两个 source(disk/bytes)均为 0;CapacityBytes() 测试访问器返回非 nil。
func TestTraincacheCapacityBytesGauge(t *testing.T) {
	r := metrics.New(metrics.Config{Node: "node-1"})

	// 构造预置:两个合法 source 子指标自启动即存在且为 0。
	snap := snapshotFamilies(t, r)
	if m := metric(t, snap, capacityBytesName, labels("source", "disk")); m.value != 0 {
		t.Errorf("构造预置 capacity_bytes{disk} = %v, want 0", m.value)
	}
	if m := metric(t, snap, capacityBytesName, labels("source", "bytes")); m.value != 0 {
		t.Errorf("构造预置 capacity_bytes{bytes} = %v, want 0", m.value)
	}

	// ① 设置 disk = 8 GiB → 断言值正确、bytes 子指标仍为 0。
	r.SetCapacityBytes(metrics.CapacitySourceDisk, 8589934592)
	if got := testutil.ToFloat64(r.CapacityBytes().WithLabelValues(metrics.CapacitySourceDisk)); got != 8589934592 {
		t.Errorf("SetCapacityBytes(disk, 8GiB) → %v, want 8589934592", got)
	}
	if got := testutil.ToFloat64(r.CapacityBytes().WithLabelValues(metrics.CapacitySourceBytes)); got != 0 {
		t.Errorf("disk 设置后 bytes 应仍为 0,实际 %v", got)
	}

	// ② 设置 bytes = 1 GiB → 断言两个 source 独立存值互不影响。
	r.SetCapacityBytes(metrics.CapacitySourceBytes, 1073741824)
	if got := testutil.ToFloat64(r.CapacityBytes().WithLabelValues(metrics.CapacitySourceDisk)); got != 8589934592 {
		t.Errorf("bytes 设置后 disk 应仍为 8GiB,实际 %v", got)
	}
	if got := testutil.ToFloat64(r.CapacityBytes().WithLabelValues(metrics.CapacitySourceBytes)); got != 1073741824 {
		t.Errorf("SetCapacityBytes(bytes, 1GiB) → %v, want 1073741824", got)
	}

	// ③ 覆盖同一个 source:再 Set disk → 新值生效,bytes 不变。
	r.SetCapacityBytes(metrics.CapacitySourceDisk, 42)
	if got := testutil.ToFloat64(r.CapacityBytes().WithLabelValues(metrics.CapacitySourceDisk)); got != 42 {
		t.Errorf("覆盖 disk → %v, want 42", got)
	}
	if got := testutil.ToFloat64(r.CapacityBytes().WithLabelValues(metrics.CapacitySourceBytes)); got != 1073741824 {
		t.Errorf("覆盖 disk 后 bytes 应不变,实际 %v", got)
	}

	// ④ 非法 source 被忽略:不 panic、不新增 series、值不变。
	r.SetCapacityBytes("bogus", 99999)
	r.SetCapacityBytes("", 99999)
	snap = snapshotFamilies(t, r)
	if len(family(t, snap, capacityBytesName).metrics) != 2 {
		t.Errorf("非法 source 不应新增 series,实际 %d 个(%v)",
			len(family(t, snap, capacityBytesName).metrics),
			labelSetsString(family(t, snap, capacityBytesName)))
	}
	if got := testutil.ToFloat64(r.CapacityBytes().WithLabelValues(metrics.CapacitySourceDisk)); got != 42 {
		t.Errorf("非法 source 后 disk 应仍为 42,实际 %v", got)
	}

	// ⑤ 测试访问器返回非 nil。
	if r.CapacityBytes() == nil {
		t.Error("CapacityBytes() 访问器返回 nil")
	}
}
