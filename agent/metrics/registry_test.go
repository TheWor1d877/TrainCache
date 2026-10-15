package metrics_test

import (
	"math"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"traincache/agent/metrics"
)

// family 名与实现侧一致(D23、M3;测试独立字面量,防改名后测试跟着漂移)。
const (
	ordersourceFormatErrorsName = "traincache_ordersource_format_errors_total"
	prefetchQueueDepthName      = "traincache_prefetch_queue_depth"
	prefetchCursorName          = "traincache_prefetch_cursor"
	stageSwitchStallName        = "traincache_stage_switch_stall_seconds"
)

func TestDatasetLabelWhitelistHit(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track", "b-track"}})

	for _, d := range []string{"a-track", "b-track"} {
		if got := r.DatasetLabel(d); got != d {
			t.Errorf("DatasetLabel(%q) = %q, want 白名单命中返回原名", d, got)
		}
	}
}

func TestDatasetLabelMiss(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})

	for _, d := range []string{"c-track", "", "other"} {
		if got := r.DatasetLabel(d); got != "other" {
			t.Errorf("DatasetLabel(%q) = %q, want 白名单外归并 %q", d, got, "other")
		}
	}
}

func TestDatasetLabelEmptyWhitelist(t *testing.T) {
	r := metrics.New(metrics.Config{})

	for _, d := range []string{"a-track", "other", ""} {
		if got := r.DatasetLabel(d); got != "other" {
			t.Errorf("DatasetLabel(%q) = %q, want 白名单为空时一律 %q", d, got, "other")
		}
	}
}

func TestPrometheusRegistry(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})

	reg := r.Prometheus()
	if reg == nil {
		t.Fatal("Prometheus() 返回 nil")
	}
	if reg != r.Prometheus() {
		t.Error("Prometheus() 应返回同一注册表")
	}

	// M3 起注册表恰含 4 个 family:ordersource 格式错误 counter(D23)、prefetch 队列深度/
	// 游标 gauge 与阶段切换停滞 histogram(M3);六线 family 仍属 M6,不在此注册。
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]string{
		ordersourceFormatErrorsName: "COUNTER",
		prefetchQueueDepthName:      "GAUGE",
		prefetchCursorName:          "GAUGE",
		stageSwitchStallName:        "HISTOGRAM",
	}
	if len(families) != len(want) {
		t.Fatalf("注册表应恰有 %d 个 family(M3),实际 %d 个", len(want), len(families))
	}
	byName := make(map[string]int, len(families))
	for i, mf := range families {
		byName[mf.GetName()] = i
	}
	for name, typ := range want {
		i, ok := byName[name]
		if !ok {
			t.Fatalf("缺少 family %q(实际 %v)", name, byName)
		}
		if got := families[i].GetType().String(); got != typ {
			t.Errorf("family %q 类型 = %q, want %q", name, got, typ)
		}
	}

	// 两个 reason 子指标自启动即存在(零值),标签取值编译期固定。
	reasons := make(map[string]bool)
	for _, m := range families[byName[ordersourceFormatErrorsName]].GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "reason" {
				reasons[lp.GetValue()] = true
			}
		}
		if got := m.GetCounter().GetValue(); got != 0 {
			t.Errorf("初始 counter 值 = %v, want 0", got)
		}
	}
	for _, want := range []string{"magic", "version"} {
		if !reasons[want] {
			t.Errorf("缺少 reason=%q 的子指标(实际 %v)", want, reasons)
		}
	}
	if len(reasons) != 2 {
		t.Errorf("reason 取值应恰为 {magic, version},实际 %v", reasons)
	}
}

func TestOrdersourceFormatErrorsSync(t *testing.T) {
	r := metrics.New(metrics.Config{})

	// 以绝对值同步:Sync(2,1) 后 Gather 数值与 reason 标签正确。
	r.SyncOrdersourceFormatErrors(2, 1)
	if magic, version := gatherFormatErrors(t, r); magic != 2 || version != 1 {
		t.Fatalf("Sync(2,1) 后 = (magic=%d, version=%d), want (2, 1)", magic, version)
	}

	// 重复同步同一绝对值幂等(counter 不因重复 Sync 增长)。
	r.SyncOrdersourceFormatErrors(2, 1)
	if magic, version := gatherFormatErrors(t, r); magic != 2 || version != 1 {
		t.Fatalf("重复 Sync(2,1) 后 = (magic=%d, version=%d), want (2, 1) 幂等", magic, version)
	}

	// 绝对值推进:按增量落到 5/3。
	r.SyncOrdersourceFormatErrors(5, 3)
	if magic, version := gatherFormatErrors(t, r); magic != 5 || version != 3 {
		t.Fatalf("Sync(5,3) 后 = (magic=%d, version=%d), want (5, 3)", magic, version)
	}

	// 测试访问器返回同一 CounterVec。
	if r.OrdersourceFormatErrors() == nil {
		t.Fatal("OrdersourceFormatErrors() 返回 nil")
	}
}

// TestPrefetchQueueDepthGauge：构造时预置 P0..P3 为 0;Set 后 Gather 值/标签正确;
// 越界优先级忽略(标签基数编译期固定)。
func TestPrefetchQueueDepthGauge(t *testing.T) {
	r := metrics.New(metrics.Config{})

	got := gatherLabeledGauge(t, r, prefetchQueueDepthName, "priority")
	if len(got) != 4 {
		t.Fatalf("priority 子指标数 = %d, want 4(P0..P3;构造时预置)", len(got))
	}
	for _, pri := range []string{"P0", "P1", "P2", "P3"} {
		if v, ok := got[pri]; !ok || v != 0 {
			t.Errorf("初始 queue_depth{%s} = %v(存在=%v), want 0", pri, v, ok)
		}
	}

	// 内部 0..3 映射 P0..P3。
	r.SetPrefetchQueueDepth(0, 3)
	r.SetPrefetchQueueDepth(3, 7)
	got = gatherLabeledGauge(t, r, prefetchQueueDepthName, "priority")
	if got["P0"] != 3 || got["P1"] != 0 || got["P2"] != 0 || got["P3"] != 7 {
		t.Errorf("Set 后队列深度 = %v, want P0=3 P1=0 P2=0 P3=7", got)
	}

	// 越界优先级忽略:不产生新子指标。
	r.SetPrefetchQueueDepth(4, 9)
	r.SetPrefetchQueueDepth(-1, 9)
	if got = gatherLabeledGauge(t, r, prefetchQueueDepthName, "priority"); len(got) != 4 {
		t.Fatalf("越界优先级不应新增子指标,实际 %d 个(%v)", len(got), got)
	}
}

// TestPrefetchCursorGauge：游标 gauge 初始 0,Set 后值正确。
func TestPrefetchCursorGauge(t *testing.T) {
	r := metrics.New(metrics.Config{})

	if got := gatherSingleGauge(t, r, prefetchCursorName); got != 0 {
		t.Errorf("初始 cursor = %v, want 0", got)
	}
	r.SetPrefetchCursor(7)
	if got := gatherSingleGauge(t, r, prefetchCursorName); got != 7 {
		t.Errorf("SetPrefetchCursor(7) 后 = %v, want 7", got)
	}
	r.SetPrefetchCursor(0)
	if got := gatherSingleGauge(t, r, prefetchCursorName); got != 0 {
		t.Errorf("SetPrefetchCursor(0) 后 = %v, want 0", got)
	}
}

// TestStageSwitchStallHistogram：Observe 后 count/sum 正确;bucket 为 prometheus.DefBuckets。
func TestStageSwitchStallHistogram(t *testing.T) {
	r := metrics.New(metrics.Config{})

	r.ObserveStageSwitchStall(0.25)
	r.ObserveStageSwitchStall(0.75)
	count, sum, upper := gatherStallHistogram(t, r)
	if count != 2 {
		t.Errorf("sample count = %d, want 2", count)
	}
	if sum != 1.0 {
		t.Errorf("sample sum = %v, want 1.0", sum)
	}
	// 桶上界(不含 +Inf)应与 prometheus.DefBuckets 逐一一致。
	if !slices.Equal(upper, prometheus.DefBuckets) {
		t.Errorf("bucket 上界 = %v, want prometheus.DefBuckets = %v", upper, prometheus.DefBuckets)
	}
}

// gatherLabeledGauge 读出指定 family 下按 label 区分的各 gauge 子指标值(标签缺失时以
// 空串为键)。只对 Gather 返回值调方法读取,不 import client_model(避免把 go.mod 中间接
// 依赖提升为直接依赖)。
func gatherLabeledGauge(t *testing.T, r *metrics.Registry, family, label string) map[string]float64 {
	t.Helper()
	families, err := r.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != family {
			continue
		}
		out := make(map[string]float64, len(mf.GetMetric()))
		for _, m := range mf.GetMetric() {
			val := ""
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label {
					val = lp.GetValue()
				}
			}
			out[val] = m.GetGauge().GetValue()
		}
		return out
	}
	t.Fatalf("family %q 不存在", family)
	return nil
}

// gatherSingleGauge 读出指定 family 唯一子指标的 gauge 值。
func gatherSingleGauge(t *testing.T, r *metrics.Registry, family string) float64 {
	t.Helper()
	families, err := r.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != family {
			continue
		}
		ms := mf.GetMetric()
		if len(ms) != 1 {
			t.Fatalf("family %q 子指标数 = %d, want 1", family, len(ms))
		}
		return ms[0].GetGauge().GetValue()
	}
	t.Fatalf("family %q 不存在", family)
	return 0
}

// gatherStallHistogram 读出阶段切换停滞 histogram 的样本数、和与非 +Inf 桶上界列表。
func gatherStallHistogram(t *testing.T, r *metrics.Registry) (count uint64, sum float64, upper []float64) {
	t.Helper()
	families, err := r.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != stageSwitchStallName {
			continue
		}
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			count += h.GetSampleCount()
			sum += h.GetSampleSum()
			for _, b := range h.GetBucket() {
				if ub := b.GetUpperBound(); !math.IsInf(ub, +1) {
					upper = append(upper, ub)
				}
			}
		}
		return count, sum, upper
	}
	t.Fatalf("family %q 不存在", stageSwitchStallName)
	return 0, 0, nil
}

// gatherFormatErrors 从注册表 Gather 结果中读出 ordersource 格式错误 counter 的
// (magic, version) 值。直接对 Gather 返回值调方法读取,不 import client_model
// (避免把 go.mod 中间接依赖提升为直接依赖)。
func gatherFormatErrors(t *testing.T, r *metrics.Registry) (magic, version int64) {
	t.Helper()
	families, err := r.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != ordersourceFormatErrorsName {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() != "reason" {
					continue
				}
				switch lp.GetValue() {
				case "magic":
					magic = int64(m.GetCounter().GetValue())
				case "version":
					version = int64(m.GetCounter().GetValue())
				}
			}
		}
	}
	return magic, version
}
