package metrics_test

import (
	"testing"

	"traincache/agent/metrics"
)

// ordersourceFormatErrorsName 与实现侧 family 名一致(D23;测试独立字面量,防改名后测试跟着漂移)。
const ordersourceFormatErrorsName = "traincache_ordersource_format_errors_total"

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

	// M2 起注册表恰含 1 个 family:ordersource 格式错误 counter(D23);
	// 六线 family 仍属 M6,不在此注册。
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) != 1 {
		t.Fatalf("注册表应恰有 1 个 family(ordersource 格式错误),实际 %d 个", len(families))
	}
	if got := families[0].GetName(); got != ordersourceFormatErrorsName {
		t.Errorf("family 名 = %q, want %q(D23)", got, ordersourceFormatErrorsName)
	}
	if got := families[0].GetType().String(); got != "COUNTER" {
		t.Errorf("family 类型 = %q, want COUNTER(D23)", got)
	}

	// 两个 reason 子指标自启动即存在(零值),标签取值编译期固定。
	reasons := make(map[string]bool)
	for _, m := range families[0].GetMetric() {
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
