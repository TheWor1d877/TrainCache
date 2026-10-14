package metrics_test

import (
	"testing"

	"traincache/agent/metrics"
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

	// M1 不注册六线 family(M6 才挂载):空注册表 Gather 无错误且无 family。
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) != 0 {
		t.Errorf("M1 注册表应无 family,实际 %d 个", len(families))
	}
}
