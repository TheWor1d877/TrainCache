// Package metrics 是 Prometheus 指标注册骨架(design §5.8;decisions D17)。
// 本里程碑只落容器与 dataset 白名单归并("other");六线 family 本体属 M6,不在 M1 注册。
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Config 是指标层配置(由 svc.Config 映射)。
type Config struct {
	// DatasetWhitelist 是 dataset 标签白名单;未命中的 dataset 一律归并为 "other"
	// (§5.8 基数纪律:标签编译期固定,不引入用户输入)。
	DatasetWhitelist []string
}

// Registry 持有 dataset 白名单与 Prometheus 注册表(M6 在此挂载六线 family)。
type Registry struct {
	whitelist map[string]struct{}
	reg       *prometheus.Registry
}

// New 构造注册骨架;白名单为空 → 所有 dataset 归并 "other"。
func New(cfg Config) *Registry {
	wl := make(map[string]struct{}, len(cfg.DatasetWhitelist))
	for _, d := range cfg.DatasetWhitelist {
		wl[d] = struct{}{}
	}
	return &Registry{whitelist: wl, reg: prometheus.NewRegistry()}
}

// DatasetLabel 返回 dataset 标签值:白名单命中返回原名,否则 "other"(§5.8/§6.5)。
func (r *Registry) DatasetLabel(dataset string) string {
	if _, ok := r.whitelist[dataset]; ok {
		return dataset
	}
	return "other"
}

// Prometheus 返回底层注册表(M6 挂载六线 family 与 /metrics HTTP handler 用)。
func (r *Registry) Prometheus() *prometheus.Registry { return r.reg }
