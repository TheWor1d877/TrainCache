// Package metrics owns the control plane's independent Prometheus registry.
//
// The controller must not share the process-global default registry
// (docs/8.1 design.md §9.5). M1 provides the empty registry only; metric
// families are added together with the features that emit them.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Registry is the controller-owned Prometheus registry. It exposes the
// Registerer and Gatherer views of the same underlying registry so callers can
// register families or expose them without depending on a concrete type.
type Registry struct {
	prometheus.Registerer
	prometheus.Gatherer
}

// New returns an empty registry. Every call creates an independent registry;
// no business metric is registered and the process-global registry is never
// touched.
func New() *Registry {
	r := prometheus.NewRegistry()
	return &Registry{Registerer: r, Gatherer: r}
}
