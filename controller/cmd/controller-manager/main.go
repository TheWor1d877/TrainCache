// Command controller-manager is the TrainCache control-plane manager entry
// point: configuration, structured logging, signal handling and the DataSet
// reconciler.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/config"
	datasetcontroller "traincache/controller/internal/controller/dataset"
	warmupcontroller "traincache/controller/internal/controller/warmup"
	"traincache/controller/internal/logging"
	"traincache/controller/internal/metrics"
	"traincache/controller/internal/storage"
)

const (
	component = "controller-manager"
	// metricsPath is the controller-owned registry endpoint served by the
	// manager metrics server. It must not be /metrics, which controller-runtime
	// reserves for its built-in registry.
	metricsPath = "/metrics/traincache"
)

// version is replaced at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", component, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	bootstrapLog := logging.New(component, version, "info")
	cfg, err := config.Load()
	if err != nil {
		bootstrapLog.Error("config load failed", "addr", "", "err", err)
		return err
	}
	log := logging.New(component, version, cfg.LogLevel)
	logStartup(log, cfg)

	// Independent registry owned by the control plane. The reconcilers emit
	// into it and managerOptions exposes it at metricsPath (docs/7.1 §9.5).
	registry := metrics.New()
	log.Info("metrics registry initialized",
		"addr", cfg.MetricsAddr,
		"path", metricsPath,
		"registerer", fmt.Sprintf("%T", registry.Registerer),
	)

	if err := runBusiness(ctx, cfg, log, registry); err != nil {
		return err
	}
	logShutdown(log, cfg)
	return nil
}

func runBusiness(ctx context.Context, cfg *config.Config, log *slog.Logger, registry *metrics.Metrics) error {
	opts := managerOptions(cfg, newScheme(), registry)
	log.Debug("manager options prepared",
		"metrics_addr", opts.Metrics.BindAddress,
		"leader_election", opts.LeaderElection,
	)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), opts)
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	factory := storage.NewFactory()
	defer func() {
		if err := factory.Close(); err != nil {
			log.Warn("close object store factory", "err", err)
		}
	}()

	reconciler := &datasetcontroller.Reconciler{
		Client:       mgr.GetClient(),
		StoreFactory: factory,
		Recorder:     mgr.GetEventRecorderFor("dataset-controller"),
		Log:          log.With("controller", "dataset"),
		Metrics:      registry,
		MaxChunks:    cfg.MaxChunks,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("register dataset controller: %w", err)
	}

	resolver := agentclient.NewResolver(mgr.GetClient(), cfg.AgentPort)
	agents := agentclient.NewPool(resolver, cfg.AgentTimeout)
	defer func() {
		if err := agents.Close(); err != nil {
			log.Warn("close agent client pool", "err", err)
		}
	}()
	warmupReconciler := &warmupcontroller.Reconciler{
		Client:                mgr.GetClient(),
		Agents:                agents,
		Recorder:              mgr.GetEventRecorderFor("warmuptask-controller"),
		Log:                   log.With("controller", "warmup"),
		Metrics:               registry,
		PollInterval:          cfg.PollInterval,
		ScoreAnnotationPrefix: cfg.ScoreAnnotationPrefix,
		GateName:              cfg.GateName,
		CacheCapacityBytes:    cfg.CacheCapacityBytes,
		StartupWindowBytes:    cfg.StartupWindowBytes,
		MaxFillFraction:       cfg.MaxFillFraction,
	}
	if err := warmupReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("register warmup controller: %w", err)
	}

	// M9 wiring: watch Pods selected by traincache.io/warmup=enabled and
	// record GateInjectionDegraded when the webhook did not inject the task
	// annotation (docs/7.1 §5.7, §7.9).
	degradedReconciler := &warmupcontroller.DegradedReconciler{
		Client:   mgr.GetClient(),
		Log:      log.With("controller", "warmup-degraded"),
		Recorder: mgr.GetEventRecorderFor("warmup-degraded"),
		Metrics:  registry,
	}
	if err := degradedReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("register degraded controller: %w", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add readiness check: %w", err)
	}

	log.Info("starting manager", "metrics_addr", cfg.MetricsAddr, "health_addr", cfg.HealthAddr)
	return mgr.Start(ctx)
}

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("register client-go scheme: %v", err))
	}
	if err := traincachev1alpha1.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("register traincache scheme: %v", err))
	}
	return scheme
}

func managerOptions(cfg *config.Config, scheme *runtime.Scheme, registry *metrics.Metrics) ctrl.Options {
	return ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: cfg.MetricsAddr,
			// controller-runtime validates that ExtraHandlers never replaces
			// its built-in /metrics, so the independent registry lives beside
			// it at metricsPath.
			ExtraHandlers: map[string]http.Handler{
				metricsPath: promhttp.HandlerFor(registry.Gatherer, promhttp.HandlerOpts{}),
			},
		},
		LeaderElection:         cfg.LeaderElection,
		LeaderElectionID:       "traincache-controller-leader",
		HealthProbeBindAddress: cfg.HealthAddr,
		// Q9 keeps Secret read RBAC namespaced. A cached client would start a
		// cluster-wide Secret informer (list/watch in every namespace), which
		// that RBAC cannot grant. Read Secrets live instead; the storage
		// factory already caches stores by resourceVersion.
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{&corev1.Secret{}},
			},
		},
	}
}

// logStartup is split out so tests can assert the structured startup fields.
func logStartup(log *slog.Logger, cfg *config.Config) {
	log.Info("config loaded",
		"addr", cfg.MetricsAddr,
		"namespace", cfg.Namespace,
		"leader_election", cfg.LeaderElection,
	)
	log.Info("starting", "addr", cfg.HealthAddr)
}

func logShutdown(log *slog.Logger, cfg *config.Config) {
	log.Info("shutting down", "addr", cfg.HealthAddr)
}
