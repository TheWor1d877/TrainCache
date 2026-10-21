// Command webhook is the TrainCache Pod mutation webhook entry point. It
// registers the scheduling-gate mutator with a controller-runtime manager and
// serves the manager health probes (docs/8.1 design.md §5.7).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	crwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/config"
	"traincache/controller/internal/logging"
	tcwebhook "traincache/controller/internal/webhook"
)

const component = "webhook"

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

	if err := runBusiness(ctx, cfg, log); err != nil {
		return err
	}
	logShutdown(log, cfg)
	return nil
}

func runBusiness(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	opts, err := managerOptions(cfg, newScheme())
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), opts)
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	mutator := &tcwebhook.PodMutator{
		Client:   mgr.GetClient(),
		Log:      log.With("webhook", "pod"),
		Recorder: mgr.GetEventRecorderFor("webhook"),
		GateName: cfg.GateName,
	}
	mgr.GetWebhookServer().Register(tcwebhook.MutatePodPath, &admission.Webhook{Handler: mutator})

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add readiness check: %w", err)
	}

	log.Info("starting webhook manager",
		"webhook_addr", cfg.WebhookAddr,
		"health_addr", cfg.HealthAddr,
		"gate_name", cfg.GateName,
	)
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

func managerOptions(cfg *config.Config, scheme *runtime.Scheme) (ctrl.Options, error) {
	host, port, err := parseListenAddr(cfg.WebhookAddr)
	if err != nil {
		return ctrl.Options{}, fmt.Errorf("webhook address: %w", err)
	}
	return ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: cfg.MetricsAddr},
		WebhookServer:          crwebhook.NewServer(crwebhook.Options{Host: host, Port: port}),
		HealthProbeBindAddress: cfg.HealthAddr,
		LeaderElection:         false,
	}, nil
}

// parseListenAddr converts the shared "host:port" configuration into the
// controller-runtime webhook server's Host/Port split.
func parseListenAddr(addr string) (string, int, error) {
	host, rawPort, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("port %q out of range 1..65535", rawPort)
	}
	return host, port, nil
}

// logStartup is split out so tests can assert the structured startup fields.
func logStartup(log *slog.Logger, cfg *config.Config) {
	log.Info("config loaded",
		"addr", cfg.WebhookAddr,
		"namespace", cfg.Namespace,
		"gate_name", cfg.GateName,
	)
	log.Info("starting", "addr", cfg.WebhookAddr, "health_addr", cfg.HealthAddr)
}

func logShutdown(log *slog.Logger, cfg *config.Config) {
	log.Info("shutting down", "addr", cfg.WebhookAddr)
}
