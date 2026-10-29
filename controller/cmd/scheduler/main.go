// Command scheduler is the TrainCache custom kube-scheduler. It embeds the
// upstream kube-scheduler command and registers the TrainCacheScore plugin on
// the score/normalizeScore extension points (docs/7.0, docs/7.1 §5.8, Q2(a)).
//
// The controller plane settings (annotation prefix, score TTL) come from the
// same environment variables as the controller so both sides agree on the
// annotation key and freshness window.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"traincache/controller/internal/config"
	"traincache/controller/internal/logging"
	"traincache/controller/internal/metrics"
	"traincache/controller/internal/scheduler"
	corelog "traincache/core/logging"
)

const (
	component = "scheduler"
	// metricsPath is the scheduler's independent registry endpoint. It is
	// served on CONTROLLER_METRICS_ADDR, never on the kube-scheduler /metrics
	// endpoint (which uses component-base's legacy registry).
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

	startDebugServer(ctx, cfg, log)

	// The scheduler is a separate process: it owns an independent registry and
	// never touches the controller-manager families (docs/7.1 §9.5).
	registry := metrics.NewScheduler()
	log.Info("metrics registry initialized",
		"addr", cfg.MetricsAddr,
		"path", metricsPath,
	)

	srv := newHealthServer(cfg.HealthAddr)
	metricsSrv := newMetricsServer(cfg.MetricsAddr, registry)
	serverErr := make(chan error, 2)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	cmd := newSchedulerCommand(cfg, log, registry)
	cmdErr := make(chan error, 1)
	go func() {
		cmdErr <- cmd.ExecuteContext(ctx)
	}()

	select {
	case <-ctx.Done():
		logShutdown(log, cfg)
		shutdownErr := errors.Join(shutdownServer(srv), shutdownServer(metricsSrv))
		// Give the scheduler's leader-election shutdown a bounded chance to
		// release the lease before the process exits.
		select {
		case <-cmdErr:
		case <-time.After(5 * time.Second):
			log.Warn("kube-scheduler did not stop before timeout")
		}
		return shutdownErr
	case err := <-serverErr:
		return fmt.Errorf("health server: %w", err)
	case err := <-cmdErr:
		logShutdown(log, cfg)
		if shutdownErr := errors.Join(shutdownServer(srv), shutdownServer(metricsSrv)); shutdownErr != nil {
			return shutdownErr
		}
		if err != nil {
			return fmt.Errorf("kube-scheduler: %w", err)
		}
		return nil
	}
}

// startDebugServer 挂载 /debug/loglevel。DebugAddr 为空时不监听;监听失败只记
// Error,不影响调度器本体(见 docs/8.1 §7)。
func startDebugServer(ctx context.Context, cfg *config.Config, log *slog.Logger) {
	go corelog.ServeLevel(ctx, cfg.DebugAddr, component, log)
}

// newSchedulerCommand builds the upstream scheduler command with the
// TrainCacheScore plugin registered. It is split out so tests can assert the
// wiring without starting the process.
func newSchedulerCommand(cfg *config.Config, log *slog.Logger, registry *metrics.Metrics) *cobra.Command {
	return app.NewSchedulerCommand(app.WithPlugin(scheduler.Name, scheduler.Factory(scheduler.Options{
		Prefix:  cfg.ScoreAnnotationPrefix,
		TTL:     cfg.SchedulerScoreTTL,
		Logger:  log,
		Metrics: registry,
	})))
}

func newHealthServer(addr string) *http.Server {
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func newMetricsServer(addr string, registry *metrics.Metrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(metricsPath, promhttp.HandlerFor(registry.Gatherer, promhttp.HandlerOpts{}))
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func shutdownServer(srv *http.Server) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// logStartup is split out so tests can assert the structured startup fields.
func logStartup(log *slog.Logger, cfg *config.Config) {
	log.Info("config loaded",
		"addr", cfg.HealthAddr,
		"namespace", cfg.Namespace,
		"score_prefix", cfg.ScoreAnnotationPrefix,
		"score_ttl", cfg.SchedulerScoreTTL,
	)
	log.Info("starting", "addr", cfg.HealthAddr, "plugin", scheduler.Name)
}

func logShutdown(log *slog.Logger, cfg *config.Config) {
	log.Info("shutting down", "addr", cfg.HealthAddr)
}
