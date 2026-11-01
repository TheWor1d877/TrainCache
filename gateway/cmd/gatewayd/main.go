// Command traincache-gatewayd 是 S3 合并回源网关的进程入口
// （design 10.1 §6.6/§7）。启动序：日志 → 配置 → 源站客户端 → server → 监听 →
// ready；收到 SIGINT/SIGTERM 后先关 ready，再排水，最后退出。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	corelog "traincache/core/logging"
	"traincache/gateway/internal/config"
	"traincache/gateway/internal/logging"
	"traincache/gateway/internal/merge"
	"traincache/gateway/internal/obs"
	"traincache/gateway/internal/origin"
	"traincache/gateway/internal/sched"
	"traincache/gateway/internal/server"
)

const (
	// debugAddr 是 /debug/loglevel 的固定回环地址（design 10.1 §7.1；不走 env）。
	debugAddr = "127.0.0.1:6060"
	// shutdownTimeout 是排水上限（design 10.1 §6.6；terminationGracePeriodSeconds=40）。
	shutdownTimeout = 30 * time.Second
	// readHeaderTimeout 防止慢速请求头占住连接。
	readHeaderTimeout = 5 * time.Second
)

// version is replaced at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// 日志先于配置初始化：配置错误也要以同一套 JSON 结构输出。
	// 未知级别在 logging.New 内退化为 info（design 10.1 §5.3）。
	logging.New("gateway", version, os.Getenv("TRAINCACHE_LOG_LEVEL"))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("配置加载失败", "err", err)
		os.Exit(1)
	}

	org, err := origin.New(cfg.ObjstoreEndpoint, cfg.ObjstoreAccessKey, cfg.ObjstoreSecretKey,
		cfg.Bucket, cfg.ObjstoreSecure)
	if err != nil {
		slog.Error("构造源站客户端失败", "origin", cfg.ObjstoreEndpoint, "err", err)
		os.Exit(1)
	}

	registry := merge.NewRegistry(cfg.BlockBufferBytes)
	scheduler := sched.New(cfg.BucketConcurrency)
	summary := obs.New(60*time.Second, slog.Default())
	defer summary.Stop()

	srv := server.New(org, server.Options{
		Bucket:      cfg.Bucket,
		Concurrency: cfg.BucketConcurrency,
		BufferBytes: cfg.BlockBufferBytes,
		Registry:    registry,
		Scheduler:   scheduler,
		Obs:         summary,
	}, slog.Default())

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		slog.Error("监听失败", "addr", cfg.Listen, "err", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 动态级别只绑回环，不进 Service；监听失败不影响网关本体。
	go corelog.ServeLevel(ctx, debugAddr, "gateway", slog.Default())

	// 监听成功后才 ready，探针不依赖源站（design 10.1 §6.6）。
	srv.SetReady(true)
	slog.Info("traincache-gatewayd 启动",
		"listen", cfg.Listen,
		"origin", cfg.ObjstoreEndpoint,
		"bucket", cfg.Bucket,
		"concurrency", cfg.BucketConcurrency,
		"buffer_bytes", cfg.BlockBufferBytes,
	)

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(lis) }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP 服务异常退出", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		slog.Info("收到退出信号，开始排水", "inflight", srv.Inflight())

		// 关闭序：ready=false → 停止接受新连接 → 等在途结束或 30s → 退出日志。
		srv.SetReady(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("HTTP 服务停止超时", "err", err)
		}
		drained := srv.Shutdown(shutdownCtx)
		slog.Info("退出完成", "drained", drained, "forced", !drained)
	}
}
