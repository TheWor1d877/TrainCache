// Command traincache-agentd 是 TrainCache 节点常驻 Agent(DaemonSet 入口,design §3/§8)。
// 启动序:配置 → 装配 → /metrics HTTP → gRPC server(health + 三服务)→ 业务后台循环
// (重扫描/淘汰/发现);收到 SIGINT/SIGTERM 优雅停止。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"traincache/agent/internal/logging"
	"traincache/agent/svc"
)

// metricsShutdownTimeout 是 /metrics 服务优雅停止的上限(与 gRPC GracefulStop 并发执行)。
const metricsShutdownTimeout = 3 * time.Second

// buildVersion 取 Go 构建信息中的主模块版本;代理模块未注入 ldflags 版本,
// 构建信息为空时回退 "dev"。
func buildVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "dev"
}

func main() {
	// 结构化 JSON 日志(slog;Q6 结案:高基数维度 path/fp/task_id 走行级日志)。
	// component/version 由 logging.New 注入并设为默认 logger;P0 级别固定 info,
	// 动态级别属于 P1。
	logging.New("agentd", buildVersion(), "info")

	cfg, err := svc.Load()
	if err != nil {
		slog.Error("配置加载失败", "err", err)
		os.Exit(1)
	}

	// 装配子模块(cas/预取/指标骨架 + 重扫描 + 钩子收端 + 淘汰器 + 发现循环;
	// collector 例外:绑单个 ring,首个订单 ring 被发现时才构造并启动,见 svc wire.go)。
	ag, err := svc.Assemble(*cfg)
	if err != nil {
		slog.Error("装配失败", "err", err)
		os.Exit(1)
	}

	gs := grpc.NewServer()
	svc.NewServer(*cfg, ag).Register(gs)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("监听 gRPC 地址失败", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}

	// /metrics(M6):Prometheus exposition,与 gRPC 同权重(监听失败致命)。
	metricsSrv := &http.Server{
		Handler:           promhttp.HandlerFor(ag.Metrics.Prometheus(), promhttp.HandlerOpts{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsLis, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		slog.Error("监听 metrics 地址失败", "addr", cfg.MetricsAddr, "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// R1/R2(6.1 §5.9/§5.10):前缀感知快照跟随 + 读者声明心跳;随 signal ctx 退出,
	// 退出时对已知 dataset 撤销 claim。
	ag.StartBackground(ctx)

	// 钩子收端(M5/§5.4):绑定失败不致命(钩子为可选观测输入,不影响缓存服务本体),
	// 记错误后继续;gRPC health 不反映该状态。
	if err := ag.HookSock.Start(ctx); err != nil {
		slog.Error("钩子收端启动失败(继续,仅钩子上报不可用)", "err", err)
	}

	// 重扫描(M4/D35)与淘汰循环(M6/§7.6):非阻塞后台循环,随 ctx 取消退出。
	ag.Rescan.Start(ctx)
	ag.Evictor.Start(ctx)

	// M2:ordersource 发现循环常驻(svc 1s 扫 TRAINCACHE_ORDERS_DIR 的 *.ring,D22);
	// ctx 取消后随优雅停止退出,context.Canceled 是正常退出路径。降级聚合判定与
	// 首个 ring → collector 绑定均在该循环内(见 svc orders.go/wire.go)。
	go func() {
		if err := ag.Orders.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("orders discovery stopped", "err", err)
		}
	}()

	slog.Info("traincache-agentd 启动",
		"component", "agentd",
		"version", buildVersion(),
		"node", ag.Cfg.NodeName.String(),
		"grpc_addr", cfg.GRPCAddr,
		"metrics_addr", cfg.MetricsAddr,
		"cache_root", cfg.CacheRoot,
		"orders_dir", cfg.OrdersDir,
		"verify", cfg.Verify,
		"components", "rescan,evictor,orders,hooksock,metrics",
	)

	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()
	metricsErr := make(chan error, 1)
	go func() { metricsErr <- metricsSrv.Serve(metricsLis) }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("gRPC 服务异常退出", "err", err)
			os.Exit(1)
		}
	case err := <-metricsErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics 服务异常退出", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		slog.Info("收到退出信号,优雅停止")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer cancel()
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("metrics 服务停止超时", "err", err)
		}
		gs.GracefulStop()
	}
}
