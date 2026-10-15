// Command traincache-agentd 是 TrainCache 节点常驻 Agent(DaemonSet 入口,design §3/§8)。
// 启动序:配置 → 装配 → gRPC server(health + 三服务);收到 SIGINT/SIGTERM 优雅停止。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"traincache/agent/svc"
)

func main() {
	// 结构化 JSON 日志(slog;Q6 结案:高基数维度 path/fp/task_id 走行级日志)。
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	cfg, err := svc.Load()
	if err != nil {
		slog.Error("配置加载失败", "error", err)
		os.Exit(1)
	}

	// 装配子模块(metrics 骨架 + ordersource 发现循环;M6 起被指标/GC 触发消费)。
	ag, err := svc.Assemble(*cfg)
	if err != nil {
		slog.Error("装配失败", "error", err)
		os.Exit(1)
	}

	gs := grpc.NewServer()
	svc.NewServer(*cfg, ag).Register(gs)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("监听 gRPC 地址失败", "addr", cfg.GRPCAddr, "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// M2:ordersource 发现循环常驻(svc 1s 扫 TRAINCACHE_ORDERS_DIR 的 *.ring,D22);
	// ctx 取消后随优雅停止退出,context.Canceled 是正常退出路径。
	go func() {
		if err := ag.Orders.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("orders discovery stopped", "err", err)
		}
	}()

	// M1 不起 /metrics(M6);MetricsAddr 仅在启动日志中登记。
	slog.Info("traincache-agentd 启动",
		"node", ag.Cfg.NodeName.String(),
		"grpc_addr", cfg.GRPCAddr,
		"metrics_addr", cfg.MetricsAddr,
		"cache_root", cfg.CacheRoot,
		"orders_dir", cfg.OrdersDir,
		"verify", cfg.Verify,
	)

	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("gRPC 服务异常退出", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		slog.Info("收到退出信号,优雅停止")
		gs.GracefulStop()
	}
}
