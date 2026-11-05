// Command checkpointd 是训练 Pod 内的 checkpoint 搬运 sidecar 入口
// （design 10.1 §6.1）。启动序：配置 → 日志 → sidecar.Run；配置非法时
// 写一条 ERROR 并以退出码 2 结束，恢复放弃以 3 结束（§6.5、10.2 D14）。
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"traincache/checkpoint/internal/config"
	"traincache/checkpoint/internal/logging"
	"traincache/checkpoint/internal/obs"
	"traincache/checkpoint/internal/sidecar"
)

// exitConfig 是配置非法的退出码（design 10.1 §6.1）。
const exitConfig = 2

// exitRestoreGiveUp 是恢复放弃的退出码（design 10.1 §6.5、10.2 D14）：与配置
// 非法的 2 分开，restartPolicy: OnFailure 下 kubelet 会重启 init 容器并计入 Job 的
// status.failed，重拉镜像对这个失败没有任何帮助。
const exitRestoreGiveUp = 3

// version 由构建时 -ldflags "-X main.version=..." 注入。
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "probe" {
		runProbe()
		return
	}

	cfg, err := config.Load()
	if err != nil {
		// 日志级别本身可能非法，这里固定用 info 构造，保证错误能写出来。
		logging.New("checkpointd", version, "info")
		slog.Error("配置校验失败", obs.InvalidSpec(err)...)
		os.Exit(exitConfig)
	}

	log := logging.New("checkpointd", version, cfg.LogLevel)
	log.Info("checkpointd 启动",
		slog.String("save_deadline", cfg.SaveDeadline.String()),
		slog.String("restore_deadline", cfg.RestoreDeadline.String()),
		slog.String("poll_interval", cfg.PollInterval.String()),
		slog.String("scan_interval", cfg.ScanInterval.String()),
		slog.Int("scan_stable", cfg.ScanStable),
	)

	opts, err := sidecar.FromEnv()
	if err != nil {
		log.Error("sidecar 配置校验失败", obs.InvalidSpec(err)...)
		os.Exit(exitConfig)
	}
	opts.PollInterval = cfg.PollInterval
	opts.ScanInterval = cfg.ScanInterval
	opts.ScanStable = cfg.ScanStable
	opts.SaveDeadline = cfg.SaveDeadline
	opts.RestoreDeadline = cfg.RestoreDeadline
	opts.Logger = log

	log.Info("checkpointd 配置加载完成",
		obs.Task(opts.TaskID), obs.Rank(opts.Rank), obs.Plugin(opts.Plugin),
		slog.Bool("nvme", opts.NVMEDir != ""),
		slog.Int("nvme_retention", opts.NVMERetention),
		slog.Bool("require_checkpoint", opts.RequireCheckpoint))

	sc, err := sidecar.New(opts, sidecar.NewPluginFactory(opts.PluginPath, opts.SocketPath, log))
	if err != nil {
		log.Error("sidecar 初始化失败", obs.InvalidSpec(err)...)
		os.Exit(exitConfig)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case sig := <-sigCh:
			log.Info("收到信号", slog.String("signal", sig.String()))
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := sc.Run(ctx); err != nil {
		log.Error("checkpointd 运行失败", obs.Err(err)...)
		if errors.Is(err, sidecar.ErrRestoreGiveUp) {
			os.Exit(exitRestoreGiveUp)
		}
		os.Exit(1)
	}
	log.Info("checkpointd 退出")
}

// runProbe 只读探针文件：内容为 ready 退出 0，其余（含文件不存在）退出 1。
func runProbe() {
	path := os.Getenv("PROBE_FILE")
	if path == "" {
		path = sidecar.DefaultProbeFile
	}
	if v, err := sidecar.ReadProbe(path); err == nil && v == "ready" {
		os.Exit(0)
	}
	os.Exit(1)
}
