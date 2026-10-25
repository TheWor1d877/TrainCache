package svc

// readerheartbeat.go:R2 读者心跳与撤销(6.1 §5.10)。心跳间隔 = 刷新周期/2(下限
// 1s),遍历已知 dataset 取 Catalog.SnapID 重新 Declare;ctx 取消时对已知 dataset
// best-effort Withdraw(5s 超时),声明对象供 GC 按读者确认删除。

import (
	"context"
	"log/slog"
	"time"

	"traincache/core/index"
)

// readerHeartbeat 是读者声明心跳循环(构造后由 run 在独立 goroutine 运行)。
type readerHeartbeat struct {
	reg       *index.ReaderRegistry
	interval  time.Duration
	snapshots func() []datasetSnapshot
}

// newReaderHeartbeat 构造心跳循环;interval = refreshInterval/2,下限 1s。
func newReaderHeartbeat(reg *index.ReaderRegistry, refreshInterval time.Duration,
	snapshots func() []datasetSnapshot) *readerHeartbeat {
	interval := refreshInterval / 2
	if interval < heartbeatMinInterval {
		interval = heartbeatMinInterval
	}
	return &readerHeartbeat{reg: reg, interval: interval, snapshots: snapshots}
}

// run 阻塞运行心跳:启动即先声明一次(尽快让 GC 看到读者),之后按 ticker 刷新;
// ctx 取消 → Withdraw 全部已知 dataset 后返回。
func (h *readerHeartbeat) run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	h.beat(ctx)
	for {
		select {
		case <-ctx.Done():
			h.withdraw()
			return
		case <-ticker.C:
			h.beat(ctx)
		}
	}
}

// beat 对每个已知 dataset 以当前 SnapID 重新声明(空 SnapID 跳过:尚无快照可声明,
// 避免覆盖既有有效 claim)。单次声明失败只告警,下一轮自然重试。
func (h *readerHeartbeat) beat(ctx context.Context) {
	for _, s := range h.snapshots() {
		if s.snapID == "" {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, snapshotDeclareTimeout)
		err := h.reg.Declare(cctx, s.dataset, s.snapID)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("reader heartbeat: 声明刷新失败(下一轮重试)",
				"dataset", s.dataset, "snap_id", s.snapID, "err", err)
		}
	}
}

// withdraw 在生命周期 ctx 取消后撤销全部已知 dataset 的声明。使用 Background 派生
// 的短超时 ctx(原 ctx 已取消);写入失败不阻止退出(best-effort)。
func (h *readerHeartbeat) withdraw() {
	seen := make(map[string]struct{})
	for _, s := range h.snapshots() {
		if _, ok := seen[s.dataset]; ok {
			continue
		}
		seen[s.dataset] = struct{}{}
		cctx, cancel := context.WithTimeout(context.Background(), snapshotWithdrawTimeout)
		err := h.reg.Withdraw(cctx, s.dataset)
		cancel()
		if err != nil {
			slog.Warn("reader claim: 退出撤销失败(不阻止退出)", "dataset", s.dataset, "err", err)
		}
	}
}
