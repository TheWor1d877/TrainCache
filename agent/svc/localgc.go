package svc

// localgc.go:节点级本地 CAS 孤儿块副本的周期回收入口(docs/5.1-design.md《回收》)。
// 回收面按节点划分:活块集合 = 全部数据集(远端枚举 ∪ 本地已知 bundle)当前快照与
// 有效读者 claim 引用块的并集,由 core LocalGC 计算;svc 侧只负责完整枚举与整轮放弃。

import (
	"context"
	"log/slog"
	"time"

	"traincache/core/index"
)

// startLocalGC 启动本地副本清扫循环(StartBackground 在 signal ctx 下调用;重复调用
// 只生效一次——一轮清扫要全量读索引并逐块判定,并行两轮只是放大 IO,互斥留在 core
// 之外靠启动点保证)。store/cas 未装配(手工骨架)时不启动。
func (a *Agent) startLocalGC(ctx context.Context) {
	if a.ObjectStore == nil || a.CS == nil {
		return
	}
	a.localGCOnce.Do(func() {
		gc := index.NewLocalGC(a.ObjectStore, a.CS, index.Config{
			RefreshInterval: a.Cfg.indexRefreshInterval(),
			OnSweepSkip:     a.onSweepSkip,
		})
		go a.runLocalGC(ctx, gc, a.Cfg.localGCInterval())
	})
}

// runLocalGC 驱动清扫轮:启动即跑一轮(本地副本可能在上一次进程存活期间就该回收了,
// 等满一个周期只是让盘上多留一会儿),之后按周期跑。Sweep 同步执行,一轮未完不起下一轮;
// ctx 取消即退出。
func (a *Agent) runLocalGC(ctx context.Context, gc *index.LocalGC, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	a.sweepLocalBlocks(ctx, gc)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sweepLocalBlocks(ctx, gc)
		}
	}
}

// sweepLocalBlocks 跑一轮节点级本地副本回收。整轮放弃条件是硬约束:LocalGC 按
// 「全部数据集」算活块集合,枚举不全时缺名的数据集会被判成孤儿整体删掉——本节点
// 那个数据集的副本一次全没,后续读全部走回源重拉,这个代价不能像对象 GC 那样
// 用降级覆盖换回收及时。
func (a *Agent) sweepLocalBlocks(ctx context.Context, gc *index.LocalGC) {
	datasets, err := a.knownDatasetsStrict(ctx)
	if err != nil {
		slog.Warn("local gc: dataset 枚举不完整,本轮放弃本地副本回收", "err", err)
		return
	}
	removed, err := gc.Sweep(ctx, datasets)
	if removed > 0 {
		// 回收是节点级动作,不归属任何数据集:dataset 标签传空串,经白名单归 other。
		if a.Metrics != nil {
			a.Metrics.AddGCBlocksRemoved("", int64(removed))
		}
		slog.Info("local gc: 回收本地孤儿块副本",
			"removed_blocks", removed, "datasets", len(datasets))
	}
	if err != nil {
		slog.Warn("local gc: 本轮本地副本回收出错(下一轮重试)",
			"removed_blocks", removed, "err", err)
	}
}
