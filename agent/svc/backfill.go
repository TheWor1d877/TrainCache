package svc

// backfill.go:读 miss 异步 demand(按需补块;D44)。
//
// D44 契约:svc 收到 read_miss 后解析 dataset 归属(不要求索引命中,D60)→
// Catalog.Lookup → cas.Open:
//   - 缺失(ErrNotFound/索引无条目/索引查询失败)→ demand:以最高优先级 P0 入队(reason=demand),
//     按 D25 键直接回源,不依赖索引;
//   - 损坏(ErrCorrupt)→ repair:Quarantine + RepairOne;
//   - demand 与 prefetch 共用同一 Fetcher / single-flight 域;best-effort,不保证本次读前补齐。
//
// 钩子读循环单 goroutine 顺序分发,故 handleReadMiss 只做打点并起后台 goroutine;
// 任一步失败仅记日志,不影响读路径、不 panic。

import (
	"context"
	"errors"
	"log/slog"

	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
	"traincache/core/cas"
)

// handleReadMiss 处理一帧 read_miss:同步计入 traincache_hook_read_miss_total{reason}
// (每帧一次;该指标统计的是帧,不是回源路径),demand 补块动作异步执行(不阻塞钩子
// 读循环)。reason 已由 hooksock 归一为 absent|corrupt。
func (a *Agent) handleReadMiss(ref ordersource.ChunkRef, reason string) {
	if a.Metrics != nil {
		a.Metrics.ObserveHookReadMiss(reason)
	}
	go a.backfill(ref)
}

// backfill 执行一次 demand 按需补块(后台 goroutine;best-effort)。
func (a *Agent) backfill(ref ordersource.ChunkRef) {
	if a.Prefetch == nil || a.CS == nil {
		slog.Warn("backfill: 装配缺失,跳过补块", "path", ref.Path, "chunk", ref.Chunk)
		return
	}
	dataset, ok := a.resolveDataset(ref)
	if !ok {
		// 无 ring 订单且无已知 bundle:数据集归属都定不出来,D25 键无从拼接,记日志返回。
		slog.Warn("backfill: 块引用无已知 dataset 归属,跳过补块", "path", ref.Path, "chunk", ref.Chunk)
		return
	}

	ctx := context.Background()
	b := a.Prefetch.bundleFor(dataset, nil)
	entry, found, err := b.catalog.Lookup(ctx, ref.Path, ref.Chunk)
	if err != nil || !found {
		// 索引未命中/查询失败(含映射未加载):按缺失处理,P0 键由 D25 直接构造,不依赖索引(D60)。
		a.enqueueBackfillP0(dataset, ref)
		return
	}

	a.markSelfOpen(entry.Fingerprint) // D49:本地命中探测打开
	br, err := a.CS.Open(entry.Fingerprint)
	switch {
	case err == nil:
		_ = br.Close()
		return // 本地命中:无需补块
	case errors.Is(err, cas.ErrCorrupt):
		a.repairBackfillCorrupt(ctx, dataset, entry.Fingerprint, ref)
	case errors.Is(err, cas.ErrNotFound):
		a.enqueueBackfillP0(dataset, ref)
	default:
		slog.Warn("backfill: 本地块打开失败,跳过补块",
			"dataset", dataset, "path", ref.Path, "chunk", ref.Chunk, "err", err)
	}
}

// repairBackfillCorrupt 执行 repair 坏块修复(D44/D35):Quarantine + Rescan.RepairOne。
func (a *Agent) repairBackfillCorrupt(ctx context.Context, dataset string, fp cas.Fingerprint, ref ordersource.ChunkRef) {
	if err := a.CS.Quarantine(fp); err != nil {
		slog.Warn("backfill: 隔离坏块失败",
			"dataset", dataset, "path", ref.Path, "chunk", ref.Chunk, "fp", fp, "err", err)
		return
	}
	if a.Rescan == nil {
		slog.Warn("backfill: 未装配重扫描修复器,坏块仅隔离",
			"dataset", dataset, "path", ref.Path, "chunk", ref.Chunk)
		return
	}
	if _, err := a.Rescan.RepairOne(ctx, dataset, ref); err != nil {
		slog.Warn("backfill: 坏块修复失败",
			"dataset", dataset, "path", ref.Path, "chunk", ref.Chunk, "err", err)
	}
}

// enqueueBackfillP0 以最高优先级 P0 入队执行 demand 补块(复用预取队列与 Fetcher,
// 回源 reason=demand)。
// 与 ForPreheat 同口径:未绑定 dataset 时以 dataset-only 最小订单播种,调度未启动则启动。
func (a *Agent) enqueueBackfillP0(dataset string, ref ordersource.ChunkRef) {
	b := a.Prefetch.bundleFor(dataset, nil)
	ctx := context.Background()

	b.mu.Lock()
	if !b.stageSeen {
		if _, err := b.prefetcher.OnStageSwitch(ctx, ordersource.ConsumeOrder{Dataset: dataset}); err != nil {
			b.mu.Unlock()
			slog.Warn("backfill: 绑定 dataset 失败,跳过补块", "dataset", dataset, "err", err)
			return
		}
		b.stageSeen = true
	}
	if !b.started {
		b.started = true
		b.prefetcher.Start(a.Prefetch.baseCtx)
	}
	err := b.prefetcher.EnqueueP0Demand(ctx, []prefetch.ChunkRef{ref})
	b.mu.Unlock()
	if err != nil {
		slog.Warn("backfill: demand P0 入队失败", "dataset", dataset, "path", ref.Path, "chunk", ref.Chunk, "err", err)
	}
}
