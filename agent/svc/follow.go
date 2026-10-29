package svc

// follow.go:R1 前缀感知快照跟随、R2 读者声明的装配侧接线(6.1 §5.9/§5.10)。
// 冻结语义:每个 dataset 一个 Catalog.Follow(周期 = Config.IndexRefreshInterval),
// prefixes 取该 dataset 最近订单的路径;快照切换经 OnSnapshotChange 立即声明,
// 单轮失败经 OnSnapshotError 计错误并保持旧映射(由 core 保证)。

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"traincache/agent/metrics"
	"traincache/core/index"
)

const (
	// snapshotDeclareTimeout 是 OnSnapshotChange 触发 Declare 的上限(6.1 §5.10)。
	snapshotDeclareTimeout = 5 * time.Second
	// snapshotWithdrawTimeout 是进程退出时 Withdraw 的上限(6.1 §5.10)。
	snapshotWithdrawTimeout = 5 * time.Second
	// conditionalWriteProbeTimeout 是装配期条件写探测上限(6.1 §5.12)。
	conditionalWriteProbeTimeout = 5 * time.Second
	// heartbeatMinInterval 是读者心跳间隔下限(IndexRefreshInterval/2 至少 1s)。
	heartbeatMinInterval = time.Second
)

// SetIndexFollow 注入快照跟随周期与读者登记器(装配期、任何 Follow 启动前调用)。
// registry 允许为 nil(无登记场景;OnSnapshotChange 仅打刷新指标)。
func (m *PrefetchManager) SetIndexFollow(interval time.Duration, reg *index.ReaderRegistry) {
	m.mu.Lock()
	m.refreshInterval = interval
	m.mu.Unlock()
	m.registry.Store(reg)
}

// startFollows 注入 Follow 生命周期 ctx,并为所有已存在 dataset 补启动(只启动一次)。
// ctx 由 agentd 的 signal ctx 提供,不得使用 context.Background 永久泄漏。
func (m *PrefetchManager) startFollows(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lifecycleCtx = ctx
	for _, b := range m.bundles {
		m.startFollowLocked(b)
	}
}

// startFollowLocked 在 m.mu 下启动单个 dataset 的 Follow(幂等:followStarted)。
// Follow 阻塞运行在独立 goroutine;ctx 取消返回 context.Canceled 属正常退出。
func (m *PrefetchManager) startFollowLocked(b *datasetBundle) {
	if m.lifecycleCtx == nil || b.followStarted {
		return
	}
	b.followStarted = true
	if m.followStarts == nil {
		m.followStarts = make(map[string]int)
	}
	m.followStarts[b.dataset]++
	ctx := m.lifecycleCtx
	go func() {
		if err := b.catalog.Follow(ctx, b.orderPrefixes); err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("catalog: 快照跟随循环退出", "dataset", b.dataset, "err", err)
		}
	}()
}

// orderPrefixes 返回该 dataset 当前订单的路径前缀(去重、首次出现序);无订单返回空
// 切片(Follow 据此走 RefreshManifest 轻量跟随)。ForOrder 写入的 followPrefixes 优先
// 于 lastOrder(首轮 tick 早于 lastOrder 安装);未写入 hint 时回退 lastOrder。
// b.mu 保护两者,临界区只做读取。
func (b *datasetBundle) orderPrefixes() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.followPrefixes != nil {
		return b.followPrefixes
	}
	if !b.hasOrder || len(b.lastOrder.Chunks) == 0 {
		return []string{}
	}
	return orderPaths(b.lastOrder.Chunks)
}

// onSnapshotChange 是 Catalog 成功切换快照后的回调(core 锁外调用):立即异步声明
// 读者 claim(5s 超时,不阻塞切换),按 Declare 成功/失败累加 index_refresh_total。
// 采用 goroutine 是为把对象存储网络 IO 移出 Catalog 刷新路径。
func (m *PrefetchManager) onSnapshotChange(dataset, snapID string) {
	if snapID == "" {
		return
	}
	// 切换已成功:success 同步计入(ForOrder 的 LoadPrefixes 路径依赖本回调,且
	// core 保证失败不回调,故不会与 measureLoadPrefixes 重复)。
	if m.m != nil {
		m.m.IncIndexRefresh(dataset, metrics.ResultSuccess)
	}
	// 与 onSnapshotError 的 Warn 成对:同带 dataset 与 result,成功另带切到的快照。
	slog.Info("catalog: 快照切换成功",
		"dataset", dataset, "snapshot", snapID, "result", metrics.ResultSuccess)
	reg := m.registry.Load()
	if reg == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), snapshotDeclareTimeout)
		defer cancel()
		if err := reg.Declare(ctx, dataset, snapID); err != nil {
			// 声明失败不改变刷新已成功的语义,按契约单独计 error。
			if m.m != nil {
				m.m.IncIndexRefresh(dataset, metrics.ResultError)
			}
			slog.Warn("reader claim: 快照切换后声明失败",
				"dataset", dataset, "snap_id", snapID, "err", err)
		}
	}()
}

// onSnapshotError 是 Catalog 单轮快照加载失败后的回调(core 锁外调用):计 error 并
// 告警;core 保证旧映射保持可用。ErrNoCurrent(尚无首个快照)只计数不告警。
func (m *PrefetchManager) onSnapshotError(dataset string, err error) {
	if err == nil {
		return
	}
	if m.m != nil {
		m.m.IncIndexRefresh(dataset, metrics.ResultError)
	}
	if !errors.Is(err, index.ErrNoCurrent) {
		slog.Warn("catalog: 快照刷新失败(保持旧映射)",
			"dataset", dataset, "result", metrics.ResultError, "err", err)
	}
}

// datasetSnapshot 是心跳的一次 dataset→当前 snapID 采样。
type datasetSnapshot struct {
	dataset string
	snapID  string
}

// snapshotSources 返回已知 dataset 的当前 snapID(已装配 bundle 的 Catalog 原子读)。
func (a *Agent) snapshotSources() []datasetSnapshot {
	if a.Prefetch == nil {
		return nil
	}
	names := a.Prefetch.bundleNames()
	out := make([]datasetSnapshot, 0, len(names))
	for _, name := range names {
		b, ok := a.Prefetch.existingBundle(name)
		if !ok {
			continue
		}
		out = append(out, datasetSnapshot{dataset: name, snapID: b.catalog.SnapID()})
	}
	return out
}

// StartBackground 启动 Agent 的后台循环:采集层(inotify/热度,D63 起无 ring 也启动)、
// 快照跟随(R1)与读者心跳(R2);必须在 agentd 的 signal ctx 下调用。ctx 取消时停止
// 后台循环,并对已知 dataset best-effort Withdraw。
func (a *Agent) StartBackground(ctx context.Context) {
	if a.Collector != nil {
		a.Collector.Start(ctx)
	}
	if a.Prefetch != nil {
		a.Prefetch.startFollows(ctx)
	}
	if a.ReaderRegistry != nil {
		hb := newReaderHeartbeat(a.ReaderRegistry, a.Cfg.indexRefreshInterval(), a.snapshotSources)
		go hb.run(ctx)
	}
}
