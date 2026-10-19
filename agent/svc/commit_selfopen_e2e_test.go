package svc

// commit_selfopen_e2e_test.go:D49 自打开登记的生产入口——index.Writer.Commit 在提交前
// 经 cas.Open 校验新块（writer.go 步骤 ④）。装配把 Store 实例级 Open 观察者接到
// SelfOpenGuard.Mark 后，Commit 的这次读打开必须被视为 Agent 自打开：
//   - inotify 兜底对它不推进 Cursor、不计 consumption_advance{source=inotify}
//     （热度 noteOpen 照常发生，证明事件已被监听循环处理）；
//   - 训练侧未登记的块 os.Open 仍推进一次，重复打开不重复计数。

import (
	"context"
	"testing"
	"time"

	"traincache/agent/collector"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
)

// TestCommitSelfOpenRegisteredAndFiltered：Commit 自开 → inotify 过滤；
// 训练侧 OPEN 推进一次且重复不重复计数。
func TestCommitSelfOpenRegisteredAndFiltered(t *testing.T) {
	ctx := context.Background()
	fx := newSvcFixture(t)

	// 训练侧块先于观察者安装写入/提交：其指纹不得被登记。
	trainSpecs := []svcBlockSpec{
		{path: "cs-001.tar", chunk: 0, content: "commit-self-train", local: true},
	}
	trainFPs := fx.commitDataset(t, testDataset, trainSpecs)
	trainRef := ordersource.ChunkRef{Path: trainSpecs[0].path, Chunk: trainSpecs[0].chunk}

	// 装配等价的观察者安装：guard.Mark 挂到 cas.Store 实例（TTL 覆盖整个用例）。
	guard := prefetch.NewSelfOpenGuard(30 * time.Second)
	fx.pm.SetSelfOpenGuard(guard)
	fx.cs.SetOpenObserver(guard.Mark)

	// Commit 自开块：Commit 步骤 ④ 的 cas.Open 经观察者登记。
	selfSpecs := []svcBlockSpec{
		{path: "cs-000.tar", chunk: 0, content: "commit-self-agent", local: true},
	}
	selfFPs := fx.commitDataset(t, testDataset, selfSpecs)
	selfRef := ordersource.ChunkRef{Path: selfSpecs[0].path, Chunk: selfSpecs[0].chunk}

	selfFP, trainFP := selfFPs[selfRef], trainFPs[trainRef]
	if !guard.SelfOpened(selfFP) {
		t.Fatal("Commit 自开块未被登记（观察者未生效）")
	}
	if guard.SelfOpened(trainFP) {
		t.Fatal("训练侧块被误登记为自打开")
	}

	// 订单：[自开块, 训练块]；索引映射在两次提交之后加载。
	fx.preloadMapping(t, testDataset)
	chunks := []ordersource.ChunkRef{selfRef, trainRef}
	b := fx.pm.bundleFor(testDataset)
	// 关闭调度启动（避免后台预取打开块污染信号），只保留阶段/订单状态。
	b.mu.Lock()
	b.started = true
	b.mu.Unlock()
	if err := fx.pm.ForOrder(ctx, "pod-commit-self", ordersource.ConsumeOrder{
		Dataset: testDataset, StageID: "stage-commit-self", PrefetchChunks: 1, Chunks: chunks,
	}); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}

	ag := &Agent{Metrics: fx.reg, Prefetch: fx.pm, CS: fx.cs, selfOpen: guard}

	// 独立的被监听目录：块文件名必须与订单指纹一致（内容无关）。
	blockDir := t.TempDir()
	selfPath := writeWatchBlock(t, blockDir, selfFP)
	trainPath := writeWatchBlock(t, blockDir, trainFP)

	c := collector.NewCollector(blockDir, nil, fx.reg, collector.CollectorConfig{})
	c.SetOpenCallback(ag.onInotifyOpen)
	c.SetOpenFilter(guard.SelfOpened)
	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	c.Start(watchCtx)

	// ① Commit 自开块：OPEN 被过滤，consume 不推进；noteOpen 照常（cache_hits 增长）。
	waitFor(t, "Commit 自打开的 OPEN 被 noteOpen 处理", func() bool {
		openReadOnce(t, selfPath)
		return gatherValue(t, fx.reg, "traincache_cache_hits_total",
			map[string]string{"dataset": "other", "result": "success",
				"source": "unknown", "phase": "unknown"}) >= 1
	})
	if got := b.prefetcher.Status().Cursor; got != (ordersource.ChunkRef{}) {
		t.Errorf("Commit 自开后 Cursor = %+v, want 零值(未推进)", got)
	}
	if got := gatherValue(t, fx.reg, "traincache_consumption_advance_total",
		map[string]string{"dataset": "other", "source": "inotify"}); got != 0 {
		t.Errorf("Commit 自开后 consumption_advance{source=inotify} = %v, want 0", got)
	}

	// ② 训练侧未登记块：首次 OPEN 推进一次。
	waitFor(t, "训练侧 OPEN 推进消费位点", func() bool {
		openReadOnce(t, trainPath)
		return b.prefetcher.Status().Cursor == chunks[1]
	})
	if got := gatherValue(t, fx.reg, "traincache_consumption_advance_total",
		map[string]string{"dataset": "other", "source": "inotify"}); got != 1 {
		t.Fatalf("首次训练 OPEN 后 consumption_advance{source=inotify} = %v, want 1", got)
	}

	// ③ 同一块再次 OPEN：consume 单调，不重复推进/计数。
	openReadOnce(t, trainPath)
	time.Sleep(300 * time.Millisecond)
	if got := b.prefetcher.Status().Cursor; got != chunks[1] {
		t.Errorf("重复 OPEN 后 Cursor = %+v, want 保持 %+v", got, chunks[1])
	}
	if got := gatherValue(t, fx.reg, "traincache_consumption_advance_total",
		map[string]string{"dataset": "other", "source": "inotify"}); got != 1 {
		t.Errorf("重复 OPEN 后 consumption_advance{source=inotify} = %v, want 仍为 1", got)
	}
}
