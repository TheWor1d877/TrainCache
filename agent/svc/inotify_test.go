package svc

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"traincache/agent/collector"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
	"traincache/core/cas"
)

// inotify_test.go:D49 inotify 兜底 OPEN → 指纹反查当前订单 → 纯消费位点推进。

// TestInotifyOpenAdvancesConsumeOnce:同一指纹的两次 OPEN 只有第一次真正推进消费位点
// 并计 source=inotify,第二次因 consume 单调返回 false、不重复计数。
func TestInotifyOpenAdvancesConsumeOnce(t *testing.T) {
	ctx := context.Background()
	fx := newSvcFixture(t)
	specs := []svcBlockSpec{
		{path: "a-000.tar", chunk: 0, content: "inotify-a", local: true},
		{path: "a-001.tar", chunk: 0, content: "inotify-b", local: true},
	}
	fps := fx.commitDataset(t, testDataset, specs)
	chunks := []ordersource.ChunkRef{
		{Path: "a-000.tar", Chunk: 0},
		{Path: "a-001.tar", Chunk: 0},
	}
	if err := fx.pm.ForOrder(ctx, "pod-inotify", ordersource.ConsumeOrder{
		Dataset: testDataset, StageID: "stage-i1", PrefetchChunks: 1, Chunks: chunks,
	}); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}

	ag := &Agent{Metrics: fx.reg, Prefetch: fx.pm}
	b := fx.pm.bundleFor(testDataset)
	fp := fps[chunks[0]]

	ag.onInotifyOpen(fp)
	if got := b.prefetcher.Status().Cursor; got != chunks[0] {
		t.Fatalf("首次 OPEN 后 Cursor = %+v, want %+v", got, chunks[0])
	}
	if got := gatherValue(t, fx.reg, "traincache_consumption_advance_total",
		map[string]string{"dataset": "other", "source": "inotify"}); got != 1 {
		t.Fatalf("首次 OPEN 后 consumption_advance{source=inotify} = %v, want 1", got)
	}

	ag.onInotifyOpen(fp)
	if got := b.prefetcher.Status().Cursor; got != chunks[0] {
		t.Errorf("重复 OPEN 后 Cursor = %+v, want 保持 %+v", got, chunks[0])
	}
	if got := gatherValue(t, fx.reg, "traincache_consumption_advance_total",
		map[string]string{"dataset": "other", "source": "inotify"}); got != 1 {
		t.Errorf("重复 OPEN 后 consumption_advance{source=inotify} = %v, want 仍为 1", got)
	}
}

// writeWatchBlock 在 blocks/<fp[:2]>/<fp>.blk 落一个可被 inotify 观测的块文件
// (内容无关:本用例只验证打开事件与消费推进,不读取块数据)。
func writeWatchBlock(t *testing.T, blockDir string, fp cas.Fingerprint) string {
	t.Helper()
	hexs := fp.String()
	dir := filepath.Join(blockDir, "blocks", hexs[:2])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("建分片目录: %v", err)
	}
	path := filepath.Join(dir, hexs+".blk")
	if err := os.WriteFile(path, []byte("watch-block"), 0o644); err != nil {
		t.Fatalf("写块文件: %v", err)
	}
	return path
}

// openReadOnce 打开并读 1 字节后关闭(仅打开即产生 IN_OPEN)。
func openReadOnce(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return
	}
	buf := make([]byte, 1)
	_, _ = f.Read(buf)
	_ = f.Close()
}

// TestInotifySelfOpenFilteredEndToEnd:D49 self-open guard 端到端——collector 以
// guard.SelfOpened 过滤 inotify 回调:
//   - Agent 自打开(Mark)的 fp 触发 OPEN 后不推进 consume(热度照常 noteOpen);
//   - 同一订单内未 Mark 的 fp 首次 OPEN 推进一次、再次 OPEN 因 consume 单调不重复推进。
func TestInotifySelfOpenFilteredEndToEnd(t *testing.T) {
	ctx := context.Background()
	fx := newSvcFixture(t)
	specs := []svcBlockSpec{
		{path: "s-000.tar", chunk: 0, content: "selfopen-a", local: true},
		{path: "s-001.tar", chunk: 0, content: "selfopen-b", local: true},
	}
	fps := fx.commitDataset(t, testDataset, specs)
	chunks := []ordersource.ChunkRef{
		{Path: "s-000.tar", Chunk: 0},
		{Path: "s-001.tar", Chunk: 0},
	}
	if err := fx.pm.ForOrder(ctx, "pod-selfopen", ordersource.ConsumeOrder{
		Dataset: testDataset, StageID: "stage-self", PrefetchChunks: 1, Chunks: chunks,
	}); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}

	guard := prefetch.NewSelfOpenGuard(5 * time.Second)
	fx.pm.SetSelfOpenGuard(guard)
	ag := &Agent{Metrics: fx.reg, Prefetch: fx.pm, CS: fx.cs, selfOpen: guard}

	// 独立的被监听目录:块文件名必须与订单指纹一致(内容无关)。
	blockDir := t.TempDir()
	selfPath := writeWatchBlock(t, blockDir, fps[chunks[0]])
	trainPath := writeWatchBlock(t, blockDir, fps[chunks[1]])

	c := collector.NewCollector(blockDir, nil, fx.reg, collector.CollectorConfig{})
	c.SetOpenCallback(ag.onInotifyOpen)
	c.SetOpenFilter(guard.SelfOpened)
	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	c.Start(watchCtx)

	// ① Agent 自打开:Mark 后 OPEN —— 回调被过滤,consume 不推进;noteOpen 仍生效
	// (cache_hits 增加,证明事件已被监听循环处理)。
	guard.Mark(fps[chunks[0]])
	waitFor(t, "Agent 自打开的 OPEN 被 noteOpen 处理", func() bool {
		openReadOnce(t, selfPath)
		return gatherValue(t, fx.reg, "traincache_cache_hits_total",
			map[string]string{"dataset": "other", "result": "success",
				"source": "unknown", "phase": "unknown"}) >= 1
	})
	b := fx.pm.bundleFor(testDataset)
	if got := b.prefetcher.Status().Cursor; got != (ordersource.ChunkRef{}) {
		t.Errorf("自打开后 Cursor = %+v, want 零值(未推进)", got)
	}
	if got := gatherValue(t, fx.reg, "traincache_consumption_advance_total",
		map[string]string{"dataset": "other", "source": "inotify"}); got != 0 {
		t.Errorf("自打开后 consumption_advance{source=inotify} = %v, want 0", got)
	}

	// ② 未 Mark 的训练打开:首次推进 consume 一次。
	waitFor(t, "未 Mark 的 OPEN 推进消费位点", func() bool {
		openReadOnce(t, trainPath)
		return b.prefetcher.Status().Cursor == chunks[1]
	})
	if got := gatherValue(t, fx.reg, "traincache_consumption_advance_total",
		map[string]string{"dataset": "other", "source": "inotify"}); got != 1 {
		t.Fatalf("首次训练 OPEN 后 consumption_advance{source=inotify} = %v, want 1", got)
	}

	// ③ 同一 fp 再次 OPEN:consume 单调,不重复推进/计数(等待事件被排空后断言)。
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

// TestLookupFingerprintRebuildsPerOrderGeneration:同内容多路径共用指纹;订单代际切换
// (stageID/首尾 ref 变化)后反查索引必须重建,返回新订单的候选块。
func TestLookupFingerprintRebuildsPerOrderGeneration(t *testing.T) {
	ctx := context.Background()
	fx := newSvcFixture(t)
	// 同一内容 → 同一指纹,分布在三个路径(覆盖"同内容多路径返回候选"口径)。
	specs := []svcBlockSpec{
		{path: "gen1/a.tar", chunk: 0, content: "same-inotify-content", local: true},
		{path: "gen1/b.tar", chunk: 0, content: "same-inotify-content", local: true},
		{path: "gen2/c.tar", chunk: 0, content: "same-inotify-content", local: true},
	}
	fps := fx.commitDataset(t, testDataset, specs)
	fp := fps[ordersource.ChunkRef{Path: "gen1/a.tar", Chunk: 0}]
	if fps[ordersource.ChunkRef{Path: "gen1/b.tar", Chunk: 0}] != fp ||
		fps[ordersource.ChunkRef{Path: "gen2/c.tar", Chunk: 0}] != fp {
		t.Fatalf("同内容块指纹不一致: %+v", fps)
	}

	first := []ordersource.ChunkRef{{Path: "gen1/a.tar", Chunk: 0}, {Path: "gen1/b.tar", Chunk: 0}}
	if err := fx.pm.ForOrder(ctx, "pod-gen", ordersource.ConsumeOrder{
		Dataset: testDataset, StageID: "gen-1", PrefetchChunks: 1, Chunks: first,
	}); err != nil {
		t.Fatalf("ForOrder(gen-1): %v", err)
	}
	b := fx.pm.bundleFor(testDataset)
	if got := b.lookupFingerprint(ctx, fp); !reflect.DeepEqual(got, first) {
		t.Fatalf("第一代际 lookupFingerprint = %+v, want %+v", got, first)
	}

	second := []ordersource.ChunkRef{{Path: "gen2/c.tar", Chunk: 0}}
	if err := fx.pm.ForOrder(ctx, "pod-gen", ordersource.ConsumeOrder{
		Dataset: testDataset, StageID: "gen-2", PrefetchChunks: 1, Chunks: second,
	}); err != nil {
		t.Fatalf("ForOrder(gen-2): %v", err)
	}
	if got := b.lookupFingerprint(ctx, fp); !reflect.DeepEqual(got, second) {
		t.Fatalf("第二代际 lookupFingerprint = %+v, want %+v(索引未按代际重建)", got, second)
	}
}
