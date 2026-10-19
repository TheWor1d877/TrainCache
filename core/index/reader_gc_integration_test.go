package index

// reader_gc_integration_test.go 是 R2/R3(design 7.1 §5.10–§5.11)的永久回归:
// 用可注入时钟覆盖 GC 的三个时间窗——
//  1. 固定保留窗口(RetainSnapshots 仅兜底):出窗快照默认可删;
//  2. PreviousUntil 切换宽限窗:声明切到 S2 后 S1 只在宽限期内受保护;
//  3. UpdatedAt/ReaderGrace 心跳有效期:读者停止心跳后 claim 整体忽略。
// 另覆盖有效 claim 的 SnapID 保护(出窗也不得删除)与切换宽限期内的旧快照保护。
// 另覆盖 protected manifest 缺失时的 fail-safe(OnSweepSkip、跳过块删除)。
// 全程使用内存 objectstore.Mock + 临时 CAS,不依赖真实 sleep 推进时间窗。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"traincache/core/cas"
)

// rgcFixture 是"同一 dataset 连续提交 4 个快照"的测试装置:S1..S4,每个快照
// 引用一个独有块,因此各快照的 manifest 与分片对象互不相同。
type rgcFixture struct {
	env    *testEnv
	writer *Writer
	snaps  []string
	blocks []cas.Fingerprint
}

// newRGCFixture 提交 4 个快照;S1 在 RetainSnapshots=1 时必然出保留窗口。
func newRGCFixture(t *testing.T, dataset string, cfg Config) *rgcFixture {
	t.Helper()
	f := &rgcFixture{env: newTestEnv(t)}
	f.writer = NewWriter(f.env.store, f.env.cs, cfg)
	for i := 0; i < 4; i++ {
		fp := f.env.putBlock(t, fmt.Sprintf("%s-block-%d", dataset, i))
		id := commitDistinct(t, f.writer, f.env.store, dataset, "/k/a", fp)
		f.snaps = append(f.snaps, id)
		f.blocks = append(f.blocks, fp)
	}
	return f
}

// snapshotKeys 返回快照目录下全部对象键(manifest + 分片)。
func (f *rgcFixture) snapshotKeys(t *testing.T, dataset string, i int) []string {
	t.Helper()
	return listObjectKeys(t, f.env.store, SnapshotPrefix(dataset, f.snaps[i]))
}

// assertSnapshotKept 断言第 i 个快照的 manifest、全部分片与引用块均存在。
func (f *rgcFixture) assertSnapshotKept(t *testing.T, dataset string, i int) {
	t.Helper()
	keys := f.snapshotKeys(t, dataset, i)
	if len(keys) < 2 {
		t.Fatalf("S%d(%s) 对象键 = %v,期望 manifest + 至少一个分片", i+1, f.snaps[i], keys)
	}
	for _, key := range keys {
		if !objectExists(t, f.env.store, key) {
			t.Fatalf("S%d 的受保护对象 %q 被删除", i+1, key)
		}
	}
	if !blockExists(t, f.env.cs, f.blocks[i]) {
		t.Fatalf("S%d 引用的块 %s 被删除", i+1, f.blocks[i])
	}
}

// assertSnapshotGone 断言第 i 个快照的 manifest/分片与引用块均已删除。
func (f *rgcFixture) assertSnapshotGone(t *testing.T, dataset string, i int) {
	t.Helper()
	if objectExists(t, f.env.store, ManifestKey(dataset, f.snaps[i])) {
		t.Fatalf("S%d(%s) 的 manifest 未被删除", i+1, f.snaps[i])
	}
	for _, key := range f.snapshotKeys(t, dataset, i) {
		if objectExists(t, f.env.store, key) {
			t.Fatalf("S%d 的出窗对象 %q 未被删除", i+1, key)
		}
	}
	if blockExists(t, f.env.cs, f.blocks[i]) {
		t.Fatalf("S%d 引用的孤儿块 %s 未被删除", i+1, f.blocks[i])
	}
}

// TestR2R3GCRetentionWindowAndActiveClaim:时间窗 1(固定保留窗口)+ 时间窗 2(SnapID)。
// S1..S4 提交后 S1 出窗;无 claim 时 S1 可删,Declare(S1) 后 S1 的 manifest/分片/块
// 全部受保护,而其他出窗快照(S2/S3)照常回收。
func TestR2R3GCRetentionWindowAndActiveClaim(t *testing.T) {
	ctx := context.Background()
	const dataset = "r2r3-window"
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10, ReaderGrace: 30 * time.Second}
	f := newRGCFixture(t, dataset, cfg)
	base := time.Unix(1_700_000_000, 0)

	// 先证明 S1 确实不在固定保留窗口:无 claim 时会被回收。
	col := NewCollector(f.env.store, f.env.cs, cfg)
	col.now = func() time.Time { return base }
	if _, _, err := col.Sweep(ctx, dataset); err != nil {
		t.Fatalf("无 claim Sweep: %v", err)
	}
	if objectExists(t, f.env.store, ManifestKey(dataset, f.snaps[0])) {
		t.Fatalf("固定保留窗口(snapID=%s)未把 S1 判为出窗", f.snaps[0])
	}
	if blockExists(t, f.env.cs, f.blocks[0]) {
		t.Fatalf("无 claim 时出窗块 %s 未被删除", f.blocks[0])
	}

	// 重新造装置,声明 S1 后它的 manifest/分片/块均受保护。
	f = newRGCFixture(t, dataset, cfg)
	reg := NewReaderRegistry(f.env.store, "worker-01", 10*time.Second)
	reg.now = func() time.Time { return base }
	if err := reg.Declare(ctx, dataset, f.snaps[0]); err != nil {
		t.Fatalf("Declare(S1): %v", err)
	}
	col = NewCollector(f.env.store, f.env.cs, cfg)
	col.now = func() time.Time { return base }
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("有 claim Sweep: %v", err)
	}
	if removedBlocks != 2 || removedObjects != 4 {
		t.Fatalf("删除 (blocks=%d, objects=%d),期望 (2,4):仅 S2/S3 可删", removedBlocks, removedObjects)
	}
	f.assertSnapshotKept(t, dataset, 0)
	if !objectExists(t, f.env.store, ManifestKey(dataset, f.snaps[3])) ||
		!blockExists(t, f.env.cs, f.blocks[3]) {
		t.Fatal("CURRENT(S4)被误删")
	}
	for _, i := range []int{1, 2} {
		f.assertSnapshotGone(t, dataset, i)
	}
}

// TestR2R3GCPreviousClaimGraceWindow:时间窗 3(PreviousUntil)。
// 声明从 S1 切到 S2 后,S1 只在 Registry 宽限期内受保护;宽限期过后即使心跳
// 仍让 claim 有效,PreviousSnapID 也不再保护 S1,而 S2 继续受保护。
func TestR2R3GCPreviousClaimGraceWindow(t *testing.T) {
	ctx := context.Background()
	const dataset = "r2r3-prev"
	const registryGrace = 10 * time.Second
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10, ReaderGrace: 10 * time.Second}
	f := newRGCFixture(t, dataset, cfg)

	base := time.Unix(1_700_000_000, 0)
	now := base
	reg := NewReaderRegistry(f.env.store, "worker-01", registryGrace)
	reg.now = func() time.Time { return now }
	if err := reg.Declare(ctx, dataset, f.snaps[0]); err != nil {
		t.Fatalf("Declare(S1): %v", err)
	}
	// t=base+1s:切换到 S2,Previous=S1、PreviousUntil=base+11s。
	now = base.Add(time.Second)
	if err := reg.Declare(ctx, dataset, f.snaps[1]); err != nil {
		t.Fatalf("Declare(S2): %v", err)
	}
	// t=base+2s:模拟周期心跳(同一 SnapID),刷新 UpdatedAt 但保留未过期的 Previous。
	now = base.Add(2 * time.Second)
	if err := reg.Declare(ctx, dataset, f.snaps[1]); err != nil {
		t.Fatalf("心跳 Declare(S2): %v", err)
	}

	col := NewCollector(f.env.store, f.env.cs, cfg)
	col.now = func() time.Time { return now }

	// 宽限期内(t=base+2s < base+11s):S1/S2 均受保护。
	if _, _, err := col.Sweep(ctx, dataset); err != nil {
		t.Fatalf("宽限期内 Sweep: %v", err)
	}
	f.assertSnapshotKept(t, dataset, 0)
	f.assertSnapshotKept(t, dataset, 1)
	f.assertSnapshotGone(t, dataset, 2)

	// 距切换已 > ReaderGrace(t=base+12s),心跳在 t=base+2s 刷新后仍有效(10s ≤ grace);
	// PreviousUntil(base+11s)已过,仅 SnapID=S2 受保护。
	now = base.Add(12 * time.Second)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("宽限期后 Sweep: %v", err)
	}
	if removedBlocks != 1 || removedObjects != 2 {
		t.Fatalf("宽限期后删除 (blocks=%d, objects=%d),期望 (1,2):S1 可删(S3 已在上轮回收)", removedBlocks, removedObjects)
	}
	f.assertSnapshotGone(t, dataset, 0)
	f.assertSnapshotKept(t, dataset, 1)
	if !blockExists(t, f.env.cs, f.blocks[3]) ||
		!objectExists(t, f.env.store, ManifestKey(dataset, f.snaps[3])) {
		t.Fatal("CURRENT(S4)被误删")
	}
}

// TestR2R3GCReaderHeartbeatGraceWindow:时间窗 3 的读者死亡分支。
// 声明 S1 后不再心跳(模拟 Agent 崩溃);时钟前进超过 ReaderGrace 后 claim 整体忽略,
// S1 重新成为出窗快照并被 Sweep 回收。
func TestR2R3GCReaderHeartbeatGraceWindow(t *testing.T) {
	ctx := context.Background()
	const dataset = "r2r3-dead"
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10, ReaderGrace: 10 * time.Second}
	f := newRGCFixture(t, dataset, cfg)

	base := time.Unix(1_700_000_000, 0)
	now := base
	reg := NewReaderRegistry(f.env.store, "worker-01", 5*time.Second)
	reg.now = func() time.Time { return now }
	if err := reg.Declare(ctx, dataset, f.snaps[0]); err != nil {
		t.Fatalf("Declare(S1): %v", err)
	}

	col := NewCollector(f.env.store, f.env.cs, cfg)
	col.now = func() time.Time { return now }

	// 心跳仍有效:S1 受保护。
	now = base.Add(5 * time.Second)
	if _, _, err := col.Sweep(ctx, dataset); err != nil {
		t.Fatalf("心跳有效时 Sweep: %v", err)
	}
	f.assertSnapshotKept(t, dataset, 0)

	// 停止心跳(不再 Declare),时钟前进超过 ReaderGrace:claim 视为读者已死。
	now = base.Add(11 * time.Second)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("心跳过期后 Sweep: %v", err)
	}
	if removedBlocks != 1 || removedObjects != 2 {
		t.Fatalf("心跳过期后删除 (blocks=%d, objects=%d),期望 (1,2):S1 可删(S2/S3 已在上轮回收)", removedBlocks, removedObjects)
	}
	f.assertSnapshotGone(t, dataset, 0)
	if !blockExists(t, f.env.cs, f.blocks[3]) ||
		!objectExists(t, f.env.store, ManifestKey(dataset, f.snaps[3])) {
		t.Fatal("CURRENT(S4)被误删")
	}
}

// TestR2R3GCProtectedManifestMissing:protected 快照 manifest 缺失时调用
// OnSweepSkip(dataset,"manifest_unavailable"),本轮跳过块删除(包括孤儿块),
// 但继续删除其他可删快照对象(S2/S3);S1 的分片与 CURRENT 均保留。
func TestR2R3GCProtectedManifestMissing(t *testing.T) {
	ctx := context.Background()
	const dataset = "r2r3-missing"
	skipCalls := 0
	skipReason := ""
	cfg := Config{
		RetainSnapshots:    1,
		MaxEntriesPerShard: 10,
		ReaderGrace:        30 * time.Second,
		OnSweepSkip: func(ds, reason string) {
			skipCalls++
			skipReason = reason
			if ds != dataset {
				t.Errorf("OnSweepSkip dataset = %q, want %q", ds, dataset)
			}
		},
	}
	f := newRGCFixture(t, dataset, cfg)
	base := time.Unix(1_700_000_000, 0)

	reg := NewReaderRegistry(f.env.store, "worker-01", 10*time.Second)
	reg.now = func() time.Time { return base }
	if err := reg.Declare(ctx, dataset, f.snaps[0]); err != nil {
		t.Fatalf("Declare(S1): %v", err)
	}
	// S1 是 protected(有效 claim),手工删除其 manifest 制造缺失。
	if err := f.env.store.Delete(ctx, ManifestKey(dataset, f.snaps[0])); err != nil {
		t.Fatalf("Delete manifest: %v", err)
	}

	col := NewCollector(f.env.store, f.env.cs, cfg)
	col.now = func() time.Time { return base }
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if skipCalls != 1 || skipReason != "manifest_unavailable" {
		t.Fatalf("OnSweepSkip 调用 %d 次、reason=%q,期望 1 次 manifest_unavailable", skipCalls, skipReason)
	}
	if removedBlocks != 0 {
		t.Fatalf("manifest 缺失时删除块 %d 个,期望 0", removedBlocks)
	}
	// 其它可删快照对象仍回收:S2/S3 各 manifest + 分片 = 4。
	if removedObjects != 4 {
		t.Fatalf("删除快照对象 %d 个,期望 4(S2/S3)", removedObjects)
	}
	for i, fp := range f.blocks {
		if !blockExists(t, f.env.cs, fp) {
			t.Fatalf("跳过块删除时 S%d 的块 %s 被删除", i+1, fp)
		}
	}
	// S1 虽 manifest 缺失,分片仍因 protected 保留。
	for _, key := range f.snapshotKeys(t, dataset, 0) {
		if !objectExists(t, f.env.store, key) {
			t.Fatalf("protected 快照 S1 的分片 %q 被删除", key)
		}
	}
	for _, i := range []int{1, 2} {
		if objectExists(t, f.env.store, ManifestKey(dataset, f.snaps[i])) {
			t.Fatalf("可删快照 S%d 的 manifest 未回收", i+1)
		}
	}
	if !objectExists(t, f.env.store, ManifestKey(dataset, f.snaps[3])) ||
		!objectExists(t, f.env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象或 S4 manifest 被删除")
	}
}
