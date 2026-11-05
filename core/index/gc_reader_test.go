package index

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// commitDistinct 提交一个只含单条、使用给定块的数据集快照。
func commitDistinct(t *testing.T, w *Writer, store objectstore.Store, dataset, path string, fp cas.Fingerprint) string {
	t.Helper()
	id, _ := commitSnapshot(t, w, store, dataset, []Entry{mkEntry(path, fp)})
	return id
}

// TestSweepReaderProtectsOutOfWindow：有效 claim 的 PreviousSnapID 在宽限期内保护
// 出窗快照——对象侧（Collector）其 manifest 不删、块侧（LocalGC）其引用的块是活块不
// 回收；宽限期过后 PreviousSnapID 失保，该快照对象与块本地副本一并被回收。
func TestSweepReaderProtectsOutOfWindow(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "reader-prev"
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10, ReaderGrace: 30 * time.Second}

	b1 := env.putBlock(t, "reader-v1")
	b2 := env.putBlock(t, "reader-v2")
	b3 := env.putBlock(t, "reader-v3")
	w := NewWriter(env.store, env.cs, cfg)
	s1 := commitDistinct(t, w, env.store, dataset, "/k/a", b1)
	commitDistinct(t, w, env.store, dataset, "/k/a", b2)
	s3 := commitDistinct(t, w, env.store, dataset, "/k/a", b3)

	base := time.Unix(1_700_000_000, 0)
	now := base
	reg := NewReaderRegistry(env.store, "node-a", 10*time.Second)
	reg.now = func() time.Time { return now }
	if err := reg.Declare(ctx, dataset, s1); err != nil {
		t.Fatalf("Declare(s1): %v", err)
	}
	now = base.Add(time.Second)
	if err := reg.Declare(ctx, dataset, s3); err != nil {
		t.Fatalf("Declare(s3): %v", err)
	}
	// claim: SnapID=s3, PreviousSnapID=s1, PreviousUntil=base+11s, UpdatedAt=base+1s.

	col := NewCollector(env.store, cfg)
	col.now = func() time.Time { return now }
	g := NewLocalGC(env.store, env.cs, cfg)
	g.now = func() time.Time { return now }

	// 宽限期内：protected = {s3, s1}；仅出窗且无 claim 的 s2 的对象被回收。
	now = base.Add(5 * time.Second)
	removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if removedObjects != 2 {
		t.Fatalf("宽限期内删除对象 %d 个，期望 2：仅 s2 出窗", removedObjects)
	}
	// 块侧活块 = CURRENT(s3) ∪ claim Previous(s1)：仅 s2 独有的 b2 被回收。
	removedBlocks, err := g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if removedBlocks != 1 {
		t.Fatalf("宽限期内删除块 %d 个，期望 1：仅 b2 非活块", removedBlocks)
	}
	if !blockExists(t, env.cs, b1) {
		t.Fatal("claim PreviousSnapID 引用的块在宽限期内被删除")
	}
	if !blockExists(t, env.cs, b3) {
		t.Fatal("CURRENT 块被删除")
	}
	if blockExists(t, env.cs, b2) {
		t.Fatal("出窗且无 claim 的块未被删除")
	}
	if !objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("宽限期内 PreviousSnapID 的 manifest 被删除")
	}

	// 宽限期过后（claim 心跳仍有效）：s1 失保，其对象与块本地副本被回收。
	now = base.Add(15 * time.Second)
	removedObjects, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("第二次 Sweep: %v", err)
	}
	if removedObjects != 2 {
		t.Fatalf("宽限期后删除对象 %d 个，期望 2：s1 失保", removedObjects)
	}
	removedBlocks, err = g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("第二次 LocalGC Sweep: %v", err)
	}
	if removedBlocks != 1 {
		t.Fatalf("宽限期后删除块 %d 个，期望 1：b1 失保", removedBlocks)
	}
	if blockExists(t, env.cs, b1) {
		t.Fatal("宽限期后 PreviousSnapID 引用的块未被删除")
	}
	if objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("宽限期后 PreviousSnapID 的 manifest 未被删除")
	}
	// CURRENT 与其对象始终保留。
	if !blockExists(t, env.cs, b3) || !objectExists(t, env.store, ManifestKey(dataset, s3)) {
		t.Fatal("CURRENT 快照或块被误删")
	}
}

// TestSweepReaderHeartbeatExpired：claim 的 UpdatedAt 超过 ReaderGrace 视为读者已死，
// 其 SnapID 不再保护出窗快照——对象（Collector）与块本地副本（LocalGC）都可回收。
func TestSweepReaderHeartbeatExpired(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "reader-dead"
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10, ReaderGrace: 10 * time.Second}

	b1 := env.putBlock(t, "dead-v1")
	b2 := env.putBlock(t, "dead-v2")
	w := NewWriter(env.store, env.cs, cfg)
	s1 := commitDistinct(t, w, env.store, dataset, "/k/a", b1)
	s2 := commitDistinct(t, w, env.store, dataset, "/k/a", b2)

	base := time.Unix(1_700_000_000, 0)
	now := base
	reg := NewReaderRegistry(env.store, "node-a", 5*time.Second)
	reg.now = func() time.Time { return now }
	if err := reg.Declare(ctx, dataset, s1); err != nil {
		t.Fatalf("Declare(s1): %v", err)
	}

	col := NewCollector(env.store, cfg)
	col.now = func() time.Time { return now }
	g := NewLocalGC(env.store, env.cs, cfg)
	g.now = func() time.Time { return now }

	// 心跳仍有效：s1 受保护，对象与块都无删除。
	now = base.Add(5 * time.Second)
	ro, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if ro != 0 {
		t.Fatalf("心跳有效时删除对象 %d 个，期望 0", ro)
	}
	rb, err := g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if rb != 0 {
		t.Fatalf("心跳有效时删除块 %d 个，期望 0", rb)
	}

	// 心跳过期：claim 整体忽略，s1 出窗可删。
	now = base.Add(11 * time.Second)
	ro, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep after expiry: %v", err)
	}
	if ro != 2 {
		t.Fatalf("心跳过期后删除对象 %d 个，期望 2", ro)
	}
	rb, err = g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("LocalGC Sweep after expiry: %v", err)
	}
	if rb != 1 {
		t.Fatalf("心跳过期后删除块 %d 个，期望 1", rb)
	}
	if blockExists(t, env.cs, b1) || objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("心跳过期后 s1 及其块未被删除")
	}
	if !blockExists(t, env.cs, b2) || !objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("CURRENT 快照或块被误删")
	}
}

// TestSweepReaderNoClaimKeepsWindow：无 claim 时对象侧沿用保留窗口语义；块侧按节点
// 口径只有 CURRENT 是活块，窗口内旧快照独有的块本地副本也被回收。
func TestSweepReaderNoClaimKeepsWindow(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "reader-none"
	cfg := Config{RetainSnapshots: 2, MaxEntriesPerShard: 10}

	b1 := env.putBlock(t, "none-v1")
	b2 := env.putBlock(t, "none-v2")
	b3 := env.putBlock(t, "none-v3")
	w := NewWriter(env.store, env.cs, cfg)
	s1 := commitDistinct(t, w, env.store, dataset, "/k/a", b1)
	s2 := commitDistinct(t, w, env.store, dataset, "/k/a", b2)
	s3 := commitDistinct(t, w, env.store, dataset, "/k/a", b3)

	col := NewCollector(env.store, cfg)
	ro, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// 对象窗口 = {s3, s2}；s1 的 1 个 manifest + 1 个分片被回收。
	if ro != 2 {
		t.Fatalf("无 claim 时删除对象 %d 个，期望 2", ro)
	}
	// 块：活块 = CURRENT(s3) 的 b3；b1、b2 无 claim 保护，本地副本被回收。
	g := NewLocalGC(env.store, env.cs, cfg)
	rb, err := g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if rb != 2 {
		t.Fatalf("无 claim 时删除块 %d 个，期望 2（b1、b2）", rb)
	}
	if objectExists(t, env.store, ManifestKey(dataset, s1)) || blockExists(t, env.cs, b1) {
		t.Fatal("出窗快照 s1 或块未被删除")
	}
	if blockExists(t, env.cs, b2) {
		t.Fatal("窗口内旧快照独有、无 claim 的块 b2 本地副本未被回收")
	}
	for _, id := range []string{s2, s3} {
		if !objectExists(t, env.store, ManifestKey(dataset, id)) {
			t.Fatalf("窗口内 manifest %q 被删除", id)
		}
	}
	if !blockExists(t, env.cs, b3) {
		t.Fatal("CURRENT 块被删除")
	}
}

// TestSweepReaderCorruptClaimFailsSafe：claim JSON 损坏时 Collector 与 LocalGC 都
// 返回错误且本轮不做任何删除（包括本可回收的出窗快照对象与块）。
func TestSweepReaderCorruptClaimFailsSafe(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "reader-corrupt"
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10}

	b1 := env.putBlock(t, "corrupt-v1")
	b2 := env.putBlock(t, "corrupt-v2")
	w := NewWriter(env.store, env.cs, cfg)
	s1 := commitDistinct(t, w, env.store, dataset, "/k/a", b1)
	s2 := commitDistinct(t, w, env.store, dataset, "/k/a", b2)

	bad := []byte("{broken")
	if _, err := env.store.Put(ctx, ReaderKey(dataset, "bad-node"), bytes.NewReader(bad), int64(len(bad))); err != nil {
		t.Fatalf("Put corrupt claim: %v", err)
	}

	col := NewCollector(env.store, cfg)
	ro, err := col.Sweep(ctx, dataset)
	if err == nil {
		t.Fatal("损坏 claim 的对象 Sweep 未返回错误")
	}
	if ro != 0 {
		t.Fatalf("损坏 claim 时删除对象 %d 个，期望 0", ro)
	}
	g := NewLocalGC(env.store, env.cs, cfg)
	rb, err := g.Sweep(ctx, []string{dataset})
	if err == nil {
		t.Fatal("损坏 claim 的块 Sweep 未返回错误")
	}
	if rb != 0 {
		t.Fatalf("损坏 claim 时删除块 %d 个，期望 0", rb)
	}
	if !blockExists(t, env.cs, b1) || !objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("fail-safe 下出窗快照或块被删除")
	}
	if !blockExists(t, env.cs, b2) || !objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("CURRENT 快照或块被删除")
	}
}

// TestSweepReaderProtectedManifestMissing：protected 快照 manifest 缺失时调用
// OnSweepSkip("manifest_unavailable")，本轮该数据集一个对象都不删（无法知道它复用了
// 哪些分片对象，继续删可能误删）；Collector 本就不碰本地块，块全部保留。
func TestSweepReaderProtectedManifestMissing(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "reader-missing"
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

	b1 := env.putBlock(t, "missing-v1")
	b2 := env.putBlock(t, "missing-v2")
	b3 := env.putBlock(t, "missing-v3")
	w := NewWriter(env.store, env.cs, cfg)
	s1 := commitDistinct(t, w, env.store, dataset, "/k/a", b1)
	s2 := commitDistinct(t, w, env.store, dataset, "/k/a", b2)
	s3 := commitDistinct(t, w, env.store, dataset, "/k/a", b3)

	base := time.Unix(1_700_000_000, 0)
	now := base
	reg := NewReaderRegistry(env.store, "node-a", 10*time.Second)
	reg.now = func() time.Time { return now }
	if err := reg.Declare(ctx, dataset, s1); err != nil {
		t.Fatalf("Declare(s1): %v", err)
	}
	// 破坏受保护快照 s1：manifest 缺失（其分片对象仍在）。
	if err := env.store.Delete(ctx, ManifestKey(dataset, s1)); err != nil {
		t.Fatalf("Delete manifest: %v", err)
	}

	col := NewCollector(env.store, cfg)
	col.now = func() time.Time { return now }
	ro, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if skipCalls != 1 || skipReason != "manifest_unavailable" {
		t.Fatalf("OnSweepSkip 调用 %d 次、reason=%q，期望 1 次 manifest_unavailable", skipCalls, skipReason)
	}
	// 本轮该数据集一个对象都不删：可删快照 s2 的对象也在。
	if ro != 0 {
		t.Fatalf("protected manifest 缺失时删除对象 %d 个，期望 0（整轮跳过）", ro)
	}
	for _, fp := range []cas.Fingerprint{b1, b2, b3} {
		if !blockExists(t, env.cs, fp) {
			t.Fatalf("Collector 回收动了本地块 %s（块回收归 LocalGC）", fp)
		}
	}
	if !objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("整轮跳过时窗口外快照 s2 的 manifest 被删除")
	}
	if !objectExists(t, env.store, ManifestKey(dataset, s3)) {
		t.Fatal("CURRENT manifest 被删除")
	}
	// s1 虽 manifest 缺失，其目录下分片仍作为快照对象保留（本轮不删任何东西）。
	var s1Shard string
	for _, key := range listObjectKeys(t, env.store, SnapshotPrefix(dataset, s1)) {
		if strings.HasPrefix(key, SnapshotPrefix(dataset, s1)) && !strings.HasSuffix(key, "/manifest.json") {
			s1Shard = key
		}
	}
	if s1Shard == "" {
		t.Fatal("测试构造失败：s1 无分片对象")
	}
	if !objectExists(t, env.store, s1Shard) {
		t.Fatalf("protected 快照 s1 的分片 %q 被删除", s1Shard)
	}
}
