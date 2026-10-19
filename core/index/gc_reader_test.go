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
// 出窗快照及其块；宽限期过后 PreviousSnapID 失保，该快照与块被回收。
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

	col := NewCollector(env.store, env.cs, cfg)
	col.now = func() time.Time { return now }

	// 宽限期内：protected = {s3, s1}；仅出窗且无 claim 的 s2 被回收。
	now = base.Add(5 * time.Second)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if removedBlocks != 1 || removedObjects != 2 {
		t.Fatalf("宽限期内删除 (%d, %d)，期望 (1, 2)：仅 s2 出窗", removedBlocks, removedObjects)
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

	// 宽限期过后（claim 心跳仍有效）：s1 失保，其对象与块被回收。
	now = base.Add(15 * time.Second)
	removedBlocks, removedObjects, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("第二次 Sweep: %v", err)
	}
	if removedBlocks != 1 || removedObjects != 2 {
		t.Fatalf("宽限期后删除 (%d, %d)，期望 (1, 2)：s1 失保", removedBlocks, removedObjects)
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
// 其 SnapID 不再保护出窗快照。
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

	col := NewCollector(env.store, env.cs, cfg)
	col.now = func() time.Time { return now }

	// 心跳仍有效：s1 受保护，无删除。
	now = base.Add(5 * time.Second)
	rb, ro, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if rb != 0 || ro != 0 {
		t.Fatalf("心跳有效时删除 (%d, %d)，期望 (0, 0)", rb, ro)
	}

	// 心跳过期：claim 整体忽略，s1 出窗可删。
	now = base.Add(11 * time.Second)
	rb, ro, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep after expiry: %v", err)
	}
	if rb != 1 || ro != 2 {
		t.Fatalf("心跳过期后删除 (%d, %d)，期望 (1, 2)", rb, ro)
	}
	if blockExists(t, env.cs, b1) || objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("心跳过期后 s1 及其块未被删除")
	}
	if !blockExists(t, env.cs, b2) || !objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("CURRENT 快照或块被误删")
	}
}

// TestSweepReaderNoClaimKeepsWindow：无 claim 时沿用原保留窗口语义。
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

	col := NewCollector(env.store, env.cs, cfg)
	rb, ro, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// 窗口 = {s3, s2}；s1 的 1 个 manifest + 1 个分片与块 b1 被回收。
	if rb != 1 || ro != 2 {
		t.Fatalf("无 claim 时删除 (%d, %d)，期望 (1, 2)", rb, ro)
	}
	if objectExists(t, env.store, ManifestKey(dataset, s1)) || blockExists(t, env.cs, b1) {
		t.Fatal("出窗快照 s1 或块未被删除")
	}
	for _, id := range []string{s2, s3} {
		if !objectExists(t, env.store, ManifestKey(dataset, id)) {
			t.Fatalf("窗口内 manifest %q 被删除", id)
		}
	}
}

// TestSweepReaderCorruptClaimFailsSafe：claim JSON 损坏时 Sweep 返回错误且本轮不做
// 任何删除（包括本可回收的出窗快照与块）。
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

	col := NewCollector(env.store, env.cs, cfg)
	rb, ro, err := col.Sweep(ctx, dataset)
	if err == nil {
		t.Fatal("损坏 claim 的 Sweep 未返回错误")
	}
	if rb != 0 || ro != 0 {
		t.Fatalf("损坏 claim 时删除 (%d, %d)，期望 (0, 0)", rb, ro)
	}
	if !blockExists(t, env.cs, b1) || !objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("fail-safe 下出窗快照或块被删除")
	}
	if !blockExists(t, env.cs, b2) || !objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("CURRENT 快照或块被删除")
	}
}

// TestSweepReaderProtectedManifestMissing：protected manifest 缺失时调用 OnSweepSkip、
// 跳过本轮块删除，但其他可删快照对象仍被回收。
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

	col := NewCollector(env.store, env.cs, cfg)
	col.now = func() time.Time { return now }
	rb, ro, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if skipCalls != 1 || skipReason != "manifest_unavailable" {
		t.Fatalf("OnSweepSkip 调用 %d 次、reason=%q，期望 1 次 manifest_unavailable", skipCalls, skipReason)
	}
	if rb != 0 {
		t.Fatalf("protected manifest 缺失时删除块 %d 个，期望 0", rb)
	}
	// 其他可删快照对象仍回收：s2 的 manifest + 分片 = 2。
	if ro != 2 {
		t.Fatalf("删除快照对象 %d 个，期望 2（s2）", ro)
	}
	// 全部块均保留（本轮跳过块删除）。
	for _, fp := range []cas.Fingerprint{b1, b2, b3} {
		if !blockExists(t, env.cs, fp) {
			t.Fatalf("跳过块删除时块 %s 被删除", fp)
		}
	}
	if objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("可删快照 s2 的 manifest 未被回收")
	}
	if !objectExists(t, env.store, ManifestKey(dataset, s3)) {
		t.Fatal("CURRENT manifest 被删除")
	}
	// s1 虽 manifest 缺失，其目录下分片作为 protected 快照对象保留。
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
