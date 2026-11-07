package index

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// local_gc_test.go 覆盖 LocalGC（节点级本地副本回收）的口径：
// 活块集合 = 各数据集当前快照（CURRENT）引用的块 ∪ 有效读者 claim 保护的快照引用的
// 块；删除 = 新快照不含（design.md《提交》）落在集合差上；任一数据集索引读不到（确定不存在除外）
// 或损坏时整轮不删任何块（fail-safe）；删除动作走 cas.Evict。

// TestLocalGCReclaimsLocalCopyOfDeletedEntry：数据集里某条目被删（提交一个不含它的
// 新快照，design.md《提交》删除 = 新快照不含）后，它的本地副本被 LocalGC 回收；仍在快照里的
// 条目引用的块保留。
func TestLocalGCReclaimsLocalCopyOfDeletedEntry(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const dataset = "lgc-del"

	a1 := env.putBlock(t, "lgc-del-a")
	b1 := env.putBlock(t, "lgc-del-b")
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{mkEntry("/d/a", a1), mkEntry("/d/b", b1)}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 删除 /d/b：新快照不再引用 b1（旧快照对象可能还在保留窗口内，但保留窗口只
	// 庇护对象存储，不庇护本地副本）。
	if _, err := w.CommitChanges(ctx, dataset, nil, []Delete{{Path: "/d/b", Chunk: -1}}); err != nil {
		t.Fatalf("CommitChanges: %v", err)
	}

	g := NewLocalGC(env.store, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if removed != 1 {
		t.Fatalf("回收块 %d 个，期望 1（b1）", removed)
	}
	if blockExists(t, env.cs, b1) {
		t.Fatal("已删条目的本地副本未被回收")
	}
	if !blockExists(t, env.cs, a1) {
		t.Fatal("仍在快照中的条目引用的块被误删")
	}

	// 再扫一轮幂等：无可回收。
	removed, err = g.Sweep(ctx, []string{dataset})
	if err != nil || removed != 0 {
		t.Fatalf("第二次 LocalGC Sweep %v，回收 %d 个，期望 (0, nil)", err, removed)
	}
}

// TestLocalGCPerNodeSweepDoesNotEvictOtherDatasetBlocks：同一节点上有两个数据集，
// 按节点全集扫一轮只回收真孤儿，不会把另一个数据集的活块当孤儿删掉。
// 这是「按单个数据集算活块集合会误删其他数据集本地副本」缺陷的回归哨兵。
func TestLocalGCPerNodeSweepDoesNotEvictOtherDatasetBlocks(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const dsA = "lgc-node-a"
	const dsB = "lgc-node-b"

	a1 := env.putBlock(t, "lgc-node-a1")
	b1 := env.putBlock(t, "lgc-node-b1")
	orphan := env.putBlock(t, "lgc-node-orphan")

	w := NewWriter(env.store, env.cs, cfg)
	commitSnapshot(t, w, env.store, dsA, []Entry{mkEntry("/a/x", a1)})
	commitSnapshot(t, w, env.store, dsB, []Entry{mkEntry("/b/x", b1)})

	g := NewLocalGC(env.store, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dsA, dsB})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if removed != 1 {
		t.Fatalf("回收块 %d 个，期望 1（orphan）", removed)
	}
	if blockExists(t, env.cs, orphan) {
		t.Fatal("孤儿块未被回收")
	}
	if !blockExists(t, env.cs, a1) || !blockExists(t, env.cs, b1) {
		t.Fatal("另一数据集的活块被误删（按数据集算活块的旧缺陷复发）")
	}
}

// TestLocalGCOtherNodeClaimKeepsOldSnapshotBlocks：其他节点的读者 claim 还在保护
// 旧快照（D33）时，即使该快照已出保留窗口、对象侧照常回收，它引用的块仍是活块，
// 本地副本不删；仅出窗且无 claim 的快照的块被回收。
func TestLocalGCOtherNodeClaimKeepsOldSnapshotBlocks(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "lgc-d33"
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10, ReaderGrace: 30 * time.Second}

	b1 := env.putBlock(t, "lgc-d33-v1")
	b2 := env.putBlock(t, "lgc-d33-v2")
	b3 := env.putBlock(t, "lgc-d33-v3")
	w := NewWriter(env.store, env.cs, cfg)
	s1 := commitDistinct(t, w, env.store, dataset, "/k/a", b1)
	commitDistinct(t, w, env.store, dataset, "/k/a", b2)
	commitDistinct(t, w, env.store, dataset, "/k/a", b3)

	// 另一节点 worker-b 声明跟随 s1（心跳新鲜）。
	base := time.Unix(1_700_000_000, 0)
	reg := NewReaderRegistry(env.store, "worker-b", 10*time.Second)
	reg.now = func() time.Time { return base }
	if err := reg.Declare(ctx, dataset, s1); err != nil {
		t.Fatalf("Declare(s1): %v", err)
	}

	// 对象侧：protected = {CURRENT, s1}，出窗且无 claim 的 s2 对象被回收。
	col := NewCollector(env.store, cfg)
	col.now = func() time.Time { return base }
	removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Collector Sweep: %v", err)
	}
	if removedObjects != 2 {
		t.Fatalf("删除对象 %d 个，期望 2（s2）", removedObjects)
	}

	// 块侧：活块 = CURRENT 的 b3 ∪ claim 保护的 b1；仅 s2 独有的 b2 被回收。
	g := NewLocalGC(env.store, env.cs, cfg)
	g.now = func() time.Time { return base }
	removed, err := g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if removed != 1 {
		t.Fatalf("回收块 %d 个，期望 1（b2）", removed)
	}
	if !blockExists(t, env.cs, b1) {
		t.Fatal("被其他节点 claim 保护的旧快照块被误删（违反 D33）")
	}
	if blockExists(t, env.cs, b2) {
		t.Fatal("出窗且无 claim 的块未被回收")
	}
	if !blockExists(t, env.cs, b3) {
		t.Fatal("CURRENT 引用的块被误删")
	}
}

// TestLocalGCNoCurrentDatasetReclaimsLocalCopies：数据集没有 CURRENT（已整体删除
// 或尚未提交）不算错误——它不贡献任何活块，其本地副本被回收；活数据集的块不受牵连。
func TestLocalGCNoCurrentDatasetReclaimsLocalCopies(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const gone = "lgc-gone"
	const alive = "lgc-alive"

	g1 := env.putBlock(t, "lgc-gone-1")
	g2 := env.putBlock(t, "lgc-gone-2")
	a1 := env.putBlock(t, "lgc-alive-1")
	w := NewWriter(env.store, env.cs, cfg)
	commitSnapshot(t, w, env.store, gone, []Entry{mkEntry("/g/a", g1), mkEntry("/g/b", g2)})
	commitSnapshot(t, w, env.store, alive, []Entry{mkEntry("/a/x", a1)})

	// 模拟 gone 数据集整体删除：CURRENT 被删（快照对象是否已清不影响块侧判定——
	// LocalGC 只经 CURRENT 与 claim 读取索引）。
	if err := env.store.Delete(ctx, CurrentKey(gone)); err != nil {
		t.Fatalf("Delete CURRENT: %v", err)
	}

	g := NewLocalGC(env.store, env.cs, cfg)
	// datasets 里混入一个存储中完全不存在的数据集名：确定不存在不是错误。
	removed, err := g.Sweep(ctx, []string{gone, alive, "lgc-ghost"})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if removed != 2 {
		t.Fatalf("回收块 %d 个，期望 2（g1、g2）", removed)
	}
	if blockExists(t, env.cs, g1) || blockExists(t, env.cs, g2) {
		t.Fatal("无 CURRENT 数据集的本地副本未被回收")
	}
	if !blockExists(t, env.cs, a1) {
		t.Fatal("活数据集的块被误删")
	}
}

// TestLocalGCBlockSharedByTwoDatasetsKept：同一块（同一内容指纹）被多个数据集共享
// 时，一个数据集删掉自己的条目不构成回收理由——只要还有活数据集引用它，就是活块。
func TestLocalGCBlockSharedByTwoDatasetsKept(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const dsA = "lgc-share-a"
	const dsB = "lgc-share-b"

	// 一份内容只落一次盘，两个数据集的条目指向同一指纹（内容寻址共享）。
	shared := env.putBlock(t, "lgc-shared-content")
	only := env.putBlock(t, "lgc-share-b-only")
	orphan := env.putBlock(t, "lgc-share-orphan")

	w := NewWriter(env.store, env.cs, cfg)
	commitSnapshot(t, w, env.store, dsA, []Entry{mkEntry("/a/x", shared)})
	commitSnapshot(t, w, env.store, dsB, []Entry{mkEntry("/b/x", shared), mkEntry("/b/y", only)})
	// B 删掉共享条目：从 B 的视角 shared 不再是它的块。
	if _, err := w.CommitChanges(ctx, dsB, nil, []Delete{{Path: "/b/x", Chunk: -1}}); err != nil {
		t.Fatalf("CommitChanges: %v", err)
	}

	g := NewLocalGC(env.store, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dsA, dsB})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	// 只有真孤儿 orphan 被回收；shared 仍被活数据集 A 的 CURRENT 引用，必须保留。
	if removed != 1 {
		t.Fatalf("回收块 %d 个，期望 1（orphan）", removed)
	}
	if blockExists(t, env.cs, orphan) {
		t.Fatal("孤儿块未被回收")
	}
	if !blockExists(t, env.cs, shared) {
		t.Fatal("与活数据集共享的块被误删")
	}
	if !blockExists(t, env.cs, only) {
		t.Fatal("B 仍在用的块被误删")
	}
}

// TestLocalGCManifestUnavailableAbortsWholeRound：fail-safe——当前快照的 manifest
// 缺失（读不到）时无法算出活块集合，整轮不删任何块并回调
// OnSweepSkip("manifest_unavailable")；Sweep 返回错误，磁盘文件全部还在。
func TestLocalGCManifestUnavailableAbortsWholeRound(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "lgc-fs-manifest"
	skipCalls := 0
	cfg := Config{MaxEntriesPerShard: 10, OnSweepSkip: func(ds, reason string) {
		skipCalls++
		if ds != dataset || reason != "manifest_unavailable" {
			t.Errorf("OnSweepSkip(%q, %q)，期望 (%q, manifest_unavailable)", ds, reason, dataset)
		}
	}}

	live := env.putBlock(t, "lgc-fs-live")
	orphan := env.putBlock(t, "lgc-fs-orphan")
	w := NewWriter(env.store, env.cs, cfg)
	snapID, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", live)})
	if err := env.store.Delete(ctx, ManifestKey(dataset, snapID)); err != nil {
		t.Fatalf("Delete manifest: %v", err)
	}

	g := NewLocalGC(env.store, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dataset})
	if err == nil {
		t.Fatal("manifest 缺失的 LocalGC Sweep 未返回错误")
	}
	if removed != 0 {
		t.Fatalf("fail-safe 下回收块 %d 个，期望 0", removed)
	}
	if skipCalls != 1 {
		t.Fatalf("OnSweepSkip 调用 %d 次，期望 1", skipCalls)
	}
	if !blockExists(t, env.cs, live) || !blockExists(t, env.cs, orphan) {
		t.Fatal("fail-safe 整轮放弃时磁盘块被删除")
	}
}

// TestLocalGCCorruptShardAbortsWholeRound：fail-safe——当前快照的分片内容被篡改
// （指纹不符，ErrInvalidShard）时整轮不删任何块。
func TestLocalGCCorruptShardAbortsWholeRound(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const dataset = "lgc-fs-shard"

	live := env.putBlock(t, "lgc-fs2-live")
	orphan := env.putBlock(t, "lgc-fs2-orphan")
	w := NewWriter(env.store, env.cs, cfg)
	_, m := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", live)})

	// 篡改 CURRENT 引用的分片：翻 1 字节后同键覆盖，内容指纹不再匹配 manifest。
	raw, err := getObject(ctx, env.store, m.Shards[0].Key)
	if err != nil {
		t.Fatalf("read shard: %v", err)
	}
	raw[0] ^= 0xFF
	if _, err := env.store.Put(ctx, m.Shards[0].Key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("tamper shard: %v", err)
	}

	g := NewLocalGC(env.store, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dataset})
	if !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("Sweep 错误 = %v，期望 ErrInvalidShard", err)
	}
	if removed != 0 {
		t.Fatalf("ErrInvalidShard fail-safe 下回收块 %d 个，期望 0", removed)
	}
	if !blockExists(t, env.cs, live) || !blockExists(t, env.cs, orphan) {
		t.Fatal("fail-safe 整轮放弃时磁盘块被删除")
	}
}

// TestLocalGCShardReadErrorAbortsWholeRound：fail-safe——分片对象读取报错（非
// 不存在，模拟瞬时 I/O 故障）时整轮不删任何块。
func TestLocalGCShardReadErrorAbortsWholeRound(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const dataset = "lgc-fs-get"

	live := env.putBlock(t, "lgc-fs3-live")
	orphan := env.putBlock(t, "lgc-fs3-orphan")
	w := NewWriter(env.store, env.cs, cfg)
	_, m := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", live)})

	fault := &failGetStore{Store: env.store, key: m.Shards[0].Key, err: errors.New("injected i/o failure")}
	g := NewLocalGC(fault, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dataset})
	if err == nil {
		t.Fatal("分片读失败的 LocalGC Sweep 未返回错误")
	}
	if removed != 0 {
		t.Fatalf("分片读失败 fail-safe 下回收块 %d 个，期望 0", removed)
	}
	if !blockExists(t, env.cs, live) || !blockExists(t, env.cs, orphan) {
		t.Fatal("fail-safe 整轮放弃时磁盘块被删除")
	}
}

// TestLocalGCSkipsInUseBlockWithoutErrorAndRetriesNextRound：正在被读的块
// （pin>0，Evict 返回 ErrInUse）本轮跳过、不报错，本地副本保留；Unpin 后
// 下一轮被回收。
func TestLocalGCSkipsInUseBlockWithoutErrorAndRetriesNextRound(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const dataset = "lgc-inuse"

	live := env.putBlock(t, "lgc-inuse-live")
	reading := env.putBlock(t, "lgc-inuse-reading")
	w := NewWriter(env.store, env.cs, cfg)
	commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", live)})

	if err := env.cs.Pin(reading); err != nil {
		t.Fatalf("Pin(%s): %v", reading, err)
	}

	g := NewLocalGC(env.store, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("正在被读的块不应让本轮报错: %v", err)
	}
	if removed != 0 {
		t.Fatalf("ErrInUse 的块被计为回收 %d 个，期望 0", removed)
	}
	if !blockExists(t, env.cs, reading) {
		t.Fatal("正在被读（pin>0）的块被删除")
	}
	if !blockExists(t, env.cs, live) {
		t.Fatal("活块被删除")
	}

	// 读结束（Unpin）后下一轮回收。
	env.cs.Unpin(reading)
	removed, err = g.Sweep(ctx, []string{dataset})
	if err != nil || removed != 1 {
		t.Fatalf("第二轮 LocalGC Sweep %v，回收 %d 个，期望 (1, nil)", err, removed)
	}
	if blockExists(t, env.cs, reading) {
		t.Fatal("Unpin 后孤儿块未被回收")
	}
}

// TestLocalGCEvictIgnoresRefcountUnlikeRemove：删除走 cas.Evict 而非 cas.Remove 的
// 语义差异——引用计数（Acquire）是快照级逻辑簿记，与本地副本在不在无关：refs>0 的
// 孤儿块 Remove 会拒绝（ErrInUse），LocalGC 经 Evict 照样回收。
func TestLocalGCEvictIgnoresRefcountUnlikeRemove(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 10}
	const dataset = "lgc-evict"

	live := env.putBlock(t, "lgc-evict-live")
	orphan1 := env.putBlock(t, "lgc-evict-orphan1")
	orphan2 := env.putBlock(t, "lgc-evict-orphan2")
	w := NewWriter(env.store, env.cs, cfg)
	commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", live)})

	for _, fp := range []cas.Fingerprint{orphan1, orphan2} {
		if err := env.cs.Acquire(fp); err != nil {
			t.Fatalf("Acquire(%s): %v", fp, err)
		}
		if err := env.cs.Acquire(fp); err != nil {
			t.Fatalf("Acquire(%s) x2: %v", fp, err)
		}
	}

	// Remove 的口径对照：refs>0 直接 ErrInUse。
	if err := env.cs.Remove(orphan1); !errors.Is(err, cas.ErrInUse) {
		t.Fatalf("Remove(refs>0) = %v，期望 ErrInUse", err)
	}

	// Evict 的口径：LocalGC 不看引用计数，refs>0 的孤儿块本轮被回收。
	g := NewLocalGC(env.store, env.cs, cfg)
	removed, err := g.Sweep(ctx, []string{dataset})
	if err != nil {
		t.Fatalf("LocalGC Sweep: %v", err)
	}
	if removed != 2 {
		t.Fatalf("回收块 %d 个，期望 2（refs>0 不挡 Evict）", removed)
	}
	if blockExists(t, env.cs, orphan1) || blockExists(t, env.cs, orphan2) {
		t.Fatal("refs>0 的孤儿块未被 Evict（误用了 Remove 语义？）")
	}
	if !blockExists(t, env.cs, live) {
		t.Fatal("活块被删除")
	}
}

// failGetStore 包装 Store，对指定对象键的 Get 注入读取错误（其余原样透传），
// 模拟瞬时 I/O 故障。
type failGetStore struct {
	objectstore.Store
	key string
	err error
}

func (s *failGetStore) Get(ctx context.Context, key string, w io.Writer) error {
	if key == s.key {
		return s.err
	}
	return s.Store.Get(ctx, key, w)
}
