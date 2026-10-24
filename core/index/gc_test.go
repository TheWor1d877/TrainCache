package index

import (
	"context"
	"errors"
	"strings"
	"testing"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// mkEntry 构造一条测试索引条目（chunk 0、固定 size/node）。
func mkEntry(path string, fp cas.Fingerprint) Entry {
	return Entry{Path: path, Chunk: 0, Size: 7, Node: "n1", Fingerprint: fp}
}

// objectExists 判断对象键在对象存储中是否存在。
func objectExists(t *testing.T, store objectstore.Store, key string) bool {
	t.Helper()
	_, err := store.GetETag(context.Background(), key)
	switch {
	case err == nil:
		return true
	case errors.Is(err, objectstore.ErrNotFound):
		return false
	default:
		t.Fatalf("GetETag(%q): %v", key, err)
		return false
	}
}

// blockExists 判断 CAS 块是否存在且校验可读（Open 通过后即关闭）。
func blockExists(t *testing.T, cs *cas.Store, fp cas.Fingerprint) bool {
	t.Helper()
	r, err := cs.Open(fp)
	switch {
	case err == nil:
		if err := r.Close(); err != nil {
			t.Fatalf("close block %s: %v", fp, err)
		}
		return true
	case errors.Is(err, cas.ErrNotFound):
		return false
	default:
		t.Fatalf("Open(%s): %v", fp, err)
		return false
	}
}

// commitSnapshot 提交一次快照，返回 snapID 与经 CURRENT 读回校验的快照 manifest。
func commitSnapshot(t *testing.T, w *Writer, store objectstore.Store, dataset string, entries []Entry) (string, *Manifest) {
	t.Helper()
	snapID, err := w.Commit(context.Background(), dataset, entries)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, m := currentSnapshot(t, store, dataset)
	if got != snapID {
		t.Fatalf("CURRENT snapID = %q, want %q", got, snapID)
	}
	return snapID, m
}

// listObjectKeys 返回前缀下全部对象键（走 Store.List）。
func listObjectKeys(t *testing.T, store objectstore.Store, prefix string) []string {
	t.Helper()
	keys, err := store.List(context.Background(), prefix)
	if err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	return keys
}

// TestSweepKeepsLiveBlocks：保留窗口内的一切都不删——CURRENT 引用的块、窗口内旧快照
// 引用的块、窗口内快照的 manifest 与分片对象，以及 CURRENT 对象本身。
func TestSweepKeepsLiveBlocks(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{RetainSnapshots: 3, MaxEntriesPerShard: 2}
	const dataset = "keeps"

	b1 := env.putBlock(t, "keep-live-v1")
	b2 := env.putBlock(t, "keep-live-v2")

	w := NewWriter(env.store, env.cs, cfg)
	s1, m1 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b1)})
	s2, m2 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b2)})
	if s1 == s2 {
		t.Fatalf("两次 Commit 的 snapID 相同: %q", s1)
	}

	// 提交 2 次、每次 1 条 1 分片 → 恰 4 个快照对象（2 manifest + 2 分片）。
	objKeys := listObjectKeys(t, env.store, snapshotsPrefix(dataset))
	if len(objKeys) != 4 {
		t.Fatalf("快照对象 = %v，期望 4 个", objKeys)
	}

	col := NewCollector(env.store, env.cs, cfg)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if removedBlocks != 0 || removedObjects != 0 {
		t.Fatalf("Sweep 删除 (%d, %d)，期望 (0, 0)：两个快照均在 RetainSnapshots=3 窗口内",
			removedBlocks, removedObjects)
	}

	// CURRENT 引用的块（b2）与窗口内旧快照（s1）引用的块（b1）一个不删。
	if !blockExists(t, env.cs, b1) {
		t.Fatal("窗口内旧快照引用的块被删除")
	}
	if !blockExists(t, env.cs, b2) {
		t.Fatal("CURRENT 引用的块被删除")
	}
	// 窗口内快照的对象（manifest + 分片）一个不删。
	for _, k := range objKeys {
		if !objectExists(t, env.store, k) {
			t.Fatalf("窗口内对象 %q 被删除", k)
		}
	}
	for _, ref := range append(append([]ShardRef{}, m1.Shards...), m2.Shards...) {
		if !objectExists(t, env.store, ref.Key) {
			t.Fatalf("窗口内分片对象 %q 被删除", ref.Key)
		}
	}
	if !objectExists(t, env.store, ManifestKey(dataset, s1)) || !objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("窗口内 manifest 对象被删除")
	}
	// CURRENT 及其解析链保持完好。
	gotSnap, _ := currentSnapshot(t, env.store, dataset)
	if gotSnap != s2 {
		t.Fatalf("CURRENT snapID = %q，期望 %q", gotSnap, s2)
	}
}

// TestSweepRemovesOrphans：CURRENT + 最近 1 个快照（RetainSnapshots=2）之外的对象与
// 块被回收——3 次 Commit 后第一次快照出窗口：其 manifest 与不再被引用的分片被删、
// 仅其引用的块被删；被后续保留快照复用的分片（D18 完整键指向 s1 目录）跨窗口保留；
// 仍被保留快照引用的旧块不删。
func TestSweepRemovesOrphans(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{RetainSnapshots: 2, MaxEntriesPerShard: 2}
	const dataset = "orphans"

	a1 := env.putBlock(t, "orphan-a1")
	b1 := env.putBlock(t, "keep-b1")
	c1 := env.putBlock(t, "keep-c1")

	w := NewWriter(env.store, env.cs, cfg)
	s1, m1 := commitSnapshot(t, w, env.store, dataset, []Entry{
		mkEntry("/k/a", a1), mkEntry("/k/b", b1), mkEntry("/k/c", c1),
	})
	// 3 条 / 每片 2 条 → 分片 [a,b] 与 [c]。
	if len(m1.Shards) != 2 {
		t.Fatalf("s1 分片数 = %d，期望 2", len(m1.Shards))
	}

	// 第二次只改 1 条：分片 [c] 内容不变 → 复用（对象键仍指向 s1 目录）。
	a2 := env.putBlock(t, "changed-a2")
	s2, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/k/a", a2)})
	// 第三次再改 1 条：第一次快照出窗口（live = {s3, s2}）。
	b2 := env.putBlock(t, "changed-b2")
	s3, m3 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/k/b", b2)})

	// 找出被 s2/s3 从 s1 复用的分片键与 s1 的"弃用"分片键。
	reusedKey := ""
	for _, r1 := range m1.Shards {
		for _, r3 := range m3.Shards {
			if r1.Key == r3.Key {
				reusedKey = r1.Key
			}
		}
	}
	if reusedKey == "" {
		t.Fatalf("无复用分片：m1 = %+v, m3 = %+v", m1.Shards, m3.Shards)
	}
	if !strings.HasPrefix(reusedKey, SnapshotPrefix(dataset, s1)) {
		t.Fatalf("复用分片键 %q 不在 s1 目录下（D18 复用语义不符）", reusedKey)
	}
	droppedKey := ""
	for _, r1 := range m1.Shards {
		if r1.Key != reusedKey {
			droppedKey = r1.Key
		}
	}
	if droppedKey == "" {
		t.Fatalf("s1 的两个分片都被复用，测试构造错误：%+v", m1.Shards)
	}

	col := NewCollector(env.store, env.cs, cfg)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// 对象：s1 manifest + s1 弃用分片 = 2；块：仅 s1 引用的 a1 = 1。
	if removedBlocks != 1 || removedObjects != 2 {
		t.Fatalf("Sweep 删除 (%d, %d)，期望 (1, 2)", removedBlocks, removedObjects)
	}

	// 孤儿块 a1 被删；仍被保留快照引用的块（b1 被 s2 引用、c1/a2/b2 被窗口内快照引用）不删。
	if blockExists(t, env.cs, a1) {
		t.Fatal("孤儿块 a1 未被删除")
	}
	for _, fp := range []cas.Fingerprint{b1, c1, a2, b2} {
		if !blockExists(t, env.cs, fp) {
			t.Fatalf("活块 %s 被删除", fp)
		}
	}
	// 窗口外 s1 的 manifest 与弃用分片被删；复用分片对象跨窗口保留。
	if objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("窗口外 manifest 未被删除")
	}
	if objectExists(t, env.store, droppedKey) {
		t.Fatalf("窗口外未复用分片 %q 未被删除", droppedKey)
	}
	if !objectExists(t, env.store, reusedKey) {
		t.Fatalf("复用分片 %q 被误删（违反 D18）", reusedKey)
	}
	// 保留窗口内 s2/s3 的 manifest 与其全部分片在。
	for _, id := range []string{s2, s3} {
		if !objectExists(t, env.store, ManifestKey(dataset, id)) {
			t.Fatalf("窗口内 manifest %q 被删除", id)
		}
	}
	for _, ref := range m3.Shards {
		if !objectExists(t, env.store, ref.Key) {
			t.Fatalf("窗口内分片 %q 被删除", ref.Key)
		}
	}
	// CURRENT 对象绝不在删除范围。
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象被删除")
	}
	gotSnap, _ := currentSnapshot(t, env.store, dataset)
	if gotSnap != s3 {
		t.Fatalf("CURRENT snapID = %q，期望 %q", gotSnap, s3)
	}
}

// TestSweepSkipsPinned：不在活块集合、但 pin>0（读者持有）或 refcount>0（逻辑引用
// 未释放）的块本次跳过不删；Unpin/Release 后再 Sweep 才删。
func TestSweepSkipsPinned(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{RetainSnapshots: 2}
	const dataset = "pinned"

	live := env.putBlock(t, "pinned-live")
	w := NewWriter(env.store, env.cs, cfg)
	commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", live)})

	// 两个孤儿块：均不被任何快照引用；一个被 Pin（在读者），一个被 Acquire（引用未释放）。
	pinned := env.putBlock(t, "pinned-orphan")
	acquired := env.putBlock(t, "acquired-orphan")
	if err := env.cs.Pin(pinned); err != nil {
		t.Fatalf("Pin(%s): %v", pinned, err)
	}
	if err := env.cs.Acquire(acquired); err != nil {
		t.Fatalf("Acquire(%s): %v", acquired, err)
	}

	col := NewCollector(env.store, env.cs, cfg)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if removedBlocks != 0 || removedObjects != 0 {
		t.Fatalf("Sweep 删除 (%d, %d)，期望 (0, 0)：孤儿块仍被引用或正在被读，应跳过", removedBlocks, removedObjects)
	}
	if !blockExists(t, env.cs, pinned) {
		t.Fatal("pin>0 的孤儿块被删除")
	}
	if !blockExists(t, env.cs, acquired) {
		t.Fatal("refcount>0 的孤儿块被删除")
	}
	if !blockExists(t, env.cs, live) {
		t.Fatal("活块被删除")
	}

	// 释放后（pin=0、refcount=0）再 Sweep：两个孤儿块本次被删，活块保留。
	env.cs.Unpin(pinned)
	env.cs.Release(acquired)
	removedBlocks, removedObjects, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if removedBlocks != 2 || removedObjects != 0 {
		t.Fatalf("second Sweep 删除 (%d, %d)，期望 (2, 0)", removedBlocks, removedObjects)
	}
	if blockExists(t, env.cs, pinned) || blockExists(t, env.cs, acquired) {
		t.Fatal("Unpin/Release 后孤儿块未被删除")
	}
	if !blockExists(t, env.cs, live) {
		t.Fatal("活块被删除")
	}
}

// TestSweepNeverDeletesCurrent：RetainSnapshots=1 时窗口外对象与孤儿块被回收，但
// CURRENT 指向的快照、其 manifest/分片对象与 CurrentKey 对象始终在；无 CURRENT 的
// 数据集 Sweep 返回 ErrNoCurrent 且不删任何东西。
func TestSweepNeverDeletesCurrent(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10}
	const dataset = "current"

	w := NewWriter(env.store, env.cs, cfg)
	v1 := env.putBlock(t, "current-v1")
	s1, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", v1)})
	v2 := env.putBlock(t, "current-v2")
	s2, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", v2)})
	v3 := env.putBlock(t, "current-v3")
	s3, m3 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", v3)})

	col := NewCollector(env.store, env.cs, cfg)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// 保留窗口 = {CURRENT}：s1/s2 各 1 manifest + 1 分片全删；块 v1、v2 成孤儿。
	if removedBlocks != 2 || removedObjects != 4 {
		t.Fatalf("Sweep 删除 (%d, %d)，期望 (2, 4)", removedBlocks, removedObjects)
	}
	if blockExists(t, env.cs, v1) || blockExists(t, env.cs, v2) {
		t.Fatal("窗口外孤儿块未被删除")
	}
	if !blockExists(t, env.cs, v3) {
		t.Fatal("CURRENT 引用的块被删除")
	}

	// CURRENT 指向的快照与其 manifest/分片对象始终在；CurrentKey 对象始终在。
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象被删除")
	}
	if objectExists(t, env.store, ManifestKey(dataset, s1)) || objectExists(t, env.store, ManifestKey(dataset, s2)) {
		t.Fatal("窗口外 manifest 未被删除")
	}
	if !objectExists(t, env.store, ManifestKey(dataset, s3)) {
		t.Fatal("CURRENT 的 manifest 被删除")
	}
	for _, ref := range m3.Shards {
		if !objectExists(t, env.store, ref.Key) {
			t.Fatalf("CURRENT 的分片 %q 被删除", ref.Key)
		}
	}
	gotSnap, _ := currentSnapshot(t, env.store, dataset)
	if gotSnap != s3 {
		t.Fatalf("CURRENT snapID = %q，期望 %q", gotSnap, s3)
	}

	// 再次 Sweep 幂等：无更多可删，CURRENT 仍在其位。
	rb, ro, err := col.Sweep(ctx, dataset)
	if err != nil || rb != 0 || ro != 0 {
		t.Fatalf("第二次 Sweep %v，删除 (%d, %d)，期望 (0, 0)", err, rb, ro)
	}
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象第二次 Sweep 后被删除")
	}

	// 无 CURRENT 的数据集：Sweep 返回 ErrNoCurrent，块与对象一律不删。
	const empty = "no-current"
	orphan := env.putBlock(t, "no-current-orphan")
	garbageKey := snapshotsPrefix(empty) + "garbage/blob"
	if _, err := env.store.Put(ctx, garbageKey, strings.NewReader("junk"), int64(len("junk"))); err != nil {
		t.Fatalf("Put(%q): %v", garbageKey, err)
	}
	col2 := NewCollector(env.store, env.cs, cfg)
	rb, ro, err = col2.Sweep(ctx, empty)
	if !errors.Is(err, ErrNoCurrent) {
		t.Fatalf("无 CURRENT 的 Sweep = %v，期望 ErrNoCurrent", err)
	}
	if rb != 0 || ro != 0 {
		t.Fatalf("无 CURRENT 的 Sweep 删除 (%d, %d)，期望 (0, 0)", rb, ro)
	}
	if !blockExists(t, env.cs, orphan) {
		t.Fatal("无 CURRENT 时块被删除")
	}
	if !objectExists(t, env.store, garbageKey) {
		t.Fatal("无 CURRENT 时对象被删除")
	}
}

// TestSweepRetainSnapshotsOne 覆盖 RetainSnapshots 边界最小值 1——保护窗口只有
// CURRENT 自身，旧快照无一保留。提交 4 次快照，前 3 次全部出窗口：其 manifest/
// 分片对象被删，引用的块（不再被 CURRENT 引用）成孤儿被删；CURRENT 引用的块
// 与其 manifest/分片、CurrentKey 一律保留。
func TestSweepRetainSnapshotsOne(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{RetainSnapshots: 1, MaxEntriesPerShard: 10}
	const dataset = "retain-1"

	w := NewWriter(env.store, env.cs, cfg)

	var (
		v1 = env.putBlock(t, "r1-v1")
		v2 = env.putBlock(t, "r1-v2")
		v3 = env.putBlock(t, "r1-v3")
		v4 = env.putBlock(t, "r1-v4")
	)

	// 4 次 Commit，每次换不同块 → s1/s2/s3 依次出窗口，s4 是 CURRENT。
	s1, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", v1)})
	s2, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", v2)})
	s3, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", v3)})
	s4, m4 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", v4)})

	col := NewCollector(env.store, env.cs, cfg)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// 出窗口快照数 = 3（s1, s2, s3），各 1 manifest + 1 分片 = 6 对象；块 v1/v2/v3 成孤儿 = 3。
	if removedBlocks != 3 || removedObjects != 6 {
		t.Fatalf("Sweep 删除 (%d, %d)，期望 (3, 6)", removedBlocks, removedObjects)
	}

	// 孤儿块 v1/v2/v3 全删；CURRENT 引用的 v4 保留。
	for _, fp := range []cas.Fingerprint{v1, v2, v3} {
		if blockExists(t, env.cs, fp) {
			t.Fatalf("RetainSnapshots=1 窗口外孤儿块 %s 未被删除", fp)
		}
	}
	if !blockExists(t, env.cs, v4) {
		t.Fatal("CURRENT 引用的块被删除")
	}

	// 出窗口 manifest 全删；CURRENT 的 manifest + 分片保留。
	for _, id := range []string{s1, s2, s3} {
		if objectExists(t, env.store, ManifestKey(dataset, id)) {
			t.Fatalf("RetainSnapshots=1 窗口外 manifest %s 未被删除", id)
		}
	}
	if !objectExists(t, env.store, ManifestKey(dataset, s4)) {
		t.Fatal("CURRENT 的 manifest 被删除")
	}
	for _, ref := range m4.Shards {
		if !objectExists(t, env.store, ref.Key) {
			t.Fatalf("CURRENT 的分片 %q 被删除", ref.Key)
		}
	}
	// CurrentKey 对象永不删。
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象被删除")
	}
	gotSnap, _ := currentSnapshot(t, env.store, dataset)
	if gotSnap != s4 {
		t.Fatalf("CURRENT snapID = %q，期望 %q", gotSnap, s4)
	}
}

// TestSweepRetainSnapshotsFive 覆盖 RetainSnapshots 边界较大值 5——保护窗口
// CURRENT + 最近 4 个旧快照，窗口相当宽。提交 3 次快照（全部在窗口内），
// 同时放一个不被任何快照引用的孤儿块 → 对象一个不删（所有快照都还活着），
// 只有孤儿块被删；再加一次提交让最老的快照刚好出窗口，验证窗口边界切换。
func TestSweepRetainSnapshotsFive(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{RetainSnapshots: 5, MaxEntriesPerShard: 10}
	const dataset = "retain-5"

	w := NewWriter(env.store, env.cs, cfg)

	var (
		b1 = env.putBlock(t, "r5-v1")
		b2 = env.putBlock(t, "r5-v2")
		b3 = env.putBlock(t, "r5-v3")
	)

	// 3 次 Commit，全部在 RetainSnapshots=5 窗口内。
	s1, m1 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b1)})
	s2, m2 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b2)})
	s3, m3 := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b3)})

	// 放一个孤儿块：不被任何快照引用 → Sweep 应删。
	orphan := env.putBlock(t, "r5-orphan")

	col := NewCollector(env.store, env.cs, cfg)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// 保护窗口宽 → 3 个快照都活，窗口外无对象；仅孤儿块被删。
	if removedBlocks != 1 || removedObjects != 0 {
		t.Fatalf("Sweep 删除 (%d, %d)，期望 (1, 0)：孤儿块应删，窗口宽不应删任何快照对象",
			removedBlocks, removedObjects)
	}
	if blockExists(t, env.cs, orphan) {
		t.Fatal("孤儿块未被删除")
	}
	for _, fp := range []cas.Fingerprint{b1, b2, b3} {
		if !blockExists(t, env.cs, fp) {
			t.Fatalf("窗口内快照引用的块 %s 被误删", fp)
		}
	}

	// 全部 3 个快照的 manifest + 分片都保留。
	for _, id := range []string{s1, s2, s3} {
		if !objectExists(t, env.store, ManifestKey(dataset, id)) {
			t.Fatalf("RetainSnapshots=5 窗口内 manifest %s 被误删", id)
		}
	}
	for _, ref := range append(append([]ShardRef{}, m1.Shards...), append(m2.Shards, m3.Shards...)...) {
		if !objectExists(t, env.store, ref.Key) {
			t.Fatalf("RetainSnapshots=5 窗口内分片 %q 被误删", ref.Key)
		}
	}
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象被删除")
	}

	// 再加 3 次 Commit（s4, s5, s6）→ 总共 6 个快照，RetainSnapshots=5 →
	// s1 刚好出窗口（s2..s6 保留）。s1 引用的 b1 仍被 s2 复用？不一定，
	// 因为每次 Commit 都换了不同的块。让 s4/s5/s6 继续用新块，保证 s1
	// 完全不被复用，b1 也成孤儿被删。
	b4 := env.putBlock(t, "r5-v4")
	b5 := env.putBlock(t, "r5-v5")
	b6 := env.putBlock(t, "r5-v6")
	_, _ = commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b4)})
	_, _ = commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b5)})
	s6, _ := commitSnapshot(t, w, env.store, dataset, []Entry{mkEntry("/d/a", b6)})

	removedBlocks, removedObjects, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	// s1 出窗口：1 manifest + 1 分片 = 2 对象；b1 不再被保留快照引用 = 1 块。
	if removedBlocks != 1 || removedObjects != 2 {
		t.Fatalf("second Sweep 删除 (%d, %d)，期望 (1, 2)：s1 刚出 RetainSnapshots=5 窗口",
			removedBlocks, removedObjects)
	}
	if blockExists(t, env.cs, b1) {
		t.Fatal("RetainSnapshots=5 窗口刚外的孤儿块 b1 未被删除")
	}
	if objectExists(t, env.store, ManifestKey(dataset, s1)) {
		t.Fatal("RetainSnapshots=5 窗口刚外的 manifest s1 未被删除")
	}
	// s2..s6 全保留。
	for _, id := range []string{s2, s3, s6} {
		if !objectExists(t, env.store, ManifestKey(dataset, id)) {
			t.Fatalf("RetainSnapshots=5 窗口内 manifest %s 被误删", id)
		}
	}
}
