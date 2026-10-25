package index

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"traincache/core/objectstore"
)

// deleteEntry 生成一条已落盘的 Entry：内容随参数唯一，便于逐字段（含指纹）断言。
func deleteEntry(t *testing.T, env *testEnv, path string, chunk, size int64, node, content string) Entry {
	t.Helper()
	e := Entry{Path: path, Chunk: chunk, Size: size, Node: node}
	e.Fingerprint = env.putBlock(t, content)
	return e
}

// snapshotObjectKeys 返回一个快照占用的全部索引对象键（CURRENT + manifest + 各分片）。
func snapshotObjectKeys(dataset, snapID string, m *Manifest) []string {
	keys := []string{CurrentKey(dataset), ManifestKey(dataset, m.SnapshotID)}
	for _, ref := range m.Shards {
		keys = append(keys, ref.Key)
	}
	return keys
}

// deleteTrackingStore 包装 Store：按对象键统计 Put / PutIfMatch 成功次数，记录 Delete
// 调用的键与读操作次数，用于删除语义的可观察断言（旧对象零删除、未变更分片零重传、
// 校验失败零访问）。
type deleteTrackingStore struct {
	objectstore.Store

	mu      sync.Mutex
	puts    map[string]int
	putMaps map[string]int
	deletes []string
	reads   int
}

func newDeleteTrackingStore(base objectstore.Store) *deleteTrackingStore {
	return &deleteTrackingStore{
		Store:   base,
		puts:    make(map[string]int),
		putMaps: make(map[string]int),
	}
}

func (s *deleteTrackingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.Store.Get(ctx, key, w)
}

func (s *deleteTrackingStore) GetETag(ctx context.Context, key string) (string, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.Store.GetETag(ctx, key)
}

func (s *deleteTrackingStore) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	etag, err := s.Store.Put(ctx, key, r, size)
	if err == nil {
		s.mu.Lock()
		s.puts[key]++
		s.mu.Unlock()
	}
	return etag, err
}

func (s *deleteTrackingStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	etag, err := s.Store.PutIfMatch(ctx, key, r, size, expectedETag)
	if err == nil {
		s.mu.Lock()
		s.putMaps[key]++
		s.mu.Unlock()
	}
	return etag, err
}

func (s *deleteTrackingStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deletes = append(s.deletes, key)
	s.mu.Unlock()
	return s.Store.Delete(ctx, key)
}

func (s *deleteTrackingStore) putCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts[key]
}

func (s *deleteTrackingStore) totalPuts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, c := range s.puts {
		n += c
	}
	return n
}

func (s *deleteTrackingStore) putIfMatchCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putMaps[key]
}

func (s *deleteTrackingStore) deleteKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deletes...)
}

func (s *deleteTrackingStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *deleteTrackingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts = make(map[string]int)
	s.putMaps = make(map[string]int)
	s.deletes = nil
	s.reads = 0
}

// TestCommitChangesDeleteRemovesFromNewSnapshotOnly：删除一条 → 新快照不含它、旧快照
// 仍逐字段可读出该条、旧快照对象一个都没被删（design.md §11.4 第 1 点：删除 = 新快照里
// 不存在，不 tombstone、不动已提交对象），且读侧刷新后未命中、其余条目照常解析。
func TestCommitChangesDeleteRemovesFromNewSnapshotOnly(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2} // 4 条 → 2 片
	const dataset = "del-one"

	a0 := deleteEntry(t, env, "/data/a.tar", 0, 10, "n1", "del-one-a0")
	a1 := deleteEntry(t, env, "/data/a.tar", 1, 11, "n2", "del-one-a1")
	b0 := deleteEntry(t, env, "/data/b.tar", 0, 20, "n1", "del-one-b0")
	c0 := deleteEntry(t, env, "/data/c.tar", 0, 30, "n3", "del-one-c0")
	s1, m1 := commitSnapshot(t, NewWriter(env.store, env.cs, cfg), env.store, dataset,
		[]Entry{a0, a1, b0, c0})
	oldKeys := snapshotObjectKeys(dataset, s1, m1)

	store := newDeleteTrackingStore(env.store)
	w := NewWriter(store, env.cs, cfg)

	// 读侧先固定在 S1：被删条目此刻可见。
	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh to S1: %v", err)
	}
	assertHit(t, c, b0)

	s2, err := w.CommitChanges(ctx, dataset, nil, []Delete{{Path: "/data/b.tar", Chunk: 0}})
	if err != nil {
		t.Fatalf("CommitChanges: %v", err)
	}
	if s2 == "" || s2 == s1 {
		t.Fatalf("CommitChanges snapID = %q, want a new snapshot (S1 = %q)", s2, s1)
	}

	// 新快照：b0 消失，其余条目逐字段保留。
	_, m2 := currentSnapshot(t, env.store, dataset)
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m2)), []Entry{a0, a1, c0})

	// 旧快照仍可整份读出（对象未被改写、内容指纹一致）：b0 逐字段照旧。
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m1)), []Entry{a0, a1, b0, c0})
	for _, k := range oldKeys {
		if !objectExists(t, env.store, k) {
			t.Fatalf("old snapshot object %s missing after delete commit", k)
		}
	}

	// 写侧零删除：索引对象（含被删条目的旧分片）与数据块都不得由 Writer 删除。
	if got := store.deleteKeys(); len(got) != 0 {
		t.Fatalf("delete commit issued %d Store.Delete (%v), want 0", len(got), got)
	}

	// 读侧传播：刷新到 S2 后 (path, chunk) 未命中，其余条目仍逐字段命中。
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh to S2: %v", err)
	}
	if got := c.SnapID(); got != s2 {
		t.Fatalf("Catalog snapID = %q, want S2 %q", got, s2)
	}
	assertMiss(t, c, b0.Path, b0.Chunk)
	for _, want := range []Entry{a0, a1, c0} {
		assertHit(t, c, want)
	}
}

// TestCommitChangesDeleteAllChunksOfPath：Chunk == -1 删该 path 的全部块、其余 path 不受
// 影响（D33）；同 path 的重复描述（-1 与指定块并存）合法且坍缩为同一结果。
func TestCommitChangesDeleteAllChunksOfPath(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{}
	const dataset = "del-all"

	a0 := deleteEntry(t, env, "/data/a.tar", 0, 10, "n1", "del-all-a0")
	a1 := deleteEntry(t, env, "/data/a.tar", 1, 11, "n1", "del-all-a1")
	a2 := deleteEntry(t, env, "/data/a.tar", 2, 12, "n2", "del-all-a2")
	b0 := deleteEntry(t, env, "/data/b.tar", 0, 20, "n2", "del-all-b0")
	b1 := deleteEntry(t, env, "/data/b.tar", 1, 21, "n3", "del-all-b1")
	c0 := deleteEntry(t, env, "/data/c.tar", 0, 30, "n1", "del-all-c0")
	_, m1 := commitSnapshot(t, NewWriter(env.store, env.cs, cfg), env.store, dataset,
		[]Entry{a0, a1, a2, b0, b1, c0})
	if refs := entryRefs(verifyManifestShards(t, env.store, m1)); len(refs) != 6 {
		t.Fatalf("seed snapshot entries = [%s], want 6", strings.Join(refs, " "))
	}

	// /data/b.tar 的 -1 与指定块 0、1 重复描述：合法，结果与只写 -1 相同。
	dels := []Delete{
		{Path: "/data/b.tar", Chunk: -1},
		{Path: "/data/b.tar", Chunk: 0},
		{Path: "/data/b.tar", Chunk: -1},
		{Path: "/data/b.tar", Chunk: 1},
	}
	if _, err := NewWriter(env.store, env.cs, cfg).CommitChanges(ctx, dataset, nil, dels); err != nil {
		t.Fatalf("CommitChanges: %v", err)
	}

	want := []Entry{a0, a1, a2, c0} // 已按 (path, chunk) 有序
	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	got, err := c.List(ctx, "")
	if err != nil {
		t.Fatalf(`List(""): %v`, err)
	}
	assertListEqual(t, got, want)
	assertListRefs(t, got, []string{"/data/a.tar#0", "/data/a.tar#1", "/data/a.tar#2", "/data/c.tar#0"})
	assertMiss(t, c, "/data/b.tar", 0)
	assertMiss(t, c, "/data/b.tar", 1)
}

// TestCommitChangesPutWinsOverDelete：同一 (path, chunk) 既删又放 → 新值生效（§11.4 第 3
// 点）；Chunk == -1 与该 path 某一块的 put 并存时，该块由 put 保留、其余块删除。
func TestCommitChangesPutWinsOverDelete(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{}
	const dataset = "del-put"

	a0 := deleteEntry(t, env, "/data/a.tar", 0, 10, "n1", "del-put-a0-v1")
	a1 := deleteEntry(t, env, "/data/a.tar", 1, 11, "n2", "del-put-a1")
	b0 := deleteEntry(t, env, "/data/b.tar", 0, 20, "n3", "del-put-b0")
	_, m1 := commitSnapshot(t, NewWriter(env.store, env.cs, cfg), env.store, dataset,
		[]Entry{a0, a1, b0})

	// 改名/重写形态：同 (path, chunk) 同时出现在 puts 与 dels → puts 赢。
	a0v2 := deleteEntry(t, env, "/data/a.tar", 0, 100, "n9", "del-put-a0-v2")
	if _, err := NewWriter(env.store, env.cs, cfg).CommitChanges(ctx, dataset,
		[]Entry{a0v2}, []Delete{{Path: "/data/a.tar", Chunk: 0}}); err != nil {
		t.Fatalf("CommitChanges(put+delete same chunk): %v", err)
	}
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m1)), []Entry{a0, a1, b0})
	_, m2 := currentSnapshot(t, env.store, dataset)
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m2)), []Entry{a0v2, a1, b0})

	// 整路径删除 + 其中一块 put：该块以新值留下，同路径其余块删除。
	a0v3 := deleteEntry(t, env, "/data/a.tar", 0, 1000, "n8", "del-put-a0-v3")
	if _, err := NewWriter(env.store, env.cs, cfg).CommitChanges(ctx, dataset,
		[]Entry{a0v3}, []Delete{{Path: "/data/a.tar", Chunk: -1}}); err != nil {
		t.Fatalf("CommitChanges(put vs whole-path delete): %v", err)
	}
	_, m3 := currentSnapshot(t, env.store, dataset)
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m3)), []Entry{a0v3, b0})

	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	assertHit(t, c, a0v3)
	assertMiss(t, c, "/data/a.tar", 1)
	assertHit(t, c, b0)
}

// TestCommitChangesDeleteIsIdempotent：dels 指向不存在的条目 = 幂等 no-op（§11.4 第 2 点）
// ——提交成功、条目集与上一快照逐字段相同；同一批 dels 重复提交仍成功。
func TestCommitChangesDeleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 1} // 3 条 → 3 片
	const dataset = "del-idem"

	a0 := deleteEntry(t, env, "/data/a.tar", 0, 10, "n1", "del-idem-a0")
	b0 := deleteEntry(t, env, "/data/b.tar", 0, 20, "n2", "del-idem-b0")
	c0 := deleteEntry(t, env, "/data/c.tar", 0, 30, "n3", "del-idem-c0")
	want := []Entry{a0, b0, c0}
	s1, m1 := commitSnapshot(t, NewWriter(env.store, env.cs, cfg), env.store, dataset, want)

	dels := []Delete{
		{Path: "/data/ghost.tar", Chunk: -1}, // path 不存在
		{Path: "/data/a.tar", Chunk: 7},      // path 存在但该块不存在
	}
	store := newDeleteTrackingStore(env.store)
	w := NewWriter(store, env.cs, cfg)
	s2, err := w.CommitChanges(ctx, dataset, nil, dels)
	if err != nil {
		t.Fatalf("CommitChanges with no-op dels: %v", err)
	}
	if s2 == "" || s2 == s1 {
		t.Fatalf("no-op delete commit snapID = %q, want a fresh snapshot (S1 = %q)", s2, s1)
	}
	_, m2 := currentSnapshot(t, env.store, dataset)
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m2)), want)

	// 同一批 dels 再提交一次仍成功，条目集不变。
	s3, err := w.CommitChanges(ctx, dataset, nil, dels)
	if err != nil {
		t.Fatalf("repeat CommitChanges with no-op dels: %v", err)
	}
	if s3 == "" || s3 == s2 {
		t.Fatalf("repeated no-op delete commit snapID = %q, want a fresh snapshot (S2 = %q)", s3, s2)
	}
	_, m3 := currentSnapshot(t, env.store, dataset)
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m3)), want)

	// 条目集不变但分片物理复用：3 片内容指纹均未变 → 只上传 manifest。
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m1)), want)
	if got := store.totalPuts(); got != 2 {
		t.Fatalf("no-op delete commits put %d objects, want 2 (manifest of S2 + manifest of S3)", got)
	}
	if got := store.deleteKeys(); len(got) != 0 {
		t.Fatalf("no-op delete commit issued %d Store.Delete (%v), want 0", len(got), got)
	}
}

// TestCommitChangesPrefixBatchEqualsPathList：一次 CommitChanges 带 N 条 {Chunk: -1} 的
// 批量删除，与逐条各提交一次 N 个删除，最终条目集完全相同（CLI --prefix 展开成显式
// path 列表的形态；比较条目集而非 snapID）。
func TestCommitChangesPrefixBatchEqualsPathList(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 4}
	const batchDataset, loopDataset = "del-batch", "del-loop"

	// 5 个待删 path × 2 块 + 1 个保留 path × 2 块 = 12 条。
	var toDelete, kept []Entry
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("/data/p%d.tar", i)
		for ch := int64(0); ch < 2; ch++ {
			toDelete = append(toDelete,
				deleteEntry(t, env, path, ch, 100+ch, fmt.Sprintf("n%d", i), fmt.Sprintf("del-batch-%s-%d", path, ch)))
		}
	}
	for ch := int64(0); ch < 2; ch++ {
		kept = append(kept,
			deleteEntry(t, env, "/data/keep.tar", ch, 7+ch, "n0", fmt.Sprintf("del-batch-keep-%d", ch)))
	}
	seed := append(append([]Entry(nil), toDelete...), kept...)
	w := NewWriter(env.store, env.cs, cfg)
	for _, dataset := range []string{batchDataset, loopDataset} {
		if _, err := w.Commit(ctx, dataset, seed); err != nil {
			t.Fatalf("seed Commit %s: %v", dataset, err)
		}
	}

	dels := make([]Delete, 0, 5)
	for i := 0; i < 5; i++ {
		dels = append(dels, Delete{Path: fmt.Sprintf("/data/p%d.tar", i), Chunk: -1})
	}
	// 形态 A：一次批量提交。
	if _, err := w.CommitChanges(ctx, batchDataset, nil, dels); err != nil {
		t.Fatalf("batch CommitChanges: %v", err)
	}
	// 形态 B：逐条各提交一次。
	for _, d := range dels {
		if _, err := w.CommitChanges(ctx, loopDataset, nil, []Delete{d}); err != nil {
			t.Fatalf("loop CommitChanges(%+v): %v", d, err)
		}
	}

	batch, loop := NewCatalog(env.store, batchDataset, cfg), NewCatalog(env.store, loopDataset, cfg)
	for _, c := range []*Catalog{batch, loop} {
		if err := c.Refresh(ctx); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
	}
	gotBatch, err := batch.List(ctx, "")
	if err != nil {
		t.Fatalf(`List("") batch: %v`, err)
	}
	gotLoop, err := loop.List(ctx, "")
	if err != nil {
		t.Fatalf(`List("") loop: %v`, err)
	}
	want := sortEntries(kept)
	assertListEqual(t, gotBatch, want)
	assertListEqual(t, gotLoop, want)
	assertListRefs(t, gotBatch, []string{"/data/keep.tar#0", "/data/keep.tar#1"})
}

// TestCommitChangesValidation：非法删除描述（Path 为空、Chunk < -1）整体拒绝、错误点名该
// 描述，且校验先于任何对象存储访问——零 Put、零 PutIfMatch、零 Delete、零读。
func TestCommitChangesValidation(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{}
	const dataset = "del-valid"

	a0 := deleteEntry(t, env, "/data/a.tar", 0, 10, "n1", "del-valid-a0")
	z0 := deleteEntry(t, env, "/data/z.tar", 0, 20, "n2", "del-valid-z0")
	seeded := []Entry{a0, z0}
	s1, m1 := commitSnapshot(t, NewWriter(env.store, env.cs, cfg), env.store, dataset, seeded)

	store := newDeleteTrackingStore(env.store)
	w := NewWriter(store, env.cs, cfg)

	cases := []struct {
		name      string
		dels      []Delete
		wantInErr string
	}{
		{"空 path + Chunk -1", []Delete{{Path: "", Chunk: -1}}, "empty path"},
		{"空 path + 指定块", []Delete{{Path: "", Chunk: 0}}, "empty path"},
		{"Chunk < -1", []Delete{{Path: "/data/a.tar", Chunk: -2}}, `delete 0: /data/a.tar chunk -2`},
		{"合法描述之后跟着非法描述", []Delete{{Path: "/data/a.tar", Chunk: 0}, {Path: "/data/b.tar", Chunk: -3}},
			`delete 1: /data/b.tar chunk -3`},
	}
	for _, tc := range cases {
		store.reset()
		// puts 刻意携带一个新条目：非法 dels 必须让整次调用作废（puts 也不得写入）。
		newEntry := deleteEntry(t, env, "/data/new.tar", 0, 99, "n9", "del-valid-"+tc.name)
		snapID, err := w.CommitChanges(ctx, dataset, []Entry{newEntry}, tc.dels)
		if err == nil {
			t.Fatalf("%s: CommitChanges 应报错，实得 snapID %q", tc.name, snapID)
		}
		if !strings.Contains(err.Error(), tc.wantInErr) {
			t.Fatalf("%s: 错误 %q 未点名出错的删除描述（应含 %q）", tc.name, err, tc.wantInErr)
		}
		if snapID != "" {
			t.Fatalf("%s: 校验失败仍返回 snapID %q", tc.name, snapID)
		}
		if got := store.totalPuts(); got != 0 {
			t.Fatalf("%s: 校验失败却上传了 %d 个对象 (%v)", tc.name, got, store.puts)
		}
		if got := store.putIfMatchCount(CurrentKey(dataset)); got != 0 {
			t.Fatalf("%s: 校验失败却更新了 CURRENT (%d 次)", tc.name, got)
		}
		if got := store.deleteKeys(); len(got) != 0 {
			t.Fatalf("%s: 校验失败却删除了对象 %v", tc.name, got)
		}
		if got := store.readCount(); got != 0 {
			t.Fatalf("%s: 校验未先于存储访问（读操作 %d 次）", tc.name, got)
		}
		// 快照仍是 S1：条目集与对象均未变。
		gotSnap, m := currentSnapshot(t, env.store, dataset)
		if gotSnap != s1 {
			t.Fatalf("%s: CURRENT snapID = %q, want unchanged %q", tc.name, gotSnap, s1)
		}
		assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m)), seeded)
		assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, m1)), seeded)
	}

	// 合法描述（-1 与指定块混用、重复）不被校验误拒：/data/a.tar 整路径删除、/data/z.tar 保留。
	if _, err := w.CommitChanges(ctx, dataset, nil, []Delete{
		{Path: "/data/a.tar", Chunk: 0}, {Path: "/data/a.tar", Chunk: -1}, {Path: "/data/a.tar", Chunk: 3},
	}); err != nil {
		t.Fatalf("CommitChanges with legal dels: %v", err)
	}
	_, mFinal := currentSnapshot(t, env.store, dataset)
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, mFinal)), []Entry{z0})
}

// TestCommitChangesShardReuseAfterDelete：删除后未变更分片按内容指纹物理复用同一对象键、
// 零重传，与 Commit 的复用形态一致（对照同一数据集上单条 upsert 的 Commit）。
func TestCommitChangesShardReuseAfterDelete(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2} // 6 条 → 3 片

	seed := func(dataset string) {
		t.Helper()
		entries := make([]Entry, 0, 6)
		for i := 0; i < 6; i++ {
			entries = append(entries, deleteEntry(t, env,
				fmt.Sprintf("/data/%c.tar", 'a'+i), 0, int64(i+1), "n1",
				fmt.Sprintf("%s-%c", dataset, 'a'+i)))
		}
		_, m := commitSnapshot(t, NewWriter(env.store, env.cs, cfg), env.store, dataset, entries)
		if len(m.Shards) != 3 {
			t.Fatalf("%s seed shards = %d, want 3", dataset, len(m.Shards))
		}
	}

	// 删除末片的一条（/data/f.tar）：前两片内容未变 → 复用旧键、不重传。
	seed("del-reuse")
	store := newDeleteTrackingStore(env.store)
	w := NewWriter(store, env.cs, cfg)
	_, m1 := currentSnapshot(t, env.store, "del-reuse")
	s2, err := w.CommitChanges(ctx, "del-reuse", nil, []Delete{{Path: "/data/f.tar", Chunk: 0}})
	if err != nil {
		t.Fatalf("CommitChanges: %v", err)
	}
	_, m2 := currentSnapshot(t, env.store, "del-reuse")
	assertReuse(t, store, m1, m2, ManifestKey("del-reuse", s2), CurrentKey("del-reuse"))

	// 对照：Commit 单条 upsert 末片条目 → 复用形态逐键相同（两片复用、一片新传、一 manifest）。
	seed("put-reuse")
	store2 := newDeleteTrackingStore(env.store)
	w2 := NewWriter(store2, env.cs, cfg)
	_, m1b := currentSnapshot(t, env.store, "put-reuse")
	f2 := deleteEntry(t, env, "/data/f.tar", 0, 60, "n9", "put-reuse-f-v2")
	s3, err := w2.Commit(ctx, "put-reuse", []Entry{f2})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, m3 := currentSnapshot(t, env.store, "put-reuse")
	assertReuse(t, store2, m1b, m3, ManifestKey("put-reuse", s3), CurrentKey("put-reuse"))
}

// assertReuse 断言一次提交的分片复用形态：m1 的前两片在 m2 中键与指纹均未变且零重传，
// 末片为新键且恰上传一次；manifest 恰上传一次、CURRENT 恰条件写一次、零删除。
func assertReuse(t *testing.T, store *deleteTrackingStore, m1, m2 *Manifest, manifestKey, currentKey string) {
	t.Helper()
	if len(m1.Shards) != 3 || len(m2.Shards) != 3 {
		t.Fatalf("shards = (%d, %d), want (3, 3)", len(m1.Shards), len(m2.Shards))
	}
	for i := 0; i < 2; i++ {
		if m2.Shards[i].Key != m1.Shards[i].Key || m2.Shards[i].Fingerprint != m1.Shards[i].Fingerprint {
			t.Fatalf("shard %d not reused: %+v vs %+v", i, m1.Shards[i], m2.Shards[i])
		}
		if got := store.putCount(m1.Shards[i].Key); got != 0 {
			t.Fatalf("reused shard %s re-uploaded %d times", m1.Shards[i].Key, got)
		}
	}
	if m2.Shards[2].Key == m1.Shards[2].Key {
		t.Fatalf("changed shard reused old key %q", m2.Shards[2].Key)
	}
	if got := store.putCount(m2.Shards[2].Key); got != 1 {
		t.Fatalf("new shard %s uploaded %d times, want 1", m2.Shards[2].Key, got)
	}
	if got := store.putCount(manifestKey); got != 1 {
		t.Fatalf("manifest %s uploaded %d times, want 1", manifestKey, got)
	}
	if got := store.totalPuts(); got != 2 {
		t.Fatalf("total puts = %d (%v), want 2 (1 new shard + 1 manifest, 无重传)", got, store.puts)
	}
	if got := store.putIfMatchCount(currentKey); got != 1 {
		t.Fatalf("CURRENT conditional writes = %d, want 1", got)
	}
	if got := store.deleteKeys(); len(got) != 0 {
		t.Fatalf("commit issued %d Store.Delete (%v), want 0", len(got), got)
	}
}

// TestCommitChangesConcurrentWithCommit：20 个 goroutine 在同一数据集上并发 CommitChanges
// （删除种子条目）与 Commit（新增各自条目）→ 全部成功、无静默丢失，最终 CURRENT 指向的
// 快照条目集与串行等价结果逐字段一致（删除只针对种子条目、新增条目从不被删，故结果与
// 提交顺序无关）。冲突重试上限沿用 TestConcurrentCommit 的 20（= 最多 19 次冲突 + 首次）。
func TestCommitChangesConcurrentWithCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	env := newTestEnv(t)
	const dataset = "del-concurrent"
	const workers = 20
	cfg := Config{CommitMaxAttempts: workers}

	// 种子快照 S0：keep00..keep19，每个 path 一块；奇数下标的将被删除，偶数下标的必须留下。
	keeps := make([]Entry, workers)
	for i := range keeps {
		keeps[i] = deleteEntry(t, env, fmt.Sprintf("/data/keep%02d.bin", i), 0, int64(i+1),
			fmt.Sprintf("n%d", i), fmt.Sprintf("del-concurrent-keep-%02d", i))
	}
	w := NewWriter(env.store, env.cs, cfg)
	s0, m0 := commitSnapshot(t, w, env.store, dataset, keeps)
	if len(m0.Shards) == 0 {
		t.Fatal("seed snapshot has no shards")
	}

	// 偶数下标的 goroutine 各自新增一条永不被删除的条目。
	adds := make([]Entry, workers)
	for i := 0; i < workers; i += 2 {
		adds[i] = deleteEntry(t, env, fmt.Sprintf("/data/add%02d.bin", i), 0, int64(1000+i),
			fmt.Sprintf("n%d", i), fmt.Sprintf("del-concurrent-add-%02d", i))
	}

	snapIDs := make([]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				snapIDs[i], errs[i] = w.Commit(ctx, dataset, []Entry{adds[i]})
				return
			}
			snapIDs[i], errs[i] = w.CommitChanges(ctx, dataset, nil,
				[]Delete{{Path: keeps[i].Path, Chunk: -1}})
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		if snapIDs[i] == "" {
			t.Fatalf("worker %d returned empty snapID", i)
		}
	}

	// 最终快照 = 偶数 keep + 全部 adds（奇数 keep 全被删除：无 lost write、也无复活）。
	finalSnapID, mFinal := currentSnapshot(t, env.store, dataset)
	if finalSnapID == s0 {
		t.Fatalf("final CURRENT still points at the seed snapshot %q", s0)
	}
	seen := false
	for _, id := range snapIDs {
		if id == finalSnapID {
			seen = true
			break
		}
	}
	if !seen {
		t.Fatalf("final CURRENT snapID %q is not any worker's returned snapID", finalSnapID)
	}
	var want []Entry
	for i := 0; i < workers; i += 2 {
		want = append(want, keeps[i], adds[i])
	}
	want = sortEntries(want)
	assertListEqual(t, sortEntries(verifyManifestShards(t, env.store, mFinal)), want)

	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for i := 0; i < workers; i++ {
		if i%2 == 0 {
			assertHit(t, c, keeps[i])
			assertHit(t, c, adds[i])
			continue
		}
		assertMiss(t, c, keeps[i].Path, keeps[i].Chunk)
	}
}
