package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// testEnv 是单测装置：mock objectstore + 临时目录 cas.Store（decisions.md D12）。
type testEnv struct {
	store *objectstore.Mock
	cs    *cas.Store
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	return &testEnv{store: objectstore.NewMock(), cs: cs}
}

// putBlock 把内容写入 CAS，返回指纹。Commit 的 ④ 验证要求新块已先行落盘。
func (env *testEnv) putBlock(t *testing.T, content string) cas.Fingerprint {
	t.Helper()
	fp, _, err := env.cs.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("cas.Put(%q): %v", content, err)
	}
	return fp
}

// newCatalogFor 构造绑定到指定数据集的 Catalog（D17：数据集经构造参数绑定）。
func newCatalogFor(t *testing.T, store objectstore.Store, cfg Config, dataset string) *Catalog {
	t.Helper()
	return NewCatalog(store, dataset, cfg)
}

// currentSnapshot 读取 CURRENT 并解析出当前快照 ID 与 manifest，同时校验
// CURRENT 的 manifestFP8 与 manifest 内容 xxh3 前 8 字节一致。
func currentSnapshot(t *testing.T, store objectstore.Store, dataset string) (string, *Manifest) {
	t.Helper()
	ctx := context.Background()
	raw, err := getObject(ctx, store, CurrentKey(dataset))
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	snapID, manifestFP8, err := parseCurrent(string(raw))
	if err != nil {
		t.Fatalf("parse CURRENT %q: %v", raw, err)
	}
	mb, err := getObject(ctx, store, ManifestKey(dataset, snapID))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if got := fp8(mb); got != manifestFP8 {
		t.Fatalf("manifest fp8 %s != CURRENT %s", got, manifestFP8)
	}
	var m Manifest
	if err := json.Unmarshal(mb, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return snapID, &m
}

// TestCommitAndLookup：跨 3 分片的数据集 → Commit → Refresh → 全部 Lookup 命中、字段逐项相等。
func TestCommitAndLookup(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2} // 5 条 → 3 个分片
	const dataset = "demo"

	entries := []Entry{
		{Path: "/data/a.tar", Chunk: 0, Size: 10, Node: "n1"},
		{Path: "/data/a.tar", Chunk: 1, Size: 20, Node: "n2"},
		{Path: "/data/b.tar", Chunk: 0, Size: 30, Node: "n1"},
		{Path: "/data/c.tar", Chunk: 0, Size: 40, Node: "n3"},
		{Path: "/data/c.tar", Chunk: 1, Size: 50, Node: "n1"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("block-%d", i))
	}

	w := NewWriter(env.store, env.cs, cfg)
	snapID, err := w.Commit(ctx, dataset, entries)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if snapID == "" {
		t.Fatal("Commit returned empty snapID")
	}

	// 5 条 / 每片 2 条 = 3 个分片；分片对象键与 manifest 指纹字段一致。
	gotSnapID, m := currentSnapshot(t, env.store, dataset)
	if gotSnapID != snapID {
		t.Fatalf("CURRENT snapID = %q, want %q", gotSnapID, snapID)
	}
	if len(m.Shards) != 3 {
		t.Fatalf("manifest shards = %d, want 3", len(m.Shards))
	}
	var total int64
	for _, ref := range m.Shards {
		total += ref.Entries
		if !strings.HasSuffix(ref.Key, "shard-"+ref.Fingerprint[:16]+".idx") {
			t.Fatalf("shard key %q does not match fingerprint %q", ref.Key, ref.Fingerprint)
		}
	}
	if total != int64(len(entries)) {
		t.Fatalf("manifest entries total = %d, want %d", total, len(entries))
	}

	c := newCatalogFor(t, env.store, cfg, dataset)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, want := range entries {
		got, ok, err := c.Lookup(ctx, want.Path, want.Chunk)
		if err != nil {
			t.Fatalf("Lookup(%q, %d): %v", want.Path, want.Chunk, err)
		}
		if !ok {
			t.Fatalf("Lookup(%q, %d): miss", want.Path, want.Chunk)
		}
		if got != want {
			t.Fatalf("Lookup(%q, %d) = %+v, want %+v", want.Path, want.Chunk, got, want)
		}
	}
}

// TestLookupMiss：未提交的 (path, chunk) → (Entry{}, false, nil)；从未刷新的 Catalog → ErrNoCurrent。
func TestLookupMiss(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{}
	const dataset = "miss"

	// 从未成功 Refresh：尚无任何快照 → ErrNoCurrent。
	c := newCatalogFor(t, env.store, cfg, dataset)
	if _, _, err := c.Lookup(ctx, "/data/a.tar", 0); !errors.Is(err, ErrNoCurrent) {
		t.Fatalf("Lookup before first refresh = %v, want ErrNoCurrent", err)
	}

	e := Entry{Path: "/data/a.tar", Chunk: 0, Size: 5, Node: "n1"}
	e.Fingerprint = env.putBlock(t, "miss-block")
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{e}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got, ok, err := c.Lookup(ctx, e.Path, e.Chunk); err != nil || !ok || got != e {
		t.Fatalf("Lookup committed = (%+v, %v, %v), want (%+v, true, nil)", got, ok, err, e)
	}

	// 未提交的 (path, chunk)：未命中 → (Entry{}, false, nil)。
	got, ok, err := c.Lookup(ctx, "/data/never.tar", 0)
	if err != nil || ok || got != (Entry{}) {
		t.Fatalf("Lookup miss = (%+v, %v, %v), want (Entry{}, false, nil)", got, ok, err)
	}
	// 同路径未提交的 chunk：同样未命中。
	if got, ok, err := c.Lookup(ctx, e.Path, 7); err != nil || ok || got != (Entry{}) {
		t.Fatalf("Lookup chunk miss = (%+v, %v, %v), want (Entry{}, false, nil)", got, ok, err)
	}
}

// TestShardFingerprintMismatch：篡改一个分片对象 → Refresh 报 ErrInvalidShard 且旧映射仍可用。
func TestShardFingerprintMismatch(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2}
	const dataset = "tamper"

	e1 := Entry{Path: "/d/a", Chunk: 0, Size: 11, Node: "n1"}
	e1.Fingerprint = env.putBlock(t, "payload-1")
	e2 := Entry{Path: "/d/b", Chunk: 0, Size: 22, Node: "n2"}
	e2.Fingerprint = env.putBlock(t, "payload-2")

	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{e1, e2}); err != nil {
		t.Fatalf("first Commit: %v", err)
	}

	c := newCatalogFor(t, env.store, cfg, dataset)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}

	// 第二次提交：upsert 覆盖 e1 并新增 e3 → CURRENT 变化、产生新分片对象。
	e1b := e1
	e1b.Size = 111
	e1b.Node = "n9"
	e1b.Fingerprint = env.putBlock(t, "payload-1-v2")
	e3 := Entry{Path: "/d/c", Chunk: 0, Size: 33, Node: "n3"}
	e3.Fingerprint = env.putBlock(t, "payload-3")
	s2, err := w.Commit(ctx, dataset, []Entry{e1b, e3})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	if s2 == "" {
		t.Fatal("second Commit returned empty snapID")
	}

	// 篡改 s2 manifest 引用的一个分片对象：内容翻 1 字节后经 mock 直接 Put 覆盖。
	_, m2 := currentSnapshot(t, env.store, dataset)
	target := m2.Shards[len(m2.Shards)-1]
	raw, err := getObject(ctx, env.store, target.Key)
	if err != nil {
		t.Fatalf("read shard %s: %v", target.Key, err)
	}
	raw[0] ^= 0xFF
	if _, err := env.store.Put(ctx, target.Key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("tamper shard: %v", err)
	}

	// CURRENT 已变（指向 s2）、分片指纹不符 → ErrInvalidShard。
	if err := c.Refresh(ctx); !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("Refresh after tamper = %v, want ErrInvalidShard", err)
	}

	// 旧映射仍可用：e1 仍是旧快照的值（e1b 的新值不可见），新条目 e3 也不可见。
	if got, ok, err := c.Lookup(ctx, e1.Path, e1.Chunk); err != nil || !ok || got != e1 {
		t.Fatalf("Lookup after failed refresh = (%+v, %v, %v), want old (%+v, true, nil)", got, ok, err, e1)
	}
	if got, ok, _ := c.Lookup(ctx, e3.Path, e3.Chunk); ok {
		t.Fatalf("entry from failed refresh visible: %+v", got)
	}
}

// TestFirstCommit：空对象存储（无 CURRENT）→ Commit 成功，CURRENT 内容格式正确。
func TestFirstCommit(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{} // 全零值 → DefaultConfig 默认值
	const dataset = "first"

	// 空对象存储：无 CURRENT。
	if _, err := env.store.GetETag(ctx, CurrentKey(dataset)); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("GetETag(CURRENT) on empty store = %v, want ErrNotFound", err)
	}

	e := Entry{Path: "/p.tar", Chunk: 0, Size: 7, Node: "n0"}
	e.Fingerprint = env.putBlock(t, "first-payload")
	w := NewWriter(env.store, env.cs, cfg)
	snapID, err := w.Commit(ctx, dataset, []Entry{e})
	if err != nil {
		t.Fatalf("Commit on empty store: %v", err)
	}
	if snapID == "" || !validSnapID(snapID) {
		t.Fatalf("snapID %q is empty or contains non-key-safe characters", snapID)
	}

	raw, err := getObject(ctx, env.store, CurrentKey(dataset))
	if err != nil {
		t.Fatalf("read CURRENT: %v", err)
	}
	// 格式："<snapID> <fp8>"，两段空格分隔、无换行/外围空白。
	parts := strings.Split(string(raw), " ")
	if len(parts) != 2 {
		t.Fatalf("CURRENT = %q, want exactly 2 space-separated fields", raw)
	}
	if parts[0] != snapID {
		t.Fatalf("CURRENT snapID = %q, want %q", parts[0], snapID)
	}
	if !validFP8(parts[1]) {
		t.Fatalf("CURRENT fp8 = %q, want 16 lowercase hex chars", parts[1])
	}
	if strings.TrimSpace(string(raw)) != string(raw) {
		t.Fatalf("CURRENT has surrounding whitespace/newline: %q", raw)
	}
	if parsedSnap, parsedFP8, err := parseCurrent(string(raw)); err != nil || parsedSnap != snapID || parsedFP8 != parts[1] {
		t.Fatalf("parseCurrent(%q) = (%q, %q, %v)", raw, parsedSnap, parsedFP8, err)
	}

	// manifest 内容 xxh3 前 8 字节 hex == CURRENT 第二段；snapshot_id 一致。
	mb, err := getObject(ctx, env.store, ManifestKey(dataset, snapID))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if got := fp8(mb); got != parts[1] {
		t.Fatalf("manifest fp8 = %s, CURRENT says %s", got, parts[1])
	}
	var m Manifest
	if err := json.Unmarshal(mb, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if m.SnapshotID != snapID {
		t.Fatalf("manifest snapshot_id = %q, want %q", m.SnapshotID, snapID)
	}
	if len(m.Shards) != 1 {
		t.Fatalf("manifest shards = %d, want 1", len(m.Shards))
	}

	// 分片对象在 manifest 声明的键上且内容指纹一致。
	ref := m.Shards[0]
	shardBytes, err := getObject(ctx, env.store, ref.Key)
	if err != nil {
		t.Fatalf("read shard %s: %v", ref.Key, err)
	}
	if got := contentFingerprint(shardBytes); got != ref.Fingerprint {
		t.Fatalf("shard content fp = %s, manifest says %s", got, ref.Fingerprint)
	}
	es, err := unmarshalEntries(shardBytes)
	if err != nil {
		t.Fatalf("decode shard: %v", err)
	}
	if int64(len(es)) != ref.Entries || es[0] != e {
		t.Fatalf("shard entries = %+v, want [%+v]", es, e)
	}
}

// countingStore 包装 Store 并记录 Put 的对象键（无重传断言，测试私有）。
type countingStore struct {
	objectstore.Store
	mu   sync.Mutex
	puts []string
}

func (s *countingStore) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	s.mu.Lock()
	s.puts = append(s.puts, key)
	s.mu.Unlock()
	return s.Store.Put(ctx, key, r, size)
}

func (s *countingStore) putKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.puts...)
}

func (s *countingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts = nil
}

// TestUpsertAndReuse：第二次 Commit 只改 1 条（upsert 覆盖）→ 未变更分片对象键不变
// （复用、无重传）、变更分片为新键。
func TestUpsertAndReuse(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2}
	const dataset = "reuse"

	e1 := Entry{Path: "/d/a", Chunk: 0, Size: 1, Node: "n1"}
	e1.Fingerprint = env.putBlock(t, "reuse-a")
	e2 := Entry{Path: "/d/b", Chunk: 0, Size: 2, Node: "n1"}
	e2.Fingerprint = env.putBlock(t, "reuse-b")
	e3 := Entry{Path: "/d/c", Chunk: 0, Size: 3, Node: "n2"}
	e3.Fingerprint = env.putBlock(t, "reuse-c")
	e4 := Entry{Path: "/d/d", Chunk: 0, Size: 4, Node: "n2"}
	e4.Fingerprint = env.putBlock(t, "reuse-d")

	store := &countingStore{Store: env.store}
	w := NewWriter(store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{e1, e2, e3, e4}); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	_, m1 := currentSnapshot(t, env.store, dataset)
	if len(m1.Shards) != 2 {
		t.Fatalf("first manifest shards = %d, want 2", len(m1.Shards))
	}

	// 第二次 Commit 只改 1 条（upsert 覆盖 e2）→ 第 2 片（e3,e4）内容不变。
	e2b := e2
	e2b.Size = 222
	e2b.Node = "n9"
	e2b.Fingerprint = env.putBlock(t, "reuse-b-v2")
	store.reset()
	s2, err := w.Commit(ctx, dataset, []Entry{e2b})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	_, m2 := currentSnapshot(t, env.store, dataset)
	if len(m2.Shards) != 2 {
		t.Fatalf("second manifest shards = %d, want 2", len(m2.Shards))
	}

	// 未变更分片：对象键与指纹均不变（复用）。
	if m2.Shards[1].Key != m1.Shards[1].Key || m2.Shards[1].Fingerprint != m1.Shards[1].Fingerprint {
		t.Fatalf("unchanged shard not reused: first %+v, second %+v", m1.Shards[1], m2.Shards[1])
	}
	// 变更分片：新键。
	if m2.Shards[0].Key == m1.Shards[0].Key {
		t.Fatalf("changed shard reused old key %q", m2.Shards[0].Key)
	}

	// 无重传：第二次 Commit 的 Put 不含复用分片键，含新分片键与新 manifest 键。
	puts := store.putKeys()
	for _, k := range puts {
		if k == m2.Shards[1].Key {
			t.Fatalf("reused shard was re-uploaded: puts = %v", puts)
		}
	}
	if !containsKey(puts, m2.Shards[0].Key) {
		t.Fatalf("changed shard not uploaded: puts = %v", puts)
	}
	if !containsKey(puts, ManifestKey(dataset, s2)) {
		t.Fatalf("manifest not uploaded: puts = %v", puts)
	}

	// 复用对象经真实读路径仍可用：Refresh 后 upsert 生效、其余条目保留。
	c := newCatalogFor(t, env.store, cfg, dataset)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, want := range []Entry{e1, e2b, e3, e4} {
		got, ok, err := c.Lookup(ctx, want.Path, want.Chunk)
		if err != nil || !ok || got != want {
			t.Fatalf("Lookup(%q, %d) = (%+v, %v, %v), want %+v", want.Path, want.Chunk, got, ok, err, want)
		}
	}
}

// containsKey 判断 keys 中是否含 want。
func containsKey(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}
