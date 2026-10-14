package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"traincache/core/objectstore"
)

// getCountingStore 包装 Store 并记录 Get 的对象键（LoadPrefixes 的下载计数断言，测试私有）。
type getCountingStore struct {
	objectstore.Store
	mu   sync.Mutex
	gets []string
}

func (s *getCountingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	s.gets = append(s.gets, key)
	s.mu.Unlock()
	return s.Store.Get(ctx, key, w)
}

func (s *getCountingStore) getKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.gets...)
}

func (s *getCountingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = nil
}

// countKey 统计 keys 中等于 want 的出现次数。
func countKey(keys []string, want string) int {
	n := 0
	for _, k := range keys {
		if k == want {
			n++
		}
	}
	return n
}

// assertHit 断言 Lookup 命中且条目逐字段相等。
func assertHit(t *testing.T, c *Catalog, want Entry) {
	t.Helper()
	got, ok, err := c.Lookup(context.Background(), want.Path, want.Chunk)
	if err != nil || !ok || got != want {
		t.Fatalf("Lookup(%q, %d) = (%+v, %v, %v), want (%+v, true, nil)", want.Path, want.Chunk, got, ok, err, want)
	}
}

// assertMiss 断言 Lookup 未命中：映射存在但无该条目 → (Entry{}, false, nil)。
func assertMiss(t *testing.T, c *Catalog, path string, chunk int64) {
	t.Helper()
	got, ok, err := c.Lookup(context.Background(), path, chunk)
	if err != nil || ok || got != (Entry{}) {
		t.Fatalf("Lookup(%q, %d) = (%+v, %v, %v), want (Entry{}, false, nil)", path, chunk, got, ok, err)
	}
}

// TestLoadPrefixes：三前缀数据集（/model、/train-data、/val-data，每前缀恰好一片）→
// LoadPrefixes(["/train-data"]) 只下载并加载相关分片：命中相关条目、未命中无关前缀条目
// （miss），且无关分片对象零下载（mock 计数）。
func TestLoadPrefixes(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2} // 6 条 → 3 片，与三个前缀一一对应
	const dataset = "prefix"

	// 按 (path,chunk) 排序后：shard0=/model/*、shard1=/train-data/*、shard2=/val-data/*。
	entries := []Entry{
		{Path: "/model/m.bin", Chunk: 0, Size: 10, Node: "n1"},
		{Path: "/model/n.bin", Chunk: 0, Size: 11, Node: "n1"},
		{Path: "/train-data/a.bin", Chunk: 0, Size: 20, Node: "n2"},
		{Path: "/train-data/b.bin", Chunk: 0, Size: 21, Node: "n2"},
		{Path: "/val-data/v.bin", Chunk: 0, Size: 30, Node: "n3"},
		{Path: "/val-data/w.bin", Chunk: 0, Size: 31, Node: "n3"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("prefix-block-%d", i))
	}

	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 分片边界与预期一致；manifest 序列化含 min_path/max_path 字段名（D22 冻结 JSON 名）。
	_, m := currentSnapshot(t, env.store, dataset)
	if len(m.Shards) != 3 {
		t.Fatalf("manifest shards = %d, want 3", len(m.Shards))
	}
	wantRanges := [][2]string{
		{"/model/m.bin", "/model/n.bin"},
		{"/train-data/a.bin", "/train-data/b.bin"},
		{"/val-data/v.bin", "/val-data/w.bin"},
	}
	for i, ref := range m.Shards {
		if ref.MinPath != wantRanges[i][0] || ref.MaxPath != wantRanges[i][1] {
			t.Fatalf("shard %d range = [%q, %q], want %q", i, ref.MinPath, ref.MaxPath, wantRanges[i])
		}
	}
	manifestRaw, err := getObject(ctx, env.store, ManifestKey(dataset, m.SnapshotID))
	if err != nil {
		t.Fatalf("read manifest raw: %v", err)
	}
	for _, field := range []string{`"min_path"`, `"max_path"`} {
		if !bytes.Contains(manifestRaw, []byte(field)) {
			t.Fatalf("manifest JSON misses field %s: %s", field, manifestRaw)
		}
	}

	counted := &getCountingStore{Store: env.store}
	c := NewCatalog(counted, dataset, cfg)
	if err := c.LoadPrefixes(ctx, []string{"/train-data"}); err != nil {
		t.Fatalf("LoadPrefixes: %v", err)
	}

	// 命中：/train-data 条目逐字段相等；未命中：/model 与 /val-data 条目 miss。
	for _, want := range entries[2:4] {
		assertHit(t, c, want)
	}
	for _, e := range append(append([]Entry{}, entries[:2]...), entries[4:]...) {
		assertMiss(t, c, e.Path, e.Chunk)
	}

	// 零下载断言：LoadPrefixes 期间的 Get 只发生在 CURRENT、manifest 与相关分片（shard1）上。
	gets := counted.getKeys()
	if len(gets) != 3 {
		t.Fatalf("LoadPrefixes Get count = %d (%v), want 3 (CURRENT+manifest+relevant shard)", len(gets), gets)
	}
	if !containsKey(gets, CurrentKey(dataset)) || !containsKey(gets, ManifestKey(dataset, m.SnapshotID)) {
		t.Fatalf("LoadPrefixes did not read CURRENT/manifest: %v", gets)
	}
	if n := countKey(gets, m.Shards[1].Key); n != 1 {
		t.Fatalf("relevant shard Get count = %d (%v), want 1", n, gets)
	}
	for _, i := range []int{0, 2} {
		if n := countKey(gets, m.Shards[i].Key); n != 0 {
			t.Fatalf("unrelated shard %s downloaded %d times: %v", m.Shards[i].Key, n, gets)
		}
	}
}

// TestLoadPrefixesRangeOverlap：单分片横跨两个前缀（/train-data 与 /val-data）→ 任一前缀
// 命中即加载该分片（分片为加载单元，片内两前缀条目均可见）；两个前缀同时给出时该分片
// 去重、只下载一次（mock 计数）。
func TestLoadPrefixesRangeOverlap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 3} // 3 条 → 1 片，边界横跨两个前缀
	const dataset = "overlap"

	entries := []Entry{
		{Path: "/train-data/a.bin", Chunk: 0, Size: 1, Node: "n1"},
		{Path: "/train-data/z.bin", Chunk: 0, Size: 2, Node: "n1"},
		{Path: "/val-data/a.bin", Chunk: 0, Size: 3, Node: "n2"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("overlap-block-%d", i))
	}
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	_, m := currentSnapshot(t, env.store, dataset)
	if len(m.Shards) != 1 {
		t.Fatalf("manifest shards = %d, want 1", len(m.Shards))
	}
	if m.Shards[0].MinPath != "/train-data/a.bin" || m.Shards[0].MaxPath != "/val-data/a.bin" {
		t.Fatalf("shard range = [%q, %q], want [/train-data/a.bin, /val-data/a.bin]",
			m.Shards[0].MinPath, m.Shards[0].MaxPath)
	}

	counted := &getCountingStore{Store: env.store}
	c := NewCatalog(counted, dataset, cfg)

	// 任一前缀（此处仅 /val-data）命中该分片 → 分片被加载；片内两个前缀的条目均可见。
	if err := c.LoadPrefixes(ctx, []string{"/val-data"}); err != nil {
		t.Fatalf("LoadPrefixes([/val-data]): %v", err)
	}
	for _, want := range entries {
		assertHit(t, c, want)
	}
	if n := countKey(counted.getKeys(), m.Shards[0].Key); n != 1 {
		t.Fatalf("shard Get count = %d (%v), want 1", n, counted.getKeys())
	}

	// 两个前缀同时给出：同一分片只下载一次（并集去重）。
	counted.reset()
	if err := c.LoadPrefixes(ctx, []string{"/train-data", "/val-data"}); err != nil {
		t.Fatalf("LoadPrefixes([/train-data, /val-data]): %v", err)
	}
	gets := counted.getKeys()
	if len(gets) != 3 {
		t.Fatalf("LoadPrefixes Get count = %d (%v), want 3 (CURRENT+manifest+one shard)", len(gets), gets)
	}
	if n := countKey(gets, m.Shards[0].Key); n != 1 {
		t.Fatalf("overlapping shard downloaded %d times (%v), want exactly 1", n, gets)
	}
	for _, want := range entries {
		assertHit(t, c, want)
	}
}

// TestLoadPrefixesEmpty：空 prefixes 列表 → 映射为空（全部 miss、非 ErrNoCurrent）、
// 零分片下载、无错误；含空串元素 → 匹配一切（全量命中，等价 Refresh 全量）。
// 另覆盖无 CURRENT 时 → ErrNoCurrent（与 Refresh 一致）。
func TestLoadPrefixesEmpty(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2}
	const dataset = "emptyprefix"

	// 尚未提交任何快照：读 CURRENT 失败 → ErrNoCurrent。
	counted := &getCountingStore{Store: env.store}
	c := NewCatalog(counted, dataset, cfg)
	if err := c.LoadPrefixes(ctx, []string{"/train-data"}); !errors.Is(err, ErrNoCurrent) {
		t.Fatalf("LoadPrefixes without CURRENT = %v, want ErrNoCurrent", err)
	}

	entries := []Entry{
		{Path: "/train-data/a.bin", Chunk: 0, Size: 1, Node: "n1"},
		{Path: "/train-data/b.bin", Chunk: 0, Size: 2, Node: "n1"},
		{Path: "/val-data/a.bin", Chunk: 0, Size: 3, Node: "n2"},
		{Path: "/val-data/b.bin", Chunk: 0, Size: 4, Node: "n2"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("empty-block-%d", i))
	}
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, m := currentSnapshot(t, env.store, dataset)

	// 空列表：原子切换成功、映射为空（miss 而非 ErrNoCurrent）、不下载任何分片。
	counted.reset()
	if err := c.LoadPrefixes(ctx, []string{}); err != nil {
		t.Fatalf("LoadPrefixes([]): %v", err)
	}
	for _, e := range entries {
		assertMiss(t, c, e.Path, e.Chunk)
	}
	gets := counted.getKeys()
	if len(gets) != 2 || countKey(gets, CurrentKey(dataset)) != 1 || countKey(gets, ManifestKey(dataset, m.SnapshotID)) != 1 {
		t.Fatalf("LoadPrefixes([]) Gets = %v, want exactly CURRENT+manifest", gets)
	}
	for _, ref := range m.Shards {
		if n := countKey(gets, ref.Key); n != 0 {
			t.Fatalf("LoadPrefixes([]) downloaded shard %s %d times", ref.Key, n)
		}
	}

	// 含空串元素：匹配一切 → 全量条目命中。
	if err := c.LoadPrefixes(ctx, []string{""}); err != nil {
		t.Fatalf("LoadPrefixes([\"\"]): %v", err)
	}
	for _, want := range entries {
		assertHit(t, c, want)
	}
}

// TestLoadPrefixesErrorKeepsOldMapping：先 Refresh 建立全量旧映射；再对相关分片注入
// 1 字节损坏 → LoadPrefixes 返回 ErrInvalidShard，旧映射（全部 4 条，含仅存在于后一
// 分片的条目）仍可用——证明失败路径未做任何部分切换。
func TestLoadPrefixesErrorKeepsOldMapping(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2}
	const dataset = "keepold"

	entries := []Entry{
		{Path: "/d/a", Chunk: 0, Size: 1, Node: "n1"},
		{Path: "/d/b", Chunk: 0, Size: 2, Node: "n1"},
		{Path: "/d/c", Chunk: 0, Size: 3, Node: "n2"},
		{Path: "/d/d", Chunk: 0, Size: 4, Node: "n2"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("keepold-block-%d", i))
	}
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// 旧映射：Refresh 全量加载，全部 4 条命中。
	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, want := range entries {
		assertHit(t, c, want)
	}

	// 对第二片（/d/c、/d/d）注入 1 字节损坏；prefix "/d" 对两片均相关。
	_, m := currentSnapshot(t, env.store, dataset)
	if len(m.Shards) != 2 || m.Shards[1].MinPath != "/d/c" {
		t.Fatalf("manifest shards = %+v, want 2 with second starting at /d/c", m.Shards)
	}
	raw, err := getObject(ctx, env.store, m.Shards[1].Key)
	if err != nil {
		t.Fatalf("read shard %s: %v", m.Shards[1].Key, err)
	}
	raw[0] ^= 0xFF
	if _, err := env.store.Put(ctx, m.Shards[1].Key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("tamper shard: %v", err)
	}

	if err := c.LoadPrefixes(ctx, []string{"/d"}); !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("LoadPrefixes after tamper = %v, want ErrInvalidShard", err)
	}

	// 旧映射仍可用：4 条全命中（后两片条目仍在，排除部分切换）。
	for _, want := range entries {
		assertHit(t, c, want)
	}
}
