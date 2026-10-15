package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// sortEntries 返回按 (path, chunk) 字典序排序的副本（与 List 输出定序约定一致）。
func sortEntries(entries []Entry) []Entry {
	out := append([]Entry(nil), entries...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Chunk < out[j].Chunk
	})
	return out
}

// entryRefs 返回 entries 的 "path#chunk" 序列（顺序断言的可读形式）。
func entryRefs(entries []Entry) []string {
	refs := make([]string, len(entries))
	for i, e := range entries {
		refs[i] = fmt.Sprintf("%s#%d", e.Path, e.Chunk)
	}
	return refs
}

// assertListEqual 断言 List 结果与 want（须已按 (path, chunk) 排序）逐元素逐字段相等。
func assertListEqual(t *testing.T, got, want []Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("List = %d 条目 [%s], want %d 条目 [%s]",
			len(got), strings.Join(entryRefs(got), " "), len(want), strings.Join(entryRefs(want), " "))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List[%d] = %+v, want %+v (全部结果 [%s])",
				i, got[i], want[i], strings.Join(entryRefs(got), " "))
		}
	}
}

// assertListRefs 断言 List 结果的 (path#chunk) 顺序恰为 wantRefs。
func assertListRefs(t *testing.T, got []Entry, wantRefs []string) {
	t.Helper()
	g, w := strings.Join(entryRefs(got), " "), strings.Join(wantRefs, " ")
	if g != w {
		t.Fatalf("List 顺序 = [%s], want [%s]", g, w)
	}
}

// TestListFull：prefix 为空串 = 全量枚举——跨 3 分片（5 条，含同路径两个 chunk；Commit
// 输入刻意乱序）→ List 返回全部条目、按 (path, chunk) 字典序排序、逐字段相等，且
// CURRENT/manifest/全部分片各下载恰一次（mock 计数）。
func TestListFull(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2} // 5 条 → 3 片
	const dataset = "listfull"

	eA0 := Entry{Path: "/data/a.tar", Chunk: 0, Size: 10, Node: "n1"}
	eA1 := Entry{Path: "/data/a.tar", Chunk: 1, Size: 11, Node: "n2"}
	eB0 := Entry{Path: "/data/b.tar", Chunk: 0, Size: 20, Node: "n1"}
	eC0 := Entry{Path: "/data/c.tar", Chunk: 0, Size: 30, Node: "n3"}
	eC1 := Entry{Path: "/data/c.tar", Chunk: 1, Size: 31, Node: "n2"}
	tagged := []struct {
		e       *Entry
		content string
	}{{&eA0, "listfull-a0"}, {&eA1, "listfull-a1"}, {&eB0, "listfull-b0"}, {&eC0, "listfull-c0"}, {&eC1, "listfull-c1"}}
	for _, tg := range tagged {
		tg.e.Fingerprint = env.putBlock(t, tg.content)
	}
	entries := []Entry{eC1, eA0, eB0, eA1, eC0} // Commit 输入乱序；List 输出须排序

	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, m := currentSnapshot(t, env.store, dataset)
	if len(m.Shards) != 3 {
		t.Fatalf("manifest shards = %d, want 3", len(m.Shards))
	}

	counted := &getCountingStore{Store: env.store}
	c := NewCatalog(counted, dataset, cfg)
	got, err := c.List(ctx, "")
	if err != nil {
		t.Fatalf("List(\"\"): %v", err)
	}
	assertListEqual(t, got, sortEntries(entries))
	assertListRefs(t, got, []string{
		"/data/a.tar#0", "/data/a.tar#1", "/data/b.tar#0", "/data/c.tar#0", "/data/c.tar#1",
	})

	// 全量枚举下载全部相关分片：Get 恰为 CURRENT + manifest + 3 个分片各一次。
	gets := counted.getKeys()
	if len(gets) != 5 {
		t.Fatalf("List(\"\") Get count = %d (%v), want 5 (CURRENT+manifest+3 shards)", len(gets), gets)
	}
	if countKey(gets, CurrentKey(dataset)) != 1 || countKey(gets, ManifestKey(dataset, m.SnapshotID)) != 1 {
		t.Fatalf("List(\"\") 未恰读一次 CURRENT/manifest: %v", gets)
	}
	for _, ref := range m.Shards {
		if n := countKey(gets, ref.Key); n != 1 {
			t.Fatalf("分片 %s Get 次数 = %d (%v), want 1", ref.Key, n, gets)
		}
	}
}

// TestListPrefixFilter：单分片横跨 /train-data 与 /val-data 两个前缀（D22 相关分片为加载
// 单元）→ List 的 prefix 过滤作用于条目而非分片：相关分片内不匹配的条目被剔除；部分前缀
// 同样生效；相关分片但条目级零匹配 → 空结果、无错误。
func TestListPrefixFilter(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 3} // 3 条 → 1 片，边界横跨两个前缀
	const dataset = "listfilter"

	eTrainA := Entry{Path: "/train-data/a.bin", Chunk: 0, Size: 1, Node: "n1"}
	eTrainZ := Entry{Path: "/train-data/z.bin", Chunk: 0, Size: 2, Node: "n1"}
	eValA := Entry{Path: "/val-data/a.bin", Chunk: 0, Size: 3, Node: "n2"}
	tagged := []struct {
		e       *Entry
		content string
	}{{&eTrainA, "listfilter-train-a"}, {&eTrainZ, "listfilter-train-z"}, {&eValA, "listfilter-val-a"}}
	for _, tg := range tagged {
		tg.e.Fingerprint = env.putBlock(t, tg.content)
	}

	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{eTrainA, eTrainZ, eValA}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, m := currentSnapshot(t, env.store, dataset)
	if len(m.Shards) != 1 || m.Shards[0].MinPath != "/train-data/a.bin" || m.Shards[0].MaxPath != "/val-data/a.bin" {
		t.Fatalf("manifest shards = %+v, want 1 片范围 [/train-data/a.bin, /val-data/a.bin]", m.Shards)
	}

	counted := &getCountingStore{Store: env.store}
	c := NewCatalog(counted, dataset, cfg)

	// 仅返回 /val-data 条目：片内 /train-data 条目被剔除（分片仍因范围交叠被下载一次）。
	got, err := c.List(ctx, "/val-data")
	if err != nil {
		t.Fatalf("List(/val-data): %v", err)
	}
	assertListEqual(t, got, []Entry{eValA})
	if n := countKey(counted.getKeys(), m.Shards[0].Key); n != 1 {
		t.Fatalf("相关分片 Get 次数 = %d (%v), want 1", n, counted.getKeys())
	}

	// 部分前缀 "/train" 命中 /train-data/* 两条（(path, chunk) 序；片内 /val-data 条目剔除）。
	counted.reset()
	got, err = c.List(ctx, "/train")
	if err != nil {
		t.Fatalf("List(/train): %v", err)
	}
	assertListEqual(t, got, []Entry{eTrainA, eTrainZ})
	if n := countKey(counted.getKeys(), m.Shards[0].Key); n != 1 {
		t.Fatalf("相关分片 Get 次数 = %d (%v), want 1", n, counted.getKeys())
	}

	// 相关分片（范围交叠）被下载，但无条目匹配前缀 "/val-data/0" → 空结果、无错误。
	counted.reset()
	got, err = c.List(ctx, "/val-data/0")
	if err != nil {
		t.Fatalf("List(/val-data/0): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List(/val-data/0) = [%s], want 空结果", strings.Join(entryRefs(got), " "))
	}
	if n := countKey(counted.getKeys(), m.Shards[0].Key); n != 1 {
		t.Fatalf("相关分片 Get 次数 = %d (%v), want 1", n, counted.getKeys())
	}
}

// TestListDownloadsOnlyRelevantShards：三前缀数据集（每前缀恰一片）→ List("/train-data")
// 只下载相关分片（mock 计数断言：无关分片零下载）；前缀与任何分片范围无关时零分片下载、
// 空结果、无错误（仅读 CURRENT+manifest）。
func TestListDownloadsOnlyRelevantShards(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2} // 6 条 → 3 片，与三个前缀一一对应
	const dataset = "listrelevant"

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
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("listrelevant-block-%d", i))
	}
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, m := currentSnapshot(t, env.store, dataset)
	if len(m.Shards) != 3 {
		t.Fatalf("manifest shards = %d, want 3", len(m.Shards))
	}

	counted := &getCountingStore{Store: env.store}
	c := NewCatalog(counted, dataset, cfg)
	got, err := c.List(ctx, "/train-data")
	if err != nil {
		t.Fatalf("List(/train-data): %v", err)
	}
	assertListEqual(t, got, entries[2:4])

	gets := counted.getKeys()
	if len(gets) != 3 {
		t.Fatalf("List(/train-data) Get count = %d (%v), want 3 (CURRENT+manifest+相关分片)", len(gets), gets)
	}
	if !containsKey(gets, CurrentKey(dataset)) || !containsKey(gets, ManifestKey(dataset, m.SnapshotID)) {
		t.Fatalf("List 未读 CURRENT/manifest: %v", gets)
	}
	if n := countKey(gets, m.Shards[1].Key); n != 1 {
		t.Fatalf("相关分片 Get 次数 = %d (%v), want 1", n, gets)
	}
	for _, i := range []int{0, 2} {
		if n := countKey(gets, m.Shards[i].Key); n != 0 {
			t.Fatalf("无关分片 %s 被下载 %d 次: %v", m.Shards[i].Key, n, gets)
		}
	}

	// 前缀与任何分片范围无关：零分片下载、空结果、无错误。
	counted.reset()
	got, err = c.List(ctx, "/nonexistent")
	if err != nil {
		t.Fatalf("List(/nonexistent): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List(/nonexistent) = [%s], want 空结果", strings.Join(entryRefs(got), " "))
	}
	gets = counted.getKeys()
	if len(gets) != 2 || countKey(gets, CurrentKey(dataset)) != 1 || countKey(gets, ManifestKey(dataset, m.SnapshotID)) != 1 {
		t.Fatalf("List(/nonexistent) Gets = %v, want 恰为 CURRENT+manifest", gets)
	}
	for _, ref := range m.Shards {
		if n := countKey(gets, ref.Key); n != 0 {
			t.Fatalf("List(/nonexistent) 下载分片 %s %d 次", ref.Key, n)
		}
	}
}

// TestListNoCurrent：无 CURRENT → ErrNoCurrent、无任何条目，且只尝试读 CURRENT（不触碰
// manifest/分片）。
func TestListNoCurrent(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{}
	const dataset = "listnocurrent"

	counted := &getCountingStore{Store: env.store}
	c := NewCatalog(counted, dataset, cfg)
	for _, prefix := range []string{"", "/data"} {
		got, err := c.List(ctx, prefix)
		if !errors.Is(err, ErrNoCurrent) {
			t.Fatalf("List(%q) 无 CURRENT = %v, want ErrNoCurrent", prefix, err)
		}
		if len(got) != 0 {
			t.Fatalf("List(%q) 无 CURRENT 返回 %d 条目, want 0", prefix, len(got))
		}
	}

	// 两次调用各只读一次 CURRENT；manifest/分片零下载。
	gets := counted.getKeys()
	if len(gets) != 2 {
		t.Fatalf("List 无 CURRENT Get count = %d (%v), want 2 (仅两次 CURRENT)", len(gets), gets)
	}
	for _, k := range gets {
		if k != CurrentKey(dataset) {
			t.Fatalf("List 无 CURRENT 读取了 %s, want 仅 %s", k, CurrentKey(dataset))
		}
	}
}

// TestListErrorKeepsOldMapping：List 读取最新快照失败（相关分片注入 1 字节损坏）→ 返回
// ErrInvalidShard、不返回任何条目；Catalog 既有映射（Refresh 建立的第一代）仍可用——旧值
// 逐字段可查、新一代条目不可见（失败不动旧映射）。
func TestListErrorKeepsOldMapping(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2}
	const dataset = "listkeepold"

	// 第一代快照：4 条 → 2 片（[/data/a,/data/b]、[/data/c,/other/x]）。
	eA := Entry{Path: "/data/a", Chunk: 0, Size: 1, Node: "n1"}
	eB := Entry{Path: "/data/b", Chunk: 0, Size: 2, Node: "n1"}
	eC := Entry{Path: "/data/c", Chunk: 0, Size: 3, Node: "n2"}
	eX := Entry{Path: "/other/x", Chunk: 0, Size: 4, Node: "n2"}
	tagged := []struct {
		e       *Entry
		content string
	}{{&eA, "listkeepold-a"}, {&eB, "listkeepold-b"}, {&eC, "listkeepold-c"}, {&eX, "listkeepold-x"}}
	for _, tg := range tagged {
		tg.e.Fingerprint = env.putBlock(t, tg.content)
	}
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{eA, eB, eC, eX}); err != nil {
		t.Fatalf("first Commit: %v", err)
	}

	// 旧映射：Refresh 建立第一代全量。
	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, want := range []Entry{eA, eB, eC, eX} {
		assertHit(t, c, want)
	}

	// 第二代快照：upsert /data/a → eA2、新增 /data/d（CURRENT 前进）。
	eA2 := eA
	eA2.Size = 111
	eA2.Node = "n9"
	eA2.Fingerprint = env.putBlock(t, "listkeepold-a-v2")
	eD := Entry{Path: "/data/d", Chunk: 0, Size: 5, Node: "n3"}
	eD.Fingerprint = env.putBlock(t, "listkeepold-d")
	if _, err := w.Commit(ctx, dataset, []Entry{eA2, eD}); err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	_, m2 := currentSnapshot(t, env.store, dataset)
	if len(m2.Shards) != 3 || m2.Shards[0].MinPath != "/data/a" || m2.Shards[0].MaxPath != "/data/b" {
		t.Fatalf("第二代 manifest shards = %+v, want 3 片且首片 [/data/a, /data/b]", m2.Shards)
	}

	// 损坏注入前 List 正常：读到第二代 /data 条目集（[a,b] 与 [c,d] 两片相关）。
	got, err := c.List(ctx, "/data")
	if err != nil {
		t.Fatalf("List(/data) 损坏前: %v", err)
	}
	assertListEqual(t, got, []Entry{eA2, eB, eC, eD})

	// 对首片（[/data/a, /data/b]，与 "/data" 相关）注入 1 字节损坏。
	raw, err := getObject(ctx, env.store, m2.Shards[0].Key)
	if err != nil {
		t.Fatalf("read shard %s: %v", m2.Shards[0].Key, err)
	}
	raw[0] ^= 0xFF
	if _, err := env.store.Put(ctx, m2.Shards[0].Key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("tamper shard: %v", err)
	}

	// List 失败：ErrInvalidShard、零条目返回。
	got, err = c.List(ctx, "/data")
	if !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("List(/data) 损坏后 = %v, want ErrInvalidShard", err)
	}
	if len(got) != 0 {
		t.Fatalf("List(/data) 损坏后返回 %d 条目, want 0", len(got))
	}

	// 旧映射仍可用：第一代 4 条全命中（/data/a 为旧值而非 eA2），第二代新条目不可见。
	for _, want := range []Entry{eA, eB, eC, eX} {
		assertHit(t, c, want)
	}
	if got, ok, err := c.Lookup(ctx, eA.Path, eA.Chunk); err != nil || !ok || got != eA {
		t.Fatalf("Lookup(/data/a) = (%+v, %v, %v), want 旧值 %+v", got, ok, err, eA)
	}
	assertMiss(t, c, eD.Path, eD.Chunk)
}
