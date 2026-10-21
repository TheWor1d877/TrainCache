package publish

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/internal/digest"
	"traincache/core/objectstore"
)

// expectedChunks 把一个 Source 的数据按 chunkSize 切成期望的块内容。
func expectedChunks(data []byte, chunkSize int64) [][]byte {
	var out [][]byte
	for off := int64(0); off < int64(len(data)); off += chunkSize {
		end := off + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		out = append(out, data[off:end])
	}
	return out
}

// verifyPublishedContent 逐 (path, chunk) 校验 Catalog 命中、Entry.Size/Fingerprint
// 与源数据一致、对象存储 BlockKey 内容正确、CURRENT 指向 wantSnapID。
func verifyPublishedContent(t *testing.T, ctx context.Context, store objectstore.Store, cs *cas.Store, dataset, wantSnapID string, chunkSize int64, contents map[string][]byte) {
	t.Helper()

	catalog := index.NewCatalog(store, dataset, index.Config{})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Catalog.Refresh: %v", err)
	}
	if got := catalog.SnapID(); got != wantSnapID {
		t.Fatalf("Catalog.SnapID = %q，期望 %q", got, wantSnapID)
	}

	raw := string(getStoredObject(t, store, index.CurrentKey(dataset)))
	fields := strings.Fields(raw)
	if len(fields) != 2 {
		t.Fatalf("CURRENT 内容 = %q，期望两段", raw)
	}
	if fields[0] != wantSnapID {
		t.Fatalf("CURRENT snapID = %q，期望 %q", fields[0], wantSnapID)
	}

	for path, data := range contents {
		for chunk, chunkData := range expectedChunks(data, chunkSize) {
			e, ok, err := catalog.Lookup(ctx, path, int64(chunk))
			if err != nil || !ok {
				t.Fatalf("Lookup(%q,%d) = (_, %v, %v)，期望命中", path, chunk, ok, err)
			}
			if e.Path != path || e.Chunk != int64(chunk) {
				t.Fatalf("%q#%d 条目 = %+v", path, chunk, e)
			}
			if e.Size != int64(len(chunkData)) {
				t.Fatalf("%q#%d Size = %d，期望 %d", path, chunk, e.Size, len(chunkData))
			}
			if e.Node != "" {
				t.Fatalf("%q#%d Node = %q，期望空", path, chunk, e.Node)
			}
			wantFP := cas.Fingerprint(digest.XXH3_128(chunkData))
			if e.Fingerprint != wantFP {
				t.Fatalf("%q#%d Fingerprint = %s，期望 %s", path, chunk, e.Fingerprint, wantFP)
			}
			if got := readCASBlock(t, cs, e.Fingerprint); !bytes.Equal(got, chunkData) {
				t.Fatalf("%q#%d CAS 内容 = %q，期望 %q", path, chunk, got, chunkData)
			}
			key := objectstore.BlockKey(dataset, path, int64(chunk))
			if got := getStoredObject(t, store, key); !bytes.Equal(got, chunkData) {
				t.Fatalf("%s 对象内容 = %q，期望 %q", key, got, chunkData)
			}
		}
	}
}

// TestMultiTarE2EPublishCatalogAndRepublish 覆盖：
//   - 两个内容不同的非空 Source（含跨块与末块不足 ChunkSize）发布成功；
//   - Inventory/Catalog 读回：每个 (Path, Chunk) 可查，Size/Fingerprint 与源一致；
//   - 每个 BlockKey 在对象存储中存在且内容正确，CURRENT 指向返回的 SnapID；
//   - 再发布第三个 Source：旧条目保留、新条目可查、CURRENT 推进。
func TestMultiTarE2EPublishCatalogAndRepublish(t *testing.T) {
	ctx := context.Background()
	store, cs, w := newTestEnv(t)
	const dataset = "multi-e2e"
	const chunkSize = int64(4)

	dataAlpha := []byte("0123456789") // 10 字节 → 4+4+2
	dataBeta := []byte("abcdefghijk") // 11 字节 → 4+4+3
	contents := map[string][]byte{
		"alpha.tar": dataAlpha,
		"beta.tar":  dataBeta,
	}
	sources := []Source{
		sourceOf("alpha.tar", dataAlpha),
		sourceOf("beta.tar", dataBeta),
	}

	res, err := Publish(ctx, store, cs, w, sources, Options{Dataset: dataset, ChunkSize: chunkSize, Concurrency: 3})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.SnapID == "" {
		t.Fatal("SnapID 为空")
	}
	if res.Entries != 6 || res.Bytes != 21 || res.UniqueBlocks != 6 {
		t.Fatalf("Result = %+v，期望 Entries=6 Bytes=21 UniqueBlocks=6", res)
	}
	verifyPublishedContent(t, ctx, store, cs, dataset, res.SnapID, chunkSize, contents)

	// 再发布第三个 Source：Writer.Commit 做增量 upsert，旧条目必须保留。
	dataGamma := []byte("WX!") // 3 字节 → 4 不足，单块
	res2, err := Publish(ctx, store, cs, w, []Source{sourceOf("gamma.tar", dataGamma)}, Options{
		Dataset:     dataset,
		ChunkSize:   chunkSize,
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("第二次 Publish: %v", err)
	}
	if res2.SnapID == "" || res2.SnapID == res.SnapID {
		t.Fatalf("第二次 SnapID = %q，期望非空且与首次 %q 不同", res2.SnapID, res.SnapID)
	}
	if res2.Entries != 1 || res2.Bytes != 3 || res2.UniqueBlocks != 1 {
		t.Fatalf("第二次 Result = %+v，期望 Entries=1 Bytes=3 UniqueBlocks=1", res2)
	}
	contents["gamma.tar"] = dataGamma
	verifyPublishedContent(t, ctx, store, cs, dataset, res2.SnapID, chunkSize, contents)

	// 重新构造 Catalog 再刷新一次，确认 CURRENT 与磁盘状态一致（非仅内存沿用）。
	catalog := index.NewCatalog(store, dataset, index.Config{})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("重新 Refresh: %v", err)
	}
	if got := catalog.SnapID(); got != res2.SnapID {
		t.Fatalf("Refresh 后 SnapID = %q，期望 %q", got, res2.SnapID)
	}
	if _, ok, err := catalog.Lookup(ctx, "alpha.tar", 0); err != nil || !ok {
		t.Fatalf("旧条目 alpha.tar#0 Lookup = (_, %v, %v)，期望仍命中", ok, err)
	}
}

// TestMultiTarE2EDedupKeepsPerPathKeys 覆盖两个 Source 含完全相同内容块时：
// UniqueBlocks 按内容去重（CAS 只存一份），而远端对象存储仍为各自 (path, chunk) 键。
func TestMultiTarE2EDedupKeepsPerPathKeys(t *testing.T) {
	ctx := context.Background()
	store, cs, w := newTestEnv(t)
	const dataset = "dedup-e2e"
	const chunkSize = int64(4)

	blockX := []byte("X1Y2")
	blockY := []byte("Z3W4")
	dataA := append(append([]byte{}, blockX...), blockY...) // #0=X, #1=Y
	dataB := append([]byte{}, blockX...)                    // #0=X（跨 Source 重复）
	contents := map[string][]byte{
		"dup-a.tar": dataA,
		"dup-b.tar": dataB,
	}
	sources := []Source{
		sourceOf("dup-a.tar", dataA),
		sourceOf("dup-b.tar", dataB),
	}

	res, err := Publish(ctx, store, cs, w, sources, Options{Dataset: dataset, ChunkSize: chunkSize, Concurrency: 4})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.Entries != 3 || res.Bytes != 12 || res.UniqueBlocks != 2 {
		t.Fatalf("Result = %+v，期望 Entries=3 Bytes=12 UniqueBlocks=2", res)
	}
	verifyPublishedContent(t, ctx, store, cs, dataset, res.SnapID, chunkSize, contents)

	catalog := index.NewCatalog(store, dataset, index.Config{})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Catalog.Refresh: %v", err)
	}
	fpA0, ok, err := catalog.Lookup(ctx, "dup-a.tar", 0)
	if err != nil || !ok {
		t.Fatalf("Lookup(dup-a.tar,0) = (_, %v, %v)", ok, err)
	}
	fpA1, ok, err := catalog.Lookup(ctx, "dup-a.tar", 1)
	if err != nil || !ok {
		t.Fatalf("Lookup(dup-a.tar,1) = (_, %v, %v)", ok, err)
	}
	fpB0, ok, err := catalog.Lookup(ctx, "dup-b.tar", 0)
	if err != nil || !ok {
		t.Fatalf("Lookup(dup-b.tar,0) = (_, %v, %v)", ok, err)
	}
	if fpA0.Fingerprint != fpB0.Fingerprint {
		t.Fatalf("相同内容块指纹不一致: %s != %s", fpA0.Fingerprint, fpB0.Fingerprint)
	}
	if fpA0.Fingerprint == fpA1.Fingerprint {
		t.Fatalf("不同内容块指纹相同: %s", fpA0.Fingerprint)
	}

	// CAS 内容去重：仅两份块。
	blocks, err := cs.List()
	if err != nil {
		t.Fatalf("cs.List: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("CAS 块数 = %d，期望 2（内容去重）", len(blocks))
	}

	// 远端仍按每个 (path, chunk) 独立键存储，共 3 个对象。
	keys, err := store.List(ctx, dataset+"/blocks/")
	if err != nil {
		t.Fatalf("store.List: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("对象存储块数 = %d，期望 3（各自 (path,chunk) 键）", len(keys))
	}
	for _, tc := range []struct {
		path  string
		chunk int64
		data  []byte
	}{
		{"dup-a.tar", 0, blockX},
		{"dup-a.tar", 1, blockY},
		{"dup-b.tar", 0, blockX},
	} {
		key := objectstore.BlockKey(dataset, tc.path, tc.chunk)
		if got := getStoredObject(t, store, key); !bytes.Equal(got, tc.data) {
			t.Fatalf("%s 对象内容 = %q，期望 %q", key, got, tc.data)
		}
	}
}
