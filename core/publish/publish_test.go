package publish

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// sourceOf 从内存数据构造一个 Source（*bytes.Reader 实现 io.ReaderAt）。
func sourceOf(path string, data []byte) Source {
	return Source{Path: path, Reader: bytes.NewReader(data), Size: int64(len(data))}
}

// newTestEnv 创建一套内存对象存储 + 临时目录 CAS + Writer。
func newTestEnv(t *testing.T) (*objectstore.Mock, *cas.Store, *index.Writer) {
	t.Helper()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	store := objectstore.NewMock()
	return store, cs, index.NewWriter(store, cs, index.Config{})
}

// readCASBlock 经指纹打开 CAS 块并读回全部数据。
func readCASBlock(t *testing.T, cs *cas.Store, fp cas.Fingerprint) []byte {
	t.Helper()
	r, err := cs.Open(fp)
	if err != nil {
		t.Fatalf("cas.Open(%s): %v", fp, err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		_ = r.Close()
		t.Fatalf("读块 %s: %v", fp, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("关闭块 %s: %v", fp, err)
	}
	return data
}

// getStoredObject 读回对象存储中的一个对象。
func getStoredObject(t *testing.T, store objectstore.Store, key string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := store.Get(context.Background(), key, &buf); err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	return buf.Bytes()
}

// TestPublishMultiSourceChunks 验证多 Source 合并、跨块边界、末块不足 ChunkSize、
// 排序提交、Catalog 逐条命中以及 Entry.Node 为空。
func TestPublishMultiSourceChunks(t *testing.T) {
	ctx := context.Background()
	store, cs, w := newTestEnv(t)
	const dataset = "multi"

	dataA := []byte("0123456789") // 10 字节，ChunkSize=4 → 4+4+2
	dataB := []byte("abc")        // 3 字节，单块不足 ChunkSize
	sources := []Source{sourceOf("a.tar", dataA), sourceOf("b.tar", dataB)}

	res, err := Publish(ctx, store, cs, w, sources, Options{Dataset: dataset, ChunkSize: 4, Concurrency: 3})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.Entries != 4 || res.Bytes != 13 || res.UniqueBlocks != 4 {
		t.Fatalf("Result = %+v，期望 Entries=4 Bytes=13 UniqueBlocks=4", res)
	}
	if res.SnapID == "" {
		t.Fatal("SnapID 为空")
	}

	want := map[string][][]byte{
		"a.tar": {dataA[0:4], dataA[4:8], dataA[8:10]},
		"b.tar": {dataB},
	}
	catalog := index.NewCatalog(store, dataset, index.Config{})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Catalog.Refresh: %v", err)
	}
	if got := catalog.SnapID(); got != res.SnapID {
		t.Fatalf("Catalog.SnapID = %q，期望 %q", got, res.SnapID)
	}
	for path, chunks := range want {
		for chunk, chunkData := range chunks {
			e, ok, err := catalog.Lookup(ctx, path, int64(chunk))
			if err != nil || !ok {
				t.Fatalf("Lookup(%q,%d) = (_, %v, %v)，期望命中", path, chunk, ok, err)
			}
			if e.Size != int64(len(chunkData)) {
				t.Fatalf("%q#%d Size = %d，期望 %d", path, chunk, e.Size, len(chunkData))
			}
			if e.Node != "" {
				t.Fatalf("%q#%d Node = %q，期望空", path, chunk, e.Node)
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

	keys, err := store.List(ctx, dataset+"/blocks/")
	if err != nil {
		t.Fatalf("store.List: %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("对象存储块数 = %d，期望 4", len(keys))
	}
}

// TestPublishDedupAcrossSources 验证相同内容块跨 Source/块边界去重时
// UniqueBlocks 正确，同时仍为每个 (path,chunk) 上传独立对象键。
func TestPublishDedupAcrossSources(t *testing.T) {
	ctx := context.Background()
	store, cs, w := newTestEnv(t)
	const dataset = "dedup"

	block := []byte("WXYZ")
	sources := []Source{
		sourceOf("dup-a.tar", bytes.Repeat(block, 2)), // 2 个相同块
		sourceOf("dup-b.tar", block),                  // 与上一 Source 内容相同
	}
	res, err := Publish(ctx, store, cs, w, sources, Options{Dataset: dataset, ChunkSize: 4, Concurrency: 4})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.Entries != 3 || res.Bytes != 12 || res.UniqueBlocks != 1 {
		t.Fatalf("Result = %+v，期望 Entries=3 Bytes=12 UniqueBlocks=1", res)
	}

	blocks, err := cs.List()
	if err != nil {
		t.Fatalf("cs.List: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("CAS 块数 = %d，期望 1（去重）", len(blocks))
	}

	catalog := index.NewCatalog(store, dataset, index.Config{})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Catalog.Refresh: %v", err)
	}
	var fps []cas.Fingerprint
	for _, tc := range []struct {
		path  string
		chunk int64
	}{{"dup-a.tar", 0}, {"dup-a.tar", 1}, {"dup-b.tar", 0}} {
		e, ok, err := catalog.Lookup(ctx, tc.path, tc.chunk)
		if err != nil || !ok {
			t.Fatalf("Lookup(%q,%d) = (_, %v, %v)，期望命中", tc.path, tc.chunk, ok, err)
		}
		if e.Node != "" {
			t.Fatalf("%q#%d Node = %q，期望空", tc.path, tc.chunk, e.Node)
		}
		fps = append(fps, e.Fingerprint)
	}
	for i := 1; i < len(fps); i++ {
		if fps[i] != fps[0] {
			t.Fatalf("相同内容块指纹不一致: %s != %s", fps[i], fps[0])
		}
	}

	keys, err := store.List(ctx, dataset+"/blocks/")
	if err != nil {
		t.Fatalf("store.List: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("对象存储块数 = %d，期望 3（每个 (path,chunk) 独立键）", len(keys))
	}
}

// TestPublishValidation 验证参数校验：空源、空 dataset、重复 Path、Path 含 '#'、
// 空 Path、负 Size；失败时不得写入任何对象。
func TestPublishValidation(t *testing.T) {
	ctx := context.Background()
	one := func(path string) []Source { return []Source{sourceOf(path, []byte("x"))} }

	cases := []struct {
		name    string
		dataset string
		sources []Source
		wantIs  error // 非 nil 时用 errors.Is 断言
	}{
		{name: "no sources", dataset: "ds", sources: nil, wantIs: ErrNoSources},
		{name: "empty dataset", dataset: "", sources: one("a.tar")},
		{name: "duplicate path", dataset: "ds", sources: []Source{sourceOf("a.tar", []byte("x")), sourceOf("a.tar", []byte("y"))}, wantIs: ErrDuplicatePath},
		{name: "path with hash", dataset: "ds", sources: one("a#b.tar")},
		{name: "empty path", dataset: "ds", sources: one("")},
		{name: "negative size", dataset: "ds", sources: []Source{{Path: "a.tar", Reader: bytes.NewReader(nil), Size: -1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, cs, w := newTestEnv(t)
			_, err := Publish(ctx, store, cs, w, tc.sources, Options{Dataset: tc.dataset})
			if err == nil {
				t.Fatal("Publish 成功，期望错误")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v，期望 errors.Is(%v)", err, tc.wantIs)
			}
			keys, listErr := store.List(ctx, "")
			if listErr != nil {
				t.Fatalf("store.List: %v", listErr)
			}
			if len(keys) != 0 {
				t.Fatalf("校验失败后出现对象: %v", keys)
			}
			blocks, listErr := cs.List()
			if listErr != nil {
				t.Fatalf("cs.List: %v", listErr)
			}
			if len(blocks) != 0 {
				t.Fatalf("校验失败后出现 CAS 块: %v", blocks)
			}
		})
	}
}

// faultStore 包装 Store，令指定键的 Put 返回注入错误。
type faultStore struct {
	objectstore.Store
	mu       sync.Mutex
	failKey  string
	err      error
	attempts int
}

func (f *faultStore) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	f.mu.Lock()
	if key == f.failKey {
		f.attempts++
		f.mu.Unlock()
		return "", f.err
	}
	f.mu.Unlock()
	return f.Store.Put(ctx, key, r, size)
}

// TestPublishStoreFailure 验证块上传失败：返回包装错误、CURRENT 未被写入。
func TestPublishStoreFailure(t *testing.T) {
	ctx := context.Background()
	mock := objectstore.NewMock()
	injected := errors.New("injected store failure")
	fs := &faultStore{
		Store:   mock,
		failKey: objectstore.BlockKey("fail", "f.tar", 1),
		err:     injected,
	}
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	w := index.NewWriter(fs, cs, index.Config{})

	sources := []Source{sourceOf("f.tar", []byte("01234"))} // ChunkSize=2 → #0、#1、#2
	_, err = Publish(ctx, fs, cs, w, sources, Options{Dataset: "fail", ChunkSize: 2, Concurrency: 2})
	if err == nil {
		t.Fatal("Publish 成功，期望注入错误")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("err = %v，期望 errors.Is 注入错误", err)
	}
	fs.mu.Lock()
	attempts := fs.attempts
	fs.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("失败键 Put 次数 = %d，期望 1", attempts)
	}

	if _, err := mock.GetETag(ctx, index.CurrentKey("fail")); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("CURRENT GetETag err = %v，期望 ErrNotFound（未提交）", err)
	}
	manifests, err := mock.List(ctx, "dataset/fail/snapshots/")
	if err != nil {
		t.Fatalf("mock.List: %v", err)
	}
	if len(manifests) != 0 {
		t.Fatalf("失败后出现快照对象: %v", manifests)
	}
}

// TestPublishZeroOptionsAndEmptySource 验证零值/负值选项回退默认值，
// 且空文件不产生 Entry。
func TestPublishZeroOptionsAndEmptySource(t *testing.T) {
	ctx := context.Background()
	store, cs, w := newTestEnv(t)
	const dataset = "fallback"

	sources := []Source{
		sourceOf("empty.tar", nil),
		sourceOf("one.tar", []byte("hello")),
	}
	res, err := Publish(ctx, store, cs, w, sources, Options{Dataset: dataset, ChunkSize: -1, Concurrency: -1})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.Entries != 1 || res.Bytes != 5 || res.UniqueBlocks != 1 {
		t.Fatalf("Result = %+v，期望 Entries=1 Bytes=5 UniqueBlocks=1", res)
	}
	if res.SnapID == "" {
		t.Fatal("SnapID 为空")
	}

	catalog := index.NewCatalog(store, dataset, index.Config{})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Catalog.Refresh: %v", err)
	}
	if _, ok, err := catalog.Lookup(ctx, "empty.tar", 0); err != nil || ok {
		t.Fatalf("Lookup(empty.tar,0) = (_, %v, %v)，期望未命中", ok, err)
	}
	e, ok, err := catalog.Lookup(ctx, "one.tar", 0)
	if err != nil || !ok {
		t.Fatalf("Lookup(one.tar,0) = (_, %v, %v)，期望命中", ok, err)
	}
	if e.Size != 5 || e.Chunk != 0 || e.Node != "" {
		t.Fatalf("one.tar 条目 = %+v，期望 Size=5 Chunk=0 Node=\"\"", e)
	}
	if got := readCASBlock(t, cs, e.Fingerprint); !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("one.tar 内容 = %q，期望 %q", got, "hello")
	}
}
