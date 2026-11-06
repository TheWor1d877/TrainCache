package server

// cache_test.go：取数顺序编排（docs/9.3-test.md §4.1）。
//
// 覆盖三条分支（在途 / 读盘命中 / 回源）与两种状态（开关开、开关关）：
// 在途优先于查盘；读盘条目回填 .meta 里的两个头；未命中回源并落盘；独立
// 回源分支同样落盘；写盘失败不影响响应；开关关闭时不查盘、不写盘、
// cache_hit 恒为 0。

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/gateway/internal/cache"
	"traincache/gateway/internal/merge"
	"traincache/gateway/internal/obs"
	"traincache/gateway/internal/origin"
	"traincache/gateway/internal/sched"
)

// newCacheStoreDir 构造落在临时目录上的缓存存储，并返回根目录；配额放到
// 1GiB，用例里不会触发淘汰。
func newCacheStoreDir(t *testing.T) (*cache.Store, string) {
	t.Helper()
	dir := t.TempDir()
	return cache.New(dir, 1<<30, 1<<30), dir
}

// newCacheServer 构造带磁盘缓存的网关服务，并返回记录日志的 handler。
func newCacheServer(t *testing.T, org Origin, st *cache.Store, enabled bool, bufBytes, limit int) (*Server, *obs.Logger, *captureHandler) {
	t.Helper()
	ob := obs.New(time.Hour, discardLogger())
	t.Cleanup(ob.Stop)

	h := &captureHandler{}
	s := New(org, Options{
		Bucket:       "traincache",
		Concurrency:  limit,
		BufferBytes:  bufBytes,
		Registry:     merge.NewRegistry(bufBytes),
		Scheduler:    sched.New(limit),
		Obs:          ob,
		Cache:        st,
		CacheEnabled: enabled,
	}, slog.New(h))
	return s, ob, h
}

// putCached 直接往缓存里写一个块，模拟已 rename 落盘的正式名。
func putCached(t *testing.T, st *cache.Store, key string, body []byte, meta cache.Metadata) {
	t.Helper()
	w, err := st.BeginPut(context.Background(), key)
	if err != nil {
		t.Fatalf("BeginPut: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(meta); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// waitCached 等块在盘上可见；返回打开的对象，调用方负责关闭。
func waitCached(t *testing.T, st *cache.Store, key string) *cache.Object {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		obj, err := st.Lookup(key)
		if err == nil {
			return obj
		}
		if time.Now().After(deadline) {
			t.Fatalf("块 %q 未落盘: %v", key, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// cachedBody 读出盘上块的字节。
func cachedBody(t *testing.T, obj *cache.Object) []byte {
	t.Helper()
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("读盘: %v", err)
	}
	return b
}

// tmpFiles 列出缓存目录里残留的 .tmp 半成品。
func tmpFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".tmp") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	return out
}

// dirEntries 列出缓存目录下的全部文件名。
func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			out = append(out, filepath.Base(path))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	return out
}

// captureHandler 收集日志记录，用于断言「读盘命中」「写盘失败」「条目结束」
// 这几行的字段。
type captureHandler struct {
	mu   sync.Mutex
	recs []capturedLog
}

// capturedLog 是一条日志记录：消息与展开后的字段。
type capturedLog struct {
	msg   string
	attrs map[string]slog.Value
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]slog.Value{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		return true
	})
	h.mu.Lock()
	h.recs = append(h.recs, capturedLog{msg: r.Message, attrs: attrs})
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// find 返回消息等于 msg 的全部记录。
func (h *captureHandler) find(msg string) []capturedLog {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []capturedLog
	for _, rec := range h.recs {
		if rec.msg == msg {
			out = append(out, rec)
		}
	}
	return out
}

// count 返回消息等于 msg 的记录条数。
func (h *captureHandler) count(msg string) int { return len(h.find(msg)) }

// TestCacheHitServesFromDiskWithMetaHeaders 覆盖读盘命中分支：字节从盘上取，
// 200 回填 .meta 里的 ETag 与 Last-Modified，且不回源。
func TestCacheHitServesFromDiskWithMetaHeaders(t *testing.T) {
	const key = "ds/blocks/cached#0"
	diskBody := []byte("cached-block-body")
	lastModified := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	var opens atomic.Int64
	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"origin-etag"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(bytes.NewReader([]byte("origin-body"))), nil
		},
	}

	st, _ := newCacheStoreDir(t)
	putCached(t, st, key, diskBody, cache.Metadata{ETag: `"disk-etag"`, LastModified: lastModified})

	s, ob, logs := newCacheServer(t, org, st, true, 1<<20, 4)
	rec := serve(s, http.MethodGet, "/traincache/ds/blocks/cached%230")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态 = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != string(diskBody) {
		t.Fatalf("body = %q, want %q", got, diskBody)
	}
	if v := rec.Header().Get("ETag"); v != `"disk-etag"` {
		t.Fatalf("ETag = %q, want %q", v, `"disk-etag"`)
	}
	if v := rec.Header().Get("Last-Modified"); v != lastModified.Format(http.TimeFormat) {
		t.Fatalf("Last-Modified = %q, want %q", v, lastModified.Format(http.TimeFormat))
	}
	if v := rec.Header().Get("Content-Length"); v != "" {
		t.Fatalf("Content-Length = %q, want 空（chunked）", v)
	}
	if got := opens.Load(); got != 0 {
		t.Fatalf("源站 GET 次数 = %d, want 0（读盘命中不回源）", got)
	}
	if got := ob.Snapshot().CacheHit; got != 1 {
		t.Fatalf("cache_hit = %d, want 1", got)
	}
	if got := ob.Snapshot().Owners; got != 0 {
		t.Fatalf("owners = %d, want 0（读盘命中不是真实源站 GET）", got)
	}

	hits := logs.find("读盘命中")
	if len(hits) != 1 {
		t.Fatalf("「读盘命中」行 = %d 条, want 1", len(hits))
	}
	if v := hits[0].attrs["dataset"]; v.String() != "ds" {
		t.Fatalf("读盘命中 dataset = %q, want %q", v.String(), "ds")
	}
	if v := hits[0].attrs["key"]; v.String() != key {
		t.Fatalf("读盘命中 key = %q, want %q", v.String(), key)
	}
}

// TestCacheMissFetchesOriginAndWritesDisk 覆盖未命中分支：回源、正常返回，
// 并把同一批字节按 §6.8 的协议落盘（写 .meta 再 rename）。
func TestCacheMissFetchesOriginAndWritesDisk(t *testing.T) {
	const key = "ds/blocks/miss#0"
	originBody := []byte("origin-block-body")
	lastModified := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)

	var opens atomic.Int64
	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"origin-etag"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(bytes.NewReader(originBody)), nil
		},
	}

	st, dir := newCacheStoreDir(t)
	s, ob, _ := newCacheServer(t, org, st, true, 1<<20, 4)
	rec := serve(s, http.MethodGet, "/traincache/ds/blocks/miss%230")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态 = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != string(originBody) {
		t.Fatalf("body = %q, want %q", got, originBody)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1", got)
	}
	if got := ob.Snapshot().CacheHit; got != 0 {
		t.Fatalf("cache_hit = %d, want 0", got)
	}
	if got := ob.Snapshot().Owners; got != 1 {
		t.Fatalf("owners = %d, want 1", got)
	}

	// 落盘：内容与源站一致，元数据与源站一致。
	obj := waitCached(t, st, key)
	if got := cachedBody(t, obj); !bytes.Equal(got, originBody) {
		t.Fatalf("盘上内容 = %q, want %q", got, originBody)
	}
	meta := obj.Metadata()
	if meta.ETag != `"origin-etag"` {
		t.Fatalf("盘上 ETag = %q, want %q", meta.ETag, `"origin-etag"`)
	}
	if !meta.LastModified.Equal(lastModified) {
		t.Fatalf("盘上 Last-Modified = %v, want %v", meta.LastModified, lastModified)
	}
	if meta.Size != int64(len(originBody)) {
		t.Fatalf("盘上 Size = %d, want %d", meta.Size, len(originBody))
	}
	if left := tmpFiles(t, dir); len(left) != 0 {
		t.Fatalf("盘上残留 .tmp: %v", left)
	}
}

// TestCacheInFlightBeatsDiskLookup 覆盖「在途优先于查盘」：条目在途时，即使
// 盘上已有同一块，请求也挂到在途条目上，不读盘、不计 cache_hit。
func TestCacheInFlightBeatsDiskLookup(t *testing.T) {
	const key = "ds/blocks/inflight#0"
	originBody := []byte("origin-block-body")
	lastModified := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

	var opens atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"origin-etag"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			opens.Add(1)
			return &gateReader{entered: entered, release: release, data: originBody}, nil
		},
	}

	st, _ := newCacheStoreDir(t)
	s, ob, _ := newCacheServer(t, org, st, true, 1<<20, 4)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	type result struct {
		body string
		etag string
		err  error
	}
	results := make(chan result, 2)
	get := func() {
		resp, err := ts.Client().Get(ts.URL + "/traincache/ds/blocks/inflight%230")
		if err != nil {
			results <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		results <- result{body: string(b), etag: resp.Header.Get("ETag"), err: err}
	}

	// 第一轮：回源建条目，拉取协程阻塞在源站读上。
	go get()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("源站未收到块 GET")
	}

	// 条目在途时把同一 key 写进盘上，模拟并发请求刚 rename 落盘。
	putCached(t, st, key, []byte("disk-body"), cache.Metadata{
		ETag: `"disk-etag"`, LastModified: lastModified})

	// 第二轮：在途优先，应挂到在途条目而不是查盘。
	go get()
	deadline := time.Now().Add(3 * time.Second)
	for {
		e, ok := s.registry.Attach(key)
		if ok && e.Subscribers() == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("第二轮请求未挂到在途条目（订阅者 = %d）", e.Subscribers())
		}
		time.Sleep(time.Millisecond)
	}

	close(release)
	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("第 %d 轮请求失败: %v", i+1, res.err)
			}
			if res.body != string(originBody) {
				t.Fatalf("第 %d 轮 body = %q, want %q（在途优先于查盘）", i+1, res.body, originBody)
			}
			if res.etag != `"origin-etag"` {
				t.Fatalf("第 %d 轮 ETag = %q, want %q", i+1, res.etag, `"origin-etag"`)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("第 %d 轮请求未结束", i+1)
		}
	}

	if got := opens.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1", got)
	}
	if got := ob.Snapshot().CacheHit; got != 0 {
		t.Fatalf("cache_hit = %d, want 0（在途优先于查盘）", got)
	}
}

// TestCacheIndependentOriginBranchWritesDisk 覆盖独立回源分支：条目在途但
// 已越过合并窗口时，这次回源的字节同样按 §6.8 落盘。
func TestCacheIndependentOriginBranchWritesDisk(t *testing.T) {
	const (
		c   = 8
		key = "ds/blocks/late#0"
	)
	secondBody := []byte("independent-body")
	lastModified := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)

	var opens atomic.Int64
	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"late-etag"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(bytes.NewReader(secondBody)), nil
		},
	}

	st, _ := newCacheStoreDir(t)
	s, ob, _ := newCacheServer(t, org, st, true, c, 4)

	// 造一个在途、但 written > C 的条目，模拟迟到场景。
	e := s.registry.Create(key, s.registry.GetBuffer())
	defer e.Finish(nil)
	hold := make(chan struct{})
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		_ = e.Pump(context.Background(), &blockAfterReader{data: make([]byte, c+1), hold: hold})
	}()
	deadline := time.Now().Add(3 * time.Second)
	for e.Written() <= c {
		if time.Now().After(deadline) {
			t.Fatal("条目未越过合并窗口")
		}
		time.Sleep(time.Millisecond)
	}

	rec := serve(s, http.MethodGet, "/traincache/ds/blocks/late%230")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态 = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != string(secondBody) {
		t.Fatalf("body = %q, want %q", got, secondBody)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1", got)
	}
	if got := ob.Snapshot().CacheHit; got != 0 {
		t.Fatalf("cache_hit = %d, want 0", got)
	}

	// 独立回源的字节同样落盘：在途条目是手工造的，没写过盘，盘上这份只能
	// 来自独立回源分支。
	obj := waitCached(t, st, key)
	if got := cachedBody(t, obj); !bytes.Equal(got, secondBody) {
		t.Fatalf("盘上内容 = %q, want %q", got, secondBody)
	}
	if got := obj.Metadata().ETag; got != `"late-etag"` {
		t.Fatalf("盘上 ETag = %q, want %q", got, `"late-etag"`)
	}

	close(hold)
	select {
	case <-pumpDone:
	case <-time.After(3 * time.Second):
		t.Fatal("拉取协程未结束")
	}
}

// TestCacheDisabledSkipsLookupAndWrite 覆盖关闭态：既不查盘也不写盘，
// cache_hit 恒为 0，行为与加缓存之前一致（9.2 D29）。
func TestCacheDisabledSkipsLookupAndWrite(t *testing.T) {
	const key = "ds/blocks/off#0"
	originBody := []byte("origin-body")
	lastModified := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)

	cases := []struct {
		name     string
		enabled  bool
		nilStore bool
	}{
		{name: "开关关闭", enabled: false},
		{name: "开关打开但没有存储", enabled: true, nilStore: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opens atomic.Int64
			org := &fakeOrigin{
				openInfo: origin.ObjectInfo{ETag: `"origin-etag"`, LastModified: lastModified},
				openFn: func(context.Context, string) (io.ReadCloser, error) {
					opens.Add(1)
					return io.NopCloser(bytes.NewReader(originBody)), nil
				},
			}

			st, dir := newCacheStoreDir(t)
			putCached(t, st, key, []byte("disk-body"), cache.Metadata{
				ETag: `"disk-etag"`, LastModified: lastModified})
			before := dirEntries(t, dir)

			store := st
			if tc.nilStore {
				store = nil
			}
			s, ob, _ := newCacheServer(t, org, store, tc.enabled, 1<<20, 4)

			rec := serve(s, http.MethodGet, "/traincache/ds/blocks/off%230")
			if rec.Code != http.StatusOK {
				t.Fatalf("状态 = %d, want 200", rec.Code)
			}
			// 关闭态不查盘：响应来自源站。
			if got := rec.Body.String(); got != string(originBody) {
				t.Fatalf("body = %q, want %q", got, originBody)
			}
			if v := rec.Header().Get("ETag"); v != `"origin-etag"` {
				t.Fatalf("ETag = %q, want %q", v, `"origin-etag"`)
			}
			if got := opens.Load(); got != 1 {
				t.Fatalf("源站 GET 次数 = %d, want 1", got)
			}
			if got := ob.Snapshot().CacheHit; got != 0 {
				t.Fatalf("cache_hit = %d, want 0", got)
			}

			// 关闭态不写盘：盘上内容与文件列表都不变。
			obj, err := st.Lookup(key)
			if err != nil {
				t.Fatalf("Lookup: %v", err)
			}
			if got := cachedBody(t, obj); !bytes.Equal(got, []byte("disk-body")) {
				t.Fatalf("盘上内容 = %q, want %q", got, "disk-body")
			}
			after := dirEntries(t, dir)
			if len(after) != len(before) {
				t.Fatalf("盘上文件数 = %d, want %d（关闭态不写盘）", len(after), len(before))
			}
		})
	}
}

// TestCacheWriteFailureKeepsResponse 覆盖写盘失败：键的分段超过 255 字节时
// BeginPut 直接报错，响应照常返回，只记一条写盘失败 Warn。
func TestCacheWriteFailureKeepsResponse(t *testing.T) {
	long := strings.Repeat("x", 300)
	originBody := []byte("origin-body")
	lastModified := time.Date(2026, 8, 9, 10, 11, 12, 0, time.UTC)

	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"origin-etag"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(originBody)), nil
		},
	}

	st, _ := newCacheStoreDir(t)
	s, ob, logs := newCacheServer(t, org, st, true, 1<<20, 4)
	rec := serve(s, http.MethodGet, "/traincache/ds/blocks/"+long+"%230")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态 = %d, want 200（写盘失败不影响响应）", rec.Code)
	}
	if got := rec.Body.String(); got != string(originBody) {
		t.Fatalf("body = %q, want %q", got, originBody)
	}
	if got := ob.Snapshot().CacheHit; got != 0 {
		t.Fatalf("cache_hit = %d, want 0", got)
	}

	fails := logs.find("写盘失败")
	if len(fails) != 1 {
		t.Fatalf("「写盘失败」行 = %d 条, want 1", len(fails))
	}
	for _, field := range []string{"dataset", "key", "reason", "err", "err_class"} {
		if _, ok := fails[0].attrs[field]; !ok {
			t.Fatalf("「写盘失败」行缺字段 %q", field)
		}
	}
	if v := fails[0].attrs["dataset"]; v.String() != "ds" {
		t.Fatalf("写盘失败 dataset = %q, want %q", v.String(), "ds")
	}
	if v := fails[0].attrs["reason"]; v.String() == "" {
		t.Fatal("写盘失败 reason 为空")
	}
}

// TestCacheAbortOnOriginFailure 覆盖中途失败：源站截断时删掉 .tmp，盘上不留
// 半成品，下一次请求仍然回源。
func TestCacheAbortOnOriginFailure(t *testing.T) {
	const key = "ds/blocks/broken#0"
	lastModified := time.Date(2026, 9, 10, 11, 12, 13, 0, time.UTC)

	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"origin-etag"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			return &errorReadCloser{err: io.ErrUnexpectedEOF}, nil
		},
	}

	st, dir := newCacheStoreDir(t)
	s, _, logs := newCacheServer(t, org, st, true, 1<<20, 4)
	rec := serve(s, http.MethodGet, "/traincache/ds/blocks/broken%230")
	if rec.Code == http.StatusOK && rec.Body.Len() > 0 {
		t.Fatalf("源站失败时不应返回完整 200：body = %q", rec.Body.String())
	}

	// 半成品必须删干净：盘上没有正式名，也没有 .tmp。
	if obj, err := st.Lookup(key); err == nil {
		obj.Close()
		t.Fatal("源站失败后盘上不应有正式名")
	}
	if left := tmpFiles(t, dir); len(left) != 0 {
		t.Fatalf("源站失败后残留 .tmp: %v", left)
	}
	if got := logs.count("写盘失败"); got != 0 {
		t.Fatalf("「写盘失败」行 = %d 条, want 0（源站失败不是写盘失败）", got)
	}
}

// TestCacheEntryEndFromField 覆盖条目结束行的 from 字段：回源为 origin，
// 读盘命中为 cache。
func TestCacheEntryEndFromField(t *testing.T) {
	const key = "ds/blocks/from#0"
	originBody := []byte("origin-body")
	lastModified := time.Date(2026, 10, 11, 12, 13, 14, 0, time.UTC)

	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"origin-etag"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(originBody)), nil
		},
	}

	st, _ := newCacheStoreDir(t)
	s, _, logs := newCacheServer(t, org, st, true, 1<<20, 4)

	serve(s, http.MethodGet, "/traincache/ds/blocks/from%230")
	ends := logs.find("条目结束")
	if len(ends) != 1 {
		t.Fatalf("「条目结束」行 = %d 条, want 1", len(ends))
	}
	if v := ends[0].attrs["from"]; v.String() != fromOrigin {
		t.Fatalf("条目结束 from = %q, want %q", v.String(), fromOrigin)
	}

	// 第二轮命中盘上：from 必须是 cache。
	serve(s, http.MethodGet, "/traincache/ds/blocks/from%230")
	ends = logs.find("条目结束")
	if len(ends) != 2 {
		t.Fatalf("「条目结束」行 = %d 条, want 2", len(ends))
	}
	if v := ends[1].attrs["from"]; v.String() != fromCache {
		t.Fatalf("条目结束 from = %q, want %q", v.String(), fromCache)
	}
}
