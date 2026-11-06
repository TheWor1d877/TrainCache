package server

// cache_integration_test.go：真实 minio-go 客户端 → 网关 HTTP → 假 S3 源站 的
// 缓存端到端用例（docs/9.3-test.md §4.3）。同一 key 先后两轮，两轮之间无并发：
// 第一轮源站 GET=1；第二轮源站 GET 不增加、条目结束行的 from=cache；两轮
// ReadAll 字节一致且 ETag 相同。

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"traincache/gateway/internal/merge"
	"traincache/gateway/internal/obs"
	"traincache/gateway/internal/origin"
	"traincache/gateway/internal/sched"
)

// cacheIntegrationKey 是被先后两轮读取的块键。
const cacheIntegrationKey = "ds/blocks/cache#0"

// cacheS3Origin 是最小的假 S3 源站：只处理对象 GET/HEAD 与桶位置查询，
// 并统计 GET/HEAD 次数。
type cacheS3Origin struct {
	body         []byte
	lastModified time.Time

	gets  atomic.Int64
	heads atomic.Int64
}

// setHeaders 回填对象头；HEAD 与 GET 使用同一组值。
func (o *cacheS3Origin) setHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Length", strconv.Itoa(len(o.body)))
	w.Header().Set("ETag", `"cache-etag"`)
	w.Header().Set("Last-Modified", o.lastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
}

// ServeHTTP 实现假源站：位置查询、对象 GET/HEAD、以及不存在 key 的 404。
func (o *cacheS3Origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// minio-go 未绑定区域时会先查桶位置；这不是对象 GET，不计入 GET 次数。
	if r.URL.Query().Has("location") {
		writeIntegrationXML(w, http.StatusOK, `<LocationConstraint>us-east-1</LocationConstraint>`)
		return
	}
	if r.URL.Path != "/"+integrationBucket+"/"+cacheIntegrationKey {
		writeIntegrationXML(w, http.StatusNotFound, `<Error><Code>NoSuchKey</Code></Error>`)
		return
	}

	switch r.Method {
	case http.MethodHead:
		o.heads.Add(1)
		o.setHeaders(w)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		o.gets.Add(1)
		o.setHeaders(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(o.body)
	default:
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// cacheHeaderRecorder 记录块键的 GET 响应头，用于断言两轮的 ETag 一致。
type cacheHeaderRecorder struct {
	base http.RoundTripper

	mu      sync.Mutex
	headers []http.Header
}

func (h *cacheHeaderRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := h.base.RoundTrip(req)
	if err == nil && req.Method == http.MethodGet &&
		req.URL.Path == "/"+integrationBucket+"/"+cacheIntegrationKey {
		h.mu.Lock()
		h.headers = append(h.headers, resp.Header.Clone())
		h.mu.Unlock()
	}
	return resp, err
}

// recorded 返回已记录响应头的副本。
func (h *cacheHeaderRecorder) recorded() []http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]http.Header(nil), h.headers...)
}

// TestIntegrationCacheSecondRoundFromDisk 覆盖同一 key 先后两轮：第二轮不回源，
// 字节与 ETag 与第一轮一致，条目结束行的 from=cache。
func TestIntegrationCacheSecondRoundFromDisk(t *testing.T) {
	body := []byte(strings.Repeat("cache-integration-body;", 4096))
	lastModified := time.Date(2026, 11, 12, 13, 14, 15, 0, time.UTC)

	src := &cacheS3Origin{body: body, lastModified: lastModified}
	srcSrv := httptest.NewServer(src)
	defer srcSrv.Close()

	org, err := origin.New(strings.TrimPrefix(srcSrv.URL, "http://"),
		"ak", "sk", integrationBucket, false, false)
	if err != nil {
		t.Fatalf("origin.New: %v", err)
	}

	st, _ := newCacheStoreDir(t)
	ob := obs.New(time.Hour, discardLogger())
	t.Cleanup(ob.Stop)

	logs := &captureHandler{}
	const bufferBytes = 2 << 20
	s := New(org, Options{
		Bucket:       integrationBucket,
		Concurrency:  8,
		BufferBytes:  bufferBytes,
		Registry:     merge.NewRegistry(bufferBytes),
		Scheduler:    sched.New(8),
		Obs:          ob,
		Cache:        st,
		CacheEnabled: true,
	}, slog.New(logs))
	gwSrv := httptest.NewServer(s.Handler())
	defer gwSrv.Close()

	recorder := &cacheHeaderRecorder{base: http.DefaultTransport}
	cli, err := minio.New(strings.TrimPrefix(gwSrv.URL, "http://"), &minio.Options{
		Creds:      credentials.NewStaticV4("ak", "sk", ""),
		Secure:     false,
		Region:     "us-east-1",
		MaxRetries: 1,
		Transport:  recorder,
	})
	if err != nil {
		t.Fatalf("minio.New: %v", err)
	}

	read := func() ([]byte, error) {
		obj, err := cli.GetObject(context.Background(), integrationBucket, cacheIntegrationKey,
			minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		defer obj.Close()
		return io.ReadAll(obj)
	}

	// 第一轮：回源。
	first, err := read()
	if err != nil {
		t.Fatalf("第一轮 ReadAll: %v", err)
	}
	if !bytes.Equal(first, body) {
		t.Fatalf("第一轮内容长度 = %d, want %d", len(first), len(body))
	}
	if got := src.gets.Load(); got != 1 {
		t.Fatalf("第一轮源站 GET = %d, want 1", got)
	}

	// 第二轮：同一 key，读盘命中，不回源。
	second, err := read()
	if err != nil {
		t.Fatalf("第二轮 ReadAll: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("两轮内容不一致：第一轮 %d 字节，第二轮 %d 字节", len(first), len(second))
	}
	if got := src.gets.Load(); got != 1 {
		t.Fatalf("第二轮后源站 GET = %d, want 1（第二轮不回源）", got)
	}
	if got := ob.Snapshot().CacheHit; got != 1 {
		t.Fatalf("cache_hit = %d, want 1", got)
	}
	if got := ob.Snapshot().Owners; got != 1 {
		t.Fatalf("owners = %d, want 1", got)
	}

	// 两轮响应的 ETag 相同：第一轮来自源站，第二轮来自盘上的 .meta。
	got := recorder.recorded()
	if len(got) != 2 {
		t.Fatalf("记录到的对象 GET 响应 = %d, want 2", len(got))
	}
	for i, h := range got {
		if v := h.Get("ETag"); v != `"cache-etag"` {
			t.Fatalf("第 %d 轮 ETag = %q, want %q", i+1, v, `"cache-etag"`)
		}
		// minio-go 对每个 GET 响应强制解析 Last-Modified，读盘命中也必须回填。
		if v := h.Get("Last-Modified"); v != lastModified.Format(http.TimeFormat) {
			t.Fatalf("第 %d 轮 Last-Modified = %q, want %q", i+1, v, lastModified.Format(http.TimeFormat))
		}
	}
	if got[0].Get("ETag") != got[1].Get("ETag") {
		t.Fatalf("两轮 ETag 不一致: %q vs %q", got[0].Get("ETag"), got[1].Get("ETag"))
	}

	// 条目结束行的 from：第一轮 origin，第二轮 cache。
	ends := logs.find("条目结束")
	if len(ends) != 2 {
		t.Fatalf("「条目结束」行 = %d 条, want 2", len(ends))
	}
	if v := ends[0].attrs["from"]; v.String() != fromOrigin {
		t.Fatalf("第一轮条目结束 from = %q, want %q", v.String(), fromOrigin)
	}
	if v := ends[1].attrs["from"]; v.String() != fromCache {
		t.Fatalf("第二轮条目结束 from = %q, want %q", v.String(), fromCache)
	}
	if hits := logs.find("读盘命中"); len(hits) != 1 {
		t.Fatalf("「读盘命中」行 = %d 条, want 1", len(hits))
	}

	// 盘上留着这一块：下一轮仍然不回源。
	obj := waitCached(t, st, cacheIntegrationKey)
	if got := cachedBody(t, obj); !bytes.Equal(got, body) {
		t.Fatalf("盘上内容长度 = %d, want %d", len(got), len(body))
	}
}

// TestIntegrationCacheDisabledKeepsOriginGet 覆盖关闭态：两轮都回源，
// cache_hit 恒为 0，缓存目录不产生文件。
func TestIntegrationCacheDisabledKeepsOriginGet(t *testing.T) {
	body := []byte("disabled-cache-body")
	lastModified := time.Date(2026, 12, 13, 14, 15, 16, 0, time.UTC)

	src := &cacheS3Origin{body: body, lastModified: lastModified}
	srcSrv := httptest.NewServer(src)
	defer srcSrv.Close()

	org, err := origin.New(strings.TrimPrefix(srcSrv.URL, "http://"),
		"ak", "sk", integrationBucket, false, false)
	if err != nil {
		t.Fatalf("origin.New: %v", err)
	}

	st, dir := newCacheStoreDir(t)
	ob := obs.New(time.Hour, discardLogger())
	t.Cleanup(ob.Stop)

	const bufferBytes = 2 << 20
	s := New(org, Options{
		Bucket:       integrationBucket,
		Concurrency:  8,
		BufferBytes:  bufferBytes,
		Registry:     merge.NewRegistry(bufferBytes),
		Scheduler:    sched.New(8),
		Obs:          ob,
		Cache:        st,
		CacheEnabled: false,
	}, slog.New(&captureHandler{}))
	gwSrv := httptest.NewServer(s.Handler())
	defer gwSrv.Close()

	cli, err := minio.New(strings.TrimPrefix(gwSrv.URL, "http://"), &minio.Options{
		Creds:      credentials.NewStaticV4("ak", "sk", ""),
		Secure:     false,
		Region:     "us-east-1",
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("minio.New: %v", err)
	}

	for i := 0; i < 2; i++ {
		obj, err := cli.GetObject(context.Background(), integrationBucket, cacheIntegrationKey,
			minio.GetObjectOptions{})
		if err != nil {
			t.Fatalf("第 %d 轮 GetObject: %v", i+1, err)
		}
		data, err := io.ReadAll(obj)
		obj.Close()
		if err != nil {
			t.Fatalf("第 %d 轮 ReadAll: %v", i+1, err)
		}
		if !bytes.Equal(data, body) {
			t.Fatalf("第 %d 轮内容 = %q, want %q", i+1, data, body)
		}
	}

	if got := src.gets.Load(); got != 2 {
		t.Fatalf("源站 GET = %d, want 2（关闭态两轮都回源）", got)
	}
	if got := ob.Snapshot().CacheHit; got != 0 {
		t.Fatalf("cache_hit = %d, want 0", got)
	}
	if left := dirEntries(t, dir); len(left) != 0 {
		t.Fatalf("缓存目录产生文件: %v", left)
	}
}
