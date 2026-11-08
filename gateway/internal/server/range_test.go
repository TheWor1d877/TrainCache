package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"traincache/gateway/internal/cache"
	"traincache/gateway/internal/origin"
)

// TestBlockRangePassthroughBypassesMergeAndCache 覆盖带 Range 的块 GET：
// 直接透传源站并保留 206/Content-Range/Content-Length/Accept-Ranges，
// 不进入在途合并表、不读盘、不写盘。
func TestBlockRangePassthroughBypassesMergeAndCache(t *testing.T) {
	const (
		key  = "ds/blocks/ranged#0"
		body = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" +
			"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	)
	full := []byte(body)
	lastModified := time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC)

	var gotStart, gotEnd atomic.Int64
	org := &fakeOrigin{
		openRangeFn: func(_ context.Context, _ string, start, end int64) (*origin.Object, error) {
			gotStart.Store(start)
			gotEnd.Store(end)
			chunk := full[start : end+1]
			info := origin.ObjectInfo{
				Size:         int64(len(chunk)),
				ETag:         `"range-etag"`,
				LastModified: lastModified,
				ContentType:  "application/octet-stream",
				ContentRange: fmt.Sprintf("bytes %d-%d/%d", start, end, len(full)),
				StatusCode:   http.StatusPartialContent,
			}
			return origin.NewObject(io.NopCloser(bytes.NewReader(chunk)), info), nil
		},
	}

	st, dir := newCacheStoreDir(t)
	// 预置一份完整块：若 Range 请求走了缓存/合并路径，会返回整块 200 而不是区间 206。
	putCached(t, st, key, full, cache.Metadata{ETag: `"disk-etag"`, LastModified: lastModified})
	before := dirEntries(t, dir)

	s, ob, _ := newCacheServer(t, org, st, true, 1<<20, 4)
	req := httptest.NewRequest(http.MethodGet, "/traincache/ds/blocks/ranged%230", nil)
	req.Header.Set("Range", "bytes=10-19")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("状态 = %d, want 206", rec.Code)
	}
	if got, want := rec.Body.Bytes(), full[10:20]; !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got, want := rec.Header().Get("Content-Range"),
		fmt.Sprintf("bytes 10-19/%d", len(full)); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
	if got, want := rec.Header().Get("Content-Length"), "10"; got != want {
		t.Fatalf("Content-Length = %q, want %q", got, want)
	}
	if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("Accept-Ranges = %q, want bytes", got)
	}
	if got := rec.Header().Get("ETag"); got != `"range-etag"` {
		t.Fatalf("ETag = %q", got)
	}

	// Range 进入源站请求（闭区间语义）。
	if gotStart.Load() != 10 || gotEnd.Load() != 19 {
		t.Fatalf("源站收到区间 = [%d,%d], want [10,19]", gotStart.Load(), gotEnd.Load())
	}

	// 不进在途合并表。
	if got := s.registry.Len(); got != 0 {
		t.Fatalf("在途条目数 = %d, want 0", got)
	}
	if _, ok := s.registry.Attach(key); ok {
		t.Fatal("Range 请求不应登记到在途合并表")
	}

	// 不写盘：缓存目录条目与请求前一致。
	if after := dirEntries(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatalf("缓存目录发生变化: before=%v after=%v", before, after)
	}
	if tmps := tmpFiles(t, dir); len(tmps) != 0 {
		t.Fatalf("缓存目录残留 .tmp: %v", tmps)
	}

	// 计数：一次源站 GET、零缓存命中。
	sum := ob.Snapshot()
	if sum.Gets != 1 || sum.Owners != 1 || sum.CacheHit != 0 {
		t.Fatalf("汇总 = %+v, want gets=1 owners=1 cache_hit=0", sum)
	}
}

// TestBlockRangeOpenEndedResolvesSize 覆盖 bytes=N- 形式：先 HEAD 拿总长度，
// 再把闭区间透传源站。
func TestBlockRangeOpenEndedResolvesSize(t *testing.T) {
	const key = "ds/blocks/open#0"
	full := []byte("0123456789")

	var statCalls, gotStart, gotEnd atomic.Int64
	org := &fakeOrigin{
		statFn: func(context.Context, string) (origin.ObjectInfo, error) {
			statCalls.Add(1)
			return origin.ObjectInfo{Size: int64(len(full))}, nil
		},
		openRangeFn: func(_ context.Context, _ string, start, end int64) (*origin.Object, error) {
			gotStart.Store(start)
			gotEnd.Store(end)
			chunk := full[start : end+1]
			info := origin.ObjectInfo{
				Size:         int64(len(chunk)),
				ContentRange: fmt.Sprintf("bytes %d-%d/%d", start, end, len(full)),
				StatusCode:   http.StatusPartialContent,
			}
			return origin.NewObject(io.NopCloser(bytes.NewReader(chunk)), info), nil
		},
	}
	s, _ := newMergeServer(t, org, 1<<20, 4)

	req := httptest.NewRequest(http.MethodGet, "/traincache/ds/blocks/open%230", nil)
	req.Header.Set("Range", "bytes=7-")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("状态 = %d, want 206", rec.Code)
	}
	if got, want := rec.Body.Bytes(), full[7:]; !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if gotStart.Load() != 7 || gotEnd.Load() != int64(len(full)-1) {
		t.Fatalf("源站收到区间 = [%d,%d], want [7,%d]", gotStart.Load(), gotEnd.Load(), len(full)-1)
	}
	if statCalls.Load() != 1 {
		t.Fatalf("Stat 调用 = %d, want 1", statCalls.Load())
	}
}
