package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/gateway/internal/cache"
	"traincache/gateway/internal/origin"
)

// rangeBody 是片段用例用的对象内容。
const rangeBody = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// rangeOrigin 返回一个按闭区间切片的假源站，并记录收到的区间次数。
func rangeOrigin(t *testing.T, full []byte, hits *atomic.Int64, gate chan struct{}) *fakeOrigin {
	t.Helper()
	org := &fakeOrigin{
		openRangeFn: func(_ context.Context, _ string, start, end int64) (*origin.Object, error) {
			hits.Add(1)
			chunk := full[start : end+1]
			var r io.Reader = bytes.NewReader(chunk)
			if gate != nil {
				r = &gateReader{entered: make(chan struct{}), release: gate, data: chunk}
			}
			info := origin.ObjectInfo{
				Size:         int64(len(chunk)),
				ETag:         `"range-etag"`,
				LastModified: time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC),
				ContentType:  "application/octet-stream",
				ContentRange: fmt.Sprintf("bytes %d-%d/%d", start, end, len(full)),
				StatusCode:   http.StatusPartialContent,
			}
			return origin.NewObject(io.NopCloser(r), info), nil
		},
	}
	return org
}

// TestBlockRangeMergesAndCaches 覆盖带 Range 的块 GET：按「键+区间」进在途表
// 与磁盘缓存。响应是 206 并带区间头；这一段落进盘上的片段缓存；第二次请求
// 从盘上读，不再回源。
func TestBlockRangeMergesAndCaches(t *testing.T) {
	const key = "ds/blocks/ranged#0"
	full := []byte(rangeBody)

	var hits atomic.Int64
	org := rangeOrigin(t, full, &hits, nil)

	st, dir := newCacheStoreDir(t)
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

	// 这一段落进盘上的片段缓存：键是「dataset/frag/原键余下段/区间」。
	fragKey := cache.FragmentKey(key, 10, 19)
	if got, want := fragKey, "ds/frag/blocks/ranged#0/10-19"; got != want {
		t.Fatalf("片段键 = %q, want %q", got, want)
	}
	entries := dirEntries(t, dir)
	sort.Strings(entries)
	want := []string{"10-19", "10-19.meta"}
	if len(entries) != len(want) {
		t.Fatalf("缓存目录 = %v, want %v", entries, want)
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Fatalf("缓存目录 = %v, want %v", entries, want)
		}
	}
	if tmps := tmpFiles(t, dir); len(tmps) != 0 {
		t.Fatalf("缓存目录残留 .tmp: %v", tmps)
	}

	// 第二次同一区间：读盘命中，不再回源。
	req2 := httptest.NewRequest(http.MethodGet, "/traincache/ds/blocks/ranged%230", nil)
	req2.Header.Set("Range", "bytes=10-19")
	rec2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusPartialContent {
		t.Fatalf("第二次状态 = %d, want 206", rec2.Code)
	}
	if got, want := rec2.Body.Bytes(), full[10:20]; !bytes.Equal(got, want) {
		t.Fatalf("第二次 body = %q, want %q", got, want)
	}
	if got, want := rec2.Header().Get("Content-Range"),
		fmt.Sprintf("bytes 10-19/%d", len(full)); got != want {
		t.Fatalf("第二次 Content-Range = %q, want %q", got, want)
	}
	if hits.Load() != 1 {
		t.Fatalf("源站 GET = %d, want 1", hits.Load())
	}

	sum := ob.Snapshot()
	if sum.Gets != 2 || sum.Owners != 1 || sum.Merged != 0 || sum.CacheHit != 1 {
		t.Fatalf("汇总 = %+v, want gets=2 owners=1 merged=0 cache_hit=1", sum)
	}
}

// TestBlockRangeConcurrentSameSegmentMerges 覆盖同一区间的并发请求合并成一次
// 回源：后到的那个挂到先到的条目上，源站只收到一次 GET。
func TestBlockRangeConcurrentSameSegmentMerges(t *testing.T) {
	const key = "ds/blocks/ranged#0"
	full := []byte(rangeBody)

	var hits atomic.Int64
	gate := make(chan struct{})
	org := rangeOrigin(t, full, &hits, gate)

	s, ob, _ := newCacheServer(t, org, nil, false, 1<<20, 4)

	var wg sync.WaitGroup
	codes := make([]int, 2)
	bodies := make([][]byte, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/traincache/ds/blocks/ranged%230", nil)
			req.Header.Set("Range", "bytes=10-19")
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			codes[i] = rec.Code
			bodies[i] = rec.Body.Bytes()
		}(i)
	}
	// 第一个请求进到源站读里就卡住，第二个请求这时挂到同一条目上。
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()

	for i := range codes {
		if codes[i] != http.StatusPartialContent {
			t.Fatalf("请求 %d 状态 = %d, want 206", i, codes[i])
		}
		if !bytes.Equal(bodies[i], full[10:20]) {
			t.Fatalf("请求 %d body = %q, want %q", i, bodies[i], full[10:20])
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("源站 GET = %d, want 1", hits.Load())
	}

	sum := ob.Snapshot()
	if sum.Gets != 2 || sum.Owners != 1 || sum.Merged != 1 {
		t.Fatalf("汇总 = %+v, want gets=2 owners=1 merged=1", sum)
	}
}

// TestBlockRangeOpenEndedResolvesSize 覆盖 bytes=N- 形式：先 HEAD 拿总长度，
// 再把闭区间回源，响应仍是 206。
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
	if got, want := rec.Header().Get("Content-Range"),
		fmt.Sprintf("bytes 7-9/%d", len(full)); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
	if gotStart.Load() != 7 || gotEnd.Load() != int64(len(full)-1) {
		t.Fatalf("源站收到区间 = [%d,%d], want [7,%d]", gotStart.Load(), gotEnd.Load(), len(full)-1)
	}
	if statCalls.Load() != 1 {
		t.Fatalf("Stat 调用 = %d, want 1", statCalls.Load())
	}
}

// TestBlockRangeInvalidRange 覆盖 Range 语法非法：回 416，源站一次都不该收到。
func TestBlockRangeInvalidRange(t *testing.T) {
	var hits atomic.Int64
	org := rangeOrigin(t, []byte(rangeBody), &hits, nil)
	s, _ := newMergeServer(t, org, 1<<20, 4)

	req := httptest.NewRequest(http.MethodGet, "/traincache/ds/blocks/ranged%230", nil)
	req.Header.Set("Range", "bytes=abc")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("状态 = %d, want 416", rec.Code)
	}
	if hits.Load() != 0 {
		t.Fatalf("源站 GET = %d, want 0", hits.Load())
	}
}
