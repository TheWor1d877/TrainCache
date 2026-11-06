package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestMockGetRange 覆盖闭区间语义：中间区间、首区间、末区间（最后一片不足
// 分片大小）、整对象，以及越界与非法区间返回 ErrInvalidRange。
func TestMockGetRange(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	content := []byte("0123456789abcdef") // 16 字节
	if _, err := m.Put(ctx, "k", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cases := []struct {
		name       string
		start, end int64
	}{
		{name: "首字节", start: 0, end: 0},
		{name: "中间区间", start: 3, end: 9},
		{name: "末尾单字节", start: 15, end: 15},
		{name: "最后一片不足分片大小", start: 10, end: 15},
		{name: "整对象", start: 0, end: 15},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := m.GetRange(ctx, "k", tc.start, tc.end, &buf); err != nil {
				t.Fatalf("GetRange(%d,%d): %v", tc.start, tc.end, err)
			}
			want := content[tc.start : tc.end+1]
			if !bytes.Equal(buf.Bytes(), want) {
				t.Fatalf("GetRange(%d,%d) = %q，期望 %q", tc.start, tc.end, buf.Bytes(), want)
			}
		})
	}

	invalid := []struct {
		name       string
		start, end int64
	}{
		{name: "start 为负", start: -1, end: 3},
		{name: "end 小于 start", start: 4, end: 3},
		{name: "end 超出对象长度", start: 10, end: 16},
		{name: "start 超出对象长度", start: 16, end: 16},
		{name: "空对象式越界", start: 0, end: 100},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := m.GetRange(ctx, "k", tc.start, tc.end, &buf); !errors.Is(err, ErrInvalidRange) {
				t.Fatalf("GetRange(%d,%d) 错误 = %v，期望 ErrInvalidRange", tc.start, tc.end, err)
			}
		})
	}

	// 不存在的对象返回 ErrNotFound。
	var buf bytes.Buffer
	if err := m.GetRange(ctx, "missing", 0, 1, &buf); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRange(不存在的 key) 错误 = %v，期望 ErrNotFound", err)
	}

	// ctx 已取消：立即返回取消错误。
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := m.GetRange(canceled, "k", 0, 1, &buf); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetRange(ctx 已取消) 错误 = %v，期望 context.Canceled", err)
	}
}

// rangeS3Server 起一个按 Range 头切片的假 S3：HEAD 恒 200，GET 解析
// bytes=start-end 并回 206 + Content-Range。
func rangeS3Server(t *testing.T, payload []byte, gotRange *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.Header().Set("ETag", `"etag"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			return
		}
		if gotRange != nil {
			*gotRange = r.Header.Get("Range")
		}
		start, end, ok := parseBytesRange(r.Header.Get("Range"), int64(len(payload)))
		if !ok {
			writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
			return
		}
		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(payload)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : end+1])
	}))
	t.Cleanup(srv.Close)
	return srv
}

// parseBytesRange 解析单区间 "bytes=start-end"；越界或格式不识别返回 ok=false。
func parseBytesRange(h string, size int64) (start, end int64, ok bool) {
	h = strings.TrimPrefix(strings.TrimSpace(h), "bytes=")
	a, b, found := strings.Cut(h, "-")
	if !found || a == "" || b == "" {
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(a, 10, 64)
	end, err2 := strconv.ParseInt(b, 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start || end >= size {
		return 0, 0, false
	}
	return start, end, true
}

func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `<Error><Code>`+code+`</Code><Message>range not satisfiable</Message></Error>`)
}

// TestS3GetRange：S3Store.GetRange 发出的 Range 头是闭区间 bytes=start-end，
// 响应体与请求区间一致。
func TestS3GetRange(t *testing.T) {
	payload := []byte("0123456789abcdefghij")
	var gotRange string
	s := newGatewayStore(t, rangeS3Server(t, payload, &gotRange))

	var buf bytes.Buffer
	if err := s.GetRange(context.Background(), "ds/blocks/a#0", 10, 19, &buf); err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if want := "bytes=10-19"; gotRange != want {
		t.Fatalf("Range 头 = %q，期望 %q", gotRange, want)
	}
	if want := payload[10:20]; !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("GetRange 响应 = %q，期望 %q", buf.Bytes(), want)
	}
}

// TestS3GetRangeUnfulfilled：源站未按请求区间返回足够字节时，统一报
// ErrInvalidRange，避免把短块当成完整块写盘。
func TestS3GetRangeUnfulfilled(t *testing.T) {
	payload := []byte("0123456789")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		// 忽略 Range 头，按整对象 200 返回。
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)
	s := newGatewayStore(t, srv)

	err := s.GetRange(context.Background(), "ds/blocks/a#0", 0, 4, io.Discard)
	if !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("GetRange(源站未按区间返回) = %v，期望 ErrInvalidRange", err)
	}
}

// TestS3GetRange416：源站回 416 InvalidRange 时映射为 ErrInvalidRange。
func TestS3GetRange416(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
			return
		}
		writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
	}))
	t.Cleanup(srv.Close)
	s := newGatewayStore(t, srv)

	err := s.GetRange(context.Background(), "ds/blocks/a#0", 0, 4, io.Discard)
	if !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("GetRange(416) = %v，期望 ErrInvalidRange", err)
	}
}
