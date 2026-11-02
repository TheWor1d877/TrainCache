package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"traincache/gateway/internal/origin"
)

// fakeOrigin 是本地假源站；未设置的函数返回错误，避免用例意外访问网络。
type fakeOrigin struct {
	statFn func(context.Context, string) (origin.ObjectInfo, error)
	openFn func(context.Context, string) (io.ReadCloser, error)
	// openInfo 是 Open 之后 Stat 返回的元数据；合并路径会用它回填响应头。
	openInfo origin.ObjectInfo
}

func (f *fakeOrigin) Stat(ctx context.Context, key string) (origin.ObjectInfo, error) {
	if f.statFn == nil {
		return origin.ObjectInfo{}, errors.New("fake origin: Stat 未设置")
	}
	return f.statFn(ctx, key)
}

func (f *fakeOrigin) Open(ctx context.Context, key string) (*origin.Object, error) {
	if f.openFn == nil {
		return nil, errors.New("fake origin: Open 未设置")
	}
	rc, err := f.openFn(ctx, key)
	if err != nil {
		return nil, err
	}
	return origin.NewObject(rc, f.openInfo), nil
}

// newTestServer 构造使用内存日志的网关服务。
func newTestServer(org Origin) *Server {
	return New(org, Options{Bucket: "traincache", Concurrency: 32, BufferBytes: 1 << 20},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// serve 直接调用 handler，返回记录器（不经过真实监听）。
func serve(s *Server, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestHealthz(t *testing.T) {
	s := newTestServer(&fakeOrigin{})

	rec := serve(s, http.MethodGet, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("body = %q, want ok", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q", ct)
	}
}

func TestReadyzReady(t *testing.T) {
	s := newTestServer(&fakeOrigin{})
	s.SetReady(true)

	rec := serve(s, http.MethodGet, "/readyz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("body = %q, want ok", rec.Body.String())
	}
}

func TestMethodNotAllowed(t *testing.T) {
	s := newTestServer(&fakeOrigin{})

	for _, tc := range []struct{ method, target string }{
		{http.MethodPut, "/traincache/ds/blocks/a%230"},
		{http.MethodDelete, "/traincache/ds/CURRENT"},
		{http.MethodPost, "/healthz"},
	} {
		t.Run(tc.method+tc.target, func(t *testing.T) {
			rec := serve(s, tc.method, tc.target)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405", rec.Code)
			}
			if got := errorCode(t, rec.Body.Bytes()); got != codeMethodNotAllowed {
				t.Fatalf("Code = %q, want %q", got, codeMethodNotAllowed)
			}
			if got := rec.Header().Get(GatewayHeaderName); got != "" {
				t.Fatalf("%s = %q, want empty", GatewayHeaderName, got)
			}
		})
	}
}

func TestWrongBucketReturns404(t *testing.T) {
	called := false
	s := newTestServer(&fakeOrigin{
		statFn: func(context.Context, string) (origin.ObjectInfo, error) {
			called = true
			return origin.ObjectInfo{}, errors.New("不应访问源站")
		},
	})

	rec := serve(s, http.MethodGet, "/other/ds/CURRENT")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if got := errorCode(t, rec.Body.Bytes()); got != codeNoSuchBucket {
		t.Fatalf("Code = %q, want %q", got, codeNoSuchBucket)
	}
	if called {
		t.Fatal("桶名不符时不应访问源站")
	}
}

func TestHeadPassthrough(t *testing.T) {
	lastModified := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	var gotKey string
	s := newTestServer(&fakeOrigin{
		statFn: func(_ context.Context, key string) (origin.ObjectInfo, error) {
			gotKey = key
			return origin.ObjectInfo{
				Size:         5,
				ETag:         "etag-1",
				LastModified: lastModified,
				ContentType:  "application/octet-stream",
			}, nil
		},
	})

	rec := serve(s, http.MethodHead, "/traincache/ds/blocks/a%230")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotKey != "ds/blocks/a#0" {
		t.Fatalf("key = %q, want ds/blocks/a#0", gotKey)
	}
	if got := rec.Header().Get("Content-Length"); got != "5" {
		t.Fatalf("Content-Length = %q, want 5", got)
	}
	if got := rec.Header().Get("ETag"); got != "etag-1" {
		t.Fatalf("ETag = %q, want etag-1", got)
	}
	if got := rec.Header().Get("Last-Modified"); got != lastModified.Format(http.TimeFormat) {
		t.Fatalf("Last-Modified = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q", got)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD body = %q, want empty", rec.Body.String())
	}
}

func TestIsBlockKey(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"ds/blocks/a#0", true},
		{"ds/blocks/sub/a#12", true},
		{"ds/CURRENT", false},
		{"ds/manifest", false},
		{"ds/snapshots/x#0", false},
		{"blocks/a#0", false},
		{"/ds/blocks/a#0", false},
		{"ds/blocksish/a#0", false},
	} {
		if got := isBlockKey(tc.key); got != tc.want {
			t.Errorf("isBlockKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestCurrentPassthrough(t *testing.T) {
	const body = "snapshot-v1"
	var gotKey string
	s := newTestServer(&fakeOrigin{
		statFn: func(_ context.Context, key string) (origin.ObjectInfo, error) {
			gotKey = key
			return origin.ObjectInfo{Size: int64(len(body)), ETag: "etag-current"}, nil
		},
		openFn: func(_ context.Context, key string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(body)), nil
		},
	})

	rec := serve(s, http.MethodGet, "/traincache/ds/CURRENT")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotKey != "ds/CURRENT" {
		t.Fatalf("key = %q, want ds/CURRENT", gotKey)
	}
	if rec.Body.String() != body {
		t.Fatalf("body = %q, want %q", rec.Body.String(), body)
	}
	if got := rec.Header().Get("Content-Length"); got != "11" {
		t.Fatalf("Content-Length = %q, want 11", got)
	}
}
