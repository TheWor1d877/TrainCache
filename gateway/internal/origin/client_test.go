package origin

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// newFakeClient 用 httptest 假 S3 构造客户端；endpoint 不带 scheme。
func newFakeClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client, err := New(strings.TrimPrefix(srv.URL, "http://"), "ak", "sk", "traincache", false)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return client
}

// fakeS3 是只需要 GET/HEAD 两种对象操作的最小假源站。
func fakeS3(body []byte, lastModified time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// minio-go 在 region 未知时先查桶位置；回 us-east-1 后走缓存。
		if r.URL.Query().Has("location") {
			_, _ = io.WriteString(w, `<LocationConstraint>us-east-1</LocationConstraint>`)
			return
		}
		if r.URL.Path != "/traincache/ds/blocks/a" {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`)
			return
		}
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("ETag", `"abc123"`)
			w.Header().Set("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Type", "application/octet-stream")
		case http.MethodGet:
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("ETag", `"abc123"`)
			w.Header().Set("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(body)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func TestStatReturnsObjectInfo(t *testing.T) {
	lastModified := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	client := newFakeClient(t, fakeS3([]byte("hello"), lastModified))

	info, err := client.Stat(context.Background(), "ds/blocks/a")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size != 5 {
		t.Fatalf("Size = %d, want 5", info.Size)
	}
	if info.ETag != "abc123" {
		t.Fatalf("ETag = %q, want abc123", info.ETag)
	}
	if !info.LastModified.Equal(lastModified) {
		t.Fatalf("LastModified = %v, want %v", info.LastModified, lastModified)
	}
	if info.ContentType != "application/octet-stream" {
		t.Fatalf("ContentType = %q", info.ContentType)
	}
}

func TestOpenReadsWholeObject(t *testing.T) {
	client := newFakeClient(t, fakeS3([]byte("hello"), time.Now()))

	rc, err := client.Open(context.Background(), "ds/blocks/a")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("body = %q, want hello", got)
	}
}

func TestOpenNotFoundKeepsMinioCode(t *testing.T) {
	client := newFakeClient(t, fakeS3(nil, time.Now()))

	rc, err := client.Open(context.Background(), "ds/blocks/missing")
	if err != nil {
		t.Fatalf("Open() error = %v, want lazy success", err)
	}
	defer rc.Close()

	_, err = io.ReadAll(rc)
	if err == nil {
		t.Fatal("首次 Read 应返回 404 错误")
	}
	if code := minio.ToErrorResponse(err).Code; code != "NoSuchKey" {
		t.Fatalf("Code = %q, want NoSuchKey (err=%v)", code, err)
	}
}

func TestOpenUnreachableExposesErrorOnFirstRead(t *testing.T) {
	// 关闭监听后端口必然拒绝连接；把重试降到 1 次，避免测试等满默认退避。
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	oldRetry := minio.MaxRetry
	minio.MaxRetry = 1
	t.Cleanup(func() { minio.MaxRetry = oldRetry })

	client, err := New(addr, "ak", "sk", "traincache", false)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rc, err := client.Open(context.Background(), "ds/blocks/a")
	if err != nil {
		t.Fatalf("Open() error = %v, want lazy success", err)
	}
	defer rc.Close()

	if _, err := rc.Read(make([]byte, 16)); err == nil {
		t.Fatal("首次 Read 应返回连接错误")
	}
}

// TestObjectStatAfterReadReturnsGetHeaders 覆盖合并路径的取元数据方式：
// 首读之后 Stat 返回 GET 响应头里的值，不额外发请求，也不截断后续读取。
func TestObjectStatAfterReadReturnsGetHeaders(t *testing.T) {
	lastModified := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	inner := fakeS3([]byte("hello world"), lastModified)

	var gets atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/traincache/ds/blocks/a" && r.Method == http.MethodGet {
			gets.Add(1)
		}
		inner.ServeHTTP(w, r)
	})
	client := newFakeClient(t, h)

	obj, err := client.Open(context.Background(), "ds/blocks/a")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer obj.Close()

	first := make([]byte, 4)
	n, err := obj.Read(first)
	if err != nil || n != 4 {
		t.Fatalf("首次 Read = (%d, %v), want (4, nil)", n, err)
	}

	info, err := obj.Stat()
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.ETag != `"abc123"` {
		t.Fatalf("ETag = %q, want \"abc123\"（GET 响应头原样）", info.ETag)
	}
	if !info.LastModified.Equal(lastModified) {
		t.Fatalf("LastModified = %v, want %v", info.LastModified, lastModified)
	}

	rest, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got := string(first[:n]) + string(rest); got != "hello world" {
		t.Fatalf("body = %q, want hello world", got)
	}
	if got := gets.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1（Stat 不得额外发请求）", got)
	}
}
