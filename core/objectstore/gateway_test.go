package objectstore

// gateway_test.go：网关错误分类的契约用例（10.1 §4.4 / 10.3）。
// 全部用 httptest 假源站，不连真实 S3；minio-go 的 5xx 重试在 TestMain 降到 1 次，
// 只影响本包测试耗时。

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// TestMain 收紧 minio-go 的重试次数：默认 10 次指数退避会让 5xx 用例显著变慢。
// 本包用例都断言单次请求的返回语义，1 次尝试足够。
func TestMain(m *testing.M) {
	minio.MaxRetry = 1
	os.Exit(m.Run())
}

// 编译期断言：NewS3Store 的老签名仍然可用（10.1 §1.5）。
var _ func(endpoint, accessKey, secretKey, bucket string, secure bool) (*S3Store, error) = NewS3Store

// fakeS3Server 启动一个假 S3 源站：HEAD（存在性探测）恒 200，GET 固定返回
// status/body；header 为 true 时附带 x-traincache-gateway: unavailable。
// 这样错误分类走 GET 响应体，与真实网关「HEAD 直通、GET 走合并路径」一致。
func fakeS3Server(t *testing.T, status int, header bool, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "5")
			w.Header().Set("ETag", `"etag"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			return
		}
		if header {
			w.Header().Set(GatewayHeaderName, GatewayHeaderUnavailable)
		}
		if strings.HasPrefix(strings.TrimSpace(body), "<") {
			w.Header().Set("Content-Type", "application/xml")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// endpointOf 去掉 httptest URL 的 scheme，得到 minio 需要的 host:port。
func endpointOf(srv *httptest.Server) string {
	return strings.TrimPrefix(strings.TrimPrefix(srv.URL, "http://"), "https://")
}

// newGatewayStore 构造带探针的网关 Store；固定 Region 避免 bucket location 请求（D17）。
func newGatewayStore(t *testing.T, srv *httptest.Server) *S3Store {
	t.Helper()
	s, err := NewS3StoreWithOptions(endpointOf(srv), "ak", "sk", "traincache", false, Options{Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewS3StoreWithOptions: %v", err)
	}
	return s
}

// newOriginStore 构造不装探针的源站 Store。
func newOriginStore(t *testing.T, srv *httptest.Server) *S3Store {
	t.Helper()
	s, err := NewS3Store(endpointOf(srv), "ak", "sk", "traincache", false)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	return s
}

// TestGatewayHeaderUnavailableWithInvalidBody：503 + 响应头，响应体不是合法 S3 XML，
// 仍应判为 ErrGatewayUnavailable（响应头优先于 Code）。
func TestGatewayHeaderUnavailableWithInvalidBody(t *testing.T) {
	srv := fakeS3Server(t, http.StatusServiceUnavailable, true, "not-an-s3-xml")
	s := newGatewayStore(t, srv)

	err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if !errors.Is(err, ErrGatewayUnavailable) {
		t.Fatalf("Get = %v，期望 ErrGatewayUnavailable", err)
	}
	if IsGatewayTransport(err) {
		t.Error("IsGatewayTransport = true，want false（响应头命中优先）")
	}
}

// TestGatewayCodeUnavailableWithoutHeader：503 + 合法 XML Code，无响应头，同样判为
// ErrGatewayUnavailable。
func TestGatewayCodeUnavailableWithoutHeader(t *testing.T) {
	body := `<Error><Code>` + GatewayCodeUnavailable + `</Code><Message>gateway origin request failed</Message></Error>`
	srv := fakeS3Server(t, http.StatusServiceUnavailable, false, body)
	s := newGatewayStore(t, srv)

	err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if !errors.Is(err, ErrGatewayUnavailable) {
		t.Fatalf("Get = %v，期望 ErrGatewayUnavailable", err)
	}
}

// TestGatewayHeaderAndCode：响应头与错误码同时命中，结果仍是 ErrGatewayUnavailable。
func TestGatewayHeaderAndCode(t *testing.T) {
	body := `<Error><Code>` + GatewayCodeUnavailable + `</Code><Message>gateway origin request failed</Message></Error>`
	srv := fakeS3Server(t, http.StatusServiceUnavailable, true, body)
	s := newGatewayStore(t, srv)

	err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if !errors.Is(err, ErrGatewayUnavailable) {
		t.Fatalf("Get = %v，期望 ErrGatewayUnavailable", err)
	}
}

// TestGatewayCodeSlowDown：503 + Code=SlowDown → ErrGatewayRejected。
func TestGatewayCodeSlowDown(t *testing.T) {
	body := `<Error><Code>` + GatewayCodeSlowDown + `</Code><Message>queue full</Message></Error>`
	srv := fakeS3Server(t, http.StatusServiceUnavailable, false, body)
	s := newGatewayStore(t, srv)

	err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if !errors.Is(err, ErrGatewayRejected) {
		t.Fatalf("Get = %v，期望 ErrGatewayRejected", err)
	}
	if errors.Is(err, ErrGatewayUnavailable) {
		t.Error("SlowDown 不应判为 ErrGatewayUnavailable")
	}
}

// TestGetNoSuchKey：404 NoSuchKey → ErrNotFound（既有语义）。
func TestGetNoSuchKey(t *testing.T) {
	body := `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`
	srv := fakeS3Server(t, http.StatusNotFound, false, body)
	s := newGatewayStore(t, srv)

	err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v，期望 ErrNotFound", err)
	}
}

// TestOriginHTTP500Passthrough：源站 500 带自定义 Code（无网关响应头）时原样返回，
// 不判为网关失败，也不类型化为连接层失败。
func TestOriginHTTP500Passthrough(t *testing.T) {
	body := `<Error><Code>SomethingCustom</Code><Message>boom</Message></Error>`
	srv := fakeS3Server(t, http.StatusInternalServerError, false, body)
	s := newOriginStore(t, srv)

	err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if err == nil {
		t.Fatal("Get = nil，期望源站错误")
	}
	if errors.Is(err, ErrGatewayUnavailable) || errors.Is(err, ErrGatewayRejected) {
		t.Fatalf("源站 500 被判为网关错误: %v", err)
	}
	if IsGatewayTransport(err) {
		t.Fatalf("源站 500 被判为连接层失败: %v", err)
	}
	if got := minio.ToErrorResponse(err).Code; got != "SomethingCustom" {
		t.Errorf("Code = %q，期望原样透传 SomethingCustom", got)
	}
}

// TestGatewayConnectFailure：端口监听后立即关闭，Get 应返回可识别的连接层失败。
func TestGatewayConnectFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	endpoint := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	s, err := NewS3StoreWithOptions(endpoint, "ak", "sk", "traincache", false, Options{Region: "us-east-1"})
	if err != nil {
		t.Fatalf("NewS3StoreWithOptions: %v", err)
	}
	err = s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if err == nil {
		t.Fatal("Get = nil，期望连接失败")
	}
	if !IsGatewayTransport(err) {
		t.Fatalf("Get = %v，期望 IsGatewayTransport 为 true", err)
	}
	if IsGatewayTimeout(err) {
		t.Error("IsGatewayTimeout = true，want false（连接拒绝不是超时）")
	}
}

// TestOriginStoreHasNoProbe：老构造入口不装探针，源站连接失败保持原样返回。
func TestOriginStoreHasNoProbe(t *testing.T) {
	s, err := NewS3Store("127.0.0.1:1", "ak", "sk", "traincache", false)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	if s.probe {
		t.Fatal("NewS3Store 不应安装网关探针")
	}
}

// TestStreamInterruptedAfterBytes：网关写出部分字节后断流 → 同样是连接层失败，
// 调用方据已写字节决定是否降级（由 backendStore 判断，这里只验证错误类型）。
func TestStreamInterruptedAfterBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			return
		}
		_, _ = io.WriteString(w, "partial")
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // 先让响应头与已写字节到达客户端
		}
		// 提前关闭连接，制造响应体截断。
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}
	}))
	t.Cleanup(srv.Close)

	s := newGatewayStore(t, srv)
	var buf bytes.Buffer
	err := s.Get(context.Background(), "ds/blocks/a#0", &buf)
	if err == nil {
		t.Fatal("Get = nil，期望读取中断错误")
	}
	if !IsGatewayTransport(err) {
		t.Fatalf("Get = %v，期望 IsGatewayTransport 为 true", err)
	}
	if buf.Len() == 0 {
		t.Error("期望已写入部分字节")
	}
}

// TestIsGatewayTimeout：超时判定覆盖 context.DeadlineExceeded 与 net.Error.Timeout。
func TestIsGatewayTimeout(t *testing.T) {
	if !IsGatewayTimeout(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded 应判为超时")
	}
	if !IsGatewayTimeout(&TransportError{Err: context.DeadlineExceeded}) {
		t.Error("TransportError 包裹的 DeadlineExceeded 应判为超时")
	}
	if !IsGatewayTimeout(&TransportError{Err: timeoutErr{}}) {
		t.Error("net.Error.Timeout 应判为超时")
	}
	if IsGatewayTimeout(errors.New("boom")) {
		t.Error("普通错误不应判为超时")
	}
}

// timeoutErr 是仅实现 net.Error 的超时错误。
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

// TestGatewayMaxRetriesOverridesSDKDefault：网关 503 是控制信号（准入拒绝 → 快速回退
// 直连；网关下线 → 快速降级），Options.MaxRetries 必须覆盖 SDK 的全局重试次数。
//
// TestMain 把 minio.MaxRetry 降到 1 会掩盖「默认 10 次」的行为，故本用例内临时把它
// 恢复为 SDK 默认 10：仍断言 GET 次数 <= 2，证明是我们的 Options 配置（而不是全局
// 变量）在起作用。
func TestGatewayMaxRetriesOverridesSDKDefault(t *testing.T) {
	prev := minio.MaxRetry
	minio.MaxRetry = 10 // SDK 默认值；未配置 Options.MaxRetries 时会被放大成 10 次请求
	t.Cleanup(func() { minio.MaxRetry = prev })

	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// HEAD（存在性探测）恒 200，只有 GET 走 503，确保重试发生在 GET 上。
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "5")
			w.Header().Set("ETag", `"etag"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			return
		}
		gets.Add(1)
		w.Header().Set(GatewayHeaderName, GatewayHeaderUnavailable)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `<Error><Code>`+GatewayCodeUnavailable+`</Code><Message>gateway down</Message></Error>`)
	}))
	t.Cleanup(srv.Close)

	s, err := NewS3StoreWithOptions(endpointOf(srv), "ak", "sk", "traincache", false,
		Options{Region: "us-east-1", MaxRetries: 1})
	if err != nil {
		t.Fatalf("NewS3StoreWithOptions: %v", err)
	}

	err = s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if !errors.Is(err, ErrGatewayUnavailable) {
		t.Fatalf("Get = %v，期望 ErrGatewayUnavailable", err)
	}
	n := gets.Load()
	t.Logf("GET 次数 = %d（minio.MaxRetry=10 时仍受 Options.MaxRetries=1 约束）", n)
	if n > 2 {
		t.Fatalf("GET 次数 = %d，期望 <= 2（网关 503 不得被 SDK 放大重试）", n)
	}
}
