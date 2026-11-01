package svc

// gateway_integration_test.go：Assemble 装配后的选择器 + 真实 HTTP 网关的降级与
// 恢复用例（docs/10.3 test.md §4）。网关关停时第一次取数降级直连源站，网关恢复
// 后由探测把模式切回 1；全程不 sleep，用状态机的假时钟推进 nextProbe。

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/core/objectstore"
)

// mockS3LastModified 是假源站返回的固定修改时间。
var mockS3LastModified = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// mockS3Server 把 objectstore.Mock 暴露成最小 S3 源站（GET/HEAD + 桶位置查询），
// 同时供 Assemble 构造的源站 S3Store 与网关的「恢复」分支使用。
type mockS3Server struct {
	bucket string
	store  *objectstore.Mock
}

// ServeHTTP 实现对象 GET/HEAD；GET 写内容，HEAD 写长度与 ETag。
func (s *mockS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// minio-go 未绑定区域时会先查桶位置，返回固定区域即可。
	if r.URL.Query().Has("location") {
		writeMockS3XML(w, http.StatusOK, `<LocationConstraint>us-east-1</LocationConstraint>`)
		return
	}

	key, ok := strings.CutPrefix(r.URL.Path, "/"+s.bucket+"/")
	if !ok || key == "" {
		writeMockS3XML(w, http.StatusNotFound, `<Error><Code>NoSuchKey</Code></Error>`)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var buf bytes.Buffer
	if err := s.store.Get(r.Context(), key, &buf); err != nil {
		writeMockS3XML(w, http.StatusNotFound, `<Error><Code>NoSuchKey</Code></Error>`)
		return
	}
	etag, err := s.store.GetETag(r.Context(), key)
	if err != nil {
		etag = ""
	}

	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	w.Header().Set("Last-Modified", mockS3LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(buf.Bytes())
	}
}

// writeMockS3XML 输出固定的 XML 响应体并显式带上 Content-Length。
func writeMockS3XML(w http.ResponseWriter, status int, body string) {
	b := []byte(body)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// unavailableGateway 返回可控的假网关：down=true 时按网关冻结契约返回
// 503 + x-traincache-gateway: unavailable + XML；down=false 时交给源站等价 HTTP。
func unavailableGateway(down *atomic.Bool, upstream http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !down.Load() {
			upstream.ServeHTTP(w, r)
			return
		}
		body := []byte(`<?xml version="1.0" encoding="UTF-8"?>` +
			`<Error><Code>XTraincacheGatewayUnavailable</Code></Error>`)
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("x-traincache-gateway", "unavailable")
		w.WriteHeader(http.StatusServiceUnavailable)
		if r.Method != http.MethodHead {
			_, _ = w.Write(body)
		}
	})
}

// getBlock 读取一个块并返回完整内容。
func getBlock(t *testing.T, store objectstore.Store, key string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := store.Get(context.Background(), key, &buf); err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return buf.Bytes()
}

// TestGatewayIntegrationFallbackAndRecovery 覆盖 Assemble 后的选择器：网关不可用时
// 同一次 Get 降级直连并计数，探测成功后模式切回 1 且后续取数仍走网关。
func TestGatewayIntegrationFallbackAndRecovery(t *testing.T) {
	const (
		bucket = "traincache"
		key    = "ds/blocks/gw#0"
	)

	// 源站放 3 个固定内容的块；用例只读其中一个，其余用于证明是真实多对象源站。
	contents := map[string][]byte{
		key:              []byte("gateway-integration-block-zero"),
		"ds/blocks/gw#1": []byte("gateway-integration-block-one"),
		"ds/blocks/gw#2": []byte("gateway-integration-block-two"),
	}
	mock := objectstore.NewMock()
	for k, content := range contents {
		if _, err := mock.Put(context.Background(), k, bytes.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("Mock.Put(%q): %v", k, err)
		}
	}

	srcSrv := httptest.NewServer(&mockS3Server{bucket: bucket, store: mock})
	t.Cleanup(srcSrv.Close)

	var down atomic.Bool
	down.Store(true)
	gwSrv := httptest.NewServer(unavailableGateway(&down, &mockS3Server{bucket: bucket, store: mock}))
	t.Cleanup(gwSrv.Close)

	cfg := testAssembleConfig(t)
	cfg.ObjstoreBucket = bucket
	cfg.ObjstoreEndpoint = strings.TrimPrefix(srcSrv.URL, "http://")
	cfg.GatewayEndpoint = strings.TrimPrefix(gwSrv.URL, "http://")
	ag, err := Assemble(cfg)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)

	if got := ag.sourceState.snapshotMode(); got != 1 {
		t.Fatalf("初始 mode = %d, want 1（配置了网关）", got)
	}

	// 1. 网关不可用：首次 Get 应降级直连源站，内容来自 Mock。
	got := getBlock(t, ag.ObjectStore, key)
	if !bytes.Equal(got, contents[key]) {
		t.Fatalf("降级取数内容 = %q, want %q", got, contents[key])
	}
	if mode := ag.sourceState.snapshotMode(); mode != 0 {
		t.Fatalf("降级后 mode = %d, want 0", mode)
	}
	if n := testutil.ToFloat64(ag.Metrics.GatewayFallback().WithLabelValues(
		metrics.GatewayReasonUnavailable)); n != 1 {
		t.Fatalf("gateway_fallback_total{reason=unavailable} = %v, want 1", n)
	}

	// 2. 把假时钟推到 nextProbe 之后并恢复网关：下一次 Get 抢占探测并切回 mode=1。
	ag.sourceState.mu.Lock()
	nextProbe := ag.sourceState.nextProbe
	ag.sourceState.mu.Unlock()
	if nextProbe.IsZero() {
		t.Fatal("降级后 nextProbe 不应为零值")
	}
	probeAt := nextProbe.Add(time.Second)
	ag.sourceState.setClock(func() time.Time { return probeAt })
	down.Store(false)

	got = getBlock(t, ag.ObjectStore, key)
	if !bytes.Equal(got, contents[key]) {
		t.Fatalf("探测取数内容 = %q, want %q", got, contents[key])
	}
	if mode := ag.sourceState.snapshotMode(); mode != 1 {
		t.Fatalf("探测成功后 mode = %d, want 1", mode)
	}

	// 3. 恢复后继续走网关，模式保持 1，内容正确。
	got = getBlock(t, ag.ObjectStore, key)
	if !bytes.Equal(got, contents[key]) {
		t.Fatalf("恢复后取数内容 = %q, want %q", got, contents[key])
	}
	if mode := ag.sourceState.snapshotMode(); mode != 1 {
		t.Fatalf("恢复后再次取数 mode = %d, want 1", mode)
	}
	if n := testutil.ToFloat64(ag.Metrics.GatewayFallback().WithLabelValues(
		metrics.GatewayReasonUnavailable)); n != 1 {
		t.Fatalf("恢复后 gateway_fallback_total{reason=unavailable} = %v, want 1（不应重复计数）", n)
	}

	t.Logf("mode 变化 1→0→1；sourceMode gauge = %v；fallback(unavailable) = %v",
		testutil.ToFloat64(ag.Metrics.SourceMode()),
		testutil.ToFloat64(ag.Metrics.GatewayFallback().WithLabelValues(metrics.GatewayReasonUnavailable)))
}
