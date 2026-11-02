package server

// integration_test.go：真实 minio-go 客户端 → 网关 HTTP → httptest 假源站 的
// 端到端合并用例（docs/10.3 test.md §4）。与 block_test.go 不同，这里不直接
// 调 handler，也不注入假 Origin：网关持有真实的 minio-go 源站客户端，客户端侧
// 也走完整 HTTP/SigV4。

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
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

const (
	// integrationBucket 是网关与客户端共用的单桶名。
	integrationBucket = "traincache"
	// integrationKey 是被 5 个并发客户端同时读取的块键。
	integrationKey = "ds/blocks/x#0"
)

// minimalS3Origin 是最小的假 S3 源站：只处理对象 GET/HEAD 与桶位置查询。
// GET 在放行前阻塞，便于测试等 5 个订阅者全部挂到同一合并条目上。
type minimalS3Origin struct {
	body         []byte
	lastModified time.Time

	// gets/heads 分别统计对象 GET 与对象 HEAD；桶位置查询不计入。
	gets  atomic.Int64
	heads atomic.Int64

	entered  chan struct{}
	release  chan struct{}
	gateOnce sync.Once
	openOnce sync.Once
}

// openGate 幂等地放行源站 GET，避免测试失败时把 httptest.Server.Close 卡住。
func (o *minimalS3Origin) openGate() {
	o.openOnce.Do(func() { close(o.release) })
}

// setHeaders 回填对象头；HEAD 与 GET 使用同一组值。
func (o *minimalS3Origin) setHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Length", strconv.Itoa(len(o.body)))
	w.Header().Set("ETag", `"integration-etag"`)
	w.Header().Set("Last-Modified", o.lastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
}

// ServeHTTP 实现假源站：位置查询、对象 GET/HEAD、以及不存在 key 的 404。
func (o *minimalS3Origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// minio-go 未绑定区域时会先查桶位置；这不是对象 GET，不计入 GET 次数。
	if r.URL.Query().Has("location") {
		writeIntegrationXML(w, http.StatusOK, `<LocationConstraint>us-east-1</LocationConstraint>`)
		return
	}
	if r.URL.Path != "/"+integrationBucket+"/"+integrationKey {
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
		o.gateOnce.Do(func() { close(o.entered) })
		<-o.release
		o.setHeaders(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(o.body)
	default:
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// writeIntegrationXML 输出固定的 XML 响应体并显式带上 Content-Length。
func writeIntegrationXML(w http.ResponseWriter, status int, body string) {
	b := []byte(body)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// headerRecorder 记录客户端收到的对象 GET 响应头，用于断言网关回填的元数据。
type headerRecorder struct {
	base http.RoundTripper

	mu      sync.Mutex
	headers []http.Header
}

func (h *headerRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := h.base.RoundTrip(req)
	if err == nil && req.Method == http.MethodGet &&
		req.URL.Path == "/"+integrationBucket+"/"+integrationKey {
		h.mu.Lock()
		h.headers = append(h.headers, resp.Header.Clone())
		h.mu.Unlock()
	}
	return resp, err
}

// recorded 返回已记录响应头的副本。
func (h *headerRecorder) recorded() []http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]http.Header(nil), h.headers...)
}

// TestIntegrationMinioClientMergesBlock 覆盖真实客户端经网关读取同一块键：
// 5 个并发请求只让源站收到 1 次 GET，HEAD 由每个客户端的 Stat 直通为 5 次。
func TestIntegrationMinioClientMergesBlock(t *testing.T) {
	const (
		clients     = 5
		bufferBytes = 2 << 20
	)

	// 固定 seed 生成 1MiB 内容，保证每次运行字节完全一致。
	body := make([]byte, 1<<20)
	_, _ = rand.New(rand.NewSource(42)).Read(body)

	src := &minimalS3Origin{
		body:         body,
		lastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	srcSrv := httptest.NewServer(src)
	defer srcSrv.Close()

	ob := obs.New(time.Hour, discardLogger())
	t.Cleanup(ob.Stop)

	org, err := origin.New(strings.TrimPrefix(srcSrv.URL, "http://"),
		"ak", "sk", integrationBucket, false)
	if err != nil {
		t.Fatalf("origin.New: %v", err)
	}

	s := New(org, Options{
		Bucket:      integrationBucket,
		Concurrency: 8,
		BufferBytes: bufferBytes,
		Registry:    merge.NewRegistry(bufferBytes),
		Scheduler:   sched.New(8),
		Obs:         ob,
	}, discardLogger())
	gwSrv := httptest.NewServer(s.Handler())
	defer gwSrv.Close()

	recorder := &headerRecorder{base: http.DefaultTransport}
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

	var wg sync.WaitGroup
	// 失败路径也要先放行源站并等客户端退出，避免连接一直挂着。
	defer func() {
		src.openGate()
		wg.Wait()
	}()

	bodies := make([][]byte, clients)
	errs := make([]error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			obj, err := cli.GetObject(context.Background(), integrationBucket, integrationKey,
				minio.GetObjectOptions{})
			if err != nil {
				errs[i] = fmt.Errorf("GetObject: %w", err)
				return
			}
			defer obj.Close()
			// Stat 走 HEAD 直通；ReadAll 的 GET 才进入合并路径。
			if _, err := obj.Stat(); err != nil {
				errs[i] = fmt.Errorf("Stat: %w", err)
				return
			}
			data, err := io.ReadAll(obj)
			if err != nil {
				errs[i] = fmt.Errorf("ReadAll: %w", err)
				return
			}
			bodies[i] = data
		}(i)
	}

	select {
	case <-src.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("源站未收到块 GET")
	}

	// 等 5 个请求都挂到同一条目上再放行源站（与 block_test.go 的等待手法一致）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		if e, ok := s.registry.Attach(integrationKey); ok && e.Subscribers() >= clients {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("合并条目的订阅者未达到 %d 个", clients)
		}
		time.Sleep(time.Millisecond)
	}

	src.openGate()
	wg.Wait()

	// 先断言源站与汇总计数，保证失败时也能看到链路实际打出的请求数。
	if got := src.gets.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1", got)
	}
	if got := src.heads.Load(); got != clients {
		t.Fatalf("源站 HEAD 次数 = %d, want %d", got, clients)
	}

	sum := ob.Snapshot()
	t.Logf("源站 GET=%d HEAD=%d；obs=%+v", src.gets.Load(), src.heads.Load(), sum)
	if sum.Gets != clients || sum.Owners != 1 || sum.Merged != clients-1 {
		t.Fatalf("汇总 = %+v, want gets=%d owners=1 merged=%d", sum, clients, clients-1)
	}

	for i := 0; i < clients; i++ {
		// minio-go 会对每个 GET 响应调用 ToObjectInfo，后者强制解析
		// Last-Modified；网关必须回填该头，否则这里会报 InternalError（10.2 D28）。
		if errs[i] != nil {
			t.Fatalf("客户端 %d 失败: %v", i, errs[i])
		}
		if !bytes.Equal(bodies[i], body) {
			t.Fatalf("客户端 %d 内容与源站不一致: len=%d, want %d", i, len(bodies[i]), len(body))
		}
	}
	if sum.Passthrough != clients {
		t.Fatalf("Passthrough = %d, want %d（每次 Stat 都是 HEAD 直通）", sum.Passthrough, clients)
	}

	// 合并路径的 200 必须回填 Last-Modified 与 ETag，且不写 Content-Length。
	got := recorder.recorded()
	if len(got) != clients {
		t.Fatalf("记录到的对象 GET 响应 = %d, want %d", len(got), clients)
	}
	wantLastModified := src.lastModified.UTC().Format(http.TimeFormat)
	for i, h := range got {
		if v := h.Get("Last-Modified"); v != wantLastModified {
			t.Fatalf("响应 %d Last-Modified = %q, want %q", i, v, wantLastModified)
		}
		if v := h.Get("ETag"); v != `"integration-etag"` {
			t.Fatalf("响应 %d ETag = %q", i, v)
		}
		if v := h.Get("Content-Length"); v != "" {
			t.Fatalf("响应 %d Content-Length = %q, want 空（chunked）", i, v)
		}
	}
	t.Logf("合并响应头: Last-Modified=%q ETag=%q（%d 个响应）",
		got[0].Get("Last-Modified"), got[0].Get("ETag"), len(got))
}
