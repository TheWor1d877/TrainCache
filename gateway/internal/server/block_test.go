package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"traincache/gateway/internal/merge"
	"traincache/gateway/internal/obs"
	"traincache/gateway/internal/origin"
	"traincache/gateway/internal/sched"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newMergeServer 构造带真实 registry/sched/obs 的测试服务。
func newMergeServer(t *testing.T, org Origin, bufBytes, limit int) (*Server, *obs.Logger) {
	t.Helper()
	ob := obs.New(time.Hour, discardLogger())
	t.Cleanup(ob.Stop)
	return New(org, Options{
		Bucket:      "traincache",
		Concurrency: limit,
		BufferBytes: bufBytes,
		Registry:    merge.NewRegistry(bufBytes),
		Scheduler:   sched.New(limit),
		Obs:         ob,
	}, discardLogger()), ob
}

// gateReader 首次 Read 通知 entered，然后阻塞到 release，再返回 data。
type gateReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	data    []byte
	off     int
}

func (g *gateReader) Read(p []byte) (int, error) {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	if g.off >= len(g.data) {
		return 0, io.EOF
	}
	n := copy(p, g.data[g.off:])
	g.off += n
	return n, nil
}

func (g *gateReader) Close() error { return nil }

// TestFiveConcurrentGetsMerge 覆盖 5 个并发 GET 同一 key 只回源一次。
func TestFiveConcurrentGetsMerge(t *testing.T) {
	const (
		body   = "merged-block-body"
		key    = "ds/blocks/a#0"
		target = "/traincache/ds/blocks/a%230"
	)
	var opens atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})

	org := &fakeOrigin{
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			opens.Add(1)
			return &gateReader{entered: entered, release: release, data: []byte(body)}, nil
		},
	}
	s, ob := newMergeServer(t, org, 1<<20, 8)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	const n = 5
	bodies := make([][]byte, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := ts.Client().Get(ts.URL + target)
			if err != nil {
				errs[i] = err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs[i] = fmt.Errorf("status %d", resp.StatusCode)
				return
			}
			bodies[i], errs[i] = io.ReadAll(resp.Body)
		}(i)
	}

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("源站未被打开")
	}
	// 等 5 个请求都挂到同一条目上再放行源站。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if e, ok := s.registry.Attach(key); ok && e.Subscribers() >= n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("订阅者未达到 %d 个", n)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()

	if got := opens.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("请求 %d 失败: %v", i, errs[i])
		}
		if string(bodies[i]) != body {
			t.Fatalf("请求 %d body = %q, want %q", i, bodies[i], body)
		}
	}
	sum := ob.Snapshot()
	if sum.Gets != n || sum.Owners != 1 || sum.Merged != n-1 {
		t.Fatalf("汇总 = %+v, want gets=%d owners=1 merged=%d", sum, n, n-1)
	}
}

// TestMergedResponseCarriesMetadata 覆盖合并路径的 200 回填 Last-Modified 与
// ETag（10.2 D28）：首读后从源站 GET 响应头取，且不写 Content-Length。
func TestMergedResponseCarriesMetadata(t *testing.T) {
	const target = "/traincache/ds/blocks/meta%230"
	lastModified := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	body := []byte("metadata-body")

	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"etag-1"`, LastModified: lastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		},
	}
	s, _ := newMergeServer(t, org, 1<<20, 4)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + target)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
	if v := resp.Header.Get("Last-Modified"); v != lastModified.Format(http.TimeFormat) {
		t.Fatalf("Last-Modified = %q, want %q", v, lastModified.Format(http.TimeFormat))
	}
	if v := resp.Header.Get("ETag"); v != `"etag-1"` {
		t.Fatalf("ETag = %q", v)
	}
	if v := resp.Header.Get("Content-Length"); v != "" {
		t.Fatalf("Content-Length = %q, want 空（chunked）", v)
	}
}

// TestLateRequestFeachesIndependently 覆盖 written>C 后到达的请求独立回源。
func TestLateRequestFeachesIndependently(t *testing.T) {
	const c = 8
	key := "ds/blocks/late#0"
	secondBody := []byte("independent")
	lateLastModified := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)

	var opens atomic.Int64
	org := &fakeOrigin{
		openInfo: origin.ObjectInfo{ETag: `"late-etag"`, LastModified: lateLastModified},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(bytes.NewReader(secondBody)), nil
		},
	}
	s, _ := newMergeServer(t, org, c, 4)

	// 直接造一个已经在途、但 written > C 的条目，模拟迟到场景：拉取协程
	// 写入 C+1 字节后阻塞，条目仍在登记表中。
	e := s.registry.Create(key, s.registry.GetBuffer())
	defer e.Finish(nil)
	hold := make(chan struct{})
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		_ = e.Pump(context.Background(), &blockAfterReader{data: make([]byte, c+1), hold: hold})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for e.Written() <= c {
		if time.Now().After(deadline) {
			t.Fatal("条目未越过合并窗口")
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok := s.registry.Attach(key); ok {
		t.Fatal("条目应已越过合并窗口")
	}

	rec := serve(s, http.MethodGet, "/traincache/"+key)
	if rec.Code != http.StatusOK {
		t.Fatalf("迟到请求状态 = %d", rec.Code)
	}
	if got := rec.Body.String(); got != string(secondBody) {
		t.Fatalf("迟到请求 body = %q, want %q", got, secondBody)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1", got)
	}
	if v := rec.Header().Get("ETag"); v != `"late-etag"` {
		t.Fatalf("独立回源 ETag = %q", v)
	}
	if v := rec.Header().Get("Last-Modified"); v != lateLastModified.Format(http.TimeFormat) {
		t.Fatalf("独立回源 Last-Modified = %q, want %q", v, lateLastModified.Format(http.TimeFormat))
	}

	close(hold)
	select {
	case <-pumpDone:
	case <-time.After(2 * time.Second):
		t.Fatal("拉取协程未结束")
	}
}

// blockAfterReader 返回一次数据，随后阻塞到 hold 关闭再返回 EOF。
type blockAfterReader struct {
	data []byte
	hold chan struct{}
	off  int
}

func (r *blockAfterReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	<-r.hold
	return 0, io.EOF
}

// TestAdmissionRejected 覆盖准入拒绝：503 + SlowDown，且不带网关响应头。
func TestAdmissionRejected(t *testing.T) {
	s, ob := newMergeServer(t, &fakeOrigin{}, 1024, 1)

	// 预先占掉唯一的槽，随后块 GET 必排队并超时。
	rel, err := s.scheduler.Acquire(context.Background(), "ds")
	if err != nil {
		t.Fatalf("占槽: %v", err)
	}
	defer rel()

	rec := serve(s, http.MethodGet, "/traincache/ds/blocks/a%230")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get(GatewayHeaderName); got != "" {
		t.Fatalf("%s = %q, want empty", GatewayHeaderName, got)
	}
	if got := errorCode(t, rec.Body.Bytes()); got != GatewayCodeSlowDown {
		t.Fatalf("Code = %q, want %q", got, GatewayCodeSlowDown)
	}
	if got := ob.Snapshot().Rejected; got != 1 {
		t.Fatalf("Rejected = %d, want 1", got)
	}
}

// gatedStreamReader 先返回 first 字节并阻塞，收到 release 后快速输出 rest。
type gatedStreamReader struct {
	first   []byte
	rest    []byte
	release chan struct{}
	stage   int
	restOff int
}

func (r *gatedStreamReader) Read(p []byte) (int, error) {
	switch r.stage {
	case 0:
		n := copy(p, r.first)
		r.first = r.first[n:]
		if len(r.first) == 0 {
			r.stage = 1
		}
		return n, nil
	case 1:
		<-r.release
		r.stage = 2
		fallthrough
	default:
		if r.restOff >= len(r.rest) {
			return 0, io.EOF
		}
		n := copy(p, r.rest[r.restOff:])
		r.restOff += n
		return n, nil
	}
}

func (r *gatedStreamReader) Close() error { return nil }

// TestKickedConnectionAborted 覆盖慢订阅者被踢时连接被截断。
func TestKickedConnectionAborted(t *testing.T) {
	const c = 4096
	firstChunk := bytes.Repeat([]byte("h"), 2048)
	rest := bytes.Repeat([]byte("x"), 8<<20)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	org := &fakeOrigin{
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			return &gatedStreamReader{first: firstChunk, rest: rest, release: release}, nil
		},
	}
	s, _ := newMergeServer(t, org, c, 4)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/traincache/ds/blocks/kick%230")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	// 先完整读走第一段，保证响应已经开始。
	if _, err := io.ReadFull(resp.Body, make([]byte, len(firstChunk))); err != nil {
		t.Fatalf("读第一段: %v", err)
	}
	close(release)

	// 之后慢速读；拉取协程会迅速超过缓冲容量，订阅者被踢、连接断开。
	total := len(firstChunk)
	buf := make([]byte, 128)
	deadline := time.Now().Add(5 * time.Second)
	var readErr error
	for time.Now().Before(deadline) {
		n, err := resp.Body.Read(buf)
		total += n
		if err != nil {
			readErr = err
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if readErr == nil && total >= len(firstChunk)+len(rest) {
		t.Fatal("慢订阅者未被踢，连接完整读完")
	}
	if readErr == nil {
		t.Fatalf("连接未被截断：已读 %d 字节", total)
	}
}

// TestAttachedFinishedEntryFallsBackToOrigin 覆盖「Attach 命中后、订阅前条目
// 结束」：TrySubscribe 失败时改走独立回源，客户端仍拿到完整字节而不是空 200。
func TestAttachedFinishedEntryFallsBackToOrigin(t *testing.T) {
	const key = "ds/blocks/finished#0"
	body := []byte("block-bytes-fetched-independently")

	var opens atomic.Int64
	org := &fakeOrigin{
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(bytes.NewReader(body)), nil
		},
	}
	s, _ := newMergeServer(t, org, 1<<20, 4)

	e := s.registry.Create(key, s.registry.GetBuffer())
	if _, ok := s.registry.Attach(key); !ok {
		t.Fatal("Attach 应命中刚创建的条目")
	}
	e.Finish(nil) // 模拟 Attach 成功后、订阅前结束

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traincache/ds/blocks/finished%230", nil)
	s.serveAttached(rec, req, key, datasetOf(key), e, nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q（不得返回空 200）", got, body)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("源站 GET 次数 = %d, want 1（应独立回源）", got)
	}
	if got := s.registry.Len(); got != 0 {
		t.Fatalf("在途条目数 = %d, want 0（独立回源不建条目）", got)
	}
}

// ctxReadCloser 的 Read 阻塞到 Open 时传入的 ctx 取消才返回错误，用于验证
// 最后一个订阅者离开能真正打断底层源站读。
type ctxReadCloser struct {
	ctx      context.Context
	entered  chan struct{}
	returned chan struct{}
	once     sync.Once
	retOnce  sync.Once
}

func (r *ctxReadCloser) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.ctx.Done()
	r.retOnce.Do(func() { close(r.returned) })
	return 0, r.ctx.Err()
}

func (r *ctxReadCloser) Close() error { return nil }

// TestLastSubscriberCancelsOriginRead 覆盖最后一个订阅者离开时取消 pumpCtx：
// 阻塞在源站 Read 的拉取协程必须在 200ms 内返回并调用 Finish。
func TestLastSubscriberCancelsOriginRead(t *testing.T) {
	const key = "ds/blocks/cancel#0"
	entered := make(chan struct{})
	returned := make(chan struct{})

	org := &fakeOrigin{
		openFn: func(ctx context.Context, _ string) (io.ReadCloser, error) {
			return &ctxReadCloser{ctx: ctx, entered: entered, returned: returned}, nil
		},
	}
	s, _ := newMergeServer(t, org, 1<<20, 4)

	e := s.registry.Create(key, s.registry.GetBuffer())
	sub := e.Subscribe()
	go s.pump(e, key, datasetOf(key), time.Now())

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("源站读未开始")
	}

	sub.Close()

	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("最后一个订阅者离开后源站 Read 未在 200ms 内返回")
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for s.registry.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("Pump 未在 200ms 内返回（Finish 未调用）")
		}
		time.Sleep(time.Millisecond)
	}
}

// errorReadCloser 首次 Read 即返回固定错误，模拟源站的 4xx/5xx。
type errorReadCloser struct{ err error }

func (r *errorReadCloser) Read([]byte) (int, error) { return 0, r.err }

func (r *errorReadCloser) Close() error { return nil }

// TestIndependentFirstReadErrorPassthrough 覆盖独立回源首读失败：必须透传
// 源站状态码与 Code，而不是先写 200 再截断。
func TestIndependentFirstReadErrorPassthrough(t *testing.T) {
	const key = "ds/blocks/indep#0"
	notFound := minio.ErrorResponse{
		StatusCode: http.StatusNotFound,
		Code:       "NoSuchKey",
		Message:    "missing",
	}

	org := &fakeOrigin{
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			return &errorReadCloser{err: notFound}, nil
		},
	}
	s, _ := newMergeServer(t, org, 1<<20, 4)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traincache/ds/blocks/indep%230", nil)
	s.serveIndependent(rec, req, key, datasetOf(key), func() {})

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404（首读失败不得写 200）", rec.Code)
	}
	if got := errorCode(t, rec.Body.Bytes()); got != "NoSuchKey" {
		t.Fatalf("Code = %q, want NoSuchKey", got)
	}
}
