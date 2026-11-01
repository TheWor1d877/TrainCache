package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"traincache/gateway/internal/origin"
)

// blockingReader 在首次 Read 时通知 entered，然后阻塞到 release。
type blockingReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	data    []byte
}

func newBlockingReader(data string) *blockingReader {
	return &blockingReader{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte(data),
	}
}

func (b *blockingReader) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func (b *blockingReader) Close() error { return nil }

func (b *blockingReader) releaseAll() {
	select {
	case <-b.release:
	default:
		close(b.release)
	}
}

// startGet 发起一次直通 GET，返回完成信号。
func startGet(t *testing.T, url string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		resp, err := http.Get(url + "/traincache/ds/CURRENT")
		if err != nil {
			done <- err
			return
		}
		defer resp.Body.Close()
		_, err = io.Copy(io.Discard, resp.Body)
		done <- err
	}()
	return done
}

func newBlockingServer(br *blockingReader) *Server {
	return newTestServer(&fakeOrigin{
		statFn: func(context.Context, string) (origin.ObjectInfo, error) {
			return origin.ObjectInfo{Size: 5}, nil
		},
		openFn: func(context.Context, string) (io.ReadCloser, error) {
			return br, nil
		},
	})
}

// TestShutdownWaitsForInflight 覆盖排水：在途请求未结束前 Shutdown 不返回。
func TestShutdownWaitsForInflight(t *testing.T) {
	br := newBlockingReader("hello")
	defer br.releaseAll()

	s := newBlockingServer(br)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	done := startGet(t, ts.URL)
	<-br.entered

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdown := make(chan bool, 1)
	go func() { shutdown <- s.Shutdown(ctx) }()

	select {
	case drained := <-shutdown:
		t.Fatalf("在途请求未结束时 Shutdown 提前返回 drained=%v", drained)
	case <-time.After(50 * time.Millisecond):
	}

	// 排水期间就绪探针立即失败。
	if rec := serve(s, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("排水期间 readyz = %d, want 503", rec.Code)
	}

	br.releaseAll()
	select {
	case drained := <-shutdown:
		if !drained {
			t.Fatal("在途请求结束后 Shutdown 应报告已排空")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown 未在请求结束后返回")
	}
	if err := <-done; err != nil {
		t.Fatalf("直通请求失败: %v", err)
	}
}

// TestShutdownForcedOnDeadline 覆盖 ctx 到期：返回 false（forced），不阻塞。
func TestShutdownForcedOnDeadline(t *testing.T) {
	br := newBlockingReader("hello")
	defer br.releaseAll()

	s := newBlockingServer(br)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	done := startGet(t, ts.URL)
	<-br.entered

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if drained := s.Shutdown(ctx); drained {
		t.Fatal("ctx 到期时 Shutdown 应报告未排空")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Shutdown 在 ctx 到期后仍阻塞 %v", elapsed)
	}

	br.releaseAll()
	if err := <-done; err != nil {
		t.Fatalf("直通请求失败: %v", err)
	}
}
