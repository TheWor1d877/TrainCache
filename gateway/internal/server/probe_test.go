package server

import (
	"net/http"
	"testing"
)

// TestReadyzFollowsSetReady 覆盖就绪探针：未 ready → 503，SetReady(false) 后回到 503。
func TestReadyzFollowsSetReady(t *testing.T) {
	s := newTestServer(&fakeOrigin{})

	if rec := serve(s, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("初始 readyz = %d, want 503", rec.Code)
	}

	s.SetReady(true)
	if rec := serve(s, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("ready 后 readyz = %d, want 200", rec.Code)
	}

	s.SetReady(false)
	if rec := serve(s, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("SetReady(false) 后 readyz = %d, want 503", rec.Code)
	}
}

// TestHealthzIndependentOfReady 保证存活探针不随 ready 变化（不依赖源站）。
func TestHealthzIndependentOfReady(t *testing.T) {
	s := newTestServer(&fakeOrigin{})
	s.SetReady(false)

	if rec := serve(s, http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", rec.Code)
	}
}
