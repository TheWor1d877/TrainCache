package fetch

import (
	"context"
	"sync"
	"testing"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// TestOnLocalOpenAfterPut:下载成功后在 cs.Open 读回校验之前以最终指纹调用 OnLocalOpen
// (D49:Agent 侧据此把 fetch 自身的读回打开登记为自打开)。nil 钩子安全由其余用例覆盖。
func TestOnLocalOpenAfterPut(t *testing.T) {
	cs, _ := newTestCAS(t)
	store := objectstore.NewMock()
	content := []byte("fetch-selfopen")
	seed(t, store, "ds/blocks/a#0", content)

	var (
		mu  sync.Mutex
		got []cas.Fingerprint
	)
	f := New(store, cs, Options{
		MaxInflightBytes: 1 << 20,
		MaxTasks:         1,
		MaxConnsPerHost:  1,
		OnLocalOpen: func(fp cas.Fingerprint) {
			mu.Lock()
			got = append(got, fp)
			mu.Unlock()
		},
	})
	fp, err := f.Fetch(context.Background(), "ds/blocks/a#0", int64(len(content)))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != fp {
		t.Fatalf("OnLocalOpen 调用 = %v, want 恰 [%s]", got, fp)
	}
	if _, err := cs.Open(fp); err != nil {
		t.Fatalf("块未落盘: %v", err)
	}
}
