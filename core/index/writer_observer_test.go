package index

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"traincache/core/cas"
)

// TestCommitSelfOpenRegisteredByObserver：Writer.Commit 提交前经 cas.Open 校验本次
// 新块（writer.go 步骤 ④），该"本进程读打开"必须触发 Store 实例上安装的观察者
// （生产装配据此登记为 Agent 自打开，避免污染消费侧信号）。
func TestCommitSelfOpenRegisteredByObserver(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "observer-commit"

	entries := make([]Entry, 3)
	for i := range entries {
		content := fmt.Sprintf("observer-commit-block-%d", i)
		entries[i] = Entry{
			Path:        fmt.Sprintf("/data/obs-%d.tar", i),
			Chunk:       int64(i),
			Size:        int64(len(content)),
			Fingerprint: env.putBlock(t, content),
		}
	}

	var (
		mu   sync.Mutex
		seen = make(map[cas.Fingerprint]int)
	)
	env.cs.SetOpenObserver(func(fp cas.Fingerprint) {
		mu.Lock()
		seen[fp]++
		mu.Unlock()
	})
	mu.Lock()
	before := len(seen)
	mu.Unlock()
	if before != 0 {
		t.Fatalf("Commit 前观察者已被触发 %d 次", before)
	}

	w := NewWriter(env.store, env.cs, Config{})
	if _, err := w.Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, e := range entries {
		if got := seen[e.Fingerprint]; got != 1 {
			t.Errorf("Commit 自开观察者 %s 调用 %d 次, want 1", e.Fingerprint, got)
		}
	}
	if len(seen) != len(entries) {
		t.Errorf("观察者登记指纹数 = %d, want %d", len(seen), len(entries))
	}
}
