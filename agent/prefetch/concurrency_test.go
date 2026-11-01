package prefetch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// concurrency_test.go covers the P0×stage-switch interleaving from
// docs/7.3 后续覆盖计划: 100 EnqueueP0 calls racing 10 OnStageSwitch calls,
// with the D45 dedup invariant (one task per blockKey) checked continuously.

// pendingInvariantLocked checks the D45 invariant and returns a message on
// violation. Callers must hold p.mu.
func pendingInvariantLocked(p *PersistentPrefetcher) string {
	seen := make(map[pendingKey]bool)
	for _, q := range p.queues {
		for _, task := range q {
			key := blockKey(task.dataset, task.ref)
			if task.dataset == "" {
				return fmt.Sprintf("queued task %s has empty dataset", key)
			}
			if seen[key] {
				return fmt.Sprintf("duplicate blockKey %s across queues", key)
			}
			seen[key] = true
			if p.pending[key] != task {
				return fmt.Sprintf("queued task %s not owned by pending table", key)
			}
		}
	}
	return ""
}

func pendingInvariant(p *PersistentPrefetcher) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return pendingInvariantLocked(p)
}

func TestConcurrentEnqueueP0AndStageSwitch(t *testing.T) {
	const n = 16
	refs := refsNamed("burst", n)
	specs := make([]blockSpec, 0, n)
	for i, r := range refs {
		specs = append(specs, blockSpec{
			path:    r.Path,
			chunk:   r.Chunk,
			content: fmt.Sprintf("burst-block-%02d", i),
			local:   false,
			source:  true,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, specs)
	p := env.newPrefetcher(Config{Lookahead: 4})
	// 先注入订单:P0 入队快照 dataset,去重键才能构造。
	if _, err := p.OnStageSwitch(ctx, orderOf("epoch-0", refs, 4)); err != nil {
		t.Fatalf("seed OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ { // 4×25 = 100 次 EnqueueP0
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				ref := refs[(seed*25+i)%n]
				if err := p.EnqueueP0(ctx, []ChunkRef{ref}); err != nil {
					t.Errorf("EnqueueP0: %v", err)
					return
				}
			}
		}(w)
	}
	for w := 0; w < 2; w++ { // 2×5 = 10 次 OnStageSwitch
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				next := orderOf(fmt.Sprintf("epoch-%d", seed*5+i+1), refs, 4)
				if _, err := p.OnStageSwitch(ctx, next); err != nil {
					t.Errorf("OnStageSwitch: %v", err)
					return
				}
			}
		}(w)
	}

	stop := make(chan struct{})
	var violation atomic.Value
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if msg := pendingInvariant(p); msg != "" {
				violation.Store(msg)
				return
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()
	close(stop)
	<-sampled
	if msg, _ := violation.Load().(string); msg != "" {
		t.Fatalf("dedup invariant violated mid-burst: %s", msg)
	}

	if msg := pendingInvariant(p); msg != "" {
		t.Fatalf("dedup invariant violated after burst: %s", msg)
	}
	p.mu.Lock()
	pendingLen := len(p.pending)
	p.mu.Unlock()
	if pendingLen > n {
		t.Fatalf("pending entries = %d, want <= %d distinct blocks", pendingLen, n)
	}
}
