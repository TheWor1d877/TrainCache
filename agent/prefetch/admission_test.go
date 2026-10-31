package prefetch

import (
	"context"
	"testing"
	"time"

	"traincache/core/fetch"
)

// TestDemandBypassesSpeculativeQuota：需求读补块按在读准入，预铺半额任务槽被占满时仍能立刻回源。
// 取 MaxTasks=2（预铺只有 1 槽）：旧口径下两个预取块占满任务额度，P0 demand 只能排队。
func TestDemandBypassesSpeculativeQuota(t *testing.T) {
	refs := []ChunkRef{{Path: "a-00.tar"}, {Path: "a-01.tar"}, {Path: "a-02.tar"}}
	env := newTestEnv(t, []blockSpec{
		{path: refs[0].Path, chunk: 0, content: "block-0", source: true},
		{path: refs[1].Path, chunk: 0, content: "block-1", source: true},
		{path: refs[2].Path, chunk: 0, content: "block-2", source: true},
	})
	env.fetch = fetch.New(env.store, env.cs, fetch.Options{
		MaxInflightBytes: 1 << 20,
		MaxTasks:         2,
		MaxConnsPerHost:  4,
		MaxRetries:       1,
	})
	for _, ref := range refs {
		env.store.holdKey(expectedBlockKey(testDataset, ref))
	}

	p := env.newPrefetcher(Config{Lookahead: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := p.OnStageSwitch(ctx, orderOf("epoch-0", refs, 2)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)

	// 预铺额度只放行 1 个块，第二个 P1 块停在预铺半额任务槽之外。
	waitBlockGets(t, env, 1)
	time.Sleep(100 * time.Millisecond)
	if got := blockGets(env.store); len(got) != 1 {
		t.Fatalf("预铺并发回源块数 = %d，want 1（半额任务槽）: %v", len(got), got)
	}

	if err := p.EnqueueP0Demand(ctx, []ChunkRef{refs[2]}); err != nil {
		t.Fatalf("EnqueueP0Demand: %v", err)
	}
	wantKey := expectedBlockKey(testDataset, refs[2])
	deadline := time.Now().Add(5 * time.Second)
	for {
		if contains(blockGets(env.store), wantKey) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("需求块未进入回源（仍排在预铺后面）: %v", blockGets(env.store))
		}
		time.Sleep(time.Millisecond)
	}

	// 放行全部回源，确认两块都完成且额度归还。
	for _, ref := range refs {
		env.store.releaseKey(expectedBlockKey(testDataset, ref))
	}
	waitForDone(t, p)
}

// waitBlockGets 等待源站数据块 Get 达到 n 个。
func waitBlockGets(t *testing.T, env *testEnv, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(blockGets(env.store)) < n {
		if time.Now().After(deadline) {
			t.Fatalf("回源块数停在 %d，want >= %d: %v", len(blockGets(env.store)), n, blockGets(env.store))
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForDone 等待预取器把已入队块全部抓完（队列深度归零）。
func waitForDone(t *testing.T, p *PersistentPrefetcher) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d := p.Status().QueueDepth
		if d == [4]int{} {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("队列未清空: %v", d)
		}
		time.Sleep(time.Millisecond)
	}
}

// contains 判断字符串切片是否包含 s。
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
