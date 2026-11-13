package prefetch

// 按需模式(SetDemandOnly)用例:置位后阶段首批(P1)与邻域窗口(P2)都不入队,
// 消费推进也不开放邻域;P0 按需入口照常回源。队列断言在 Start 前做(任务不出队,
// 无时序竞态);下载断言走真实契约路径(Mock 源站 + 计数)。

import (
	"context"
	"fmt"
	"slices"
	"testing"
)

// demandSpecs 构造 n 个需下载的块(local=false, source=true),path 与 refsNamed("d", n) 对齐。
func demandSpecs(n int) []blockSpec {
	specs := make([]blockSpec, 0, n)
	for i := 0; i < n; i++ {
		specs = append(specs, blockSpec{
			path:    fmt.Sprintf("d-%02d.tar", i),
			content: fmt.Sprintf("demand-block-%02d", i),
			local:   false,
			source:  true,
		})
	}
	return specs
}

func TestDemandOnlySuppressesStagePrefetch(t *testing.T) {
	env := newTestEnv(t, demandSpecs(8))
	refs := refsNamed("d", 8)
	p := env.newPrefetcher(Config{Lookahead: 4})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	p.SetDemandOnly(true)
	if _, err := p.OnStageSwitch(ctx, orderOf("s1", refs, 3)); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	if d := p.Status().QueueDepth; d[priP1] != 0 || d[priP2] != 0 {
		t.Fatalf("按需模式 P1/P2 队列深度 = %v, want 0/0(K=3 也不入队)", d)
	}
	// 消费推进同样不开放邻域窗口。
	for i := 0; i < 4; i++ {
		if !p.OnConsumeSideAdvance(ctx, refs[i]) {
			t.Fatalf("advance(%d) 未推进", i)
		}
	}
	if d := p.Status().QueueDepth; d[priP1] != 0 || d[priP2] != 0 {
		t.Fatalf("advance 后 P1/P2 仍入队: %v", d)
	}

	// 按需入口不受影响:启动调度后只回源这一块,无投机预取。
	if err := p.EnqueueP0Demand(ctx, refs[4:5]); err != nil {
		t.Fatalf("EnqueueP0Demand: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)
	key := expectedBlockKey(testDataset, refs[4])
	waitFor(t, "按需块回源", func() bool {
		return slices.Contains(blockGets(env.store), key)
	})
	if got := blockGets(env.store); len(got) != 1 || got[0] != key {
		t.Fatalf("回源键 = %v, want 仅 %s", got, key)
	}
}

func TestDemandOnlyToggleRestoresPrefetch(t *testing.T) {
	env := newTestEnv(t, demandSpecs(8))
	refs := refsNamed("d", 8)
	p := env.newPrefetcher(Config{Lookahead: 4})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	p.SetDemandOnly(true)
	if _, err := p.OnStageSwitch(ctx, orderOf("s1", refs, 3)); err != nil {
		t.Fatalf("OnStageSwitch(按需): %v", err)
	}
	if d := p.Status().QueueDepth; d[priP1] != 0 {
		t.Fatalf("按需模式 P1 队列深度 = %d, want 0", d[priP1])
	}

	// 解除后阶段首批恢复入队(容量回升的订单走这条路径)。
	p.SetDemandOnly(false)
	if _, err := p.OnStageSwitch(ctx, orderOf("s2", refs, 3)); err != nil {
		t.Fatalf("OnStageSwitch(恢复): %v", err)
	}
	if d := p.Status().QueueDepth; d[priP1] != 3 {
		t.Fatalf("解除后 P1 队列深度 = %d, want 3", d[priP1])
	}
}
