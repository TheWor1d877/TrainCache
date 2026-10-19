package svc

import (
	"testing"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// phase_test.go:M9/D46 phase 状态机(四态、switch 优先、按 dataset 独立、K 块就绪)。

func phaseRef(path string, chunk int64) ordersource.ChunkRef {
	return ordersource.ChunkRef{Path: path, Chunk: chunk}
}

// phaseResolver 把 path 直接当 dataset(测试用确定性归属)。
func phaseResolver(ref ordersource.ChunkRef) (string, bool) { return ref.Path, true }

// TestPhaseStartupToSteady:startup 至首个订单的 K 块就绪后转 steady。
func TestPhaseStartupToSteady(t *testing.T) {
	p := newPhaseTracker(3)
	p.setDatasetResolver(phaseResolver)

	if got := p.Current("ds"); got != metrics.PhaseStartup {
		t.Fatalf("启动期无事件 phase = %q, want startup", got)
	}
	p.OnOrder(phaseRef("ds", 0))
	if got := p.Current("ds"); got != metrics.PhaseStartup {
		t.Fatalf("订单后窗口未就绪 phase = %q, want startup", got)
	}
	p.OnBlockReady(phaseRef("ds", 0))
	p.OnBlockReady(phaseRef("ds", 1))
	if got := p.Current("ds"); got != metrics.PhaseStartup {
		t.Fatalf("2/3 块就绪 phase = %q, want startup", got)
	}
	p.OnBlockReady(phaseRef("ds", 1)) // 重复块不重复计数
	if got := p.Current("ds"); got != metrics.PhaseStartup {
		t.Fatalf("重复块后 phase = %q, want startup(去重计数)", got)
	}
	p.OnBlockReady(phaseRef("ds", 2))
	if got := p.Current("ds"); got != metrics.PhaseSteady {
		t.Fatalf("K 块就绪后 phase = %q, want steady", got)
	}
}

// TestPhaseSwitchPriority:switch 活跃时优先于 startup;新窗口 K 块就绪后回 steady。
func TestPhaseSwitchPriority(t *testing.T) {
	p := newPhaseTracker(3)
	p.setDatasetResolver(phaseResolver)

	p.OnOrder(phaseRef("ds", 0))
	p.OnBlockReady(phaseRef("ds", 0)) // startup 1/3,仍活跃
	p.OnStageSwitchFor("ds", "epoch-2")
	if got := p.Current("ds"); got != metrics.PhaseSwitch {
		t.Fatalf("switch 与 startup 同时活跃 phase = %q, want switch", got)
	}
	p.OnBlockReady(phaseRef("ds", 10))
	p.OnBlockReady(phaseRef("ds", 11))
	if got := p.Current("ds"); got != metrics.PhaseSwitch {
		t.Fatalf("新窗口 2/3 就绪 phase = %q, want switch", got)
	}
	p.OnBlockReady(phaseRef("ds", 12))
	if got := p.Current("ds"); got != metrics.PhaseSteady {
		t.Fatalf("新窗口 K 块就绪 phase = %q, want steady", got)
	}
}

// TestPhasePerDataset:各 dataset 窗口独立推进,互不影响。
func TestPhasePerDataset(t *testing.T) {
	p := newPhaseTracker(2)
	p.setDatasetResolver(phaseResolver)

	p.OnOrder(phaseRef("a", 0))
	p.OnOrder(phaseRef("b", 0))
	p.OnBlockReady(phaseRef("a", 0))
	p.OnBlockReady(phaseRef("a", 1))
	p.OnBlockReady(phaseRef("b", 0)) // b 仅 1/2

	if got := p.Current("a"); got != metrics.PhaseSteady {
		t.Errorf("a phase = %q, want steady", got)
	}
	if got := p.Current("b"); got != metrics.PhaseStartup {
		t.Errorf("b phase = %q, want startup(窗口独立)", got)
	}
	p.OnStageSwitchFor("b", "epoch-2")
	if got := p.Current("b"); got != metrics.PhaseSwitch {
		t.Errorf("b phase = %q, want switch", got)
	}
	if got := p.Current("a"); got != metrics.PhaseSteady {
		t.Errorf("b 切换后 a phase = %q, want steady 不变", got)
	}
}

// TestPhaseUnknown:空 dataset / 全局 startup 结束后的无状态 dataset 均 unknown;
// 订单到达前的块就绪不结束 startup。
func TestPhaseUnknown(t *testing.T) {
	p := newPhaseTracker(1)
	p.setDatasetResolver(phaseResolver)

	if got := p.Current(""); got != metrics.PhaseUnknown {
		t.Errorf("Current(\"\") = %q, want unknown", got)
	}
	if got := p.Current("none"); got != metrics.PhaseStartup {
		t.Errorf("启动期无状态 dataset = %q, want startup", got)
	}
	// 未收到订单,块就绪不推进 startup 窗口(D46:首个 ConsumeOrder 的首 K 块)。
	p.OnBlockReady(phaseRef("ds", 0))
	if got := p.Current("ds"); got != metrics.PhaseStartup {
		t.Errorf("订单前块就绪 phase = %q, want startup", got)
	}
	// 另一 dataset 完成首个订单窗口后,全局 startup 结束,无状态 dataset → unknown。
	p.OnOrder(phaseRef("done", 0))
	p.OnBlockReady(phaseRef("done", 0))
	if got := p.Current("none"); got != metrics.PhaseUnknown {
		t.Errorf("startup 结束后无状态 dataset = %q, want unknown", got)
	}
}

// TestPhaseFrozenSignatures:冻结签名 OnStageSwitch(stageID) 用最近一次事件归属 dataset。
func TestPhaseFrozenSignatures(t *testing.T) {
	p := newPhaseTracker(2)
	p.setDatasetResolver(phaseResolver)

	p.OnOrder(phaseRef("ds", 0))
	p.OnStageSwitch("epoch-2")
	if got := p.Current("ds"); got != metrics.PhaseSwitch {
		t.Fatalf("OnStageSwitch 后 phase = %q, want switch", got)
	}
	p.OnBlockReady(phaseRef("ds", 5))
	p.OnBlockReady(phaseRef("ds", 6))
	if got := p.Current("ds"); got != metrics.PhaseSteady {
		t.Fatalf("窗口就绪后 phase = %q, want steady", got)
	}
}

// TestPhaseOrderPrefetchChunksWindow:订单注入的 K(ConsumeOrder.PrefetchChunks)同时
// 决定 startup 与 switch 窗口:K=3 时第 2 块不就绪、第 3 块结束窗口。
func TestPhaseOrderPrefetchChunksWindow(t *testing.T) {
	p := newPhaseTracker(prefetchLookahead) // 缺省 8:未注入 K 时不应在第 3 块结束
	p.setDatasetResolver(phaseResolver)

	p.OnOrderWithK(phaseRef("ds", 0), 3)
	p.OnBlockReady(phaseRef("ds", 0))
	p.OnBlockReady(phaseRef("ds", 1))
	if got := p.Current("ds"); got != metrics.PhaseStartup {
		t.Fatalf("订单 K=3 下 2/3 块就绪 phase = %q, want startup", got)
	}
	p.OnBlockReady(phaseRef("ds", 2))
	if got := p.Current("ds"); got != metrics.PhaseSteady {
		t.Fatalf("订单 K=3 下 3/3 块就绪 phase = %q, want steady", got)
	}

	p.OnStageSwitchFor("ds", "epoch-2")
	p.OnBlockReady(phaseRef("ds", 10))
	p.OnBlockReady(phaseRef("ds", 11))
	if got := p.Current("ds"); got != metrics.PhaseSwitch {
		t.Fatalf("switch K=3 下 2/3 块就绪 phase = %q, want switch", got)
	}
	p.OnBlockReady(phaseRef("ds", 12))
	if got := p.Current("ds"); got != metrics.PhaseSteady {
		t.Fatalf("switch K=3 下 3/3 块就绪 phase = %q, want steady", got)
	}
}

// TestPhaseOrderKFallback:未注入订单 K 的 dataset 回退构造缺省 K(prefetchLookahead=8)。
func TestPhaseOrderKFallback(t *testing.T) {
	p := newPhaseTracker(prefetchLookahead)
	p.setDatasetResolver(phaseResolver)

	p.OnOrder(phaseRef("fallback", 0))
	for i := int64(0); i < prefetchLookahead-1; i++ {
		p.OnBlockReady(phaseRef("fallback", i))
	}
	if got := p.Current("fallback"); got != metrics.PhaseStartup {
		t.Fatalf("缺省 K 下 %d/%d 块就绪 phase = %q, want startup", prefetchLookahead-1, prefetchLookahead, got)
	}
	p.OnBlockReady(phaseRef("fallback", int64(prefetchLookahead-1)))
	if got := p.Current("fallback"); got != metrics.PhaseSteady {
		t.Fatalf("缺省 K 下 %d/%d 块就绪 phase = %q, want steady", prefetchLookahead, prefetchLookahead, got)
	}
}

// TestPhaseZeroWindow:K=0 时窗口视为立即就绪(有订单即 steady)。
func TestPhaseZeroWindow(t *testing.T) {
	p := newPhaseTracker(0)

	if got := p.Current("ds"); got != metrics.PhaseUnknown {
		t.Fatalf("K=0 无订单 phase = %q, want unknown", got)
	}
	p.OnStageSwitchFor("ds", "epoch-1")
	if got := p.Current("ds"); got != metrics.PhaseSteady {
		t.Fatalf("K=0 切换后 phase = %q, want steady", got)
	}
}
