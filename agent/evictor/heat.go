package evictor

import (
	"math"
	"sort"
	"time"
)

// HeatPolicy 是 heat 档策略(M4 D8):包装现有淘汰排序逻辑,行为逐字不变。
//
//   - normal 轮:S = max(h, R),R = 0.5^(Δt/T_R),T_R = heatHalfLife(6h);
//   - danger 轮:R 清零,只按 h 升序;
//   - 按 S 升序(小的先淘汰),sort.SliceStable 同分保持原顺序。
type HeatPolicy struct {
	halfLife time.Duration
}

// NewHeatPolicy 构造 heat 档策略(半衰期取包级常量 heatHalfLife)。
func NewHeatPolicy() *HeatPolicy { return &HeatPolicy{halfLife: heatHalfLife} }

// Name 返回策略名 "heat"。
func (p *HeatPolicy) Name() string { return "heat" }

// Rank 按现有淘汰得分排序(与 evictor.go 原 candidates 逻辑逐字一致;D8 heat 档行为不变)。
// 返回 blocks 的副本排序,不修改入参。
func (p *HeatPolicy) Rank(blocks []Block, danger bool) []Block {
	out := make([]Block, len(blocks))
	copy(out, blocks)
	if danger {
		sort.SliceStable(out, func(i, j int) bool { return out[i].Heat < out[j].Heat })
		return out
	}
	now := time.Now()
	score := func(b Block) float64 { return math.Max(b.Heat, freshness(now, b.LastTouch)) }
	sort.SliceStable(out, func(i, j int) bool { return score(out[i]) < score(out[j]) })
	return out
}
