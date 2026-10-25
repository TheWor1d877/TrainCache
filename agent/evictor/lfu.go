package evictor

import (
	"math"
	"sort"
	"time"
)

// LFUPolicy 是 LFU 档策略(M4):带老化的访问频次淘汰。
// aged_accesses = Accesses * 0.5^(Δt/halfLife),Δt = now − LastTouch;
// 按 aged_accesses 升序(老化后访问频次最低的块优先淘汰)。
// 老化半衰期默认 6h(与 heat 档 T_R 同口径)。
type LFUPolicy struct {
	halfLife time.Duration
}

// NewLFUPolicy 构造 LFU 档策略(老化半衰期默认 6h)。
func NewLFUPolicy() *LFUPolicy { return &LFUPolicy{halfLife: 6 * time.Hour} }

// Name 返回策略名 "lfu"。
func (p *LFUPolicy) Name() string { return "lfu" }

// Rank 按老化访问频次升序排序。返回 blocks 的副本排序,不修改入参。
func (p *LFUPolicy) Rank(blocks []Block, danger bool) []Block {
	out := make([]Block, len(blocks))
	copy(out, blocks)
	now := time.Now()
	aged := func(b Block) float64 {
		return float64(b.Accesses) * math.Pow(0.5, float64(now.Sub(b.LastTouch))/float64(p.halfLife))
	}
	sort.SliceStable(out, func(i, j int) bool { return aged(out[i]) < aged(out[j]) })
	return out
}
