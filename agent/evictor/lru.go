package evictor

import (
	"sort"
)

// LRUPolicy 是 LRU 档策略(M4):按 LastTouch 升序(最久未访问的先淘汰)。
// 同分(相同 LastTouch)用 sort.SliceStable 保持原顺序。
type LRUPolicy struct{}

// Name 返回策略名 "lru"。
func (p *LRUPolicy) Name() string { return "lru" }

// Rank 按 LastTouch 升序排序(最久未访问的块排在前面,优先淘汰)。
// 返回 blocks 的副本排序,不修改入参。
func (p *LRUPolicy) Rank(blocks []Block, danger bool) []Block {
	out := make([]Block, len(blocks))
	copy(out, blocks)
	sort.SliceStable(out, func(i, j int) bool { return out[i].LastTouch.Before(out[j].LastTouch) })
	return out
}
