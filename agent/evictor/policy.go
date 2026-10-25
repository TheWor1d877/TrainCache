package evictor

import (
	"time"

	"traincache/core/cas"
)

// Block 是淘汰策略看到的块元数据(M4 策略抽象)。
// 字段来自 EvictionEntry 的策略可见子集:Heat/LastTouch 由 heat 档消费,
// LastTouch/Accesses 分别由 lru/lfu 档消费。
type Block struct {
	FP        cas.Fingerprint
	Heat      float64
	LastTouch time.Time
	Accesses  int64
}

// Policy 是淘汰策略接口(M4 三档策略 heat/lru/lfu 的统一抽象;D9 接口收缩:
// 只保留 Rank,准入过滤随 W-TinyLFU 撤档删除,三档全部准入)。
// Rank 按"淘汰优先级"排序:返回切片中排在前面的块优先被淘汰;
// danger=true 表示当前是危险线轮(util > M),策略可更激进(heat 档靠它做新鲜度豁免)。
type Policy interface {
	Name() string
	Rank(blocks []Block, danger bool) []Block
}
