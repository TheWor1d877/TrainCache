package collector

import (
	"sync"

	"traincache/agent/ordersource"
)

// calibrate.go:交叉校准(design §5.5、decisions D15):滑动窗口 CalibrateWindow 次打开内,
// "实际打开顺序 与 订单期望顺序(ConsumeOrder.Chunks)"的匹配率 < CalibrateThreshold
// (默认 0.8,即 7.0"默认 20% 偏差"的精确口径)→ 触发 cursor 重对齐到实际位点。
//
// 口径:
//   - 期望顺序 = provider 在本次打开返回的订单 Chunks(装配层每次现取,阶段切换自然生效);
//   - 期望位点 expectedIdx 初始为 0,命中期望块则前移;未命中时向后重定位
//     (跳读)或从头重定位(阶段/顺序整体改变),重定位本身不计为匹配;
//   - 滑动窗口满员后每次观测都评估:匹配率 < 阈值 → 触发一次(触发后清窗,避免
//     同一段顺序持续触发形成重对齐风暴),回调(SetAdvanceCallback)收到订单 dataset
//     与本次打开的 ref。

// calibrator 是交叉校准窗口状态(独立锁;由 Collector 的打开事件路径调用)。
type calibrator struct {
	mu        sync.Mutex
	window    int
	threshold float64
	obs       []bool // 命中标记(滑动窗口;容量 window)
	next      int    // 满员后的覆写位置
	filled    int    // 已记录观测数(≤ window)
	expected  int    // 期望位点:Chunks 中的下一个期望下标
	triggers  uint64 // 触发次数(统计/测试)
}

// newCalibrator 构造校准器;window ≤ 0 / threshold ≤ 0 已由 CollectorConfig.withDefaults 补齐。
func newCalibrator(window int, threshold float64) *calibrator {
	return &calibrator{
		window:    window,
		threshold: threshold,
		obs:       make([]bool, 0, window),
	}
}

// observe 记录一次带订单引用的打开:返回 (是否触发重对齐, 实际位点 chunk)。
func (k *calibrator) observe(order ordersource.ConsumeOrder, ref ordersource.ChunkRef) (bool, int64) {
	k.mu.Lock()
	defer k.mu.Unlock()

	matched := false
	if i := findChunk(order.Chunks, ref, k.expected); i >= 0 {
		matched = i == k.expected
		k.expected = i + 1
	} else if j := findChunk(order.Chunks, ref, 0); j >= 0 {
		// 期望位点之后找不到(消费者已跳过或顺序切换):从头重定位,本次计为不匹配。
		k.expected = j + 1
	}

	if k.filled < k.window {
		k.obs = append(k.obs, matched)
		k.filled++
	} else {
		k.obs[k.next] = matched
		k.next = (k.next + 1) % k.window
	}
	if k.filled < k.window {
		return false, 0
	}

	matches := 0
	for _, m := range k.obs {
		if m {
			matches++
		}
	}
	if float64(matches)/float64(k.window) < k.threshold {
		k.triggers++
		k.obs = k.obs[:0]
		k.filled = 0
		k.next = 0
		return true, ref.Chunk
	}
	return false, 0
}

// count 返回触发次数(测试/诊断)。
func (k *calibrator) count() uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.triggers
}

// findChunk 在 chunks 的 [from, len) 内查找 ref 的下标;未找到返回 -1。
func findChunk(chunks []ordersource.ChunkRef, ref ordersource.ChunkRef, from int) int {
	if from < 0 {
		from = 0
	}
	for i := from; i < len(chunks); i++ {
		if chunks[i] == ref {
			return i
		}
	}
	return -1
}
