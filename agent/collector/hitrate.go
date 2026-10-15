package collector

import (
	"math"
	"sync"
	"time"
)

// hitrate.go:命中率统计(design §5.5:打开块集合与本地块求交集,滚动窗口(分钟级)+
// 按训练阶段(StageID)聚合;不落长期历史,重启丢弃)。
//
// 窗口结构:按分钟分桶的环形数组,容量 = 窗口分钟数;写入时按分钟归档(跨分钟自动滚动),
// 读取时只统计"距最近一次观测不超过窗口容量"的桶。阶段聚合为进程生命周期累计(内存表,
// 随进程重启丢弃)。

// hitRateWindow 是命中率滚动窗口长度(分钟级;包内变量,构造时读取,测试可改小)。
var hitRateWindow = 5 * time.Minute

// hitRateBucket 是单格(一分钟)的命中/未命中计数。
type hitRateBucket struct {
	minute int64  // 该格所属分钟(Unix 秒 / 60);math.MinInt64 = 空格
	hits   uint64 // 命中数
	misses uint64 // 未命中数
}

// hitRate 是滚动窗口 + 阶段聚合状态(独立锁:HitRate 可由状态查询路径并发调用)。
type hitRate struct {
	mu      sync.Mutex
	buckets []hitRateBucket // 环形分钟桶,容量 = 窗口分钟数
	last    int64           // 最近一次观测的分钟(计算在窗桶用)
	stages  map[string]*hitRateBucket
}

// newHitRate 构造命中率统计;window ≤ 0 → hitRateWindow,不足 1 分钟按 1 分钟。
func newHitRate(window time.Duration) *hitRate {
	if window <= 0 {
		window = hitRateWindow
	}
	n := int(window / time.Minute)
	if n < 1 {
		n = 1
	}
	buckets := make([]hitRateBucket, n)
	for i := range buckets {
		buckets[i].minute = math.MinInt64
	}
	return &hitRate{
		buckets: buckets,
		last:    math.MinInt64,
		stages:  make(map[string]*hitRateBucket),
	}
}

// observe 记录一次打开判定(hit 为真表示本地命中);stage 为空不计入阶段聚合。
func (h *hitRate) observe(now time.Time, stage string, hit bool) {
	m := now.Unix() / 60
	h.mu.Lock()
	defer h.mu.Unlock()

	idx := int(m % int64(len(h.buckets)))
	if idx < 0 {
		idx += len(h.buckets)
	}
	b := &h.buckets[idx]
	if b.minute != m {
		*b = hitRateBucket{minute: m} // 该格滚入新分钟:旧计数过期
	}
	if hit {
		b.hits++
	} else {
		b.misses++
	}
	if m > h.last {
		h.last = m
	}

	if stage != "" {
		s := h.stages[stage]
		if s == nil {
			s = &hitRateBucket{minute: m}
			h.stages[stage] = s
		}
		if hit {
			s.hits++
		} else {
			s.misses++
		}
	}
}

// rate 返回滚动窗口命中率;窗口内无样本 → 0。
func (h *hitRate) rate() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return ratioWindow(h.buckets, h.last)
}

// stageRate 返回某训练阶段的进程内命中率;无样本 → 0。
func (h *hitRate) stageRate(stage string) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.stages[stage]
	if s == nil {
		return 0
	}
	return ratio(s.hits, s.misses)
}

// ratioWindow 汇总窗口内的桶(距最近观测分钟不超过桶数);无样本 → 0。
func ratioWindow(buckets []hitRateBucket, last int64) float64 {
	var hits, misses uint64
	for i := range buckets {
		b := buckets[i]
		if b.minute == math.MinInt64 {
			continue
		}
		if last-b.minute >= int64(len(buckets)) {
			continue // 已滚出窗口
		}
		hits += b.hits
		misses += b.misses
	}
	return ratio(hits, misses)
}

// ratio 计算 命中/(命中+未命中);分母为 0 → 0。
func ratio(hits, misses uint64) float64 {
	if hits+misses == 0 {
		return 0
	}
	return float64(hits) / float64(hits+misses)
}
