package svc

// stallattr.go:read_stall 实测归因(D47;M9 落地)。
//
// D47 冻结:仅 wait ≥100ms 入直方图(阈值可配);按 ts 关联窗口(默认前后各 2500ms):
//   - 本地存在且非当前窗口预取 → local_slow;
//   - core fetch 在途 → origin_slow;
//   - 未本地且未在途(pending 或未知)→ prefetch_lag;
//   - 无法解析/超窗/dataset 无法归属 → unknown。
//
// Q15 备注:阈值默认 100ms(stallMinWaitMS)可由 svc 配置 TRAINCACHE_READ_STALL_MIN_WAIT_MS
// 注入;关联窗口(C3)由 TRAINCACHE_READ_STALL_WINDOW_MS 注入,默认 5000ms,以帧 ts
// 为中心前后各取半个窗口。

import (
	"time"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

const (
	// stallMinWaitMS 是 read_stall 入直方图的最小等待(D47/Q15:默认 100ms,可配)。
	stallMinWaitMS = int64(100)
)

// stallAttributor 是读等待归因器(依赖由 svc 现有能力提供)。
type stallAttributor struct {
	// minWaitMS 是入直方图的最小等待毫秒(D47/Q15:默认 100,可由 svc 配置注入)。
	minWaitMS int64
	// halfWindow 是 ts 关联的半窗口(C3:帧 ts 前后各 windowMS 内可关联)。
	halfWindow time.Duration
	// exists 做本地存在性检查:返回 resolveRef 得到的 dataset 与块是否本地可 Open。
	exists func(ref ordersource.ChunkRef) (dataset string, found bool)
	// inflight 报告该块是否在 fetcher/prefetch pending 中(origin_slow 判据)。
	inflight func(dataset string, ref ordersource.ChunkRef) bool
	// currentWindow 报告该块是否属于当前消费窗口(D46 prefetch 来源判据)。
	currentWindow func(dataset string, ref ordersource.ChunkRef) bool
	// now 是时间源(测试注入;生产默认 time.Now)。
	now func() time.Time
}

// newStallAttributor 构造归因器(参数可为 nil;阈值取 D47 默认 100ms,窗口取默认 5000ms)。
func newStallAttributor(
	exists func(ref ordersource.ChunkRef) (dataset string, found bool),
	inflight func(dataset string, ref ordersource.ChunkRef) bool,
	currentWindow func(dataset string, ref ordersource.ChunkRef) bool,
) *stallAttributor {
	return newStallAttributorWithThreshold(exists, inflight, currentWindow,
		stallMinWaitMS, defaultReadStallWindowMS)
}

// newStallAttributorWithThreshold 构造归因器并注入最小等待阈值与关联窗口
// (D47/Q15、C3 可配;minWaitMS<=0 回退默认 100ms,windowMS<=0 回退默认 5000ms)。
func newStallAttributorWithThreshold(
	exists func(ref ordersource.ChunkRef) (dataset string, found bool),
	inflight func(dataset string, ref ordersource.ChunkRef) bool,
	currentWindow func(dataset string, ref ordersource.ChunkRef) bool,
	minWaitMS int64,
	windowMS int64,
) *stallAttributor {
	if minWaitMS <= 0 {
		minWaitMS = stallMinWaitMS
	}
	if windowMS <= 0 {
		windowMS = defaultReadStallWindowMS
	}
	return &stallAttributor{
		minWaitMS:     minWaitMS,
		halfWindow:    time.Duration(windowMS) * time.Millisecond,
		exists:        exists,
		inflight:      inflight,
		currentWindow: currentWindow,
		now:           time.Now,
	}
}

// Attribute 返回归因结果与是否记录。最小等待阈值过滤与 ts 关联窗口由本方法负责
// (D47/C3;不在 hooksock dispatch 做)。wait < 阈值 → 不记录;ts 为零值(解析失败)或
// |now-ts| > 窗口/2 → unknown 且记录。
func (a *stallAttributor) Attribute(ref ordersource.ChunkRef, waitMS int64, ts time.Time) (cause string, record bool) {
	minWaitMS := a.minWaitMS
	if minWaitMS <= 0 {
		minWaitMS = stallMinWaitMS
	}
	if waitMS < minWaitMS {
		return metrics.CauseUnknown, false
	}
	now := time.Now
	if a.now != nil {
		now = a.now
	}
	if ts.IsZero() || absDuration(now().Sub(ts)) > a.halfWindow {
		return metrics.CauseUnknown, true
	}

	// dataset 从 exists 回调获得;取不到(无回调/映射缺失)→ 无法判定。
	dataset, found := "", false
	if a.exists != nil {
		dataset, found = a.exists(ref)
	}
	if dataset == "" {
		return metrics.CauseUnknown, true
	}

	current := false
	if a.currentWindow != nil {
		current = a.currentWindow(dataset, ref)
	}
	inflight := false
	if a.inflight != nil {
		inflight = a.inflight(dataset, ref)
	}

	switch {
	case found && !current:
		return metrics.CauseLocalSlow, true
	case inflight:
		return metrics.CauseOriginSlow, true
	case !found:
		return metrics.CausePrefetchLag, true
	default:
		return metrics.CauseUnknown, true
	}
}

// absDuration 返回 d 的绝对值。
func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// stallLocalExists 是归因依赖:resolveRef + cas.Open 判断块是否本地可用(返回 dataset)。
func (a *Agent) stallLocalExists(ref ordersource.ChunkRef) (dataset string, found bool) {
	ds, fp, ok := a.resolveRef(ref)
	if !ok {
		return "", false
	}
	if a.CS == nil {
		return ds, false
	}
	a.markSelfOpen(fp) // D49:stall 归因的本地存在性探测打开
	br, err := a.CS.Open(fp)
	if err != nil {
		return ds, false
	}
	_ = br.Close()
	return ds, true
}

// stallInflight 是归因依赖:该 dataset 的预取器 pending/在途表。
func (a *Agent) stallInflight(dataset string, ref ordersource.ChunkRef) bool {
	if a.Prefetch == nil {
		return false
	}
	b, ok := a.Prefetch.existingBundle(dataset)
	if !ok {
		return false
	}
	return b.prefetcher.InFlight(dataset, ref)
}

// stallCurrentWindow 是归因依赖:该块是否属于当前消费窗口。
func (a *Agent) stallCurrentWindow(dataset string, ref ordersource.ChunkRef) bool {
	if a.Prefetch == nil {
		return false
	}
	b, ok := a.Prefetch.existingBundle(dataset)
	if !ok {
		return false
	}
	return b.prefetcher.IsCurrentWindow(dataset, ref)
}

// handleReadStall 是 HookHandlers.Stall 的 svc 接线:归因成功(record)后打点
// traincache_read_stall_seconds{dataset,phase,cause}(D47)。
func (a *Agent) handleReadStall(ref ordersource.ChunkRef, waitMS int64, ts time.Time) {
	if a.stall == nil {
		return
	}
	cause, record := a.stall.Attribute(ref, waitMS, ts)
	if !record {
		return
	}
	dataset, _ := a.resolveDataset(ref)
	phase := metrics.PhaseUnknown
	if a.phase != nil {
		phase = a.phase.Current(dataset)
	}
	if a.Metrics != nil {
		a.Metrics.ObserveReadStall(dataset, phase, cause, float64(waitMS)/1000.0)
	}
}
