package svc

// metricswiring.go:M8/M9 新增 Registry 打点的集中适配(D44/D46/D47/D48)。
// 数据源现有的取实测值,没有的传零值/跳过;业务判定留给各 stub(M8/M9 填写)。
//
// M9 落地:D48 bandwidth_limit 取 core/fetch 实际生效上限;D46 命中 source/phase 经
// collector.CacheLabeler 注入(collector 负责打点,svc 负责判定)。lead_blocks 由预取器
// 在 consume/front 事件推进时直接上报(与 Status.Cursor/PrefetchFrontier 同源),svc 不重复计算。

import (
	"strings"

	"traincache/agent/collector"
	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// wireMetrics 完成一次性指标接线(装配期调用):singleflight 合并钩子、限速上限、
// 修复队列初始深度与命中打点标签适配。
func (a *Agent) wireMetrics() {
	if a.Metrics == nil {
		return
	}
	if a.Fetcher != nil {
		// D48:同 key 合并由 Agent 侧包装器计数,不改 core/fetch 契约。
		a.Fetcher.OnMerge = a.reportSingleflightMerged
		// D48 fetch_bandwidth_limit_bytes:实际生效的在途字节上限(默认 1 GiB)。
		a.reportBandwidthLimit(a.Fetcher.MaxInflightBytes())
	} else {
		a.reportBandwidthLimit(0)
	}
	a.reportRepairQueueDepth()
	// D46:命中 source/phase 由 svc 判定,collector 打点。
	collector.SetCacheLabeler(&collector.CacheLabeler{
		Source: a.cacheSource,
		Phase:  a.cachePhase,
	})
}

// cacheSource 判定一次命中的来源(D46):
//   - 命中块属于该 dataset 当前订单窗口 → prefetch;
//   - 命中块在订单内但窗口外(已有缓存)→ cache;
//   - 无预取记录/无法解析订单归属 → unknown。
//
// “由本 Agent 预取任务写入”在冻结的 prefetch 状态里没有独立来源标记,以当前窗口
// (cursor 起 lookahead)作为等价判据;DECISIONS 说明。
func (a *Agent) cacheSource(dataset string, fp cas.Fingerprint, ref ordersource.ChunkRef) string {
	_ = fp
	if a.Prefetch == nil || dataset == "" {
		return metrics.SourceUnknown
	}
	b, ok := a.Prefetch.existingBundle(dataset)
	if !ok {
		return metrics.SourceUnknown
	}
	known, inWindow := b.refWindowState(ref)
	if !known {
		return metrics.SourceUnknown
	}
	if inWindow {
		return metrics.SourcePrefetch
	}
	return metrics.SourceCache
}

// cachePhase 返回 dataset 当前 phase(D46;无跟踪器/无状态 → unknown)。
func (a *Agent) cachePhase(dataset string) string {
	if a.phase == nil {
		return metrics.PhaseUnknown
	}
	return a.phase.Current(dataset)
}

// reportSingleflightMerged 是 core/fetch OnMerge 回调:仅等待者路径触发(leader 不计),
// 从 D25 键解析 dataset 后计数;解析失败(非 <dataset>/blocks/... 布局)忽略(D48)。
func (a *Agent) reportSingleflightMerged(key string) {
	if a.Metrics == nil {
		return
	}
	dataset, ok := datasetFromBlockKey(key)
	if !ok {
		return
	}
	a.Metrics.IncFetchSingleflightMerged(dataset)
}

// datasetFromBlockKey 从 D25 键 <dataset>/blocks/<path>#<chunk> 提取 dataset。
func datasetFromBlockKey(key string) (string, bool) {
	dataset, rest, ok := strings.Cut(key, "/blocks/")
	if !ok || dataset == "" || rest == "" {
		return "", false
	}
	return dataset, true
}

// reportConsumptionAdvance 记一次消费推进(D48;source∈{hook,inotify})。
func (a *Agent) reportConsumptionAdvance(dataset, source string) {
	if a.Metrics == nil {
		return
	}
	a.Metrics.IncConsumptionAdvance(dataset, source)
}

// reportRepairQueueDepth 上报当前修复队列长度(traincache_repair_queue_depth;D48):
// 数据源 = rescan.Status().RepairQueue,装配期与每轮淘汰周期各刷新一次。
func (a *Agent) reportRepairQueueDepth() {
	if a.Metrics == nil || a.Rescan == nil {
		return
	}
	a.Metrics.SetRepairQueueDepth("", int(a.Rescan.Status().RepairQueue))
}

// reportBandwidthLimit 上报回源字节信号量上限(D48;core/fetch 的 MaxInflightBytes)。
// 限速为进程级单值,dataset 维度取空串(经 DatasetLabel 归并为 other)。
func (a *Agent) reportBandwidthLimit(n int64) {
	if a.Metrics == nil {
		return
	}
	a.Metrics.SetFetchBandwidthLimitBytes("", n)
}

// reportPrefetchHitRatio 上报预取命中率(D46;数据源 = 预取器 100 样本窗口,直接打点,
// 此适配留给 svc 侧调用)。
func (a *Agent) reportPrefetchHitRatio(dataset string, v float64) {
	if a.Metrics == nil {
		return
	}
	a.Metrics.SetPrefetchHitRatio(dataset, v)
}

// reportLeadBlocks 上报预取领先块数(D48;负值 = 消费领先)。
func (a *Agent) reportLeadBlocks(dataset string, n int64) {
	if a.Metrics == nil {
		return
	}
	a.Metrics.SetPrefetchLeadBlocks(dataset, n)
}

// reportTaskDuration 上报一次预取任务耗时(D48;priority 取大写 P0..P3)。
func (a *Agent) reportTaskDuration(dataset, priority string, seconds float64) {
	if a.Metrics == nil {
		return
	}
	a.Metrics.ObservePrefetchTaskDuration(dataset, priority, seconds)
}
