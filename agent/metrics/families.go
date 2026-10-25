// Package metrics： families.go 落 M6 指标底座:design §5.8 七线指标全量注册(23 个 family 中新增 20 个;
// prefetch_queue_depth / prefetch_cursor / stage_switch_eta_seconds 三个由 M3 注册,复用不动)
// 与主对话冻结的专用打点 API(M4 rescan / M5 collector / M6 淘汰循环按这里冻结的方法名打点)。
//
// 标签纪律(§5.8 基数纪律;decisions D17):
//   - dataset 一律先经 DatasetLabel 归并(白名单外 "other");
//   - result / endpoint 取值编译期固定(见下方词汇表),集合外取值忽略该次打点,不新增基数
//     (与 SetPrefetchQueueDepth 越界忽略同风格);
//   - node 取 Config.Node(进程内单值,基数 1);
//   - bucket 固定为 {.01,.05,.1,.5,1,5,10,30}s(design §5.8 冻结);
//   - 构造时预置子指标(dataset="other" × 各固定 result 取值、四 endpoint × success|error、
//     node 标签预置、agent_up=1):family 自启动即在 /metrics 可见,与 formatErrors/queueDepth
//     既有预置风格一致。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// 七线新增 20 个 family 名,逐字冻结于 design §5.8(测试侧独立字面量,防改名后测试跟着漂移)。
const (
	// ① 命中率线。
	cacheHitsName     = "traincache_cache_hits_total"
	cacheMissesName   = "traincache_cache_misses_total"
	cacheHitRatioName = "traincache_cache_hit_ratio"
	// ② 回源线。
	fetchBytesName         = "traincache_fetch_bytes_total"
	fetchRequestsName      = "traincache_fetch_requests_total"
	fetchInflightBytesName = "traincache_fetch_inflight_bytes"
	// ③ 索引线。
	indexRefreshName      = "traincache_index_refresh_total"
	indexRefreshDurName   = "traincache_index_refresh_duration_seconds"
	indexShardsLoadedName = "traincache_index_shards_loaded"
	// ④ GC 线。
	gcSweepsName         = "traincache_gc_sweeps_total"
	gcBlocksRemovedName  = "traincache_gc_blocks_removed_total"
	gcObjectsRemovedName = "traincache_gc_objects_removed_total"
	// ⑤ 磁盘淘汰线。
	evictBlocksName           = "traincache_evict_blocks_total"
	evictProtectedSkippedName = "traincache_evict_protected_skipped_total"
	// capacityBytesName 是字节累计 gauge(§14 D58;source∈{disk,bytes})。
	capacityBytesName = "traincache_cache_capacity_bytes_total"
	// ⑥ 延迟线(stage_switch_eta_seconds 已由 M3 注册)。
	requestDurationName = "traincache_request_duration_seconds"
	// ⑦ 节点状态线(prefetch_queue_depth / prefetch_cursor 已由 M3 注册)。
	agentUpName           = "traincache_agent_up"
	agentDegradedName     = "traincache_agent_degraded"
	rescanProgressName    = "traincache_rescan_progress"
	quarantinedBlocksName = "traincache_quarantined_blocks"
	diskUsageRatioName    = "traincache_disk_usage_ratio"
)

// M8/M9 新增 family 名(design §5.8;逐字冻结)。
const (
	hookReadMissName             = "traincache_hook_read_miss_total"
	prefetchHitRatioName         = "traincache_prefetch_hit_ratio"
	readStallName                = "traincache_read_stall_seconds"
	fetchSingleflightMergedName  = "traincache_fetch_singleflight_merged_total"
	repairQueueDepthName         = "traincache_repair_queue_depth"
	prefetchTaskDurationName     = "traincache_prefetch_task_duration_seconds"
	prefetchLeadBlocksName       = "traincache_prefetch_lead_blocks"
	consumptionAdvanceName       = "traincache_consumption_advance_total"
	fetchBandwidthLimitBytesName = "traincache_fetch_bandwidth_limit_bytes"
	// R3/R4(6.1 §5.11/§5.12)。
	indexSweepSkippedName                    = "traincache_index_sweep_skipped_total"
	objectstoreConditionalWriteSupportedName = "traincache_objectstore_conditional_write_supported"
)

// GC 跳过原因词汇表(6.1 §5.11:protected 快照 manifest 缺失/损坏;集合外归一 other)。
const (
	ReasonManifestUnavailable = "manifest_unavailable"
	sweepReasonOther          = "other"
)

// phase / source / cause / reason / priority 标签词汇表(D44/D46/D47/D48;编译期固定)。
const (
	PhaseStartup = "startup"
	PhaseSteady  = "steady"
	PhaseSwitch  = "switch"
	PhaseUnknown = "unknown"

	SourcePrefetch = "prefetch"
	SourceCache    = "cache"
	SourceUnknown  = "unknown"

	SourceHook    = "hook"
	SourceInotify = "inotify"

	CauseLocalSlow   = "local_slow"
	CausePrefetchLag = "prefetch_lag"
	CauseOriginSlow  = "origin_slow"
	CauseUnknown     = "unknown"

	ReasonAbsent  = "absent"
	ReasonCorrupt = "corrupt"

	FetchReasonPrefetch = "prefetch"
	FetchReasonDemand   = "demand"
	FetchReasonRepair   = "repair"
	FetchReasonOther    = "other"
)

// M9 固定 bucket(design §5.8)。
var (
	readStallBuckets            = []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60}
	prefetchTaskDurationBuckets = []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120}
)

// otherDatasetLabel 是 DatasetLabel 的白名单外归并值(§5.8;构造预置与打点归并共用)。
const otherDatasetLabel = "other"

// result 标签词汇表(编译期固定;DECISIONS 回传):
//   - ResultSuccess / ResultError:全局二元结果,适用于回源、索引刷新、GC 清扫与 RPC 延迟
//     (hits / fetch_bytes / index_refresh_duration / gc_*_removed 的签名无 result 参数,
//     固定写入 ResultSuccess);
//   - ResultRefetched:仅 cache_misses_total(§7.1:坏块修复重下成功计 result="refetched");
//   - ResultNormal / ResultDanger:仅磁盘淘汰线,取值是触发线而非结果(§5.8 "result(normal/danger)";
//     D24:normal=利用率超 k 触发,k danger=危险线超 M 激进淘汰)。
const (
	ResultSuccess   = "success"
	ResultError     = "error"
	ResultRefetched = "refetched"
	ResultNormal    = "normal"
	ResultDanger    = "danger"
)

// policy 标签词汇表(M4 淘汰策略;编译期固定;D9 撤 W-TinyLFU 档后收缩为三档)。
const (
	PolicyHeat = "heat"
	PolicyLRU  = "lru"
	PolicyLFU  = "lfu"
)

// policyLabels 是全部合法 policy 取值(编译期固定词汇表)。
var policyLabels = [...]string{
	PolicyHeat,
	PolicyLRU,
	PolicyLFU,
}

// validPolicy 判断 policy ∈ {heat,lru,lfu}(编译期固定词汇表)。
func validPolicy(policy string) bool {
	for _, p := range policyLabels {
		if policy == p {
			return true
		}
	}
	return false
}

// capacitySource 是 capacity_bytes gauge 的 source 标签取值(§14 D58;编译期固定)。
const (
	CapacitySourceDisk  = "disk"
	CapacitySourceBytes = "bytes"
)

// capacitySources 是全部合法 source 取值(构造时逐一预置)。
var capacitySources = [...]string{
	CapacitySourceDisk,
	CapacitySourceBytes,
}

// endpoint 标签词汇表(RPC 方法名;DECISIONS 回传):取值字面即 agent.proto 服务定义中的
// 方法名,不带服务前缀;ReportService(北向)不参与本线。
const (
	EndpointSubmitPreheat    = "SubmitPreheat"
	EndpointGetPreheatStatus = "GetPreheatStatus"
	EndpointListSnapshot     = "ListSnapshot"
	EndpointGetNodeStatus    = "GetNodeStatus"
	// 8.x 增量(2026-10-20):两个动作 RPC,与既有 endpoint 标签同风格。
	EndpointUpdatePreheat = "UpdatePreheat"
	EndpointCancelPreheat = "CancelPreheat"
)

// endpointLabels 是全部合法 endpoint 值(构造时逐一预置)。
var endpointLabels = [...]string{
	EndpointSubmitPreheat,
	EndpointGetPreheatStatus,
	EndpointListSnapshot,
	EndpointGetNodeStatus,
	EndpointUpdatePreheat,
	EndpointCancelPreheat,
}

// fixedDurationBuckets 是固定 bucket 集 {.01,.05,.1,.5,1,5,10,30}s(design §5.8 冻结,延迟线);
// index_refresh_duration_seconds 的 bucket 文档未冻结,复用同一固定集(该 family 量级与 RPC
// 延迟同档,30s 上界覆盖大 catalog 刷新;DECISIONS 回传)。
var fixedDurationBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30}

// hitMiss 是 cache_hit_ratio 的内部计数(按归一化 dataset 分桶;桶数上界 = 白名单大小 + 1)。
type hitMiss struct {
	hits   uint64
	misses uint64
}

// initSevenLine 构造、注册并预置七线新增的 20 个 family(New 内调用一次;MustRegister 保证
// 每 family 只注册一次)。
func (r *Registry) initSevenLine() {
	r.cacheHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: cacheHitsName,
		Help: "缓存命中次数(design §5.8;result 固定 success;source=prefetch|cache|unknown;phase=startup|steady|switch|unknown)",
	}, []string{"dataset", "result", "source", "phase"})
	r.cacheMisses = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: cacheMissesName,
		Help: "缓存未命中次数(result=success|refetched|error;phase=startup|steady|switch|unknown)",
	}, []string{"dataset", "result", "phase"})
	r.cacheHitRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: cacheHitRatioName,
		Help: "缓存命中率 = 命中/(命中+未命中)(按 dataset,phase;Registry 内部计数维护)",
	}, []string{"dataset", "phase"})
	r.fetchBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: fetchBytesName,
		Help: "回源成功交付字节数(§5.8;result 固定 success;reason=prefetch|demand|repair|other)",
	}, []string{"dataset", "result", "reason"})
	r.fetchRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: fetchRequestsName,
		Help: "回源请求次数(result=success|error;reason=prefetch|demand|repair|other)",
	}, []string{"dataset", "result", "reason"})
	r.fetchInflightBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fetchInflightBytesName,
		Help: "回源在途字节数",
	}, []string{"dataset"})
	r.indexRefresh = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: indexRefreshName,
		Help: "索引刷新次数(result=success|error)",
	}, []string{"dataset", "result"})
	r.indexRefreshDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    indexRefreshDurName,
		Help:    "索引刷新耗时秒数(result 固定 success;固定 bucket {.01,.05,.1,.5,1,5,10,30}s)",
		Buckets: fixedDurationBuckets,
	}, []string{"dataset", "result"})
	r.indexShardsLoaded = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: indexShardsLoadedName,
		Help: "已加载索引分片数",
	}, []string{"dataset"})
	r.gcSweeps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: gcSweepsName,
		Help: "GC 清扫次数(result=success|error)",
	}, []string{"dataset", "result"})
	r.gcBlocksRemoved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: gcBlocksRemovedName,
		Help: "GC 回收块数(result 固定 success)",
	}, []string{"dataset", "result"})
	r.gcObjectsRemoved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: gcObjectsRemovedName,
		Help: "GC 回收对象数(result 固定 success)",
	}, []string{"dataset", "result"})
	r.evictBlocks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: evictBlocksName,
		Help: "磁盘淘汰删除的本地副本块数(result=normal|danger,取触发线;decisions D24)",
	}, []string{"dataset", "result"})
	r.evictProtectedSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: evictProtectedSkippedName,
		Help: "受保护(cursor+lookahead)跳过淘汰的块数(result=normal|danger,取触发线;D24)",
	}, []string{"dataset", "result"})
	r.requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    requestDurationName,
		Help:    "RPC 请求耗时秒数(endpoint=RPC 方法名;result=success|error;固定 bucket {.01,.05,.1,.5,1,5,10,30}s)",
		Buckets: fixedDurationBuckets,
	}, []string{"endpoint", "result"})
	r.agentUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: agentUpName,
		Help: "Agent 进程存活(1=up;node=节点名)",
	}, []string{"node"})
	r.agentDegraded = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: agentDegradedName,
		Help: "Agent 降级(1=sidecar 退出降级;node=节点名)",
	}, []string{"node"})
	r.rescanProgress = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: rescanProgressName,
		Help: "重扫描进度 0..1(design §7.1)",
	})
	r.quarantined = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: quarantinedBlocksName,
		Help: "quarantine 目录坏块数(§5.7 修复成功递减)",
	}, []string{"dataset"})
	r.diskUsageRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: diskUsageRatioName,
		Help: "缓存盘利用率 0..1(decisions D24 淘汰触发线的输入)",
	}, []string{"dataset"})
	r.capacityBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: capacityBytesName,
		Help: "缓存字节累计(§14 D58;source=disk 为磁盘已用、source=bytes 为字节累计)",
	}, []string{"source"})

	r.reg.MustRegister(
		r.cacheHits, r.cacheMisses, r.cacheHitRatio,
		r.fetchBytes, r.fetchRequests, r.fetchInflightBytes,
		r.indexRefresh, r.indexRefreshDuration, r.indexShardsLoaded,
		r.gcSweeps, r.gcBlocksRemoved, r.gcObjectsRemoved,
		r.evictBlocks, r.evictProtectedSkipped,
		r.requestDuration,
		r.agentUp, r.agentDegraded, r.rescanProgress, r.quarantined, r.diskUsageRatio,
		r.capacityBytes,
	)

	// 预置 dataset="other" × 固定 result 取值:family 自启动即出现,取值词汇表可见。
	preset := func(vec *prometheus.CounterVec, results ...string) {
		for _, res := range results {
			vec.WithLabelValues(otherDatasetLabel, res)
		}
	}
	r.cacheHits.WithLabelValues(otherDatasetLabel, ResultSuccess, SourceUnknown, PhaseUnknown)
	for _, res := range []string{ResultSuccess, ResultError, ResultRefetched} {
		r.cacheMisses.WithLabelValues(otherDatasetLabel, res, PhaseUnknown)
	}
	r.fetchBytes.WithLabelValues(otherDatasetLabel, ResultSuccess, FetchReasonOther)
	for _, res := range []string{ResultSuccess, ResultError} {
		r.fetchRequests.WithLabelValues(otherDatasetLabel, res, FetchReasonOther)
	}
	preset(r.indexRefresh, ResultSuccess, ResultError)
	preset(r.gcSweeps, ResultSuccess, ResultError)
	preset(r.gcBlocksRemoved, ResultSuccess)
	preset(r.gcObjectsRemoved, ResultSuccess)
	preset(r.evictBlocks, ResultNormal, ResultDanger)
	preset(r.evictProtectedSkipped, ResultNormal, ResultDanger)
	r.indexRefreshDuration.WithLabelValues(otherDatasetLabel, ResultSuccess)
	// dataset 单标签 gauge 预置 "other"。
	r.cacheHitRatio.WithLabelValues(otherDatasetLabel, PhaseUnknown)
	r.fetchInflightBytes.WithLabelValues(otherDatasetLabel)
	r.indexShardsLoaded.WithLabelValues(otherDatasetLabel)
	r.quarantined.WithLabelValues(otherDatasetLabel)
	r.diskUsageRatio.WithLabelValues(otherDatasetLabel)
	// capacity_bytes:预置全部合法 source 取值(family 自启动可见)。
	for _, src := range capacitySources {
		r.capacityBytes.WithLabelValues(src)
	}
	// 四 endpoint × {success,error} 全量预置。
	for _, ep := range endpointLabels {
		r.requestDuration.WithLabelValues(ep, ResultSuccess)
		r.requestDuration.WithLabelValues(ep, ResultError)
	}
	// 节点状态线:agent_up 构造时预置 1(主对话冻结),agent_degraded 预置 0。
	r.agentUp.WithLabelValues(r.node).Set(1)
	r.agentDegraded.WithLabelValues(r.node).Set(0)
}

// —— ① 命中率线 ——

// ObserveCacheHit 记一次缓存命中:traincache_cache_hits_total{dataset,result="success",
// source,phase}(result 固定 success;D46)。可选参数依次为 source(默认 unknown)、
// phase(默认 unknown);集合外取值归一为 unknown,不新增基数。同时即时更新命中率 gauge。
func (r *Registry) ObserveCacheHit(dataset string, labels ...string) {
	source, phase := SourceUnknown, PhaseUnknown
	if len(labels) > 0 {
		source = normalizeSource(labels[0])
	}
	if len(labels) > 1 {
		phase = normalizePhase(labels[1])
	}
	d := r.DatasetLabel(dataset)
	r.cacheHits.WithLabelValues(d, ResultSuccess, source, phase).Inc()
	r.observeCacheHitRatio(d, phase, true)
}

// ObserveCacheMiss 记一次缓存未命中:traincache_cache_misses_total{dataset,result,phase},
// result∈{success,refetched,error}(D46;§7.1:坏块修复重下成功计 refetched);集合外忽略。
// 可选参数为 phase(默认 unknown)。未命中不论结果(含 error)一律计入命中率分母。
func (r *Registry) ObserveCacheMiss(dataset, result string, phases ...string) {
	if !validResult(result, ResultSuccess, ResultError, ResultRefetched) {
		return
	}
	phase := PhaseUnknown
	if len(phases) > 0 {
		phase = normalizePhase(phases[0])
	}
	d := r.DatasetLabel(dataset)
	r.cacheMisses.WithLabelValues(d, result, phase).Inc()
	r.observeCacheHitRatio(d, phase, false)
}

// observeCacheHitRatio 以 命中/(命中+未命中) 维护 traincache_cache_hit_ratio{dataset,phase}
// (D46:ratio 由 Registry 内部计数维护;持锁内 Set,保证 gauge 与计数一致)。
func (r *Registry) observeCacheHitRatio(dataset, phase string, hit bool) {
	r.muCacheRatio.Lock()
	defer r.muCacheRatio.Unlock()
	k := ratioKey{dataset: dataset, phase: phase}
	st := r.cacheRatio[k]
	if st == nil {
		st = &hitMiss{}
		r.cacheRatio[k] = st
	}
	if hit {
		st.hits++
	} else {
		st.misses++
	}
	r.cacheHitRatio.WithLabelValues(dataset, phase).Set(float64(st.hits) / float64(st.hits+st.misses))
}

// —— ② 回源线 ——

// AddFetchBytes 累加成功回源交付的字节量:traincache_fetch_bytes_total{dataset,
// result="success",reason}(D48;reason=prefetch|demand|repair|other,默认 other)。
// n<=0 忽略(counter 只增,避免负增量 panic)。
func (r *Registry) AddFetchBytes(dataset string, n int64, reasons ...string) {
	if n <= 0 {
		return
	}
	reason := FetchReasonOther
	if len(reasons) > 0 {
		reason = normalizeFetchReason(reasons[0])
	}
	r.fetchBytes.WithLabelValues(r.DatasetLabel(dataset), ResultSuccess, reason).Add(float64(n))
}

// IncFetchRequests 记一次回源请求:traincache_fetch_requests_total{dataset,result,reason},
// result∈{success,error};reason 默认 other(D48);集合外 result 忽略、reason 归一 other。
func (r *Registry) IncFetchRequests(dataset, result string, reasons ...string) {
	if !validSuccessError(result) {
		return
	}
	reason := FetchReasonOther
	if len(reasons) > 0 {
		reason = normalizeFetchReason(reasons[0])
	}
	r.fetchRequests.WithLabelValues(r.DatasetLabel(dataset), result, reason).Inc()
}

// SetFetchInflightBytes 设置回源在途字节数(traincache_fetch_inflight_bytes{dataset})。
func (r *Registry) SetFetchInflightBytes(dataset string, n int64) {
	r.fetchInflightBytes.WithLabelValues(r.DatasetLabel(dataset)).Set(float64(n))
}

// —— ③ 索引线 ——

// IncIndexRefresh 记一次索引刷新:traincache_index_refresh_total{dataset,result},
// result∈{success,error};集合外忽略。
func (r *Registry) IncIndexRefresh(dataset, result string) {
	if !validSuccessError(result) {
		return
	}
	r.indexRefresh.WithLabelValues(r.DatasetLabel(dataset), result).Inc()
}

// ObserveIndexRefreshDuration 记录一次索引刷新耗时:traincache_index_refresh_duration_seconds
// {dataset,result="success"}(签名无 result 固定 success),固定 bucket 集。
func (r *Registry) ObserveIndexRefreshDuration(dataset string, seconds float64) {
	r.indexRefreshDuration.WithLabelValues(r.DatasetLabel(dataset), ResultSuccess).Observe(seconds)
}

// SetIndexShardsLoaded 设置已加载索引分片数(traincache_index_shards_loaded{dataset})。
func (r *Registry) SetIndexShardsLoaded(dataset string, n int64) {
	r.indexShardsLoaded.WithLabelValues(r.DatasetLabel(dataset)).Set(float64(n))
}

// IncIndexSweepSkipped 记一次 GC 跳过块删除:traincache_index_sweep_skipped_total
// {dataset,reason}(6.1 §5.11;reason 集合外归一 other,dataset 走白名单归并)。
func (r *Registry) IncIndexSweepSkipped(dataset, reason string) {
	if reason != ReasonManifestUnavailable {
		reason = sweepReasonOther
	}
	r.indexSweepSkipped.WithLabelValues(r.DatasetLabel(dataset), reason).Inc()
}

// SetObjectstoreConditionalWriteSupported 设置条件写探测结果 gauge(1=支持,0=不支持/
// 探测失败;6.1 §5.12,无标签)。
func (r *Registry) SetObjectstoreConditionalWriteSupported(supported bool) {
	v := 0.0
	if supported {
		v = 1
	}
	r.objectstoreConditionalWriteSupported.Set(v)
}

// —— ④ GC 线 ——

// IncGCSweeps 记一次 GC 清扫:traincache_gc_sweeps_total{dataset,result},
// result∈{success,error};集合外忽略。
func (r *Registry) IncGCSweeps(dataset, result string) {
	if !validSuccessError(result) {
		return
	}
	r.gcSweeps.WithLabelValues(r.DatasetLabel(dataset), result).Inc()
}

// AddGCBlocksRemoved 累加一次 GC 回收的块数:traincache_gc_blocks_removed_total{dataset,
// result="success"}(签名无 result);n<=0 忽略。
func (r *Registry) AddGCBlocksRemoved(dataset string, n int64) {
	if n <= 0 {
		return
	}
	r.gcBlocksRemoved.WithLabelValues(r.DatasetLabel(dataset), ResultSuccess).Add(float64(n))
}

// AddGCObjectsRemoved 累加一次 GC 回收的对象数:traincache_gc_objects_removed_total{dataset,
// result="success"}(签名无 result);n<=0 忽略。
func (r *Registry) AddGCObjectsRemoved(dataset string, n int64) {
	if n <= 0 {
		return
	}
	r.gcObjectsRemoved.WithLabelValues(r.DatasetLabel(dataset), ResultSuccess).Add(float64(n))
}

// —— ⑤ 磁盘淘汰线 ——

// IncEvictBlocks 累加一次磁盘淘汰删除的本地副本块数:traincache_evict_blocks_total{dataset,
// result},result∈{normal,danger} 取触发线(D24:normal=利用率超 k 触发,danger=危险线超 M);
// 集合外忽略;n<=0 忽略。
func (r *Registry) IncEvictBlocks(dataset, result string, n int64) {
	if !validEvictTrigger(result) || n <= 0 {
		return
	}
	r.evictBlocks.WithLabelValues(r.DatasetLabel(dataset), result).Add(float64(n))
}

// IncEvictProtectedSkipped 累加受保护(cursor+lookahead,任何情况不淘汰)跳过淘汰的块数:
// traincache_evict_protected_skipped_total{dataset,result},result∈{normal,danger};
// 集合外忽略;n<=0 忽略。
func (r *Registry) IncEvictProtectedSkipped(dataset, result string, n int64) {
	if !validEvictTrigger(result) || n <= 0 {
		return
	}
	r.evictProtectedSkipped.WithLabelValues(r.DatasetLabel(dataset), result).Add(float64(n))
}

// —— ⑥ 延迟线 ——

// ObserveRequestDuration 记录一次 RPC 请求耗时:traincache_request_duration_seconds{endpoint,
// result},endpoint∈{SubmitPreheat,GetPreheatStatus,ListSnapshot,GetNodeStatus,UpdatePreheat,
// CancelPreheat},result∈{success,error};集合外忽略;固定 bucket {.01,.05,.1,.5,1,5,10,30}s。
func (r *Registry) ObserveRequestDuration(endpoint, result string, seconds float64) {
	if !validEndpoint(endpoint) || !validSuccessError(result) {
		return
	}
	r.requestDuration.WithLabelValues(endpoint, result).Observe(seconds)
}

// —— ⑦ 节点状态线 ——

// SetAgentUp 设置 traincache_agent_up(1=进程存活;node 标签取 Config.Node,构造时预置 1)。
func (r *Registry) SetAgentUp(v float64) {
	r.agentUp.WithLabelValues(r.node).Set(v)
}

// SetAgentDegraded 设置 traincache_agent_degraded(1=sidecar 退出降级,sidecar 新代际恢复自动
// 退出降级;node 标签取 Config.Node,构造时预置 0)。
func (r *Registry) SetAgentDegraded(v float64) {
	r.agentDegraded.WithLabelValues(r.node).Set(v)
}

// SetRescanProgress 设置重扫描进度 0..1(traincache_rescan_progress,无标签)。
func (r *Registry) SetRescanProgress(v float64) {
	r.rescanProgress.Set(v)
}

// SetQuarantined 设置 quarantine 坏块数(traincache_quarantined_blocks{dataset};§5.7 修复
// 成功后递减)。
func (r *Registry) SetQuarantined(dataset string, n int64) {
	r.quarantined.WithLabelValues(r.DatasetLabel(dataset)).Set(float64(n))
}

// SetDiskUsageRatio 设置缓存盘利用率 0..1(traincache_disk_usage_ratio{dataset};D24 触发线
// 与危险线的输入)。
func (r *Registry) SetDiskUsageRatio(dataset string, v float64) {
	r.diskUsageRatio.WithLabelValues(r.DatasetLabel(dataset)).Set(v)
}

// SetCapacityBytes 设置字节累计 gauge(traincache_cache_capacity_bytes_total{source};§14 D58;
// source∈{disk,bytes};集合外忽略)。
func (r *Registry) SetCapacityBytes(source string, n int64) {
	if source != CapacitySourceDisk && source != CapacitySourceBytes {
		return
	}
	r.capacityBytes.WithLabelValues(source).Set(float64(n))
}

// —— 取值校验(集合外取值忽略该次打点,不新增基数)——

// validResult 判断 result 是否属于给定冻结取值集。
func validResult(result string, allowed ...string) bool {
	for _, a := range allowed {
		if result == a {
			return true
		}
	}
	return false
}

// validSuccessError 判断 result 是否 ∈ {success, error}(全局二元结果词汇表)。
func validSuccessError(result string) bool {
	return validResult(result, ResultSuccess, ResultError)
}

// validEvictTrigger 判断 result 是否 ∈ {normal, danger}(磁盘淘汰线触发线词汇表)。
func validEvictTrigger(result string) bool {
	return validResult(result, ResultNormal, ResultDanger)
}

// validEndpoint 判断 endpoint 是否 ∈ 冻结的 RPC 方法名(8.x 起六个)。
func validEndpoint(endpoint string) bool {
	for _, e := range endpointLabels {
		if endpoint == e {
			return true
		}
	}
	return false
}

// —— M8/D44 与 M9/D46~D48 新增 family 的构造、注册与预置 ——

// initHooksAndEvidence 构造并注册读 miss 线(D44)与 M9 的 8 个 family(D46~D48),
// 并预置最小子指标使 family 自启动即在 /metrics 可见。
func (r *Registry) initHooksAndEvidence() {
	r.hookReadMiss = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: hookReadMissName,
		Help: "读 miss 上报次数(reason=absent|corrupt;D44;不带 dataset)",
	}, []string{"reason"})
	r.prefetchHitRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prefetchHitRatioName,
		Help: "预取命中率(最近 100 个消费样本中消费时已就绪的比例;D46)",
	}, []string{"dataset"})
	r.readStall = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    readStallName,
		Help:    "实测消费读等待秒数(dataset,phase,cause;仅 ≥100ms;D47)",
		Buckets: readStallBuckets,
	}, []string{"dataset", "phase", "cause"})
	r.fetchSingleflight = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: fetchSingleflightMergedName,
		Help: "同 key 在途时被合并的 fetch 调用次数(D48)",
	}, []string{"dataset"})
	r.repairQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: repairQueueDepthName,
		Help: "坏块修复队列当前长度(D48)",
	}, []string{"dataset"})
	r.prefetchTaskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prefetchTaskDurationName,
		Help:    "预取任务从入队到终态的耗时秒数(priority=P0..P3;D48)",
		Buckets: prefetchTaskDurationBuckets,
	}, []string{"dataset", "priority"})
	r.prefetchLeadBlocks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prefetchLeadBlocksName,
		Help: "预取前沿 − 消费游标块数(负值表示消费领先;D48)",
	}, []string{"dataset"})
	r.consumptionAdvance = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: consumptionAdvanceName,
		Help: "消费推进事件次数(source=hook|inotify;D48)",
	}, []string{"dataset", "source"})
	r.fetchBandwidthLimit = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: fetchBandwidthLimitBytesName,
		Help: "配置的回源字节信号量上限(D48)",
	}, []string{"dataset"})
	r.indexSweepSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: indexSweepSkippedName,
		Help: "GC 因 protected 快照 manifest 不可用跳过块删除的次数(dataset,reason;6.1 §5.11)",
	}, []string{"dataset", "reason"})
	r.objectstoreConditionalWriteSupported = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: objectstoreConditionalWriteSupportedName,
		Help: "对象存储条件写探测结果(1=支持,0=不支持/探测失败;6.1 §5.12)",
	})

	r.reg.MustRegister(
		r.hookReadMiss, r.prefetchHitRatio, r.readStall, r.fetchSingleflight,
		r.repairQueueDepth, r.prefetchTaskDuration, r.prefetchLeadBlocks,
		r.consumptionAdvance, r.fetchBandwidthLimit,
		r.indexSweepSkipped, r.objectstoreConditionalWriteSupported,
	)

	// 最小预置(dataset="other",固定枚举):family 自启动即出现。
	r.hookReadMiss.WithLabelValues(ReasonAbsent)
	r.hookReadMiss.WithLabelValues(ReasonCorrupt)
	r.prefetchHitRatio.WithLabelValues(otherDatasetLabel)
	r.readStall.WithLabelValues(otherDatasetLabel, PhaseUnknown, CauseUnknown)
	r.fetchSingleflight.WithLabelValues(otherDatasetLabel)
	r.repairQueueDepth.WithLabelValues(otherDatasetLabel)
	for _, pri := range priorityLabels {
		r.prefetchTaskDuration.WithLabelValues(otherDatasetLabel, pri)
	}
	r.prefetchLeadBlocks.WithLabelValues(otherDatasetLabel)
	for _, src := range []string{SourceHook, SourceInotify} {
		r.consumptionAdvance.WithLabelValues(otherDatasetLabel, src)
	}
	r.fetchBandwidthLimit.WithLabelValues(otherDatasetLabel)
	r.indexSweepSkipped.WithLabelValues(otherDatasetLabel, ReasonManifestUnavailable)
	r.indexSweepSkipped.WithLabelValues(otherDatasetLabel, sweepReasonOther)
	r.objectstoreConditionalWriteSupported.Set(0)
}

// ObserveHookReadMiss 记一次读 miss:traincache_hook_read_miss_total{reason}(D44;
// reason∈{absent,corrupt},不带 dataset)。集合外忽略该次打点。
func (r *Registry) ObserveHookReadMiss(reason string) {
	if reason != ReasonAbsent && reason != ReasonCorrupt {
		return
	}
	r.hookReadMiss.WithLabelValues(reason).Inc()
}

// HookReadMiss 返回读 miss counter(测试访问器;生产读取走 /metrics)。
func (r *Registry) HookReadMiss() *prometheus.CounterVec { return r.hookReadMiss }

// ObserveReadStall 记录一次实测读等待:traincache_read_stall_seconds{dataset,phase,cause}
// (D47;仅应记录 ≥100ms 的等待,阈值过滤由接线层 stallattr 负责)。标签集合外忽略。
func (r *Registry) ObserveReadStall(dataset, phase, cause string, seconds float64) {
	if !validPhase(phase) || !validCause(cause) || seconds < 0 {
		return
	}
	r.readStall.WithLabelValues(r.DatasetLabel(dataset), phase, cause).Observe(seconds)
}

// SetPrefetchHitRatio 设置预取命中率(traincache_prefetch_hit_ratio{dataset};D46)。
func (r *Registry) SetPrefetchHitRatio(dataset string, v float64) {
	r.prefetchHitRatio.WithLabelValues(r.DatasetLabel(dataset)).Set(v)
}

// IncFetchSingleflightMerged 记一次同 key 合并的 fetch 调用(D48;Agent 侧包装器统计)。
func (r *Registry) IncFetchSingleflightMerged(dataset string) {
	r.fetchSingleflight.WithLabelValues(r.DatasetLabel(dataset)).Inc()
}

// SetRepairQueueDepth 设置坏块修复队列长度(traincache_repair_queue_depth{dataset};D48)。
func (r *Registry) SetRepairQueueDepth(dataset string, n int) {
	r.repairQueueDepth.WithLabelValues(r.DatasetLabel(dataset)).Set(float64(n))
}

// ObservePrefetchTaskDuration 记录一次预取任务耗时(D48;priority∈{P0..P3},
// bucket {0.1,0.5,1,2,5,10,30,60,120}s)。集合外 priority 忽略该次打点。
func (r *Registry) ObservePrefetchTaskDuration(dataset, priority string, seconds float64) {
	if !validPriority(priority) || seconds < 0 {
		return
	}
	r.prefetchTaskDuration.WithLabelValues(r.DatasetLabel(dataset), priority).Observe(seconds)
}

// SetPrefetchLeadBlocks 设置预取前沿与消费游标的块数差(D48;负值 = 消费领先)。
func (r *Registry) SetPrefetchLeadBlocks(dataset string, n int64) {
	r.prefetchLeadBlocks.WithLabelValues(r.DatasetLabel(dataset)).Set(float64(n))
}

// IncConsumptionAdvance 记一次消费推进(D48;source∈{hook,inotify})。
func (r *Registry) IncConsumptionAdvance(dataset, source string) {
	if source != SourceHook && source != SourceInotify {
		return
	}
	r.consumptionAdvance.WithLabelValues(r.DatasetLabel(dataset), source).Inc()
}

// SetFetchBandwidthLimitBytes 设置回源字节信号量上限(D48)。
func (r *Registry) SetFetchBandwidthLimitBytes(dataset string, n int64) {
	r.fetchBandwidthLimit.WithLabelValues(r.DatasetLabel(dataset)).Set(float64(n))
}

// —— M8/M9 标签归一与校验 ——

// normalizePhase 归一 phase(D46):集合外 → unknown。
func normalizePhase(phase string) string {
	if validPhase(phase) {
		return phase
	}
	return PhaseUnknown
}

// validPhase 判断 phase ∈ {startup,steady,switch,unknown}。
func validPhase(phase string) bool {
	return validResult(phase, PhaseStartup, PhaseSteady, PhaseSwitch, PhaseUnknown)
}

// normalizeSource 归一命中来源(D46):集合外 → unknown。
func normalizeSource(source string) string {
	if source == SourcePrefetch || source == SourceCache || source == SourceUnknown {
		return source
	}
	return SourceUnknown
}

// normalizeFetchReason 归一回源 reason(D48):集合外 → other。
func normalizeFetchReason(reason string) string {
	switch reason {
	case FetchReasonPrefetch, FetchReasonDemand, FetchReasonRepair, FetchReasonOther:
		return reason
	default:
		return FetchReasonOther
	}
}

// validCause 判断 stall cause ∈ {local_slow,prefetch_lag,origin_slow,unknown}。
func validCause(cause string) bool {
	return validResult(cause, CauseLocalSlow, CausePrefetchLag, CauseOriginSlow, CauseUnknown)
}

// validPriority 判断 priority ∈ {P0,P1,P2,P3}(指标标签统一大写)。
func validPriority(priority string) bool {
	for _, p := range priorityLabels {
		if priority == p {
			return true
		}
	}
	return false
}

// —— 测试访问器(与 OrdersourceFormatErrors 同风格;生产读取走 /metrics)——

// CacheHits 返回命中 counter(测试访问器)。
func (r *Registry) CacheHits() *prometheus.CounterVec { return r.cacheHits }

// CacheMisses 返回未命中 counter(测试访问器)。
func (r *Registry) CacheMisses() *prometheus.CounterVec { return r.cacheMisses }

// CacheHitRatio 返回命中率 gauge(测试访问器)。
func (r *Registry) CacheHitRatio() *prometheus.GaugeVec { return r.cacheHitRatio }

// FetchBytes 返回回源字节 counter(测试访问器)。
func (r *Registry) FetchBytes() *prometheus.CounterVec { return r.fetchBytes }

// FetchRequests 返回回源请求 counter(测试访问器)。
func (r *Registry) FetchRequests() *prometheus.CounterVec { return r.fetchRequests }

// FetchInflightBytes 返回回源在途字节 gauge(测试访问器)。
func (r *Registry) FetchInflightBytes() *prometheus.GaugeVec { return r.fetchInflightBytes }

// IndexRefresh 返回索引刷新 counter(测试访问器)。
func (r *Registry) IndexRefresh() *prometheus.CounterVec { return r.indexRefresh }

// IndexRefreshDuration 返回索引刷新耗时 histogram(测试访问器)。
func (r *Registry) IndexRefreshDuration() *prometheus.HistogramVec { return r.indexRefreshDuration }

// IndexShardsLoaded 返回已加载分片数 gauge(测试访问器)。
func (r *Registry) IndexShardsLoaded() *prometheus.GaugeVec { return r.indexShardsLoaded }

// GCSweeps 返回 GC 清扫 counter(测试访问器)。
func (r *Registry) GCSweeps() *prometheus.CounterVec { return r.gcSweeps }

// GCBlocksRemoved 返回 GC 回收块 counter(测试访问器)。
func (r *Registry) GCBlocksRemoved() *prometheus.CounterVec { return r.gcBlocksRemoved }

// GCObjectsRemoved 返回 GC 回收对象 counter(测试访问器)。
func (r *Registry) GCObjectsRemoved() *prometheus.CounterVec { return r.gcObjectsRemoved }

// IndexSweepSkipped 返回 GC 跳过 counter(测试访问器)。
func (r *Registry) IndexSweepSkipped() *prometheus.CounterVec { return r.indexSweepSkipped }

// ObjectstoreConditionalWriteSupported 返回条件写支持 gauge(测试访问器)。
func (r *Registry) ObjectstoreConditionalWriteSupported() prometheus.Gauge {
	return r.objectstoreConditionalWriteSupported
}

// EvictBlocks 返回淘汰块 counter(测试访问器)。
func (r *Registry) EvictBlocks() *prometheus.CounterVec { return r.evictBlocks }

// EvictProtectedSkipped 返回受保护跳过 counter(测试访问器)。
func (r *Registry) EvictProtectedSkipped() *prometheus.CounterVec { return r.evictProtectedSkipped }

// RequestDuration 返回 RPC 耗时 histogram(测试访问器)。
func (r *Registry) RequestDuration() *prometheus.HistogramVec { return r.requestDuration }

// AgentUp 返回 agent_up gauge(测试访问器)。
func (r *Registry) AgentUp() *prometheus.GaugeVec { return r.agentUp }

// AgentDegraded 返回 agent_degraded gauge(测试访问器)。
func (r *Registry) AgentDegraded() *prometheus.GaugeVec { return r.agentDegraded }

// RescanProgress 返回重扫描进度 gauge(测试访问器)。
func (r *Registry) RescanProgress() prometheus.Gauge { return r.rescanProgress }

// QuarantinedBlocks 返回坏块数 gauge(测试访问器)。
func (r *Registry) QuarantinedBlocks() *prometheus.GaugeVec { return r.quarantined }

// DiskUsageRatio 返回磁盘利用率 gauge(测试访问器)。
func (r *Registry) DiskUsageRatio() *prometheus.GaugeVec { return r.diskUsageRatio }

// CapacityBytes 返回字节累计 gauge(测试访问器)。
func (r *Registry) CapacityBytes() *prometheus.GaugeVec { return r.capacityBytes }
