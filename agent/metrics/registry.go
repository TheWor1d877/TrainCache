// Package metrics 是 Prometheus 指标注册骨架(design §5.8;decisions D17)。
// M2 落容器与 dataset 白名单归并("other")及 ordersource 格式错误 counter(D23);
// M3 增 prefetch 队列深度/游标 gauge 与阶段切换停滞 histogram;
// M6 落七线 23 个 family 全量与专用打点 API(families.go)。
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// ordersource 格式错误 counter 的固定维度(decisions D23:counter,labels=["reason"]∈{magic,version};
// 不带 dataset 标签——坏文件读不出 dataset,防基数膨胀)。
const (
	// ordersourceFormatErrorsName 是 family 名(D23)。
	ordersourceFormatErrorsName = "traincache_ordersource_format_errors_total"
	// reason 标签取值,编译期固定(D23)。
	reasonMagic   = "magic"
	reasonVersion = "version"
)

// M3 预取线 family 名(design §5.8 节点状态线与延迟线;主对话冻结的三个打点方法)。
const (
	prefetchQueueDepthName = "traincache_prefetch_queue_depth"
	prefetchCursorName     = "traincache_prefetch_cursor"
	stageSwitchETAName     = "traincache_stage_switch_eta_seconds"
)

// 钩子帧格式错误 counter 的固定维度(decisions D33:counter,labels=["reason"]∈{parse,size};
// 与 D23 同风格,不带 dataset 标签——§6.3"解析失败丢弃并计数",防基数膨胀)。
const (
	// hookFormatErrorsName 是 family 名(D33)。
	hookFormatErrorsName = "traincache_hook_format_errors_total"
	// ReasonParse / ReasonSize 是 reason 标签取值(M5 hooksock 调用 IncHookFormatErrors 时
	// 引用,防字面量漂移):parse=帧 JSON 解析/字段校验失败,size=帧超过 4 KiB 上限(§5.4/§6.3)。
	ReasonParse = "parse"
	ReasonSize  = "size"
)

// priorityLabels 是 prefetch 队列深度 gauge 的 priority 标签取值:下标即内部优先级
// 0..3(P0>P1>P2>P3,design §5.2);标签取值编译期固定,不引入用户输入。
var priorityLabels = [4]string{"P0", "P1", "P2", "P3"}

// Config 是指标层配置(由 svc.Config 映射)。
type Config struct {
	// DatasetWhitelist 是 dataset 标签白名单;未命中的 dataset 一律归并为 "other"
	// (§5.8 基数纪律:标签编译期固定,不引入用户输入)。
	DatasetWhitelist []string
	// Node 是 node 标签取值(traincache_agent_up / traincache_agent_degraded;§5.8
	// "node 为第二正交维度"),由 svc 映射 NODE_NAME(§8:必填,启动校验非空)。
	// 空串照原样进标签(svc 契约保证非空)。
	Node string
}

// Registry 持有 dataset 白名单与 Prometheus 注册表(七线 23 个 family 在此挂载;
// 新增 family 的构造/注册/预置见 families.go 的 initSevenLine / initHooksAndEvidence;
// M9 后全量 34 个 family,含 D23/D33 两条格式错误线与 D44 读 miss 线)。
type Registry struct {
	whitelist map[string]struct{}
	reg       *prometheus.Registry
	node      string // traincache_agent_up / traincache_agent_degraded 的 node 标签值(Config.Node)

	// formatErrors 是 ordersource ring 头格式错误 counter(D23);New 内 MustRegister。
	formatErrors *prometheus.CounterVec

	// hookFormatErrors 是钩子帧格式错误 counter(D33,与 D23 同风格);New 内 MustRegister,
	// 两个 reason 子指标构造时预置 0。
	hookFormatErrors *prometheus.CounterVec

	// M3 预取线(New 内 MustRegister;队列深度构造时预置 4 个子指标为 0)。
	prefetchQueueDepth *prometheus.GaugeVec     // labels=["priority"]∈{P0..P3}
	prefetchCursor     prometheus.Gauge         // 值 = 游标块的 chunk 号
	stageSwitchStall   *prometheus.HistogramVec // stage_switch_eta_seconds;labels=["dataset"](M9/D46 补);prometheus.DefBuckets

	// 七线新增 20 个 family(design §5.8;M6;New 内构造/注册/预置,打点 API 见 families.go)。
	// ① 命中率线。
	cacheHits     *prometheus.CounterVec // dataset,result 固定 success,source,phase
	cacheMisses   *prometheus.CounterVec // dataset,result∈{success,refetched,error},phase
	cacheHitRatio *prometheus.GaugeVec   // dataset,phase;值由内部计数维护(见 muCacheRatio)
	// ② 回源线。
	fetchBytes         *prometheus.CounterVec // dataset,result 固定 success,reason
	fetchRequests      *prometheus.CounterVec // dataset,result∈{success,error},reason
	fetchInflightBytes *prometheus.GaugeVec   // dataset
	// ③ 索引线。
	indexRefresh         *prometheus.CounterVec   // dataset,result∈{success,error}
	indexRefreshDuration *prometheus.HistogramVec // dataset,result 固定 success;固定 bucket
	indexShardsLoaded    *prometheus.GaugeVec     // dataset
	// ④ GC 线。
	gcSweeps         *prometheus.CounterVec // dataset,result∈{success,error}
	gcBlocksRemoved  *prometheus.CounterVec // dataset,result 固定 success
	gcObjectsRemoved *prometheus.CounterVec // dataset,result 固定 success
	// ⑤ 磁盘淘汰线。
	evictBlocks           *prometheus.CounterVec // dataset,result∈{normal,danger}
	evictProtectedSkipped *prometheus.CounterVec // dataset,result∈{normal,danger}
	// ⑥ 延迟线(stageSwitchStall 字段见 M3,metric 名为 stage_switch_eta_seconds)。
	requestDuration *prometheus.HistogramVec // endpoint,result∈{success,error};固定 bucket
	// ⑦ 节点状态线(prefetchQueueDepth / prefetchCursor 见 M3 字段)。
	agentUp        *prometheus.GaugeVec // node;构造时预置 1
	agentDegraded  *prometheus.GaugeVec // node;构造时预置 0
	rescanProgress prometheus.Gauge     // 无标签
	quarantined    *prometheus.GaugeVec // dataset
	diskUsageRatio *prometheus.GaugeVec // dataset

	// M8/D44:读 miss 线(不带 dataset)。
	hookReadMiss *prometheus.CounterVec // reason∈{absent,corrupt}

	// M9/D46~D48:8 个证据/阶段 family。
	prefetchHitRatio     *prometheus.GaugeVec     // dataset
	readStall            *prometheus.HistogramVec // dataset,phase,cause
	fetchSingleflight    *prometheus.CounterVec   // dataset
	repairQueueDepth     *prometheus.GaugeVec     // dataset
	prefetchTaskDuration *prometheus.HistogramVec // dataset,priority
	prefetchLeadBlocks   *prometheus.GaugeVec     // dataset
	consumptionAdvance   *prometheus.CounterVec   // dataset,source
	fetchBandwidthLimit  *prometheus.GaugeVec     // dataset

	// mu 保护 synced:Prometheus counter 只能增,Sync 以"上次同步的绝对值"为基线做增量 Add。
	mu     sync.Mutex
	synced [2]int64 // [magic, version] 上次同步的绝对值

	// muCacheRatio 保护 cacheRatio:cache_hit_ratio gauge 由 Registry 内部计数
	// (按归一化 dataset + phase 记 hits/misses)维护,两 Observe 方法每次调用即时更新。
	muCacheRatio sync.Mutex
	cacheRatio   map[ratioKey]*hitMiss
}

// ratioKey 是 cache_hit_ratio 内部计数的键(dataset 已归一,phase 为编译期枚举)。
type ratioKey struct {
	dataset string
	phase   string
}

// New 构造注册骨架;白名单为空 → 所有 dataset 归并 "other"。
func New(cfg Config) *Registry {
	wl := make(map[string]struct{}, len(cfg.DatasetWhitelist))
	for _, d := range cfg.DatasetWhitelist {
		wl[d] = struct{}{}
	}
	r := &Registry{
		whitelist: wl,
		reg:       prometheus.NewRegistry(),
		node:      cfg.Node,
		formatErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: ordersourceFormatErrorsName,
			Help: "ordersource ring 头格式错误累计次数(reason=magic|version;decisions D23)",
		}, []string{"reason"}),
		hookFormatErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: hookFormatErrorsName,
			Help: "钩子帧格式错误累计次数(reason=parse|size;decisions D33)",
		}, []string{"reason"}),
		cacheRatio: make(map[ratioKey]*hitMiss),
	}
	r.prefetchQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prefetchQueueDepthName,
		Help: "预取器 P0..P3 各优先级队列长度(design §5.2 四优先级队列)",
	}, []string{"priority"})
	r.prefetchCursor = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: prefetchCursorName,
		Help: "预取器调度游标块的 chunk 号(调度进度,非 Status.Cursor 消费位点;design §5.2/D49)",
	})
	r.stageSwitchStall = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    stageSwitchETAName,
		Help:    "阶段切换 ETA 秒数(OnStageSwitch 返回的\"切换到新窗口就绪\"预估时间,非实测 stall;dataset 标签;design §5.2/D46)",
		Buckets: prometheus.DefBuckets,
	}, []string{"dataset"})
	r.reg.MustRegister(r.formatErrors, r.hookFormatErrors, r.prefetchQueueDepth, r.prefetchCursor, r.stageSwitchStall)
	// 预置两个 reason 子指标为 0:family 自启动即出现在 /metrics(无坏文件时也可见)。
	r.formatErrors.WithLabelValues(reasonMagic)
	r.formatErrors.WithLabelValues(reasonVersion)
	// 钩子帧错误 counter 同风格预置 reason∈{parse,size}(D33)。
	r.hookFormatErrors.WithLabelValues(ReasonParse)
	r.hookFormatErrors.WithLabelValues(ReasonSize)
	// 预置 P0..P3 四个队列深度子指标为 0(同上:family 自启动即可见)。
	for _, pri := range priorityLabels {
		r.prefetchQueueDepth.WithLabelValues(pri)
	}
	// stage_switch_eta 预置 dataset="other"(M9 补 dataset 标签后自启动可见)。
	r.stageSwitchStall.WithLabelValues(otherDatasetLabel)
	// 七线新增 20 个 family:构造、注册与预置(含 agent_up=1)。
	r.initSevenLine()
	// M8/D44 与 M9/D46~D48 新增 9 个 family。
	r.initHooksAndEvidence()
	return r
}

// DatasetLabel 返回 dataset 标签值:白名单命中返回原名,否则 "other"(§5.8/§6.5)。
func (r *Registry) DatasetLabel(dataset string) string {
	if _, ok := r.whitelist[dataset]; ok {
		return dataset
	}
	return otherDatasetLabel
}

// SyncOrdersourceFormatErrors 把 ordersource 包级累计计数以绝对值同步到 counter
// (D23:svc 每 tick 调用)。Prometheus counter 只增不降:内部记录上次同步值,按正增量 Add;
// 重复同步同一值幂等(绝对值语义;ordersource.FormatErrorTotals 是单调 atomic 计数)。
func (r *Registry) SyncOrdersourceFormatErrors(magic, version int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d := magic - r.synced[0]; d > 0 {
		r.formatErrors.WithLabelValues(reasonMagic).Add(float64(d))
	}
	r.synced[0] = magic
	if d := version - r.synced[1]; d > 0 {
		r.formatErrors.WithLabelValues(reasonVersion).Add(float64(d))
	}
	r.synced[1] = version
}

// OrdersourceFormatErrors 返回 ordersource 格式错误 counter(测试访问器;生产读取走 /metrics)。
func (r *Registry) OrdersourceFormatErrors() *prometheus.CounterVec { return r.formatErrors }

// IncHookFormatErrors 记一次钩子帧格式错误(traincache_hook_format_errors_total{reason},
// decisions D33;由 M5 hooksock 收端调用)。reason∈{parse,size}(见 ReasonParse / ReasonSize);
// 集合外忽略该次打点,不新增基数(标签取值编译期固定,与 SetPrefetchQueueDepth 越界忽略同风格)。
func (r *Registry) IncHookFormatErrors(reason string) {
	if reason != ReasonParse && reason != ReasonSize {
		return
	}
	r.hookFormatErrors.WithLabelValues(reason).Inc()
}

// HookFormatErrors 返回钩子帧格式错误 counter(测试访问器;生产读取走 /metrics)。
func (r *Registry) HookFormatErrors() *prometheus.CounterVec { return r.hookFormatErrors }

// SetPrefetchQueueDepth 设置某一优先级队列的深度(gauge traincache_prefetch_queue_depth;
// priority 取 0..3 内部编号,映射为标签 P0..P3)。越界优先级忽略:标签取值编译期固定,
// 不引入新基数。
func (r *Registry) SetPrefetchQueueDepth(priority int, depth int) {
	if priority < 0 || priority >= len(priorityLabels) {
		return
	}
	r.prefetchQueueDepth.WithLabelValues(priorityLabels[priority]).Set(float64(depth))
}

// SetPrefetchCursor 设置预取游标(gauge traincache_prefetch_cursor;值 = 游标块的 chunk 号)。
func (r *Registry) SetPrefetchCursor(cursor int64) {
	r.prefetchCursor.Set(float64(cursor))
}

// ObserveStageSwitchStall 记录一次阶段切换 ETA(histogram traincache_stage_switch_eta_seconds,
// prometheus.DefBuckets):值 = OnStageSwitch 返回的"切换到新窗口就绪"预估秒数(非实测 stall;
// 方法名沿用冻结签名)。
// M9/D46 起补 dataset 标签;datasets 为可选参数(缺省 → 归并值 other),兼容冻结的
// prefetch 调用点 p.m.ObserveStageSwitchStall(eta.Seconds())。
func (r *Registry) ObserveStageSwitchStall(seconds float64, datasets ...string) {
	dataset := ""
	if len(datasets) > 0 {
		dataset = datasets[0]
	}
	r.stageSwitchStall.WithLabelValues(r.DatasetLabel(dataset)).Observe(seconds)
}

// Prometheus 返回底层注册表(M6 挂载六线 family 与 /metrics HTTP handler 用)。
func (r *Registry) Prometheus() *prometheus.Registry { return r.reg }
