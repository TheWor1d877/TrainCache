package metrics_test

import (
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"traincache/agent/metrics"
)

// 七线新增 20 个 family 名(M6;测试侧独立字面量,防改名后测试跟着漂移。
// M3 复用的三个名字在 registry_test.go 中已定义)。
const (
	cacheHitsName             = "traincache_cache_hits_total"
	cacheMissesName           = "traincache_cache_misses_total"
	cacheHitRatioName         = "traincache_cache_hit_ratio"
	fetchBytesName            = "traincache_fetch_bytes_total"
	fetchRequestsName         = "traincache_fetch_requests_total"
	fetchInflightBytesName    = "traincache_fetch_inflight_bytes"
	indexRefreshName          = "traincache_index_refresh_total"
	indexRefreshDurationName  = "traincache_index_refresh_duration_seconds"
	indexShardsLoadedName     = "traincache_index_shards_loaded"
	gcSweepsName              = "traincache_gc_sweeps_total"
	gcBlocksRemovedName       = "traincache_gc_blocks_removed_total"
	gcObjectsRemovedName      = "traincache_gc_objects_removed_total"
	evictBlocksName           = "traincache_evict_blocks_total"
	evictProtectedSkippedName = "traincache_evict_protected_skipped_total"
	requestDurationName       = "traincache_request_duration_seconds"
	agentUpName               = "traincache_agent_up"
	agentDegradedName         = "traincache_agent_degraded"
	rescanProgressName        = "traincache_rescan_progress"
	quarantinedBlocksName     = "traincache_quarantined_blocks"
	diskUsageRatioName        = "traincache_disk_usage_ratio"
)

// M8/M9 新增 family 名(测试侧独立字面量,防改名后测试跟着漂移)。
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
)

// sevenLineFamilies 是 36 个 family → 标签名集合(design §5.8 冻结契约;无标签为 nil;
// 含 R3/R4 新增的 2 个)。
// M9/D46 变更 6 个既有 family 的标签(source/phase/reason/dataset),D44+D48 新增 9 个。
var sevenLineFamilies = map[string][]string{
	cacheHitsName:             {"dataset", "result", "source", "phase"},
	cacheMissesName:           {"dataset", "result", "phase"},
	cacheHitRatioName:         {"dataset", "phase"},
	fetchBytesName:            {"dataset", "result", "reason"},
	fetchRequestsName:         {"dataset", "result", "reason"},
	fetchInflightBytesName:    {"dataset"},
	indexRefreshName:          {"dataset", "result"},
	indexRefreshDurationName:  {"dataset", "result"},
	indexShardsLoadedName:     {"dataset"},
	gcSweepsName:              {"dataset", "result"},
	gcBlocksRemovedName:       {"dataset", "result"},
	gcObjectsRemovedName:      {"dataset", "result"},
	evictBlocksName:           {"dataset", "result"},
	evictProtectedSkippedName: {"dataset", "result"},
	requestDurationName:       {"endpoint", "result"},
	agentUpName:               {"node"},
	agentDegradedName:         {"node"},
	rescanProgressName:        nil,
	quarantinedBlocksName:     {"dataset"},
	diskUsageRatioName:        {"dataset"},
	// M3 复用(prefetch/阶段切换),标签形态一并冻结校验(M9 补 dataset):
	prefetchQueueDepthName: {"priority"},
	prefetchCursorName:     nil,
	stageSwitchETAName:     {"dataset"},
	// D44/D46/D47/D48。
	hookReadMissName:             {"reason"},
	prefetchHitRatioName:         {"dataset"},
	readStallName:                {"dataset", "phase", "cause"},
	fetchSingleflightMergedName:  {"dataset"},
	repairQueueDepthName:         {"dataset"},
	prefetchTaskDurationName:     {"dataset", "priority"},
	prefetchLeadBlocksName:       {"dataset"},
	consumptionAdvanceName:       {"dataset", "source"},
	fetchBandwidthLimitBytesName: {"dataset"},
	// R3/R4(7.1 §5.11/§5.12)。
	indexSweepSkippedName:                    {"dataset", "reason"},
	objectstoreConditionalWriteSupportedName: nil,
}

// sevenLineTypes 是七线 23 个 family 的类型冻结(Prometheus 文本枚举)。
var sevenLineTypes = map[string]string{
	cacheHitsName:                            "COUNTER",
	cacheMissesName:                          "COUNTER",
	cacheHitRatioName:                        "GAUGE",
	fetchBytesName:                           "COUNTER",
	fetchRequestsName:                        "COUNTER",
	fetchInflightBytesName:                   "GAUGE",
	indexRefreshName:                         "COUNTER",
	indexRefreshDurationName:                 "HISTOGRAM",
	indexShardsLoadedName:                    "GAUGE",
	gcSweepsName:                             "COUNTER",
	gcBlocksRemovedName:                      "COUNTER",
	gcObjectsRemovedName:                     "COUNTER",
	evictBlocksName:                          "COUNTER",
	evictProtectedSkippedName:                "COUNTER",
	requestDurationName:                      "HISTOGRAM",
	agentUpName:                              "GAUGE",
	agentDegradedName:                        "GAUGE",
	rescanProgressName:                       "GAUGE",
	quarantinedBlocksName:                    "GAUGE",
	diskUsageRatioName:                       "GAUGE",
	prefetchQueueDepthName:                   "GAUGE",
	prefetchCursorName:                       "GAUGE",
	stageSwitchETAName:                       "HISTOGRAM",
	hookReadMissName:                         "COUNTER",
	prefetchHitRatioName:                     "GAUGE",
	readStallName:                            "HISTOGRAM",
	fetchSingleflightMergedName:              "COUNTER",
	repairQueueDepthName:                     "GAUGE",
	prefetchTaskDurationName:                 "HISTOGRAM",
	prefetchLeadBlocksName:                   "GAUGE",
	consumptionAdvanceName:                   "COUNTER",
	fetchBandwidthLimitBytesName:             "GAUGE",
	indexSweepSkippedName:                    "COUNTER",
	objectstoreConditionalWriteSupportedName: "GAUGE",
}

// TestSevenLineFamiliesRegisteredAtStartup:构造后(未打点)gather 恰含 36 个 family
// (七线 23 + ordersource 格式错误 counter[D23] + 钩子帧格式错误 counter[D33] + D44 读 miss
// + M9 的 8 个证据 family + R3/R4 的 2 个);每个 family 的类型与标签名集合与冻结清单一致,且靠构造预置
// 自启动即有子指标(dataset 类预置值全为归并值 other;agent_up=1 / agent_degraded=0 取
// Config.Node)。
func TestSevenLineFamiliesRegisteredAtStartup(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}, Node: "node-1"})
	snap := snapshotFamilies(t, r)

	if len(snap) != 36 {
		t.Fatalf("注册表 family 数 = %d, want 36(七线 23 + D23 + D33 + D44 + M9 8 个 + R3/R4 2 个);实际 %v",
			len(snap), slices.Sorted(maps.Keys(snap)))
	}
	for _, name := range []string{ordersourceFormatErrorsName, hookFormatErrorsName} {
		if _, ok := snap[name]; !ok {
			t.Errorf("缺少非七线 family %q", name)
		}
	}

	for name, wantLabels := range sevenLineFamilies {
		f, ok := snap[name]
		if !ok {
			t.Errorf("缺少 family %q", name)
			continue
		}
		if f.typ != sevenLineTypes[name] {
			t.Errorf("family %q 类型 = %q, want %q", name, f.typ, sevenLineTypes[name])
		}
		if len(f.metrics) == 0 {
			t.Errorf("family %q 构造后应已预置子指标(自启动即在 /metrics 可见)", name)
			continue
		}
		for _, m := range f.metrics {
			if got := labelNameSet(m); !slices.Equal(got, sorted(wantLabels)) {
				t.Errorf("family %q 子指标标签名 = %v, want %v", name, got, wantLabels)
			}
		}
	}

	// 构造预置:dataset 类 family 的子指标一律 dataset="other"。
	for name, wantLabels := range sevenLineFamilies {
		if !slices.Contains(wantLabels, "dataset") {
			continue
		}
		for _, m := range family(t, snap, name).metrics {
			if m.labels["dataset"] != "other" {
				t.Errorf("family %q 构造预置 dataset = %q, want 归并值 other", name, m.labels["dataset"])
			}
		}
	}
	// 构造预置:agent_up=1(主对话冻结)、agent_degraded=0,node 标签取 Config.Node。
	if m := metric(t, snap, agentUpName, labels("node", "node-1")); m.value != 1 {
		t.Errorf("构造预置 agent_up{node=node-1} = %v, want 1", m.value)
	}
	if m := metric(t, snap, agentDegradedName, labels("node", "node-1")); m.value != 0 {
		t.Errorf("构造预置 agent_degraded{node=node-1} = %v, want 0", m.value)
	}
}

// TestSevenLinePointValues:打满一遍专用 API 后按 Gather 逐 family 断言点值:
// dataset 白名单命中原名、白名单外(打点入口)归并 other(全快照无白名单外 dataset 取值);
// result 取值集合逐 family 等于冻结词汇表;endpoint 取值恰为冻结六方法名;
// 命中率 gauge = 命中/(命中+未命中) 按 dataset 独立维护。
func TestSevenLinePointValues(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track", "b-track"}, Node: "node-1"})

	// ① 命中率线:a-track 3/4、b-track 1/2、other(c/d/e-track 归并)1/3。
	// D46:命中显式带 source/phase,未命中带 phase。
	r.ObserveCacheHit("a-track", metrics.SourcePrefetch, metrics.PhaseSteady)
	r.ObserveCacheHit("a-track", metrics.SourcePrefetch, metrics.PhaseSteady)
	r.ObserveCacheHit("a-track", metrics.SourcePrefetch, metrics.PhaseSteady)
	r.ObserveCacheMiss("a-track", metrics.ResultSuccess, metrics.PhaseSteady)
	r.ObserveCacheHit("b-track", metrics.SourceCache, metrics.PhaseSteady)
	r.ObserveCacheMiss("b-track", metrics.ResultError, metrics.PhaseSteady)
	r.ObserveCacheHit("c-track") // 缺省 source/phase → unknown/unknown
	r.ObserveCacheMiss("d-track", metrics.ResultRefetched)
	r.ObserveCacheMiss("e-track", metrics.ResultError)

	// ② 回源线。
	r.AddFetchBytes("a-track", 1024)
	r.AddFetchBytes("d-track", 512)
	r.IncFetchRequests("a-track", metrics.ResultSuccess)
	r.IncFetchRequests("d-track", metrics.ResultError)
	r.SetFetchInflightBytes("a-track", 2048)

	// ③ 索引线。
	r.IncIndexRefresh("a-track", metrics.ResultSuccess)
	r.ObserveIndexRefreshDuration("a-track", 0.3)
	r.SetIndexShardsLoaded("a-track", 12)

	// ④ GC 线。
	r.IncGCSweeps("a-track", metrics.ResultSuccess)
	r.IncGCSweeps("a-track", metrics.ResultError)
	r.AddGCBlocksRemoved("a-track", 3)
	r.AddGCObjectsRemoved("a-track", 5)

	// ⑤ 磁盘淘汰线。
	r.IncEvictBlocks("a-track", metrics.ResultNormal, 4)
	r.IncEvictBlocks("a-track", metrics.ResultDanger, 2)
	r.IncEvictProtectedSkipped("a-track", metrics.ResultNormal, 1)

	// ⑥ 延迟线。
	r.ObserveRequestDuration(metrics.EndpointSubmitPreheat, metrics.ResultSuccess, 0.2)
	r.ObserveRequestDuration(metrics.EndpointGetNodeStatus, metrics.ResultError, 1.5)

	// ⑦ 节点状态线。
	r.SetAgentUp(1)
	r.SetAgentDegraded(1)
	r.SetRescanProgress(0.42)
	r.SetQuarantined("a-track", 7)
	r.SetDiskUsageRatio("a-track", 0.66)

	snap := snapshotFamilies(t, r)

	// 打点入口归并:全快照 dataset 取值 ⊆ 白名单 + "other",白名单外原名不出现。
	for name, f := range snap {
		for _, m := range f.metrics {
			if d, ok := m.labels["dataset"]; ok && d != "a-track" && d != "b-track" && d != "other" {
				t.Errorf("family %q 出现白名单外 dataset 标签值 %q(应归并 other)", name, d)
			}
		}
	}

	wantCounters := []struct {
		family string
		labels map[string]string
		value  float64
	}{
		{cacheHitsName, labels("dataset", "a-track", "result", "success", "source", "prefetch", "phase", "steady"), 3},
		{cacheHitsName, labels("dataset", "b-track", "result", "success", "source", "cache", "phase", "steady"), 1},
		{cacheHitsName, labels("dataset", "other", "result", "success", "source", "unknown", "phase", "unknown"), 1},
		{cacheMissesName, labels("dataset", "a-track", "result", "success", "phase", "steady"), 1},
		{cacheMissesName, labels("dataset", "b-track", "result", "error", "phase", "steady"), 1},
		{cacheMissesName, labels("dataset", "other", "result", "refetched", "phase", "unknown"), 1},
		{cacheMissesName, labels("dataset", "other", "result", "error", "phase", "unknown"), 1},
		{fetchBytesName, labels("dataset", "a-track", "result", "success", "reason", "other"), 1024},
		{fetchBytesName, labels("dataset", "other", "result", "success", "reason", "other"), 512},
		{fetchRequestsName, labels("dataset", "a-track", "result", "success", "reason", "other"), 1},
		{fetchRequestsName, labels("dataset", "other", "result", "error", "reason", "other"), 1},
		{indexRefreshName, labels("dataset", "a-track", "result", "success"), 1},
		{gcSweepsName, labels("dataset", "a-track", "result", "success"), 1},
		{gcSweepsName, labels("dataset", "a-track", "result", "error"), 1},
		{gcBlocksRemovedName, labels("dataset", "a-track", "result", "success"), 3},
		{gcObjectsRemovedName, labels("dataset", "a-track", "result", "success"), 5},
		{evictBlocksName, labels("dataset", "a-track", "result", "normal"), 4},
		{evictBlocksName, labels("dataset", "a-track", "result", "danger"), 2},
		{evictProtectedSkippedName, labels("dataset", "a-track", "result", "normal"), 1},
	}
	for _, w := range wantCounters {
		m := metric(t, snap, w.family, w.labels)
		if !m.counter {
			t.Errorf("family %q 子指标 %v 不是 counter", w.family, w.labels)
		}
		if m.value != w.value {
			t.Errorf("%s%v = %v, want %v", w.family, w.labels, m.value, w.value)
		}
	}

	wantGauges := []struct {
		family string
		labels map[string]string
		value  float64
	}{
		{cacheHitRatioName, labels("dataset", "a-track", "phase", "steady"), 0.75},
		{cacheHitRatioName, labels("dataset", "b-track", "phase", "steady"), 0.5},
		{cacheHitRatioName, labels("dataset", "other", "phase", "unknown"), 1.0 / 3.0},
		{fetchInflightBytesName, labels("dataset", "a-track"), 2048},
		{indexShardsLoadedName, labels("dataset", "a-track"), 12},
		{agentUpName, labels("node", "node-1"), 1},
		{agentDegradedName, labels("node", "node-1"), 1},
		{rescanProgressName, labels(), 0.42},
		{quarantinedBlocksName, labels("dataset", "a-track"), 7},
		{diskUsageRatioName, labels("dataset", "a-track"), 0.66},
	}
	for _, w := range wantGauges {
		m := metric(t, snap, w.family, w.labels)
		if !m.gauge {
			t.Errorf("family %q 子指标 %v 不是 gauge", w.family, w.labels)
		}
		if math.Abs(m.value-w.value) > 1e-9 {
			t.Errorf("%s%v = %v, want %v", w.family, w.labels, m.value, w.value)
		}
	}

	wantHistograms := []struct {
		family string
		labels map[string]string
		count  uint64
		sum    float64
	}{
		{indexRefreshDurationName, labels("dataset", "a-track", "result", "success"), 1, 0.3},
		{requestDurationName, labels("endpoint", "SubmitPreheat", "result", "success"), 1, 0.2},
		{requestDurationName, labels("endpoint", "GetNodeStatus", "result", "error"), 1, 1.5},
	}
	for _, w := range wantHistograms {
		m := metric(t, snap, w.family, w.labels)
		if !m.histogram {
			t.Errorf("family %q 子指标 %v 不是 histogram", w.family, w.labels)
		}
		if m.count != w.count || math.Abs(m.sum-w.sum) > 1e-9 {
			t.Errorf("%s%v = (count=%d, sum=%v), want (count=%d, sum=%v)", w.family, w.labels, m.count, m.sum, w.count, w.sum)
		}
	}

	// result 取值集合逐 family 等于冻结词汇表(hits/fetch_bytes/index_refresh_duration/
	// gc_*_removed 的签名无 result 参数,固定 success)。
	wantResults := map[string][]string{
		cacheHitsName:             {metrics.ResultSuccess},
		cacheMissesName:           {metrics.ResultSuccess, metrics.ResultError, metrics.ResultRefetched},
		fetchBytesName:            {metrics.ResultSuccess},
		fetchRequestsName:         {metrics.ResultSuccess, metrics.ResultError},
		indexRefreshName:          {metrics.ResultSuccess, metrics.ResultError},
		indexRefreshDurationName:  {metrics.ResultSuccess},
		gcSweepsName:              {metrics.ResultSuccess, metrics.ResultError},
		gcBlocksRemovedName:       {metrics.ResultSuccess},
		gcObjectsRemovedName:      {metrics.ResultSuccess},
		evictBlocksName:           {metrics.ResultNormal, metrics.ResultDanger},
		evictProtectedSkippedName: {metrics.ResultNormal, metrics.ResultDanger},
		requestDurationName:       {metrics.ResultSuccess, metrics.ResultError},
	}
	for name, want := range wantResults {
		got := map[string]bool{}
		for _, m := range family(t, snap, name).metrics {
			got[m.labels["result"]] = true
		}
		if !maps.Equal(got, valueSet(want...)) {
			t.Errorf("family %q result 取值 = %v, want %v", name, slices.Sorted(maps.Keys(got)), want)
		}
	}

	// endpoint 取值恰为冻结六方法名。
	gotEndpoints := map[string]bool{}
	for _, m := range family(t, snap, requestDurationName).metrics {
		gotEndpoints[m.labels["endpoint"]] = true
	}
	wantEndpoints := valueSet(
		metrics.EndpointSubmitPreheat, metrics.EndpointGetPreheatStatus,
		metrics.EndpointListSnapshot, metrics.EndpointGetNodeStatus,
		metrics.EndpointUpdatePreheat, metrics.EndpointCancelPreheat,
	)
	if !maps.Equal(gotEndpoints, wantEndpoints) {
		t.Errorf("request_duration endpoint 取值 = %v, want %v",
			slices.Sorted(maps.Keys(gotEndpoints)), slices.Sorted(maps.Keys(wantEndpoints)))
	}
}

// TestSevenLineInvalidResultAndEndpointIgnored:result / endpoint 集合外取值忽略该次打点,
// 不产生任何新 series(标签基数编译期固定,与 SetPrefetchQueueDepth 越界忽略同风格)。
func TestSevenLineInvalidResultAndEndpointIgnored(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}, Node: "node-1"})

	r.IncGCSweeps("a-track", "notfound")
	r.IncFetchRequests("a-track", metrics.ResultRefetched) // refetched 仅属命中率线
	r.IncIndexRefresh("a-track", metrics.ResultNormal)
	r.IncEvictBlocks("a-track", metrics.ResultSuccess, 1)
	r.IncEvictProtectedSkipped("a-track", metrics.ResultError, 1)
	r.ObserveCacheMiss("a-track", "hit")
	r.ObserveCacheMiss("a-track", "")
	r.ObserveRequestDuration("ReportBlocks", metrics.ResultSuccess, 1) // ReportService(北向)不在冻结端点
	r.ObserveRequestDuration(metrics.EndpointGetNodeStatus, "timeout", 1)

	snap := snapshotFamilies(t, r)

	// 各 (dataset,result) family 仅剩构造预置(len(results) 个,dataset 全为 other)。
	for name, results := range map[string][]string{
		cacheHitsName:             {metrics.ResultSuccess},
		cacheMissesName:           {metrics.ResultSuccess, metrics.ResultError, metrics.ResultRefetched},
		fetchRequestsName:         {metrics.ResultSuccess, metrics.ResultError},
		indexRefreshName:          {metrics.ResultSuccess, metrics.ResultError},
		gcSweepsName:              {metrics.ResultSuccess, metrics.ResultError},
		evictBlocksName:           {metrics.ResultNormal, metrics.ResultDanger},
		evictProtectedSkippedName: {metrics.ResultNormal, metrics.ResultDanger},
	} {
		f := family(t, snap, name)
		if len(f.metrics) != len(results) {
			t.Errorf("family %q 子指标数 = %d, want %d(无效取值不应新增 series;实际 %v)",
				name, len(f.metrics), len(results), labelSetsString(f))
		}
		for _, m := range f.metrics {
			if m.labels["dataset"] != "other" {
				t.Errorf("family %q 不应出现 dataset=%q 的 series(无效取值被忽略)", name, m.labels["dataset"])
			}
		}
	}

	// 无效 result 不进命中率分母:other/unknown 仍为 0。
	if m := metric(t, snap, cacheHitRatioName, labels("dataset", "other", "phase", "unknown")); m.value != 0 {
		t.Errorf("无效 result 打点后 cache_hit_ratio{other,unknown} = %v, want 0", m.value)
	}
	// request_duration 仍只有构造预置的 12 个 series(6 endpoint × 2 result)。
	if n := len(family(t, snap, requestDurationName).metrics); n != 12 {
		t.Errorf("request_duration 子指标数 = %d, want 12(无效 endpoint/result 不应新增)", n)
	}
}

// TestSevenLineNonPositiveAddIgnored:Add/Inc 类方法的非正增量忽略(counter 只增,
// 负增量会 panic);正值正常累加。
func TestSevenLineNonPositiveAddIgnored(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})

	r.AddFetchBytes("a-track", 10)
	r.AddFetchBytes("a-track", 0)
	r.AddFetchBytes("a-track", -5)
	r.AddGCBlocksRemoved("a-track", 3)
	r.AddGCBlocksRemoved("a-track", -1)
	r.AddGCObjectsRemoved("a-track", 3)
	r.AddGCObjectsRemoved("a-track", 0)
	r.IncEvictBlocks("a-track", metrics.ResultNormal, 3)
	r.IncEvictBlocks("a-track", metrics.ResultNormal, -1)
	r.IncEvictProtectedSkipped("a-track", metrics.ResultDanger, 2)
	r.IncEvictProtectedSkipped("a-track", metrics.ResultDanger, 0)

	snap := snapshotFamilies(t, r)

	cases := []struct {
		family string
		labels map[string]string
		value  float64
	}{
		{fetchBytesName, labels("dataset", "a-track", "result", "success", "reason", "other"), 10},
		{gcBlocksRemovedName, labels("dataset", "a-track", "result", "success"), 3},
		{gcObjectsRemovedName, labels("dataset", "a-track", "result", "success"), 3},
		{evictBlocksName, labels("dataset", "a-track", "result", "normal"), 3},
		{evictProtectedSkippedName, labels("dataset", "a-track", "result", "danger"), 2},
	}
	for _, c := range cases {
		if m := metric(t, snap, c.family, c.labels); m.value != c.value {
			t.Errorf("%s%v = %v, want %v(非正增量应忽略)", c.family, c.labels, m.value, c.value)
		}
	}
	if hasMetric(snap, evictBlocksName, labels("dataset", "a-track", "result", "danger")) {
		t.Error("n=0 不应新增 evict_blocks{danger} series")
	}
}

// TestCacheHitRatioInternalCounting:cache_hit_ratio 由 Registry 内部计数维护
// (命中/总次数),按归一化 dataset 独立;失败未命中计入分母,非法 result 不计。
func TestCacheHitRatioInternalCounting(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})

	r.ObserveCacheHit("a-track")
	if got := ratioOf(t, r, "a-track"); got != 1 {
		t.Errorf("1 命中 0 未命中:ratio = %v, want 1", got)
	}
	r.ObserveCacheMiss("a-track", metrics.ResultError) // 失败也算未命中
	if got := ratioOf(t, r, "a-track"); got != 0.5 {
		t.Errorf("1 命中 1 未命中:ratio = %v, want 0.5", got)
	}
	r.ObserveCacheHit("a-track")
	r.ObserveCacheHit("a-track")
	if got := ratioOf(t, r, "a-track"); got != 0.75 {
		t.Errorf("3 命中 1 未命中:ratio = %v, want 0.75", got)
	}
	// 非法 result 不进分母。
	r.ObserveCacheMiss("a-track", "bogus")
	if got := ratioOf(t, r, "a-track"); got != 0.75 {
		t.Errorf("非法 result 后 ratio = %v, want 0.75 不变", got)
	}
	// 白名单外 dataset 独立计 other,互不影响。
	r.ObserveCacheHit("x-track")
	r.ObserveCacheMiss("x-track", metrics.ResultSuccess)
	if got := ratioOf(t, r, "other"); got != 0.5 {
		t.Errorf("other 1 命中 1 未命中:ratio = %v, want 0.5", got)
	}
	if got := ratioOf(t, r, "a-track"); got != 0.75 {
		t.Errorf("other 打点后 a-track ratio = %v, want 0.75 不变", got)
	}
}

// TestSevenLineFixedBuckets:延迟线 histogram 的固定 bucket 集 {.01,.05,.1,.5,1,5,10,30}s
// (design §5.8 冻结);index_refresh_duration 复用同一固定集;样本落桶正确。
func TestSevenLineFixedBuckets(t *testing.T) {
	want := []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30}
	r := metrics.New(metrics.Config{Node: "node-1"})

	r.ObserveRequestDuration(metrics.EndpointGetNodeStatus, metrics.ResultError, 0.03)
	r.ObserveIndexRefreshDuration("a-track", 12) // 白名单外 → other

	snap := snapshotFamilies(t, r)

	m := metric(t, snap, requestDurationName, labels("endpoint", "GetNodeStatus", "result", "error"))
	if !m.histogram {
		t.Fatalf("%s 子指标不是 histogram", requestDurationName)
	}
	if !slices.Equal(m.buckets, want) {
		t.Errorf("request_duration bucket 上界 = %v, want 固定集 %v", m.buckets, want)
	}
	// 0.03 秒落在 (.01,.05]:≤.01 桶计数 0,≤.05 桶计数 1。
	if m.bucketCounts[0] != 0 || m.bucketCounts[1] != 1 {
		t.Errorf("request_duration 0.03s 落桶 = %v, want 前两桶 [0 1]", m.bucketCounts[:2])
	}

	m = metric(t, snap, indexRefreshDurationName, labels("dataset", "other", "result", "success"))
	if !m.histogram {
		t.Fatalf("%s 子指标不是 histogram", indexRefreshDurationName)
	}
	if !slices.Equal(m.buckets, want) {
		t.Errorf("index_refresh_duration bucket 上界 = %v, want 固定集 %v", m.buckets, want)
	}
	// 12 秒落在 (10,30]:≤10 桶计数 0,≤30 桶计数 1。
	last := len(m.bucketCounts) - 1
	if m.bucketCounts[last-1] != 0 || m.bucketCounts[last] != 1 {
		t.Errorf("index_refresh_duration 12s 落桶 = %v, want 末两桶 [0 1]", m.bucketCounts[last-1:])
	}
}

// TestSevenLineConcurrentUpdates:-race 下并发打点(含命中率内部计数)计数不丢;
// 800 命中 / 800 未命中 → ratio 恰为 0.5。
func TestSevenLineConcurrentUpdates(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}, Node: "node-1"})

	const (
		workers = 8
		iters   = 100
	)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				r.ObserveCacheHit("a-track")
				r.ObserveCacheMiss("a-track", metrics.ResultSuccess)
				r.AddFetchBytes("a-track", 1)
				r.IncFetchRequests("a-track", metrics.ResultSuccess)
				r.IncGCSweeps("a-track", metrics.ResultError)
				r.IncEvictBlocks("a-track", metrics.ResultDanger, 1)
				r.ObserveRequestDuration(metrics.EndpointSubmitPreheat, metrics.ResultSuccess, 0.01)
				r.SetQuarantined("a-track", 1)
			}
		}()
	}
	wg.Wait()

	snap := snapshotFamilies(t, r)
	want := float64(workers * iters)
	if m := metric(t, snap, cacheHitsName, labels("dataset", "a-track", "result", "success", "source", "unknown", "phase", "unknown")); m.value != want {
		t.Errorf("并发后 hits = %v, want %v", m.value, want)
	}
	if m := metric(t, snap, fetchBytesName, labels("dataset", "a-track", "result", "success", "reason", "other")); m.value != want {
		t.Errorf("并发后 fetch_bytes = %v, want %v", m.value, want)
	}
	if m := metric(t, snap, evictBlocksName, labels("dataset", "a-track", "result", "danger")); m.value != want {
		t.Errorf("并发后 evict_blocks{danger} = %v, want %v", m.value, want)
	}
	if m := metric(t, snap, requestDurationName, labels("endpoint", "SubmitPreheat", "result", "success")); m.count != workers*iters {
		t.Errorf("并发后 request_duration 样本数 = %d, want %d", m.count, workers*iters)
	}
	if got := metric(t, snap, cacheHitRatioName, labels("dataset", "a-track", "phase", "unknown")).value; got != 0.5 {
		t.Errorf("并发后 cache_hit_ratio = %v, want 0.5", got)
	}
}

// TestSevenLineAccessors:20 个新增 family 的测试访问器均返回非 nil(与
// OrdersourceFormatErrors 同风格)。
func TestSevenLineAccessors(t *testing.T) {
	r := metrics.New(metrics.Config{Node: "node-1"})

	checks := []struct {
		name string
		v    any
	}{
		{"CacheHits", r.CacheHits()},
		{"CacheMisses", r.CacheMisses()},
		{"CacheHitRatio", r.CacheHitRatio()},
		{"FetchBytes", r.FetchBytes()},
		{"FetchRequests", r.FetchRequests()},
		{"FetchInflightBytes", r.FetchInflightBytes()},
		{"IndexRefresh", r.IndexRefresh()},
		{"IndexRefreshDuration", r.IndexRefreshDuration()},
		{"IndexShardsLoaded", r.IndexShardsLoaded()},
		{"GCSweeps", r.GCSweeps()},
		{"GCBlocksRemoved", r.GCBlocksRemoved()},
		{"GCObjectsRemoved", r.GCObjectsRemoved()},
		{"EvictBlocks", r.EvictBlocks()},
		{"EvictProtectedSkipped", r.EvictProtectedSkipped()},
		{"RequestDuration", r.RequestDuration()},
		{"AgentUp", r.AgentUp()},
		{"AgentDegraded", r.AgentDegraded()},
		{"RescanProgress", r.RescanProgress()},
		{"QuarantinedBlocks", r.QuarantinedBlocks()},
		{"DiskUsageRatio", r.DiskUsageRatio()},
	}
	for _, c := range checks {
		if c.v == nil || reflect.ValueOf(c.v).IsNil() {
			t.Errorf("访问器 %s() 返回 nil", c.name)
		}
	}
}

// TestEvidenceFamilyBuckets:M9 新增两个 histogram 的 bucket 逐字冻结
// (read_stall 0.1/0.25/0.5/1/2/5/10/30/60;task_duration 0.1/0.5/1/2/5/10/30/60/120)。
func TestEvidenceFamilyBuckets(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})

	r.ObserveReadStall("a-track", metrics.PhaseSwitch, metrics.CausePrefetchLag, 0.3)
	r.ObservePrefetchTaskDuration("a-track", "P1", 3)
	snap := snapshotFamilies(t, r)

	wantRead := []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60}
	m := metric(t, snap, readStallName, labels("dataset", "a-track", "phase", "switch", "cause", "prefetch_lag"))
	if !m.histogram || !slices.Equal(m.buckets, wantRead) {
		t.Errorf("read_stall bucket = %v, want %v", m.buckets, wantRead)
	}
	// 0.3 秒落在 (0.25,0.5]:≤0.25 计数 0,≤0.5 计数 1。
	if m.bucketCounts[1] != 0 || m.bucketCounts[2] != 1 {
		t.Errorf("read_stall 0.3s 落桶 = %v, want 第 3 桶起 1", m.bucketCounts[:3])
	}

	wantTask := []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120}
	m = metric(t, snap, prefetchTaskDurationName, labels("dataset", "a-track", "priority", "P1"))
	if !m.histogram || !slices.Equal(m.buckets, wantTask) {
		t.Errorf("prefetch_task_duration bucket = %v, want %v", m.buckets, wantTask)
	}
	// 3 秒落在 (2,5]。
	if m.bucketCounts[3] != 0 || m.bucketCounts[4] != 1 {
		t.Errorf("prefetch_task_duration 3s 落桶 = %v, want (2,5]", m.bucketCounts[:5])
	}
}

// TestEvidencePointValues:D44/D48 新增 family 的打点值逐项校验。
func TestEvidencePointValues(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})

	r.ObserveHookReadMiss(metrics.ReasonAbsent)
	r.ObserveHookReadMiss(metrics.ReasonAbsent)
	r.ObserveHookReadMiss(metrics.ReasonCorrupt)
	r.SetPrefetchHitRatio("a-track", 0.75)
	r.IncFetchSingleflightMerged("a-track")
	r.IncFetchSingleflightMerged("a-track")
	r.SetRepairQueueDepth("a-track", 3)
	r.ObservePrefetchTaskDuration("a-track", "P0", 0.4)
	r.SetPrefetchLeadBlocks("a-track", -2)
	r.IncConsumptionAdvance("a-track", metrics.SourceHook)
	r.IncConsumptionAdvance("a-track", metrics.SourceInotify)
	r.IncConsumptionAdvance("a-track", metrics.SourceInotify)
	r.SetFetchBandwidthLimitBytes("a-track", 1<<30)
	r.ObserveReadStall("a-track", metrics.PhaseSteady, metrics.CauseLocalSlow, 0.2)

	snap := snapshotFamilies(t, r)

	for _, c := range []struct {
		family string
		labels map[string]string
		value  float64
	}{
		{hookReadMissName, labels("reason", "absent"), 2},
		{hookReadMissName, labels("reason", "corrupt"), 1},
		{fetchSingleflightMergedName, labels("dataset", "a-track"), 2},
		{consumptionAdvanceName, labels("dataset", "a-track", "source", "hook"), 1},
		{consumptionAdvanceName, labels("dataset", "a-track", "source", "inotify"), 2},
	} {
		if m := metric(t, snap, c.family, c.labels); m.value != c.value {
			t.Errorf("%s%v = %v, want %v", c.family, c.labels, m.value, c.value)
		}
	}
	for _, c := range []struct {
		family string
		labels map[string]string
		value  float64
	}{
		{prefetchHitRatioName, labels("dataset", "a-track"), 0.75},
		{repairQueueDepthName, labels("dataset", "a-track"), 3},
		{prefetchLeadBlocksName, labels("dataset", "a-track"), -2},
		{fetchBandwidthLimitBytesName, labels("dataset", "a-track"), 1 << 30},
	} {
		if m := metric(t, snap, c.family, c.labels); m.value != c.value {
			t.Errorf("%s%v = %v, want %v", c.family, c.labels, m.value, c.value)
		}
	}
	if m := metric(t, snap, readStallName, labels("dataset", "a-track", "phase", "steady", "cause", "local_slow")); m.count != 1 || m.sum != 0.2 {
		t.Errorf("read_stall sample = (count=%d,sum=%v), want (1,0.2)", m.count, m.sum)
	}
}

// TestEvidenceInvalidLabelsIgnored:phase/cause/priority/source/reason 集合外取值忽略,
// 不新增 series(标签基数编译期固定);priority 只认大写 P0..P3。
func TestEvidenceInvalidLabelsIgnored(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})
	before := snapshotFamilies(t, r)
	count := func(name string) int { return len(family(t, before, name).metrics) }

	r.ObserveReadStall("a-track", "bogus", metrics.CauseLocalSlow, 1)
	r.ObserveReadStall("a-track", metrics.PhaseSteady, "bogus", 1)
	r.ObserveReadStall("a-track", metrics.PhaseSteady, metrics.CauseLocalSlow, -1)
	r.ObservePrefetchTaskDuration("a-track", "p0", 1)
	r.ObservePrefetchTaskDuration("a-track", "P9", 1)
	r.IncConsumptionAdvance("a-track", "bogus")
	r.IncConsumptionAdvance("a-track", "")
	r.ObserveHookReadMiss("bogus")

	after := snapshotFamilies(t, r)
	for _, name := range []string{readStallName, prefetchTaskDurationName, consumptionAdvanceName, hookReadMissName} {
		if got := len(family(t, after, name).metrics); got != count(name) {
			t.Errorf("family %q 子指标数 = %d, want %d(非法标签应忽略)", name, got, count(name))
		}
	}

	// 合法大写 priority 正常新增。
	r.ObservePrefetchTaskDuration("a-track", "P2", 2)
	after = snapshotFamilies(t, r)
	metric(t, after, prefetchTaskDurationName, labels("dataset", "a-track", "priority", "P2"))
}

// TestEvidenceDatasetMerge:新增 family 的 dataset 标签同样经白名单归并(白名单外 → other)。
func TestEvidenceDatasetMerge(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"a-track"}})

	r.SetPrefetchHitRatio("x-track", 0.5)
	r.IncFetchSingleflightMerged("x-track")
	r.SetRepairQueueDepth("x-track", 1)
	r.SetPrefetchLeadBlocks("x-track", 1)
	r.SetFetchBandwidthLimitBytes("x-track", 1)
	r.ObserveReadStall("x-track", metrics.PhaseSteady, metrics.CauseOriginSlow, 1)
	r.ObservePrefetchTaskDuration("x-track", "P3", 1)
	r.IncConsumptionAdvance("x-track", metrics.SourceHook)
	snap := snapshotFamilies(t, r)

	for _, name := range []string{
		prefetchHitRatioName, fetchSingleflightMergedName, repairQueueDepthName,
		prefetchLeadBlocksName, fetchBandwidthLimitBytesName, readStallName,
		prefetchTaskDurationName, consumptionAdvanceName,
	} {
		for _, m := range family(t, snap, name).metrics {
			if d := m.labels["dataset"]; d != "a-track" && d != "other" {
				t.Errorf("family %q 出现白名单外 dataset=%q(应归并 other)", name, d)
			}
		}
	}
	for _, name := range []string{prefetchHitRatioName, fetchSingleflightMergedName, repairQueueDepthName, prefetchLeadBlocksName, fetchBandwidthLimitBytesName} {
		metric(t, snap, name, labels("dataset", "other"))
	}
}

// —— 快照助手(不 import client_model:只在方法调用面读取 Gather 返回值)——

// metricSnapshot 是单个子指标的浅视图。
type metricSnapshot struct {
	labels       map[string]string
	counter      bool
	gauge        bool
	value        float64   // counter / gauge 值
	histogram    bool      // histogram 样本数、和、桶
	count        uint64    // histogram 样本数
	sum          float64   // histogram 样本和
	buckets      []float64 // histogram 桶上界(不含 +Inf)
	bucketCounts []float64 // histogram 累计桶计数(与 buckets 对齐)
}

// familySnapshot 是单个 family 的浅视图。
type familySnapshot struct {
	name    string
	typ     string // COUNTER / GAUGE / HISTOGRAM(Prometheus 文本枚举)
	metrics []metricSnapshot
}

// snapshotFamilies 把 Gather 结果摊平成按 family 名索引的浅视图。
func snapshotFamilies(t *testing.T, r *metrics.Registry) map[string]familySnapshot {
	t.Helper()
	families, err := r.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]familySnapshot, len(families))
	for _, mf := range families {
		f := familySnapshot{name: mf.GetName(), typ: mf.GetType().String()}
		for _, m := range mf.GetMetric() {
			ms := metricSnapshot{labels: make(map[string]string, len(m.GetLabel()))}
			for _, lp := range m.GetLabel() {
				ms.labels[lp.GetName()] = lp.GetValue()
			}
			switch {
			case m.GetCounter() != nil:
				ms.counter = true
				ms.value = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				ms.gauge = true
				ms.value = m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				h := m.GetHistogram()
				ms.histogram = true
				ms.count = h.GetSampleCount()
				ms.sum = h.GetSampleSum()
				for _, b := range h.GetBucket() {
					if math.IsInf(b.GetUpperBound(), +1) {
						continue
					}
					ms.buckets = append(ms.buckets, b.GetUpperBound())
					ms.bucketCounts = append(ms.bucketCounts, float64(b.GetCumulativeCount()))
				}
			}
			f.metrics = append(f.metrics, ms)
		}
		out[f.name] = f
	}
	return out
}

// family 取指定 family(不存在即 Fail)。
func family(t *testing.T, snap map[string]familySnapshot, name string) familySnapshot {
	t.Helper()
	f, ok := snap[name]
	if !ok {
		t.Fatalf("family %q 不存在(现有 %v)", name, slices.Sorted(maps.Keys(snap)))
	}
	return f
}

// metric 取指定 family 下标签集合恰为 labels 的子指标(不存在即 Fail)。
func metric(t *testing.T, snap map[string]familySnapshot, name string, labels map[string]string) metricSnapshot {
	t.Helper()
	f := family(t, snap, name)
	for _, m := range f.metrics {
		if maps.Equal(m.labels, labels) {
			return m
		}
	}
	t.Fatalf("family %q 无标签集合 %v 的子指标(现有 %v)", name, sortedMap(labels), labelSetsString(f))
	return metricSnapshot{}
}

// hasMetric 判断指定 family 下是否存在标签集合恰为 labels 的子指标。
func hasMetric(snap map[string]familySnapshot, name string, labels map[string]string) bool {
	f, ok := snap[name]
	if !ok {
		return false
	}
	for _, m := range f.metrics {
		if maps.Equal(m.labels, labels) {
			return true
		}
	}
	return false
}

// ratioOf 从 Gather 读出 cache_hit_ratio{dataset} 的当前值(新构造 Registry,仅测试用)。
func ratioOf(t *testing.T, r *metrics.Registry, dataset string) float64 {
	t.Helper()
	snap := snapshotFamilies(t, r)
	return metric(t, snap, cacheHitRatioName, labels("dataset", dataset, "phase", "unknown")).value
}

// labels 由 k,v 交替参数构造标签集合。
func labels(kv ...string) map[string]string {
	m := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// valueSet 构造 string 集合。
func valueSet(vals ...string) map[string]bool {
	s := make(map[string]bool, len(vals))
	for _, v := range vals {
		s[v] = true
	}
	return s
}

// labelNameSet 返回子指标标签名(升序)。
func labelNameSet(m metricSnapshot) []string {
	return sorted(slices.Collect(maps.Keys(m.labels)))
}

// sorted 返回升序副本(nil 输入得空切片;slices.Equal 不区分 nil 与空)。
func sorted(vals []string) []string {
	return slices.Sorted(slices.Values(vals))
}

// sortedMap 把标签集合渲染为升序 "k=v" 列表(错误信息用)。
func sortedMap(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return sorted(out)
}

// labelSetsString 把 family 的全部子指标标签集合渲染为升序列表(错误信息用)。
func labelSetsString(f familySnapshot) []string {
	out := make([]string, 0, len(f.metrics))
	for _, m := range f.metrics {
		out = append(out, "{"+strings.Join(sortedMap(m.labels), ",")+"}")
	}
	return sorted(out)
}
