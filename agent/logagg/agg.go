// Package logagg 汇总 Agent 高频路径的日志(docs/8.1 design.md §3.3/§4.2)。
//
// 口径:逐块、逐次明细一律走 Debug(采样由 core/logging 的 handler 统一按 1/100 做,
// 本包不自造随机数);Info 只在 60s 窗口到期时输出一条汇总,任何情况下都不逐块写 Info。
// 两类汇总共用一个调度器(一条 ticker,见 Start):
//   - 逐块下载/校验:按 (task_id, dataset, reason) 累加块数、字节数、失败数与单块耗时,
//     窗口到期或任务结束时输出一条 Info(含 P50/P99);
//   - 缓存命中/未命中/read_miss:按 (dataset, reason, phase, source) 累加,窗口到期输出。
//
// 计数只服务日志汇总:指标仍由 metrics.Registry 与 collector 现有打点提供,本包不新增
// 指标计数源。Warn/Error 不经过本包,失败日志保持原级别与文案。
package logagg

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	// DefaultInterval 是 60s 汇总窗口(§3.3 固定值,不开放配置)。
	DefaultInterval = 60 * time.Second
	// maxDurationSamples 限制单桶保留的耗时样本,防止超长窗口下无界增长;
	// 超出后仍计数,只是不再参与分位计算。
	maxDurationSamples = 65536
	// defaultDownloadReason 与预取器默认回源 reason 对齐。
	defaultDownloadReason = "prefetch"
)

// Download 是一条逐块下载/校验完成记录(成功或失败)。
type Download struct {
	TaskID   string        // 预热任务 ID;非 P0 预取块为空
	Dataset  string        // 数据集
	Path     string        // 块路径(仅 Debug 明细使用)
	Chunk    int64         // 块号
	Reason   string        // 回源 reason:prefetch|demand|repair
	Size     int64         // 块字节数
	Ready    bool          // 就绪(本地命中或下载成功)
	Duration time.Duration // 单块抓取/校验耗时
}

// Options 是聚合器构造参数。
type Options struct {
	Interval time.Duration    // 汇总窗口,≤0 取 DefaultInterval
	Now      func() time.Time // 时间源(测试注入可控时钟;nil 取 time.Now)
	Verify   string           // 校验等级(xxh3 / xxh3+sha256),汇总字段 verify
	Logger   *slog.Logger     // 输出 logger;nil 取 slog.Default
}

// downloadKey 是下载汇总的分组键:同一任务、同一 dataset、同一 reason 合成一条。
type downloadKey struct {
	taskID  string
	dataset string
	reason  string
}

// downloadBucket 是一个窗口内的下载计数。
type downloadBucket struct {
	chunks    int64
	bytes     int64
	failed    int64
	durations []float64 // 毫秒;单块耗时样本(P50/P99)
}

// cacheKey 是缓存命中汇总的分组键。
type cacheKey struct {
	dataset string
	reason  string
	phase   string
	source  string
}

// cacheBucket 是一个窗口内的命中/未命中/read_miss 计数。
type cacheBucket struct {
	hits     int64
	misses   int64
	readMiss int64
}

// Aggregator 是两类日志汇总共用的 60s 聚合器。零值不可用,须经 New 构造。
type Aggregator struct {
	interval time.Duration
	now      func() time.Time
	verify   string
	logger   *slog.Logger

	mu        sync.Mutex
	started   bool
	lastFlush time.Time
	downloads map[downloadKey]*downloadBucket
	cache     map[cacheKey]*cacheBucket
}

// New 构造聚合器。
func New(opts Options) *Aggregator {
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Aggregator{
		interval:  interval,
		now:       now,
		verify:    opts.Verify,
		logger:    opts.Logger,
		lastFlush: now(),
		downloads: make(map[downloadKey]*downloadBucket),
		cache:     make(map[cacheKey]*cacheBucket),
	}
}

// Start 启动唯一的 60s 调度协程(ctx 取消即退出);重复调用无副作用。
func (a *Aggregator) Start(ctx context.Context) {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return
	}
	a.started = true
	a.lastFlush = a.now()
	a.mu.Unlock()
	go a.run(ctx)
}

// run 是唯一的窗口调度循环:到点调用 Tick,不另起 goroutine。
func (a *Aggregator) run(ctx context.Context) {
	t := time.NewTicker(a.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.Tick()
		}
	}
}

// Tick 在窗口到期时输出汇总;距上次刷新不足一个窗口时直接返回(测试以可控时钟驱动)。
func (a *Aggregator) Tick() {
	if a == nil {
		return
	}
	now := a.now()
	a.mu.Lock()
	if !a.lastFlush.IsZero() && now.Sub(a.lastFlush) < a.interval {
		a.mu.Unlock()
		return
	}
	a.lastFlush = now
	downloads := a.takeDownloadsLocked("")
	cache := a.takeCacheLocked()
	a.mu.Unlock()
	a.emitDownloads(downloads)
	a.emitCache(cache)
}

// Flush 立即输出全部汇总(窗口未到期也输出;仅供测试与关停前收尾)。
func (a *Aggregator) Flush() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.lastFlush = a.now()
	downloads := a.takeDownloadsLocked("")
	cache := a.takeCacheLocked()
	a.mu.Unlock()
	a.emitDownloads(downloads)
	a.emitCache(cache)
}

// FlushTask 在任务结束时提前输出该任务的下载汇总(其余任务与缓存桶留到窗口到期)。
func (a *Aggregator) FlushTask(taskID string) {
	if a == nil || taskID == "" {
		return
	}
	a.mu.Lock()
	downloads := a.takeDownloadsLocked(taskID)
	a.mu.Unlock()
	a.emitDownloads(downloads)
}

// ObserveDownload 累加一条逐块下载/校验完成记录,并按 Debug 输出逐块明细
// (是否真正落盘由 core/logging 的 1/100 采样决定)。
func (a *Aggregator) ObserveDownload(d Download) {
	if a == nil {
		return
	}
	reason := d.Reason
	if reason == "" {
		reason = defaultDownloadReason
	}
	key := downloadKey{taskID: d.TaskID, dataset: d.Dataset, reason: reason}
	a.mu.Lock()
	b := a.downloads[key]
	if b == nil {
		b = &downloadBucket{}
		a.downloads[key] = b
	}
	b.chunks++
	if d.Ready {
		b.bytes += d.Size
	} else {
		b.failed++
	}
	if len(b.durations) < maxDurationSamples {
		b.durations = append(b.durations, durationMS(d.Duration))
	}
	a.mu.Unlock()

	a.log().Debug("prefetch: 块完成",
		"task_id", d.TaskID,
		"dataset", d.Dataset,
		"path", d.Path,
		"chunk", d.Chunk,
		"size", d.Size,
		"verify", a.verify,
		"reason", reason,
		"duration_ms", durationMS(d.Duration),
		"ok", d.Ready)
}

// ObserveCacheHit 记一次缓存命中(指标已由 collector 打点,这里只做汇总与明细)。
func (a *Aggregator) ObserveCacheHit(dataset string, chunk int64, source, phase string) {
	if a == nil {
		return
	}
	a.addCache(cacheKey{dataset: dataset, reason: "hit", phase: phase, source: source}, true, false)
	a.log().Debug("cache: 命中明细",
		"dataset", dataset, "chunk", chunk, "reason", "hit",
		"phase", phase, "source", source)
}

// ObserveCacheMiss 记一次缓存未命中。
func (a *Aggregator) ObserveCacheMiss(dataset string, chunk int64, phase string) {
	if a == nil {
		return
	}
	a.addCache(cacheKey{dataset: dataset, reason: "miss", phase: phase}, false, false)
	a.log().Debug("cache: 未命中明细",
		"dataset", dataset, "chunk", chunk, "reason", "miss", "phase", phase)
}

// ObserveReadMiss 记一次钩子 read_miss(reason = absent|corrupt)。
func (a *Aggregator) ObserveReadMiss(dataset string, chunk int64, reason, phase string) {
	if a == nil {
		return
	}
	a.addCache(cacheKey{dataset: dataset, reason: reason, phase: phase}, false, true)
	a.log().Debug("cache: read_miss 明细",
		"dataset", dataset, "chunk", chunk, "reason", reason, "phase", phase)
}

// addCache 在锁内累加缓存计数。
func (a *Aggregator) addCache(key cacheKey, hit, readMiss bool) {
	a.mu.Lock()
	b := a.cache[key]
	if b == nil {
		b = &cacheBucket{}
		a.cache[key] = b
	}
	switch {
	case hit:
		b.hits++
	case readMiss:
		b.readMiss++
	default:
		b.misses++
	}
	a.mu.Unlock()
}

// takeDownloadsLocked 摘取(并删除)指定任务的下载桶;taskID 为空表示全部。
func (a *Aggregator) takeDownloadsLocked(taskID string) map[downloadKey]downloadBucket {
	out := make(map[downloadKey]downloadBucket)
	for key, b := range a.downloads {
		if taskID != "" && key.taskID != taskID {
			continue
		}
		out[key] = *b
		delete(a.downloads, key)
	}
	return out
}

// takeCacheLocked 摘取(并删除)全部缓存桶。
func (a *Aggregator) takeCacheLocked() map[cacheKey]cacheBucket {
	out := make(map[cacheKey]cacheBucket, len(a.cache))
	for key, b := range a.cache {
		out[key] = *b
		delete(a.cache, key)
	}
	return out
}

// emitDownloads 按稳定顺序输出下载汇总(每个分组一条 Info)。
func (a *Aggregator) emitDownloads(buckets map[downloadKey]downloadBucket) {
	if len(buckets) == 0 {
		return
	}
	keys := make([]downloadKey, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].taskID != keys[j].taskID {
			return keys[i].taskID < keys[j].taskID
		}
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		return keys[i].reason < keys[j].reason
	})
	log := a.log()
	for _, key := range keys {
		b := buckets[key]
		p50, p99 := percentiles(b.durations)
		log.Info("prefetch: 下载汇总",
			"task_id", key.taskID,
			"dataset", key.dataset,
			"chunk", b.chunks,
			"size", b.bytes,
			"verify", a.verify,
			"reason", key.reason,
			"failed", b.failed,
			"duration_ms_p50", p50,
			"duration_ms_p99", p99)
	}
}

// emitCache 按稳定顺序输出缓存命中汇总(每个 dataset×reason×phase×source 一条 Info)。
func (a *Aggregator) emitCache(buckets map[cacheKey]cacheBucket) {
	if len(buckets) == 0 {
		return
	}
	keys := make([]cacheKey, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		switch {
		case keys[i].dataset != keys[j].dataset:
			return keys[i].dataset < keys[j].dataset
		case keys[i].reason != keys[j].reason:
			return keys[i].reason < keys[j].reason
		case keys[i].phase != keys[j].phase:
			return keys[i].phase < keys[j].phase
		default:
			return keys[i].source < keys[j].source
		}
	})
	log := a.log()
	for _, key := range keys {
		b := buckets[key]
		attrs := []any{
			"dataset", key.dataset,
			"reason", key.reason,
			"phase", key.phase,
			"hits", b.hits,
			"misses", b.misses,
			"read_miss", b.readMiss,
		}
		if key.source != "" {
			attrs = append(attrs, "source", key.source)
		}
		log.Info("cache: 命中汇总", attrs...)
	}
}

// log 返回输出 logger(构造时注入优先,否则跟随 slog.Default)。
func (a *Aggregator) log() *slog.Logger {
	if a.logger != nil {
		return a.logger
	}
	return slog.Default()
}

// durationMS 把耗时换算为毫秒并保留 3 位小数。
func durationMS(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000
}

// percentiles 返回 P50/P99(输入为未排序的毫秒样本;空集返回 0)。
func percentiles(samples []float64) (float64, float64) {
	if len(samples) == 0 {
		return 0, 0
	}
	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	sort.Float64s(sorted)
	return pick(sorted, 0.50), pick(sorted, 0.99)
}

// pick 取最近秩分位(idx = ceil(p·n) − 1,夹在 [0,n-1])。
func pick(sorted []float64, p float64) float64 {
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
