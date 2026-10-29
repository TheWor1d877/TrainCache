// Package collector 是 Agent 的采集层。
//
// collector 不产生订单数据，也不持有 ring。它接收接线层推入的事件，完成统计并把
// 热度图持久化；订单上下文（dataset / stage / 期望顺序）由装配层经 provider 注入，
// 没有订单时统计照常，标签归 unknown。
//
// 职责：
//   - 热度映射与淘汰得分：记录数据块被打开、预取、消费的情况，算出热度与淘汰得分；
//   - 命中率统计：滚动窗口内统计，并按训练阶段聚合；
//   - 交叉校准：实际打开顺序与订单期望顺序比对，失配时回调重对齐；
//   - heatmap.json：周期 dump，启动时恢复。
//
// 输入事件（由 svc 接线层调用下列方法推入 collector）：
//   - Open(fp)：打开事件。主源是钩子 open_report，接线层把 (path, chunk) 经索引
//     解析为指纹后调用；兜底源是 inotify，Start 后监听 blockDir/blocks 下的 IN_OPEN。
//   - Touch(fp, t)：预取写入与订单刷新事件，t_touch 取三源最大值。
//   - Advance(chunk)：消费侧推进事件，来自钩子 advance 帧，只做推进计数。
//
// 装配与边界：
//   - collector 跑在 Go Agent 进程内，由 svc 创建并接线；
//   - 订单通道健康由 svc 聚合，collector 不判定、不写 degraded gauge；
//   - NewCollector 只接收 blockDir / metrics / 参数；订单上下文经
//     SetOrderContextProvider 注入，多 Pod（多 ring）的聚合在装配层完成。
//
// 设计见 design §5.5/§6.4/§7.4，决策见 D14/D15/D24/D31/D63。

package collector

import (
	"context"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"traincache/agent/logagg"
	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// 热度模型默认值(design §5.5 表;D14/D24)。CollectorConfig 中 ≤0 的字段一律取默认。
const (
	defaultHeatDecayAge       = 24 * time.Hour   // N:超龄未 touch 直接衰减
	defaultHeatDumpInterval   = 30 * time.Minute // DumpInterval:heatmap.json 周期
	defaultHeatDecayInterval  = 24 * time.Hour   // T:周期衰减间隔
	defaultHeatDecayFactor    = 0.5              // 周期衰减因子
	defaultHeatOpenBeta       = 0.15             // β:单次打开热度增量系数
	defaultHeatTouchHalfLife  = 6 * time.Hour    // T_R:新鲜度半衰期
	defaultCalibrateWindow    = 100              // 交叉校准滑动窗口(打开次数)
	defaultCalibrateThreshold = 0.8              // 匹配率低于该值触发重对齐
)

// 目录与文件名常量(design §6.4、core cas 块布局)。
const (
	// blocksDirName 是 cas 块目录名:blockDir/blocks/<fp[0:2]hex>/<fp hex>.blk。
	blocksDirName = "blocks"
	// heatmapFileName 是热力图文件名:blockDir/heatmap.json,默认即
	// /var/cache/traincache/heatmap.json(blockDir 为 cas 根目录,§6.4)。
	heatmapFileName = "heatmap.json"
)

// CollectorConfig 是采集层参数(design §5.5 冻结;≤0 取默认值)。
type CollectorConfig struct {
	HeatDecayAge       time.Duration // 超过 N 的记录直接衰减热度,默认 24h
	HeatDumpInterval   time.Duration // 每 DumpInterval dump 一次,默认 30min
	HeatDecayInterval  time.Duration // 每隔 T 全体乘衰减因子,默认 24h
	HeatDecayFactor    float64       // 默认 0.5
	HeatOpenBeta       float64       // 单次打开的热度增量系数 β,默认 0.15(D24)
	HeatTouchHalfLife  time.Duration // 新鲜度 R 半衰期 T_R,默认 6h(D24)
	CalibrateWindow    int           // 交叉校准滑动窗口,默认 100 次打开
	CalibrateThreshold float64       // 匹配率低于该值重对齐,默认 0.8
}

// EvictionEntry 是淘汰候选快照条目(D31 冻结签名;M6 淘汰循环按同签名窄接口消费,不 import 本包)。
// M4 增 Accesses 字段供 LFU 档策略消费。
type EvictionEntry struct {
	FP        cas.Fingerprint
	Heat      float64
	LastTouch time.Time
	Accesses  int64
}

// heatEntry 是单指纹的热度记录(h ∈ [0,1):β 增量收敛到 1;access_count 仅 dump 用)。
type heatEntry struct {
	heat        float64
	lastTouch   time.Time
	accessCount int64
}

// Collector 是采集层主体:热度表、命中率窗口与校准器由 Start 起的后台循环驱动
// (decay/dump/inotify)。构造后即可用,Start 每实例只生效一次。
type Collector struct {
	blockDir    string
	blocksDir   string
	heatmapPath string

	m   *metrics.Registry
	cfg CollectorConfig

	// clock 是时间源(可注入,测试用;生产默认 time.Now)。
	clock func() time.Time

	// 后台循环周期与门控窗:构造时从包内变量固化(测试改包变量后构造即可覆盖),
	// 避免运行中并发读写包变量。
	watchInterval time.Duration // inotify 读轮询周期
	hookSilence   time.Duration // 钩子主源沉默窗(inotify 兜底生效条件)

	mu         sync.Mutex
	entries    map[cas.Fingerprint]*heatEntry
	lastHookOp time.Time // 钩子主源最近一次打开事件(零值 = 从未;inotify 兜底判定用)

	advMu     sync.Mutex
	advanceCb func(dataset string, ref ordersource.ChunkRef)
	advances  uint64 // advance 事件计数

	// orderCtx 是订单上下文来源(装配期写入一次,打开路径并发读;nil = 未注入)。
	orderCtx atomic.Pointer[orderContextProvider]

	openMu     sync.Mutex
	openCb     func(fp cas.Fingerprint)      // inotify 兜底打开回调(仅在门控通过后触发)
	openFilter func(fp cas.Fingerprint) bool // 回调前过滤(D49:Agent 自打开不触发回调)

	hr  *hitRate
	cal *calibrator

	// logAgg 是 60s 汇总聚合器(8.1 §4.2;装配期注入一次,读路径原子读)。nil 安全。
	logAgg atomic.Pointer[logagg.Aggregator]


	started atomic.Bool // Start 幂等守卫
}

// CacheLabeler 是命中打点的 source/phase 标签适配(D46):collector 负责事件与计数,
// svc 装配期注入判定器(命中来源依赖预取窗口与 phaseTracker,不属于采集层)。
type CacheLabeler struct {
	// Source 返回 metrics.SourcePrefetch / SourceCache / SourceUnknown。
	Source func(dataset string, fp cas.Fingerprint, ref ordersource.ChunkRef) string
	// Phase 返回 metrics.Phase* 之一。
	Phase func(dataset string) string
}

// cacheLabeler 是进程级注入口(atomic;装配期写入一次,读路径并发读)。
// 未注入时 noteOpen 显式传 unknown/unknown,不引入额外标签基数。
var cacheLabeler atomic.Pointer[CacheLabeler]

// SetCacheLabeler 注入命中标签判定器(传 nil 解除)。装配期调用一次。
func SetCacheLabeler(l *CacheLabeler) {
	if l == nil {
		cacheLabeler.Store(nil)
		return
	}
	cacheLabeler.Store(l)
}

// cacheLabels 取本次命中/未命中的 source 与 phase(D46;缺省 unknown)。
func (c *Collector) cacheLabels(dataset string, fp cas.Fingerprint, ref ordersource.ChunkRef) (source, phase string) {
	source, phase = metrics.SourceUnknown, metrics.PhaseUnknown
	if l := cacheLabeler.Load(); l != nil {
		if l.Source != nil {
			source = l.Source(dataset, fp, ref)
		}
		if l.Phase != nil {
			phase = l.Phase(dataset)
		}
	}
	return source, phase
}

// orderContextProvider 是装配层注入的订单上下文来源:按本次打开的引用与指纹返回
// dataset / StageID / 订单期望顺序;ok=false 表示当前没有可用订单。
type orderContextProvider func(ref ordersource.ChunkRef, fp cas.Fingerprint) (dataset, stage string, order ordersource.ConsumeOrder, ok bool)

// SetOrderContextProvider 注入订单上下文来源(传 nil 解除)。装配期写入一次,
// 打开路径并发读;未注入时 dataset/stage 为空、无订单,统计照常、标签归 unknown。
func (c *Collector) SetOrderContextProvider(fn func(ref ordersource.ChunkRef, fp cas.Fingerprint) (dataset, stage string, order ordersource.ConsumeOrder, ok bool)) {
	if fn == nil {
		c.orderCtx.Store(nil)
		return
	}
	p := orderContextProvider(fn)
	c.orderCtx.Store(&p)
}

// orderInfo 取本次打开的订单上下文(provider 未注入 → 空值与无订单)。provider 返回
// !ok 时仍保留 dataset/stage(命中标签可用),只把订单视为不可用(不参与交叉校准)。
func (c *Collector) orderInfo(ref ordersource.ChunkRef, fp cas.Fingerprint) (dataset, stage string, order ordersource.ConsumeOrder, ok bool) {
	p := c.orderCtx.Load()
	if p == nil {
		return "", "", ordersource.ConsumeOrder{}, false
	}
	dataset, stage, order, ok = (*p)(ref, fp)
	if !ok {
		return dataset, stage, ordersource.ConsumeOrder{}, false
	}
	return dataset, stage, order, true
}

// NewCollector 构造采集层(design §5.5 冻结签名):blockDir = cas 根目录
// (blocks/ 与 heatmap.json 的父目录,即 svc.Config.CacheRoot);m 允许为 nil(无指标场景)。
// 订单上下文不在此注入,装配层随后调用 SetOrderContextProvider。
func NewCollector(blockDir string, m *metrics.Registry, cfg CollectorConfig) *Collector {
	cfg = cfg.withDefaults()
	return &Collector{
		blockDir:      blockDir,
		blocksDir:     filepath.Join(blockDir, blocksDirName),
		heatmapPath:   filepath.Join(blockDir, heatmapFileName),
		m:             m,
		cfg:           cfg,
		clock:         time.Now,
		watchInterval: inotifyPollInterval,
		hookSilence:   hookSilenceWindow,
		entries:       make(map[cas.Fingerprint]*heatEntry),
		hr:            newHitRate(hitRateWindow),
		cal:           newCalibrator(cfg.CalibrateWindow, cfg.CalibrateThreshold),
	}
}

// withDefaults 补齐 ≤0 字段的默认值(§5.5:≤0 取默认)。
func (cfg CollectorConfig) withDefaults() CollectorConfig {
	if cfg.HeatDecayAge <= 0 {
		cfg.HeatDecayAge = defaultHeatDecayAge
	}
	if cfg.HeatDumpInterval <= 0 {
		cfg.HeatDumpInterval = defaultHeatDumpInterval
	}
	if cfg.HeatDecayInterval <= 0 {
		cfg.HeatDecayInterval = defaultHeatDecayInterval
	}
	if cfg.HeatDecayFactor <= 0 {
		cfg.HeatDecayFactor = defaultHeatDecayFactor
	}
	if cfg.HeatOpenBeta <= 0 {
		cfg.HeatOpenBeta = defaultHeatOpenBeta
	}
	if cfg.HeatTouchHalfLife <= 0 {
		cfg.HeatTouchHalfLife = defaultHeatTouchHalfLife
	}
	if cfg.CalibrateWindow <= 0 {
		cfg.CalibrateWindow = defaultCalibrateWindow
	}
	if cfg.CalibrateThreshold <= 0 {
		cfg.CalibrateThreshold = defaultCalibrateThreshold
	}
	return cfg
}

// SetLogAggregator 注入 60s 汇总聚合器(装配期调用一次;传 nil 解除)。只影响日志
// 汇总,指标打点与计数源不变(§4.2:不新增重复计数源)。
func (c *Collector) SetLogAggregator(a *logagg.Aggregator) {
	if c == nil {
		return
	}
	if a == nil {
		c.logAgg.Store(nil)
		return
	}
	c.logAgg.Store(a)
}

// Start 启动采集层后台循环(非阻塞;ctx 取消后各循环退出):
// 先同步 Load 恢复 heatmap.json(容错,失败仅告警),再起 decay ticker、dump ticker
// 与 inotify 兜底监听。每实例只生效一次(重复调用仅告警)。
func (c *Collector) Start(ctx context.Context) {
	if !c.started.CompareAndSwap(false, true) {
		slog.Warn("collector: Start 重复调用,忽略", "block_dir", c.blockDir)
		return
	}
	if err := c.Load(); err != nil {
		slog.Warn("collector: 读取 heatmap 失败(以空表继续)", "path", c.heatmapPath, "err", err)
	}
	go c.decayLoop(ctx)
	go c.dumpLoop(ctx)
	go c.watchBlocks(ctx)
}

// decayLoop 周期衰减循环(间隔 HeatDecayInterval)。
func (c *Collector) decayLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.HeatDecayInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.decay()
		}
	}
}

// dumpLoop 周期 dump 循环(间隔 HeatDumpInterval;失败不影响服务,仅告警)。
func (c *Collector) dumpLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.HeatDumpInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Dump(); err != nil {
				slog.Warn("collector: heatmap dump 失败(不影响服务)", "path", c.heatmapPath, "err", err)
			}
		}
	}
}

// Open 记录一次打开事件(钩子 open_report 主源):fp 为接线层经索引解析出的块指纹,
// ref 为钩子帧原始 (path, chunk)(用于交叉校准;空 Path 表示无引用,inotify 兜底源走此形)。
// 见 D33 HookHandlers.Open 与 §5.4。
func (c *Collector) Open(fp cas.Fingerprint, ref ordersource.ChunkRef) {
	now := c.clock()
	c.markHookOpen(now)
	c.noteOpen(fp, ref, now)
}

// Touch 刷新指纹的 t_touch(幂等;不存在则建 h=0 条目)。D24:t_touch 取"最后打开 /
// 预取写入 / 新订单刷新"三者最大值,后两源由接线阶段调用本方法注入;传入的 t 早于
// 现值时不回退(重复注入同一 t 不产生变化)。
func (c *Collector) Touch(fp cas.Fingerprint, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[fp]
	if !ok {
		c.entries[fp] = &heatEntry{lastTouch: t}
		return
	}
	if t.After(e.lastTouch) {
		e.lastTouch = t
	}
}

// ListForEviction 返回热度表全量快照(D31 冻结:调用方按得分排序,本方法不排序)。
// M4:填充 Accesses = e.accessCount,供 LFU 档策略消费。
func (c *Collector) ListForEviction() []EvictionEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]EvictionEntry, 0, len(c.entries))
	for fp, e := range c.entries {
		out = append(out, EvictionEntry{FP: fp, Heat: e.heat, LastTouch: e.lastTouch, Accesses: e.accessCount})
	}
	return out
}

// HitRate 返回滚动窗口命中率(最近 hitRateWindow 内的 命中/(命中+未命中);无样本 → 0)。
func (c *Collector) HitRate() float64 { return c.hr.rate() }

// HitRateForStage 返回某训练阶段(StageID)的进程内命中率(§5.5 按阶段聚合;无样本 → 0)。
func (c *Collector) HitRateForStage(stage string) float64 { return c.hr.stageRate(stage) }

// Score 返回指纹的淘汰得分 S = max(h, R)(§5.5/D24):R = 0.5^(Δt/T_R),
// Δt = now − t_touch,T_R = HeatTouchHalfLife;未知指纹 → 0。
func (c *Collector) Score(fp cas.Fingerprint) float64 {
	c.mu.Lock()
	e, ok := c.entries[fp]
	var heat float64
	var lastTouch time.Time
	if ok {
		heat, lastTouch = e.heat, e.lastTouch
	}
	c.mu.Unlock()
	return scoreAt(c.clock(), heat, lastTouch, c.cfg.HeatTouchHalfLife)
}

// ScoreAt 计算单条快照的淘汰得分 S = max(h, R)(纯函数:接线层/M6 淘汰循环对
// ListForEviction 条目复用同一口径,无需 import 本包实现细节)。
func ScoreAt(now time.Time, entry EvictionEntry, halfLife time.Duration) float64 {
	return scoreAt(now, entry.Heat, entry.LastTouch, halfLife)
}

// scoreAt 是 S = max(h, R) 的实现(半衰期 ≤0 视为无新鲜度,R=0)。
func scoreAt(now time.Time, heat float64, lastTouch time.Time, halfLife time.Duration) float64 {
	r := freshness(now, lastTouch, halfLife)
	if heat > r {
		return heat
	}
	return r
}

// freshness 计算新鲜度 R = 0.5^(Δt/T_R):t_touch 零值(从未 touch)→ 0;Δt 为负按 0。
func freshness(now, lastTouch time.Time, halfLife time.Duration) float64 {
	if lastTouch.IsZero() || halfLife <= 0 {
		return 0
	}
	dt := now.Sub(lastTouch)
	if dt < 0 {
		dt = 0
	}
	return math.Pow(0.5, dt.Seconds()/halfLife.Seconds())
}

// Advance 记录一次消费侧推进事件(钩子 advance 帧经接线层注入):只做推进计数,
// 不分发回调;重对齐回调只由交叉校准触发。
func (c *Collector) Advance(chunk int64) {
	c.advMu.Lock()
	c.advances++
	c.advMu.Unlock()
}

// SetAdvanceCallback 注册重对齐回调:交叉校准触发重对齐时,以 noteOpen 已解析的
// dataset 与本次打开的 ref 调用(传 nil 解除)。
func (c *Collector) SetAdvanceCallback(fn func(dataset string, ref ordersource.ChunkRef)) {
	c.advMu.Lock()
	c.advanceCb = fn
	c.advMu.Unlock()
}

// fireAdvance 锁外分发重对齐动作(回调可安全回溯本对象的读接口)。
func (c *Collector) fireAdvance(dataset string, ref ordersource.ChunkRef) {
	c.advMu.Lock()
	cb := c.advanceCb
	c.advMu.Unlock()
	if cb != nil {
		cb(dataset, ref)
	}
}

// SetOpenCallback 注册 inotify 兜底打开回调(门控通过且计入热度后以指纹调用;传 nil 解除)。
// 不做去重:同一指纹的重复打开逐次回调,去重由上层消费位点的单调性负责。
func (c *Collector) SetOpenCallback(fn func(fp cas.Fingerprint)) {
	c.openMu.Lock()
	c.openCb = fn
	c.openMu.Unlock()
}

// SetOpenFilter 注册兜底打开回调的前置过滤器(D49 self-open guard):返回 true 的打开
// 事件不触发 open 回调(热度/命中率的 noteOpen 不受影响);传 nil 视为不过滤。
// 装配期调用一次;过滤函数须并发安全且不得阻塞。
func (c *Collector) SetOpenFilter(fn func(fp cas.Fingerprint) bool) {
	c.openMu.Lock()
	c.openFilter = fn
	c.openMu.Unlock()
}

// fireOpen 锁外分发兜底打开回调(回调可安全回溯本对象的读接口)。filter 在校外调用,
// 返回 true 时跳过回调(D49:Agent 自身读打开不推进消费)。
func (c *Collector) fireOpen(fp cas.Fingerprint) {
	c.openMu.Lock()
	cb, filter := c.openCb, c.openFilter
	c.openMu.Unlock()
	if cb == nil {
		return
	}
	if filter != nil && filter(fp) {
		return
	}
	cb(fp)
}

// noteOpen 是打开事件的统一处理路径(钩子主源与 inotify 兜底共用):
// 热度 β 增量 + t_touch → 命中率(本地块交集)与指标 → 交叉校准(仅带订单引用的钩子源)。
func (c *Collector) noteOpen(fp cas.Fingerprint, ref ordersource.ChunkRef, now time.Time) {
	dataset, stage, order, hasOrder := c.orderInfo(ref, fp)

	// ① 热度(D24:OPEN 合并后 h ← h + (1−h)·β;t_touch = now)。
	c.mu.Lock()
	e, ok := c.entries[fp]
	if !ok {
		e = &heatEntry{}
		c.entries[fp] = e
	}
	e.heat += (1 - e.heat) * c.cfg.HeatOpenBeta
	e.lastTouch = now
	e.accessCount++
	c.mu.Unlock()

	// ② 命中率:打开块与本地块求交集(命中 = 本地副本存在)。
	hit := c.localBlock(fp)
	// 汇总复用同一判定结果:指标仍由 c.m 打点,聚合器只做 60s 日志汇总(不新增计数源)。
	if agg, m := c.logAgg.Load(), c.m; m != nil || agg != nil {
		source, phase := c.cacheLabels(dataset, fp, ref)
		if m != nil {
			if hit {
				m.ObserveCacheHit(dataset, source, phase)
			} else {
				// 采集层只观测"块不在本地"这一事实,不掌握回源结果:按 D36 二元口径记
				// result=success(正常未命中,消费侧将自行回源);error/refetched 由 fetch/
				// 修复路径分别计数(接线阶段)。D46:带上 phase。
				m.ObserveCacheMiss(dataset, metrics.ResultSuccess, phase)
			}
		}
		if agg != nil {
			if hit {
				agg.ObserveCacheHit(dataset, ref.Chunk, source, phase)
			} else {
				agg.ObserveCacheMiss(dataset, ref.Chunk, phase)
			}
		}
	}
	c.hr.observe(now, stage, hit)

	// ③ 交叉校准(D15):仅带订单引用的钩子源参与(实际顺序 vs 订单期望顺序)。
	if hasOrder && ref.Path != "" {
		if trigger, _ := c.cal.observe(order, ref); trigger {
			slog.Warn("collector: 交叉校准匹配率低于阈值,cursor 重对齐到实际位点",
				"block_dir", c.blockDir, "dataset", dataset, "chunk", ref.Chunk,
				"threshold", c.cfg.CalibrateThreshold)
			c.fireAdvance(dataset, ref)
		}
	}
}

// localBlock 判定指纹是否有本地副本(cas 布局:blockDir/blocks/<fp[0:2]hex>/<fp hex>.blk)。
func (c *Collector) localBlock(fp cas.Fingerprint) bool {
	hexs := fp.String()
	_, err := os.Stat(filepath.Join(c.blocksDir, hexs[:2], hexs+".blk"))
	return err == nil
}

// markHookOpen 记录钩子主源活跃时间(§5.4"钩子为主";inotify 兜底判定用)。
func (c *Collector) markHookOpen(now time.Time) {
	c.mu.Lock()
	c.lastHookOp = now
	c.mu.Unlock()
}

// inotifyAccepted 报告 inotify 打开事件当前是否计入:钩子沉默 ≥ hookSilenceWindow
// 或从未报告过打开事件时才计入(同一打开事件双源只算一次,主源优先)。
func (c *Collector) inotifyAccepted(now time.Time) bool {
	c.mu.Lock()
	last := c.lastHookOp
	c.mu.Unlock()
	return last.IsZero() || now.Sub(last) >= c.hookSilence
}
