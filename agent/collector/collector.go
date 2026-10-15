// Package collector 是 Agent 的采集与降级层(design §5.5/§6.4/§7.4;decisions D14/D15/
// D24/D29/D31):热度映射与淘汰得分、命中率(滚动窗口 + 按训练阶段聚合)、交叉校准、30s
// 降级状态机,以及 heatmap.json 的周期 dump 与启动恢复。覆盖验收 10。
//
// 数据来源(§5.4/§5.5):
//   - 打开事件:钩子 open_report(主源,svc 接线把 (path, chunk) 经索引解析为指纹后调 Open)
//     与 inotify 旁路(兜底源,Start 起监听 blockDir/blocks 下 IN_OPEN);
//   - 预取写入与订单刷新:由接线阶段调 Touch(fp, t) 注入(D24"t_touch 三源最大值");
//   - 消费侧推进:钩子 advance 帧经接线层调 Advance(chunk),与校准重对齐共用回调;
//   - ring 探活:ordersource.Reader.LastAdvance()/Missing()(D29,30s 停滞判定)。
//
// 单 ring 语义:NewCollector 冻结形参为单个 *ordersource.Reader(decisions D31);
// 多 Pod(多 ring)场景由接线层自行聚合。
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
type EvictionEntry struct {
	FP        cas.Fingerprint
	Heat      float64
	LastTouch time.Time
}

// heatEntry 是单指纹的热度记录(h ∈ [0,1):β 增量收敛到 1;access_count 仅 dump 用)。
type heatEntry struct {
	heat        float64
	lastTouch   time.Time
	accessCount int64
}

// Collector 是采集层主体:热度表、命中率窗口、校准器与降级状态机由 Start 起的
// 后台循环驱动(decay/dump/degrade/inotify)。构造后即可用,Start 每实例只生效一次。
type Collector struct {
	blockDir    string
	blocksDir   string
	heatmapPath string

	ring *ordersource.Reader
	m    *metrics.Registry
	cfg  CollectorConfig

	// clock 是时间源(可注入,测试用;生产默认 time.Now)。
	clock func() time.Time

	// 后台循环周期与判定阈值:构造时从包内变量固化(测试改包变量后构造即可覆盖),
	// 避免运行中并发读写包变量。
	watchInterval   time.Duration // inotify 读轮询周期
	degradeInterval time.Duration // 降级判定周期
	hookSilence     time.Duration // 钩子主源沉默窗(inotify 兜底生效条件)
	stallThreshold  time.Duration // ring 写入停滞阈值(30s)

	mu         sync.Mutex
	entries    map[cas.Fingerprint]*heatEntry
	lastHookOp time.Time // 钩子主源最近一次打开事件(零值 = 从未;inotify 兜底判定用)

	advMu     sync.Mutex
	advanceCb func(chunk int64)
	advances  uint64 // advance 事件计数(nil 回调时仅统计)

	hr  *hitRate
	cal *calibrator

	degMu    sync.Mutex
	degraded bool

	started atomic.Bool // Start 幂等守卫
}

// NewCollector 构造采集层(design §5.5 冻结签名):blockDir = cas 根目录
// (blocks/ 与 heatmap.json 的父目录,即 svc.Config.CacheRoot);ring 允许为 nil
// (无 OrderSource:dataset 取空、降级判定恒 degraded);m 允许为 nil(无指标场景)。
func NewCollector(blockDir string, ring *ordersource.Reader, m *metrics.Registry, cfg CollectorConfig) *Collector {
	cfg = cfg.withDefaults()
	return &Collector{
		blockDir:        blockDir,
		blocksDir:       filepath.Join(blockDir, blocksDirName),
		heatmapPath:     filepath.Join(blockDir, heatmapFileName),
		ring:            ring,
		m:               m,
		cfg:             cfg,
		clock:           time.Now,
		watchInterval:   inotifyPollInterval,
		degradeInterval: degradeCheckInterval,
		hookSilence:     hookSilenceWindow,
		stallThreshold:  degradeStallThreshold,
		entries:         make(map[cas.Fingerprint]*heatEntry),
		hr:              newHitRate(hitRateWindow),
		cal:             newCalibrator(cfg.CalibrateWindow, cfg.CalibrateThreshold),
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

// Start 启动采集层后台循环(非阻塞;ctx 取消后各循环退出):
// 先同步 Load 恢复 heatmap.json(容错,失败仅告警),再起 decay ticker、dump ticker、
// 降级判定(1s)与 inotify 兜底监听。每实例只生效一次(重复调用仅告警)。
func (c *Collector) Start(ctx context.Context) {
	if !c.started.CompareAndSwap(false, true) {
		slog.Warn("collector: Start 重复调用,忽略", "block_dir", c.blockDir)
		return
	}
	if err := c.Load(); err != nil {
		slog.Warn("collector: 读取 heatmap 失败(以空表继续)", "path", c.heatmapPath, "err", err)
	}
	c.checkDegrade() // 启动即判定一次:未推进 / 文件缺失立即降级,不等一个 tick
	go c.decayLoop(ctx)
	go c.dumpLoop(ctx)
	go c.degradeLoop(ctx)
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
func (c *Collector) ListForEviction() []EvictionEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]EvictionEntry, 0, len(c.entries))
	for fp, e := range c.entries {
		out = append(out, EvictionEntry{FP: fp, Heat: e.heat, LastTouch: e.lastTouch})
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

// Advance 记录一次消费侧推进事件(钩子 advance 帧经接线层注入;主对话裁决:advance 事件
// 与校准重对齐共用同一回调):计数 + 分发;未注册回调(nil)时仅统计不回调。
func (c *Collector) Advance(chunk int64) {
	c.advMu.Lock()
	c.advances++
	c.advMu.Unlock()
	c.fireAdvance(chunk)
}

// SetAdvanceCallback 注册推进回调(主对话裁决;校准触发重对齐时以实际位点调用;传 nil 解除)。
func (c *Collector) SetAdvanceCallback(fn func(chunk int64)) {
	c.advMu.Lock()
	c.advanceCb = fn
	c.advMu.Unlock()
}

// fireAdvance 锁外分发推进动作(回调可安全回溯本对象的读接口)。
func (c *Collector) fireAdvance(chunk int64) {
	c.advMu.Lock()
	cb := c.advanceCb
	c.advMu.Unlock()
	if cb != nil {
		cb(chunk)
	}
}

// noteOpen 是打开事件的统一处理路径(钩子主源与 inotify 兜底共用):
// 热度 β 增量 + t_touch → 命中率(本地块交集)与指标 → 交叉校准(仅带订单引用的钩子源)。
func (c *Collector) noteOpen(fp cas.Fingerprint, ref ordersource.ChunkRef, now time.Time) {
	dataset, stage, order, hasOrder := c.currentOrderInfo()

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
	if c.m != nil {
		if hit {
			c.m.ObserveCacheHit(dataset)
		} else {
			// 采集层只观测"块不在本地"这一事实,不掌握回源结果:按 D36 二元口径记
			// result=success(正常未命中,消费侧将自行回源);error/refetched 由 fetch/
			// 修复路径分别计数(接线阶段)。
			c.m.ObserveCacheMiss(dataset, metrics.ResultSuccess)
		}
	}
	c.hr.observe(now, stage, hit)

	// ③ 交叉校准(D15):仅带订单引用的钩子源参与(实际顺序 vs ring 期望顺序)。
	if hasOrder && ref.Path != "" {
		if trigger, chunk := c.cal.observe(order, ref); trigger {
			slog.Warn("collector: 交叉校准匹配率低于阈值,cursor 重对齐到实际位点",
				"block_dir", c.blockDir, "chunk", chunk, "threshold", c.cfg.CalibrateThreshold)
			c.fireAdvance(chunk)
		}
	}
}

// currentOrderInfo 取 ring 当前订单的 dataset / StageID / 订单(无 ring 或无订单 → 空值)。
// dataset 空串经 metrics.DatasetLabel 归并为 other(§5.8)。
func (c *Collector) currentOrderInfo() (dataset, stage string, order ordersource.ConsumeOrder, ok bool) {
	if c.ring == nil {
		return "", "", ordersource.ConsumeOrder{}, false
	}
	o, ok := c.ring.CurrentOrder()
	if !ok {
		return "", "", ordersource.ConsumeOrder{}, false
	}
	return o.Dataset, o.StageID, o, true
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
