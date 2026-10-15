// Package evictor 实现磁盘淘汰循环(design §7.6;decisions D24/D31/D32)。
//
// 分层(D24/Q5):淘汰与 GC 是两件事——core Sweep 只做引用回收(语义不变),本包经
// core cas.Evict(core D28)删除仍是 CURRENT 活块的本地副本:索引条目与引用关系不动,
// 之后要用经 Lookup → Open miss → fetch 重下(不重复 Acquire)。循环每 Interval(默认
// 30s)查一次磁盘利用率:
//
//   - util ≤ k(默认 80%):空转(利用率 gauge 每轮照常刷新);
//   - k < util ≤ M(默认 90%):触发线——先驱动一次 GC Sweep(接线注入;失败仅日志),
//     再按得分 S = max(h, R) 升序逐块 Evict,直至 util ≤ k-Δ(Δ 默认 5%)或候选耗尽;
//   - util > M:危险线——先 Sweep,R 清零只按 h 升序激进淘汰(「激进」= 牺牲纯投机块,
//     即预取了还没被读的)至 k-2Δ,ERROR 日志告警;打点 result=danger 与触发线区分。
//
// 每块 Evict 后重新查利用率(直至 ≤ 目标线);硬保护(cursor + lookahead 8 块,§7.6)经
// ProtectedSource 注入,任何情况跳过;pin > 0 的块 cas.Evict 返回 ErrInUse 同样跳过;
// 两类跳过都计 traincache_evict_protected_skipped(D32:告警不新增 family)。
//
// 热度只做淘汰的决策输入,簿记在 collector(§5.5);本包以窄接口 HeatSource/
// ProtectedSource 消费热度数据(D31),不 import collector,接线侧负责适配。
package evictor

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sort"
	"time"

	"traincache/agent/metrics"
	"traincache/core/cas"
)

// 配置默认值(D24;k/Δ/M 的百分数经 Config 传入,内部折算为 0..1)。
const (
	defaultInterval        = 30 * time.Second
	defaultTriggerPercent  = 80 // k
	defaultDeltaPercent    = 5  // Δ
	defaultCriticalPercent = 90 // M
)

// heatHalfLife 是新鲜度 R 的半衰期 T_R(D24:6h;R = 0.5^(Δt/T_R),Δt = now − LastTouch)。
const heatHalfLife = 6 * time.Hour

// EvictionEntry 是淘汰候选(热度侧全量快照的一条;形状冻结于 D31)。
type EvictionEntry struct {
	FP        cas.Fingerprint // 块指纹(cas.Evict 的入参)
	Heat      float64         // 热度 h(h ∈ [0,1],由 collector 簿记)
	LastTouch time.Time       // 最近触摸时刻 t_touch(打开/预取写入/新 ConsumeOrder 三者最大值)
}

// HeatSource 是淘汰循环消费热度的窄接口(D31:与 collector 同签名,独立声明不 import,
// 接线侧适配)。ListForEviction 返回全量快照,所有权归调用方(可原地排序)。
type HeatSource interface {
	ListForEviction() []EvictionEntry
}

// ProtectedSource 是硬保护集合接口(§7.6:cursor + lookahead 8 块,任何情况不淘汰)。
// ProtectedFPs 返回本次快照的保护集合(8 块为接线侧的约定值,本包不感知具体条数)。
type ProtectedSource interface {
	ProtectedFPs() []cas.Fingerprint
}

// Config 是淘汰循环配置(冻结形状;零值字段取默认)。Usage 与 Sweep 由接线注入:
// Usage 为 nil 且 BlockDir 非空时取 statfs(BlockDir) 默认实现(disk.go);Sweep 为 nil
// 视为未接线(跳过 GC 阶段,不打 GC 指标)。
type Config struct {
	Interval        time.Duration // 检查周期,默认 30s(≤0 取默认;§7.6)
	BlockDir        string        // statfs 对象(缓存根)
	Dataset         string        // 指标 dataset 标签(可空,经白名单归 other)
	TriggerPercent  float64       // k,默认 80
	DeltaPercent    float64       // Δ,默认 5
	CriticalPercent float64       // M,默认 90
	// Usage 返回磁盘 (已用, 总量) 字节数;nil = statfs(BlockDir) 默认实现(evictor/disk.go)。
	Usage func() (used, total uint64, err error)
	// Sweep 驱动一轮 core GC(接线注入:遍历已知 dataset 跑 core Sweep);失败仅日志,
	// 不阻塞淘汰(D32)。nil = 不驱动 GC。
	Sweep func(ctx context.Context) (blocks, objects int64, err error)
}

// withDefaults 返回补齐默认值的配置(Interval/k/Δ/M ≤0 回退默认)。
func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.TriggerPercent <= 0 {
		c.TriggerPercent = defaultTriggerPercent
	}
	if c.DeltaPercent <= 0 {
		c.DeltaPercent = defaultDeltaPercent
	}
	if c.CriticalPercent <= 0 {
		c.CriticalPercent = defaultCriticalPercent
	}
	return c
}

// Evictor 是磁盘淘汰循环(单实例;字段在 New 后不再变更,RunOnce 不共享可变状态,
// 可与 Start 的 ticker 并发调用)。
type Evictor struct {
	cs        *cas.Store
	heat      HeatSource
	protected ProtectedSource
	m         *metrics.Registry
	cfg       Config

	interval time.Duration // cfg.Interval 补齐默认后的值
	trigger  float64       // k/100
	delta    float64       // Δ/100
	critical float64       // M/100

	// usage 是磁盘利用率来源(Config.Usage,或 BlockDir 非空时的 statfs 默认实现);
	// 两者都没有时为 nil,RunOnce 记 ERROR 日志并空转。
	usage func() (used, total uint64, err error)
}

// New 构造淘汰循环。cs 必填(只经 cas.Evict 删本地副本);heat/protected 可为 nil
// (无候选/无保护);m 可为 nil(不打点);cfg 零值字段取默认(D24)。
func New(cs *cas.Store, heat HeatSource, protected ProtectedSource, m *metrics.Registry, cfg Config) *Evictor {
	cfg = cfg.withDefaults()
	usage := cfg.Usage
	if usage == nil && cfg.BlockDir != "" {
		usage = statfsUsage(cfg.BlockDir)
	}
	return &Evictor{
		cs:        cs,
		heat:      heat,
		protected: protected,
		m:         m,
		cfg:       cfg,
		interval:  cfg.Interval,
		trigger:   cfg.TriggerPercent / 100,
		delta:     cfg.DeltaPercent / 100,
		critical:  cfg.CriticalPercent / 100,
		usage:     usage,
	}
}

// Start 启动周期检查:非阻塞(立即返回),后台协程每 Interval 触发一次 RunOnce;
// ctx 取消退出(不等在轮 RunOnce 结束)。
func (e *Evictor) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(e.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.RunOnce(ctx)
			}
		}
	}()
}

// RunOnce 执行一轮检查(D32 语义;Start 的 ticker 与接线探针共用):
//
//  1. util = Usage()/总量(除零防御;查询失败仅日志并结束本轮),打点 SetDiskUsageRatio;
//  2. util ≤ k → 空转;util > M → danger 轮(ERROR 告警 + result=danger),否则 normal 轮;
//  3. 先 Sweep(见 Config.Sweep),再取候选(HeatSource,normal 按 S=max(h,R) 升序 /
//     danger 只按 h 升序,同分稳定保持来源顺序)逐块处理,每块后重查 util,直至
//     util ≤ 目标线(k-Δ / k-2Δ)或候选耗尽;
//  4. 每块:ProtectedFPs 命中 → 跳过计 protected_skipped;否则 cas.Evict——成功计
//     evict_blocks,ErrInUse(pin>0)→ 跳过计 protected_skipped,ErrNotFound → 跳过不计,
//     其他错误仅日志。
func (e *Evictor) RunOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if e.usage == nil {
		slog.Error("evictor: 未配置磁盘利用率来源(Usage 与 BlockDir 均为空),本轮跳过")
		return
	}
	used, total, err := e.usage()
	if err != nil {
		slog.Warn("evictor: 磁盘利用率查询失败,本轮跳过", "dataset", e.cfg.Dataset, "err", err)
		return
	}
	util := ratio(used, total)
	if e.m != nil {
		e.m.SetDiskUsageRatio(e.cfg.Dataset, util) // 每轮刷新(含空转),gauge 即触发线输入
	}
	if util <= e.trigger {
		return // 空转:util ≤ k
	}

	result := metrics.ResultNormal
	target := e.trigger - e.delta // k-Δ
	if util > e.critical {
		result = metrics.ResultDanger
		target = e.trigger - 2*e.delta // k-2Δ
		slog.Error("evictor: 磁盘利用率超危险线,激进淘汰", "dataset", e.cfg.Dataset,
			"util", util, "critical_percent", e.cfg.CriticalPercent, "target", target)
	}

	e.sweep(ctx)

	entries := e.candidates(result == metrics.ResultDanger)
	protected := e.protectedSet()
	var evicted, skipped int64
	for _, ent := range entries {
		if ctx.Err() != nil || util <= target {
			break
		}
		if isProtected(protected, ent.FP) {
			// 硬保护(cursor + lookahead):任何情况跳过并计数(D32)。
			skipped++
			e.incProtectedSkipped(result, 1)
		} else {
			switch err := e.cs.Evict(ent.FP); {
			case err == nil:
				evicted++
				e.incEvictBlocks(result, 1)
			case errors.Is(err, cas.ErrInUse):
				// pin > 0(正被读):跳过并同样计 protected_skipped(D32 口径)。
				skipped++
				e.incProtectedSkipped(result, 1)
			case errors.Is(err, cas.ErrNotFound):
				// 已被并发删除/Sweep 回收:跳过不计(D32 口径)。
			default:
				slog.Warn("evictor: 块淘汰失败", "fp", ent.FP, "err", err)
			}
		}
		used, total, err = e.usage() // 每块后重查利用率,达标即停
		if err != nil {
			slog.Warn("evictor: 淘汰中磁盘利用率查询失败,本轮提前结束",
				"dataset", e.cfg.Dataset, "err", err)
			break
		}
		util = ratio(used, total)
	}
	if evicted > 0 || skipped > 0 {
		slog.Info("evictor: 淘汰轮结束", "dataset", e.cfg.Dataset, "result", result,
			"evicted_blocks", evicted, "protected_skipped", skipped, "util", util)
	}
}

// sweep 驱动一轮 GC(Config.Sweep 为 nil 视为未接线,跳过且不打点)。成功打点
// gc_sweeps{result=success} + gc_blocks_removed/gc_objects_removed;失败打点
// gc_sweeps{result=error} 并仅日志(D32:失败继续淘汰)。
func (e *Evictor) sweep(ctx context.Context) {
	if e.cfg.Sweep == nil {
		return
	}
	blocks, objects, err := e.cfg.Sweep(ctx)
	if err != nil {
		if e.m != nil {
			e.m.IncGCSweeps(e.cfg.Dataset, metrics.ResultError)
		}
		slog.Warn("evictor: GC Sweep 失败,继续淘汰", "dataset", e.cfg.Dataset, "err", err)
		return
	}
	if e.m != nil {
		e.m.IncGCSweeps(e.cfg.Dataset, metrics.ResultSuccess)
		e.m.AddGCBlocksRemoved(e.cfg.Dataset, blocks)
		e.m.AddGCObjectsRemoved(e.cfg.Dataset, objects)
	}
}

// candidates 取本轮候选的全量快照并排序(所有权归本次调用,D31 允许原地排序):
// danger 轮 R 清零,只按 h 升序(忽略 LastTouch);normal 轮按 S = max(h, R) 升序。
// sort.SliceStable:同分保持 HeatSource 返回顺序(排序确定、可复现)。
func (e *Evictor) candidates(danger bool) []EvictionEntry {
	if e.heat == nil {
		return nil
	}
	entries := e.heat.ListForEviction()
	if danger {
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].Heat < entries[j].Heat })
		return entries
	}
	now := time.Now()
	score := func(en EvictionEntry) float64 { return math.Max(en.Heat, freshness(now, en.LastTouch)) }
	sort.SliceStable(entries, func(i, j int) bool { return score(entries[i]) < score(entries[j]) })
	return entries
}

// protectedSet 物化本轮硬保护集合(ProtectedSource 为 nil 或空视为无保护)。
func (e *Evictor) protectedSet() map[cas.Fingerprint]struct{} {
	if e.protected == nil {
		return nil
	}
	fps := e.protected.ProtectedFPs()
	if len(fps) == 0 {
		return nil
	}
	set := make(map[cas.Fingerprint]struct{}, len(fps))
	for _, fp := range fps {
		set[fp] = struct{}{}
	}
	return set
}

// incEvictBlocks 计成功淘汰块数(m 为 nil 跳过),result ∈ {normal, danger}。
func (e *Evictor) incEvictBlocks(result string, n int64) {
	if e.m != nil {
		e.m.IncEvictBlocks(e.cfg.Dataset, result, n)
	}
}

// incProtectedSkipped 计硬保护/pin 跳过块数(m 为 nil 跳过),result ∈ {normal, danger}。
func (e *Evictor) incProtectedSkipped(result string, n int64) {
	if e.m != nil {
		e.m.IncEvictProtectedSkipped(e.cfg.Dataset, result, n)
	}
}

// freshness 计算新鲜度 R = 0.5^(Δt/T_R)(D24;T_R = heatHalfLife,Δt = now − LastTouch,
// LastTouch 为零值 → R 下溢为 0)。
func freshness(now, lastTouch time.Time) float64 {
	return math.Pow(0.5, float64(now.Sub(lastTouch))/float64(heatHalfLife))
}

// ratio 计算利用率 used/total(除零防御:total == 0 → 0)。
func ratio(used, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(used) / float64(total)
}

// isProtected 判断指纹是否在硬保护集合内。
func isProtected(set map[cas.Fingerprint]struct{}, fp cas.Fingerprint) bool {
	if len(set) == 0 {
		return false
	}
	_, ok := set[fp]
	return ok
}
