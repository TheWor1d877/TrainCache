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
// 每块 Evict 后重新查利用率(直至 ≤ 目标线);硬保护(cursor + lookahead 16 块,§7.6)经
// ProtectedSource 注入,任何情况跳过;pin > 0 的块 cas.Evict 返回 ErrInUse 同样跳过;
// 两类跳过都计 traincache_evict_protected_skipped(D32:告警不新增 family)。
//
// 热度只做淘汰的决策输入,簿记在 collector(§5.5);本包以窄接口 HeatSource/
// ProtectedSource 消费热度数据(D31),不 import collector,接线侧负责适配。
package evictor

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
// M4 增 Accesses 字段供 LFU/W-TinyLFU 档策略消费。
type EvictionEntry struct {
	FP        cas.Fingerprint // 块指纹(cas.Evict 的入参)
	Heat      float64         // 热度 h(h ∈ [0,1],由 collector 簿记)
	LastTouch time.Time       // 最近触摸时刻 t_touch(打开/预取写入/新 ConsumeOrder 三者最大值)
	Accesses  int64           // 累计打开次数(collector.accessCount;LFU/W-TinyLFU 用)
}

// HeatSource 是淘汰循环消费热度的窄接口(D31:与 collector 同签名,独立声明不 import,
// 接线侧适配)。ListForEviction 返回全量快照,所有权归调用方(可原地排序)。
type HeatSource interface {
	ListForEviction() []EvictionEntry
}

// ProtectedSource 是硬保护集合接口(§7.6:cursor + lookahead 16 块,任何情况不淘汰)。
// ProtectedFPs 返回本次快照的保护集合(16 块为接线侧的约定值,本包不感知具体条数)。
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
	// CacheCapacityBytes 是字节维度缓存上限(§14;>0 启用双维度淘汰,默认 0=关闭只走磁盘水位)。
	CacheCapacityBytes int64
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
	policy    Policy // M4 淘汰策略(nil 回退 HeatPolicy;D8 heat 档行为不变)

	interval time.Duration // cfg.Interval 补齐默认后的值
	trigger  float64       // k/100
	delta    float64       // Δ/100
	critical float64       // M/100

	// usage 是磁盘利用率来源(Config.Usage,或 BlockDir 非空时的 statfs 默认实现);
	// 两者都没有时为 nil,RunOnce 记 ERROR 日志并空转。
	usage func() (used, total uint64, err error)

	// cacheCapacityBytes 是字节维度上限(§14;0 = 关闭字节维度)。
	cacheCapacityBytes int64
	// blocksDir 是 <BlockDir>/blocks/ 目录路径(初始值来自 BlockDir;字节维度启用时用于 stat)。
	blocksDir string
	// bytesAccum 是字节累计(原子维护;初始值 stat blocks/ 目录得到,Evict 成功后减);
	// 0 表示未启用或目录为空。
	bytesAccum atomic.Int64
	// seenFPs 记录已累计过的块指纹(避免同一文件多次 Open 重复加累计;sync.Map 高并发友好)。
	seenFPs sync.Map
}

// New 构造淘汰循环。cs 必填(只经 cas.Evict 删本地副本);heat/protected 可为 nil
// (无候选/无保护);m 可为 nil(不打点);policy 为 nil 时回退 HeatPolicy(M4 D8:
// heat 档行为逐字不变);cfg 零值字段取默认(D24)。
func New(cs *cas.Store, heat HeatSource, protected ProtectedSource, m *metrics.Registry, policy Policy, cfg Config) *Evictor {
	cfg = cfg.withDefaults()
	usage := cfg.Usage
	if usage == nil && cfg.BlockDir != "" {
		usage = statfsUsage(cfg.BlockDir)
	}
	if policy == nil {
		policy = NewHeatPolicy()
	}
	e := &Evictor{
		cs:                 cs,
		heat:               heat,
		protected:          protected,
		m:                  m,
		cfg:                cfg,
		policy:             policy,
		interval:           cfg.Interval,
		trigger:            cfg.TriggerPercent / 100,
		delta:              cfg.DeltaPercent / 100,
		critical:           cfg.CriticalPercent / 100,
		usage:              usage,
		cacheCapacityBytes: cfg.CacheCapacityBytes,
	}
	if cfg.BlockDir != "" {
		e.blocksDir = filepath.Join(cfg.BlockDir, "blocks")
	}
	// 初始字节累计:启用字节维度且有 BlockDir → stat blocks/ 目录总字节。
	if e.cacheCapacityBytes > 0 && e.blocksDir != "" {
		e.bytesAccum.Store(statBlocksDirBytes(e.blocksDir))
		if m != nil {
			m.SetCapacityBytes(metrics.CapacitySourceBytes, e.bytesAccum.Load())
		}
	}
	return e
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
//     若 CacheCapacityBytes > 0,同时读取字节累计 bytesAccum,打点 SetCapacityBytes{bytes};
//  2. 判定两个维度的触发级别(磁盘:util ≤ k → 空转,util > M → danger,否则 trigger;
//     字节:bytes ≤ C → 空转,bytes >= 0.95*C → danger,否则(>= C)trigger)。两者独立,取
//     先到触发(更激进:danger > trigger)级别;均空转则直接返回;
//  3. 先 Sweep(见 Config.Sweep),再取候选(HeatSource,normal 按 S=max(h,R) 升序 /
//     danger 只按 h 升序,同分稳定保持来源顺序)逐块处理,每块后重查磁盘利用率,
//     并从字节累计减去被删块的 size,直至两个维度都达标或候选耗尽;
//  4. 每块:ProtectedFPs 命中 → 跳过计 protected_skipped;否则 cas.Evict——成功计
//     evict_blocks 并从 bytesAccum 减去块 size,ErrInUse(pin>0)→ 跳过计 protected_skipped,
//     ErrNotFound → 跳过不计,其他错误仅日志。
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

	// 字节维度:启用时读累计,不启用则视为 0(永不触发)。
	bytes := int64(0)
	bytesEnabled := e.cacheCapacityBytes > 0
	if bytesEnabled {
		bytes = e.bytesAccum.Load()
		if e.m != nil {
			e.m.SetCapacityBytes(metrics.CapacitySourceBytes, bytes)
		}
	}

	// 判定两个维度的触发级别(独立)。
	diskResult := classifyDisk(util, e.trigger, e.critical)
	bytesResult := classifyBytes(bytes, e.cacheCapacityBytes)
	// 取更激进的级别(danger > trigger > idle)。
	overall := moreSevere(diskResult, bytesResult)
	if overall == idleResult {
		return // 空转:两个维度都未触发
	}

	isDanger := overall == dangerResult
	result := metrics.ResultNormal
	if isDanger {
		result = metrics.ResultDanger
		slog.Error("evictor: 超危险线,激进淘汰", "dataset", e.cfg.Dataset,
			"disk_util", util, "disk_trigger", diskResult == triggerResult,
			"bytes", bytes, "bytes_trigger", bytesResult == triggerResult || bytesResult == dangerResult,
			"capacity_bytes", e.cacheCapacityBytes)
	}

	// 计算各维度目标(达标即停)。
	// 磁盘:trigger → k-Δ,danger → k-2Δ。
	diskTarget := e.trigger - e.delta
	if isDanger {
		diskTarget = e.trigger - 2*e.delta
	}
	// 字节:trigger → 85% C,danger → 80% C。
	bytesTarget := int64(float64(e.cacheCapacityBytes) * 0.85)
	if isDanger {
		bytesTarget = int64(float64(e.cacheCapacityBytes) * 0.80)
	}

	e.sweep(ctx)

	entries := e.candidates(isDanger)
	protected := e.protectedSet()
	var evicted, skipped int64
	for _, ent := range entries {
		if ctx.Err() != nil {
			break
		}
		// 双维度都达标 → 停
		if util <= diskTarget && (!bytesEnabled || bytes <= bytesTarget) {
			break
		}
		if isProtected(protected, ent.FP) {
			// 硬保护(cursor + lookahead):任何情况跳过并计数(D32)。
			skipped++
			e.incProtectedSkipped(result, 1)
		} else {
			// 淘汰前先 stat 目标块 size(成功后从累计减)。
			var blockSize int64
			if bytesEnabled {
				blockSize = e.statBlockSize(ent.FP)
			}
			switch err := e.cs.Evict(ent.FP); {
			case err == nil:
				evicted++
				e.incEvictBlocks(result, 1)
				// 字节累计减(stat 到的 size,可能为 0 文件不存在或 stat 失败)。
				if bytesEnabled && blockSize > 0 {
					e.bytesAccum.Add(-blockSize)
					bytes = e.bytesAccum.Load()
					e.seenFPs.Delete(ent.FP) // 允许后续同 fp 再次 Open 时重新累计(罕见)
				}
			case errors.Is(err, cas.ErrInUse):
				// pin > 0(正被读):跳过并同样计 protected_skipped(D32 口径)。
				skipped++
				e.incProtectedSkipped(result, 1)
			case errors.Is(err, cas.ErrNotFound):
				// 已被并发删除/Sweep 回收:跳过不计(D32 口径)。但如果我们之前 stat 到了 size,
				// 说明文件其实是存在的——这种情况极罕见(并发竞态),不调整累计。
			default:
				slog.Warn("evictor: 块淘汰失败", "fp", ent.FP, "err", err)
			}
		}
		// 每块后重查磁盘利用率。
		used, total, err = e.usage()
		if err != nil {
			slog.Warn("evictor: 淘汰中磁盘利用率查询失败,本轮提前结束",
				"dataset", e.cfg.Dataset, "err", err)
			break
		}
		util = ratio(used, total)
		if bytesEnabled {
			bytes = e.bytesAccum.Load()
			if e.m != nil {
				e.m.SetCapacityBytes(metrics.CapacitySourceBytes, bytes)
			}
		}
	}
	if evicted > 0 || skipped > 0 {
		slog.Info("evictor: 淘汰轮结束", "dataset", e.cfg.Dataset, "result", result,
			"evicted_blocks", evicted, "protected_skipped", skipped,
			"disk_util", util, "bytes", bytes)
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

// candidates 取本轮候选的全量快照并排序(所有权归本次调用,D31 允许原地排序)。
// M4:排序委托给注入的 Policy.Rank(heat 档行为与原内联逻辑逐字一致,D8)。
// entries → Block 切片,经 policy.Rank 返回淘汰优先级序,再映射回 EvictionEntry。
func (e *Evictor) candidates(danger bool) []EvictionEntry {
	if e.heat == nil {
		return nil
	}
	entries := e.heat.ListForEviction()
	blocks := make([]Block, len(entries))
	for i, en := range entries {
		blocks[i] = Block{FP: en.FP, Heat: en.Heat, LastTouch: en.LastTouch, Accesses: en.Accesses}
	}
	ranked := e.policy.Rank(blocks, danger)
	out := make([]EvictionEntry, len(ranked))
	fpIndex := make(map[cas.Fingerprint]int, len(entries))
	for i, en := range entries {
		fpIndex[en.FP] = i
	}
	for i, b := range ranked {
		out[i] = entries[fpIndex[b.FP]]
	}
	return out
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
// LastTouch 为零值 → R 下溢为 0)。包级函数,供 HeatPolicy 与测试复用(M4)。
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

// —— 字节维度辅助(§14)——

// idleResult / triggerResult / dangerResult 是内部触发级别常量(用于 classifyDisk/classifyBytes 返回值与 moreSevere 合并)。
const (
	idleResult    = iota // 未触发
	triggerResult        // 触发线
	dangerResult         // 危险线
)

// classifyDisk 按磁盘利用率判定触发级别。
func classifyDisk(util, trigger, critical float64) int {
	if util <= trigger {
		return idleResult
	}
	if util > critical {
		return dangerResult
	}
	return triggerResult
}

// classifyBytes 按字节累计判定触发级别(C=0 永不触发;危险线 >= 0.95*C,触发线 >= C)。
func classifyBytes(bytes, C int64) int {
	if C <= 0 {
		return idleResult
	}
	if bytes < C {
		return idleResult
	}
	if bytes >= int64(float64(C)*0.95) {
		return dangerResult
	}
	return triggerResult
}

// moreSevere 取两个级别中更激进的(danger > trigger > idle)。
func moreSevere(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// statBlocksDirBytes 递归扫描 blocksDir 下所有 .blk 文件的实际磁盘字节数(用于初始化累计)。
// 目录不存在或扫描失败返回 0;临时文件 tmp-* 与非 .blk 文件忽略(同 cas.listBlockDir 口径)。
func statBlocksDirBytes(blocksDir string) int64 {
	var total int64
	_ = filepath.WalkDir(blocksDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // 跳过访问错误的条目
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".blk") {
			return nil
		}
		if _, ok := strings.CutPrefix(name, "tmp-"); ok {
			return nil
		}
		info, err := d.Info()
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// statBlockSize stat 单个块文件的 size。文件不存在或 stat 失败返回 0。
func (e *Evictor) statBlockSize(fp cas.Fingerprint) int64 {
	if e.cs == nil {
		return 0
	}
	// 复用 cas.Store 的 blockPath:路径布局冻结于 core cas.blockPath → blocks/<fp[:2]hex>/<fp hex>.blk。
	// 我们手工构造避免 import core 私有方法。
	hexs := fp.String()
	path := filepath.Join(e.blocksDir, hexs[:2], hexs+".blk")
	if st, err := os.Stat(path); err == nil {
		return st.Size()
	}
	return 0
}

// OnStoreOpen 处理 Store.Open 前置事件:对首次 Open 的块 stat 文件 size 并累计
// 到字节维度(D58"只统计 Agent 自己写的块")。同一 fp 重复 Open 幂等(seenFPs 去重);
// 未启用字节维度(cacheCapacityBytes ≤ 0)时直接 no-op。供装配层合并 observer 调用。
func (e *Evictor) OnStoreOpen(fp cas.Fingerprint) {
	if e.cacheCapacityBytes <= 0 {
		return
	}
	if _, loaded := e.seenFPs.LoadOrStore(fp, true); loaded {
		return // 已累计过
	}
	e.AddBytes(e.statBlockSize(fp))
}

// AddBytes 增量加字节累计(§14 D58:外部 hook 注册;cas.Open 成功后调用)。
// n < 0 忽略(避免负增量异常)。
func (e *Evictor) AddBytes(n int64) {
	if n > 0 {
		e.bytesAccum.Add(n)
		if e.m != nil {
			e.m.SetCapacityBytes(metrics.CapacitySourceBytes, e.bytesAccum.Load())
		}
	}
}

// SetBytesAccum 直接覆盖字节累计(初始化用;通常由 New 内部调用,外部仅极特殊场景使用)。
func (e *Evictor) SetBytesAccum(n int64) {
	e.bytesAccum.Store(n)
	if e.m != nil {
		e.m.SetCapacityBytes(metrics.CapacitySourceBytes, n)
	}
}

// BytesAccum 返回当前字节累计(测试/诊断用)。
func (e *Evictor) BytesAccum() int64 { return e.bytesAccum.Load() }
