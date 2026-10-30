package svc

// wire.go:M4/M5/M6 组件与装配层的接线胶水(collector 懒绑定、钩子三型 handler、淘汰
// HeatSource/ProtectedSource 适配、GC Sweep 闭包、rescan 指标适配、读时坏块修复)。
// 冻结:各子模块导出签名与语义不变(6.3 D28~D37),全部语义映射集中在本文件与 assemble.go。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"traincache/agent/collector"
	"traincache/agent/evictor"
	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
	"traincache/agent/rescan"
	"traincache/core/cas"
	"traincache/core/index"
)

// defaultHookSockPath 是钩子套接字冻结路径(design §5.4:/run/traincache/agent.sock),
// 仅在 OrdersDir 为空(手工配置/测试骨架)时兜底。
const defaultHookSockPath = "/run/traincache/agent.sock"

// hookSockPath 返回钩子套接字路径:默认取发现目录的父目录 + agent.sock ——
// TRAINCACHE_ORDERS_DIR=/run/traincache/orders(默认)时恰为 §5.4 冻结路径
// /run/traincache/agent.sock;两侧 Pod 共享同一 hostPath 目录(§6.3),订单 ring 与
// 钩子 socket 必须同卷,故随订单目录联动(测试/重部署可导流到临时目录)。
func hookSockPath(cfg Config) string {
	if strings.TrimSpace(cfg.OrdersDir) == "" {
		return defaultHookSockPath
	}
	return filepath.Join(filepath.Dir(cfg.OrdersDir), "agent.sock")
}

// collectorConfig 把 svc 热度参数映射到 collector 冻结配置(§5.5/D30;字段一一对应)。
func collectorConfig(h HeatConfig) collector.CollectorConfig {
	return collector.CollectorConfig{
		HeatDecayAge:       h.DecayAge,
		HeatDumpInterval:   h.DumpInterval,
		HeatDecayInterval:  h.DecayInterval,
		HeatDecayFactor:    h.DecayFactor,
		HeatOpenBeta:       h.HeatOpenBeta,
		HeatTouchHalfLife:  h.HeatTouchHalfLife,
		CalibrateWindow:    h.CalibrateWindow,
		CalibrateThreshold: h.CalibrateThreshold,
	}
}

// —— collector 订单上下文与回调(D63/§5.5:collector 不持有 ring)——

// collector 返回采集层(装配期构造;测试注入点)。
func (a *Agent) collector() *collector.Collector {
	return a.Collector
}

// orderContext 是 collector 的订单上下文 provider(D63):按打开引用/指纹解析
// dataset、StageID 与当前订单;没有订单时返回 dataset 但 ok=false(统计照常,
// 标签仍可用 dataset),完全无归属时返回空串。
func (a *Agent) orderContext(ref ordersource.ChunkRef, fp cas.Fingerprint) (dataset, stage string, order ordersource.ConsumeOrder, ok bool) {
	if a.Prefetch == nil {
		return "", "", ordersource.ConsumeOrder{}, false
	}
	return a.Prefetch.orderContext(ref, fp)
}

// onCalibratedAdvance 是 collector 交叉校准重对齐回调(§5.5):以校准打开时的 dataset
// 与 ref 直接转投预取器;dataset 缺失时按 ref 归属回退解析,仍失败则丢弃。
func (a *Agent) onCalibratedAdvance(dataset string, ref ordersource.ChunkRef) {
	if dataset == "" {
		dataset, _ = a.resolveDataset(ref)
	}
	if dataset == "" {
		return
	}
	a.Prefetch.OnConsumeSideAdvance(context.Background(), dataset, ref)
}

// onInotifyOpen 是 collector inotify 兜底打开回调(D49):按指纹反查各 dataset 当前订单的
// 候选块引用,逐个尝试推进纯消费位点;只要有一个真正推进(idx > consume)就记一次
// consumption_advance{source=inotify} 并结束。重复 OPEN 由 consume 单调自然去重
// (idx <= consume 返回 false,无副作用),不需要已见集合。
func (a *Agent) onInotifyOpen(fp cas.Fingerprint) {
	if a.Prefetch == nil {
		return
	}
	ctx := context.Background()
	for _, ds := range a.Prefetch.bundleNames() {
		b, ok := a.Prefetch.existingBundle(ds)
		if !ok {
			continue
		}
		for _, ref := range b.lookupFingerprint(ctx, fp) {
			if a.Prefetch.OnConsumeSideAdvance(ctx, ds, ref) {
				a.reportConsumptionAdvance(ds, metrics.SourceInotify)
				return
			}
		}
	}
}

// —— 钩子三型 handler(§5.4/D33)——

// resolveRef 解析一个块引用所属的 dataset 与指纹(§5.4 接线口径):按已知 dataset
// (bundle 集合)逐个查索引;都未命中 → ok=false(调用方丢弃或跳过)。
// 指纹只能来自索引条目,要求命中的调用方(open/advance、本地探测)适用本口径;
// demand 补块只需 dataset 归属,走 resolveDataset,不受索引未命中影响(D60)。
func (a *Agent) resolveRef(ref ordersource.ChunkRef) (dataset string, fp cas.Fingerprint, ok bool) {
	for _, ds := range a.Prefetch.bundleNames() {
		if e, found, err := a.Prefetch.lookupRef(ds, ref); err == nil && found {
			return ds, e.Fingerprint, true
		}
	}
	return "", cas.Fingerprint{}, false
}

// resolveDataset 解析块引用的 dataset 归属,不要求路径索引命中(§5.4/D60):
// 先按引用/指纹找订单上下文,再回退已知 bundle 集合的第一个;都没有 → ok=false
// (调用方丢弃帧)。D25 键由 (dataset, path, chunk) 直接构造,demand 补块不依赖索引。
func (a *Agent) resolveDataset(ref ordersource.ChunkRef) (dataset string, ok bool) {
	if ds, _, _, _ := a.orderContext(ref, cas.Fingerprint{}); ds != "" {
		return ds, true
	}
	if names := a.Prefetch.bundleNames(); len(names) > 0 {
		return names[0], true
	}
	return "", false
}

// hookHandlers 组装钩子三型 handler(帧型见 hooksock.go;读循环单 goroutine 顺序回调):
//   - open_report:解析指纹 → collector.Open(fp, ref)(未绑定 collector / 解析失败 → 丢弃);
//   - advance:collector.Advance(chunk) + phaseTracker.OnOrder + 预取器
//     OnConsumeSideAdvance + consumption_advance{source=hook};
//   - stage_switch:以最近一次成功安装的订单为基底覆盖 stage_id/shuffle_seed 构造订单
//     转交预取器(去重:与当前订单同 stage_id+shuffle_seed → 跳过;ring 路径先到时由此闭环);
//   - read_miss(D44):async demand 补块入口 handleReadMiss;
//   - read_stall(D47):stallAttributor.Attribute + ObserveReadStall(见 stallattr.go)。
//
// 钩子帧不带 dataset:stage_switch 的基底取最近安装的订单,多条 dataset 并发时以
// 最后安装者为准(已知限制,见 §7.4)。handler 只做内存/非阻塞操作;ctx 用后台 ctx
// (集帧自身无 ctx,预取器生命周期仍受其调度 ctx 约束)。
func (a *Agent) hookHandlers() HookHandlers {
	// 帧身份暂存(Meta 先于类型回调、同一读循环 goroutine 顺序执行):read_miss 的补块
	// 动作在后台 goroutine,而 Miss 签名冻结不带身份,故在此读出后随闭包转交。
	// 加锁仅为让任何调用方在 -race 下都安全,开销可忽略。
	var metaMu sync.Mutex
	var curMeta HookMeta
	return HookHandlers{
		Meta: func(_ string, m HookMeta) {
			metaMu.Lock()
			curMeta = m
			metaMu.Unlock()
		},
		Open: func(ref ordersource.ChunkRef) {
			c := a.collector()
			if c == nil {
				return
			}
			if _, fp, ok := a.resolveRef(ref); ok {
				c.Open(fp, ref)
			}
		},
		Advance: func(ref ordersource.ChunkRef) {
			if c := a.collector(); c != nil {
				c.Advance(ref.Chunk)
			}
			if ds, _, ok := a.resolveRef(ref); ok {
				if a.phase != nil {
					a.phase.OnOrder(ref)
				}
				a.reportConsumptionAdvance(ds, metrics.SourceHook)
				a.Prefetch.OnConsumeSideAdvance(context.Background(), ds, prefetch.ChunkRef(ref))
			}
		},
		StageSwitch: a.onHookStageSwitch,
		Miss: func(ref ordersource.ChunkRef, reason string) {
			metaMu.Lock()
			m := curMeta
			metaMu.Unlock()
			a.handleReadMissMeta(ref, reason, m)
		},
		Stall: a.handleReadStall,
	}
}

// onHookStageSwitch 处理阶段切换直报帧(D33):最近安装的订单为基底 + 帧内 stage_id/seed
// 覆盖 → ForOrder 转交(前缀已加载,只走 OnStageSwitch)。与当前订单完全同阶段 → 去重跳过
// (ring 路径已转交;D11 先到者为准)。尚无任何订单时丢弃帧(无 dataset 归属)。
func (a *Agent) onHookStageSwitch(stageID string, seed int64) {
	pod, o, ok := a.Prefetch.lastInstalled()
	if !ok {
		slog.Warn("hooksock: stage_switch 缺少订单基底,丢弃", "stage_id", stageID)
		return
	}
	if o.StageID == stageID && o.ShuffleSeed == seed {
		return
	}
	next := o
	next.StageID, next.ShuffleSeed = stageID, seed
	if err := a.Prefetch.ForOrder(context.Background(), pod, next); err != nil {
		slog.Warn("hooksock: stage_switch 转投预取器失败", "dataset", o.Dataset, "stage_id", stageID, "err", err)
	}
}

// —— 预取器回调接线(D24/D28/D35)——

// onOrderTouch 是新 ConsumeOrder 到达时的 t_touch 刷新(装配注入 PrefetchManager.
// SetOrderTouchHook;§7.6 任务 4;D24 t_touch 三源之"新订单刷新"):对该订单 Chunks 全部
// 块引用,经该 dataset 的 Catalog.Lookup 解析指纹后 Collector.Touch(fp, now)(同一订单
// 取同一时刻 now);Lookup 未命中/未加载映射的块跳过,空 chunks 不进入本回调。
// 在 ForOrder 调用链内同步执行(索引解析为纯内存查询,不阻塞);collector 未装配时
// 整体跳过(测试骨架)。
func (a *Agent) onOrderTouch(dataset string, cat *index.Catalog, refs []prefetch.ChunkRef) {
	c := a.collector()
	if c == nil || cat == nil {
		return
	}
	now := time.Now()
	for _, ref := range refs {
		entry, ok, err := cat.Lookup(context.Background(), ref.Path, ref.Chunk)
		if err != nil || !ok {
			continue
		}
		c.Touch(entry.Fingerprint, now)
	}
}

// onBlockDone 是块完成触达(装配注入 prefetch.Config.OnDone):仅块就绪/下载成功
// (err == nil)时按索引解析指纹并刷新 t_touch(§7.6:预取写入属热度来源;D24 失败不 Touch)。
// 在预取器调度协程内同步调用,只做内存查询,不阻塞。
func (a *Agent) onBlockDone(_ string, cat *index.Catalog, ref prefetch.ChunkRef, err error) {
	if err != nil {
		return
	}
	// D46:块就绪/本地命中 → phaseTracker 的窗口 ready 样本(M9 填写状态机)。
	if a.phase != nil {
		a.phase.OnBlockReady(ref)
	}
	c := a.collector()
	if c == nil || cat == nil {
		return
	}
	entry, ok, lerr := cat.Lookup(context.Background(), ref.Path, ref.Chunk)
	if lerr != nil || !ok {
		return
	}
	c.Touch(entry.Fingerprint, time.Now())
}

// repairCorrupt 是读时坏块修复(装配注入 prefetch.Config.OnCorrupt;D35 冻结语义):
//   - dataset 未绑定(空)→ 直接返回错误走下载(无订单归属,无法构造 D25 键);
//   - 索引未命中 → 返回错误走下载(无指纹无法隔离);
//   - 命中 → cs.Quarantine(fp) + rescan.RepairOne(ctx, dataset, ref);nil = 重开可用。
//
// 在工作协程内调用,可阻塞(重下);Quarantine 的真实 IO 错误按失败上报(fetch 会以
// 下载路径覆盖坏块,留待下次修复)。
func (a *Agent) repairCorrupt(ctx context.Context, dataset string, cat *index.Catalog, ref prefetch.ChunkRef) error {
	if dataset == "" {
		return errors.New("svc: 坏块修复缺少 dataset 归属(订单未绑定),走下载")
	}
	if cat == nil {
		return errors.New("svc: 坏块修复缺少索引")
	}
	entry, ok, err := cat.Lookup(ctx, ref.Path, ref.Chunk)
	if err != nil {
		return fmt.Errorf("svc: 坏块修复查询索引(%s#%d): %w", ref.Path, ref.Chunk, err)
	}
	if !ok {
		return fmt.Errorf("svc: 坏块修复索引未命中 %s#%d,走下载", ref.Path, ref.Chunk)
	}
	if err := a.CS.Quarantine(entry.Fingerprint); err != nil {
		return fmt.Errorf("svc: 隔离坏块 %s: %w", entry.Fingerprint, err)
	}
	if _, err := a.Rescan.RepairOne(ctx, dataset, ref); err != nil {
		return fmt.Errorf("svc: 坏块修复 %s#%d(dataset %q): %w", ref.Path, ref.Chunk, dataset, err)
	}
	return nil
}

// catalogFor 是 rescan 的按 dataset 取索引入口(D35):与预取管理器共用同一 bundle
// (§7.3 同一 dataset 一个 Catalog),故重扫描顺带把对象存储可见 dataset 注册进 bundle 表。
func (a *Agent) catalogFor(_ context.Context, dataset string) (*index.Catalog, error) {
	return a.Prefetch.bundleFor(dataset, nil).catalog, nil
}

// —— 淘汰循环接线(M6/§7.6)——

// evictHeatSource 把 collector 热度快照过桥到 evictor 的候选接口(D31 冻结形状;接线适配,
// evictor 不 import collector)。collector 未绑定 → 无候选。
type evictHeatSource struct{ ag *Agent }

// ListForEviction 返回热度表全量快照(转换出栈后所有权归 evictor,可原地排序)。
// M4:透传 Accesses 字段供 LFU 档策略消费。
func (h evictHeatSource) ListForEviction() []evictor.EvictionEntry {
	c := h.ag.collector()
	if c == nil {
		return nil
	}
	src := c.ListForEviction()
	out := make([]evictor.EvictionEntry, 0, len(src))
	for _, e := range src {
		out = append(out, evictor.EvictionEntry{FP: e.FP, Heat: e.Heat, LastTouch: e.LastTouch, Accesses: e.Accesses})
	}
	return out
}

// sweepDatasets 驱动一轮 core GC(evictor.Config.Sweep 接线;D32:失败仅日志不阻塞淘汰):
// 遍历已知 dataset 逐个 index.Collector.Sweep 并累加;ErrNoCurrent(该 dataset 尚无快照,
// Sweep 口径"绝不删")视为空操作跳过,其余错误记日志并返回首个(evictor 按 result=error 打点)。
// 打点 dataset 标签由 evictor.Config.Dataset 提供("" 经白名单归 other)。
func (a *Agent) sweepDatasets(ctx context.Context) (blocks, objects int64, err error) {
	// D48:借用淘汰周期刷新修复队列深度(数据源 = Rescan.Status)。
	a.reportRepairQueueDepth()
	if a.ObjectStore == nil || a.CS == nil {
		return 0, 0, nil
	}
	var firstErr error
	col := index.NewCollector(a.ObjectStore, a.CS, index.Config{
		RefreshInterval: a.Cfg.indexRefreshInterval(),
		OnSweepSkip:     a.onSweepSkip,
	})
	for _, ds := range a.knownDatasets(ctx) {
		if ctx.Err() != nil {
			break
		}
		b, o, serr := col.Sweep(ctx, ds)
		blocks += int64(b)
		objects += int64(o)
		switch {
		case serr == nil:
		case errors.Is(serr, index.ErrNoCurrent):
			// 尚无保留快照:无可回收对象,不算失败。
		default:
			slog.Warn("sweep: dataset GC 失败(跳过,不阻塞淘汰)", "dataset", ds, "err", serr)
			if firstErr == nil {
				firstErr = serr
			}
		}
	}
	return blocks, objects, firstErr
}

// onSweepSkip 是 Collector 的跳过回调(6.1 §5.11):protected 快照 manifest 缺失/
// 损坏时计 index_sweep_skipped_total{dataset,reason} 并告警;core 继续删除其他可删
// 快照对象,不整体停摆。
func (a *Agent) onSweepSkip(dataset, reason string) {
	if a.Metrics != nil {
		a.Metrics.IncIndexSweepSkipped(dataset, reason)
	}
	slog.Warn("sweep: protected 快照不可用,跳过本轮块删除", "dataset", dataset, "reason", reason)
}

// knownDatasets 返回已知 dataset 名(升序去重):已装配 bundle 集合 ∪ 对象存储枚举
// (与 rescan.buildMapping 同款粗筛规则)。枚举失败降级为仅 bundle 集合(只记日志,
// GC 覆盖不全不影响淘汰)。
func (a *Agent) knownDatasets(ctx context.Context) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	if a.Prefetch != nil {
		for _, name := range a.Prefetch.bundleNames() {
			add(name)
		}
	}
	if a.ObjectStore != nil {
		if keys, err := a.ObjectStore.List(ctx, ""); err != nil {
			slog.Warn("sweep: 枚举对象存储失败,本轮仅覆盖已装配 dataset", "err", err)
		} else {
			for _, name := range datasetsOfKeys(keys) {
				add(name)
			}
		}
	}
	sort.Strings(out)
	return out
}

// datasetsOfKeys 从对象键列表提取数据集名(去重;与 rescan.datasetsOf 同规则,子模块
// 互不依赖,各自持有该函数):
//   - 无 "/" 或首段为空(如顶层 "CURRENT")→ 跳过;
//   - 首段 "blocks"(顶层 blocks/ 目录,非 dataset 前缀)→ 跳过;
//   - 首段 "dataset"(core 索引键布局 dataset/<name>/...)→ 取第二段;第二段为空或为
//     "blocks"(名为 "dataset" 的数据集的数据键,其索引键仍会被取到)→ 跳过;
//   - 其余取首段(D25 数据键布局 <dataset>/blocks/...)。
//
// 只做"可能是数据集名"的粗筛:非 dataset 前缀由 Catalog.List 失败自然淘汰。
func datasetsOfKeys(keys []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0)
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		out = append(out, name)
	}
	for _, k := range keys {
		seg1, rest, ok := strings.Cut(k, "/")
		if !ok || seg1 == "" || seg1 == "blocks" {
			continue
		}
		if seg1 == "dataset" {
			name, _, _ := strings.Cut(rest, "/")
			if name == "blocks" {
				continue
			}
			add(name)
			continue
		}
		add(seg1)
	}
	return out
}

// rescanMetrics 把 rescan 的指标窄接口(D35;无标签)适配到 metrics.Registry:
// SetQuarantined 是节点级计数,经 DatasetLabel("") 归一(白名单外 → "other",D36 标签纪律)。
type rescanMetrics struct{ m *metrics.Registry }

// SetRescanProgress 直通(rescan_progress 无标签)。
func (r rescanMetrics) SetRescanProgress(v float64) {
	if r.m != nil {
		r.m.SetRescanProgress(v)
	}
}

// SetQuarantined 归并 dataset 标签后直通。
func (r rescanMetrics) SetQuarantined(n int64) {
	if r.m != nil {
		r.m.SetQuarantined(r.m.DatasetLabel(""), n)
	}
}

// AddFetchBytes / IncFetchRequests 实现 rescan 的可选 repair 回源打点接口(D48):
// RepairOne 的真实下载以 reason=repair 计入回源线;nil Registry 时静默跳过。
func (r rescanMetrics) AddFetchBytes(dataset string, n int64, reasons ...string) {
	if r.m != nil {
		r.m.AddFetchBytes(dataset, n, reasons...)
	}
}

func (r rescanMetrics) IncFetchRequests(dataset, result string, reasons ...string) {
	if r.m != nil {
		r.m.IncFetchRequests(dataset, result, reasons...)
	}
}

// boolToRatio 把布尔状态映射为 gauge 值(1/0)。
func boolToRatio(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// 编译期断言:接线适配满足各子模块的窄接口(形状冻结于 D31/D35)。
var (
	_ evictor.HeatSource      = evictHeatSource{}
	_ evictor.ProtectedSource = (*PrefetchManager)(nil)
	_ rescan.Metrics          = rescanMetrics{}
)
