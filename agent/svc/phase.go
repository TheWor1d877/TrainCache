package svc

// phase.go:分阶段命中率状态机(D46;M9 落地)。
//
// 冻结词汇:phase ∈ {startup, steady, switch, unknown}。
//   - startup:Agent 启动起,至首个 ConsumeOrder 的首 K 块就绪;
//   - steady:有当前订单且无切换;
//   - switch:收到 stage_switch / 新 ConsumeOrder 起,至新窗口 K 块就绪;
//   - unknown:订单缺失、降级或无法判定。
//
// 状态按 dataset 独立维护;startup 与 switch 同时活跃时 switch 优先。块归属 dataset
// 由装配层注入的 resolver(基于按 dataset 的 Catalog 查询)解析,resolver 不可用/未命中
// 时回退到最近一次事件归属的 dataset;仍无法归属则忽略该事件。全局 startup 期(首个
// 订单窗口就绪前)无事件的 dataset 也按 startup 返回,之后无状态 dataset 返回 unknown。
//
// 降级(unknown)输入:当前冻结 API 未提供降级事件;降级场景由"订单缺失/映射缺失"覆盖,
// 后续若接入 degraded 信号可在本类型内扩展。

import (
	"sync"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// phaseState 是单个 dataset 的阶段状态(phaseTracker.mu 保护)。
type phaseState struct {
	hasOrder     bool // 是否见过该 dataset 的订单/切换
	startupDone  bool // 启动窗口(或已被 switch 取代)完成
	switchActive bool // 切换窗口进行中(优先于 startup)

	// 窗口就绪按去重块计数(同一块重复完成不重复推进)。
	startupSeen map[ordersource.ChunkRef]struct{}
	switchSeen  map[ordersource.ChunkRef]struct{}
}

// phaseTracker 是 svc 侧阶段状态机(并发安全;事件可来自钩子读循环与预取调度协程)。
type phaseTracker struct {
	k int // 缺省启动/切换窗口大小 K(构造时注入;D46 取 prefetch_lookahead)

	mu            sync.Mutex
	states        map[string]*phaseState
	kByDataset    map[string]int // 订单注入的窗口 K(D46:ConsumeOrder.PrefetchChunks)
	lastDataset   string         // 最近一次可归属事件/切换的 dataset(无 resolver 时的回退)
	resolver      func(ordersource.ChunkRef) (string, bool)
	startupClosed bool // 全局 startup 已结束(首个订单窗口/首次切换完成)
}

// newPhaseTracker 构造阶段跟踪器(k = 缺省窗口块数;≤0 视为窗口立即就绪)。
func newPhaseTracker(k int) *phaseTracker {
	if k < 0 {
		k = 0
	}
	return &phaseTracker{
		k:             k,
		states:        make(map[string]*phaseState),
		kByDataset:    make(map[string]int),
		startupClosed: k == 0,
	}
}

// windowKLocked 返回 dataset 的窗口大小:订单注入值优先,缺省回退构造时的 K
// (调用方持 p.mu)。
func (p *phaseTracker) windowKLocked(dataset string) int {
	if k, ok := p.kByDataset[dataset]; ok {
		return k
	}
	return p.k
}

// setWindowKLocked 记录 dataset 的订单窗口大小(D46:K = ConsumeOrder.PrefetchChunks;
// k<0 归一为 0,K=0 表示窗口立即就绪;调用方持 p.mu)。
func (p *phaseTracker) setWindowKLocked(dataset string, k int) {
	if k < 0 {
		k = 0
	}
	p.kByDataset[dataset] = k
	st := p.stateLocked(dataset)
	if k == 0 {
		st.startupDone = true
		p.startupClosed = true
	}
}

// OnOrderForDataset 记录订单上下文并注入该订单的窗口 K(D46;装配层在 ForOrder 处调用)。
// k 为该订单的 ConsumeOrder.PrefetchChunks;未注入的 dataset 回退缺省 K。
func (p *phaseTracker) OnOrderForDataset(dataset string, k int) {
	if dataset == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastDataset = dataset
	p.setWindowKLocked(dataset, k)
	p.stateLocked(dataset).hasOrder = true
}

// OnOrderWithK 是 ref 形式的订单事件入口(新增方法;不改变冻结的 OnOrder(ref) 签名):
// 解析 ref 归属 dataset 后注入该订单的窗口 K(D46)。
func (p *phaseTracker) OnOrderWithK(ref ordersource.ChunkRef, k int) {
	dataset, ok := p.resolveDataset(ref)
	if !ok {
		return
	}
	p.OnOrderForDataset(dataset, k)
}

// setDatasetResolver 注入 ref → dataset 归属解析(装配期调用一次;并发使用前)。
func (p *phaseTracker) setDatasetResolver(fn func(ordersource.ChunkRef) (string, bool)) {
	p.mu.Lock()
	p.resolver = fn
	p.mu.Unlock()
}

// resolveDataset 解析 ref 归属的 dataset:优先 resolver,失败回退最近一次归属。
func (p *phaseTracker) resolveDataset(ref ordersource.ChunkRef) (string, bool) {
	p.mu.Lock()
	resolver, last := p.resolver, p.lastDataset
	p.mu.Unlock()
	if resolver != nil {
		if ds, ok := resolver(ref); ok && ds != "" {
			return ds, true
		}
	}
	if last != "" {
		return last, true
	}
	return "", false
}

// stateLocked 取/建 dataset 状态(调用方持 p.mu)。
func (p *phaseTracker) stateLocked(dataset string) *phaseState {
	st := p.states[dataset]
	if st == nil {
		st = &phaseState{
			startupDone: p.windowKLocked(dataset) <= 0,
			startupSeen: make(map[ordersource.ChunkRef]struct{}),
			switchSeen:  make(map[ordersource.ChunkRef]struct{}),
		}
		p.states[dataset] = st
	}
	return st
}

// OnOrder 记录一次消费推进/订单上下文事件(D46):标记 dataset 已有订单。
// 不直接迁移 phase——startup 的结束只由窗口块就绪触发,switch 由 OnStageSwitch 触发。
func (p *phaseTracker) OnOrder(ref ordersource.ChunkRef) {
	dataset, ok := p.resolveDataset(ref)
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastDataset = dataset
	p.stateLocked(dataset).hasOrder = true
}

// OnStageSwitch 记录一次阶段切换(冻结签名;dataset 取最近一次事件归属)。
// 无法归属 dataset 时忽略:Current 仍按各 dataset 既有状态返回(无状态 → unknown)。
func (p *phaseTracker) OnStageSwitch(stageID string) {
	p.mu.Lock()
	dataset := p.lastDataset
	p.mu.Unlock()
	p.OnStageSwitchFor(dataset, stageID)
}

// OnStageSwitchFor 按 dataset 记录一次阶段切换(D46:迁移到 switch,直到新窗口 K 块就绪)。
// assemble 层的 OnStageSwitch 闭包携带 dataset 调用本方法。
func (p *phaseTracker) OnStageSwitchFor(dataset, stageID string) {
	_ = stageID
	if dataset == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastDataset = dataset
	st := p.stateLocked(dataset)
	st.hasOrder = true
	st.switchSeen = make(map[ordersource.ChunkRef]struct{})
	if p.windowKLocked(dataset) <= 0 {
		st.switchActive = false
		st.startupDone = true
		p.startupClosed = true
		return
	}
	st.switchActive = true
}

// OnBlockReady 记录一块就绪(D46):按 ref 归属 dataset 推进窗口就绪计数。
// switch 活跃时只推进 switch 窗口(switch 优先);startup 阶段推进 startup 窗口。
func (p *phaseTracker) OnBlockReady(ref ordersource.ChunkRef) {
	dataset, ok := p.resolveDataset(ref)
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastDataset = dataset
	st := p.stateLocked(dataset)
	k := p.windowKLocked(dataset)
	if st.switchActive {
		if _, dup := st.switchSeen[ref]; !dup {
			st.switchSeen[ref] = struct{}{}
			if len(st.switchSeen) >= k {
				st.switchActive = false
				st.startupDone = true
				p.startupClosed = true
			}
		}
		return
	}
	// startup 窗口只归属于已见订单的 dataset(D46:首个 ConsumeOrder 的首 K 块);
	// 订单到达前的预取块不结束 startup。
	if st.startupDone || !st.hasOrder {
		return
	}
	if _, dup := st.startupSeen[ref]; dup {
		return
	}
	st.startupSeen[ref] = struct{}{}
	if len(st.startupSeen) >= k {
		st.startupDone = true
		p.startupClosed = true
	}
}

// Current 返回 dataset 当前 phase(D46)。
func (p *phaseTracker) Current(dataset string) string {
	if dataset == "" {
		return metrics.PhaseUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.states[dataset]
	switch {
	case st == nil:
		// 无该 dataset 事件:全局 startup 期(首个订单窗口就绪前)按 startup;
		// 之后无法判定 → unknown。
		if !p.startupClosed {
			return metrics.PhaseStartup
		}
		return metrics.PhaseUnknown
	case st.switchActive:
		return metrics.PhaseSwitch
	case !st.startupDone:
		return metrics.PhaseStartup
	case st.hasOrder:
		return metrics.PhaseSteady
	default:
		return metrics.PhaseUnknown
	}
}
