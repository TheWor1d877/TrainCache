package prefetch

// pending.go:D45 跨队列去重表。
//
// pending 表覆盖 P0–P3 队列与运行中 task,键 = blockKey(dataset, ChunkRef) =
// <dataset>/blocks/<path>#<chunk>(与 core fetch 调用键一致)。并发约定:
//   - 所有读写均在 PersistentPrefetcher.mu 保护下(函数名带 Locked);
//   - 同一 blockKey 任一时刻至多一个 task;
//   - task 成功或终止后移除键,失败重试期间保留;
//   - 入队语义:键不存在 → 入队;已存在且未开始 → 按最高优先级提升(不新建 task);
//     已在运行 → 忽略。

import (
	"traincache/agent/ordersource"
	"traincache/core/objectstore"
)

// pendingKey 是 pending 表的键(D45;与 fetch key 同布局)。
type pendingKey string

// blockKey 是数据块源站对象键(D25/D45):<dataset>/blocks/<path>#<chunk>。
// path 约定不含 '#';chunk 为 int64 十进制。键构造统一由 objectstore.BlockKey 提供,
// 此处仅保留 pendingKey 类型适配。
func blockKey(dataset string, ref ordersource.ChunkRef) pendingKey {
	return pendingKey(objectstore.BlockKey(dataset, ref.Path, ref.Chunk))
}

// enqueueDedupLocked 是 D45 入队裁决(调用方持 p.mu):
//   - "enqueue":新任务,调用方追加到对应优先级队列;
//   - "promote":键已存在且未开始,已按最高优先级提升(不新建 task);
//   - "ignore":同键任务已在运行。
//
// dataset 为空时无法构造有效 D25 键(该任务本身也无法回源),不参与去重,保持旧行为。
func (p *PersistentPrefetcher) enqueueDedupLocked(t *task, priority int) string {
	if p.pending == nil || t.dataset == "" {
		return "enqueue"
	}
	key := blockKey(t.dataset, t.ref)
	existing, ok := p.pending[key]
	if !ok || existing == nil {
		p.pending[key] = t
		return "enqueue"
	}
	if existing == t {
		// 同一 task 被摘出队列后重新入队(阶段切换降级):队列中已有则忽略,否则放回。
		if p.taskQueuedLocked(t) {
			return "ignore"
		}
		return "enqueue"
	}

	if p.taskQueuedLocked(existing) {
		// 未开始:不新建 task。跨阶段同块时把队列中的旧 task 归属迁到本次请求
		// (旧阶段已不是当前阶段,避免新阶段游标/就绪簿记缺口);不重置 start。
		if !p.sameScopeLocked(t, existing) && t.st != nil {
			existing.st = t.st
			existing.idx = t.idx
			existing.dataset = t.dataset
		}
		if priority < existing.pri {
			p.removeQueuedLocked(existing)
			existing.pri = priority
			p.queues[priority] = append(p.queues[priority], existing)
		}
		if t.p0 {
			// P0 请求去重到已有 task:该 task 完成时仍需 OnP0Done(预热 total/completed
			// 与在途保护按块收敛),故继承 P0 归属。
			existing.p0 = true
		}
		return "promote"
	}

	// 未在队列:运行中(下载/回源重试中)。同阶段 → 忽略;跨阶段(阶段切换瞬间被摘出
	// 队列的旧阶段 task 或仍在运行的旧阶段 task)→ 旧 task 不能服务新阶段,登记新 task。
	if p.sameScopeLocked(t, existing) {
		return "ignore"
	}
	p.pending[key] = t
	return "enqueue"
}

// sameScopeLocked 判断两个同键 task 是否属于同一阶段作用域(P0 请求以当前阶段为准)。
func (p *PersistentPrefetcher) sameScopeLocked(t, other *task) bool {
	ts := t.st
	if ts == nil {
		ts = p.stage
	}
	os := other.st
	if os == nil {
		os = p.stage
	}
	if ts != nil || os != nil {
		return ts == os
	}
	return true // 同键必同 dataset;均无阶段(P0 播种/无订单)视为同作用域
}

// taskQueuedLocked 报告 task 是否仍在某个优先级队列中(未开始;调用方持 p.mu)。
func (p *PersistentPrefetcher) taskQueuedLocked(t *task) bool {
	for pri := range p.queues {
		for _, q := range p.queues[pri] {
			if q == t {
				return true
			}
		}
	}
	return false
}

// removeQueuedLocked 从所在优先级队列按指针摘除 task(保持其余 FIFO 顺序;未找到为 no-op)。
func (p *PersistentPrefetcher) removeQueuedLocked(t *task) {
	for pri := range p.queues {
		q := p.queues[pri]
		for i, item := range q {
			if item != t {
				continue
			}
			p.queues[pri] = append(q[:i:i], q[i+1:]...)
			return
		}
	}
}

// clearPendingLocked 在 task 进入终态后移除去重表键(失败重试由 core fetch 在 Fetch 内部
// 完成,期间键保留;调用方持 p.mu)。
func (p *PersistentPrefetcher) clearPendingLocked(dataset string, ref ordersource.ChunkRef) {
	if p.pending == nil || dataset == "" {
		return
	}
	delete(p.pending, blockKey(dataset, ref))
}

// clearPendingTaskLocked 是 clearPendingLocked 的身份安全版本:仅当 pending 表仍指向
// 该 task 时删除。跨阶段同键替换时(旧 task 在途、新 task 已登记),旧 task 完成不得
// 删除新 task 的去重键;否则去重缺口会导致同块重复 task/重复完成回调。
func (p *PersistentPrefetcher) clearPendingTaskLocked(t *task) {
	if p.pending == nil || t.dataset == "" {
		return
	}
	key := blockKey(t.dataset, t.ref)
	if p.pending[key] == t {
		delete(p.pending, key)
	}
}

// InFlight 报告该块当前是否有运行中的 task(已出队、尚未进入终态;stall 归因
// origin_slow 判据;D47)。仅入队未开始的块不算在途。
func (p *PersistentPrefetcher) InFlight(dataset string, ref ordersource.ChunkRef) bool {
	if dataset == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.pending[blockKey(dataset, ref)]
	if !ok || t == nil {
		return false
	}
	return !p.taskQueuedLocked(t)
}

// IsCurrentWindow 报告该块是否属于当前消费窗口(D46 source=prefetch 判据;D47
// local_slow 判据):当前阶段、位置在 [cursor, cursor+lookahead] 内,且启动窗口 K 尚未
// 处理完时包含整个 K 窗口。无法判定(无阶段/块不在订单/非本 dataset)返回 false。
func (p *PersistentPrefetcher) IsCurrentWindow(dataset string, ref ordersource.ChunkRef) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.stage
	if st == nil || st.dataset != dataset {
		return false
	}
	idx, ok := st.pos[ref]
	if !ok {
		return false
	}
	lo := st.cursor
	if lo < 0 {
		lo = 0
	}
	hi := st.cursor + p.cfg.Lookahead
	if st.kDone < st.k && st.k-1 > hi {
		hi = st.k - 1
	}
	if hi > len(st.refs)-1 {
		hi = len(st.refs) - 1
	}
	return idx >= lo && idx <= hi
}
