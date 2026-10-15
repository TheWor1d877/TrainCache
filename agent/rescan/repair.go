package rescan

import (
	"context"
	"log"
	"strconv"

	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// repairJob 是修复队列的一项:修复所需的 (dataset, 块引用) 对(D25 键直接可构造)。
type repairJob struct {
	dataset string
	ref     ordersource.ChunkRef
}

// repairTarget 是映射值:块指纹 → (dataset, path, chunk)(修复队列入队用)。
type repairTarget struct {
	dataset string
	ref     ordersource.ChunkRef
}

// repairFlight 是一次进行中的修复(同键 RepairOne 并发合并,语义类比 core fetch 的
// in-flight 合并:等待者共享 owner 的结果,等待者 ctx 取消只影响自身、不取消实际下载)。
type repairFlight struct {
	done    chan struct{}
	fp      cas.Fingerprint
	err     error
	waiters int // 等待者数(flights 与 waiters 均受 r.mu 保护;白盒测试观测合并行为用)
}

// RepairOne 修复单块(D35/D25):按 D25 键 <dataset>/blocks/<path>#<chunk> 经
// fetch.Fetch(ctx, key, expectSize=0) 重下(Fetch 内部 Put 落块并读回校验),成功后
// RemoveQuarantined(重下指纹;约定内容同源、指纹与原隔离块一致)。返回重下块的指纹。
//
// Quarantine 由调用方负责(冻结语义:读时路径的注入包装 = Quarantine + RepairOne;扫描
// 路径在发现 ErrCorrupt 时已隔离)。成功 = err == nil;RemoveQuarantined 的真实 IO 失败
// 按修复失败上报(块已可用,隔离副本留待下次清理)。同键并发调用在本方法内合并(单飞),
// 仅一次实际下载与一次清理;订阅结果 = (fp, err)。
func (r *Rescanner) RepairOne(ctx context.Context, dataset string, ref ordersource.ChunkRef) (cas.Fingerprint, error) {
	var zero cas.Fingerprint
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	key := blockKey(dataset, ref)

	// 同键已有进行中的修复:登记为等待者共享结果(不重复下载/不重复清理)。
	r.mu.Lock()
	if fl, ok := r.flights[key]; ok {
		fl.waiters++
		r.mu.Unlock()
		select {
		case <-fl.done:
			return fl.fp, fl.err
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	fl := &repairFlight{done: make(chan struct{})}
	r.flights[key] = fl
	r.mu.Unlock()

	// owner 路径(仅一次实际下载方):同步 quarantine/ 快照并上报修复前的基准值
	// (独立修复路径没有 Start 的初始装载;修复成功后同一快照递减)。
	if n, ok := r.ensureQuarantine(); ok {
		r.reportQuarantined(n)
	}

	fp, err := r.f.Fetch(ctx, key, 0)
	if err == nil {
		err = r.cs.RemoveQuarantined(fp)
	}
	if err == nil {
		if n, changed := r.noteRepaired(fp); changed {
			r.reportQuarantined(n)
		}
	}

	fl.fp, fl.err = fp, err
	close(fl.done)
	r.mu.Lock()
	delete(r.flights, key)
	r.mu.Unlock()
	return fp, err
}

// blockKey 是数据块源站对象键(D25):<dataset>/blocks/<path>#<chunk>(path 约定不含 '#',
// chunk 为 int64 十进制)。与 prefetch.blockKey 同布局(子模块互不依赖,各自持有该函数)。
func blockKey(dataset string, ref ordersource.ChunkRef) string {
	return dataset + "/blocks/" + ref.Path + "#" + strconv.FormatInt(ref.Chunk, 10)
}

// buildMapping 构建 块指纹 → (dataset, path, chunk) 映射(D35 修复队列入队用):
// dataset 枚举 = objectstore.List(ctx,"") 经 datasetsOf 取名;每个 dataset 经 catalogFor
// 取 Catalog → List(ctx,"") 全量条目(含不在本地的块)。Catalog 不可用(ErrNoCurrent 等)
// 或枚举失败的 dataset 跳过:其坏块只隔离不修复(留待订单到达后由读时路径修)。映射在
// 进入 FULL_SCAN 时构建一次(快照语义);同指纹别名块取首见 dataset。不得持 r.mu 调用。
func (r *Rescanner) buildMapping(ctx context.Context) map[cas.Fingerprint]repairTarget {
	m := make(map[cas.Fingerprint]repairTarget)
	if r.catalogFor == nil {
		return m
	}
	keys, err := r.obj.List(ctx, "")
	if err != nil {
		log.Printf("rescan: 枚举对象存储失败,本轮坏块只隔离不修复: %v", err)
		return m
	}
	for _, ds := range datasetsOf(keys) {
		if ctx.Err() != nil {
			return m
		}
		cat, err := r.catalogFor(ctx, ds)
		if err != nil {
			log.Printf("rescan: dataset %q 索引不可用,其坏块只隔离不修复: %v", ds, err)
			continue
		}
		entries, err := cat.List(ctx, "")
		if err != nil {
			log.Printf("rescan: dataset %q 索引枚举失败,其坏块只隔离不修复: %v", ds, err)
			continue
		}
		for _, e := range entries {
			if _, dup := m[e.Fingerprint]; dup {
				continue
			}
			m[e.Fingerprint] = repairTarget{dataset: ds, ref: ordersource.ChunkRef{Path: e.Path, Chunk: e.Chunk}}
		}
	}
	return m
}

// enqueueRepair 把块指纹按映射转为修复队列项(FIFO 尾插);映射缺该指纹 = 不修(只隔离)。
func (r *Rescanner) enqueueRepair(fp cas.Fingerprint, mapping map[cas.Fingerprint]repairTarget) {
	t, ok := mapping[fp]
	if !ok {
		return
	}
	r.mu.Lock()
	r.queue = append(r.queue, repairJob{dataset: t.dataset, ref: t.ref})
	r.mu.Unlock()
}

// drainQueue 按 FIFO 排空修复队列(修复优先于剩余扫描:调用方在每块扫描前调用)。
// 单项失败只记日志并丢弃(块保持隔离,留待读时路径按需重修);ctx 取消即返回(剩余项
// 保留在队列)。
func (r *Rescanner) drainQueue(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		r.mu.Lock()
		if len(r.queue) == 0 {
			r.mu.Unlock()
			return
		}
		job := r.queue[0]
		r.queue = r.queue[1:]
		r.mu.Unlock()
		if _, err := r.RepairOne(ctx, job.dataset, job.ref); err != nil {
			log.Printf("rescan: 修复 %s#%d(dataset %q)失败,保持隔离留待读时重修: %v",
				job.ref.Path, job.ref.Chunk, job.dataset, err)
		}
	}
}
