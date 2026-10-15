// Package rescan 实现启动期重扫描与坏块修复(design §7.1/§7.5;decisions D13/D35)。
//
// Start 后在内部 goroutine 内执行(D35 冻结流程):枚举 blocks/(cas.List)→ 1% 抽样全读
// 校验(cas.Open 自校验 xxh3;四舍五入、非空至少 1 块,空目录直接 DONE)→ Phase=FULL_SCAN,
// 后台全量校验其余块。发现 ErrCorrupt → Quarantine 入修复队列(计数器),全量扫描期间修复
// 队列优先(先进先出:队列非空先修队内块,再继续扫描)。修复 = RepairOne:按 D25 键
// <dataset>/blocks/<path>#<chunk> 经 core fetch 重下(Fetch 内部 Put 落块并读回校验,同键
// 并发单飞)→ RemoveQuarantined(同指纹;约定内容同源、重下指纹不变)。读时坏块路径
// (prefetch Config.OnCorrupt)复用同一 RepairOne,由接线阶段注入"Quarantine + RepairOne"包装。
//
// dataset 映射(D35):objectstore.List(ctx,"") 按对象键提取数据集名去重(规则见 datasetsOf),
// 每个 dataset 经 catalogFor 取 Catalog 后 List(ctx,"") 全量构建 块指纹→(dataset,path,chunk)
// 映射;Catalog 不可用(ErrNoCurrent 等)的 dataset 其坏块只隔离不修复,留待订单到达后由
// 读时路径修。映射在进入 FULL_SCAN 时构建一次(快照语义);此前已处于 quarantine/ 的块
// 不主动重修,同样留待读时路径。
//
// 并发:Start/Status/RepairOne 可并发调用;IO(Open/Quarantine/Fetch/RemoveQuarantined)
// 不持内部锁,计数器与队列在锁下维护。ctx 取消 = 扫描协程退出(Phase 保持当前值),
// 在途修复随各自 ctx 收敛。
package rescan

import (
	"context"
	"errors"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"

	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// Metrics 是 rescan 的指标窄接口(D35 冻结;由接线层注入——agent/metrics.Registry 的
// SetQuarantined 带 dataset 标签,需适配器包装,见 DECISIONS)。实现须并发安全;
// m 允许为 nil(无指标场景,打点静默跳过)。
type Metrics interface {
	// SetRescanProgress 设置重扫描进度 0..1(已校验块数 ÷ 总块数;DONE 时 1)。
	SetRescanProgress(v float64)
	// SetQuarantined 设置当前 quarantine/ 目录中的坏块数(修复成功后递减;design §5.7)。
	SetQuarantined(n int64)
}

// Phase 是重扫描阶段(D35):SAMPLING(1% 抽样,完成后即可服务)→ FULL_SCAN(后台全量,
// 修复队列优先)→ DONE(全量扫描与队列修复完成)。
type Phase string

const (
	PhaseSampling Phase = "SAMPLING"
	PhaseFullScan Phase = "FULL_SCAN"
	PhaseDone     Phase = "DONE"
)

// Status 是重扫描状态快照(D35;锁下并发安全)。
// 计数口径:Sampled = 校验尝试累计数(抽样 + 全量,含损坏/跳过块),Verified = 其中通过数,
// Quarantined = 累计隔离数(本 Rescanner 检测到 ErrCorrupt 并执行隔离的坏块数),
// RepairQueue = 当前修复队列长度。
type Status struct {
	Phase       Phase
	Sampled     int64
	Verified    int64
	Quarantined int64
	RepairQueue int64
}

// Rescanner 执行重扫描与坏块修复(D35 冻结接口;字段并发语义见包注释)。
type Rescanner struct {
	cs         *cas.Store
	f          *fetch.Fetcher
	obj        objectstore.Store
	catalogFor func(ctx context.Context, dataset string) (*index.Catalog, error)
	m          Metrics

	// pickSample 是抽样选择钩子(非导出白盒测试用;生产为 nil → defaultPickSample)。
	pickSample func(n, k int) []int

	mu          sync.Mutex
	started     bool
	runDone     chan struct{} // Start 内创建;扫描协程退出即 close(测试等待用)
	phase       Phase
	sampled     int64
	verified    int64
	quarantined int64
	queue       []repairJob // 修复队列(FIFO)
	flights     map[string]*repairFlight

	quarantineLoaded bool                     // qset 是否已从 quarantine/ 装载
	qset             map[cas.Fingerprint]bool // 当前已知位于 quarantine/ 的块(节点级快照)
}

// New 构造 Rescanner(D35 冻结签名)。cs/fetcher/obj/catalogFor 分别提供本地块存取、下载
// 编排、对象枚举与按 dataset 取索引;m 允许为 nil。Start 前 Status 报告 Phase=SAMPLING、
// 计数全零;构造不做任何 IO(quarantine 目录的快照与 gauge 初值在 Start/首次修复时装载)。
func New(cs *cas.Store, fetcher *fetch.Fetcher, obj objectstore.Store,
	catalogFor func(ctx context.Context, dataset string) (*index.Catalog, error), m Metrics) *Rescanner {
	return &Rescanner{
		cs:         cs,
		f:          fetcher,
		obj:        obj,
		catalogFor: catalogFor,
		m:          m,
		phase:      PhaseSampling,
		flights:    make(map[string]*repairFlight),
	}
}

// Start 开始重扫描:启动内部 goroutine 后立即返回(非阻塞);ctx 取消即退出(Phase 保持
// 当前值)。重复调用为 no-op(幂等,首个 ctx 有效——与 prefetch.Start 同口径)。
//
// 实现口径:cs.List() 失败(真实 FS 错误的极端情形)记录日志并退出扫描协程,Phase 停留
// SAMPLING——不谎报就绪,由进程重启/运维介入;空块目录直接 DONE(进度置 1)。
func (r *Rescanner) Start(ctx context.Context) {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.runDone = make(chan struct{})
	r.mu.Unlock()
	go r.run(ctx)
}

// Status 返回当前状态快照(D35;锁下并发安全):阶段、四计数(口径见 Status 注释)。
func (r *Rescanner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		Phase:       r.phase,
		Sampled:     r.sampled,
		Verified:    r.verified,
		Quarantined: r.quarantined,
		RepairQueue: int64(len(r.queue)),
	}
}

// run 是扫描协程:抽样(SAMPLING)→ 全量(FULL_SCAN,修复队列优先)→ DONE。
func (r *Rescanner) run(ctx context.Context) {
	defer close(r.runDone)
	if ctx.Err() != nil {
		return
	}
	// 启动时装载 quarantine/ 当前快照并同步 gauge(修复成功后该 gauge 递减)。
	if n, ok := r.ensureQuarantine(); ok {
		r.reportQuarantined(n)
	}

	fps, err := r.cs.List()
	if err != nil {
		log.Printf("rescan: 枚举块目录失败,重扫描未进行(Phase 停留 SAMPLING): %v", err)
		return
	}
	n := len(fps)
	if n == 0 {
		r.finish()
		return
	}

	// 阶段一 SAMPLING:1% 抽样(四舍五入,至少 1 块)全读校验。抽样阶段发现的坏块先记下,
	// 待 FULL_SCAN 进入后构建映射再入修复队列(入队需要 dataset)。
	k := sampleSize(n)
	pick := r.pickSample
	if pick == nil {
		pick = defaultPickSample
	}
	sampled := make([]bool, n)
	var pendingCorrupt []cas.Fingerprint
	var done int64
	for _, i := range pick(n, k) {
		if ctx.Err() != nil {
			return
		}
		if i < 0 || i >= n || sampled[i] {
			continue
		}
		sampled[i] = true
		done++
		if r.verify(fps[i]) {
			pendingCorrupt = append(pendingCorrupt, fps[i])
		}
		r.reportProgress(done, int64(n))
	}
	r.setPhase(PhaseFullScan)

	// 阶段二 FULL_SCAN:先构建 块指纹→(dataset,path,chunk) 映射(修复队的 D25 键来源),
	// 再后台全量校验其余块;修复队列优先(队列非空先修队内块,再继续扫描)。
	mapping := r.buildMapping(ctx)
	for _, fp := range pendingCorrupt {
		r.enqueueRepair(fp, mapping)
	}
	for i := range fps {
		if ctx.Err() != nil {
			return
		}
		if sampled[i] {
			continue
		}
		r.drainQueue(ctx)
		done++
		if r.verify(fps[i]) {
			r.enqueueRepair(fps[i], mapping)
		}
		r.reportProgress(done, int64(n))
	}
	r.drainQueue(ctx)
	if ctx.Err() != nil {
		return
	}
	r.finish()
}

// verify 校验单块(全读;xxh3 由 cas.Open 内部自校验):通过 → Verified++;ErrCorrupt →
// Quarantine + Quarantined++ 并返回 true(调用方按映射入修复队列);ErrNotFound(已被并发
// 隔离/淘汰)与 IO 错误只记日志(仍计 Sampled,不计 Verified)。不得持 r.mu 调用(内含 IO)。
func (r *Rescanner) verify(fp cas.Fingerprint) (corrupt bool) {
	br, err := r.cs.Open(fp)
	if err == nil {
		_ = br.Close()
	}
	switch {
	case err == nil:
	case errors.Is(err, cas.ErrCorrupt):
		if qerr := r.cs.Quarantine(fp); qerr != nil {
			log.Printf("rescan: 隔离坏块 %s 失败: %v", fp, qerr)
		}
		if n, changed := r.noteQuarantined(fp); changed {
			r.reportQuarantined(n)
		}
	case errors.Is(err, cas.ErrNotFound):
		// 并发隔离/淘汰:跳过,不算损坏。
	default:
		log.Printf("rescan: 校验块 %s 出错: %v", fp, err)
	}

	r.mu.Lock()
	r.sampled++
	if err == nil {
		r.verified++
	}
	if errors.Is(err, cas.ErrCorrupt) {
		r.quarantined++
	}
	r.mu.Unlock()
	return errors.Is(err, cas.ErrCorrupt)
}

// finish 收尾:DONE + 进度置 1。
func (r *Rescanner) finish() {
	r.setPhase(PhaseDone)
	r.reportProgress(1, 1)
}

// setPhase 置阶段(锁下)。
func (r *Rescanner) setPhase(p Phase) {
	r.mu.Lock()
	r.phase = p
	r.mu.Unlock()
}

// reportProgress 上报重扫描进度(已校验数 ÷ 总数;total≤0 视为 1;m 为 nil 时跳过)。
func (r *Rescanner) reportProgress(done, total int64) {
	if r.m == nil {
		return
	}
	if total <= 0 {
		r.m.SetRescanProgress(1)
		return
	}
	r.m.SetRescanProgress(float64(done) / float64(total))
}

// reportQuarantined 上报当前 quarantine/ 块数(节点级当前值;m 为 nil 时跳过)。
func (r *Rescanner) reportQuarantined(n int64) {
	if r.m != nil {
		r.m.SetQuarantined(n)
	}
}

// ensureQuarantine 惰性装载 quarantine/ 目录的当前快照(仅一次 IO;幂等,失败可重试)。
// 返回装载成功后 qset 的规模与是否已装载;未装载(枚举失败)时调用方跳过 gauge 更新。
func (r *Rescanner) ensureQuarantine() (n int64, loaded bool) {
	r.mu.Lock()
	if r.quarantineLoaded {
		n = int64(len(r.qset))
		r.mu.Unlock()
		return n, true
	}
	r.mu.Unlock()

	fps, err := r.cs.ListQuarantined()
	if err != nil {
		log.Printf("rescan: 枚举 quarantine 目录失败(隔离计数暂不完整): %v", err)
		return 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.quarantineLoaded {
		r.qset = make(map[cas.Fingerprint]bool, len(fps))
		for _, fp := range fps {
			r.qset[fp] = true
		}
		r.quarantineLoaded = true
	}
	return int64(len(r.qset)), true
}

// noteQuarantined 把一个新隔离块并入本地快照,返回并入后的当前数与是否变化(未变化 =
// 快照不可用或该块已在快照中,调用方跳过 gauge 更新;Status.Quarantined 不受影响)。
func (r *Rescanner) noteQuarantined(fp cas.Fingerprint) (int64, bool) {
	if _, ok := r.ensureQuarantine(); !ok {
		return 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.qset[fp] {
		return int64(len(r.qset)), false
	}
	r.qset[fp] = true
	return int64(len(r.qset)), true
}

// noteRepaired 把修复成功(RemoveQuarantined 完成)的块移出本地快照,返回移出后的当前数
// 与是否变化(该块不在快照中 = 不变,调用方跳过 gauge 更新——防止重复修复把 gauge 减负)。
func (r *Rescanner) noteRepaired(fp cas.Fingerprint) (int64, bool) {
	if _, ok := r.ensureQuarantine(); !ok {
		return 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.qset[fp] {
		return int64(len(r.qset)), false
	}
	delete(r.qset, fp)
	return int64(len(r.qset)), true
}

// sampleSize 返回抽样块数(D35/§7.1.3:1% 四舍五入,空列表 0,非空至少 1 块)。
func sampleSize(n int) int {
	if n <= 0 {
		return 0
	}
	k := int(math.Round(0.01 * float64(n)))
	if k < 1 {
		k = 1
	}
	return k
}

// defaultPickSample 从 n 个位置中无放回随机选 k 个(math/rand 全局源,Go 1.20+ 自动播种;
// 抽样随机源决策见 DECISIONS,测试经非导出钩子 pickSample 注入定序)。
func defaultPickSample(n, k int) []int {
	if k >= n {
		idx := make([]int, n)
		for i := range idx {
			idx[i] = i
		}
		return idx
	}
	return rand.Perm(n)[:k]
}

// datasetsOf 从对象键列表提取数据集名(去重,保持首见顺序;obj.List 结果字典序,输出有序):
//   - 无 "/" 或首段为空(如顶层 "CURRENT")→ 跳过;
//   - 首段 "blocks"(顶层 blocks/ 目录,非 dataset 前缀)→ 跳过;
//   - 首段 "dataset"(core 索引键布局 dataset/<name>/...)→ 取第二段为数据集名;第二段为空
//     或为 "blocks"(名为 "dataset" 的数据集的数据键,其索引键仍会被取到)→ 跳过;
//   - 其余取首段(D25 数据键布局 <dataset>/blocks/...)。
//
// 只做"可能是数据集名"的粗筛:非数据集前缀由 catalogFor / Catalog.List 失败自然淘汰。
func datasetsOf(keys []string) []string {
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
