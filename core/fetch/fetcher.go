package fetch

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// ErrTooManyRetries 表示校验失败重下达到 MaxRetries 仍未成功。
var ErrTooManyRetries = errors.New("fetch: verification failed after max retries")

// backoffBase 是重试退避基数（decisions.md Q6：指数退避 100ms 起、无抖动）。
const backoffBase = 100 * time.Millisecond

// inflightCall 是同 key 合并下载的共享结果：owner 下载完成后写入 fp/err 并 close(done)，
// 等待者从 done 上接收（channel close 建立 happens-before，无需额外锁）。
type inflightCall struct {
	done    chan struct{}
	fp      cas.Fingerprint
	err     error
	waiters int // 当前等待者数量，f.mu 保护；测试观测合并行为用
	// promoted 在前台请求合并进来时关闭：后台 owner 据此放弃让路、改按前台准入，
	// 避免需求读排在"让路门"后面等一个后台下载。
	promoted    chan struct{}
	promoteOnce sync.Once
}

// newInflightCall 构造一次合并下载记录。
func newInflightCall() *inflightCall {
	return &inflightCall{done: make(chan struct{}), promoted: make(chan struct{})}
}

// promote 标记该次下载已被前台请求合并；重复调用安全。
func (c *inflightCall) promote() { c.promoteOnce.Do(func() { close(c.promoted) }) }

// isPromoted 返回该次下载是否已被前台请求合并。
func (c *inflightCall) isPromoted() bool {
	select {
	case <-c.promoted:
		return true
	default:
		return false
	}
}

// Fetcher 持有 objectstore.Store、*cas.Store、limiter、in-flight map（内部）。
type Fetcher struct {
	store objectstore.Store
	cs    *cas.Store
	opts  Options
	lim   *limiter

	// OnMerge 是同 key 合并钩子:本调用因已有在途下载而登记为等待者时调用一次,
	// 参数为 fetch key。由装配层在 Fetcher 构造后设置(并发使用前);nil = 不回调。
	// 回调在调用方 goroutine 内同步执行,不得阻塞(仅用于计数,如 D48 singleflight)。
	OnMerge func(key string)

	mu       sync.Mutex
	inflight map[string]*inflightCall
}

// New 创建 Fetcher；opts 缺省字段按 design.md §8 默认值补齐。
func New(store objectstore.Store, cs *cas.Store, opts Options) *Fetcher {
	opts = opts.withDefaults()
	return &Fetcher{
		store:    store,
		cs:       cs,
		opts:     opts,
		lim:      newLimiter(opts),
		inflight: make(map[string]*inflightCall),
	}
}

// MaxInflightBytes 返回实际生效的在途下载字节上限（Options 补齐默认值后），
// 供 Agent 侧 traincache_fetch_bandwidth_limit_bytes 指标观测（D48）。只读访问器，
// 不改变 Fetch 语义。
func (f *Fetcher) MaxInflightBytes() int64 { return f.opts.MaxInflightBytes }

// Fetch 下载对象（前台优先级：需求读补块、坏块修复）：Acquire(expectSize) 字节信号量 →
// store.Get 流经 cas.Put 落盘 → Open 读回校验 → 失败则 Remove 后重试（≤ MaxRetries）→
// Release 信号量。相同 key 的并发 Fetch 合并为一次实际下载（in-flight map，等待者共享结果）；
// 等待者的 ctx 取消只影响等待者自身，不取消实际下载。
// 返回块指纹。命中检查是调用方职责：先经 index.Catalog.Lookup 取指纹、cas.Open
// 验存在，未命中才调用本方法（对应 docs/2 "缓存命中就不触及数据源"）。
func (f *Fetcher) Fetch(ctx context.Context, key string, expectSize int64) (cas.Fingerprint, error) {
	return f.FetchWithPriority(ctx, key, expectSize, PriorityForeground)
}

// FetchWithPriority 与 Fetch 相同，多了准入等级：后台（预取）受半额任务/字节额度约束，
// 且前台有下载在途时先让路；前台的额度与让路门均不生效。
// 同 key 合并跨等级生效：后到的前台请求若撞上已在途的后台下载，直接共享该次下载结果。
func (f *Fetcher) FetchWithPriority(ctx context.Context, key string, expectSize int64, prio Priority) (cas.Fingerprint, error) {
	var zero cas.Fingerprint

	// 同 key 已有实际下载：登记为等待者，共享其结果与错误（不占字节/任务额度）。
	f.mu.Lock()
	if c, ok := f.inflight[key]; ok {
		c.waiters++
		if prio == PriorityForeground {
			c.promote()
		}
		f.mu.Unlock()
		if f.OnMerge != nil {
			f.OnMerge(key)
		}
		defer func() {
			f.mu.Lock()
			c.waiters--
			f.mu.Unlock()
		}()
		select {
		case <-c.done:
			return c.fp, c.err
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	c := newInflightCall()
	f.inflight[key] = c
	f.mu.Unlock()

	// 本调用者是 owner：同步执行实际下载（全部尝试），完成后广播结果并摘除登记。
	fp, err := f.download(ctx, key, expectSize, prio, c)

	c.fp, c.err = fp, err
	close(c.done)

	f.mu.Lock()
	delete(f.inflight, key)
	f.mu.Unlock()

	return fp, err
}

// download 是实际下载路径（仅 in-flight 合并的 owner 执行）：
// 准入（前台记账让路门后按全场额度；后台先等前台在途归零，再按后台半额、最后按全场额度）
// → 逐次尝试（≤ MaxRetries 次重下，指数退避 100ms 起、受 ctx 控制）。
// 校验失败返回 ErrTooManyRetries；其他失败（Get/Put 错误等）耗尽重试后返回最后一次的错误。
// c 是该次下载的合并记录：后台 owner 在让路期间被前台请求合并时，按前台口径准入。
func (f *Fetcher) download(ctx context.Context, key string, expectSize int64, prio Priority, c *inflightCall) (cas.Fingerprint, error) {
	var zero cas.Fingerprint

	// 后台准入：先给前台让路，再占半额任务/字节。让路期间若被前台请求合并（promote），
	// 立刻改按前台口径，避免需求读被自己的合并对象挡在门外。
	bg := prio == PriorityBackground && !c.isPromoted()
	if bg {
		if err := f.lim.gate.wait(ctx, c.promoted); err != nil {
			return zero, err
		}
		bg = !c.isPromoted()
	}
	if bg {
		if err := f.lim.bgTasks.acquire(ctx); err != nil {
			return zero, err
		}
		defer f.lim.bgTasks.release()
		bgGot, err := f.lim.bgBytes.acquire(ctx, expectSize)
		if err != nil {
			return zero, err
		}
		defer f.lim.bgBytes.release(bgGot)
	} else {
		f.lim.gate.enter()
		defer f.lim.gate.exit()
	}

	if err := f.lim.tasks.acquire(ctx); err != nil {
		return zero, err
	}
	defer f.lim.tasks.release()

	got, err := f.lim.bytes.acquire(ctx, expectSize)
	if err != nil {
		return zero, err
	}
	defer f.lim.bytes.release(got)

	var lastErr error
	for attempt := 1; attempt <= f.opts.MaxRetries+1; attempt++ {
		if attempt > 1 {
			if err := sleepCtx(ctx, backoffDelay(attempt-1)); err != nil {
				return zero, err
			}
		}
		fp, err := f.tryOnce(ctx, key)
		if err == nil {
			return fp, nil
		}
		lastErr = err
		// 下载失败也算一次尝试；但 ctx 已取消时不再退避重试，立即返回。
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, ctxErr
		}
	}
	if errors.Is(lastErr, cas.ErrCorrupt) {
		return zero, ErrTooManyRetries
	}
	return zero, lastErr
}

// tryOnce 执行一次完整下载尝试：单主机连接额度 → store.Get 经 io.Pipe 流经 cas.Put
// 落盘（ctx 取消/流错误均不残留）→ cas.Open 读回校验；校验失败则 cas.Remove 后
// 返回 ErrCorrupt（此时块无引用无 pin，Remove 不会 ErrInUse，design.md §7.5）。
func (f *Fetcher) tryOnce(ctx context.Context, key string) (cas.Fingerprint, error) {
	var zero cas.Fingerprint

	host := hostOf(key)
	if err := f.lim.hosts.acquire(ctx, host); err != nil {
		return zero, err
	}
	defer f.lim.hosts.release(host)

	// store.Get 是推模式（写 io.Writer），cas.Put 是拉模式（读 io.Reader），用 io.Pipe 对接。
	// Get 在独立 goroutine 中执行；无论结果如何都先收尾该 goroutine 再返回（不泄漏）。
	pr, pw := io.Pipe()
	getDone := make(chan error, 1)
	go func() {
		err := f.store.Get(ctx, key, pw)
		_ = pw.CloseWithError(err) // err == nil 时等价 Close()，向 Put 交付 EOF
		getDone <- err
	}()

	fp, _, putErr := f.cs.Put(ctx, pr)
	_ = pr.CloseWithError(putErr) // Put 提前退出时解除 Get 侧写阻塞
	getErr := <-getDone

	if ctxErr := ctx.Err(); ctxErr != nil {
		return zero, ctxErr
	}
	if getErr != nil {
		return zero, getErr
	}
	if putErr != nil {
		return zero, putErr
	}

	if f.opts.OnLocalOpen != nil {
		f.opts.OnLocalOpen(fp)
	}
	br, err := f.cs.Open(fp)
	if err != nil {
		if errors.Is(err, cas.ErrCorrupt) {
			_ = f.cs.Remove(fp) // 校验失败即删；失败也无妨，重下的 Put 会 rename 覆盖
		}
		return zero, err
	}
	_ = br.Close()
	return fp, nil
}

// backoffDelay 返回第 retry 次重试前的退避时长：100ms、200ms、400ms…（无抖动，Q6）。
func backoffDelay(retry int) time.Duration {
	if retry < 1 {
		retry = 1
	}
	if retry > 20 { // 防移位溢出；100ms<<19 ≈ 14.5h，远超实际使用
		retry = 20
	}
	return backoffBase << (retry - 1)
}

// sleepCtx 等待 d，ctx 取消则立即返回 ctx.Err()（不等待剩余退避）。
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
