// Package fetch 是下载编排层：限流、指令合并、校验失败重下（design.md §5.3、§7.5）。
package fetch

import (
	"context"
	"strings"
	"sync"

	"traincache/core/cas"
)

// Options 是 Fetcher 的限流与重试参数（默认值见 design.md §8）。
type Options struct {
	MaxInflightBytes int64 // 在途下载字节上限（信号量口径，非 goroutine 数），默认 1 GiB
	MaxConnsPerHost  int   // 单主机连接数上限，默认 64
	MaxTasks         int   // 最大并发下载任务数，默认 8
	MaxRetries       int   // 校验失败重下次数，默认 3
	// OnLocalOpen 在 Put 成功、Open 读回校验之前以块指纹调用一次（可为 nil）。
	// 通用钩子：core 不依赖 Agent 侧类型；实现不得阻塞（调用方通常仅做内存记账）。
	OnLocalOpen func(fp cas.Fingerprint)
}

// 默认值（design.md §8）：Options 字段 <= 0 视为未设置，取默认。
const (
	defaultMaxInflightBytes int64 = 1 << 30 // 1 GiB
	defaultMaxConnsPerHost        = 64
	defaultMaxTasks               = 8
	defaultMaxRetries             = 3
)

// withDefaults 返回补齐默认值后的 Options；零值（及负值）字段取 design.md §8 默认值。
func (o Options) withDefaults() Options {
	if o.MaxInflightBytes <= 0 {
		o.MaxInflightBytes = defaultMaxInflightBytes
	}
	if o.MaxConnsPerHost <= 0 {
		o.MaxConnsPerHost = defaultMaxConnsPerHost
	}
	if o.MaxTasks <= 0 {
		o.MaxTasks = defaultMaxTasks
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = defaultMaxRetries
	}
	return o
}

// limiter 是 fetch 包内私有限流组件（不导出，design.md §5.3）：三个维度——
// 并发任务数（MaxTasks）、在途下载字节数（MaxInflightBytes）、单主机连接数（MaxConnsPerHost）。
type limiter struct {
	bytes *byteSem
	tasks *countSem
	hosts *hostSem
}

// newLimiter 按已补齐默认值的 Options 构造 limiter。
func newLimiter(opts Options) *limiter {
	return &limiter{
		bytes: newByteSem(opts.MaxInflightBytes),
		tasks: newCountSem(opts.MaxTasks),
		hosts: newHostSem(opts.MaxConnsPerHost),
	}
}

// inflightBytes 返回当前在途（已 Acquire 未 Release）字节数；单测观测用。
func (l *limiter) inflightBytes() int64 { return l.bytes.inflight() }

// hostOf 从对象键提取“主机”维度：键的首段路径（到首个 '/' 为止），无 '/' 时取整个键。
// 对象存储键本身不含主机（design.md §6.1），此处以首段做稳定划分，mock 场景即键前缀。
func hostOf(key string) string {
	if i := strings.IndexByte(key, '/'); i >= 0 {
		return key[:i]
	}
	return key
}

// byteSem 是字节加权信号量：在途字节总量恒 <= cap。
// n > cap 时按 cap 记账（独占全部预算，待在途归零后进入），保证超限对象仍可下载。
// 等待可被 ctx 取消；取消时不占额度、不泄漏。
type byteSem struct {
	cap     int64
	mu      sync.Mutex
	used    int64
	changed chan struct{} // 广播通道：Release 时 close 并换新，唤醒全部等待者重试
}

func newByteSem(capacity int64) *byteSem {
	return &byteSem{cap: capacity, changed: make(chan struct{})}
}

// acquire 阻塞至在途字节 + n <= cap；返回实际记账字节数（n 被裁剪到 cap 之内），
// Release 时须传回同一数值。ctx 取消返回 ctx.Err() 且未占任何额度。
func (s *byteSem) acquire(ctx context.Context, n int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if n < 0 {
		n = 0
	}
	if n > s.cap {
		n = s.cap
	}
	for {
		s.mu.Lock()
		if s.used+n <= s.cap {
			s.used += n
			s.mu.Unlock()
			return n, nil
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// release 归还 n 字节并唤醒等待者。
func (s *byteSem) release(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.used -= n
	if s.used < 0 {
		s.used = 0
	}
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

// inflight 返回当前在途字节数。
func (s *byteSem) inflight() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// countSem 是计数信号量（MaxTasks：并发下载任务数上限，等待者可被 ctx 取消）。
type countSem struct {
	max     int
	mu      sync.Mutex
	used    int
	changed chan struct{}
}

func newCountSem(max int) *countSem {
	return &countSem{max: max, changed: make(chan struct{})}
}

func (s *countSem) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		s.mu.Lock()
		if s.used < s.max {
			s.used++
			s.mu.Unlock()
			return nil
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *countSem) release() {
	s.mu.Lock()
	if s.used > 0 {
		s.used--
	}
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

// current 返回当前占用数；单测观测用。
func (s *countSem) current() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// hostSem 按主机维度限制并发连接数（MaxConnsPerHost）；host 划分见 hostOf。
// 不同 host 计数独立；等待者可被 ctx 取消。
type hostSem struct {
	max     int
	mu      sync.Mutex
	counts  map[string]int
	changed chan struct{}
}

func newHostSem(max int) *hostSem {
	return &hostSem{max: max, counts: make(map[string]int), changed: make(chan struct{})}
}

func (s *hostSem) acquire(ctx context.Context, host string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		s.mu.Lock()
		if s.counts[host] < s.max {
			s.counts[host]++
			s.mu.Unlock()
			return nil
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *hostSem) release(host string) {
	s.mu.Lock()
	if n := s.counts[host]; n > 1 {
		s.counts[host] = n - 1
	} else {
		delete(s.counts, host)
	}
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}
