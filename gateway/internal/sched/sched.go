// Package sched 实现按 dataset 的公平准入（design 10.1 §6.5；10.2 D11）。
//
// 规则：全局上限 L；A 为 inflight+waiting>0 的 dataset 集合；
// base = clamp(floor(L/|A|), 1, floor(L/2))（L<2 时 base=L）。候选按
// inflight/base 升序（同比值按排队到达先后）；inflight < base 时放行一个，
// 否则仅在「全局还有空位且没有其他 dataset 排队」时放行。不做加权、
// 不做抢占。
package sched

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

const (
	// queueLimit 是等待中的请求总数上限（design 10.1 §6.5）。
	queueLimit = 64
	// waitLimit 是单个请求的最长排队时间。
	waitLimit = 2 * time.Second
)

// 准入失败原因。
var (
	ErrQueueFull   = errors.New("sched: queue full")
	ErrWaitTimeout = errors.New("sched: wait timeout")
)

type waiter struct {
	dataset string
	ch      chan struct{}
	granted bool
}

// Scheduler 持有每个 dataset 的 inflight/waiting 与 FIFO 等待队列。
type Scheduler struct {
	mu     sync.Mutex
	limit  int
	seq    uint64
	queues map[string][]*waiter
	// inflight/waiting 只保留非零项；arrival 记录 dataset 开始排队的序号。
	inflight map[string]int
	waiting  map[string]int
	arrival  map[string]uint64

	totalInflight int
	totalWaiting  int
}

// New 构造调度器。limit 是到桶并发上限 L（TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY）。
func New(limit int) *Scheduler {
	if limit < 0 {
		limit = 0
	}
	return &Scheduler{
		limit:    limit,
		queues:   make(map[string][]*waiter),
		inflight: make(map[string]int),
		waiting:  make(map[string]int),
		arrival:  make(map[string]uint64),
	}
}

// Acquire 申请一个到桶下载槽。成功返回幂等的 release；队列满返回
// ErrQueueFull；排队超过 2s（或 ctx 到期）返回 ErrWaitTimeout。
func (s *Scheduler) Acquire(ctx context.Context, dataset string) (release func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	timer := time.NewTimer(waitLimit)
	defer timer.Stop()

	s.mu.Lock()
	if s.totalWaiting >= queueLimit {
		s.mu.Unlock()
		return nil, ErrQueueFull
	}

	w := &waiter{dataset: dataset, ch: make(chan struct{})}
	s.enqueueLocked(dataset, w)
	s.dispatchLocked()
	s.mu.Unlock()

	var waitErr error
	select {
	case <-w.ch:
		// 已获准。
	case <-timer.C:
		waitErr = ErrWaitTimeout
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			waitErr = ErrWaitTimeout
		} else {
			waitErr = ctx.Err()
		}
	}

	if waitErr != nil {
		s.mu.Lock()
		if w.granted {
			// 与放行同时发生：槽已经记在我们账上，按成功处理。
			s.mu.Unlock()
			return s.releaseFunc(dataset), nil
		}
		s.removeWaiterLocked(dataset, w)
		s.mu.Unlock()
		return nil, waitErr
	}

	return s.releaseFunc(dataset), nil
}

// releaseFunc 返回幂等的释放闭包；释放时立即重新派发空闲份额。
func (s *Scheduler) releaseFunc(dataset string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			if n := s.inflight[dataset]; n > 0 {
				if n == 1 {
					delete(s.inflight, dataset)
				} else {
					s.inflight[dataset] = n - 1
				}
				s.totalInflight--
			}
			s.dispatchLocked()
			s.mu.Unlock()
		})
	}
}

func (s *Scheduler) enqueueLocked(dataset string, w *waiter) {
	if s.waiting[dataset] == 0 {
		s.seq++
		s.arrival[dataset] = s.seq
	}
	s.queues[dataset] = append(s.queues[dataset], w)
	s.waiting[dataset]++
	s.totalWaiting++
}

func (s *Scheduler) removeWaiterLocked(dataset string, w *waiter) {
	q := s.queues[dataset]
	for i, x := range q {
		if x == w {
			q = append(q[:i], q[i+1:]...)
			break
		}
	}
	if len(q) == 0 {
		delete(s.queues, dataset)
	} else {
		s.queues[dataset] = q
	}

	if n := s.waiting[dataset]; n > 0 {
		if n == 1 {
			delete(s.waiting, dataset)
			delete(s.arrival, dataset)
		} else {
			s.waiting[dataset] = n - 1
		}
		s.totalWaiting--
	}
	s.dispatchLocked()
}

// dispatchLocked 反复按 6.5 的规则放行，直到没有可放行的请求。
func (s *Scheduler) dispatchLocked() {
	for {
		if s.totalWaiting == 0 || s.totalInflight >= s.limit {
			return
		}

		active := len(s.inflight)
		for d, n := range s.waiting {
			if n > 0 {
				if _, ok := s.inflight[d]; !ok {
					active++
				}
			}
		}
		base := s.baseLocked(active)

		cand := make([]string, 0, len(s.waiting))
		for d, n := range s.waiting {
			if n > 0 && len(s.queues[d]) > 0 {
				cand = append(cand, d)
			}
		}
		sort.Slice(cand, func(i, j int) bool {
			ai, aj := s.inflight[cand[i]], s.inflight[cand[j]]
			if ai != aj {
				return ai < aj
			}
			return s.arrival[cand[i]] < s.arrival[cand[j]]
		})

		progressed := false
		for _, d := range cand {
			if s.totalInflight >= s.limit {
				break
			}
			if s.inflight[d] < base {
				s.admitLocked(d)
				progressed = true
				break
			}
			if !s.otherWaitingLocked(d) {
				s.admitLocked(d)
				progressed = true
				break
			}
			break
		}
		if !progressed {
			return
		}
	}
}

// baseLocked 计算 floor(L/|A|) 并夹在 [1, floor(L/2)]；L<2 时取 L。
func (s *Scheduler) baseLocked(active int) int {
	if s.limit < 2 {
		return s.limit
	}
	if active <= 0 {
		return s.limit
	}
	b := s.limit / active
	if b < 1 {
		b = 1
	}
	if max := s.limit / 2; b > max {
		b = max
	}
	return b
}

func (s *Scheduler) otherWaitingLocked(dataset string) bool {
	for d, n := range s.waiting {
		if d != dataset && n > 0 {
			return true
		}
	}
	return false
}

func (s *Scheduler) admitLocked(dataset string) {
	q := s.queues[dataset]
	w := q[0]
	q = q[1:]
	if len(q) == 0 {
		delete(s.queues, dataset)
	} else {
		s.queues[dataset] = q
	}

	if n := s.waiting[dataset]; n > 1 {
		s.waiting[dataset] = n - 1
	} else {
		delete(s.waiting, dataset)
		delete(s.arrival, dataset)
	}
	s.totalWaiting--

	s.inflight[dataset]++
	s.totalInflight++

	w.granted = true
	close(w.ch)
}

// inflightTotal 与 waitingTotal 供同包测试观察全局不变量。
func (s *Scheduler) inflightTotal() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalInflight
}

func (s *Scheduler) waitingTotal() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalWaiting
}
