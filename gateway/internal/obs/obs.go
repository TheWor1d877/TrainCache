// Package obs 维护网关的 60s 汇总计数并输出单行 Info 日志
// （design 10.1 §7.3；10.2 D20）。
//
// 所有计数在窗口切换时用原子交换清零；每个请求的每个计数恰好落在一个
// 窗口里，不重不漏。datasets 是窗口内出现过的 dataset 集合大小，切换时清空。
package obs

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// SummaryMsg 是汇总行的固定消息（design 10.1 §7.3 冻结）。
const SummaryMsg = "gateway: 60s 汇总"

// Summary 是一个窗口的九个计数。Snapshot 返回当前窗口的实时值，不清零。
type Summary struct {
	WindowS     int64
	Gets        int64
	Merged      int64
	Owners      int64
	Passthrough int64
	Kicked      int64
	Rejected    int64
	Bytes       int64
	Datasets    int64
}

// Logger 是并发安全的窗口计数器；New 起 ticker 协程，Stop 停止。
type Logger struct {
	interval time.Duration
	log      *slog.Logger

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	gets        atomic.Int64
	merged      atomic.Int64
	owners      atomic.Int64
	passthrough atomic.Int64
	kicked      atomic.Int64
	rejected    atomic.Int64
	bytes       atomic.Int64

	dsMu     sync.Mutex
	datasets map[string]struct{}
}

// New 构造汇总器并启动 ticker。interval<=0 时退化为 60s。
func New(interval time.Duration, log *slog.Logger) *Logger {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	l := &Logger{
		interval: interval,
		log:      log,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		datasets: make(map[string]struct{}),
	}
	go l.run()
	return l
}

func (l *Logger) run() {
	defer close(l.done)
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			l.emit()
		case <-l.stop:
			return
		}
	}
}

// Stop 停止 ticker 并等待协程退出；幂等。
func (l *Logger) Stop() {
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
}

// Get 记一次块 GET，并登记窗口内的 dataset。
func (l *Logger) Get(dataset string) {
	l.gets.Add(1)
	l.dsMu.Lock()
	l.datasets[dataset] = struct{}{}
	l.dsMu.Unlock()
}

// Merged 记一次挂到已有条目的请求。
func (l *Logger) Merged(string) { l.merged.Add(1) }

// Owner 记一次新建条目（真实源站 GET）。
func (l *Logger) Owner(string) { l.owners.Add(1) }

// Passthrough 记一次直通 GET/HEAD。
func (l *Logger) Passthrough() { l.passthrough.Add(1) }

// Kicked 记一个被踢的订阅者。
func (l *Logger) Kicked() { l.kicked.Add(1) }

// Rejected 记一次准入拒绝。
func (l *Logger) Rejected() { l.rejected.Add(1) }

// Bytes 记从源站读入的字节数。
func (l *Logger) Bytes(n int64) {
	if n > 0 {
		l.bytes.Add(n)
	}
}

// Snapshot 返回当前窗口计数，不清零（测试用）。
func (l *Logger) Snapshot() Summary {
	l.dsMu.Lock()
	datasets := int64(len(l.datasets))
	l.dsMu.Unlock()
	return Summary{
		WindowS:     l.windowSeconds(),
		Gets:        l.gets.Load(),
		Merged:      l.merged.Load(),
		Owners:      l.owners.Load(),
		Passthrough: l.passthrough.Load(),
		Kicked:      l.kicked.Load(),
		Rejected:    l.rejected.Load(),
		Bytes:       l.bytes.Load(),
		Datasets:    datasets,
	}
}

// emit 原子切换窗口并输出一行 Info。
func (l *Logger) emit() {
	gets := l.gets.Swap(0)
	merged := l.merged.Swap(0)
	owners := l.owners.Swap(0)
	passthrough := l.passthrough.Swap(0)
	kicked := l.kicked.Swap(0)
	rejected := l.rejected.Swap(0)
	nbytes := l.bytes.Swap(0)

	l.dsMu.Lock()
	datasets := int64(len(l.datasets))
	l.datasets = make(map[string]struct{}, len(l.datasets))
	l.dsMu.Unlock()

	l.log.Info(SummaryMsg,
		"window_s", l.windowSeconds(),
		"gets", gets,
		"merged", merged,
		"owners", owners,
		"passthrough", passthrough,
		"kicked", kicked,
		"rejected", rejected,
		"bytes", nbytes,
		"datasets", datasets,
	)
}

func (l *Logger) windowSeconds() int64 {
	return int64(l.interval / time.Second)
}
