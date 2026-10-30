package logagg

// 本文件用可控时钟驱动窗口,断言:窗口到期只输出一条 Info、计数与分位正确、
// 任务结束可提前收口、失败明细不升级为 Info(原有 Warn 不受影响)。

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	corelog "traincache/core/logging"
)

// fakeClock 是测试用可控时钟(并发安全;生产走 time.Now)。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// capture 返回捕获 JSON 日志的 logger 与读取函数。
func capture() (*slog.Logger, func() []map[string]any) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return l, func() []map[string]any {
		var out []map[string]any
		for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if raw == "" {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(raw), &rec); err != nil {
				panic(err)
			}
			out = append(out, rec)
		}
		return out
	}
}

func findByMsg(recs []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, rec := range recs {
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

func num(t *testing.T, rec map[string]any, key string) float64 {
	t.Helper()
	v, ok := rec[key].(float64)
	if !ok {
		t.Fatalf("字段 %s 不是数字: %#v", key, rec)
	}
	return v
}

// TestDownloadWindowSingleSummary: 窗口未到期不输出;到期后每个分组只出一条 Info,
// 块数/字节/失败数与 P50/P99 正确;逐块明细只有 Debug。
func TestDownloadWindowSingleSummary(t *testing.T) {
	clk := newFakeClock()
	logger, recs := capture()
	agg := New(Options{Interval: 60 * time.Second, Now: clk.Now, Verify: "xxh3", Logger: logger})

	for _, d := range []Download{
		{TaskID: "task-1", Dataset: "ds", Path: "a.tar", Chunk: 0, Reason: "prefetch", Size: 100, Ready: true, Duration: 10 * time.Millisecond},
		{TaskID: "task-1", Dataset: "ds", Path: "b.tar", Chunk: 1, Reason: "prefetch", Size: 200, Ready: true, Duration: 20 * time.Millisecond},
		{TaskID: "task-1", Dataset: "ds", Path: "c.tar", Chunk: 2, Reason: "prefetch", Size: 0, Ready: false, Duration: 5 * time.Millisecond},
		{TaskID: "task-1", Dataset: "ds", Path: "d.tar", Chunk: 3, Reason: "prefetch", Size: 0, Ready: true, Duration: 30 * time.Millisecond},
	} {
		agg.ObserveDownload(d)
	}

	agg.Tick() // 未到窗口:不得输出
	if got := findByMsg(recs(), "prefetch: 下载汇总"); len(got) != 0 {
		t.Fatalf("窗口未到期就输出了 %d 条汇总", len(got))
	}
	// 逐块明细全部是 Debug(任何情况下不得逐块写 Info)。
	detail := findByMsg(recs(), "prefetch: 块完成")
	if len(detail) != 4 {
		t.Fatalf("逐块明细 %d 条, want 4", len(detail))
	}
	for _, rec := range detail {
		if rec["level"] != "DEBUG" {
			t.Fatalf("逐块明细级别 = %v, want DEBUG", rec["level"])
		}
	}

	clk.Advance(60 * time.Second)
	agg.Tick()
	sum := findByMsg(recs(), "prefetch: 下载汇总")
	if len(sum) != 1 {
		t.Fatalf("窗口到期汇总 %d 条, want 1", len(sum))
	}
	rec := sum[0]
	if rec["level"] != "INFO" {
		t.Fatalf("汇总级别 = %v, want INFO", rec["level"])
	}
	if rec["task_id"] != "task-1" || rec["dataset"] != "ds" || rec["reason"] != "prefetch" || rec["verify"] != "xxh3" {
		t.Fatalf("汇总标签不符: %#v", rec)
	}
	if got := num(t, rec, "chunk"); got != 4 {
		t.Errorf("chunk(块数) = %v, want 4", got)
	}
	if got := num(t, rec, "size"); got != 300 {
		t.Errorf("size = %v, want 300", got)
	}
	if got := num(t, rec, "failed"); got != 1 {
		t.Errorf("failed = %v, want 1", got)
	}
	if got := num(t, rec, "duration_ms_p50"); got != 10 {
		t.Errorf("p50 = %v, want 10", got)
	}
	if got := num(t, rec, "duration_ms_p99"); got != 30 {
		t.Errorf("p99 = %v, want 30", got)
	}

	// 同一窗口再 Tick:不得重复输出。
	agg.Tick()
	if got := findByMsg(recs(), "prefetch: 下载汇总"); len(got) != 1 {
		t.Fatalf("重复 Tick 后汇总 %d 条, want 1", len(got))
	}
}

// TestCacheSummaryPerGroup: 命中/未命中/read_miss 按 dataset×reason×phase×source
// 各输出一条 Info,计数正确。
func TestCacheSummaryPerGroup(t *testing.T) {
	clk := newFakeClock()
	logger, recs := capture()
	agg := New(Options{Interval: 60 * time.Second, Now: clk.Now, Logger: logger})

	agg.ObserveCacheHit("ds", 0, "prefetch", "steady")
	agg.ObserveCacheHit("ds", 1, "prefetch", "steady")
	agg.ObserveCacheMiss("ds", 2, "steady")
	agg.ObserveReadMiss("ds", 3, "absent", "steady")

	clk.Advance(60 * time.Second)
	agg.Tick()

	sum := findByMsg(recs(), "cache: 命中汇总")
	if len(sum) != 3 {
		t.Fatalf("缓存汇总 %d 条, want 3(hit/miss/absent): %#v", len(sum), sum)
	}
	byReason := map[string]map[string]any{}
	for _, rec := range sum {
		if rec["level"] != "INFO" {
			t.Fatalf("缓存汇总级别 = %v, want INFO", rec["level"])
		}
		byReason[rec["reason"].(string)] = rec
	}
	if got := num(t, byReason["hit"], "hits"); got != 2 {
		t.Errorf("hits = %v, want 2", got)
	}
	if byReason["hit"]["source"] != "prefetch" || byReason["hit"]["phase"] != "steady" {
		t.Errorf("hit 组 source/phase 不符: %#v", byReason["hit"])
	}
	if got := num(t, byReason["miss"], "misses"); got != 1 {
		t.Errorf("misses = %v, want 1", got)
	}
	if _, ok := byReason["miss"]["source"]; ok {
		t.Errorf("无 source 的分组不应带 source: %#v", byReason["miss"])
	}
	if got := num(t, byReason["absent"], "read_miss"); got != 1 {
		t.Errorf("read_miss = %v, want 1", got)
	}
}

// TestFlushTaskEarly: 任务结束时提前收口该任务,窗口到期不再重复输出。
func TestFlushTaskEarly(t *testing.T) {
	clk := newFakeClock()
	logger, recs := capture()
	agg := New(Options{Interval: 60 * time.Second, Now: clk.Now, Logger: logger})

	agg.ObserveDownload(Download{TaskID: "t1", Dataset: "ds", Size: 10, Ready: true, Duration: time.Millisecond})
	agg.ObserveDownload(Download{TaskID: "t2", Dataset: "ds", Size: 20, Ready: true, Duration: time.Millisecond})

	agg.FlushTask("t1")
	sum := findByMsg(recs(), "prefetch: 下载汇总")
	if len(sum) != 1 || sum[0]["task_id"] != "t1" {
		t.Fatalf("任务结束提前汇总不符: %#v", sum)
	}

	clk.Advance(60 * time.Second)
	agg.Tick()
	sum = findByMsg(recs(), "prefetch: 下载汇总")
	if len(sum) != 2 || sum[1]["task_id"] != "t2" {
		t.Fatalf("窗口到期应只剩 t2: %#v", sum)
	}
}

// TestFailureKeepsWarn: 失败块只进汇总与 Debug 明细;调用方的 Warn(采样率为 0 也
// 不丢)照常落盘,失败场景不因汇总降级。
func TestFailureKeepsWarn(t *testing.T) {
	t.Setenv("TRAINCACHE_LOG_SAMPLE_RATE", "0")
	t.Setenv("TRAINCACHE_LOG_DEBUG_SAMPLE_RATE", "0")
	clk := newFakeClock()
	var buf bytes.Buffer
	logger := slog.New(corelog.NewSamplingHandler(
		slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		corelog.NewAgentSampler("test")))

	agg := New(Options{Interval: 60 * time.Second, Now: clk.Now, Logger: logger})
	agg.ObserveDownload(Download{TaskID: "t1", Dataset: "ds", Ready: false, Duration: time.Millisecond})
	logger.Warn("prefetch: 块下载失败", "task_id", "t1", "dataset", "ds", "reason", "io")

	read := func() []map[string]any {
		var out []map[string]any
		for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if raw == "" {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(raw), &rec); err != nil {
				t.Fatalf("invalid log line %q: %v", raw, err)
			}
			out = append(out, rec)
		}
		return out
	}

	recs := read()
	if len(recs) != 1 || recs[0]["level"] != "WARN" {
		t.Fatalf("失败场景 Warn 丢失或级别变化: %#v", recs)
	}

	// Info 采样率 0:汇总被采样丢弃,但这不是失败路径的唯一记录(Warn 已在上面断言)。
	clk.Advance(60 * time.Second)
	agg.Tick()
	if got := findByMsg(read(), "prefetch: 下载汇总"); len(got) != 0 {
		t.Fatalf("Info 采样率 0 时仍输出了汇总: %#v", got)
	}
}
