package svc

// 8.1 §4.2:read_miss 只进 60s 汇总(指标口径不变),失败路径原有 Warn 不受影响;
// 预热任务结束时提前输出该任务的下载汇总。

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"traincache/agent/logagg"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
)

// svcClock 是测试用可控时钟。
type svcClock struct {
	mu sync.Mutex
	t  time.Time
}

func newSvcClock() *svcClock {
	return &svcClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *svcClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *svcClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// findAllLogsTS 返回线程安全 buffer 中 msg 精确匹配的全部 JSON 记录。
func findAllLogsTS(t *testing.T, buf *m12threadSafeBuf, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("invalid log line %q: %v", raw, err)
		}
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// TestReadMissAggregateKeepsWarn: 坏块 read_miss → 一条 Warn 照旧;窗口到期只出
// 一条 read_miss 汇总,计数为 1。
func TestReadMissAggregateKeepsWarn(t *testing.T) {
	buf := m12CaptureLogsTS(t)

	ag, cfg, _ := newBackfillAgent(t)
	clk := newSvcClock()
	ag.LogAgg = logagg.New(logagg.Options{Now: clk.Now})

	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "bad-000.tar", content: "bad-content", source: true})
	fp := fps["bad-000.tar#0"]
	preloadBackfillMapping(t, ag, wiringDataset)
	if err := os.WriteFile(wiringBlockFile(cfg, fp), []byte("corrupted-not-matching"), 0o644); err != nil {
		t.Fatalf("破坏本地块: %v", err)
	}
	ag.Rescan = nil // 坏块只能隔离:这条 Warn 是本用例的断言目标

	ag.handleReadMiss(ordersource.ChunkRef{Path: "bad-000.tar"}, "corrupt")
	rec := m12WaitForLogTS(t, buf, "backfill: 未装配重扫描修复器,坏块仅隔离", hookTestTimeout)
	m12RequireLevel(t, rec, "WARN")

	// 窗口未到期:不得出现汇总。
	ag.LogAgg.Tick()
	if got := findAllLogsTS(t, buf, "cache: 命中汇总"); len(got) != 0 {
		t.Fatalf("窗口未到期就输出了汇总: %#v", got)
	}

	clk.Advance(60 * time.Second)
	ag.LogAgg.Tick()
	sums := findAllLogsTS(t, buf, "cache: 命中汇总")
	if len(sums) != 1 {
		t.Fatalf("read_miss 汇总 %d 条, want 1", len(sums))
	}
	sum := sums[0]
	m12RequireLevel(t, sum, "INFO")
	m12RequireFields(t, sum, "dataset", "reason", "phase", "hits", "misses", "read_miss")
	if sum["dataset"] != wiringDataset {
		t.Errorf("dataset = %v, want %s", sum["dataset"], wiringDataset)
	}
	if sum["reason"] != "corrupt" {
		t.Errorf("reason = %v, want corrupt", sum["reason"])
	}
	if got := sum["read_miss"].(float64); got != 1 {
		t.Errorf("read_miss = %v, want 1", got)
	}
}

// TestPreheatEndFlushesDownloadSummary: 任务终态时提前输出该任务下载汇总,窗口到期
// 不再重复输出。
func TestPreheatEndFlushesDownloadSummary(t *testing.T) {
	buf := m12CaptureLogsTS(t)

	clk := newSvcClock()
	agg := logagg.New(logagg.Options{Now: clk.Now})
	tasks := NewPreheatTasks(m12PreheatCfg(), nil)
	tasks.SetLogAggregator(agg)

	tk := newPreheatTask("task-end-agg", "worker-01", preheatRequest{dataset: testDataset})
	tk.logAgg = agg
	refs := []prefetch.ChunkRef{{Path: "a-000.tar"}}
	tasks.mu.Lock()
	tasks.tasks[tk.taskID] = tk
	tasks.mu.Unlock()
	tasks.register(tk, refs)

	agg.ObserveDownload(logagg.Download{TaskID: tk.taskID, Dataset: testDataset, Size: 10, Ready: true})
	tk.completed.Store(1)
	tk.total.Store(1)
	tk.finish()

	sums := findAllLogsTS(t, buf, "prefetch: 下载汇总")
	if len(sums) != 1 {
		t.Fatalf("任务结束汇总 %d 条, want 1", len(sums))
	}
	if sums[0]["task_id"] != tk.taskID {
		t.Errorf("task_id = %v, want %s", sums[0]["task_id"], tk.taskID)
	}

	clk.Advance(60 * time.Second)
	agg.Tick()
	if got := findAllLogsTS(t, buf, "prefetch: 下载汇总"); len(got) != 1 {
		t.Fatalf("窗口到期重复输出: %#v", got)
	}
}
