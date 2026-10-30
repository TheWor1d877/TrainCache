package prefetch

// 8.1 §4.2:逐块下载只累加,任务结束由预热侧调用 FlushTask 输出一条汇总;本用例
// 用白盒抓取钩子构造两成一败,断言计数、字段与"只输出一条 Info"。

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"traincache/agent/logagg"
)

func TestLogAggDownloadSummaryTaskScoped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(t, nil)

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	agg := logagg.New(logagg.Options{Verify: "xxh3", Logger: logger})

	done := make(chan struct{}, 3)
	p := env.newPrefetcher(Config{
		LogAgg: agg,
		OnP0Done: func(ChunkRef, error) {
			done <- struct{}{}
		},
	})
	p.grab = func(_ context.Context, _ string, ref ChunkRef) (int64, bool) {
		if ref.Path == "bad.tar" {
			return 100, false
		}
		return 100, true
	}
	// 绑定 dataset(P0 入队快照用它拼回源键;本用例只走白盒 grab)。
	if _, err := p.OnStageSwitch(ctx, ConsumeOrder{Dataset: testDataset}); err != nil {
		t.Fatalf("OnStageSwitch: %v", err)
	}
	startPrefetcher(t, p, ctx, cancel)

	refs := []ChunkRef{{Path: "a.tar"}, {Path: "bad.tar"}, {Path: "c.tar"}}
	if err := p.EnqueueP0ScopedTask(ctx, ctx, "task-x", refs); err != nil {
		t.Fatalf("EnqueueP0ScopedTask: %v", err)
	}
	for i := 0; i < len(refs); i++ {
		select {
		case <-done:
		case <-time.After(testTimeout):
			t.Fatalf("等待块完成超时(%d/%d)", i, len(refs))
		}
	}

	agg.FlushTask("task-x")
	recs := parseRecords(t, &buf)
	var sums []map[string]any
	for _, rec := range recs {
		if rec["msg"] == "prefetch: 下载汇总" {
			sums = append(sums, rec)
		}
	}
	if len(sums) != 1 {
		t.Fatalf("任务结束汇总 %d 条, want 1: %s", len(sums), buf.String())
	}
	rec := sums[0]
	if rec["level"] != "INFO" {
		t.Errorf("汇总级别 = %v, want INFO", rec["level"])
	}
	if rec["task_id"] != "task-x" || rec["dataset"] != testDataset || rec["verify"] != "xxh3" {
		t.Errorf("汇总标签不符: %#v", rec)
	}
	for key, want := range map[string]float64{"chunk": 3, "size": 200, "failed": 1} {
		got, ok := rec[key].(float64)
		if !ok || got != want {
			t.Errorf("%s = %v, want %v", key, rec[key], want)
		}
	}
	for _, rec := range recs {
		if rec["msg"] == "prefetch: 块完成" && rec["level"] != "DEBUG" {
			t.Errorf("逐块明细级别 = %v, want DEBUG", rec["level"])
		}
	}
}

func parseRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
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
		out = append(out, rec)
	}
	return out
}
