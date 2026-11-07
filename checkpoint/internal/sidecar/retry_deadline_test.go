package sidecar

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"traincache/checkpoint/internal/plugin"
)

// 本文件覆盖 10.3 覆盖矩阵里的「保存重试与总时限」一行：已有用例注入的是快速失败的
// CodeUnavailable，只锁住了重试次数；这里注入每次都慢的后端——一次尝试
// 吃掉预算里很大一段，而不是立刻返回错误。对应 10.2 D12：保存与恢复
// 都加总时限，到点停止重试并快速失败。
//
// 真实预算是 15 分钟总时限、5 次 30 秒间隔，单测按比例缩到毫秒级，
// 但形状保持两种：慢尝试占满预算（D12 说「极端慢链路会提前失败」）与
// 30 秒间隔叠加慢尝试。

// slowBackend 是每次都慢的插件替身。一次尝试要么跑满 each 再以 UNAVAILABLE
// 失败，要么在总时限到点时被 saveCtx 掐断；两条路径都返回可重试错误，
// 因此只有总时限能终止循环（10.1「关键流程」）。
type slowBackend struct {
	each time.Duration

	mu        sync.Mutex
	calls     int
	remaining []time.Duration // 每次尝试开始时距离总时限还剩多少
}

func (b *slowBackend) save(ctx context.Context, _ plugin.SaveParams) (plugin.TransferResult, error) {
	b.mu.Lock()
	b.calls++
	if deadline, ok := ctx.Deadline(); ok {
		b.remaining = append(b.remaining, time.Until(deadline))
	}
	calls := b.calls
	b.mu.Unlock()

	timer := time.NewTimer(b.each)
	defer timer.Stop()
	select {
	case <-timer.C:
		return plugin.TransferResult{}, &plugin.Error{
			Code:    plugin.CodeUnavailable,
			Message: fmt.Sprintf("后端响应慢，第 %d 次尝试未返回", calls),
		}
	case <-ctx.Done():
		// 非插件错误按 INTERNAL 分类（protocol.go CodeOf），同样可重试，
		// 所以这里不能靠错误码终止，只能靠总时限。
		return plugin.TransferResult{}, fmt.Errorf("后端在时限内未响应: %w", ctx.Err())
	}
}

func (b *slowBackend) snapshot() (int, []time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls, append([]time.Duration(nil), b.remaining...)
}

// TestSlowBackendStopsRetryingAtDeadline 断言慢后端在总时限到点时停止重试：
// 尝试次数被预算截断（小于次数上限）、耗时不超过时限太多、终态只有一条
// ERROR 且 retry 与实际尝试次数一致、工作目录保留并停止接收新 step。
func TestSlowBackendStopsRetryingAtDeadline(t *testing.T) {
	cases := []struct {
		name        string
		deadline    time.Duration
		each        time.Duration
		intervals   []time.Duration
		wantMinCall int
		wantMaxCall int // 次数上限 len(intervals)+1 会更多，这里给预算允许的值
		wantSlack   time.Duration
	}{
		{
			// 形状一：每次尝试约 60ms，200ms 预算只装得下 4 次；
			// 不封顶则跑满 6 次、约 305ms。
			name:        "慢尝试吃掉预算",
			deadline:    200 * time.Millisecond,
			each:        60 * time.Millisecond,
			intervals:   []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond},
			wantMinCall: 2,
			wantMaxCall: 4,
			wantSlack:   80 * time.Millisecond,
		},
		{
			// 形状二：30 秒间隔按比例缩到 30ms（10.1「关键流程」的 5 段间隔），
			// 间隔本身吃掉预算；不封顶则 6 次尝试加 5 段间隔约 168ms。
			name:        "30秒节奏的间隔叠加慢尝试",
			deadline:    100 * time.Millisecond,
			each:        3 * time.Millisecond,
			intervals:   []time.Duration{30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond, 30 * time.Millisecond},
			wantMinCall: 2,
			wantMaxCall: 4,
			wantSlack:   40 * time.Millisecond,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := testOptions(t)
			log, buf := newTestLogger()
			opts.Logger = log
			opts.SaveDeadline = tc.deadline
			opts.SaveRetryIntervals = tc.intervals

			backend := &slowBackend{each: tc.each}
			f := &fakeFactory{build: func() *fakePlugin {
				return &fakePlugin{save: backend.save}
			}}
			s := newTestSidecar(t, opts, f)
			dir := writeStep(t, opts.TaskDir, 41, "abcdef", true)

			start := time.Now()
			err := s.processSave(context.Background(), 41, dir)
			elapsed := time.Since(start)

			if err == nil {
				t.Fatal("慢后端在总时限到达后应返回失败")
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("终态错误应是被总时限封顶: %v", err)
			}
			calls, remaining := backend.snapshot()
			if calls < tc.wantMinCall {
				t.Fatalf("尝试次数 = %d, want >= %d（慢后端应至少重试一次）", calls, tc.wantMinCall)
			}
			if calls > tc.wantMaxCall {
				t.Fatalf("时限到达后仍在重试: 尝试 %d 次, want <= %d（次数上限 %d）",
					calls, tc.wantMaxCall, len(tc.intervals)+1)
			}
			if elapsed < tc.deadline {
				t.Fatalf("耗时 %s 小于总时限 %s，说明提前放弃重试", elapsed, tc.deadline)
			}
			if elapsed > tc.deadline+tc.wantSlack {
				t.Fatalf("耗时 %s 超出总时限 %s 太多，说明时限没有终止重试", elapsed, tc.deadline)
			}
			// 总时限必须传到插件调用上，否则慢后端只会跑满整段预算。
			if len(remaining) != calls {
				t.Fatalf("记录到 deadline 的尝试数 = %d, want %d", len(remaining), calls)
			}
			if remaining[0] > tc.deadline || remaining[0] < tc.deadline-30*time.Millisecond {
				t.Fatalf("首次尝试看到的剩余预算 = %s, want 接近总时限 %s", remaining[0], tc.deadline)
			}
			for i, r := range remaining {
				// 到点后不再发起新尝试由上面的次数上限把关；这里只允许
				// 定时器与时钟之间的毫秒级抖动，不允许越过后还跑很久。
				if r < -10*time.Millisecond {
					t.Fatalf("第 %d 次尝试在时限外 %s 才开始", i+1, -r)
				}
			}
			if f.count() != calls {
				t.Fatalf("插件启动次数 = %d, want %d", f.count(), calls)
			}

			// 10.1「关键流程」：失败后保留数据、停止接收新 step，写一条 ERROR。
			if !pathExists(dir) {
				t.Fatal("失败后应保留工作目录")
			}
			s.mu.Lock()
			accepting, failed := s.accepting, s.failed
			s.mu.Unlock()
			if accepting || !failed {
				t.Fatalf("accepting=%v failed=%v, want false/true", accepting, failed)
			}

			logs := buf.String()
			line := onlyFinalErrorLine(t, logs)
			if got := intField(t, line, "retry"); got != calls {
				t.Fatalf("终态 ERROR 的 retry = %d, want 实际尝试次数 %d\n%s", got, calls, logs)
			}
			if !strings.Contains(line, "err_class=") {
				t.Fatalf("终态 ERROR 缺 err 与 err_class 成对字段:\n%s", line)
			}
			if !strings.Contains(line, "step=41") || !strings.Contains(line, "task_id=") {
				t.Fatalf("终态 ERROR 缺 step 或 task_id:\n%s", line)
			}
		})
	}
}

// TestSlowBackendKeepsSingleTerminalError 确认时限到点后终态 ERROR 只有一条，
// 而不是每轮重试各写一条终态；逐次失败仍按 10.1「可观测出口」单独记「保存失败」。
func TestSlowBackendKeepsSingleTerminalError(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	opts.SaveDeadline = 120 * time.Millisecond
	opts.SaveRetryIntervals = []time.Duration{2 * time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond}

	backend := &slowBackend{each: 30 * time.Millisecond}
	f := &fakeFactory{build: func() *fakePlugin { return &fakePlugin{save: backend.save} }}
	s := newTestSidecar(t, opts, f)
	dir := writeStep(t, opts.TaskDir, 42, "abcdef", true)

	if err := s.processSave(context.Background(), 42, dir); err == nil {
		t.Fatal("慢后端在总时限到达后应返回失败")
	}
	logs := buf.String()
	line := onlyFinalErrorLine(t, logs)
	if strings.Contains(line, "level=WARN") {
		t.Fatalf("终态失败应是 ERROR:\n%s", line)
	}
	calls, _ := backend.snapshot()
	// 10.1「可观测出口」：保存失败是 ERROR 级并带 err 与 err_class；逐次一条，
	// 级别降到 Debug 就不再是「保存或恢复失败」这一日志点。
	if got := countErrorLines(t, logs, "msg=保存失败"); got != calls {
		t.Fatalf("逐次失败的 ERROR 条数 = %d, want 尝试次数 %d\n%s", got, calls, logs)
	}
	// 队列在终态后不再接收新 step（10.1「关键流程」）。
	if s.enqueue(43, dir) {
		t.Fatal("保存失败后不应再接收新 step")
	}
}

// countErrorLines 统计 ERROR 级且包含 want 片段的行数；级别与消息一起算，
// 免得把 ERROR 降级成 Debug 的改动只靠消息文本也能数出同样的条数。
func countErrorLines(t *testing.T, logs, want string) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, want) {
			n++
		}
	}
	return n
}

// onlyFinalErrorLine 返回唯一的终态 ERROR 行，条数不是 1 时直接失败。
func onlyFinalErrorLine(t *testing.T, logs string) string {
	t.Helper()
	var hits []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "保存最终失败") {
			hits = append(hits, line)
		}
	}
	if len(hits) != 1 {
		t.Fatalf("终态 ERROR 条数 = %d, want 1:\n%s", len(hits), logs)
	}
	if !strings.Contains(hits[0], "level=ERROR") {
		t.Fatalf("终态日志级别不是 ERROR:\n%s", hits[0])
	}
	return hits[0]
}

// intField 从 slog 文本行里取一个整数属性。
func intField(t *testing.T, line, key string) int {
	t.Helper()
	for _, field := range strings.Fields(line) {
		if rest, ok := strings.CutPrefix(field, key+"="); ok {
			n, err := strconv.Atoi(rest)
			if err != nil {
				t.Fatalf("字段 %s=%q 不是整数:\n%s", key, rest, line)
			}
			return n
		}
	}
	t.Fatalf("日志行缺字段 %s:\n%s", key, line)
	return 0
}
