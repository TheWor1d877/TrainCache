package svc

// hooksock_meta_test.go:C6 钩子帧任务身份(帧字段口径见 8.0)——旧帧/新帧/混合帧表驱动,
// 断言 Meta 回调先于类型回调、取值逐字一致;旧帧(含遗留 task/seq 键)的类型回调次数与
// 行为不变。

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// metaEvent 是一次回调记录:kind=meta 表示任务身份回调,其余为类型回调(读循环单 goroutine
// 顺序调用,按序入通道)。
type metaEvent struct {
	kind   string
	frame  string
	meta   HookMeta
	ref    ordersource.ChunkRef
	reason string
	stage  string
	seed   int64
	waitMS int64
}

// startHookSockMeta 起一个五型回调 + Meta 都记录到同一通道的收端;清理时先取消 ctx 再 Close。
func startHookSockMeta(t *testing.T, path string, m *metrics.Registry) chan metaEvent {
	t.Helper()
	rec := make(chan metaEvent, 32)
	ctx, cancel := context.WithCancel(context.Background())
	s := New(path, m, HookHandlers{
		Meta: func(kind string, hm HookMeta) {
			rec <- metaEvent{kind: "meta", frame: kind, meta: hm}
		},
		Advance: func(ref ordersource.ChunkRef) { rec <- metaEvent{kind: "advance", ref: ref} },
		Open:    func(ref ordersource.ChunkRef) { rec <- metaEvent{kind: "open", ref: ref} },
		StageSwitch: func(stage string, seed int64) {
			rec <- metaEvent{kind: "stage", stage: stage, seed: seed}
		},
		Miss: func(ref ordersource.ChunkRef, reason string) {
			rec <- metaEvent{kind: "miss", ref: ref, reason: reason}
		},
		Stall: func(ref ordersource.ChunkRef, waitMS int64, _ time.Time) {
			rec <- metaEvent{kind: "stall", ref: ref, waitMS: waitMS}
		},
	})
	if err := s.Start(ctx); err != nil {
		cancel()
		t.Fatalf("HookSock.Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = s.Close()
	})
	return rec
}

// recvMetaEvents 带超时按序收取 n 条回调记录。
func recvMetaEvents(t *testing.T, ch <-chan metaEvent, n int) []metaEvent {
	t.Helper()
	out := make([]metaEvent, 0, n)
	for i := 0; i < n; i++ {
		select {
		case ev := <-ch:
			out = append(out, ev)
		case <-time.After(hookTestTimeout):
			t.Fatalf("等待钩子回调超时:已收到 %d/%d 条(%+v)", i, n, out)
		}
	}
	return out
}

// TestHookSockFrameMeta:表驱动校验帧身份——旧帧 Meta 以零值回调且类型回调行为不变;
// 新帧身份逐字段透传;混合帧按序分发;遗留 task/seq 键被忽略(帧字段口径不出现它们)。
func TestHookSockFrameMeta(t *testing.T) {
	full := HookMeta{WarmupTask: "ns/warmup-a", Pod: "ns/worker-0", PodIndex: 3, Dataset: "ds-8m", Generation: 7}
	ref := func(p string, c int64) ordersource.ChunkRef { return ordersource.ChunkRef{Path: p, Chunk: c} }

	cases := []struct {
		name    string
		payload string
		want    []metaEvent
	}{
		{
			name:    "旧帧 advance 无身份",
			payload: `{"type":"advance","path":"p1.tar","chunk":1}`,
			want: []metaEvent{
				{kind: "meta", frame: "advance"},
				{kind: "advance", ref: ref("p1.tar", 1)},
			},
		},
		{
			name: "新帧 read_miss 全身份",
			payload: `{"type":"read_miss","path":"p2.tar","chunk":2,"reason":"corrupt",` +
				`"warmup_task":"ns/warmup-a","pod":"ns/worker-0","pod_index":3,"dataset":"ds-8m","generation":7}`,
			want: []metaEvent{
				{kind: "meta", frame: "read_miss", meta: full},
				{kind: "miss", ref: ref("p2.tar", 2), reason: "corrupt"},
			},
		},
		{
			name:    "部分身份 read_stall",
			payload: `{"type":"read_stall","path":"p3.tar","chunk":3,"wait_ms":120,"warmup_task":"ns/warmup-a","generation":8}`,
			want: []metaEvent{
				{kind: "meta", frame: "read_stall", meta: HookMeta{WarmupTask: "ns/warmup-a", Generation: 8}},
				{kind: "stall", ref: ref("p3.tar", 3), waitMS: 120},
			},
		},
		{
			name: "混合帧同数据报(旧/新/旧)",
			payload: `{"type":"advance","path":"p4.tar","chunk":4}` + "\n" +
				`{"type":"stage_switch","stage_id":"epoch-2","shuffle_seed":12345,"warmup_task":"ns/warmup-b","pod_index":1}` + "\n" +
				`{"type":"open_report","path":"p4.tar","chunk":5}`,
			want: []metaEvent{
				{kind: "meta", frame: "advance"},
				{kind: "advance", ref: ref("p4.tar", 4)},
				{kind: "meta", frame: "stage_switch", meta: HookMeta{WarmupTask: "ns/warmup-b", PodIndex: 1}},
				{kind: "stage", stage: "epoch-2", seed: 12345},
				{kind: "meta", frame: "open_report"},
				{kind: "open", ref: ref("p4.tar", 5)},
			},
		},
		{
			name:    "遗留 task/seq 键被忽略",
			payload: `{"type":"open_report","path":"p5.tar","chunk":6,"task":"x","seq":9}`,
			want: []metaEvent{
				{kind: "meta", frame: "open_report"},
				{kind: "open", ref: ref("p5.tar", 6)},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newHookMetrics()
			path := hookPath(t)
			rec := startHookSockMeta(t, path, m)
			c := dialHook(t, path)

			sendHook(t, c, []byte(tc.payload))
			got := recvMetaEvents(t, rec, len(tc.want))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("回调序列 → %+v, want %+v", got, tc.want)
			}
			if n := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)); n != 0 {
				t.Errorf("合法帧不应计 parse 错误,实际 %v", n)
			}
		})
	}
}

// TestHookSockFrameMetaUnknownType:未知帧类型仍只计 parse 丢弃,不回调任务身份。
func TestHookSockFrameMetaUnknownType(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	rec := startHookSockMeta(t, path, m)
	c := dialHook(t, path)

	sendHook(t, c, []byte(`{"type":"bogus","warmup_task":"ns/warmup-a","generation":7}`))
	expectNoMetaEvent(t, rec)
	if n := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)); n != 1 {
		t.Errorf("未知类型应计 1 次 parse 错误,实际 %v", n)
	}
}

// TestHookHandlersMetaReachesBackfill:接线层 Meta → Miss 的身份转交——帧身份最终落到后台
// 补块日志字段(warmup_task/pod/pod_index/generation);task_id 由 Agent 侧反查,不在帧里。
func TestHookHandlersMetaReachesBackfill(t *testing.T) {
	buf := m12CaptureLogsTS(t)

	ag, cfg, _ := newBackfillAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "bad-000.tar", content: "bad-content", source: true})
	fp := fps["bad-000.tar#0"]
	preloadBackfillMapping(t, ag, wiringDataset)
	if err := os.WriteFile(wiringBlockFile(cfg, fp), []byte("corrupted-not-matching"), 0o644); err != nil {
		t.Fatalf("破坏本地块: %v", err)
	}
	ag.Rescan = nil // 坏块只能隔离:该 Warn 是断言目标

	// 按 hooksock 的分发顺序:同一 goroutine 内先 Meta 再类型回调。
	h := ag.hookHandlers()
	h.Meta("read_miss", HookMeta{WarmupTask: "ns/warmup-a", Pod: "ns/worker-0", PodIndex: 2, Generation: 5})
	h.Miss(ordersource.ChunkRef{Path: "bad-000.tar"}, "corrupt")

	rec := m12WaitForLogTS(t, buf, "backfill: 未装配重扫描修复器,坏块仅隔离", hookTestTimeout)
	m12RequireFields(t, rec, "warmup_task", "pod", "pod_index", "generation")
	if rec["warmup_task"] != "ns/warmup-a" || rec["pod"] != "ns/worker-0" ||
		rec["pod_index"] != float64(2) || rec["generation"] != float64(5) {
		t.Errorf("补块日志身份字段 → %v/%v/%v/%v, want ns/warmup-a/ns/worker-0/2/5",
			rec["warmup_task"], rec["pod"], rec["pod_index"], rec["generation"])
	}
}

// expectNoMetaEvent 负断言:观察窗内不应有 Meta 或类型回调。
func expectNoMetaEvent(t *testing.T, ch <-chan metaEvent) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("不应有回调,却收到 %+v", ev)
	case <-time.After(hookQuietWindow):
	}
}
