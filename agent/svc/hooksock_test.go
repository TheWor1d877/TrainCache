package svc

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// hooksock_test.go:HookSock 收端(§5.4/D33)——三型帧分发、行分隔、非法帧丢弃计数、
// 超 4 KiB 丢弃、nil handler 丢弃、生命周期(Start/Close/ctx 取消)。

const (
	hookTestTimeout = 3 * time.Second
	hookQuietWindow = 150 * time.Millisecond
)

// hookRec 是一次 handler 调用的记录(读循环单 goroutine 顺序调用)。
type hookRec struct {
	kind  string
	ref   ordersource.ChunkRef
	stage string
	seed  int64
}

// newHookMetrics 构造钩子测试用指标库(node 非空即可)。
func newHookMetrics() *metrics.Registry {
	return metrics.New(metrics.Config{Node: "worker-01"})
}

// hookPath 返回临时目录下的套接字路径(Unix sun_path 限长,保持短名)。
func hookPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "agent.sock")
}

// startHookSock 起一个收端,handler 统一转发到记录通道;清理时先取消 ctx 再 Close。
func startHookSock(t *testing.T, path string, m *metrics.Registry) (*HookSock, chan hookRec) {
	t.Helper()
	rec := make(chan hookRec, 16)
	ctx, cancel := context.WithCancel(context.Background())
	s := New(path, m, HookHandlers{
		Advance:     func(ref ordersource.ChunkRef) { rec <- hookRec{kind: "advance", ref: ref} },
		Open:        func(ref ordersource.ChunkRef) { rec <- hookRec{kind: "open", ref: ref} },
		StageSwitch: func(stage string, seed int64) { rec <- hookRec{kind: "stage", stage: stage, seed: seed} },
	})
	if err := s.Start(ctx); err != nil {
		cancel()
		t.Fatalf("HookSock.Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = s.Close()
	})
	return s, rec
}

// dialHook 以 unixgram 连接收端(内核自动绑定发送端)。
func dialHook(t *testing.T, path string) *net.UnixConn {
	t.Helper()
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatalf("连接钩子套接字 %s: %v", path, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// sendHook 发一个数据报。
func sendHook(t *testing.T, c *net.UnixConn, payload []byte) {
	t.Helper()
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("发送钩子帧: %v", err)
	}
}

// recvHook 带超时接收一次 handler 调用记录。
func recvHook(t *testing.T, ch <-chan hookRec) hookRec {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(hookTestTimeout):
		t.Fatal("等待钩子 handler 调用超时")
		return hookRec{}
	}
}

// expectNoHook 负断言:观察窗内不应有 handler 调用。
func expectNoHook(t *testing.T, ch <-chan hookRec) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("不应有 handler 调用,却收到 %+v", r)
	case <-time.After(hookQuietWindow):
	}
}

// TestHookSockDispatchThreeFrameTypes:三型帧(§5.4)分发到对应 handler,字段逐字一致。
func TestHookSockDispatchThreeFrameTypes(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	_, rec := startHookSock(t, path, m)
	c := dialHook(t, path)

	sendHook(t, c, []byte(`{"type":"advance","path":"shard-000.tar","chunk":3}`))
	if got := recvHook(t, rec); got.kind != "advance" || got.ref != (ordersource.ChunkRef{Path: "shard-000.tar", Chunk: 3}) {
		t.Errorf("advance 帧 → %+v, want advance{shard-000.tar,3}", got)
	}

	sendHook(t, c, []byte(`{"type":"open_report","path":"shard-007.tar","chunk":9}`))
	if got := recvHook(t, rec); got.kind != "open" || got.ref != (ordersource.ChunkRef{Path: "shard-007.tar", Chunk: 9}) {
		t.Errorf("open_report 帧 → %+v, want open{shard-007.tar,9}", got)
	}

	sendHook(t, c, []byte(`{"type":"stage_switch","stage_id":"epoch-2","shuffle_seed":12345}`))
	if got := recvHook(t, rec); got.kind != "stage" || got.stage != "epoch-2" || got.seed != 12345 {
		t.Errorf("stage_switch 帧 → %+v, want stage{epoch-2,12345}", got)
	}

	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)); got != 0 {
		t.Errorf("合法帧不应计 parse 错误,实际 %v", got)
	}
	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize)); got != 0 {
		t.Errorf("合法帧不应计 size 错误,实际 %v", got)
	}
}

// TestHookSockLineDelimited:行分隔(含尾部换行)与单报多行均按帧处理;重复类型帧按序分发。
func TestHookSockLineDelimited(t *testing.T) {
	path := hookPath(t)
	_, rec := startHookSock(t, path, newHookMetrics())
	c := dialHook(t, path)

	sendHook(t, c, []byte("{\"type\":\"advance\",\"path\":\"p\",\"chunk\":1}\n"))
	sendHook(t, c, []byte("{\"type\":\"advance\",\"path\":\"p\",\"chunk\":2}\n{\"type\":\"advance\",\"path\":\"p\",\"chunk\":3}\n"))
	sendHook(t, c, []byte("\n\n{\"type\":\"advance\",\"path\":\"p\",\"chunk\":4}\n")) // 空行跳过

	for want := int64(1); want <= 4; want++ {
		if got := recvHook(t, rec); got.ref.Chunk != want {
			t.Fatalf("帧序 %d → %+v, want chunk=%d(顺序分发)", want, got, want)
		}
	}
}

// TestHookSockInvalidJSONCounted:非法 JSON 丢弃并计 parse;后续帧不受影响。
func TestHookSockInvalidJSONCounted(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	_, rec := startHookSock(t, path, m)
	c := dialHook(t, path)

	sendHook(t, c, []byte("not json"))
	mustHookEventually(t, "parse 计数", func() bool {
		return testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)) == 1
	})
	sendHook(t, c, []byte(`{"type":"advance","path":"p","chunk":7}`))
	if got := recvHook(t, rec); got.ref.Chunk != 7 {
		t.Errorf("坏帧后续帧未分发: %+v", got)
	}
}

// TestHookSockUnknownTypeCounted:未知帧类型丢弃并计 parse(D33:集合外不新增基数)。
func TestHookSockUnknownTypeCounted(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	_, rec := startHookSock(t, path, m)
	c := dialHook(t, path)

	sendHook(t, c, []byte(`{"type":"bogus","path":"p","chunk":1}`))
	mustHookEventually(t, "parse 计数", func() bool {
		return testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)) == 1
	})
	expectNoHook(t, rec)
}

// TestHookSockOversizeDropped:超 4 KiB 帧整报丢弃并计 size;恰为 4 KiB 的帧仍受理(边界)。
func TestHookSockOversizeDropped(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	_, rec := startHookSock(t, path, m)
	c := dialHook(t, path)

	// 恰为 4 KiB:合法帧 + 尾部空白填充 → 受理。
	base := `{"type":"advance","path":"p","chunk":5}`
	exact := base + strings.Repeat(" ", hookMaxFrame-len(base))
	if len(exact) != hookMaxFrame {
		t.Fatalf("构造 4 KiB 帧长度 = %d", len(exact))
	}
	sendHook(t, c, []byte(exact))
	if got := recvHook(t, rec); got.ref.Chunk != 5 {
		t.Errorf("4 KiB 边界帧未受理: %+v", got)
	}

	// 4 KiB + 1:丢弃 + size 计数。
	oversize := base + strings.Repeat(" ", hookMaxFrame+1-len(base))
	sendHook(t, c, []byte(oversize))
	mustHookEventually(t, "size 计数", func() bool {
		return testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize)) == 1
	})
	expectNoHook(t, rec)
}

// TestHookSockNilHandlerDropped:nil handler(未接线)→ 合法帧静默丢弃,不计数不 panic。
func TestHookSockNilHandlerDropped(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := New(path, m, HookHandlers{})
	if err := s.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { cancel(); _ = s.Close() })
	c := dialHook(t, path)

	sendHook(t, c, []byte(`{"type":"advance","path":"p","chunk":1}`))
	sendHook(t, c, []byte(`{"type":"open_report","path":"p","chunk":2}`))
	sendHook(t, c, []byte(`{"type":"stage_switch","stage_id":"epoch-2","shuffle_seed":1}`))
	time.Sleep(hookQuietWindow)
	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)); got != 0 {
		t.Errorf("nil handler 为合法帧,应不计数,实际 parse=%v", got)
	}
	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize)); got != 0 {
		t.Errorf("nil handler 应不计数,实际 size=%v", got)
	}
}

// TestHookSockLifecycle:Start 建父目录与套接字;Close 幂等并删除套接字;
// Close 后/重复 Start 报错(§5.4 收端生命周期)。
func TestHookSockLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "agent.sock") // 父目录不存在
	s := New(path, nil, HookHandlers{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start(自动建目录): %v", err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode()&os.ModeSocket == 0 {
		t.Fatalf("套接字未建立: fi=%v err=%v", fi, err)
	}
	if err := s.Start(ctx); err == nil {
		t.Error("重复 Start 应报错")
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("Close 后套接字应被删除,Stat err=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close 幂等,第二次返回 %v", err)
	}
	if err := s.Start(ctx); err == nil {
		t.Error("Close 后 Start 应报错")
	}
}

// TestHookSockCtxCancelCloses:ctx 取消 → 收端自动关闭并删套接字文件。
func TestHookSockCtxCancelCloses(t *testing.T) {
	path := hookPath(t)
	s, _ := startHookSock(t, path, nil)
	_ = s

	// 另起一个受测实例(不用 startHookSock 的清理,验证取消路径)。
	path2 := filepath.Join(t.TempDir(), "cancel.sock")
	ctx, cancel := context.WithCancel(context.Background())
	s2 := New(path2, nil, HookHandlers{})
	if err := s2.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("Start 后套接字应存在: %v", err)
	}
	cancel()
	mustHookEventually(t, "ctx 取消后套接字清理", func() bool {
		_, err := os.Stat(path2)
		return os.IsNotExist(err)
	})
}

// TestHookSockStaleSocketReplaced:崩溃残留的陈旧套接字文件被 Start 清理后重新绑定。
func TestHookSockStaleSocketReplaced(t *testing.T) {
	path := hookPath(t)
	// 先建一个"陈旧"套接字(模拟上次崩溃残留,无进程监听)。
	stale, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatalf("建陈旧套接字: %v", err)
	}
	_ = stale.Close() // 关闭但保留文件(unixgram 关闭不删文件)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("陈旧套接字文件应残留: %v", err)
	}

	_, rec := startHookSock(t, path, newHookMetrics())
	c := dialHook(t, path)
	sendHook(t, c, []byte(`{"type":"advance","path":"p","chunk":42}`))
	if got := recvHook(t, rec); got.ref.Chunk != 42 {
		t.Errorf("陈旧套接字场景下帧未分发: %+v", got)
	}
}

// mustHookEventually 带 deadline 轮询 cond(hooksock 测试专用,避免与其他测试文件重名)。
func mustHookEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(hookTestTimeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待超时(%v):%s", hookTestTimeout, what)
		}
		time.Sleep(time.Millisecond)
	}
}

// —— M8/D44:read_miss 帧(D44)——

// missRec 是一次 Miss handler 调用记录(读循环单 goroutine 顺序调用)。
type missRec struct {
	ref    ordersource.ChunkRef
	reason string
}

// startHookSockMiss 起一个仅带 Miss handler 的收端;清理时先取消 ctx 再 Close。
func startHookSockMiss(t *testing.T, path string, m *metrics.Registry) chan missRec {
	t.Helper()
	rec := make(chan missRec, 16)
	ctx, cancel := context.WithCancel(context.Background())
	s := New(path, m, HookHandlers{
		Miss: func(ref ordersource.ChunkRef, reason string) {
			rec <- missRec{ref: ref, reason: reason}
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

// recvMiss 带超时接收一次 Miss 调用记录。
func recvMiss(t *testing.T, ch <-chan missRec) missRec {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(hookTestTimeout):
		t.Fatal("等待 read_miss handler 调用超时")
		return missRec{}
	}
}

// TestHookSockReadMissFrame:合法 read_miss 分发;reason 缺省/非法归一 absent,
// corrupt 原样保留;不产生格式错误计数。
func TestHookSockReadMissFrame(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	rec := startHookSockMiss(t, path, m)
	c := dialHook(t, path)

	cases := []struct {
		payload string
		ref     ordersource.ChunkRef
		reason  string
	}{
		{`{"type":"read_miss","path":"shard-1.tar","chunk":2,"reason":"absent","ts":"2026-10-18T00:00:00Z"}`,
			ordersource.ChunkRef{Path: "shard-1.tar", Chunk: 2}, "absent"},
		{`{"type":"read_miss","path":"shard-2.tar","chunk":3,"reason":"corrupt"}`,
			ordersource.ChunkRef{Path: "shard-2.tar", Chunk: 3}, "corrupt"},
		{`{"type":"read_miss","path":"shard-3.tar","chunk":4}`,
			ordersource.ChunkRef{Path: "shard-3.tar", Chunk: 4}, "absent"},
		{`{"type":"read_miss","path":"shard-4.tar","chunk":5,"reason":"bogus"}`,
			ordersource.ChunkRef{Path: "shard-4.tar", Chunk: 5}, "absent"},
	}
	for i, tc := range cases {
		sendHook(t, c, []byte(tc.payload))
		got := recvMiss(t, rec)
		if got.ref != tc.ref || got.reason != tc.reason {
			t.Errorf("第 %d 帧 → %+v/%q, want %+v/%q", i, got.ref, got.reason, tc.ref, tc.reason)
		}
	}
	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)); got != 0 {
		t.Errorf("合法 read_miss 帧不应计 parse,实际 %v", got)
	}
	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize)); got != 0 {
		t.Errorf("合法 read_miss 帧不应计 size,实际 %v", got)
	}
}

// —— M9/D47:read_stall 帧 ——

// stallRec 是一次 Stall handler 调用记录(读循环单 goroutine 顺序调用)。
type stallRec struct {
	ref    ordersource.ChunkRef
	waitMS int64
	ts     time.Time
}

// startHookSockStall 起一个仅带 Stall handler 的收端;清理时先取消 ctx 再 Close。
func startHookSockStall(t *testing.T, path string, m *metrics.Registry) chan stallRec {
	t.Helper()
	rec := make(chan stallRec, 16)
	ctx, cancel := context.WithCancel(context.Background())
	s := New(path, m, HookHandlers{
		Stall: func(ref ordersource.ChunkRef, waitMS int64, ts time.Time) {
			rec <- stallRec{ref: ref, waitMS: waitMS, ts: ts}
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

// recvStall 带超时接收一次 Stall 调用记录。
func recvStall(t *testing.T, ch <-chan stallRec) stallRec {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(hookTestTimeout):
		t.Fatal("等待 read_stall handler 调用超时")
		return stallRec{}
	}
}

// TestHookSockReadStallFrame:合法帧分发到 Stall(字段 wait_ms/ts 正确);ts 非法不 panic
// (传递零值);wait_ms 负数归一为 0;非法 JSON 计 parse;超 4 KiB 计 size。
func TestHookSockReadStallFrame(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	rec := startHookSockStall(t, path, m)
	c := dialHook(t, path)

	validTS := "2026-10-18T10:00:00.123456789Z"
	sendHook(t, c, []byte(`{"type":"read_stall","path":"shard-1.tar","chunk":3,"wait_ms":250,"ts":"`+validTS+`"}`))
	got := recvStall(t, rec)
	wantTS, err := time.Parse(time.RFC3339Nano, validTS)
	if err != nil {
		t.Fatalf("构造时间戳解析: %v", err)
	}
	if got.ref != (ordersource.ChunkRef{Path: "shard-1.tar", Chunk: 3}) || got.waitMS != 250 || !got.ts.Equal(wantTS) {
		t.Errorf("合法 read_stall → %+v, want ref{shard-1.tar,3}/250/%v", got, wantTS)
	}

	// ts 非法:零值传递,不 panic。
	sendHook(t, c, []byte(`{"type":"read_stall","path":"p","chunk":1,"wait_ms":150,"ts":"not-a-time"}`))
	if got := recvStall(t, rec); !got.ts.IsZero() || got.waitMS != 150 {
		t.Errorf("非法 ts → %+v, want 零值 ts/wait=150", got)
	}

	// wait_ms 负数:归一为 0 后仍分发。
	sendHook(t, c, []byte(`{"type":"read_stall","path":"p","chunk":2,"wait_ms":-5,"ts":"`+validTS+`"}`))
	if got := recvStall(t, rec); got.waitMS != 0 {
		t.Errorf("负 wait_ms → %d, want 0", got.waitMS)
	}

	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)); got != 0 {
		t.Errorf("合法 read_stall 帧不应计 parse,实际 %v", got)
	}
	if got := testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize)); got != 0 {
		t.Errorf("合法 read_stall 帧不应计 size,实际 %v", got)
	}

	// 非法 JSON:计 parse,不分发。
	sendHook(t, c, []byte("{not-json"))
	mustHookEventually(t, "parse 计数", func() bool {
		return testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)) == 1
	})

	// 超 4 KiB:整报丢弃计 size,不分发。
	base := `{"type":"read_stall","path":"p","chunk":9,"wait_ms":150}`
	oversize := base + strings.Repeat(" ", hookMaxFrame+1-len(base))
	sendHook(t, c, []byte(oversize))
	mustHookEventually(t, "size 计数", func() bool {
		return testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize)) == 1
	})
	select {
	case r := <-rec:
		t.Fatalf("超限帧不应分发,却收到 %+v", r)
	case <-time.After(hookQuietWindow):
	}
}

// TestHookSockReadMissInvalidJSONCounted:非法 JSON 计 parse,后续 read_miss 不受影响。
func TestHookSockReadMissInvalidJSONCounted(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	rec := startHookSockMiss(t, path, m)
	c := dialHook(t, path)

	sendHook(t, c, []byte("{not-json"))
	mustHookEventually(t, "parse 计数", func() bool {
		return testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonParse)) == 1
	})
	sendHook(t, c, []byte(`{"type":"read_miss","path":"p","chunk":7,"reason":"corrupt"}`))
	if got := recvMiss(t, rec); got.ref.Chunk != 7 || got.reason != "corrupt" {
		t.Errorf("坏帧后续 read_miss 未正确分发: %+v", got)
	}
}

// TestHookSockReadMissOversizeCounted:超 4 KiB 帧整报丢弃计 size;恰为 4 KiB 仍受理。
func TestHookSockReadMissOversizeCounted(t *testing.T) {
	m := newHookMetrics()
	path := hookPath(t)
	rec := startHookSockMiss(t, path, m)
	c := dialHook(t, path)

	base := `{"type":"read_miss","path":"p","chunk":9,"reason":"absent"}`
	exact := base + strings.Repeat(" ", hookMaxFrame-len(base))
	if len(exact) != hookMaxFrame {
		t.Fatalf("构造 4 KiB 帧长度 = %d", len(exact))
	}
	sendHook(t, c, []byte(exact))
	if got := recvMiss(t, rec); got.ref.Chunk != 9 {
		t.Errorf("4 KiB 边界 read_miss 未受理: %+v", got)
	}

	oversize := base + strings.Repeat(" ", hookMaxFrame+1-len(base))
	sendHook(t, c, []byte(oversize))
	mustHookEventually(t, "size 计数", func() bool {
		return testutil.ToFloat64(m.HookFormatErrors().WithLabelValues(metrics.ReasonSize)) == 1
	})
	select {
	case r := <-rec:
		t.Fatalf("超限帧不应分发,却收到 %+v", r)
	case <-time.After(hookQuietWindow):
	}
}
