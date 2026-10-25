package ordersource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// ---------------- 测试辅助(等待一律带 deadline 轮询,不用裸 sleep) ----------------

const (
	// testTimeout 是 eventually / recv 的等待上限。取 10s：本包正常跑完约 1.9s,
	// 绝大多数时间花在 quietWindow 的负断言上;超时只在断言失败时才走到,
	// 因此放宽上限不增加常规耗时,却能挡住"宿主负载高(CI 上并发编译/多个 agent
	// 同时跑)时首次运行超时、复跑即过"这类假失败。
	testTimeout = 10 * time.Second       // eventually / recv 的 deadline
	quietWindow = 150 * time.Millisecond // 负断言观察窗
)

// fastPoll 缩小 poll 周期(白盒)。须先于本测试所有 Reader 的构造调用:
// 本测试的 Reader 全部 Close(清理 LIFO:Close 先于恢复)后才恢复包变量,pollInterval 无并发读写。
func fastPoll(t *testing.T) {
	t.Helper()
	old := pollInterval
	pollInterval = 2 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })
}

// eventually 带 deadline 轮询 cond,超时返回 false。
func eventually(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// mustEventually deadline 内等待 cond 为真,超时 Fatal。
func mustEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !eventually(t, cond) {
		t.Fatalf("等待超时(%v):%s", testTimeout, what)
	}
}

// recvOrder 带超时接收订阅事件。
func recvOrder(t *testing.T, ch <-chan ConsumeOrder) ConsumeOrder {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(testTimeout):
		t.Fatal("等待订阅事件超时")
		return ConsumeOrder{}
	}
}

// expectNoEvent 负断言:quietWindow 内不应收到事件,且 channel 不应被 close(约定:永不 close)。
// 有界观察窗用于"不该发生"类断言,是带 deadline 的轮询而非裸 sleep。
func expectNoEvent(t *testing.T, ch <-chan ConsumeOrder) {
	t.Helper()
	select {
	case o, ok := <-ch:
		if !ok {
			t.Fatal("订阅 channel 被 close(约定:channel 永不 close)")
		}
		t.Fatalf("不应收到事件,却收到 %+v", o)
	case <-time.After(quietWindow):
	}
}

// testOrder 构造全字段测试订单。
func testOrder(stage string, lastChunk int64) ConsumeOrder {
	return ConsumeOrder{
		Dataset:        "b-track",
		SnapshotID:     "snap-20261014",
		StageID:        stage,
		ShuffleSeed:    20261015,
		BatchSize:      32,
		PrefetchChunks: 256,
		Chunks: []ChunkRef{
			{Path: "shard-000.tar", Chunk: 0},
			{Path: "shard-007.tar", Chunk: lastChunk},
		},
	}
}

func orderEqual(a, b ConsumeOrder) bool { return reflect.DeepEqual(a, b) }

// assertOrderEqual 全字段(含 chunks)一致性断言。
func assertOrderEqual(t *testing.T, got, want ConsumeOrder) {
	t.Helper()
	if !orderEqual(got, want) {
		t.Errorf("订单不一致:\ngot  %+v\nwant %+v", got, want)
	}
}

// newTestReader 构造 Reader 并注册 t.Cleanup 关闭(fastPoll 须先调用)。
func newTestReader(t *testing.T, path string) *Reader {
	t.Helper()
	r := NewReader(path, filepath.Dir(path))
	t.Cleanup(r.Close)
	return r
}

// subscribeTest 订阅并断言无错误。
func subscribeTest(t *testing.T, r *Reader) <-chan ConsumeOrder {
	t.Helper()
	ch, err := r.SubscribeStageSwitch(context.Background())
	if err != nil {
		t.Fatalf("SubscribeStageSwitch: %v", err)
	}
	return ch
}

// readerState 白盒状态快照(测试用,锁内读取)。
func readerState(r *Reader) (attached bool, gen, lastSeq int64, missing bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attached, r.lastGen, r.lastReadSeq, r.missing
}

// newRingPath 在临时目录下建 ring 路径。
func newRingPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "pod-uid.ring")
}

// ---------------- 用例 ----------------

// TestReaderCurrentOrderEmpty 新空 ring(write_seq=0)→ CurrentOrder 返回 false。
func TestReaderCurrentOrderEmpty(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	newRingWriter(t, path, 1, 128, 4)
	r := newTestReader(t, path)

	if _, ok := r.CurrentOrder(); ok {
		t.Fatal("空 ring(write_seq=0)上 CurrentOrder 应返回 false")
	}
	if r.Missing() {
		t.Fatal("文件存在,Missing 应为 false")
	}
}

// TestReaderMissingFile 路径不存在 → Missing()==true、CurrentOrder false、不 panic。
func TestReaderMissingFile(t *testing.T) {
	fastPoll(t)
	path := filepath.Join(t.TempDir(), "absent.ring")
	r := newTestReader(t, path)

	if !r.Missing() {
		t.Fatal("文件不存在,Missing 应为 true")
	}
	if _, ok := r.CurrentOrder(); ok {
		t.Fatal("文件不存在时 CurrentOrder 应返回 false")
	}
	// 不 panic:订阅仍可用(missing 状态照常轮询)
	ch := subscribeTest(t, r)
	expectNoEvent(t, ch)
}

// TestReaderReadsFirstOrder 写一份订单 → 追读后 CurrentOrder 各字段一致
// (先写再构造:初始快照路径同步可见,且不重放到订阅通道)。
func TestReaderReadsFirstOrder(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	want := testOrder("epoch-1", 3)
	w.WriteOrder(want)

	r := newTestReader(t, path)
	got, ok := r.CurrentOrder()
	if !ok {
		t.Fatal("启动初始快照应能看到当前订单")
	}
	assertOrderEqual(t, got, want)
}

// TestReaderFollowsSequentialSlots 连写两份 → 逐槽按序推送(事件顺序=写入顺序)。
func TestReaderFollowsSequentialSlots(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 8)
	r := newTestReader(t, path)
	ch := subscribeTest(t, r)

	o1 := testOrder("epoch-1", 1)
	w.WriteOrder(o1)
	assertOrderEqual(t, recvOrder(t, ch), o1)

	o2 := testOrder("epoch-2", 2)
	w.WriteOrder(o2)
	assertOrderEqual(t, recvOrder(t, ch), o2)

	mustEventually(t, "CurrentOrder 跟进第二份", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, o2)
	})
}

// TestReaderSkipsToLatestWhenBehind slot_count=2、连写 5 份 → 跳读最新:拿到最后一份、无 panic、不阻塞。
func TestReaderSkipsToLatestWhenBehind(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 2) // 落后超过 slot_count 即跳读
	r := newTestReader(t, path)

	var last ConsumeOrder
	for i := 1; i <= 5; i++ {
		last = testOrder(fmt.Sprintf("epoch-%d", i), int64(i))
		w.WriteOrder(last)
	}
	mustEventually(t, "跳读到最新订单", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, last)
	})
	if r.Missing() {
		t.Fatal("文件存在,Missing 应为 false")
	}
}

// TestReaderGenerationChangeRereads 同文件 gen+1 重写 → 丢弃旧状态读最新(拿到新代际订单)。
func TestReaderGenerationChangeRereads(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 8)
	r := newTestReader(t, path)

	a := testOrder("epoch-1", 1)
	w.WriteOrder(a)
	mustEventually(t, "初始代际订单就绪", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, a)
	})
	ch := subscribeTest(t, r)

	w.SetGeneration(2) // sidecar 重启:代际 +1
	b := testOrder("epoch-2", 2)
	w.WriteOrder(b) // write_seq 跨代际续增

	assertOrderEqual(t, recvOrder(t, ch), b)
	mustEventually(t, "CurrentOrder 切到新代际订单", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, b)
	})
	attached, gen, _, _ := readerState(r)
	if !attached || gen != 2 {
		t.Fatalf("白盒状态:attached=%v gen=%d, want true/2", attached, gen)
	}
}

// TestReaderGenerationChangeSamePayloadNoRedelivery gen 变化但 payload 字节相同 → 不重推(去重)。
func TestReaderGenerationChangeSamePayloadNoRedelivery(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 8)
	r := newTestReader(t, path)

	o := testOrder("epoch-1", 1)
	w.WriteOrder(o)
	mustEventually(t, "初始订单就绪", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, o)
	})
	ch := subscribeTest(t, r)

	w.SetGeneration(2) // 代际变化(同 payload,seq 未推进)
	w.WriteOrder(o)    // 同 payload 字节,新 seq

	// 白盒:读端已复验新代际并追到 seq=2(证明处理已发生,而非尚未轮询)
	mustEventually(t, "读端完成代际复验与追读", func() bool {
		attached, gen, lastSeq, _ := readerState(r)
		return attached && gen == 2 && lastSeq == 2
	})
	expectNoEvent(t, ch) // 按字节去重:不重推

	cur, ok := r.CurrentOrder()
	if !ok {
		t.Fatal("去重不应清空当前订单")
	}
	assertOrderEqual(t, cur, o)
}

// TestReaderBadMagicIgnoredAndCounted magic 非法 → CurrentOrder false、无事件、magic 计数 +1。
func TestReaderBadMagicIgnoredAndCounted(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	h := buildRingHeader(1, 0, 256, 4)
	copy(h[0:4], "XXXX") // magic 非法
	writeRingFile(t, path, h[:])

	magic0, version0 := FormatErrorTotals()
	r := newTestReader(t, path)
	ch := subscribeTest(t, r)

	mustEventually(t, "magic 计数 +1", func() bool {
		m, _ := FormatErrorTotals()
		return m == magic0+1
	})
	if _, ok := r.CurrentOrder(); ok {
		t.Fatal("magic 非法文件不应产生 CurrentOrder")
	}
	if r.Missing() {
		t.Fatal("文件存在,Missing 应为 false")
	}
	expectNoEvent(t, ch)

	// 多轮 poll 后仍只计一次(同一文件同一坏头状态去重)
	m, v := FormatErrorTotals()
	if m != magic0+1 || v != version0 {
		t.Fatalf("格式错误计数被重复累计: magic=%d(+%d) version=%d(+%d), want +1/+0",
			m, m-magic0, v, v-version0)
	}
}

// TestReaderBadVersionIgnoredAndCounted version 非法 → 同上,version +1。
func TestReaderBadVersionIgnoredAndCounted(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	h := buildRingHeader(1, 0, 256, 4)
	h[4] = 2 // version=2,非 1
	writeRingFile(t, path, h[:])

	magic0, version0 := FormatErrorTotals()
	r := newTestReader(t, path)
	ch := subscribeTest(t, r)

	mustEventually(t, "version 计数 +1", func() bool {
		_, v := FormatErrorTotals()
		return v == version0+1
	})
	if _, ok := r.CurrentOrder(); ok {
		t.Fatal("version 非法文件不应产生 CurrentOrder")
	}
	expectNoEvent(t, ch)

	m, v := FormatErrorTotals()
	if v != version0+1 || m != magic0 {
		t.Fatalf("格式错误计数异常: magic=%d(+%d) version=%d(+%d), want +0/+1",
			m, m-magic0, v, v-version0)
	}
}

// TestReaderHeaderGeometryInvalid slot_size=0 / slot_count=0 等 → invalid + version 计数、不 panic。
func TestReaderHeaderGeometryInvalid(t *testing.T) {
	fastPoll(t)
	cases := []struct {
		name      string
		slotSize  uint32
		slotCount uint32
	}{
		{"slot_size=0", 0, 64},
		{"slot_size 过小", 8, 64},
		{"slot_size 过大", 128 << 20, 64},
		{"slot_count=0", 1 << 20, 0},
		{"slot_count 过大", 1 << 20, 2048},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fastPoll(t)
			path := newRingPath(t)
			h := buildRingHeader(1, 0, tc.slotSize, tc.slotCount)
			writeRingFile(t, path, h[:])

			magic0, version0 := FormatErrorTotals()
			r := newTestReader(t, path)

			mustEventually(t, "几何非法 → version 计数 +1", func() bool {
				_, v := FormatErrorTotals()
				return v == version0+1
			})
			if _, ok := r.CurrentOrder(); ok {
				t.Fatal("几何非法文件不应产生 CurrentOrder")
			}
			if r.Missing() {
				t.Fatal("文件存在,Missing 应为 false")
			}
			m, v := FormatErrorTotals()
			if m != magic0 || v != version0+1 {
				t.Fatalf("计数异常: magic=%d(+%d) version=%d(+%d), want +0/+1",
					m, m-magic0, v, v-version0)
			}
		})
	}
}

// TestReaderLenOutOfRangeSkipped 槽 len 越界或为负 → 该槽跳过、不推送非法载荷、不崩溃。
func TestReaderLenOutOfRangeSkipped(t *testing.T) {
	cases := []struct {
		name   string
		declen int32
	}{
		{"len 为负", -5},
		{"len 越界", 1000}, // slot_size=512 → 载荷上限 500
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fastPoll(t)
			path := newRingPath(t)
			w := newRingWriter(t, path, 1, 512, 2)
			payload := mustMarshal(t, testOrder("epoch-1", 1))
			w.WriteSlotBadLen(1, tc.declen, payload)
			w.CommitSeq(1) // commit 已推,但槽 len 非法

			magic0, version0 := FormatErrorTotals()
			r := newTestReader(t, path)
			ch := subscribeTest(t, r)

			expectNoEvent(t, ch) // 不推送非法载荷
			if _, ok := r.CurrentOrder(); ok {
				t.Fatal("len 非法槽不应产生 CurrentOrder")
			}

			// 恢复:继续写合法订单;slot_count=2 时 3 号订单会覆盖坏槽 → 跳读越过并继续追读
			o2 := testOrder("epoch-2", 2)
			o3 := testOrder("epoch-3", 3)
			w.WriteOrder(o2)
			w.WriteOrder(o3)
			mustEventually(t, "坏 len 槽后被越过并恢复追读", func() bool {
				cur, ok := r.CurrentOrder()
				return ok && orderEqual(cur, o3)
			})

			// 计数只留给 magic/version:len 非法不计数
			m, v := FormatErrorTotals()
			if m != magic0 || v != version0 {
				t.Fatalf("len 非法不应计入格式错误计数: magic=%d(+%d) version=%d(+%d), want +0/+0",
					m, m-magic0, v, v-version0)
			}
		})
	}
}

// TestReaderHalfWrittenSlotNotRead 槽内容已写但头 write_seq 未推 → 不读半写槽(commit 语义)。
func TestReaderHalfWrittenSlotNotRead(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	r := newTestReader(t, path)
	ch := subscribeTest(t, r)

	o := testOrder("epoch-1", 1)
	w.WriteRawSlot(1, mustMarshal(t, o)) // 整槽已写,头 write_seq 未推
	expectNoEvent(t, ch)
	if _, ok := r.CurrentOrder(); ok {
		t.Fatal("未 commit 的槽不应可见(半写槽不可见)")
	}

	w.CommitSeq(1) // 推头:commit 标记最后写
	assertOrderEqual(t, recvOrder(t, ch), o)
}

// TestReaderRemapOnFileGrowth 文件从 64+1槽 扩到多槽 → remap 后继续追读。
func TestReaderRemapOnFileGrowth(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4) // 初始文件仅 64+1 槽
	o1 := testOrder("epoch-1", 1)
	w.WriteOrder(o1)

	r := newTestReader(t, path) // 映射仅覆盖 slot 0
	mustEventually(t, "初始订单就绪", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, o1)
	})
	ch := subscribeTest(t, r)

	for i := 2; i <= 4; i++ {
		o := testOrder(fmt.Sprintf("epoch-%d", i), int64(i))
		w.WriteOrder(o) // 槽越界 → 写端扩文件,读端须 remap 才能继续
		assertOrderEqual(t, recvOrder(t, ch), o)
	}
	last := testOrder("epoch-4", 4)
	mustEventually(t, "扩槽后追读到最新", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, last)
	})
}

// TestReaderFileRecreatedAfterDelete 删除 → Missing 标记;重建(新代际)→ 恢复读最新。
func TestReaderFileRecreatedAfterDelete(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	a := testOrder("epoch-1", 1)
	w.WriteOrder(a)
	r := newTestReader(t, path)
	mustEventually(t, "初始订单就绪", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, a)
	})
	ch := subscribeTest(t, r)

	if err := os.Remove(path); err != nil {
		t.Fatalf("删除 ring 文件: %v", err)
	}
	mustEventually(t, "删除后进入 Missing", func() bool { return r.Missing() })

	w2 := newRingWriter(t, path, 2, 512, 4) // 重建:新代际,write_seq 从 1 起
	b := testOrder("epoch-2", 1)
	w2.WriteOrder(b)

	assertOrderEqual(t, recvOrder(t, ch), b) // 恢复读最新并推送
	mustEventually(t, "CurrentOrder 切到重建后的订单", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, b)
	})
	if r.Missing() {
		t.Fatal("重建后 Missing 应为 false")
	}
}

// TestReaderCloseStops Close 幂等;之后 Subscribe 返回 ErrClosed、无新事件、goroutine 退出。
func TestReaderCloseStops(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	r := newTestReader(t, path) // 本测试显式 Close;清理再 Close 一次(幂等)
	ch := subscribeTest(t, r)

	r.Close()
	r.Close() // 幂等:第二次不 panic 且立即返回

	if _, err := r.SubscribeStageSwitch(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close 后 SubscribeStageSwitch = %v, want ErrClosed", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.SubscribeStageSwitch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 已取消时 SubscribeStageSwitch = %v, want context.Canceled", err)
	}
	if _, ok := r.CurrentOrder(); ok {
		t.Fatal("Close 后 CurrentOrder 应返回 false")
	}

	select {
	case <-r.stopped:
	case <-time.After(testTimeout):
		t.Fatal("Close 返回后 poll goroutine 未退出")
	}

	w.WriteOrder(testOrder("epoch-1", 1)) // 关闭后写入也不产生事件
	expectNoEvent(t, ch)
}

// TestSubscribeStageSwitchDelivery 订阅后写两份 → channel 按序收到两份。
func TestSubscribeStageSwitchDelivery(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 8)
	r := newTestReader(t, path)
	ch := subscribeTest(t, r)

	o1 := testOrder("epoch-1", 1)
	w.WriteOrder(o1)
	assertOrderEqual(t, recvOrder(t, ch), o1)

	o2 := testOrder("epoch-2", 2)
	w.WriteOrder(o2)
	assertOrderEqual(t, recvOrder(t, ch), o2)
}

// TestSubscribeStageSwitchBeforeFirstPush 先订阅、后写 → 不漏事件(含文件后发现路径)。
func TestSubscribeStageSwitchBeforeFirstPush(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t) // 文件尚不存在
	r := newTestReader(t, path)
	if !r.Missing() {
		t.Fatal("前置:文件尚未创建,Missing 应为 true")
	}
	ch := subscribeTest(t, r)

	w := newRingWriter(t, path, 1, 512, 4) // 文件出现(发现路径)
	o := testOrder("epoch-1", 1)
	w.WriteOrder(o)

	assertOrderEqual(t, recvOrder(t, ch), o) // 首份订单不漏
}

// TestSubscribeStageSwitchMultiSubscriber 两个订阅者均收到(fan-out)。
func TestSubscribeStageSwitchMultiSubscriber(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 8)
	r := newTestReader(t, path)
	ch1 := subscribeTest(t, r)
	ch2 := subscribeTest(t, r)

	o := testOrder("epoch-1", 1)
	w.WriteOrder(o)

	assertOrderEqual(t, recvOrder(t, ch1), o)
	assertOrderEqual(t, recvOrder(t, ch2), o)
}

// TestSubscribeStageSwitchCancel ctx 取消 → 订阅者移除、channel 不被 close、后续事件不投递。
func TestSubscribeStageSwitchCancel(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 8)
	r := newTestReader(t, path)

	ctx, cancel := context.WithCancel(context.Background())
	chCanceled, err := r.SubscribeStageSwitch(ctx)
	if err != nil {
		t.Fatalf("SubscribeStageSwitch(ctx): %v", err)
	}
	chAlive := subscribeTest(t, r)

	cancel()
	mustEventually(t, "ctx 取消后订阅者从订阅表移除", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.subs) == 1
	})
	// 已退订:不投递、channel 未 close(expectNoEvent 同时校验未 close)
	o := testOrder("epoch-1", 1)
	w.WriteOrder(o)
	assertOrderEqual(t, recvOrder(t, chAlive), o) // 证明读端已处理
	expectNoEvent(t, chCanceled)                  // 已退订:不投递、channel 未 close
}

// TestSubscribeStageSwitchSlowConsumerLatestWins 订阅者不消费时连推多份 → 消费到的必为最新(丢旧保新)。
func TestSubscribeStageSwitchSlowConsumerLatestWins(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 8)
	r := newTestReader(t, path)
	ch := subscribeTest(t, r)

	var last ConsumeOrder
	for i := 1; i <= 5; i++ {
		last = testOrder(fmt.Sprintf("epoch-%d", i), int64(i))
		w.WriteOrder(last)
	}
	mustEventually(t, "读端追到最新订单", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, last)
	})
	assertOrderEqual(t, recvOrder(t, ch), last) // 丢旧保新
}

// TestCurrentOrderReturnsCopy 修改返回值 chunks 不影响 Reader 内部状态。
func TestCurrentOrderReturnsCopy(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	want := testOrder("epoch-1", 1)
	w.WriteOrder(want)
	r := newTestReader(t, path)
	mustEventually(t, "初始订单就绪", func() bool {
		cur, ok := r.CurrentOrder()
		return ok && orderEqual(cur, want)
	})

	got, ok := r.CurrentOrder()
	if !ok {
		t.Fatal("CurrentOrder 应返回订单")
	}
	got.Dataset = "mutated"
	got.Chunks[0].Path = "mutated"
	got.Chunks = append(got.Chunks, ChunkRef{Path: "extra", Chunk: 9})

	again, ok := r.CurrentOrder()
	if !ok {
		t.Fatal("CurrentOrder 二次调用失败")
	}
	assertOrderEqual(t, again, want)
}

// ---------------- LastAdvance(M5 降级判定,decisions D29) ----------------

// TestReaderLastAdvanceMissingFile 文件不存在 → LastAdvance 零值(从未推进);多轮 poll 后保持。
func TestReaderLastAdvanceMissingFile(t *testing.T) {
	fastPoll(t)
	r := newTestReader(t, filepath.Join(t.TempDir(), "absent.ring"))
	if !r.Missing() {
		t.Fatal("文件不存在,Missing 应为 true")
	}
	time.Sleep(quietWindow) // 观察窗:跨多个 poll 周期
	if got := r.LastAdvance(); !got.IsZero() {
		t.Fatalf("缺失文件 LastAdvance = %v, want 零值", got)
	}
}

// TestReaderLastAdvanceInitialSnapshot 构造时的首次有效快照即视为推进(D29);
// 头有效但 write_seq=0 的空 ring 同样计(写端可读即可观测)。
func TestReaderLastAdvanceInitialSnapshot(t *testing.T) {
	fastPoll(t)
	before := time.Now()
	path := newRingPath(t)
	newRingWriter(t, path, 1, 128, 4)
	r := newTestReader(t, path)

	la := r.LastAdvance()
	if la.IsZero() {
		t.Fatal("首次有效快照应视为推进(D29)")
	}
	if !la.After(before) && !la.Equal(before) {
		t.Errorf("LastAdvance = %v 早于构造前 %v", la, before)
	}
}

// TestReaderLastAdvanceOnWriteSeqAdvance 同一代际 write_seq 前进 → LastAdvance 前移。
func TestReaderLastAdvanceOnWriteSeqAdvance(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	w.WriteOrder(testOrder("epoch-1", 3))
	r := newTestReader(t, path)

	t1 := r.LastAdvance()
	if t1.IsZero() {
		t.Fatal("初始快照应视为推进")
	}
	w.WriteOrder(testOrder("epoch-2", 5))
	mustEventually(t, "write_seq 推进后 LastAdvance 前移", func() bool {
		return r.LastAdvance().After(t1)
	})
}

// TestReaderLastAdvanceStableWithoutAdvance 无新写入时 LastAdvance 保持(不随 poll 漂移)。
func TestReaderLastAdvanceStableWithoutAdvance(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	w.WriteOrder(testOrder("epoch-1", 3))
	r := newTestReader(t, path)
	mustEventually(t, "初始推进可见", func() bool { return !r.LastAdvance().IsZero() })

	t1 := r.LastAdvance()
	time.Sleep(quietWindow) // 多个 poll 周期内不应变化
	if t2 := r.LastAdvance(); !t2.Equal(t1) {
		t.Errorf("无写入时 LastAdvance 漂移:%v → %v", t1, t2)
	}
}

// TestReaderLastAdvanceOnGenerationChange 代际变化(write_seq 不变)视为推进(D29,sidecar 重启)。
func TestReaderLastAdvanceOnGenerationChange(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	w.WriteOrder(testOrder("epoch-1", 3))
	r := newTestReader(t, path)

	t1 := r.LastAdvance()
	if t1.IsZero() {
		t.Fatal("初始快照应视为推进")
	}
	w.SetGeneration(2) // 仅代际推进,write_seq 不变
	mustEventually(t, "代际变化后 LastAdvance 前移", func() bool {
		return r.LastAdvance().After(t1)
	})
}

// TestReaderLastAdvanceBadHeaderStaysZero 头非法(magic 破坏)→ 不视为推进,保持零值。
func TestReaderLastAdvanceBadHeaderStaysZero(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	bad := buildRingHeader(1, 0, 128, 4)
	bad[0] = 'X'
	writeRingFile(t, path, bad[:])
	r := newTestReader(t, path)

	if r.Missing() {
		t.Fatal("文件存在,Missing 应为 false")
	}
	time.Sleep(quietWindow)
	if got := r.LastAdvance(); !got.IsZero() {
		t.Fatalf("坏头 LastAdvance = %v, want 零值(D29)", got)
	}
}

// TestReaderLastAdvanceKeepsValueOnFileDisappear 文件消失(删除)→ 不回退最后观测值
// (降级判定以 Missing 为准,不依赖 LastAdvance 被清零)。
func TestReaderLastAdvanceKeepsValueOnFileDisappear(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	w.WriteOrder(testOrder("epoch-1", 3))
	r := newTestReader(t, path)

	t1 := r.LastAdvance()
	if t1.IsZero() {
		t.Fatal("初始快照应视为推进")
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("删除 ring 文件: %v", err)
	}
	mustEventually(t, "Missing 标记", r.Missing)
	if t2 := r.LastAdvance(); !t2.Equal(t1) {
		t.Errorf("文件消失后 LastAdvance 变化:%v → %v", t1, t2)
	}
}

// TestReaderLastAdvancePreservedAfterClose Close 后保持最后观测值不变(D29)。
func TestReaderLastAdvancePreservedAfterClose(t *testing.T) {
	fastPoll(t)
	path := newRingPath(t)
	w := newRingWriter(t, path, 1, 512, 4)
	w.WriteOrder(testOrder("epoch-1", 3))
	r := NewReader(path, filepath.Dir(path)) // 手动 Close(不用 newTestReader 的清理)
	t1 := r.LastAdvance()
	if t1.IsZero() {
		t.Fatal("初始快照应视为推进")
	}
	r.Close()
	if t2 := r.LastAdvance(); !t2.Equal(t1) {
		t.Errorf("Close 后 LastAdvance 变化:%v → %v", t1, t2)
	}
}
