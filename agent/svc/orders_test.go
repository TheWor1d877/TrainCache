package svc

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// 本文件是 OrderDiscovery 的集成测试:测试内自带小写端 helper 落 ring 文件
// (布局与写序按 design §6.2 / decisions D22 冻结,只读测试自建内容)。

// ring 布局常量(design §6.2;与 ordersource 读端一致,测试内独立字面量)。
const (
	testRingHeaderSize = 64
	testSlotHeaderSize = 12
)

// waitTimeout 是轮询/等待类断言的统一 deadline。
const waitTimeout = 5 * time.Second

// delivery 是一次订单投递(pod-uid + 载荷)。
type delivery struct {
	pod string
	o   ordersource.ConsumeOrder
}

// setScanInterval 把白盒扫描周期改小并注册恢复(用例串行,Run 返回后再恢复,无竞态)。
func setScanInterval(t *testing.T, d time.Duration) {
	t.Helper()
	old := scanInterval
	scanInterval = d
	t.Cleanup(func() { scanInterval = old })
}

// startDiscovery 在后台起 Run;t.Cleanup 兜底取消并等待 Run 退出(不泄漏 goroutine)。
// 返回取消函数与 Run 的返回值通道(缓冲 1,不读也不阻塞)。
func startDiscovery(t *testing.T, d *OrderDiscovery) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		runErr <- d.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return cancel, runErr
}

// waitFor 带 deadline 轮询 cond(不用裸 sleep 断言;poll 间隔 5ms)。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("超时未满足: %s(deadline %s)", what, waitTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// writeTestRing 落一个单槽 ring 文件:写序与 sidecar 一致(先整槽、后头 = commit),
// 并以临时文件 + rename 原子出现,避免读端见到半写文件。
func writeTestRing(t *testing.T, path string, gen, seq int64, slotSize, slotCount int32, order ordersource.ConsumeOrder) {
	t.Helper()
	payload, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("序列化 ConsumeOrder: %v", err)
	}
	if room := int(slotSize) - testSlotHeaderSize; len(payload) > room {
		t.Fatalf("payload %dB 超出槽容量 %dB:用例请选更大的 slot_size", len(payload), room)
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("创建测试 ring %s: %v", tmp, err)
	}

	// 槽:seq int64 + len int32 + payload,写到 64 + ((seq-1) mod slot_count) * slot_size。
	off := int64(testRingHeaderSize) + ((seq-1)%int64(slotCount))*int64(slotSize)
	slot := make([]byte, int(slotSize))
	binary.LittleEndian.PutUint64(slot[0:8], uint64(seq))
	binary.LittleEndian.PutUint32(slot[8:12], uint32(len(payload)))
	copy(slot[testSlotHeaderSize:], payload)
	if _, err := f.WriteAt(slot, off); err != nil {
		_ = f.Close()
		t.Fatalf("写测试 ring 槽: %v", err)
	}

	// 头(commit 最后写):magic "TCO1" / version 1 / 几何。
	header := make([]byte, testRingHeaderSize)
	copy(header[0:4], "TCO1")
	binary.LittleEndian.PutUint16(header[4:6], 1)
	binary.LittleEndian.PutUint64(header[8:16], uint64(gen))
	binary.LittleEndian.PutUint64(header[16:24], uint64(seq))
	binary.LittleEndian.PutUint32(header[24:28], uint32(slotSize))
	binary.LittleEndian.PutUint32(header[28:32], uint32(slotCount))
	if _, err := f.WriteAt(header, 0); err != nil {
		_ = f.Close()
		t.Fatalf("写测试 ring 头: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("关闭测试 ring %s: %v", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("落位测试 ring %s: %v", path, err)
	}
}

// writeTestRingRaw 直接落盘一段字节(坏头用例:文件原子出现,读端见到的就是坏头)。
func writeTestRingRaw(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("写测试 ring %s: %v", path, err)
	}
}

// testOrder 构造字段齐全的 ConsumeOrder(chunks 两个、顺序固定)。
func testOrder(dataset, stage string) ordersource.ConsumeOrder {
	return ordersource.ConsumeOrder{
		Dataset:        dataset,
		SnapshotID:     "snap-" + dataset,
		StageID:        stage,
		ShuffleSeed:    -7,
		BatchSize:      32,
		PrefetchChunks: 256,
		Chunks: []ordersource.ChunkRef{
			{Path: "shard-000.tar", Chunk: 0},
			{Path: "shard-007.tar", Chunk: 3},
		},
	}
}

// assertOrder 断言订单全字段一致(含 chunks 顺序)。
func assertOrder(t *testing.T, got, want ordersource.ConsumeOrder) {
	t.Helper()
	if got.Dataset != want.Dataset || got.SnapshotID != want.SnapshotID || got.StageID != want.StageID ||
		got.ShuffleSeed != want.ShuffleSeed || got.BatchSize != want.BatchSize ||
		got.PrefetchChunks != want.PrefetchChunks {
		t.Errorf("ConsumeOrder 标量字段 = %+v, want %+v", got, want)
	}
	if len(got.Chunks) != len(want.Chunks) {
		t.Fatalf("chunks 长度 = %d, want %d", len(got.Chunks), len(want.Chunks))
	}
	for i := range want.Chunks {
		if got.Chunks[i] != want.Chunks[i] {
			t.Errorf("chunks[%d] = %+v, want %+v", i, got.Chunks[i], want.Chunks[i])
		}
	}
}

// gatherFormatErrors 从注册表 Gather 结果中读出 ordersource 格式错误 counter 的
// (magic, version) 值(不 import client_model,避免间接依赖变直接依赖)。
func gatherFormatErrors(t *testing.T, m *metrics.Registry) (magic, version int64) {
	t.Helper()
	families, err := m.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "traincache_ordersource_format_errors_total" {
			continue
		}
		for _, mm := range mf.GetMetric() {
			for _, lp := range mm.GetLabel() {
				if lp.GetName() != "reason" {
					continue
				}
				switch lp.GetValue() {
				case "magic":
					magic = int64(mm.GetCounter().GetValue())
				case "version":
					version = int64(mm.GetCounter().GetValue())
				}
			}
		}
	}
	return magic, version
}

// TestOrderDiscoveryFindsNewRing:发现循环在 Run 期间新出现的 <pod-uid>.ring,
// 投递 pod-uid 与 ConsumeOrder 全字段(started 后落盘 → 走轮询发现路径)。
func TestOrderDiscoveryFindsNewRing(t *testing.T) {
	setScanInterval(t, 10*time.Millisecond)
	dir := t.TempDir()

	m := metrics.New(metrics.Config{})
	got := make(chan delivery, 8)
	d := NewOrderDiscovery(dir, m, func(pod string, o ordersource.ConsumeOrder) {
		got <- delivery{pod: pod, o: o}
	})
	_, runErr := startDiscovery(t, d)

	want := testOrder("b-track", "epoch-2")
	writeTestRing(t, filepath.Join(dir, "pod-abc.ring"), 1, 1, 256, 2, want)

	select {
	case g := <-got:
		if g.pod != "pod-abc" {
			t.Fatalf("pod_uid = %q, want pod-abc(文件名去 .ring)", g.pod)
		}
		assertOrder(t, g.o, want)
	case err := <-runErr:
		t.Fatalf("Run 提前返回: %v", err)
	case <-time.After(waitTimeout):
		t.Fatal("超时未收到 ConsumeOrder")
	}
}

// TestOrderDiscoveryIgnoresNonRingFiles:非 *.ring 文件不触发投递(即使内容是合法 ring)。
func TestOrderDiscoveryIgnoresNonRingFiles(t *testing.T) {
	setScanInterval(t, 10*time.Millisecond)
	dir := t.TempDir()
	writeTestRing(t, filepath.Join(dir, "pod-b.txt"), 1, 1, 256, 2, testOrder("b-track", "epoch-1"))
	writeTestRing(t, filepath.Join(dir, "pod-c.ring.bak"), 1, 1, 256, 2, testOrder("c-track", "epoch-1"))
	writeTestRing(t, filepath.Join(dir, "pod-a.ring"), 1, 1, 256, 2, testOrder("a-track", "epoch-1"))

	m := metrics.New(metrics.Config{})
	got := make(chan delivery, 8)
	d := NewOrderDiscovery(dir, m, func(pod string, o ordersource.ConsumeOrder) {
		got <- delivery{pod: pod, o: o}
	})
	startDiscovery(t, d)

	// 首个投递必须来自 pod-a.ring。
	select {
	case g := <-got:
		if g.pod != "pod-a" {
			t.Fatalf("投递 pod_uid = %q, want pod-a(非 .ring 文件不得被发现)", g.pod)
		}
	case <-time.After(waitTimeout):
		t.Fatal("超时未收到 pod-a 的 ConsumeOrder")
	}
	// 其后 ≥2 个 scan 周期内不得再有投递(pod-b.txt / pod-c.ring.bak 不得被当作 ring)。
	select {
	case g := <-got:
		t.Fatalf("非 .ring 文件触发了投递: pod_uid=%q", g.pod)
	case <-time.After(3 * scanInterval):
	}
}

// TestOrderDiscoverySyncsFormatErrors:坏 magic ring → ordersource 包级计数增长,
// 发现循环每 tick 以绝对值同步到 metrics counter(reason=magic,version 不增)。
func TestOrderDiscoverySyncsFormatErrors(t *testing.T) {
	setScanInterval(t, 10*time.Millisecond)
	dir := t.TempDir()
	// 坏 magic:≥64B 且首 4B 非 "TCO1"(读端按 magic 类计一次;D23)。
	writeTestRingRaw(t, filepath.Join(dir, "pod-bad.ring"), bytes.Repeat([]byte("X"), 64))

	m := metrics.New(metrics.Config{})
	beforeMagic, beforeVersion := gatherFormatErrors(t, m)
	d := NewOrderDiscovery(dir, m, func(string, ordersource.ConsumeOrder) {})
	startDiscovery(t, d)

	waitFor(t, "ordersource 格式错误 counter(magic)增长", func() bool {
		magic, _ := gatherFormatErrors(t, m)
		return magic > beforeMagic
	})
	if _, version := gatherFormatErrors(t, m); version != beforeVersion {
		t.Errorf("version 计数 = %d, want 未增长 %d(坏 magic 只计 magic 类)", version, beforeVersion)
	}
}

// TestOrderDiscoveryStopsOnCtxCancel:ctx 取消 → Run 返回 context.Canceled、
// reader 全 Close、无 goroutine 泄漏。
func TestOrderDiscoveryStopsOnCtxCancel(t *testing.T) {
	setScanInterval(t, 10*time.Millisecond)
	dir := t.TempDir()
	writeTestRing(t, filepath.Join(dir, "pod-a.ring"), 1, 1, 256, 2, testOrder("a-track", "epoch-1"))
	writeTestRing(t, filepath.Join(dir, "pod-b.ring"), 1, 1, 256, 2, testOrder("b-track", "epoch-1"))

	// 基线:先触发一次 GC 让 GC worker 就位,降低 NumGoroutine 误报。
	runtime.GC()
	before := runtime.NumGoroutine()

	m := metrics.New(metrics.Config{})
	got := make(chan delivery, 8)
	d := NewOrderDiscovery(dir, m, func(pod string, o ordersource.ConsumeOrder) {
		got <- delivery{pod: pod, o: o}
	})
	cancel, runErr := startDiscovery(t, d)

	// 等两个 pod 的初始投递到齐(确认 Reader 与转发 goroutine 已就位)。
	seen := make(map[string]bool)
	waitFor(t, "两个 pod 完成初始投递", func() bool {
		select {
		case g := <-got:
			seen[g.pod] = true
		default:
		}
		return seen["pod-a"] && seen["pod-b"]
	})

	cancel()
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run 返回 %v, want context.Canceled", err)
	}

	// reader 全 Close:已关闭的 Reader 上再订阅返回 ErrClosed(不新建 goroutine)。
	d.mu.Lock()
	readers := make([]*ordersource.Reader, 0, len(d.readers))
	for _, r := range d.readers {
		readers = append(readers, r)
	}
	d.mu.Unlock()
	if len(readers) != 2 {
		t.Fatalf("发现到的 reader 数 = %d, want 2", len(readers))
	}
	for i, r := range readers {
		if _, err := r.SubscribeStageSwitch(context.Background()); !errors.Is(err, ordersource.ErrClosed) {
			t.Errorf("reader[%d] 未关闭: SubscribeStageSwitch err = %v, want ErrClosed", i, err)
		}
	}

	// 无 goroutine 泄漏:reader poll goroutine、转发 goroutine 与订阅注销 goroutine 全退出。
	waitFor(t, "goroutine 回落到基线", func() bool { return runtime.NumGoroutine() <= before })
}

// TestOrderDiscoveryMkdirFailureNotFatal:发现目录不可创建(父路径是普通文件)只 warn,
// Run 不退出、不 panic。
func TestOrderDiscoveryMkdirFailureNotFatal(t *testing.T) {
	setScanInterval(t, 10*time.Millisecond)
	tmp := t.TempDir()
	blocker := filepath.Join(tmp, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("准备阻塞文件: %v", err)
	}
	dir := filepath.Join(blocker, "orders") // MkdirAll/ReadDir 均失败

	m := metrics.New(metrics.Config{})
	d := NewOrderDiscovery(dir, m, func(string, ordersource.ConsumeOrder) {})
	_, runErr := startDiscovery(t, d)

	select {
	case err := <-runErr:
		t.Fatalf("Run 因目录错误退出: %v", err)
	case <-time.After(3 * scanInterval):
		// 仍在运行:目录错误不致命(用例结束时由 cleanup 取消)。
	}
}
