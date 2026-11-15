package svc

// 缓存压力订单降级(ForOrder × TRAINCACHE_CACHE_CAPACITY_BYTES × 邻域窗口)用例:容量 > 0
// 且邻域预取窗口(订单头 lookahead 个去重块)字节 > 容量一半 → 首批与邻域都不回源,按需入口
// 照常;窗口 ≤ 容量一半,或容量 = 0(字节维度未启用)→ 首批照常回源。判据比的是 lookahead
// 块而非整单数据集字节,所以「数据集超容量但窗口在容量一半内」的中间档照常预取(这是相对旧
// 判据的修复点)。源站块 Get 被闸 store 挂起:降级用例以「按需块已回源而其他块未回源」为判据
// (调度器活着,负断言不靠静默窗口),保留用例等「首批回源已发生」。

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"

	"traincache/agent/ordersource"
	"traincache/core/objectstore"
)

// capHoldStore 包装 Mock:记录数据块对象键并挂起 Get(release 放行;ctx 取消也放行,
// 清理不泄漏下载协程)。索引元数据键直通。
type capHoldStore struct {
	*objectstore.Mock
	mu   sync.Mutex
	keys []string
	gate chan struct{}
}

func newCapHoldStore() *capHoldStore {
	return &capHoldStore{Mock: objectstore.NewMock(), gate: make(chan struct{})}
}

func (s *capHoldStore) Get(ctx context.Context, key string, w io.Writer) error {
	if strings.HasPrefix(key, testDataset+"/blocks/") {
		s.mu.Lock()
		s.keys = append(s.keys, key)
		s.mu.Unlock()
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Mock.Get(ctx, key, w)
}

func (s *capHoldStore) release() { close(s.gate) }

func (s *capHoldStore) blockKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.keys)
}

var _ objectstore.Store = (*capHoldStore)(nil)

// capBlockBytes 是单块字节数(内容 7B 前缀 + 1023B 填充);8 块总字节 8240。
const capBlockBytes = 1030

// capLookahead 是这些用例统一设定的邻域窗口块数,使窗口字节 = 2×1030 = 2060,明显小于
// 整单数据集 8240,从而把「比窗口」与「比整单」两种判据分开。
const capLookahead = 2

// newCapPrefetchFixture 落 8 块数据集(均需回源)并挂起源站块 Get;
// t.Cleanup 放行闸,避免下载协程滞留。
func newCapPrefetchFixture(t *testing.T) (*svcFixture, *capHoldStore, []ordersource.ChunkRef) {
	t.Helper()
	setLookahead(t, capLookahead)
	hs := newCapHoldStore()
	fx := newSvcFixtureWithStore(t, hs)
	specs := make([]svcBlockSpec, 0, 8)
	refs := make([]ordersource.ChunkRef, 0, 8)
	for i := 0; i < 8; i++ {
		path := fmt.Sprintf("c-%02d.tar", i)
		specs = append(specs, svcBlockSpec{
			path:    path,
			content: fmt.Sprintf("cap-%02d-", i) + strings.Repeat("x", capBlockBytes-7),
			local:   false,
			source:  true,
		})
		refs = append(refs, ordersource.ChunkRef{Path: path})
	}
	fx.commitDataset(t, testDataset, specs)
	t.Cleanup(hs.release)
	return fx, hs, refs
}

// setLookahead 把包级邻域窗口覆盖为 n,用例结束还原:bundle 内的 prefetcher 在首个
// ForOrder 时按当时的 lookahead 装配,用例必须先设再触发订单。
func setLookahead(t *testing.T, n int) {
	t.Helper()
	saved := lookahead
	lookahead = n
	t.Cleanup(func() { lookahead = saved })
}

// capOrder 构造首批 K = k 的订单(wiringOrder 默认 K=64,这里显式覆盖)。
func capOrder(stage string, refs []ordersource.ChunkRef, k int) ordersource.ConsumeOrder {
	o := wiringOrder(testDataset, stage, refs...)
	o.PrefetchChunks = k
	return o
}

// 窗口 2060 > 容量 2048 的一半 1024 → 降级为按需,只有显式按需块回源。
func TestForOrderDemotesWhenWindowOverHalfCapacity(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(2048) // half=1024 < 窗口 2×1030=2060
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	// 按需入口仍回源:以它的 Get 作调度器存活判据,再断言没有其他块回源。
	if err := fx.pm.bundleFor(testDataset, nil).prefetcher.EnqueueP0Demand(ctx, refs[7:8]); err != nil {
		t.Fatalf("EnqueueP0Demand: %v", err)
	}
	demandKey := blockKeyLiteral(testDataset, refs[7])
	waitFor(t, "按需块回源", func() bool { return slices.Contains(hs.blockKeys(), demandKey) })
	if got := hs.blockKeys(); len(got) != 1 || got[0] != demandKey {
		t.Fatalf("回源键 = %v, want 仅 %s(首批与邻域未入队)", got, demandKey)
	}
}

// 容量 = 0 字节维度未启用 → 不降级,首批照常回源。
func TestForOrderKeepsPrefetchWhenCapacityDisabled(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(0)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	waitFor(t, "首批 2 块回源", func() bool { return len(hs.blockKeys()) >= 2 })
	want := []string{blockKeyLiteral(testDataset, refs[0]), blockKeyLiteral(testDataset, refs[1])}
	if got := hs.blockKeys(); !sameKeySet(got, want) {
		t.Fatalf("回源键 = %v, want %v", got, want)
	}
}

// 修复点:数据集总字节(8240)超过容量(5000),旧判据会降级;新判据看窗口(2060)在
// 容量一半(2500)内 → 照常预取。
func TestForOrderKeepsPrefetchWhenDatasetOverCapacityButWindowFits(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(5000) // half=2500 ≥ 窗口 2060,但 < 数据集 8240
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	waitFor(t, "首批 2 块回源", func() bool { return len(hs.blockKeys()) >= 2 })
	want := []string{blockKeyLiteral(testDataset, refs[0]), blockKeyLiteral(testDataset, refs[1])}
	if got := hs.blockKeys(); !sameKeySet(got, want) {
		t.Fatalf("回源键 = %v, want %v(窗口在容量一半内不降级)", got, want)
	}
}

// 边界:窗口恰好等于容量一半(2060 == 4120/2)→ 不降级(判据是严格大于)。
func TestForOrderKeepsPrefetchWhenWindowEqualHalfCapacity(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(4120) // half=2060 == 窗口 2060
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	waitFor(t, "首批 2 块回源", func() bool { return len(hs.blockKeys()) >= 2 })
	want := []string{blockKeyLiteral(testDataset, refs[0]), blockKeyLiteral(testDataset, refs[1])}
	if got := hs.blockKeys(); !sameKeySet(got, want) {
		t.Fatalf("回源键 = %v, want %v", got, want)
	}
}

// 窗口随 lookahead 放大:lookahead=8 → 窗口 = 整单 8240 > 容量 16000 的一半 8000 → 降级。
func TestForOrderDemotesWhenLookaheadWide(t *testing.T) {
	setLookahead(t, 8) // 覆盖 fixture 的 2;窗口 = 8×1030 = 8240
	hs := newCapHoldStore()
	fx := newSvcFixtureWithStore(t, hs)
	specs := make([]svcBlockSpec, 0, 8)
	refs := make([]ordersource.ChunkRef, 0, 8)
	for i := 0; i < 8; i++ {
		path := fmt.Sprintf("c-%02d.tar", i)
		specs = append(specs, svcBlockSpec{
			path:    path,
			content: fmt.Sprintf("cap-%02d-", i) + strings.Repeat("x", capBlockBytes-7),
			local:   false,
			source:  true,
		})
		refs = append(refs, ordersource.ChunkRef{Path: path})
	}
	fx.commitDataset(t, testDataset, specs)
	t.Cleanup(hs.release)

	fx.pm.SetCacheCapacityBytes(16000) // half=8000 < 窗口 8240
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-wide", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	if err := fx.pm.bundleFor(testDataset, nil).prefetcher.EnqueueP0Demand(ctx, refs[7:8]); err != nil {
		t.Fatalf("EnqueueP0Demand: %v", err)
	}
	demandKey := blockKeyLiteral(testDataset, refs[7])
	waitFor(t, "按需块回源", func() bool { return slices.Contains(hs.blockKeys(), demandKey) })
	if got := hs.blockKeys(); len(got) != 1 || got[0] != demandKey {
		t.Fatalf("回源键 = %v, want 仅 %s(宽窗口降级,首批与邻域未入队)", got, demandKey)
	}
}

// 去重:订单头出现重复 ref 时,窗口按去重块累加。dup=[r0,r0,r1,r1,...] 去重前两块
// = r0+r1 = 2060 > 容量 3000 的一半 1500 → 降级;若误按列表前两项 r0+r0=1030 则不会降级。
func TestForOrderWindowCountsUniqueChunks(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(3000) // half=1500,去重窗口 2060 > 1500 → 降级
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	dup := []ordersource.ChunkRef{refs[0], refs[0], refs[1], refs[1], refs[2], refs[3]}
	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", dup, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	if err := fx.pm.bundleFor(testDataset, nil).prefetcher.EnqueueP0Demand(ctx, refs[7:8]); err != nil {
		t.Fatalf("EnqueueP0Demand: %v", err)
	}
	demandKey := blockKeyLiteral(testDataset, refs[7])
	waitFor(t, "按需块回源", func() bool { return slices.Contains(hs.blockKeys(), demandKey) })
	if got := hs.blockKeys(); len(got) != 1 || got[0] != demandKey {
		t.Fatalf("回源键 = %v, want 仅 %s(窗口按去重块计,降级)", got, demandKey)
	}
}

// 容量回升后下一单恢复首批预取(开关随订单重判,不粘滞)。
func TestForOrderRestoresPrefetchWhenCapacityRaised(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(2048) // half=1024 < 窗口 2060 → 降级
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder(降级): %v", err)
	}
	// 降级期不开放抓取闸:首批块尚未回源。
	if got := hs.blockKeys(); len(got) != 0 {
		t.Fatalf("降级期不应回源,实际 %v", got)
	}

	// 容量回升:窗口 2060 ≤ 新半 12288 → 恢复首批。
	fx.pm.SetCacheCapacityBytes(24576)
	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-2", refs, 2)); err != nil {
		t.Fatalf("ForOrder(恢复): %v", err)
	}
	waitFor(t, "恢复后首批块回源", func() bool {
		keys := hs.blockKeys()
		return slices.Contains(keys, blockKeyLiteral(testDataset, refs[0])) &&
			slices.Contains(keys, blockKeyLiteral(testDataset, refs[1]))
	})
}

// sameKeySet 按集合比较回源键(抓取协程并发,记录顺序不定)。
func sameKeySet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	g, w := slices.Clone(got), slices.Clone(want)
	slices.Sort(g)
	slices.Sort(w)
	return slices.Equal(g, w)
}
