package svc

// 缓存压力订单降级(ForOrder × TRAINCACHE_CACHE_CAPACITY_BYTES)用例:容量 > 0 且
// 订单数据集总字节 > 容量 → 首批与邻域都不回源,按需入口照常;容量 = 0 或容量足够 →
// 首批照常回源;重复 ref 去重后计字节。源站块 Get 被闸 store 挂起:降级用例以「按需块
// 已回源而其他块未回源」为判据(调度器活着,负断言不靠静默窗口),首批用例等「回源已发生」。

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

// capBlockBytes 是单块字节数(内容 7B 前缀 + 1023B 填充);4 块总字节 4120。
const capBlockBytes = 1030

// newCapPrefetchFixture 落 4 块数据集(均需回源)并挂起源站块 Get;
// t.Cleanup 放行闸,避免下载协程滞留。
func newCapPrefetchFixture(t *testing.T) (*svcFixture, *capHoldStore, []ordersource.ChunkRef) {
	t.Helper()
	hs := newCapHoldStore()
	fx := newSvcFixtureWithStore(t, hs)
	specs := make([]svcBlockSpec, 0, 4)
	refs := make([]ordersource.ChunkRef, 0, 4)
	for i := 0; i < 4; i++ {
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

// capOrder 构造首批 K = k 的订单(wiringOrder 默认 K=64,这里显式覆盖)。
func capOrder(stage string, refs []ordersource.ChunkRef, k int) ordersource.ConsumeOrder {
	o := wiringOrder(testDataset, stage, refs...)
	o.PrefetchChunks = k
	return o
}

func TestForOrderDemotesWhenCapacityBelowDataset(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(2048) // < 4×1030 = 4120
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	// 按需入口仍回源:以它的 Get 作调度器存活判据,再断言没有其他块回源。
	if err := fx.pm.bundleFor(testDataset, nil).prefetcher.EnqueueP0Demand(ctx, refs[3:4]); err != nil {
		t.Fatalf("EnqueueP0Demand: %v", err)
	}
	demandKey := blockKeyLiteral(testDataset, refs[3])
	waitFor(t, "按需块回源", func() bool { return slices.Contains(hs.blockKeys(), demandKey) })
	if got := hs.blockKeys(); len(got) != 1 || got[0] != demandKey {
		t.Fatalf("回源键 = %v, want 仅 %s(首批与邻域未入队)", got, demandKey)
	}
}

func TestForOrderKeepsPrefetchWhenCapacityDisabledOrSufficient(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity int64
	}{
		{"字节维度未启用", 0},
		{"容量足够", 8192},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx, hs, refs := newCapPrefetchFixture(t)
			fx.pm.SetCacheCapacityBytes(tc.capacity)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
				t.Fatalf("ForOrder: %v", err)
			}
			// 首批 2 块回源(闸上挂起不完成,K 未就绪前邻域不开放,不会有第 3 块)。
			// 抓取协程并发,Get 记录顺序不定,按集合比。
			waitFor(t, "首批 2 块回源", func() bool { return len(hs.blockKeys()) >= 2 })
			want := []string{blockKeyLiteral(testDataset, refs[0]), blockKeyLiteral(testDataset, refs[1])}
			if got := hs.blockKeys(); !sameKeySet(got, want) {
				t.Fatalf("回源键 = %v, want %v", got, want)
			}
		})
	}
}

func TestForOrderCapacityCountsUniqueChunks(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	// 2×1030 = 2060 < 3072 < 4×1030 = 4120:不去重会误降级。
	fx.pm.SetCacheCapacityBytes(3072)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	dup := []ordersource.ChunkRef{refs[0], refs[0], refs[1], refs[1]}
	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", dup, 2)); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	key0 := blockKeyLiteral(testDataset, refs[0])
	waitFor(t, "首批块回源", func() bool { return slices.Contains(hs.blockKeys(), key0) })
	// P1 两槽同为 refs[0],去重表合并为一次回源;refs[1] 在 K 就绪前不回源。
	if got := hs.blockKeys(); len(got) != 1 || got[0] != key0 {
		t.Fatalf("回源键 = %v, want 仅 %s", got, key0)
	}
}

func TestForOrderRestoresPrefetchWhenCapacityRaised(t *testing.T) {
	fx, hs, refs := newCapPrefetchFixture(t)
	fx.pm.SetCacheCapacityBytes(2048)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-1", refs, 2)); err != nil {
		t.Fatalf("ForOrder(降级): %v", err)
	}
	// 容量回升后下一单恢复首批预取(开关随订单重判,不粘滞)。
	fx.pm.SetCacheCapacityBytes(8192)
	if err := fx.pm.ForOrder(ctx, "pod-cap", capOrder("epoch-2", refs, 2)); err != nil {
		t.Fatalf("ForOrder(恢复): %v", err)
	}
	waitFor(t, "恢复后首批 2 块回源", func() bool { return len(hs.blockKeys()) >= 2 })
	want := []string{blockKeyLiteral(testDataset, refs[0]), blockKeyLiteral(testDataset, refs[1])}
	if got := hs.blockKeys(); !sameKeySet(got, want) {
		t.Fatalf("回源键 = %v, want %v", got, want)
	}
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
