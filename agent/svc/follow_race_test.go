package svc

// follow_race_test.go:ForOrder 首次索引加载与 Catalog.Follow 首轮 tick 的竞态回归。
// 首轮 tick 在 lastOrder 安装前读到空前缀,会走 RefreshManifest 清空刚加载的映射;
// 因 snapID 未变,后续 tick 全部跳过,空映射永久固化。

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/agent/nodeid"
	"traincache/agent/ordersource"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// raceGateStore 包装 Mock,armed 后卡住首个 CURRENT 读(Follow 首轮的廉价检查)与
// 首个分片读(ForOrder 的 LoadPrefixes 已持有 Catalog 锁),用于确定性复现竞态。
type raceGateStore struct {
	*objectstore.Mock
	currentKey string

	armed atomic.Bool

	mu          sync.Mutex
	currentGets int
	shardGets   int

	firstCurrent   chan struct{}
	releaseCurrent chan struct{}
	firstShard     chan struct{}
	releaseShard   chan struct{}
}

// Get 在 armed 后闸住首个 CURRENT 读与首个分片读;其余读直接透传。
func (s *raceGateStore) Get(ctx context.Context, key string, w io.Writer) error {
	if s.armed.Load() {
		switch {
		case key == s.currentKey:
			s.mu.Lock()
			s.currentGets++
			n := s.currentGets
			s.mu.Unlock()
			if n == 1 {
				close(s.firstCurrent)
				select {
				case <-s.releaseCurrent:
				case <-ctx.Done():
				}
			}
		case strings.Contains(key, "/shard-") && strings.HasSuffix(key, ".idx"):
			s.mu.Lock()
			s.shardGets++
			n := s.shardGets
			s.mu.Unlock()
			if n == 1 {
				close(s.firstShard)
				select {
				case <-s.releaseShard:
				case <-ctx.Done():
				}
			}
		}
	}
	return s.Mock.Get(ctx, key, w)
}

// TestForOrderFirstIndexLoadNotClearedByFollow 复现并守住修复:先启动 Follow(首轮
// tick 卡在 CURRENT 读),再让 ForOrder 的 LoadPrefixes 持有 Catalog 锁加载分片;放行
// 首轮 tick 后,其空前缀 RefreshManifest 会排在同一锁后执行。订单 256 块、K=16,
// 断言稳定窗口内 K 之后的块仍可从索引命中。
func TestForOrderFirstIndexLoadNotClearedByFollow(t *testing.T) {
	const (
		ds      = "follow-race"
		nChunks = 256
		k       = 16
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &raceGateStore{
		Mock:           objectstore.NewMock(),
		currentKey:     index.CurrentKey(ds),
		firstCurrent:   make(chan struct{}),
		releaseCurrent: make(chan struct{}),
		firstShard:     make(chan struct{}),
		releaseShard:   make(chan struct{}),
	}
	dir := t.TempDir()
	cfg := Config{
		NodeName:             nodeid.ID("worker-01"),
		CacheRoot:            filepath.Join(dir, "cache"),
		OrdersDir:            filepath.Join(dir, "orders"),
		Verify:               VerifyXXH3,
		IndexRefreshInterval: 10 * time.Millisecond,
	}
	ag, err := assembleWith(store, cfg)
	if err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)

	// 256 个路径各一块,提交为单个快照。
	chunks := make([]ordersource.ChunkRef, nChunks)
	entries := make([]index.Entry, nChunks)
	for i := 0; i < nChunks; i++ {
		p := fmt.Sprintf("blocks/p%03d.tar", i)
		fp, n, err := ag.CS.Put(ctx, strings.NewReader("content:"+p))
		if err != nil {
			t.Fatalf("cas.Put(%s): %v", p, err)
		}
		entries[i] = index.Entry{Path: p, Chunk: 0, Size: n, Fingerprint: fp}
		chunks[i] = ordersource.ChunkRef{Path: p, Chunk: 0}
	}
	if _, err := index.NewWriter(ag.ObjectStore, ag.CS, index.Config{}).Commit(ctx, ds, entries); err != nil {
		t.Fatalf("Commit(%s): %v", ds, err)
	}

	b := ag.Prefetch.bundleFor(ds, nil)
	store.armed.Store(true)
	ag.StartBackground(ctx)

	waitSignal := func(ch chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("超时等待: %s", what)
		}
	}
	// Follow 首轮 tick 进入 CURRENT 读并停在此处(state 仍为空)。
	waitSignal(store.firstCurrent, "Follow 首轮 CURRENT 读")

	order := ordersource.ConsumeOrder{
		Dataset:        ds,
		StageID:        "stage-1",
		PrefetchChunks: k,
		Chunks:         chunks,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- ag.Prefetch.ForOrder(ctx, "pod-race", order) }()

	// LoadPrefixes 已持 Catalog 锁并开始下载分片。
	waitSignal(store.firstShard, "LoadPrefixes 首个分片读")
	close(store.releaseCurrent) // 放行首轮 tick:它将在锁上排队
	time.Sleep(20 * time.Millisecond)
	close(store.releaseShard) // LoadPrefixes 先完成,首轮 tick 随后执行

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ForOrder: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ForOrder 超时")
	}

	// 稳定窗口内 K 之后的块必须始终命中;修复前首轮 tick 会在此窗口内清空映射。
	stableUntil := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(stableUntil) {
		for i := k; i < nChunks; i++ {
			if _, ok, err := b.catalog.Lookup(ctx, chunks[i].Path, chunks[i].Chunk); err != nil || !ok {
				t.Fatalf("第 %d 块(K=%d 之后)索引未命中: ok=%v err=%v", i, k, ok, err)
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}
