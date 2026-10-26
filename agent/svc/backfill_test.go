package svc

// backfill_test.go:D44 读 miss 异步补块(M8)——缺失 → EnqueueP0、坏块 →
// Quarantine+RepairOne、本地命中零下载、映射缺失不 panic、空映射 demand 照常回源(D60)、
// read_miss 指标计数。装置经 assembleWith(objectstore.Mock, 临时目录)构造;
// 等待一律带 deadline 轮询。

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/agent/ordersource"
	"traincache/core/cas"
	"traincache/core/objectstore"
)

// backfillQuietWindow 是"不应发生下载"类负断言的观察窗。
const backfillQuietWindow = 150 * time.Millisecond

// backfillStore 包装 objectstore.Mock,记录 Get 的对象键(断言补块下载次数用)。
type backfillStore struct {
	*objectstore.Mock
	mu   sync.Mutex
	keys []string
}

func (s *backfillStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.Mock.Get(ctx, key, w)
}

// blockDownloads 返回数据块对象键(<dataset>/blocks/...)的 Get 次数。
func (s *backfillStore) blockDownloads(dataset string) int {
	prefix := dataset + "/blocks/"
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range s.keys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

var _ objectstore.Store = (*backfillStore)(nil)

// newBackfillAgent 构造装配完整的 Agent(不启动后台循环);t.Cleanup 关闭预取调度。
func newBackfillAgent(t *testing.T) (*Agent, Config, *backfillStore) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		NodeName:  nodeid.ID("worker-01"),
		CacheRoot: filepath.Join(dir, "cache"),
		OrdersDir: filepath.Join(dir, "orders"),
		Verify:    VerifyXXH3,
		Heat: HeatConfig{
			DiskTriggerPercent:  1,
			DiskDeltaPercent:    5,
			DiskCriticalPercent: 90,
		},
	}
	store := &backfillStore{Mock: objectstore.NewMock()}
	ag, err := assembleWith(store, cfg)
	if err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)
	return ag, cfg, store
}

// preloadBackfillMapping 加载 dataset 的索引映射(模拟订单已到达后 resolveRef 可解析)。
func preloadBackfillMapping(t *testing.T, ag *Agent, dataset string) {
	t.Helper()
	if err := ag.Prefetch.bundleFor(dataset, nil).catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("加载索引映射: %v", err)
	}
}

// fetchRequestsCount 读取回源请求计数{result,reason}(dataset 先经白名单归并)。
func fetchRequestsCount(t *testing.T, reg *metrics.Registry, dataset, result, reason string) float64 {
	t.Helper()
	return testutil.ToFloat64(reg.FetchRequests().WithLabelValues(reg.DatasetLabel(dataset), result, reason))
}

// hookReadMissCount 读取 traincache_hook_read_miss_total{reason} 的计数。
func hookReadMissCount(t *testing.T, reg *metrics.Registry, reason string) float64 {
	t.Helper()
	mfs, err := reg.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "traincache_hook_read_miss_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "reason" && lp.GetValue() == reason {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestBackfillAbsentEnqueuesP0:本地缺失 → resolveRef/Lookup/Open(ErrNotFound)→ P0 补块,
// 块落本地;read_miss 指标按 reason 计数且只下载一次。
func TestBackfillAbsentEnqueuesP0(t *testing.T) {
	ag, _, store := newBackfillAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "miss-000.tar", content: "miss-content", source: true})
	fp := fps["miss-000.tar#0"]
	ref := ordersource.ChunkRef{Path: "miss-000.tar"}
	preloadBackfillMapping(t, ag, wiringDataset)

	if err := ag.CS.Evict(fp); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if _, err := ag.CS.Open(fp); !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("Evict 后 Open err = %v, want ErrNotFound", err)
	}

	ag.handleReadMiss(ref, "absent")
	if got := hookReadMissCount(t, ag.Metrics, "absent"); got != 1 {
		t.Errorf("hook_read_miss{absent} = %v, want 1", got)
	}

	waitFor(t, "缺失块被 P0 补回本地", func() bool {
		br, err := ag.CS.Open(fp)
		if err != nil {
			return false
		}
		_ = br.Close()
		return true
	})
	if n := store.blockDownloads(wiringDataset); n != 1 {
		t.Errorf("补块下载次数 = %d, want 1", n)
	}
	if got := fetchRequestsCount(t, ag.Metrics, wiringDataset, metrics.ResultSuccess, metrics.FetchReasonDemand); got != 1 {
		t.Errorf("fetch_requests{success,demand} = %v, want 1(demand 路径 reason 打点)", got)
	}
	if got := fetchRequestsCount(t, ag.Metrics, wiringDataset, metrics.ResultSuccess, metrics.FetchReasonPrefetch); got != 0 {
		t.Errorf("fetch_requests{success,prefetch} = %v, want 0(demand 不得记成 prefetch)", got)
	}
	if got := hookReadMissCount(t, ag.Metrics, "corrupt"); got != 0 {
		t.Errorf("hook_read_miss{corrupt} = %v, want 0", got)
	}
}

// TestBackfillCorruptQuarantineRepair:本地坏块 → Quarantine + RepairOne 重下修复,
// 隔离副本清空、块可重开;read_miss{corrupt} 计数。
func TestBackfillCorruptQuarantineRepair(t *testing.T) {
	ag, cfg, store := newBackfillAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "bad-000.tar", content: "bad-content", source: true})
	fp := fps["bad-000.tar#0"]
	ref := ordersource.ChunkRef{Path: "bad-000.tar"}
	preloadBackfillMapping(t, ag, wiringDataset)

	if err := os.WriteFile(wiringBlockFile(cfg, fp), []byte("corrupted-not-matching"), 0o644); err != nil {
		t.Fatalf("破坏本地块: %v", err)
	}
	if _, err := ag.CS.Open(fp); !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("坏块 Open err = %v, want ErrCorrupt", err)
	}

	ag.handleReadMiss(ref, "corrupt")
	if got := hookReadMissCount(t, ag.Metrics, "corrupt"); got != 1 {
		t.Errorf("hook_read_miss{corrupt} = %v, want 1", got)
	}

	waitFor(t, "坏块修复后可重开且隔离副本清空", func() bool {
		qs, err := ag.CS.ListQuarantined()
		if err != nil || len(qs) != 0 {
			return false
		}
		br, err := ag.CS.Open(fp)
		if err != nil {
			return false
		}
		_ = br.Close()
		return true
	})
	if n := store.blockDownloads(wiringDataset); n != 1 {
		t.Errorf("坏块修复下载次数 = %d, want 1", n)
	}
	if got := fetchRequestsCount(t, ag.Metrics, wiringDataset, metrics.ResultSuccess, metrics.FetchReasonRepair); got != 1 {
		t.Errorf("fetch_requests{success,repair} = %v, want 1(repair 路径 reason 打点)", got)
	}
	if got := fetchRequestsCount(t, ag.Metrics, wiringDataset, metrics.ResultSuccess, metrics.FetchReasonDemand); got != 0 {
		t.Errorf("fetch_requests{success,demand} = %v, want 0(repair 不得记成 demand)", got)
	}
}

// TestBackfillHitNoDownload:本地命中 → 直接返回,不启动预取/不下载。
func TestBackfillHitNoDownload(t *testing.T) {
	ag, _, store := newBackfillAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "hit-000.tar", content: "hit-content", source: true})
	fp := fps["hit-000.tar#0"]
	ref := ordersource.ChunkRef{Path: "hit-000.tar"}
	preloadBackfillMapping(t, ag, wiringDataset)

	ag.handleReadMiss(ref, "absent")
	waitFor(t, "resolveRef 创建 bundle", func() bool {
		_, ok := ag.Prefetch.existingBundle(wiringDataset)
		return ok
	})
	time.Sleep(backfillQuietWindow) // 补块 goroutine 已完成(本地 Open 无 IO 等待)

	if n := store.blockDownloads(wiringDataset); n != 0 {
		t.Errorf("本地命中不应下载,实际数据块 Get %d 次", n)
	}
	b, _ := ag.Prefetch.existingBundle(wiringDataset)
	b.mu.Lock()
	started := b.started
	b.mu.Unlock()
	if started {
		t.Error("本地命中不应启动预取调度")
	}
	if br, err := ag.CS.Open(fp); err != nil {
		t.Errorf("命中块应可打开: %v", err)
	} else {
		_ = br.Close()
	}
}

// TestBackfillEmptyMappingStillEnqueuesDemand:dataset 已知(bundle 存在)但索引映射
// 为空(Follow 首轮空前缀清空 / 从未 LoadPrefixes)→ demand 仍按 D25 键入 P0 回源,
// 不得因 Lookup 未命中丢帧(D60 故障场景)。
func TestBackfillEmptyMappingStillEnqueuesDemand(t *testing.T) {
	ag, _, store := newBackfillAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "emptymap-000.tar", content: "emptymap-content", source: true})
	fp := fps["emptymap-000.tar#0"]
	ref := ordersource.ChunkRef{Path: "emptymap-000.tar"}
	if err := ag.CS.Evict(fp); err != nil {
		t.Fatalf("Evict: %v", err)
	}

	// 只注册 bundle,不加载映射——归属可解,条目必未命中。
	ag.Prefetch.bundleFor(wiringDataset, nil)

	ag.handleReadMiss(ref, "absent")
	waitFor(t, "空映射下 demand 补块回本地", func() bool {
		br, err := ag.CS.Open(fp)
		if err != nil {
			return false
		}
		_ = br.Close()
		return true
	})
	if n := store.blockDownloads(wiringDataset); n != 1 {
		t.Errorf("空映射补块下载次数 = %d, want 1", n)
	}
	if got := fetchRequestsCount(t, ag.Metrics, wiringDataset, metrics.ResultSuccess, metrics.FetchReasonDemand); got != 1 {
		t.Errorf("fetch_requests{success,demand} = %v, want 1", got)
	}
}

// TestBackfillMappingMissingNoPanic:映射缺失/无法解析 dataset → 只记日志返回,不 panic;
// 指标仍按帧计数。装配缺失(Prefetch/CS 为 nil)同样安全。
func TestBackfillMappingMissingNoPanic(t *testing.T) {
	ag, _, _ := newBackfillAgent(t)
	ref := ordersource.ChunkRef{Path: "unknown-000.tar"}

	ag.handleReadMiss(ref, "absent")
	if got := hookReadMissCount(t, ag.Metrics, "absent"); got != 1 {
		t.Errorf("hook_read_miss{absent} = %v, want 1", got)
	}
	time.Sleep(backfillQuietWindow)
	if _, ok := ag.Prefetch.existingBundle(wiringDataset); ok {
		t.Error("映射缺失时不应创建 dataset bundle")
	}

	// 装配缺失场景:直接构造只带指标表的 Agent。
	reg := metrics.New(metrics.Config{Node: "worker-02"})
	dummy := &Agent{Metrics: reg}
	dummy.handleReadMiss(ref, "absent")
	if got := hookReadMissCount(t, reg, "absent"); got != 1 {
		t.Errorf("装配缺失时 hook_read_miss{absent} = %v, want 1", got)
	}
}
