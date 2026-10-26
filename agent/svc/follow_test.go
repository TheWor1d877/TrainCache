package svc

// follow_test.go:R1(6.1 §5.9)快照跟随接线测试:周期内切换、失败保持旧映射并计
// error、同一 dataset 只启动一个 Follow;R4 条件写探测与 R3 跳过指标。

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/agent/ordersource"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// newFollowAgent 装配一个低刷新周期的 Agent(Mock 源站 + 临时目录)。
func newFollowAgent(t *testing.T, interval time.Duration, probe bool) *Agent {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		NodeName:                      nodeid.ID("worker-01"),
		CacheRoot:                     filepath.Join(dir, "cache"),
		OrdersDir:                     filepath.Join(dir, "orders"),
		Verify:                        VerifyXXH3,
		IndexRefreshInterval:          interval,
		ObjstoreProbeConditionalWrite: probe,
	}
	ag, err := assembleWith(objectstore.NewMock(), cfg)
	if err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)
	return ag
}

// commitFollowEntries 把若干 (path,#0) 条目写入 CAS 并提交索引,返回 snapID。
func commitFollowEntries(t *testing.T, ag *Agent, dataset string, paths ...string) string {
	t.Helper()
	ctx := context.Background()
	entries := make([]index.Entry, 0, len(paths))
	for _, p := range paths {
		content := "content:" + p
		fp, n, err := ag.CS.Put(ctx, strings.NewReader(content))
		if err != nil {
			t.Fatalf("cas.Put(%s): %v", p, err)
		}
		entries = append(entries, index.Entry{Path: p, Chunk: 0, Size: n, Fingerprint: fp})
	}
	snapID, err := index.NewWriter(ag.ObjectStore, ag.CS, index.Config{}).Commit(ctx, dataset, entries)
	if err != nil {
		t.Fatalf("Commit(%s): %v", dataset, err)
	}
	return snapID
}

// TestFollowSwitchesSnapshotWithinInterval:CURRENT 更新后 ≤ RefreshInterval 内
// Catalog 切到新快照,且前缀感知加载的新条目可见。
func TestFollowSwitchesSnapshotWithinInterval(t *testing.T) {
	const interval = 20 * time.Millisecond
	ag := newFollowAgent(t, interval, false)
	const ds = "follow"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snap1 := commitFollowEntries(t, ag, ds, "a.tar")
	b := ag.Prefetch.bundleFor(ds, nil)
	// 模拟 ForOrder 安装订单(prefixesFn 数据源;避免真实预取器在测试中发回源请求)。
	b.mu.Lock()
	b.lastOrder = ordersource.ConsumeOrder{
		Dataset: ds, StageID: "s1",
		Chunks: []ordersource.ChunkRef{{Path: "a.tar", Chunk: 0}},
	}
	b.hasOrder = true
	b.mu.Unlock()

	ag.StartBackground(ctx)
	waitFor(t, "首个快照生效", func() bool { return b.catalog.SnapID() == snap1 })
	waitFor(t, "初始映射加载", func() bool {
		_, ok, err := b.catalog.Lookup(ctx, "a.tar", 0)
		return err == nil && ok
	})

	// 新快照增加同 path 的新 chunk(旧条目仍在)。
	content := "content:a.tar"
	fp, n, err := ag.CS.Put(ctx, strings.NewReader(content))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	snap2, err := index.NewWriter(ag.ObjectStore, ag.CS, index.Config{}).Commit(ctx, ds, []index.Entry{
		{Path: "a.tar", Chunk: 0, Size: n, Fingerprint: fp},
		{Path: "a.tar", Chunk: 1, Size: 1, Fingerprint: fp},
	})
	if err != nil {
		t.Fatalf("Commit(snap2): %v", err)
	}
	if snap2 == snap1 {
		t.Fatalf("期望新 snapID,实际未变 %q", snap2)
	}

	// ≤ RefreshInterval(加调度余量)内切到新快照,新条目可见。
	waitFor(t, "切换到新快照", func() bool { return b.catalog.SnapID() == snap2 })
	waitFor(t, "新条目可见", func() bool {
		_, ok, err := b.catalog.Lookup(ctx, "a.tar", 1)
		return err == nil && ok
	})
}

// TestFollowFailureKeepsOldMappingAndCountsError:CURRENT 指向缺失 manifest 时,
// 旧映射保持可用,index_refresh_total{result=error} 递增。
func TestFollowFailureKeepsOldMappingAndCountsError(t *testing.T) {
	const interval = 20 * time.Millisecond
	ag := newFollowAgent(t, interval, false)
	const ds = "follow-err"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snap1 := commitFollowEntries(t, ag, ds, "a.tar")
	b := ag.Prefetch.bundleFor(ds, nil)
	b.mu.Lock()
	b.lastOrder = ordersource.ConsumeOrder{
		Dataset: ds, StageID: "s1",
		Chunks: []ordersource.ChunkRef{{Path: "a.tar", Chunk: 0}},
	}
	b.hasOrder = true
	b.mu.Unlock()
	ag.StartBackground(ctx)

	waitFor(t, "初始映射加载", func() bool {
		_, ok, err := b.catalog.Lookup(ctx, "a.tar", 0)
		return err == nil && ok
	})

	// 指向不存在的 manifest,触发单轮加载失败。
	bad := "snap-bad 0000000000000000"
	if _, err := ag.ObjectStore.Put(ctx, index.CurrentKey(ds), strings.NewReader(bad), int64(len(bad))); err != nil {
		t.Fatalf("Put CURRENT: %v", err)
	}
	label := ag.Metrics.DatasetLabel(ds)
	waitFor(t, "error 指标递增", func() bool {
		return testutil.ToFloat64(ag.Metrics.IndexRefresh().WithLabelValues(label, metrics.ResultError)) >= 1
	})

	// 失败不切换、不清空:仍是旧 snapID 且旧条目可查。
	if got := b.catalog.SnapID(); got != snap1 {
		t.Fatalf("失败后 snapID = %q,期望保持 %q", got, snap1)
	}
	if _, ok, err := b.catalog.Lookup(ctx, "a.tar", 0); err != nil || !ok {
		t.Fatalf("失败后旧映射丢失: ok=%v err=%v", ok, err)
	}
}

// TestFollowStartsOncePerDataset:同一 dataset 只启动一个 Follow(含 StartBackground
// 对既有 bundle 的补启动与后续 bundleFor)。
func TestFollowStartsOncePerDataset(t *testing.T) {
	ag := newFollowAgent(t, 50*time.Millisecond, false)
	const ds = "follow-once"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := ag.Prefetch.bundleFor(ds, nil)
	ag.StartBackground(ctx)
	ag.StartBackground(ctx) // 重复调用不得重复启动
	if again := ag.Prefetch.bundleFor(ds, nil); again != b {
		t.Fatalf("bundleFor 应返回同一 bundle")
	}

	ag.Prefetch.mu.Lock()
	starts := ag.Prefetch.followStarts[ds]
	started := b.followStarted
	ag.Prefetch.mu.Unlock()
	if !started || starts != 1 {
		t.Fatalf("Follow 启动次数 = %d (started=%v),期望 1", starts, started)
	}
}

// unconditionalStore 模拟忽略条件写前置条件的后端(ProbeConditionalWrite 判不支持)。
type unconditionalStore struct{ *objectstore.Mock }

func (s unconditionalStore) PutIfMatch(ctx context.Context, key string, r io.Reader,
	size int64, _ string) (string, error) {
	return s.Mock.Put(ctx, key, r, size)
}

// TestConditionalWriteProbe:支持 → gauge=1;不支持 → gauge=0 且装配不失败。
func TestConditionalWriteProbe(t *testing.T) {
	t.Run("supported", func(t *testing.T) {
		ag := newFollowAgent(t, 50*time.Millisecond, true)
		if got := testutil.ToFloat64(ag.Metrics.ObjectstoreConditionalWriteSupported()); got != 1 {
			t.Fatalf("supported gauge = %v,期望 1", got)
		}
	})
	t.Run("unsupported", func(t *testing.T) {
		dir := t.TempDir()
		cfg := Config{
			NodeName:                      nodeid.ID("worker-01"),
			CacheRoot:                     filepath.Join(dir, "cache"),
			OrdersDir:                     filepath.Join(dir, "orders"),
			Verify:                        VerifyXXH3,
			IndexRefreshInterval:          50 * time.Millisecond,
			ObjstoreProbeConditionalWrite: true,
		}
		ag, err := assembleWith(unconditionalStore{objectstore.NewMock()}, cfg)
		if err != nil {
			t.Fatalf("不支持条件写不得阻止装配: %v", err)
		}
		t.Cleanup(ag.Prefetch.Close)
		if got := testutil.ToFloat64(ag.Metrics.ObjectstoreConditionalWriteSupported()); got != 0 {
			t.Fatalf("unsupported gauge = %v,期望 0", got)
		}
	})
}

// TestOnSweepSkipMetric:OnSweepSkip 驱动 index_sweep_skipped_total{dataset,reason}。
func TestOnSweepSkipMetric(t *testing.T) {
	ag := newFollowAgent(t, 50*time.Millisecond, false)
	const ds = "sweep-ds"
	ag.onSweepSkip(ds, metrics.ReasonManifestUnavailable)
	ag.onSweepSkip(ds, "unknown-reason") // 集合外归一 other
	label := ag.Metrics.DatasetLabel(ds)
	if got := testutil.ToFloat64(ag.Metrics.IndexSweepSkipped().WithLabelValues(label, metrics.ReasonManifestUnavailable)); got != 1 {
		t.Fatalf("manifest_unavailable 计数 = %v,期望 1", got)
	}
	if got := testutil.ToFloat64(ag.Metrics.IndexSweepSkipped().WithLabelValues(label, "other")); got != 1 {
		t.Fatalf("other 计数 = %v,期望 1", got)
	}
}
