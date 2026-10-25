package svc

// r1r3_e2e_test.go 是 R1/R2/R4(design 6.1 §5.9–§5.12)的永久回归:
//  - R1:CURRENT 更新后 Catalog 在刷新周期内切到新快照并加载映射;刷新失败保持
//        旧映射且累加 index_refresh_total{result="error"};同一 dataset 只启动一个 Follow。
//  - R2:周期心跳写出当前 snapID 的 claim;生命周期 ctx 取消后写出 Withdraw claim。
//  - R4:ObjstoreProbeConditionalWrite 开关接线,指标仅在支持时为 1,不支持时仍可启动。
// 复用 follow_test.go 的装配/提交装置与 orders_test.go 的 waitFor。

import (
	"context"
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

// installTestOrder 把一份最小订单安装到 bundle(prefixesFn 的数据源),避免预取器
// 在测试中发起无意义的回源请求。
func installTestOrder(b *datasetBundle, ds, path string) {
	b.mu.Lock()
	b.lastOrder = ordersource.ConsumeOrder{
		Dataset: ds, StageID: "s1",
		Chunks: []ordersource.ChunkRef{{Path: path, Chunk: 0}},
	}
	b.hasOrder = true
	b.mu.Unlock()
}

// TestR1R2E2E_FollowDeclareHeartbeatWithdraw 覆盖 R1 的快照切换与 R2 的 claim 生命周期:
// CURRENT 更新后 ≤ 刷新周期内切换映射;心跳/切换回调写出 claim;ctx 取消写 Withdraw。
func TestR1R2E2E_FollowDeclareHeartbeatWithdraw(t *testing.T) {
	const interval = 20 * time.Millisecond
	ag := newFollowAgent(t, interval, false)
	const ds = "r1r2-e2e"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snap1 := commitFollowEntries(t, ag, ds, "a.tar")
	b := ag.Prefetch.bundleFor(ds)
	installTestOrder(b, ds, "a.tar")
	ag.StartBackground(ctx)

	waitFor(t, "初始快照生效", func() bool { return b.catalog.SnapID() == snap1 })
	waitFor(t, "初始映射加载", func() bool {
		_, ok, err := b.catalog.Lookup(ctx, "a.tar", 0)
		return err == nil && ok
	})
	waitFor(t, "心跳写出 claim", func() bool {
		c := readOneClaim(t, ag.ObjectStore, ds)
		return c != nil && c.NodeID == ag.Cfg.NodeName.String() && c.SnapID == snap1
	})

	// 新快照增加 chunk 1;CURRENT 更新后一个刷新周期左右应切换。
	content := "content:a.tar"
	fp, n, err := ag.CS.Put(ctx, strings.NewReader(content))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	start := time.Now()
	snap2, err := index.NewWriter(ag.ObjectStore, ag.CS, index.Config{}).Commit(ctx, ds, []index.Entry{
		{Path: "a.tar", Chunk: 0, Size: n, Fingerprint: fp},
		{Path: "a.tar", Chunk: 1, Size: 1, Fingerprint: fp},
	})
	if err != nil {
		t.Fatalf("Commit(snap2): %v", err)
	}
	waitFor(t, "切换到新快照", func() bool { return b.catalog.SnapID() == snap2 })
	// 断言点:刷新由 ticker 驱动,允许一个周期加调度余量。
	if elapsed := time.Since(start); elapsed > 20*interval {
		t.Fatalf("快照切换耗时 %v,超过 20×IndexRefreshInterval(%v)", elapsed, 20*interval)
	}
	waitFor(t, "新条目可见", func() bool {
		_, ok, err := b.catalog.Lookup(ctx, "a.tar", 1)
		return err == nil && ok
	})
	waitFor(t, "claim 切到新快照", func() bool {
		c := readOneClaim(t, ag.ObjectStore, ds)
		return c != nil && c.SnapID == snap2
	})

	// 生命周期 ctx 取消 → 心跳对已知 dataset 写 Withdraw claim。
	cancel()
	waitFor(t, "ctx 取消写出 Withdraw claim", func() bool {
		c := readOneClaim(t, ag.ObjectStore, ds)
		return c != nil && c.SnapID == "" && c.PreviousSnapID == snap2
	})
}

// TestR1R2E2E_FollowFailureKeepsOldMapping:CURRENT 指向缺失 manifest 时刷新失败,
// 旧映射保持可用且 index_refresh_total{result="error"} 递增。
func TestR1R2E2E_FollowFailureKeepsOldMapping(t *testing.T) {
	const interval = 20 * time.Millisecond
	ag := newFollowAgent(t, interval, false)
	const ds = "r1r2-err"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snap1 := commitFollowEntries(t, ag, ds, "a.tar")
	b := ag.Prefetch.bundleFor(ds)
	installTestOrder(b, ds, "a.tar")
	ag.StartBackground(ctx)
	waitFor(t, "初始映射加载", func() bool {
		_, ok, err := b.catalog.Lookup(ctx, "a.tar", 0)
		return err == nil && ok
	})

	bad := "snap-bad 0000000000000000"
	if _, err := ag.ObjectStore.Put(ctx, index.CurrentKey(ds), strings.NewReader(bad), int64(len(bad))); err != nil {
		t.Fatalf("Put CURRENT: %v", err)
	}
	label := ag.Metrics.DatasetLabel(ds)
	waitFor(t, "error 指标递增", func() bool {
		return testutil.ToFloat64(ag.Metrics.IndexRefresh().WithLabelValues(label, metrics.ResultError)) >= 1
	})

	// 断言点:失败不切换、不清空。
	if got := b.catalog.SnapID(); got != snap1 {
		t.Fatalf("失败后 snapID = %q,期望保持 %q", got, snap1)
	}
	if _, ok, err := b.catalog.Lookup(ctx, "a.tar", 0); err != nil || !ok {
		t.Fatalf("失败后旧映射丢失: ok=%v err=%v", ok, err)
	}
}

// TestR1R2E2E_FollowOncePerDataset:同一 dataset 只启动一个 Follow(重复
// StartBackground、重复 bundleFor 均不得新增)。
func TestR1R2E2E_FollowOncePerDataset(t *testing.T) {
	ag := newFollowAgent(t, 50*time.Millisecond, false)
	const ds = "r1r2-once"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := ag.Prefetch.bundleFor(ds)
	ag.StartBackground(ctx)
	ag.StartBackground(ctx)
	if again := ag.Prefetch.bundleFor(ds); again != b {
		t.Fatal("bundleFor 应返回同一 bundle")
	}

	ag.Prefetch.mu.Lock()
	starts := ag.Prefetch.followStarts[ds]
	started := b.followStarted
	ag.Prefetch.mu.Unlock()
	if !started || starts != 1 {
		t.Fatalf("Follow 启动次数 = %d (started=%v),期望 1", starts, started)
	}
}

// TestR4E2E_ConditionalWriteProbeGauge:开关关闭时不探测(gauge=0);开启且后端支持
// 条件写时 gauge=1;开启但后端忽略条件写时 gauge=0 且装配不失败。
func TestR4E2E_ConditionalWriteProbeGauge(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		ag := newFollowAgent(t, 50*time.Millisecond, false)
		if got := testutil.ToFloat64(ag.Metrics.ObjectstoreConditionalWriteSupported()); got != 0 {
			t.Fatalf("probe disabled gauge = %v,期望 0", got)
		}
	})
	t.Run("enabled_supported", func(t *testing.T) {
		ag := newFollowAgent(t, 50*time.Millisecond, true)
		if got := testutil.ToFloat64(ag.Metrics.ObjectstoreConditionalWriteSupported()); got != 1 {
			t.Fatalf("probe enabled gauge = %v,期望 1", got)
		}
	})
	t.Run("enabled_unsupported", func(t *testing.T) {
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
