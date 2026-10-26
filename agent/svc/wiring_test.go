package svc

// wiring_test.go:M4/M5/M6 接线集成测试(接线工程验收,§7.1 九项裁决的端到端验证)。
// 全部装置经 assembleWith(objectstore.Mock, 临时目录)构造,不依赖网络与外部服务;
// 断言覆盖:
//   - 字段完整性(Agent 导出字段齐全 + collector 懒绑定前为 nil);
//   - 重扫描修复闭环(损坏本地块 → 隔离 → 经 D25 键修复 → NodeStatus 动态字段);
//   - /metrics handler 可 gather 七线指标族;
//   - collector 懒绑定与钩子三型帧(open_report 热度 / advance 预取命中 / stage_switch 阶段);
//   - 淘汰闭环(装配层 HeatSource/ProtectedSource/Sweep 适配 + RunOnce);
//   - 降级聚合判定(注入时钟:无 reader 不降级 / 全停滞降级且不推送订单 / 恢复自动续)。

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/evictor"
	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/agent/ordersource"
	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// wiringDataset 是接线集成测试的数据集名。
const wiringDataset = "wd"

// wiringBlock 描述一个接线测试块(本地 CAS + 索引 + 可选源站 D25 键)。
type wiringBlock struct {
	path    string
	content string
	source  bool // true = 在源站放 D25 键(<dataset>/blocks/<path>#0)
}

// newWiringAgent 以 Mock 源站 + 临时目录装配 Agent(与 Assemble 同构,仅换 store 与路径);
// 淘汰触发线压到 1%(测试内更易跨线,真实水位下亦保守)。t.Cleanup 关闭预取调度 ctx。
func newWiringAgent(t *testing.T) (*Agent, Config) {
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
	ag, err := assembleWith(objectstore.NewMock(), cfg)
	if err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)
	return ag, cfg
}

// commitWiringBlocks 把块落本地 CAS 并提交索引(chunk 恒 0);按需放源站对象;
// 返回 path#chunk → 指纹。
func commitWiringBlocks(t *testing.T, ag *Agent, dataset string, blocks ...wiringBlock) map[string]cas.Fingerprint {
	t.Helper()
	ctx := context.Background()
	fps := make(map[string]cas.Fingerprint, len(blocks))
	entries := make([]index.Entry, 0, len(blocks))
	for _, b := range blocks {
		fp, n, err := ag.CS.Put(ctx, strings.NewReader(b.content))
		if err != nil {
			t.Fatalf("cas.Put(%s): %v", b.path, err)
		}
		fps[b.path+"#0"] = fp
		entries = append(entries, index.Entry{Path: b.path, Chunk: 0, Size: n, Fingerprint: fp})
		if b.source {
			key := dataset + "/blocks/" + b.path + "#0" // D25 字面量(独立于实现,防键布局漂移)
			if _, err := ag.ObjectStore.Put(ctx, key, strings.NewReader(b.content), int64(len(b.content))); err != nil {
				t.Fatalf("源站 Put(%s): %v", key, err)
			}
		}
	}
	if _, err := index.NewWriter(ag.ObjectStore, ag.CS, index.Config{}).Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("index Commit(%s): %v", dataset, err)
	}
	return fps
}

// wiringBlockFile 按 CAS 冻结布局拼本地块文件路径(blocks/<fp[0:2]hex>/<fp hex>.blk)。
func wiringBlockFile(cfg Config, fp cas.Fingerprint) string {
	h := fp.String()
	return filepath.Join(cfg.CacheRoot, "blocks", h[:2], h+".blk")
}

// wiringOrder 构造字段齐全的 ConsumeOrder。
func wiringOrder(dataset, stage string, chunks ...ordersource.ChunkRef) ordersource.ConsumeOrder {
	return ordersource.ConsumeOrder{
		Dataset:        dataset,
		SnapshotID:     "snap-1",
		StageID:        stage,
		ShuffleSeed:    7,
		BatchSize:      16,
		PrefetchChunks: 64,
		Chunks:         chunks,
	}
}

// startWiringBackground 起装配后的全部后台组件(重扫描/淘汰/钩子收端/发现循环),
// cleanup 取消 ctx 并等待发现循环退出。
func startWiringBackground(t *testing.T, ag *Agent) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// 发现目录先建好(发现循环内 MkdirAll 是异步的;测试随后要立即落 ring)。
	if err := os.MkdirAll(ag.Cfg.OrdersDir, 0o755); err != nil {
		t.Fatalf("创建 orders 目录: %v", err)
	}
	ag.Rescan.Start(ctx)
	ag.Evictor.Start(ctx)
	if err := ag.HookSock.Start(ctx); err != nil {
		t.Fatalf("HookSock.Start: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ag.Orders.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// TestWiringFieldsRescanRepairAndStatus:装配字段完整性 → Rescan SAMPLING→DONE → 坏块
// 隔离/修复闭环可观察(NodeStatus.quarantined ≥ 1、quarantine 目录清空、块可重开)→
// GetNodeStatus 动态字段 → /metrics 七线可 gather。
func TestWiringFieldsRescanRepairAndStatus(t *testing.T) {
	ag, cfg := newWiringAgent(t)

	// 字段完整性(D34):导出字段齐全;collector 懒绑定(首个 ring 前为 nil)。
	if ag.CS == nil || ag.Fetcher == nil || ag.ObjectStore == nil || ag.Metrics == nil ||
		ag.Orders == nil || ag.Prefetch == nil || ag.Preheat == nil || ag.Rescan == nil ||
		ag.Evictor == nil || ag.HookSock == nil {
		t.Fatalf("Agent 字段不齐全: %+v", ag)
	}
	if ag.Collector != nil {
		t.Fatalf("首个 ring 前 collector 应为 nil(懒绑定)")
	}

	// 预置:一块正常 + 一块将被破坏(源站备好 D25 键 → 修复重下可成功)。
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "g-000.tar", content: "wiring-good"},
		wiringBlock{path: "r-000.tar", content: "wiring-bad", source: true},
	)
	badFP := fps["r-000.tar#0"]
	if err := os.WriteFile(wiringBlockFile(cfg, badFP), []byte("corrupted-not-matching"), 0o644); err != nil {
		t.Fatalf("破坏本地块: %v", err)
	}

	conn := startServerCfg(t, cfg, ag)
	client := agentpb.NewNodeServiceClient(conn)

	// Start 前:SAMPLING(readiness = phase != SAMPLING;design §8)。
	st, err := client.GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "worker-01", Item: agentpb.NodeStatusItem_RESCAN})
	if err != nil {
		t.Fatalf("GetNodeStatus(RESCAN): %v", err)
	}
	if got := st.GetRescan().GetPhase(); got != agentpb.RescanStatus_SAMPLING {
		t.Fatalf("Start 前 rescan.phase = %v, want SAMPLING", got)
	}

	startWiringBackground(t, ag)

	// 轮询:小批量块的重扫描应快速到 DONE,且坏块被检出(quarantined ≥ 1)。
	deadline := time.Now().Add(testTimeout)
	for {
		st, err = client.GetNodeStatus(context.Background(),
			&agentpb.GetNodeStatusRequest{Node: "worker-01", Item: agentpb.NodeStatusItem_RESCAN})
		if err != nil {
			t.Fatalf("GetNodeStatus(RESCAN): %v", err)
		}
		if st.GetRescan().GetPhase() == agentpb.RescanStatus_DONE {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rescan 超时未 DONE: %v", st.GetRescan())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := st.GetRescan().GetQuarantined(); got < 1 {
		t.Errorf("rescan.quarantined = %d, want ≥ 1(损坏块被隔离)", got)
	}
	if got := st.GetRescan().GetSampled(); got < 2 {
		t.Errorf("rescan.sampled = %d, want ≥ 2(两块均校验)", got)
	}
	if got := st.GetRescan().GetRepairQueue(); got != 0 {
		t.Errorf("rescan.repair_queue = %d, want 0(DONE 前队列排空)", got)
	}

	// 修复闭环:隔离目录已清空(修复成功移除隔离副本)、块可重新打开、gauge 归零。
	waitFor(t, "quarantine 目录清空(修复成功)", func() bool {
		fps, err := ag.CS.ListQuarantined()
		return err == nil && len(fps) == 0
	})
	if br, err := ag.CS.Open(badFP); err != nil {
		t.Errorf("修复后本地块应可重开: %v", err)
	} else {
		_ = br.Close()
	}
	if got := testutil.ToFloat64(ag.Metrics.QuarantinedBlocks().WithLabelValues("other")); got != 0 {
		t.Errorf("quarantined_blocks = %v, want 0(修复成功递减)", got)
	}

	// 全量 GetNodeStatus:动态字段(CacheRoot/cached_bytes/free_bytes/catalog/rescan)。
	st, err = client.GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "worker-01"})
	if err != nil {
		t.Fatalf("GetNodeStatus(全量): %v", err)
	}
	if !st.GetAgentOk() || st.GetNode() != "worker-01" || st.GetCacheRoot() != cfg.CacheRoot {
		t.Errorf("node/agent_ok/cache_root = %q/%v/%q", st.GetNode(), st.GetAgentOk(), st.GetCacheRoot())
	}
	if st.GetCachedBytes() <= 0 {
		t.Errorf("cached_bytes = %d, want > 0(本地两块已落盘)", st.GetCachedBytes())
	}
	if st.GetFreeBytes() <= 0 {
		t.Errorf("free_bytes = %d, want > 0", st.GetFreeBytes())
	}
	cat := st.GetCatalog()
	if len(cat) != 1 || cat[0].GetDataset() != wiringDataset {
		t.Fatalf("catalog = %v, want 单条 dataset=%q", cat, wiringDataset)
	}
	if cat[0].GetSnapshotId() == "" || cat[0].GetShards() < 1 || cat[0].GetEntries() != 2 {
		t.Errorf("catalog 条目 = snapshot %q / shards %d / entries %d, want 非空/≥1/2",
			cat[0].GetSnapshotId(), cat[0].GetShards(), cat[0].GetEntries())
	}

	// /metrics handler(与 agentd 同构)可 gather 七线指标族。
	rec := httptest.NewRecorder()
	promhttp.HandlerFor(ag.Metrics.Prometheus(), promhttp.HandlerOpts{}).
		ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("/metrics 状态码 = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, name := range []string{
		"traincache_cache_hits_total",                // ① 命中率线
		"traincache_fetch_bytes_total",               // ② 回源线
		"traincache_index_refresh_total",             // ③ 索引线
		"traincache_gc_sweeps_total",                 // ④ GC 线
		"traincache_evict_blocks_total",              // ⑤ 磁盘淘汰线
		"traincache_request_duration_seconds",        // ⑥ 延迟线
		"traincache_agent_up",                        // ⑦ 节点状态线
		"traincache_agent_degraded",                  // ⑦ 节点状态线
		"traincache_rescan_progress",                 // ⑦(M4)
		"traincache_quarantined_blocks",              // ⑦(M4/M5)
		"traincache_disk_usage_ratio",                // ⑦(M6)
		"traincache_ordersource_format_errors_total", // M2 既有线
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics 缺 family %s", name)
		}
	}
}

// TestWiringCollectorHooksAndEvictor:首个 ring → collector 懒绑定;钩子三型帧端到端
// (open_report 入热度 / advance 进预取命中率 / stage_switch 换阶段);淘汰闭环经装配层
// 适配(HeatSource=collector 快照、ProtectedSource=预取窗口、Sweep=core GC)删本地冷块。
func TestWiringCollectorHooksAndEvictor(t *testing.T) {
	ag, cfg := newWiringAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "p-000.tar", content: "wire-p0"}, // 订单窗口块(硬保护)
		wiringBlock{path: "p-001.tar", content: "wire-p1"}, // 淘汰目标
	)
	fp0, fp1 := fps["p-000.tar#0"], fps["p-001.tar#0"]

	setScanInterval(t, 10*time.Millisecond)
	startWiringBackground(t, ag)
	if ag.collector() != nil {
		t.Fatalf("首个 ring 前 collector 应为 nil")
	}

	// 落订单 ring → 发现 → 绑定 collector 并启动。
	writeTestRing(t, filepath.Join(cfg.OrdersDir, "pod-a.ring"), 1, 1, 256, 2,
		wiringOrder(wiringDataset, "epoch-1", ordersource.ChunkRef{Path: "p-000.tar"}))
	waitFor(t, "collector 绑定首个 ring", func() bool { return ag.collector() != nil })

	b := ag.Prefetch.bundleFor(wiringDataset, nil)
	waitFor(t, "订单阶段安装 epoch-1", func() bool { return b.prefetcher.Status().Stage == "epoch-1" })

	// 钩子三型帧(§5.4 冻结 JSON 标签)。
	c := dialHook(t, hookSockPath(cfg))

	// ① open_report:经索引解析指纹 → collector 热度表(本地存在的块)。
	sendHook(t, c, []byte(`{"type":"open_report","path":"p-001.tar","chunk":0}`))
	waitFor(t, "open_report 进入 collector 热度表", func() bool {
		for _, e := range ag.collector().ListForEviction() {
			if e.FP == fp1 && e.Heat > 0 {
				return true
			}
		}
		return false
	})

	// ② advance:collector 推进 + 预取器消费侧推进(命中率出现样本;本地命中 → > 0)。
	sendHook(t, c, []byte(`{"type":"advance","path":"p-000.tar","chunk":0}`))
	waitFor(t, "advance 进预取器(命中率出现样本)", func() bool {
		return b.prefetcher.Status().HitRate > 0
	})

	// ③ stage_switch:以 ring 当前订单为基底换阶段,转交预取器。
	sendHook(t, c, []byte(`{"type":"stage_switch","stage_id":"epoch-2","shuffle_seed":12345}`))
	waitFor(t, "stage_switch 直报新阶段", func() bool {
		return b.prefetcher.Status().Stage == "epoch-2"
	})

	// 淘汰闭环:与装配同构的淘汰器(Usage 注入固定利用率,不依赖宿主磁盘水位;
	// HeatSource/ProtectedSource/Sweep 均为装配层真实接线适配)。
	ev := evictor.New(ag.CS, evictHeatSource{ag}, ag.Prefetch, ag.Metrics, nil, evictor.Config{
		TriggerPercent:  1,
		DeltaPercent:    5,
		CriticalPercent: 90,
		Usage:           func() (uint64, uint64, error) { return 80, 100, nil },
		Sweep:           ag.sweepDatasets,
	})
	if br, err := ag.CS.Open(fp1); err != nil {
		t.Fatalf("前置:p-001 块应本地存在: %v", err)
	} else {
		_ = br.Close()
	}
	ev.RunOnce(context.Background())

	if got := testutil.ToFloat64(ag.Metrics.GCSweeps().WithLabelValues("other", metrics.ResultSuccess)); got != 1 {
		t.Errorf("gc_sweeps(success) = %v, want 1(触发线以上先驱动一轮 core GC)", got)
	}
	if got := testutil.ToFloat64(ag.Metrics.EvictBlocks().WithLabelValues("other", metrics.ResultNormal)); got != 1 {
		t.Errorf("evict_blocks(normal) = %v, want 1(冷块被淘汰)", got)
	}
	if got := testutil.ToFloat64(ag.Metrics.EvictProtectedSkipped().WithLabelValues("other", metrics.ResultNormal)); got < 1 {
		t.Errorf("evict_protected_skipped(normal) = %v, want ≥ 1(订单窗口块受保护)", got)
	}
	if _, err := ag.CS.Open(fp1); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("p-001 本地副本应被淘汰: Open err = %v", err)
	}
	if _, err := os.Stat(wiringBlockFile(cfg, fp1)); !os.IsNotExist(err) {
		t.Errorf("p-001 块文件应被删除: Stat err = %v", err)
	}
	if br, err := ag.CS.Open(fp0); err != nil {
		t.Errorf("订单窗口块应受保护未被淘汰: %v", err)
	} else {
		_ = br.Close()
	}

	// 装配实例的 RunOnce 可直接执行(真实 statfs 水位;不 panic、不破坏保护语义)。
	ag.Evictor.RunOnce(context.Background())
}

// setDegradeTickInterval 把白盒降级判定周期改小并注册恢复。
func setDegradeTickInterval(t *testing.T, d time.Duration) {
	t.Helper()
	old := degradeTickInterval
	degradeTickInterval = d
	t.Cleanup(func() { degradeTickInterval = old })
}

// TestWiringDegradeAggregation:降级聚合判定与推送闸门(§7.4;注入时钟造停滞)——
//   - 无 reader:不降级(即使时钟错位);
//   - reader 出现且推进正常:订单正常推送(gauge=0);
//   - 时钟错位(> 停滞阈值视为停滞):gauge=1,降级期间订单被抑制;
//   - 时钟恢复(模拟推进恢复):gauge=0,后续订单恢复推送。
func TestWiringDegradeAggregation(t *testing.T) {
	ag, cfg := newWiringAgent(t)
	commitWiringBlocks(t, ag, wiringDataset, wiringBlock{path: "p-000.tar", content: "degrade-p0"})

	setScanInterval(t, 10*time.Millisecond)
	setDegradeTickInterval(t, 10*time.Millisecond)

	// 时钟注入:offset 错位"当前时间"模拟订单源停滞(reader 的 LastAdvance 用真实时钟)。
	var offset atomic.Int64
	ag.Orders.now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	offset.Store(int64(31 * time.Second)) // 任何 reader 一旦出现即视为停滞

	startDiscovery(t, ag.Orders)
	gauge := func() float64 {
		return testutil.ToFloat64(ag.Metrics.AgentDegraded().WithLabelValues("worker-01"))
	}
	stage := func() string { return ag.Prefetch.bundleFor(wiringDataset, nil).prefetcher.Status().Stage }

	// ① 无 reader:不降级(gauge=0),并观察若干拍确认不是"尚未判定"。
	waitFor(t, "无 reader 时不降级", func() bool { return gauge() == 0 })
	quiet := time.Now().Add(5 * degradeTickInterval)
	for time.Now().Before(quiet) {
		if got := gauge(); got != 0 {
			t.Fatalf("无 reader 时 agent_degraded = %v, want 0", got)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// ② 时钟恢复 → 落 ring:reader 出现且正常推进 → 订单推送、阶段安装、gauge=0。
	offset.Store(0)
	ring := filepath.Join(cfg.OrdersDir, "pod-a.ring")
	writeTestRing(t, ring, 1, 1, 256, 2,
		wiringOrder(wiringDataset, "epoch-1", ordersource.ChunkRef{Path: "p-000.tar"}))
	waitFor(t, "epoch-1 阶段安装(正常推送)", func() bool { return stage() == "epoch-1" })
	waitFor(t, "推进正常时 gauge=0", func() bool { return gauge() == 0 })

	// ③ 时钟错位(全部 reader 停滞)→ 聚合判定降级。
	offset.Store(int64(31 * time.Second))
	waitFor(t, "全部 reader 停滞 → gauge=1", func() bool { return gauge() == 1 })

	// ④ 降级期间新订单被抑制(推送闸门;观察窗 ≥ 数个读端轮询周期)。
	writeTestRing(t, ring, 2, 2, 256, 2,
		wiringOrder(wiringDataset, "epoch-2", ordersource.ChunkRef{Path: "p-000.tar"}))
	quiet = time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(quiet) {
		if got := stage(); got != "epoch-1" {
			t.Fatalf("降级期间订单被推送:stage = %q, want 保持 epoch-1", got)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// ⑤ 恢复(时钟回正,等价"任一 reader 推进")→ 解除降级,后续订单自动续。
	offset.Store(0)
	waitFor(t, "推进恢复 → gauge=0", func() bool { return gauge() == 0 })
	writeTestRing(t, ring, 3, 3, 256, 2,
		wiringOrder(wiringDataset, "epoch-3", ordersource.ChunkRef{Path: "p-000.tar"}))
	waitFor(t, "epoch-3 阶段安装(恢复推送)", func() bool { return stage() == "epoch-3" })
}
