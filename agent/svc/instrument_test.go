package svc

// instrument_test.go:综合验收补齐的埋点断言(埋点实现见 instrument.go 与 server.go/
// snapshot.go 的 observeRequest):
//   - 索引线(③):ForOrder 的 LoadPrefixes 成功/失败 → index_refresh_total{success|error}、
//     index_refresh_duration_seconds{success} 样本、index_shards_loaded 置真实分片数;
//   - 延迟线(⑥):四个业务 RPC 成功与错误各计一次 request_duration_seconds{endpoint,result};
//   - 订单到达 t_touch 刷新(④;§7.6 任务 4):订单内块经索引解析指纹后被
//     Collector.Touch 刷新(LastTouch 推进),索引未命中/空 chunks/未绑定 collector 跳过。

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// 直方图 family 名(独立字面量,防改名后测试跟着漂移)。
const (
	requestDurationFamily = "traincache_request_duration_seconds"
	indexRefreshDurFamily = "traincache_index_refresh_duration_seconds"
)

// histogramSamples 读 registry 中某直方图指标(按 name + 标签 kv 子集筛选)的样本计数
// (testutil.ToFloat64 仅支持 counter/gauge;Gather 口径与 wiring_test 的
// gatherFormatErrors 同风格,不 import client_model)。
func histogramSamples(t *testing.T, reg *metrics.Registry, name string, kv ...string) float64 {
	t.Helper()
	families, err := reg.Prometheus().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := make(map[string]string, len(kv)/2)
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			match := true
			for i := 0; i+1 < len(kv); i += 2 {
				if got[kv[i]] != kv[i+1] {
					match = false
					break
				}
			}
			if match && m.GetHistogram() != nil {
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return 0
}

// TestIndexLineMetricsOnOrderRefresh(综合验收 ②):ForOrder 的 LoadPrefixes 打点——
// 成功:index_refresh_total{dataset,success} +1、index_refresh_duration_seconds 样本 +1、
// index_shards_loaded 置真实分片数(5.3 D30:Catalog.ShardsLoaded;夹具单块数据集 manifest
// 恰 1 片);失败(无 CURRENT 的 dataset,LoadPrefixes 报错):{error} +1 且不上报耗时(避免
// 失败以 success 混入直方图),index_shards_loaded 保持前值(失败不更新该 gauge)。
// dataset 标签经白名单归并为 "other"(装置无白名单)。
func TestIndexLineMetricsOnOrderRefresh(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "i-000.tar", chunk: 0, content: "idx-0", local: true},
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // ForOrder 启动的预取调度生命周期 = 调用方 ctx

	refresh := func(result string) float64 {
		return testutil.ToFloat64(fx.reg.IndexRefresh().WithLabelValues("other", result))
	}
	durSamples := func() float64 {
		return histogramSamples(t, fx.reg, indexRefreshDurFamily, "dataset", "other", "result", metrics.ResultSuccess)
	}

	// 成功路径:订单触达 → 前缀分片加载一次。
	o := wiringOrder(testDataset, "epoch-1", ordersource.ChunkRef{Path: "i-000.tar"})
	if err := fx.pm.ForOrder(ctx, "pod-a", o); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}
	if got := refresh(metrics.ResultSuccess); got != 1 {
		t.Errorf("index_refresh{success} = %v, want 1(一次真实 LoadPrefixes)", got)
	}
	if got := durSamples(); got != 1 {
		t.Errorf("index_refresh_duration 样本数 = %v, want 1(成功上报耗时)", got)
	}
	if got := testutil.ToFloat64(fx.reg.IndexShardsLoaded().WithLabelValues("other")); got != 1 {
		t.Errorf("index_shards_loaded = %v, want 1(单块数据集 manifest 恰 1 片,D30)", got)
	}

	// 失败路径:无 CURRENT 的 dataset → LoadPrefixes 报错 → {error} +1;耗时样本不变。
	bad := wiringOrder("no-such-ds", "epoch-1", ordersource.ChunkRef{Path: "x.tar"})
	if err := fx.pm.ForOrder(ctx, "pod-a", bad); err == nil {
		t.Fatal("ForOrder(无 CURRENT dataset) 应返回 LoadPrefixes 错误")
	}
	if got := refresh(metrics.ResultError); got != 1 {
		t.Errorf("index_refresh{error} = %v, want 1", got)
	}
	if got := refresh(metrics.ResultSuccess); got != 1 {
		t.Errorf("index_refresh{success} = %v, want 保持 1(失败不重复计入)", got)
	}
	if got := durSamples(); got != 1 {
		t.Errorf("index_refresh_duration 样本数 = %v, want 保持 1(失败不上报耗时)", got)
	}
	if got := testutil.ToFloat64(fx.reg.IndexShardsLoaded().WithLabelValues("other")); got != 1 {
		t.Errorf("index_shards_loaded(失败后) = %v, want 保持 1(失败不更新该 gauge)", got)
	}
}

// TestRPCLatencyMetricsAllEndpoints(综合验收 ③):四个业务 RPC 成功与错误各计一次
// request_duration_seconds{endpoint,result}——endpoint 取 RPC 方法名;result 判定:
// nil → success,任何非 nil 错误(含 InvalidArgument/NotFound 等客户端输入类错误)→ error
// (裁决见回传)。断言以直方图样本数为口径。
func TestRPCLatencyMetricsAllEndpoints(t *testing.T) {
	fx := newSvcFixture(t)
	conn := startServerCfg(t, fx.cfg, fx.agent())
	pc := agentpb.NewPreheatServiceClient(conn)
	nc := agentpb.NewNodeServiceClient(conn)
	ctx := context.Background()

	// —— 成功路径:四 RPC 各一次 ——
	if _, err := pc.SubmitPreheat(ctx, validSubmitReq()); err != nil {
		t.Fatalf("SubmitPreheat: %v", err)
	}
	if _, err := pc.GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{TaskId: "task-1"}); err != nil {
		t.Fatalf("GetPreheatStatus: %v", err)
	}
	if _, err := nc.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{Dataset: testDataset}); err != nil {
		t.Fatalf("ListSnapshot: %v", err)
	}
	if _, err := nc.GetNodeStatus(ctx, &agentpb.GetNodeStatusRequest{Node: "worker-01"}); err != nil {
		t.Fatalf("GetNodeStatus: %v", err)
	}

	// —— 错误路径:四 RPC 各一次(InvalidArgument/NotFound 均归 error)——
	invalidNode := validSubmitReq()
	invalidNode.Node = "other-node"
	if _, err := pc.SubmitPreheat(ctx, invalidNode); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SubmitPreheat(错误路径) code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := pc.GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{TaskId: "no-such-task"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetPreheatStatus(错误路径) code = %v, want NotFound", status.Code(err))
	}
	if _, err := nc.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListSnapshot(错误路径) code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := nc.GetNodeStatus(ctx, &agentpb.GetNodeStatusRequest{Node: "other-node"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetNodeStatus(错误路径) code = %v, want InvalidArgument", status.Code(err))
	}

	// —— 断言:每 endpoint × 每 result 的直方图样本数 ≥ 1 ——
	for _, ep := range []string{
		metrics.EndpointSubmitPreheat,
		metrics.EndpointGetPreheatStatus,
		metrics.EndpointListSnapshot,
		metrics.EndpointGetNodeStatus,
	} {
		for _, result := range []string{metrics.ResultSuccess, metrics.ResultError} {
			if got := histogramSamples(t, fx.reg, requestDurationFamily, "endpoint", ep, "result", result); got < 1 {
				t.Errorf("request_duration{endpoint=%s, result=%s} 样本数 = %v, want ≥1", ep, result, got)
			}
		}
	}
}

// TestOrderTouchRefreshOnConsumeOrder(综合验收 ④;§7.6 任务 4):新 ConsumeOrder 到达 →
// 订单内每个块经 dataset 索引解析指纹后 Collector.Touch(now)——预置旧 LastTouch 的块被
// 推进到订单处理时刻、订单内新块建档;索引未命中的 ref 跳过;空 chunks 订单不触发任何
// 刷新(ForOrder 守卫)。隔离手段:订单窗口块本地副本 Evict 且源站无对象(下载必失败),
// onBlockDone 仅在下载成功时 Touch,排除既有 t_touch 源对断言的干扰;collector 经真实
// Reader(落盘 ring 手工绑定,等价首个 ring 发现回调;不跑发现循环)。
func TestOrderTouchRefreshOnConsumeOrder(t *testing.T) {
	ag, _ := newWiringAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "t-000.tar", content: "touch-0"},
		wiringBlock{path: "t-001.tar", content: "touch-1"},
		wiringBlock{path: "t-002.tar", content: "touch-2"}, // 订单外对照块
	)
	fp0, fp1, fp2 := fps["t-000.tar#0"], fps["t-001.tar#0"], fps["t-002.tar#0"]

	// 订单窗口块 Evict(本地无、源站无 → 预取下载必失败,不产生 t_touch)。
	for _, fp := range []cas.Fingerprint{fp0, fp1} {
		if err := ag.CS.Evict(fp); err != nil {
			t.Fatalf("cas.Evict(%s): %v", fp, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	ringPath := filepath.Join(t.TempDir(), "pod-a.ring")
	writeTestRing(t, ringPath, 1, 1, 256, 2, wiringOrder(wiringDataset, "ring-seed"))
	r := ordersource.NewReader(ringPath, filepath.Dir(ringPath))
	t.Cleanup(func() {
		cancel() // 先停 collector 后台循环,再关 reader
		r.Close()
	})
	ag.bindCollector(ctx, "pod-a", r)
	c := ag.collector()
	if c == nil {
		t.Fatal("collector 未绑定")
	}

	// 预置旧 t_touch:fp0 将被订单刷新推进;fp2 作订单外对照应保持不变。
	tOld := time.Now().Add(-time.Hour)
	c.Touch(fp0, tOld)
	c.Touch(fp2, tOld)

	// 新订单到达(含一个索引未命中的路径 → 该 ref 跳过)。
	before := time.Now()
	o := wiringOrder(wiringDataset, "epoch-1",
		ordersource.ChunkRef{Path: "t-000.tar"},
		ordersource.ChunkRef{Path: "t-001.tar"},
		ordersource.ChunkRef{Path: "no-such.tar"})
	if err := ag.Prefetch.ForOrder(ctx, "pod-a", o); err != nil {
		t.Fatalf("ForOrder: %v", err)
	}

	lastTouch := make(map[cas.Fingerprint]time.Time)
	for _, e := range c.ListForEviction() {
		lastTouch[e.FP] = e.LastTouch
	}
	if len(lastTouch) != 3 {
		t.Fatalf("热度表条目 = %d, want 3(预置 2 + 订单内新块 1;索引未命中跳过)", len(lastTouch))
	}
	if lt, ok := lastTouch[fp0]; !ok || !lt.After(tOld) || lt.Before(before) {
		t.Errorf("订单内已记录块 t-000 的 LastTouch = %v(存在 %v), want 推进到订单处理时刻(> %v)", lt, ok, tOld)
	}
	if lt, ok := lastTouch[fp1]; !ok || lt.Before(before) {
		t.Errorf("订单内新块 t-001 的 LastTouch = %v(存在 %v), want 已建档且不早于订单处理时刻", lt, ok)
	}
	if lt := lastTouch[fp2]; !lt.Equal(tOld) {
		t.Errorf("订单外块 t-002 的 LastTouch = %v, want 保持 %v(仅订单内块刷新)", lt, tOld)
	}

	// 空 chunks 订单:守卫生效,不触发任何刷新(全部条目 LastTouch 快照不变)。
	beforeEmpty := make(map[cas.Fingerprint]time.Time, len(lastTouch))
	for fp, lt := range lastTouch {
		beforeEmpty[fp] = lt
	}
	if err := ag.Prefetch.ForOrder(ctx, "pod-a", wiringOrder(wiringDataset, "epoch-empty")); err != nil {
		t.Fatalf("ForOrder(空 chunks): %v", err)
	}
	after := c.ListForEviction()
	if len(after) != 3 {
		t.Errorf("空 chunks 订单后条目数 = %d, want 3", len(after))
	}
	for _, e := range after {
		if lt, ok := beforeEmpty[e.FP]; !ok || !lt.Equal(e.LastTouch) {
			t.Errorf("空 chunks 订单不应刷新 t_touch:%s 的 LastTouch = %v, want %v", e.FP, e.LastTouch, lt)
		}
	}
}

// TestOrderTouchSkipsUnboundCollector(综合验收 ④ 边界):collector 未绑定(首个 ring
// 发现回调之前)时订单到达不报错、不留 t_touch 条目(跳过语义;后续订单到达自然补齐)。
func TestOrderTouchSkipsUnboundCollector(t *testing.T) {
	ag, _ := newWiringAgent(t)
	fps := commitWiringBlocks(t, ag, wiringDataset, wiringBlock{path: "u-000.tar", content: "unbound-0"})
	// 本地副本 Evict 且源站无对象:预取下载必失败(不产生 onBlockDone 成功触达)。
	if err := ag.CS.Evict(fps["u-000.tar#0"]); err != nil {
		t.Fatalf("cas.Evict: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := ag.Prefetch.ForOrder(ctx, "pod-a",
		wiringOrder(wiringDataset, "epoch-1", ordersource.ChunkRef{Path: "u-000.tar"})); err != nil {
		t.Fatalf("ForOrder(未绑定 collector): %v", err)
	}

	// 此后绑定 collector:此前的订单未留下任何热度条目。
	ringPath := filepath.Join(t.TempDir(), "pod-a.ring")
	writeTestRing(t, ringPath, 1, 1, 256, 2, wiringOrder(wiringDataset, "ring-seed"))
	r := ordersource.NewReader(ringPath, filepath.Dir(ringPath))
	t.Cleanup(func() {
		cancel()
		r.Close()
	})
	ag.bindCollector(ctx, "pod-a", r)
	if got := len(ag.collector().ListForEviction()); got != 0 {
		t.Fatalf("热度表条目 = %d, want 0(未绑定 collector 时订单不触发 t_touch)", got)
	}
}
