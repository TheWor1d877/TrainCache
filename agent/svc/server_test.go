package svc

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/agent/rescan"
	"traincache/core/fetch"
	"traincache/core/index"
)

// testConfig 返回静态测试配置。
func testConfig() Config {
	return Config{
		NodeName:    nodeid.ID("worker-01"),
		GRPCAddr:    ":0",
		MetricsAddr: ":0",
		Verify:      VerifyXXH3,
		CacheRoot:   "/var/cache/traincache",
	}
}

// startServer 在 bufconn 内存网络上起全套服务(health + 三业务服务),业务组件用
// 标准测试装置(Mock 源站 + 临时 CAS)。
func startServer(t *testing.T) *grpc.ClientConn {
	t.Helper()
	return startServerWith(t, newSvcFixture(t).agent())
}

// startServerWith 同上,但注入给定 Agent(需要预置数据集/任务的用例)。
func startServerWith(t *testing.T, ag *Agent) *grpc.ClientConn {
	t.Helper()
	return startServerCfg(t, testConfig(), ag)
}

// startServerCfg 以给定配置与 Agent 起服务(动态字段用例:Server 的 CacheRoot 须指向
// 装置临时目录,而非 testConfig 的生产路径)。
func startServerCfg(t *testing.T, cfg Config, ag *Agent) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(cfg, ag).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestGetNodeStatusDynamicFields:M5 起 GetNodeStatus 的用量/目录/重扫描字段均为动态实测
// (取代 M1 静态字段):
//   - cached_bytes = CacheRoot 下 blocks/ 与 quarantine/ 文件字节和(精确断言);
//   - free_bytes = statfs 实测 > 0;
//   - catalog = 已知 dataset(已装配 bundle)的快照 ID/分片数/条目数(经对象存储实测);
//   - rescan:未装配 Rescanner → DONE(枚举零值 SAMPLING 会卡 readiness,显式 DONE)。
func TestGetNodeStatusDynamicFields(t *testing.T) {
	fx := newSvcFixture(t)

	// 本地副本全部保源站/Evict(local 缺省 false):blocks/ 内不留提交产生的块文件,
	// 目录用量只剩下面预置的已知大小文件。
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "dyn-000.tar", chunk: 0, content: "dyn-a0"},
		{path: "dyn-001.tar", chunk: 0, content: "dyn-a1"},
	})
	fx.preloadMapping(t, testDataset) // 建立 bundle(已知 dataset)+ 加载索引映射

	writeSized := func(rel string, n int) {
		t.Helper()
		p := filepath.Join(fx.cfg.CacheRoot, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", rel, err)
		}
		if err := os.WriteFile(p, bytes.Repeat([]byte{'x'}, n), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", rel, err)
		}
	}
	writeSized(filepath.Join("blocks", "aa", "b.blk"), 5) // blocks/ 子树
	writeSized(filepath.Join("quarantine", "bad.blk"), 7) // quarantine/ 子树

	conn := startServerCfg(t, fx.cfg, fx.agent())
	st, err := agentpb.NewNodeServiceClient(conn).GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "worker-01"})
	if err != nil {
		t.Fatalf("GetNodeStatus: %v", err)
	}

	if st.GetNode() != "worker-01" || !st.GetAgentOk() {
		t.Errorf("node/agent_ok = %q/%v, want worker-01/true", st.GetNode(), st.GetAgentOk())
	}
	if st.GetCacheRoot() != fx.cfg.CacheRoot {
		t.Errorf("cache_root = %q, want %q", st.GetCacheRoot(), fx.cfg.CacheRoot)
	}
	if got := st.GetCachedBytes(); got != 12 {
		t.Errorf("cached_bytes = %d, want 12(blocks/ 5 字节 + quarantine/ 7 字节)", got)
	}
	if got := st.GetFreeBytes(); got <= 0 {
		t.Errorf("free_bytes = %d, want > 0(statfs 实测)", got)
	}
	if st.GetRunningTasks() != 0 {
		t.Errorf("running_tasks = %d, want 0(空任务表)", st.GetRunningTasks())
	}
	// 未装配 Rescanner:必须显式 DONE(枚举零值是 SAMPLING,readiness = phase != SAMPLING)。
	if got := st.GetRescan().GetPhase(); got != agentpb.RescanStatus_DONE {
		t.Errorf("rescan.phase = %v, want DONE(无 Rescanner;SAMPLING 会被 readiness 判为未就绪)", got)
	}

	cat := st.GetCatalog()
	if len(cat) != 1 || cat[0].GetDataset() != testDataset {
		t.Fatalf("catalog = %v, want 单条 dataset=%q", cat, testDataset)
	}
	if cat[0].GetSnapshotId() == "" {
		t.Error("catalog.snapshot_id 为空,应取 CURRENT 内容")
	}
	if cat[0].GetShards() < 1 {
		t.Errorf("catalog.shards = %d, want ≥1(manifest 解析)", cat[0].GetShards())
	}
	if cat[0].GetEntries() != 2 {
		t.Errorf("catalog.entries = %d, want 2(提交的两块)", cat[0].GetEntries())
	}

	// CATALOG 单项:同源数据,且不含其他字段组。
	st, err = agentpb.NewNodeServiceClient(conn).GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "worker-01", Item: agentpb.NodeStatusItem_CATALOG})
	if err != nil {
		t.Fatalf("GetNodeStatus(CATALOG): %v", err)
	}
	if len(st.GetCatalog()) != 1 || st.GetCatalog()[0].GetEntries() != 2 {
		t.Errorf("CATALOG 项 = %v, want 与全量一致的目录", st.GetCatalog())
	}
	if st.GetCachedBytes() != 0 || st.GetFreeBytes() != 0 {
		t.Errorf("CATALOG 不应含用量字段,实际 cached=%d free=%d", st.GetCachedBytes(), st.GetFreeBytes())
	}
}

// TestGetNodeStatusRescanPhaseDynamic:rescan 状态为实时映射——未 Start 的 Rescanner 报
// SAMPLING(readiness 未就绪),Start 后空 CAS 直接 DONE;四计数逐字段透传(置零初值)。
func TestGetNodeStatusRescanPhaseDynamic(t *testing.T) {
	fx := newSvcFixture(t)
	f := fetch.New(fx.store, fx.cs, fetch.Options{
		MaxInflightBytes: 1 << 20,
		MaxTasks:         2,
		MaxConnsPerHost:  1,
		MaxRetries:       0,
	})
	res := rescan.New(fx.cs, f, fx.store,
		func(context.Context, string) (*index.Catalog, error) { return nil, errors.New("测试未装配") },
		rescanMetrics{metrics.New(metrics.Config{})})

	ag := fx.agent()
	ag.Rescan = res
	conn := startServerCfg(t, fx.cfg, ag)
	client := agentpb.NewNodeServiceClient(conn)

	// 未 Start:初始阶段 SAMPLING。
	st, err := client.GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "worker-01", Item: agentpb.NodeStatusItem_RESCAN})
	if err != nil {
		t.Fatalf("GetNodeStatus(RESCAN): %v", err)
	}
	if got := st.GetRescan().GetPhase(); got != agentpb.RescanStatus_SAMPLING {
		t.Fatalf("未 Start 的 phase = %v, want SAMPLING(初始值)", got)
	}

	// Start 后:空 CAS 直接 DONE(轮询观察过渡)。
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	res.Start(ctx)
	deadline := time.Now().Add(testTimeout)
	for {
		st, err = client.GetNodeStatus(context.Background(),
			&agentpb.GetNodeStatusRequest{Node: "worker-01", Item: agentpb.NodeStatusItem_RESCAN})
		if err != nil {
			t.Fatalf("GetNodeStatus(RESCAN): %v", err)
		}
		if st.GetRescan().GetPhase() == agentpb.RescanStatus_DONE {
			if st.GetRescan().GetQuarantined() != 0 || st.GetRescan().GetRepairQueue() != 0 {
				t.Errorf("空扫描 quarantined/repair_queue = %d/%d, want 0/0",
					st.GetRescan().GetQuarantined(), st.GetRescan().GetRepairQueue())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rescan 超时未 DONE: %v", st.GetRescan())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestGetNodeStatusEmptyNodeMeansLocal(t *testing.T) {
	conn := startServer(t)
	st, err := agentpb.NewNodeServiceClient(conn).GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{})
	if err != nil {
		t.Fatalf("GetNodeStatus(node 空): %v", err)
	}
	if st.GetNode() != "worker-01" {
		t.Errorf("node = %q, want 本机 worker-01", st.GetNode())
	}
}

func TestGetNodeStatusNodeMismatch(t *testing.T) {
	conn := startServer(t)
	_, err := agentpb.NewNodeServiceClient(conn).GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "other-node"})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument(design §5.1 节点不符)", got)
	}
	if msg := status.Convert(err).Message(); msg == "" {
		t.Error("InvalidArgument 应带明确 message(指明请求节点与本节点)")
	}
}

func TestGetNodeStatusItemFilter(t *testing.T) {
	conn := startServer(t)
	client := agentpb.NewNodeServiceClient(conn)
	ctx := context.Background()

	tests := []struct {
		name  string
		item  agentpb.NodeStatusItem
		check func(t *testing.T, st *agentpb.NodeStatus)
	}{
		{"UNSPECIFIED 全量", agentpb.NodeStatusItem_UNSPECIFIED, func(t *testing.T, st *agentpb.NodeStatus) {
			if !st.GetAgentOk() {
				t.Error("全量应含 agent_ok=true")
			}
			if st.GetCacheRoot() != "/var/cache/traincache" {
				t.Errorf("全量应含 cache_root,实际 %q", st.GetCacheRoot())
			}
			if st.GetRescan().GetPhase() != agentpb.RescanStatus_DONE {
				t.Errorf("全量应含 rescan.phase=DONE,实际 %v", st.GetRescan().GetPhase())
			}
		}},
		{"AGENT 仅 node+agent_ok", agentpb.NodeStatusItem_AGENT, func(t *testing.T, st *agentpb.NodeStatus) {
			if !st.GetAgentOk() {
				t.Error("AGENT 应含 agent_ok=true")
			}
			if st.GetCacheRoot() != "" || st.GetCachedBytes() != 0 || st.GetFreeBytes() != 0 ||
				st.GetRunningTasks() != 0 || st.GetRescan() != nil || len(st.GetCatalog()) != 0 {
				t.Errorf("AGENT 应只含 node+agent_ok,实际 %v", st)
			}
		}},
		{"CATALOG 仅 node+catalog", agentpb.NodeStatusItem_CATALOG, func(t *testing.T, st *agentpb.NodeStatus) {
			if st.GetAgentOk() || st.GetCacheRoot() != "" || st.GetRescan() != nil {
				t.Errorf("CATALOG 应只含 node+catalog,实际 %v", st)
			}
		}},
		{"SPACE 仅 node+cache_root+cached_bytes+free_bytes", agentpb.NodeStatusItem_SPACE, func(t *testing.T, st *agentpb.NodeStatus) {
			if st.GetCacheRoot() != "/var/cache/traincache" {
				t.Errorf("SPACE 应含 cache_root,实际 %q", st.GetCacheRoot())
			}
			if st.GetAgentOk() || st.GetRunningTasks() != 0 || st.GetRescan() != nil {
				t.Errorf("SPACE 不应含其他字段,实际 %v", st)
			}
		}},
		{"TASKS 仅 node+running_tasks", agentpb.NodeStatusItem_TASKS, func(t *testing.T, st *agentpb.NodeStatus) {
			if st.GetCacheRoot() != "" || st.GetAgentOk() || st.GetRescan() != nil {
				t.Errorf("TASKS 应只含 node+running_tasks,实际 %v", st)
			}
		}},
		{"RESCAN 仅 node+rescan", agentpb.NodeStatusItem_RESCAN, func(t *testing.T, st *agentpb.NodeStatus) {
			if st.GetRescan().GetPhase() != agentpb.RescanStatus_DONE {
				t.Errorf("RESCAN 的 phase = %v, want DONE", st.GetRescan().GetPhase())
			}
			if st.GetAgentOk() || st.GetCacheRoot() != "" {
				t.Errorf("RESCAN 不应含其他字段,实际 %v", st)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, err := client.GetNodeStatus(ctx, &agentpb.GetNodeStatusRequest{Node: "worker-01", Item: tt.item})
			if err != nil {
				t.Fatalf("GetNodeStatus(item=%v): %v", tt.item, err)
			}
			if st.GetNode() != "worker-01" {
				t.Errorf("node = %q, want worker-01", st.GetNode())
			}
			tt.check(t, st)
		})
	}
}

func TestHealthCheckServing(t *testing.T) {
	conn := startServer(t)
	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(),
		&healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("health Check: %v", err)
	}
	if got := resp.GetStatus(); got != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health = %v, want SERVING(design §8:liveness = gRPC health check)", got)
	}
}

// TestReflectionListServices 验证服务端注册了 reflection(M1 判据:原生 grpcurl
// 不带 -proto 时必须能列出服务)。用 reflection v1 客户端发一次 ListServices 请求。
func TestReflectionListServices(t *testing.T) {
	conn := startServer(t)

	stream, err := grpc_reflection_v1.NewServerReflectionClient(conn).
		ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerReflectionInfo: %v(server 未注册 reflection?)", err)
	}
	if err := stream.Send(&grpc_reflection_v1.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1.ServerReflectionRequest_ListServices{ListServices: "*"},
	}); err != nil {
		t.Fatalf("Send(ListServices): %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(ListServices): %v", err)
	}

	got := make(map[string]bool)
	for _, svc := range resp.GetListServicesResponse().GetService() {
		got[svc.GetName()] = true
	}
	for _, want := range []string{
		"traincache.agent.v1.PreheatService",
		"traincache.agent.v1.NodeService",
		"traincache.agent.v1.ReportService",
		"grpc.health.v1.Health",
	} {
		if !got[want] {
			t.Errorf("ListServices 缺 %q,实际 = %v", want, got)
		}
	}
}

// TestReportBlocksUnimplemented:北向流服务端推迟至 Controller 里程碑(decisions Q1)。
func TestReportBlocksUnimplemented(t *testing.T) {
	conn := startServer(t)
	stream, err := agentpb.NewReportServiceClient(conn).ReportBlocks(context.Background())
	if err != nil {
		t.Fatalf("ReportBlocks: %v", err)
	}
	// 客户端流:关闭并等待响应,取服务端状态(Unimplemented)。
	_, err = stream.CloseAndRecv()
	assertUnimplemented(t, err)
}

// TestSubmitPreheatRPC:经 bufconn 全链路提交(本地命中块)→ GetPreheatStatus 进度到 DONE。
func TestSubmitPreheatRPC(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "s-000.tar", chunk: 0, content: "rpc-s0", local: true},
		{path: "s-001.tar", chunk: 0, content: "rpc-s1", local: true},
	})
	fx.preloadMapping(t, testDataset) // 本地命中块零下载(映射已加载)
	conn := startServerWith(t, fx.agent())
	ctx := context.Background()

	pc := agentpb.NewPreheatServiceClient(conn)
	resp, err := pc.SubmitPreheat(ctx, validSubmitReq())
	if err != nil {
		t.Fatalf("SubmitPreheat: %v", err)
	}
	if !resp.GetAccepted() || resp.GetTaskId() != "task-1" {
		t.Fatalf("SubmitPreheat 响应 = %+v, want accepted=true task-1", resp)
	}

	deadline := time.Now().Add(testTimeout)
	for {
		st, err := pc.GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{TaskId: "task-1"})
		if err != nil {
			t.Fatalf("GetPreheatStatus: %v", err)
		}
		if st.GetState() == agentpb.PreheatStatus_DONE {
			if st.GetTotalBlocks() != 2 || st.GetCompletedBlocks() != 2 {
				t.Errorf("进度 = %d/%d, want 2/2", st.GetCompletedBlocks(), st.GetTotalBlocks())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("任务超时未 DONE: %v", st)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestSubmitPreheatRPCInvalidNode:node 不符 → InvalidArgument(design §5.1)。
func TestSubmitPreheatRPCInvalidNode(t *testing.T) {
	conn := startServer(t)
	req := validSubmitReq()
	req.Node = "other-node"
	_, err := agentpb.NewPreheatServiceClient(conn).SubmitPreheat(context.Background(), req)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument(err=%v)", got, err)
	}
}

// TestGetPreheatStatusRPCNotFound:未知 task_id → NotFound。
func TestGetPreheatStatusRPCNotFound(t *testing.T) {
	conn := startServer(t)
	_, err := agentpb.NewPreheatServiceClient(conn).GetPreheatStatus(context.Background(),
		&agentpb.GetPreheatStatusRequest{TaskId: "no-such-task"})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v, want NotFound(err=%v)", got, err)
	}
}

// TestListSnapshotRPC:条目映射 (path, chunk, size) 与 (path, chunk) 字典序。
func TestListSnapshotRPC(t *testing.T) {
	fx := newSvcFixture(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "b-000.tar", chunk: 0, content: "rpc-b0", local: true},
		{path: "a-001.tar", chunk: 2, content: "rpc-a1", local: true},
		{path: "a-000.tar", chunk: 0, content: "rpc-a0", local: true},
	})
	conn := startServerWith(t, fx.agent())

	resp, err := agentpb.NewNodeServiceClient(conn).ListSnapshot(context.Background(),
		&agentpb.ListSnapshotRequest{Dataset: testDataset})
	if err != nil {
		t.Fatalf("ListSnapshot: %v", err)
	}
	want := []struct {
		path  string
		chunk int64
		size  int64
	}{
		{"a-000.tar", 0, int64(len("rpc-a0"))},
		{"a-001.tar", 2, int64(len("rpc-a1"))},
		{"b-000.tar", 0, int64(len("rpc-b0"))},
	}
	if len(resp.GetEntries()) != len(want) {
		t.Fatalf("entries 数 = %d, want %d", len(resp.GetEntries()), len(want))
	}
	for i, w := range want {
		e := resp.GetEntries()[i]
		if e.GetPath() != w.path || e.GetChunk() != w.chunk || e.GetSize() != w.size {
			t.Errorf("entries[%d] = (%s, %d, %d), want (%s, %d, %d)",
				i, e.GetPath(), e.GetChunk(), e.GetSize(), w.path, w.chunk, w.size)
		}
	}
}

// TestGetNodeStatusRunningTasks:running_tasks 取任务表实际值(QUEUED/RUNNING 计数)。
func TestGetNodeStatusRunningTasks(t *testing.T) {
	fx := newSvcFixture(t)
	tk := newPreheatTask("t-run", "worker-01", preheatRequest{dataset: testDataset})
	tk.state.Store(int32(agentpb.PreheatStatus_QUEUED))
	fx.tasks.mu.Lock()
	fx.tasks.tasks[tk.taskID] = tk
	fx.tasks.mu.Unlock()

	conn := startServerWith(t, fx.agent())
	client := agentpb.NewNodeServiceClient(conn)

	st, err := client.GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "worker-01", Item: agentpb.NodeStatusItem_TASKS})
	if err != nil {
		t.Fatalf("GetNodeStatus(TASKS): %v", err)
	}
	if st.GetRunningTasks() != 1 {
		t.Errorf("running_tasks = %d, want 1(QUEUED 计入)", st.GetRunningTasks())
	}

	st, err = client.GetNodeStatus(context.Background(), &agentpb.GetNodeStatusRequest{Node: "worker-01"})
	if err != nil {
		t.Fatalf("GetNodeStatus(全量): %v", err)
	}
	if st.GetRunningTasks() != 1 {
		t.Errorf("全量 running_tasks = %d, want 1", st.GetRunningTasks())
	}
}

func assertUnimplemented(t *testing.T, err error) {
	t.Helper()
	if got := status.Code(err); got != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented(err=%v)", got, err)
	}
}
