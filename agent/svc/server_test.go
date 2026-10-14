package svc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/nodeid"
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

// startServer 在 bufconn 内存网络上起全套服务(health + 三业务服务)。
func startServer(t *testing.T) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(testConfig()).Register(gs)
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

func TestGetNodeStatusStaticFields(t *testing.T) {
	conn := startServer(t)
	st, err := agentpb.NewNodeServiceClient(conn).GetNodeStatus(context.Background(),
		&agentpb.GetNodeStatusRequest{Node: "worker-01"})
	if err != nil {
		t.Fatalf("GetNodeStatus: %v", err)
	}

	if st.GetNode() != "worker-01" {
		t.Errorf("node = %q, want worker-01", st.GetNode())
	}
	if !st.GetAgentOk() {
		t.Error("agent_ok = false, want true")
	}
	if st.GetCacheRoot() != "/var/cache/traincache" {
		t.Errorf("cache_root = %q, want /var/cache/traincache", st.GetCacheRoot())
	}
	if st.GetCachedBytes() != 0 {
		t.Errorf("cached_bytes = %d, want 0(M1 静态)", st.GetCachedBytes())
	}
	if st.GetFreeBytes() != 0 {
		t.Errorf("free_bytes = %d, want 0(M1 静态)", st.GetFreeBytes())
	}
	if st.GetRunningTasks() != 0 {
		t.Errorf("running_tasks = %d, want 0(M1 静态)", st.GetRunningTasks())
	}
	// 必须显式 DONE:枚举零值是 SAMPLING,readiness = phase != SAMPLING(design §8)。
	if got := st.GetRescan().GetPhase(); got != agentpb.RescanStatus_DONE {
		t.Errorf("rescan.phase = %v, want DONE(M1 静态;SAMPLING 会被 readiness 判为未就绪)", got)
	}
	if len(st.GetCatalog()) != 0 {
		t.Errorf("catalog = %v, want 空(M1 静态)", st.GetCatalog())
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

func TestUnimplementedRPCs(t *testing.T) {
	conn := startServer(t)
	ctx := context.Background()

	t.Run("SubmitPreheat", func(t *testing.T) {
		_, err := agentpb.NewPreheatServiceClient(conn).SubmitPreheat(ctx, &agentpb.SubmitPreheatRequest{Node: "worker-01"})
		assertUnimplemented(t, err)
	})
	t.Run("GetPreheatStatus", func(t *testing.T) {
		_, err := agentpb.NewPreheatServiceClient(conn).GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{TaskId: "task-1"})
		assertUnimplemented(t, err)
	})
	t.Run("ListSnapshot", func(t *testing.T) {
		_, err := agentpb.NewNodeServiceClient(conn).ListSnapshot(ctx, &agentpb.ListSnapshotRequest{Dataset: "b-track"})
		assertUnimplemented(t, err)
	})
	t.Run("ReportBlocks", func(t *testing.T) {
		stream, err := agentpb.NewReportServiceClient(conn).ReportBlocks(ctx)
		if err != nil {
			t.Fatalf("ReportBlocks: %v", err)
		}
		// 客户端流:关闭并等待响应,取服务端状态(Unimplemented)。
		_, err = stream.CloseAndRecv()
		assertUnimplemented(t, err)
	})
}

func assertUnimplemented(t *testing.T, err error) {
	t.Helper()
	if got := status.Code(err); got != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented(err=%v)", got, err)
	}
}
