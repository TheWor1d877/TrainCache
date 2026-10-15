package svc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
)

// Server 实现 design §5.1 的三个 gRPC 服务。M3:预热任务、GetPreheatStatus、ListSnapshot
// 与 GetNodeStatus.running_tasks 接入业务组件(ag);北向上报(Controller 里程碑)未实现。
type Server struct {
	agentpb.UnimplementedPreheatServiceServer
	agentpb.UnimplementedNodeServiceServer
	agentpb.UnimplementedReportServiceServer

	cfg    Config
	ag     *Agent
	health *health.Server
}

// NewServer 构造服务并把 health 置为 SERVING(design §8:liveness = gRPC health check)。
// ag 提供业务组件(预热任务表/预取管理器);nil 时业务 RPC 返回 Unavailable(仅测试骨架用)。
func NewServer(cfg Config, ag *Agent) *Server {
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	return &Server{cfg: cfg, ag: ag, health: h}
}

// Register 注册三个业务服务、health 与 gRPC reflection 服务。
// reflection 使用 grpc 模块自带子包,零新依赖(M1 判据:原生 grpcurl 不带 -proto 可用)。
func (s *Server) Register(gs *grpc.Server) {
	agentpb.RegisterPreheatServiceServer(gs, s)
	agentpb.RegisterNodeServiceServer(gs, s)
	agentpb.RegisterReportServiceServer(gs, s)
	healthpb.RegisterHealthServer(gs, s.health)
	reflection.Register(gs)
}

// GetNodeStatus 返回节点状态(M1 为静态字段;后续里程碑填充运行值)。
// node 为空 → 查本机;node 与 NODE_NAME 不符 → InvalidArgument(design §5.1 错误语义)。
// item 过滤:UNSPECIFIED 全量,其余仅返回请求字段,未请求字段置零。
func (s *Server) GetNodeStatus(_ context.Context, req *agentpb.GetNodeStatusRequest) (*agentpb.NodeStatus, error) {
	node := s.cfg.NodeName.String()
	if req.GetNode() != "" && req.GetNode() != node {
		return nil, status.Errorf(codes.InvalidArgument,
			"node %q 与本节点 %q 不符(design §5.1:节点校验失败 → InvalidArgument)", req.GetNode(), node)
	}

	switch req.GetItem() {
	case agentpb.NodeStatusItem_UNSPECIFIED:
		return &agentpb.NodeStatus{
			Node:         node,
			AgentOk:      true,
			CacheRoot:    s.cfg.CacheRoot,
			CachedBytes:  0, // 静态 0;M4 统计 cas 目录用量
			FreeBytes:    0, // 静态 0;M4 统计磁盘剩余
			RunningTasks: s.runningTasks(),
			Rescan:       rescanDONE(),
			Catalog:      nil, // 静态空;数据集目录属后续里程碑
		}, nil
	case agentpb.NodeStatusItem_AGENT:
		return &agentpb.NodeStatus{Node: node, AgentOk: true}, nil
	case agentpb.NodeStatusItem_CATALOG:
		return &agentpb.NodeStatus{Node: node}, nil // catalog 静态为空
	case agentpb.NodeStatusItem_SPACE:
		return &agentpb.NodeStatus{Node: node, CacheRoot: s.cfg.CacheRoot}, nil
	case agentpb.NodeStatusItem_TASKS:
		return &agentpb.NodeStatus{Node: node, RunningTasks: s.runningTasks()}, nil
	case agentpb.NodeStatusItem_RESCAN:
		return &agentpb.NodeStatus{Node: node, Rescan: rescanDONE()}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument,
			"item %d 未知(仅支持 全量/AGENT/CATALOG/SPACE/TASKS/RESCAN)", req.GetItem())
	}
}

// runningTasks 返回运行中预热任务数(任务表未装配时 0)。
func (s *Server) runningTasks() int64 {
	if s.ag == nil || s.ag.Preheat == nil {
		return 0
	}
	return s.ag.Preheat.RunningCount()
}

// rescanDONE 返回 M1 静态重扫描状态。必须显式 DONE:枚举零值是 SAMPLING,
// 而 design §8 readiness = rescan.phase != SAMPLING。
func rescanDONE() *agentpb.RescanStatus {
	return &agentpb.RescanStatus{Phase: agentpb.RescanStatus_DONE}
}

// SubmitPreheat 下发预热任务(§5.1/§7.2;校验顺序与状态机在 PreheatTasks)。
func (s *Server) SubmitPreheat(ctx context.Context, req *agentpb.SubmitPreheatRequest) (*agentpb.SubmitPreheatResponse, error) {
	if s.ag == nil || s.ag.Preheat == nil {
		return nil, status.Error(codes.Unavailable, "预热服务未装配")
	}
	return s.ag.Preheat.Submit(ctx, req)
}

// GetPreheatStatus 查询预热任务进度(§5.1;不存在 → NotFound)。
func (s *Server) GetPreheatStatus(ctx context.Context, req *agentpb.GetPreheatStatusRequest) (*agentpb.PreheatStatus, error) {
	if s.ag == nil || s.ag.Preheat == nil {
		return nil, status.Error(codes.Unavailable, "预热服务未装配")
	}
	return s.ag.Preheat.Status(ctx, req.GetTaskId())
}

// ReportBlocks 未实现(decisions Q1:北向流服务端实现推迟至 Controller 里程碑)。
func (s *Server) ReportBlocks(_ agentpb.ReportService_ReportBlocksServer) error {
	return status.Error(codes.Unimplemented, "ReportBlocks 未实现(Controller 里程碑,decisions Q1)")
}
