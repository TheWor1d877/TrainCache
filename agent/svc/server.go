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

// Server 实现 design §5.1 的三个 gRPC 服务。M1 骨架:health 与 GetNodeStatus
// 静态字段可用;预热任务(M3)、ListSnapshot(M3)、北向上报(Controller 里程碑)未实现。
type Server struct {
	agentpb.UnimplementedPreheatServiceServer
	agentpb.UnimplementedNodeServiceServer
	agentpb.UnimplementedReportServiceServer

	cfg    Config
	health *health.Server
}

// NewServer 构造服务并把 health 置为 SERVING(design §8:liveness = gRPC health check)。
func NewServer(cfg Config) *Server {
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	return &Server{cfg: cfg, health: h}
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
			RunningTasks: 0, // 静态 0;M3 统计运行中预热任务
			Rescan:       rescanDONE(),
			Catalog:      nil, // 静态空;M3 填充数据集目录
		}, nil
	case agentpb.NodeStatusItem_AGENT:
		return &agentpb.NodeStatus{Node: node, AgentOk: true}, nil
	case agentpb.NodeStatusItem_CATALOG:
		return &agentpb.NodeStatus{Node: node}, nil // catalog 静态为空
	case agentpb.NodeStatusItem_SPACE:
		return &agentpb.NodeStatus{Node: node, CacheRoot: s.cfg.CacheRoot}, nil
	case agentpb.NodeStatusItem_TASKS:
		return &agentpb.NodeStatus{Node: node}, nil // running_tasks 静态 0
	case agentpb.NodeStatusItem_RESCAN:
		return &agentpb.NodeStatus{Node: node, Rescan: rescanDONE()}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument,
			"item %d 未知(仅支持 全量/AGENT/CATALOG/SPACE/TASKS/RESCAN)", req.GetItem())
	}
}

// rescanDONE 返回 M1 静态重扫描状态。必须显式 DONE:枚举零值是 SAMPLING,
// 而 design §8 readiness = rescan.phase != SAMPLING。
func rescanDONE() *agentpb.RescanStatus {
	return &agentpb.RescanStatus{Phase: agentpb.RescanStatus_DONE}
}

// SubmitPreheat 未实现(里程碑 M3:四优先级预取队列 + 预热任务全链路)。
func (s *Server) SubmitPreheat(_ context.Context, _ *agentpb.SubmitPreheatRequest) (*agentpb.SubmitPreheatResponse, error) {
	return nil, status.Error(codes.Unimplemented, "SubmitPreheat 未实现(里程碑 M3)")
}

// GetPreheatStatus 未实现(里程碑 M3)。
func (s *Server) GetPreheatStatus(_ context.Context, _ *agentpb.GetPreheatStatusRequest) (*agentpb.PreheatStatus, error) {
	return nil, status.Error(codes.Unimplemented, "GetPreheatStatus 未实现(里程碑 M3)")
}

// ListSnapshot 未实现(里程碑 M3:复用 core Catalog)。
func (s *Server) ListSnapshot(_ context.Context, _ *agentpb.ListSnapshotRequest) (*agentpb.ListSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListSnapshot 未实现(里程碑 M3)")
}

// ReportBlocks 未实现(decisions Q1:北向流服务端实现推迟至 Controller 里程碑)。
func (s *Server) ReportBlocks(_ agentpb.ReportService_ReportBlocksServer) error {
	return status.Error(codes.Unimplemented, "ReportBlocks 未实现(Controller 里程碑,decisions Q1)")
}
