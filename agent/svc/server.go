package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/metrics"
	"traincache/agent/rescan"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// catalogTimeout 是 GetNodeStatus 读取目录(CURRENT/manifest/分片)的整体超时(接线层):
// 状态查询尽力而为,不得被对象存储拖死;超时后返回已完成部分。
const catalogTimeout = 3 * time.Second

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

// observeRequest 记录一次 RPC 请求耗时(延迟线⑥:traincache_request_duration_seconds;
// 四个业务 RPC 入口 defer 调用,endpoint 取 agent.proto 方法名)。result 判定(裁决见
// DECISIONS 回传):返回 err == nil → success;任何非 nil 错误(含 InvalidArgument /
// NotFound 等客户端输入类错误)→ error——result 表达"请求是否成功完成",与回源/索引线
// "失败即 error"同口径。ag / Metrics 未装配(测试骨架)→ 不打点。
func (s *Server) observeRequest(endpoint string, start time.Time, err error) {
	if s.ag == nil || s.ag.Metrics == nil {
		return
	}
	result := metrics.ResultSuccess
	if err != nil {
		result = metrics.ResultError
	}
	s.ag.Metrics.ObserveRequestDuration(endpoint, result, time.Since(start).Seconds())
}

// GetNodeStatus 返回节点状态(M5 起为动态字段:目录用量/磁盘余量由本机文件系统实测,
// rescan 由重扫描器实时状态映射,catalog 由已知 dataset 索引汇总)。
// node 为空 → 查本机;node 与 NODE_NAME 不符 → InvalidArgument(design §5.1 错误语义)。
// item 过滤:UNSPECIFIED 全量,其余仅返回请求字段,未请求字段置零。
// 延迟线:入口 defer 打点(observeRequest)。
func (s *Server) GetNodeStatus(ctx context.Context, req *agentpb.GetNodeStatusRequest) (resp *agentpb.NodeStatus, err error) {
	start := time.Now()
	defer func() { s.observeRequest(metrics.EndpointGetNodeStatus, start, err) }()
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
			CachedBytes:  s.cachedBytes(),
			FreeBytes:    s.freeBytes(),
			RunningTasks: s.runningTasks(),
			Rescan:       s.rescanStatus(),
			Catalog:      s.catalogList(ctx),
		}, nil
	case agentpb.NodeStatusItem_AGENT:
		return &agentpb.NodeStatus{Node: node, AgentOk: true}, nil
	case agentpb.NodeStatusItem_CATALOG:
		return &agentpb.NodeStatus{Node: node, Catalog: s.catalogList(ctx)}, nil
	case agentpb.NodeStatusItem_SPACE:
		return &agentpb.NodeStatus{
			Node:        node,
			CacheRoot:   s.cfg.CacheRoot,
			CachedBytes: s.cachedBytes(),
			FreeBytes:   s.freeBytes(),
		}, nil
	case agentpb.NodeStatusItem_TASKS:
		return &agentpb.NodeStatus{Node: node, RunningTasks: s.runningTasks()}, nil
	case agentpb.NodeStatusItem_RESCAN:
		return &agentpb.NodeStatus{Node: node, Rescan: s.rescanStatus()}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument,
			"item %d 未知(仅支持 全量/AGENT/CATALOG/SPACE/TASKS/RESCAN)", req.GetItem())
	}
}

// cachedBytes 统计 CacheRoot 下本地缓存占用:blocks/ 与 quarantine/ 两个子树的常规文件
// 大小合计(walk;目录缺失/单项不可读跳过——状态查询尽力而为,不因局部错误失败)。
func (s *Server) cachedBytes() int64 {
	var total int64
	for _, sub := range []string{"blocks", "quarantine"} {
		root := filepath.Join(s.cfg.CacheRoot, sub)
		_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // 目录缺失/不可读:跳过(含整棵子树)
			}
			if d.Type().IsRegular() {
				if info, ierr := d.Info(); ierr == nil {
					total += info.Size()
				}
			}
			return nil
		})
	}
	return total
}

// freeBytes 返回 CacheRoot 所在文件系统的可用字节数(statfs Bavail×Bsize;statfs 失败 → 0)。
func (s *Server) freeBytes() int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.cfg.CacheRoot, &st); err != nil {
		return 0
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

// rescanStatus 返回重扫描实时状态(D35 映射):未装配重扫描器 → DONE(枚举零值 SAMPLING
// 会卡 readiness,design §8:readiness = rescan.phase != SAMPLING);未知阶段 → SAMPLING
// (零值安全,保守不误报就绪)。
func (s *Server) rescanStatus() *agentpb.RescanStatus {
	if s.ag == nil || s.ag.Rescan == nil {
		return &agentpb.RescanStatus{Phase: agentpb.RescanStatus_DONE}
	}
	st := s.ag.Rescan.Status()
	return &agentpb.RescanStatus{
		Phase:       rescanPhase(st.Phase),
		Sampled:     st.Sampled,
		Verified:    st.Verified,
		Quarantined: st.Quarantined,
		RepairQueue: st.RepairQueue,
	}
}

// rescanPhase 把 rescan 子模块阶段映射到 proto 枚举。
func rescanPhase(p rescan.Phase) agentpb.RescanStatus_Phase {
	switch p {
	case rescan.PhaseFullScan:
		return agentpb.RescanStatus_FULL_SCAN
	case rescan.PhaseDone:
		return agentpb.RescanStatus_DONE
	default:
		return agentpb.RescanStatus_SAMPLING // PhaseSampling 与未知阶段同映射
	}
}

// catalogList 汇总已知 dataset(已装配 bundle 集合)的目录信息:Dataset 恒填;
// SnapshotId 取自 CURRENT 内容首字段("<snapID> <manifestFP8>");Shards 由 manifest 解析
// 分片数;Entries 为该 dataset 索引全量条目数(Catalog.List)。任一读取失败相应字段留零,
// 不使 RPC 失败(尽力而为;整体限时 catalogTimeout)。
// 数据集来源口径:仅已装配 bundle(订单/预热/重扫描触达过的),不在状态 RPC 内做对象
// 存储全量枚举(避免状态查询触发大范围列举)。
func (s *Server) catalogList(ctx context.Context) []*agentpb.DatasetCatalog {
	if s.ag == nil || s.ag.Prefetch == nil || s.ag.ObjectStore == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, catalogTimeout)
	defer cancel()

	names := s.ag.Prefetch.bundleNames()
	out := make([]*agentpb.DatasetCatalog, 0, len(names))
	for _, ds := range names {
		if ctx.Err() != nil {
			break // 超时:返回已完成部分
		}
		item := &agentpb.DatasetCatalog{Dataset: ds}
		if snapID, ok := readCurrentSnapshot(ctx, s.ag.ObjectStore, ds); ok {
			item.SnapshotId = snapID
			item.Shards = readManifestShards(ctx, s.ag.ObjectStore, ds, snapID)
		}
		if entries, err := s.ag.Prefetch.ForListSnapshot(ctx, ds, ""); err == nil {
			item.Entries = int64(len(entries))
		}
		out = append(out, item)
	}
	return out
}

// readCurrentSnapshot 读取 dataset 的 CURRENT 对象并解析快照 ID(内容 "<snapID> <manifestFP8>";
// 对象缺失/格式不符 → false)。
func readCurrentSnapshot(ctx context.Context, store objectstore.Store, dataset string) (string, bool) {
	var buf bytes.Buffer
	if err := store.Get(ctx, index.CurrentKey(dataset), &buf); err != nil {
		return "", false
	}
	parts := strings.Split(strings.TrimSpace(buf.String()), " ")
	if len(parts) != 2 || parts[0] == "" {
		return "", false
	}
	return parts[0], true
}

// readManifestShards 读取并解析 manifest 返回分片数(读取/解析失败 → 0)。
func readManifestShards(ctx context.Context, store objectstore.Store, dataset, snapID string) int64 {
	var buf bytes.Buffer
	if err := store.Get(ctx, index.ManifestKey(dataset, snapID), &buf); err != nil {
		return 0
	}
	var m index.Manifest
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		return 0
	}
	return int64(len(m.Shards))
}

// runningTasks 返回运行中预热任务数(任务表未装配时 0)。
func (s *Server) runningTasks() int64 {
	if s.ag == nil || s.ag.Preheat == nil {
		return 0
	}
	return s.ag.Preheat.RunningCount()
}

// SubmitPreheat 下发预热任务(§5.1/§7.2;校验顺序与状态机在 PreheatTasks)。
// 延迟线:入口 defer 打点(observeRequest)。
func (s *Server) SubmitPreheat(ctx context.Context, req *agentpb.SubmitPreheatRequest) (resp *agentpb.SubmitPreheatResponse, err error) {
	start := time.Now()
	defer func() { s.observeRequest(metrics.EndpointSubmitPreheat, start, err) }()
	if s.ag == nil || s.ag.Preheat == nil {
		return nil, status.Error(codes.Unavailable, "预热服务未装配")
	}
	return s.ag.Preheat.Submit(ctx, req)
}

// GetPreheatStatus 查询预热任务进度(§5.1;不存在 → NotFound)。
// 延迟线:入口 defer 打点(observeRequest)。
func (s *Server) GetPreheatStatus(ctx context.Context, req *agentpb.GetPreheatStatusRequest) (resp *agentpb.PreheatStatus, err error) {
	start := time.Now()
	defer func() { s.observeRequest(metrics.EndpointGetPreheatStatus, start, err) }()
	if s.ag == nil || s.ag.Preheat == nil {
		return nil, status.Error(codes.Unavailable, "预热服务未装配")
	}
	return s.ag.Preheat.Status(ctx, req.GetTaskId())
}

// UpdatePreheat 全量更新预热窗口(8.x D55/D56;位序=本次 chunks 顺序)。
// 延迟线:入口 defer 打点(observeRequest)。
func (s *Server) UpdatePreheat(ctx context.Context, req *agentpb.UpdatePreheatRequest) (resp *agentpb.UpdatePreheatResponse, err error) {
	start := time.Now()
	defer func() { s.observeRequest(metrics.EndpointUpdatePreheat, start, err) }()
	if s.ag == nil || s.ag.Preheat == nil {
		return nil, status.Error(codes.Unavailable, "预热服务未装配")
	}
	return s.ag.Preheat.Update(ctx, req)
}

// CancelPreheat 取消预热任务(8.x D55 + D62;幂等),并撤回本任务尚未完成的下载。
// 延迟线:入口 defer 打点(observeRequest)。
func (s *Server) CancelPreheat(ctx context.Context, req *agentpb.CancelPreheatRequest) (resp *agentpb.CancelPreheatResponse, err error) {
	start := time.Now()
	defer func() { s.observeRequest(metrics.EndpointCancelPreheat, start, err) }()
	if s.ag == nil || s.ag.Preheat == nil {
		return nil, status.Error(codes.Unavailable, "预热服务未装配")
	}
	return s.ag.Preheat.Cancel(ctx, req)
}

// ReportBlocks 未实现(decisions Q1:北向流服务端实现推迟至 Controller 里程碑;8.x
// 不使用推送模型,D55)。
func (s *Server) ReportBlocks(_ agentpb.ReportService_ReportBlocksServer) error {
	return status.Error(codes.Unimplemented, "ReportBlocks 未实现(Controller 里程碑,decisions Q1)")
}
