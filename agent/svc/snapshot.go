package svc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/metrics"
	"traincache/core/index"
)

// ListSnapshot 枚举 dataset 当前快照的条目(§5.6 CLI --chunks 名字解析;§7.2):dataset
// 空 → InvalidArgument;snapshot_id 非空 → InvalidArgument(M3 仅支持 CURRENT);prefix
// 任意过滤(空 = 全量,字面语义同 core Catalog.List)。条目取自 Catalog.List(D29):
// 只下载相关分片、不切换内存映射,按 (path, chunk) 字典序返回;Size 为块字节数。
//
// dataset 尚无 CURRENT(ErrNoCurrent)→ 空 entries 且无错误(裁决:与"前缀无匹配返回空"
// 同语义,CLI 据此报"无可解析条目";dataset 名等校验错误语义不受影响)。
// 延迟线:入口 defer 打点(observeRequest)。
func (s *Server) ListSnapshot(ctx context.Context, req *agentpb.ListSnapshotRequest) (resp *agentpb.ListSnapshotResponse, err error) {
	start := time.Now()
	defer func() { s.observeRequest(metrics.EndpointListSnapshot, start, err) }()
	dataset := req.GetDataset()
	if dataset == "" {
		return nil, status.Error(codes.InvalidArgument, "dataset 必填")
	}
	if req.GetSnapshotId() != "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"snapshot_id %q 不支持(M3 仅支持 CURRENT,须留空)", req.GetSnapshotId())
	}
	if s.ag == nil || s.ag.Prefetch == nil {
		return nil, status.Error(codes.Unavailable, "预取管理器未装配")
	}

	entries, err := s.ag.Prefetch.ForListSnapshot(ctx, dataset, req.GetPrefix())
	if err != nil {
		if errors.Is(err, index.ErrNoCurrent) {
			return &agentpb.ListSnapshotResponse{}, nil // 尚无 CURRENT:空列表无错误(裁决)
		}
		return nil, status.Errorf(codes.Internal, "ListSnapshot(%q, %q): %v", dataset, req.GetPrefix(), err)
	}

	resp = &agentpb.ListSnapshotResponse{Entries: make([]*agentpb.EntryRef, 0, len(entries))}
	for _, e := range entries {
		resp.Entries = append(resp.Entries, &agentpb.EntryRef{Path: e.Path, Chunk: e.Chunk, Size: e.Size})
	}
	return resp, nil
}
