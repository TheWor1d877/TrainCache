// Package grpcmeta 把 Controller 经 gRPC metadata 传来的关联键写进 Agent 的
// context logger（docs/8.1 §5 第 4 跳）。
package grpcmeta

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"traincache/agent/internal/logging"
	corelog "traincache/core/logging"
)

// UnaryServerInterceptor 解析三个关联键并补充到 ctx 的 logger 上。缺键或
// generation 无法解析时直接跳过该字段，不影响 RPC 本身。
func UnaryServerInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		log := logging.From(ctx)
		changed := false
		if values := md.Get(corelog.MetadataKeyRequestID); len(values) > 0 && values[0] != "" {
			log = log.With("request_id", values[0])
			changed = true
		}
		if values := md.Get(corelog.MetadataKeyTask); len(values) > 0 && values[0] != "" {
			log = log.With("task_id", values[0])
			changed = true
		}
		if values := md.Get(corelog.MetadataKeyGeneration); len(values) > 0 && values[0] != "" {
			if generation, err := strconv.ParseUint(values[0], 10, 64); err == nil && generation != 0 {
				log = log.With("generation", generation)
				changed = true
			}
		}
		if changed {
			ctx = logging.Into(ctx, log)
		}
	}
	return handler(ctx, req)
}
