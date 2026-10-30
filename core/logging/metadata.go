package logging

import "context"

// gRPC metadata 关联键（docs/8.1 §5 第 4 跳）。Controller 客户端注入，Agent
// 拦截器解析后写进 context logger。键名是运维契约，只能加不能改。
const (
	MetadataKeyRequestID  = "x-traincache-request-id"
	MetadataKeyTask       = "x-traincache-task"
	MetadataKeyGeneration = "x-traincache-generation"
)

// RequestMeta 是一次调用携带的请求身份。Generation 为 0 表示未知（Kubernetes
// 的 generation 从 1 起）。
type RequestMeta struct {
	RequestID  string
	TaskID     string
	Generation uint64
}

type requestMetaKey struct{}

// WithRequestMeta 把请求身份写进 ctx。空字段沿用 ctx 里已有的值，便于分层
// 补充：reconcile 先放 request_id，下发 RPC 前再补 task 与 generation。
func WithRequestMeta(ctx context.Context, requestID, taskID string, generation uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	meta := RequestMetaFrom(ctx)
	if requestID != "" {
		meta.RequestID = requestID
	}
	if taskID != "" {
		meta.TaskID = taskID
	}
	if generation != 0 {
		meta.Generation = generation
	}
	return context.WithValue(ctx, requestMetaKey{}, meta)
}

// RequestMetaFrom 返回 ctx 里的请求身份，没有时返回零值。
func RequestMetaFrom(ctx context.Context) RequestMeta {
	if ctx == nil {
		return RequestMeta{}
	}
	meta, _ := ctx.Value(requestMetaKey{}).(RequestMeta)
	return meta
}
