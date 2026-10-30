package agentclient

// errclass.go:把 Agent RPC 错误映射到日志 err_class 枚举(docs/8.1 §3.1)。
// 分类只用于日志属性,不参与重试或状态决策。

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corelog "traincache/core/logging"
)

// ErrClass 把 call 返回的错误映射到固定枚举:超时 → timeout,Agent 不可达 →
// unavailable,任务不存在 → not_found,协议拒绝 → permanent;其余回退 core 的
// 通用分类。err 为 nil 时返回空串。
func ErrClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return corelog.ErrClassTimeout
	case status.Code(err) == codes.DeadlineExceeded:
		return corelog.ErrClassTimeout
	case errors.Is(err, ErrAgentUnavailable):
		return corelog.ErrClassUnavailable
	case errors.Is(err, ErrTaskNotFound):
		return corelog.ErrClassNotFound
	case errors.Is(err, ErrPermanent):
		return corelog.ErrClassPermanent
	}
	return corelog.Classify(err)
}
