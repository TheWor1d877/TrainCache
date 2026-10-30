package logging

import (
	"context"
	"errors"
)

// 错误分类枚举(docs/8.1 design.md §3.1 冻结,只增不改)。
//
// err 给人看,err_class 给聚合与告警;两者必须成对出现。各层特有错误
// (cas/objectstore/agentclient)在各自包内先映射,再回退到这里的通用分类,
// core 不反向依赖上层包。
const (
	ErrClassInvalidSpec = "invalid_spec"
	ErrClassNotFound    = "not_found"
	ErrClassUnavailable = "unavailable"
	ErrClassPermanent   = "permanent"
	ErrClassCorrupt     = "corrupt"
	ErrClassIO          = "io"
	ErrClassTimeout     = "timeout"
)

// Classify 把错误归到通用分类:超时 → timeout,取消 → unavailable,
// 其余(含本地与网络 I/O)一律 io。err 为 nil 时返回空串,调用方不应写
// err_class。
func Classify(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return ErrClassTimeout
	case errors.Is(err, context.Canceled):
		return ErrClassUnavailable
	default:
		return ErrClassIO
	}
}
