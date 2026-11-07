// Package obs 统一 checkpointd 的日志字段名、状态取值与错误字段对
// （design 10.1「可观测出口」；docs/8.1 §3.1）。
package obs

import (
	"log/slog"

	corelog "traincache/core/logging"
)

// 字段名（design 10.1「可观测出口」）。
const (
	FieldTaskID     = "task_id"
	FieldStep       = "step"
	FieldRank       = "rank"
	FieldJob        = "job"
	FieldPlugin     = "plugin"
	FieldBytes      = "bytes"
	FieldBytesTotal = "bytes_total"
	FieldRetry      = "retry"
	FieldState      = "state"
	FieldFile       = "file"
	FieldErr        = "err"
	FieldErrClass   = "err_class"
)

// State 是 step 与探针的状态取值（design 10.1「准入契约」、「可观测出口」）。
type State string

const (
	StateWaiting   State = "waiting"
	StateReady     State = "ready"
	StateRestoring State = "restoring"
	StateUploading State = "uploading"
	StateCached    State = "cached"
	StateDone      State = "done"
	StateFailed    State = "failed"
)

// Task / Step / Rank / Job / Plugin / Bytes / Retry / State 返回常用字段属性，
// 业务代码不直接写字段名，避免拼写漂移。
func Task(id string) slog.Attr { return slog.String(FieldTaskID, id) }
func Step(n int64) slog.Attr   { return slog.Int64(FieldStep, n) }
func Rank(n int) slog.Attr     { return slog.Int(FieldRank, n) }

// Job 返回 job 字段（docs/10.2 D26）。空串是合法取值：裸 Pod 或自定义 CR
// 直接建出的 Pod 没有 owner Job，字段仍要出现。
func Job(name string) slog.Attr    { return slog.String(FieldJob, name) }
func Plugin(name string) slog.Attr { return slog.String(FieldPlugin, name) }
func Bytes(n int64) slog.Attr      { return slog.Int64(FieldBytes, n) }
func Retry(n int) slog.Attr        { return slog.Int(FieldRetry, n) }
func StateAttr(s State) slog.Attr  { return slog.String(FieldState, string(s)) }

// InvalidSpec 用于配置或参数校验失败：err_class 固定为 invalid_spec
// （design 10.1「可观测出口」）。err 为 nil 时不产生字段。
func InvalidSpec(err error) []any {
	if err == nil {
		return nil
	}
	return []any{
		slog.Any(FieldErr, err),
		slog.String(FieldErrClass, corelog.ErrClassInvalidSpec),
	}
}

// Err 返回成对的 err 与 err_class 属性（docs/8.1 §3.1）。
// 错误路径必须成对写；err 为 nil 时不产生字段。
// 用法：log.Error("保存失败", obs.Err(err)...)
func Err(err error) []any {
	if err == nil {
		return nil
	}
	return []any{
		slog.Any(FieldErr, err),
		slog.String(FieldErrClass, corelog.Classify(err)),
	}
}
