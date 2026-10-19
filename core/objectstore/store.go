// Package objectstore 提供对 S3 兼容对象存储的最小抽象（design.md §5.1）。
//
// 实现：
//   - minio.go：minio-go 后端（条件写入使用 SDK 的 If-Match / If-None-Match 支持）；
//   - mock.go：内存 Mock（单测用，ETag = md5hex(内容)）。
package objectstore

import (
	"context"
	"errors"
	"io"
)

var (
	ErrNotFound = errors.New("objectstore: object not found")
	ErrConflict = errors.New("objectstore: conditional write conflict")
	// ErrConditionalWriteUnsupported 表示后端忽略了条件写前置条件（PutIfMatch
	// 在 ETag 不匹配时仍然成功）。可用 ProbeConditionalWrite 探测。
	ErrConditionalWriteUnsupported = errors.New("objectstore: conditional write unsupported")
)

// Store 是对 S3 兼容对象存储的最小抽象。
type Store interface {
	// Get 将对象内容完整写入 w；不存在返回 ErrNotFound。
	Get(ctx context.Context, key string, w io.Writer) error
	// Put 上传对象，返回服务端 ETag。对相同内容 ETag 必须稳定（S3 简单上传语义）。
	Put(ctx context.Context, key string, r io.Reader, size int64) (etag string, err error)
	// Delete 删除对象；对象不存在不算错误。
	Delete(ctx context.Context, key string) error
	// GetETag 返回对象当前 ETag；不存在返回 ErrNotFound。
	GetETag(ctx context.Context, key string) (string, error)
	// PutIfMatch 条件写入：expectedETag 为空串时要求 key 不存在（If-None-Match: *），
	// 否则要求当前 ETag == expectedETag（If-Match）。冲突返回 ErrConflict，不写入。
	PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (newETag string, err error)
	// List 返回匹配 prefix 的全部对象键（字典序升序）；无匹配返回空切片（D19）。
	// 供 index.Collector.Sweep 枚举快照对象用。
	List(ctx context.Context, prefix string) ([]string, error)
}
