package objectstore

import (
	"context"
	"io"
	"net/http"
	"sort"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Store 是基于 minio-go SDK 的 Store 实现，支持任何 S3 兼容对象存储
// （腾讯云 COS、阿里云 OSS、AWS S3、本地 MinIO 等）。
type S3Store struct {
	client *minio.Client
	bucket string
}

var _ Store = (*S3Store)(nil)

// NewS3Store 构造任意 S3 兼容后端的 Store。endpoint 形如
// "127.0.0.1:9000" 或 "cos.ap-guangzhou.myqcloud.com"，secure 为 true 时使用 HTTPS。
// bucket 为对象存储桶名（v1 约定单桶，见 decisions.md Q5）。
func NewS3Store(endpoint, accessKey, secretKey, bucket string, secure bool) (*S3Store, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}
	return &S3Store{client: client, bucket: bucket}, nil
}

// Get 实现 Store.Get（GET 前先 Stat 确认存在性，不存在返回 ErrNotFound）。
func (s *S3Store) Get(ctx context.Context, key string, w io.Writer) error {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		if isNoSuchObject(err) {
			return ErrNotFound
		}
		return err
	}
	defer obj.Close()
	if _, err := obj.Stat(); err != nil {
		if isNoSuchObject(err) {
			return ErrNotFound
		}
		return err
	}
	if _, err := io.Copy(w, obj); err != nil {
		return err
	}
	return nil
}

// Put 实现 Store.Put（简单上传语义，ETag 由服务端返回）。
func (s *S3Store) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	info, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

// Delete 实现 Store.Delete（S3 DeleteObject 幂等，对象不存在不报错）。
func (s *S3Store) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// GetETag 实现 Store.GetETag。
func (s *S3Store) GetETag(ctx context.Context, key string) (string, error) {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if isNoSuchObject(err) {
			return "", ErrNotFound
		}
		return "", err
	}
	return info.ETag, nil
}

// PutIfMatch 实现 Store.PutIfMatch：使用 minio-go 的条件写入支持
// （PutObjectOptions.SetMatchETag / SetMatchETagExcept，即 HTTP If-Match / If-None-Match 头）。
// 条件不满足时服务端返回 412 PreconditionFailed（If-Match 目标不存在时为 404 NoSuchKey），
// 统一映射为 ErrConflict。
func (s *S3Store) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	opts := minio.PutObjectOptions{}
	if expectedETag == "" {
		// If-None-Match: * —— 要求 key 不存在。
		opts.SetMatchETagExcept("*")
	} else {
		// If-Match: "<etag>" —— 要求当前 ETag 相等。
		opts.SetMatchETag(expectedETag)
	}
	info, err := s.client.PutObject(ctx, s.bucket, key, r, size, opts)
	if err != nil {
		if isPreconditionConflict(err) {
			return "", ErrConflict
		}
		return "", err
	}
	return info.ETag, nil
}

// List 实现 Store.List：minio-go v7 的 ListObjects 以 channel 流式返回结果
// （单条错误以 ObjectInfo.Err 形式送达，ctx 取消同样走该路径）；Recursive: true
// 使 prefix 下全部分层对象一次枚举完。返回键按字典序升序（S3 列表本身有序，此处
// 排序兜底以严格满足接口约定）。
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	keys := make([]string, 0)
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	for obj := range s.client.ListObjects(ctx, s.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	sort.Strings(keys)
	return keys, nil
}

// isNoSuchObject 判断错误是否表示对象不存在。
// GET/HEAD 请求的 404 均可视为对象不存在（服务端 XML 错误码通常为 NoSuchKey）。
func isNoSuchObject(err error) bool {
	resp := minio.ToErrorResponse(err)
	switch resp.Code {
	case minio.NoSuchKey, "NoSuchObject", "NotFound":
		return true
	}
	return resp.StatusCode == http.StatusNotFound
}

// isPreconditionConflict 判断错误是否是条件写入前置条件不满足。
// 412 PreconditionFailed：ETag 不匹配或 If-None-Match: * 命中已存在对象；
// 404 NoSuchKey：If-Match 指向不存在的对象（S3 语义，见 minio-go 功能测试）；
// 409 Conflict：并发条件写入冲突。
func isPreconditionConflict(err error) bool {
	resp := minio.ToErrorResponse(err)
	switch resp.Code {
	case minio.PreconditionFailed, minio.NoSuchKey, minio.Conflict:
		return true
	}
	switch resp.StatusCode {
	case http.StatusPreconditionFailed, http.StatusConflict:
		return true
	}
	return false
}
