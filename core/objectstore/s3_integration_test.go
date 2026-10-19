package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestS3ConditionalWriteIntegration 对真实 S3 兼容后端验证条件写语义。
// 需要环境变量 TC_S3_ENDPOINT / TC_S3_ACCESS_KEY / TC_S3_SECRET_KEY / TC_S3_BUCKET；
// 任一缺失则跳过（不计失败）。可以连任何 S3 兼容后端——本地开发可以起 MinIO 或 SeaweedFS，
// 真实测试可以直接连腾讯云 COS / 阿里云 OSS / AWS S3。
func TestS3ConditionalWriteIntegration(t *testing.T) {
	endpoint := os.Getenv("TC_S3_ENDPOINT")
	accessKey := os.Getenv("TC_S3_ACCESS_KEY")
	secretKey := os.Getenv("TC_S3_SECRET_KEY")
	bucket := os.Getenv("TC_S3_BUCKET")
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("未设置 TC_S3_ENDPOINT/TC_S3_ACCESS_KEY/TC_S3_SECRET_KEY/TC_S3_BUCKET，跳过真实后端集成测试")
	}

	ctx := context.Background()
	s, err := NewS3Store(endpoint, accessKey, secretKey, bucket, false)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}

	key := fmt.Sprintf("traincache-it/%d/conditional-write", time.Now().UTC().UnixNano())
	defer func() {
		_ = s.Delete(context.Background(), key)
	}()

	// 初始写入，取得当前 ETag。
	v1 := []byte("conditional-write-v1")
	etag1, err := s.Put(ctx, key, bytes.NewReader(v1), int64(len(v1)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// 正确 ETag 的条件写应成功。
	v2 := []byte("conditional-write-v2")
	if _, err := s.PutIfMatch(ctx, key, bytes.NewReader(v2), int64(len(v2)), etag1); err != nil {
		t.Fatalf("PutIfMatch(正确 ETag): %v", err)
	}

	// 过期 ETag 的条件写应返回 ErrConflict，且对象内容保持不变。
	v3 := []byte("conditional-write-v3")
	if _, err := s.PutIfMatch(ctx, key, bytes.NewReader(v3), int64(len(v3)), etag1); !errors.Is(err, ErrConflict) {
		t.Fatalf("PutIfMatch(过期 ETag) = %v，期望 ErrConflict", err)
	}
	var got bytes.Buffer
	if err := s.Get(ctx, key, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.Bytes(), v2) {
		t.Fatalf("冲突后内容 = %q，期望 %q", got.Bytes(), v2)
	}
}
