package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// probeKeyPrefix 是探测临时键前缀；探测结束会尽力删除该对象。
const probeKeyPrefix = "traincache-probe/conditional-write-"

// ProbeConditionalWrite 探测 Store 后端是否真正执行条件写（design.md 7.1 §5.12）：
// 写入一个临时键，再用故意不匹配的 ETag 调用 PutIfMatch。
//
//   - 返回 ErrConflict：后端支持条件写，返回 nil；
//   - 返回 nil：后端忽略了前置条件，返回 ErrConditionalWriteUnsupported；
//   - 其它错误：原样返回。
//
// 无论结果如何，结束时都会尽力删除临时键（删除错误忽略）。
func ProbeConditionalWrite(ctx context.Context, s Store) error {
	var r [8]byte
	if _, err := rand.Read(r[:]); err != nil {
		return fmt.Errorf("objectstore: probe conditional write: generate key: %w", err)
	}
	key := probeKeyPrefix + hex.EncodeToString(r[:])

	content := []byte("traincache conditional write probe")
	etag, err := s.Put(ctx, key, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return fmt.Errorf("objectstore: probe conditional write: put temp object: %w", err)
	}
	defer func() {
		_ = s.Delete(ctx, key)
	}()

	// 构造一个确定不同于当前 ETag 的期望值；32 位 hex 与 S3 ETag 形式兼容。
	mismatch := strings.Repeat("0", 32)
	if etag == mismatch {
		mismatch = strings.Repeat("1", 32)
	}
	mismatchContent := []byte("traincache conditional write probe mismatch")
	_, err = s.PutIfMatch(ctx, key, bytes.NewReader(mismatchContent), int64(len(mismatchContent)), mismatch)
	switch {
	case err == nil:
		return ErrConditionalWriteUnsupported
	case errors.Is(err, ErrConflict):
		return nil
	default:
		return err
	}
}
