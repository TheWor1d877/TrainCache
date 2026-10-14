package objectstore

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"sync"
)

// Mock 是 Store 的内存实现，用于单元测试（decisions.md D12）。
//
// ETag 语义与 S3 简单上传一致：ETag = md5hex(内容)，相同内容 ETag 稳定。
// 方法可并发调用；PutIfMatch 的“比较 + 写入”在同一把锁内完成，可模拟真实竞争。
// 零值 Mock 可直接使用（等价于空存储），也可经 NewMock 构造。
type Mock struct {
	mu   sync.RWMutex
	objs map[string][]byte
}

var _ Store = (*Mock)(nil)

// NewMock 创建空的内存 Mock 存储。
func NewMock() *Mock {
	return &Mock{objs: make(map[string][]byte)}
}

// md5hex 返回内容 md5 的小写 hex 编码（S3 简单上传 ETag 语义）。
func md5hex(content []byte) string {
	sum := md5.Sum(content)
	return hex.EncodeToString(sum[:])
}

// Get 实现 Store.Get。
func (m *Mock) Get(ctx context.Context, key string, w io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	content, ok := m.objs[key]
	m.mu.RUnlock()
	if !ok {
		return ErrNotFound
	}
	_, err := w.Write(content)
	return err
}

// Put 实现 Store.Put；size 仅作 S3 语义提示，内容以 r 读至 EOF 为准。
func (m *Mock) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	content, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	etag := md5hex(content)
	m.mu.Lock()
	if m.objs == nil {
		m.objs = make(map[string][]byte)
	}
	m.objs[key] = content
	m.mu.Unlock()
	return etag, nil
}

// Delete 实现 Store.Delete；对象不存在不算错误。
func (m *Mock) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.objs, key)
	m.mu.Unlock()
	return nil
}

// GetETag 实现 Store.GetETag。
func (m *Mock) GetETag(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.RLock()
	content, ok := m.objs[key]
	m.mu.RUnlock()
	if !ok {
		return "", ErrNotFound
	}
	return md5hex(content), nil
}

// PutIfMatch 实现 Store.PutIfMatch：
//   - expectedETag == ""：要求 key 不存在（If-None-Match: *）；
//   - expectedETag != ""：要求当前 ETag == expectedETag（If-Match）。
//
// 不满足条件返回 ErrConflict，原对象保持不变。
func (m *Mock) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	content, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.objs == nil {
		m.objs = make(map[string][]byte)
	}
	current, exists := m.objs[key]
	if expectedETag == "" {
		if exists {
			return "", ErrConflict
		}
	} else if !exists || md5hex(current) != expectedETag {
		return "", ErrConflict
	}
	m.objs[key] = content
	return md5hex(content), nil
}

// List 实现 Store.List：返回匹配 prefix 的全部对象键，按字典序升序；无匹配返回空切片。
func (m *Mock) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	keys := make([]string, 0, len(m.objs))
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	m.mu.RUnlock()
	sort.Strings(keys)
	return keys, nil
}
