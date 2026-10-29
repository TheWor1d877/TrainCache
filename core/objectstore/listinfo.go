package objectstore

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// ObjectInfo 是对象键的元数据：大小与最后修改时间。孤儿块清理按最后修改时间
// 给在途发布留宽限期，键本身不带时间信息，只能从存储侧取。
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ErrListInfoUnsupported 表示后端没有提供对象元数据枚举（只实现了 Store 的 6 个方法）。
var ErrListInfoUnsupported = errors.New("objectstore: object metadata listing unsupported")

// InfoLister 是可选的元数据枚举能力；Store 接口保持 6 个方法不变，需要对象年龄的
// 调用方经 ListInfo 探测。
type InfoLister interface {
	ListInfo(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// ListInfo 返回匹配 prefix 的对象元数据（按键升序）；后端不支持时返回
// ErrListInfoUnsupported。
func ListInfo(ctx context.Context, s Store, prefix string) ([]ObjectInfo, error) {
	lister, ok := s.(InfoLister)
	if !ok {
		return nil, ErrListInfoUnsupported
	}
	return lister.ListInfo(ctx, prefix)
}

// ListInfo 实现 InfoLister：Mock 记录每次写入的时间，测试可经 SetModTime 回拨。
func (m *Mock) ListInfo(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	out := make([]ObjectInfo, 0, len(m.objs))
	for k, v := range m.objs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		out = append(out, ObjectInfo{Key: k, Size: int64(len(v)), LastModified: m.modTimes[k]})
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// SetModTime 把对象的最后修改时间改成 t，供需要"老对象"的测试使用；对象不存在时无效。
func (m *Mock) SetModTime(key string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[key]; !ok {
		return
	}
	if m.modTimes == nil {
		m.modTimes = make(map[string]time.Time)
	}
	m.modTimes[key] = t
}

// ListInfo 实现 InfoLister：S3 侧直接取 ListObjects 的 LastModified。
func (s *S3Store) ListInfo(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	out := make([]ObjectInfo, 0)
	for obj := range s.client.ListObjects(ctx, s.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, ObjectInfo{Key: obj.Key, Size: obj.Size, LastModified: obj.LastModified})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
