package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// readAll 读取 key 的全部内容；出错直接失败。
func readAll(t *testing.T, m *Mock, key string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := m.Get(context.Background(), key, &buf); err != nil {
		t.Fatalf("Get(%q) 失败: %v", key, err)
	}
	return buf.Bytes()
}

func TestMockPutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	content := []byte("hello traincache")

	etag, err := m.Put(ctx, "dataset/a/snapshots/s1/manifest.json", bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	if etag != md5hex(content) {
		t.Fatalf("Put 返回 ETag = %q，期望 md5hex(content) = %q", etag, md5hex(content))
	}

	if got := readAll(t, m, "dataset/a/snapshots/s1/manifest.json"); !bytes.Equal(got, content) {
		t.Fatalf("Get 内容 = %q，期望 %q", got, content)
	}

	gotETag, err := m.GetETag(ctx, "dataset/a/snapshots/s1/manifest.json")
	if err != nil {
		t.Fatalf("GetETag 失败: %v", err)
	}
	if gotETag != etag {
		t.Fatalf("GetETag = %q，期望 %q", gotETag, etag)
	}
}

func TestMockPutETagStableForSameContent(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	content := []byte("same bytes")

	etag1, err := m.Put(ctx, "k1", bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put(k1) 失败: %v", err)
	}
	etag2, err := m.Put(ctx, "k2", bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put(k2) 失败: %v", err)
	}
	if etag1 != etag2 {
		t.Fatalf("相同内容 ETag 不稳定: %q != %q", etag1, etag2)
	}

	// 覆盖写相同内容，ETag 仍稳定。
	etag3, err := m.Put(ctx, "k1", bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("重复 Put(k1) 失败: %v", err)
	}
	if etag3 != etag1 {
		t.Fatalf("相同内容覆盖写后 ETag 变化: %q != %q", etag3, etag1)
	}

	// 不同内容 ETag 不同。
	other := []byte("other bytes")
	etag4, err := m.Put(ctx, "k3", bytes.NewReader(other), int64(len(other)))
	if err != nil {
		t.Fatalf("Put(k3) 失败: %v", err)
	}
	if etag4 == etag1 {
		t.Fatalf("不同内容 ETag 相同: %q", etag4)
	}
}

func TestMockGetAndGetETagNotFound(t *testing.T) {
	ctx := context.Background()
	m := NewMock()

	var buf bytes.Buffer
	if err := m.Get(ctx, "missing", &buf); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(不存在的 key) 错误 = %v，期望 ErrNotFound", err)
	}
	if _, err := m.GetETag(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetETag(不存在的 key) 错误 = %v，期望 ErrNotFound", err)
	}
}

func TestMockDelete(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	content := []byte("to be deleted")
	if _, err := m.Put(ctx, "k", bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if err := m.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete 失败: %v", err)
	}
	var buf bytes.Buffer
	if err := m.Get(ctx, "k", &buf); !errors.Is(err, ErrNotFound) {
		t.Fatalf("删除后 Get 错误 = %v，期望 ErrNotFound", err)
	}

	// 删除不存在的对象不算错误。
	if err := m.Delete(ctx, "k"); err != nil {
		t.Fatalf("重复 Delete 返回错误: %v", err)
	}
}

// PutIfMatch 三态之一：key 不存在 + expectedETag == "" → 写成功。
func TestMockPutIfMatchCreateOnMissingKey(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	content := []byte("first content")

	newETag, err := m.PutIfMatch(ctx, "CURRENT", bytes.NewReader(content), int64(len(content)), "")
	if err != nil {
		t.Fatalf("PutIfMatch(key 不存在, expectedETag=\"\") 失败: %v", err)
	}
	if newETag != md5hex(content) {
		t.Fatalf("返回 ETag = %q，期望 %q", newETag, md5hex(content))
	}
	if got := readAll(t, m, "CURRENT"); !bytes.Equal(got, content) {
		t.Fatalf("写入内容 = %q，期望 %q", got, content)
	}
}

// PutIfMatch 三态之一：key 已存在 + expectedETag == "" → ErrConflict 且原对象未被改动。
func TestMockPutIfMatchCreateOnExistingKeyConflict(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	orig := []byte("snap1 manifestfp")
	if _, err := m.Put(ctx, "CURRENT", bytes.NewReader(orig), int64(len(orig))); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	origETag, err := m.GetETag(ctx, "CURRENT")
	if err != nil {
		t.Fatalf("GetETag 失败: %v", err)
	}

	next := []byte("snap2 manifestfp")
	_, err = m.PutIfMatch(ctx, "CURRENT", bytes.NewReader(next), int64(len(next)), "")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("PutIfMatch(已存在, expectedETag=\"\") 错误 = %v，期望 ErrConflict", err)
	}
	if got := readAll(t, m, "CURRENT"); !bytes.Equal(got, orig) {
		t.Fatalf("冲突后内容被改动: %q，期望 %q", got, orig)
	}
	if gotETag, _ := m.GetETag(ctx, "CURRENT"); gotETag != origETag {
		t.Fatalf("冲突后 ETag 被改动: %q，期望 %q", gotETag, origETag)
	}
}

// PutIfMatch 三态之二：ETag 匹配 → 写成功且返回新 ETag。
func TestMockPutIfMatchOnMatchingETag(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	orig := []byte("snap1 manifestfp")
	origETag, err := m.Put(ctx, "CURRENT", bytes.NewReader(orig), int64(len(orig)))
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	next := []byte("snap2 manifestfp")
	newETag, err := m.PutIfMatch(ctx, "CURRENT", bytes.NewReader(next), int64(len(next)), origETag)
	if err != nil {
		t.Fatalf("PutIfMatch(ETag 匹配) 失败: %v", err)
	}
	if newETag != md5hex(next) {
		t.Fatalf("返回 ETag = %q，期望 md5hex(新内容) = %q", newETag, md5hex(next))
	}
	if newETag == origETag {
		t.Fatalf("内容变化但 ETag 未变: %q", newETag)
	}
	if got := readAll(t, m, "CURRENT"); !bytes.Equal(got, next) {
		t.Fatalf("写入内容 = %q，期望 %q", got, next)
	}
	if gotETag, err := m.GetETag(ctx, "CURRENT"); err != nil || gotETag != newETag {
		t.Fatalf("GetETag = %q (err=%v)，期望 %q", gotETag, err, newETag)
	}
}

// PutIfMatch 三态之三：ETag 不匹配 → ErrConflict 且原对象未被改动。
func TestMockPutIfMatchOnMismatchedETag(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	orig := []byte("snap1 manifestfp")
	origETag, err := m.Put(ctx, "CURRENT", bytes.NewReader(orig), int64(len(orig)))
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	next := []byte("snap2 manifestfp")
	wrongETag := md5hex([]byte("stale snapshot"))
	if wrongETag == origETag {
		t.Fatal("测试构造错误：wrongETag 不应等于当前 ETag")
	}
	_, err = m.PutIfMatch(ctx, "CURRENT", bytes.NewReader(next), int64(len(next)), wrongETag)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("PutIfMatch(ETag 不匹配) 错误 = %v，期望 ErrConflict", err)
	}
	if got := readAll(t, m, "CURRENT"); !bytes.Equal(got, orig) {
		t.Fatalf("冲突后内容被改动: %q，期望 %q", got, orig)
	}
	if gotETag, _ := m.GetETag(ctx, "CURRENT"); gotETag != origETag {
		t.Fatalf("冲突后 ETag 被改动: %q，期望 %q", gotETag, origETag)
	}
}

// PutIfMatch 边界：key 不存在 + expectedETag != "" → ErrConflict，key 仍不存在。
func TestMockPutIfMatchOnMissingKeyWithExpectedETag(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	content := []byte("snap2 manifestfp")

	_, err := m.PutIfMatch(ctx, "CURRENT", bytes.NewReader(content), int64(len(content)), md5hex([]byte("whatever")))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("PutIfMatch(key 不存在, expectedETag!= \"\") 错误 = %v，期望 ErrConflict", err)
	}
	var buf bytes.Buffer
	if err := m.Get(ctx, "CURRENT", &buf); !errors.Is(err, ErrNotFound) {
		t.Fatalf("冲突后 key 应仍不存在，Get 错误 = %v", err)
	}
}

// PutIfMatch 并发：多个 goroutine 同时以 expectedETag="" 创建同一 key，恰一个成功。
func TestMockPutIfMatchConcurrentCreate(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	const n = 16

	contents := make([][]byte, n)
	for i := range contents {
		contents[i] = []byte(fmt.Sprintf("snap-%d fp-%d", i, i))
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		okCount   int
		conflicts int
		winner    []byte
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			content := contents[i]
			etag, err := m.PutIfMatch(ctx, "CURRENT", bytes.NewReader(content), int64(len(content)), "")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				okCount++
				winner = content
				if etag != md5hex(content) {
					t.Errorf("成功者返回 ETag = %q，期望 %q", etag, md5hex(content))
				}
			case errors.Is(err, ErrConflict):
				conflicts++
			default:
				t.Errorf("意外错误: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if okCount != 1 || conflicts != n-1 {
		t.Fatalf("成功 = %d、冲突 = %d，期望 1 与 %d", okCount, conflicts, n-1)
	}
	if got := readAll(t, m, "CURRENT"); !bytes.Equal(got, winner) {
		t.Fatalf("最终内容 = %q，期望成功者内容 %q", got, winner)
	}
}

func TestMockContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := NewMock()

	var buf bytes.Buffer
	if err := m.Get(ctx, "k", &buf); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get(ctx 已取消) 错误 = %v，期望 context.Canceled", err)
	}
	if _, err := m.Put(ctx, "k", bytes.NewReader([]byte("x")), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put(ctx 已取消) 错误 = %v，期望 context.Canceled", err)
	}
	if _, err := m.PutIfMatch(ctx, "k", bytes.NewReader([]byte("x")), 1, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutIfMatch(ctx 已取消) 错误 = %v，期望 context.Canceled", err)
	}
}

// TestMockList 覆盖 D19：按 prefix 过滤、字典序升序、无匹配返回空切片、ctx 取消报错。
func TestMockList(t *testing.T) {
	ctx := context.Background()
	m := NewMock()
	for _, k := range []string{
		"dataset/b/snapshots/s1/manifest.json",
		"dataset/a/CURRENT",
		"dataset/a/snapshots/s2/shard-abcd.idx",
		"dataset/a/snapshots/s1/manifest.json",
	} {
		if _, err := m.Put(ctx, k, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatalf("Put(%q) 失败: %v", k, err)
		}
	}

	got, err := m.List(ctx, "dataset/a/")
	if err != nil {
		t.Fatalf("List 失败: %v", err)
	}
	want := []string{
		"dataset/a/CURRENT",
		"dataset/a/snapshots/s1/manifest.json",
		"dataset/a/snapshots/s2/shard-abcd.idx",
	}
	if len(got) != len(want) {
		t.Fatalf("List = %v，期望 %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List = %v，期望 %v", got, want)
		}
	}

	// 无匹配：空切片（非 nil）。
	empty, err := m.List(ctx, "dataset/zzz/")
	if err != nil {
		t.Fatalf("List(无匹配) 失败: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("无匹配应返回空切片，得到 %#v", empty)
	}

	// ctx 取消：立即返回错误。
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.List(canceled, "dataset/"); !errors.Is(err, context.Canceled) {
		t.Fatalf("List(ctx 已取消) 错误 = %v，期望 context.Canceled", err)
	}
}
