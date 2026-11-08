package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// putBlock 写一个块并正常收尾：BeginPut → Write → Close。
func putBlock(t *testing.T, s *Store, key string, data []byte, m Metadata) {
	t.Helper()
	w, err := s.BeginPut(context.Background(), key)
	if err != nil {
		t.Fatalf("BeginPut(%q) = %v, want nil", key, err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write(%q) = %v, want nil", key, err)
	}
	if err := w.Close(m); err != nil {
		t.Fatalf("Close(%q) = %v, want nil", key, err)
	}
}

// blockPaths 返回键对应的正式名与 .meta 路径。
func blockPaths(t *testing.T, dir, key string) (string, string) {
	t.Helper()
	lay, err := keyLayout(dir, key)
	if err != nil {
		t.Fatalf("keyLayout(%q) = %v, want nil", key, err)
	}
	return lay.final, lay.meta
}

// exists 报告路径是否存在。
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// countFiles 返回目录下的普通文件数（.meta 与 .tmp 也算）。
func countFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return n
}

// TestKeyLayoutUsesWholeKeyAsRelativePath 覆盖键到路径的映射：对象键整段当相对
// 路径，dataset 是第一层目录，其余分段不解析。
func TestKeyLayoutUsesWholeKeyAsRelativePath(t *testing.T) {
	dir := t.TempDir()
	lay, err := keyLayout(dir, "ds1/blocks/shard-00000.tar#0")
	if err != nil {
		t.Fatalf("keyLayout = %v, want nil", err)
	}
	if lay.dataset != "ds1" {
		t.Fatalf("dataset = %q, want %q", lay.dataset, "ds1")
	}
	want := filepath.Join(dir, "ds1", "blocks", "shard-00000.tar#0")
	if lay.final != want {
		t.Fatalf("final = %q, want %q", lay.final, want)
	}
	if lay.meta != want+metaSuffix {
		t.Fatalf("meta = %q, want %q", lay.meta, want+metaSuffix)
	}
	if lay.tmp != want+tmpSuffix {
		t.Fatalf("tmp = %q, want %q", lay.tmp, want+tmpSuffix)
	}

	// 多余的分隔段原样保留，不解析。
	deep, err := keyLayout(dir, "ds1/blocks/a/b/c#0")
	if err != nil {
		t.Fatalf("keyLayout = %v, want nil", err)
	}
	if deep.final != filepath.Join(dir, "ds1", "blocks", "a", "b", "c#0") {
		t.Fatalf("final = %q, want nested path", deep.final)
	}
}

// TestLongSegmentSkipsWrite 覆盖 ext4 单目录名上限：任一分隔段超过 255 字节
// 时跳过写盘，不建文件。
func TestLongSegmentSkipsWrite(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)

	long := strings.Repeat("x", 254) + "#0" // 最后一个分段 256 字节
	key := "ds1/blocks/" + long
	if _, err := st.BeginPut(context.Background(), key); err == nil {
		t.Fatal("BeginPut(256 字节分段) = nil error, want error")
	}
	if n := countFiles(t, dir); n != 0 {
		t.Fatalf("盘上文件数 = %d, want 0", n)
	}

	// 255 字节正好在上限内，必须能写。注意 .tmp 与 .meta 的后缀也占目录名
	// 长度，所以实际可用的分段长度要比 255 略短。
	ok := "ds1/blocks/" + strings.Repeat("x", 200) + "#0"
	w, err := st.BeginPut(context.Background(), ok)
	if err != nil {
		t.Fatalf("BeginPut(255 字节分段) = %v, want nil", err)
	}
	if err := w.Close(Metadata{}); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
}

// TestLookupIgnoresTmpBeforeRename 覆盖 rename 之前 Lookup 一律未命中：半块
// 对读侧不可见。
func TestLookupIgnoresTmpBeforeRename(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/b#0"

	w, err := st.BeginPut(context.Background(), key)
	if err != nil {
		t.Fatalf("BeginPut = %v, want nil", err)
	}
	if _, err := w.Write([]byte("half")); err != nil {
		t.Fatalf("Write = %v, want nil", err)
	}
	if _, err := st.Lookup(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(写入中) = %v, want ErrNotFound", err)
	}
	lay, err := keyLayout(dir, key)
	if err != nil {
		t.Fatalf("keyLayout = %v, want nil", err)
	}
	if !exists(lay.tmp) {
		t.Fatal(".tmp 不在盘上")
	}
	if exists(lay.final) {
		t.Fatal("正式名在 rename 之前就出现了")
	}

	if err := w.Close(Metadata{}); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	if _, err := st.Lookup(key); err != nil {
		t.Fatalf("Lookup(rename 之后) = %v, want nil", err)
	}
}

// TestLookupRejectsIllegalKey 覆盖逃逸路径的键：非法键按未命中处理。
func TestLookupRejectsIllegalKey(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	for _, key := range []string{"", "../escape", "ds1/../ds2/blocks/b#0", "/ds1/blocks/b#0"} {
		if _, err := st.Lookup(key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Lookup(%q) = %v, want ErrNotFound", key, err)
		}
	}
}

// TestObjectMetadataKeepsLastModified 覆盖元数据往返：LastModified 保留到
// 亚秒以下的精度由 JSON 承载。
func TestObjectMetadataKeepsLastModified(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/b#0"
	lm := time.Now().UTC().Truncate(time.Millisecond)
	putBlock(t, st, key, []byte("hello"), Metadata{ETag: "etag-1", LastModified: lm})

	obj, err := st.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup = %v, want nil", err)
	}
	defer obj.Close()
	m := obj.Metadata()
	if m.ETag != "etag-1" {
		t.Fatalf("ETag = %q, want %q", m.ETag, "etag-1")
	}
	if !m.LastModified.Equal(lm) {
		t.Fatalf("LastModified = %v, want %v", m.LastModified, lm)
	}
}

// TestFragmentKeyLivesUnderFragSubtree 覆盖片段键：dataset 仍是第一段，其后是
// frag 子树与闭区间；整块键与片段键不在同一个位置，互不覆盖。
func TestFragmentKeyLivesUnderFragSubtree(t *testing.T) {
	got := FragmentKey("ds1/blocks/b#0", 0, 2097151)
	want := "ds1/frag/blocks/b#0/0-2097151"
	if got != want {
		t.Fatalf("FragmentKey = %q, want %q", got, want)
	}
	if got := FragmentKey("ds1", 10, 19); got != "ds1/frag/10-19" {
		t.Fatalf("FragmentKey(无斜杠) = %q, want %q", got, "ds1/frag/10-19")
	}
}

// TestFragmentMetadataRoundTrip 覆盖片段元数据往返：区间与对象总长落进 .meta
// 再读回来，命中缓存时网关靠它拼 Content-Range。
func TestFragmentMetadataRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := FragmentKey("ds1/blocks/b#0", 10, 19)
	putBlock(t, st, key, []byte("0123456789"), Metadata{
		ETag:       "etag-1",
		RangeStart: 10,
		RangeEnd:   19,
		ObjectSize: 100,
	})

	obj, err := st.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup = %v, want nil", err)
	}
	defer obj.Close()
	m := obj.Metadata()
	if m.RangeStart != 10 || m.RangeEnd != 19 {
		t.Fatalf("区间 = [%d,%d], want [10,19]", m.RangeStart, m.RangeEnd)
	}
	if m.ObjectSize != 100 {
		t.Fatalf("对象总长 = %d, want 100", m.ObjectSize)
	}
	if m.Size != 10 {
		t.Fatalf("片段长度 = %d, want 10", m.Size)
	}
}
