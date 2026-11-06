package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

// TestWriteLookupRoundTrip 覆盖收尾后的落盘形态：块与 .meta 成对存在，读回
// 字节一致，元数据往返一致。
func TestWriteLookupRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/shard-00000.tar#0"
	data := []byte("hello cache")

	lm := time.Now().UTC().Truncate(time.Millisecond)
	putBlock(t, st, key, data, Metadata{ETag: "etag-1", LastModified: lm})

	final, meta := blockPaths(t, dir, key)
	if !exists(final) {
		t.Fatal("块内容不在盘上")
	}
	if !exists(meta) {
		t.Fatal(".meta 不在盘上")
	}
	// .tmp 已经不在盘上。
	lay, err := keyLayout(dir, key)
	if err != nil {
		t.Fatalf("keyLayout = %v, want nil", err)
	}
	if exists(lay.tmp) {
		t.Fatal(".tmp 没被 rename 掉")
	}

	obj, err := st.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup = %v, want nil", err)
	}
	got, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("ReadAll = %v, want nil", err)
	}
	if err := obj.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("读回 = %q, want %q", got, data)
	}
	m := obj.Metadata()
	if m.ETag != "etag-1" {
		t.Fatalf("ETag = %q, want %q", m.ETag, "etag-1")
	}
	if !m.LastModified.Equal(lm) {
		t.Fatalf("LastModified = %v, want %v", m.LastModified, lm)
	}
	if m.Size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", m.Size, len(data))
	}
	if st.TotalBytes() != int64(len(data)) {
		t.Fatalf("TotalBytes = %d, want %d", st.TotalBytes(), len(data))
	}
	if st.DatasetBytes("ds1") != int64(len(data)) {
		t.Fatalf("DatasetBytes = %d, want %d", st.DatasetBytes("ds1"), len(data))
	}
}

// TestAbortLeavesNoHalfBlock 覆盖中途失败：Abort 删掉 .tmp，盘上不出现正式名。
func TestAbortLeavesNoHalfBlock(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/b#0"

	w, err := st.BeginPut(context.Background(), key)
	if err != nil {
		t.Fatalf("BeginPut = %v, want nil", err)
	}
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatalf("Write = %v, want nil", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("Abort = %v, want nil", err)
	}

	lay, err := keyLayout(dir, key)
	if err != nil {
		t.Fatalf("keyLayout = %v, want nil", err)
	}
	if exists(lay.tmp) {
		t.Fatal(".tmp 还在盘上")
	}
	if exists(lay.final) {
		t.Fatal("正式名出现了")
	}
	if exists(lay.meta) {
		t.Fatal(".meta 出现了")
	}
	if _, err := st.Lookup(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup = %v, want ErrNotFound", err)
	}
	if st.TotalBytes() != 0 {
		t.Fatalf("TotalBytes = %d, want 0", st.TotalBytes())
	}
}

// TestCorruptMetaIsMiss 覆盖 .meta 损坏：按未命中处理。
func TestCorruptMetaIsMiss(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/b#0"
	putBlock(t, st, key, []byte("hello"), Metadata{ETag: "e"})

	_, meta := blockPaths(t, dir, key)
	if err := os.WriteFile(meta, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("WriteFile = %v, want nil", err)
	}
	if _, err := st.Lookup(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(.meta 损坏) = %v, want ErrNotFound", err)
	}

	// .meta 缺失同样按未命中处理。
	if err := os.Remove(meta); err != nil {
		t.Fatalf("Remove = %v, want nil", err)
	}
	if _, err := st.Lookup(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(.meta 缺失) = %v, want ErrNotFound", err)
	}
}

// TestReadSurvivesConcurrentDelete 覆盖并发删除：Lookup 已打开的文件在块被淘汰
// 之后仍能读完。
func TestReadSurvivesConcurrentDelete(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 6000, 1<<20)
	key := "ds1/blocks/b#0"
	data := bytes.Repeat([]byte("x"), 4096)
	putBlock(t, st, key, data, Metadata{})

	obj, err := st.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup = %v, want nil", err)
	}
	// 再写一块把旧块按 mtime 淘汰掉。
	putBlock(t, st, "ds1/blocks/c#0", bytes.Repeat([]byte("y"), 4096), Metadata{})

	final, _ := blockPaths(t, dir, key)
	if exists(final) {
		t.Fatal("旧块没有被淘汰")
	}
	got, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("ReadAll = %v, want nil", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("读回 = %d 字节, want %d", len(got), len(data))
	}
	if err := obj.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
}

// TestConcurrentLookupAndWrite 覆盖并发访问：读写与淘汰同时发生不出错。
func TestConcurrentLookupAndWrite(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 4096, 8192)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("ds%d/blocks/b#%d", i%3, i)
			w, err := st.BeginPut(context.Background(), key)
			if err != nil {
				t.Errorf("BeginPut = %v, want nil", err)
				return
			}
			if _, err := w.Write(bytes.Repeat([]byte("z"), 256)); err != nil {
				t.Errorf("Write = %v, want nil", err)
				w.Abort()
				return
			}
			if err := w.Close(Metadata{ETag: "e"}); err != nil {
				// 配额放不下时允许失败，不写盘。
				return
			}
			obj, err := st.Lookup(key)
			if err != nil {
				return
			}
			io.ReadAll(obj)
			obj.Close()
		}(i)
	}
	wg.Wait()
}

// TestMetaFileFieldNames 覆盖 .meta 的落盘格式：JSON，字段 etag /
// last_modified / size。
func TestMetaFileFieldNames(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/b#0"
	putBlock(t, st, key, []byte("hello"), Metadata{ETag: "etag-1"})

	_, meta := blockPaths(t, dir, key)
	raw, err := os.ReadFile(meta)
	if err != nil {
		t.Fatalf("ReadFile = %v, want nil", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("Unmarshal(.meta) = %v, want nil", err)
	}
	for _, name := range []string{"etag", "last_modified", "size"} {
		if _, ok := fields[name]; !ok {
			t.Fatalf(".meta 缺字段 %q，实际字段 %v", name, fields)
		}
	}
	if fields["etag"] != "etag-1" {
		t.Fatalf(".meta etag = %v, want etag-1", fields["etag"])
	}
	if fields["size"] != float64(5) {
		t.Fatalf(".meta size = %v, want 5", fields["size"])
	}
}

// TestScanRecoversAccounting 覆盖启动扫描：驻写字节账按盘上实际内容重建。
func TestScanRecoversAccounting(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	putBlock(t, st, "ds1/blocks/b#0", bytes.Repeat([]byte("a"), 100), Metadata{})
	putBlock(t, st, "ds2/blocks/b#0", bytes.Repeat([]byte("b"), 200), Metadata{})

	fresh := New(dir, 1<<20, 1<<20)
	if err := fresh.Scan(context.Background()); err != nil {
		t.Fatalf("Scan = %v, want nil", err)
	}
	if fresh.TotalBytes() != 300 {
		t.Fatalf("TotalBytes = %d, want 300", fresh.TotalBytes())
	}
	if fresh.DatasetBytes("ds1") != 100 {
		t.Fatalf("DatasetBytes(ds1) = %d, want 100", fresh.DatasetBytes("ds1"))
	}
	if fresh.DatasetBytes("ds2") != 200 {
		t.Fatalf("DatasetBytes(ds2) = %d, want 200", fresh.DatasetBytes("ds2"))
	}
}
