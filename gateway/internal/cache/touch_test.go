package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestTouchRefreshedByWriteAndLookup 覆盖 last-touch 表：写盘成功与读命中都
// 刷新表里的时间。
func TestTouchRefreshedByWriteAndLookup(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/b#0"

	putBlock(t, st, key, []byte("hello"), Metadata{})
	afterWrite, ok := st.LastTouch("ds1")
	if !ok {
		t.Fatal("写盘成功后表里没有 ds1")
	}
	if afterWrite.IsZero() {
		t.Fatal("表里的时间是零值")
	}

	time.Sleep(2 * time.Millisecond)
	obj, err := st.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup = %v, want nil", err)
	}
	obj.Close()

	afterRead, ok := st.LastTouch("ds1")
	if !ok {
		t.Fatal("读命中后表里没有 ds1")
	}
	if !afterRead.After(afterWrite) {
		t.Fatalf("读命中后的时间 = %v, want > %v", afterRead, afterWrite)
	}
}

// TestLastTouchMissingDataset 覆盖表里没有的 dataset。
func TestLastTouchMissingDataset(t *testing.T) {
	st := New(t.TempDir(), 1<<20, 1<<20)
	if _, ok := st.LastTouch("nope"); ok {
		t.Fatal("表里没有的 dataset 返回了 ok")
	}
}

// TestScanFillsTableFromDirectoryMtime 覆盖启动扫描：表按 dataset 目录的 mtime
// 填。
func TestScanFillsTableFromDirectoryMtime(t *testing.T) {
	dir := t.TempDir()
	dsDir := filepath.Join(dir, "ds1")
	if err := os.MkdirAll(dsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll = %v, want nil", err)
	}
	want := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(dsDir, want, want); err != nil {
		t.Fatalf("Chtimes = %v, want nil", err)
	}

	st := New(dir, 1<<20, 1<<20)
	if err := st.Scan(context.Background()); err != nil {
		t.Fatalf("Scan = %v, want nil", err)
	}
	got, ok := st.LastTouch("ds1")
	if !ok {
		t.Fatal("Scan 之后表里没有 ds1")
	}
	if !got.Equal(want) {
		t.Fatalf("表里的时间 = %v, want %v", got, want)
	}
}

// TestPureReadsLeaveDirectoryMtimeUnchanged 覆盖 Linux 上目录 mtime 只在条目
// 增删时变：一轮纯读之后目录 mtime 不动，而表里的时间前进。
func TestPureReadsLeaveDirectoryMtimeUnchanged(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 1<<20)
	key := "ds1/blocks/b#0"
	putBlock(t, st, key, []byte("hello"), Metadata{})

	dsDir := filepath.Join(dir, "ds1")
	before, err := os.Stat(dsDir)
	if err != nil {
		t.Fatalf("Stat = %v, want nil", err)
	}
	first, ok := st.LastTouch("ds1")
	if !ok {
		t.Fatal("表里没有 ds1")
	}

	time.Sleep(2 * time.Millisecond)
	for i := 0; i < 3; i++ {
		obj, err := st.Lookup(key)
		if err != nil {
			t.Fatalf("Lookup = %v, want nil", err)
		}
		obj.Close()
	}

	after, err := os.Stat(dsDir)
	if err != nil {
		t.Fatalf("Stat = %v, want nil", err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("目录 mtime = %v, want %v", after.ModTime(), before.ModTime())
	}
	second, ok := st.LastTouch("ds1")
	if !ok {
		t.Fatal("表里没有 ds1")
	}
	if !second.After(first) {
		t.Fatalf("表里的时间 = %v, want > %v", second, first)
	}
}
