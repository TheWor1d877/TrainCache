package cache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDatasetLimitEvictsOldestBlock 覆盖第 1 步：dataset 超自己的上限时，按块
// 文件 mtime 从老到新删这个 dataset 里的块，.meta 同批删。
func TestDatasetLimitEvictsOldestBlock(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 250, 1<<20)

	old := time.Now().Add(-time.Hour)
	putBlock(t, st, "ds1/blocks/a#0", bytes.Repeat([]byte("a"), 100), Metadata{})
	oldFinal, oldMeta := blockPaths(t, dir, "ds1/blocks/a#0")
	if err := os.Chtimes(oldFinal, old, old); err != nil {
		t.Fatalf("Chtimes = %v, want nil", err)
	}
	putBlock(t, st, "ds1/blocks/a#1", bytes.Repeat([]byte("b"), 100), Metadata{})
	putBlock(t, st, "ds1/blocks/a#2", bytes.Repeat([]byte("c"), 100), Metadata{})

	// 最老的块被删掉，块与 .meta 一起没了。
	if exists(oldFinal) {
		t.Fatal("mtime 最老的块还在盘上")
	}
	if exists(oldMeta) {
		t.Fatal("mtime 最老的块的 .meta 还在盘上")
	}
	// 其余两块还在。
	for _, key := range []string{"ds1/blocks/a#1", "ds1/blocks/a#2"} {
		final, meta := blockPaths(t, dir, key)
		if !exists(final) || !exists(meta) {
			t.Fatalf("%s 的块或 .meta 不在盘上", key)
		}
	}
	if st.DatasetBytes("ds1") != 200 {
		t.Fatalf("DatasetBytes = %d, want 200", st.DatasetBytes("ds1"))
	}
	if st.TotalBytes() != 200 {
		t.Fatalf("TotalBytes = %d, want 200", st.TotalBytes())
	}
}

// TestGlobalLimitEvictsLeastTouchedDataset 覆盖第 2 步：全局超总上限时，按
// last-touch 表挑最久没出现的 dataset，删掉它的整个第一层目录。
func TestGlobalLimitEvictsLeastTouchedDataset(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 250)

	putBlock(t, st, "a/blocks/x#0", bytes.Repeat([]byte("a"), 100), Metadata{})
	putBlock(t, st, "b/blocks/x#0", bytes.Repeat([]byte("b"), 100), Metadata{})
	// 写第三个 dataset 时全局放不下，回收最久没出现的 a。
	putBlock(t, st, "c/blocks/x#0", bytes.Repeat([]byte("c"), 100), Metadata{})

	if exists(filepath.Join(dir, "a")) {
		t.Fatal("最久没出现的 dataset 的目录还在盘上")
	}
	for _, name := range []string{"b", "c"} {
		if !exists(filepath.Join(dir, name)) {
			t.Fatalf("dataset %s 被误删", name)
		}
	}
	if st.TotalBytes() != 200 {
		t.Fatalf("TotalBytes = %d, want 200", st.TotalBytes())
	}

	// 读命中刷新 b 的 last-touch，下一次回收应当删 c。
	obj, err := st.Lookup("b/blocks/x#0")
	if err != nil {
		t.Fatalf("Lookup = %v, want nil", err)
	}
	obj.Close()
	time.Sleep(2 * time.Millisecond)
	putBlock(t, st, "d/blocks/x#0", bytes.Repeat([]byte("d"), 100), Metadata{})

	if exists(filepath.Join(dir, "c")) {
		t.Fatal("c 应当被回收")
	}
	if !exists(filepath.Join(dir, "b")) {
		t.Fatal("b 刚被读过，不该被回收")
	}
	if !exists(filepath.Join(dir, "d")) {
		t.Fatal("d 不在盘上")
	}
}

// TestCannotFitIsNotWritten 覆盖第 3 步：前两步做完仍放不下这个块时，本次不
// 写盘，盘上文件数不增加。
func TestCannotFitIsNotWritten(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 10, 10)
	key := "ds1/blocks/big#0"

	before := countFiles(t, dir)
	w, err := st.BeginPut(context.Background(), key)
	if err != nil {
		t.Fatalf("BeginPut = %v, want nil", err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("x"), 100)); err != nil {
		t.Fatalf("Write = %v, want nil", err)
	}
	if err := w.Close(Metadata{}); !errors.Is(err, ErrQuota) {
		t.Fatalf("Close = %v, want ErrQuota", err)
	}

	final, meta := blockPaths(t, dir, key)
	if exists(final) {
		t.Fatal("放不下的块写进了盘上")
	}
	if exists(meta) {
		t.Fatal("放不下的块留下了 .meta")
	}
	if after := countFiles(t, dir); after != before {
		t.Fatalf("盘上文件数 = %d, want %d", after, before)
	}
	if st.TotalBytes() != 0 {
		t.Fatalf("TotalBytes = %d, want 0", st.TotalBytes())
	}
}

// TestBeginPutFailsWhenQuotaCannotFit 覆盖第 3 步在 BeginPut 上的表现：盘上已
// 超出总上限且没有别的 dataset 可回收时，BeginPut 直接报错，不建文件。
func TestBeginPutFailsWhenQuotaCannotFit(t *testing.T) {
	dir := t.TempDir()
	full := New(dir, 1<<20, 1<<20)
	putBlock(t, full, "ds1/blocks/x#0", bytes.Repeat([]byte("x"), 100), Metadata{})

	st := New(dir, 1<<20, 10)
	if err := st.Scan(context.Background()); err != nil {
		t.Fatalf("Scan = %v, want nil", err)
	}
	if st.TotalBytes() != 100 {
		t.Fatalf("TotalBytes = %d, want 100", st.TotalBytes())
	}
	before := countFiles(t, dir)
	if _, err := st.BeginPut(context.Background(), "ds1/blocks/y#0"); !errors.Is(err, ErrQuota) {
		t.Fatalf("BeginPut = %v, want ErrQuota", err)
	}
	if after := countFiles(t, dir); after != before {
		t.Fatalf("盘上文件数 = %d, want %d", after, before)
	}
}

// TestEvictionKeepsTotalWithinLimit 覆盖回收后的账：全局驻写字节回到上限内。
func TestEvictionKeepsTotalWithinLimit(t *testing.T) {
	dir := t.TempDir()
	st := New(dir, 1<<20, 300)
	for i := 0; i < 6; i++ {
		key := "ds" + string(rune('a'+i)) + "/blocks/x#0"
		w, err := st.BeginPut(context.Background(), key)
		if err != nil {
			t.Fatalf("BeginPut = %v, want nil", err)
		}
		if _, err := w.Write(bytes.Repeat([]byte("x"), 100)); err != nil {
			t.Fatalf("Write = %v, want nil", err)
		}
		if err := w.Close(Metadata{}); err != nil {
			t.Fatalf("Close = %v, want nil", err)
		}
		if st.TotalBytes() > 300 {
			t.Fatalf("TotalBytes = %d, want ≤ 300", st.TotalBytes())
		}
	}
	if st.TotalBytes() > 300 {
		t.Fatalf("TotalBytes = %d, want ≤ 300", st.TotalBytes())
	}
}
