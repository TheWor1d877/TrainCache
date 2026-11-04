package sidecar

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestBackfillCopiesDataAndWritesComplete(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	s := newTestSidecar(t, opts, &fakeFactory{})
	src := writeStep(t, opts.TaskDir, 12, "checkpoint-data", true)

	if err := s.backfill(12, src); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	dst := filepath.Join(opts.NVMEDir, opts.TaskID, "step-12")
	if !pathExists(filepath.Join(dst, completeMarker)) {
		t.Fatal("缓存缺少 .complete")
	}
	if pathExists(filepath.Join(dst, doneMarker)) {
		t.Fatal(".done 不应被复制到缓存")
	}
	b, err := os.ReadFile(filepath.Join(dst, "model.bin"))
	if err != nil || string(b) != "checkpoint-data" {
		t.Fatalf("缓存数据 = %q, %v", b, err)
	}
	s.mu.Lock()
	got := s.cache[12]
	s.mu.Unlock()
	if got != dst {
		t.Fatalf("cache[12] = %q, want %q", got, dst)
	}
}

func TestBackfillReusesExistingComplete(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	s := newTestSidecar(t, opts, &fakeFactory{})
	final := writeCachedStep(t, opts.NVMEDir, opts.TaskID, 5, "old")
	src := writeStep(t, opts.TaskDir, 5, "new", true)

	if err := s.backfill(5, src); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(final, "model.bin"))
	if err != nil || string(b) != "old" {
		t.Fatalf("已有完整缓存不应被覆盖: %q, %v", b, err)
	}
	s.mu.Lock()
	got, ok := s.cache[5]
	s.mu.Unlock()
	if !ok || got != final {
		t.Fatalf("cache[5] = %q, %v; want %q", got, ok, final)
	}
}

func TestBackfillFailureLeavesNoComplete(t *testing.T) {
	opts := testOptions(t)
	s := newTestSidecar(t, opts, &fakeFactory{})
	src := writeStep(t, opts.TaskDir, 6, "data", true)
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.nvmeTaskDir = filepath.Join(blocker, "task")
	if err := s.backfill(6, src); err == nil {
		t.Fatal("父路径不是目录时 backfill 应失败")
	}
	s.mu.Lock()
	n := len(s.cache)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("失败后不应登记缓存, cache 条目 = %d", n)
	}
}

func TestEvictCacheKeepsRetention(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	opts.NVMERetention = 2
	s := newTestSidecar(t, opts, &fakeFactory{})
	for step := int64(1); step <= 3; step++ {
		dir := writeCachedStep(t, opts.NVMEDir, opts.TaskID, step, "x")
		s.addCache(step, dir)
	}
	s.evictCache()
	if pathExists(filepath.Join(opts.NVMEDir, opts.TaskID, "step-1")) {
		t.Fatal("最旧的 step 应被淘汰")
	}
	for _, step := range []int64{2, 3} {
		if !pathExists(filepath.Join(opts.NVMEDir, opts.TaskID, fmt.Sprintf("step-%d", step))) {
			t.Fatalf("step-%d 不应被淘汰", step)
		}
	}
	s.mu.Lock()
	got := len(s.cache)
	s.mu.Unlock()
	if got != 2 {
		t.Fatalf("缓存条目 = %d, want 2", got)
	}
}

func TestEvictCacheSkipsLinkedStep(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	opts.NVMERetention = 1
	s := newTestSidecar(t, opts, &fakeFactory{})
	d1 := writeCachedStep(t, opts.NVMEDir, opts.TaskID, 1, "one")
	d2 := writeCachedStep(t, opts.NVMEDir, opts.TaskID, 2, "two")
	s.addCache(1, d1)
	s.addCache(2, d2)
	s.recordLink(1, d1)

	s.evictCache()
	if !pathExists(d1) {
		t.Fatal("被软链引用的缓存不应删除")
	}
	if pathExists(d2) {
		t.Fatal("未被引用的较新缓存应被淘汰以满足保留份数")
	}
}

func TestCacheStepRejectsIncompleteContent(t *testing.T) {
	opts := testOptions(t)
	dir := t.TempDir()
	s := newTestSidecar(t, opts, &fakeFactory{})
	s.addCache(1, dir)
	if _, ok := s.cacheStep(1); ok {
		t.Fatal("没有 .complete 的目录不应命中")
	}
	s.mu.Lock()
	_, exists := s.cache[1]
	s.mu.Unlock()
	if exists {
		t.Fatal("无效缓存条目应被移除")
	}
}

func TestBackfillAndEvictConcurrent(t *testing.T) {
	opts := testOptions(t)
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	opts.NVMERetention = 1
	s := newTestSidecar(t, opts, &fakeFactory{})

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for step := int64(1); step <= 8; step++ {
		src := writeStep(t, opts.TaskDir, step, fmt.Sprintf("data-%d", step), true)
		wg.Add(1)
		go func(step int64, src string) {
			defer wg.Done()
			if err := s.backfill(step, src); err != nil {
				errCh <- err
			}
		}(step, src)
	}
	for i := 0; i < 8; i++ {
		s.evictCache()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发回填失败: %v", err)
	}
	// 只断言状态一致；并发回填与淘汰的最终份数允许在 1 到 8 之间。
	s.mu.Lock()
	for step, dir := range s.cache {
		if !pathExists(filepath.Join(dir, completeMarker)) {
			t.Errorf("cache[%d]=%s 没有 .complete", step, dir)
		}
	}
	s.mu.Unlock()
}
