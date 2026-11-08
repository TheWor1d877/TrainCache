package sidecar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/checkpoint/internal/plugin"
)

// 本文件覆盖 10.3 覆盖矩阵里的「缓存回填、淘汰与并发」一行：cache_test.go 里的回填与
// 淘汰是各跑一轮，queue_test.go 只锁队列。这里让保存循环（入队、串行上传、
// 回填、淘汰）与一个高频淘汰循环同时跑，在 -race 下断言两条不变量：
// 在途上传的目标不会被删，被软链引用的缓存不会被删（10.1「关键流程」）。

// cacheReporter 是 *testing.T 用到的最小子集：一致性断言既能接真正的测试，
// 也能接变异自证里的记录器（见 evict_race_mutant_test.go）。
type cacheReporter interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// cacheSnapshot 是索引与磁盘目录的一次对照快照。
type cacheSnapshot struct {
	registered map[int64]string
	onDisk     map[int64]string
}

func (s *Sidecar) snapshotCache(t cacheReporter, nvmeTaskDir string) cacheSnapshot {
	t.Helper()
	entries, err := os.ReadDir(nvmeTaskDir)
	if err != nil {
		t.Fatalf("读缓存目录 %s: %v", nvmeTaskDir, err)
	}
	onDisk := make(map[int64]string)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if strings.Contains(e.Name(), ".tmp-") {
			t.Errorf("回填的临时目录残留在缓存里: %s", filepath.Join(nvmeTaskDir, e.Name()))
			continue
		}
		step, ok := parseStepName(e.Name())
		if !ok {
			continue
		}
		onDisk[step] = filepath.Join(nvmeTaskDir, e.Name())
	}
	s.mu.Lock()
	registered := make(map[int64]string, len(s.cache))
	for step, dir := range s.cache {
		registered[step] = dir
	}
	s.mu.Unlock()
	return cacheSnapshot{registered: registered, onDisk: onDisk}
}

// assertCacheConsistent 断言索引与磁盘一致，且每一项都落到了 .complete：
// backfill 用 rename 提交、evictCache 删目录与删索引成对进行，两者并发时
// 不该出现「索引指向已删目录」或「目录在但不在索引」这两种半截状态。
func assertCacheConsistent(t cacheReporter, s *Sidecar, snap cacheSnapshot) {
	t.Helper()
	for step, dir := range snap.registered {
		if got, ok := snap.onDisk[step]; !ok || got != dir {
			t.Errorf("索引里的 step %d 指向 %s，磁盘上没有这个目录", step, dir)
			continue
		}
		if !pathExists(filepath.Join(dir, completeMarker)) {
			t.Errorf("索引里的 step %d 缺 .complete: %s", step, dir)
		}
		if !hasDataFiles(dir) {
			t.Errorf("索引里的 step %d 没有数据文件: %s", step, dir)
		}
	}
	for step, dir := range snap.onDisk {
		if _, ok := snap.registered[step]; !ok {
			t.Errorf("磁盘上的缓存目录 %s（step %d）不在索引里", dir, step)
		}
	}
}

// TestConcurrentUploadAndEvictKeepsInFlightAndLinked 用保存循环与高频淘汰循环
// 并发跑，断言在途上传的工作目录与缓存目标没有被删，被软链引用的缓存留着。
func TestConcurrentUploadAndEvictKeepsInFlightAndLinked(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	opts.NVMERetention = 1
	nvmeTaskDir := filepath.Join(opts.NVMEDir, opts.TaskID)

	var (
		mu            sync.Mutex
		lostWorkDir   []int64 // 上传在途却已被删的工作目录
		prematureDir  []int64 // 上传在途时就已经存在的缓存目标
		overlapRounds int64   // 一次上传期间观察到的淘汰轮次
	)
	evictRounds := &atomic.Int64{}
	inFlight := &atomic.Int32{}

	factory := &fakeFactory{build: func() *fakePlugin {
		return &fakePlugin{save: func(_ context.Context, p plugin.SaveParams) (plugin.TransferResult, error) {
			inFlight.Add(1)
			defer inFlight.Add(-1)
			before := evictRounds.Load()
			if pathExists(filepath.Join(nvmeTaskDir, fmt.Sprintf("step-%d", p.Step))) {
				mu.Lock()
				prematureDir = append(prematureDir, p.Step)
				mu.Unlock()
			}
			// 上传期间淘汰循环照跑：这一步模拟真实的在途窗口。
			time.Sleep(2 * time.Millisecond)
			if !pathExists(p.LocalPath) {
				mu.Lock()
				lostWorkDir = append(lostWorkDir, p.Step)
				mu.Unlock()
			}
			if delta := evictRounds.Load() - before; delta > 0 {
				mu.Lock()
				overlapRounds += delta
				mu.Unlock()
			}
			return plugin.TransferResult{Bytes: 8, Files: 1}, nil
		}}
	}}
	s := newTestSidecar(t, opts, factory)

	// 被软链引用的缓存：restore 命中时由 linkCache 建链并登记（10.1「关键流程」）。
	const linkedStep = int64(3)
	linkedDir := writeCachedStep(t, opts.NVMEDir, opts.TaskID, linkedStep, "linked-payload")
	s.addCache(linkedStep, linkedDir)
	if hit, err := s.linkCache(linkedStep, linkedDir); err != nil || !hit {
		t.Fatalf("linkCache = %v, %v; want true/nil", hit, err)
	}

	stop := make(chan struct{})
	var evictWG sync.WaitGroup
	evictWG.Add(1)
	go func() {
		defer evictWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.evictCache()
			evictRounds.Add(1)
			time.Sleep(100 * time.Microsecond)
		}
	}()

	const uploadCount = 8
	paths := make(map[int64]string, uploadCount)
	for i := 0; i < uploadCount; i++ {
		step := int64(10 + i)
		paths[step] = writeStep(t, opts.TaskDir, step, fmt.Sprintf("payload-%d", step), true)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.saveLoop(ctx)
	}()
	// 轮询与保存并行：入队读缓存索引，淘汰写缓存索引（10.1「关键流程」）。
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		s.pollLoop(ctx)
	}()

	waitFor(t, 10*time.Second, "全部 step 上传完成", func() bool {
		s.mu.Lock()
		done := len(s.completed)
		s.mu.Unlock()
		return done >= uploadCount
	})
	close(stop)
	evictWG.Wait()
	cancel()
	<-loopDone
	<-pollDone

	logs := buf.String()
	if strings.Contains(logs, "缓存回填失败") || strings.Contains(logs, "淘汰缓存失败") || strings.Contains(logs, "删除工作卷副本失败") {
		t.Fatalf("并发上传与淘汰期间出现失败日志:\n%s", logs)
	}
	if got := strings.Count(logs, "淘汰缓存 step"); got == 0 {
		t.Fatalf("淘汰一次都没发生，用例没有构成竞争:\n%s", logs)
	}

	mu.Lock()
	lost, premature, overlap := lostWorkDir, prematureDir, overlapRounds
	mu.Unlock()
	if len(lost) != 0 {
		t.Fatalf("在途上传的工作目录被淘汰循环删掉: step %v", lost)
	}
	if len(premature) != 0 {
		t.Fatalf("上传还没结束，缓存目标已经出现: step %v", premature)
	}
	if overlap == 0 {
		t.Fatal("上传期间没有观察到任何淘汰轮次，竞争未发生")
	}

	// 被软链引用的缓存必须原样留着，且仍在索引里。
	abs, err := filepath.Abs(linkedDir)
	if err != nil {
		t.Fatal(err)
	}
	if target, err := os.Readlink(s.stepDir(linkedStep)); err != nil || target != abs {
		t.Fatalf("软链目标 = %q, %v; want %q", target, err, abs)
	}
	if string(readFileOrFail(t, filepath.Join(linkedDir, "model.bin"))) != "linked-payload" {
		t.Fatalf("被引用的缓存内容被改动: %s", linkedDir)
	}

	snap := s.snapshotCache(t, nvmeTaskDir)
	assertCacheConsistent(t, s, snap)
	if _, ok := snap.registered[linkedStep]; !ok {
		t.Errorf("被软链引用的 step %d 从索引里消失", linkedStep)
	}
	// 保留份数是 1，被软链引用的那份又占着名额，已上传的 step 被淘汰掉是
	// 合法终态；这里锁的是另一件事：上传成功后工作卷副本一定已经清掉。
	for step, dir := range paths {
		if pathExists(dir) {
			t.Errorf("上传完成的 step %d 工作卷副本没有被删除: %s", step, dir)
		}
	}
}

// TestConcurrentRestoreBackfillAndEvict 覆盖恢复成功后的异步回填与淘汰并发
// （10.1「关键流程」）：回填途中的目录不会被删成半截，
// 被引用的那份照样留着。
func TestConcurrentRestoreBackfillAndEvict(t *testing.T) {
	opts := testOptions(t)
	log, buf := newTestLogger()
	opts.Logger = log
	opts.NVMEDir = filepath.Join(t.TempDir(), "nvme")
	opts.NVMERetention = 1
	nvmeTaskDir := filepath.Join(opts.NVMEDir, opts.TaskID)
	s := newTestSidecar(t, opts, &fakeFactory{})

	const linkedStep = int64(1)
	linkedDir := writeCachedStep(t, opts.NVMEDir, opts.TaskID, linkedStep, "linked-payload")
	s.addCache(linkedStep, linkedDir)
	if hit, err := s.linkCache(linkedStep, linkedDir); err != nil || !hit {
		t.Fatalf("linkCache = %v, %v; want true/nil", hit, err)
	}

	srcs := make(map[int64]string, 5)
	for i := 0; i < 5; i++ {
		step := int64(30 + i)
		srcs[step] = writeStep(t, opts.TaskDir, step, fmt.Sprintf("restored-%d", step), true)
	}

	stop := make(chan struct{})
	var evictWG sync.WaitGroup
	evictWG.Add(1)
	go func() {
		defer evictWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.evictCache()
			s.cacheStep(linkedStep)
			time.Sleep(100 * time.Microsecond)
		}
	}()
	for step, src := range srcs {
		s.backfillAsync(step, src)
	}
	s.bg.Wait()
	close(stop)
	evictWG.Wait()

	logs := buf.String()
	if strings.Contains(logs, "缓存回填失败") || strings.Contains(logs, "淘汰缓存失败") {
		t.Fatalf("异步回填与淘汰并发期间出现失败日志:\n%s", logs)
	}
	assertCacheConsistent(t, s, s.snapshotCache(t, nvmeTaskDir))
	if !pathExists(filepath.Join(nvmeTaskDir, "step-1", "model.bin")) {
		t.Fatal("被软链引用的缓存被删除")
	}
	if got := string(readFileOrFail(t, filepath.Join(s.stepDir(linkedStep), "model.bin"))); got != "linked-payload" {
		t.Fatalf("透过软链读到 %q, want linked-payload", got)
	}
	for step := range srcs {
		dir := filepath.Join(nvmeTaskDir, fmt.Sprintf("step-%d", step))
		if !pathExists(dir) {
			continue // 已被淘汰是合法终态
		}
		if !pathExists(filepath.Join(dir, completeMarker)) {
			t.Errorf("回填中的 step %d 目录出现在缓存里但没有 .complete: %s", step, dir)
		}
	}
}

func readFileOrFail(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读 %s: %v", path, err)
	}
	return b
}
