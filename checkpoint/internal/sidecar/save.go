package sidecar

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"traincache/checkpoint/internal/obs"
	"traincache/checkpoint/internal/plugin"
)

// 两个标记文件由训练程序与 checkpointd 维护（design 10.1「本地目录契约」），
// 不属于训练数据，扫描与缓存复制时都排除。
const (
	doneMarker     = ".done"
	completeMarker = ".complete"
)

var tmpSeq atomic.Uint64

type saveJob struct {
	step int64
	path string
}

type scanState struct {
	files  int
	bytes  int64
	stable int
}

type stepObservation struct {
	files   int
	bytes   int64
	hasDone bool
}

// pollLoop 每 PollInterval 轮询工作卷，只处理已经出现 .done 的 step。
func (s *Sidecar) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pollOnce()
		}
	}
}

// scanLoop 每 ScanInterval 全量扫描一次，用连续稳定次数兜底没有 .done 的训练程序。
func (s *Sidecar) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scanOnce()
		}
	}
}

// pollOnce 扫描 step-* 目录。有 .done 时，本轮与上一轮字节数相同才入队；
// 首次发现直接入队，因为 .done 的契约是训练程序写完并 fsync 后创建。
func (s *Sidecar) pollOnce() {
	entries, err := os.ReadDir(s.opts.TaskDir)
	if err != nil {
		s.log.Warn("轮询工作卷失败",
			append([]any{obs.Task(s.opts.TaskID), slog.String("path", s.opts.TaskDir)}, obs.Err(err)...)...)
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		step, ok := parseStepName(e.Name())
		if !ok {
			continue
		}
		dir := filepath.Join(s.opts.TaskDir, e.Name())
		obsStep, err := observeStep(dir)
		if err != nil {
			s.log.Debug("扫描 step 失败",
				append([]any{obs.Task(s.opts.TaskID), obs.Step(step), slog.String("path", dir)}, obs.Err(err)...)...)
			continue
		}
		if !obsStep.hasDone {
			continue
		}
		s.mu.Lock()
		prev, seen := s.lastPoll[step]
		s.lastPoll[step] = obsStep.bytes
		s.mu.Unlock()

		if obsStep.files == 0 {
			s.logSkip(step, dir, "empty_dir", slog.LevelWarn)
			continue
		}
		if seen && prev != obsStep.bytes {
			s.log.Debug("step 字节数仍在变化，等待下一轮",
				obs.Task(s.opts.TaskID), obs.Step(step), obs.Bytes(obsStep.bytes), slog.Int64("prev_bytes", prev))
			continue
		}
		if !s.tracked(step) {
			s.log.Info(fmt.Sprintf("发现 .done step %d", step), obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateReady))
		}
		s.enqueue(step, dir)
	}
}

// scanOnce 是全量扫描。没有 .done 的目录要求文件数与总字节数连续
// ScanStable 次不变；有 .done 的目录仍需排除正在变化的目录。
func (s *Sidecar) scanOnce() {
	entries, err := os.ReadDir(s.opts.TaskDir)
	if err != nil {
		s.log.Warn("全量扫描工作卷失败",
			append([]any{obs.Task(s.opts.TaskID), slog.String("path", s.opts.TaskDir)}, obs.Err(err)...)...)
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		step, ok := parseStepName(e.Name())
		if !ok {
			continue
		}
		dir := filepath.Join(s.opts.TaskDir, e.Name())
		cur, err := observeStep(dir)
		if err != nil {
			s.log.Debug("扫描 step 失败",
				append([]any{obs.Task(s.opts.TaskID), obs.Step(step), slog.String("path", dir)}, obs.Err(err)...)...)
			continue
		}
		if cur.files == 0 {
			s.logSkip(step, dir, "empty_dir", slog.LevelWarn)
			continue
		}

		s.mu.Lock()
		prev, seen := s.scans[step]
		s.mu.Unlock()
		if cur.hasDone {
			state := scanState{files: cur.files, bytes: cur.bytes, stable: 1}
			s.mu.Lock()
			s.scans[step] = state
			s.mu.Unlock()
			if seen && prev.bytes != cur.bytes {
				s.log.Warn("扫描参数不一致，等待下一轮",
					obs.Task(s.opts.TaskID), obs.Step(step), slog.String("path", dir),
					slog.String("reason", "size_mismatch"))
				continue
			}
			if !s.tracked(step) {
				s.log.Info(fmt.Sprintf("发现 .done step %d", step), obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateReady))
			}
			s.enqueue(step, dir)
			continue
		}

		state := scanState{files: cur.files, bytes: cur.bytes, stable: 1}
		if seen && prev.files == cur.files && prev.bytes == cur.bytes {
			state.stable = prev.stable + 1
		} else if seen {
			s.log.Warn("扫描参数不一致，重新计数",
				obs.Task(s.opts.TaskID), obs.Step(step), slog.String("path", dir),
				slog.String("reason", "size_mismatch"))
		}
		s.mu.Lock()
		s.scans[step] = state
		s.mu.Unlock()
		if state.stable >= s.opts.ScanStable {
			s.enqueue(step, dir)
		}
	}
}

// enqueue 去重后把 step 放进保存队列。队列满不阻塞轮询，记一条 Warn。
func (s *Sidecar) enqueue(step int64, path string) bool {
	reason := ""
	s.mu.Lock()
	switch {
	case s.stopped || !s.accepting || s.failed:
		reason = "not_accepting"
	case s.completed[step]:
		reason = "completed"
	case s.queued[step]:
		reason = "queued"
	default:
		if _, ok := s.cache[step]; ok {
			reason = "cached"
		}
	}
	if reason != "" {
		s.mu.Unlock()
		s.logSkip(step, path, reason, slog.LevelDebug)
		return false
	}
	if len(s.queued) >= queueLimit {
		// 轮询 200ms 一次，队满会持续命中；限成 5 秒一条，避免刷屏。
		warn := time.Since(s.lastQueueWarn) >= 5*time.Second
		if warn {
			s.lastQueueWarn = time.Now()
		}
		s.mu.Unlock()
		if !warn {
			s.logSkip(step, path, "queue_full", slog.LevelDebug)
			return false
		}
		s.log.Warn("保存队列已满，跳过本轮",
			obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateReady),
			slog.String("result", "skipped"), slog.String("reason", "queue_full"),
			slog.Int("queue_depth", queueLimit))
		return false
	}
	s.queue = append(s.queue, saveJob{step: step, path: path})
	s.queued[step] = true
	depth := len(s.queued)
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default:
	}
	s.log.Info(fmt.Sprintf("step %d 入队", step),
		obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateReady),
		slog.Int("queue_depth", depth))
	return true
}

// saveLoop 串行处理队列，每次取最小 step。
func (s *Sidecar) saveLoop(ctx context.Context) {
	for {
		job, ok := s.nextJob()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
				continue
			}
		}
		err := s.processSave(ctx, job.step, job.path)
		s.finishJob(job.step)
		if err != nil {
			return
		}
	}
}

func (s *Sidecar) nextJob() (saveJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return saveJob{}, false
	}
	idx := 0
	for i := 1; i < len(s.queue); i++ {
		if s.queue[i].step < s.queue[idx].step {
			idx = i
		}
	}
	job := s.queue[idx]
	s.queue = append(s.queue[:idx], s.queue[idx+1:]...)
	return job, true
}

func (s *Sidecar) finishJob(step int64) {
	s.mu.Lock()
	delete(s.queued, step)
	s.mu.Unlock()
}

func (s *Sidecar) tracked(step int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queued[step] || s.completed[step] {
		return true
	}
	_, ok := s.cache[step]
	return ok
}

func (s *Sidecar) logSkip(step int64, path, reason string, level slog.Level) {
	attrs := []any{
		obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateReady),
		slog.String("result", "skipped"), slog.String("reason", reason),
	}
	if path != "" {
		attrs = append(attrs, slog.String("path", path))
	}
	s.log.Log(context.Background(), level, "跳过 step", attrs...)
}

// processSave 按 SaveDeadline 与 SaveRetryIntervals 重试保存。不可重试错误码
// 立即失败；最终失败会停止接收新 step，但保留工作目录（design 10.1「关键流程」）。
func (s *Sidecar) processSave(ctx context.Context, step int64, path string) error {
	start := time.Now()
	saveCtx, cancel := context.WithTimeout(ctx, s.opts.SaveDeadline)
	defer cancel()
	deadlineErr := fmt.Errorf("sidecar: 保存 step %d 超过总时限 %s: %w", step, s.opts.SaveDeadline, context.DeadlineExceeded)

	s.log.Info(fmt.Sprintf("上传开始 step %d", step),
		obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateUploading))

	attempts := len(s.opts.SaveRetryIntervals) + 1
	var lastErr error
	tried := 0
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if saveCtx.Err() != nil {
			lastErr = deadlineErr
			break
		}
		// retry 记的是真正打到插件上的次数：时限在循环开头就掐断本轮时，
		// 这次尝试没有发生，不能计进终态日志。
		tried = attempt
		res, err := s.saveAttempt(saveCtx, step, path)
		if err == nil {
			s.saveSucceeded(step, path, res, start)
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		lastErr = err
		s.log.Error("保存失败",
			append([]any{obs.Task(s.opts.TaskID), obs.Step(step), obs.Retry(attempt)}, obs.Err(err)...)...)
		if isPermanent(err) {
			break
		}
		if saveCtx.Err() != nil {
			lastErr = deadlineErr
			break
		}
		if attempt == attempts {
			break
		}
		delay := s.opts.SaveRetryIntervals[attempt-1]
		remaining := s.opts.SaveDeadline - time.Since(start)
		if remaining <= 0 {
			lastErr = deadlineErr
			break
		}
		if delay > remaining {
			delay = remaining
		}
		if !sleepCtx(saveCtx, delay) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = deadlineErr
			break
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.log.Error("保存最终失败，保留工作目录并停止接收新 step",
		append([]any{obs.Task(s.opts.TaskID), obs.Step(step), obs.Retry(tried)}, obs.Err(lastErr)...)...)
	s.mu.Lock()
	s.accepting = false
	s.failed = true
	s.mu.Unlock()
	return lastErr
}

func (s *Sidecar) saveAttempt(ctx context.Context, step int64, path string) (plugin.TransferResult, error) {
	opCtx, cancel := context.WithCancel(ctx)
	s.setCancel(cancel)
	defer func() {
		s.clearCancel()
		cancel()
	}()

	p, err := s.factory.Start(opCtx, s.progressFunc(step))
	if err != nil {
		return plugin.TransferResult{}, err
	}
	defer func() { _ = p.Close() }()

	return p.Save(opCtx, plugin.SaveParams{
		TaskID:    s.opts.TaskID,
		Rank:      s.opts.Rank,
		WorldSize: s.opts.WorldSize,
		Policy:    plugin.Policy{Mode: "step", Step: step},
		Backend:   s.opts.Backend,
		Step:      step,
		LocalPath: path,
		StatePath: s.statePath(step),
	})
}

// progressFunc 把插件进度降到 Debug，并按 1/100 采样，避免大文件刷日志。
func (s *Sidecar) progressFunc(step int64) func(done, total int64) {
	var n int
	return func(done, total int64) {
		n++
		if n%100 != 0 && done != total {
			return
		}
		s.log.Debug("保存进度",
			obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateUploading),
			obs.Bytes(done), slog.Int64(obs.FieldBytesTotal, total))
	}
}

// saveSucceeded 依次回填缓存、淘汰旧副本、删除工作卷副本。回填失败只记
// WARN，不影响已经完成的保存。
func (s *Sidecar) saveSucceeded(step int64, path string, res plugin.TransferResult, start time.Time) {
	s.mu.Lock()
	s.completed[step] = true
	s.mu.Unlock()

	if s.nvmeTaskDir != "" {
		backfillStart := time.Now()
		if err := s.backfill(step, path); err != nil {
			s.log.Warn("缓存回填失败",
				append([]any{obs.Task(s.opts.TaskID), obs.Step(step)}, obs.Err(err)...)...)
		} else {
			s.log.Info(fmt.Sprintf("缓存回填完成 step %d", step),
				obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateCached),
				obs.Bytes(res.Bytes), slog.Int64("duration_ms", time.Since(backfillStart).Milliseconds()))
		}
		s.evictCache()
	}

	if err := os.RemoveAll(path); err != nil {
		s.log.Warn("删除工作卷副本失败",
			append([]any{obs.Task(s.opts.TaskID), obs.Step(step), slog.String("path", path)}, obs.Err(err)...)...)
	} else {
		s.log.Info(fmt.Sprintf("删除工作卷副本 step %d", step), obs.Task(s.opts.TaskID), obs.Step(step))
	}
	if err := os.RemoveAll(s.statePath(step)); err != nil {
		s.log.Warn("删除续传状态失败",
			append([]any{obs.Task(s.opts.TaskID), obs.Step(step)}, obs.Err(err)...)...)
	}
	s.log.Info(fmt.Sprintf("上传完成 step %d", step),
		obs.Task(s.opts.TaskID), obs.Step(step), obs.Bytes(res.Bytes),
		slog.Int("files", res.Files), slog.Int64("duration_ms", time.Since(start).Milliseconds()),
		slog.String("result", "ok"))
}

// backfill 把工作卷 step 目录复制到 NVMe 临时目录，fsync 后 rename，最后
// 写 .complete（design 10.1「数据格式」、「关键流程」）。
func (s *Sidecar) backfill(step int64, src string) error {
	if s.nvmeTaskDir == "" {
		return nil
	}
	if _, ok := s.cacheStep(step); ok {
		return nil
	}
	if err := os.MkdirAll(s.nvmeTaskDir, 0o755); err != nil {
		return fmt.Errorf("创建缓存目录 %s: %w", s.nvmeTaskDir, err)
	}
	tmp, err := os.MkdirTemp(s.nvmeTaskDir, fmt.Sprintf("step-%d.tmp-", step))
	if err != nil {
		return fmt.Errorf("创建缓存临时目录: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	if err := copyTree(src, tmp); err != nil {
		return fmt.Errorf("复制到缓存临时目录: %w", err)
	}
	if err := syncDir(tmp); err != nil {
		return fmt.Errorf("fsync 缓存临时目录: %w", err)
	}

	final := filepath.Join(s.nvmeTaskDir, fmt.Sprintf("step-%d", step))
	if pathExists(filepath.Join(final, completeMarker)) {
		s.addCache(step, final)
		return nil
	}
	if err := os.RemoveAll(final); err != nil {
		return fmt.Errorf("清理旧缓存目录 %s: %w", final, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("rename 缓存目录 %s: %w", final, err)
	}
	if err := syncDir(s.nvmeTaskDir); err != nil {
		return fmt.Errorf("fsync 缓存父目录: %w", err)
	}
	if err := writeComplete(final); err != nil {
		return err
	}
	s.mu.Lock()
	s.cache[step] = final
	s.mu.Unlock()
	return nil
}

// evictCache 按 step 从旧到新淘汰，保留 NVMERetention 份；被本进程软链
// 引用的目录不删（design 10.1「关键流程」）。
func (s *Sidecar) evictCache() {
	s.mu.Lock()
	steps := make([]int64, 0, len(s.cache))
	for step := range s.cache {
		steps = append(steps, step)
	}
	s.mu.Unlock()
	sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })

	excess := len(steps) - s.opts.NVMERetention
	for _, step := range steps {
		if excess <= 0 {
			return
		}
		s.mu.Lock()
		dir, ok := s.cache[step]
		_, linked := s.links[step]
		s.mu.Unlock()
		if !ok || linked {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			s.log.Warn("淘汰缓存失败",
				append([]any{obs.Task(s.opts.TaskID), obs.Step(step), slog.String("path", dir)}, obs.Err(err)...)...)
			continue
		}
		s.mu.Lock()
		delete(s.cache, step)
		s.mu.Unlock()
		excess--
		s.log.Info(fmt.Sprintf("淘汰缓存 step %d", step),
			obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateCached),
			slog.String("reason", "retention"))
	}
}

// cacheStep 返回可用的缓存目录：.complete 存在且至少有一个数据文件。
func (s *Sidecar) cacheStep(step int64) (string, bool) {
	s.mu.Lock()
	dir, ok := s.cache[step]
	s.mu.Unlock()
	if !ok {
		return "", false
	}
	if !pathExists(filepath.Join(dir, completeMarker)) || !hasDataFiles(dir) {
		s.mu.Lock()
		if s.cache[step] == dir {
			delete(s.cache, step)
		}
		s.mu.Unlock()
		return "", false
	}
	return dir, true
}

func (s *Sidecar) addCache(step int64, dir string) {
	s.mu.Lock()
	s.cache[step] = dir
	s.mu.Unlock()
}

func observeStep(dir string) (stepObservation, error) {
	var obsStep stepObservation
	if pathExists(filepath.Join(dir, doneMarker)) {
		obsStep.hasDone = true
	}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if path == filepath.Join(dir, doneMarker) || path == filepath.Join(dir, completeMarker) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		obsStep.files++
		obsStep.bytes += info.Size()
		return nil
	})
	return obsStep, err
}

func hasDataFiles(dir string) bool {
	found := false
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if path == filepath.Join(dir, doneMarker) || path == filepath.Join(dir, completeMarker) {
			return nil
		}
		if info, err := d.Info(); err == nil && info.Mode().IsRegular() {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	return found
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == doneMarker || rel == completeMarker {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		target := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(link, target)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, target, info.Mode().Perm())
	})
}

func copyFile(src, dst string, perm fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func writeComplete(dir string) error {
	f, err := os.OpenFile(filepath.Join(dir, completeMarker), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("写 .complete: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync .complete: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("关闭 .complete: %w", err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func isPermanent(err error) bool {
	switch plugin.CodeOf(err) {
	case plugin.CodeNotFound, plugin.CodeInvalidArgument, plugin.CodeAuth:
		return true
	default:
		return false
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func randomSuffix() string {
	return fmt.Sprintf("%d-%d", os.Getpid(), tmpSeq.Add(1))
}
