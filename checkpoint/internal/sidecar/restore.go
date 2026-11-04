package sidecar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"traincache/checkpoint/internal/obs"
	"traincache/checkpoint/internal/plugin"
)

var errNoCheckpoint = errors.New("sidecar: 后端没有完整 checkpoint")

type restoreState struct {
	policy   plugin.Policy
	latest   bool
	switches int
	fallback bool
	step     int64
}

// restore 按 design 10.1 §6.4、§6.5 恢复。失败时进程不退出，探针保持
// 非 ready；超过 RestoreDeadline 后转为每 RestoreSlowInterval 一轮。
func (s *Sidecar) restore(ctx context.Context) error {
	st := &restoreState{policy: plugin.Policy{Mode: "latest"}, latest: true}
	if s.opts.CheckpointStep != nil {
		st.policy = plugin.Policy{Mode: "step", Step: *s.opts.CheckpointStep}
		st.latest = false
	}

	start := time.Now()
	attempt := 0
	slow := false
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attempt++
		attemptCtx := ctx
		cancel := func() {}
		if !slow {
			remaining := s.opts.RestoreDeadline - time.Since(start)
			if remaining <= 0 {
				slow = true
			} else {
				attemptCtx, cancel = context.WithTimeout(ctx, remaining)
			}
		}
		err := s.restoreOnce(attemptCtx, st)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if slow || time.Since(start) >= s.opts.RestoreDeadline {
			slow = true
			s.log.Error("恢复失败，等待下一轮慢速重试",
				append([]any{obs.Task(s.opts.TaskID), obs.Step(st.step), obs.Retry(attempt)}, obs.Err(err)...)...)
			if !sleepCtx(ctx, s.opts.RestoreSlowInterval) {
				return ctx.Err()
			}
			continue
		}
		s.log.Error("恢复失败",
			append([]any{obs.Task(s.opts.TaskID), obs.Step(st.step), obs.Retry(attempt)}, obs.Err(err)...)...)
		delay := s.opts.RestoreRetryIntervals[(attempt-1)%len(s.opts.RestoreRetryIntervals)]
		remaining := s.opts.RestoreDeadline - time.Since(start)
		if remaining <= 0 {
			continue
		}
		if delay > remaining {
			delay = remaining
		}
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
	}
}

// restoreOnce 完成一次 resolve 与恢复。缓存命中时建软链；未命中时调插件。
// CORRUPT 且策略为 latest 时换成上一个 step，最多换 2 次。
func (s *Sidecar) restoreOnce(ctx context.Context, st *restoreState) error {
	opCtx, cancel := context.WithCancel(ctx)
	s.setCancel(cancel)
	defer func() {
		s.clearCancel()
		cancel()
	}()

	p, err := s.factory.Start(opCtx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = p.Close() }()

	res, err := p.Resolve(opCtx, plugin.ResolveParams{
		TaskID:    s.opts.TaskID,
		Rank:      s.opts.Rank,
		WorldSize: s.opts.WorldSize,
		Policy:    st.policy,
		Backend:   s.opts.Backend,
	})
	if err != nil {
		return err
	}
	if !res.Found || !res.Complete {
		if s.opts.RequireCheckpoint || st.fallback {
			return fmt.Errorf("%w (task=%s step=%d complete=%v)", errNoCheckpoint, s.opts.TaskID, res.Step, res.Complete)
		}
		s.log.Info("后端没有完整 checkpoint，跳过恢复",
			obs.Task(s.opts.TaskID), obs.StateAttr(obs.StateDone),
			slog.String("result", "skipped"))
		return nil
	}
	if res.Step < 0 {
		return fmt.Errorf("sidecar: resolve 返回非法 step=%d", res.Step)
	}

	step := res.Step
	st.step = step
	s.log.Info(fmt.Sprintf("恢复选中版本 step %d", step),
		obs.Task(s.opts.TaskID), obs.Step(step), slog.Int("epoch", res.Epoch),
		obs.StateAttr(obs.StateRestoring))

	if dir, ok := s.cacheStep(step); ok {
		hit, linkErr := s.linkCache(step, dir)
		if linkErr != nil {
			s.log.Warn("缓存软链失败，改走后端恢复",
				append([]any{obs.Task(s.opts.TaskID), obs.Step(step), slog.String("path", dir)}, obs.Err(linkErr)...)...)
		} else if hit {
			s.markCompleted(step)
			s.log.Info(fmt.Sprintf("缓存命中 step %d，建立软链", step),
				obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateCached),
				slog.String("result", "ok"))
			return nil
		}
	}

	target := s.stepDir(step)
	// 容器重启后 target 可能已经存在：真目录说明上一次恢复成功，训练可能
	// 正在读它，不能覆盖；空目录与悬空软链是残骸，清掉后重来。
	if info, statErr := os.Lstat(target); statErr == nil {
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			if err := os.Remove(target); err != nil {
				return err
			}
		case info.IsDir() && hasDataFiles(target):
			s.markCompleted(step)
			s.log.Info(fmt.Sprintf("恢复目标 step %d 已存在，跳过重复恢复", step),
				obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateDone),
				slog.String("result", "skipped"), slog.String("reason", "target_exists"))
			return nil
		default:
			if err := os.RemoveAll(target); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(statErr) {
		return statErr
	}

	restoreStart := time.Now()
	tres, err := p.Restore(opCtx, plugin.RestoreParams{
		TaskID:     s.opts.TaskID,
		Rank:       s.opts.Rank,
		WorldSize:  s.opts.WorldSize,
		Policy:     plugin.Policy{Mode: "step", Step: step},
		Backend:    s.opts.Backend,
		Step:       step,
		TargetPath: target,
		StatePath:  s.statePath(step),
	})
	if err != nil {
		if plugin.CodeOf(err) == plugin.CodeCorrupt && st.latest && st.switches < 2 {
			next := step - 1
			if next >= 0 {
				st.switches++
				st.policy = plugin.Policy{Mode: "step", Step: next}
				st.fallback = true
				s.log.Warn("恢复发现损坏，换更早版本",
					obs.Task(s.opts.TaskID), obs.Step(step), slog.Int("next_step", int(next)),
					slog.String("reason", "corrupt"))
			}
		}
		return err
	}

	s.markCompleted(step)
	s.log.Info(fmt.Sprintf("恢复完成 step %d", step),
		obs.Task(s.opts.TaskID), obs.Step(step), slog.Int("epoch", res.Epoch),
		obs.Bytes(tres.Bytes), slog.Int("files", tres.Files),
		slog.Int64("duration_ms", time.Since(restoreStart).Milliseconds()),
		slog.String("result", "ok"))
	s.backfillAsync(step, target)
	return nil
}

func (s *Sidecar) markCompleted(step int64) {
	s.mu.Lock()
	s.completed[step] = true
	s.mu.Unlock()
}

// linkCache 在工作卷建软链指向 NVMe 目录：先建临时名再 rename。目标已
// 是同一软链时视为命中；目标是非软链时不覆盖训练数据。
func (s *Sidecar) linkCache(step int64, dir string) (bool, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return false, err
	}
	link := s.stepDir(step)
	if info, err := os.Lstat(link); err == nil {
		if info.Mode()&os.ModeSymlink == 0 {
			return false, fmt.Errorf("恢复目标 %s 已存在且不是软链", link)
		}
		old, err := os.Readlink(link)
		if err != nil {
			return false, err
		}
		if old == abs {
			s.recordLink(step, abs)
			return true, nil
		}
		if err := os.Remove(link); err != nil {
			return false, err
		}
	} else if !os.IsNotExist(err) {
		return false, err
	}

	temp := filepath.Join(s.opts.TaskDir, fmt.Sprintf(".step-%d.link-%s", step, randomSuffix()))
	if err := os.Symlink(abs, temp); err != nil {
		return false, err
	}
	s.recordLink(step, abs)
	if err := os.Rename(temp, link); err != nil {
		_ = os.Remove(temp)
		s.unrecordLink(step, abs)
		return false, err
	}
	return true, nil
}

func (s *Sidecar) recordLink(step int64, target string) {
	s.mu.Lock()
	s.links[step] = target
	s.mu.Unlock()
}

func (s *Sidecar) unrecordLink(step int64, target string) {
	s.mu.Lock()
	if s.links[step] == target {
		delete(s.links, step)
	}
	s.mu.Unlock()
}

// backfillAsync 在恢复成功后异步回填 NVMe，失败只记 WARN。
func (s *Sidecar) backfillAsync(step int64, src string) {
	if s.nvmeTaskDir == "" {
		return
	}
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		start := time.Now()
		if err := s.backfill(step, src); err != nil {
			s.log.Warn("缓存回填失败",
				append([]any{obs.Task(s.opts.TaskID), obs.Step(step)}, obs.Err(err)...)...)
			return
		}
		s.log.Info(fmt.Sprintf("缓存回填完成 step %d", step),
			obs.Task(s.opts.TaskID), obs.Step(step), obs.StateAttr(obs.StateCached),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()))
	}()
}
