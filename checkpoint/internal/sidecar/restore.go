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

// ErrRestoreGiveUp 标记「慢速重试到放弃期限仍未恢复成功」这一终态。Run 把它
// 原样向上返回，cmd/checkpointd 按 errors.Is 识别并以退出码 3 结束（design
// 10.1「可观测出口」、「关键流程」），与配置非法的退出码 2 分开：2 重拉一定没用，3 交给 Job 的
// backoffLimit 记账，重启次数用完才判 Failed。
var ErrRestoreGiveUp = errors.New("sidecar: 恢复放弃")

type restoreState struct {
	policy   plugin.Policy
	latest   bool
	switches int
	fallback bool
	step     int64
}

// restore 按 design 10.1「关键流程」与 10.2 D14 分三段恢复：
// RestoreDeadline 内密集重试；到点后每 RestoreSlowInterval 一轮慢速重试，
// 每轮写一条 ERROR；到 RestoreGiveUpAfter 记 reason=restore_give_up 的 ERROR
// 并返回 ErrRestoreGiveUp，让进程退出，不再无限重试。
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
			// 放弃判定放在慢速段开头：恢复期限到点后才可能进入慢速段，
			// 放在这里正好覆盖「慢速段跑满到放弃期限」的情形。
			if time.Since(start) >= s.opts.RestoreGiveUpAfter {
				s.log.Error("恢复放弃，进程以非零码退出",
					append([]any{obs.Task(s.opts.TaskID), obs.Step(st.step), obs.Retry(attempt),
						slog.String("reason", "restore_give_up")}, obs.Err(err)...)...)
				return fmt.Errorf("%w (task_id=%s, 已重试 %d 次): %w", ErrRestoreGiveUp, s.opts.TaskID, attempt, err)
			}
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
	// 插件进程的 stderr 不接进日志，协议里也没有日志帧，所以 D25 的
	// reason=rank_beyond_world_size WARN 只能由这里基于两个插件同语义的
	// MaxExtraRank 统一发出——两侧的行为与日志字段因此天然一致。
	// 不带 step：latest 模式跨多个 step 枚举，多余份未必落在选中的 step 上。
	if res.MaxExtraRank > 0 {
		s.log.Warn("存在超出 world_size 的多余 rank",
			obs.Task(s.opts.TaskID), slog.String("reason", "rank_beyond_world_size"),
			slog.Int("max_rank", res.MaxExtraRank), slog.Int("world_size", s.opts.WorldSize))
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
