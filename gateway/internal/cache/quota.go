package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// blockFile 是一个正式名的块文件及其元数据文件名。
type blockFile struct {
	path  string
	meta  string
	size  int64
	mtime time.Time
}

// makeRoom 在写盘之前跑一遍三步淘汰（design 9.1 §6.8；9.2 D33）。
//
// need 是本次要写入的字节数。BeginPut 时还不知道块有多大，按 0 算；Writer.Close
// 时按实际写入量算。
func (s *Store) makeRoom(ctx context.Context, dataset string, need int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evictLocked(ctx, dataset, need)
}

// evictLocked 是三步淘汰本体，调用方持有 mu。
//
// 第 1 步：dataset 驻写字节超自己的上限时，按块文件 mtime 从老到新删这个
// dataset 里的块，.meta 同批删，删到回到上限内。
// 第 2 步：全局驻写字节仍超总上限时，按 last-touch 表挑最久没出现的 dataset，
// 删掉它的整个第一层目录，删到回到总上限内或只剩请求中这一个 dataset。
// 第 3 步：前两步做完仍放不下这个块时，返回 ErrQuota，本次不写盘。
func (s *Store) evictLocked(ctx context.Context, dataset string, need int64) error {
	// 第 1 步：dataset 超自己的上限 → 先写先删。
	if s.datasetLimit > 0 && s.datasetBytesLocked(dataset)+need > s.datasetLimit {
		if err := s.evictDatasetBlocksLocked(ctx, dataset, s.datasetLimit-need); err != nil {
			return err
		}
	}
	// 第 2 步：全局仍超总上限 → 按 last-touch 表删最久没出现的 dataset。
	if s.totalLimit > 0 {
		for s.total+need > s.totalLimit {
			if ctx != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			victim := s.leastTouchedLocked(dataset)
			if victim == "" {
				break
			}
			if err := s.removeDatasetLocked(victim); err != nil {
				return err
			}
		}
	}
	// 第 3 步：仍放不下 → 本次不写盘。
	if s.datasetLimit > 0 && s.datasetBytesLocked(dataset)+need > s.datasetLimit {
		return errQuotaWrap("dataset %q over limit: %d + %d > %d",
			dataset, s.datasetBytesLocked(dataset), need, s.datasetLimit)
	}
	if s.totalLimit > 0 && s.total+need > s.totalLimit {
		return errQuotaWrap("total over limit: %d + %d > %d", s.total, need, s.totalLimit)
	}
	return nil
}

// evictDatasetBlocksLocked 删掉 dataset 里最老的块，直到驻写字节回到 target
// 以内。块与 .meta 同批删。
func (s *Store) evictDatasetBlocksLocked(ctx context.Context, dataset string, target int64) error {
	blocks, err := listBlocks(filepath.Join(s.dir, dataset))
	if err != nil {
		return err
	}
	// 块文件 mtime 是写盘时间，第 1 步因此是先写先删。
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].mtime.Equal(blocks[j].mtime) {
			return blocks[i].path < blocks[j].path
		}
		return blocks[i].mtime.Before(blocks[j].mtime)
	})
	cur := int64(0)
	for _, b := range blocks {
		cur += b.size
	}
	// 账以表为准，盘上为准挑要删的块。两者不一致时取大的那个，宁可多删也不
	// 让占用超限。
	eff := s.datasetBytesLocked(dataset)
	if cur > eff {
		eff = cur
	}
	var removed int64
	for _, b := range blocks {
		if eff <= target {
			break
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := os.Remove(b.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Remove(b.meta); err != nil && !os.IsNotExist(err) {
			return err
		}
		eff -= b.size
		removed += b.size
	}
	// 回写账，并保持全局账等于各 dataset 之和。
	rest := s.datasetBytesLocked(dataset) - removed
	if rest < 0 {
		rest = 0
	}
	s.setDatasetBytesLocked(dataset, rest)
	s.total -= removed
	if s.total < 0 {
		s.total = 0
	}
	return nil
}

// removeDatasetLocked 删掉一个 dataset 的整个第一层目录。
func (s *Store) removeDatasetLocked(dataset string) error {
	if err := os.RemoveAll(filepath.Join(s.dir, dataset)); err != nil {
		return err
	}
	if st := s.datasets[dataset]; st != nil {
		s.total -= st.bytes
	}
	delete(s.datasets, dataset)
	return nil
}

// setDatasetBytesLocked 改写单个 dataset 的驻写字节，调用方持有 mu。
func (s *Store) setDatasetBytesLocked(dataset string, bytes int64) {
	st := s.datasets[dataset]
	if st == nil {
		st = &datasetState{}
		s.datasets[dataset] = st
	}
	st.bytes = bytes
}

// errQuotaWrap 给配额错误补上上下文，保留 errors.Is 判定。
func errQuotaWrap(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrQuota, fmt.Sprintf(format, args...))
}
