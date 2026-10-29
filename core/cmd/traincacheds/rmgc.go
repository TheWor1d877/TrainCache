package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// apply 返回本次调用是否真正落盘：只有显式 --yes 且未带 --dry-run 才执行。
func (c *cli) apply() bool {
	return c.yes && !c.dryRun
}

// executeRm 处理 rm <ds> (--path p[,p...] | --prefix p/) [--dry-run] [--yes]。
//
// 删除只走 Writer.CommitChanges：读 CURRENT 展开目标条目，把"这次不带它"做成新快照。
// core 的 Delete 保持精确 (path, chunk) 粒度，--prefix 的展开在 CLI 侧完成（D33）。
func (c *cli) executeRm(rest []string) int {
	if len(rest) != 1 {
		reportError(c.stderr, fmt.Errorf("rm needs exactly one dataset name, got %d argument(s)", len(rest)))
		c.usage()
		return exitUsageError
	}
	if !c.allowFlags("path", "prefix", "dry-run", "yes") {
		return exitUsageError
	}
	paths := splitPathList(c.path)
	switch {
	case len(paths) == 0 && c.prefix == "":
		reportError(c.stderr, errors.New("rm needs --path or --prefix"))
		c.usage()
		return exitUsageError
	case len(paths) > 0 && c.prefix != "":
		reportError(c.stderr, errors.New("rm takes --path or --prefix, not both"))
		c.usage()
		return exitUsageError
	}
	dataset := rest[0]
	return c.withStore(func(store objectstore.Store) error {
		entries, err := index.NewCatalog(store, dataset, index.Config{}).List(c.ctx, c.prefix)
		if err != nil {
			return entryListError(dataset, err)
		}
		if len(paths) > 0 {
			entries = filterEntriesByPath(entries, paths)
		}
		if len(entries) == 0 {
			fmt.Fprintln(c.stdout, "nothing to remove")
			return nil
		}
		npaths, nbytes := entryTotals(entries)
		if !c.apply() {
			fmt.Fprintf(c.stdout, "would remove entries=%d paths=%d bytes=%d\n", len(entries), npaths, nbytes)
			return nil
		}
		cs, cleanup, err := tempCAS()
		if err != nil {
			return err
		}
		defer cleanup()
		dels := make([]index.Delete, len(entries))
		for i, e := range entries {
			dels[i] = index.Delete{Path: e.Path, Chunk: e.Chunk}
		}
		snapID, err := index.NewWriter(store, cs, index.DefaultConfig()).CommitChanges(c.ctx, dataset, nil, dels)
		if err != nil {
			return fmt.Errorf("commit removal for dataset %s: %w", dataset, err)
		}
		fmt.Fprintf(c.stdout, "removed entries=%d paths=%d bytes=%d snapshot=%s\n", len(entries), npaths, nbytes, snapID)
		return nil
	})
}

// flagGiven 判断某个 flag 是否被显式给出（默认值不算）。
func (c *cli) flagGiven(name string) bool {
	given := false
	c.fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			given = true
		}
	})
	return given
}

// executeGC 处理 gc <ds> [--orphan-blocks] [--orphan-grace d] [--dry-run] [--yes]，只触发一次 Collector.Sweep。
//
// dry-run 用 dryRunStore 吞掉 Delete 并记录键：同一套回收规则走完，只是不落盘，
// 计数照常返回，所以预览结果与真正执行一致。
func (c *cli) executeGC(rest []string) int {
	if len(rest) != 1 {
		reportError(c.stderr, fmt.Errorf("gc needs exactly one dataset name, got %d argument(s)", len(rest)))
		c.usage()
		return exitUsageError
	}
	if !c.allowFlags("dry-run", "yes", "orphan-blocks", "orphan-grace") {
		return exitUsageError
	}
	if c.orphanGrace < 0 {
		reportError(c.stderr, fmt.Errorf("--orphan-grace must be >= 0, got %s", c.orphanGrace))
		c.usage()
		return exitUsageError
	}
	if !c.orphanBlocks && c.flagGiven("orphan-grace") {
		reportError(c.stderr, errors.New("--orphan-grace requires --orphan-blocks"))
		c.usage()
		return exitUsageError
	}
	dataset := rest[0]
	return c.withStore(func(store objectstore.Store) error {
		cs, cleanup, err := tempCAS()
		if err != nil {
			return err
		}
		defer cleanup()

		// 孤儿块先算后删，且用删除前的快照集合判定：dry-run 与真正执行看到同一批
		// 候选，结果可复现。快照回收后新变成孤儿的块留给下一轮。
		var orphans []objectstore.ObjectInfo
		if c.orphanBlocks {
			orphans, err = index.FindOrphanBlocks(c.ctx, store, dataset, c.orphanGrace)
			if err != nil {
				return err
			}
		}

		target := store
		var dry *dryRunStore
		if !c.apply() {
			dry = &dryRunStore{Store: store}
			target = dry
		}
		removedBlocks, removedObjects, err := index.NewCollector(target, cs, index.DefaultConfig()).Sweep(c.ctx, dataset)
		if errors.Is(err, index.ErrNoCurrent) {
			return noCurrentError(dataset)
		}
		if err != nil {
			return err
		}

		orphanBytes := int64(0)
		for _, o := range orphans {
			orphanBytes += o.Size
		}
		if c.apply() {
			for _, o := range orphans {
				if err := store.Delete(c.ctx, o.Key); err != nil {
					return fmt.Errorf("delete orphan block %s: %w", o.Key, err)
				}
			}
		}

		prefix := ""
		if dry != nil {
			prefix = "would remove"
		} else {
			prefix = "removed"
		}
		if c.orphanBlocks {
			fmt.Fprintf(c.stdout, "%s objects=%d local_blocks=%d orphan_blocks=%d orphan_bytes=%d\n",
				prefix, removedObjects, removedBlocks, len(orphans), orphanBytes)
			return nil
		}
		fmt.Fprintf(c.stdout, "%s objects=%d local_blocks=%d\n", prefix, removedObjects, removedBlocks)
		return nil
	})
}

// dryRunStore 把 Delete 变成记账：其余读写原样透传给真实存储，供 gc 预览。
type dryRunStore struct {
	objectstore.Store
	deleted []string
}

func (s *dryRunStore) Delete(_ context.Context, key string) error {
	s.deleted = append(s.deleted, key)
	return nil
}

// tempCAS 在系统临时目录建一个空 CAS，只为满足 Writer/Collector 的构造参数；
// rm/gc 不读写本节点缓存，退出时整目录删除。
func tempCAS() (*cas.Store, func(), error) {
	dir, err := os.MkdirTemp("", "traincacheds-")
	if err != nil {
		return nil, nil, fmt.Errorf("create temp CAS dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	cs, err := cas.New(dir, cas.VerifyXXH3)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("open temp CAS: %w", err)
	}
	return cs, cleanup, nil
}

// splitPathList 解析 --path 的逗号列表：去空白、丢空项、按首次出现去重。
func splitPathList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	seen := make(map[string]bool)
	paths := make([]string, 0, 4)
	for _, part := range strings.Split(raw, ",") {
		p := strings.TrimSpace(part)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		paths = append(paths, p)
	}
	return paths
}

// filterEntriesByPath 只保留 path 在给定集合里的条目。
func filterEntriesByPath(entries []index.Entry, paths []string) []index.Entry {
	want := make(map[string]bool, len(paths))
	for _, p := range paths {
		want[p] = true
	}
	kept := make([]index.Entry, 0, len(entries))
	for _, e := range entries {
		if want[e.Path] {
			kept = append(kept, e)
		}
	}
	return kept
}
