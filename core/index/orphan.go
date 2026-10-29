package index

import (
	"context"
	"fmt"
	"time"

	"traincache/core/objectstore"
)

// FindOrphanBlocks 找出远端孤儿块：不被任何快照引用、且最后修改时间早于
// now-minAge 的块对象。
//
// 活块集合从全部快照（含保留窗口外的旧快照）重建——旧快照还没被 Sweep 删掉时，
// 它引用的块仍可能被读者打开，不能算孤儿。minAge 给在途发布留宽限期：发布器
// 先传块再 Commit，这期间块已存在但还没进任何快照。
//
// 任一 manifest 或分片缺失、损坏即返回错误且不返回部分结果：调用方据此放弃本轮
// 删除，避免把读不懂的索引当成"没有引用"。
func FindOrphanBlocks(ctx context.Context, store objectstore.Store, dataset string, minAge time.Duration) ([]objectstore.ObjectInfo, error) {
	prefix := snapshotsPrefix(dataset)
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("index: orphan sweep list %s: %w", prefix, err)
	}
	// Collector 的 loadManifest 允许传空 manifestFP8（只做存在性与 snapshot_id 校验），
	// Catalog 的同名方法要求校验指纹；这里只需要前者。cs 传 nil：不触发本地块回收。
	col := NewCollector(store, nil, DefaultConfig())
	cat := NewCatalog(store, dataset, DefaultConfig())
	live := make(map[string]struct{})
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		snapID := snapshotIDFromKey(prefix, key)
		if snapID == "" || seen[snapID] {
			continue
		}
		seen[snapID] = true
		manifest, err := col.loadManifest(ctx, dataset, snapID, "")
		if err != nil {
			return nil, fmt.Errorf("index: orphan sweep manifest %s: %w", snapID, err)
		}
		for _, ref := range manifest.Shards {
			entries, err := cat.loadShardEntries(ctx, ref)
			if err != nil {
				return nil, fmt.Errorf("index: orphan sweep shard %s: %w", ref.Key, err)
			}
			for _, e := range entries {
				live[objectstore.BlockKey(dataset, e.Path, e.Chunk)] = struct{}{}
			}
		}
	}

	infos, err := objectstore.ListInfo(ctx, store, dataset+"/blocks/")
	if err != nil {
		return nil, fmt.Errorf("index: orphan sweep list blocks: %w", err)
	}
	cutoff := time.Now().Add(-minAge)
	orphans := make([]objectstore.ObjectInfo, 0)
	for _, info := range infos {
		if _, ok := live[info.Key]; ok {
			continue
		}
		// 时间未知（后端没给 LastModified）时不动：宁可留着，不可误删。
		if info.LastModified.IsZero() || info.LastModified.After(cutoff) {
			continue
		}
		orphans = append(orphans, info)
	}
	return orphans, nil
}
