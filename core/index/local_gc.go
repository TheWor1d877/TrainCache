package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// LocalGC 回收节点本地 CAS 里的孤儿块副本（docs/5.1-design.md §11.5、§7.3）。
//
// 本地 CAS 是节点级全局、内容寻址，块不按数据集分区：一个块被几个数据集引用、
// 属于哪个数据集，只存在于索引里。因此「本地副本能不能删」只能按节点算——
// 按单个数据集算会把别的数据集的块当成孤儿删掉。
//
// 活块集合 = 各数据集当前快照（CURRENT）引用的块 ∪ 有效读者 claim 保护的快照引用的
// 块。删除语义（§11.4：删除 = 新快照不含）落在集合差上：条目被删后新快照不再引用
// 它的块，本地副本在本轮清扫里回收；别的节点还在读旧快照时，读者 claim 把旧快照
// 的块留在集合里（D33）。
//
// fail-safe：任一数据集的索引读不到（确定不存在除外）或损坏，整轮不删任何块——
// 此时活块集合算不出来，可能删掉的范围是本地全部副本。删错的代价按读取状态分两种：
// 已经打开的句柄照常读完（unlink 不影响 fd），下一轮 fetch 发现块不在再回源重拉。
type LocalGC struct {
	store objectstore.Store
	cs    *cas.Store
	cfg   Config
	now   func() time.Time // 可注入时钟（测试用），默认 time.Now

	mu sync.Mutex // 同一节点同一时刻只跑一轮清扫
}

// NewLocalGC 构造 LocalGC；cfg 的零值字段回退 DefaultConfig 默认值。
func NewLocalGC(store objectstore.Store, cs *cas.Store, cfg Config) *LocalGC {
	return &LocalGC{store: store, cs: cs, cfg: cfg.withDefaults(), now: time.Now}
}

// Sweep 计算节点活块集合，删除不在集合内的本地块，返回删除数。
//
// datasets 是本轮要考虑的数据集名全集（远端枚举 ∪ 本地已知）。漏掉一个数据集，
// 它的块会被当孤儿删掉——调用方必须保证枚举完整；枚举失败时调用方应当整轮放弃，
// 不调用本方法。
//
// 删除动作走 cas.Evict（删本地副本，不看引用计数；正在读的块 ErrInUse 跳过）。
func (g *LocalGC) Sweep(ctx context.Context, datasets []string) (removed int, err error) {
	if g.cs == nil || g.store == nil {
		return 0, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	local, err := g.cs.List()
	if err != nil {
		return 0, fmt.Errorf("index: local gc list blocks: %w", err)
	}
	if len(local) == 0 {
		return 0, nil
	}
	// 活块判定按本地块集合做：内存占用等于本地块数，与索引条目总数无关。
	localSet := make(map[cas.Fingerprint]struct{}, len(local))
	for _, fp := range local {
		localSet[fp] = struct{}{}
	}
	live := make(map[cas.Fingerprint]struct{})
	for _, dataset := range datasets {
		if dataset == "" {
			continue
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if err := g.markDatasetLive(ctx, dataset, localSet, live); err != nil {
			return 0, err
		}
	}

	for _, fp := range local {
		if _, ok := live[fp]; ok {
			continue
		}
		switch err := g.cs.Evict(fp); {
		case err == nil:
			removed++
		case errors.Is(err, cas.ErrInUse), errors.Is(err, cas.ErrNotFound):
			// 正在被读或已被并发删除：留待下一轮。
		default:
			return removed, fmt.Errorf("index: local gc evict block %s: %w", fp, err)
		}
	}
	return removed, nil
}

// markDatasetLive 把该 dataset 当前快照与有效 claim 保护快照引用的、本地已有的块
// 标记为活块。
func (g *LocalGC) markDatasetLive(ctx context.Context, dataset string, local, live map[cas.Fingerprint]struct{}) error {
	snapID, manifestFP8, err := g.currentSnapshot(ctx, dataset)
	if err != nil {
		return err
	}
	if snapID != "" {
		if err := g.markSnapshotLive(ctx, dataset, snapID, manifestFP8, local, live, true); err != nil {
			return err
		}
	}
	claims, err := ReadReaderClaims(ctx, g.store, dataset)
	if err != nil {
		return err
	}
	now := g.now()
	for _, claim := range claims {
		if now.Sub(claim.UpdatedAt) > g.cfg.ReaderGrace {
			continue
		}
		if claim.SnapID != "" && claim.SnapID != snapID {
			if err := g.markSnapshotLive(ctx, dataset, claim.SnapID, "", local, live, false); err != nil {
				return err
			}
		}
		if claim.PreviousSnapID != "" && claim.PreviousSnapID != snapID && !now.After(claim.PreviousUntil) {
			if err := g.markSnapshotLive(ctx, dataset, claim.PreviousSnapID, "", local, live, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// currentSnapshot 读 CURRENT，返回快照 ID 与 manifest 指纹；数据集没有 CURRENT
// （已删除或尚未提交）返回空串且不算错误——它不贡献任何活块。
func (g *LocalGC) currentSnapshot(ctx context.Context, dataset string) (snapID string, manifestFP8 string, err error) {
	raw, err := getObject(ctx, g.store, CurrentKey(dataset))
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return "", "", nil
		}
		return "", "", err
	}
	snapID, manifestFP8, err = parseCurrent(string(raw))
	if err != nil {
		return "", "", err
	}
	return snapID, manifestFP8, nil
}

// markSnapshotLive 标记一个快照引用的本地块。required 为真表示这是该 dataset 的
// 当前快照：manifest 或分片读不到即报错（fail-safe，整轮放弃）；为假表示这是
// claim 保护的旧快照：对象已被回收时跳过该快照（它引用的块不再是活块）。
func (g *LocalGC) markSnapshotLive(ctx context.Context, dataset, snapID, manifestFP8 string, local, live map[cas.Fingerprint]struct{}, required bool) error {
	m, err := loadManifest(ctx, g.store, dataset, snapID, manifestFP8)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			if !required {
				return nil
			}
			if g.cfg.OnSweepSkip != nil {
				g.cfg.OnSweepSkip(dataset, "manifest_unavailable")
			}
		}
		return err
	}
	for _, ref := range m.Shards {
		data, err := getObject(ctx, g.store, ref.Key)
		if err != nil {
			return err
		}
		if contentFingerprint(data) != ref.Fingerprint {
			return fmt.Errorf("%w: %s", ErrInvalidShard, ref.Key)
		}
		entries, err := unmarshalEntries(data)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if _, ok := local[e.Fingerprint]; !ok {
				continue
			}
			live[e.Fingerprint] = struct{}{}
		}
	}
	return nil
}
