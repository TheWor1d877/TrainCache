package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// Collector 回收旧快照/分片/块（docs/7 步骤 7、design.md §7.3）。
// 内部持有 objectstore.Store（快照对象枚举与删除）与 *cas.Store（块枚举与删除）。
type Collector struct {
	store objectstore.Store
	cs    *cas.Store
	cfg   Config
	now   func() time.Time // 可注入时钟（测试用），默认 time.Now
}

// NewCollector 构造 Collector；cfg 的零值字段回退 DefaultConfig 默认值
// （RetainSnapshots 经 withDefaults 保证 ≥1，即至少保留 CURRENT）。
func NewCollector(store objectstore.Store, cs *cas.Store, cfg Config) *Collector {
	return &Collector{store: store, cs: cs, cfg: cfg.withDefaults(), now: time.Now}
}

// Sweep ① 计算活块集合 = 保留快照（CURRENT + 按 snapID 字典序最大的最近
// RetainSnapshots-1 个已提交快照）引用的指纹；② 扫描 cas 目录，删除不在活块集合
// 且 refcount=0、pin=0 的块；③ 删除不在保留窗口内、且不被保留快照复用的旧快照
// 清单与分片对象（CURRENT 对象绝不删）。
//
// 安全性：ErrNoCurrent（尚无任何快照）时不删任何东西；任一活快照的 manifest 或
// 分片下载/解析失败、分片指纹不符（ErrInvalidShard）时同样不删任何东西（先完成
// 全部读取与校验，再执行删除）。Sweep 不承诺与并发 Commit 安全：调用方应串行调用
// （Agent 层调度职责；并发下最坏为 Commit 冲突重试）。
func (c *Collector) Sweep(ctx context.Context, dataset string) (removedBlocks int, removedObjects int, err error) {
	prefix := snapshotsPrefix(dataset)
	keys, err := c.store.List(ctx, prefix)
	if err != nil {
		return 0, 0, fmt.Errorf("index: sweep list %s: %w", prefix, err)
	}

	// CURRENT：尚无快照时绝不删任何东西（所有块可能正被写入）。
	raw, err := getObject(ctx, c.store, CurrentKey(dataset))
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return 0, 0, ErrNoCurrent
		}
		return 0, 0, err
	}
	curSnapID, curManifestFP8, err := parseCurrent(string(raw))
	if err != nil {
		return 0, 0, err
	}

	// 保留窗口：{CURRENT} ∪ snapID 字典序最大的 RetainSnapshots-1 个
	// （snapID 为 16 位 hex unix-nanos 前缀定长 → 字典序 ≈ 提交顺序）。
	seen := make(map[string]struct{}, len(keys))
	candidates := make([]string, 0, len(keys))
	for _, key := range keys {
		id := snapshotIDFromKey(prefix, key)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if id != curSnapID {
			candidates = append(candidates, id)
		}
	}
	sort.Strings(candidates)
	protected := map[string]struct{}{curSnapID: {}}
	for i, keep := len(candidates)-1, c.cfg.RetainSnapshots-1; i >= 0 && keep > 0; i, keep = i-1, keep-1 {
		protected[candidates[i]] = struct{}{}
	}

	// 读者确认：有效 claim 的 SnapID 与未过宽限期的 PreviousSnapID 都计入
	// protected；UpdatedAt 超过 ReaderGrace 的 claim 整体忽略（读者已死）。
	// claims 读取失败或 JSON 损坏属于 fail-safe：本轮不做任何删除。
	claims, err := ReadReaderClaims(ctx, c.store, dataset)
	if err != nil {
		return 0, 0, err
	}
	now := c.now()
	for _, claim := range claims {
		if now.Sub(claim.UpdatedAt) > c.cfg.ReaderGrace {
			continue
		}
		if claim.SnapID != "" {
			protected[claim.SnapID] = struct{}{}
		}
		if claim.PreviousSnapID != "" && !now.After(claim.PreviousUntil) {
			protected[claim.PreviousSnapID] = struct{}{}
		}
	}

	// 读取全部 protected 快照的 manifest 与分片（一次下载两用）：得到活对象键集合
	// （D18 完整键，未变更分片跨快照复用）与活块集合；分片内容必须重算指纹校验
	// （ErrInvalidShard 时不删任何东西），否则损坏的分片会让其引用的块被误判为孤儿。
	// 任一 protected manifest 缺失/损坏：跳过本轮块删除（OnSweepSkip），但继续删除
	// 其他可删快照对象，不得整体停摆。
	liveKeys := map[string]struct{}{CurrentKey(dataset): {}}
	liveBlocks := make(map[cas.Fingerprint]struct{})
	skipBlocks := false
	for id := range protected {
		manifestFP8 := ""
		if id == curSnapID {
			manifestFP8 = curManifestFP8
		}
		m, err := c.loadManifest(ctx, dataset, id, manifestFP8)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, 0, ctxErr
			}
			if !skipBlocks {
				skipBlocks = true
				if c.cfg.OnSweepSkip != nil {
					c.cfg.OnSweepSkip(dataset, "manifest_unavailable")
				}
			}
			continue
		}
		liveKeys[ManifestKey(dataset, id)] = struct{}{}
		for _, ref := range m.Shards {
			data, err := getObject(ctx, c.store, ref.Key)
			if err != nil {
				return 0, 0, err
			}
			if contentFingerprint(data) != ref.Fingerprint {
				return 0, 0, fmt.Errorf("%w: %s", ErrInvalidShard, ref.Key)
			}
			entries, err := unmarshalEntries(data)
			if err != nil {
				return 0, 0, err
			}
			for _, e := range entries {
				liveBlocks[e.Fingerprint] = struct{}{}
			}
			liveKeys[ref.Key] = struct{}{}
		}
	}

	// 删除窗口外快照对象：非 protected 快照 ID 的对象中，凡不在活对象键集合
	// （完整键比对，保证被保留快照复用的分片保留）的一律删除；CURRENT 不在本
	// 枚举前缀内，天然不碰。
	for _, key := range keys {
		if _, ok := protected[snapshotIDFromKey(prefix, key)]; ok {
			continue
		}
		if _, ok := liveKeys[key]; ok {
			continue
		}
		if err := c.store.Delete(ctx, key); err != nil {
			return removedBlocks, removedObjects, fmt.Errorf("index: sweep delete %s: %w", key, err)
		}
		removedObjects++
	}

	// protected manifest 不可用时无法重建完整 liveBlocks，本轮跳过块删除。
	if skipBlocks {
		return removedBlocks, removedObjects, nil
	}

	// 块侧：枚举磁盘全量指纹，删除不在活块集合的孤儿块；仍被引用或正在被读
	// （ErrInUse）与已被并发删除（ErrNotFound）的跳过，留待下次。
	fps, err := c.cs.List()
	if err != nil {
		return removedBlocks, removedObjects, err
	}
	for _, fp := range fps {
		if _, ok := liveBlocks[fp]; ok {
			continue
		}
		switch err := c.cs.Remove(fp); {
		case err == nil:
			removedBlocks++
		case errors.Is(err, cas.ErrInUse), errors.Is(err, cas.ErrNotFound):
		default:
			return removedBlocks, removedObjects, fmt.Errorf("index: sweep remove block %s: %w", fp, err)
		}
	}
	return removedBlocks, removedObjects, nil
}

// loadManifest 下载并解析一个快照的 manifest；manifestFP8 非空时比对内容指纹
// （CURRENT → manifest 校验链），并校验 snapshot_id 与快照 ID 一致。
func (c *Collector) loadManifest(ctx context.Context, dataset, snapID, manifestFP8 string) (*Manifest, error) {
	key := ManifestKey(dataset, snapID)
	data, err := getObject(ctx, c.store, key)
	if err != nil {
		return nil, err
	}
	if manifestFP8 != "" && fp8(data) != manifestFP8 {
		return nil, fmt.Errorf("%w: manifest %s", ErrInvalidShard, key)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("index: decode manifest %s: %w", key, err)
	}
	if m.SnapshotID != snapID {
		return nil, fmt.Errorf("index: manifest snapshot_id %q does not match %q", m.SnapshotID, snapID)
	}
	return &m, nil
}

// snapshotsPrefix 返回数据集全部快照对象的前缀 "dataset/<name>/snapshots/"。
// SnapshotPrefix(dataset, "") 在空 snapID 之后仍接分隔符，结果为
// "dataset/<name>/snapshots//"（连续两个斜杠），此处折并为一个，否则 List 匹配不到任何键。
func snapshotsPrefix(dataset string) string {
	p := SnapshotPrefix(dataset, "")
	if strings.HasSuffix(p, "//") {
		p = p[:len(p)-1]
	}
	return p
}

// snapshotIDFromKey 从快照前缀下的对象键解析第一段快照 ID；非规范快照目录
// （第一段不是 newSnapID 生成的 "<16hex>-<8hex>"）返回空串——这类对象不参与
// 保留窗口计算，且因不属于任何活快照 ID 而在对象清理阶段被回收（垃圾对象）。
func snapshotIDFromKey(prefix, key string) string {
	rest, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return ""
	}
	id, _, _ := strings.Cut(rest, "/")
	if !isSnapshotID(id) {
		return ""
	}
	return id
}

// isSnapshotID 判断 s 是否为规范快照 ID："<16 位小写 hex>-<8 位小写 hex>"
// （newSnapID 生成；定长使字典序 ≈ 提交顺序）。
func isSnapshotID(s string) bool {
	if len(s) != 25 || s[16] != '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if i == 16 {
			continue
		}
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
