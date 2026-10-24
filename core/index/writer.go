package index

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// Writer 是写入方 + 分片构建任务 + 提交方（docs/7 步骤 2–5）。
type Writer struct {
	store objectstore.Store
	cs    *cas.Store
	cfg   Config
}

// NewWriter 构造 Writer；cfg 的零值字段回退 DefaultConfig 默认值。
func NewWriter(store objectstore.Store, cs *cas.Store, cfg Config) *Writer {
	return &Writer{store: store, cs: cs, cfg: cfg.withDefaults()}
}

// Delete 是一条删除描述：Path 必填；Chunk >= 0 只删该 path 的这一块，
// Chunk == -1 删该 path 的全部块（D33）。
//
// 删除不产生 tombstone、不原地修改任何对象：commitOnce 本就把全量现存条目重新物化为
// 新快照，"删除"只是"新快照里没带上它"（design.md §11.4 第 1 点）。dels 中的重复描述
// （含同一 path 的 -1 与指定块并存）合法且坍缩为同一结果；指向不存在条目的描述是幂等
// no-op（§11.4 第 2 点）。
type Delete struct {
	Path  string
	Chunk int64
}

// Commit 将 entries（本次变更的增量 upsert）与当前快照合并，构建新快照并原子提交：
// 读 CURRENT → 读当前 manifest+受影响分片 → 合并重排（分片 ≤ MaxEntriesPerShard 条，
// 未变更分片物理复用同一对象键）→ 上传新分片与新 manifest（独立键，未提交不可见）→
// 验证（分片指纹、manifest 完整）→ PutIfMatch 更新 CURRENT（内容 "<snapID> <manifestFP8>"，
// expectedETag = MD5(期望的旧 CURRENT 内容)）→ 冲突则重读最新快照重试。
// 新块本身（步骤 1）由调用方先行经 cas.Put / fetch.Fetch 落盘，Commit 负责索引侧。
//
// 条件提交冲突（objectstore.ErrConflict）时完整重走一次"读最新状态 → 合并 → 构建 →
// 上传 → 验证 → 提交"（每次重试前重新读取 CURRENT 的最新 ETag）；总尝试次数上限为
// CommitMaxAttempts（含首次，默认 5），耗尽仍未成功返回含尝试次数的错误。
//
// 等价于 CommitChanges(ctx, dataset, entries, nil)，行为与 M4/M5 建立的契约逐字一致。
func (w *Writer) Commit(ctx context.Context, dataset string, entries []Entry) (snapID string, err error) {
	return w.CommitChanges(ctx, dataset, entries, nil)
}

// CommitChanges 是 Commit 的扩展形态：puts 与 dels 可同时出现（改名 = put 新 + delete 旧）。
// 合并规则：new = filter(old, dels) ∪ puts，同 (path, chunk) 冲突时 puts 赢（D33、
// design.md §11.4 第 3 点）——dels 只过滤现有条目，随后叠加的 puts 始终生效。
//
// 其余流程（排序、切分、未变更分片物理复用、验证、PutIfMatch、冲突重试）与 Commit 逐字
// 一致，同样受 CommitMaxAttempts 的冲突重试上限约束。
//
// dels 先于任何对象存储访问做整体校验：任一条描述 Path 为空（空 path + Chunk == -1
// 会清空整个索引，CLI 的 --prefix 正是为此展开成显式 path 列表）或 Chunk < -1 时直接
// 报错返回，错误信息点名出错的删除描述，且本次调用不产生任何写入（无分片、无 manifest、
// 无 CURRENT 更新）。puts 的校验口径与 Commit 保持不变。
func (w *Writer) CommitChanges(ctx context.Context, dataset string,
	puts []Entry, dels []Delete) (snapID string, err error) {
	if err := validateDeletes(dels); err != nil {
		return "", err
	}
	var lastErr error
	for attempt := 1; attempt <= w.cfg.CommitMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		snapID, err = w.commitOnce(ctx, dataset, puts, dels)
		if err == nil {
			return snapID, nil
		}
		if !errors.Is(err, objectstore.ErrConflict) {
			return "", err
		}
		lastErr = err
	}
	return "", fmt.Errorf("index: commit conflict after %d attempts: %w", w.cfg.CommitMaxAttempts, lastErr)
}

// validateDeletes 校验删除描述列表：Path 必填、Chunk 只能取 -1（该 path 全部块）或
// >= 0（该块）；首个非法描述即报错并点名它（dels 允许重复描述，此处不做去重）。
func validateDeletes(dels []Delete) error {
	for i, d := range dels {
		if d.Path == "" {
			return fmt.Errorf("index: invalid delete %d: empty path (chunk %d)", i, d.Chunk)
		}
		if d.Chunk < -1 {
			return fmt.Errorf("index: invalid delete %d: %s chunk %d (want -1 or >= 0)", i, d.Path, d.Chunk)
		}
	}
	return nil
}

// commitOnce 是一次提交尝试：读 CURRENT 的最新 ETag 与内容 → 读当前 manifest 与受影响
// 分片 → 按 dels 过滤现有条目后合并 puts → 构建新分片（未变更分片复用旧键）→ 上传 →
// 验证 → PutIfMatch。ErrConflict 由 CommitChanges 的循环基于最新快照重试处理。
func (w *Writer) commitOnce(ctx context.Context, dataset string, puts []Entry, dels []Delete) (snapID string, err error) {
	// 记录本次 attempt 新 Put 的分片键与 manifest 键；任何失败（含冲突重试前、
	// 验证失败）都 best-effort 删除它们。复用的旧分片键不在其中，绝不删除。
	var uploaded []string
	defer func() {
		if err != nil {
			w.deleteUploaded(uploaded)
		}
	}()

	curKey := CurrentKey(dataset)
	expectedETag := ""

	// 1) 读 CURRENT：ErrNotFound = 空数据集首次提交，expectedETag=""（If-None-Match: *）。
	etag, err := w.store.GetETag(ctx, curKey)
	switch {
	case err == nil:
		expectedETag = etag
	case errors.Is(err, objectstore.ErrNotFound):
	default:
		return "", fmt.Errorf("index: read CURRENT etag: %w", err)
	}

	// 2) 有当前快照时读旧 manifest 与全部分片，解析出现有 entries。
	var oldEntries []Entry
	oldShards := make(map[string]ShardRef) // 旧分片内容指纹 → 分片引用（复用判定）
	if expectedETag != "" {
		raw, err := getObject(ctx, w.store, curKey)
		if err != nil {
			return "", err
		}
		oldSnapID, oldManifestFP8, err := parseCurrent(string(raw))
		if err != nil {
			return "", err
		}
		manifest, err := w.loadManifest(ctx, dataset, oldSnapID, oldManifestFP8)
		if err != nil {
			return "", err
		}
		for _, ref := range manifest.Shards {
			data, err := getObject(ctx, w.store, ref.Key)
			if err != nil {
				return "", err
			}
			if contentFingerprint(data) != ref.Fingerprint {
				return "", fmt.Errorf("%w: %s", ErrInvalidShard, ref.Key)
			}
			es, err := unmarshalEntries(data)
			if err != nil {
				return "", err
			}
			if int64(len(es)) != ref.Entries {
				return "", fmt.Errorf("index: shard %s has %d entries, manifest says %d", ref.Key, len(es), ref.Entries)
			}
			oldEntries = append(oldEntries, es...)
			oldShards[ref.Fingerprint] = ref
		}
	}

	// 3) 合并：new = filter(old, dels) ∪ puts。删除描述先归一（Chunk == -1 收进整路径
	//    集合，其余收进 (path, chunk) 集合，重复描述自然坍缩），只作用于现有条目；
	//    随后叠加 puts（同 (path, chunk) 冲突时 puts 赢）。本次不涉及的现有条目一律保留。
	wholePath := make(map[string]struct{}, len(dels))
	specific := make(map[entryKey]struct{}, len(dels))
	for _, d := range dels {
		if d.Chunk == -1 {
			wholePath[d.Path] = struct{}{}
			continue
		}
		specific[entryKey{path: d.Path, chunk: d.Chunk}] = struct{}{}
	}
	merged := make(map[entryKey]Entry, len(oldEntries)+len(puts))
	for _, e := range oldEntries {
		k := entryKey{path: e.Path, chunk: e.Chunk}
		if _, ok := wholePath[e.Path]; ok {
			continue
		}
		if _, ok := specific[k]; ok {
			continue
		}
		merged[k] = e
	}
	for _, e := range puts {
		merged[entryKey{path: e.Path, chunk: e.Chunk}] = e
	}
	all := make([]Entry, 0, len(merged))
	for _, e := range merged {
		all = append(all, e)
	}

	// 4) 重排切分：内容指纹未变的分片复用旧 ShardRef（对象键不变、不重传），
	//    变了的分片用新键 shard-<fp8>.idx。
	shards, err := buildShards(all, w.cfg.MaxEntriesPerShard)
	if err != nil {
		return "", err
	}
	newSnapID, err := newSnapID()
	if err != nil {
		return "", err
	}
	type upload struct {
		key  string
		data []byte
	}
	refs := make([]ShardRef, 0, len(shards))
	var uploads []upload
	for _, sh := range shards {
		// MinPath/MaxPath 恒按本次构建的分片内容填充（D22）：命中复用（内容指纹相同）时
		// 重算值与旧值一致，同时覆盖旧 manifest 可能缺失的字段。
		if old, ok := oldShards[sh.fp]; ok {
			refs = append(refs, ShardRef{
				Key:         old.Key,
				MinPath:     sh.minPath,
				MaxPath:     sh.maxPath,
				Entries:     old.Entries,
				Fingerprint: old.Fingerprint,
			})
			continue
		}
		key := ShardKey(dataset, newSnapID, sh.fp[:16])
		refs = append(refs, ShardRef{
			Key:         key,
			MinPath:     sh.minPath,
			MaxPath:     sh.maxPath,
			Entries:     int64(sh.count),
			Fingerprint: sh.fp,
		})
		uploads = append(uploads, upload{key: key, data: sh.data})
	}

	// 5) 上传新分片与新 manifest（独立对象键，CURRENT 未更新前对读侧不可见）。
	for _, up := range uploads {
		uploaded = append(uploaded, up.key)
		if _, err := w.store.Put(ctx, up.key, bytes.NewReader(up.data), int64(len(up.data))); err != nil {
			return "", fmt.Errorf("index: upload shard %s: %w", up.key, err)
		}
	}
	manifest := Manifest{SnapshotID: newSnapID, CreatedAt: time.Now().UTC(), Shards: refs}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("index: encode manifest: %w", err)
	}
	manifestKey := ManifestKey(dataset, newSnapID)
	uploaded = append(uploaded, manifestKey)
	if _, err := w.store.Put(ctx, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes))); err != nil {
		return "", fmt.Errorf("index: upload manifest: %w", err)
	}
	manifestFP8 := fp8(manifestBytes)

	// 6) 提交前四步验证（design.md §7.1 第 4 点）：
	//    ① 新 manifest 已上传成功（读回内容指纹一致）；
	//    ② manifest 引用的所有分片可下载且内容指纹一致；
	//    ③ 分片条目数与其上限相符；
	//    ④ 本次 puts 引用的每个指纹经 cas.Open 校验通过。
	back, err := getObject(ctx, w.store, manifestKey)
	if err != nil {
		return "", err
	}
	if fp8(back) != manifestFP8 {
		return "", fmt.Errorf("index: manifest verify mismatch at %s", manifestKey)
	}
	for _, ref := range refs {
		data, err := getObject(ctx, w.store, ref.Key)
		if err != nil {
			return "", err
		}
		if contentFingerprint(data) != ref.Fingerprint {
			return "", fmt.Errorf("%w: %s", ErrInvalidShard, ref.Key)
		}
		es, err := unmarshalEntries(data)
		if err != nil {
			return "", err
		}
		if int64(len(es)) != ref.Entries || len(es) > w.cfg.MaxEntriesPerShard {
			return "", fmt.Errorf("index: shard %s has %d entries (manifest %d, limit %d)",
				ref.Key, len(es), ref.Entries, w.cfg.MaxEntriesPerShard)
		}
	}
	seen := make(map[cas.Fingerprint]struct{}, len(puts))
	for _, e := range puts {
		if _, ok := seen[e.Fingerprint]; ok {
			continue
		}
		seen[e.Fingerprint] = struct{}{}
		r, err := w.cs.Open(e.Fingerprint)
		if err != nil {
			return "", fmt.Errorf("index: verify block %s: %w", e.Fingerprint, err)
		}
		if err := r.Close(); err != nil {
			return "", fmt.Errorf("index: close block %s: %w", e.Fingerprint, err)
		}
	}

	// 7) 原子提交：PutIfMatch(CURRENT, "<snapID> <manifestFP8>", expectedETag)。
	content := formatCurrent(newSnapID, manifestFP8)
	if _, err := w.store.PutIfMatch(ctx, curKey, strings.NewReader(content), int64(len(content)), expectedETag); err != nil {
		if errors.Is(err, objectstore.ErrConflict) {
			return "", fmt.Errorf("index: commit conflict on %s: %w", curKey, err)
		}
		return "", fmt.Errorf("index: commit CURRENT: %w", err)
	}
	return newSnapID, nil
}

// deleteUploaded 对本次 attempt 新上传的对象做 best-effort 清理：逆序删除、整体
// 5s 超时、逐个忽略错误。清理失败不影响（也不覆盖）触发它的原始错误。
func (w *Writer) deleteUploaded(keys []string) {
	if len(keys) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := len(keys) - 1; i >= 0; i-- {
		_ = w.store.Delete(ctx, keys[i])
	}
}

// loadManifest 读取快照清单并校验：内容指纹必须与 CURRENT 记录的 manifestFP8 一致，
// 且 snapshot_id 与 CURRENT 的 snapID 一致（校验链 CURRENT → manifest）。
func (w *Writer) loadManifest(ctx context.Context, dataset string, snapID string, manifestFP8 string) (*Manifest, error) {
	key := ManifestKey(dataset, snapID)
	data, err := getObject(ctx, w.store, key)
	if err != nil {
		return nil, err
	}
	if fp8(data) != manifestFP8 {
		return nil, fmt.Errorf("%w: manifest %s", ErrInvalidShard, key)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("index: decode manifest %s: %w", key, err)
	}
	if m.SnapshotID != snapID {
		return nil, fmt.Errorf("index: manifest snapshot_id %q does not match CURRENT %q", m.SnapshotID, snapID)
	}
	return &m, nil
}

// newSnapID 生成快照 ID："<16 位 hex unix-nanos>-<8 位 hex 随机>"。
// 时间前缀使同数据集内按字典序 ≈ 提交顺序（供 M6 GC 判定"最近快照"）；字符集仅
// [0-9a-f-]，均为对象键安全字符（裁决 c）。
func newSnapID() (string, error) {
	var r [4]byte
	if _, err := rand.Read(r[:]); err != nil {
		return "", fmt.Errorf("index: generate snapshot id: %w", err)
	}
	return fmt.Sprintf("%016x-%s", uint64(time.Now().UTC().UnixNano()), hex.EncodeToString(r[:])), nil
}
