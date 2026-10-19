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
func (w *Writer) Commit(ctx context.Context, dataset string, entries []Entry) (snapID string, err error) {
	var lastErr error
	for attempt := 1; attempt <= w.cfg.CommitMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		snapID, err = w.commitOnce(ctx, dataset, entries)
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

// commitOnce 是一次提交尝试：读 CURRENT 的最新 ETag 与内容 → 读当前 manifest 与受影响
// 分片 → 合并本次 entries → 构建新分片（未变更分片复用旧键）→ 上传 → 验证 → PutIfMatch。
// ErrConflict 由 Commit 的循环基于最新快照重试处理。
func (w *Writer) commitOnce(ctx context.Context, dataset string, entries []Entry) (snapID string, err error) {
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

	// 3) 合并本次 entries（upsert：同 (path, chunk) 覆盖；本次不出现的现有条目保留）。
	merged := make(map[entryKey]Entry, len(oldEntries)+len(entries))
	for _, e := range oldEntries {
		merged[entryKey{path: e.Path, chunk: e.Chunk}] = e
	}
	for _, e := range entries {
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
	//    ④ 本次 entries 引用的每个指纹经 cas.Open 校验通过。
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
	seen := make(map[cas.Fingerprint]struct{}, len(entries))
	for _, e := range entries {
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
