package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"traincache/core/objectstore"
)

// snapshotState 是一次刷新构建出的内存映射（全量代或前缀代）；构建完成后不再修改，
// 经 atomic.Pointer 一次性切换发布（design.md §7.2）。
type snapshotState struct {
	// raw 是 CURRENT 原始内容（"CURRENT 未变则跳过"的判据）。LoadPrefixes 构建的局部
	// 映射置空：它不代表 CURRENT 的完整快照，混用模式下后续 Refresh 将全量重建。
	raw     string
	snapID  string
	entries map[entryKey]Entry
}

// Catalog 是清单管理方（读侧）：内存维护 块→位置 映射，定时刷新，原子切换。
type Catalog struct {
	store   objectstore.Store
	dataset string // 数据集名（CURRENT/manifest/分片对象键均以此为前缀）
	refresh time.Duration

	mu    sync.Mutex // 串行化 Refresh（后台循环与手动刷新可能并发）
	state atomic.Pointer[snapshotState]
}

// NewCatalog 构造绑定到单个数据集 dataset 的 Catalog（D17：Refresh/Lookup 无 dataset
// 参数，数据集在构造时绑定）；cfg 的零值字段回退 DefaultConfig 默认值。
func NewCatalog(store objectstore.Store, dataset string, cfg Config) *Catalog {
	return &Catalog{store: store, dataset: dataset, refresh: cfg.withDefaults().RefreshInterval}
}

// Start 启动后台刷新循环（每 refreshInterval 读一次 CURRENT）；ctx 取消即停止。
// 循环在调用方 goroutine 中运行（需要后台执行时由调用方 `go c.Start(ctx)` 启动），
// 返回即表示循环已退出。单次刷新失败（含 ErrNoCurrent、校验失败等）不退出循环：
// 经标准库 log.Printf 记录错误后继续下一轮；成功/失败均不影响原子切换语义。
func (c *Catalog) Start(ctx context.Context) {
	ticker := time.NewTicker(c.refresh)
	defer ticker.Stop()
	for {
		if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
			log.Printf("index: catalog refresh (dataset %q): %v", c.dataset, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Refresh 手动执行一次刷新：CURRENT 未变则跳过；变了则下载 manifest → 逐分片下载并
// 验证指纹 → 构建新映射 → atomic.Pointer 一次性切换（绝不边加载边改旧映射）。
//
// 校验链（裁决 b）：CURRENT 中的 manifestFP8 与 manifest 内容 xxh3 前 8 字节 hex 比对；
// 逐分片重算完整指纹与 ShardRef.Fingerprint 比对；任一不符返回 ErrInvalidShard。
// 失败时旧映射保持可用：新映射完整构建成功后才切换。
func (c *Catalog) Refresh(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	content, snapID, manifestFP8, err := c.readCurrent(ctx)
	if err != nil {
		return err
	}
	if st := c.state.Load(); st != nil && st.raw == content {
		return nil // CURRENT 未变：跳过（不再下载 manifest）
	}
	m, err := c.loadManifest(ctx, snapID, manifestFP8)
	if err != nil {
		return err
	}

	// 逐分片下载并验证指纹，构建完整新映射（此过程不触碰旧映射）。
	entries := make(map[entryKey]Entry)
	for _, ref := range m.Shards {
		es, err := c.loadShardEntries(ctx, ref)
		if err != nil {
			return err
		}
		for _, e := range es {
			entries[entryKey{path: e.Path, chunk: e.Chunk}] = e
		}
	}

	// 全部加载/验证通过后才一次性切换（失败路径绝不修改旧映射）。
	c.state.Store(&snapshotState{raw: content, snapID: m.SnapshotID, entries: entries})
	return nil
}

// LoadPrefixes 按前缀加载索引分片（D23）：读 CURRENT → 下载 manifest（仅 ShardRef 列表，
// 体积小，全量）→ 仅下载、校验、加载与任一 prefix 路径前缀匹配相关的分片（D22 范围判定：
// 分片相关 ⟺ MaxPath ≥ prefix 且 (MinPath ≤ prefix 或 MinPath 以 prefix 开头)）→
// 构建局部内存映射 → atomic.Pointer 原子切换。空 prefixes = 不加载任何分片（映射为空）；
// 含空串元素 = 匹配一切（全量）。失败不动旧映射。与 Refresh/Start 为互斥使用模式
// （宿主二选一；本方法不做后台循环，前缀集变化由宿主再次调用）。
func (c *Catalog) LoadPrefixes(ctx context.Context, prefixes []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	_, snapID, manifestFP8, err := c.readCurrent(ctx)
	if err != nil {
		return err
	}
	m, err := c.loadManifest(ctx, snapID, manifestFP8)
	if err != nil {
		return err
	}

	// 多前缀取并集：同一分片对多个 prefix 匹配时只下载、加载一次。
	entries := make(map[entryKey]Entry)
	for _, ref := range m.Shards {
		if !shardMatchesAnyPrefix(ref, prefixes) {
			continue
		}
		es, err := c.loadShardEntries(ctx, ref)
		if err != nil {
			return err
		}
		for _, e := range es {
			entries[entryKey{path: e.Path, chunk: e.Chunk}] = e
		}
	}

	// 全部相关分片加载/验证通过后才一次性切换（失败路径绝不修改旧映射）；raw 置空见
	// snapshotState 注释。
	c.state.Store(&snapshotState{snapID: m.SnapshotID, entries: entries})
	return nil
}

// readCurrent 读取并解析 CURRENT（不存在 → ErrNoCurrent），返回原始内容、快照 ID 与
// manifest 指纹。Refresh 在解析后、下载 manifest 前做"CURRENT 未变则跳过"判定（design.md §7.2）。
func (c *Catalog) readCurrent(ctx context.Context) (content string, snapID string, manifestFP8 string, err error) {
	raw, err := getObject(ctx, c.store, CurrentKey(c.dataset))
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return "", "", "", ErrNoCurrent
		}
		return "", "", "", err
	}
	content = string(raw)
	snapID, manifestFP8, err = parseCurrent(content)
	if err != nil {
		return "", "", "", err
	}
	return content, snapID, manifestFP8, nil
}

// loadManifest 下载并校验当前 manifest：内容 xxh3 前 8 字节 hex 必须与 CURRENT 记录的
// manifestFP8 一致、snapshot_id 必须与 CURRENT 的 snapID 一致（校验链 CURRENT → manifest）。
func (c *Catalog) loadManifest(ctx context.Context, snapID string, manifestFP8 string) (*Manifest, error) {
	manifestKey := ManifestKey(c.dataset, snapID)
	manifestBytes, err := getObject(ctx, c.store, manifestKey)
	if err != nil {
		return nil, err
	}
	if fp8(manifestBytes) != manifestFP8 {
		return nil, fmt.Errorf("%w: manifest %s", ErrInvalidShard, manifestKey)
	}
	var m Manifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return nil, fmt.Errorf("index: decode manifest %s: %w", manifestKey, err)
	}
	if m.SnapshotID != snapID {
		return nil, fmt.Errorf("index: manifest snapshot_id %q does not match CURRENT %q", m.SnapshotID, snapID)
	}
	return &m, nil
}

// loadShardEntries 下载并校验单个分片：键的文件名部分必须与指纹一致，内容 xxh3 完整
// 指纹必须与 ShardRef.Fingerprint 一致（不符 → ErrInvalidShard），条目数必须与
// ShardRef.Entries 一致；返回分片内的条目。
func (c *Catalog) loadShardEntries(ctx context.Context, ref ShardRef) ([]Entry, error) {
	if len(ref.Fingerprint) != 32 || !strings.HasSuffix(ref.Key, "shard-"+ref.Fingerprint[:16]+".idx") {
		return nil, fmt.Errorf("index: manifest shard key %q does not match fingerprint %q", ref.Key, ref.Fingerprint)
	}
	data, err := getObject(ctx, c.store, ref.Key)
	if err != nil {
		return nil, err
	}
	if contentFingerprint(data) != ref.Fingerprint {
		return nil, fmt.Errorf("%w: %s", ErrInvalidShard, ref.Key)
	}
	es, err := unmarshalEntries(data)
	if err != nil {
		return nil, err
	}
	if int64(len(es)) != ref.Entries {
		return nil, fmt.Errorf("index: shard %s has %d entries, manifest says %d", ref.Key, len(es), ref.Entries)
	}
	return es, nil
}

// shardMatchesAnyPrefix 判断分片是否与 prefixes 中任一前缀相关（多前缀取并集；D22 判定式）。
func shardMatchesAnyPrefix(ref ShardRef, prefixes []string) bool {
	for _, p := range prefixes {
		if shardMatchesPrefix(ref, p) {
			return true
		}
	}
	return false
}

// shardMatchesPrefix 是 D22 的相关判定公式：分片相关 ⟺
// MaxPath ≥ prefix 且 (MinPath ≤ prefix 或 MinPath 以 prefix 开头)。
// 空串 prefix 恒相关（strings.HasPrefix(x, "") 恒真），即"匹配一切"。
func shardMatchesPrefix(ref ShardRef, prefix string) bool {
	return ref.MaxPath >= prefix && (ref.MinPath <= prefix || strings.HasPrefix(ref.MinPath, prefix))
}

// Lookup 查询块→位置映射；false = 当前快照中无该条目；ErrNoCurrent = 尚无任何快照。
// 纯内存查询（不访问对象存储），与 Refresh 并发安全。
func (c *Catalog) Lookup(ctx context.Context, path string, chunk int64) (Entry, bool, error) {
	st := c.state.Load()
	if st == nil {
		return Entry{}, false, ErrNoCurrent
	}
	e, ok := st.entries[entryKey{path: path, chunk: chunk}]
	if !ok {
		return Entry{}, false, nil
	}
	return e, true, nil
}
