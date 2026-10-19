package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
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
	// shards 是构建该快照所用 Manifest 的分片数（Shards 长度；D30：ShardsLoaded 的数据源）。
	shards int
}

// Catalog 是清单管理方（读侧）：内存维护 块→位置 映射，定时刷新，原子切换。
type Catalog struct {
	store   objectstore.Store
	dataset string // 数据集名（CURRENT/manifest/分片对象键均以此为前缀）
	refresh time.Duration

	// 可选回调在锁外调用；nil 表示不回调（7.1 §5.9）。
	onSnapshotChange func(dataset, snapID string)
	onSnapshotError  func(dataset string, err error)

	mu    sync.Mutex // 串行化 Refresh/LoadPrefixes/RefreshManifest（后台循环与手动刷新可能并发）
	state atomic.Pointer[snapshotState]
}

// NewCatalog 构造绑定到单个数据集 dataset 的 Catalog（D17：Refresh/Lookup 无 dataset
// 参数，数据集在构造时绑定）；cfg 的零值字段回退 DefaultConfig 默认值。
func NewCatalog(store objectstore.Store, dataset string, cfg Config) *Catalog {
	cfg = cfg.withDefaults()
	return &Catalog{
		store:            store,
		dataset:          dataset,
		refresh:          cfg.RefreshInterval,
		onSnapshotChange: cfg.OnSnapshotChange,
		onSnapshotError:  cfg.OnSnapshotError,
	}
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
	snapID, switched, err := c.refreshLocked(ctx)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if switched {
		c.notifySnapshotChange(snapID)
	}
	return nil
}

// refreshLocked 是 Refresh 的持锁主体：switched=false 表示 CURRENT 未变而跳过（不回调）。
func (c *Catalog) refreshLocked(ctx context.Context) (snapID string, switched bool, err error) {
	content, currentSnapID, manifestFP8, err := c.readCurrent(ctx)
	if err != nil {
		return "", false, err
	}
	if st := c.state.Load(); st != nil && st.raw == content {
		return "", false, nil // CURRENT 未变：跳过（不再下载 manifest）
	}
	m, err := c.loadManifest(ctx, currentSnapID, manifestFP8)
	if err != nil {
		return "", false, err
	}

	// 逐分片下载并验证指纹，构建完整新映射（此过程不触碰旧映射）。
	entries := make(map[entryKey]Entry)
	for _, ref := range m.Shards {
		es, err := c.loadShardEntries(ctx, ref)
		if err != nil {
			return "", false, err
		}
		for _, e := range es {
			entries[entryKey{path: e.Path, chunk: e.Chunk}] = e
		}
	}

	// 全部加载/验证通过后才一次性切换（失败路径绝不修改旧映射）。
	c.state.Store(&snapshotState{raw: content, snapID: m.SnapshotID, entries: entries, shards: len(m.Shards)})
	return m.SnapshotID, true, nil
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
	snapID, err := c.loadPrefixesLocked(ctx, prefixes)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	c.notifySnapshotChange(snapID)
	return nil
}

// loadPrefixesLocked 是 LoadPrefixes 的持锁主体；成功后返回新 snapID。
func (c *Catalog) loadPrefixesLocked(ctx context.Context, prefixes []string) (string, error) {
	_, currentSnapID, manifestFP8, err := c.readCurrent(ctx)
	if err != nil {
		return "", err
	}
	m, err := c.loadManifest(ctx, currentSnapID, manifestFP8)
	if err != nil {
		return "", err
	}

	// 多前缀取并集：同一分片对多个 prefix 匹配时只下载、加载一次。
	entries := make(map[entryKey]Entry)
	for _, ref := range m.Shards {
		if !shardMatchesAnyPrefix(ref, prefixes) {
			continue
		}
		es, err := c.loadShardEntries(ctx, ref)
		if err != nil {
			return "", err
		}
		for _, e := range es {
			entries[entryKey{path: e.Path, chunk: e.Chunk}] = e
		}
	}

	// 全部相关分片加载/验证通过后才一次性切换（失败路径绝不修改旧映射）；raw 置空见
	// snapshotState 注释；shards 记 Manifest 分片数（D30 口径为全量 Shards 长度，
	// 非本次前缀匹配到的分片数——manifest 本身已全量读取）。
	c.state.Store(&snapshotState{snapID: m.SnapshotID, entries: entries, shards: len(m.Shards)})
	return m.SnapshotID, nil
}

// List 按前缀枚举条目（D29）：读 CURRENT → 下载 manifest → 复用 D22 分片相关性判定只
// 下载、校验、加载相关分片（无关分片零下载）→ 收集 path 以 prefix 开头的条目 → 按
// (path, chunk) 字典序排序返回；无匹配返回空切片。prefix 为空串 = 全量。无 CURRENT
// 返回 ErrNoCurrent。只读方法：不触碰内存映射（Lookup 所见的既有映射不受影响，"失败
// 不动旧映射"恒成立），可与 Refresh/LoadPrefixes/Start 并发调用。
func (c *Catalog) List(ctx context.Context, prefix string) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, snapID, manifestFP8, err := c.readCurrent(ctx)
	if err != nil {
		return nil, err
	}
	m, err := c.loadManifest(ctx, snapID, manifestFP8)
	if err != nil {
		return nil, err
	}

	// 分片是加载单元、片内路径范围可跨前缀（D22）：片内不匹配 prefix 的条目在此剔除。
	entries := make([]Entry, 0)
	for _, ref := range m.Shards {
		if !shardMatchesPrefix(ref, prefix) {
			continue
		}
		es, err := c.loadShardEntries(ctx, ref)
		if err != nil {
			return nil, err
		}
		for _, e := range es {
			if strings.HasPrefix(e.Path, prefix) {
				entries = append(entries, e)
			}
		}
	}
	// 输出恒按 (path, chunk) 字典序排序（与分片构建同一定序），不依赖 manifest 的分片顺序。
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Path != entries[j].Path {
			return entries[i].Path < entries[j].Path
		}
		return entries[i].Chunk < entries[j].Chunk
	})
	return entries, nil
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

// ShardsLoaded 返回当前生效索引快照的 Manifest 分片数（Manifest.Shards 长度；D30）。
// 尚无快照（未 Start/Refresh/LoadPrefixes 成功）返回 0。纯读：与 Lookup 同口径的
// atomic.Pointer 快照读取，不取 mu、不长持锁，与 Refresh/LoadPrefixes 并发安全。
func (c *Catalog) ShardsLoaded() int {
	st := c.state.Load()
	if st == nil {
		return 0
	}
	return st.shards
}

// SnapID 原子读当前生效快照的 snapID；尚无任何快照（未成功执行 Start/Refresh/
// LoadPrefixes/RefreshManifest）返回空串。纯读：不取 mu、不访问对象存储，
// 与刷新路径并发安全。
func (c *Catalog) SnapID() string {
	st := c.state.Load()
	if st == nil {
		return ""
	}
	return st.snapID
}

// RefreshManifest 是无活跃前缀时的轻量跟随（7.1 §5.9）：读 CURRENT 并下载、校验
// manifest（校验链同 Refresh：manifest 指纹与 snapshot_id 必须与 CURRENT 一致），
// 不下载任何分片；成功后原子切换 snapID 与空 entries（旧映射整体作废）。
//
// 前置条件：与 Refresh/LoadPrefixes 互斥使用，由 Catalog 内部 mu 串行化。
// 错误语义：ctx 已取消返回 ctx.Err()；CURRENT 不存在返回 ErrNoCurrent；manifest
// 缺失/损坏、指纹不符或 snapshot_id 不符返回相应包装错误（指纹不符为
// ErrInvalidShard）。任何失败都不切换、不清空，旧映射保持可用。
// 成功后、锁外调用 Config.OnSnapshotChange(dataset, snapID)（若已配置）。
func (c *Catalog) RefreshManifest(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	snapID, err := c.refreshManifestLocked(ctx)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	c.notifySnapshotChange(snapID)
	return nil
}

// refreshManifestLocked 是 RefreshManifest 的持锁主体：只下载并校验 manifest，切换为
// snapID + 空 entries（raw 置空：后续 Refresh 会因内容不匹配而全量重建）。
func (c *Catalog) refreshManifestLocked(ctx context.Context) (string, error) {
	_, currentSnapID, manifestFP8, err := c.readCurrent(ctx)
	if err != nil {
		return "", err
	}
	m, err := c.loadManifest(ctx, currentSnapID, manifestFP8)
	if err != nil {
		return "", err
	}
	c.state.Store(&snapshotState{
		snapID:  m.SnapshotID,
		entries: make(map[entryKey]Entry),
		shards:  len(m.Shards),
	})
	return m.SnapshotID, nil
}

// Follow 是阻塞式快照跟随循环（7.1 §5.9）：每 Config.RefreshInterval 检查一次
// CURRENT；未变则跳过本轮，变更后按 prefixes() 的返回值选择加载方式——非空调用
// LoadPrefixes，空调用 RefreshManifest。prefixes 每轮调用，nil 按空处理。
//
// 前置条件：与 Start/Refresh/LoadPrefixes 互斥使用，加载由 Catalog 内部 mu 串行化。
// 后置条件：单轮加载失败不退出、不切换、不清空，记录错误后保持旧映射继续下一轮；
// 仅当 ctx 取消时返回 ctx.Err()。成功切换后由被调方法在锁外触发
// Config.OnSnapshotChange（若已配置）。
func (c *Catalog) Follow(ctx context.Context, prefixes func() []string) error {
	ticker := time.NewTicker(c.refresh)
	defer ticker.Stop()
	for {
		if err := c.followOnce(ctx, prefixes); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// 失败保持旧映射并继续下一轮（7.1 §5.9）。
			c.notifySnapshotError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// followOnce 执行一轮廉价检查：只读 CURRENT（不下载 manifest）；snapID 未变即跳过，
// 变化后按 prefixes() 选择加载方式。ctx 取消时返回 ctx.Err()。
func (c *Catalog) followOnce(ctx context.Context, prefixes func() []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, snapID, _, err := c.readCurrent(ctx)
	if err != nil {
		return err
	}
	if st := c.state.Load(); st != nil && st.snapID == snapID {
		return nil
	}
	var ps []string
	if prefixes != nil {
		ps = prefixes()
	}
	if len(ps) > 0 {
		return c.LoadPrefixes(ctx, ps)
	}
	return c.RefreshManifest(ctx)
}

// notifySnapshotChange 在锁外调用成功切换回调；nil 安全，panic 不向调用方传播。
func (c *Catalog) notifySnapshotChange(snapID string) {
	if c.onSnapshotChange == nil {
		return
	}
	defer func() { _ = recover() }()
	c.onSnapshotChange(c.dataset, snapID)
}

// notifySnapshotError 在锁外调用加载失败回调；nil 安全，panic 不向调用方传播。
func (c *Catalog) notifySnapshotError(err error) {
	if c.onSnapshotError == nil {
		return
	}
	defer func() { _ = recover() }()
	c.onSnapshotError(c.dataset, err)
}
