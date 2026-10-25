// Package index 是路径索引层：快照、分片、提交、刷新、回收（design.md §5.4、§6）。
//
// 对象存储键布局（design.md §6.1、decisions.md D8）：
//
//	<bucket>/
//	└── dataset/<name>/
//	    ├── snapshots/<snapID>/manifest.json
//	    ├── snapshots/<snapID>/shard-<fp8>.idx      # fp8 = 分片内容 xxh3 前 8 字节 hex
//	    └── CURRENT                                 # 内容："<snapID> <manifestFP8>"（空格分隔，无换行）
//
// 快照逻辑上是完整快照、物理上复用旧分片（decisions.md D6）：内容未变的分片在对象存储中
// 只有一个对象（键含内容指纹），新快照的 manifest 直接引用首次创建它的快照目录下的对象，
// 对象键跨快照保持不变、无需重传。
package index

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"traincache/core/cas"
	"traincache/core/internal/digest"
	"traincache/core/objectstore"
)

// Config 是 index 包的全局配置（默认值见 DefaultConfig；design.md §8 配置项表）。
type Config struct {
	ChunkSize          int64         // 块切分粒度，默认 64 MiB（decisions D4）
	MaxEntriesPerShard int           // 分片条目上限，默认 100_000（doc/5.0 硬约束）
	CommitMaxAttempts  int           // 条件提交冲突重试上限，默认 5
	RetainSnapshots    int           // GC 保留窗口，默认 3（CURRENT + 2，decisions D14）
	RefreshInterval    time.Duration // Catalog 后台刷新周期，默认 5 s
	// ReaderGrace 是读者 claim 的有效期：UpdatedAt 超过该时长的 claim 整体忽略
	// （视为读者已死）。零值/负值回退为 2*RefreshInterval（默认 10 s，6.1 §5.11）。
	ReaderGrace time.Duration
	// OnSnapshotChange 在每次成功原子切换后、锁外调用；参数为数据集名与新 snapID。
	// nil 表示不回调；实现不得阻塞，回调 panic 不得向调用方传播。
	OnSnapshotChange func(dataset, snapID string)
	// OnSnapshotError 在 Follow 的单轮快照加载失败后、锁外调用；参数为数据集名与错误。
	// nil 表示不回调；实现不得阻塞，回调 panic 不得向调用方传播。
	OnSnapshotError func(dataset string, err error)
	// OnSweepSkip 在 GC 因需要保护的快照 manifest 不可用而跳过本轮块删除时调用，
	// 参数为数据集名与原因（如 "manifest_unavailable"）。nil 表示不回调（6.1 §5.11）。
	OnSweepSkip func(dataset, reason string)
}

// 默认值（design.md §8）。
const (
	defaultChunkSize          int64 = 64 << 20 // 64 MiB
	defaultMaxEntriesPerShard       = 100_000  // doc/5.0 硬约束
	defaultCommitMaxAttempts        = 5
	defaultRetainSnapshots          = 3
	defaultRefreshInterval          = 5 * time.Second
)

// DefaultConfig 返回 design.md §8 的默认配置。
func DefaultConfig() Config {
	return Config{
		ChunkSize:          defaultChunkSize,
		MaxEntriesPerShard: defaultMaxEntriesPerShard,
		CommitMaxAttempts:  defaultCommitMaxAttempts,
		RetainSnapshots:    defaultRetainSnapshots,
		RefreshInterval:    defaultRefreshInterval,
		ReaderGrace:        2 * defaultRefreshInterval,
	}
}

// withDefaults 返回补齐默认值后的配置：零值（及负值）字段回退 DefaultConfig 对应值
// （与 fetch.Options 的 M3 语义一致）。
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.ChunkSize <= 0 {
		c.ChunkSize = d.ChunkSize
	}
	if c.MaxEntriesPerShard <= 0 {
		c.MaxEntriesPerShard = d.MaxEntriesPerShard
	}
	if c.CommitMaxAttempts <= 0 {
		c.CommitMaxAttempts = d.CommitMaxAttempts
	}
	if c.RetainSnapshots <= 0 {
		c.RetainSnapshots = d.RetainSnapshots
	}
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = d.RefreshInterval
	}
	// ReaderGrace 依赖回退后的 RefreshInterval（默认 2*5s = 10s）。
	// OnSnapshotChange / OnSnapshotError / OnSweepSkip 为可选回调，nil 保持 nil，不覆盖。
	if c.ReaderGrace <= 0 {
		c.ReaderGrace = 2 * c.RefreshInterval
	}
	return c
}

// Entry 是索引表的一条记录：路径下第 chunk 块。
type Entry struct {
	Path        string          `json:"path"`
	Chunk       int64           `json:"chunk"`          // 块序号；块字节范围 = [chunk*ChunkSize, +Size)
	Size        int64           `json:"size"`           // 数据字节数（末块可不足）
	Fingerprint cas.Fingerprint `json:"fp"`             // CAS 指纹，JSON 编码为 hex
	Node        string          `json:"node,omitempty"` // 块实际落盘的节点名
}

// entryWire 是 Entry 的 JSON 中间结构：Fingerprint 编码为 32 字符小写 hex（design.md §6.2）。
type entryWire struct {
	Path  string `json:"path"`
	Chunk int64  `json:"chunk"`
	Size  int64  `json:"size"`
	FP    string `json:"fp"`
	Node  string `json:"node,omitempty"`
}

// MarshalJSON 实现 json.Marshaler：Fingerprint 编码为 32 字符小写 hex。
func (e Entry) MarshalJSON() ([]byte, error) {
	return json.Marshal(entryWire{
		Path:  e.Path,
		Chunk: e.Chunk,
		Size:  e.Size,
		FP:    e.Fingerprint.String(),
		Node:  e.Node,
	})
}

// UnmarshalJSON 实现 json.Unmarshaler：解析 32 字符 hex 指纹（长度/字符不符报错）。
func (e *Entry) UnmarshalJSON(b []byte) error {
	var w entryWire
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("index: decode entry: %w", err)
	}
	fp, err := cas.FingerprintFromHex(w.FP)
	if err != nil {
		return fmt.Errorf("index: decode entry fingerprint: %w", err)
	}
	e.Path, e.Chunk, e.Size, e.Fingerprint, e.Node = w.Path, w.Chunk, w.Size, fp, w.Node
	return nil
}

// ShardRef 是 manifest 中对一个索引分片的引用。
type ShardRef struct {
	Key         string `json:"key"`      // 分片对象键（内容寻址，文件名部分 shard-<fp8>.idx）；未变更分片跨快照复用同一对象键（D18）
	MinPath     string `json:"min_path"` // 分片内首条 Entry 的 Path（条目按 (path,chunk) 字典序；D22）
	MaxPath     string `json:"max_path"` // 分片内末条 Entry 的 Path（D22）
	Entries     int64  `json:"entries"`  // 该分片条目数
	Fingerprint string `json:"fp"`       // 分片内容 xxh3-128（hex），下载后完整性验证用
}

// Manifest 是快照清单。
type Manifest struct {
	SnapshotID string     `json:"snapshot_id"`
	CreatedAt  time.Time  `json:"created_at"`
	Shards     []ShardRef `json:"shards"`
}

// 对象存储键函数（布局见 design.md §6.1）：

// SnapshotPrefix 返回 dataset/<name>/snapshots/<id>/。
func SnapshotPrefix(dataset string, snapID string) string {
	return "dataset/" + dataset + "/snapshots/" + snapID + "/"
}

// ManifestKey 返回 dataset/<name>/snapshots/<id>/manifest.json。
func ManifestKey(dataset string, snapID string) string {
	return SnapshotPrefix(dataset, snapID) + "manifest.json"
}

// ShardKey 返回 dataset/<name>/snapshots/<id>/shard-<fp8>.idx
// （fp8 = 分片内容 xxh3-128 前 8 字节 hex）。
func ShardKey(dataset string, snapID string, fp8 string) string {
	return SnapshotPrefix(dataset, snapID) + "shard-" + fp8 + ".idx"
}

// CurrentKey 返回 dataset/<name>/CURRENT。
func CurrentKey(dataset string) string {
	return "dataset/" + dataset + "/CURRENT"
}

var (
	ErrInvalidShard = errors.New("index: shard fingerprint mismatch")
	ErrNoCurrent    = errors.New("index: CURRENT not found")
)

// entryKey 是"块→位置"映射的键：(path, chunk)。
type entryKey struct {
	path  string
	chunk int64
}

// contentFingerprint 返回内容的 xxh3-128 完整指纹（32 字符小写 hex，design.md §6.2）。
func contentFingerprint(b []byte) string {
	sum := digest.XXH3_128(b)
	return hex.EncodeToString(sum[:])
}

// fp8 返回内容的 xxh3-128 前 8 字节 hex（16 字符；分片对象键文件名与 CURRENT 中的
// manifestFP8 用）。
func fp8(b []byte) string {
	return contentFingerprint(b)[:16]
}

// shardSlice 是一段编码后的分片内容及其内容指纹与路径范围。
type shardSlice struct {
	data    []byte
	fp      string // 完整 32 字符小写 hex
	count   int    // 条目数
	minPath string // 首条 Entry 的 Path（排序后；D22）
	maxPath string // 末条 Entry 的 Path（排序后；D22）
}

// buildShards 将条目按 (path, chunk) 字典序排序后按条目数切分（range-based）并编码为
// JSON 数组分片，每片条目数 ≤ maxEntries（design.md §6.2）；每片的路径范围
// （首末条 Path）随分片一并返回（D22）。
func buildShards(entries []Entry, maxEntries int) ([]shardSlice, error) {
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Path != sorted[j].Path {
			return sorted[i].Path < sorted[j].Path
		}
		return sorted[i].Chunk < sorted[j].Chunk
	})
	var out []shardSlice
	for start := 0; start < len(sorted); start += maxEntries {
		end := start + maxEntries
		if end > len(sorted) {
			end = len(sorted)
		}
		data, err := json.Marshal(sorted[start:end])
		if err != nil {
			return nil, fmt.Errorf("index: encode shard: %w", err)
		}
		out = append(out, shardSlice{
			data:    data,
			fp:      contentFingerprint(data),
			count:   end - start,
			minPath: sorted[start].Path,
			maxPath: sorted[end-1].Path,
		})
	}
	return out, nil
}

// unmarshalEntries 解析一个分片（JSON 数组 of Entry）。
func unmarshalEntries(b []byte) ([]Entry, error) {
	var entries []Entry
	if err := json.Unmarshal(b, &entries); err != nil {
		return nil, fmt.Errorf("index: decode shard: %w", err)
	}
	return entries, nil
}

// formatCurrent 组装 CURRENT 内容："<snapID> <manifestFP8>"（空格分隔、无换行，D8）。
func formatCurrent(snapID string, manifestFP8 string) string {
	return snapID + " " + manifestFP8
}

// parseCurrent 严格解析 CURRENT 内容：恰好两段（空格分隔）、snapID 仅含对象键安全字符、
// manifestFP8 为 16 字符小写 hex。
func parseCurrent(raw string) (snapID string, manifestFP8 string, err error) {
	parts := strings.Split(raw, " ")
	if len(parts) != 2 || !validSnapID(parts[0]) || !validFP8(parts[1]) {
		return "", "", fmt.Errorf("index: invalid CURRENT content %q", raw)
	}
	return parts[0], parts[1], nil
}

// validFP8 判断 s 是否为 16 字符小写 hex。
func validFP8(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// validSnapID 判断 s 非空且仅含对象键安全字符 [A-Za-z0-9-_.]（裁决 c）。
func validSnapID(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}

// getObject 读取对象全部内容；错误经 %w 包装原样返回（含 objectstore.ErrNotFound）。
func getObject(ctx context.Context, store objectstore.Store, key string) ([]byte, error) {
	var buf bytes.Buffer
	if err := store.Get(ctx, key, &buf); err != nil {
		return nil, fmt.Errorf("index: get %s: %w", key, err)
	}
	return buf.Bytes(), nil
}
