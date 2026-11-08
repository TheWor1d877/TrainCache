// Package cache 是网关的磁盘块缓存层（design 9.1「盘上布局与配额淘汰」；9.2 D32、D33）。
//
// 包只依赖标准库：它不认识 minio-go，也不知道 merge、server、origin 的存在。
// 落盘布局、原子写、读盘扇出、配额与三步淘汰、last-touch 表都在这里闭环。
//
// 盘上布局（对象键整段当缓存根目录下的相对路径，dataset 是第一层目录）：
//
//	<root>/<dataset>/blocks/<path>#<chunk>        块内容
//	<root>/<dataset>/blocks/<path>#<chunk>.meta   元数据 JSON
//	<root>/<dataset>/blocks/<path>#<chunk>.tmp    写入中的临时名
//
// Lookup 只认 rename 之后的正式名，因此永远读不到半块。
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// RootDir 是缓存根目录的默认位置。代码常量，不走 env（部署时挂卷到这个点）。
const RootDir = "/var/lib/traincache-gateway/cache"

// ErrNotFound 表示盘上没有这一块。Lookup 未命中时返回它。
var ErrNotFound = errors.New("cache: object not found")

// ErrQuota 表示三步淘汰做完后这块仍放不下，本次不写盘（design 9.1「盘上布局与配额淘汰」第 3 步）。
var ErrQuota = errors.New("cache: quota exceeded")

const (
	// metaSuffix 是元数据文件后缀，与块同名加后缀，一起写、一起删。
	metaSuffix = ".meta"
	// tmpSuffix 是写入中的临时文件后缀，rename 之前对 Lookup 不可见。
	tmpSuffix = ".tmp"
	// maxSegmentBytes 是单个路径分段的字节上限，即 ext4 单个目录名的上限。
	// 键里任一分隔段超过它就跳过写盘（design 9.1「网关 HTTP 契约」；9.2 D32）。
	maxSegmentBytes = 255
)

// Metadata 是块在盘上的元数据，落成 .meta 里的 JSON。
//
// 片段比整块多三项：区间的起点与终点、对象总长。对象总长是回 206 时填
// Content-Range 用的，命中缓存时网关手里没有源站响应，总长只能从这里取。
type Metadata struct {
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	Size         int64     `json:"size"`
	RangeStart   int64     `json:"range_start,omitempty"`
	RangeEnd     int64     `json:"range_end,omitempty"`
	ObjectSize   int64     `json:"object_size,omitempty"`
}

// layout 是一次「键 → 盘上路径」的映射结果。
type layout struct {
	dataset string // 第一层目录名
	final   string // 块内容正式名
	meta    string // 元数据名
	tmp     string // 写入中的临时名
}

// keyLayout 把对象键整段当缓存根目录下的相对路径。
//
// dataset 取键的第一段；其余分段原样保留，不解析。任一分隔段超过 255 字节、
// 分段为空或为 . / .. 的键直接判非法，调用方据此跳过写盘。
func keyLayout(root, key string) (layout, error) {
	if key == "" {
		return layout{}, fmt.Errorf("cache: empty key")
	}
	if strings.ContainsRune(key, 0) {
		return layout{}, fmt.Errorf("cache: key contains NUL byte")
	}
	segs := strings.Split(key, "/")
	for _, seg := range segs {
		switch {
		case seg == "":
			return layout{}, fmt.Errorf("cache: empty path segment in key %q", key)
		case seg == "." || seg == "..":
			return layout{}, fmt.Errorf("cache: illegal path segment %q in key", seg)
		case len(seg) > maxSegmentBytes:
			return layout{}, fmt.Errorf("cache: path segment longer than %d bytes in key %q", maxSegmentBytes, key)
		}
	}
	final := filepath.Join(root, filepath.Join(segs...))
	return layout{
		dataset: segs[0],
		final:   final,
		meta:    final + metaSuffix,
		tmp:     final + tmpSuffix,
	}, nil
}

// FragmentKey 给出片段的缓存键：dataset 仍是第一段，其后是 frag 子树、
// 原键剩下的部分与闭区间。整块缓存的键就是对象键，两套键落在不同子树上，
// 同一个块既可以整块落盘也可以分段落盘，互不覆盖。
//
// 键里含 NUL 字节时按非法键处理，由调用方跳过写盘。
func FragmentKey(key string, start, end int64) string {
	rangeSeg := strconv.FormatInt(start, 10) + "-" + strconv.FormatInt(end, 10)
	dataset, rest, ok := strings.Cut(key, "/")
	if !ok {
		return key + "/frag/" + rangeSeg
	}
	return dataset + "/frag/" + rest + "/" + rangeSeg
}

// writeMetaAtomic 先写临时文件再 rename，避免 .meta 出现半截内容。
// 临时名以 .tmp 结尾，不计入驻写字节。
func writeMetaAtomic(path string, m Metadata) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*"+tmpSuffix)
	if err != nil {
		return err
	}
	name := f.Name()
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

// readMeta 读 .meta。文件缺失或解析失败都算未命中，调用方按 ErrNotFound 处理。
func readMeta(path string) (Metadata, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Metadata{}, err
	}
	var m Metadata
	if err := json.Unmarshal(b, &m); err != nil {
		return Metadata{}, err
	}
	return m, nil
}
