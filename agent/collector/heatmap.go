package collector

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// heatmap.go:热度映射的周期衰减、heatmap.json 的原子写与启动恢复(§6.4),以及 inotify
// 旁路监听打开事件(§5.5"inotify 旁路监听缓存目录打开事件,不侵入训练进程";D18)。

// heatmapVersion 是 heatmap.json 格式版本(§6.4:version=1)。
const heatmapVersion = 1

// heatmapFile 是 heatmap.json 的磁盘格式(§6.4 冻结):
//
//	{"version":1,"dumped_at":RFC3339,"entries":{"<fp32hex>":{"heat":1.5,
//	 "last_access":1763000000,"access_count":3}}}
//
// last_access 为 Unix 秒(0 = 从未 touch);access_count 仅记录打开次数(D24 热度增量口径
// 不使用计数,计数供观测)。
type heatmapFile struct {
	Version  int                     `json:"version"`
	DumpedAt string                  `json:"dumped_at"`
	Entries  map[string]heatmapEntry `json:"entries"`
}

// heatmapEntry 是单条热度记录(§6.4)。
type heatmapEntry struct {
	Heat        float64 `json:"heat"`
	LastAccess  int64   `json:"last_access"`
	AccessCount int64   `json:"access_count"`
}

// decay 执行一轮周期衰减(D24):超过 HeatDecayAge 未 touch 的条目 h 置 0(主对话冻结
// "直接衰减"口径),其余条目 h × HeatDecayFactor。
func (c *Collector) decay() {
	now := c.clock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		if now.Sub(e.lastTouch) > c.cfg.HeatDecayAge {
			e.heat = 0
			continue
		}
		e.heat *= c.cfg.HeatDecayFactor
	}
}

// Dump 原子写出 heatmap.json(§6.4:临时文件 + rename;失败返回错误,调用方记日志,
// 不影响服务)。时间戳取 clock(注入时钟下与判定口径一致)。
func (c *Collector) Dump() error {
	f := heatmapFile{
		Version:  heatmapVersion,
		DumpedAt: c.clock().UTC().Format(time.RFC3339),
		Entries:  c.snapshotHeatmap(),
	}
	b, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("collector: 序列化 heatmap: %w", err)
	}
	b = append(b, '\n')

	if err := os.MkdirAll(c.blockDir, 0o755); err != nil {
		return fmt.Errorf("collector: 创建 heatmap 目录 %s: %w", c.blockDir, err)
	}
	tmp, err := os.CreateTemp(c.blockDir, heatmapFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("collector: 创建 heatmap 临时文件: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("collector: 写 heatmap 临时文件: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("collector: 关闭 heatmap 临时文件: %w", err)
	}
	if err := os.Rename(tmpName, c.heatmapPath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("collector: rename heatmap %s: %w", c.heatmapPath, err)
	}
	return nil
}

// snapshotHeatmap 生成磁盘格式快照(last_access 秒;零值 t_touch 写 0)。
func (c *Collector) snapshotHeatmap() map[string]heatmapEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]heatmapEntry, len(c.entries))
	for fp, e := range c.entries {
		var last int64
		if !e.lastTouch.IsZero() {
			last = e.lastTouch.Unix()
		}
		out[fp.String()] = heatmapEntry{Heat: e.heat, LastAccess: last, AccessCount: e.accessCount}
	}
	return out
}

// Load 读取 heatmap.json 并以文件内容替换内存热度表(§6.4/§7.1 启动恢复):
//   - 文件缺失(首次启动 / 被清理)→ 空表,nil(容错口径);
//   - 文件损坏(非 JSON / 非法指纹键)→ 逐条容错:整体解析失败置空表,单条非法跳过,均告警;
//   - version 非 1 → 告警但仍按同字段读取(字段自描述,宽松兼容);
//   - 真实读错误(权限等)→ 空表 + 返回错误(调用方决定是否致命;Start 内仅告警)。
func (c *Collector) Load() error {
	b, err := os.ReadFile(c.heatmapPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			c.replaceEntries(nil)
			return nil
		}
		c.replaceEntries(nil)
		return fmt.Errorf("collector: 读取 heatmap %s: %w", c.heatmapPath, err)
	}

	var f heatmapFile
	if err := json.Unmarshal(b, &f); err != nil {
		slog.Warn("collector: heatmap 解析失败,以空表继续", "path", c.heatmapPath, "err", err)
		c.replaceEntries(nil)
		return nil
	}
	if f.Version != heatmapVersion {
		slog.Warn("collector: heatmap 版本非预期,按同字段宽容读取",
			"path", c.heatmapPath, "version", f.Version, "want", heatmapVersion)
	}
	entries := make(map[cas.Fingerprint]*heatEntry, len(f.Entries))
	for key, v := range f.Entries {
		fp, err := cas.FingerprintFromHex(key)
		if err != nil {
			slog.Warn("collector: heatmap 条目指纹非法,跳过", "path", c.heatmapPath, "key", key, "err", err)
			continue
		}
		e := &heatEntry{heat: v.Heat, accessCount: v.AccessCount}
		if v.LastAccess > 0 {
			e.lastTouch = time.Unix(v.LastAccess, 0)
		}
		entries[fp] = e
	}
	c.replaceEntries(entries)
	return nil
}

// replaceEntries 以 m 替换热度表(nil → 空表)。
func (c *Collector) replaceEntries(m map[cas.Fingerprint]*heatEntry) {
	if m == nil {
		m = make(map[cas.Fingerprint]*heatEntry)
	}
	c.mu.Lock()
	c.entries = m
	c.mu.Unlock()
}

// hookSilenceWindow 是"钩子为主,inotify 兜底"的判定窗(§5.4/D15):钩子在该窗内报告过
// 打开事件时,inotify 打开事件不计入统计(同一打开事件双源只算一次,主源优先);
// 钩子沉默超过该窗(或从未报告)→ inotify 兜底生效(降级场景)。包内变量,NewCollector
// 构造时固化为 Collector.hookSilence,测试改包变量后构造即可覆盖。
var hookSilenceWindow = 30 * time.Second

// —— inotify 旁路(§5.5;平台锁定 linux,与 ordersource 直接 syscall.Mmap 同风格)——
//
// 实现说明:fsnotify(v1.9/v1.10)的公开 API 不暴露 IN_OPEN/IN_ACCESS(xUnportableOpen 等
// 未导出,AddWith 的 withOps 选项亦未导出),外部包无法观测"打开事件";故此处用 stdlib
// syscall 直接建 inotify(DECISIONS 回传,零新依赖)。

const (
	// blkSuffix 是块文件名后缀(cas 布局 blocks/<fp[0:2]hex>/<fp hex>.blk)。
	blkSuffix = ".blk"
	// fpHexLen 是指纹 hex 长度(cas.Fingerprint 16B → 32 hex)。
	fpHexLen = 32
	// tmpPrefix 是 cas 写临时文件前缀(core cas tempPrefix;不作为打开事件源)。
	tmpPrefix = "tmp-"
	// inotifyBufSize 是单次读取缓冲(内核单事件 ≤ 16 + NAME_MAX + 1,64KiB 足够)。
	inotifyBufSize = 64 << 10
)

// inotifyPollInterval 是监听循环的读轮询周期(非阻塞 fd + ticker;单次读取耗尽全部待处理
// 事件,间隔只影响事件到达延迟)。包内变量,NewCollector 构造时固化为
// Collector.watchInterval,测试改包变量后构造即可覆盖。
var inotifyPollInterval = 100 * time.Millisecond

// inotifyWatchMask 是目录 watch 掩码:IN_OPEN(打开事件)、子目录增删(CREATE/MOVED_TO/
// MOVED_FROM/DELETE)、被监听目录自身消失(DELETE_SELF/MOVE_SELF)、IN_ONLYDIR(仅目录可挂)。
const inotifyWatchMask = syscall.IN_OPEN | syscall.IN_CREATE | syscall.IN_MOVED_TO |
	syscall.IN_MOVED_FROM | syscall.IN_DELETE | syscall.IN_DELETE_SELF |
	syscall.IN_MOVE_SELF | syscall.IN_ONLYDIR

// inotifyEvent 是一个已解析的 inotify 事件(name 为空 = 被监听目录自身的事件)。
type inotifyEvent struct {
	wd   int32
	mask uint32
	name string
}

// blockWatcher 是 blocks 目录树的 inotify 监听器(根目录 + 各分片子目录;wd → 路径映射)。
type blockWatcher struct {
	fd     int
	root   string
	rootWD int32
	paths  map[int32]string
}

// newBlockWatcher 建立非阻塞 inotify 实例并挂根目录与既有分片子目录 watch。
func newBlockWatcher(blocksDir string) (*blockWatcher, error) {
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		return nil, err
	}
	w := &blockWatcher{fd: fd, root: blocksDir, rootWD: -1, paths: make(map[int32]string)}
	if err := w.addRoot(); err != nil {
		w.close()
		return nil, err
	}
	// 既有分片子目录(blocks/<fp[0:2]hex>);读目录失败不致命(下一轮循环重试根,子目录由
	// CREATE 事件补挂)。
	if ents, err := os.ReadDir(blocksDir); err == nil {
		for _, ent := range ents {
			if ent.IsDir() {
				_, _ = w.add(filepath.Join(blocksDir, ent.Name()))
			}
		}
	}
	return w, nil
}

// addRoot 挂根目录 watch(目录可能晚于采集层出现:失败由监听循环重试)。
func (w *blockWatcher) addRoot() error {
	wd, err := w.add(w.root)
	if err != nil {
		return err
	}
	w.rootWD = wd
	return nil
}

// add 挂一个目录 watch,返回 wd(同一路径重复挂由内核去重并返回同 wd)。
func (w *blockWatcher) add(path string) (int32, error) {
	wd, err := syscall.InotifyAddWatch(w.fd, path, inotifyWatchMask)
	if err != nil {
		return -1, err
	}
	w.paths[int32(wd)] = path
	return int32(wd), nil
}

// rmPath 移除指定路径的 watch(目录被删除/移走;正常删除由内核自动摘除,此处兜底)。
func (w *blockWatcher) rmPath(path string) {
	for wd, p := range w.paths {
		if p == path {
			_, _ = syscall.InotifyRmWatch(w.fd, uint32(wd))
			delete(w.paths, wd)
			return
		}
	}
}

// drop 丢弃一个 wd 的映射(IN_IGNORED / 目录自身消失:内核已释放 watch)。
func (w *blockWatcher) drop(wd int32) { delete(w.paths, wd) }

// path 返回 wd 对应的路径(未知 wd → 空串)。
func (w *blockWatcher) path(wd int32) string { return w.paths[wd] }

// close 关闭 inotify 实例(幂等)。
func (w *blockWatcher) close() {
	if w.fd >= 0 {
		_ = syscall.Close(w.fd)
		w.fd = -1
	}
}

// read 非阻塞读取一批事件;EAGAIN/EINTR 视为无事件。截断事件(缓冲尾)丢弃。
func (w *blockWatcher) read(buf []byte) ([]inotifyEvent, error) {
	n, err := syscall.Read(w.fd, buf)
	if n <= 0 {
		if err == nil || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR) {
			return nil, nil
		}
		return nil, err
	}
	sz := syscall.SizeofInotifyEvent
	var out []inotifyEvent
	for off := 0; off+sz <= n; {
		wd := int32(binary.LittleEndian.Uint32(buf[off : off+4]))
		mask := binary.LittleEndian.Uint32(buf[off+4 : off+8])
		nameLen := int(binary.LittleEndian.Uint32(buf[off+12 : off+16]))
		if off+sz+nameLen > n {
			break
		}
		raw := buf[off+sz : off+sz+nameLen]
		if i := bytes.IndexByte(raw, 0); i >= 0 {
			raw = raw[:i]
		}
		out = append(out, inotifyEvent{wd: wd, mask: mask, name: string(raw)})
		off += sz + nameLen
	}
	return out, nil
}

// watchBlocks 监听 blocks 目录树下的打开事件(ctx 取消退出):目录事件维护子目录 watch,
// 块文件 IN_OPEN → 兜底打开事件(经钩子沉默判定后计入热度/命中率)。
func (c *Collector) watchBlocks(ctx context.Context) {
	w, err := newBlockWatcher(c.blocksDir)
	if err != nil {
		slog.Warn("collector: inotify 监听初始化失败,兜底源停用", "dir", c.blocksDir, "err", err)
		return
	}
	defer w.close()

	t := time.NewTicker(c.watchInterval)
	defer t.Stop()
	buf := make([]byte, inotifyBufSize)
	for {
		evs, err := w.read(buf)
		if err != nil {
			slog.Warn("collector: inotify 读取失败,兜底源退出", "dir", c.blocksDir, "err", err)
			return
		}
		for _, ev := range evs {
			c.handleWatchEvent(w, ev)
		}
		if w.path(w.rootWD) == "" {
			// 根目录 watch 缺失(目录晚出现 / 被重建):重试挂上。
			_ = w.addRoot()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// handleWatchEvent 处理一个 inotify 事件。
func (c *Collector) handleWatchEvent(w *blockWatcher, ev inotifyEvent) {
	if ev.mask&syscall.IN_Q_OVERFLOW != 0 {
		slog.Warn("collector: inotify 事件队列溢出,可能有打开事件丢失", "dir", c.blocksDir)
		return
	}
	if ev.mask&syscall.IN_IGNORED != 0 {
		w.drop(ev.wd)
		return
	}
	if ev.name == "" {
		// 被监听目录自身的事件:消失(删除/移走)→ 丢弃映射(根目录由循环重试补挂)。
		if ev.mask&(syscall.IN_DELETE_SELF|syscall.IN_MOVE_SELF) != 0 {
			w.drop(ev.wd)
		}
		return
	}

	dir := w.path(ev.wd)
	if dir == "" {
		return
	}
	full := filepath.Join(dir, ev.name)

	if ev.mask&syscall.IN_ISDIR != 0 {
		switch {
		case ev.mask&(syscall.IN_CREATE|syscall.IN_MOVED_TO) != 0:
			if _, err := w.add(full); err != nil {
				slog.Warn("collector: 新分片子目录挂 watch 失败", "dir", full, "err", err)
			}
		case ev.mask&(syscall.IN_DELETE|syscall.IN_MOVED_FROM) != 0:
			w.rmPath(full)
		}
		return
	}

	if ev.mask&syscall.IN_OPEN == 0 {
		return
	}
	fp, ok := parseBlockName(ev.name)
	if !ok {
		return
	}
	c.openFromWatch(fp, c.clock())
}

// openFromWatch 处理一次 inotify 兜底打开事件:钩子主源活跃(hookSilenceWindow 内报告过
// 打开事件)时丢弃(双源去重,主源优先);否则按打开事件计入热度与命中率。inotify 来源
// 无订单引用,不参与交叉校准。
func (c *Collector) openFromWatch(fp cas.Fingerprint, now time.Time) {
	if !c.inotifyAccepted(now) {
		return
	}
	c.noteOpen(fp, ordersource.ChunkRef{}, now)
	c.fireOpen(fp)
}

// parseBlockName 把块文件名(<32 位 hex>.blk)解析为指纹;临时文件与非法名返回 false。
func parseBlockName(name string) (cas.Fingerprint, bool) {
	if strings.HasPrefix(name, tmpPrefix) || !strings.HasSuffix(name, blkSuffix) {
		return cas.Fingerprint{}, false
	}
	stem := strings.TrimSuffix(name, blkSuffix)
	if len(stem) != fpHexLen {
		return cas.Fingerprint{}, false
	}
	fp, err := cas.FingerprintFromHex(stem)
	if err != nil {
		return cas.Fingerprint{}, false
	}
	return fp, true
}
