package cache

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Object 是 io.ReadCloser，Writer 是 io.Writer。
var (
	_ io.ReadCloser = (*Object)(nil)
	_ io.Writer     = (*Writer)(nil)
)

// Store 是网关的磁盘块缓存（design 9.1 §5.5、§6.8；9.2 D32、D33）。
//
// 锁：所有内部状态由 mu 保护。mu 之外的锁顺序只有一种：先 mu 后文件 I/O，
// 不存在反向获取，因此不会死锁。
type Store struct {
	dir          string
	datasetLimit int64
	totalLimit   int64

	mu       sync.Mutex
	total    int64
	datasets map[string]*datasetState
}

// datasetState 是单个 dataset 的驻写字节与最后出现时间。
type datasetState struct {
	bytes int64
	touch time.Time
}

// New 构造缓存。dir 为空时用 RootDir。
//
// datasetLimit 是单 dataset 驻写上限，totalLimit 是全局上限，两者非正时表示
// 不限（不触发对应那一步淘汰）。
func New(dir string, datasetLimit, totalLimit int64) *Store {
	if dir == "" {
		dir = RootDir
	}
	return &Store{
		dir:          dir,
		datasetLimit: datasetLimit,
		totalLimit:   totalLimit,
		datasets:     make(map[string]*datasetState),
	}
}

// Object 是盘上读出来的一块：io.ReadCloser 加元数据。
//
// 已经打开的块即使随后被淘汰删除，读侧仍能读到文件尾（Linux 上 open 拿到的是
// inode 引用，unlink 不影响已打开的描述符）。
type Object struct {
	mu   sync.Mutex
	file *os.File
	meta Metadata
}

// Read 从块内容里读字节。
func (o *Object) Read(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return 0, os.ErrClosed
	}
	return o.file.Read(p)
}

// Close 关闭块内容文件。重复调用返回 nil。
func (o *Object) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return nil
	}
	err := o.file.Close()
	o.file = nil
	return err
}

// Metadata 返回块在盘上记录的元数据。
func (o *Object) Metadata() Metadata {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.meta
}

// Writer 是写盘句柄。字节先进 .tmp，Close 写 .meta 再把 .tmp rename 成正式名。
type Writer struct {
	store   *Store
	lay     layout
	file    *os.File
	tmp     string
	written int64

	mu     sync.Mutex
	closed bool
}

// Write 把字节追加进 .tmp。半块对 Lookup 不可见。
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("cache: writer is closed")
	}
	n, err := w.file.Write(p)
	w.written += int64(n)
	return n, err
}

// Close 收尾：先写 .meta，再把 .tmp rename 成正式名。
//
// 写盘前会再算一次三步淘汰，这次知道块有多大；仍放不下时删掉 .tmp 并返回
// ErrQuota，盘上不留半成品，也不出现正式名。
func (w *Writer) Close(m Metadata) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("cache: writer is closed")
	}
	w.closed = true

	if err := w.file.Close(); err != nil {
		os.Remove(w.tmp)
		return err
	}
	size := w.written

	// 写盘之前算一次三步淘汰（design 9.1 §6.8）。
	if err := w.store.makeRoom(context.Background(), w.lay.dataset, size); err != nil {
		os.Remove(w.tmp)
		return err
	}

	meta := m
	if size > 0 {
		meta.Size = size
	}
	if err := writeMetaAtomic(w.lay.meta, meta); err != nil {
		os.Remove(w.tmp)
		return err
	}
	if err := os.Rename(w.tmp, w.lay.final); err != nil {
		os.Remove(w.lay.meta)
		os.Remove(w.tmp)
		return err
	}
	w.store.commit(w.lay.dataset, size)
	return nil
}

// Abort 中途失败：删掉 .tmp，盘上不留半成品。已经 Close 过则什么都不做。
func (w *Writer) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.file != nil {
		w.file.Close()
	}
	if err := os.Remove(w.tmp); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Lookup 读盘。只有 rename 之后的正式名可见；.meta 缺失或解析失败按未命中处理。
func (s *Store) Lookup(key string) (*Object, error) {
	lay, err := keyLayout(s.dir, key)
	if err != nil {
		return nil, ErrNotFound
	}
	f, err := os.Open(lay.final)
	if err != nil {
		return nil, ErrNotFound
	}
	meta, err := readMeta(lay.meta)
	if err != nil {
		f.Close()
		return nil, ErrNotFound
	}
	// 读命中刷新 last-touch 表。
	s.touch(lay.dataset)
	return &Object{file: f, meta: meta}, nil
}

// BeginPut 打开 .tmp 开始写盘。
//
// 键的任一分隔段超过 255 字节、或者配额放不下时返回错误，不建文件。调用方
// 丢弃这个 Writer，请求照原路返回。
func (s *Store) BeginPut(ctx context.Context, key string) (*Writer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lay, err := keyLayout(s.dir, key)
	if err != nil {
		return nil, err
	}
	// 写盘之前算一次三步淘汰。这一步还不知道块有多大，按 0 算：作用是把已经
	// 超限的 dataset 与全局占用拉回上限内；放不下（连空块都放不下）时直接
	// 报错，调用方不写盘。
	if err := s.makeRoom(ctx, lay.dataset, 0); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(lay.tmp), 0o755); err != nil {
		return nil, err
	}
	f, tmp, err := openTmp(lay.tmp)
	if err != nil {
		return nil, err
	}
	return &Writer{store: s, lay: lay, file: f, tmp: tmp}, nil
}

// Scan 启动时扫描盘上内容，填 last-touch 表并重建驻写字节账。
//
// 表里的时间取 dataset 目录的 mtime。Linux 上目录 mtime 只在条目增删时变，
// 纯读不刷新，所以空闲判断只读这张表，不读目录 mtime。
func (s *Store) Scan(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	next := make(map[string]*datasetState)
	var total int64
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		info, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		dir := filepath.Join(s.dir, name)
		blocks, err := listBlocks(dir)
		if err != nil {
			return err
		}
		var bytes int64
		for _, b := range blocks {
			bytes += b.size
		}
		next[name] = &datasetState{bytes: bytes, touch: info.ModTime()}
		total += bytes
		// 上个进程留下的半成品 .tmp 在启动时清掉。
		if err := removeStaleTmp(dir); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.datasets = next
	s.total = total
	s.mu.Unlock()
	return nil
}

// TotalBytes 返回全局驻写字节。只算正式名的块内容，不含 .meta 与 .tmp。
func (s *Store) TotalBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

// DatasetBytes 返回单个 dataset 的驻写字节。
func (s *Store) DatasetBytes(dataset string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.datasetBytesLocked(dataset)
}

// LastTouch 返回 last-touch 表里 dataset 的最后出现时间。表里没有则第二个
// 返回值是 false。
func (s *Store) LastTouch(dataset string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.datasets[dataset]
	if st == nil {
		return time.Time{}, false
	}
	return st.touch, true
}

// commit 记一笔写盘成功：驻写字节加上，last-touch 刷新。
func (s *Store) commit(dataset string, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.datasets[dataset]
	if st == nil {
		st = &datasetState{}
		s.datasets[dataset] = st
	}
	st.bytes += size
	st.touch = time.Now()
	s.total += size
}

// datasetBytesLocked 返回单个 dataset 的驻写字节，调用方持有 mu。
func (s *Store) datasetBytesLocked(dataset string) int64 {
	if st := s.datasets[dataset]; st != nil {
		return st.bytes
	}
	return 0
}

// openTmp 打开写入用的临时文件。
//
// 优先用布局里的固定名 <name>.tmp；同名文件已存在（另一个请求正在写同一块）
// 时改用同目录下的唯一临时名，避免两个写者互相截断对方的文件。
func openTmp(path string) (*os.File, string, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err == nil {
		return f, path, nil
	}
	if !os.IsExist(err) {
		return nil, "", err
	}
	stem := strings.TrimSuffix(filepath.Base(path), tmpSuffix)
	f, err = os.CreateTemp(filepath.Dir(path), stem+".*"+tmpSuffix)
	if err != nil {
		return nil, "", err
	}
	return f, f.Name(), nil
}

// listBlocks 列出目录下的正式名块文件（跳过 .meta 与 .tmp）。
func listBlocks(dir string) ([]blockFile, error) {
	var out []blockFile
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, metaSuffix) || strings.HasSuffix(name, tmpSuffix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		out = append(out, blockFile{
			path:  path,
			meta:  path + metaSuffix,
			size:  info.Size(),
			mtime: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// removeStaleTmp 删掉目录下的所有 .tmp 半成品。
func removeStaleTmp(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), tmpSuffix) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		return nil
	})
}
