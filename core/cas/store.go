package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"traincache/core/internal/digest"
)

// Fingerprint 是 xxh3-128 摘要，作为存储键；String()/FingerprintFromHex（header.go）
// 提供 32 字符小写 hex 编解码。
type Fingerprint [16]byte

// VerifyLevel 是校验等级开关（design.md §8：verify_level = xxh3 | xxh3+sha256）。
type VerifyLevel int

const (
	VerifyXXH3       VerifyLevel = iota // 默认：只验 xxh3
	VerifyXXH3SHA256                    // 新写块时同时算 sha256 写入块头，读时双重校验
)

var (
	ErrCorrupt  = errors.New("cas: block verification failed")
	ErrNotFound = errors.New("cas: block not found")
	ErrInUse    = errors.New("cas: block is referenced or being read")
)

const (
	// blocksDirName 是块目录名：块文件布局 blocks/<fp[0:2]hex>/<fp hex>.blk（design.md §6.3）。
	blocksDirName = "blocks"
	// quarantineDirName 是隔离目录名（D25）：坏块布局 quarantine/<fp[0:2]hex>/<fp hex>.blk，
	// 与 blocks/ 并列在 cas 根下（同一文件系统，移动用 os.Rename 原子完成）。
	quarantineDirName = "quarantine"
	// tempPrefix 是 Put 临时文件前缀（临时文件在完成后 rename 为块文件，不做 fsync，D11）。
	tempPrefix = "tmp-"
	// dirPerm 是块目录权限。
	dirPerm = 0o755
)

// Store 是 CAS 本地块存储；内部持有 rootDir、blocks 目录、verify、refcount/pin 表。
type Store struct {
	rootDir string // 用户传入的根路径（blocks/ 与 quarantine/ 的父目录，D25）
	dir     string // 块目录（<rootDir>/blocks）
	verify  VerifyLevel

	mu   sync.Mutex          // 保护 refs/pins；Remove 的"检查 → 删除"在同一把锁内完成
	refs map[Fingerprint]int //refs是查看快照级别的引用计数
	pins map[Fingerprint]int //pins是查看读进程级别的引用计数
}

// New 创建（必要时初始化）dir 下的块存储（创建块目录 blocks/ 与隔离目录 quarantine/）；
// verify 决定新写块的校验等级。
func New(dir string, verify VerifyLevel) (*Store, error) {
	blocksDir := filepath.Join(dir, blocksDirName)
	if err := os.MkdirAll(blocksDir, dirPerm); err != nil {
		return nil, fmt.Errorf("cas: create blocks dir %q: %w", blocksDir, err)
	}
	quarantineDir := filepath.Join(dir, quarantineDirName)
	if err := os.MkdirAll(quarantineDir, dirPerm); err != nil {
		return nil, fmt.Errorf("cas: create quarantine dir %q: %w", quarantineDir, err)
	}
	return &Store{
		rootDir: dir,
		dir:     blocksDir,
		verify:  verify,
		refs:    make(map[Fingerprint]int),
		pins:    make(map[Fingerprint]int),
	}, nil
}

// blockPath 返回块文件的最终路径 blocks/<fp[0:2]hex>/<fp hex>.blk。
func (s *Store) blockPath(fp Fingerprint) string {
	hexs := fp.String()
	return filepath.Join(s.dir, hexs[:2], hexs+".blk")
}

// quarantinePath 返回隔离块文件的最终路径 quarantine/<fp[0:2]hex>/<fp hex>.blk（与 blocks/ 同两级分片，D25）。
func (s *Store) quarantinePath(fp Fingerprint) string {
	hexs := fp.String()
	return filepath.Join(s.rootDir, quarantineDirName, hexs[:2], hexs+".blk")
}

// Put 流式写入：边读边算指纹，写临时文件，完成后 rename 到 blocks/<fp[0:2]hex>/<fp hex>.blk。
// 同指纹并发写安全（rename 原子，最终文件完整）；返回指纹与数据字节数。
// 不做 fsync（丢失代价 = 重拉，D11）。
func (s *Store) Put(ctx context.Context, r io.Reader) (Fingerprint, int64, error) {
	var zero Fingerprint
	if err := ctx.Err(); err != nil {
		return zero, 0, err
	}

	// 1) 临时文件：先写 128B 块头占位，数据流式写入其后（崩溃残留 = tmp-*，不是块文件）。
	tmp, err := os.CreateTemp(s.dir, tempPrefix+"*")
	if err != nil {
		return zero, 0, fmt.Errorf("cas: create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	published := false
	defer func() {
		if !published {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(make([]byte, HeaderSize)); err != nil {
		return zero, 0, fmt.Errorf("cas: write header placeholder: %w", err)
	}

	// 2) 流式读：边写盘边算指纹（verify 决定是否同时算 sha256）；ctx 取消经 contextReader 生效。
	hXXH := digest.NewXXH3()
	writers := []io.Writer{tmp, hXXH}
	var hSHA hash.Hash
	if s.verify == VerifyXXH3SHA256 {
		hSHA = digest.NewSHA256()
		writers = append(writers, hSHA)
	}
	n, err := io.Copy(io.MultiWriter(writers...), contextReader{ctx: ctx, r: r})
	if err != nil {
		return zero, 0, err
	}
	if err := ctx.Err(); err != nil {
		return zero, 0, err
	}
	if n > math.MaxUint32 {
		return zero, 0, fmt.Errorf("cas: block too large: %d bytes exceeds uint32 block_size limit", n)
	}

	// 3) 回填块头（数据字节数 + 指纹）并关闭；不做 fsync（D11）。
	var fp Fingerprint
	copy(fp[:], hXXH.Sum(nil))
	hdr := Header{Version: headerVersion, BlockSize: uint32(n), XXH3: fp}
	if hSHA != nil {
		hdr.Flags |= FlagHasSHA256
		copy(hdr.SHA256[:], hSHA.Sum(nil))
	}
	enc := hdr.Encode()
	if _, err := tmp.WriteAt(enc[:], 0); err != nil {
		return zero, 0, fmt.Errorf("cas: write block header: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return zero, 0, fmt.Errorf("cas: close temp file: %w", err)
	}

	// 4) 原子发布：先建分片目录再 rename。并发写同一指纹各写各的临时文件，
	// rename 原子覆盖，最终文件必为完整块；其他进程/goroutine 永远看不到写了一半的块。
	finalPath := s.blockPath(fp)
	if err := os.MkdirAll(filepath.Dir(finalPath), dirPerm); err != nil {
		return zero, 0, fmt.Errorf("cas: create block dir: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return zero, 0, fmt.Errorf("cas: publish block: %w", err)
	}
	published = true
	return fp, n, nil
}

// Open 打开块并校验：读块头验证 magic/version/reserved（及与请求指纹一致），
// 流式重算数据 xxh3 比对；块头 flags bit0 置位时还验 sha256（按块头 flags，
// 与 Store.verify 无关，D13）。不符返回 ErrCorrupt（调用方负责 Remove 后重拉）。
// 校验通过后返回从数据区起读的句柄。
func (s *Store) Open(fp Fingerprint) (*BlockReader, error) {
	f, err := os.Open(s.blockPath(fp))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("cas: open block %s: %w", fp, err)
	}
	verified := false
	defer func() {
		if !verified {
			_ = f.Close()
		}
	}()

	// 1) 块头校验。
	var head [HeaderSize]byte
	if _, err := io.ReadFull(f, head[:]); err != nil {
		return nil, fmt.Errorf("%w: read header: %v", ErrCorrupt, err)
	}
	hdr, err := ParseHeader(head[:])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if Fingerprint(hdr.XXH3) != fp {
		return nil, fmt.Errorf("%w: header fingerprint %s does not match requested %s", ErrCorrupt, Fingerprint(hdr.XXH3), fp)
	}

	// 2) 流式重算数据指纹并按需双重校验。
	hXXH := digest.NewXXH3()
	writers := []io.Writer{hXXH}
	var hSHA hash.Hash
	if hdr.Flags&FlagHasSHA256 != 0 {
		hSHA = digest.NewSHA256()
		writers = append(writers, hSHA)
	}
	n, err := io.Copy(io.MultiWriter(writers...), io.LimitReader(f, int64(hdr.BlockSize)))
	if err != nil {
		return nil, fmt.Errorf("%w: read data: %v", ErrCorrupt, err)
	}
	if n != int64(hdr.BlockSize) {
		return nil, fmt.Errorf("%w: data truncated: got %d bytes, block_size %d", ErrCorrupt, n, hdr.BlockSize)
	}
	if !bytes.Equal(hXXH.Sum(nil), hdr.XXH3[:]) {
		return nil, fmt.Errorf("%w: xxh3 mismatch", ErrCorrupt)
	}
	if hSHA != nil && !bytes.Equal(hSHA.Sum(nil), hdr.SHA256[:]) {
		return nil, fmt.Errorf("%w: sha256 mismatch", ErrCorrupt)
	}

	// 3) 校验通过：回到数据区起点，返回读句柄。
	if _, err := f.Seek(HeaderSize, io.SeekStart); err != nil {
		return nil, fmt.Errorf("cas: seek to data region: %w", err)
	}
	verified = true
	return &BlockReader{f: f, size: int64(hdr.BlockSize)}, nil
}

// Remove 删除块；仍有引用或读者（pin）时返回 ErrInUse，块不存在返回 ErrNotFound。
// 在 Store.mu 内完成"检查 → 删除"，与 Acquire/Release/Pin/Unpin 串行化。
func (s *Store) Remove(fp Fingerprint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs[fp] > 0 || s.pins[fp] > 0 {
		return ErrInUse
	}
	if err := os.Remove(s.blockPath(fp)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("cas: remove block %s: %w", fp, err)
	}
	return nil
}

// Quarantine 把 Open 校验失败的坏块物理移入 cas 根下 quarantine/ 目录（与 blocks/ 并列），
// 不再出现在 List() 中。幂等：源块不在 blocks/（已在 quarantine 或从未存在）返回 nil；
// 目标已存在时 rename 原子覆盖。不检查 refs/pins（坏块隔离是正确性动作，引用计数是
// 索引层的逻辑关系，修复后同指纹块回原位、引用不变）；持 s.mu 与 Remove/其他 Quarantine
// 串行。仅真实 I/O 错误报错。
func (s *Store) Quarantine(fp Fingerprint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dst := s.quarantinePath(fp)
	if err := os.MkdirAll(filepath.Dir(dst), dirPerm); err != nil {
		return fmt.Errorf("cas: quarantine block %s: %w", fp, err)
	}
	if err := os.Rename(s.blockPath(fp), dst); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cas: quarantine block %s: %w", fp, err)
	}
	return nil
}

// List 枚举 blocks/ 下全部块指纹（D20）：递归扫描块目录，解析块文件名
// （<32 位 hex>.blk）为指纹；忽略 .tmp-* 临时文件与无法解析为 32 位 hex 的文件名。
// 返回按指纹字典序（字节序）升序排序的切片；无块返回空切片。
func (s *Store) List() ([]Fingerprint, error) {
	fps, err := listBlockDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("cas: list blocks: %w", err)
	}
	return fps, nil
}

// ListQuarantined 枚举 quarantine/ 下全部块指纹（与 List 同规则：忽略临时文件与非法
// 文件名，按指纹字节序升序，无块返回空切片）。供修复队列与重扫描枚举隔离块用。
func (s *Store) ListQuarantined() ([]Fingerprint, error) {
	fps, err := listBlockDir(filepath.Join(s.rootDir, quarantineDirName))
	if err != nil {
		return nil, fmt.Errorf("cas: list quarantined blocks: %w", err)
	}
	return fps, nil
}

// RemoveQuarantined 删除 quarantine/ 下的块（修复成功后清理）；目标不存在返回 nil
// （同 objectstore.Delete“不存在不算错误”语义）。仅真实 I/O 错误报错。
func (s *Store) RemoveQuarantined(fp Fingerprint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.quarantinePath(fp)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cas: remove quarantined block %s: %w", fp, err)
	}
	return nil
}

// listBlockDir 枚举 dir 下全部块指纹（List 与 ListQuarantined 共用）：递归扫描，
// 解析块文件名（<32 位 hex>.blk）为指纹；忽略 .tmp-* 临时文件与无法解析为 32 位 hex
// 的文件名。返回按指纹字典序（字节序）升序排序的切片；无块返回空切片。
func listBlockDir(dir string) ([]Fingerprint, error) {
	fps := make([]Fingerprint, 0)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".blk") || strings.HasPrefix(name, tempPrefix) {
			return nil // 临时文件与非块文件：忽略
		}
		fp, err := FingerprintFromHex(strings.TrimSuffix(name, ".blk"))
		if err != nil {
			return nil // 非法文件名：忽略
		}
		fps = append(fps, fp)
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(fps, func(a, b Fingerprint) int { return bytes.Compare(a[:], b[:]) })
	return fps, nil
}

// contextReader 在每次 Read 前检查 ctx：写循环（io.Copy）借它感知取消。
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c contextReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// BlockReader 包装 *os.File，从块头之后开始读（恰好 block_size 字节）。
type BlockReader struct {
	f    *os.File
	size int64 // 数据字节数（块头 block_size）
	off  int64 // 已从数据区读出的字节数
}

// Read 实现 io.Reader（从数据区起读）；读满 size 字节后返回 io.EOF。
func (r *BlockReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.off >= r.size {
		return 0, io.EOF
	}
	if rem := r.size - r.off; int64(len(p)) > rem {
		p = p[:rem]
	}
	n, err := r.f.Read(p)
	r.off += int64(n)
	return n, err
}

// Close 关闭块文件。
func (r *BlockReader) Close() error {
	return r.f.Close()
}

// Size 返回数据字节数（块头 block_size）。
func (r *BlockReader) Size() int64 {
	return r.size
}
