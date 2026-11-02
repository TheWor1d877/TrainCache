package cas

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"traincache/core/internal/digest"
)

// newTestStore 在临时目录里建一个 Store，返回 Store 与块目录路径。
func newTestStore(t *testing.T, verify VerifyLevel) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	s, err := New(root, verify)
	if err != nil {
		t.Fatalf("New(%q, %v) 失败: %v", root, verify, err)
	}
	return s, filepath.Join(root, blocksDirName)
}

// putBlock 写入内容（失败即终止），返回指纹。
func putBlock(t *testing.T, s *Store, content []byte) Fingerprint {
	t.Helper()
	fp, n, err := s.Put(context.Background(), bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	if n != int64(len(content)) {
		t.Fatalf("Put 返回字节数 = %d，期望 %d", n, len(content))
	}
	return fp
}

// readBlock 打开块读回全部数据，校验大小并失败即终止。
func readBlock(t *testing.T, s *Store, fp Fingerprint) []byte {
	t.Helper()
	r, err := s.Open(fp)
	if err != nil {
		t.Fatalf("Open(%s) 失败: %v", fp, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("读取块 %s 失败: %v", fp, err)
	}
	if int64(len(data)) != r.Size() {
		t.Fatalf("读回 %d 字节，Size() = %d", len(data), r.Size())
	}
	return data
}

// listFiles 遍历块目录，返回 .blk 块文件与其余文件（临时残留）的路径。
func listFiles(t *testing.T, root string) (blocks, others []string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".blk") {
			blocks = append(blocks, path)
		} else {
			others = append(others, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历 %s 失败: %v", root, err)
	}
	return blocks, others
}

// assertNoResidue 断言块目录中恰有 wantBlocks 个块文件、且无临时文件残留。
func assertNoResidue(t *testing.T, blocksDir string, wantBlocks int) {
	t.Helper()
	files, others := listFiles(t, blocksDir)
	if len(files) != wantBlocks {
		t.Fatalf("块文件数 = %d（%v），期望 %d", len(files), files, wantBlocks)
	}
	if len(others) != 0 {
		t.Fatalf("存在临时文件残留: %v", others)
	}
}

// testContent 生成确定性内容（相同 seed 生成相同字节）。
func testContent(seed int64, size int) []byte {
	b := make([]byte, size)
	rng := rand.New(rand.NewSource(seed))
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	return b
}

// blockFilePath 返回指纹对应的块文件路径。
func blockFilePath(blocksDir string, fp Fingerprint) string {
	hexs := fp.String()
	return filepath.Join(blocksDir, hexs[:2], hexs+".blk")
}

// flipByte 翻转 path 文件 offset 处的一个字节。
func flipByte(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("打开 %s 失败: %v", path, err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], offset); err != nil {
		t.Fatalf("读 %s@%d 失败: %v", path, offset, err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], offset); err != nil {
		t.Fatalf("写 %s@%d 失败: %v", path, offset, err)
	}
}

// cancelAfterReader 在累计读出 after 字节后触发 cancel。
type cancelAfterReader struct {
	r      io.Reader
	cancel context.CancelFunc
	after  int64
	read   int64
}

func (c *cancelAfterReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += int64(n)
	if c.read >= c.after {
		c.cancel()
	}
	return n, err
}

func TestNewCreatesBlocksDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "cache")
	s, err := New(root, VerifyXXH3)
	if err != nil {
		t.Fatalf("New(%q) 失败: %v", root, err)
	}
	st, err := os.Stat(filepath.Join(root, blocksDirName))
	if err != nil {
		t.Fatalf("blocks 目录未创建: %v", err)
	}
	if !st.IsDir() {
		t.Fatal("blocks 不是目录")
	}

	content := []byte("hello cas")
	fp := putBlock(t, s, content)
	if want := Fingerprint(digest.XXH3_128(content)); fp != want {
		t.Fatalf("Put 指纹 = %s，期望 %s", fp, want)
	}
	if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
		t.Fatalf("读回内容 = %q，期望 %q", got, content)
	}
}

// 场景 1：多个 goroutine 并发写同一个块 → 全部返回同一指纹、文件完整、无临时残留。
func TestConcurrentPutSameBlock(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	content := testContent(1, 256<<10)
	want := Fingerprint(digest.XXH3_128(content))

	const goroutines = 16
	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		fps     = make([]Fingerprint, goroutines)
		sizes   = make([]int64, goroutines)
		putErrs = make([]error, goroutines)
	)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			fps[i], sizes[i], putErrs[i] = s.Put(context.Background(), bytes.NewReader(content))
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < goroutines; i++ {
		if putErrs[i] != nil {
			t.Fatalf("goroutine %d Put 失败: %v", i, putErrs[i])
		}
		if fps[i] != want {
			t.Fatalf("goroutine %d 指纹 = %s，期望 %s", i, fps[i], want)
		}
		if sizes[i] != int64(len(content)) {
			t.Fatalf("goroutine %d 字节数 = %d，期望 %d", i, sizes[i], len(content))
		}
	}

	if got := readBlock(t, s, want); !bytes.Equal(got, content) {
		t.Fatalf("并发写后读回的块内容不一致（%d 字节 vs %d 字节）", len(got), len(content))
	}
	assertNoResidue(t, blocksDir, 1)
}

// 场景 2：写一半中断 + 孤儿临时文件。
func TestCrashDuringWrite(t *testing.T) {
	t.Run("midwrite_failure", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		content := testContent(2, 64<<10)
		injected := errors.New("injected read failure")
		halved := io.MultiReader(bytes.NewReader(content[:len(content)/2]), iotest.ErrReader(injected))

		// 注入失败点：流到一半报错 → Put 终止并清理临时文件。
		if _, _, err := s.Put(context.Background(), halved); !errors.Is(err, injected) {
			t.Fatalf("Put(中途失败的 reader) 错误 = %v，期望包含 %v", err, injected)
		}
		assertNoResidue(t, blocksDir, 0)

		// 重开 Put 同内容成功、Open 正常。
		fp := putBlock(t, s, content)
		if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
			t.Fatal("重试 Put 后读回内容不一致")
		}
		assertNoResidue(t, blocksDir, 1)
	})

	t.Run("orphan_temp_files", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		content := testContent(3, 32<<10)
		fp := Fingerprint(digest.XXH3_128(content))
		hexs := fp.String()

		// 预置孤儿临时文件（模拟上次崩溃残留）：<fp>.tmp-* 与 tmp-* 两种命名。
		shardDir := filepath.Join(blocksDir, hexs[:2])
		if err := os.MkdirAll(shardDir, dirPerm); err != nil {
			t.Fatalf("建分片目录失败: %v", err)
		}
		orphans := []string{
			filepath.Join(shardDir, hexs+".tmp-orphan"),
			filepath.Join(blocksDir, tempPrefix+"orphan"),
		}
		for _, p := range orphans {
			if err := os.WriteFile(p, []byte("half written block"), 0o644); err != nil {
				t.Fatalf("预置孤儿文件 %s 失败: %v", p, err)
			}
		}

		// 孤儿不是块：Open 报 ErrNotFound、不被当作块读。
		if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Open(仅孤儿存在) 错误 = %v，期望 ErrNotFound", err)
		}

		// 后续 Put 正常完成、Open 正常，孤儿文件不被误用。
		fp2 := putBlock(t, s, content)
		if fp2 != fp {
			t.Fatalf("Put 指纹 = %s，期望 %s", fp2, fp)
		}
		if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
			t.Fatal("Put 后读回内容不一致")
		}
		for _, p := range orphans {
			if _, err := os.Stat(p); err != nil {
				t.Errorf("孤儿文件 %s 被意外删除/改动: %v", p, err)
			}
		}
	})
}

// 场景 3：相同内容两次 Put → 同一指纹、磁盘上仅一个块文件。
func TestDedup(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	content := testContent(4, 32<<10)

	fp1, n1, err := s.Put(context.Background(), bytes.NewReader(content))
	if err != nil {
		t.Fatalf("第一次 Put 失败: %v", err)
	}
	// 第二次用逐字节 reader（覆盖流式路径），同内容应得同指纹。
	fp2, n2, err := s.Put(context.Background(), iotest.OneByteReader(bytes.NewReader(content)))
	if err != nil {
		t.Fatalf("第二次 Put 失败: %v", err)
	}
	if fp1 != fp2 {
		t.Fatalf("同内容两次 Put 指纹不同: %s vs %s", fp1, fp2)
	}
	if n1 != n2 || n1 != int64(len(content)) {
		t.Fatalf("两次 Put 字节数 = %d, %d，期望 %d", n1, n2, len(content))
	}

	blocks, others := listFiles(t, blocksDir)
	if len(blocks) != 1 {
		t.Fatalf("块文件数 = %d（%v），期望 1", len(blocks), blocks)
	}
	if len(others) != 0 {
		t.Fatalf("存在临时文件残留: %v", others)
	}
	if got := readBlock(t, s, fp1); !bytes.Equal(got, content) {
		t.Fatal("去重后读回内容不一致")
	}
}

// 场景 4：校验失败 → ErrCorrupt（篡改数据 / 篡改 magic / sha256 双重校验）。
func TestVerifyFailure(t *testing.T) {
	t.Run("tampered_data", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		content := testContent(5, 8<<10)
		fp := putBlock(t, s, content)
		path := blockFilePath(blocksDir, fp)

		flipByte(t, path, HeaderSize+100)
		if _, err := s.Open(fp); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("篡改数据后 Open 错误 = %v，期望 ErrCorrupt", err)
		}

		// 校验失败后 Remove + 重新 Put 可恢复（对应"校验失败重下"路径）。
		if err := s.Remove(fp); err != nil {
			t.Fatalf("Remove(受损块) 失败: %v", err)
		}
		if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
			t.Fatalf("移除后 Open 错误 = %v，期望 ErrNotFound", err)
		}
		fp2 := putBlock(t, s, content)
		if fp2 != fp {
			t.Fatalf("重写指纹 = %s，期望 %s", fp2, fp)
		}
		if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
			t.Fatal("重写后读回内容不一致")
		}
	})

	t.Run("tampered_magic", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		fp := putBlock(t, s, testContent(6, 4<<10))

		flipByte(t, blockFilePath(blocksDir, fp), 0) // 破坏 magic 首字节
		if _, err := s.Open(fp); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("篡改 magic 后 Open 错误 = %v，期望 ErrCorrupt", err)
		}
	})

	t.Run("sha256_catches_rekeyed_block", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3SHA256)
		content := testContent(7, 8<<10)
		fp := putBlock(t, s, content)

		// 好块可正常读回。
		if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
			t.Fatal("好块读回内容不一致")
		}

		// 篡改 1 字节数据，同时把块头 xxh3 改成重算值并把文件改到新指纹名下：
		// xxh3 校验将通过，双重校验只剩 sha256 兜底 → 仍须报 ErrCorrupt。
		raw, err := os.ReadFile(blockFilePath(blocksDir, fp))
		if err != nil {
			t.Fatalf("读块文件失败: %v", err)
		}
		hdr, err := ParseHeader(raw[:HeaderSize])
		if err != nil {
			t.Fatalf("ParseHeader 失败: %v", err)
		}
		if hdr.Flags&FlagHasSHA256 == 0 {
			t.Fatal("VerifyXXH3SHA256 写入的块头未置 FlagHasSHA256")
		}
		tampered := append([]byte(nil), raw...)
		tampered[HeaderSize] ^= 0xFF
		newFP := Fingerprint(digest.XXH3_128(tampered[HeaderSize:]))
		if newFP == fp {
			t.Fatal("测试构造失败：篡改后 xxh3 未变")
		}
		newHdr := Header{
			Version:   headerVersion,
			Flags:     hdr.Flags,
			BlockSize: hdr.BlockSize,
			XXH3:      [16]byte(newFP),
			SHA256:    hdr.SHA256, // 仍是篡改前数据的 sha256
		}
		enc := newHdr.Encode()
		copy(tampered[:HeaderSize], enc[:])
		newPath := blockFilePath(blocksDir, newFP)
		if err := os.MkdirAll(filepath.Dir(newPath), dirPerm); err != nil {
			t.Fatalf("建分片目录失败: %v", err)
		}
		if err := os.WriteFile(newPath, tampered, 0o644); err != nil {
			t.Fatalf("写篡改块失败: %v", err)
		}

		if _, err := s.Open(newFP); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("xxh3 重算通过但 sha256 不符时 Open 错误 = %v，期望 ErrCorrupt", err)
		}
	})
}

// Remove 在引用计数/pin 非零时返回 ErrInUse。
func TestRemoveInUse(t *testing.T) {
	t.Run("reference_count", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		fp := putBlock(t, s, testContent(9, 4<<10))
		path := blockFilePath(blocksDir, fp)

		if err := s.Acquire(fp); err != nil {
			t.Fatalf("Acquire 失败: %v", err)
		}
		if err := s.Acquire(fp); err != nil {
			t.Fatalf("第二次 Acquire 失败: %v", err)
		}
		if err := s.Remove(fp); !errors.Is(err, ErrInUse) {
			t.Fatalf("refcount=2 时 Remove 错误 = %v，期望 ErrInUse", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("ErrInUse 后块文件不应被删除: %v", err)
		}
		s.Release(fp)
		if err := s.Remove(fp); !errors.Is(err, ErrInUse) {
			t.Fatalf("refcount=1 时 Remove 错误 = %v，期望 ErrInUse", err)
		}
		s.Release(fp)
		if err := s.Remove(fp); err != nil {
			t.Fatalf("refcount=0 时 Remove 失败: %v", err)
		}
		if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Remove 后 Open 错误 = %v，期望 ErrNotFound", err)
		}
	})

	t.Run("reader_pin", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		fp := putBlock(t, s, testContent(10, 4<<10))
		path := blockFilePath(blocksDir, fp)

		if err := s.Pin(fp); err != nil {
			t.Fatalf("Pin 失败: %v", err)
		}
		if err := s.Remove(fp); !errors.Is(err, ErrInUse) {
			t.Fatalf("pin>0 时 Remove 错误 = %v，期望 ErrInUse", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("ErrInUse 后块文件不应被删除: %v", err)
		}
		s.Unpin(fp)
		if err := s.Remove(fp); err != nil {
			t.Fatalf("pin 归零后 Remove 失败: %v", err)
		}
	})
}

func TestRemoveNotFound(t *testing.T) {
	s, _ := newTestStore(t, VerifyXXH3)
	missing := Fingerprint(digest.XXH3_128([]byte("never put")))
	if err := s.Remove(missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Remove(不存在) 错误 = %v，期望 ErrNotFound", err)
	}

	fp := putBlock(t, s, []byte("gone soon"))
	if err := s.Remove(fp); err != nil {
		t.Fatalf("Remove 失败: %v", err)
	}
	if err := s.Remove(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("重复 Remove 错误 = %v，期望 ErrNotFound", err)
	}
	if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open(已删除) 错误 = %v，期望 ErrNotFound", err)
	}
}

func TestOpenRead(t *testing.T) {
	s, _ := newTestStore(t, VerifyXXH3)

	// 边界：空块。
	fpEmpty := putBlock(t, s, nil)
	r, err := s.Open(fpEmpty)
	if err != nil {
		t.Fatalf("Open(空块) 失败: %v", err)
	}
	if r.Size() != 0 {
		t.Fatalf("空块 Size() = %d，期望 0", r.Size())
	}
	if b, err := io.ReadAll(r); err != nil || len(b) != 0 {
		t.Fatalf("空块 ReadAll = %q, %v，期望空内容", b, err)
	}
	if _, err := r.Read(make([]byte, 4)); !errors.Is(err, io.EOF) {
		t.Fatalf("空块读完后 Read 错误 = %v，期望 io.EOF", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close(空块) 失败: %v", err)
	}

	// 一般块：非对齐小缓冲分块读。
	content := testContent(11, 5000)
	fp := putBlock(t, s, content)
	br, err := s.Open(fp)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if br.Size() != int64(len(content)) {
		t.Fatalf("Size() = %d，期望 %d", br.Size(), len(content))
	}
	var buf bytes.Buffer
	chunk := make([]byte, 7)
	for {
		n, err := br.Read(chunk)
		buf.Write(chunk[:n])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("读块失败: %v", err)
		}
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatalf("分块读回内容不一致（读回 %d 字节，期望 %d）", buf.Len(), len(content))
	}
	if _, err := br.Read(chunk); !errors.Is(err, io.EOF) {
		t.Fatalf("读完后 Read 错误 = %v，期望 io.EOF", err)
	}
	if err := br.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
}

// 块头落盘布局：xxh3 模式 flags=0 且 sha256 全 0；xxh3+sha256 模式 flags bit0 置位。
func TestPutBlockHeader(t *testing.T) {
	content := testContent(8, 1<<10)

	t.Run("xxh3", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		fp := putBlock(t, s, content)
		raw, err := os.ReadFile(blockFilePath(blocksDir, fp))
		if err != nil {
			t.Fatalf("读块文件失败: %v", err)
		}
		if len(raw) != HeaderSize+len(content) {
			t.Fatalf("块文件大小 = %d，期望 %d", len(raw), HeaderSize+len(content))
		}
		if string(raw[0:4]) != Magic {
			t.Fatalf("magic = %q，期望 %q", raw[0:4], Magic)
		}
		hdr, err := ParseHeader(raw[:HeaderSize])
		if err != nil {
			t.Fatalf("ParseHeader 失败: %v", err)
		}
		if hdr.Flags != 0 {
			t.Fatalf("xxh3 模式 flags = %d，期望 0", hdr.Flags)
		}
		if hdr.SHA256 != ([32]byte{}) {
			t.Fatal("xxh3 模式块头 sha256 应为全 0")
		}
		if hdr.BlockSize != uint32(len(content)) {
			t.Fatalf("block_size = %d，期望 %d", hdr.BlockSize, len(content))
		}
		if Fingerprint(hdr.XXH3) != fp {
			t.Fatalf("块头 xxh3 = %s，期望指纹 %s", Fingerprint(hdr.XXH3), fp)
		}
	})

	t.Run("xxh3_sha256", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3SHA256)
		fp := putBlock(t, s, content)
		raw, err := os.ReadFile(blockFilePath(blocksDir, fp))
		if err != nil {
			t.Fatalf("读块文件失败: %v", err)
		}
		hdr, err := ParseHeader(raw[:HeaderSize])
		if err != nil {
			t.Fatalf("ParseHeader 失败: %v", err)
		}
		if hdr.Flags&FlagHasSHA256 == 0 {
			t.Fatal("块头未置 FlagHasSHA256")
		}
		if want := digest.SHA256(content); hdr.SHA256 != want {
			t.Fatalf("块头 sha256 = %x，期望 %x", hdr.SHA256, want)
		}
		if Fingerprint(hdr.XXH3) != fp {
			t.Fatalf("块头 xxh3 = %s，期望指纹 %s", Fingerprint(hdr.XXH3), fp)
		}
		if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
			t.Fatal("sha256 模式读回内容不一致")
		}
	})
}

func TestPutContextCanceled(t *testing.T) {
	t.Run("before_start", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := s.Put(ctx, bytes.NewReader([]byte("data"))); !errors.Is(err, context.Canceled) {
			t.Fatalf("Put(ctx 已取消) 错误 = %v，期望 context.Canceled", err)
		}
		assertNoResidue(t, blocksDir, 0)
	})

	t.Run("mid_stream", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		content := testContent(12, 1<<20)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := &cancelAfterReader{r: bytes.NewReader(content), cancel: cancel, after: 64 << 10}

		if _, _, err := s.Put(ctx, r); !errors.Is(err, context.Canceled) {
			t.Fatalf("Put(中途取消) 错误 = %v，期望 context.Canceled", err)
		}
		// 中断的 Put 清理临时文件，且不留下可被 Open 的块。
		assertNoResidue(t, blocksDir, 0)
		if _, err := s.Open(Fingerprint(digest.XXH3_128(content))); !errors.Is(err, ErrNotFound) {
			t.Fatalf("取消的 Put 后 Open 错误 = %v，期望 ErrNotFound", err)
		}
	})
}

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{
		Version:   headerVersion,
		Flags:     FlagHasSHA256,
		BlockSize: 0xDEADBEEF,
		XXH3:      [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		SHA256:    [32]byte{31: 0xFF},
	}
	enc := h.Encode()

	// 布局逐字段核对（design.md §6.3，全部小端）。
	if string(enc[0:4]) != Magic {
		t.Fatalf("magic = %q，期望 %q", enc[0:4], Magic)
	}
	if v := binary.LittleEndian.Uint16(enc[4:6]); v != headerVersion {
		t.Fatalf("version = %d，期望 %d", v, headerVersion)
	}
	if v := binary.LittleEndian.Uint16(enc[6:8]); v != FlagHasSHA256 {
		t.Fatalf("flags = %d，期望 %d", v, FlagHasSHA256)
	}
	if v := binary.LittleEndian.Uint32(enc[8:12]); v != h.BlockSize {
		t.Fatalf("block_size = %d，期望 %d", v, h.BlockSize)
	}
	if !bytes.Equal(enc[12:28], h.XXH3[:]) {
		t.Fatal("xxh3 字段不一致")
	}
	if !bytes.Equal(enc[28:60], h.SHA256[:]) {
		t.Fatal("sha256 字段不一致")
	}
	for i := 60; i < HeaderSize; i++ {
		if enc[i] != 0 {
			t.Fatalf("reserved[%d] = %d，期望 0", i, enc[i])
		}
	}

	got, err := ParseHeader(enc[:])
	if err != nil {
		t.Fatalf("ParseHeader(Encode 结果) 失败: %v", err)
	}
	if got != h {
		t.Fatalf("往返不一致: %+v vs %+v", got, h)
	}

	// 长于 128B 的输入按前 128B 解析。
	got, err = ParseHeader(append(enc[:], 0xFF))
	if err != nil || got != h {
		t.Fatalf("ParseHeader(超长输入) = %+v, %v", got, err)
	}
}

func TestParseHeaderErrors(t *testing.T) {
	valid := Header{Version: headerVersion}.Encode()

	for _, n := range []int{0, 1, 4, 60, 127} {
		if _, err := ParseHeader(valid[:n]); err == nil {
			t.Fatalf("ParseHeader(长度 %d) 应当报错", n)
		}
	}
	if _, err := ParseHeader(nil); err == nil {
		t.Fatal("ParseHeader(nil) 应当报错")
	}

	badMagic := valid
	badMagic[0] = 'X'
	if _, err := ParseHeader(badMagic[:]); err == nil {
		t.Fatal("坏 magic 应当报错")
	}

	badVersion := valid
	binary.LittleEndian.PutUint16(badVersion[4:6], 2)
	if _, err := ParseHeader(badVersion[:]); err == nil {
		t.Fatal("不支持的 version 应当报错")
	}

	badReserved := valid
	badReserved[HeaderSize-1] = 1
	if _, err := ParseHeader(badReserved[:]); err == nil {
		t.Fatal("reserved 非 0 应当报错")
	}
}

func TestFingerprintHex(t *testing.T) {
	var f Fingerprint
	for i := range f {
		f[i] = byte(i * 17)
	}
	s := f.String()
	if len(s) != 32 {
		t.Fatalf("String() 长度 = %d，期望 32", len(s))
	}
	if want := hex.EncodeToString(f[:]); s != want {
		t.Fatalf("String() = %q，期望 %q", s, want)
	}
	if s != strings.ToLower(s) {
		t.Fatalf("String() 含大写字符: %q", s)
	}

	got, err := FingerprintFromHex(s)
	if err != nil {
		t.Fatalf("FingerprintFromHex(%q) 失败: %v", s, err)
	}
	if got != f {
		t.Fatalf("往返不一致: %s vs %s", got, f)
	}

	if _, err := FingerprintFromHex(s[:31]); err == nil {
		t.Fatal("短一个字符应当报错")
	}
	if _, err := FingerprintFromHex(s + "00"); err == nil {
		t.Fatal("长两个字符应当报错")
	}
	if _, err := FingerprintFromHex(strings.Repeat("z", 32)); err == nil {
		t.Fatal("非 hex 字符应当报错")
	}
	if _, err := FingerprintFromHex(""); err == nil {
		t.Fatal("空串应当报错")
	}

	var zero Fingerprint
	if z := zero.String(); z != strings.Repeat("0", 32) {
		t.Fatalf("零指纹 String() = %q", z)
	}
	if got, err := FingerprintFromHex(zero.String()); err != nil || got != zero {
		t.Fatalf("零指纹往返失败: %v", err)
	}
}

// TestList 覆盖 D20：枚举 blocks/ 下全部块指纹（升序、忽略临时文件与非法文件名）。
func TestList(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)

	// 空存储 → 空切片（非 nil）。
	fps, err := s.List()
	if err != nil {
		t.Fatalf("List 失败: %v", err)
	}
	if fps == nil || len(fps) != 0 {
		t.Fatalf("空存储 List = %#v，期望空切片", fps)
	}

	fp1 := putBlock(t, s, []byte("list-a"))
	fp2 := putBlock(t, s, []byte("list-b"))
	fp3 := putBlock(t, s, []byte("list-c"))

	// 预置干扰文件：孤儿临时文件、非法 .blk 文件名、子目录中的非法名 → 必须全部忽略。
	if err := os.WriteFile(filepath.Join(blocksDir, tempPrefix+"orphan"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("写临时文件失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(blocksDir, "not-a-fingerprint.blk"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("写非法块文件名失败: %v", err)
	}
	oddDir := filepath.Join(blocksDir, "zz")
	if err := os.MkdirAll(oddDir, dirPerm); err != nil {
		t.Fatalf("建目录失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(oddDir, "bad.blk"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("写子目录非法块文件名失败: %v", err)
	}

	fps, err = s.List()
	if err != nil {
		t.Fatalf("List 失败: %v", err)
	}
	if len(fps) != 3 {
		t.Fatalf("List 返回 %d 个块指纹（%v），期望 3", len(fps), fps)
	}
	seen := make(map[Fingerprint]bool, len(fps))
	for _, fp := range fps {
		seen[fp] = true
	}
	for _, want := range []Fingerprint{fp1, fp2, fp3} {
		if !seen[want] {
			t.Fatalf("List 缺少块 %s：%v", want, fps)
		}
	}
	// 升序断言（字节序）。
	for i := 1; i < len(fps); i++ {
		if bytes.Compare(fps[i-1][:], fps[i][:]) >= 0 {
			t.Fatalf("List 未按升序返回: %v", fps)
		}
	}
	// 枚举出的每个块均可经 Open 校验读回。
	for _, fp := range fps {
		readBlock(t, s, fp)
	}
}

// TestEvictIgnoresRefcount 覆盖 D28：refcount 不拦 Evict——Acquire 后仍可淘汰
// 本地副本、块文件消失；Evict 不动 refs 簿记，后续 Release 不受影响。
func TestEvictIgnoresRefcount(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	fp := putBlock(t, s, testContent(13, 4<<10))
	path := blockFilePath(blocksDir, fp)

	if err := s.Acquire(fp); err != nil {
		t.Fatalf("第一次 Acquire 失败: %v", err)
	}
	if err := s.Acquire(fp); err != nil {
		t.Fatalf("第二次 Acquire 失败: %v", err)
	}

	if err := s.Evict(fp); err != nil {
		t.Fatalf("refcount=2 时 Evict 失败: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Evict 后块文件应消失，Stat 错误 = %v", err)
	}
	if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Evict 后 Open 错误 = %v，期望 ErrNotFound", err)
	}
	// refs 表不变：引用计数仍为 2，Evict 不触碰簿记。
	s.mu.Lock()
	refs := s.refs[fp]
	s.mu.Unlock()
	if refs != 2 {
		t.Fatalf("Evict 后 refs[%s] = %d，期望 2（Evict 不动 refs）", fp, refs)
	}
	// 行为校验：引用未 Release 时 Remove 仍被 refcount 拦（ErrInUse 先于缺块判定）。
	if err := s.Remove(fp); !errors.Is(err, ErrInUse) {
		t.Fatalf("Evict 后（引用未 Release）Remove 错误 = %v，期望 ErrInUse", err)
	}
	// 后续 Release 不受影响：两次 Release 归零（空操作语义与 Evict 前一致）。
	s.Release(fp)
	s.Release(fp)
	s.mu.Lock()
	refs = s.refs[fp]
	s.mu.Unlock()
	if refs != 0 {
		t.Fatalf("两次 Release 后 refs[%s] = %d，期望 0", fp, refs)
	}
	if err := s.Remove(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Release 归零后 Remove 错误 = %v，期望 ErrNotFound（引用已归零，仅缺块）", err)
	}
}

// TestEvictPinned 覆盖 D28：pin>0 时 Evict 返回 ErrInUse 且块文件仍在；
// Unpin 后 Evict 成功、块可被淘汰。
func TestEvictPinned(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	content := testContent(14, 4<<10)
	fp := putBlock(t, s, content)
	path := blockFilePath(blocksDir, fp)

	if err := s.Pin(fp); err != nil {
		t.Fatalf("Pin 失败: %v", err)
	}
	if err := s.Evict(fp); !errors.Is(err, ErrInUse) {
		t.Fatalf("pin>0 时 Evict 错误 = %v，期望 ErrInUse", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("ErrInUse 后块文件不应被删除: %v", err)
	}
	if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
		t.Fatal("ErrInUse 后读回内容不一致")
	}

	s.Unpin(fp)
	if err := s.Evict(fp); err != nil {
		t.Fatalf("pin 归零后 Evict 失败: %v", err)
	}
	if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Evict 后 Open 错误 = %v，期望 ErrNotFound", err)
	}
}

// TestEvictNotFound 覆盖 D28：Evict 对 blocks/ 中不存在的块返回 ErrNotFound
// （从未存在、已淘汰过、仅剩 quarantine/ 副本均如此）。
func TestEvictNotFound(t *testing.T) {
	s, _ := newTestStore(t, VerifyXXH3)
	missing := Fingerprint(digest.XXH3_128([]byte("never put")))
	if err := s.Evict(missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Evict(不存在) 错误 = %v，期望 ErrNotFound", err)
	}

	fp := putBlock(t, s, []byte("evicted soon"))
	if err := s.Evict(fp); err != nil {
		t.Fatalf("Evict 失败: %v", err)
	}
	if err := s.Evict(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("重复 Evict 错误 = %v，期望 ErrNotFound", err)
	}

	// 块仅存在于 quarantine/ 时同样报 ErrNotFound，且隔离副本不受影响：
	// Evict 只删 blocks/ 下的正常块路径，不涉及 quarantine/。
	fpQ := putBlock(t, s, []byte("quarantined only"))
	if err := s.Quarantine(fpQ); err != nil {
		t.Fatalf("Quarantine 失败: %v", err)
	}
	if err := s.Evict(fpQ); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Evict(仅隔离副本) 错误 = %v，期望 ErrNotFound", err)
	}
	if _, err := os.Stat(s.quarantinePath(fpQ)); err != nil {
		t.Fatalf("Evict 不应触碰 quarantine/ 副本: %v", err)
	}
}

// TestEvictThenReput 覆盖 D28 的淘汰后重下路径：Evict 后同内容 Put 复原（同指纹）、
// Open 校验通过、无临时残留。
func TestEvictThenReput(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	content := testContent(15, 64<<10)
	fp := putBlock(t, s, content)

	if err := s.Evict(fp); err != nil {
		t.Fatalf("Evict 失败: %v", err)
	}
	if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Evict 后 Open 错误 = %v，期望 ErrNotFound", err)
	}

	// 模拟 fetch 重下：同内容再次 Put → 同指纹，块回到原位。
	fp2 := putBlock(t, s, content)
	if fp2 != fp {
		t.Fatalf("重下指纹 = %s，期望 %s", fp2, fp)
	}
	if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
		t.Fatal("重下后读回内容不一致")
	}
	assertNoResidue(t, blocksDir, 1)
}

// TestVerifyXXH3SHA256Sha256Mismatch 覆盖双重校验分支——块头 SHA256 与数据实际
// SHA256 不符（XXH3 仍正确）时，Open 必须返回 ErrCorrupt。
// 现有 tampered_data / tampered_magic / sha256_catches_rekeyed_block 分别覆盖了
// 数据区篡改（XXH3 失败）、magic 篡改、以及"重算 XXH3 通过但 SHA256 不匹配且指纹
// 也变了"的场景。本测试补齐"指纹没变、XXH3 通过但 SHA256 签名被单独破坏"分支。
//
// 块头布局（header.go）：offset 28..60 是 SHA256 字段，offset 12..28 是 XXH3 字段。
// 本测试只翻转 SHA256 区域的 1 字节，保证：
//   - hdr.XXH3 == fp（步骤 2 通过）
//   - 数据区完全不变 → 重算 XXH3 == hdr.XXH3（步骤 4 通过）
//   - 重算 SHA256 != 被篡改的 hdr.SHA256（步骤 5 失败 → ErrCorrupt）
func TestVerifyXXH3SHA256Sha256Mismatch(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3SHA256)
	content := testContent(16, 8<<10)
	fp := putBlock(t, s, content)
	path := blockFilePath(blocksDir, fp)

	// 好块先验证能正常读回。
	if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
		t.Fatal("写入后读回内容不一致")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读块文件失败: %v", err)
	}
	if _, err := ParseHeader(raw[:HeaderSize]); err != nil {
		t.Fatalf("原始块头不可解析: %v", err)
	}

	// 翻转 SHA256 区域的 1 字节（offset 28 是块头 SHA256 字段首字节）。
	// XXH3 区域（offset 12..28）与数据区（offset HeaderSize..末尾）一律不动。
	tampered := append([]byte(nil), raw...)
	tampered[28] ^= 0xFF
	if err := os.WriteFile(path, tampered, 0o644); err != nil {
		t.Fatalf("写篡改块失败: %v", err)
	}

	if _, err := s.Open(fp); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("SHA256 区域被单独篡改后 Open 错误 = %v，期望 ErrCorrupt", err)
	}
}
