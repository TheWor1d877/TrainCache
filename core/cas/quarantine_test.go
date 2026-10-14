package cas

// 坏块隔离（quarantine）能力测试（M8）：Quarantine / ListQuarantined /
// RemoveQuarantined 三方法的幂等语义、修复循环、枚举规则与并发安全。

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"traincache/core/internal/digest"
)

// quarantineFilePath 返回指纹对应的隔离块文件路径（与 blockPath 同两级分片布局）。
func quarantineFilePath(quarantineDir string, fp Fingerprint) string {
	hexs := fp.String()
	return filepath.Join(quarantineDir, hexs[:2], hexs+".blk")
}

// containsFingerprint 报告 fps 中是否含 fp。
func containsFingerprint(fps []Fingerprint, fp Fingerprint) bool {
	return slices.Contains(fps, fp)
}

// TestNewCreatesQuarantineDir：New 后根下 quarantine/ 目录存在，且与 blocks/ 并列。
func TestNewCreatesQuarantineDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "cache")
	if _, err := New(root, VerifyXXH3); err != nil {
		t.Fatalf("New(%q) 失败: %v", root, err)
	}
	blocksSt, err := os.Stat(filepath.Join(root, blocksDirName))
	if err != nil {
		t.Fatalf("blocks 目录未创建: %v", err)
	}
	quarantineSt, err := os.Stat(filepath.Join(root, quarantineDirName))
	if err != nil {
		t.Fatalf("quarantine 目录未创建: %v", err)
	}
	if !blocksSt.IsDir() || !quarantineSt.IsDir() {
		t.Fatal("blocks 与 quarantine 均应为目录")
	}
}

// TestQuarantine：隔离把块从 blocks/ 原子移入 quarantine/，此后 Open/List/Remove
// 均视其不在 blocks/，而 ListQuarantined 可见。
func TestQuarantine(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	quarantineDir := filepath.Join(filepath.Dir(blocksDir), quarantineDirName)
	content := testContent(101, 4<<10)
	fp := putBlock(t, s, content)

	// 隔离前：块可读。
	if _, err := s.Open(fp); err != nil {
		t.Fatalf("隔离前 Open 失败: %v", err)
	}
	raw, err := os.ReadFile(blockFilePath(blocksDir, fp))
	if err != nil {
		t.Fatalf("读块文件失败: %v", err)
	}

	if err := s.Quarantine(fp); err != nil {
		t.Fatalf("Quarantine 失败: %v", err)
	}

	// blocks 下原文件消失、quarantine 下出现且字节原样（rename 移动而非改写）。
	if _, err := os.Stat(blockFilePath(blocksDir, fp)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("隔离后 blocks 下原文件仍存在: %v", err)
	}
	moved, err := os.ReadFile(quarantineFilePath(quarantineDir, fp))
	if err != nil {
		t.Fatalf("quarantine 下文件不存在: %v", err)
	}
	if !bytes.Equal(moved, raw) {
		t.Fatal("隔离文件中字节与原块不一致")
	}

	// 隔离后：Open、List、Remove 均视其不在 blocks/。
	if _, err := s.Open(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("隔离后 Open 错误 = %v，期望 ErrNotFound", err)
	}
	fps, err := s.List()
	if err != nil {
		t.Fatalf("List 失败: %v", err)
	}
	if containsFingerprint(fps, fp) {
		t.Fatalf("隔离后 List 仍含 %s: %v", fp, fps)
	}
	if err := s.Remove(fp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("隔离后 Remove 错误 = %v，期望 ErrNotFound", err)
	}

	// ListQuarantined 可见该块。
	qfps, err := s.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined 失败: %v", err)
	}
	if !containsFingerprint(qfps, fp) {
		t.Fatalf("ListQuarantined 缺少 %s: %v", fp, qfps)
	}
}

// TestQuarantineIdempotent：重复隔离与隔离从未存在的块均为空操作。
func TestQuarantineIdempotent(t *testing.T) {
	t.Run("already_quarantined", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		quarantineDir := filepath.Join(filepath.Dir(blocksDir), quarantineDirName)
		fp := putBlock(t, s, testContent(102, 2<<10))

		if err := s.Quarantine(fp); err != nil {
			t.Fatalf("第一次 Quarantine 失败: %v", err)
		}
		if err := s.Quarantine(fp); err != nil {
			t.Fatalf("第二次 Quarantine(已在 quarantine) = %v，期望 nil", err)
		}

		// quarantine 下恰一份块文件、无多余文件。
		blocks, others := listFiles(t, quarantineDir)
		want := quarantineFilePath(quarantineDir, fp)
		if len(blocks) != 1 || blocks[0] != want {
			t.Fatalf("quarantine 下块文件 = %v，期望恰为 %s", blocks, want)
		}
		if len(others) != 0 {
			t.Fatalf("quarantine 下存在多余文件: %v", others)
		}
		if _, err := os.Stat(blockFilePath(blocksDir, fp)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("blocks 下不应再有该块: %v", err)
		}
	})

	t.Run("never_existed", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		quarantineDir := filepath.Join(filepath.Dir(blocksDir), quarantineDirName)
		fp := Fingerprint(digest.XXH3_128([]byte("never put quarantine")))

		if err := s.Quarantine(fp); err != nil {
			t.Fatalf("Quarantine(从未存在的块) = %v，期望 nil", err)
		}
		if _, err := os.Stat(blockFilePath(blocksDir, fp)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("blocks 下不应有该文件: %v", err)
		}
		if _, err := os.Stat(quarantineFilePath(quarantineDir, fp)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("quarantine 下不应有该文件: %v", err)
		}
	})
}

// TestQuarantineRepairCycle：坏块隔离 → 修复重写 → 清理隔离副本的完整回路。
func TestQuarantineRepairCycle(t *testing.T) {
	t.Run("corrupt_then_repair", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		quarantineDir := filepath.Join(filepath.Dir(blocksDir), quarantineDirName)
		content := testContent(103, 8<<10)
		fp := putBlock(t, s, content)

		// 翻数据区 1 字节 → Open 报 ErrCorrupt。
		flipByte(t, blockFilePath(blocksDir, fp), HeaderSize+100)
		if _, err := s.Open(fp); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("篡改后 Open 错误 = %v，期望 ErrCorrupt", err)
		}

		if err := s.Quarantine(fp); err != nil {
			t.Fatalf("Quarantine(坏块) 失败: %v", err)
		}
		if _, err := os.Stat(quarantineFilePath(quarantineDir, fp)); err != nil {
			t.Fatalf("坏块未移入 quarantine: %v", err)
		}

		// 修复重写同内容：新块落回 blocks 原位、可读。
		fp2 := putBlock(t, s, content)
		if fp2 != fp {
			t.Fatalf("重写指纹 = %s，期望 %s", fp2, fp)
		}
		if got := readBlock(t, s, fp); !bytes.Equal(got, content) {
			t.Fatal("重写后读回内容不一致")
		}

		// 修复成功后清理隔离副本；重复清理为幂等空操作。
		if err := s.RemoveQuarantined(fp); err != nil {
			t.Fatalf("RemoveQuarantined 失败: %v", err)
		}
		if _, err := os.Stat(quarantineFilePath(quarantineDir, fp)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("RemoveQuarantined 后隔离文件仍存在: %v", err)
		}
		qfps, err := s.ListQuarantined()
		if err != nil {
			t.Fatalf("ListQuarantined 失败: %v", err)
		}
		if containsFingerprint(qfps, fp) {
			t.Fatalf("清理后 ListQuarantined 仍含 %s: %v", fp, qfps)
		}
		if err := s.RemoveQuarantined(fp); err != nil {
			t.Fatalf("重复 RemoveQuarantined = %v，期望 nil", err)
		}
	})

	t.Run("refs_preserved", func(t *testing.T) {
		s, blocksDir := newTestStore(t, VerifyXXH3)
		content := testContent(104, 4<<10)
		fp := putBlock(t, s, content)

		if err := s.Acquire(fp); err != nil {
			t.Fatalf("Acquire 失败: %v", err)
		}
		if err := s.Quarantine(fp); err != nil {
			t.Fatalf("Quarantine 失败: %v", err)
		}
		// 修复重写回 blocks/。
		if fp2 := putBlock(t, s, content); fp2 != fp {
			t.Fatalf("重写指纹 = %s，期望 %s", fp2, fp)
		}
		// 隔离不动引用计数：refs 仍为 1 → Remove 报 ErrInUse。
		if err := s.Remove(fp); !errors.Is(err, ErrInUse) {
			t.Fatalf("refs=1 时 Remove 错误 = %v，期望 ErrInUse（引用不因隔离清零）", err)
		}
		// Release 一次归零 → Remove 成功。
		s.Release(fp)
		if err := s.Remove(fp); err != nil {
			t.Fatalf("引用归零后 Remove 失败: %v", err)
		}
		if _, err := os.Stat(blockFilePath(blocksDir, fp)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Remove 后块文件仍存在: %v", err)
		}
	})
}

// TestListQuarantined：枚举规则与 List 一致（升序、空切片、忽略干扰物），
// 且与 List 互斥（正常块不在隔离列表，隔离块不在正常列表）。
func TestListQuarantined(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	quarantineDir := filepath.Join(filepath.Dir(blocksDir), quarantineDirName)

	// 空 quarantine → 空切片（非 nil）。
	fps, err := s.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined 失败: %v", err)
	}
	if fps == nil || len(fps) != 0 {
		t.Fatalf("空 quarantine ListQuarantined = %#v，期望空切片", fps)
	}

	// 隔离 3 块，另留 1 个正常块用于互斥断言。
	fp1 := putBlock(t, s, []byte("quarantine-a"))
	fp2 := putBlock(t, s, []byte("quarantine-b"))
	fp3 := putBlock(t, s, []byte("quarantine-c"))
	keep := putBlock(t, s, []byte("stay-in-blocks"))
	for _, fp := range []Fingerprint{fp1, fp2, fp3} {
		if err := s.Quarantine(fp); err != nil {
			t.Fatalf("Quarantine(%s) 失败: %v", fp, err)
		}
	}

	// 预置干扰物：tmp- 临时文件、非法 .blk 文件名、非法子目录 → 必须全部忽略。
	if err := os.WriteFile(filepath.Join(quarantineDir, tempPrefix+"orphan"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("写临时文件失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(quarantineDir, "not-a-fingerprint.blk"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("写非法块文件名失败: %v", err)
	}
	oddDir := filepath.Join(quarantineDir, "zz")
	if err := os.MkdirAll(oddDir, dirPerm); err != nil {
		t.Fatalf("建目录失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(oddDir, "bad.blk"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("写子目录非法块文件名失败: %v", err)
	}

	fps, err = s.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined 失败: %v", err)
	}
	if len(fps) != 3 {
		t.Fatalf("ListQuarantined 返回 %d 个块指纹（%v），期望 3", len(fps), fps)
	}
	seen := make(map[Fingerprint]bool, len(fps))
	for _, fp := range fps {
		seen[fp] = true
	}
	for _, want := range []Fingerprint{fp1, fp2, fp3} {
		if !seen[want] {
			t.Fatalf("ListQuarantined 缺少块 %s: %v", want, fps)
		}
	}
	// 升序断言（字节序）。
	for i := 1; i < len(fps); i++ {
		if bytes.Compare(fps[i-1][:], fps[i][:]) >= 0 {
			t.Fatalf("ListQuarantined 未按升序返回: %v", fps)
		}
	}

	// 互斥：正常块不在 ListQuarantined，隔离块不在 List。
	if containsFingerprint(fps, keep) {
		t.Fatalf("ListQuarantined 含 blocks 中的正常块 %s: %v", keep, fps)
	}
	blocksFps, err := s.List()
	if err != nil {
		t.Fatalf("List 失败: %v", err)
	}
	if len(blocksFps) != 1 || blocksFps[0] != keep {
		t.Fatalf("List = %v，期望仅 %s", blocksFps, keep)
	}
	for _, fp := range []Fingerprint{fp1, fp2, fp3} {
		if containsFingerprint(blocksFps, fp) {
			t.Fatalf("List 含已被隔离的块 %s: %v", fp, blocksFps)
		}
	}
}

// TestQuarantineConcurrent：并发隔离同一指纹全部成功（幂等 + 串行化），
// 最终 blocks 无该文件、quarantine 恰一份、无临时残留。
func TestQuarantineConcurrent(t *testing.T) {
	s, blocksDir := newTestStore(t, VerifyXXH3)
	quarantineDir := filepath.Join(filepath.Dir(blocksDir), quarantineDirName)
	fp := putBlock(t, s, testContent(105, 32<<10))

	const goroutines = 8
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make([]error, goroutines)
	)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = s.Quarantine(fp)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d Quarantine 失败: %v", i, err)
		}
	}
	if _, err := os.Stat(blockFilePath(blocksDir, fp)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("并发隔离后 blocks 下仍有该文件: %v", err)
	}
	assertNoResidue(t, blocksDir, 0)
	assertNoResidue(t, quarantineDir, 1)
	blocks, _ := listFiles(t, quarantineDir)
	if want := quarantineFilePath(quarantineDir, fp); len(blocks) != 1 || blocks[0] != want {
		t.Fatalf("quarantine 下块文件 = %v，期望恰为 %s", blocks, want)
	}
}
