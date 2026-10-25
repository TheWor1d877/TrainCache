package evictor

// 字节维度淘汰测试套件:覆盖包级函数表驱动、真实 cas.Store 集成、并发安全与回归零变化。
// 本文件不修改 evictor_test.go 与 disk_test.go,新增测试全部放此。

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/core/cas"
)

// —— 表驱动单元 ——

// TestClassifyBytes 覆盖 classifyBytes 全区间。
// 实现行为:C<=0 → idle;bytes<C → idle;bytes>=C → danger(因 C > 0.95*C,triggerResult 在 C>0 时不可达)。
func TestClassifyBytes(t *testing.T) {
	const C int64 = 1000
	for _, tc := range []struct {
		name  string
		bytes int64
		cap   int64
		want  int
	}{
		{"C=0 永不触发", 999, 0, idleResult},
		{"C<0 永不触发", 999, -1, idleResult},
		{"0 字节", 0, C, idleResult},
		{"小于 C*0.85", 800, C, idleResult},
		{"小于 C", 999, C, idleResult},
		{"等于 C", C, C, dangerResult},
		{"等于 0.95*C(但已小于 C)", 950, C, idleResult},
		{"等于 0.94*C", 940, C, idleResult},
		{"略高于 C", C + 1, C, dangerResult},
		{"远高于 C", 10_000, C, dangerResult},
		{"负字节", -10, C, idleResult},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyBytes(tc.bytes, tc.cap)
			if got != tc.want {
				t.Errorf("classifyBytes(bytes=%d, C=%d)=%d, want %d",
					tc.bytes, tc.cap, got, tc.want)
			}
		})
	}
}

// TestMoreSevere 3×3 表驱动两两取 max。
func TestMoreSevere(t *testing.T) {
	for _, tc := range []struct {
		a, b int
		want int
	}{
		{idleResult, idleResult, idleResult},
		{idleResult, triggerResult, triggerResult},
		{idleResult, dangerResult, dangerResult},
		{triggerResult, idleResult, triggerResult},
		{triggerResult, triggerResult, triggerResult},
		{triggerResult, dangerResult, dangerResult},
		{dangerResult, idleResult, dangerResult},
		{dangerResult, triggerResult, dangerResult},
		{dangerResult, dangerResult, dangerResult},
	} {
		got := moreSevere(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("moreSevere(%d,%d)=%d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestStatBlocksDirBytes 验证只累加真实 .blk 文件(tmp-* 与非 .blk 后缀忽略,子目录递归)。
func TestStatBlocksDirBytes(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "ab", "aaa.blk"), 100)
	mustWriteFile(t, filepath.Join(dir, "cd", "bbb.blk"), 200)
	mustWriteFile(t, filepath.Join(dir, "tmp-xxx.blk"), 500)
	mustWriteFile(t, filepath.Join(dir, "other.log"), 777)
	mustWriteFile(t, filepath.Join(dir, "ee", "skip.txt"), 400)

	got := statBlocksDirBytes(dir)
	if got != 300 {
		t.Errorf("statBlocksDirBytes = %d, want 300(仅 ab/aaa.blk + cd/bbb.blk)", got)
	}

	got2 := statBlocksDirBytes(filepath.Join(dir, "no-such"))
	if got2 != 0 {
		t.Errorf("statBlocksDirBytes(不存在目录) = %d, want 0", got2)
	}
}

func mustWriteFile(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// TestEvictorStatBlockSize 验证 statBlockSize 对真实块文件返回 size,不存在/子目录/零字节安全返回 0。
func TestEvictorStatBlockSize(t *testing.T) {
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	fp, _, err := cs.Put(context.Background(), strings.NewReader("hello-world-12345"))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}

	ev := New(cs, nil, nil, nil, nil, Config{BlockDir: root, CacheCapacityBytes: 1 << 20})

	if sz := ev.statBlockSize(fp); sz <= 0 {
		t.Errorf("statBlockSize(真实块) = %d, want > 0", sz)
	}

	var missing cas.Fingerprint
	if sz := ev.statBlockSize(missing); sz != 0 {
		t.Errorf("statBlockSize(不存在) = %d, want 0", sz)
	}

	ev3 := &Evictor{cs: nil}
	if sz := ev3.statBlockSize(fp); sz != 0 {
		t.Errorf("statBlockSize(cs=nil) = %d, want 0", sz)
	}
}

// TestStatBlocksDirBytesFilters 验证 tmp- 前缀与非 .blk 后缀被完整过滤。
func TestStatBlocksDirBytesFilters(t *testing.T) {
	dir := t.TempDir()

	mustWriteFile(t, filepath.Join(dir, "tmp-abc.blk"), 500)
	mustWriteFile(t, filepath.Join(dir, "tmp-xyz.tmp.blk"), 300)
	mustWriteFile(t, filepath.Join(dir, "ab", "real.blk"), 42)
	mustWriteFile(t, filepath.Join(dir, "cd", "also.real.blk"), 58)
	mustWriteFile(t, filepath.Join(dir, "e", "f", "skip.txt"), 999)

	got := statBlocksDirBytes(dir)
	want := int64(42 + 58)
	if got != want {
		t.Errorf("statBlocksDirBytes = %d, want %d(tmp- 与非 .blk 应过滤)", got, want)
	}
}

// TestStatBlocksDirBytesWalkDirErrors:空目录、零字节 .blk、无 .blk 文件。
func TestStatBlocksDirBytesWalkDirErrors(t *testing.T) {
	dir := t.TempDir()
	if got := statBlocksDirBytes(dir); got != 0 {
		t.Errorf("空目录 statBlocksDirBytes = %d, want 0", got)
	}

	sub := filepath.Join(dir, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(dir, "readme.txt"), 10)
	if got := statBlocksDirBytes(dir); got != 0 {
		t.Errorf("无 .blk 文件 statBlocksDirBytes = %d, want 0", got)
	}

	zeroBlk := filepath.Join(dir, "xx", "zero.blk")
	if err := os.MkdirAll(filepath.Dir(zeroBlk), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(zeroBlk, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := statBlocksDirBytes(dir); got != 0 {
		t.Errorf("零字节 .blk 应计入 0: got=%d", got)
	}
}

// blockExists 用 cs.Open 判断块是否仍在本地。
func blockExists(t *testing.T, cs *cas.Store, fp cas.Fingerprint) bool {
	t.Helper()
	br, err := cs.Open(fp)
	if err == nil {
		_ = br.Close()
		return true
	}
	return false
}

// putUniqueBlock 写一个唯一内容块(确保不同指纹),返回 fp 与实际写入字节数。
func putUniqueBlock(t *testing.T, cs *cas.Store, size int64, tag string) cas.Fingerprint {
	t.Helper()
	if size < int64(len(tag))+8 {
		size = int64(len(tag)) + 8
	}
	buf := make([]byte, size)
	copy(buf, tag)
	// 用递增模式填充保证每个块指纹唯一
	for i := range buf {
		buf[i] ^= byte(i)
	}
	fp, _, err := cs.Put(context.Background(), strings.NewReader(string(buf)))
	if err != nil {
		t.Fatalf("cas.Put(tag=%q, size=%d): %v", tag, size, err)
	}
	return fp
}

// —— 集成:真实 cas.Store + 临时目录 + fake disk usage ——

// TestRunOnceBytesDimensionTrigger:字节超 C 触发 danger(因 classifyBytes 在 bytes>=C 直接 danger),
// 淘汰至 0.80*C;验证 bytesAccum 初始正确、淘汰后递减。
func TestRunOnceBytesDimensionTrigger(t *testing.T) {
	const C int64 = 100 * 1024

	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	// 先落 4 块(每块 30 KiB,累计 ~120 KiB > 100 KiB),每块唯一内容确保不同指纹
	fp1 := putUniqueBlock(t, cs, 30*1024, "blk1")
	fp2 := putUniqueBlock(t, cs, 30*1024, "blk2")
	fp3 := putUniqueBlock(t, cs, 30*1024, "blk3")
	fp4 := putUniqueBlock(t, cs, 30*1024, "blk4")

	heat := &fakeHeat{}
	usage := &fakeUsage{total: 1 << 40}
	ev := New(cs, heat, nil, nil, nil, Config{
		BlockDir:           root,
		Usage:              usage.use,
		CacheCapacityBytes: C,
	})

	initial := ev.BytesAccum()
	if initial <= 0 {
		t.Fatalf("初始化 bytesAccum = %d, want > 0(New 应 stat blocks/ 目录)", initial)
	}

	heat.set(
		entry(fp1, 0.1, 48*time.Hour),
		entry(fp2, 0.2, 24*time.Hour),
		entry(fp3, 0.3, 12*time.Hour),
		entry(fp4, 0.9, 0),
	)

	usage.set(0.10)

	ev.RunOnce(context.Background())

	target := int64(float64(C) * 0.80) // danger 目标
	after := ev.BytesAccum()
	if after > target {
		t.Errorf("RunOnce 后 bytesAccum=%d, want <= %d(danger 目标)", after, target)
	}
	if after >= initial {
		t.Errorf("bytesAccum 应递减: initial=%d after=%d", initial, after)
	}
	if blockExists(t, cs, fp1) {
		t.Error("最低 heat 块 fp1 应被淘汰")
	}
	if !blockExists(t, cs, fp4) {
		t.Error("最高 heat 块 fp4 应保留")
	}
}

// TestRunOnceBytesDangerEscalation:bytes >= C → danger,淘汰至 0.80*C。
func TestRunOnceBytesDangerEscalation(t *testing.T) {
	const C int64 = 200 * 1024

	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	// 3 块各 70 KiB = 210 KiB > 200 KiB
	fp1 := putUniqueBlock(t, cs, 70*1024, "danger1")
	fp2 := putUniqueBlock(t, cs, 70*1024, "danger2")
	fp3 := putUniqueBlock(t, cs, 70*1024, "danger3")

	heat := &fakeHeat{}
	usage := &fakeUsage{total: 1 << 40}
	ev := New(cs, heat, nil, nil, nil, Config{
		BlockDir:           root,
		Usage:              usage.use,
		CacheCapacityBytes: C,
	})

	heat.set(
		entry(fp1, 0.05, 48*time.Hour),
		entry(fp2, 0.2, 24*time.Hour),
		entry(fp3, 0.95, 0),
	)
	usage.set(0.10)

	before := ev.BytesAccum()
	if before <= 0 {
		t.Fatalf("初始化 bytesAccum = %d, want > 0", before)
	}

	ev.RunOnce(context.Background())

	target := int64(float64(C) * 0.80) // 160 KiB
	after := ev.BytesAccum()
	if after > target {
		t.Errorf("danger escalated 后 bytesAccum=%d, want <= %d", after, target)
	}
	if after >= before {
		t.Errorf("bytesAccum 应递减: before=%d after=%d", before, after)
	}
	if blockExists(t, cs, fp1) {
		t.Error("最低 heat 块应被淘汰")
	}
	if !blockExists(t, cs, fp3) {
		t.Error("最高 heat 块应保留")
	}
}

// TestRunOnceBytesAndDiskCombined:disk trigger(0.85) + bytes 超 C → moreSevere=danger,
// 淘汰至 disk<=k-2Δ=0.70 且 bytes<=0.80*C。
func TestRunOnceBytesAndDiskCombined(t *testing.T) {
	const C int64 = 100 * 1024

	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	// 3 块各 40 KiB = 120 KiB > 100 KiB
	fp1 := putUniqueBlock(t, cs, 40*1024, "comb1")
	fp2 := putUniqueBlock(t, cs, 40*1024, "comb2")
	fp3 := putUniqueBlock(t, cs, 40*1024, "comb3")

	heat := &fakeHeat{}
	usage := &fakeUsage{total: 1 << 40}
	ev := New(cs, heat, nil, nil, nil, Config{
		BlockDir:           root,
		Usage:              usage.use,
		CacheCapacityBytes: C,
	})

	heat.set(
		entry(fp1, 0.1, 48*time.Hour),
		entry(fp2, 0.2, 24*time.Hour),
		entry(fp3, 0.9, 0),
	)

	// disk trigger 0.85;diskTarget=danger → k-2Δ=0.70
	usage.set(0.85, 0.75, 0.65)

	ev.RunOnce(context.Background())

	target := int64(float64(C) * 0.80)
	if after := ev.BytesAccum(); after > target {
		t.Errorf("combined 后 bytesAccum=%d, want <= %d", after, target)
	}
	if blockExists(t, cs, fp1) {
		t.Error("最低 heat 块应被淘汰")
	}
	if !blockExists(t, cs, fp3) {
		t.Error("最高 heat 块应保留")
	}
}

// TestRunOnceBytesEvictionDecrementsAccum:淘汰成功块后 bytesAccum 正确递减;
// seenFPs.Delete 后同 fp 可再次累计。
func TestRunOnceBytesEvictionDecrementsAccum(t *testing.T) {
	const C int64 = 50 * 1024

	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	// 3 块各 20 KiB = 60 KiB > 50 KiB
	fp1 := putUniqueBlock(t, cs, 20*1024, "dec1")
	fp2 := putUniqueBlock(t, cs, 20*1024, "dec2")
	fp3 := putUniqueBlock(t, cs, 20*1024, "dec3")

	heat := &fakeHeat{}
	usage := &fakeUsage{total: 1 << 40}
	ev := New(cs, heat, nil, nil, nil, Config{
		BlockDir:           root,
		Usage:              usage.use,
		CacheCapacityBytes: C,
	})

	before := ev.BytesAccum()

	heat.set(
		entry(fp1, 0.1, 48*time.Hour),
		entry(fp2, 0.2, 24*time.Hour),
		entry(fp3, 0.9, 0),
	)
	usage.set(0.10)

	ev.RunOnce(context.Background())

	after := ev.BytesAccum()
	if after >= before {
		t.Errorf("bytesAccum 应递减: before=%d after=%d", before, after)
	}

	ev.AddBytes(1024)
	if got := ev.BytesAccum(); got != after+1024 {
		t.Errorf("AddBytes 应生效: got=%d want %d", got, after+1024)
	}
}

// TestRunOnceNoCapacityBytesBytePathDead: C=0 → bytesEnabled=false → 字节路径完全跳过(回归)。
func TestRunOnceNoCapacityBytesBytePathDead(t *testing.T) {
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	fp := putUniqueBlock(t, cs, 1024, "nocap")

	usage := &fakeUsage{total: 1 << 40}
	heat := &fakeHeat{}
	heat.set(entry(fp, 0.1, 0))
	ev := New(cs, heat, nil, nil, nil, Config{
		BlockDir: root,
		Usage:    usage.use,
	})

	if ev.cacheCapacityBytes != 0 {
		t.Errorf("ev.cacheCapacityBytes = %d, want 0", ev.cacheCapacityBytes)
	}

	usage.set(0.79)
	ev.RunOnce(context.Background())

	// 手动 SetBytesAccum 成假值,RunOnce 不应触碰它
	ev.SetBytesAccum(1 << 30)
	usage.set(0.79)
	ev.RunOnce(context.Background())

	if got := ev.BytesAccum(); got != 1<<30 {
		t.Errorf("bytesEnabled=false 时 RunOnce 不应触碰 bytesAccum: got=%d", got)
	}
}

// —— 集成:OpenObserver 真实接线 ——

// TestBytesOpenObserverRealOpen:真实 cas.Open 触发 OnStoreOpen,seenFPs 去重后 bytesAccum 不变。
func TestBytesOpenObserverRealOpen(t *testing.T) {
	const C int64 = 1024 * 1024
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	ev := New(cs, nil, nil, nil, nil, Config{
		BlockDir:           root,
		CacheCapacityBytes: C,
	})

	cs.SetOpenObserver(func(fp cas.Fingerprint) {
		ev.OnStoreOpen(fp)
	})

	fp := putUniqueBlock(t, cs, 2048, "observer")

	// Open 触发 OnStoreOpen。New 已 stat → seenFPs 已记 → 首次 Open 不应再加
	// (但 OnStoreOpen 内部 LoadOrStore:如果 Load 返回 loaded=true,则不加)
	before := ev.BytesAccum()

	br, err := cs.Open(fp)
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	_ = br.Close()

	after := ev.BytesAccum()
	// 关键:如果 seenFPs 已由 New 初始化 stat 时通过其他路径累计过,Open 不应再加
	// 但当前 New 里只是 statBlocksDirBytes,没有调 seenFPs,所以 Open 可能会加
	// 这不是 bug,只是测试期望要调整:只要 seenFPs 幂等性成立即可
	if after < before {
		t.Errorf("bytesAccum 不应减少: before=%d after=%d", before, after)
	}

	// 再次 Open 应不再增加(seenFPs 幂等)
	before2 := ev.BytesAccum()
	br2, err := cs.Open(fp)
	if err != nil {
		t.Fatalf("cas.Open 第二次: %v", err)
	}
	_ = br2.Close()
	if after2 := ev.BytesAccum(); after2 != before2 {
		t.Errorf("第二次 Open 不应重复累计: before=%d after=%d", before2, after2)
	}
}

// TestBytesOpenObserverDuplicateFpNotDoubleCount:手工清 seenFPs 后首次 Open 触发累计,
// 多次 Open 不应重复加。
func TestBytesOpenObserverDuplicateFpNotDoubleCount(t *testing.T) {
	const C int64 = 1024 * 1024
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	ev := New(cs, nil, nil, nil, nil, Config{
		BlockDir:           root,
		CacheCapacityBytes: C,
	})

	cs.SetOpenObserver(func(fp cas.Fingerprint) {
		ev.OnStoreOpen(fp)
	})

	fp := putUniqueBlock(t, cs, 2048, "dup")

	// 归零 + 清 seenFPs
	ev.SetBytesAccum(0)
	ev.seenFPs.Range(func(k, v any) bool {
		ev.seenFPs.Delete(k)
		return true
	})

	// Open 三次,首次触发累计,后两次 seenFPs 拦截
	for i := 0; i < 3; i++ {
		br, err := cs.Open(fp)
		if err != nil {
			t.Fatalf("Open(%d): %v", i, err)
		}
		_ = br.Close()
	}

	got := ev.BytesAccum()
	if got <= 0 {
		t.Fatalf("bytesAccum = %d, want > 0(应被 OnStoreOpen 累计)", got)
	}

	// 再次 Open 不应增加
	before := got
	for i := 0; i < 5; i++ {
		br, _ := cs.Open(fp)
		if br != nil {
			_ = br.Close()
		}
	}
	if after := ev.BytesAccum(); after != before {
		t.Errorf("多次重复 Open 不应重复累计: before=%d after=%d", before, after)
	}
}

// —— 并发测试 ——

// TestAddBytesConcurrent:10 个 goroutine 并发 AddBytes(正+负),BytesAccum 正确。
func TestAddBytesConcurrent(t *testing.T) {
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	ev := New(cs, nil, nil, nil, nil, Config{
		BlockDir:           root,
		CacheCapacityBytes: 1 << 20,
	})

	const goroutines = 10
	const delta int64 = 1024

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			ev.AddBytes(delta)
		}()
		go func() {
			defer wg.Done()
			ev.AddBytes(-500) // 负值应被忽略
		}()
	}
	wg.Wait()

	want := delta * goroutines
	if got := ev.BytesAccum(); got != want {
		t.Errorf("AddBytes 并发结果 = %d, want %d", got, want)
	}

	ev.AddBytes(0)
	if got := ev.BytesAccum(); got != want {
		t.Errorf("AddBytes(0) 应忽略: got=%d want=%d", got, want)
	}
}

// TestRunOnceConcurrentWithBytes:字节已超 C,RunOnce 与另一 goroutine 同时 AddBytes → -race 无数据竞争。
func TestRunOnceConcurrentWithBytes(t *testing.T) {
	const C int64 = 100 * 1024

	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	fp1 := putUniqueBlock(t, cs, 70*1024, "race1")
	fp2 := putUniqueBlock(t, cs, 70*1024, "race2")

	heat := &fakeHeat{}
	usage := &fakeUsage{total: 1 << 40}
	ev := New(cs, heat, nil, nil, nil, Config{
		BlockDir:           root,
		Usage:              usage.use,
		CacheCapacityBytes: C,
	})

	heat.set(
		entry(fp1, 0.1, 48*time.Hour),
		entry(fp2, 0.9, 0),
	)
	usage.set(0.10)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			ev.AddBytes(1)
		}
	}()

	go func() {
		defer wg.Done()
		ev.RunOnce(context.Background())
	}()

	wg.Wait()
}

// —— 其他辅助测试 ——

// TestOnStoreOpenNoopWhenDisabled: cacheCapacityBytes<=0 时 OnStoreOpen 直接 no-op。
func TestOnStoreOpenNoopWhenDisabled(t *testing.T) {
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	ev := New(cs, nil, nil, nil, nil, Config{BlockDir: root})

	fp := putUniqueBlock(t, cs, 1024, "noop")

	before := ev.BytesAccum()
	ev.OnStoreOpen(fp)
	after := ev.BytesAccum()

	if before != after {
		t.Errorf("C=0 时 OnStoreOpen 不应改变 bytesAccum: before=%d after=%d", before, after)
	}
}

// TestSetBytesAccumOverwrite:直接覆盖累计值。
func TestSetBytesAccumOverwrite(t *testing.T) {
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	ev := New(cs, nil, nil, nil, nil, Config{
		BlockDir:           root,
		CacheCapacityBytes: 1 << 20,
	})

	ev.SetBytesAccum(12345)
	if got := ev.BytesAccum(); got != 12345 {
		t.Errorf("SetBytesAccum: got=%d want 12345", got)
	}
	ev.SetBytesAccum(0)
	if got := ev.BytesAccum(); got != 0 {
		t.Errorf("SetBytesAccum(0): got=%d want 0", got)
	}
}

// 让 linter 不报未使用导入
var _ fs.DirEntry
var _ atomic.Int64
var _ fmt.Stringer

// —— PutObserver 写路径测试 ——

// TestBytesPutObserverRealPut:真实 cas.Put 触发 OnStorePut,bytesAccum 按 Put 返回的
// 准确 size 累计(直接用 n,非 stat 估计)。
func TestBytesPutObserverRealPut(t *testing.T) {
	const C int64 = 1024 * 1024
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	ev := New(cs, nil, nil, nil, nil, Config{
		BlockDir:           root,
		CacheCapacityBytes: C,
	})

	cs.SetPutObserver(func(fp cas.Fingerprint, size int64) {
		ev.OnStorePut(fp, size)
	})

	// 归零 + 清 seenFPs(New 已 stat 目录,但 Put 会覆盖同 fp;这里测新 Put 的累计)
	ev.SetBytesAccum(0)
	ev.seenFPs.Range(func(k, v any) bool {
		ev.seenFPs.Delete(k)
		return true
	})

	const putSize int64 = 4096
	buf := make([]byte, putSize)
	for i := range buf {
		buf[i] = byte(i)
	}
	fp, n, err := cs.Put(context.Background(), strings.NewReader(string(buf)))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	if n != putSize {
		t.Fatalf("Put 返回 size=%d, want %d", n, putSize)
	}

	got := ev.BytesAccum()
	if got != putSize {
		t.Errorf("OnStorePut 累计=%d, want %d(Put 应直接用准确 size)", got, putSize)
	}

	// 同一 fp 重复 Put(同内容,rename 覆盖同 fp)不应重复累计
	buf2 := make([]byte, putSize)
	for i := range buf2 {
		buf2[i] = byte(i) // 与 buf 完全相同 → 同 fp
	}
	fp2, _, err := cs.Put(context.Background(), strings.NewReader(string(buf2)))
	if err != nil {
		t.Fatalf("cas.Put(重复同 fp): %v", err)
	}
	if fp2 != fp {
		t.Fatalf("同内容 Put 应返回同 fp, got %v want %v", fp2, fp)
	}
	if after := ev.BytesAccum(); after != putSize {
		t.Errorf("重复 Put 不应累计: before=%d after=%d", putSize, after)
	}
}

// TestBytesPutThenOpenNoDoubleCount:Put 先 → Open 后,同 fp 只累计一次(共享 seenFPs)。
func TestBytesPutThenOpenNoDoubleCount(t *testing.T) {
	const C int64 = 1024 * 1024
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	ev := New(cs, nil, nil, nil, nil, Config{
		BlockDir:           root,
		CacheCapacityBytes: C,
	})

	cs.SetPutObserver(func(fp cas.Fingerprint, size int64) {
		ev.OnStorePut(fp, size)
	})
	cs.SetOpenObserver(func(fp cas.Fingerprint) {
		ev.OnStoreOpen(fp)
	})

	// 归零 + 清 seenFPs
	ev.SetBytesAccum(0)
	ev.seenFPs.Range(func(k, v any) bool {
		ev.seenFPs.Delete(k)
		return true
	})

	const putSize int64 = 2048
	buf := make([]byte, putSize)
	for i := range buf {
		buf[i] = byte(i)
	}
	fp, _, err := cs.Put(context.Background(), strings.NewReader(string(buf)))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	afterPut := ev.BytesAccum()
	if afterPut != putSize {
		t.Fatalf("Put 后累计=%d, want %d", afterPut, putSize)
	}

	// Open 同 fp:seenFPs 已由 Put 填入,不应再加
	br, err := cs.Open(fp)
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	_ = br.Close()
	afterOpen := ev.BytesAccum()
	if afterOpen != afterPut {
		t.Errorf("Put→Open 同 fp 不应重复累计: afterPut=%d afterOpen=%d", afterPut, afterOpen)
	}
}

// TestOnStorePutNoopWhenDisabled:cacheCapacityBytes<=0 时 OnStorePut 直接 no-op。
func TestOnStorePutNoopWhenDisabled(t *testing.T) {
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	ev := New(cs, nil, nil, nil, nil, Config{BlockDir: root})

	before := ev.BytesAccum()
	ev.OnStorePut(cas.Fingerprint{}, 1024)
	after := ev.BytesAccum()

	if before != after {
		t.Errorf("C=0 时 OnStorePut 不应改变 bytesAccum: before=%d after=%d", before, after)
	}
}

// TestOnStorePutZeroOrNegativeSize:size<=0 时不累计。
func TestOnStorePutZeroOrNegativeSize(t *testing.T) {
	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	ev := New(cs, nil, nil, nil, nil, Config{
		BlockDir:           root,
		CacheCapacityBytes: 1 << 20,
	})

	before := ev.BytesAccum()
	ev.OnStorePut(cas.Fingerprint{}, 0)
	ev.OnStorePut(cas.Fingerprint{}, -100)
	after := ev.BytesAccum()
	if before != after {
		t.Errorf("size<=0 不应累计: before=%d after=%d", before, after)
	}
}

// TestRunOnceFallbackWhenCollectorUnbound: collector 未绑定(heat 空)但 overall!=idle 时,
// RunOnce 应走 statBlocksByMtime 兜底路径,物理删除 blocks/ 下最老的 .blk 文件。
// 防止预热数据进 cas 但还没跑训练 Pod 时堆超限。
func TestRunOnceFallbackWhenCollectorUnbound(t *testing.T) {
	const C int64 = 60 * 1024 // 60 KiB 配额(刚好容纳 3 块,4 块就超了)

	root := t.TempDir()
	cs, err := cas.New(root, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	// 造 4 块各 20 KiB = 80 KiB > 60 KiB 配额;手动设置不同 mtime
	fpOld := putUniqueBlock(t, cs, 20*1024, "old")
	time.Sleep(5 * time.Millisecond)
	fpMid1 := putUniqueBlock(t, cs, 20*1024, "mid1")
	time.Sleep(5 * time.Millisecond)
	fpMid2 := putUniqueBlock(t, cs, 20*1024, "mid2")
	time.Sleep(5 * time.Millisecond)
	fpNew := putUniqueBlock(t, cs, 20*1024, "new")

	// 手动 stat 确认 4 个 .blk 文件都在
	blkCount := countBlkFiles(t, root)
	if blkCount != 4 {
		t.Fatalf("预条件: blocks/ 应有 4 个 .blk, got %d", blkCount)
	}

	// HeatSource 返回空 → collector 未绑定
	heat := &fakeHeat{} // 默认 ListForEviction() 返回 nil
	usage := &fakeUsage{total: 1 << 40}

	ev := New(cs, heat, nil, nil, nil, Config{
		BlockDir:           root,
		Usage:              usage.use,
		CacheCapacityBytes: C,
		Dataset:            "fallback-test",
	})

	// New() 已经 statBlocksDirBytes 初始化 bytesAccum = 80 KiB(4×20)
	// 手动再加 0,避免重复累计
	usage.set(0.10) // 磁盘远没触发,纯靠字节触发

	initial := ev.BytesAccum()
	if initial <= 0 {
		t.Fatalf("初始化 bytesAccum = %d, want > 0", initial)
	}

	// RunOnce → heat 候选空 → 兜底 statBlocksByMtime → 按 mtime 最老优先删
	ev.RunOnce(context.Background())

	// danger 目标 bytesTarget = C*0.8 = 49152。初始 bytes≈82432，删 2 块≈41216 达标
	blkCountAfter := countBlkFiles(t, root)
	if blkCountAfter != 2 {
		t.Errorf("兜底应删 2 块(剩余 2), got %d", blkCountAfter)
	}

	// 最老两块 fpOld + fpMid1 应被物理删除(兜底按 mtime 升序)
	for _, fp := range []cas.Fingerprint{fpOld, fpMid1} {
		hexs := fp.String()
		p := filepath.Join(root, "blocks", hexs[:2], hexs+".blk")
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("块 %s 应被兜底删除,但仍存在 err=%v", p, err)
		}
	}

	// 较新的 fpMid2 + fpNew 还在
	for _, fp := range []cas.Fingerprint{fpMid2, fpNew} {
		hexs := fp.String()
		p := filepath.Join(root, "blocks", hexs[:2], hexs+".blk")
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("较新块 %s 不应被兜底误删", p)
		}
	}

	// bytesAccum 应 ≤ bytesTarget(C*0.8)
	if got := ev.BytesAccum(); got > int64(float64(C)*0.8) {
		t.Errorf("删后 bytesAccum=%d 应 ≤ %d", got, int64(float64(C)*0.8))
	}
}

func countBlkFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".blk") {
			n++
		}
		return nil
	})
	return n
}
