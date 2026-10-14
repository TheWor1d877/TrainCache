package index

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"traincache/core/cas"
	"traincache/core/fetch"
)

// TestEndToEndLifecycle 端到端全链路（mock objectstore + 临时目录 cas + fetch.Fetcher）：
//
//	写块（fetch.Fetch 经 mock 下载）→ 测试代码扮演调用方 cas.Acquire(fp)+1（§7.1 步骤 1）
//	→ Writer.Commit → Catalog.Refresh → Lookup 命中 → cas.Open 读回与源数据逐字节一致
//	→ 数据集合更新（新条目可见、旧条目保留）→ Sweep 回收窗口外块/对象（复用分片保留）
//	→ 后台刷新运行中（go c.Start）Pin 持有的孤儿块 Sweep 不删，Unpin 后才删。
func TestEndToEndLifecycle(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	// 每片 2 条：条目按 (path, chunk) 排序后 [eval,meta] 为一片、[train] 为一片；
	// 保留窗口 = CURRENT + 1，保证第 3 次 Commit 后第 1 次快照出窗口。
	cfg := Config{MaxEntriesPerShard: 2, RetainSnapshots: 2, RefreshInterval: 20 * time.Millisecond}
	const dataset = "e2e"

	fetcher := fetch.New(env.store, env.cs, fetch.Options{})

	// download 把源数据放进 mock 对象存储，经 fetch.Fetch 下载落盘为 CAS 块，
	// 并扮演调用方 Acquire（引用计数 = 已提交快照对该块的逻辑引用）。
	download := func(t *testing.T, key string, content []byte) cas.Fingerprint {
		t.Helper()
		if _, err := env.store.Put(ctx, key, bytes.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("upload source %s: %v", key, err)
		}
		fp, err := fetcher.Fetch(ctx, key, int64(len(content)))
		if err != nil {
			t.Fatalf("Fetch(%s): %v", key, err)
		}
		if err := env.cs.Acquire(fp); err != nil {
			t.Fatalf("Acquire(%s): %v", fp, err)
		}
		return fp
	}

	entryOf := func(path string, fp cas.Fingerprint, size int) Entry {
		return Entry{Path: path, Chunk: 0, Size: int64(size), Node: "node-a", Fingerprint: fp}
	}

	// readBack 经 Catalog 查询 → cas.Open 读回，与源数据逐字节比对（含 Size 字段）。
	readBack := func(t *testing.T, c *Catalog, path string, want []byte) Entry {
		t.Helper()
		e, ok, err := c.Lookup(ctx, path, 0)
		if err != nil || !ok {
			t.Fatalf("Lookup(%q) = (_, %v, %v)，期望命中", path, ok, err)
		}
		r, err := env.cs.Open(e.Fingerprint)
		if err != nil {
			t.Fatalf("cas.Open(%s): %v", e.Fingerprint, err)
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("读块 %s: %v", e.Fingerprint, err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("关闭块 %s: %v", e.Fingerprint, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%q 读回 %d 字节与源数据（%d 字节）不一致", path, len(got), len(want))
		}
		if e.Size != int64(len(want)) {
			t.Fatalf("%q 条目 Size = %d，期望 %d", path, e.Size, len(want))
		}
		return e
	}

	// ---- 第 1 次 Commit：3 条数据（2 个分片），全链路读回逐字节一致 ----
	train1 := bytes.Repeat([]byte("train-v1;"), 16)
	eval1 := bytes.Repeat([]byte("eval-v1;"), 12)
	meta1 := []byte("meta-v1-payload")
	trainFP1 := download(t, "src/train-v1", train1)
	evalFP := download(t, "src/eval-v1", eval1)
	metaFP := download(t, "src/meta-v1", meta1)

	w := NewWriter(env.store, env.cs, cfg)
	_, m1 := commitSnapshot(t, w, env.store, dataset, []Entry{
		entryOf("/data/eval.bin", evalFP, len(eval1)),
		entryOf("/data/meta.json", metaFP, len(meta1)),
		entryOf("/data/train.bin", trainFP1, len(train1)),
	})
	if len(m1.Shards) != 2 {
		t.Fatalf("s1 分片数 = %d，期望 2", len(m1.Shards))
	}

	catalog := NewCatalog(env.store, dataset, cfg)
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	readBack(t, catalog, "/data/train.bin", train1)
	readBack(t, catalog, "/data/eval.bin", eval1)
	readBack(t, catalog, "/data/meta.json", meta1)

	// ---- 第 2 次 Commit：train 更新为 v2；刷新后新条目可见、旧条目保留 ----
	train2 := bytes.Repeat([]byte("train-v2;"), 24)
	trainFP2 := download(t, "src/train-v2", train2)
	commitSnapshot(t, w, env.store, dataset, []Entry{
		entryOf("/data/train.bin", trainFP2, len(train2)),
	})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after commit 2: %v", err)
	}
	gotTrain := readBack(t, catalog, "/data/train.bin", train2)
	if gotTrain.Fingerprint != trainFP2 {
		t.Fatalf("train 指纹 = %s，期望 %s", gotTrain.Fingerprint, trainFP2)
	}
	// 旧条目（eval/meta）未变更，仍在映射中且可读。
	gotEval := readBack(t, catalog, "/data/eval.bin", eval1)
	gotMeta := readBack(t, catalog, "/data/meta.json", meta1)
	if gotEval.Fingerprint != evalFP || gotMeta.Fingerprint != metaFP {
		t.Fatalf("旧条目指纹漂移: eval=%s meta=%s", gotEval.Fingerprint, gotMeta.Fingerprint)
	}
	// s1 仍在窗口内（RetainSnapshots=2）：其引用链（含 train v1 块）仍完整。
	if !blockExists(t, env.cs, trainFP1) {
		t.Fatal("窗口内旧快照引用的 train v1 块被删除")
	}

	// ---- 第 3 次 Commit：train 更新为 v3，s1 出窗口 ----
	train3 := bytes.Repeat([]byte("train-v3;"), 32)
	trainFP3 := download(t, "src/train-v3", train3)
	commitSnapshot(t, w, env.store, dataset, []Entry{
		entryOf("/data/train.bin", trainFP3, len(train3)),
	})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after commit 3: %v", err)
	}
	readBack(t, catalog, "/data/train.bin", train3)

	col := NewCollector(env.store, env.cs, cfg)

	// 调用方记账：train v1 所属快照已出保留窗口，Release 其逻辑引用。
	env.cs.Release(trainFP1)
	removedBlocks, removedObjects, err := col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep 1: %v", err)
	}
	// s1：manifest + 其独占的 [train-v1] 分片被删（[eval,meta] 分片被保留快照复用）；train v1 块被删。
	if removedBlocks != 1 || removedObjects != 2 {
		t.Fatalf("Sweep 1 删除 (%d, %d)，期望 (1, 2)", removedBlocks, removedObjects)
	}
	if blockExists(t, env.cs, trainFP1) {
		t.Fatal("窗口外 train v1 块未被删除")
	}
	// 活块：eval/meta（跨所有快照）、train v2（窗口内的 s2）、train v3（CURRENT）。
	for _, fp := range []cas.Fingerprint{evalFP, metaFP, trainFP2, trainFP3} {
		if !blockExists(t, env.cs, fp) {
			t.Fatalf("活块 %s 被删除", fp)
		}
	}
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象被删除")
	}

	// ---- 第 4 次 Commit：train 更新为 v4，s2 出窗口；后台刷新运行中做并发读与回收 ----
	train4 := bytes.Repeat([]byte("train-v4;"), 40)
	trainFP4 := download(t, "src/train-v4", train4)
	s4, _ := commitSnapshot(t, w, env.store, dataset, []Entry{
		entryOf("/data/train.bin", trainFP4, len(train4)),
	})
	if err := catalog.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after commit 4: %v", err)
	}
	readBack(t, catalog, "/data/train.bin", train4)

	// 后台刷新循环（Start 阻塞语义：调用方 go 启动，ctx 取消即退出）。
	bgCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		catalog.Start(bgCtx)
	}()

	// 模拟在途读者：train v2 已出窗口且引用已释放，Pin 令其生命周期延长。
	env.cs.Release(trainFP2)
	if err := env.cs.Pin(trainFP2); err != nil {
		t.Fatalf("Pin(%s): %v", trainFP2, err)
	}
	removedBlocks, removedObjects, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep 2: %v", err)
	}
	// s2 的 manifest 与独占分片被删；train v2 块因 pin>0 跳过。
	if removedBlocks != 0 || removedObjects != 2 {
		t.Fatalf("Sweep 2 删除 (%d, %d)，期望 (0, 2)", removedBlocks, removedObjects)
	}
	if !blockExists(t, env.cs, trainFP2) {
		t.Fatal("pin 持有的块被 Sweep 删除")
	}
	// 持 pin 期间仍可完整读回（并发读），且后台刷新未受影响。
	readBackPinned := func() {
		t.Helper()
		gotTrain2 := readBackFromFP(t, env, trainFP2)
		if !bytes.Equal(gotTrain2, train2) {
			t.Fatalf("pin 期间读回 train v2 不一致：%d 字节", len(gotTrain2))
		}
	}
	readBackPinned()
	// CURRENT 与窗口内对象不受后台刷新/Sweep 并发影响。
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("CURRENT 对象被删除")
	}
	gotSnap, _ := currentSnapshot(t, env.store, dataset)
	if gotSnap != s4 {
		t.Fatalf("CURRENT snapID = %q，期望 %q", gotSnap, s4)
	}

	// Unpin 后再 Sweep：train v2 孤儿块本次被删，其余活块保留。
	env.cs.Unpin(trainFP2)
	removedBlocks, removedObjects, err = col.Sweep(ctx, dataset)
	if err != nil {
		t.Fatalf("Sweep 3: %v", err)
	}
	if removedBlocks != 1 || removedObjects != 0 {
		t.Fatalf("Sweep 3 删除 (%d, %d)，期望 (1, 0)", removedBlocks, removedObjects)
	}
	if blockExists(t, env.cs, trainFP2) {
		t.Fatal("Unpin 后孤儿块未被删除")
	}
	for _, fp := range []cas.Fingerprint{evalFP, metaFP, trainFP3, trainFP4} {
		if !blockExists(t, env.cs, fp) {
			t.Fatalf("活块 %s 被删除", fp)
		}
	}

	// 停止后台刷新并等待退出。
	cancel()
	wg.Wait()

	// 最终一致性：数据全部可读回，Sweep 幂等且 CURRENT 仍在。
	readBack(t, catalog, "/data/train.bin", train4)
	readBack(t, catalog, "/data/eval.bin", eval1)
	readBack(t, catalog, "/data/meta.json", meta1)
	removedBlocks, removedObjects, err = col.Sweep(ctx, dataset)
	if err != nil || removedBlocks != 0 || removedObjects != 0 {
		t.Fatalf("收尾 Sweep 删除 (%d, %d) err=%v，期望 (0, 0)", removedBlocks, removedObjects, err)
	}
	if !objectExists(t, env.store, CurrentKey(dataset)) {
		t.Fatal("收尾后 CURRENT 对象缺失")
	}
}

// readBackFromFP 直接经指纹打开块读回全部数据（pin 场景的并发读）。
func readBackFromFP(t *testing.T, env *testEnv, fp cas.Fingerprint) []byte {
	t.Helper()
	r, err := env.cs.Open(fp)
	if err != nil {
		t.Fatalf("cas.Open(%s): %v", fp, err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("读块 %s: %v", fp, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("关闭块 %s: %v", fp, err)
	}
	return data
}
