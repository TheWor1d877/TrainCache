package rescan

// RepairOne 单元/并发测试(D35;验收场景 ① 的 RepairOne 独立闭环与 ② 同块并发仅一次下载)。

import (
	"context"
	"errors"
	"sync"
	"testing"

	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// TestRepairOneClosesLoop:手动"损坏 → 隔离 → RepairOne"闭环(不依赖 Start):按 D25 键
// 恰一次下载、指纹不变、RemoveQuarantined 使 ListQuarantined 归零、隔离 gauge 递减到 0;
// 未 Start 时 Status 保持初值(SAMPLING/零计数),修复独立工作。
func TestRepairOneClosesLoop(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "r.tar", chunk: 5, content: "repair-one", source: true},
	})
	id := blockID{dataset: testDataset, path: "r.tar", chunk: 5}
	fp := env.fpOf[id]
	env.corruptBlock(id)
	if err := env.cs.Quarantine(fp); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}

	m := &fakeMetrics{}
	r := New(env.cs, env.fetch, env.store, env.catalogFor, m)
	got, err := r.RepairOne(context.Background(), testDataset, ordersource.ChunkRef{Path: "r.tar", Chunk: 5})
	if err != nil {
		t.Fatalf("RepairOne: %v", err)
	}
	if got != fp {
		t.Fatalf("RepairOne 指纹 = %s, want %s(内容同源,指纹不变)", got, fp)
	}
	assertRepairClosedLoop(t, env, id, "repair-one")
	if n := m.lastQuarantined(); n != 0 {
		t.Fatalf("隔离 gauge 终值 = %d, want 0", n)
	}

	st := r.Status()
	if st.Phase != PhaseSampling || st.Sampled != 0 || st.Verified != 0 || st.Quarantined != 0 || st.RepairQueue != 0 {
		t.Fatalf("未 Start 时 Status = %+v, want Phase=SAMPLING 计数全零", st)
	}
}

// TestRepairOneCanceledCtx:ctx 已取消 → 立即返回 context.Canceled,不产生任何下载。
func TestRepairOneCanceledCtx(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "c.tar", content: "cancel-repair", source: true},
	})
	id := blockID{dataset: testDataset, path: "c.tar"}
	env.corruptBlock(id)
	if err := env.cs.Quarantine(env.fpOf[id]); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	r := New(env.cs, env.fetch, env.store, env.catalogFor, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.RepairOne(ctx, testDataset, ordersource.ChunkRef{Path: "c.tar"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RepairOne(canceled) err = %v, want context.Canceled", err)
	}
	if gets := blockGets(env.store); len(gets) != 0 {
		t.Fatalf("取消的修复不应下载: %v", gets)
	}
}

// TestRepairOneSourceMissing:源站无该块 → 修复失败上报,块保持隔离(留待读时路径按需重修)。
func TestRepairOneSourceMissing(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "m.tar", content: "missing-source", source: false},
	})
	id := blockID{dataset: testDataset, path: "m.tar"}
	fp := env.fpOf[id]
	env.corruptBlock(id)
	if err := env.cs.Quarantine(fp); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	r := New(env.cs, env.fetch, env.store, env.catalogFor, nil)
	if _, err := r.RepairOne(context.Background(), testDataset, ordersource.ChunkRef{Path: "m.tar"}); err == nil {
		t.Fatal("源站缺块时 RepairOne 应报错")
	}
	q, err := env.cs.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined: %v", err)
	}
	if len(q) != 1 || q[0] != fp {
		t.Fatalf("修复失败后 quarantine = %v, want [%s](保持隔离)", q, fp)
	}
	// 请求键仍为 D25 布局。
	want := blockKey(testDataset, ordersource.ChunkRef{Path: "m.tar"})
	for _, k := range blockGets(env.store) {
		if k != want {
			t.Fatalf("下载键 = %q, want %q", k, want)
		}
	}
	if len(blockGets(env.store)) == 0 {
		t.Fatal("修复失败路径也应发生过下载请求")
	}
}

// TestRepairOneConcurrentSameRefSingleDownload(验收 ②):同一 (dataset, ref) 被 4 个
// goroutine 并发 RepairOne——在本层单飞合并(1 owner + 3 waiters),实际下载恰一次
// (门控 Mock 的 Get 阻塞至 3 个等待者登记后放行),全部调用得到同一 (fp, nil)。
func TestRepairOneConcurrentSameRefSingleDownload(t *testing.T) {
	const callers = 4
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "f.tar", chunk: 7, content: "flight-content", source: true},
	})
	id := blockID{dataset: testDataset, path: "f.tar", chunk: 7}
	fp := env.fpOf[id]
	ref := ordersource.ChunkRef{Path: "f.tar", Chunk: 7}
	key := blockKey(testDataset, ref)
	env.corruptBlock(id)
	if err := env.cs.Quarantine(fp); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	gate := env.store.gate(key) // Get(该 D25 键)阻塞至 close:强制并发窗口

	r := New(env.cs, env.fetch, env.store, env.catalogFor, nil)

	var wg sync.WaitGroup
	fps := make([]cas.Fingerprint, callers)
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, err := r.RepairOne(context.Background(), testDataset, ref)
			fps[i] = f
			errs[i] = err
		}(i)
	}

	// 1 owner 下载中 + 3 等待者登记(白盒:r.flights[key].waiters 受 r.mu 保护)。
	waitFor(t, "3 个并发调用合并为等待者", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		fl := r.flights[key]
		return fl != nil && fl.waiters == callers-1
	})
	close(gate)
	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("调用 %d: err = %v", i, errs[i])
		}
		if fps[i] != fp {
			t.Fatalf("调用 %d: fp = %s, want %s", i, fps[i], fp)
		}
	}
	if gets := blockGets(env.store); len(gets) != 1 || gets[0] != key {
		t.Fatalf("并发 RepairOne 的下载请求 = %v, want 恰 [%s](仅一次)", gets, key)
	}
	q, err := env.cs.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined: %v", err)
	}
	if len(q) != 0 {
		t.Fatalf("修复后 quarantine 未归零: %v", q)
	}
	got, err := env.readBlock(fp)
	if err != nil || got != "flight-content" {
		t.Fatalf("修复后内容 = (%q, %v), want (flight-content, nil)", got, err)
	}
}
