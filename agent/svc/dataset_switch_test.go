package svc

import (
	"context"
	"testing"
	"time"

	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// datasetSwitchRef 是两份同形状数据共用的块引用:path 与 chunk 都相同,
// 只有内容指纹不同。归属解析只能靠活跃数据集集合区分,不能靠引用本身。
func datasetSwitchRef() ordersource.ChunkRef {
	return ordersource.ChunkRef{Path: "shard-000000.tar", Chunk: 0}
}

func installDatasetOrder(t *testing.T, pm *PrefetchManager, pod, dataset string, ref ordersource.ChunkRef) {
	t.Helper()
	o := ordersource.ConsumeOrder{
		Dataset: dataset,
		StageID: "0",
		Chunks:  []ordersource.ChunkRef{ref},
	}
	if err := pm.ForOrder(context.Background(), pod, o); err != nil {
		t.Fatalf("ForOrder(%s): %v", dataset, err)
	}
}

// TestDatasetResolutionFollowsActiveSet 覆盖切换后旧数据集退出解析、空集退化、
// 同引用多候选取最近安装、订单回来恢复且复用原 bundle。
func TestDatasetResolutionFollowsActiveSet(t *testing.T) {
	f := newIntegrationFixture(t)
	ref := datasetSwitchRef()
	ctx := context.Background()
	f.commit(t, "train-e2e", []svcBlockSpec{
		{path: ref.Path, chunk: ref.Chunk, content: "old-set", local: true}}, 0)
	f.commit(t, "train-e2e-alt", []svcBlockSpec{
		{path: ref.Path, chunk: ref.Chunk, content: "new-set", local: true}}, 0)

	pm := f.fx.pm
	oldBundle := pm.bundleFor("train-e2e", nil)
	newBundle := pm.bundleFor("train-e2e-alt", nil)
	if err := oldBundle.catalog.Refresh(ctx); err != nil {
		t.Fatalf("旧数据集索引刷新: %v", err)
	}
	if err := newBundle.catalog.Refresh(ctx); err != nil {
		t.Fatalf("新数据集索引刷新: %v", err)
	}
	oldFP, okOld, err := oldBundle.catalog.Lookup(ctx, ref.Path, ref.Chunk)
	if err != nil || !okOld {
		t.Fatalf("旧数据集索引未命中: ok=%v err=%v", okOld, err)
	}
	newFP, okNew, err := newBundle.catalog.Lookup(ctx, ref.Path, ref.Chunk)
	if err != nil || !okNew {
		t.Fatalf("新数据集索引未命中: ok=%v err=%v", okNew, err)
	}
	if oldFP.Fingerprint == newFP.Fingerprint {
		t.Fatal("fixture 的两份数据指纹必须不同")
	}

	installDatasetOrder(t, pm, "pod-old", "train-e2e", ref)
	time.Sleep(2 * time.Millisecond)
	installDatasetOrder(t, pm, "pod-new", "train-e2e-alt", ref)

	// 活跃集合只剩 alt:旧数据集退出归属解析。
	pm.SetActiveDatasets([]string{"train-e2e-alt"})
	if got := pm.resolutionNames(); len(got) != 1 || got[0] != "train-e2e-alt" {
		t.Fatalf("resolutionNames = %v, want [train-e2e-alt]", got)
	}
	if ds, _, _, ok := pm.orderContext(ref, cas.Fingerprint{}); ds != "train-e2e-alt" || !ok {
		t.Fatalf("切换后 orderContext = %q ok=%v, want train-e2e-alt", ds, ok)
	}

	// 活跃集合为空:退化为全量,行为回到修复前。
	pm.SetActiveDatasets(nil)
	if got := pm.resolutionNames(); len(got) != 2 {
		t.Fatalf("空集 resolutionNames = %v, want 两个 dataset", got)
	}

	// 两个都活跃且同引用:取最近安装订单者,不按字典序。
	pm.SetActiveDatasets([]string{"train-e2e", "train-e2e-alt"})
	if ds, _, _, _ := pm.orderContext(ref, cas.Fingerprint{}); ds != "train-e2e-alt" {
		t.Fatalf("两活跃候选 orderContext = %q, want train-e2e-alt(最近安装)", ds)
	}

	// 旧数据集订单回来:立即恢复解析,且复用原 bundle;下一拍活跃快照再收敛集合。
	pm.SetActiveDatasets([]string{"train-e2e-alt"})
	installDatasetOrder(t, pm, "pod-old-2", "train-e2e", ref)
	if got := pm.resolutionNames(); len(got) == 0 || got[0] != "train-e2e" {
		t.Fatalf("恢复后 resolutionNames = %v, want train-e2e 在首位", got)
	}
	if pm.bundleFor("train-e2e", nil) != oldBundle {
		t.Fatal("恢复后必须复用原 bundle")
	}
	if ds, _, _, _ := pm.orderContext(ref, cas.Fingerprint{}); ds != "train-e2e" {
		t.Fatalf("恢复后 orderContext = %q, want train-e2e", ds)
	}
}
