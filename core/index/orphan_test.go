package index

import (
	"context"
	"strings"
	"testing"
	"time"

	"traincache/core/cas"
	"traincache/core/objectstore"
)

// orphanEnv 造一个只有一次提交的数据集，并允许直接写远端块对象。
func orphanEnv(t *testing.T, dataset string) (objectstore.Store, *objectstore.Mock, *cas.Store, string) {
	t.Helper()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	mock := objectstore.NewMock()
	store := objectstore.Store(mock)
	fp, _, err := cs.Put(context.Background(), strings.NewReader("live-block"))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	if _, err := NewWriter(store, cs, Config{}).Commit(context.Background(), dataset, []Entry{mkEntry("live.tar", fp)}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return store, mock, cs, objectstore.BlockKey(dataset, "live.tar", 0)
}

// backdate 把对象的最后修改时间回拨，模拟超过宽限期的孤儿。
func backdate(t *testing.T, mock *objectstore.Mock, store objectstore.Store, key string, age time.Duration) {
	t.Helper()
	if _, err := store.Put(context.Background(), key, strings.NewReader("x"), 1); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
	mock.SetModTime(key, time.Now().Add(-age))
}

func TestFindOrphanBlocksSelectsOnlyOldUnreferenced(t *testing.T) {
	const dataset = "orphans"
	store, mock, _, liveKey := orphanEnv(t, dataset)

	oldOrphan := objectstore.BlockKey(dataset, "gone.tar", 0)
	freshOrphan := objectstore.BlockKey(dataset, "wip.tar", 0)
	backdate(t, mock, store, oldOrphan, 48*time.Hour)
	backdate(t, mock, store, freshOrphan, time.Minute)
	backdate(t, mock, store, liveKey, 72*time.Hour) // 活块再老也不删

	orphans, err := FindOrphanBlocks(context.Background(), store, dataset, 24*time.Hour)
	if err != nil {
		t.Fatalf("FindOrphanBlocks: %v", err)
	}
	if len(orphans) != 1 || orphans[0].Key != oldOrphan {
		t.Fatalf("orphans = %+v, want 只有 %s", orphans, oldOrphan)
	}
	if orphans[0].Size != 1 {
		t.Fatalf("Size = %d, want 1", orphans[0].Size)
	}
}

func TestFindOrphanBlocksKeepsBlocksOfOldSnapshots(t *testing.T) {
	const dataset = "orphan-old-snap"
	store, mock, cs, _ := orphanEnv(t, dataset)

	// 第二次提交只带新条目：旧条目仍被上一个快照引用，其块不能算孤儿。
	fp, _, err := cs.Put(context.Background(), strings.NewReader("new-block"))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	if _, err := NewWriter(store, cs, Config{}).Commit(context.Background(), dataset, []Entry{mkEntry("new.tar", fp)}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	oldBlock := objectstore.BlockKey(dataset, "live.tar", 0)
	backdate(t, mock, store, oldBlock, 72*time.Hour)

	orphans, err := FindOrphanBlocks(context.Background(), store, dataset, 24*time.Hour)
	if err != nil {
		t.Fatalf("FindOrphanBlocks: %v", err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans = %+v, want 空：旧快照样引用该块", orphans)
	}
}

func TestFindOrphanBlocksFailsOnBrokenManifest(t *testing.T) {
	const dataset = "orphan-broken"
	store, mock, _, _ := orphanEnv(t, dataset)
	backdate(t, mock, store, objectstore.BlockKey(dataset, "gone.tar", 0), 48*time.Hour)

	keys, err := store.List(context.Background(), snapshotsPrefix(dataset))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var manifestKey string
	for _, k := range keys {
		if strings.HasSuffix(k, "/manifest.json") {
			manifestKey = k
			break
		}
	}
	if manifestKey == "" {
		t.Fatalf("找不到 manifest 键: %v", keys)
	}
	if _, err := store.Put(context.Background(), manifestKey, strings.NewReader("{"), 1); err != nil {
		t.Fatalf("Put manifest: %v", err)
	}

	if orphans, err := FindOrphanBlocks(context.Background(), store, dataset, 24*time.Hour); err == nil {
		t.Fatalf("manifest 损坏时返回了 %d 个孤儿，want 错误", len(orphans))
	}
}
