package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// snapshotChange 是 OnSnapshotChange 的测试记录。
type snapshotChange struct {
	dataset string
	snapID  string
}

// waitSnapshotChange 等待一次匹配 (dataset, snapID) 的切换回调。
func waitSnapshotChange(t *testing.T, ch <-chan snapshotChange, dataset, snapID string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case got := <-ch:
			if got.dataset == dataset && got.snapID == snapID {
				return
			}
		case <-deadline:
			t.Fatalf("did not observe snapshot change (%s, %s) within 3s", dataset, snapID)
		}
	}
}

// stopFollow 取消 Follow 并断言其返回 context.Canceled。
func stopFollow(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Follow did not return after context cancellation")
	}
}

// TestCatalogSnapID 覆盖 SnapID 在 Refresh / LoadPrefixes / RefreshManifest 后的取值，
// 以及无 CURRENT 时 SnapID 为空串、RefreshManifest 返回 ErrNoCurrent。
func TestCatalogSnapID(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "snapid"
	cfg := Config{MaxEntriesPerShard: 2}

	a := Entry{Path: "/d/a", Chunk: 0, Size: 1, Node: "n1"}
	a.Fingerprint = env.putBlock(t, "snapid-a")
	w := NewWriter(env.store, env.cs, cfg)
	snap1, err := w.Commit(ctx, dataset, []Entry{a})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	c := NewCatalog(env.store, dataset, cfg)
	if got := c.SnapID(); got != "" {
		t.Fatalf("SnapID before any load = %q, want empty", got)
	}
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := c.SnapID(); got != snap1 {
		t.Fatalf("SnapID after Refresh = %q, want %q", got, snap1)
	}
	if err := c.LoadPrefixes(ctx, []string{"/d"}); err != nil {
		t.Fatalf("LoadPrefixes: %v", err)
	}
	if got := c.SnapID(); got != snap1 {
		t.Fatalf("SnapID after LoadPrefixes = %q, want %q", got, snap1)
	}

	b := Entry{Path: "/d/b", Chunk: 0, Size: 2, Node: "n2"}
	b.Fingerprint = env.putBlock(t, "snapid-b")
	snap2, err := w.Commit(ctx, dataset, []Entry{b})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	if snap2 == snap1 {
		t.Fatalf("second Commit reused snapID %q", snap2)
	}
	if err := c.RefreshManifest(ctx); err != nil {
		t.Fatalf("RefreshManifest: %v", err)
	}
	if got := c.SnapID(); got != snap2 {
		t.Fatalf("SnapID after RefreshManifest = %q, want %q", got, snap2)
	}

	// 无 CURRENT：SnapID 为空串，RefreshManifest 返回 ErrNoCurrent。
	empty := NewCatalog(env.store, "no-such-dataset", cfg)
	if got := empty.SnapID(); got != "" {
		t.Fatalf("SnapID without CURRENT = %q, want empty", got)
	}
	if err := empty.RefreshManifest(ctx); !errors.Is(err, ErrNoCurrent) {
		t.Fatalf("RefreshManifest without CURRENT = %v, want ErrNoCurrent", err)
	}
	if got := empty.SnapID(); got != "" {
		t.Fatalf("SnapID after failed RefreshManifest = %q, want empty", got)
	}
}

// TestRefreshManifest 覆盖：成功切换后 entries 为空、snapID/shards 正确且回调触发；
// 失败时旧映射保持、OnSnapshotChange 不再调用；回调 panic 不向调用方传播。
func TestRefreshManifest(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "refreshmanifest"

	entries := []Entry{
		{Path: "/d/a", Chunk: 0, Size: 1, Node: "n1"},
		{Path: "/d/b", Chunk: 0, Size: 2, Node: "n1"},
		{Path: "/d/c", Chunk: 0, Size: 3, Node: "n2"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("refreshmanifest-%d", i))
	}
	cfg := Config{MaxEntriesPerShard: 2}
	w := NewWriter(env.store, env.cs, cfg)
	snap1, err := w.Commit(ctx, dataset, entries)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, m1 := currentSnapshot(t, env.store, dataset)

	changes := make(chan snapshotChange, 32)
	var changeCount atomic.Int64
	cfg.OnSnapshotChange = func(ds, id string) {
		changeCount.Add(1)
		changes <- snapshotChange{dataset: ds, snapID: id}
	}
	cfg.OnSnapshotError = func(ds string, err error) {
		t.Errorf("unexpected OnSnapshotError(%q, %v)", ds, err)
	}
	c := NewCatalog(env.store, dataset, cfg)

	// 建立非空旧映射。
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	waitSnapshotChange(t, changes, dataset, snap1)

	if err := c.RefreshManifest(ctx); err != nil {
		t.Fatalf("RefreshManifest: %v", err)
	}
	waitSnapshotChange(t, changes, dataset, snap1)
	if got := c.SnapID(); got != snap1 {
		t.Fatalf("SnapID = %q, want %q", got, snap1)
	}
	for _, e := range entries {
		assertMiss(t, c, e.Path, e.Chunk)
	}
	if got, want := c.ShardsLoaded(), len(m1.Shards); got != want {
		t.Fatalf("ShardsLoaded = %d, want %d", got, want)
	}

	// 回调 panic 不得传播。
	panicking := NewCatalog(env.store, dataset, Config{
		OnSnapshotChange: func(string, string) { panic("callback panic") },
	})
	if err := panicking.Refresh(ctx); err != nil {
		t.Fatalf("Refresh with panicking callback = %v, want nil", err)
	}

	// 失败路径：重新建立非空映射 → 提交新快照并损坏其 manifest → RefreshManifest 失败。
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh (rebuild): %v", err)
	}
	for _, e := range entries {
		assertHit(t, c, e)
	}
	d := Entry{Path: "/d/d", Chunk: 0, Size: 4, Node: "n3"}
	d.Fingerprint = env.putBlock(t, "refreshmanifest-d")
	snap2, err := w.Commit(ctx, dataset, []Entry{d})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	raw, err := getObject(ctx, env.store, ManifestKey(dataset, snap2))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	raw[0] ^= 0xFF
	if _, err := env.store.Put(ctx, ManifestKey(dataset, snap2), bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}

	before := changeCount.Load()
	if err := c.RefreshManifest(ctx); !errors.Is(err, ErrInvalidShard) {
		t.Fatalf("RefreshManifest after tamper = %v, want ErrInvalidShard", err)
	}
	if got := c.SnapID(); got != snap1 {
		t.Fatalf("SnapID after failed RefreshManifest = %q, want %q", got, snap1)
	}
	for _, e := range entries {
		assertHit(t, c, e)
	}
	assertMiss(t, c, d.Path, d.Chunk)
	if got := changeCount.Load(); got != before {
		t.Fatalf("OnSnapshotChange called %d times after failed switch, want %d", got, before)
	}
}

// TestFollowPrefixes 覆盖非空 prefixes 的 Follow：变化后 ≤ RefreshInterval 经
// LoadPrefixes 加载相关分片（无关前缀条目不加载），成功切换触发 OnSnapshotChange。
func TestFollowPrefixes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	env := newTestEnv(t)
	const dataset = "followprefix"

	entries := []Entry{
		{Path: "/model/m.bin", Chunk: 0, Size: 1, Node: "n1"},
		{Path: "/train-data/a.bin", Chunk: 0, Size: 2, Node: "n1"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("followprefix-%d", i))
	}
	cfg := Config{MaxEntriesPerShard: 1, RefreshInterval: 10 * time.Millisecond}
	changes := make(chan snapshotChange, 64)
	cfg.OnSnapshotChange = func(ds, id string) {
		changes <- snapshotChange{dataset: ds, snapID: id}
	}
	errs := make(chan error, 64)
	cfg.OnSnapshotError = func(_ string, err error) { errs <- err }

	w := NewWriter(env.store, env.cs, cfg)
	snap1, err := w.Commit(ctx, dataset, entries)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	c := NewCatalog(env.store, dataset, cfg)
	followCtx, followCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Follow(followCtx, func() []string { return []string{"/train-data"} }) }()

	waitSnapshotChange(t, changes, dataset, snap1)
	if got := c.SnapID(); got != snap1 {
		t.Fatalf("SnapID = %q, want %q", got, snap1)
	}
	assertHit(t, c, entries[1])
	assertMiss(t, c, entries[0].Path, entries[0].Chunk)

	// 新一代快照：新增一条 /train-data 条目。
	next := Entry{Path: "/train-data/b.bin", Chunk: 0, Size: 3, Node: "n2"}
	next.Fingerprint = env.putBlock(t, "followprefix-next")
	snap2, err := w.Commit(ctx, dataset, []Entry{next})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	waitSnapshotChange(t, changes, dataset, snap2)
	assertHit(t, c, entries[1])
	assertHit(t, c, next)
	assertMiss(t, c, entries[0].Path, entries[0].Chunk)
	select {
	case err := <-errs:
		t.Fatalf("unexpected OnSnapshotError: %v", err)
	default:
	}

	stopFollow(t, followCancel, done)
}

// TestFollowEmptyPrefixesUsesRefreshManifest 覆盖 nil prefixes 按空处理：走
// RefreshManifest，成功切换后 entries 为空。
func TestFollowEmptyPrefixesUsesRefreshManifest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	env := newTestEnv(t)
	const dataset = "followempty"

	a := Entry{Path: "/d/a", Chunk: 0, Size: 1, Node: "n1"}
	a.Fingerprint = env.putBlock(t, "followempty-a")
	cfg := Config{RefreshInterval: 10 * time.Millisecond}
	changes := make(chan snapshotChange, 64)
	cfg.OnSnapshotChange = func(ds, id string) {
		changes <- snapshotChange{dataset: ds, snapID: id}
	}
	var errorCount atomic.Int64
	cfg.OnSnapshotError = func(string, error) { errorCount.Add(1) }

	w := NewWriter(env.store, env.cs, cfg)
	snap1, err := w.Commit(ctx, dataset, []Entry{a})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	c := NewCatalog(env.store, dataset, cfg)
	followCtx, followCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Follow(followCtx, func() []string { return nil }) }()

	waitSnapshotChange(t, changes, dataset, snap1)
	if got := c.SnapID(); got != snap1 {
		t.Fatalf("SnapID = %q, want %q", got, snap1)
	}
	assertMiss(t, c, a.Path, a.Chunk)

	// 新快照同样只切 snapID、清空映射。
	b := Entry{Path: "/d/b", Chunk: 0, Size: 2, Node: "n2"}
	b.Fingerprint = env.putBlock(t, "followempty-b")
	snap2, err := w.Commit(ctx, dataset, []Entry{b})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	waitSnapshotChange(t, changes, dataset, snap2)
	if got := c.SnapID(); got != snap2 {
		t.Fatalf("SnapID = %q, want %q", got, snap2)
	}
	assertMiss(t, c, a.Path, a.Chunk)
	assertMiss(t, c, b.Path, b.Chunk)
	if got := errorCount.Load(); got != 0 {
		t.Fatalf("OnSnapshotError called %d times, want 0", got)
	}

	stopFollow(t, followCancel, done)
}

// TestFollowErrorKeepsOldMapping 覆盖 Follow 失败路径：manifest 校验失败时旧映射与
// 旧 snapID 保持、OnSnapshotError 触发、OnSnapshotChange 不再调用、循环继续。
func TestFollowErrorKeepsOldMapping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	env := newTestEnv(t)
	const dataset = "followfail"

	entries := []Entry{
		{Path: "/d/a", Chunk: 0, Size: 1, Node: "n1"},
		{Path: "/d/b", Chunk: 0, Size: 2, Node: "n2"},
	}
	for i := range entries {
		entries[i].Fingerprint = env.putBlock(t, fmt.Sprintf("followfail-%d", i))
	}
	cfg := Config{RefreshInterval: 10 * time.Millisecond}
	var changeCount atomic.Int64
	cfg.OnSnapshotChange = func(string, string) { changeCount.Add(1) }
	errs := make(chan error, 64)
	cfg.OnSnapshotError = func(_ string, err error) { errs <- err }

	w := NewWriter(env.store, env.cs, cfg)
	snap1, err := w.Commit(ctx, dataset, entries)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	c := NewCatalog(env.store, dataset, cfg)
	// 先手工建立非空旧映射（同时触发一次成功回调）。
	if err := c.LoadPrefixes(ctx, []string{""}); err != nil {
		t.Fatalf("LoadPrefixes: %v", err)
	}
	for _, e := range entries {
		assertHit(t, c, e)
	}
	before := changeCount.Load()

	// 新快照发布后损坏其 manifest：Follow 必然加载失败。
	bad := Entry{Path: "/d/bad", Chunk: 0, Size: 3, Node: "n3"}
	bad.Fingerprint = env.putBlock(t, "followfail-bad")
	snap2, err := w.Commit(ctx, dataset, []Entry{bad})
	if err != nil {
		t.Fatalf("second Commit: %v", err)
	}
	raw, err := getObject(ctx, env.store, ManifestKey(dataset, snap2))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	raw[0] ^= 0xFF
	if _, err := env.store.Put(ctx, ManifestKey(dataset, snap2), bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}

	followCtx, followCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Follow(followCtx, func() []string { return []string{} }) }()

	select {
	case err := <-errs:
		if !errors.Is(err, ErrInvalidShard) {
			t.Fatalf("OnSnapshotError = %v, want ErrInvalidShard", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Follow did not report the manifest validation failure")
	}

	// 失败后循环继续重试若干轮，旧映射与旧 snapID 始终不变。
	time.Sleep(5 * cfg.RefreshInterval)
	if got := c.SnapID(); got != snap1 {
		t.Fatalf("SnapID after failed Follow refresh = %q, want %q", got, snap1)
	}
	for _, e := range entries {
		assertHit(t, c, e)
	}
	assertMiss(t, c, bad.Path, bad.Chunk)
	if got := changeCount.Load(); got != before {
		t.Fatalf("OnSnapshotChange called %d times after failed refresh, want %d", got, before)
	}

	stopFollow(t, followCancel, done)
}
