package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/core/objectstore"
)

// verifyManifestShards 逐一读回 manifest 引用的分片，校验对象键与内容指纹一致、条目数
// 与 manifest 相符，返回全部分片中的条目（顺序与 manifest 一致）。
func verifyManifestShards(t *testing.T, store objectstore.Store, m *Manifest) []Entry {
	t.Helper()
	ctx := context.Background()
	var all []Entry
	for _, ref := range m.Shards {
		if !strings.HasSuffix(ref.Key, "shard-"+ref.Fingerprint[:16]+".idx") {
			t.Fatalf("shard key %q does not match fingerprint %q", ref.Key, ref.Fingerprint)
		}
		data, err := getObject(ctx, store, ref.Key)
		if err != nil {
			t.Fatalf("read shard %s: %v", ref.Key, err)
		}
		if got := contentFingerprint(data); got != ref.Fingerprint {
			t.Fatalf("shard %s content fp = %s, manifest says %s", ref.Key, got, ref.Fingerprint)
		}
		es, err := unmarshalEntries(data)
		if err != nil {
			t.Fatalf("decode shard %s: %v", ref.Key, err)
		}
		if int64(len(es)) != ref.Entries {
			t.Fatalf("shard %s has %d entries, manifest says %d", ref.Key, len(es), ref.Entries)
		}
		all = append(all, es...)
	}
	return all
}

// waitForEntry 轮询 Catalog 直到 (path, chunk) 命中 want（逐字段相等）或超时。
func waitForEntry(t *testing.T, c *Catalog, want Entry) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, ok, err := c.Lookup(context.Background(), want.Path, want.Chunk)
		if err == nil && ok && got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("entry %+v not visible via Catalog within 3s", want)
}

// sendErr 非阻塞地把第一个错误送入 errCh（供 goroutine 内上报，避免阻塞）。
func sendErr(errCh chan error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

// TestConcurrentCommit：20 个 goroutine 并发 Commit 不同 entries → 全部成功；最终
// CURRENT 有效、manifest 可解析、所有分片指纹一致，20 组 entries 全部在最终快照中。
//
// 确定性论证：某 writer 的第 k 次尝试冲突 ⇔ 其读状态与 PutIfMatch 之间至少有一个其他
// writer 提交成功；各次尝试的时间窗互不重叠且其他 writer 最多各成功一次，故冲突次数
// ≤ 19。CommitMaxAttempts 取 20（= 19 + 首次尝试）保证所有 writer 必然成功、无重试耗尽。
func TestConcurrentCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	env := newTestEnv(t)
	const dataset = "concurrent"
	const writers = 20
	cfg := Config{CommitMaxAttempts: writers}

	entries := make([]Entry, writers)
	for i := range entries {
		e := Entry{
			Path:  fmt.Sprintf("/data/w%02d.bin", i),
			Chunk: 0,
			Size:  int64(i + 1),
			Node:  fmt.Sprintf("n%d", i),
		}
		e.Fingerprint = env.putBlock(t, fmt.Sprintf("concurrent-payload-%02d", i))
		entries[i] = e
	}

	w := NewWriter(env.store, env.cs, cfg)
	snapIDs := make([]string, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			snapIDs[i], errs[i] = w.Commit(ctx, dataset, entries[i:i+1])
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d Commit: %v", i, err)
		}
		if snapIDs[i] == "" {
			t.Fatalf("writer %d returned empty snapID", i)
		}
	}

	// 最终 CURRENT 有效、manifest 可解析、所有分片指纹一致、条目总数 = 20。
	finalSnapID, m := currentSnapshot(t, env.store, dataset)
	all := verifyManifestShards(t, env.store, m)
	if len(all) != writers {
		t.Fatalf("final snapshot has %d entries, want %d", len(all), writers)
	}
	inList := false
	for _, id := range snapIDs {
		if id == finalSnapID {
			inList = true
			break
		}
	}
	if !inList {
		t.Fatalf("final CURRENT snapID %q is not any writer's returned snapID", finalSnapID)
	}

	// 20 组 entries 全部出现在最终快照中（无静默丢失）。
	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, want := range entries {
		got, ok, err := c.Lookup(ctx, want.Path, want.Chunk)
		if err != nil || !ok || got != want {
			t.Fatalf("Lookup(%q, %d) = (%+v, %v, %v), want %+v", want.Path, want.Chunk, got, ok, err, want)
		}
	}
}

// gateStore 包装 objectstore.Store，为 TestCommitConflictRetry 制造确定性竞争：
// 对 CURRENT 的 GetETag，前两个调用者各自读到底层 ETag 后阻塞，直到两个调用者都完成
// 读取（必然读到同一代 CURRENT）才一起放行；此后不再阻塞。PutIfMatch 仅委托并统计
// ErrConflict 次数。
type gateStore struct {
	objectstore.Store
	currentKey string

	mu        sync.Mutex
	readers   int
	gate      chan struct{}
	conflicts int
}

func newGateStore(inner objectstore.Store, currentKey string) *gateStore {
	return &gateStore{Store: inner, currentKey: currentKey, gate: make(chan struct{})}
}

func (s *gateStore) GetETag(ctx context.Context, key string) (string, error) {
	etag, err := s.Store.GetETag(ctx, key)
	if key != s.currentKey || err != nil {
		return etag, err
	}
	s.mu.Lock()
	s.readers++
	if s.readers == 2 {
		close(s.gate)
	}
	gate := s.gate
	s.mu.Unlock()
	select {
	case <-gate:
		return etag, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *gateStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	etag, err := s.Store.PutIfMatch(ctx, key, r, size, expectedETag)
	if errors.Is(err, objectstore.ErrConflict) {
		s.mu.Lock()
		s.conflicts++
		s.mu.Unlock()
	}
	return etag, err
}

func (s *gateStore) conflictCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conflicts
}

// TestCommitConflictRetry：两个 Writer 在屏障放行后基于同一代 CURRENT（同 ETag）提交 →
// PutIfMatch 恰一个成功、另一个 ErrConflict 后重读最新快照重试成功；两条数据都在最终快照。
func TestCommitConflictRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	env := newTestEnv(t)
	const dataset = "conflict"
	cfg := Config{}

	// 先提交种子快照 S0，使两个 Writer 的首次 GetETag(CURRENT) 都读到同一非空 ETag。
	seed := Entry{Path: "/d/seed", Chunk: 0, Size: 1, Node: "n0"}
	seed.Fingerprint = env.putBlock(t, "conflict-seed")
	if _, err := NewWriter(env.store, env.cs, cfg).Commit(ctx, dataset, []Entry{seed}); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	gs := newGateStore(env.store, CurrentKey(dataset))
	w := NewWriter(gs, env.cs, cfg)

	e1 := Entry{Path: "/d/one", Chunk: 0, Size: 11, Node: "n1"}
	e1.Fingerprint = env.putBlock(t, "conflict-one")
	e2 := Entry{Path: "/d/two", Chunk: 0, Size: 22, Node: "n2"}
	e2.Fingerprint = env.putBlock(t, "conflict-two")

	snapIDs := make([]string, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, e := range []Entry{e1, e2} {
		wg.Add(1)
		go func(i int, e Entry) {
			defer wg.Done()
			<-start
			snapIDs[i], errs[i] = w.Commit(ctx, dataset, []Entry{e})
		}(i, e)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d Commit: %v", i, err)
		}
	}
	if snapIDs[0] == snapIDs[1] {
		t.Fatalf("both writers returned the same snapID %q", snapIDs[0])
	}
	if got := gs.conflictCount(); got != 1 {
		t.Fatalf("PutIfMatch conflicts = %d, want exactly 1", got)
	}

	// 恰一个生效、另一个重试后基于新快照提交成功：最终快照同时含 seed + 两条数据。
	finalSnapID, m := currentSnapshot(t, env.store, dataset)
	if finalSnapID != snapIDs[0] && finalSnapID != snapIDs[1] {
		t.Fatalf("final CURRENT snapID %q is neither writer result (%q, %q)", finalSnapID, snapIDs[0], snapIDs[1])
	}
	if all := verifyManifestShards(t, env.store, m); len(all) != 3 {
		t.Fatalf("final snapshot has %d entries, want 3", len(all))
	}

	c := NewCatalog(env.store, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	for _, want := range []Entry{seed, e1, e2} {
		got, ok, err := c.Lookup(ctx, want.Path, want.Chunk)
		if err != nil || !ok || got != want {
			t.Fatalf("Lookup(%q, %d) = (%+v, %v, %v), want %+v", want.Path, want.Chunk, got, ok, err, want)
		}
	}
}

// TestRefreshAtomicSwitch：后台 Start（小 RefreshInterval）运行中持续 Lookup，同时连续
// 多次 Commit（每代成对更新 rev/pair 两条 + anchor 常驻不变）→ 每次 Lookup 命中的一组
// 条目都必须与某一代快照的完整条目集逐字段一致（新旧不混合）。
func TestRefreshAtomicSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	env := newTestEnv(t)
	const dataset = "switch"
	const generations = 6
	cfg := Config{RefreshInterval: 5 * time.Millisecond}

	// 每代 g 的完整条目集：rev（Size=g）与 pair（Size=1000+g）成对更新；anchor 恒不变。
	type generation struct{ rev, pair Entry }
	gens := make([]generation, generations)
	anchor := Entry{Path: "/d/anchor", Chunk: 0, Size: 42, Node: "n-anchor"}
	anchor.Fingerprint = env.putBlock(t, "switch-anchor")
	for g := 0; g < generations; g++ {
		rev := Entry{Path: "/d/rev", Chunk: 0, Size: int64(g), Node: fmt.Sprintf("gen-%d", g)}
		rev.Fingerprint = env.putBlock(t, fmt.Sprintf("switch-rev-%d", g))
		pair := Entry{Path: "/d/pair", Chunk: 0, Size: 1000 + int64(g), Node: fmt.Sprintf("gen-%d", g)}
		pair.Fingerprint = env.putBlock(t, fmt.Sprintf("switch-pair-%d", g))
		gens[g] = generation{rev: rev, pair: pair}
	}

	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{anchor, gens[0].rev, gens[0].pair}); err != nil {
		t.Fatalf("Commit gen 0: %v", err)
	}

	c := NewCatalog(env.store, dataset, cfg)
	if _, _, err := c.Lookup(ctx, gens[0].rev.Path, gens[0].rev.Chunk); !errors.Is(err, ErrNoCurrent) {
		t.Fatalf("Lookup before Start = %v, want ErrNoCurrent", err)
	}

	startCtx, startCancel := context.WithCancel(ctx)
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		c.Start(startCtx)
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			startCancel()
			<-startDone
		}
	}
	defer stop()

	// 等待首轮刷新完成（gen 0 的完整条目集可见）。
	waitForEntry(t, c, gens[0].rev)
	waitForEntry(t, c, gens[0].pair)
	waitForEntry(t, c, anchor)

	// 持续 Lookup。相邻两次 Lookup 之间可能发生一次原子切换（切换前后的值分别来自两个
	// 完整快照），因此检查三条可观察不变量：
	//  ① 每次命中的 Entry 必须逐字段等于某一代完整快照中的同键条目（绝不出现混合/半写值）；
	//  ② 观察到的代次只增不减（原子切换只前进、不回退到旧快照）；
	//  ③ 相邻两次命中同代（= 同属一个已发布状态）时，必须逐字段等于该代完整条目集。
	var observations, sameGen, lastGen atomic.Int64
	lastGen.Store(-1)
	readerErr := make(chan error, 1)
	stopRead := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		observe := func(g int) error {
			for {
				last := lastGen.Load()
				if int64(g) < last {
					return fmt.Errorf("snapshot generation went backwards: %d after %d", g, last)
				}
				if int64(g) == last || lastGen.CompareAndSwap(last, int64(g)) {
					return nil
				}
			}
		}
		for {
			select {
			case <-stopRead:
				return
			default:
			}
			rev, ok, err := c.Lookup(ctx, gens[0].rev.Path, gens[0].rev.Chunk)
			if err != nil || !ok {
				sendErr(readerErr, fmt.Errorf("Lookup rev = (%+v, %v, %v)", rev, ok, err))
				return
			}
			rg := int(rev.Size)
			if rg < 0 || rg >= generations || rev != gens[rg].rev {
				sendErr(readerErr, fmt.Errorf("rev hit is not an entry of any committed generation: %+v", rev))
				return
			}
			pair, ok, err := c.Lookup(ctx, gens[0].pair.Path, gens[0].pair.Chunk)
			if err != nil || !ok {
				sendErr(readerErr, fmt.Errorf("Lookup pair = (%+v, %v, %v)", pair, ok, err))
				return
			}
			pg := int(pair.Size) - 1000
			if pg < 0 || pg >= generations || pair != gens[pg].pair {
				sendErr(readerErr, fmt.Errorf("pair hit is not an entry of any committed generation: %+v", pair))
				return
			}
			anc, ok, err := c.Lookup(ctx, anchor.Path, anchor.Chunk)
			if err != nil || !ok || anc != anchor {
				sendErr(readerErr, fmt.Errorf("Lookup anchor = (%+v, %v, %v)", anc, ok, err))
				return
			}
			if pg < rg {
				sendErr(readerErr, fmt.Errorf("snapshot generation went backwards: rev gen %d, pair gen %d", rg, pg))
				return
			}
			if pg == rg {
				// 两次命中同代 → 同属一个已发布状态 → 必须是该代完整条目集。
				if rev != gens[rg].rev || pair != gens[rg].pair {
					sendErr(readerErr, fmt.Errorf("same-generation entries inconsistent (gen %d): rev=%+v pair=%+v", rg, rev, pair))
					return
				}
				sameGen.Add(1)
			}
			if err := observe(rg); err != nil {
				sendErr(readerErr, err)
				return
			}
			if err := observe(pg); err != nil {
				sendErr(readerErr, err)
				return
			}
			observations.Add(1)
		}
	}()
	readStopped := false
	stopReader := func() {
		if !readStopped {
			readStopped = true
			close(stopRead)
			<-readerDone
		}
	}
	defer stopReader()

	// 期间连续多次 Commit（每代成对更新 rev/pair）。
	for g := 1; g < generations; g++ {
		if _, err := w.Commit(ctx, dataset, []Entry{gens[g].rev, gens[g].pair}); err != nil {
			t.Fatalf("Commit gen %d: %v", g, err)
		}
		time.Sleep(10 * time.Millisecond) // 让后台刷新至少完成一轮（interval 5ms）
	}
	// 最终一代最终可见（证明刷新循环持续推进到最新快照）。
	waitForEntry(t, c, gens[generations-1].rev)
	waitForEntry(t, c, gens[generations-1].pair)

	// 提交停止后快照不再变化：等读者观察到末代、且至少一次"同代完整条目集"。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (lastGen.Load() != int64(generations-1) || sameGen.Load() == 0) {
		select {
		case err := <-readerErr:
			t.Fatalf("concurrent Lookup observed an inconsistent snapshot: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if got := lastGen.Load(); got != int64(generations-1) {
		t.Fatalf("reader last observed generation %d, want %d", got, generations-1)
	}
	if sameGen.Load() == 0 {
		t.Fatal("reader never observed a same-generation (complete) entry set")
	}

	stopReader()
	select {
	case err := <-readerErr:
		t.Fatalf("concurrent Lookup observed an inconsistent snapshot: %v", err)
	default:
	}
	if observations.Load() == 0 {
		t.Fatal("no concurrent Lookup observations recorded")
	}
	stop()
}

// faultStore 包装 objectstore.Store，为 TestRefreshErrorTolerance 提供两类注入：
//  1. armCorrupt：对目标分片对象的首次 Get 返回"翻转 1 字节"的内容（注入一次即停），
//     同时置 paused 并清 armed；
//  2. paused 置位后，对 CURRENT 的 Get 先向 pausedAt 发信号、再阻塞至 release 关闭，
//     把失败后的下一轮刷新精确卡在断言窗口之后。
type faultStore struct {
	objectstore.Store
	currentKey string
	targetKey  string // 在启动后台循环前于测试主 goroutine 内设置

	corrupted chan struct{} // 缓冲 1：损坏注入已发生
	pausedAt  chan struct{} // 缓冲 1：失败后循环再次读 CURRENT 并阻塞
	release   chan struct{} // 关闭后放行被暂停的 CURRENT 读取

	mu     sync.Mutex
	armed  bool
	paused bool
}

func newFaultStore(inner objectstore.Store, currentKey string) *faultStore {
	return &faultStore{
		Store:      inner,
		currentKey: currentKey,
		corrupted:  make(chan struct{}, 1),
		pausedAt:   make(chan struct{}, 1),
		release:    make(chan struct{}),
	}
}

// armCorrupt 装配一次性损坏注入（一次 Get 命中后自动停止）。
func (s *faultStore) armCorrupt() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

// releaseGate 放行被暂停的 CURRENT 读取。
func (s *faultStore) releaseGate() { close(s.release) }

func (s *faultStore) Get(ctx context.Context, key string, w io.Writer) error {
	if key == s.currentKey {
		s.mu.Lock()
		paused, release := s.paused, s.release
		s.mu.Unlock()
		if paused {
			select {
			case s.pausedAt <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if key == s.targetKey {
		s.mu.Lock()
		corrupt := s.armed
		if corrupt {
			s.armed = false
			s.paused = true
		}
		s.mu.Unlock()
		if corrupt {
			select {
			case s.corrupted <- struct{}{}:
			default:
			}
			var buf bytes.Buffer
			if err := s.Store.Get(ctx, key, &buf); err != nil {
				return err
			}
			b := buf.Bytes()
			if len(b) > 0 {
				b[0] ^= 0xFF
			}
			_, err := w.Write(b)
			return err
		}
	}
	return s.Store.Get(ctx, key, w)
}

// TestRefreshErrorTolerance：mock 注入一次分片损坏 → 后台刷新失败但循环继续、映射保持
// 旧快照（Lookup 仍返回旧值）；放行（注入已停止）后下一次刷新恢复，新快照可见。
func TestRefreshErrorTolerance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	env := newTestEnv(t)
	const dataset = "tolerance"
	cfg := Config{RefreshInterval: 20 * time.Millisecond}

	e1 := Entry{Path: "/d/a", Chunk: 0, Size: 1, Node: "n1"}
	e1.Fingerprint = env.putBlock(t, "tolerance-a-v1")
	w := NewWriter(env.store, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{e1}); err != nil {
		t.Fatalf("first Commit: %v", err)
	}

	fs := newFaultStore(env.store, CurrentKey(dataset))
	c := NewCatalog(fs, dataset, cfg)
	if err := c.Refresh(ctx); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}
	if got, ok, err := c.Lookup(ctx, e1.Path, e1.Chunk); err != nil || !ok || got != e1 {
		t.Fatalf("initial Lookup = (%+v, %v, %v), want %+v", got, ok, err, e1)
	}

	// 第二代快照：upsert e1 → e1b，并新增 e2。
	e1b := e1
	e1b.Size = 111
	e1b.Node = "n9"
	e1b.Fingerprint = env.putBlock(t, "tolerance-a-v2")
	e2 := Entry{Path: "/d/b", Chunk: 0, Size: 22, Node: "n2"}
	e2.Fingerprint = env.putBlock(t, "tolerance-b")
	if _, err := w.Commit(ctx, dataset, []Entry{e1b, e2}); err != nil {
		t.Fatalf("second Commit: %v", err)
	}

	// 定位第二代 manifest 的首个分片对象，装配一次性损坏注入。
	_, m2 := currentSnapshot(t, env.store, dataset)
	if len(m2.Shards) == 0 {
		t.Fatal("second manifest has no shards")
	}
	fs.targetKey = m2.Shards[0].Key
	fs.armCorrupt()

	// 启动后台刷新循环：首轮刷新命中损坏分片失败（映射保持旧快照）；失败后循环继续，
	// 第二轮刷新在读 CURRENT 处被闸门卡住（证明失败不退出循环，且断言期间映射不变）。
	startCtx, startCancel := context.WithCancel(ctx)
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		c.Start(startCtx)
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			startCancel()
			<-startDone
		}
	}
	defer stop()

	select {
	case <-fs.corrupted:
	case <-startDone:
		t.Fatal("refresh loop exited after corrupted refresh")
	case <-time.After(3 * time.Second):
		t.Fatal("background refresh did not hit the injected corruption")
	}
	select {
	case <-fs.pausedAt:
	case <-startDone:
		t.Fatal("refresh loop exited after the failed refresh")
	case <-time.After(3 * time.Second):
		t.Fatal("refresh loop did not attempt another refresh after the failure")
	}

	// 首轮刷新已失败、第二轮被卡在读 CURRENT 之前 → 映射必为旧快照。
	if got, ok, err := c.Lookup(ctx, e1.Path, e1.Chunk); err != nil || !ok || got != e1 {
		t.Fatalf("Lookup after failed refresh = (%+v, %v, %v), want old %+v", got, ok, err, e1)
	}
	if got, ok, _ := c.Lookup(ctx, e2.Path, e2.Chunk); ok {
		t.Fatalf("entry from the failed refresh is visible: %+v", got)
	}
	// 再等数个刷新周期（闸门卡住循环）：旧映射保持、新快照条目始终不可见。
	time.Sleep(3 * cfg.RefreshInterval)
	if got, ok, err := c.Lookup(ctx, e1.Path, e1.Chunk); err != nil || !ok || got != e1 {
		t.Fatalf("Lookup while gated = (%+v, %v, %v), want old %+v", got, ok, err, e1)
	}
	if _, ok, _ := c.Lookup(ctx, e2.Path, e2.Chunk); ok {
		t.Fatal("gen-2 entry became visible before the gate was released")
	}

	// 放行：下一轮刷新读到完好的分片 → 新快照可见（证明失败后循环继续工作）。
	fs.releaseGate()
	waitForEntry(t, c, e1b)
	waitForEntry(t, c, e2)
	stop()
}
