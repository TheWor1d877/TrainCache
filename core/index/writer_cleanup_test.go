package index

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"traincache/core/objectstore"
)

// errOrphanInjectedFailure 是故障注入用的哨兵错误。
var errOrphanInjectedFailure = errors.New("injected object store failure")

// orphanFaultStore 包装 Store，按对象键注入上传/读取故障，并记录本次尝试新写入的键。
// 仅测试使用；字段访问经锁，保证 -race 下安全。
type orphanFaultStore struct {
	objectstore.Store

	mu              sync.Mutex
	failShardPut    bool
	failManifestPut bool
	conflictAlways  bool
	corruptNewGets  bool
	newKeys         map[string]struct{}
}

func newOrphanFaultStore(base objectstore.Store) *orphanFaultStore {
	return &orphanFaultStore{Store: base, newKeys: make(map[string]struct{})}
}

func (s *orphanFaultStore) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	s.mu.Lock()
	failShard := s.failShardPut && strings.Contains(key, "/shard-") && strings.HasSuffix(key, ".idx")
	failManifest := s.failManifestPut && strings.HasSuffix(key, "/manifest.json")
	s.mu.Unlock()
	if failShard || failManifest {
		return "", errOrphanInjectedFailure
	}
	etag, err := s.Store.Put(ctx, key, r, size)
	if err == nil {
		s.mu.Lock()
		s.newKeys[key] = struct{}{}
		s.mu.Unlock()
	}
	return etag, err
}

func (s *orphanFaultStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	corrupt := s.corruptNewGets
	_, isNew := s.newKeys[key]
	s.mu.Unlock()
	if !corrupt || !isNew {
		return s.Store.Get(ctx, key, w)
	}
	var buf bytes.Buffer
	if err := s.Store.Get(ctx, key, &buf); err != nil {
		return err
	}
	data := buf.Bytes()
	if len(data) > 0 {
		data[0] ^= 0xFF
	}
	_, err := w.Write(data)
	return err
}

func (s *orphanFaultStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	s.mu.Lock()
	conflict := s.conflictAlways
	s.mu.Unlock()
	if conflict {
		return "", objectstore.ErrConflict
	}
	return s.Store.PutIfMatch(ctx, key, r, size, expectedETag)
}

// resetNewKeys 清空本次尝试的新键记录（用于区分前后两次 Commit）。
func (s *orphanFaultStore) resetNewKeys() {
	s.mu.Lock()
	s.newKeys = make(map[string]struct{})
	s.mu.Unlock()
}

// setCorruptNewGets 控制是否损坏本次新上传对象的读回内容。
func (s *orphanFaultStore) setCorruptNewGets(corrupt bool) {
	s.mu.Lock()
	s.corruptNewGets = corrupt
	s.mu.Unlock()
}

// orphanKeySet 返回对象存储中的全部键。
func orphanKeySet(t *testing.T, s objectstore.Store) map[string]struct{} {
	t.Helper()
	keys, err := s.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return set
}

// orphanCleanupEntry 构造一条条目并先把其块写入 CAS（提交前验证要求块已落盘）。
func orphanCleanupEntry(t *testing.T, env *testEnv, path string, chunk int64, content string) Entry {
	t.Helper()
	return Entry{
		Path:        path,
		Chunk:       chunk,
		Size:        int64(len(content)),
		Node:        "n1",
		Fingerprint: env.putBlock(t, content),
	}
}

// assertOrphanKeySet 断言对象键集合与 want 完全一致。
func assertOrphanKeySet(t *testing.T, got, want map[string]struct{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("对象键数量 = %d，期望 %d：got=%v want=%v", len(got), len(want), got, want)
	}
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Fatalf("缺少对象键 %q：got=%v", k, got)
		}
	}
}

// TestCommitCleansUpShardUploadFailure：分片上传失败 → 本次新上传对象全部清理。
func TestCommitCleansUpShardUploadFailure(t *testing.T) {
	env := newTestEnv(t)
	fs := newOrphanFaultStore(env.store)
	fs.failShardPut = true

	e := orphanCleanupEntry(t, env, "/d/a", 0, "shard-upload-fail")
	w := NewWriter(fs, env.cs, Config{})
	if _, err := w.Commit(context.Background(), "shardfail", []Entry{e}); err == nil {
		t.Fatal("Commit 应因分片上传失败而返回错误")
	}
	assertOrphanKeySet(t, orphanKeySet(t, fs), nil)
}

// TestCommitCleansUpManifestUploadFailure：manifest 上传失败 → 已上传的新分片被清理。
func TestCommitCleansUpManifestUploadFailure(t *testing.T) {
	env := newTestEnv(t)
	fs := newOrphanFaultStore(env.store)
	fs.failManifestPut = true

	e := orphanCleanupEntry(t, env, "/d/a", 0, "manifest-upload-fail")
	w := NewWriter(fs, env.cs, Config{})
	if _, err := w.Commit(context.Background(), "manifestfail", []Entry{e}); err == nil {
		t.Fatal("Commit 应因 manifest 上传失败而返回错误")
	}
	assertOrphanKeySet(t, orphanKeySet(t, fs), nil)
}

// TestCommitCleansUpVerificationFailure：提交前验证失败 → 新对象清理，复用旧分片保留。
func TestCommitCleansUpVerificationFailure(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	cfg := Config{MaxEntriesPerShard: 2}
	const dataset = "verifyfail"

	e1 := orphanCleanupEntry(t, env, "/d/a", 0, "verify-a")
	e2 := orphanCleanupEntry(t, env, "/d/b", 0, "verify-b")
	e3 := orphanCleanupEntry(t, env, "/d/c", 0, "verify-c")
	e4 := orphanCleanupEntry(t, env, "/d/d", 0, "verify-d")

	fs := newOrphanFaultStore(env.store)
	w := NewWriter(fs, env.cs, cfg)
	if _, err := w.Commit(ctx, dataset, []Entry{e1, e2, e3, e4}); err != nil {
		t.Fatalf("首次 Commit: %v", err)
	}
	before := orphanKeySet(t, fs)

	// 第二次提交只改 e1 → 第 2 个分片被复用；让本次新上传对象的读回损坏。
	e1b := e1
	e1b.Size = 111
	e1b.Fingerprint = env.putBlock(t, "verify-a-v2")
	fs.resetNewKeys()
	fs.setCorruptNewGets(true)
	if _, err := w.Commit(ctx, dataset, []Entry{e1b}); err == nil {
		t.Fatal("Commit 应在提交前验证失败时返回错误")
	}
	fs.setCorruptNewGets(false)

	// 失败尝试的新分片与 manifest 已删除；旧 CURRENT/manifest/复用分片全部保留。
	assertOrphanKeySet(t, orphanKeySet(t, fs), before)
}

// TestCommitCleansUpConflictExhausted：CURRENT 冲突耗尽重试 → 尝试的新对象均清理。
func TestCommitCleansUpConflictExhausted(t *testing.T) {
	env := newTestEnv(t)
	fs := newOrphanFaultStore(env.store)
	fs.conflictAlways = true

	e := orphanCleanupEntry(t, env, "/d/a", 0, "conflict-exhausted")
	w := NewWriter(fs, env.cs, Config{CommitMaxAttempts: 1})
	_, err := w.Commit(context.Background(), "conflictfail", []Entry{e})
	if !errors.Is(err, objectstore.ErrConflict) {
		t.Fatalf("Commit 错误 = %v，期望包装 objectstore.ErrConflict", err)
	}
	assertOrphanKeySet(t, orphanKeySet(t, fs), nil)
}

// TestCommitSuccessKeepsObjects：成功提交后不清理任何对象。
func TestCommitSuccessKeepsObjects(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	const dataset = "keep"

	e := orphanCleanupEntry(t, env, "/d/a", 0, "keep-a")
	w := NewWriter(env.store, env.cs, Config{})
	snapID, err := w.Commit(ctx, dataset, []Entry{e})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	keys := orphanKeySet(t, env.store)
	if _, ok := keys[CurrentKey(dataset)]; !ok {
		t.Fatalf("成功后 CURRENT 不存在：%v", keys)
	}
	if _, ok := keys[ManifestKey(dataset, snapID)]; !ok {
		t.Fatalf("成功后 manifest 不存在：%v", keys)
	}
	_, m := currentSnapshot(t, env.store, dataset)
	if len(m.Shards) == 0 {
		t.Fatal("成功后 manifest 无分片")
	}
	for _, ref := range m.Shards {
		if _, ok := keys[ref.Key]; !ok {
			t.Fatalf("成功后分片 %q 不存在：%v", ref.Key, keys)
		}
	}
}
