package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// testEnv 是 CLI 单测装置：内存 Mock 对象存储 + 临时目录 CAS（与 core 既有测试同口径）。
// 夹具经真实 index.Writer 写成：CURRENT/manifest/分片都是真对象，指纹自洽，
// 读侧（Catalog.List）的完整性校验链走完整路径。写侧只存在于测试装置，
// traincacheds 自身只读。
type testEnv struct {
	store *objectstore.Mock
	cs    *cas.Store
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	return &testEnv{store: objectstore.NewMock(), cs: cs}
}

// putBlock 把内容写入 CAS 并返回指纹；index.Writer.Commit 要求新块已先行落盘。
func (env *testEnv) putBlock(t *testing.T, content string) cas.Fingerprint {
	t.Helper()
	fp, _, err := env.cs.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("cas.Put(%q): %v", content, err)
	}
	return fp
}

// entry 造一条索引条目（内容落 CAS，fp 取内容指纹）。
func (env *testEnv) entry(t *testing.T, path string, chunk, size int64, content string) index.Entry {
	t.Helper()
	return index.Entry{Path: path, Chunk: chunk, Size: size, Fingerprint: env.putBlock(t, content)}
}

// commit 用真实 index.Writer 把条目提交为一个快照，返回 snapID。
func (env *testEnv) commit(t *testing.T, dataset string, cfg index.Config, entries []index.Entry) string {
	t.Helper()
	snapID, err := index.NewWriter(env.store, env.cs, cfg).Commit(context.Background(), dataset, entries)
	if err != nil {
		t.Fatalf("Commit(%s): %v", dataset, err)
	}
	return snapID
}

// put 往 Mock 里手写一个对象（构造杂项键与损坏夹具用）。
func (env *testEnv) put(t *testing.T, key, content string) {
	t.Helper()
	if _, err := env.store.Put(context.Background(), key, strings.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("Put(%s): %v", key, err)
	}
}

// remove 删除 Mock 里的一个对象（构造"CURRENT 缺失"夹具用）。
func (env *testEnv) remove(t *testing.T, key string) {
	t.Helper()
	if err := env.store.Delete(context.Background(), key); err != nil {
		t.Fatalf("Delete(%s): %v", key, err)
	}
}

// storeArgs 记录 fake newStore 收到的参数。
type storeArgs struct {
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
	secure    bool
}

// capture 记录依赖调用情况，用于断言用法错误时完全不触碰对象存储。
type capture struct {
	storeCalls int
	storeArgs  storeArgs
	storeErr   error
}

// mockDeps 返回把对象存储换成给定实现的依赖（CLI 无 CAS/Writer/probe 依赖）。
func mockDeps(store objectstore.Store, c *capture) deps {
	return deps{
		newStore: func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error) {
			c.storeCalls++
			c.storeArgs = storeArgs{endpoint: endpoint, accessKey: accessKey, secretKey: secretKey, bucket: bucket, secure: secure}
			if c.storeErr != nil {
				return nil, c.storeErr
			}
			return store, nil
		},
	}
}

// runWith 以给定对象存储执行一次 CLI 调用，返回退出码与两个输出流。
func runWith(store objectstore.Store, args ...string) (int, string, string) {
	c := &capture{}
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr, mockDeps(store, c))
	return code, stdout.String(), stderr.String()
}

// entryLine 是条目行的期望格式（与 CLI 输出同口径）。
func entryLine(e index.Entry) string {
	return fmt.Sprintf("%s %d %d %s", e.Path, e.Chunk, e.Size, e.Fingerprint.String())
}

// wantLines 把若干行拼成期望的完整 stdout。
func wantLines(lines ...string) string {
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

// seedEntries 返回一个 5 条 / 3 分片的稳定夹具：
// shard0=[data/a.tar#0, data/a.tar#1]、shard1=[data/b.tar#0, logs/c.tar#0]、shard2=[logs/d.tar#0]。
// 条目已按 (path, chunk) 字典序排列（与 Catalog.List 输出定序一致）。
func seedEntries(t *testing.T, env *testEnv, dataset string) (string, []index.Entry) {
	t.Helper()
	entries := []index.Entry{
		env.entry(t, "data/a.tar", 0, 10, dataset+"-a0"),
		env.entry(t, "data/a.tar", 1, 11, dataset+"-a1"),
		env.entry(t, "data/b.tar", 0, 20, dataset+"-b0"),
		env.entry(t, "logs/c.tar", 0, 30, dataset+"-c0"),
		env.entry(t, "logs/d.tar", 0, 40, dataset+"-d0"),
	}
	snapID := env.commit(t, dataset, index.Config{MaxEntriesPerShard: 2}, entries)
	return snapID, entries
}

// assertResult 断言退出码与 stdout，并要求 stderr 为空。
func assertResult(t *testing.T, code int, stdout, stderr string, wantCode int, wantStdout string) {
	t.Helper()
	if code != wantCode {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", code, wantCode, stderr)
	}
	if stdout != wantStdout {
		t.Fatalf("stdout = %q, want %q", stdout, wantStdout)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

// assertRuntimeError 断言运行期错误：退出码 1、stdout 空、stderr 带程序名前缀与关键片段。
func assertRuntimeError(t *testing.T, code int, stdout, stderr, wantFragment string) {
	t.Helper()
	if code != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", code, exitRuntimeError, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
	if !strings.HasPrefix(stderr, "traincacheds: ") {
		t.Fatalf("stderr = %q, want %q 前缀", stderr, "traincacheds: ")
	}
	if !strings.Contains(stderr, wantFragment) {
		t.Fatalf("stderr = %q, want 含 %q", stderr, wantFragment)
	}
}

func TestLSDatasetsListsCurrentBackedNames(t *testing.T) {
	env := newTestEnv(t)
	env.commit(t, "beta", index.Config{}, []index.Entry{env.entry(t, "x.tar", 0, 1, "beta-x")})
	env.commit(t, "alpha", index.Config{}, []index.Entry{env.entry(t, "y.tar", 0, 1, "alpha-y")})

	code, stdout, stderr := runWith(env.store, "ls", "datasets")
	assertResult(t, code, stdout, stderr, exitOK, wantLines("alpha", "beta"))
}

func TestLSDatasetsIgnoresNonCurrentKeys(t *testing.T) {
	env := newTestEnv(t)
	snapID, _ := seedEntries(t, env, "keepme")

	// 杂项键：只有快照对象没有 CURRENT 的数据集、空数据集名、多层嵌套 CURRENT、
	// 前缀相邻但不同层的键、CURRENT 名字被拉长的键。keepme 自己的 manifest 与
	// 分片对象也必须被忽略（只认形状恰为 dataset/<ds>/CURRENT 的键）。
	env.put(t, "dataset/orphan/snapshots/s-1/manifest.json", `{"snapshot_id":"s-1"}`)
	env.put(t, "dataset//CURRENT", snapID)
	env.put(t, "dataset/nested/inner/CURRENT", snapID)
	env.put(t, "datasets/other/CURRENT", snapID)
	env.put(t, "dataset/keepme/CURRENTX", snapID)
	env.put(t, "other/dataset/keepme/CURRENT", snapID)

	code, stdout, stderr := runWith(env.store, "ls", "datasets")
	assertResult(t, code, stdout, stderr, exitOK, wantLines("keepme"))
}

func TestLSDatasetsEmptyStore(t *testing.T) {
	env := newTestEnv(t)
	code, stdout, stderr := runWith(env.store, "ls", "datasets")
	assertResult(t, code, stdout, stderr, exitOK, "")
}

func TestDatasetNameFromCurrentKey(t *testing.T) {
	cases := []struct {
		key  string
		want string
		ok   bool
	}{
		{"dataset/a/CURRENT", "a", true},
		{"dataset/dir.sub-1/CURRENT", "dir.sub-1", true},
		{"dataset//CURRENT", "", false},
		{"dataset/a/b/CURRENT", "", false},
		{"dataset/a/snapshots/s-1/manifest.json", "", false},
		{"dataset/a/CURRENTX", "", false},
		{"datasets/a/CURRENT", "", false},
		{"other/a/CURRENT", "", false},
	}
	for _, tc := range cases {
		got, ok := datasetNameFromCurrentKey(tc.key)
		if got != tc.want || ok != tc.ok {
			t.Errorf("datasetNameFromCurrentKey(%q) = (%q, %v), want (%q, %v)", tc.key, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSnapshotIDsIgnoresShardsAndDedups(t *testing.T) {
	prefix := snapshotsPrefix("ds")
	keys := []string{
		prefix + "s-2/manifest.json",
		prefix + "s-1/manifest.json",
		prefix + "s-1/shard-aaaaaaaaaaaaaaaa.idx",
		prefix + "s-1/manifest.json", // 重复键（真实 List 不会给，防御性去重）
		prefix + "s-3/other.json",
		prefix + "nested/s-4/manifest.json",
		prefix + "manifest.json",
		"dataset/other/snapshots/s-9/manifest.json",
	}
	got := snapshotIDs(prefix, keys)
	if strings.Join(got, ",") != "s-1,s-2" {
		t.Fatalf("snapshotIDs = %v, want [s-1 s-2]", got)
	}

	// 输出恒按字典序，与输入顺序无关。
	shuffled := []string{prefix + "c/manifest.json", prefix + "a/manifest.json", prefix + "b/manifest.json"}
	if got := strings.Join(snapshotIDs(prefix, shuffled), ","); got != "a,b,c" {
		t.Fatalf("snapshotIDs(shuffled) = %q, want a,b,c", got)
	}
}

func TestLSSnapshotsMarksCurrent(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "snapshotmarks"
	first := env.commit(t, dataset, index.Config{MaxEntriesPerShard: 1}, []index.Entry{
		env.entry(t, "a.tar", 0, 1, "mark-a0"),
		env.entry(t, "b.tar", 0, 2, "mark-b0"),
	})
	second := env.commit(t, dataset, index.Config{MaxEntriesPerShard: 1}, []index.Entry{
		env.entry(t, "c.tar", 0, 3, "mark-c0"),
	})

	ids := []string{first, second}
	sort.Strings(ids)
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == second {
			lines = append(lines, id+" current")
			continue
		}
		lines = append(lines, id)
	}

	code, stdout, stderr := runWith(env.store, "ls", "snapshots", dataset)
	assertResult(t, code, stdout, stderr, exitOK, wantLines(lines...))
}

func TestLSSnapshotsWithoutCurrent(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "nocurrentmarker"
	snapID := env.commit(t, dataset, index.Config{}, []index.Entry{env.entry(t, "a.tar", 0, 1, "nocurrent-a")})
	env.remove(t, index.CurrentKey(dataset))

	code, stdout, stderr := runWith(env.store, "ls", "snapshots", dataset)
	assertResult(t, code, stdout, stderr, exitOK, wantLines(snapID))
}

func TestLSSnapshotsEmptyStore(t *testing.T) {
	env := newTestEnv(t)
	code, stdout, stderr := runWith(env.store, "ls", "snapshots", "ghost")
	assertResult(t, code, stdout, stderr, exitOK, "")
}

func TestLSEntriesFullListingOrder(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "entriesorder"
	_, entries := seedEntries(t, env, dataset)

	code, stdout, stderr := runWith(env.store, "ls", "entries", dataset)
	assertResult(t, code, stdout, stderr, exitOK, wantLines(entryLines(entries)...))
}

func TestLSEntriesPrefixFilter(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "entriesprefix"
	_, entries := seedEntries(t, env, dataset)

	code, stdout, stderr := runWith(env.store, "ls", "entries", dataset, "--prefix", "logs/")
	assertResult(t, code, stdout, stderr, exitOK, wantLines(entryLines(entries[3:])...))

	// 部分前缀同样生效（Catalog.List 的前缀语义）。
	code, stdout, stderr = runWith(env.store, "ls", "entries", dataset, "--prefix", "data/a")
	assertResult(t, code, stdout, stderr, exitOK, wantLines(entryLines(entries[:2])...))

	// 无匹配：零行、退出码 0。
	code, stdout, stderr = runWith(env.store, "ls", "entries", dataset, "--prefix", "missing/")
	assertResult(t, code, stdout, stderr, exitOK, "")
}

func TestLSEntriesLimit(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "entrieslimit"
	_, entries := seedEntries(t, env, dataset)

	// --limit 0 = 不截断；n>0 只保留已排序结果前 n 条；n 超过条数不截断。
	code, stdout, stderr := runWith(env.store, "ls", "entries", dataset, "--limit", "2")
	assertResult(t, code, stdout, stderr, exitOK, wantLines(entryLines(entries[:2])...))

	code, stdout, stderr = runWith(env.store, "ls", "entries", dataset, "--limit", "0")
	assertResult(t, code, stdout, stderr, exitOK, wantLines(entryLines(entries)...))

	code, stdout, stderr = runWith(env.store, "ls", "entries", dataset, "--limit", "99")
	assertResult(t, code, stdout, stderr, exitOK, wantLines(entryLines(entries)...))

	// 与 --prefix 叠加：先按前缀过滤再截断。
	code, stdout, stderr = runWith(env.store, "ls", "entries", dataset, "--prefix", "data/", "--limit", "1")
	assertResult(t, code, stdout, stderr, exitOK, wantLines(entryLines(entries[:1])...))
}

func TestLSEntriesNoCurrent(t *testing.T) {
	env := newTestEnv(t)
	code, stdout, stderr := runWith(env.store, "ls", "entries", "neverpublished")
	assertRuntimeError(t, code, stdout, stderr, "has no CURRENT")
	if !strings.Contains(stderr, "neverpublished") {
		t.Fatalf("stderr = %q, want 含数据集名", stderr)
	}
}

func TestStatDatasetFromManifest(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "statmanifest"
	snapID, entries := seedEntries(t, env, dataset)

	// manifest 口径：条目数 = Σ ShardRef.Entries = 5（夹具 5 条 / 3 分片）。
	// 不带 --path 时不输出条目级的 paths/bytes，也不下载任何索引分片。
	want := fmt.Sprintf("dataset=%s snapshot=%s shards=3 entries=%d\n", dataset, snapID, len(entries))
	counter := &countingStore{Store: env.store}
	code, stdout, stderr := runWith(counter, "stat", dataset)
	assertResult(t, code, stdout, stderr, exitOK, want)
	// 只 GET CURRENT 与 manifest.json 两次，分片对象零下载。
	if counter.gets != 2 {
		t.Fatalf("stat 的 GET 次数 = %d, want 2（CURRENT + manifest）", counter.gets)
	}
	for _, key := range counter.gotKeys {
		if strings.HasSuffix(key, ".idx") {
			t.Fatalf("stat 不该下载索引分片，却 GET 了 %s", key)
		}
	}
}

func TestStatWithPath(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "statpath"
	snapID, entries := seedEntries(t, env, dataset)

	// 精确路径：两条 chunk；shards 仍是当前 manifest 的分片数。
	code, stdout, stderr := runWith(env.store, "stat", dataset, "--path", "data/a.tar")
	assertResult(t, code, stdout, stderr, exitOK, fmt.Sprintf(
		"dataset=%s snapshot=%s shards=3 entries=2 paths=1 bytes=21\n", dataset, snapID))

	// 前缀语义：logs/ 命中两条、两个路径、30+40 字节。
	code, stdout, stderr = runWith(env.store, "stat", dataset, "--path", "logs/")
	assertResult(t, code, stdout, stderr, exitOK, fmt.Sprintf(
		"dataset=%s snapshot=%s shards=3 entries=2 paths=2 bytes=70\n", dataset, snapID))

	// 零命中：entries/paths/bytes 全 0。
	code, stdout, stderr = runWith(env.store, "stat", dataset, "--path", "missing.tar")
	assertResult(t, code, stdout, stderr, exitOK, fmt.Sprintf(
		"dataset=%s snapshot=%s shards=3 entries=0 paths=0 bytes=0\n", dataset, snapID))

	if len(entries) != 5 {
		t.Fatalf("夹具条目数 = %d, want 5", len(entries))
	}
}

func TestStatNoCurrent(t *testing.T) {
	env := newTestEnv(t)
	for _, args := range [][]string{{"stat", "ghost"}, {"stat", "ghost", "--path", "a.tar"}} {
		code, stdout, stderr := runWith(env.store, args...)
		assertRuntimeError(t, code, stdout, stderr, "has no CURRENT")
	}
}

func TestStatAndLSSnapshotsMalformedObjects(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "malformed"

	// CURRENT 段数不符（缺 manifestFP8）。
	env.put(t, index.CurrentKey(dataset), "only-one-field")
	for _, args := range [][]string{
		{"stat", dataset},
		{"stat", dataset, "--path", "a.tar"},
		{"ls", "snapshots", dataset},
	} {
		code, stdout, stderr := runWith(env.store, args...)
		assertRuntimeError(t, code, stdout, stderr, "malformed CURRENT")
	}

	// CURRENT 合法但 manifest.json 缺失。
	env.put(t, index.CurrentKey(dataset), "s-9 0123456789abcdef")
	code, stdout, stderr := runWith(env.store, "stat", dataset)
	assertRuntimeError(t, code, stdout, stderr, "not found")

	// manifest 可读但 snapshot_id 与 CURRENT 不符。
	env.put(t, index.ManifestKey(dataset, "s-9"), `{"snapshot_id":"other-id","shards":[]}`)
	code, stdout, stderr = runWith(env.store, "stat", dataset)
	assertRuntimeError(t, code, stdout, stderr, "does not match CURRENT")

	// manifest 不是合法 JSON。
	env.put(t, index.ManifestKey(dataset, "s-9"), `not json`)
	code, stdout, stderr = runWith(env.store, "stat", dataset)
	assertRuntimeError(t, code, stdout, stderr, "malformed manifest")
}

func TestUsageErrors(t *testing.T) {
	env := newTestEnv(t)

	cases := []struct {
		name string
		args []string
	}{
		{"no args", nil},
		{"unknown command", []string{"bogus"}},
		{"ls without kind", []string{"ls"}},
		{"unknown ls kind", []string{"ls", "tables"}},
		{"ls datasets with extra arg", []string{"ls", "datasets", "extra"}},
		{"ls snapshots missing ds", []string{"ls", "snapshots"}},
		{"ls snapshots extra arg", []string{"ls", "snapshots", "ds", "extra"}},
		{"ls entries missing ds", []string{"ls", "entries"}},
		{"ls entries extra arg", []string{"ls", "entries", "ds", "extra"}},
		{"stat missing ds", []string{"stat"}},
		{"stat extra arg", []string{"stat", "ds", "extra"}},
		{"negative limit", []string{"ls", "entries", "ds", "--limit", "-1"}},
		{"non numeric limit", []string{"ls", "entries", "ds", "--limit", "abc"}},
		{"prefix on ls datasets", []string{"ls", "datasets", "--prefix", "p/"}},
		{"prefix on stat", []string{"stat", "ds", "--prefix", "p/"}},
		{"limit on ls snapshots", []string{"ls", "snapshots", "ds", "--limit", "1"}},
		{"path on ls entries", []string{"ls", "entries", "ds", "--path", "a.tar"}},
		{"unknown flag", []string{"ls", "entries", "ds", "--no-such-flag"}},
		{"missing flag value", []string{"ls", "entries", "ds", "--prefix"}},
		{"missing global flag value", []string{"--endpoint"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &capture{}
			var stdout, stderr bytes.Buffer
			if got := run(tc.args, &stdout, &stderr, mockDeps(env.store, c)); got != exitUsageError {
				t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitUsageError, stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: traincacheds") {
				t.Fatalf("stderr missing usage: %q", stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if c.storeCalls != 0 {
				t.Fatalf("usage error constructed object store %d times, want 0", c.storeCalls)
			}
		})
	}
}

func TestRmAndGcRejected(t *testing.T) {
	env := newTestEnv(t)
	for _, sub := range []string{"rm", "gc"} {
		c := &capture{}
		var stdout, stderr bytes.Buffer
		if got := run([]string{sub, "ds"}, &stdout, &stderr, mockDeps(env.store, c)); got != exitUsageError {
			t.Fatalf("%s: exit code = %d, want %d (stderr=%q)", sub, got, exitUsageError, stderr.String())
		}
		if !strings.Contains(stderr.String(), "not implemented yet") {
			t.Fatalf("%s: stderr = %q, want not implemented yet", sub, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("%s: stdout = %q, want empty", sub, stdout.String())
		}
		if c.storeCalls != 0 {
			t.Fatalf("%s: object store constructed %d times, want 0", sub, c.storeCalls)
		}
	}
}

func TestHelpReturnsOK(t *testing.T) {
	env := newTestEnv(t)
	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"-h"}, &stdout, &stderr, mockDeps(env.store, c)); got != exitOK {
		t.Fatalf("exit code = %d, want %d", got, exitOK)
	}
	if !strings.Contains(stderr.String(), "usage: traincacheds") {
		t.Fatalf("stderr = %q, want usage", stderr.String())
	}
	if c.storeCalls != 0 {
		t.Fatalf("help constructed object store %d times, want 0", c.storeCalls)
	}
}

func TestObjstoreDefaults(t *testing.T) {
	for _, key := range []string{envObjstoreEndpoint, envObjstoreAccessKey, envObjstoreSecretKey, envObjstoreBucket, envObjstoreSecure} {
		t.Setenv(key, "")
	}
	env := newTestEnv(t)
	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"ls", "datasets"}, &stdout, &stderr, mockDeps(env.store, c)); got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	want := storeArgs{endpoint: defaultObjstoreEndpoint, bucket: defaultObjstoreBucket}
	if c.storeArgs != want {
		t.Fatalf("store args = %+v, want %+v", c.storeArgs, want)
	}
}

func TestObjstoreEnvFallback(t *testing.T) {
	t.Setenv(envObjstoreEndpoint, "env.example:9000")
	t.Setenv(envObjstoreAccessKey, "env-ak")
	t.Setenv(envObjstoreSecretKey, "env-sk")
	t.Setenv(envObjstoreBucket, "env-bucket")
	t.Setenv(envObjstoreSecure, "yes")

	env := newTestEnv(t)
	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"ls", "datasets"}, &stdout, &stderr, mockDeps(env.store, c)); got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	want := storeArgs{
		endpoint:  "env.example:9000",
		accessKey: "env-ak",
		secretKey: "env-sk",
		bucket:    "env-bucket",
		secure:    true,
	}
	if c.storeArgs != want {
		t.Fatalf("store args = %+v, want %+v", c.storeArgs, want)
	}
}

func TestObjstoreFlagsOverrideEnv(t *testing.T) {
	t.Setenv(envObjstoreEndpoint, "env.example:9000")
	t.Setenv(envObjstoreBucket, "env-bucket")
	t.Setenv(envObjstoreSecure, "1")
	t.Setenv(envObjstoreAccessKey, "env-ak")
	t.Setenv(envObjstoreSecretKey, "env-sk")

	env := newTestEnv(t)
	snapID := env.commit(t, "override", index.Config{}, []index.Entry{env.entry(t, "a.tar", 0, 1, "override-a")})

	c := &capture{}
	var stdout, stderr bytes.Buffer
	got := run([]string{
		"--endpoint", "flag.example:1234",
		"--bucket", "flag-bucket",
		"--secure=false",
		"ls", "snapshots", "override",
	}, &stdout, &stderr, mockDeps(env.store, c))
	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	// 显式 flag 覆盖环境变量；未显式指定的 access-key/secret-key 仍走环境变量。
	want := storeArgs{
		endpoint:  "flag.example:1234",
		accessKey: "env-ak",
		secretKey: "env-sk",
		bucket:    "flag-bucket",
	}
	if c.storeArgs != want {
		t.Fatalf("store args = %+v, want %+v", c.storeArgs, want)
	}
	if stdout.String() != wantLines(snapID+" current") {
		t.Fatalf("stdout = %q, want %q", stdout.String(), wantLines(snapID+" current"))
	}
}

func TestStoreCreationErrorIsRuntime(t *testing.T) {
	c := &capture{storeErr: errors.New("boom")}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"ls", "datasets"}, &stdout, &stderr, mockDeps(objectstore.NewMock(), c)); got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "create object store: boom") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

// failingStore 包装 Mock，按注入开关让 List 或 Get 固定失败。
type failingStore struct {
	*objectstore.Mock
	listErr error
	getErr  error
}

func (s *failingStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.Mock.List(ctx, prefix)
}

func (s *failingStore) Get(ctx context.Context, key string, w io.Writer) error {
	if s.getErr != nil {
		return s.getErr
	}
	return s.Mock.Get(ctx, key, w)
}

func TestStoreErrorsAreRuntime(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "storefail"
	env.commit(t, dataset, index.Config{}, []index.Entry{env.entry(t, "a.tar", 0, 1, "storefail-a")})

	injected := errors.New("injected store failure")
	cases := []struct {
		name  string
		store objectstore.Store
		args  []string
		want  string
	}{
		{"ls datasets list error", &failingStore{Mock: env.store, listErr: injected}, []string{"ls", "datasets"}, "list dataset/"},
		{"ls snapshots list error", &failingStore{Mock: env.store, listErr: injected}, []string{"ls", "snapshots", dataset}, "list dataset/storefail/snapshots/"},
		{"stat get error", &failingStore{Mock: env.store, getErr: injected}, []string{"stat", dataset}, "get dataset/storefail/CURRENT"},
		{"ls entries get error", &failingStore{Mock: env.store, getErr: injected}, []string{"ls", "entries", dataset}, "list entries of dataset storefail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runWith(tc.store, tc.args...)
			assertRuntimeError(t, code, stdout, stderr, tc.want)
			if !strings.Contains(stderr, injected.Error()) {
				t.Fatalf("stderr = %q, want 含注入错误 %q", stderr, injected.Error())
			}
		})
	}
}

// countingStore 统计读写调用次数与读过的对象键，用于断言只读命令面零改动对象存储、
// 以及 stat 不下载索引分片。
type countingStore struct {
	objectstore.Store
	puts    int
	deletes int
	gets    int
	gotKeys []string
}

func (s *countingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.gets++
	s.gotKeys = append(s.gotKeys, key)
	return s.Store.Get(ctx, key, w)
}

func (s *countingStore) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	s.puts++
	return s.Store.Put(ctx, key, r, size)
}

func (s *countingStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	s.puts++
	return s.Store.PutIfMatch(ctx, key, r, size, expectedETag)
}

func (s *countingStore) Delete(ctx context.Context, key string) error {
	s.deletes++
	return s.Store.Delete(ctx, key)
}

func TestReadCommandsNeverMutateStore(t *testing.T) {
	env := newTestEnv(t)
	const dataset = "readonly"
	snapID, _ := seedEntries(t, env, dataset)

	commands := [][]string{
		{"ls", "datasets"},
		{"ls", "snapshots", dataset},
		{"ls", "entries", dataset},
		{"ls", "entries", dataset, "--prefix", "data/", "--limit", "1"},
		{"stat", dataset},
		{"stat", dataset, "--path", "data/a.tar"},
	}

	before := storeContents(t, env.store)
	wrapped := &countingStore{Store: env.store}
	for _, args := range commands {
		code, stdout, stderr := runWith(wrapped, args...)
		if code != exitOK {
			t.Fatalf("%v: exit code = %d, want %d (stderr=%q)", args, code, exitOK, stderr)
		}
		if stdout == "" {
			t.Fatalf("%v: stdout 为空，夹具未生效（stderr=%q）", args, stderr)
		}
		if stderr != "" {
			t.Fatalf("%v: stderr = %q, want empty", args, stderr)
		}
	}
	if wrapped.puts != 0 || wrapped.deletes != 0 {
		t.Fatalf("只读命令发出写请求: puts=%d deletes=%d, want 0/0", wrapped.puts, wrapped.deletes)
	}
	if after := storeContents(t, env.store); after != before {
		t.Fatalf("跑完只读命令后索引对象发生变化:\n before=%s\n after=%s", before, after)
	}
	if !strings.Contains(before, snapID) {
		t.Fatalf("夹具对象缺失 snapID %s: %s", snapID, before)
	}
}

// storeContents 返回 dataset/ 前缀下的键集合与逐键内容摘要（写操作自检用）。
func storeContents(t *testing.T, store *objectstore.Mock) string {
	t.Helper()
	ctx := context.Background()
	keys, err := store.List(ctx, "dataset/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		raw, err := getObject(ctx, store, key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		lines = append(lines, key+"="+string(raw))
	}
	return strings.Join(lines, "\n")
}

func TestSplitArgs(t *testing.T) {
	cases := []struct {
		name         string
		args         []string
		wantFlags    []string
		wantPosition []string
		wantErr      bool
	}{
		{"command only", []string{"ls", "datasets"}, nil, []string{"ls", "datasets"}, false},
		{"flag before command", []string{"--bucket", "b", "stat", "ds"}, []string{"--bucket=b"}, []string{"stat", "ds"}, false},
		{"flag after positional", []string{"ls", "entries", "ds", "--prefix", "p/"}, []string{"--prefix=p/"}, []string{"ls", "entries", "ds"}, false},
		{"equals form", []string{"ls", "entries", "ds", "--limit=3"}, []string{"--limit=3"}, []string{"ls", "entries", "ds"}, false},
		{"single dash form", []string{"-limit", "3", "ls", "entries", "ds"}, []string{"-limit=3"}, []string{"ls", "entries", "ds"}, false},
		{"bool flag", []string{"--secure", "ls", "datasets"}, []string{"--secure"}, []string{"ls", "datasets"}, false},
		{"dash terminator", []string{"ls", "entries", "--", "weird-name"}, nil, []string{"ls", "entries", "weird-name"}, false},
		{"unknown flag passes through", []string{"--nope", "ls"}, []string{"--nope"}, []string{"ls"}, false},
		{"missing value", []string{"ls", "entries", "ds", "--prefix"}, nil, nil, true},
		{"value is next flag", []string{"--limit", "--secure"}, []string{"--limit=--secure"}, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flagArgs, positional, err := splitArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("splitArgs(%v) err = nil, want 非空", tc.args)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitArgs(%v): %v", tc.args, err)
			}
			if strings.Join(flagArgs, " ") != strings.Join(tc.wantFlags, " ") {
				t.Fatalf("flagArgs = %v, want %v", flagArgs, tc.wantFlags)
			}
			if strings.Join(positional, " ") != strings.Join(tc.wantPosition, " ") {
				t.Fatalf("positional = %v, want %v", positional, tc.wantPosition)
			}
		})
	}
}

// entryLines 把条目序列渲染成期望行（保持 List 的 (path, chunk) 定序）。
func entryLines(entries []index.Entry) []string {
	lines := make([]string, len(entries))
	for i, e := range entries {
		lines[i] = entryLine(e)
	}
	return lines
}
