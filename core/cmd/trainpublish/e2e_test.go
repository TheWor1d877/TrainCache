package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
	"traincache/core/publish"
)

// e2eEnv 记录 run 期间注入依赖实际创建的 CAS 目录与 Writer。
type e2eEnv struct {
	casDir string
	cs     *cas.Store
	writer *index.Writer
}

// e2eDeps 返回一套端到端依赖：真实 publish.Publish、真实 index.Writer 与临时 CAS，
// 仅对象存储换成给定的内存实现。
func e2eDeps(store objectstore.Store, env *e2eEnv) deps {
	return deps{
		newStore: func(_, _, _, _ string, _ bool) (objectstore.Store, error) {
			return store, nil
		},
		newCAS: func(dir string) (*cas.Store, error) {
			env.casDir = dir
			cs, err := cas.New(dir, cas.VerifyXXH3)
			if err != nil {
				return nil, err
			}
			env.cs = cs
			return cs, nil
		},
		newWriter: func(s objectstore.Store, cs *cas.Store) *index.Writer {
			w := index.NewWriter(s, cs, index.Config{})
			env.writer = w
			return w
		},
		probe:     objectstore.ProbeConditionalWrite,
		publishFn: publish.Publish,
	}
}

// e2eWriteFile 在 dir 下写一个临时 tar 文件并返回路径。
func e2eWriteFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// e2eReadObject 读回对象存储中的一个对象。
func e2eReadObject(t *testing.T, store objectstore.Store, key string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := store.Get(context.Background(), key, &buf); err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	return buf.Bytes()
}

// e2eStdoutRE 匹配 CLI 成功输出的唯一一行。
var e2eStdoutRE = regexp.MustCompile(`^snapID=(\S+) entries=(\d+) bytes=(\d+) unique_blocks=(\d+)\n$`)

// TestRunE2EMultiTarPublish 用真实 publish.Publish + objectstore.Mock + 临时 CAS
// 经 run() 发布两个临时 tar，校验退出码、stdout 格式、CURRENT 与工作目录清理。
func TestRunE2EMultiTarPublish(t *testing.T) {
	const dataset = "e2e-ok"
	parent := t.TempDir()
	dataA := []byte("0123456789")  // ChunkSize=4 → 4+4+2，3 块
	dataB := []byte("abcdefghijk") // 11 字节 → 4+4+3，3 块
	a := e2eWriteFile(t, parent, "a.tar", dataA)
	b := e2eWriteFile(t, parent, "b.tar", dataB)

	mock := objectstore.NewMock()
	env := &e2eEnv{}
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--dataset", dataset,
		"--chunk-size", "4",
		"--concurrency", "3",
		"--work-dir", parent,
		a, b,
	}, &stdout, &stderr, e2eDeps(mock, env))

	if code != exitOK {
		t.Fatalf("exit code = %d，期望 %d（stderr=%q）", code, exitOK, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q，期望空", stderr.String())
	}
	if strings.Count(stdout.String(), "\n") != 1 || !strings.HasSuffix(stdout.String(), "\n") {
		t.Fatalf("stdout 不是单行: %q", stdout.String())
	}
	m := e2eStdoutRE.FindStringSubmatch(stdout.String())
	if m == nil {
		t.Fatalf("stdout 格式不符: %q", stdout.String())
	}
	snapID := m[1]
	if snapID == "" {
		t.Fatal("stdout SnapID 为空")
	}
	if m[2] != "6" || m[3] != "21" || m[4] != "6" {
		t.Fatalf("stdout 统计 = entries=%s bytes=%s unique=%s，期望 6/21/6", m[2], m[3], m[4])
	}

	// CURRENT 指向 CLI 返回的 SnapID。
	fields := strings.Fields(string(e2eReadObject(t, mock, index.CurrentKey(dataset))))
	if len(fields) != 2 || fields[0] != snapID {
		t.Fatalf("CURRENT = %q，期望首段是 %q", fields, snapID)
	}

	// Catalog 能从对象存储读回两个 tar 的每个块。
	catalog := index.NewCatalog(mock, dataset, index.Config{})
	if err := catalog.Refresh(context.Background()); err != nil {
		t.Fatalf("Catalog.Refresh: %v", err)
	}
	if got := catalog.SnapID(); got != snapID {
		t.Fatalf("Catalog.SnapID = %q，期望 %q", got, snapID)
	}
	for _, tc := range []struct {
		path  string
		chunk int64
	}{
		{"a.tar", 0}, {"a.tar", 1}, {"a.tar", 2},
		{"b.tar", 0}, {"b.tar", 1}, {"b.tar", 2},
	} {
		if _, ok, err := catalog.Lookup(context.Background(), tc.path, tc.chunk); err != nil || !ok {
			t.Fatalf("Lookup(%q,%d) = (_, %v, %v)，期望命中", tc.path, tc.chunk, ok, err)
		}
	}

	// 工作目录：由 run 在 parent 下创建的唯一子目录，结束后必须被清理。
	if env.casDir == "" {
		t.Fatal("未创建 CAS 工作目录")
	}
	if filepath.Dir(env.casDir) != parent {
		t.Fatalf("工作目录 %s 不在 %s 下", env.casDir, parent)
	}
	if _, err := os.Stat(env.casDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("工作目录 %s 未清理（err=%v）", env.casDir, err)
	}
}

// e2eFailingStore 包装 Mock，令指定键的 Put 返回注入错误，并统计命中次数。
type e2eFailingStore struct {
	*objectstore.Mock
	failKey string
	err     error

	mu     sync.Mutex
	failed int
}

func (f *e2eFailingStore) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	f.mu.Lock()
	if key == f.failKey {
		f.failed++
		f.mu.Unlock()
		return "", f.err
	}
	f.mu.Unlock()
	return f.Mock.Put(ctx, key, r, size)
}

// TestRunE2EPublishFailureLeavesNoCurrent 让某个块 Put 失败，校验退出码 1、
// stderr 带错误、CURRENT 未写入、无快照对象、工作目录仍被清理。
func TestRunE2EPublishFailureLeavesNoCurrent(t *testing.T) {
	const dataset = "e2e-fail"
	parent := t.TempDir()
	a := e2eWriteFile(t, parent, "a.tar", []byte("0123456789")) // 块 #0/#1/#2
	b := e2eWriteFile(t, parent, "b.tar", []byte("abc"))

	mock := objectstore.NewMock()
	injected := errors.New("e2e injected put failure")
	fs := &e2eFailingStore{
		Mock:    mock,
		failKey: objectstore.BlockKey(dataset, "a.tar", 1),
		err:     injected,
	}
	env := &e2eEnv{}
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--dataset", dataset,
		"--chunk-size", "4",
		"--concurrency", "3",
		"--work-dir", parent,
		a, b,
	}, &stdout, &stderr, e2eDeps(fs, env))

	if code != exitRuntimeError {
		t.Fatalf("exit code = %d，期望 %d（stderr=%q）", code, exitRuntimeError, stderr.String())
	}
	if !strings.Contains(stderr.String(), injected.Error()) {
		t.Fatalf("stderr = %q，期望包含 %q", stderr.String(), injected.Error())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q，期望空", stdout.String())
	}
	fs.mu.Lock()
	failed := fs.failed
	fs.mu.Unlock()
	if failed != 1 {
		t.Fatalf("失败键 Put 次数 = %d，期望 1", failed)
	}

	ctx := context.Background()
	if _, err := mock.GetETag(ctx, index.CurrentKey(dataset)); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("CURRENT GetETag err = %v，期望 ErrNotFound（未提交）", err)
	}
	snapshots, err := mock.List(ctx, "dataset/"+dataset+"/snapshots/")
	if err != nil {
		t.Fatalf("mock.List: %v", err)
	}
	if len(snapshots) != 0 {
		t.Fatalf("失败后出现快照对象: %v", snapshots)
	}

	if env.casDir == "" {
		t.Fatal("未创建 CAS 工作目录")
	}
	if _, err := os.Stat(env.casDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("工作目录 %s 未清理（err=%v）", env.casDir, err)
	}
}
