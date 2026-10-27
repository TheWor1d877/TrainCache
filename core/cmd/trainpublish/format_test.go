package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"traincache/core/objectstore"
	"traincache/core/publish"
)

// mustReadFormat 返回桶里格式声明的解析结果。
func mustReadFormat(t *testing.T, store objectstore.Store) bucketFormat {
	t.Helper()
	f, err := readBucketFormat(context.Background(), store)
	if err != nil {
		t.Fatalf("readBucketFormat: %v", err)
	}
	return f
}

// seedFormat 用生产代码写入一条桶声明，作为“已有声明”的起点。
func seedFormat(t *testing.T, store objectstore.Store, chunkSize int64) {
	t.Helper()
	if _, err := ensureBucketFormat(context.Background(), store, chunkSize, true); err != nil {
		t.Fatalf("seed format %d: %v", chunkSize, err)
	}
}

// TestEnsureBucketFormatInitializesEmptyBucket 空桶首次发布：写入声明并返回 requested。
func TestEnsureBucketFormatInitializesEmptyBucket(t *testing.T) {
	store := objectstore.NewMock()

	got, err := ensureBucketFormat(context.Background(), store, 4096, true)
	if err != nil {
		t.Fatalf("ensureBucketFormat = %v", err)
	}
	if got != 4096 {
		t.Fatalf("chunkSize = %d, want 4096", got)
	}

	var buf bytes.Buffer
	if err := store.Get(context.Background(), formatKey, &buf); err != nil {
		t.Fatalf("Get(%s): %v", formatKey, err)
	}
	var raw map[string]any
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("declaration is not JSON: %v (%q)", err, buf.String())
	}
	if raw["version"] != float64(formatVersion) || raw["chunkSize"] != float64(4096) {
		t.Fatalf("declaration = %s, want version=1 chunkSize=4096", buf.String())
	}
}

// TestEnsureBucketFormatReusesExistingWithoutExplicit 未显式传 --chunk-size 时沿用已有值。
func TestEnsureBucketFormatReusesExistingWithoutExplicit(t *testing.T) {
	store := objectstore.NewMock()
	seedFormat(t, store, 1024)

	got, err := ensureBucketFormat(context.Background(), store, 64<<20, false)
	if err != nil {
		t.Fatalf("ensureBucketFormat = %v", err)
	}
	if got != 1024 {
		t.Fatalf("chunkSize = %d, want existing 1024", got)
	}
	if f := mustReadFormat(t, store); f.ChunkSize != 1024 {
		t.Fatalf("declaration chunkSize = %d, want 1024", f.ChunkSize)
	}
}

// TestEnsureBucketFormatExplicitMismatch 显式传不同大小：报错并给出两个值与处置提示。
func TestEnsureBucketFormatExplicitMismatch(t *testing.T) {
	store := objectstore.NewMock()
	seedFormat(t, store, 1024)

	got, err := ensureBucketFormat(context.Background(), store, 2048, true)
	if err == nil {
		t.Fatalf("ensureBucketFormat = %d, want error", got)
	}
	for _, want := range []string{"1024", "2048", "改大小需要清空桶或换桶后重新发布"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want substring %q", err, want)
		}
	}
	if f := mustReadFormat(t, store); f.ChunkSize != 1024 {
		t.Fatalf("declaration chunkSize = %d, want unchanged 1024", f.ChunkSize)
	}
}

// TestEnsureBucketFormatExplicitSameValue 显式传与声明相同的值：正常放行。
func TestEnsureBucketFormatExplicitSameValue(t *testing.T) {
	store := objectstore.NewMock()
	seedFormat(t, store, 1024)

	got, err := ensureBucketFormat(context.Background(), store, 1024, true)
	if err != nil {
		t.Fatalf("ensureBucketFormat = %v", err)
	}
	if got != 1024 {
		t.Fatalf("chunkSize = %d, want 1024", got)
	}
}

// conflictStore 在第一次 PutIfMatch 时写入竞争者的声明并返回 ErrConflict。
type conflictStore struct {
	objectstore.Store
	content []byte
	once    sync.Once
}

func (s *conflictStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	conflicted := false
	s.once.Do(func() { conflicted = true })
	if conflicted {
		if _, err := s.Store.Put(ctx, key, bytes.NewReader(s.content), int64(len(s.content))); err != nil {
			return "", err
		}
		return "", objectstore.ErrConflict
	}
	return s.Store.PutIfMatch(ctx, key, r, size, expectedETag)
}

// TestEnsureBucketFormatConflictReusesExisting 创建声明遇到 ErrConflict 时回读已有值。
func TestEnsureBucketFormatConflictReusesExisting(t *testing.T) {
	wrapped := &conflictStore{
		Store:   objectstore.NewMock(),
		content: []byte(`{"version":1,"chunkSize":777}`),
	}

	got, err := ensureBucketFormat(context.Background(), wrapped, 512, false)
	if err != nil {
		t.Fatalf("ensureBucketFormat = %v", err)
	}
	if got != 777 {
		t.Fatalf("chunkSize = %d, want the concurrently written 777", got)
	}
}

// unsupportedStore 模拟忽略条件写的后端。
type unsupportedStore struct {
	objectstore.Store
}

func (s unsupportedStore) PutIfMatch(context.Context, string, io.Reader, int64, string) (string, error) {
	return "", objectstore.ErrConditionalWriteUnsupported
}

// TestEnsureBucketFormatConditionalWriteUnsupported 不支持条件写时明确报错，不静默降级。
func TestEnsureBucketFormatConditionalWriteUnsupported(t *testing.T) {
	store := unsupportedStore{Store: objectstore.NewMock()}

	_, err := ensureBucketFormat(context.Background(), store, 1024, true)
	if !errors.Is(err, objectstore.ErrConditionalWriteUnsupported) {
		t.Fatalf("err = %v, want ErrConditionalWriteUnsupported", err)
	}
	if !strings.Contains(err.Error(), "不支持条件写") {
		t.Fatalf("error = %q, want explicit message", err)
	}
}

// TestEnsureBucketFormatRejectsInvalidDeclaration 声明内容不合法时报错，不覆盖。
func TestEnsureBucketFormatRejectsInvalidDeclaration(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"bad version", `{"version":2,"chunkSize":1024}`},
		{"zero chunk size", `{"version":1,"chunkSize":0}`},
		{"not json", `chunkSize=1024`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := objectstore.NewMock()
			if _, err := store.Put(context.Background(), formatKey, strings.NewReader(tc.body), int64(len(tc.body))); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if _, err := ensureBucketFormat(context.Background(), store, 1024, false); err == nil {
				t.Fatal("ensureBucketFormat succeeded on invalid declaration")
			}
		})
	}
}

// datasetYAML 生成一份最小 DataSet 清单。
func datasetYAML(kind, name, datasetName string, chunkSize int64) string {
	return fmt.Sprintf(`apiVersion: traincache.io/v1alpha1
kind: %s
metadata:
  name: %s
spec:
  datasetName: %s
  chunkSize: %d
`, kind, name, datasetName, chunkSize)
}

// writeDatasetYAML 把清单写到临时文件并返回路径。
func writeDatasetYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dataset.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write dataset yaml: %v", err)
	}
	return path
}

// TestCheckDatasetFileMatch 名字与块大小都一致时通过。
func TestCheckDatasetFileMatch(t *testing.T) {
	path := writeDatasetYAML(t, datasetYAML("DataSet", "coco", "coco", 1048576))
	if err := checkDatasetFile(path, "coco", 1048576); err != nil {
		t.Fatalf("checkDatasetFile = %v", err)
	}
}

// TestCheckDatasetFileMismatch 名字、块大小、kind 或不合法 YAML 都报出可读错误。
func TestCheckDatasetFileMismatch(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantSub string
	}{
		{"dataset name", datasetYAML("DataSet", "coco", "other", 1048576), "spec.datasetName"},
		{"chunk size", datasetYAML("DataSet", "coco", "coco", 4096), "spec.chunkSize"},
		{"kind", datasetYAML("DataSetList", "coco", "coco", 1048576), "kind"},
		{"bad yaml", "kind: [unclosed", "解析"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkDatasetFile(writeDatasetYAML(t, tc.body), "coco", 1048576)
			if err == nil {
				t.Fatal("checkDatasetFile succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantSub)
			}
		})
	}
}

// TestCheckDatasetFileMissing 文件不存在时报错。
func TestCheckDatasetFileMissing(t *testing.T) {
	if err := checkDatasetFile(filepath.Join(t.TempDir(), "nope.yaml"), "coco", 1024); err == nil {
		t.Fatal("checkDatasetFile succeeded, want error")
	}
}

// depsWithStore 用 fakeDeps 的观察点，但把 newStore 换成给定的内存 store。
func depsWithStore(c *capture, store objectstore.Store) deps {
	d := fakeDeps(c)
	d.newStore = func(_, _, _, _ string, _ bool) (objectstore.Store, error) {
		c.storeCalls++
		return store, nil
	}
	return d
}

// TestRunChunkSizeDefaultIsNotExplicit 未传 --chunk-size 时 explicit=false，请求值取默认值。
func TestRunChunkSizeDefaultIsNotExplicit(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-fmt-1"}}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--dataset", "ds", tarPath}, &stdout, &stderr, fakeDeps(c)); got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if c.ensureCalls != 1 {
		t.Fatalf("ensureFormat calls = %d, want 1", c.ensureCalls)
	}
	if c.ensureExplicit {
		t.Fatal("explicit = true, want false without --chunk-size")
	}
	if c.ensureRequested != publish.DefaultChunkSize {
		t.Fatalf("requested = %d, want default %d", c.ensureRequested, publish.DefaultChunkSize)
	}
}

// TestRunUsesEffectiveChunkSize 发布用 ensureFormat 返回的实际块大小。
func TestRunUsesEffectiveChunkSize(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-fmt-2"}, ensureEffective: 512}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", "--chunk-size", "64", tarPath}, &stdout, &stderr, fakeDeps(c))
	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if !c.ensureExplicit || c.ensureRequested != 64 {
		t.Fatalf("ensure(explicit=%v, requested=%d), want (true, 64)", c.ensureExplicit, c.ensureRequested)
	}
	if c.options.ChunkSize != 512 {
		t.Fatalf("publish chunkSize = %d, want effective 512", c.options.ChunkSize)
	}
}

// TestRunEnsureFormatError 声明初始化/校验失败：退出 1，且不发布。
func TestRunEnsureFormatError(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{ensureErr: errors.New("chunk size fixed at 1024")}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--dataset", "ds", tarPath}, &stdout, &stderr, fakeDeps(c)); got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "chunk size fixed at 1024") {
		t.Fatalf("stderr = %q, want ensure error", stderr.String())
	}
	if c.publishCalls != 0 || c.casCalls != 0 {
		t.Fatalf("publish/cas called after ensure error: %+v", c)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

// TestRunCheckDatasetOK 声明与 DataSet 一致：单行 ok，退出 0。
func TestRunCheckDatasetOK(t *testing.T) {
	store := objectstore.NewMock()
	seedFormat(t, store, 1048576)
	path := writeDatasetYAML(t, datasetYAML("DataSet", "coco", "coco", 1048576))

	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--dataset", "coco", "--check-dataset", path}, &stdout, &stderr, depsWithStore(c, store)); got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if want := "ok dataset=coco chunk_size=1048576 dataset_file=" + path + "\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	if c.publishCalls != 0 || c.casCalls != 0 {
		t.Fatalf("check mode touched publish/cas: %+v", c)
	}
}

// TestRunCheckDatasetMismatch DataSet 块大小与声明不符：退出 1，并说明两边的值。
func TestRunCheckDatasetMismatch(t *testing.T) {
	store := objectstore.NewMock()
	seedFormat(t, store, 1048576)
	path := writeDatasetYAML(t, datasetYAML("DataSet", "coco", "coco", 4096))

	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--dataset", "coco", "--check-dataset", path}, &stdout, &stderr, depsWithStore(c, store)); got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	for _, want := range []string{"4096", "1048576"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want substring %q", stderr.String(), want)
		}
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

// TestRunCheckDatasetWithoutDeclaration 桶里还没有声明：只读校验不创建声明。
func TestRunCheckDatasetWithoutDeclaration(t *testing.T) {
	store := objectstore.NewMock()
	path := writeDatasetYAML(t, datasetYAML("DataSet", "coco", "coco", 1048576))

	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--dataset", "coco", "--check-dataset", path}, &stdout, &stderr, depsWithStore(c, store)); got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "先完成一次数据发布") {
		t.Fatalf("stderr = %q, want publish-first hint", stderr.String())
	}
	if _, err := store.GetETag(context.Background(), formatKey); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("check mode created %s (err=%v)", formatKey, err)
	}
}

// TestRunCheckDatasetUsageErrors 只校验模式的参数错误退出 2，且不建 store。
func TestRunCheckDatasetUsageErrors(t *testing.T) {
	store := objectstore.NewMock()
	path := writeDatasetYAML(t, datasetYAML("DataSet", "coco", "coco", 1048576))
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("x"))

	cases := [][]string{
		{"--check-dataset", path},                               // 缺 --dataset
		{"--dataset", "coco", "--check-dataset", path, tarPath}, // 带 tar 位置参数
	}
	for _, args := range cases {
		c := &capture{}
		var stdout, stderr bytes.Buffer
		if got := run(args, &stdout, &stderr, depsWithStore(c, store)); got != exitUsageError {
			t.Fatalf("args %v: exit code = %d, want %d", args, got, exitUsageError)
		}
		if !strings.Contains(stderr.String(), "usage: trainpublish") {
			t.Fatalf("args %v: stderr missing usage: %q", args, stderr.String())
		}
		if c.storeCalls != 0 {
			t.Fatalf("args %v: store created on usage error", args)
		}
	}
}
