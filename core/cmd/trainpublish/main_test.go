package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
	"traincache/core/publish"
)

// capturedStoreArgs 记录 fake newStore 收到的参数。
type capturedStoreArgs struct {
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
	secure    bool
}

// capture 记录 fake 依赖的调用与入参，便于断言。
type capture struct {
	storeCalls int
	storeArgs  capturedStoreArgs
	storeErr   error

	casCalls int
	casDir   string
	casErr   error

	writerCalls  int
	probeCalls   int
	probeErr     error
	publishCalls int
	sources      []publish.Source
	sourceData   []string
	readErr      error
	options      publish.Options
	result       publish.Result
	publishErr   error
}

// fakeDeps 构造全部依赖均可观察、且完全不访问真实对象存储的 deps。
func fakeDeps(c *capture) deps {
	return deps{
		newStore: func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error) {
			c.storeCalls++
			c.storeArgs = capturedStoreArgs{
				endpoint:  endpoint,
				accessKey: accessKey,
				secretKey: secretKey,
				bucket:    bucket,
				secure:    secure,
			}
			if c.storeErr != nil {
				return nil, c.storeErr
			}
			return nil, nil
		},
		newCAS: func(dir string) (*cas.Store, error) {
			c.casCalls++
			c.casDir = dir
			if c.casErr != nil {
				return nil, c.casErr
			}
			return cas.New(dir, cas.VerifyXXH3)
		},
		newWriter: func(store objectstore.Store, cs *cas.Store) *index.Writer {
			c.writerCalls++
			return index.NewWriter(store, cs, index.Config{})
		},
		probe: func(_ context.Context, _ objectstore.Store) error {
			c.probeCalls++
			return c.probeErr
		},
		publishFn: func(_ context.Context, _ objectstore.Store, _ *cas.Store, _ *index.Writer, sources []publish.Source, opt publish.Options) (publish.Result, error) {
			c.publishCalls++
			c.sources = append([]publish.Source(nil), sources...)
			c.options = opt
			for _, src := range sources {
				buf := make([]byte, int(src.Size))
				if src.Size > 0 {
					if _, err := src.Reader.ReadAt(buf, 0); err != nil {
						c.readErr = err
						break
					}
				}
				c.sourceData = append(c.sourceData, string(buf))
			}
			return c.result, c.publishErr
		},
	}
}

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestRunUsageErrors(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("x"))

	cases := []struct {
		name string
		args []string
	}{
		{"missing dataset", []string{tarPath}},
		{"no tars", []string{"--dataset", "ds"}},
		{"zero chunk size", []string{"--dataset", "ds", "--chunk-size", "0", tarPath}},
		{"negative chunk size", []string{"--dataset", "ds", "--chunk-size", "-1", tarPath}},
		{"zero concurrency", []string{"--dataset", "ds", "--concurrency", "0", tarPath}},
		{"negative concurrency", []string{"--dataset", "ds", "--concurrency", "-1", tarPath}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &capture{}
			var stdout, stderr bytes.Buffer
			if got := run(tc.args, &stdout, &stderr, fakeDeps(c)); got != exitUsageError {
				t.Fatalf("exit code = %d, want %d", got, exitUsageError)
			}
			if !strings.Contains(stderr.String(), "usage: trainpublish") {
				t.Fatalf("stderr missing usage: %q", stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if c.storeCalls != 0 || c.casCalls != 0 || c.publishCalls != 0 {
				t.Fatalf("dependencies called on usage error: %+v", c)
			}
		})
	}
}

func TestRunUnknownFlag(t *testing.T) {
	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--no-such-flag"}, &stdout, &stderr, fakeDeps(c)); got != exitUsageError {
		t.Fatalf("exit code = %d, want %d", got, exitUsageError)
	}
	if !strings.Contains(stderr.String(), "usage: trainpublish") {
		t.Fatalf("stderr missing usage: %q", stderr.String())
	}
}

func TestRunHelp(t *testing.T) {
	c := &capture{}
	var stdout, stderr bytes.Buffer
	if got := run([]string{"-h"}, &stdout, &stderr, fakeDeps(c)); got != exitOK {
		t.Fatalf("exit code = %d, want %d", got, exitOK)
	}
	if !strings.Contains(stderr.String(), "usage: trainpublish") {
		t.Fatalf("stderr missing usage: %q", stderr.String())
	}
}

func TestRunSuccess(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-1", Entries: 3, Bytes: 5, UniqueBlocks: 2}}
	var stdout, stderr bytes.Buffer
	got := run([]string{
		"--dataset", "ds",
		"--chunk-size", "123",
		"--concurrency", "7",
		tarPath,
	}, &stdout, &stderr, fakeDeps(c))

	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if want := "snapID=snap-1 entries=3 bytes=5 unique_blocks=2\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	if c.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", c.publishCalls)
	}
	if c.options.Dataset != "ds" || c.options.ChunkSize != 123 || c.options.Concurrency != 7 {
		t.Fatalf("options = %+v", c.options)
	}
	if len(c.sources) != 1 || c.sources[0].Path != "a.tar" || c.sources[0].Size != 5 {
		t.Fatalf("sources = %+v", c.sources)
	}
	if c.readErr != nil {
		t.Fatalf("read source: %v", c.readErr)
	}
	if len(c.sourceData) != 1 || c.sourceData[0] != "hello" {
		t.Fatalf("source data = %q", c.sourceData)
	}
	if c.casDir == "" {
		t.Fatal("cas dir not created")
	}
	if _, err := os.Stat(c.casDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("work dir %s not removed (err=%v)", c.casDir, err)
	}
}

func TestRunMultipleTarsDuplicateBaseNames(t *testing.T) {
	root := t.TempDir()
	dir1 := filepath.Join(root, "one")
	dir2 := filepath.Join(root, "two")
	if err := os.MkdirAll(dir1, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir2, 0o755); err != nil {
		t.Fatal(err)
	}
	path1 := filepath.Join(dir1, "x.tar")
	path2 := filepath.Join(dir2, "x.tar")
	mustWriteFile(t, path1, []byte("first"))
	mustWriteFile(t, path2, []byte("second"))

	// 重复 base 名由 publish.Publish 判定；CLI 仅原样传参，不去重、不改名。
	c := &capture{publishErr: publish.ErrDuplicatePath}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", path1, path2}, &stdout, &stderr, fakeDeps(c))

	if got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "duplicate path") {
		t.Fatalf("stderr = %q, want duplicate path", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if len(c.sources) != 2 {
		t.Fatalf("sources = %d, want 2", len(c.sources))
	}
	if c.sources[0].Path != "x.tar" || c.sources[1].Path != "x.tar" {
		t.Fatalf("source paths = %q, %q; want both x.tar", c.sources[0].Path, c.sources[1].Path)
	}
	if len(c.sourceData) != 2 || c.sourceData[0] != "first" || c.sourceData[1] != "second" {
		t.Fatalf("source data = %q", c.sourceData)
	}
	if c.casDir == "" {
		t.Fatal("cas dir not created")
	}
	if _, err := os.Stat(c.casDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("work dir %s not removed after failure (err=%v)", c.casDir, err)
	}
}

func TestRunWorkDirUnderUserParent(t *testing.T) {
	parent := t.TempDir()
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-2"}}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", "--work-dir", parent, tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if c.casDir == "" {
		t.Fatal("cas dir not created")
	}
	if filepath.Dir(c.casDir) != parent {
		t.Fatalf("cas dir %s is not directly under %s", c.casDir, parent)
	}
	if c.casDir == parent {
		t.Fatalf("cas dir must be a unique subdir of %s, not the parent itself", parent)
	}
	if _, err := os.Stat(c.casDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("work dir %s not removed (err=%v)", c.casDir, err)
	}
	if _, err := os.Stat(parent); err != nil {
		t.Fatalf("user work dir parent %s removed: %v", parent, err)
	}
}

func TestRunKeepWorkDir(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-3"}}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", "--keep-work-dir", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if !strings.Contains(stderr.String(), "keeping work dir") {
		t.Fatalf("stderr = %q, want keeping work dir notice", stderr.String())
	}
	if c.casDir == "" {
		t.Fatal("cas dir not created")
	}
	t.Cleanup(func() { _ = os.RemoveAll(c.casDir) })
	if _, err := os.Stat(c.casDir); err != nil {
		t.Fatalf("work dir %s should be kept: %v", c.casDir, err)
	}
}

func TestRunProbeUnsupported(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{probeErr: fmt.Errorf("probe: %w", objectstore.ErrConditionalWriteUnsupported)}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", "--probe-conditional-write", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "conditional write unsupported") {
		t.Fatalf("stderr = %q, want conditional write unsupported", stderr.String())
	}
	if c.publishCalls != 0 {
		t.Fatalf("publish called %d times, want 0", c.publishCalls)
	}
	if c.casCalls != 0 {
		t.Fatalf("cas created %d times, want 0", c.casCalls)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestRunProbeFromEnv(t *testing.T) {
	t.Setenv(envObjstoreProbe, "yes")
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-4"}}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if c.probeCalls != 1 {
		t.Fatalf("probe calls = %d, want 1", c.probeCalls)
	}
}

func TestRunProbeExplicitFalseOverridesEnv(t *testing.T) {
	t.Setenv(envObjstoreProbe, "1")
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-5"}}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", "--probe-conditional-write=false", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if c.probeCalls != 0 {
		t.Fatalf("probe calls = %d, want 0", c.probeCalls)
	}
}

func TestRunObjstoreEnvFallback(t *testing.T) {
	t.Setenv(envObjstoreEndpoint, "env.example:9000")
	t.Setenv(envObjstoreAccessKey, "env-ak")
	t.Setenv(envObjstoreSecretKey, "env-sk")
	t.Setenv(envObjstoreBucket, "env-bucket")
	t.Setenv(envObjstoreSecure, "yes")

	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-6"}}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	want := capturedStoreArgs{
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

func TestRunExplicitFlagsOverrideEnv(t *testing.T) {
	t.Setenv(envObjstoreEndpoint, "env.example:9000")
	t.Setenv(envObjstoreSecure, "1")

	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{result: publish.Result{SnapID: "snap-7"}}
	var stdout, stderr bytes.Buffer
	got := run([]string{
		"--dataset", "ds",
		"--endpoint", "flag.example:1234",
		"--secure=false",
		tarPath,
	}, &stdout, &stderr, fakeDeps(c))

	if got != exitOK {
		t.Fatalf("exit code = %d, want %d (stderr=%q)", got, exitOK, stderr.String())
	}
	if c.storeArgs.endpoint != "flag.example:1234" {
		t.Fatalf("endpoint = %q, want flag.example:1234", c.storeArgs.endpoint)
	}
	if c.storeArgs.secure {
		t.Fatal("secure = true, want explicit false to override env")
	}
}

func TestRunOpenError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.tar")

	c := &capture{}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", missing}, &stdout, &stderr, fakeDeps(c))

	if got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "open") {
		t.Fatalf("stderr = %q, want open error", stderr.String())
	}
	if c.storeCalls != 0 || c.casCalls != 0 || c.publishCalls != 0 {
		t.Fatalf("dependencies called after open error: %+v", c)
	}
}

func TestRunStoreError(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{storeErr: errors.New("boom")}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "create object store: boom") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if c.casCalls != 0 || c.publishCalls != 0 {
		t.Fatalf("dependencies called after store error: %+v", c)
	}
}

func TestRunCASError(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{casErr: errors.New("cas boom")}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "create cas: cas boom") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if c.publishCalls != 0 {
		t.Fatalf("publish called %d times, want 0", c.publishCalls)
	}
}

func TestRunPublishError(t *testing.T) {
	tarPath := filepath.Join(t.TempDir(), "a.tar")
	mustWriteFile(t, tarPath, []byte("hello"))

	c := &capture{publishErr: errors.New("publish boom")}
	var stdout, stderr bytes.Buffer
	got := run([]string{"--dataset", "ds", tarPath}, &stdout, &stderr, fakeDeps(c))

	if got != exitRuntimeError {
		t.Fatalf("exit code = %d, want %d", got, exitRuntimeError)
	}
	if !strings.Contains(stderr.String(), "publish boom") {
		t.Fatalf("stderr = %q, want publish boom", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if c.casDir == "" {
		t.Fatal("cas dir not created")
	}
	if _, err := os.Stat(c.casDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("work dir %s not removed after publish error (err=%v)", c.casDir, err)
	}
}
