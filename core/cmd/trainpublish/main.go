// Command trainpublish 是多 tar 发布器的命令行入口。
//
// 它解析对象存储与发布参数，构造 S3 Store、临时 CAS 与 index.Writer，
// 然后把全部 tar 交给 publish.Publish 切块上传并提交一次索引快照。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
	"traincache/core/publish"
)

// 对象存储相关环境变量与默认值，与 agent/svc/config.go 同名同口径：
// flag 显式值优先，未显式指定时以环境变量为默认值。
const (
	envObjstoreEndpoint     = "TRAINCACHE_OBJSTORE_ENDPOINT"
	envObjstoreAccessKey    = "TRAINCACHE_OBJSTORE_ACCESS_KEY"
	envObjstoreSecretKey    = "TRAINCACHE_OBJSTORE_SECRET_KEY"
	envObjstoreBucket       = "TRAINCACHE_OBJSTORE_BUCKET"
	envObjstoreSecure       = "TRAINCACHE_OBJSTORE_SECURE"
	envObjstoreProbe        = "TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE"
	defaultObjstoreEndpoint = "127.0.0.1:9000"
	defaultObjstoreBucket   = "traincache"
	exitOK                  = 0
	exitRuntimeError        = 1
	exitUsageError          = 2
	workDirTempPattern      = "trainpublish-"
	workDirPermission       = 0o755
)

// deps 是可注入的构造依赖，便于测试中替换成不连网的 fake。
type deps struct {
	newStore  func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error)
	newCAS    func(dir string) (*cas.Store, error)
	newWriter func(store objectstore.Store, cs *cas.Store) *index.Writer
	probe     func(ctx context.Context, store objectstore.Store) error
	publishFn func(ctx context.Context, store objectstore.Store, cs *cas.Store, w *index.Writer, sources []publish.Source, opt publish.Options) (publish.Result, error)
}

// defaultDeps 返回真实实现。
func defaultDeps() deps {
	return deps{
		newStore: func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error) {
			return objectstore.NewS3Store(endpoint, accessKey, secretKey, bucket, secure)
		},
		newCAS: func(dir string) (*cas.Store, error) {
			return cas.New(dir, cas.VerifyXXH3)
		},
		newWriter: func(store objectstore.Store, cs *cas.Store) *index.Writer {
			return index.NewWriter(store, cs, index.Config{})
		},
		probe:     objectstore.ProbeConditionalWrite,
		publishFn: publish.Publish,
	}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, defaultDeps()))
}

// run 执行一次 CLI 调用并返回退出码；不调用 os.Exit，便于测试。
//
// 退出码：0 成功；1 运行期错误（文件、对象存储、发布等）；2 参数校验失败。
func run(args []string, stdout, stderr io.Writer, d deps) int {
	fs := flag.NewFlagSet("trainpublish", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: trainpublish --dataset <name> [options] <tar> [<tar>...]")
		fs.PrintDefaults()
	}

	dataset := fs.String("dataset", "", "dataset name (required)")
	endpoint := fs.String("endpoint", envOr(envObjstoreEndpoint, defaultObjstoreEndpoint), "object store endpoint")
	accessKey := fs.String("access-key", envOr(envObjstoreAccessKey, ""), "object store access key")
	secretKey := fs.String("secret-key", envOr(envObjstoreSecretKey, ""), "object store secret key")
	bucket := fs.String("bucket", envOr(envObjstoreBucket, defaultObjstoreBucket), "object store bucket")
	secure := fs.Bool("secure", envBool(envObjstoreSecure), "use TLS for the object store")
	chunkSize := fs.Int64("chunk-size", publish.DefaultChunkSize, "chunk size in bytes")
	concurrency := fs.Int("concurrency", publish.DefaultConcurrency, "concurrent uploads")
	workDirFlag := fs.String("work-dir", "", "work directory")
	keepWorkDir := fs.Bool("keep-work-dir", false, "keep the work directory")
	probeConditionalWrite := fs.Bool("probe-conditional-write", envBool(envObjstoreProbe), "probe conditional write support")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsageError
	}

	if *dataset == "" || fs.NArg() < 1 || *chunkSize <= 0 || *concurrency <= 0 {
		fs.Usage()
		return exitUsageError
	}

	sources, files, err := openSources(fs.Args())
	if err != nil {
		reportError(stderr, err)
		return exitRuntimeError
	}
	defer closeFiles(files)

	store, err := d.newStore(*endpoint, *accessKey, *secretKey, *bucket, *secure)
	if err != nil {
		reportError(stderr, fmt.Errorf("create object store: %w", err))
		return exitRuntimeError
	}

	ctx := context.Background()
	if *probeConditionalWrite {
		if err := d.probe(ctx, store); err != nil {
			reportError(stderr, fmt.Errorf("probe conditional write: %w", err))
			return exitRuntimeError
		}
	}

	workDir, err := newWorkDir(*workDirFlag)
	if err != nil {
		reportError(stderr, fmt.Errorf("create work dir: %w", err))
		return exitRuntimeError
	}
	if *keepWorkDir {
		fmt.Fprintf(stderr, "trainpublish: keeping work dir %s\n", workDir)
	} else {
		defer func() { _ = os.RemoveAll(workDir) }()
	}

	cs, err := d.newCAS(workDir)
	if err != nil {
		reportError(stderr, fmt.Errorf("create cas: %w", err))
		return exitRuntimeError
	}

	writer := d.newWriter(store, cs)
	result, err := d.publishFn(ctx, store, cs, writer, sources, publish.Options{
		Dataset:     *dataset,
		ChunkSize:   *chunkSize,
		Concurrency: *concurrency,
	})
	if err != nil {
		reportError(stderr, fmt.Errorf("publish: %w", err))
		return exitRuntimeError
	}

	fmt.Fprintf(stdout, "snapID=%s entries=%d bytes=%d unique_blocks=%d\n",
		result.SnapID, result.Entries, result.Bytes, result.UniqueBlocks)
	return exitOK
}

// openSources 打开全部 tar 并构造 Source。Path 取 filepath.Base，不做去重或改名；
// 重复路径由 publish.Publish 返回 publish.ErrDuplicatePath。
func openSources(paths []string) ([]publish.Source, []*os.File, error) {
	sources := make([]publish.Source, 0, len(paths))
	files := make([]*os.File, 0, len(paths))
	fail := func(err error) ([]publish.Source, []*os.File, error) {
		closeFiles(files)
		return nil, nil, err
	}

	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return fail(fmt.Errorf("open %s: %w", path, err))
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return fail(fmt.Errorf("stat %s: %w", path, err))
		}
		files = append(files, f)
		sources = append(sources, publish.Source{
			Path:   filepath.Base(path),
			Reader: f,
			Size:   info.Size(),
		})
	}
	return sources, files, nil
}

// closeFiles 尽力关闭全部已打开文件。
func closeFiles(files []*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}

// newWorkDir 创建本次发布的临时 CAS 根目录。
// 未指定父目录时使用系统临时目录；指定时先确保父目录存在，再在其下建唯一子目录。
func newWorkDir(parent string) (string, error) {
	if parent == "" {
		return os.MkdirTemp("", workDirTempPattern)
	}
	if err := os.MkdirAll(parent, workDirPermission); err != nil {
		return "", err
	}
	return os.MkdirTemp(parent, workDirTempPattern)
}

// reportError 把运行期错误写到 stderr。
func reportError(stderr io.Writer, err error) {
	fmt.Fprintf(stderr, "trainpublish: %v\n", err)
}

// envOr 返回环境变量值；未设置或全空白时返回默认值（与 agent/svc 同口径）。
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// envBool 解析布尔环境变量："1"/"true"/"yes"（大小写不敏感）为真，
// 其余取值及未设置一律为假（与 agent/svc.envBool 同口径）。
func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes":
		return true
	}
	return false
}
