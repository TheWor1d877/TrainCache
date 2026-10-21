// Package publish 实现多 tar 发布器：把若干 tar 源切块上传为数据块，并提交一次索引快照。
//
// 发布流程：
//   - 按 Options.ChunkSize 对每个 Source 切块（末块可不足）；
//   - 以 Options.Concurrency 并发上传数据块到对象存储，块键由
//     objectstore.BlockKey 生成；
//   - 全部块上传成功后，仅调用一次 index.Writer.Commit 提交索引；
//   - 索引条目的 Node 字段留空（块不绑定特定节点）；
//   - 顺序约束：先块后 Commit，块未全部就绪前不提交索引。
package publish

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
)

const (
	// DefaultChunkSize 是默认切块粒度：64 MiB。
	DefaultChunkSize int64 = 64 << 20
	// DefaultConcurrency 是默认并发上传数。
	DefaultConcurrency = 4
)

var (
	// ErrNoSources 表示未提供任何 tar 源。
	ErrNoSources = errors.New("publish: no sources")
	// ErrDuplicatePath 表示 sources 中存在重复的 Path。
	ErrDuplicatePath = errors.New("publish: duplicate path")
)

// Source 是一个待发布的 tar 源：Path 为索引路径，Reader 提供数据，
// Size 为数据字节数。
type Source struct {
	Path   string
	Reader io.ReaderAt
	Size   int64
}

// Options 是发布选项。
type Options struct {
	Dataset     string
	ChunkSize   int64
	Concurrency int
}

// Result 是一次发布的统计结果。
type Result struct {
	SnapID       string
	Entries      int
	Bytes        int64
	UniqueBlocks int
}

// task 是一个待上传的块：src 的第 chunk 块，数据区为 [off, off+n)。
type task struct {
	src   Source
	chunk int64
	off   int64
	n     int64
}

// Publish 将 sources 切块上传并提交一次快照。
//
// 实现契约：按 opt.ChunkSize 切块（零值回退 DefaultChunkSize），以
// opt.Concurrency 并发上传（零值回退 DefaultConcurrency）；所有块上传成功后
// 调用一次 w.Commit 提交索引；写入的 index.Entry.Node 留空；严格保持
// “先块后 Commit”的顺序，Commit 失败时不留下可见快照。
func Publish(ctx context.Context, store objectstore.Store, cs *cas.Store, w *index.Writer, sources []Source, opt Options) (Result, error) {
	if opt.Dataset == "" {
		return Result{}, fmt.Errorf("publish: dataset must not be empty")
	}
	if len(sources) == 0 {
		return Result{}, ErrNoSources
	}

	seen := make(map[string]struct{}, len(sources))
	for i, src := range sources {
		if src.Path == "" {
			return Result{}, fmt.Errorf("publish: source %d: path must not be empty", i)
		}
		if strings.Contains(src.Path, "#") {
			return Result{}, fmt.Errorf("publish: source %d: path %q must not contain '#'", i, src.Path)
		}
		if src.Size < 0 {
			return Result{}, fmt.Errorf("publish: source %d: path %q: negative size %d", i, src.Path, src.Size)
		}
		if _, ok := seen[src.Path]; ok {
			return Result{}, fmt.Errorf("publish: path %q: %w", src.Path, ErrDuplicatePath)
		}
		seen[src.Path] = struct{}{}
	}

	chunkSize := opt.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	concurrency := opt.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}

	// 展开任务：每个 (source, chunkIndex) 一个任务；空文件不产生任务/条目。
	var tasks []task
	for _, src := range sources {
		for chunk, off := int64(0), int64(0); off < src.Size; chunk++ {
			n := chunkSize
			if rem := src.Size - off; rem < n {
				n = rem
			}
			tasks = append(tasks, task{src: src, chunk: chunk, off: off, n: n})
			off += n
		}
	}

	// 同一 worker 池并发执行全部任务；首个错误触发取消，后续任务不再提交。
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan task)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var entries []index.Entry
	var totalBytes int64
	unique := make(map[cas.Fingerprint]struct{})

	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}

	workers := concurrency
	if workers > len(tasks) {
		workers = len(tasks)
	}

	go func() {
		defer close(jobs)
		for _, t := range tasks {
			select {
			case jobs <- t:
			case <-runCtx.Done():
				return
			}
		}
	}()

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for t := range jobs {
				if runCtx.Err() != nil {
					continue // 已取消：只排空 jobs，避免 feeder 阻塞
				}
				fp, n, err := uploadChunk(runCtx, store, cs, opt.Dataset, t)
				if err != nil {
					setErr(err)
					continue
				}
				mu.Lock()
				entries = append(entries, index.Entry{
					Path:        t.src.Path,
					Chunk:       t.chunk,
					Size:        n,
					Fingerprint: fp,
				})
				totalBytes += n
				unique[fp] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return Result{}, firstErr
	}

	// 全部块就绪后才提交；按 (Path, Chunk) 排序后一次性交给 Commit。
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Path != entries[j].Path {
			return entries[i].Path < entries[j].Path
		}
		return entries[i].Chunk < entries[j].Chunk
	})
	snapID, err := w.Commit(ctx, opt.Dataset, entries)
	if err != nil {
		return Result{}, fmt.Errorf("publish: commit dataset %q: %w", opt.Dataset, err)
	}

	return Result{
		SnapID:       snapID,
		Entries:      len(entries),
		Bytes:        totalBytes,
		UniqueBlocks: len(unique),
	}, nil
}

// uploadChunk 上传单个块：先本地 CAS（cs.Put），再远端对象存储（store.Put），
// 顺序不可颠倒；返回 CAS 指纹与写入字节数。
func uploadChunk(ctx context.Context, store objectstore.Store, cs *cas.Store, dataset string, t task) (cas.Fingerprint, int64, error) {
	var zero cas.Fingerprint

	fp, n, err := cs.Put(ctx, io.NewSectionReader(t.src.Reader, t.off, t.n))
	if err != nil {
		return zero, 0, fmt.Errorf("publish: cas put %s chunk %d: %w", t.src.Path, t.chunk, err)
	}
	if n != t.n {
		return zero, 0, fmt.Errorf("publish: cas put %s chunk %d: wrote %d bytes, expected %d", t.src.Path, t.chunk, n, t.n)
	}

	key := objectstore.BlockKey(dataset, t.src.Path, t.chunk)
	if _, err := store.Put(ctx, key, io.NewSectionReader(t.src.Reader, t.off, t.n), t.n); err != nil {
		return zero, 0, fmt.Errorf("publish: upload %s: %w", key, err)
	}
	return fp, n, nil
}
