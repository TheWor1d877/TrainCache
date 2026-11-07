// Package s3 实现基于 S3 兼容对象存储的 checkpoint 插件。
//
// 远端布局、manifest 与续传状态由 internal/plugin 与 internal/manifest 定义；
// 本包只负责把本地目录按该布局写入对象存储，并在恢复时校验后原子落位。
// 插件不复用 core/objectstore：Store 是读路径契约，表达不了分片与跨进程续传
// （design 10.2 D6）。
package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"traincache/checkpoint/internal/manifest"
	"traincache/checkpoint/internal/plugin"
)

const (
	defaultPartSize    = 8 << 20
	minPartSize        = 5 << 20
	defaultConcurrency = 4
	maxParts           = 10000 // S3 multipart 的分片数上限
	maxManifestBytes   = 64 << 20

	accessKeyEnv = "TRAINCACHE_CHECKPOINT_ACCESS_KEY"
	secretKeyEnv = "TRAINCACHE_CHECKPOINT_SECRET_KEY"

	kindUpload = "upload"
)

// objectAPI 是插件用到的对象存储原语。抽象成接口是为了让单元测试用内存
// 替身验证分片、续传与写入顺序，而不必启动 MinIO；生产实现是 *minio.Core。
type objectAPI interface {
	NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.PutObjectOptions) (string, error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error)
	ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (minio.ListObjectPartsResult, error)
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error
	RemoveObject(ctx context.Context, bucket, object string, opts minio.RemoveObjectOptions) error
	PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, md5Base64, sha256Hex string, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, minio.ObjectInfo, http.Header, error)
	ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
}

// backend 是解析后的 backend JSON 与已构造的客户端。
// minioStore 显式转发 ListObjects。*minio.Core 自带同名的旧版 ListObjects，
// 会遮住内嵌 *minio.Client 的带 context 版本，接口无法直接由 *minio.Core 满足。
type minioStore struct {
	*minio.Core
}

func (m minioStore) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	return m.Core.Client.ListObjects(ctx, bucket, opts)
}

type backend struct {
	prefix      string
	bucket      string
	partSize    int64
	concurrency int
	store       objectAPI
}

// backendJSON 的指针字段用来区分「未提供」与「显式写 0」：未提供走默认值，
// 显式写 0 或过小值按契约返回 INVALID_ARGUMENT。
type backendJSON struct {
	Endpoint    string `json:"endpoint"`
	Bucket      string `json:"bucket"`
	Prefix      string `json:"prefix"`
	Secure      bool   `json:"secure"`
	PartSize    *int64 `json:"part_size"`
	Concurrency *int   `json:"concurrency"`
}

// Handler 是插件进程的分发入口。params 是 checkpointd 原样转交的请求体，
// backend 由本包解析，协议层不感知对象存储细节。
func Handler(ctx context.Context, method string, params json.RawMessage, sess *plugin.Session) (any, error) {
	progress := func(done, total int64) {
		_ = sess.Progress(done, total)
	}

	switch method {
	case "resolve":
		var p plugin.ResolveParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, invalidf("解析 resolve 参数: %v", err)
		}
		b, err := loadBackend(p.Backend)
		if err != nil {
			return nil, err
		}
		return resolve(ctx, b, p)
	case "save":
		var p plugin.SaveParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, invalidf("解析 save 参数: %v", err)
		}
		b, err := loadBackend(p.Backend)
		if err != nil {
			return nil, err
		}
		return save(ctx, b, p, progress)
	case "restore":
		var p plugin.RestoreParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, invalidf("解析 restore 参数: %v", err)
		}
		b, err := loadBackend(p.Backend)
		if err != nil {
			return nil, err
		}
		return restore(ctx, b, p, progress)
	default:
		return nil, invalidf("未知方法 %q", method)
	}
}

func loadBackend(raw json.RawMessage) (*backend, error) {
	if len(raw) == 0 {
		return nil, invalidf("backend 不能为空")
	}
	var cfg backendJSON
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, invalidf("解析 backend: %v", err)
	}
	if cfg.Endpoint == "" {
		return nil, invalidf("backend.endpoint 不能为空")
	}
	// endpoint 只写 host:port；带 scheme 会被 minio 当成路径的一部分，
	// 这里提前拒绝，免得错误在第一次请求时才暴露。
	if strings.Contains(cfg.Endpoint, "://") {
		return nil, invalidf("backend.endpoint 不能带 scheme: %q", cfg.Endpoint)
	}
	if cfg.Bucket == "" {
		return nil, invalidf("backend.bucket 不能为空")
	}

	partSize := int64(defaultPartSize)
	if cfg.PartSize != nil {
		partSize = *cfg.PartSize
	}
	if partSize < minPartSize {
		return nil, invalidf("backend.part_size=%d 小于下限 %d", partSize, minPartSize)
	}

	concurrency := defaultConcurrency
	if cfg.Concurrency != nil {
		concurrency = *cfg.Concurrency
	}
	if concurrency < 1 {
		return nil, invalidf("backend.concurrency=%d 需 >= 1", concurrency)
	}

	core, err := minio.NewCore(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv(accessKeyEnv), os.Getenv(secretKeyEnv), ""),
		Secure: cfg.Secure,
		// region 留空：由 minio 按桶位置探测，避免把部署区域写死进配置。
	})
	if err != nil {
		return nil, invalidf("构造 S3 客户端: %v", err)
	}

	return &backend{
		prefix:      cfg.Prefix,
		bucket:      cfg.Bucket,
		partSize:    partSize,
		concurrency: concurrency,
		store:       minioStore{Core: core},
	}, nil
}

// resolve 把 rank 集合恰好等于 {0 … world_size-1} 且 world_size 份 manifest
// 全部能 Unmarshal 的 step 视为完整（design 10.2 D25）。latest 从大到小找第一个
// 完整 step，step 模式只认指定值；两者都把枚举期间看到过的最大多余 rank 编号经
// MaxExtraRank 带回，供 checkpointd 记 reason=rank_beyond_world_size 的 WARN。
func resolve(ctx context.Context, b *backend, p plugin.ResolveParams) (plugin.ResolveResult, error) {
	if err := validateResolve(p); err != nil {
		return plugin.ResolveResult{}, err
	}

	if p.Policy.Mode == "step" {
		epoch, maxExtra, ok, err := b.completeStep(ctx, p.TaskID, p.Policy.Step, p.WorldSize)
		if err != nil {
			return plugin.ResolveResult{}, err
		}
		if !ok {
			return plugin.ResolveResult{MaxExtraRank: maxExtra}, nil
		}
		return plugin.ResolveResult{Found: true, Step: p.Policy.Step, Complete: true, Epoch: epoch}, nil
	}

	root := taskRoot(b.prefix, p.TaskID)
	objs, err := b.listPrefix(ctx, root+"/", false)
	if err != nil {
		return plugin.ResolveResult{}, toPluginError(err)
	}

	seen := make(map[int64]struct{})
	var steps []int64
	for _, obj := range objs {
		step, ok := parseStepPrefix(obj.Key, root)
		if !ok {
			continue
		}
		if _, dup := seen[step]; dup {
			continue
		}
		seen[step] = struct{}{}
		steps = append(steps, step)
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i] > steps[j] })

	var maxExtra int
	for _, step := range steps {
		epoch, extra, ok, err := b.completeStep(ctx, p.TaskID, step, p.WorldSize)
		if err != nil {
			return plugin.ResolveResult{}, err
		}
		if ok {
			// 选中 step 自身没有多余份（有的话就判不完整了），带回去的是
			// 更靠新 step 上的编号：异常拓扑只要在本次枚举里出现过就要报。
			return plugin.ResolveResult{Found: true, Step: step, Complete: true, Epoch: epoch, MaxExtraRank: maxExtra}, nil
		}
		if extra > maxExtra {
			maxExtra = extra
		}
	}
	return plugin.ResolveResult{MaxExtraRank: maxExtra}, nil
}

// completeStep 按 design 10.2 D25 判完整性：实际存在的 rank 集合必须恰好
// 等于 {0 … worldSize-1}。缺任一份判不完整；编号 ≥ worldSize 的已提交份
// 同样判不完整，经 maxExtra 带回看到过的最大编号（0 表示没有多余份）。
// 多余份的编号在既有的那一次非递归 LIST 里就能看到，不额外请求；只有真
// 出现候选时才为每个候选付一次 GET，用来确认它是否已提交。
func (b *backend) completeStep(ctx context.Context, taskID string, step int64, worldSize int) (epoch int, maxExtra int, complete bool, err error) {
	stepPrefix := plugin.StepPrefix(b.prefix, taskID, step)
	objs, err := b.listPrefix(ctx, stepPrefix+"/", false)
	if err != nil {
		return 0, 0, false, toPluginError(err)
	}

	ranks := make(map[int]struct{}, worldSize)
	var extra []int
	for _, obj := range objs {
		rank, ok := parseRankPrefix(obj.Key, stepPrefix)
		if !ok {
			continue
		}
		if rank < worldSize {
			ranks[rank] = struct{}{}
			continue
		}
		extra = append(extra, rank)
	}
	// 期望集合缺份与多余份都判不完整，但先确认多余份：它决定 WARN 的取值，
	// 两侧插件的判定顺序一致，同一个 step 才能给出同样的 MaxExtraRank。
	maxExtra, err = b.committedMaxExtra(ctx, taskID, step, extra)
	if err != nil {
		return 0, maxExtra, false, err
	}
	if maxExtra > 0 {
		return 0, maxExtra, false, nil
	}
	if len(ranks) < worldSize {
		return 0, 0, false, nil
	}

	// rank 0 的 epoch 优先；其余 rank 只用于确认 manifest 可用且彼此一致。
	for rank := 0; rank < worldSize; rank++ {
		data, err := b.getManifest(ctx, plugin.ManifestKey(b.prefix, taskID, step, rank))
		if err != nil {
			if plugin.CodeOf(err) == plugin.CodeNotFound {
				return 0, 0, false, nil
			}
			return 0, 0, false, err
		}
		m, err := manifest.Unmarshal(data)
		if err != nil {
			return 0, 0, false, nil
		}
		if m.TaskID != taskID || m.Step != step || m.Rank != rank || m.WorldSize != worldSize {
			return 0, 0, false, nil
		}
		if rank == 0 {
			epoch = m.Epoch
		}
	}
	return epoch, 0, true, nil
}

// committedMaxExtra 确认候选 rank 是否真的提交过：manifest 存在、能解析且
// task_id/step/rank 与请求相符才算。有意不比对 world_size——副本数变小正是
// 这里要发现的异常本身，按请求值比对只会把它藏起来。
// 缺 manifest 是在途半成品，按 D25 不影响完整性。
func (b *backend) committedMaxExtra(ctx context.Context, taskID string, step int64, candidates []int) (int, error) {
	maxRank := 0
	for _, rank := range candidates {
		data, err := b.getManifest(ctx, plugin.ManifestKey(b.prefix, taskID, step, rank))
		if err != nil {
			if plugin.CodeOf(err) == plugin.CodeNotFound {
				continue
			}
			return 0, err
		}
		m, err := manifest.Unmarshal(data)
		if err != nil {
			continue
		}
		if m.TaskID != taskID || m.Step != step || m.Rank != rank {
			continue
		}
		if rank > maxRank {
			maxRank = rank
		}
	}
	return maxRank, nil
}

// save 先扫描并哈希本地文件，再逐个分片上传，最后写 manifest。
// 中途失败时续传状态留在 state_path，下一次调用从已确认分片继续。
func save(ctx context.Context, b *backend, p plugin.SaveParams, progress func(int64, int64)) (plugin.TransferResult, error) {
	if err := validateSave(p); err != nil {
		return plugin.TransferResult{}, err
	}

	files, total, err := scanLocal(p.LocalPath)
	if err != nil {
		return plugin.TransferResult{}, err
	}
	if len(files) == 0 {
		return plugin.TransferResult{}, invalidf("本地目录 %s 没有可保存的文件", p.LocalPath)
	}
	if err := os.MkdirAll(p.StatePath, 0o755); err != nil {
		return plugin.TransferResult{}, internalf("创建续传状态目录 %s: %v", p.StatePath, err)
	}

	// 进度总量按「哈希 + 上传」两段计，保证 bytes_done 单调不减；
	// 计数本身只用于存活探测，不参与传输决策。
	workTotal := total * 2
	progress(0, workTotal)

	manifestKey := plugin.ManifestKey(b.prefix, p.TaskID, p.Step, p.Rank)
	createdAt := time.Now().UTC()
	// 同一 step 重跑时沿用首次写入的 created_at，使重复保存得到相同字节；
	// 读不到旧 manifest 不影响覆盖写，真正的问题会在最后 PutObject 暴露。
	if old, err := b.getManifest(ctx, manifestKey); err == nil {
		if prev, err := manifest.Unmarshal(old); err == nil &&
			prev.TaskID == p.TaskID && prev.Step == p.Step &&
			prev.Rank == p.Rank && prev.WorldSize == p.WorldSize {
			createdAt = prev.CreatedAt
		}
	}

	mf := &manifest.Manifest{
		Version:   manifest.Version,
		TaskID:    p.TaskID,
		Step:      p.Step,
		Rank:      p.Rank,
		WorldSize: p.WorldSize,
		CreatedAt: createdAt,
	}
	var hashed int64
	for i := range files {
		fe := files[i]
		size, sum, err := hashFile(ctx, fe.path, func(n int64) {
			progress(hashed+n, workTotal)
		})
		if err != nil {
			return plugin.TransferResult{}, toPluginError(err)
		}
		fe.size = size
		fe.sha = sum
		mf.Files = append(mf.Files, manifest.File{Path: fe.rel, Size: size, SHA256: sum})
		hashed += size
	}
	if hashed != total {
		return plugin.TransferResult{}, internalf("本地文件在保存过程中发生变化，统计 %d 字节，实际 %d 字节", total, hashed)
	}
	if err := mf.Validate(); err != nil {
		return plugin.TransferResult{}, internalf("构造 manifest: %v", err)
	}
	data, err := mf.Marshal()
	if err != nil {
		return plugin.TransferResult{}, internalf("编码 manifest: %v", err)
	}

	// manifest 是提交标记。重新保存同一 step 时先撤掉上一版，否则文件
	// 覆盖期间 resolve 会拿旧 manifest 把正在变化的目录判成完整。
	if err := b.store.RemoveObject(ctx, b.bucket, manifestKey, minio.RemoveObjectOptions{}); err != nil {
		return plugin.TransferResult{}, toPluginError(err)
	}

	var sent int64
	var progMu sync.Mutex
	report := func(delta int64) {
		progMu.Lock()
		sent += delta
		progress(total+sent, workTotal)
		progMu.Unlock()
	}
	for i := range files {
		if err := b.uploadFile(ctx, p, files[i], report); err != nil {
			return plugin.TransferResult{}, err
		}
	}

	// manifest 是提交标记，必须最后写；它出现之前对象存储上的分片都不算完成。
	if _, err := b.store.PutObject(ctx, b.bucket, manifestKey, bytes.NewReader(data), int64(len(data)), "", "",
		minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		return plugin.TransferResult{}, toPluginError(err)
	}

	for i := range files {
		path := stateFile(p.StatePath, files[i].sha)
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return plugin.TransferResult{}, internalf("删除续传状态 %s: %v", path, err)
		}
	}

	progress(workTotal, workTotal)
	return plugin.TransferResult{Bytes: total, Files: len(files)}, nil
}

func (b *backend) uploadFile(ctx context.Context, p plugin.SaveParams, fe *localFile, report func(int64)) error {
	key := plugin.FileKey(b.prefix, p.TaskID, p.Step, p.Rank, fe.rel)

	// 空文件没有分片；部分后端不接受零长分片，改用单次 PUT。
	if fe.size == 0 {
		if _, err := b.store.PutObject(ctx, b.bucket, key, bytes.NewReader(nil), 0, "", "",
			minio.PutObjectOptions{}); err != nil {
			return toPluginError(err)
		}
		report(0)
		return nil
	}

	statePath := stateFile(p.StatePath, fe.sha)
	uploadID, reused, err := b.beginUpload(ctx, key, fe, statePath)
	if err != nil {
		return err
	}

	totalParts := int((fe.size + b.partSize - 1) / b.partSize)
	if totalParts > maxParts {
		return invalidf("文件 %s 需要 %d 个分片，超过 S3 上限 %d；请增大 part_size", fe.rel, totalParts, maxParts)
	}

	parts := make([]minio.CompletePart, totalParts)
	for part, info := range reused {
		if part < 1 || part > totalParts {
			continue
		}
		parts[part-1] = minio.CompletePart{PartNumber: part, ETag: info.ETag}
		report(info.Size)
	}

	f, err := os.Open(fe.path)
	if err != nil {
		return toPluginError(err)
	}
	defer f.Close()

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, b.concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}

	for part := 1; part <= totalParts; part++ {
		if parts[part-1].ETag != "" {
			continue
		}
		if uploadCtx.Err() != nil {
			setErr(uploadCtx.Err())
			break
		}
		acquired := false
		select {
		case sem <- struct{}{}:
			acquired = true
		case <-uploadCtx.Done():
		}
		if !acquired {
			setErr(uploadCtx.Err())
			break
		}

		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			defer func() { <-sem }()

			size, ok := partSizeAt(part, fe.size, b.partSize)
			if !ok {
				setErr(fmt.Errorf("计算 %s 分片 %d 大小失败", fe.rel, part))
				return
			}
			buf := make([]byte, size)
			if _, err := f.ReadAt(buf, int64(part-1)*b.partSize); err != nil {
				setErr(fmt.Errorf("读取 %s 分片 %d: %w", fe.rel, part, err))
				return
			}
			obj, err := b.store.PutObjectPart(uploadCtx, b.bucket, key, uploadID, part,
				bytes.NewReader(buf), size, minio.PutObjectPartOptions{})
			if err != nil {
				setErr(err)
				return
			}

			// 记录与结果放在同一把锁下，保证状态行的顺序与分片完成顺序一致；
			// AppendPart 内部 fsync，崩溃后这一片才算已确认。
			mu.Lock()
			parts[part-1] = minio.CompletePart{PartNumber: part, ETag: obj.ETag}
			appendErr := plugin.AppendPart(statePath, plugin.PartRecord{
				Part:  part,
				ETag:  obj.ETag,
				Bytes: size,
			})
			mu.Unlock()
			if appendErr != nil {
				setErr(appendErr)
				return
			}
			report(size)
		}(part)
	}
	wg.Wait()
	if firstErr != nil {
		return toPluginError(firstErr)
	}

	for i := range parts {
		if parts[i].ETag == "" {
			return internalf("文件 %s 缺少第 %d 片", fe.rel, i+1)
		}
	}
	info, err := b.store.CompleteMultipartUpload(ctx, b.bucket, key, uploadID, parts, minio.PutObjectOptions{})
	if err != nil {
		return toPluginError(err)
	}
	if err := verifyCompositeETag(info.ETag, parts); err != nil {
		return err
	}
	return nil
}

// beginUpload 判断已有上传是否还能续用。只有本地记录与远端 ListObjectParts
// 在分片号、ETag、大小三处都一致时才跳过，任何一项不同都重传该片；
// upload id 失效时丢弃旧状态，重新发起一个 multipart。
func (b *backend) beginUpload(ctx context.Context, key string, fe *localFile, statePath string) (string, map[int]minio.ObjectPart, error) {
	st, loadErr := loadUploadState(statePath)
	if loadErr == nil && st != nil && stateHeaderValid(st.header, key, fe.rel) {
		server, err := b.listObjectParts(ctx, key, st.header.UploadID)
		if err == nil {
			return st.header.UploadID, reusableParts(st, server, fe, b.partSize), nil
		}
		pe := toPluginError(err)
		if plugin.CodeOf(pe) != plugin.CodeNotFound {
			return "", nil, pe
		}
	}

	// 旧状态不可信或 upload id 已失效：删除后从头开始。
	if err := os.Remove(statePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", nil, internalf("删除失效的续传状态 %s: %v", statePath, err)
	}
	uploadID, err := b.store.NewMultipartUpload(ctx, b.bucket, key, minio.PutObjectOptions{})
	if err != nil {
		return "", nil, toPluginError(err)
	}
	if err := plugin.AppendPart(statePath, plugin.PartRecord{
		Kind:     kindUpload,
		UploadID: uploadID,
		Key:      key,
		File:     fe.rel,
	}); err != nil {
		_ = b.store.AbortMultipartUpload(context.WithoutCancel(ctx), b.bucket, key, uploadID)
		return "", nil, toPluginError(err)
	}
	return uploadID, nil, nil
}

func (b *backend) listObjectParts(ctx context.Context, key, uploadID string) (map[int]minio.ObjectPart, error) {
	out := make(map[int]minio.ObjectPart)
	marker := 0
	for {
		res, err := b.store.ListObjectParts(ctx, b.bucket, key, uploadID, marker, 1000)
		if err != nil {
			return nil, err
		}
		for _, part := range res.ObjectParts {
			out[part.PartNumber] = part
		}
		if !res.IsTruncated || res.NextPartNumberMarker <= marker {
			return out, nil
		}
		marker = res.NextPartNumberMarker
	}
}

// restore 先把所有对象下载到 target 同文件系统的临时目录，VerifyFiles 通过
// 后 rename；任何一步失败都删掉临时目录，target 不会出现半成品。
func restore(ctx context.Context, b *backend, p plugin.RestoreParams, progress func(int64, int64)) (plugin.TransferResult, error) {
	if err := validateRestore(p); err != nil {
		return plugin.TransferResult{}, err
	}
	if _, err := os.Lstat(p.TargetPath); err == nil {
		return plugin.TransferResult{}, invalidf("目标 %s 已存在", p.TargetPath)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return plugin.TransferResult{}, toPluginError(err)
	}

	data, err := b.getManifest(ctx, plugin.ManifestKey(b.prefix, p.TaskID, p.Step, p.Rank))
	if err != nil {
		return plugin.TransferResult{}, err
	}
	m, err := manifest.Unmarshal(data)
	if err != nil {
		return plugin.TransferResult{}, corruptf("解析 manifest: %v", err)
	}
	if m.TaskID != p.TaskID || m.Step != p.Step || m.Rank != p.Rank || m.WorldSize != p.WorldSize {
		return plugin.TransferResult{}, corruptf("manifest 与请求的 task/step/rank/world_size 不一致")
	}

	parent := filepath.Dir(p.TargetPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return plugin.TransferResult{}, internalf("创建目标父目录 %s: %v", parent, err)
	}
	tmp, err := os.MkdirTemp(parent, filepath.Base(p.TargetPath)+".tmp-")
	if err != nil {
		return plugin.TransferResult{}, invalidf("创建临时目录: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tmp)
		}
	}()

	var total int64
	for _, f := range m.Files {
		total += f.Size
	}
	var done int64
	for _, f := range m.Files {
		key := plugin.FileKey(b.prefix, p.TaskID, p.Step, p.Rank, f.Path)
		dst := filepath.Join(tmp, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return plugin.TransferResult{}, toPluginError(err)
		}
		if err := b.downloadFile(ctx, key, dst, func(n int64) {
			done += n
			progress(done, total)
		}); err != nil {
			return plugin.TransferResult{}, err
		}
	}

	// 复用 manifest 的校验口径，避免这里出现第二套大小与哈希判断。
	if err := m.VerifyFiles(tmp); err != nil {
		return plugin.TransferResult{}, corruptf("校验恢复文件: %v", err)
	}
	if err := os.Rename(tmp, p.TargetPath); err != nil {
		return plugin.TransferResult{}, toPluginError(err)
	}
	committed = true
	return plugin.TransferResult{Bytes: total, Files: len(m.Files)}, nil
}

func (b *backend) downloadFile(ctx context.Context, key, dst string, progress func(int64)) error {
	rc, info, _, err := b.store.GetObject(ctx, b.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return toPluginError(err)
	}
	defer rc.Close()

	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return toPluginError(err)
	}
	reader := &progressReader{r: rc, onRead: progress}
	n, copyErr := io.Copy(f, reader)
	syncErr := f.Sync()
	closeErr := f.Close()

	if copyErr != nil {
		return toPluginError(copyErr)
	}
	if info.Size >= 0 && n != info.Size {
		return corruptf("下载 %s 得到 %d 字节，期望 %d", key, n, info.Size)
	}
	if syncErr != nil {
		return toPluginError(syncErr)
	}
	if closeErr != nil {
		return toPluginError(closeErr)
	}
	return nil
}

func (b *backend) getManifest(ctx context.Context, key string) ([]byte, error) {
	rc, info, _, err := b.store.GetObject(ctx, b.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, toPluginError(err)
	}
	defer rc.Close()

	if info.Size > maxManifestBytes {
		return nil, corruptf("manifest 大小 %d 超过上限 %d", info.Size, maxManifestBytes)
	}
	data, err := io.ReadAll(io.LimitReader(rc, maxManifestBytes+1))
	if err != nil {
		return nil, toPluginError(err)
	}
	if int64(len(data)) > maxManifestBytes {
		return nil, corruptf("manifest 超过 %d 字节", maxManifestBytes)
	}
	return data, nil
}

func (b *backend) listPrefix(ctx context.Context, prefix string, recursive bool) ([]minio.ObjectInfo, error) {
	var out []minio.ObjectInfo
	for obj := range b.store.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, obj)
	}
	return out, nil
}

// taskRoot 从共享的 StepPrefix 反推任务前缀，避免在插件里再拼一套键格式。
func taskRoot(prefix, taskID string) string {
	return strings.TrimSuffix(plugin.StepPrefix(prefix, taskID, 0), "/step-0")
}

func parseStepPrefix(key, root string) (int64, bool) {
	if !strings.HasPrefix(key, root+"/") {
		return 0, false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(key, root+"/"), "/")
	if strings.Contains(rest, "/") {
		return 0, false
	}
	return parseNumber(rest, "step-")
}

func parseRankPrefix(key, stepPrefix string) (int, bool) {
	base := stepPrefix + "/"
	if !strings.HasPrefix(key, base) {
		return 0, false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(key, base), "/")
	if strings.Contains(rest, "/") {
		return 0, false
	}
	n, ok := parseNumber(rest, "rank-")
	if !ok || n > 1<<30 {
		return 0, false
	}
	return int(n), true
}

func parseNumber(s, prefix string) (int64, bool) {
	if !strings.HasPrefix(s, prefix) {
		return 0, false
	}
	digits := s[len(prefix):]
	if digits == "" {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

type localFile struct {
	rel  string
	path string
	size int64
	sha  string
}

// scanLocal 只收集常规文件；符号链接与设备/管道等非常规文件按契约拒绝，
// 避免把工作目录外的内容带进 checkpoint。
func scanLocal(root string) ([]*localFile, int64, error) {
	info, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, invalidf("本地目录 %s 不存在", root)
		}
		return nil, 0, toPluginError(err)
	}
	if !info.IsDir() {
		return nil, 0, invalidf("本地路径 %s 不是目录", root)
	}

	var files []*localFile
	var total int64
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() == ".done" || d.Name() == ".complete" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return invalidf("本地文件 %s 是符号链接", path)
		}
		if !info.Mode().IsRegular() {
			return invalidf("本地文件 %s 不是常规文件", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." || strings.HasPrefix(rel, "../") {
			return invalidf("本地文件 %s 不在本地目录内", path)
		}
		files = append(files, &localFile{rel: rel, path: path, size: info.Size()})
		total += info.Size()
		return nil
	})
	if err != nil {
		var pe *plugin.Error
		if errors.As(err, &pe) {
			return nil, 0, pe
		}
		return nil, 0, toPluginError(err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files, total, nil
}

func hashFile(ctx context.Context, path string, progress func(int64)) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, "", err
	}
	if !info.Mode().IsRegular() {
		return 0, "", fmt.Errorf("文件 %s 在保存过程中变成非常规文件", path)
	}
	size := info.Size()

	h := sha256.New()
	buf := make([]byte, 1<<20)
	var done int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, "", err
		}
		n, readErr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			done += int64(n)
			if progress != nil {
				progress(done)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, "", readErr
		}
	}
	if done != size {
		return 0, "", fmt.Errorf("文件 %s 大小在保存过程中变化：读取 %d 字节，期望 %d", path, done, size)
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}

func partSizeAt(part int, size, partSize int64) (int64, bool) {
	if part <= 0 || partSize <= 0 {
		return 0, false
	}
	offset := int64(part-1) * partSize
	if offset >= size {
		return 0, false
	}
	n := size - offset
	if n > partSize {
		n = partSize
	}
	return n, true
}

type uploadState struct {
	header plugin.PartRecord
	parts  map[int]plugin.PartRecord
}

func loadUploadState(path string) (*uploadState, error) {
	recs, err := plugin.LoadParts(path)
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, nil
	}
	if recs[0].Kind != kindUpload {
		return nil, fmt.Errorf("续传状态 %s 首行不是 upload 头", path)
	}
	st := &uploadState{header: recs[0], parts: make(map[int]plugin.PartRecord)}
	for _, rec := range recs[1:] {
		if rec.Kind != "" {
			return nil, fmt.Errorf("续传状态 %s 含多余头记录", path)
		}
		if rec.Part <= 0 {
			return nil, fmt.Errorf("续传状态 %s 分片号 %d 非法", path, rec.Part)
		}
		// 同一分片可能因 ETag 不符被重传，后写入的记录覆盖旧记录。
		st.parts[rec.Part] = rec
	}
	return st, nil
}

func stateHeaderValid(h plugin.PartRecord, key, file string) bool {
	return h.Kind == kindUpload && h.UploadID != "" && h.Key == key && h.File == file
}

func reusableParts(st *uploadState, server map[int]minio.ObjectPart, fe *localFile, partSize int64) map[int]minio.ObjectPart {
	out := make(map[int]minio.ObjectPart)
	for part, rec := range st.parts {
		want, ok := partSizeAt(part, fe.size, partSize)
		if !ok || rec.Bytes != want {
			continue
		}
		srv, ok := server[part]
		if !ok || srv.Size != rec.Bytes || srv.ETag == "" {
			continue
		}
		if !strings.EqualFold(strings.Trim(srv.ETag, `"`), strings.Trim(rec.ETag, `"`)) {
			continue
		}
		out[part] = srv
	}
	return out
}

func stateFile(dir, sha string) string {
	return filepath.Join(dir, sha+".jsonl")
}

// verifyCompositeETag 用分片 ETag 推算 S3 的复合 ETag。分片 ETag 不是 32 位
// MD5 十六进制时（例如后端使用非 MD5 校验）无法推算，跳过而不是误判。
func verifyCompositeETag(actual string, parts []minio.CompletePart) error {
	want, ok := expectedCompositeETag(parts)
	if !ok {
		return nil
	}
	if !strings.EqualFold(strings.Trim(actual, `"`), want) {
		return corruptf("复合 ETag %q 与分片推算值 %q 不符", actual, want)
	}
	return nil
}

func expectedCompositeETag(parts []minio.CompletePart) (string, bool) {
	if len(parts) == 0 {
		return "", false
	}
	h := md5.New()
	for _, p := range parts {
		raw, err := hex.DecodeString(strings.Trim(p.ETag, `"`))
		if err != nil || len(raw) != md5.Size {
			return "", false
		}
		h.Write(raw)
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts)), true
}

type progressReader struct {
	r      io.Reader
	onRead func(int64)
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 && p.onRead != nil {
		p.onRead(int64(n))
	}
	return n, err
}

func validateResolve(p plugin.ResolveParams) error {
	if err := validateTaskID(p.TaskID); err != nil {
		return err
	}
	if p.WorldSize <= 0 {
		return invalidf("world_size=%d 需 > 0", p.WorldSize)
	}
	if p.Rank < 0 || p.Rank >= p.WorldSize {
		return invalidf("rank=%d 需在 [0,%d) 内", p.Rank, p.WorldSize)
	}
	switch p.Policy.Mode {
	case "latest":
	case "step":
		if p.Policy.Step < 0 {
			return invalidf("policy.step=%d 不能为负", p.Policy.Step)
		}
	default:
		return invalidf("policy.mode=%q 需为 latest 或 step", p.Policy.Mode)
	}
	return nil
}

func validateSave(p plugin.SaveParams) error {
	if err := validateTaskID(p.TaskID); err != nil {
		return err
	}
	if p.WorldSize <= 0 {
		return invalidf("world_size=%d 需 > 0", p.WorldSize)
	}
	if p.Rank < 0 || p.Rank >= p.WorldSize {
		return invalidf("rank=%d 需在 [0,%d) 内", p.Rank, p.WorldSize)
	}
	if p.Step < 0 {
		return invalidf("step=%d 不能为负", p.Step)
	}
	if p.LocalPath == "" {
		return invalidf("local_path 不能为空")
	}
	if p.StatePath == "" {
		return invalidf("state_path 不能为空")
	}
	return nil
}

func validateRestore(p plugin.RestoreParams) error {
	if err := validateTaskID(p.TaskID); err != nil {
		return err
	}
	if p.WorldSize <= 0 {
		return invalidf("world_size=%d 需 > 0", p.WorldSize)
	}
	if p.Rank < 0 || p.Rank >= p.WorldSize {
		return invalidf("rank=%d 需在 [0,%d) 内", p.Rank, p.WorldSize)
	}
	if p.Step < 0 {
		return invalidf("step=%d 不能为负", p.Step)
	}
	if p.TargetPath == "" {
		return invalidf("target_path 不能为空")
	}
	return nil
}

// validateTaskID 挡住会把对象键拼到任务前缀之外的取值；task_id 直接进键路径。
func validateTaskID(id string) error {
	if id == "" {
		return invalidf("task_id 不能为空")
	}
	if id == "." || id == ".." || strings.ContainsAny(id, "/\\") {
		return invalidf("task_id %q 不能包含路径分隔符或为 . / ..", id)
	}
	return nil
}

func invalidf(format string, args ...any) error {
	return &plugin.Error{Code: plugin.CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

func corruptf(format string, args ...any) error {
	return &plugin.Error{Code: plugin.CodeCorrupt, Message: fmt.Sprintf(format, args...)}
}

func internalf(format string, args ...any) error {
	return &plugin.Error{Code: plugin.CodeInternal, Message: fmt.Sprintf(format, args...)}
}

// toPluginError 把 minio 与网络错误收敛到 10.1「插件进程契约」的错误码。已是插件
// 错误时原样返回，避免二次分类改变调用方看到的语义。
func toPluginError(err error) error {
	if err == nil {
		return nil
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return pe
	}
	if errors.Is(err, context.Canceled) {
		return &plugin.Error{Code: plugin.CodeInternal, Message: "请求已取消"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &plugin.Error{Code: plugin.CodeUnavailable, Message: err.Error()}
	}

	var respPtr *minio.ErrorResponse
	if errors.As(err, &respPtr) {
		return mapS3Error(respPtr.StatusCode, respPtr.Code, respPtr.Message)
	}
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		return mapS3Error(resp.StatusCode, resp.Code, resp.Message)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return &plugin.Error{Code: plugin.CodeUnavailable, Message: err.Error()}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return &plugin.Error{Code: plugin.CodeUnavailable, Message: err.Error()}
	}
	return &plugin.Error{Code: plugin.CodeInternal, Message: err.Error()}
}

func mapS3Error(status int, code, message string) error {
	if message == "" {
		message = code
	}
	switch {
	case status == http.StatusNotFound || code == "NoSuchKey" || code == "NoSuchUpload" || code == "NoSuchBucket":
		return &plugin.Error{Code: plugin.CodeNotFound, Message: message}
	case status == http.StatusUnauthorized || status == http.StatusForbidden ||
		code == "AccessDenied" || code == "InvalidAccessKeyId" || code == "SignatureDoesNotMatch":
		return &plugin.Error{Code: plugin.CodeAuth, Message: message}
	case status == http.StatusTooManyRequests || code == "SlowDown":
		return &plugin.Error{Code: plugin.CodeThrottled, Message: message}
	case status >= 500:
		return &plugin.Error{Code: plugin.CodeUnavailable, Message: message}
	default:
		return &plugin.Error{Code: plugin.CodeInternal, Message: message}
	}
}
