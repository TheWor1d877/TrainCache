package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"traincache/checkpoint/internal/manifest"
	"traincache/checkpoint/internal/plugin"
)

func TestBackendValidation(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantCode plugin.Code
		wantPart int64
		wantConc int
	}{
		{
			name:     "缺少 endpoint",
			raw:      `{"bucket":"b"}`,
			wantCode: plugin.CodeInvalidArgument,
		},
		{
			name:     "缺少 bucket",
			raw:      `{"endpoint":"127.0.0.1:9000"}`,
			wantCode: plugin.CodeInvalidArgument,
		},
		{
			name:     "endpoint 带 scheme",
			raw:      `{"endpoint":"http://127.0.0.1:9000","bucket":"b"}`,
			wantCode: plugin.CodeInvalidArgument,
		},
		{
			name:     "part_size 小于 5MiB",
			raw:      `{"endpoint":"127.0.0.1:9000","bucket":"b","part_size":4194304}`,
			wantCode: plugin.CodeInvalidArgument,
		},
		{
			name:     "concurrency 为 0",
			raw:      `{"endpoint":"127.0.0.1:9000","bucket":"b","concurrency":0}`,
			wantCode: plugin.CodeInvalidArgument,
		},
		{
			name:     "默认值",
			raw:      `{"endpoint":"127.0.0.1:9000","bucket":"b"}`,
			wantPart: defaultPartSize,
			wantConc: defaultConcurrency,
		},
		{
			name:     "显式值",
			raw:      `{"endpoint":"127.0.0.1:9000","bucket":"b","part_size":5242880,"concurrency":2}`,
			wantPart: 5242880,
			wantConc: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := loadBackend([]byte(tc.raw))
			if tc.wantCode != "" {
				if err == nil {
					t.Fatalf("期望错误 %s，实际成功", tc.wantCode)
				}
				if got := plugin.CodeOf(err); got != tc.wantCode {
					t.Fatalf("错误码 = %s，期望 %s (%v)", got, tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadBackend: %v", err)
			}
			if b.partSize != tc.wantPart {
				t.Errorf("partSize = %d，期望 %d", b.partSize, tc.wantPart)
			}
			if b.concurrency != tc.wantConc {
				t.Errorf("concurrency = %d，期望 %d", b.concurrency, tc.wantConc)
			}
		})
	}
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want plugin.Code
	}{
		{name: "404", err: minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchKey"}, want: plugin.CodeNotFound},
		{name: "403 指针", err: &minio.ErrorResponse{StatusCode: http.StatusForbidden, Code: "AccessDenied"}, want: plugin.CodeAuth},
		{name: "401", err: minio.ErrorResponse{StatusCode: http.StatusUnauthorized, Code: "InvalidAccessKeyId"}, want: plugin.CodeAuth},
		{name: "429", err: minio.ErrorResponse{StatusCode: http.StatusTooManyRequests, Code: "SlowDown"}, want: plugin.CodeThrottled},
		{name: "500", err: minio.ErrorResponse{StatusCode: http.StatusInternalServerError, Code: "InternalError"}, want: plugin.CodeUnavailable},
		{name: "503", err: minio.ErrorResponse{StatusCode: http.StatusServiceUnavailable}, want: plugin.CodeUnavailable},
		{name: "400 归 INTERNAL", err: minio.ErrorResponse{StatusCode: http.StatusBadRequest, Code: "BadRequest"}, want: plugin.CodeInternal},
		{name: "网络", err: &net.DNSError{Err: "lookup failed", IsTimeout: true}, want: plugin.CodeUnavailable},
		{name: "插件错误原样返回", err: &plugin.Error{Code: plugin.CodeAuth, Message: "denied"}, want: plugin.CodeAuth},
		{name: "取消", err: context.Canceled, want: plugin.CodeInternal},
		{name: "其他", err: errors.New("boom"), want: plugin.CodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := plugin.CodeOf(toPluginError(tc.err))
			if got != tc.want {
				t.Fatalf("错误码 = %s，期望 %s", got, tc.want)
			}
		})
	}
}

func TestUploadStateHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	header := plugin.PartRecord{Kind: kindUpload, UploadID: "upload-1", Key: "key", File: "a.bin"}
	if err := plugin.AppendPart(path, header); err != nil {
		t.Fatal(err)
	}
	if err := plugin.AppendPart(path, plugin.PartRecord{Part: 1, ETag: "e1", Bytes: 10}); err != nil {
		t.Fatal(err)
	}
	// 同一分片重传后追加新记录，读取时应以后写入的为准。
	if err := plugin.AppendPart(path, plugin.PartRecord{Part: 2, ETag: "old", Bytes: 20}); err != nil {
		t.Fatal(err)
	}
	if err := plugin.AppendPart(path, plugin.PartRecord{Part: 2, ETag: "new", Bytes: 20}); err != nil {
		t.Fatal(err)
	}

	st, err := loadUploadState(path)
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		t.Fatal("状态为空")
	}
	if !stateHeaderValid(st.header, "key", "a.bin") {
		t.Fatal("头记录应有效")
	}
	if stateHeaderValid(st.header, "other", "a.bin") {
		t.Fatal("对象键不同时头记录应无效")
	}
	if st.parts[2].ETag != "new" {
		t.Fatalf("重复分片取 %q，期望 new", st.parts[2].ETag)
	}
	if st.parts[1].Bytes != 10 {
		t.Fatalf("分片 1 大小 = %d，期望 10", st.parts[1].Bytes)
	}

	// 首行不是 upload 头时不能按续传处理。
	if err := os.WriteFile(path, []byte(`{"part":1,"etag":"x","bytes":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadUploadState(path); err == nil {
		t.Fatal("首行不是 upload 头时应报错")
	}
}

func TestKeyLayout(t *testing.T) {
	if got := plugin.ManifestKey("p", "task", 7, 2); got != "p/task/step-7/rank-2/manifest.json" {
		t.Fatalf("ManifestKey = %q", got)
	}
	if got := plugin.FileKey("p", "task", 7, 2, "sub/a.bin"); got != "p/task/step-7/rank-2/files/sub/a.bin" {
		t.Fatalf("FileKey = %q", got)
	}
	if got := taskRoot("p", "task"); got != "p/task" {
		t.Fatalf("taskRoot = %q", got)
	}
	if got := taskRoot("", "task"); got != "task" {
		t.Fatalf("taskRoot(空前缀) = %q", got)
	}

	stepPrefix := plugin.StepPrefix("p", "task", 7) + "/"
	if step, ok := parseStepPrefix(stepPrefix, "p/task"); !ok || step != 7 {
		t.Fatalf("parseStepPrefix(%q) = %d,%v", stepPrefix, step, ok)
	}
	if _, ok := parseStepPrefix(plugin.ManifestKey("p", "task", 7, 2), "p/task"); ok {
		t.Fatal("manifest 键不应被当成 step 前缀")
	}

	rankPrefix := plugin.RankPrefix("p", "task", 7, 2) + "/"
	if rank, ok := parseRankPrefix(rankPrefix, plugin.StepPrefix("p", "task", 7)); !ok || rank != 2 {
		t.Fatalf("parseRankPrefix(%q) = %d,%v", rankPrefix, rank, ok)
	}
	if _, ok := parseRankPrefix("p/task/step-7/rank-x/", "p/task/step-7"); ok {
		t.Fatal("非数字 rank 不应被接受")
	}
}

func TestSaveResumeSkipsConfirmedParts(t *testing.T) {
	src := t.TempDir()
	data := bytes.Repeat([]byte("x"), int(minPartSize)*2+1024)
	file := filepath.Join(src, "model.bin")
	if err := os.WriteFile(file, data, 0o644); err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	store := newFakeStore()
	b := testBackend(store)
	p := plugin.SaveParams{TaskID: "task", Rank: 0, WorldSize: 1, Step: 1, LocalPath: src, StatePath: stateDir}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	_, err := save(ctx, b, p, func(done, total int64) {
		// 哈希阶段 done 最多到 totalBytes；上传第一片后 done 才越过 total/2。
		if done > total/2 {
			once.Do(cancel)
		}
	})
	if err == nil {
		t.Fatal("中断的保存应返回错误")
	}
	first := store.partCalls()
	if first < 1 || first >= 3 {
		t.Fatalf("首次上传分片数 = %d，期望 1..2", first)
	}
	stateFiles, _ := filepath.Glob(filepath.Join(stateDir, "*.jsonl"))
	if len(stateFiles) != 1 {
		t.Fatalf("中断后状态文件数 = %d，期望 1", len(stateFiles))
	}

	store.resetPartCalls()
	if _, err := save(context.Background(), b, p, func(int64, int64) {}); err != nil {
		t.Fatalf("续传保存失败: %v", err)
	}
	second := store.partCalls()
	if second >= 3 {
		t.Fatalf("续传重传了 %d 片，期望少于 3", second)
	}
	if first+second < 3 {
		t.Fatalf("首次 %d 片 + 续传 %d 片 < 3", first, second)
	}

	stateFiles, _ = filepath.Glob(filepath.Join(stateDir, "*.jsonl"))
	if len(stateFiles) != 0 {
		t.Fatalf("成功后仍残留状态文件: %v", stateFiles)
	}

	data2, err := b.getManifest(context.Background(), plugin.ManifestKey(b.prefix, p.TaskID, p.Step, p.Rank))
	if err != nil {
		t.Fatal(err)
	}
	m, err := manifest.Unmarshal(data2)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 || m.Files[0].Path != "model.bin" {
		t.Fatalf("manifest files = %+v", m.Files)
	}

	// manifest 必须是最后一个写入操作。
	ops := store.operationLog()
	if len(ops) == 0 || ops[len(ops)-1] != "put:"+plugin.ManifestKey(b.prefix, p.TaskID, p.Step, p.Rank) {
		t.Fatalf("最后一个操作 = %v，期望写 manifest", ops)
	}
}

func TestSaveCompositeETagMismatchIsCorrupt(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.bin"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	store.wrongETag = true
	b := testBackend(store)
	p := plugin.SaveParams{TaskID: "task", Rank: 0, WorldSize: 1, Step: 1, LocalPath: src, StatePath: t.TempDir()}

	_, err := save(context.Background(), b, p, func(int64, int64) {})
	if got := plugin.CodeOf(err); got != plugin.CodeCorrupt {
		t.Fatalf("错误码 = %s，期望 %s (%v)", got, plugin.CodeCorrupt, err)
	}
}

func TestResolveRequiresAllRanks(t *testing.T) {
	store := newFakeStore()
	b := testBackend(store)
	putTestManifest(t, store, b.prefix, "task", 1, 0, 2, 5)
	putTestManifest(t, store, b.prefix, "task", 2, 0, 2, 9)

	res, err := resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: "task", Rank: 0, WorldSize: 2, Policy: plugin.Policy{Mode: "latest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Found || res.Complete {
		t.Fatalf("缺少 rank 1 时不应完整: %+v", res)
	}

	putTestManifest(t, store, b.prefix, "task", 1, 1, 2, 6)
	res, err = resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: "task", Rank: 0, WorldSize: 2, Policy: plugin.Policy{Mode: "latest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || !res.Complete || res.Step != 1 || res.Epoch != 5 {
		t.Fatalf("latest 结果 = %+v，期望 step 1 epoch 5", res)
	}

	putTestManifest(t, store, b.prefix, "task", 2, 1, 2, 10)
	res, err = resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: "task", Rank: 0, WorldSize: 2, Policy: plugin.Policy{Mode: "latest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || res.Step != 2 || res.Epoch != 9 {
		t.Fatalf("latest 结果 = %+v，期望 step 2 epoch 9", res)
	}

	res, err = resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: "task", Rank: 0, WorldSize: 2, Policy: plugin.Policy{Mode: "step", Step: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Found || res.Complete {
		t.Fatalf("不存在的 step 3 不应完整: %+v", res)
	}
}

func TestRestoreRejectsExistingTarget(t *testing.T) {
	target := filepath.Join(t.TempDir(), "restore")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	b := testBackend(newFakeStore())
	_, err := restore(context.Background(), b, plugin.RestoreParams{
		TaskID: "task", Rank: 0, WorldSize: 1, Step: 1, TargetPath: target,
	}, func(int64, int64) {})
	if got := plugin.CodeOf(err); got != plugin.CodeInvalidArgument {
		t.Fatalf("错误码 = %s，期望 INVALID_ARGUMENT (%v)", got, err)
	}
}

func TestBeginUploadRestartsWhenUploadIDInvalid(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.jsonl")
	if err := plugin.AppendPart(statePath, plugin.PartRecord{
		Kind: kindUpload, UploadID: "missing", Key: "key", File: "a.bin",
	}); err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	b := testBackend(store)
	fe := &localFile{rel: "a.bin", size: 10}
	uploadID, reused, err := b.beginUpload(context.Background(), "key", fe, statePath)
	if err != nil {
		t.Fatal(err)
	}
	if uploadID == "missing" {
		t.Fatal("失效的 upload id 不应继续使用")
	}
	if len(reused) != 0 {
		t.Fatalf("重新开始时不应复用分片: %v", reused)
	}
	st, err := loadUploadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if st.header.UploadID != uploadID {
		t.Fatalf("状态头 upload id = %q，期望 %q", st.header.UploadID, uploadID)
	}
}

func testBackend(store objectAPI) *backend {
	return &backend{prefix: "pfx", bucket: "bucket", partSize: minPartSize, concurrency: 2, store: store}
}

func putTestManifest(t *testing.T, store *fakeStore, prefix, task string, step int64, rank, worldSize, epoch int) {
	t.Helper()
	m := &manifest.Manifest{
		Version:   manifest.Version,
		TaskID:    task,
		Step:      step,
		Rank:      rank,
		WorldSize: worldSize,
		Epoch:     epoch,
		CreatedAt: time.Unix(0, 0).UTC(),
		Files:     []manifest.File{{Path: "a.bin", Size: 1, SHA256: strings.Repeat("0", 64)}},
	}
	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	store.putObject(plugin.ManifestKey(prefix, task, step, rank), data)
}

// fakeStore 是 objectAPI 的内存实现，用来在不联网的情况下验证分片、续传
// 与写入顺序。并发安全由 mu 保证，因为上传分片会并发进入。
type fakeStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	uploads   map[string]*fakeUpload
	nextID    int
	partCount int
	wrongETag bool
	ops       []string
}

type fakeUpload struct {
	key   string
	parts map[int]minio.ObjectPart
	data  map[int][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: make(map[string][]byte), uploads: make(map[string]*fakeUpload)}
}

func (f *fakeStore) putObject(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), data...)
}

func (f *fakeStore) partCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.partCount
}

func (f *fakeStore) resetPartCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partCount = 0
}

func (f *fakeStore) operationLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

func (f *fakeStore) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.PutObjectOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := fmt.Sprintf("upload-%d", f.nextID)
	f.uploads[id] = &fakeUpload{key: object, parts: make(map[int]minio.ObjectPart), data: make(map[int][]byte)}
	return id, nil
}

func (f *fakeStore) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error) {
	if err := ctx.Err(); err != nil {
		return minio.ObjectPart{}, err
	}
	buf, err := io.ReadAll(data)
	if err != nil {
		return minio.ObjectPart{}, err
	}
	sum := md5.Sum(buf)
	part := minio.ObjectPart{PartNumber: partID, ETag: hex.EncodeToString(sum[:]), Size: int64(len(buf))}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.partCount++
	up, ok := f.uploads[uploadID]
	if !ok {
		return minio.ObjectPart{}, minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchUpload"}
	}
	up.parts[partID] = part
	up.data[partID] = buf
	return part, nil
}

func (f *fakeStore) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (minio.ListObjectPartsResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	up, ok := f.uploads[uploadID]
	if !ok {
		return minio.ListObjectPartsResult{}, minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchUpload"}
	}
	var parts []minio.ObjectPart
	for _, p := range up.parts {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return minio.ListObjectPartsResult{ObjectParts: parts}, nil
}

func (f *fakeStore) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	up, ok := f.uploads[uploadID]
	if !ok {
		return minio.UploadInfo{}, minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchUpload"}
	}
	var all []byte
	for _, p := range parts {
		all = append(all, up.data[p.PartNumber]...)
	}
	etag, ok := expectedCompositeETag(parts)
	if !ok || f.wrongETag {
		etag = "deadbeef-" + strconv.Itoa(len(parts))
	}
	f.objects[object] = all
	delete(f.uploads, uploadID)
	f.ops = append(f.ops, "complete:"+object)
	return minio.UploadInfo{ETag: etag, Size: int64(len(all))}, nil
}

func (f *fakeStore) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.uploads, uploadID)
	return nil
}

func (f *fakeStore) RemoveObject(ctx context.Context, bucket, object string, opts minio.RemoveObjectOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, object)
	f.ops = append(f.ops, "remove:"+object)
	return nil
}

func (f *fakeStore) PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, md5Base64, sha256Hex string, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	if err := ctx.Err(); err != nil {
		return minio.UploadInfo{}, err
	}
	buf, err := io.ReadAll(data)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	sum := md5.Sum(buf)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[object] = buf
	f.ops = append(f.ops, "put:"+object)
	return minio.UploadInfo{ETag: hex.EncodeToString(sum[:]), Size: int64(len(buf))}, nil
}

func (f *fakeStore) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, minio.ObjectInfo, http.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[object]
	if !ok {
		return nil, minio.ObjectInfo{}, nil, minio.ErrorResponse{
			StatusCode: http.StatusNotFound, Code: "NoSuchKey", Key: object, Message: "not found",
		}
	}
	return io.NopCloser(bytes.NewReader(data)), minio.ObjectInfo{Key: object, Size: int64(len(data))}, nil, nil
}

func (f *fakeStore) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	f.mu.Lock()
	var keys []string
	for key := range f.objects {
		if strings.HasPrefix(key, opts.Prefix) {
			keys = append(keys, key)
		}
	}
	f.mu.Unlock()
	sort.Strings(keys)

	ch := make(chan minio.ObjectInfo)
	go func() {
		defer close(ch)
		seen := make(map[string]struct{})
		for _, key := range keys {
			select {
			case <-ctx.Done():
				ch <- minio.ObjectInfo{Err: ctx.Err()}
				return
			default:
			}
			if opts.Recursive {
				ch <- minio.ObjectInfo{Key: key}
				continue
			}
			rest := strings.TrimPrefix(key, opts.Prefix)
			if i := strings.Index(rest, "/"); i >= 0 {
				prefix := opts.Prefix + rest[:i+1]
				if _, dup := seen[prefix]; dup {
					continue
				}
				seen[prefix] = struct{}{}
				ch <- minio.ObjectInfo{Key: prefix}
				continue
			}
			ch <- minio.ObjectInfo{Key: key}
		}
	}()
	return ch
}

func TestScanLocalFiltersMarkersAndRejectsSymlink(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.bin"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, ".done"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, ".complete"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	files, total, err := scanLocal(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].rel != "a.bin" || total != 1 {
		t.Fatalf("scanLocal = %+v total=%d", files, total)
	}

	if _, _, err := scanLocal(filepath.Join(src, "missing")); plugin.CodeOf(err) != plugin.CodeInvalidArgument {
		t.Fatalf("不存在的目录错误 = %v，期望 INVALID_ARGUMENT", err)
	}

	target := filepath.Join(t.TempDir(), "real.bin")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(src, "link.bin")); err != nil {
		t.Skipf("当前环境不支持符号链接: %v", err)
	}
	if _, _, err := scanLocal(src); plugin.CodeOf(err) != plugin.CodeInvalidArgument {
		t.Fatalf("符号链接错误 = %v，期望 INVALID_ARGUMENT", err)
	}
}

// requestCounter 只统计 GET 与 LIST 次数，用来锁住 D25 的枚举成本：多余份的
// 编号来自既有的那一次非递归 LIST，确认提交才按候选多付一次 GET。
type requestCounter struct {
	objectAPI
	mu    sync.Mutex
	gets  int
	lists int
}

func (c *requestCounter) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, minio.ObjectInfo, http.Header, error) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.objectAPI.GetObject(ctx, bucket, object, opts)
}

func (c *requestCounter) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.objectAPI.ListObjects(ctx, bucket, opts)
}

func (c *requestCounter) counts() (gets, lists int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.lists
}

// TestResolveRequiresExactRankSet 锁住 D25 在 S3 侧的三种 rank 集合：
// world_size=3 时 {0,1,2} 完整、{0,1,3} 不完整（缺 2）、{0,1,2,3} 不完整（多出 3）。
func TestResolveRequiresExactRankSet(t *testing.T) {
	cases := []struct {
		name         string
		ranks        []int
		wantFound    bool
		wantMaxExtra int
	}{
		{"集合 {0,1,2} 完整", []int{0, 1, 2}, true, 0},
		{"集合 {0,1,3} 缺 2", []int{0, 1, 3}, false, 3},
		{"集合 {0,1,2,3} 多出 3", []int{0, 1, 2, 3}, false, 3},
		{"多份取最大编号", []int{0, 1, 2, 3, 5}, false, 5},
		{"只缺份没有多余", []int{0, 1}, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			b := testBackend(store)
			for _, rank := range tc.ranks {
				worldSize := 3
				if rank >= worldSize {
					// 多余份来自副本数更大的上一轮，manifest 自身要过校验。
					worldSize = rank + 1
				}
				putTestManifest(t, store, b.prefix, "task", 4, rank, worldSize, 7)
			}
			res, err := resolve(context.Background(), b, plugin.ResolveParams{
				TaskID: "task", Rank: 0, WorldSize: 3, Policy: plugin.Policy{Mode: "step", Step: 4},
			})
			if err != nil {
				t.Fatal(err)
			}
			if res.Found != tc.wantFound || res.Complete != tc.wantFound {
				t.Fatalf("resolve = %+v, want found=%v", res, tc.wantFound)
			}
			if res.MaxExtraRank != tc.wantMaxExtra {
				t.Fatalf("max_extra_rank = %d, want %d", res.MaxExtraRank, tc.wantMaxExtra)
			}
		})
	}
}

// TestResolveExtraRankWithoutManifest 锁住「多余份只在已提交时才算」：
// rank-3/ 只有文件对象、没有 manifest 时是在途半成品，step 仍判完整。
func TestResolveExtraRankWithoutManifest(t *testing.T) {
	store := newFakeStore()
	b := testBackend(store)
	for _, rank := range []int{0, 1, 2} {
		putTestManifest(t, store, b.prefix, "task", 4, rank, 3, 7)
	}
	store.putObject(plugin.FileKey(b.prefix, "task", 4, 3, "a.bin"), []byte("x"))

	res, err := resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: "task", Rank: 0, WorldSize: 3, Policy: plugin.Policy{Mode: "latest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || !res.Complete || res.Step != 4 {
		t.Fatalf("在途多余份不该判不完整: %+v", res)
	}
	if res.MaxExtraRank != 0 {
		t.Fatalf("max_extra_rank = %d, want 0", res.MaxExtraRank)
	}
}

// TestResolveIgnoresNonRankPrefixes 确认 step 目录下的杂项前缀不会被当成
// 多余份：标记文件、临时名与不认识的名字都不参与集合比对（design 10.1 §5）。
func TestResolveIgnoresNonRankPrefixes(t *testing.T) {
	store := newFakeStore()
	b := testBackend(store)
	for _, rank := range []int{0, 1, 2} {
		putTestManifest(t, store, b.prefix, "task", 4, rank, 3, 7)
	}
	stepPrefix := plugin.StepPrefix(b.prefix, "task", 4)
	for _, key := range []string{
		stepPrefix + "/.done",
		stepPrefix + "/.complete/x",
		stepPrefix + "/manifest.json.tmp-1",
		stepPrefix + "/rank-x/manifest.json",
		stepPrefix + "/rank-/a.bin",
		stepPrefix + "/rank-99999999999/manifest.json",
	} {
		store.putObject(key, []byte("junk"))
	}

	res, err := resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: "task", Rank: 0, WorldSize: 3, Policy: plugin.Policy{Mode: "latest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || !res.Complete || res.MaxExtraRank != 0 {
		t.Fatalf("杂项前缀不应影响完整性: %+v", res)
	}
}

// TestResolveExtraRankRequestCost 把 D25 的成本钉住：干净 step 上枚举不多付
// 请求；出现多余候选前缀时按候选各付至多一次确认 GET，LIST 次数不变。
func TestResolveExtraRankRequestCost(t *testing.T) {
	costFor := func(withExtra bool) (gets, lists, maxExtra int) {
		t.Helper()
		base := newFakeStore()
		counter := &requestCounter{objectAPI: base}
		b := testBackend(counter)
		for _, rank := range []int{0, 1, 2} {
			putTestManifest(t, base, b.prefix, "task", 4, rank, 3, 7)
		}
		if withExtra {
			putTestManifest(t, base, b.prefix, "task", 4, 3, 4, 7)
		}
		res, err := resolve(context.Background(), b, plugin.ResolveParams{
			TaskID: "task", Rank: 0, WorldSize: 3, Policy: plugin.Policy{Mode: "latest"},
		})
		if err != nil {
			t.Fatal(err)
		}
		g, l := counter.counts()
		return g, l, res.MaxExtraRank
	}

	// latest 模式：1 次任务根 LIST + 1 次 step LIST + world_size 次 manifest GET。
	cleanGets, cleanLists, cleanExtra := costFor(false)
	if cleanLists != 2 || cleanGets != 3 || cleanExtra != 0 {
		t.Fatalf("干净 step = gets %d lists %d extra %d, want 3/2/0", cleanGets, cleanLists, cleanExtra)
	}

	extraGets, extraLists, extraRank := costFor(true)
	if extraLists != cleanLists {
		t.Fatalf("多余份的检测不应增加 LIST 次数: %d vs %d", extraLists, cleanLists)
	}
	if extraRank != 3 {
		t.Fatalf("max_extra_rank = %d, want 3", extraRank)
	}
	// 确认到多余份就判不完整，期望集合的 manifest 一次都不取：一个候选一次 GET。
	if extraGets != 1 {
		t.Fatalf("多余份确认 = %d 次 GET, want 1", extraGets)
	}
}
