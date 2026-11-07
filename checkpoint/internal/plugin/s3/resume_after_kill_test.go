package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"traincache/checkpoint/internal/plugin"
)

// 本文件覆盖 10.3 覆盖矩阵里的「续传跨进程」一行：续传不只要在同一个进程内生效，
// 还要能跨进程。上一次运行的插件进程被 SIGKILL 之后，已确认分片的状态
// 仍然在磁盘上，下一次运行读到它、跳过这些分片（design 10.1「数据格式」、
// 10.2 D16）。
//
// 这里用磁盘版 objectAPI 替身：它的上传记录放在目录里，因此父子两个
// 进程看到的是同一份远端状态，父进程才能验证「第二次运行没有重传已
// 确认分片」。

const resumeStoreEnv = "TC_RESUME_STORE_DIR"

// TestResumeChildHelper 是子进程入口：它跑一次真实的 save，把第一个分片
// 传上去之后卡住，等父进程 SIGKILL。卡住的分片让父进程有机会看到
// 「已确认分片的状态已落盘、进程却已经死了」这个瞬间。
func TestResumeChildHelper(t *testing.T) {
	dir := os.Getenv(resumeStoreEnv)
	if dir == "" {
		t.Skip("只在子进程里跑")
	}
	store := newDiskStore(dir)
	// 卡在第 2 片：第 1 片已经确认并落盘，后面的一片永远传不完。
	store.hangFromPart = 2
	b := &backend{prefix: "pfx", bucket: "bucket", partSize: minPartSize, concurrency: 1, store: store}
	p := plugin.SaveParams{
		TaskID:    "task",
		Rank:      0,
		WorldSize: 1,
		Step:      1,
		LocalPath: os.Getenv(resumeStoreEnv + "_SRC"),
		StatePath: os.Getenv(resumeStoreEnv + "_STATE"),
	}
	_, err := save(context.Background(), b, p, func(int64, int64) {})
	if err != nil {
		t.Fatalf("子进程 save: %v", err)
	}
}

// TestResumeAcrossProcessKillSkipsConfirmedParts 断言：
//  1. 子进程被 SIGKILL 之后，已确认分片的续传状态仍在磁盘上；
//  2. 第二次运行从该状态继续，已确认分片没有被重传；
//  3. 第二次运行补齐剩余分片并写出 manifest，最终对象内容与源文件一致。
func TestResumeAcrossProcessKillSkipsConfirmedParts(t *testing.T) {
	src := t.TempDir()
	data := bytes.Repeat([]byte("x"), int(minPartSize)*2+1024)
	if err := os.WriteFile(filepath.Join(src, "model.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir()
	stateDir := t.TempDir()

	// 状态文件按源文件的 sha256 命名（design 10.1「数据格式」）。
	sum := sha256.Sum256(data)
	statePath := filepath.Join(stateDir, hex.EncodeToString(sum[:])+".jsonl")

	cmd := exec.Command(os.Args[0], "-test.run=^TestResumeChildHelper$", "-test.v")
	cmd.Env = append(os.Environ(),
		resumeStoreEnv+"="+storeDir,
		resumeStoreEnv+"_SRC="+src,
		resumeStoreEnv+"_STATE="+stateDir,
	)
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程: %v", err)
	}

	// 等第一个分片被确认为止：状态文件里有头记录加一条分片记录。
	if err := waitForStateParts(t, statePath, 1); err != nil {
		files, _ := filepath.Glob(filepath.Join(stateDir, "*"))
		t.Fatalf("%v\n状态目录 %s 内容 %v\n子进程输出:\n%s", err, stateDir, files, out.String())
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("杀子进程: %v", err)
	}
	_ = cmd.Wait()
	if !strings.Contains(out.String(), "TestResumeChildHelper") {
		t.Fatalf("子进程没有跑到用例:\n%s", out.String())
	}

	recs, err := plugin.LoadParts(statePath)
	if err != nil {
		t.Fatalf("LoadParts() error = %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("子进程被杀后状态记录数 = %d，期望 2（头记录 + 1 条分片）", len(recs))
	}
	if recs[0].Kind != kindUpload {
		t.Fatalf("首行不是上传头: %+v", recs[0])
	}
	if recs[1].Part != 1 || recs[1].ETag == "" {
		t.Fatalf("第一条分片记录 = %+v，期望 part=1 且带 etag", recs[1])
	}

	// 第二次运行：同一个后端状态目录，同一份续传状态。
	store := newDiskStore(storeDir)
	b := testBackend(store)
	p := plugin.SaveParams{
		TaskID:    "task",
		Rank:      0,
		WorldSize: 1,
		Step:      1,
		LocalPath: src,
		StatePath: stateDir,
	}
	if _, err := save(context.Background(), b, p, func(int64, int64) {}); err != nil {
		t.Fatalf("续传保存失败: %v", err)
	}

	calls := store.partCallsByNumber()
	if calls[1] != 1 {
		t.Fatalf("已确认的第 1 片被传了 %d 次，期望 1 次（第二次运行必须跳过）", calls[1])
	}
	if calls[2] != 1 || calls[3] != 1 {
		t.Fatalf("剩余分片上传次数 = %v，期望各 1 次", calls)
	}
	if got := store.newUploadCalls(); got != 1 {
		t.Fatalf("新建 multipart 上传次数 = %d，期望 1（续传必须复用原来的 upload id）", got)
	}

	// 最终对象内容与源文件一致：续传拼出来的分片没有错位。
	got, err := b.getManifest(context.Background(), plugin.ManifestKey(b.prefix, p.TaskID, p.Step, p.Rank))
	if err != nil {
		t.Fatalf("读 manifest: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("manifest 为空")
	}
	obj, _, _, err := store.GetObject(context.Background(), b.bucket, plugin.FileKey(b.prefix, p.TaskID, p.Step, p.Rank, "model.bin"), minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("读最终对象: %v", err)
	}
	body, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("读最终对象内容: %v", err)
	}
	if !bytes.Equal(body, data) {
		t.Fatalf("最终对象长度 = %d，期望 %d", len(body), len(data))
	}

	// 保存成功后状态目录被清空（design 10.1「数据格式」）。
	leftover, _ := filepath.Glob(filepath.Join(stateDir, "*.jsonl"))
	if len(leftover) != 0 {
		t.Fatalf("成功后仍残留续传状态文件: %v", leftover)
	}
}

// waitForStateParts 等续传状态文件里出现 n 条分片记录（不含头记录）。
func waitForStateParts(t *testing.T, path string, n int) error {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		recs, err := plugin.LoadParts(path)
		if err == nil {
			parts := 0
			for _, rec := range recs {
				if rec.Kind == "" {
					parts++
				}
			}
			if parts >= n {
				return nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("等待续传状态出现 %d 条分片记录超时", n)
}

// diskStore 是落在磁盘上的 objectAPI 替身：上传的每个分片写成一个文件，
// 对象内容也写文件，因此父子进程共享同一份「远端」状态。调用次数记在
// calls.log 里，用来断言第二次运行没有重传已确认分片。
type diskStore struct {
	dir string
	mu  sync.Mutex
	// logMu 只护住 calls.log；状态锁 mu 不与它嵌套，避免自锁。
	logMu sync.Mutex
	// hangFromPart 大于 0 时，从该分片号起上传会一直卡住，用来制造
	// 「进程被杀在中途」的现场。只有子进程用它。
	hangFromPart int
}

type diskUpload struct {
	Key   string                     `json:"key"`
	Parts map[string]json.RawMessage `json:"parts"`
}

type diskPart struct {
	ETag string `json:"etag"`
	Size int64  `json:"size"`
}

func newDiskStore(dir string) *diskStore {
	return &diskStore{dir: dir}
}

func (d *diskStore) objectPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(d.dir, "objects", hex.EncodeToString(sum[:])+".bin")
}

func (d *diskStore) uploadDir(id string) string {
	return filepath.Join(d.dir, "uploads", id)
}

func (d *diskStore) uploadMeta(id string) diskUpload {
	meta := diskUpload{Key: "", Parts: map[string]json.RawMessage{}}
	b, err := os.ReadFile(filepath.Join(d.uploadDir(id), "meta.json"))
	if err == nil {
		_ = json.Unmarshal(b, &meta)
	}
	if meta.Parts == nil {
		meta.Parts = map[string]json.RawMessage{}
	}
	return meta
}

func (d *diskStore) saveUploadMeta(id string, meta diskUpload) {
	_ = os.MkdirAll(d.uploadDir(id), 0o755)
	b, err := json.Marshal(meta)
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(d.uploadDir(id), "meta.json"), b, 0o644)
}

func (d *diskStore) logCall(format string, args ...any) {
	d.logMu.Lock()
	defer d.logMu.Unlock()
	_ = os.MkdirAll(d.dir, 0o755)
	f, err := os.OpenFile(filepath.Join(d.dir, "calls.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = fmt.Fprintf(f, format+"\n", args...)
}

func (d *diskStore) calls() []string {
	d.logMu.Lock()
	defer d.logMu.Unlock()
	b, err := os.ReadFile(filepath.Join(d.dir, "calls.log"))
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}

// partCallsByNumber 统计每个分片号被上传的次数。
func (d *diskStore) partCallsByNumber() map[int]int {
	out := map[int]int{}
	for _, line := range d.calls() {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "part" {
			continue
		}
		n, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		out[n]++
	}
	return out
}

// newUploadCalls 统计新建 multipart 上传的次数。
func (d *diskStore) newUploadCalls() int {
	n := 0
	for _, line := range d.calls() {
		if strings.HasPrefix(line, "newupload ") {
			n++
		}
	}
	return n
}

func (d *diskStore) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.PutObjectOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	id := "upload-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	meta := diskUpload{Key: object, Parts: map[string]json.RawMessage{}}
	d.saveUploadMeta(id, meta)
	d.logCall("newupload %s", object)
	return id, nil
}

func (d *diskStore) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error) {
	if err := ctx.Err(); err != nil {
		return minio.ObjectPart{}, err
	}
	if d.hangFromPart > 0 && partID >= d.hangFromPart {
		// 卡住不放，父进程会 SIGKILL 掉这个进程。
		for {
			time.Sleep(time.Second)
		}
	}
	buf, err := io.ReadAll(data)
	if err != nil {
		return minio.ObjectPart{}, err
	}
	sum := md5.Sum(buf)
	etag := hex.EncodeToString(sum[:])

	d.mu.Lock()
	defer d.mu.Unlock()
	meta := d.uploadMeta(uploadID)
	if meta.Key == "" {
		return minio.ObjectPart{}, minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchUpload"}
	}
	if err := os.MkdirAll(d.uploadDir(uploadID), 0o755); err != nil {
		return minio.ObjectPart{}, err
	}
	name := filepath.Join(d.uploadDir(uploadID), fmt.Sprintf("part-%d", partID))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return minio.ObjectPart{}, err
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return minio.ObjectPart{}, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return minio.ObjectPart{}, err
	}
	_ = f.Close()

	raw, _ := json.Marshal(diskPart{ETag: etag, Size: int64(len(buf))})
	meta.Parts[strconv.Itoa(partID)] = raw
	d.saveUploadMeta(uploadID, meta)
	d.logCall("part %s %d", object, partID)
	return minio.ObjectPart{PartNumber: partID, ETag: etag, Size: int64(len(buf))}, nil
}

func (d *diskStore) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (minio.ListObjectPartsResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	meta := d.uploadMeta(uploadID)
	if meta.Key == "" {
		return minio.ListObjectPartsResult{}, minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchUpload"}
	}
	var parts []minio.ObjectPart
	for key, raw := range meta.Parts {
		var p diskPart
		if err := json.Unmarshal(raw, &p); err != nil {
			continue
		}
		n, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		parts = append(parts, minio.ObjectPart{PartNumber: n, ETag: p.ETag, Size: p.Size})
	}
	return minio.ListObjectPartsResult{ObjectParts: parts}, nil
}

func (d *diskStore) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	meta := d.uploadMeta(uploadID)
	if meta.Key == "" {
		return minio.UploadInfo{}, minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchUpload"}
	}
	var all []byte
	for _, p := range parts {
		b, err := os.ReadFile(filepath.Join(d.uploadDir(uploadID), fmt.Sprintf("part-%d", p.PartNumber)))
		if err != nil {
			return minio.UploadInfo{}, err
		}
		all = append(all, b...)
	}
	if err := os.MkdirAll(filepath.Dir(d.objectPath(object)), 0o755); err != nil {
		return minio.UploadInfo{}, err
	}
	if err := os.WriteFile(d.objectPath(object), all, 0o644); err != nil {
		return minio.UploadInfo{}, err
	}
	etag, ok := expectedCompositeETag(parts)
	if !ok {
		etag = "deadbeef-" + strconv.Itoa(len(parts))
	}
	d.logCall("complete %s", object)
	return minio.UploadInfo{ETag: etag, Size: int64(len(all))}, nil
}

func (d *diskStore) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return os.RemoveAll(d.uploadDir(uploadID))
}

func (d *diskStore) RemoveObject(ctx context.Context, bucket, object string, opts minio.RemoveObjectOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := os.Remove(d.objectPath(object)); err != nil && !os.IsNotExist(err) {
		return err
	}
	d.logCall("remove %s", object)
	return nil
}

func (d *diskStore) PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, md5Base64, sha256Hex string, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	if err := ctx.Err(); err != nil {
		return minio.UploadInfo{}, err
	}
	buf, err := io.ReadAll(data)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(d.objectPath(object)), 0o755); err != nil {
		return minio.UploadInfo{}, err
	}
	if err := os.WriteFile(d.objectPath(object), buf, 0o644); err != nil {
		return minio.UploadInfo{}, err
	}
	sum := md5.Sum(buf)
	d.logCall("put %s", object)
	return minio.UploadInfo{ETag: hex.EncodeToString(sum[:]), Size: int64(len(buf))}, nil
}

func (d *diskStore) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, minio.ObjectInfo, http.Header, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, err := os.ReadFile(d.objectPath(object))
	if err != nil {
		return nil, minio.ObjectInfo{}, nil, minio.ErrorResponse{
			StatusCode: http.StatusNotFound, Code: "NoSuchKey", Key: object, Message: "not found",
		}
	}
	return io.NopCloser(bytes.NewReader(b)), minio.ObjectInfo{Key: object, Size: int64(len(b))}, nil, nil
}

func (d *diskStore) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)
	go func() {
		defer close(ch)
		_ = ctx
	}()
	return ch
}
