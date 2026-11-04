package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"traincache/checkpoint/internal/plugin"
)

// TestS3RoundTrip 需要真实 MinIO。未设置 TC_S3_* 时跳过，普通 go test ./...
// 仍能通过。
func TestS3RoundTrip(t *testing.T) {
	b := integrationBackend(t)
	task := "task-roundtrip"
	src := t.TempDir()

	big := bytes.Repeat([]byte("0123456789abcdef"), (11<<20)/16)
	if err := os.WriteFile(filepath.Join(src, "model.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "config.json"), []byte(`{"lr":0.1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "empty.bin"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, ".done"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", ".complete"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	p := plugin.SaveParams{
		TaskID: task, Rank: 0, WorldSize: 1, Step: 1,
		LocalPath: src, StatePath: t.TempDir(),
	}
	res, err := save(context.Background(), b, p, func(int64, int64) {})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if res.Files != 3 {
		t.Fatalf("保存文件数 = %d，期望 3", res.Files)
	}

	rr, err := resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: task, Rank: 0, WorldSize: 1, Policy: plugin.Policy{Mode: "latest"},
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !rr.Found || !rr.Complete || rr.Step != 1 {
		t.Fatalf("resolve = %+v，期望 step 1 完整", rr)
	}

	target := filepath.Join(t.TempDir(), "restored")
	tr, err := restore(context.Background(), b, plugin.RestoreParams{
		TaskID: task, Rank: 0, WorldSize: 1, Step: 1,
		TargetPath: target, StatePath: t.TempDir(),
	}, func(int64, int64) {})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if tr.Files != 3 {
		t.Fatalf("恢复文件数 = %d，期望 3", tr.Files)
	}

	got, err := os.ReadFile(filepath.Join(target, "model.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, big) {
		t.Fatal("model.bin 内容不一致")
	}
	if _, err := os.Stat(filepath.Join(target, "sub", "config.json")); err != nil {
		t.Fatalf("config.json 未恢复: %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "empty.bin")); err != nil {
		t.Fatalf("empty.bin 未恢复: %v", err)
	}
	for _, marker := range []string{".done", filepath.Join("sub", ".complete")} {
		if _, err := os.Stat(filepath.Join(target, marker)); !os.IsNotExist(err) {
			t.Fatalf("目标中不应出现 %s: %v", marker, err)
		}
	}

	// 目标已存在必须拒绝，避免覆盖训练程序还没消费完的目录。
	if _, err := restore(context.Background(), b, plugin.RestoreParams{
		TaskID: task, Rank: 0, WorldSize: 1, Step: 1,
		TargetPath: target, StatePath: t.TempDir(),
	}, func(int64, int64) {}); plugin.CodeOf(err) != plugin.CodeInvalidArgument {
		t.Fatalf("重复恢复错误 = %v，期望 INVALID_ARGUMENT", err)
	}

	// 重复保存同一 step：manifest 字节应完全一致（created_at 沿用首次值）。
	first, err := b.getManifest(context.Background(), plugin.ManifestKey(b.prefix, task, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	p.StatePath = t.TempDir()
	if _, err := save(context.Background(), b, p, func(int64, int64) {}); err != nil {
		t.Fatalf("重复 save: %v", err)
	}
	second, err := b.getManifest(context.Background(), plugin.ManifestKey(b.prefix, task, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("重复保存 manifest 不一致:\n%s\n%s", first, second)
	}
}

// TestS3ResumeAfterCancel 在第一次上传途中取消 context，第二次保存应复用
// 已确认分片，只补传剩余分片。
func TestS3ResumeAfterCancel(t *testing.T) {
	b := integrationBackend(t)
	task := "task-resume"
	src := t.TempDir()
	data := bytes.Repeat([]byte("r"), int(minPartSize)*2+1024)
	if err := os.WriteFile(filepath.Join(src, "model.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	counting := &countingStore{objectAPI: b.store}
	b.store = counting
	p := plugin.SaveParams{
		TaskID: task, Rank: 0, WorldSize: 1, Step: 1,
		LocalPath: src, StatePath: t.TempDir(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	_, err := save(ctx, b, p, func(done, total int64) {
		// 哈希阶段 done 最多等于 total/2；上传第一片后才会越过。
		if done > total/2 {
			once.Do(cancel)
		}
	})
	if err == nil {
		t.Fatal("中断的保存应返回错误")
	}
	first := counting.parts()
	if first < 1 {
		t.Fatalf("中断前没有确认任何分片: %d", first)
	}
	stateFiles, _ := filepath.Glob(filepath.Join(p.StatePath, "*.jsonl"))
	if len(stateFiles) != 1 {
		t.Fatalf("中断后状态文件数 = %d，期望 1", len(stateFiles))
	}

	counting.reset()
	if _, err := save(context.Background(), b, p, func(int64, int64) {}); err != nil {
		t.Fatalf("续传保存: %v", err)
	}
	second := counting.parts()
	if second >= 3 {
		t.Fatalf("续传重传了 %d 片，期望少于 3", second)
	}
	if first+second < 3 {
		t.Fatalf("首次 %d 片 + 续传 %d 片 < 3", first, second)
	}
	stateFiles, _ = filepath.Glob(filepath.Join(p.StatePath, "*.jsonl"))
	if len(stateFiles) != 0 {
		t.Fatalf("成功后仍残留状态文件: %v", stateFiles)
	}

	rr, err := resolve(context.Background(), b, plugin.ResolveParams{
		TaskID: task, Rank: 0, WorldSize: 1,
		Policy: plugin.Policy{Mode: "step", Step: 1},
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !rr.Found || !rr.Complete {
		t.Fatalf("续传完成后 resolve = %+v", rr)
	}
}

func integrationBackend(t *testing.T) *backend {
	t.Helper()
	endpoint := os.Getenv("TC_S3_ENDPOINT")
	access := os.Getenv("TC_S3_ACCESS_KEY")
	secret := os.Getenv("TC_S3_SECRET_KEY")
	bucket := os.Getenv("TC_S3_BUCKET")
	if endpoint == "" || access == "" || secret == "" || bucket == "" {
		t.Skip("未设置 TC_S3_ENDPOINT/TC_S3_ACCESS_KEY/TC_S3_SECRET_KEY/TC_S3_BUCKET")
	}
	t.Setenv(accessKeyEnv, access)
	t.Setenv(secretKeyEnv, secret)

	prefix := fmt.Sprintf("checkpoint-it/%d", time.Now().UnixNano())
	raw, err := json.Marshal(map[string]any{
		"endpoint":    endpoint,
		"bucket":      bucket,
		"prefix":      prefix,
		"secure":      false,
		"part_size":   minPartSize,
		"concurrency": 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := loadBackend(raw)
	if err != nil {
		t.Fatalf("loadBackend: %v", err)
	}

	store := b.store
	t.Cleanup(func() { cleanupPrefix(bucket, prefix, store) })
	return b
}

func cleanupPrefix(bucket, prefix string, store objectAPI) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	remover, ok := store.(interface {
		RemoveObject(ctx context.Context, bucket, object string, opts minio.RemoveObjectOptions) error
	})
	if !ok {
		return
	}
	for obj := range store.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return
		}
		_ = remover.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
	}
}

// countingStore 只统计 PutObjectPart 次数，用来判断续传是否真的跳过了
// 已确认分片。其余方法由内嵌接口转发。
type countingStore struct {
	objectAPI
	mu    sync.Mutex
	count int
}

func (c *countingStore) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, opts minio.PutObjectPartOptions) (minio.ObjectPart, error) {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
	return c.objectAPI.PutObjectPart(ctx, bucket, object, uploadID, partID, data, size, opts)
}

func (c *countingStore) parts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *countingStore) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = 0
}
