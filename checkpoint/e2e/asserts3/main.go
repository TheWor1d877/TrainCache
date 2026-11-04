// Command asserts3 verifies the checkpoint manifest the e2e save produced.
//
// It is used twice by run.sh: --ensure-bucket creates the MinIO bucket during
// setup, and the default mode reads a manifest, checks its task/step/rank
// fields and that files is non-empty, then confirms every listed object exists
// at the size recorded in the manifest.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const maxManifestBytes = 1 << 20

type manifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type manifest struct {
	Version   int            `json:"version"`
	TaskID    string         `json:"task_id"`
	Step      int64          `json:"step"`
	Rank      int            `json:"rank"`
	WorldSize int            `json:"world_size"`
	Files     []manifestFile `json:"files"`
}

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:19000", "S3 endpoint")
	accessKey := flag.String("access-key", "traincache", "access key")
	secretKey := flag.String("secret-key", "traincache-probe", "secret key")
	bucket := flag.String("bucket", "traincache", "bucket name")
	secure := flag.Bool("secure", false, "use TLS")
	key := flag.String("key", "e2e/t1/step-1/rank-0/manifest.json", "manifest object key")
	wantTask := flag.String("task", "t1", "expected task_id")
	wantStep := flag.Int64("step", 1, "expected step")
	wantRank := flag.Int("rank", 0, "expected rank")
	wantWorld := flag.Int("world-size", 1, "expected world_size")
	ensureBucket := flag.Bool("ensure-bucket", false, "create the bucket if absent and exit")
	flag.Parse()

	client, err := minio.New(*endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(*accessKey, *secretKey, ""),
		Secure: *secure,
	})
	if err != nil {
		die("construct client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if *ensureBucket {
		if err := ensureBucketExists(ctx, client, *bucket); err != nil {
			die("ensure bucket: %v", err)
		}
		fmt.Printf("bucket %s ready\n", *bucket)
		return
	}

	obj, err := client.GetObject(ctx, *bucket, *key, minio.GetObjectOptions{})
	if err != nil {
		die("get %s: %v", *key, err)
	}
	defer obj.Close()
	data, err := io.ReadAll(io.LimitReader(obj, maxManifestBytes+1))
	if err != nil {
		die("read %s: %v", *key, err)
	}
	if len(data) > maxManifestBytes {
		die("manifest %s exceeds %d bytes", *key, maxManifestBytes)
	}

	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		die("decode manifest %s: %v", *key, err)
	}
	if m.TaskID != *wantTask || m.Step != *wantStep || m.Rank != *wantRank || m.WorldSize != *wantWorld {
		die("manifest fields = task %q step %d rank %d world %d, want %q %d %d %d",
			m.TaskID, m.Step, m.Rank, m.WorldSize, *wantTask, *wantStep, *wantRank, *wantWorld)
	}
	if len(m.Files) == 0 {
		die("manifest %s has no files", *key)
	}

	filesPrefix := path.Join(path.Dir(*key), "files")
	var total int64
	for _, f := range m.Files {
		if err := validatePath(f.Path); err != nil {
			die("manifest file entry: %v", err)
		}
		objKey := path.Join(filesPrefix, f.Path)
		info, err := client.StatObject(ctx, *bucket, objKey, minio.StatObjectOptions{})
		if err != nil {
			die("stat %s: %v", objKey, err)
		}
		if info.Size != f.Size {
			die("%s size = %d, manifest says %d", objKey, info.Size, f.Size)
		}
		total += f.Size
	}
	fmt.Printf("manifest ok: task=%s step=%d rank=%d files=%d bytes=%d\n",
		m.TaskID, m.Step, m.Rank, len(m.Files), total)
}

func ensureBucketExists(ctx context.Context, client *minio.Client, bucket string) error {
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		code := minio.ToErrorResponse(err).Code
		if code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
			return nil
		}
		return err
	}
	return nil
}

func validatePath(p string) error {
	if p == "" {
		return fmt.Errorf("empty path")
	}
	if strings.HasPrefix(p, "/") || strings.Contains(p, "\\") {
		return fmt.Errorf("path %q is not a clean relative path", p)
	}
	for _, part := range strings.Split(p, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("path %q is not a clean relative path", p)
		}
	}
	return nil
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "asserts3: "+format+"\n", args...)
	os.Exit(1)
}
