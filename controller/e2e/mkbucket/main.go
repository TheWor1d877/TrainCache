// Command mkbucket ensures the e2e MinIO bucket exists.
//
// The object-store library has no bucket-management API (v1 is single-bucket),
// so the e2e harness creates the bucket explicitly before trainpublish.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:19000", "S3 endpoint")
	accessKey := flag.String("access-key", "traincache", "access key")
	secretKey := flag.String("secret-key", "traincache-probe", "secret key")
	bucket := flag.String("bucket", "traincache", "bucket name")
	secure := flag.Bool("secure", false, "use TLS")
	flag.Parse()

	client, err := minio.New(*endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(*accessKey, *secretKey, ""),
		Secure: *secure,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkbucket: %v\n", err)
		os.Exit(1)
	}
	err = client.MakeBucket(context.Background(), *bucket, minio.MakeBucketOptions{})
	if err == nil {
		fmt.Printf("created bucket %s\n", *bucket)
		return
	}
	code := minio.ToErrorResponse(err).Code
	if code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" || strings.Contains(err.Error(), "already exists") {
		fmt.Printf("bucket %s already exists\n", *bucket)
		return
	}
	if errors.Is(err, context.Canceled) {
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "mkbucket: %v\n", err)
	os.Exit(1)
}
