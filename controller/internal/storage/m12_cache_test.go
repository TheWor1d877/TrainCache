package storage

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"traincache/core/objectstore"
)

// TestM12StoreCacheInvalidatesOnSecretDeletion fills the gap left by the M2
// tests: a cached Store must not outlive its Secret. The factory re-reads the
// Secret on every call, so deletion surfaces as ErrSecretNotFound instead of a
// stale store.
func TestM12StoreCacheInvalidatesOnSecretDeletion(t *testing.T) {
	ctx := context.Background()
	secret := validSecret("default", "creds", "1")
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(secret).Build()

	f := NewFactory()
	var calls int
	f.newStore = func(string, string, string, string, bool) (objectstore.Store, error) {
		calls++
		return objectstore.NewMock(), nil
	}
	t.Cleanup(func() { _ = f.Close() })

	ref := corev1.LocalObjectReference{Name: "creds"}
	first, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if first == nil || calls != 1 {
		t.Fatalf("first Store = (%v, %d constructor calls), want (store, 1)", first, calls)
	}

	if err := c.Delete(ctx, secret); err != nil {
		t.Fatalf("delete secret: %v", err)
	}
	if _, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket"); !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("Store after secret deletion = %v, want ErrSecretNotFound", err)
	}
	if calls != 1 {
		t.Fatalf("constructor calls = %d, want 1 (no rebuild for a missing Secret)", calls)
	}
}

// TestM12StoreCacheRebuildsOnBucketChange covers the invalidation key that the
// M2 tests do not: endpoint changes are tested, bucket changes are not.
func TestM12StoreCacheRebuildsOnBucketChange(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithObjects(validSecret("default", "creds", "1")).Build()

	f := NewFactory()
	var calls int
	var buckets []string
	f.newStore = func(_ string, _ string, _ string, bucket string, _ bool) (objectstore.Store, error) {
		calls++
		buckets = append(buckets, bucket)
		return objectstore.NewMock(), nil
	}
	t.Cleanup(func() { _ = f.Close() })

	ref := corev1.LocalObjectReference{Name: "creds"}
	first, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket-a")
	if err != nil {
		t.Fatalf("Store bucket-a: %v", err)
	}
	second, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket-b")
	if err != nil {
		t.Fatalf("Store bucket-b: %v", err)
	}
	if first == second || calls != 2 {
		t.Fatalf("bucket change reused store=%v calls=%d, want new store and 2 calls", first == second, calls)
	}
	if len(buckets) != 2 || buckets[0] != "bucket-a" || buckets[1] != "bucket-b" {
		t.Fatalf("buckets seen = %v, want [bucket-a bucket-b]", buckets)
	}
}

// TestM12StoreCacheSeparatesNamespaces guards the namespace/name cache key: the
// same Secret name in two namespaces must not share a Store.
func TestM12StoreCacheSeparatesNamespaces(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		validSecret("default", "creds", "1"),
		validSecret("other", "creds", "1"),
	).Build()

	f := NewFactory()
	var calls int
	f.newStore = func(string, string, string, string, bool) (objectstore.Store, error) {
		calls++
		return objectstore.NewMock(), nil
	}
	t.Cleanup(func() { _ = f.Close() })

	ref := corev1.LocalObjectReference{Name: "creds"}
	first, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket")
	if err != nil {
		t.Fatalf("Store default/creds: %v", err)
	}
	second, err := f.Store(ctx, c, "other", ref, "minio:9000", "bucket")
	if err != nil {
		t.Fatalf("Store other/creds: %v", err)
	}
	if first == second || calls != 2 {
		t.Fatalf("namespace key collision: same=%v calls=%d, want false and 2", first == second, calls)
	}
}
