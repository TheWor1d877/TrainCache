package storage

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"traincache/core/objectstore"
)

func TestStoreFactoryCachesBySecretResourceVersion(t *testing.T) {
	ctx := context.Background()
	secret := validSecret("default", "creds", "1")
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(secret).Build()

	f := NewFactory()
	var calls int
	var accessKeys []string
	f.newStore = func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error) {
		calls++
		accessKeys = append(accessKeys, accessKey)
		return objectstore.NewMock(), nil
	}
	t.Cleanup(func() { _ = f.Close() })

	ref := corev1.LocalObjectReference{Name: "creds"}
	first, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	second, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket")
	if err != nil {
		t.Fatalf("Store again: %v", err)
	}
	if first != second || calls != 1 {
		t.Fatalf("cache miss: same=%v calls=%d, want same=true calls=1", first == second, calls)
	}

	var updated corev1.Secret
	if err := c.Get(ctx, clientKey("default", "creds"), &updated); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	updated.Data[SecretKeyAccessKey] = []byte("ak2")
	if err := c.Update(ctx, &updated); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	third, err := f.Store(ctx, c, "default", ref, "minio:9000", "bucket")
	if err != nil {
		t.Fatalf("Store after update: %v", err)
	}
	if third == first || calls != 2 {
		t.Fatalf("stale cache entry: same=%v calls=%d, want same=false calls=2", third == first, calls)
	}
	if len(accessKeys) != 2 || accessKeys[0] != "ak" || accessKeys[1] != "ak2" {
		t.Fatalf("credentials seen = %v, want [ak ak2]", accessKeys)
	}
}

func TestStoreFactoryReportsSecretErrors(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "partial"},
			Data: map[string][]byte{SecretKeyAccessKey: []byte("ak")}},
	).Build()
	f := NewFactory()
	t.Cleanup(func() { _ = f.Close() })

	if _, err := f.Store(ctx, c, "default", corev1.LocalObjectReference{Name: "missing"}, "e", "b"); !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("missing secret err = %v, want ErrSecretNotFound", err)
	}
	if _, err := f.Store(ctx, c, "default", corev1.LocalObjectReference{Name: "partial"}, "e", "b"); !errors.Is(err, ErrSecretInvalid) {
		t.Fatalf("partial secret err = %v, want ErrSecretInvalid", err)
	}
	if _, err := f.Store(ctx, c, "default", corev1.LocalObjectReference{}, "e", "b"); !errors.Is(err, ErrSecretInvalid) {
		t.Fatalf("empty ref err = %v, want ErrSecretInvalid", err)
	}
}

func TestStoreFactoryCloseDropsCache(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(validSecret("default", "creds", "1")).Build()
	f := NewFactory()
	var calls int
	f.newStore = func(string, string, string, string, bool) (objectstore.Store, error) {
		calls++
		return objectstore.NewMock(), nil
	}
	ref := corev1.LocalObjectReference{Name: "creds"}
	if _, err := f.Store(ctx, c, "default", ref, "e", "b"); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.Store(ctx, c, "default", ref, "e", "b"); err != nil {
		t.Fatalf("Store after Close: %v", err)
	}
	if calls != 2 {
		t.Fatalf("constructor calls = %d, want 2", calls)
	}
}

func TestStoreFactoryRebuildsOnEndpointChange(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(validSecret("default", "creds", "1")).Build()
	f := NewFactory()
	var calls int
	f.newStore = func(string, string, string, string, bool) (objectstore.Store, error) {
		calls++
		return objectstore.NewMock(), nil
	}
	ref := corev1.LocalObjectReference{Name: "creds"}
	if _, err := f.Store(ctx, c, "default", ref, "a:9000", "bucket"); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := f.Store(ctx, c, "default", ref, "b:9000", "bucket"); err != nil {
		t.Fatalf("Store other endpoint: %v", err)
	}
	if calls != 2 {
		t.Fatalf("constructor calls = %d, want 2", calls)
	}
}

func TestSplitEndpoint(t *testing.T) {
	tests := []struct {
		in     string
		host   string
		secure bool
	}{
		{in: "minio:9000", host: "minio:9000"},
		{in: "http://minio:9000", host: "minio:9000"},
		{in: "https://s3.example.com", host: "s3.example.com", secure: true},
		{in: "https://s3.example.com/", host: "s3.example.com", secure: true},
	}
	for _, tt := range tests {
		host, secure := splitEndpoint(tt.in)
		if host != tt.host || secure != tt.secure {
			t.Fatalf("splitEndpoint(%q) = (%q,%v), want (%q,%v)", tt.in, host, secure, tt.host, tt.secure)
		}
	}
}

func validSecret(namespace, name, rv string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, ResourceVersion: rv},
		Data: map[string][]byte{
			SecretKeyAccessKey: []byte("ak"),
			SecretKeySecretKey: []byte("sk"),
		},
	}
}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}

func clientKey(namespace, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: name}
}
