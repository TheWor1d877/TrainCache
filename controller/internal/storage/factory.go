// Package storage assembles object stores from Secrets and validates published
// snapshots. It is a thin read-only adapter over core/objectstore and
// core/index (docs/7.1 design.md §5.10, decisions D22).
package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"traincache/core/objectstore"
)

// Secret data keys required by DataSet.spec.secretRef.
const (
	SecretKeyAccessKey = "accessKey"
	SecretKeySecretKey = "secretKey"
)

// Factory resolves a DataSet Secret reference to a cached object store.
// Implementations are safe for concurrent use.
type Factory interface {
	Store(ctx context.Context, reader client.Reader, namespace string, ref corev1.LocalObjectReference, endpoint, bucket string) (objectstore.Store, error)
	Close() error
}

type storeEntry struct {
	resourceVersion string
	endpoint        string
	bucket          string
	store           objectstore.Store
}

// StoreFactory is the default Factory. Stores are cached per namespace/name and
// rebuilt when the Secret's resourceVersion or the endpoint/bucket changes.
type StoreFactory struct {
	mu       sync.Mutex
	entries  map[string]*storeEntry
	newStore func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error)
}

var _ Factory = (*StoreFactory)(nil)

// NewFactory returns an empty StoreFactory backed by core/objectstore.
func NewFactory() *StoreFactory {
	return &StoreFactory{
		entries:  make(map[string]*storeEntry),
		newStore: defaultNewStore,
	}
}

func defaultNewStore(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error) {
	return objectstore.NewS3Store(endpoint, accessKey, secretKey, bucket, secure)
}

// Store returns the object store for ref, reading the Secret only when the
// cached entry is absent or stale.
func (f *StoreFactory) Store(ctx context.Context, reader client.Reader, namespace string, ref corev1.LocalObjectReference, endpoint, bucket string) (objectstore.Store, error) {
	if ref.Name == "" {
		return nil, fmt.Errorf("%w: empty secretRef name", ErrSecretInvalid)
	}
	key := namespace + "/" + ref.Name

	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrSecretNotFound, key)
		}
		return nil, fmt.Errorf("%w: get secret %s: %w", ErrStoreUnavailable, key, err)
	}
	accessKey, secretKey := string(secret.Data[SecretKeyAccessKey]), string(secret.Data[SecretKeySecretKey])
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("%w: %s is missing %s/%s", ErrSecretInvalid, key, SecretKeyAccessKey, SecretKeySecretKey)
	}

	f.mu.Lock()
	if e := f.entries[key]; e != nil &&
		e.resourceVersion == secret.ResourceVersion && e.endpoint == endpoint && e.bucket == bucket {
		store := e.store
		f.mu.Unlock()
		return store, nil
	}
	f.mu.Unlock()

	host, secure := splitEndpoint(endpoint)
	store, err := f.newStore(host, accessKey, secretKey, bucket, secure)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}

	f.mu.Lock()
	old := f.entries[key]
	f.entries[key] = &storeEntry{
		resourceVersion: secret.ResourceVersion,
		endpoint:        endpoint,
		bucket:          bucket,
		store:           store,
	}
	f.mu.Unlock()
	closeStore(old)
	return store, nil
}

// Close drops every cached store and closes those that implement io.Closer.
func (f *StoreFactory) Close() error {
	f.mu.Lock()
	entries := f.entries
	f.entries = make(map[string]*storeEntry)
	f.mu.Unlock()

	var errs []error
	for _, e := range entries {
		if err := closeStore(e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func closeStore(e *storeEntry) error {
	if e == nil {
		return nil
	}
	if closer, ok := e.store.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// splitEndpoint accepts both "host:port" and explicit "http(s)://host:port"
// forms; the scheme decides TLS because DataSetSpec has no secure flag.
func splitEndpoint(endpoint string) (host string, secure bool) {
	endpoint = strings.TrimSuffix(endpoint, "/")
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		return strings.TrimPrefix(endpoint, "https://"), true
	case strings.HasPrefix(endpoint, "http://"):
		return strings.TrimPrefix(endpoint, "http://"), false
	default:
		return endpoint, false
	}
}
