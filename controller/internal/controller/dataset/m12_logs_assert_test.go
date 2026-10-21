package dataset

// M12 log assertions for docs/8.1 design.md §9.3/§9.4. The tests assert
// structured JSON output from the real reconcile path, including the rule
// that Secret credential values never reach the log stream.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"traincache/controller/internal/storage"
	"traincache/core/objectstore"
)

// §9.3 快照/存储校验失败: storage factory failure is Warn with dataset/err.
func TestM12DatasetStorageUnavailableLogFields(t *testing.T) {
	ds := newDataSet()
	env := newTestEnv(t, []client.Object{ds}, &fakeFactory{err: storage.ErrSecretNotFound}, nil)

	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); !errors.Is(err, storage.ErrSecretNotFound) {
		t.Fatalf("Reconcile err = %v, want ErrSecretNotFound", err)
	}

	line := findLogLine(t, env.logLines(t), "dataset storage unavailable")
	if line["level"] != "WARN" {
		t.Fatalf("level = %v, want WARN", line["level"])
	}
	for _, field := range []string{"dataset", "namespace", "generation", "request_id", "reason", "err"} {
		if _, ok := line[field]; !ok {
			t.Fatalf("storage log missing %q: %#v", field, line)
		}
	}
}

// m12CredentialFactory proves the secret really reached the storage layer and
// returns a credential-free failure, mirroring the production error shape.
type m12CredentialFactory struct {
	accessKey string
	secretKey string
	err       error
}

func (f *m12CredentialFactory) Store(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	ref corev1.LocalObjectReference,
	endpoint, bucket string,
) (objectstore.Store, error) {
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, secret); err != nil {
		return nil, err
	}
	f.accessKey = string(secret.Data[storage.SecretKeyAccessKey])
	f.secretKey = string(secret.Data[storage.SecretKeySecretKey])
	return nil, f.err
}

func (f *m12CredentialFactory) Close() error { return nil }

// §9.4 脱敏: Secret/accessKey/secretKey values never appear in controller logs.
func TestM12DatasetLogsRedactSecretCredentials(t *testing.T) {
	const (
		accessKey = "M12-ACCESS-KEY-SENTINEL"
		secretKey = "M12-SECRET-KEY-SENTINEL"
	)
	ds := newDataSet()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "creds"},
		Data: map[string][]byte{
			storage.SecretKeyAccessKey: []byte(accessKey),
			storage.SecretKeySecretKey: []byte(secretKey),
		},
	}
	factory := &m12CredentialFactory{
		err: fmt.Errorf("%w: object store rejected the credentials", storage.ErrStoreUnavailable),
	}
	env := newTestEnv(t, []client.Object{ds, secret}, factory, nil)

	if _, err := env.reconciler.Reconcile(context.Background(), requestFor(ds)); err == nil {
		t.Fatal("Reconcile succeeded, want storage failure")
	}

	// Guard the guard: the secret values must have been read by the factory,
	// otherwise this test would pass without exercising a credential path.
	if factory.accessKey != accessKey || factory.secretKey != secretKey {
		t.Fatalf("factory saw credentials %q/%q, want the sentinel values", factory.accessKey, factory.secretKey)
	}
	findLogLine(t, env.logLines(t), "dataset storage unavailable")

	raw := env.logs.String()
	for _, value := range []string{accessKey, secretKey} {
		if strings.Contains(raw, value) {
			t.Fatalf("log output leaked credential value %q:\n%s", value, raw)
		}
	}
}
