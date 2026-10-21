package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"traincache/core/index"
	"traincache/core/objectstore"
)

func TestResolveSnapshotFromCurrent(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMock()
	putSnapshot(t, store, "ds", "snap-1", true)

	got, err := ResolveSnapshot(ctx, store, "ds", "")
	if err != nil {
		t.Fatalf("ResolveSnapshot: %v", err)
	}
	if got.ID != "snap-1" {
		t.Fatalf("ID = %q, want snap-1", got.ID)
	}
	if got.Manifest.SnapshotID != "snap-1" || len(got.Manifest.Shards) == 0 {
		t.Fatalf("unexpected manifest: %#v", got.Manifest)
	}
}

func TestResolveSnapshotExplicitID(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMock()
	putSnapshot(t, store, "ds", "snap-2", false)

	got, err := ResolveSnapshot(ctx, store, "ds", "snap-2")
	if err != nil {
		t.Fatalf("ResolveSnapshot: %v", err)
	}
	if got.ID != "snap-2" {
		t.Fatalf("ID = %q, want snap-2", got.ID)
	}
}

func TestResolveSnapshotClassifiedErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("missing CURRENT", func(t *testing.T) {
		_, err := ResolveSnapshot(ctx, objectstore.NewMock(), "ds", "")
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("err = %v, want ErrSnapshotNotFound", err)
		}
	})

	t.Run("missing manifest", func(t *testing.T) {
		store := objectstore.NewMock()
		put(t, store, index.CurrentKey("ds"), "snap-1 0011223344556677")
		_, err := ResolveSnapshot(ctx, store, "ds", "")
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("err = %v, want ErrSnapshotNotFound", err)
		}
	})

	t.Run("invalid CURRENT", func(t *testing.T) {
		store := objectstore.NewMock()
		put(t, store, index.CurrentKey("ds"), "broken")
		_, err := ResolveSnapshot(ctx, store, "ds", "")
		if !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("err = %v, want ErrInvalidSnapshot", err)
		}
	})

	t.Run("snapshot id mismatch", func(t *testing.T) {
		store := objectstore.NewMock()
		putManifest(t, store, "ds", "snap-1", "other")
		_, err := ResolveSnapshot(ctx, store, "ds", "snap-1")
		if !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("err = %v, want ErrInvalidSnapshot", err)
		}
	})

	t.Run("manifest without shards", func(t *testing.T) {
		store := objectstore.NewMock()
		raw, _ := json.Marshal(index.Manifest{SnapshotID: "snap-1"})
		put(t, store, index.ManifestKey("ds", "snap-1"), string(raw))
		_, err := ResolveSnapshot(ctx, store, "ds", "snap-1")
		if !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("err = %v, want ErrInvalidSnapshot", err)
		}
	})

	t.Run("invalid snapshot id", func(t *testing.T) {
		_, err := ResolveSnapshot(ctx, objectstore.NewMock(), "ds", "../escape")
		if !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("err = %v, want ErrInvalidSnapshot", err)
		}
	})
}

func TestResolveSnapshotStoreUnavailable(t *testing.T) {
	_, err := ResolveSnapshot(context.Background(), failingStore{}, "ds", "")
	if !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("err = %v, want ErrStoreUnavailable", err)
	}
}

func putSnapshot(t *testing.T, store objectstore.Store, dataset, snapID string, current bool) {
	t.Helper()
	putManifest(t, store, dataset, snapID, snapID)
	if current {
		put(t, store, index.CurrentKey(dataset), snapID+" 0011223344556677")
	}
}

func putManifest(t *testing.T, store objectstore.Store, dataset, keySnapID, manifestSnapID string) {
	t.Helper()
	raw, err := json.Marshal(index.Manifest{
		SnapshotID: manifestSnapID,
		Shards:     []index.ShardRef{{Key: "dataset/ds/snapshots/x/shard-0011223344556677.idx", Entries: 1}},
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	put(t, store, index.ManifestKey(dataset, keySnapID), string(raw))
}

func put(t *testing.T, store objectstore.Store, key, content string) {
	t.Helper()
	if _, err := store.Put(context.Background(), key, strings.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

type failingStore struct{}

func (failingStore) Get(context.Context, string, io.Writer) error { return errors.New("boom") }
func (failingStore) Put(context.Context, string, io.Reader, int64) (string, error) {
	return "", errors.New("boom")
}
func (failingStore) Delete(context.Context, string) error { return errors.New("boom") }
func (failingStore) GetETag(context.Context, string) (string, error) {
	return "", errors.New("boom")
}
func (failingStore) PutIfMatch(context.Context, string, io.Reader, int64, string) (string, error) {
	return "", errors.New("boom")
}
func (failingStore) List(context.Context, string) ([]string, error) { return nil, errors.New("boom") }
