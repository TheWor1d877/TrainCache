package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"traincache/core/index"
	"traincache/core/objectstore"
)

// Classified storage errors. The controller uses errors.Is to decide whether a
// failure is a permanent condition or a retryable storage error.
var (
	ErrSecretNotFound   = errors.New("storage: secret not found")
	ErrSecretInvalid    = errors.New("storage: secret is missing credentials")
	ErrStoreUnavailable = errors.New("storage: object store unavailable")
	ErrSnapshotNotFound = errors.New("storage: snapshot not found")
	ErrInvalidSnapshot  = errors.New("storage: invalid snapshot metadata")
)

// Snapshot is the resolved snapshot identity and its manifest.
type Snapshot struct {
	ID       string
	Manifest *index.Manifest
}

// ResolveSnapshot resolves snapshotID (empty = CURRENT) and reads the
// corresponding manifest. Only core/index key layout and Manifest are reused;
// no object is written.
func ResolveSnapshot(ctx context.Context, store objectstore.Store, dataset, snapshotID string) (*Snapshot, error) {
	id := snapshotID
	if id == "" {
		raw, err := getObject(ctx, store, index.CurrentKey(dataset))
		if err != nil {
			if errors.Is(err, objectstore.ErrNotFound) {
				return nil, fmt.Errorf("%w: CURRENT %q", ErrSnapshotNotFound, dataset)
			}
			return nil, fmt.Errorf("%w: %w", ErrStoreUnavailable, err)
		}
		parsed, err := parseCurrent(string(raw))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
		}
		id = parsed
	} else if !validSnapshotID(id) {
		return nil, fmt.Errorf("%w: invalid snapshot id %q", ErrInvalidSnapshot, id)
	}

	raw, err := getObject(ctx, store, index.ManifestKey(dataset, id))
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: manifest for snapshot %q", ErrSnapshotNotFound, id)
		}
		return nil, fmt.Errorf("%w: %w", ErrStoreUnavailable, err)
	}
	var manifest index.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("%w: decode manifest %q: %v", ErrInvalidSnapshot, id, err)
	}
	if manifest.SnapshotID != id {
		return nil, fmt.Errorf("%w: manifest snapshot_id %q does not match %q", ErrInvalidSnapshot, manifest.SnapshotID, id)
	}
	if len(manifest.Shards) == 0 {
		return nil, fmt.Errorf("%w: snapshot %q has no shards", ErrInvalidSnapshot, id)
	}
	return &Snapshot{ID: id, Manifest: &manifest}, nil
}

// parseCurrent parses "<snapID> <manifestFP8>" exactly. The fingerprint is
// shape-checked but not recomputed: xxh3 fingerprint verification stays in
// core/index, which owns the digest implementation.
func parseCurrent(raw string) (string, error) {
	parts := strings.Split(raw, " ")
	if len(parts) != 2 || !validSnapshotID(parts[0]) || !validFP8(parts[1]) {
		return "", fmt.Errorf("invalid CURRENT content %q", raw)
	}
	return parts[0], nil
}

func validFP8(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validSnapshotID(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func getObject(ctx context.Context, store objectstore.Store, key string) ([]byte, error) {
	var buf bytes.Buffer
	if err := store.Get(ctx, key, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
