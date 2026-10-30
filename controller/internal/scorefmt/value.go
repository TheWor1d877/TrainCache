// Package scorefmt encodes and decodes the Pod score annotation value
// (docs/7.1 design.md §6.2, D49).
//
// The annotation lives on the training Pod itself and is written in the same
// patch that removes its scheduling gate, so the scheduler reads the snapshot
// from the Pod object it is already evaluating. The package has no Kubernetes
// imports so that controller and scheduler binaries share exactly one
// implementation.
package scorefmt

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	// DefaultAnnotation is the default Pod annotation key.
	DefaultAnnotation = "traincache.io/node-scores"

	// valueVersion is the only supported annotation value format version.
	valueVersion = 1
)

// Score bounds for entries in Value.S.
const (
	minScore = int32(0)
	maxScore = int32(100)
)

// Value is the JSON payload stored in the Pod score annotation.
//
// The wire format is frozen by docs/7.1 design.md §6.2:
//
//	{"v":1,"task":"default/train-1","g":12,"t":"2026-10-20T08:00:00Z","s":{"node-a":87}}
//
// V is the format version, Task is "<namespace>/<name>", G is the WarmupTask
// generation at write time, T is the RFC3339 UTC write instant, and S maps
// preheat node names to scores in [0,100].
type Value struct {
	V    int              `json:"v"`
	Task string           `json:"task"`
	G    int64            `json:"g"`
	T    time.Time        `json:"t"`
	S    map[string]int32 `json:"s"`
}

// EncodeValue validates v and returns its single-line UTF-8 JSON encoding with
// no trailing newline. Timestamps are normalized to UTC so controller and
// scheduler always observe the RFC3339 UTC form from §6.2.
func EncodeValue(v Value) (string, error) {
	if err := v.validate(); err != nil {
		return "", fmt.Errorf("scorefmt: encode value: %w", err)
	}
	v.T = v.T.UTC()
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("scorefmt: encode value: %w", err)
	}
	return string(b), nil
}

// DecodeValue parses an annotation value produced by EncodeValue. It rejects
// empty input, malformed JSON, unsupported versions, an empty task, an empty
// node name, and scores outside [0,100].
func DecodeValue(raw string) (Value, error) {
	if raw == "" {
		return Value{}, fmt.Errorf("scorefmt: decode value: empty input")
	}
	var v Value
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return Value{}, fmt.Errorf("scorefmt: decode value: %w", err)
	}
	if err := v.validate(); err != nil {
		return Value{}, fmt.Errorf("scorefmt: decode value: %w", err)
	}
	return v, nil
}

// Fresh reports whether v is usable at now for a score TTL. A zero T is never
// fresh; otherwise the entry is fresh while now-T <= ttl, so the TTL boundary
// itself is still fresh.
func Fresh(v Value, now time.Time, ttl time.Duration) bool {
	if v.T.IsZero() {
		return false
	}
	return now.Sub(v.T) <= ttl
}

func (v Value) validate() error {
	if v.V != valueVersion {
		return fmt.Errorf("unsupported version %d, want %d", v.V, valueVersion)
	}
	if v.Task == "" {
		return fmt.Errorf("task must not be empty")
	}
	for node, score := range v.S {
		if node == "" {
			return fmt.Errorf("node name must not be empty")
		}
		if score < minScore || score > maxScore {
			return fmt.Errorf("node %q score %d out of range %d..%d", node, score, minScore, maxScore)
		}
	}
	return nil
}
