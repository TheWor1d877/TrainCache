// Package scorefmt encodes and decodes the per-WarmupTask Node annotation key.
//
// The key format is frozen by D27:
//
//	<prefix>-<namePrefix>-<hash12>
//
// where namePrefix is the task name truncated to 32 bytes with trailing '-'
// and '.' removed, and hash12 is the first 12 lowercase hex characters of
// sha256(namespace + "/" + name). The part after the final '/' is at most 52
// characters (7 + 32 + 1 + 12), below Kubernetes' 63-character key limit.
//
// The package has no Kubernetes imports so that controller and scheduler
// binaries share exactly one implementation.
package scorefmt

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// DefaultPrefix is the default annotation key prefix.
	DefaultPrefix = "traincache.io/scores"

	maxNamePrefix = 32
	hashLength    = 12
)

// Key returns the annotation key for a task using DefaultPrefix.
func Key(namespace, name string) string {
	return KeyWithPrefix(DefaultPrefix, namespace, name)
}

// KeyWithPrefix returns the annotation key for a task using an explicit
// prefix. Controller and scheduler must pass the same configured prefix.
func KeyWithPrefix(prefix, namespace, name string) string {
	return prefix + "-" + namePrefix(name) + "-" + hash12(namespace, name)
}

// ParseKey validates a key produced by Key and returns its namePrefix and
// hash12 components. The full namespace and name cannot be recovered: the
// hash is one-way by design. It returns ok=false for malformed keys.
func ParseKey(key string) (namePrefix, hash12 string, ok bool) {
	return parseKey(DefaultPrefix, key)
}

func parseKey(prefix, key string) (string, string, bool) {
	head := prefix + "-"
	if !strings.HasPrefix(key, head) {
		return "", "", false
	}
	rest := key[len(head):]

	// hash12 contains no '-', so the final '-' separates the prefix from the
	// hash even when namePrefix itself contains hyphens.
	sep := strings.LastIndexByte(rest, '-')
	if sep <= 0 {
		return "", "", false
	}
	np, h := rest[:sep], rest[sep+1:]

	if len(np) > maxNamePrefix || !isLowerHex12(h) {
		return "", "", false
	}
	if strings.HasSuffix(np, "-") || strings.HasSuffix(np, ".") {
		return "", "", false
	}
	return np, h, true
}

func namePrefix(name string) string {
	if len(name) > maxNamePrefix {
		name = name[:maxNamePrefix]
	}
	return strings.TrimRight(name, "-.")
}

func hash12(namespace, name string) string {
	sum := sha256.Sum256([]byte(namespace + "/" + name))
	return hex.EncodeToString(sum[:])[:hashLength]
}

func isLowerHex12(s string) bool {
	if len(s) != hashLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
