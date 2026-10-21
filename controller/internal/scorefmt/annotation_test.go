package scorefmt

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func wantHash(ns, name string) string {
	sum := sha256.Sum256([]byte(ns + "/" + name))
	return hex.EncodeToString(sum[:])[:12]
}

func keyNamePart(key string) string {
	return key[strings.LastIndex(key, "/")+1:]
}

func TestKeyRoundTripAndStableHash(t *testing.T) {
	const (
		ns   = "default"
		name = "train-1"
	)
	key := Key(ns, name)
	want := DefaultPrefix + "-train-1-" + wantHash(ns, name)
	if key != want {
		t.Fatalf("Key() = %q, want %q", key, want)
	}

	np, h, ok := ParseKey(key)
	if !ok {
		t.Fatalf("ParseKey(%q) failed", key)
	}
	if np != "train-1" {
		t.Fatalf("namePrefix = %q, want train-1", np)
	}
	if h != wantHash(ns, name) {
		t.Fatalf("hash12 = %q, want %q", h, wantHash(ns, name))
	}
	if got := keyNamePart(key); got != "scores-train-1-"+wantHash(ns, name) {
		t.Fatalf("key name part = %q", got)
	}
}

func TestKeyTruncation(t *testing.T) {
	long := strings.Repeat("a", 40)
	key := Key("ns", long)
	np, _, ok := ParseKey(key)
	if !ok {
		t.Fatalf("ParseKey(%q) failed", key)
	}
	if len(np) != 32 {
		t.Fatalf("namePrefix length = %d, want 32", len(np))
	}
	if np != strings.Repeat("a", 32) {
		t.Fatalf("namePrefix = %q", np)
	}

	// Trailing '-' and '.' are removed after truncation.
	name := strings.Repeat("b", 30) + "-."
	key = Key("ns", name)
	np, _, ok = ParseKey(key)
	if !ok {
		t.Fatalf("ParseKey(%q) failed", key)
	}
	if strings.HasSuffix(np, "-") || strings.HasSuffix(np, ".") {
		t.Fatalf("namePrefix %q still has a trimmed suffix", np)
	}
}

func TestKeyNamePartLength(t *testing.T) {
	names := []string{"a", strings.Repeat("x", 32), strings.Repeat("y", 33), strings.Repeat("z", 200)}
	for _, name := range names {
		key := Key("namespace", name)
		if got := len(keyNamePart(key)); got > 52 {
			t.Fatalf("key %q name part length = %d, want <= 52", key, got)
		}
	}
}

func TestDifferentNamespacesDoNotCollide(t *testing.T) {
	const name = "train-1"
	a := Key("ns-a", name)
	b := Key("ns-b", name)

	npA, hashA, okA := ParseKey(a)
	npB, hashB, okB := ParseKey(b)
	if !okA || !okB {
		t.Fatalf("ParseKey failed: %q=%v %q=%v", a, okA, b, okB)
	}
	if npA != npB {
		t.Fatalf("namePrefix differs: %q vs %q", npA, npB)
	}
	if hashA == hashB {
		t.Fatalf("hash collision for different namespaces: %q", hashA)
	}
	if a == b {
		t.Fatalf("keys collide: %q", a)
	}
}

func TestSamePrefixDifferentHashDoNotCollide(t *testing.T) {
	prefix := strings.Repeat("q", 32)
	a := Key("ns", prefix+"-one")
	b := Key("ns", prefix+"-two")

	npA, hashA, okA := ParseKey(a)
	npB, hashB, okB := ParseKey(b)
	if !okA || !okB {
		t.Fatalf("ParseKey failed")
	}
	if npA != npB {
		t.Fatalf("expected identical 32-byte namePrefix, got %q vs %q", npA, npB)
	}
	if hashA == hashB || a == b {
		t.Fatalf("keys collide for different full names: %q vs %q", a, b)
	}
}

func TestParseKeyRejectsMalformed(t *testing.T) {
	goodHash := wantHash("ns", "x")
	cases := []string{
		"",
		"other-prefix-x-" + goodHash,
		DefaultPrefix + "-x",
		DefaultPrefix + "-x-" + goodHash[:11],
		DefaultPrefix + "-x-" + strings.ToUpper(goodHash),
		DefaultPrefix + "-x-" + goodHash + "-extra",
		DefaultPrefix + "-" + strings.Repeat("a", 33) + "-" + goodHash,
		DefaultPrefix + "-x.-" + goodHash,
	}
	for _, key := range cases {
		if _, _, ok := ParseKey(key); ok {
			t.Errorf("ParseKey(%q) = ok, want failure", key)
		}
	}
}

func TestKeyWithCustomPrefix(t *testing.T) {
	const prefix = "example.io/scores"
	key := KeyWithPrefix(prefix, "ns", "task")
	if !strings.HasPrefix(key, prefix+"-task-") {
		t.Fatalf("KeyWithPrefix() = %q, want prefix %q", key, prefix)
	}
	if _, _, ok := ParseKey(key); ok {
		t.Fatalf("ParseKey accepted a custom-prefix key")
	}
	np, h, ok := parseKey(prefix, key)
	if !ok || np != "task" || h != wantHash("ns", "task") {
		t.Fatalf("parseKey(custom prefix) = %q, %q, %v", np, h, ok)
	}
}
