package scorefmt

import (
	"strings"
	"testing"
)

// TestM12S18KeyReproducible asserts the encoding is a pure function: repeated
// calls for the same namespace and name return byte-identical keys.
func TestM12S18KeyReproducible(t *testing.T) {
	cases := []struct{ ns, name string }{
		{"default", "train-1"},
		{"ns-a", strings.Repeat("x", 32)},
		{"ns-b", strings.Repeat("y", 200)},
		{"namespace", "task.with.dots-and-hyphens"},
	}
	for _, tc := range cases {
		first := Key(tc.ns, tc.name)
		for i := 0; i < 50; i++ {
			if got := Key(tc.ns, tc.name); got != first {
				t.Fatalf("Key(%q,%q) call %d = %q, want %q", tc.ns, tc.name, i, got, first)
			}
			if got := KeyWithPrefix(DefaultPrefix, tc.ns, tc.name); got != first {
				t.Fatalf("KeyWithPrefix(DefaultPrefix,%q,%q) call %d = %q, want %q",
					tc.ns, tc.name, i, got, first)
			}
		}
	}
}

// TestM12S18TruncationBoundary pins the 32-byte boundary and the <=52 name
// part limit around it.
func TestM12S18TruncationBoundary(t *testing.T) {
	for _, n := range []int{30, 31, 32, 33, 64, 200} {
		name := strings.Repeat("a", n)
		key := Key("namespace", name)
		np, _, ok := ParseKey(key)
		if !ok {
			t.Fatalf("ParseKey(%q) failed for name length %d", key, n)
		}
		want := n
		if want > 32 {
			want = 32
		}
		if len(np) != want || np != strings.Repeat("a", want) {
			t.Fatalf("name length %d: namePrefix = %q (len %d), want %d a's",
				n, np, len(np), want)
		}
		if part := len(key[strings.LastIndex(key, "/")+1:]); part > 52 {
			t.Fatalf("name length %d: key name part = %d chars, want <= 52", n, part)
		}
	}
}

// TestM12S18HashCoversFullName asserts that names sharing the same 32-byte
// prefix still receive distinct hashes, and that namespace participates in the
// hash input.
func TestM12S18HashCoversFullName(t *testing.T) {
	base := strings.Repeat("z", 32)
	a := Key("ns", base+"-alpha")
	b := Key("ns", base+"-beta")
	c := Key("other-ns", base+"-alpha")

	npA, hashA, okA := ParseKey(a)
	npB, hashB, okB := ParseKey(b)
	npC, hashC, okC := ParseKey(c)
	if !okA || !okB || !okC {
		t.Fatalf("ParseKey failed: %v %v %v", okA, okB, okC)
	}
	if npA != npB || npA != npC {
		t.Fatalf("expected identical namePrefix, got %q %q %q", npA, npB, npC)
	}
	if hashA == hashB || a == b {
		t.Fatalf("same-prefix different names collide: %q vs %q", a, b)
	}
	if hashA == hashC || a == c {
		t.Fatalf("different namespaces collide: %q vs %q", a, c)
	}
	for _, h := range []string{hashA, hashB, hashC} {
		if len(h) != 12 || h != strings.ToLower(h) {
			t.Fatalf("hash %q is not 12 lowercase hex chars", h)
		}
	}
}
