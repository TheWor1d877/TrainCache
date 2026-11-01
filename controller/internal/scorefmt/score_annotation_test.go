package scorefmt

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestDefaultAnnotation pins the Pod score annotation key that both the
// controller-manager and the scheduler default to (docs/7.1 design.md §6.2).
func TestDefaultAnnotation(t *testing.T) {
	if DefaultAnnotation != "traincache.io/node-scores" {
		t.Fatalf("DefaultAnnotation = %q, want traincache.io/node-scores", DefaultAnnotation)
	}
}

// TestEncodeDeterministic asserts encoding is a pure function even though
// S is a map: repeated calls return byte-identical values, because encoding/json
// sorts map keys.
func TestEncodeDeterministic(t *testing.T) {
	value := Value{
		V:    valueVersion,
		Task: "default/train-1",
		G:    12,
		T:    time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC),
		S:    map[string]int32{"node-b": 73, "node-a": 87, "node-c": 0},
	}
	first := mustEncode(t, value)
	for i := 0; i < 50; i++ {
		if got := mustEncode(t, value); got != first {
			t.Fatalf("EncodeValue call %d = %q, want %q", i, got, first)
		}
	}
}

// TestNodeNamesPreserved asserts node names of any length round-trip
// unchanged: the annotation value has no key-size budget to truncate against.
func TestNodeNamesPreserved(t *testing.T) {
	for _, n := range []int{1, 32, 33, 64, 200, 253} {
		name := strings.Repeat("a", n)
		value := Value{
			V:    valueVersion,
			Task: "default/train-1",
			G:    3,
			T:    time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC),
			S:    map[string]int32{name: 50},
		}
		out, err := DecodeValue(mustEncode(t, value))
		if err != nil {
			t.Fatalf("node name length %d: DecodeValue error: %v", n, err)
		}
		if !reflect.DeepEqual(out.S, value.S) {
			t.Fatalf("node name length %d: S = %v, want %v", n, out.S, value.S)
		}
	}
}
