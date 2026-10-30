package scorefmt

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func mustEncode(t *testing.T, v Value) string {
	t.Helper()
	raw, err := EncodeValue(v)
	if err != nil {
		t.Fatalf("EncodeValue(%+v) error: %v", v, err)
	}
	return raw
}

func TestValueRoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	in := Value{
		V:    valueVersion,
		Task: "default/train-1",
		G:    12,
		T:    ts,
		S:    map[string]int32{"node-a": 87, "node-b": 73},
	}

	raw, err := EncodeValue(in)
	if err != nil {
		t.Fatalf("EncodeValue() error: %v", err)
	}
	if strings.ContainsAny(raw, "\r\n") {
		t.Fatalf("encoded value contains a line break: %q", raw)
	}
	want := `{"v":1,"task":"default/train-1","g":12,"t":"2026-10-20T08:00:00Z","s":{"node-a":87,"node-b":73}}`
	if raw != want {
		t.Fatalf("EncodeValue() = %q, want %q", raw, want)
	}

	out, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue() error: %v", err)
	}
	if out.V != in.V {
		t.Fatalf("V = %d, want %d", out.V, in.V)
	}
	if out.Task != in.Task {
		t.Fatalf("Task = %q, want %q", out.Task, in.Task)
	}
	if out.G != in.G {
		t.Fatalf("G = %d, want %d", out.G, in.G)
	}
	if !out.T.Equal(in.T) {
		t.Fatalf("T = %v, want %v", out.T, in.T)
	}
	if !reflect.DeepEqual(out.S, in.S) {
		t.Fatalf("S = %v, want %v", out.S, in.S)
	}
}

func TestEncodeValueNormalizesToUTC(t *testing.T) {
	zone := time.FixedZone("UTC+8", 8*60*60)
	in := Value{
		V:    valueVersion,
		Task: "ns/task",
		G:    3,
		T:    time.Date(2026, 10, 20, 16, 0, 0, 0, zone),
		S:    map[string]int32{},
	}
	raw := mustEncode(t, in)
	if !strings.Contains(raw, `"t":"2026-10-20T08:00:00Z"`) {
		t.Fatalf("encoded T is not RFC3339 UTC: %q", raw)
	}
	out, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue() error: %v", err)
	}
	if !out.T.Equal(in.T) {
		t.Fatalf("round trip changed the instant: got %v, want %v", out.T, in.T)
	}
}

func TestEncodeValueValidates(t *testing.T) {
	base := Value{
		V:    valueVersion,
		Task: "ns/task",
		G:    1,
		T:    time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC),
		S:    map[string]int32{"node-a": 50},
	}

	cases := []struct {
		name string
		edit func(*Value)
	}{
		{"version zero", func(v *Value) { v.V = 0 }},
		{"version unsupported", func(v *Value) { v.V = 2 }},
		{"empty task", func(v *Value) { v.Task = "" }},
		{"empty node", func(v *Value) { v.S = map[string]int32{"": 50} }},
		{"score above max", func(v *Value) { v.S = map[string]int32{"node-a": 101} }},
		{"score below min", func(v *Value) { v.S = map[string]int32{"node-a": -1} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := base
			tc.edit(&v)
			if _, err := EncodeValue(v); err == nil {
				t.Fatalf("EncodeValue(%+v) = nil error, want failure", v)
			}
		})
	}

	// Boundary scores and an empty node map are valid.
	base.S = map[string]int32{"node-a": minScore, "node-b": maxScore}
	if _, err := EncodeValue(base); err != nil {
		t.Fatalf("EncodeValue(boundary scores) error: %v", err)
	}
	base.S = map[string]int32{}
	if _, err := EncodeValue(base); err != nil {
		t.Fatalf("EncodeValue(empty node map) error: %v", err)
	}
}

func TestDecodeValueRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"whitespace only", "   "},
		{"malformed", `{"v":1,`},
		{"not object", `[1,2,3]`},
		{"null", `null`},
		{"trailing garbage", `{"v":1,"task":"ns/task","g":1,"t":"2026-10-20T08:00:00Z","s":{}}x`},
		{"missing version", `{"task":"ns/task","g":1,"t":"2026-10-20T08:00:00Z","s":{}}`},
		{"unsupported version", `{"v":2,"task":"ns/task","g":1,"t":"2026-10-20T08:00:00Z","s":{}}`},
		{"missing task", `{"v":1,"g":1,"t":"2026-10-20T08:00:00Z","s":{}}`},
		{"empty node", `{"v":1,"task":"ns/task","g":1,"t":"2026-10-20T08:00:00Z","s":{"":50}}`},
		{"score above max", `{"v":1,"task":"ns/task","g":1,"t":"2026-10-20T08:00:00Z","s":{"node-a":101}}`},
		{"score below min", `{"v":1,"task":"ns/task","g":1,"t":"2026-10-20T08:00:00Z","s":{"node-a":-1}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeValue(tc.raw); err == nil {
				t.Fatalf("DecodeValue(%q) = nil error, want failure", tc.raw)
			}
		})
	}

	// Boundary scores decode successfully.
	raw := `{"v":1,"task":"ns/task","g":1,"t":"2026-10-20T08:00:00Z","s":{"node-a":0,"node-b":100}}`
	v, err := DecodeValue(raw)
	if err != nil {
		t.Fatalf("DecodeValue(boundary scores) error: %v", err)
	}
	if v.S["node-a"] != 0 || v.S["node-b"] != 100 {
		t.Fatalf("decoded scores = %v", v.S)
	}
}

func TestFresh(t *testing.T) {
	base := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	const ttl = 5 * time.Minute

	cases := []struct {
		name string
		t    time.Time
		now  time.Time
		ttl  time.Duration
		want bool
	}{
		{"zero timestamp", time.Time{}, base, ttl, false},
		{"just written", base, base, ttl, true},
		{"within ttl", base, base.Add(ttl - time.Nanosecond), ttl, true},
		{"ttl boundary", base, base.Add(ttl), ttl, true},
		{"one nanosecond past ttl", base, base.Add(ttl + time.Nanosecond), ttl, false},
		{"expired", base, base.Add(time.Hour), ttl, false},
		{"zero ttl at write instant", base, base, 0, true},
		{"future timestamp within ttl", base.Add(time.Minute), base, ttl, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := Value{V: valueVersion, Task: "ns/task", T: tc.t}
			if got := Fresh(v, tc.now, tc.ttl); got != tc.want {
				t.Fatalf("Fresh() = %v, want %v", got, tc.want)
			}
		})
	}
}
