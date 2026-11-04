package plugin

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAppendAndLoadParts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part-state.jsonl")
	want := []PartRecord{
		{Part: 0, ETag: "etag-0", Bytes: 8 << 20},
		{Part: 1, ETag: "etag-1", Bytes: 8 << 20},
	}
	for _, rec := range want {
		if err := AppendPart(path, rec); err != nil {
			t.Fatalf("AppendPart(%+v) error = %v", rec, err)
		}
	}
	got, err := LoadParts(path)
	if err != nil {
		t.Fatalf("LoadParts() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadParts() = %+v, want %+v", got, want)
	}
}

func TestLoadParts(t *testing.T) {
	line0 := `{"part":0,"etag":"a","bytes":8}`
	line1 := `{"part":1,"etag":"b","bytes":9}`
	cases := []struct {
		name    string
		content string
		create  bool
		want    []PartRecord
		wantErr bool
	}{
		{
			name:   "file missing",
			create: false,
			want:   nil,
		},
		{
			name:    "two complete lines",
			content: line0 + "\n" + line1 + "\n",
			create:  true,
			want:    []PartRecord{{Part: 0, ETag: "a", Bytes: 8}, {Part: 1, ETag: "b", Bytes: 9}},
		},
		{
			name:    "truncated last line is dropped",
			content: line0 + "\n" + `{"part":1,"etag":"b"`,
			create:  true,
			want:    []PartRecord{{Part: 0, ETag: "a", Bytes: 8}},
		},
		{
			name:    "corrupt middle line",
			content: line0 + "\n" + "{not-json}\n" + line1 + "\n",
			create:  true,
			wantErr: true,
		},
		{
			name:    "empty middle line",
			content: line0 + "\n\n" + line1 + "\n",
			create:  true,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.jsonl")
			if tc.create {
				if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
			}
			got, err := LoadParts(path)
			if tc.wantErr {
				if err == nil {
					t.Fatal("LoadParts() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadParts() error = %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("LoadParts() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
