package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func validManifest() *Manifest {
	return &Manifest{
		Version:   Version,
		TaskID:    "task-0",
		Step:      1200,
		Rank:      1,
		WorldSize: 8,
		Epoch:     2,
		CreatedAt: time.Unix(1700000000, 0).UTC(),
		Files: []File{{
			Path:   "model.safetensors",
			Size:   1024,
			SHA256: strings.Repeat("a", 64),
		}},
	}
}

func TestValidateRejectsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Manifest)
		want   string
	}{
		{"version", func(m *Manifest) { m.Version = 2 }, "version"},
		{"missing task_id", func(m *Manifest) { m.TaskID = "" }, "task_id"},
		{"negative step", func(m *Manifest) { m.Step = -1 }, "step"},
		{"negative rank", func(m *Manifest) { m.Rank = -1 }, "rank"},
		{"zero world_size", func(m *Manifest) { m.WorldSize = 0 }, "world_size"},
		{"rank beyond world", func(m *Manifest) { m.Rank = 8 }, "world_size"},
		{"empty files", func(m *Manifest) { m.Files = nil }, "files"},
		{"absolute path", func(m *Manifest) { m.Files[0].Path = "/etc/passwd" }, "相对路径"},
		{"parent segment", func(m *Manifest) { m.Files[0].Path = "a/../b" }, ".."},
		{"empty path", func(m *Manifest) { m.Files[0].Path = "" }, "path"},
		{"negative size", func(m *Manifest) { m.Files[0].Size = -1 }, "size"},
		{"short sha256", func(m *Manifest) { m.Files[0].SHA256 = "abc" }, "sha256"},
		{"uppercase sha256", func(m *Manifest) { m.Files[0].SHA256 = strings.Repeat("A", 64) }, "sha256"},
		{"non hex sha256", func(m *Manifest) { m.Files[0].SHA256 = strings.Repeat("g", 64) }, "sha256"},
		{"duplicate path", func(m *Manifest) { m.Files = append(m.Files, m.Files[0]) }, "重复"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.mutate(m)
			err := m.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestMarshalOrderIsStable(t *testing.T) {
	a := validManifest()
	a.Files = []File{
		{Path: "b.bin", Size: 2, SHA256: strings.Repeat("b", 64)},
		{Path: "a.bin", Size: 1, SHA256: strings.Repeat("a", 64)},
	}
	b := validManifest()
	b.Files = []File{
		{Path: "a.bin", Size: 1, SHA256: strings.Repeat("a", 64)},
		{Path: "b.bin", Size: 2, SHA256: strings.Repeat("b", 64)},
	}

	ba, err := a.Marshal()
	if err != nil {
		t.Fatalf("a.Marshal() error = %v", err)
	}
	bb, err := b.Marshal()
	if err != nil {
		t.Fatalf("b.Marshal() error = %v", err)
	}
	if !bytes.Equal(ba, bb) {
		t.Fatalf("文件顺序不同导致字节不同:\n%s\n%s", ba, bb)
	}

	got, err := Unmarshal(ba)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.Files[0].Path != "a.bin" || got.Files[1].Path != "b.bin" {
		t.Fatalf("Files 未按 path 排序: %+v", got.Files)
	}
}

func TestUnmarshalValidates(t *testing.T) {
	m := validManifest()
	m.Version = 99
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if _, err := Unmarshal(data); err == nil {
		t.Fatal("Unmarshal(invalid) error = nil, want error")
	}
}

func TestVerifyFiles(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"a.txt":     "hello",
		"sub/b.bin": "world",
	}
	for name, body := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
	}

	base := validManifest()
	base.Files = []File{
		{Path: "a.txt", Size: int64(len(files["a.txt"])), SHA256: hashOf(files["a.txt"])},
		{Path: "sub/b.bin", Size: int64(len(files["sub/b.bin"])), SHA256: hashOf(files["sub/b.bin"])},
	}
	if err := base.VerifyFiles(root); err != nil {
		t.Fatalf("VerifyFiles(valid) error = %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Manifest)
		want   string
	}{
		{"size mismatch", func(m *Manifest) { m.Files[0].Size++ }, "大小"},
		{"hash mismatch", func(m *Manifest) { m.Files[1].SHA256 = strings.Repeat("0", 64) }, "sha256"},
		{"missing file", func(m *Manifest) { m.Files[0].Path = "missing.txt" }, "missing.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := *base
			m.Files = append([]File(nil), base.Files...)
			tc.mutate(&m)
			err := m.VerifyFiles(root)
			if err == nil {
				t.Fatal("VerifyFiles() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("VerifyFiles() error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func hashOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
