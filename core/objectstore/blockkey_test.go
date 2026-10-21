package objectstore

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

// TestBlockKey 锁定 D25 源站键布局:<dataset>/blocks/<path>#<chunk>(逐字节)。
func TestBlockKey(t *testing.T) {
	cases := []struct {
		name    string
		dataset string
		path    string
		chunk   int64
		want    string
	}{
		{"普通值", "ds", "a/b/c.bin", 7, "ds/blocks/a/b/c.bin#7"},
		{"chunk=0", "ds", "empty.bin", 0, "ds/blocks/empty.bin#0"},
		{"大 chunk", "ds", "big.bin", math.MaxInt64, "ds/blocks/big.bin#" + strconv.FormatInt(math.MaxInt64, 10)},
		{"path 含 / 不转义", "ds", "dir/sub/file.tar", 1, "ds/blocks/dir/sub/file.tar#1"},
		{"dataset 前缀拼接", "tenant/ds", "f.tar", 3, "tenant/ds/blocks/f.tar#3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := BlockKey(tc.dataset, tc.path, tc.chunk)
			if got != tc.want {
				t.Fatalf("BlockKey(%q, %q, %d) = %q, want %q", tc.dataset, tc.path, tc.chunk, got, tc.want)
			}
			if !strings.HasPrefix(got, tc.dataset+"/blocks/") {
				t.Fatalf("前缀 = %q, want %q", got, tc.dataset+"/blocks/")
			}
		})
	}
}
