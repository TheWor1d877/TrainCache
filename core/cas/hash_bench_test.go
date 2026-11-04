package cas

import (
	"testing"

	"github.com/zeebo/xxh3"
)

// BenchmarkXXH3Hash 测 xxh3-128 对 8 MiB 缓冲的单核吞吐，供 docs/5.1 §8 的
// 「算指纹不额外占 I/O」给数字口径。机型与 Go 版本见 docs/5.1-design.md。
func BenchmarkXXH3Hash(b *testing.B) {
	buf := make([]byte, 8<<20)
	b.SetBytes(int64(len(buf)))
	for i := 0; i < b.N; i++ {
		sinkXXH3 = xxh3.Hash(buf)
	}
}

// sinkXXH3 防止编译器把哈希结果判为死值后消掉整次计算。
var sinkXXH3 uint64
