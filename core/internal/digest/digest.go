// Package digest 封装 xxh3-128 与 sha256 摘要（design.md §5.5，decisions.md D10）。
//
// 一次性与流式接口输出一致：XXH3_128 与 NewXXH3 的 Sum 使用同一编码
// （zeebo/xxh3 的 Sum128().Bytes()，大端规范形式）。
package digest

import (
	"crypto/sha256"
	"hash"

	"github.com/zeebo/xxh3"
)

// XXH3_128 返回 b 的 xxh3-128 一次性摘要（16 字节，作为 CAS 存储键）。
func XXH3_128(b []byte) [16]byte {
	return xxh3.Hash128(b).Bytes()
}

// SHA256 返回 b 的 sha256 一次性摘要（32 字节）。
func SHA256(b []byte) [32]byte {
	return sha256.Sum256(b)
}

// NewXXH3 返回流式 xxh3-128 摘要器（Size() == 16，与 XXH3_128 编码一致）。
func NewXXH3() hash.Hash {
	return xxh3.New128()
}

// NewSHA256 返回流式 sha256 摘要器（Size() == 32）。
func NewSHA256() hash.Hash {
	return sha256.New()
}
