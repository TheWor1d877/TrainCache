// Package cas 是内容寻址的本地块存储（design.md §5.2、§6.3）。
//
// 本文件负责 128 字节块头的编解码、magic/version 校验与指纹的 hex 编解码。
// 块头布局（design.md §6.3、decisions.md D3，v1 即最终格式）：
//
//	offset  size  field
//	0       4     magic  "TCBK"（0x54 43 42 4B）
//	4       2     version = 1
//	6       2     flags   bit0 = 块头含有效 sha256
//	8       4     block_size   uint32 LE（数据字节数，不含块头）
//	12      16    xxh3_128     数据指纹 = 存储键 = 文件名
//	28      32    sha256       flags bit0 置位时有效，否则全 0
//	60      68    reserved     全 0（对齐到 128B）
//	128     n     数据
package cas

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

const (
	// HeaderSize 是块头字节数（design.md §6.3，对齐到 128B）。
	HeaderSize = 128
	// FlagHasSHA256 是 flags bit0：置位时块头 SHA256 字段有效。
	FlagHasSHA256 = 1 << 0
)

// Magic 是块头 magic（4 字节，"TCBK"，0x54 43 42 4B）。
const Magic = "TCBK"

// headerVersion 是块头格式版本；v1 即最终格式（decisions.md D3）。
const headerVersion uint16 = 1

// Header 是 128 字节块头（字段偏移见文件顶部布局说明）。
type Header struct {
	Version   uint16   // = headerVersion
	Flags     uint16   // FlagHasSHA256 置位时 SHA256 字段有效
	BlockSize uint32   // 数据字节数（不含块头）
	XXH3      [16]byte // 数据指纹 = 存储键 = 文件名
	SHA256    [32]byte
}

// ParseHeader 解析块头：校验 magic/version/reserved 区，长度不足 128B 报错；
// 长于 128B 的输入按前 128B 解析（尾部多余字节忽略）。
func ParseHeader(b []byte) (Header, error) {
	var h Header
	if len(b) < HeaderSize {
		return h, fmt.Errorf("cas: block header too short: got %d bytes, need %d", len(b), HeaderSize)
	}
	if string(b[0:4]) != Magic {
		return h, fmt.Errorf("cas: bad block header magic %q", b[0:4])
	}
	h.Version = binary.LittleEndian.Uint16(b[4:6])
	if h.Version != headerVersion {
		return h, fmt.Errorf("cas: unsupported block header version %d", h.Version)
	}
	h.Flags = binary.LittleEndian.Uint16(b[6:8])
	h.BlockSize = binary.LittleEndian.Uint32(b[8:12])
	copy(h.XXH3[:], b[12:28])
	copy(h.SHA256[:], b[28:60])
	for _, c := range b[60:HeaderSize] {
		if c != 0 {
			return Header{}, fmt.Errorf("cas: block header reserved bytes not zero")
		}
	}
	return h, nil
}

// Encode 把块头编码为 128 字节（reserved 区置 0）。
func (h Header) Encode() [HeaderSize]byte {
	var b [HeaderSize]byte
	copy(b[0:4], Magic)
	binary.LittleEndian.PutUint16(b[4:6], h.Version)
	binary.LittleEndian.PutUint16(b[6:8], h.Flags)
	binary.LittleEndian.PutUint32(b[8:12], h.BlockSize)
	copy(b[12:28], h.XXH3[:])
	copy(b[28:60], h.SHA256[:])
	return b
}

// String 返回指纹的 32 字符小写 hex 编码（design.md §6.2：fp 为 32 字符 hex）。
func (f Fingerprint) String() string {
	return hex.EncodeToString(f[:])
}

// FingerprintFromHex 解析 32 字符 hex 指纹；长度或字符不符报错。
func FingerprintFromHex(s string) (Fingerprint, error) {
	var f Fingerprint
	if len(s) != 2*len(f) {
		return f, fmt.Errorf("cas: invalid fingerprint length %d, want %d hex chars", len(s), 2*len(f))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return f, fmt.Errorf("cas: invalid fingerprint hex: %w", err)
	}
	copy(f[:], b)
	return f, nil
}
