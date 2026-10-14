package digest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// sha256 标准已知向量（NIST FIPS 180 示例）。
func TestSHA256KnownVectors(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"empty", nil, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", []byte("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	}
	for _, tc := range cases {
		sum := SHA256(tc.in)
		if got := hex.EncodeToString(sum[:]); got != tc.want {
			t.Errorf("SHA256(%s) = %s，期望 %s", tc.name, got, tc.want)
		}
	}
}

// xxh3-128 已知向量：来自 xxHash 官方 C 实现生成的测试向量
// （zeebo/xxh3 v1.1.0 的 compat_test.go testVecs128，输入为 buf[:i]，
// buf[i] = byte((i+1)%251)，Bytes() 为大端规范编码）。
func TestXXH3KnownVectors(t *testing.T) {
	want := []string{
		"99aa06d3014798d86001c324468d497f", // 空输入
		"51025a4491835505e12ef9d2eb86ceeb", // [0x01]
		"d2f0f9428898845f08130b77ddef5807", // [0x01 0x02]
		"ac77eb88cbc4b8d4ebce9b7632ae733b", // [0x01 0x02 0x03]
	}
	buf := make([]byte, len(want))
	for i := range buf {
		buf[i] = byte((i + 1) % 251)
	}
	for i, w := range want {
		sum := XXH3_128(buf[:i])
		if got := hex.EncodeToString(sum[:]); got != w {
			t.Errorf("XXH3_128(前 %d 字节) = %s，期望 %s", i, got, w)
		}
	}
}

// 流式与一次性输出一致（含空输入、跨内部块边界的大输入），且重复调用稳定。
func TestXXH3StreamingMatchesOneShot(t *testing.T) {
	inputs := [][]byte{
		nil,
		[]byte("a"),
		[]byte("the quick brown fox jumps over the lazy dog"),
		bytes.Repeat([]byte{0xAB}, 1024),
		bytes.Repeat([]byte("xxh3-stream"), (1<<20)/11+1),
	}
	for _, in := range inputs {
		one := XXH3_128(in)

		h := NewXXH3()
		if h.Size() != 16 {
			t.Fatalf("NewXXH3().Size() = %d，期望 16", h.Size())
		}
		for i := 0; i < len(in); i += 97 {
			end := i + 97
			if end > len(in) {
				end = len(in)
			}
			if n, err := h.Write(in[i:end]); err != nil || n != end-i {
				t.Fatalf("xxh3 Write 返回 (%d, %v)", n, err)
			}
		}
		stream := h.Sum(nil)
		if !bytes.Equal(one[:], stream) {
			t.Fatalf("长度 %d：一次性与流式 xxh3 不一致", len(in))
		}
		if again := XXH3_128(in); again != one {
			t.Fatalf("长度 %d：重复调用结果不稳定", len(in))
		}
	}

	if XXH3_128([]byte("a")) == XXH3_128([]byte("b")) {
		t.Fatal("不同输入得到相同 xxh3 摘要")
	}
}

// sha256 流式与一次性输出一致，且与标准库结果相同。
func TestSHA256StreamingMatchesOneShot(t *testing.T) {
	data := bytes.Repeat([]byte("traincache"), 4096)
	want := sha256.Sum256(data)

	h := NewSHA256()
	if h.Size() != 32 {
		t.Fatalf("NewSHA256().Size() = %d，期望 32", h.Size())
	}
	for i := 0; i < len(data); i += 1000 {
		end := i + 1000
		if end > len(data) {
			end = len(data)
		}
		if n, err := h.Write(data[i:end]); err != nil || n != end-i {
			t.Fatalf("sha256 Write 返回 (%d, %v)", n, err)
		}
	}
	stream := h.Sum(nil)
	if !bytes.Equal(stream, want[:]) {
		t.Fatal("流式 sha256 与标准库结果不一致")
	}
	one := SHA256(data)
	if !bytes.Equal(stream, one[:]) {
		t.Fatal("流式与一次性 sha256 不一致")
	}
}
