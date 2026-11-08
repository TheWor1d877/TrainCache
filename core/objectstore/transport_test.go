package objectstore

import (
	"net/http"
	"testing"
)

// TestBaseTransportCompression 覆盖压缩开关到传输层的映射：开关关闭时
// DisableCompression 为真（不声明 gzip），打开时为假（由 Go 传输层协商并解压）。
func TestBaseTransportCompression(t *testing.T) {
	cases := []struct {
		name        string
		compression bool
		wantDisable bool
	}{
		{name: "默认关闭", compression: false, wantDisable: true},
		{name: "开关打开", compression: true, wantDisable: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr, err := baseTransport(Options{Compression: tc.compression}, false)
			if err != nil {
				t.Fatalf("baseTransport: %v", err)
			}
			ht, ok := tr.(*http.Transport)
			if !ok {
				t.Fatalf("传输层类型 = %T，期望 *http.Transport", tr)
			}
			if ht.DisableCompression != tc.wantDisable {
				t.Fatalf("DisableCompression = %v，期望 %v", ht.DisableCompression, tc.wantDisable)
			}
		})
	}
}

// TestBaseTransportClonesCallerTransport 调用方传入 *http.Transport 时，开关
// 只改副本，原传输层保持调用方设定的值。
func TestBaseTransportClonesCallerTransport(t *testing.T) {
	orig := &http.Transport{DisableCompression: true}
	tr, err := baseTransport(Options{Transport: orig, Compression: true}, false)
	if err != nil {
		t.Fatalf("baseTransport: %v", err)
	}
	got, ok := tr.(*http.Transport)
	if !ok {
		t.Fatalf("传输层类型 = %T，期望 *http.Transport", tr)
	}
	if got == orig {
		t.Fatal("应复制调用方的传输层，不能返回同一实例")
	}
	if got.DisableCompression {
		t.Fatal("副本的 DisableCompression = true，期望 false")
	}
	if !orig.DisableCompression {
		t.Fatal("调用方传输层的 DisableCompression 被改动")
	}
}
