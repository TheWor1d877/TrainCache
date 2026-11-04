package plugin

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   any
	}{
		{"hello", &Hello{Frame: frameHello, Protocol: 1, Plugin: "s3", Version: "0.1"}},
		{"hello_ok", &HelloOK{Frame: frameHelloOK, Protocol: 1, Methods: []string{"resolve", "save"}}},
		{"request", &Request{Frame: frameRequest, ID: "1", Method: "resolve", Params: json.RawMessage(`{"task_id":"t"}`)}},
		{"progress", &Progress{Frame: frameProgress, ID: "1", BytesDone: 8, BytesTotal: 16}},
		{"result", &Result{Frame: frameResult, ID: "1", OK: true, Data: json.RawMessage(`{"found":true}`)}},
		{"error result", &Result{Frame: frameResult, ID: "2", OK: false, Error: &Error{Code: CodeCorrupt, Message: "坏分片"}}},
		{"cancel", &Cancel{Frame: frameCancel, ID: "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line, err := Encode(tc.in)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			if len(line) == 0 || line[len(line)-1] != '\n' {
				t.Fatalf("Encode() = %q, want trailing newline", line)
			}
			if got := bytes.Count(line, []byte{'\n'}); got != 1 {
				t.Fatalf("换行数量 = %d, want 1 (%q)", got, line)
			}
			got, err := Decode(line)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if !reflect.DeepEqual(got, tc.in) {
				t.Fatalf("Decode() = %#v, want %#v", got, tc.in)
			}
		})
	}
}

func TestDecodeFieldNames(t *testing.T) {
	cases := []struct {
		name   string
		v      any
		want   []string
		absent []string
	}{
		{
			name: "progress",
			v:    &Progress{Frame: frameProgress, ID: "1", BytesDone: 1, BytesTotal: 2},
			want: []string{`"bytes_done"`, `"bytes_total"`},
			absent: []string{
				"BytesDone", "BytesTotal",
			},
		},
		{
			name: "request",
			v:    &Request{Frame: frameRequest, ID: "1", Method: "save", Params: json.RawMessage(`{}`)},
			want: []string{`"params"`, `"method"`, `"id"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := Encode(tc.v)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			for _, want := range tc.want {
				if !bytes.Contains(b, []byte(want)) {
					t.Fatalf("Encode() = %s, want key %s", b, want)
				}
			}
			for _, absent := range tc.absent {
				if bytes.Contains(b, []byte(absent)) {
					t.Fatalf("Encode() = %s, unexpected key %s", b, absent)
				}
			}
		})
	}
}

func TestReadFrameDropsPartialLine(t *testing.T) {
	full, err := Encode(&Hello{Frame: frameHello, Protocol: 1, Plugin: "p", Version: "v"})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	// 完整帧之后跟一个没有换行的半行，第二次读取必须返回 io.EOF 而不是解析它。
	stream := append(append([]byte(nil), full...), []byte(`{"frame":"hello","protocol":1`)...)
	r := bufio.NewReader(bytes.NewReader(stream))

	if _, err := ReadFrame(r); err != nil {
		t.Fatalf("第一次 ReadFrame() error = %v", err)
	}
	if _, err := ReadFrame(r); !errors.Is(err, io.EOF) {
		t.Fatalf("半行 ReadFrame() error = %v, want io.EOF", err)
	}

	// Decode 本身允许没有结尾换行，读取方是否接受半行由它自己决定。
	if _, err := Decode(bytes.TrimSuffix(full, []byte{'\n'})); err != nil {
		t.Fatalf("Decode(no newline) error = %v", err)
	}
}

func TestFrameTooLong(t *testing.T) {
	long := append([]byte(`{"frame":"hello","plugin":"`), bytes.Repeat([]byte("x"), maxFrameBytes)...)
	long = append(long, []byte(`"}`)...)

	if _, err := Decode(long); err == nil {
		t.Fatal("Decode(too long) error = nil, want error")
	}
	r := bufio.NewReader(bytes.NewReader(append(long, '\n')))
	if _, err := ReadFrame(r); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("ReadFrame(too long) error = %v, want length error", err)
	}

	_, err := Encode(&Hello{Frame: frameHello, Plugin: strings.Repeat("x", maxFrameBytes)})
	if err == nil {
		t.Fatal("Encode(too long) error = nil, want error")
	}
}

func TestDecodeUnknownFrame(t *testing.T) {
	if _, err := Decode([]byte(`{"frame":"nope"}`)); err == nil {
		t.Fatal("Decode(unknown) error = nil, want error")
	} else if !strings.Contains(err.Error(), "未知帧") {
		t.Fatalf("Decode(unknown) error = %v, want 未知帧", err)
	}
}

func TestErrorRetryable(t *testing.T) {
	cases := []struct {
		code Code
		want bool
	}{
		{CodeNotFound, false},
		{CodeInvalidArgument, false},
		{CodeAuth, false},
		{CodeUnavailable, true},
		{CodeThrottled, true},
		{CodeCorrupt, true},
		{CodeInternal, true},
		// 未知码不在 design 的可重试集合里，按不可重试处理，避免无限重试。
		{Code("OTHER"), false},
	}
	for _, tc := range cases {
		t.Run(string(tc.code), func(t *testing.T) {
			e := &Error{Code: tc.code, Message: "x"}
			if got := e.Retryable(); got != tc.want {
				t.Fatalf("Retryable() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCodeOf(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Code
	}{
		{"direct", &Error{Code: CodeCorrupt}, CodeCorrupt},
		{"wrapped", fmt.Errorf("保存: %w", &Error{Code: CodeThrottled}), CodeThrottled},
		{"plain", errors.New("boom"), CodeInternal},
		{"nil", nil, CodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CodeOf(tc.err); got != tc.want {
				t.Fatalf("CodeOf() = %s, want %s", got, tc.want)
			}
		})
	}
}
