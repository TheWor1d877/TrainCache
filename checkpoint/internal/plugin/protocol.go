// Package plugin 实现 checkpointd 与存储插件子进程之间的 NDJSON 帧协议
// （design 10.1 §4.1）。
//
// 一帧一行，判别键固定为 frame；握手版本固定为 1。编解码与读取都集中在
// 本文件，client 与 serve 只处理业务字段，避免两端对线格式产生分叉。
package plugin

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ProtocolVersion 是当前插件协议版本（design 10.1 §4.1）。
const ProtocolVersion = 1

// maxFrameBytes 是单行 JSON 的上限。插件故障时可能写出没有换行的超长
// 内容，若不设上限会随读取无限增长内存，因此读取与解码都按此值拒绝。
const maxFrameBytes = 1 << 20

// 帧名。两端共用同一组常量，避免手写字符串时出现拼写漂移。
const (
	frameHello    = "hello"
	frameHelloOK  = "hello_ok"
	frameRequest  = "request"
	frameProgress = "progress"
	frameResult   = "result"
	frameCancel   = "cancel"
)

// Hello 是插件连接后的首帧，声明协议版本与自身标识。
type Hello struct {
	Frame    string `json:"frame"`
	Protocol int    `json:"protocol"`
	Plugin   string `json:"plugin"`
	Version  string `json:"version"`
}

// HelloOK 是 checkpointd 的握手应答，methods 是本次允许调用的方法集合。
type HelloOK struct {
	Frame    string   `json:"frame"`
	Protocol int      `json:"protocol"`
	Methods  []string `json:"methods"`
}

// Request 是 checkpointd 发起的一次方法调用。
type Request struct {
	Frame  string          `json:"frame"`
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// Progress 是插件上报的传输进度，用于重置僵死计时。
type Progress struct {
	Frame      string `json:"frame"`
	ID         string `json:"id"`
	BytesDone  int64  `json:"bytes_done"`
	BytesTotal int64  `json:"bytes_total"`
}

// Result 是一次调用的终态。Data 与 Error 二选一，ok 为 false 时读 Error。
type Result struct {
	Frame string          `json:"frame"`
	ID    string          `json:"id"`
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *Error          `json:"error,omitempty"`
}

// Cancel 要求插件停止指定请求。checkpointd 关闭或调用方取消时发送。
type Cancel struct {
	Frame string `json:"frame"`
	ID    string `json:"id"`
}

// Code 是插件错误码，取值与 design 10.1 §4.1 的错误分类表一致。
type Code string

const (
	CodeNotFound        Code = "NOT_FOUND"
	CodeInvalidArgument Code = "INVALID_ARGUMENT"
	CodeAuth            Code = "AUTH"
	CodeUnavailable     Code = "UNAVAILABLE"
	CodeThrottled       Code = "THROTTLED"
	CodeCorrupt         Code = "CORRUPT"
	CodeInternal        Code = "INTERNAL"
)

// 设计表直接使用全大写错误码，保留同名常量供调用方按文档书写。
const (
	NOT_FOUND        = CodeNotFound
	INVALID_ARGUMENT = CodeInvalidArgument
	AUTH             = CodeAuth
	UNAVAILABLE      = CodeUnavailable
	THROTTLED        = CodeThrottled
	CORRUPT          = CodeCorrupt
	INTERNAL         = CodeInternal
)

// Error 是帧内错误体，也可作为方法返回值直接交给调用方判断。
type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

// Error 实现 error。统一加 plugin 前缀，日志里能看出错误来自插件协议层。
func (e *Error) Error() string {
	if e == nil {
		return "plugin: <nil>"
	}
	if e.Message == "" {
		return "plugin: " + string(e.Code)
	}
	return "plugin: " + string(e.Code) + ": " + e.Message
}

// Retryable 按 design 10.1 §4.1 分类：只有前三个错误码不重试。
func (e *Error) Retryable() bool {
	if e == nil {
		return false
	}
	switch e.Code {
	case CodeUnavailable, CodeThrottled, CodeCorrupt, CodeInternal:
		return true
	default:
		return false
	}
}

// CodeOf 取错误链中的插件错误码。非插件错误一律按 INTERNAL 处理，
// 避免调用方把传输层失败误判成可重试的插件错误。
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeInternal
}

// Encode 把一帧编码成单行 NDJSON。结尾换行由这里补上，调用方不需要再拼。
func Encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("plugin: 编码帧: %w", err)
	}
	if len(b) > maxFrameBytes {
		return nil, fmt.Errorf("plugin: 帧长度 %d 超过上限 %d 字节", len(b), maxFrameBytes)
	}
	return append(b, '\n'), nil
}

// Decode 按 frame 把一行还原成对应结构体。允许行尾没有换行，方便直接
// 解码已截取的单行；是否把半行当作完整帧由读取方决定。
func Decode(line []byte) (any, error) {
	line = bytes.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, errors.New("plugin: 空帧")
	}
	if len(line) > maxFrameBytes {
		return nil, fmt.Errorf("plugin: 帧长度 %d 超过上限 %d 字节", len(line), maxFrameBytes)
	}

	var head struct {
		Frame string `json:"frame"`
	}
	if err := json.Unmarshal(line, &head); err != nil {
		return nil, fmt.Errorf("plugin: 解码帧: %w", err)
	}

	switch head.Frame {
	case frameHello:
		v := new(Hello)
		if err := json.Unmarshal(line, v); err != nil {
			return nil, fmt.Errorf("plugin: 解码 hello: %w", err)
		}
		return v, nil
	case frameHelloOK:
		v := new(HelloOK)
		if err := json.Unmarshal(line, v); err != nil {
			return nil, fmt.Errorf("plugin: 解码 hello_ok: %w", err)
		}
		return v, nil
	case frameRequest:
		v := new(Request)
		if err := json.Unmarshal(line, v); err != nil {
			return nil, fmt.Errorf("plugin: 解码 request: %w", err)
		}
		return v, nil
	case frameProgress:
		v := new(Progress)
		if err := json.Unmarshal(line, v); err != nil {
			return nil, fmt.Errorf("plugin: 解码 progress: %w", err)
		}
		return v, nil
	case frameResult:
		v := new(Result)
		if err := json.Unmarshal(line, v); err != nil {
			return nil, fmt.Errorf("plugin: 解码 result: %w", err)
		}
		return v, nil
	case frameCancel:
		v := new(Cancel)
		if err := json.Unmarshal(line, v); err != nil {
			return nil, fmt.Errorf("plugin: 解码 cancel: %w", err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("plugin: 未知帧 %q", head.Frame)
	}
}

// ReadFrame 从流里读一帧。单行超过上限立即报错，不继续缓冲；EOF 时如果
// 只剩没有换行的半行，按 design 10.1 §5.2 的续传语义丢弃并返回 io.EOF。
func ReadFrame(r *bufio.Reader) (any, error) {
	var buf []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if len(buf)+len(chunk) > maxFrameBytes+1 {
			return nil, fmt.Errorf("plugin: 帧长度超过上限 %d 字节", maxFrameBytes)
		}
		buf = append(buf, chunk...)
		switch {
		case err == nil:
			return Decode(buf)
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return nil, io.EOF
		default:
			return nil, fmt.Errorf("plugin: 读取帧: %w", err)
		}
	}
}
