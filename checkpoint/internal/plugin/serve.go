package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Handler 是插件的业务入口。返回 *Error 时按其中的 code/message 回传，
// 返回其他 error 统一按 INTERNAL，避免插件把内部细节泄漏成可重试错误。
type Handler func(ctx context.Context, method string, params json.RawMessage, sess *Session) (any, error)

// Session 绑定一次 request。Progress 只能上报当前请求的进度；
// Canceled 在收到 cancel 或 SIGTERM 时关闭，插件应尽快刷盘退出。
type Session struct {
	srv *server
	id  string
	ctx context.Context
}

// Progress 上报当前请求的已传字节数，用于 checkpointd 重置僵死计时。
func (s *Session) Progress(done, total int64) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	return s.srv.writeFrame(&Progress{
		Frame:      frameProgress,
		ID:         s.id,
		BytesDone:  done,
		BytesTotal: total,
	})
}

// Canceled 返回随请求 ctx 关闭的通道，插件用它响应 cancel 与 SIGTERM。
func (s *Session) Canceled() <-chan struct{} {
	return s.ctx.Done()
}

// requestState 是在途请求的取消入口与完成信号。done 在结果写完且
// current 清空后才关闭，terminate 用它把 5 秒收尾等待限制在 handler 上。
type requestState struct {
	id     string
	cancel context.CancelFunc
	done   chan struct{}
}

type server struct {
	conn net.Conn
	r    *bufio.Reader

	writeMu sync.Mutex

	mu      sync.Mutex
	current *requestState

	stopping atomic.Bool
}

// Serve 是插件进程的入口：连接 os.Args[1] 指定的 socket，完成握手后
// 逐个处理 request。返回 nil 表示对端正常关闭或收到 SIGTERM。
func Serve(name, version string, methods []string, handle Handler) error {
	if len(os.Args) < 2 {
		return errors.New("plugin: 缺少 socket 路径参数")
	}
	conn, err := net.Dial("unix", os.Args[1])
	if err != nil {
		return fmt.Errorf("plugin: 连接 %s: %w", os.Args[1], err)
	}
	defer conn.Close()

	s := &server{conn: conn, r: bufio.NewReader(conn)}
	if err := s.handshake(name, version, methods); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	done := make(chan struct{})
	go func() {
		select {
		case <-sigCh:
			s.terminate(closeGrace)
		case <-done:
		}
	}()

	err = s.loop(ctx, handle)
	close(done)
	if s.stopping.Load() {
		return nil
	}
	return err
}

func (s *server) handshake(name, version string, methods []string) error {
	hello := &Hello{
		Frame:    frameHello,
		Protocol: ProtocolVersion,
		Plugin:   name,
		Version:  version,
	}
	if err := s.writeFrame(hello); err != nil {
		return fmt.Errorf("plugin: 发送 hello: %w", err)
	}

	v, err := ReadFrame(s.r)
	if err != nil {
		return fmt.Errorf("plugin: 等待 hello_ok: %w", err)
	}
	ok, isOK := v.(*HelloOK)
	if !isOK {
		return fmt.Errorf("plugin: 握手应答不是 hello_ok，实际为 %T", v)
	}
	if ok.Protocol != ProtocolVersion {
		return fmt.Errorf("plugin: 协议版本 %d 不受支持(需 %d)", ok.Protocol, ProtocolVersion)
	}
	// 服务端会列出全部方法；若它明确列出了方法集合，本地声明的方法必须
	// 在其中，否则说明插件与服务端版本不匹配，早失败比运行中报错清楚。
	if len(ok.Methods) > 0 {
		declared := make(map[string]struct{}, len(ok.Methods))
		for _, m := range ok.Methods {
			declared[m] = struct{}{}
		}
		for _, m := range methods {
			if _, found := declared[m]; !found {
				return fmt.Errorf("plugin: 服务端未声明方法 %q", m)
			}
		}
	}
	return nil
}

// loop 只负责读帧与分发。写结果在各自 goroutine 里做，读循环不会被
// 慢 handler 卡住，因而 cancel 与 SIGTERM 总能被及时读到。
func (s *server) loop(ctx context.Context, handle Handler) error {
	for {
		v, err := ReadFrame(s.r)
		if err != nil {
			if s.stopping.Load() || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("plugin: 读取 request: %w", err)
		}
		switch f := v.(type) {
		case *Request:
			s.dispatch(ctx, f, handle)
		case *Cancel:
			s.cancel(f.ID)
		default:
			return fmt.Errorf("plugin: 握手后收到意外帧 %T", v)
		}
	}
}

// dispatch 保证同一时刻只有一个 handler 在跑。并发 request 属于线协议
// 违规，直接用 INVALID_ARGUMENT 拒绝，避免两个 handler 争同一份状态。
func (s *server) dispatch(parent context.Context, req *Request, handle Handler) {
	s.mu.Lock()
	if s.current != nil {
		s.mu.Unlock()
		_ = s.writeFrame(&Result{
			Frame: frameResult,
			ID:    req.ID,
			OK:    false,
			Error: &Error{Code: CodeInvalidArgument, Message: "已有请求在途"},
		})
		return
	}
	reqCtx, cancel := context.WithCancel(parent)
	st := &requestState{id: req.ID, cancel: cancel, done: make(chan struct{})}
	s.current = st
	s.mu.Unlock()

	sess := &Session{srv: s, id: req.ID, ctx: reqCtx}
	go func() {
		defer func() {
			cancel()
			s.mu.Lock()
			if s.current == st {
				s.current = nil
			}
			close(st.done)
			s.mu.Unlock()
		}()
		_ = s.writeFrame(s.result(reqCtx, req, sess, handle))
	}()
}

// result 执行 handler 并构造结果帧。panic 也收敛成 INTERNAL，保证进程
// 仍能把 result 发回去，而不是只留一个退出码。
func (s *server) result(ctx context.Context, req *Request, sess *Session, handle Handler) *Result {
	res := &Result{Frame: frameResult, ID: req.ID}
	data, err := invoke(ctx, req, sess, handle)
	if err != nil {
		res.OK = false
		var pluginErr *Error
		if errors.As(err, &pluginErr) {
			res.Error = pluginErr
			return res
		}
		res.Error = &Error{Code: CodeInternal, Message: err.Error()}
		return res
	}
	if data == nil {
		res.OK = true
		return res
	}
	raw, err := json.Marshal(data)
	if err != nil {
		res.OK = false
		res.Error = &Error{Code: CodeInternal, Message: "编码结果: " + err.Error()}
		return res
	}
	res.OK = true
	res.Data = raw
	return res
}

func invoke(ctx context.Context, req *Request, sess *Session, handle Handler) (data any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("处理 %s 时 panic: %v", req.Method, r)
		}
	}()
	return handle(ctx, req.Method, req.Params, sess)
}

func (s *server) cancel(id string) {
	s.mu.Lock()
	st := s.current
	if st != nil && st.id == id {
		st.cancel()
	}
	s.mu.Unlock()
}

// terminate 响应 SIGTERM：取消在途 handler，最多等 closeGrace 让它把
// 续传状态刷盘，然后断开连接。没有在途请求时立即退出。
func (s *server) terminate(grace time.Duration) {
	s.stopping.Store(true)
	s.mu.Lock()
	st := s.current
	s.mu.Unlock()
	if st != nil {
		st.cancel()
		select {
		case <-st.done:
		case <-time.After(grace):
		}
	}
	_ = s.conn.Close()
}

func (s *server) writeFrame(v any) error {
	b, err := Encode(v)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.conn.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("plugin: 短写 %d/%d 字节", n, len(b))
	}
	return nil
}
