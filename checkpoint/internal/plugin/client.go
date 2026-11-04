package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// 默认超时来自 design 10.1 §4.1：握手 10 秒，连续 5 分钟无进展判僵死。
const (
	defaultHandshakeTimeout = 10 * time.Second
	defaultLivenessTimeout  = 5 * time.Minute
	closeGrace              = 5 * time.Second
)

// methods 是 checkpointd 在 hello_ok 中声明的方法集合。
var methods = []string{"resolve", "save", "restore"}

// Options 是启动插件的全部输入。Path 与 SocketPath 必填；其余字段留空时
// 使用协议默认值，Logger 留空时丢弃日志。
type Options struct {
	Path             string
	SocketPath       string
	Logger           *slog.Logger
	HandshakeTimeout time.Duration
	LivenessTimeout  time.Duration
	OnProgress       func(done, total int64)
}

// Policy 选择 resolve 的目标 step，mode 为 latest 或 step。
type Policy struct {
	Mode string `json:"mode"`
	Step int64  `json:"step,omitempty"`
}

// ResolveParams 是 resolve 的入参。
type ResolveParams struct {
	TaskID    string          `json:"task_id"`
	Rank      int             `json:"rank"`
	WorldSize int             `json:"world_size"`
	Policy    Policy          `json:"policy"`
	Backend   json.RawMessage `json:"backend"`
}

// SaveParams 是 save 的入参。StatePath 只放续传状态，由插件自己维护。
type SaveParams struct {
	TaskID    string          `json:"task_id"`
	Rank      int             `json:"rank"`
	WorldSize int             `json:"world_size"`
	Policy    Policy          `json:"policy"`
	Backend   json.RawMessage `json:"backend"`
	Step      int64           `json:"step"`
	LocalPath string          `json:"local_path"`
	StatePath string          `json:"state_path"`
}

// RestoreParams 是 restore 的入参，target_path 代替 save 的 local_path。
type RestoreParams struct {
	TaskID     string          `json:"task_id"`
	Rank       int             `json:"rank"`
	WorldSize  int             `json:"world_size"`
	Policy     Policy          `json:"policy"`
	Backend    json.RawMessage `json:"backend"`
	Step       int64           `json:"step"`
	TargetPath string          `json:"target_path"`
	StatePath  string          `json:"state_path"`
}

// ResolveResult 是 resolve 的返回。
type ResolveResult struct {
	Found    bool  `json:"found"`
	Step     int64 `json:"step"`
	Complete bool  `json:"complete"`
	Epoch    int   `json:"epoch"`
}

// TransferResult 是 save 与 restore 的返回。
type TransferResult struct {
	Bytes int64 `json:"bytes"`
	Files int   `json:"files"`
}

// pendingCall 是一次在途请求。done 与 activity 都由 mu 保护后使用，
// close(done) 之前的写入对等待方可见。
type pendingCall struct {
	id       string
	done     chan struct{}
	activity chan struct{}
	result   *Result
	err      error
	settled  bool
}

// Client 是 checkpointd 侧的插件子进程与连接。一个 Client 同时只跑一个
// 请求：协议本身按 id 关联结果，但 design 10.1 §4.4 要求同 rank 串行，
// 这里提前拒绝并发调用，免得插件侧还要排队。
type Client struct {
	opts Options
	log  *slog.Logger

	conn       net.Conn
	r          *bufio.Reader
	listener   net.Listener
	cmd        *exec.Cmd
	socketPath string

	exited   chan struct{}
	readDone chan struct{}

	killOnce  sync.Once
	closeOnce sync.Once
	writeMu   sync.Mutex

	mu      sync.Mutex
	pending *pendingCall
	closed  bool

	seq atomic.Int64
}

// Start 清理旧 socket、监听、启动插件子进程，并完成 hello/hello_ok 握手。
// 只有协议版本一致且握手成功才返回可用 Client；任何超时都会杀掉进程，
// 避免留下无法通信的子进程。
func Start(ctx context.Context, opts Options) (*Client, error) {
	if opts.Path == "" {
		return nil, errors.New("plugin: 未指定插件路径")
	}
	if opts.SocketPath == "" {
		return nil, errors.New("plugin: 未指定 socket 路径")
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = defaultHandshakeTimeout
	}
	if opts.LivenessTimeout <= 0 {
		opts.LivenessTimeout = defaultLivenessTimeout
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// 上次异常退出会留下 socket 文件，先删掉才能重新 Listen。
	if err := os.Remove(opts.SocketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("plugin: 删除旧 socket %s: %w", opts.SocketPath, err)
	}
	listener, err := net.Listen("unix", opts.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("plugin: 监听 %s: %w", opts.SocketPath, err)
	}

	cmd := exec.Command(opts.Path, opts.SocketPath)
	if err := cmd.Start(); err != nil {
		_ = listener.Close()
		_ = os.Remove(opts.SocketPath)
		return nil, fmt.Errorf("plugin: 启动 %s: %w", opts.Path, err)
	}

	c := &Client{
		opts:       opts,
		log:        log,
		listener:   listener,
		cmd:        cmd,
		socketPath: opts.SocketPath,
		exited:     make(chan struct{}),
		readDone:   make(chan struct{}),
	}
	go c.waitProcess()

	started := time.Now()
	type accepted struct {
		conn net.Conn
		err  error
	}
	acceptCh := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		acceptCh <- accepted{conn: conn, err: err}
	}()

	timer := time.NewTimer(opts.HandshakeTimeout)
	defer timer.Stop()

	select {
	case got := <-acceptCh:
		if got.err != nil {
			return c.abort(fmt.Errorf("plugin: 接受插件连接: %w", got.err))
		}
		c.conn = got.conn
	case <-timer.C:
		return c.abort(fmt.Errorf("plugin: 握手超时 (%s)", opts.HandshakeTimeout))
	case <-ctx.Done():
		return c.abort(fmt.Errorf("plugin: 握手取消: %w", ctx.Err()))
	case <-c.exited:
		return c.abort(errors.New("plugin: 插件进程在握手前退出"))
	}

	// 本次握手的总预算从 Start 开始算，Accept 花掉的时间也要计入。
	if err := c.conn.SetReadDeadline(started.Add(opts.HandshakeTimeout)); err != nil {
		return c.abort(fmt.Errorf("plugin: 设置握手超时: %w", err))
	}
	c.r = bufio.NewReader(c.conn)
	v, err := ReadFrame(c.r)
	if err != nil {
		return c.abort(fmt.Errorf("plugin: 握手未收到 hello: %w", err))
	}
	hello, ok := v.(*Hello)
	if !ok {
		return c.abort(fmt.Errorf("plugin: 握手首帧不是 hello，实际为 %T", v))
	}
	if hello.Protocol != ProtocolVersion {
		return c.abort(fmt.Errorf("plugin: 协议版本 %d 不受支持(需 %d)", hello.Protocol, ProtocolVersion))
	}
	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return c.abort(fmt.Errorf("plugin: 清除握手超时: %w", err))
	}
	if err := c.writeFrame(&HelloOK{Frame: frameHelloOK, Protocol: ProtocolVersion, Methods: methods}); err != nil {
		return c.abort(fmt.Errorf("plugin: 发送 hello_ok: %w", err))
	}

	go c.readLoop()
	log.Debug("插件握手完成", slog.String("plugin", hello.Plugin), slog.String("version", hello.Version))
	return c, nil
}

// abort 是 Start 失败时的统一收尾：杀进程、关连接与监听、删 socket，
// 并等待子进程被回收，避免测试或重启后留下孤儿。
func (c *Client) abort(err error) (*Client, error) {
	c.kill()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	_ = c.listener.Close()
	_ = os.Remove(c.socketPath)
	c.shutdownProcess(2 * time.Second)
	return nil, err
}

// waitProcess 回收子进程。退出码只用于诊断，成败以 result 为准
// （design 10.1 §4.1），因此这里不向业务侧传播退出码。
func (c *Client) waitProcess() {
	err := c.cmd.Wait()
	if err != nil {
		c.log.Debug("插件进程退出", slog.Any("err", err))
	}
	close(c.exited)
}

// Resolve 查询后端是否存在可用版本。
func (c *Client) Resolve(ctx context.Context, params ResolveParams) (ResolveResult, error) {
	var out ResolveResult
	err := c.call(ctx, "resolve", params, &out)
	return out, err
}

// Save 把一个 step 上传到后端，插件按 state_path 维护续传状态。
func (c *Client) Save(ctx context.Context, params SaveParams) (TransferResult, error) {
	var out TransferResult
	err := c.call(ctx, "save", params, &out)
	return out, err
}

// Restore 把一个 step 恢复到 target_path。
func (c *Client) Restore(ctx context.Context, params RestoreParams) (TransferResult, error) {
	var out TransferResult
	err := c.call(ctx, "restore", params, &out)
	return out, err
}

// call 串起一次请求的完整生命周期：登记 pending、发帧、等待 result，
// 期间用 activity 重置僵死计时。liveness 超时会杀进程并按 INTERNAL 返回。
func (c *Client) call(ctx context.Context, method string, params any, out any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("plugin: 编码 %s 入参: %w", method, err)
	}
	id := strconv.FormatInt(c.seq.Add(1), 10)
	p := &pendingCall{
		id:       id,
		done:     make(chan struct{}),
		activity: make(chan struct{}, 1),
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("plugin: 客户端已关闭")
	}
	if c.pending != nil {
		c.mu.Unlock()
		return errors.New("plugin: 已有请求在途")
	}
	c.pending = p
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.pending == p {
			c.pending = nil
		}
		c.mu.Unlock()
	}()

	if err := c.writeFrame(&Request{Frame: frameRequest, ID: id, Method: method, Params: raw}); err != nil {
		return fmt.Errorf("plugin: 发送 %s 请求: %w", method, err)
	}

	timer := time.NewTimer(c.opts.LivenessTimeout)
	defer timer.Stop()
	for {
		select {
		case <-p.done:
			return c.finishCall(method, p, out)
		case <-p.activity:
			resetTimer(timer, c.opts.LivenessTimeout)
		case <-timer.C:
			// 计时器与 result 同时就绪时先看 result，避免误杀已完成调用。
			select {
			case <-p.done:
				continue
			default:
			}
			c.kill()
			return &Error{
				Code:    CodeInternal,
				Message: fmt.Sprintf("%s 请求 %s 超过 %s 无 progress 也无 result", method, id, c.opts.LivenessTimeout),
			}
		case <-ctx.Done():
			if err := c.writeFrame(&Cancel{Frame: frameCancel, ID: id}); err != nil {
				return fmt.Errorf("plugin: %s 请求取消: %w (发送 cancel 失败: %v)", method, ctx.Err(), err)
			}
			return fmt.Errorf("plugin: %s 请求取消: %w", method, ctx.Err())
		}
	}
}

// finishCall 把 result 转成 typed 返回值。插件错误原样返回，调用方可以
// errors.As 到 *Error 并按 Code 决定是否重试。
func (c *Client) finishCall(method string, p *pendingCall, out any) error {
	if p.err != nil {
		return p.err
	}
	if p.result == nil {
		return &Error{Code: CodeInternal, Message: method + " 请求完成但没有 result"}
	}
	res := p.result
	if !res.OK {
		if res.Error != nil {
			return res.Error
		}
		return &Error{Code: CodeInternal, Message: method + " 请求 ok=false 但没有 error"}
	}
	if out == nil || len(res.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(res.Data, out); err != nil {
		return fmt.Errorf("plugin: 解码 %s 结果: %w", method, err)
	}
	return nil
}

// readLoop 串行处理服务端发来的 progress 与 result。读失败或 EOF 时若
// 仍有在途请求，按 design 10.1 §4.1 转成 INTERNAL，而不是让调用方等超时。
func (c *Client) readLoop() {
	defer close(c.readDone)
	for {
		v, err := ReadFrame(c.r)
		if err != nil {
			c.failPending(&Error{Code: CodeInternal, Message: fmt.Sprintf("插件未返回 result 就断开: %v", err)})
			return
		}
		switch f := v.(type) {
		case *Progress:
			c.handleProgress(f)
		case *Result:
			c.handleResult(f)
		default:
			c.log.Warn("插件发来意外帧", slog.String("frame", fmt.Sprintf("%T", v)))
		}
	}
}

func (c *Client) handleProgress(f *Progress) {
	c.mu.Lock()
	p := c.pending
	match := p != nil && p.id == f.ID && !p.settled
	if match {
		select {
		case p.activity <- struct{}{}:
		default:
		}
	}
	c.mu.Unlock()
	if match && c.opts.OnProgress != nil {
		c.opts.OnProgress(f.BytesDone, f.BytesTotal)
	}
}

func (c *Client) handleResult(f *Result) {
	c.mu.Lock()
	p := c.pending
	if p == nil || p.id != f.ID || p.settled {
		c.mu.Unlock()
		c.log.Warn("插件返回未知请求的 result", slog.String("id", f.ID))
		return
	}
	p.result = f
	p.settled = true
	close(p.done)
	c.mu.Unlock()
}

func (c *Client) failPending(err error) {
	c.mu.Lock()
	p := c.pending
	if p == nil || p.settled {
		c.mu.Unlock()
		return
	}
	p.err = err
	p.settled = true
	close(p.done)
	c.mu.Unlock()
}

// writeFrame 串行化写，避免握手帧与业务帧、cancel 与 request 交错。
func (c *Client) writeFrame(v any) error {
	b, err := Encode(v)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, err := c.conn.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("plugin: 短写 %d/%d 字节", n, len(b))
	}
	return nil
}

// Close 按 design 10.1 §4.1 收尾：有在途请求先发 cancel 并最多等 5 秒，
// 没有则直接断开让插件走 EOF；超时杀进程，最后关连接、监听并删 socket。
func (c *Client) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		p := c.pending
		c.mu.Unlock()

		if p != nil && !p.settled {
			_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
			if err := c.writeFrame(&Cancel{Frame: frameCancel, ID: p.id}); err != nil {
				// 连接可能已经断了，收尾仍要继续；错误留给调用方诊断。
				closeErr = fmt.Errorf("plugin: 发送 cancel: %w", err)
			}
			_ = c.conn.SetWriteDeadline(time.Time{})
			if !c.waitExit(closeGrace) {
				c.kill()
				c.waitExit(closeGrace)
			}
		} else {
			_ = c.conn.Close()
			if !c.waitExit(closeGrace) {
				c.kill()
				c.waitExit(closeGrace)
			}
		}

		_ = c.conn.Close()
		_ = c.listener.Close()
		select {
		case <-c.readDone:
		case <-time.After(time.Second):
		}
		if err := os.Remove(c.socketPath); err != nil && !os.IsNotExist(err) && closeErr == nil {
			closeErr = fmt.Errorf("plugin: 删除 socket %s: %w", c.socketPath, err)
		}
	})
	return closeErr
}

// waitExit 在 d 内等待子进程被回收，返回是否已经退出。
func (c *Client) waitExit(d time.Duration) bool {
	select {
	case <-c.exited:
		return true
	case <-time.After(d):
		return false
	}
}

func (c *Client) shutdownProcess(d time.Duration) {
	if !c.waitExit(d) {
		c.kill()
		c.waitExit(d)
	}
}

func (c *Client) kill() {
	c.killOnce.Do(func() {
		if c.cmd != nil && c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
	})
}

// resetTimer 复用同一个 timer，避免每次 progress 都重新分配。
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
