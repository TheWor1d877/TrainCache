package svc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// hooksock.go:钩子 Unix datagram 收端(design §5.4;decisions D33)。
//
// DataLoader 钩子(Python,训练 Pod 内)→ Agent:Unix datagram socket
// (默认 /run/traincache/agent.sock),行分隔 JSON、单向(Agent 不回帧)。帧类型(§5.4
// 冻结三型,字段名即 JSON 标签):
//
//	{"type":"advance","path":"...","chunk":3}                            → Advance(ChunkRef)
//	{"type":"stage_switch","stage_id":"epoch-2","shuffle_seed":12345}    → StageSwitch(stageID, seed)
//	{"type":"open_report","path":"...","chunk":3}                        → Open(ChunkRef)
//	{"type":"read_miss","path":"...","chunk":3,"reason":"absent","ts":"..."} → Miss(ChunkRef, reason)
//	{"type":"read_stall","path":"...","chunk":3,"wait_ms":250,"ts":"..."}    → Stall(ChunkRef, waitMS, ts)
//
// 处理语义:
//   - 非法 JSON / 未知帧类型 / 超限帧丢弃并计数(metrics.IncHookFormatErrors,
//     reason∈{parse,size};D33/D36;集合外取值不新增基数);
//   - handler 为 nil 的帧丢弃(合法帧,仅无消费者,不计数);
//   - 分发经注入的 HookHandlers(D33:接线阶段不改本文件);读循环单 goroutine 顺序
//     分发(handler 不并发调用);
//   - 套接字文件权限放宽为 0666:钩子进程与 Agent 可能跨容器/跨用户(§6.3 部署口径)。

// hookMaxFrame 是单帧字节上限(§5.4:单帧 ≤ 4 KiB;超限丢弃计 reason=size)。
const hookMaxFrame = 4 << 10

// hookReadBuf 是收包缓冲:上限 +1 字节,恰好用于检出超限数据报。
const hookReadBuf = hookMaxFrame + 1

// hookSockMode 是套接字文件权限(训练 Pod 内钩子需写权限)。
const hookSockMode = 0o666

// HookHandlers 是钩子帧分发目标(D33 冻结):nil 字段 = 丢弃该类帧。
type HookHandlers struct {
	// Advance 接收消费侧推进帧(advance):实际消费到的块引用。
	Advance func(ref ordersource.ChunkRef)
	// StageSwitch 接收阶段切换直报帧(stage_switch):仅作信号,接线层结合
	// ring CurrentOrder 构造新订单(D33),故只传 stageID 与 shuffle_seed。
	StageSwitch func(stageID string, seed int64)
	// Open 接收打开上报帧(open_report):接线层经索引解析为指纹后进入 collector。
	Open func(ref ordersource.ChunkRef)
	// Miss 接收读 miss 帧(read_miss;D44):reason ∈ {absent,corrupt};无法区分时按
	// absent 归一。接线层触发 Agent 侧异步补块。
	Miss func(ref ordersource.ChunkRef, reason string)
	// Stall 接收读等待帧(read_stall;D47):waitMS 为消费侧实测等待毫秒,ts 为 RFC3339Nano
	// 上报时间(解析失败为零值)。阈值过滤与归因由接线层 stallattr 负责。
	Stall func(ref ordersource.ChunkRef, waitMS int64, ts time.Time)
}

// hookFrame 是线上帧的并集结构(五型共用;未知/缺失字段留零值)。
type hookFrame struct {
	Type        string `json:"type"`
	Path        string `json:"path"`
	Chunk       int64  `json:"chunk"`
	StageID     string `json:"stage_id"`
	ShuffleSeed int64  `json:"shuffle_seed"`
	Reason      string `json:"reason"`
	TS          string `json:"ts"`
	WaitMS      int64  `json:"wait_ms"`
}

// HookSock 是钩子 socket 收端(D33 冻结构造签名):New 后 Start 一次,
// ctx 取消或 Close 后停止并清理套接字文件。
type HookSock struct {
	path string
	m    *metrics.Registry
	h    HookHandlers

	mu      sync.Mutex
	conn    *net.UnixConn
	started bool
	closed  bool
}

// New 构造收端(不绑定;Start 时绑定路径)。
func New(path string, m *metrics.Registry, h HookHandlers) *HookSock {
	return &HookSock{path: path, m: m, h: h}
}

// Start 绑定 Unix datagram 套接字并起读循环(非阻塞返回):
//   - 父目录缺失自动创建;陈旧套接字文件(上次崩溃残留)先清理;
//   - 套接字 chmod 0666(hook 进程跨容器写);
//   - ctx 取消 → 自动 Close(停读循环并删套接字文件);
//   - 每实例只允许 Start 一次;Close 后再 Start 返回错误。
func (s *HookSock) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("hooksock: 已关闭,不能再次 Start")
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("hooksock: 重复 Start")
	}
	s.started = true
	s.mu.Unlock()

	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("hooksock: 创建套接字目录 %s: %w", dir, err)
		}
	}
	if err := os.Remove(s.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("hooksock: 清理陈旧套接字 %s: %w", s.path, err)
	}
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: s.path, Net: "unixgram"})
	if err != nil {
		return fmt.Errorf("hooksock: 监听 %s: %w", s.path, err)
	}
	if err := os.Chmod(s.path, hookSockMode); err != nil {
		_ = conn.Close()
		_ = os.Remove(s.path)
		return fmt.Errorf("hooksock: 设置套接字权限 %s: %w", s.path, err)
	}

	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	slog.Info("hooksock: 钩子收端就绪", "path", s.path)
	go s.readLoop(conn)
	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()
	return nil
}

// Close 幂等停止收端:关连接(读循环随之退出)并删除套接字文件。
func (s *HookSock) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()

	var err error
	if conn != nil {
		err = conn.Close()
	}
	if rmErr := os.Remove(s.path); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) && err == nil {
		err = fmt.Errorf("hooksock: 删除套接字 %s: %w", s.path, rmErr)
	}
	return err
}

// readLoop 顺序读数据报并分发(ctx/Close 关闭连接后退出)。
func (s *HookSock) readLoop(conn *net.UnixConn) {
	buf := make([]byte, hookReadBuf)
	for {
		n, _, err := conn.ReadFromUnix(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Warn("hooksock: 读帧失败,收端退出", "path", s.path, "err", err)
			}
			return
		}
		s.handleDatagram(buf[:n])
	}
}

// handleDatagram 处理一个数据报:去尾部换行后判上限(超限计 size 整报丢弃),
// 再按行分隔逐帧分发(行分隔 JSON;空行跳过)。
func (s *HookSock) handleDatagram(d []byte) {
	payload := d
	if n := len(payload); n > 0 && payload[n-1] == '\n' {
		payload = payload[:n-1]
	}
	if n := len(payload); n > 0 && payload[n-1] == '\r' {
		payload = payload[:n-1]
	}
	if len(payload) > hookMaxFrame {
		s.countFormatError(metrics.ReasonSize)
		slog.Warn("hooksock: 帧超过 4 KiB,丢弃", "path", s.path, "size", len(payload))
		return
	}
	for _, line := range strings.Split(string(payload), "\n") {
		if line = strings.TrimSpace(line); line == "" {
			continue
		}
		s.dispatch(line)
	}
}

// dispatch 解析并分发单帧:非法 JSON → parse 计数;未知类型 → parse 计数;
// handler 为 nil → 丢弃(不计数)。
func (s *HookSock) dispatch(line string) {
	var f hookFrame
	if err := json.Unmarshal([]byte(line), &f); err != nil {
		s.countFormatError(metrics.ReasonParse)
		slog.Warn("hooksock: 帧 JSON 非法,丢弃", "path", s.path, "err", err)
		return
	}
	ref := ordersource.ChunkRef{Path: f.Path, Chunk: f.Chunk}
	switch f.Type {
	case "advance":
		if s.h.Advance != nil {
			s.h.Advance(ref)
		}
	case "open_report":
		if s.h.Open != nil {
			s.h.Open(ref)
		}
	case "stage_switch":
		if s.h.StageSwitch != nil {
			s.h.StageSwitch(f.StageID, f.ShuffleSeed)
		}
	case "read_miss":
		reason := f.Reason
		if reason != "corrupt" {
			reason = "absent" // D44:无法区分 absent/corrupt 时按 absent 上报
		}
		if s.h.Miss != nil {
			s.h.Miss(ref, reason)
		}
	case "read_stall":
		ts, err := time.Parse(time.RFC3339Nano, f.TS)
		if err != nil {
			ts = time.Time{} // 时间戳不可解析 → 零值(归因侧按无法关联处理)
		}
		waitMS := f.WaitMS
		if waitMS < 0 {
			waitMS = 0
		}
		if s.h.Stall != nil {
			s.h.Stall(ref, waitMS, ts)
		}
	default:
		s.countFormatError(metrics.ReasonParse)
		slog.Warn("hooksock: 未知帧类型,丢弃", "path", s.path, "type", f.Type)
	}
}

// countFormatError 计一次帧格式错误(reason∈{parse,size};m 为 nil 时跳过)。
func (s *HookSock) countFormatError(reason string) {
	if s.m != nil {
		s.m.IncHookFormatErrors(reason)
	}
}
