package svc

// backend.go：Agent 侧后端选择器与降级状态机（10.1 design §4.3/§4.4）。
//
// 只有 Get 走选择逻辑；Put/Delete/GetETag/PutIfMatch/List 一律直连源站。状态只在
// 进程内存里，不写 CRD；Agent 重启回到配置默认（gatewayEndpoint 非空即 mode=1）。

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"traincache/agent/metrics"
	"traincache/core/objectstore"
)

// 降级状态机的常量（10.2 D5 / 10.1 §4.3）。
const (
	// gatewayInitialBackoff 是首次降级的退避时长。
	gatewayInitialBackoff = 10 * time.Second
	// gatewayMaxBackoff 是退避上限。
	gatewayMaxBackoff = 120 * time.Second
	// gatewayProbeWindow 是抢占探测权后 nextProbe 的推进量：窗口内其余并发请求直连，
	// 保证同一时刻最多一个请求发到网关。
	gatewayProbeWindow = 5 * time.Second
)

// backendStore 实现 objectstore.Store：Get 按 sourceState 决定走网关或源站，
// 网关失败时在「已写字节为 0」的前提下同次改走源站；其余方法一律转 origin。
type backendStore struct {
	origin  objectstore.Store
	gateway objectstore.Store // nil 表示未启用
	state   *sourceState
}

var _ objectstore.Store = (*backendStore)(nil)

// newBackendStore 构造选择器。gateway 为 nil 时 state 也为 nil（未启用，全部直连）。
func newBackendStore(origin, gateway objectstore.Store) *backendStore {
	s := &backendStore{origin: origin, gateway: gateway}
	if gateway != nil {
		s.state = newSourceState(true)
	}
	return s
}

// Get 实现 objectstore.Store。失败分类与降级规则见 10.1 §4.4 表。
func (s *backendStore) Get(ctx context.Context, key string, w io.Writer) error {
	if s.gateway == nil || s.state == nil {
		return s.origin.Get(ctx, key, w)
	}
	viaGateway, gen := s.state.route()
	if !viaGateway {
		return s.origin.Get(ctx, key, w)
	}

	cw := &countingWriter{w: w}
	err := s.gateway.Get(ctx, key, cw)
	if err == nil {
		// mode=1 下成功不变；探测成功则由状态机切回网关模式并清零退避。
		s.state.reportSuccess(gen)
		return nil
	}

	switch {
	case errors.Is(err, objectstore.ErrGatewayUnavailable):
		s.state.reportFailure(gen, metrics.GatewayReasonUnavailable)
	case objectstore.IsGatewayTransport(err):
		reason := metrics.GatewayReasonTransport
		if objectstore.IsGatewayTimeout(err) {
			reason = metrics.GatewayReasonTimeout
		}
		s.state.reportFailure(gen, reason)
	case errors.Is(err, objectstore.ErrGatewayRejected):
		// 准入拒绝说明网关这一跳仍健康，不写降级状态。
	default:
		// 源站 4xx/5xx、ErrNotFound 等：普通取数失败，不切后端。
		return err
	}

	// 已向调用方写过任何字节就不能重试，否则会污染已落盘内容（10.2 D6）。
	if cw.written == 0 {
		return s.origin.Get(ctx, key, w)
	}
	return err
}

// Put 实现 objectstore.Store（直连源站）。
func (s *backendStore) Put(ctx context.Context, key string, r io.Reader, size int64) (string, error) {
	return s.origin.Put(ctx, key, r, size)
}

// Delete 实现 objectstore.Store（直连源站）。
func (s *backendStore) Delete(ctx context.Context, key string) error {
	return s.origin.Delete(ctx, key)
}

// GetETag 实现 objectstore.Store（直连源站）。
func (s *backendStore) GetETag(ctx context.Context, key string) (string, error) {
	return s.origin.GetETag(ctx, key)
}

// PutIfMatch 实现 objectstore.Store（直连源站）。
func (s *backendStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, expectedETag string) (string, error) {
	return s.origin.PutIfMatch(ctx, key, r, size, expectedETag)
}

// List 实现 objectstore.Store（直连源站）。
func (s *backendStore) List(ctx context.Context, prefix string) ([]string, error) {
	return s.origin.List(ctx, prefix)
}

// countingWriter 统计已成功写入调用方 w 的字节数，用于「已写字节为 0」判定。
type countingWriter struct {
	w       io.Writer
	written int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.written += int64(n)
	return n, err
}

// sourceState 是网关降级状态机（10.1 §4.3）。用一把 Mutex 串行化，等价于文档里的
// CAS 抢占：同一时刻最多一个探测请求（只有抢占成功者才把 nextProbe 推到 5s 后）。
type sourceState struct {
	mu         sync.Mutex
	mode       int           // 0 直连，1 网关
	gen        uint64        // 每次模式变化 +1，用于丢弃过期结果
	nextProbe  time.Time     // 下次允许试网关的时刻；零值表示不限制
	backoff    time.Duration // 当前退避时长，0 表示 10s 起点
	now        func() time.Time
	onMode     func(int)
	onFallback func(string)
}

// newSourceState 构造状态机：启动时 mode=0；enabled（配置了网关地址）时 mode=1、nextProbe=0。
func newSourceState(enabled bool) *sourceState {
	s := &sourceState{now: time.Now}
	if enabled {
		s.mode = 1
		s.nextProbe = time.Time{}
	}
	return s
}

// setClock 注入时钟（测试用假时钟；nil 忽略）。
func (s *sourceState) setClock(now func() time.Time) {
	if now == nil {
		return
	}
	s.mu.Lock()
	s.now = now
	s.mu.Unlock()
}

// setObservers 注入模式变化与降级通知回调；nil 安全。
func (s *sourceState) setObservers(onMode func(int), onFallback func(string)) {
	s.mu.Lock()
	s.onMode = onMode
	s.onFallback = onFallback
	s.mu.Unlock()
}

// route 决定本次 Get 是否走网关，并返回请求开始时的代际。
// mode=0 且已到探测时刻时，抢占成功者走网关并把 nextProbe 推进 5s；其余直连。
func (s *sourceState) route() (bool, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mode == 1 {
		return true, s.gen
	}
	now := s.now()
	if now.Before(s.nextProbe) {
		return false, s.gen
	}
	s.nextProbe = now.Add(gatewayProbeWindow)
	return true, s.gen
}

// reportSuccess 记录一次网关成功：mode=1 不变；mode=0 的探测成功则恢复网关模式并清零。
// 代际已变（期间发生过其它迁移）时丢弃该结果。
func (s *sourceState) reportSuccess(gen uint64) {
	s.mu.Lock()
	if gen != s.gen {
		s.mu.Unlock()
		return
	}
	if s.mode == 1 {
		s.mu.Unlock()
		return
	}
	s.mode = 1
	s.backoff = 0
	s.nextProbe = time.Time{}
	s.gen++
	onMode := s.onMode
	s.mu.Unlock()
	if onMode != nil {
		onMode(1)
	}
}

// reportFailure 记录一次网关失败：切直连、按 10s 起点翻倍（上限 120s）设置退避与下次探测
// 时刻、推进代际并通知。代际已变时丢弃该结果。
func (s *sourceState) reportFailure(gen uint64, reason string) {
	s.mu.Lock()
	if gen != s.gen {
		s.mu.Unlock()
		return
	}
	s.mode = 0
	s.backoff = growGatewayBackoff(s.backoff)
	s.nextProbe = s.now().Add(s.backoff)
	s.gen++
	onMode, onFallback := s.onMode, s.onFallback
	s.mu.Unlock()
	if onMode != nil {
		onMode(0)
	}
	if onFallback != nil {
		onFallback(reason)
	}
}

// snapshotMode 返回当前模式（装配期写入 gauge、测试断言用）。
func (s *sourceState) snapshotMode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// growGatewayBackoff 返回下一次退避：0 → 10s，否则翻倍并以 120s 封顶。
func growGatewayBackoff(cur time.Duration) time.Duration {
	if cur <= 0 {
		return gatewayInitialBackoff
	}
	next := cur * 2
	if next > gatewayMaxBackoff {
		next = gatewayMaxBackoff
	}
	return next
}
