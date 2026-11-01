// Package server 实现网关的 HTTP 契约：探针、GET/HEAD 直通、块 GET 的合并
// 路径、错误响应与 SIGTERM 排水（design 10.1 §4.1/§4.2/§6.1/§6.6）。
package server

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"

	corelog "traincache/core/logging"
	"traincache/gateway/internal/merge"
	"traincache/gateway/internal/obs"
	"traincache/gateway/internal/origin"
	"traincache/gateway/internal/sched"
)

// 网关错误码与响应头（design 10.1 §4.2/§4.4 冻结；10.2 D24/K8：与
// core/objectstore 各写一份，修改一处必须同批修改另一处）。
const (
	GatewayHeaderName            = "x-traincache-gateway"
	GatewayHeaderUnavailable     = "unavailable"
	GatewayCodeUnavailable       = "XTraincacheGatewayUnavailable"
	GatewayCodeSlowDown          = "XTraincacheGatewaySlowDown"
	GatewayCodeOriginUnavailable = "XTraincacheGatewayOriginUnavailable"
)

// 固定的 S3 错误码与路径片段。
const (
	codeMethodNotAllowed = "MethodNotAllowed"
	codeNoSuchBucket     = "NoSuchBucket"
	codeNoSuchKey        = "NoSuchKey"

	blocksSegment = "/blocks/"
)

// Origin 是 server 对源站客户端的最小依赖（origin.Client 实现）。
type Origin interface {
	Stat(ctx context.Context, key string) (origin.ObjectInfo, error)
	Open(ctx context.Context, key string) (*origin.Object, error)
}

// Options 是网关服务参数。Registry/Scheduler/Obs 由 cmd 注入；为兼容测试，
// New 在字段为 nil 时按 Concurrency/BufferBytes 构造默认实例。
type Options struct {
	Bucket      string
	Concurrency int
	BufferBytes int

	Registry  *merge.Registry
	Scheduler *sched.Scheduler
	Obs       *obs.Logger
}

// Server 持有路由、探针与排水状态。HTTP server 的 Shutdown 由 cmd 调用，
// 本类型只负责 ready 与在途计数（design 10.1 §6.6）。
type Server struct {
	origin Origin
	opts   Options
	log    *slog.Logger

	registry  *merge.Registry
	scheduler *sched.Scheduler
	obs       *obs.Logger

	// createMu 串行化「再次 Attach → Create」临界区，保证同一 key 同时只有
	// 一个拉取协程（M4 不变量 7）。
	createMu sync.Mutex

	ready     atomic.Bool
	inflight  sync.WaitGroup
	inflightN atomic.Int64

	// baseCtx 是所有拉取协程的父 ctx；Shutdown 排空后取消。
	baseCtx context.Context
	cancel  context.CancelFunc
}

// New 构造服务。ready 初始为 false，监听成功后由 cmd 调 SetReady(true)
// （design 10.1 §6.6 的启动序）。
func New(org Origin, opts Options, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if opts.BufferBytes <= 0 {
		opts.BufferBytes = 10 * 1024 * 1024
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 32
	}
	if opts.Registry == nil {
		opts.Registry = merge.NewRegistry(opts.BufferBytes)
	}
	if opts.Scheduler == nil {
		opts.Scheduler = sched.New(opts.Concurrency)
	}
	if opts.Obs == nil {
		opts.Obs = obs.New(60*time.Second, log)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		origin:    org,
		opts:      opts,
		log:       log,
		registry:  opts.Registry,
		scheduler: opts.Scheduler,
		obs:       opts.Obs,
		baseCtx:   ctx,
		cancel:    cancel,
	}
}

// Handler 返回计入在途请求的 HTTP handler；每个请求 inflight.Add(1)/Done()。
// panic 统一恢复到 503 unavailable（design 10.1 §4.2）。
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.inflight.Add(1)
		s.inflightN.Add(1)
		defer func() {
			s.inflightN.Add(-1)
			s.inflight.Done()
		}()

		tw := &statusWriter{ResponseWriter: w}
		defer s.recoverPanic(tw)
		s.route(tw, r)
	})
}

// SetReady 设置 readyz 的判定结果。收到退出信号后立即置 false。
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// Inflight 返回当前在途请求数，供退出日志使用（design 10.1 §7.2）。
func (s *Server) Inflight() int64 {
	return s.inflightN.Load()
}

// Shutdown 先关闭 ready，再等在途请求结束或 ctx 到期，返回是否排空。
// 排空后取消 baseCtx，M4 的拉取协程随父 ctx 退出。
func (s *Server) Shutdown(ctx context.Context) bool {
	s.SetReady(false)

	drained := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		s.cancel()
		return true
	case <-ctx.Done():
		// ctx 到期的同一瞬间可能刚好排空：优先认排空，避免把 drained 记成 forced。
		select {
		case <-drained:
			s.cancel()
			return true
		default:
		}
		s.cancel()
		return false
	}
}

// route 按 design 10.1 §4.1 分发请求：探针、方法校验、桶校验，然后 GET/HEAD。
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		s.serveProbe(w, r, true)
		return
	case "/readyz":
		s.serveProbe(w, r, false)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		s.writeS3Error(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "method not allowed", false)
		return
	}

	bucket, key := splitObjectPath(r.URL.Path)
	if bucket != s.opts.Bucket {
		s.writeS3Error(w, http.StatusNotFound, codeNoSuchBucket, "bucket not configured", false)
		return
	}
	if key == "" {
		s.writeS3Error(w, http.StatusNotFound, codeNoSuchKey, "object key is empty", false)
		return
	}

	if r.Method == http.MethodHead {
		s.serveHead(w, r, key)
		return
	}
	if isBlockKey(key) {
		s.serveBlock(w, r, key)
		return
	}
	s.servePassthrough(w, r, key)
}

// serveProbe 处理 /healthz 与 /readyz。存活探针不依赖源站；就绪探针只看 ready。
func (s *Server) serveProbe(w http.ResponseWriter, r *http.Request, health bool) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		s.writeS3Error(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "method not allowed", false)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !health && !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "not ready")
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// serveHead 直通 HEAD，不回源字节（design 10.1 §4.1；10.2 D1）。
func (s *Server) serveHead(w http.ResponseWriter, r *http.Request, key string) {
	s.obs.Passthrough()
	info, err := s.origin.Stat(r.Context(), key)
	if err != nil {
		s.writeOriginError(w, r, key, err)
		return
	}
	s.setObjectHeaders(w.Header(), info)
	w.WriteHeader(http.StatusOK)
}

// serveBlock 处理块键 GET 的合并路径（design 10.1 §6.1）：
// Attach 命中即转为订阅者；否则取准入槽后再次 Attach，仍未命中才建条目、
// 起拉取协程，并把发起请求本身作为第一个订阅者。
func (s *Server) serveBlock(w http.ResponseWriter, r *http.Request, key string) {
	dataset := datasetOf(key)

	s.obs.Get(dataset)

	if e, ok := s.registry.Attach(key); ok {
		s.serveAttached(w, r, key, dataset, e, nil)
		return
	}

	release, ok := s.acquire(w, r, dataset)
	if !ok {
		return
	}

	// 拿槽期间可能已有其他请求建成条目。
	s.createMu.Lock()
	if e, ok := s.registry.Attach(key); ok {
		s.createMu.Unlock()
		s.serveAttached(w, r, key, dataset, e, release)
		return
	}
	if s.registry.InFlight(key) {
		// 条目在途但 written > C：不加入、也不新建同名条目，独立回源
		// （design 10.1 §6.2；10.2 D8）。仍占一个准入槽。
		s.createMu.Unlock()
		s.serveIndependent(w, r, key, dataset, release)
		return
	}

	buf := s.registry.GetBuffer()
	e := s.registry.Create(key, buf)
	s.createMu.Unlock()

	e.SetRelease(release)
	s.obs.Owner(dataset)
	s.log.Debug("条目创建", "dataset", dataset, "key", key)

	sub := e.Subscribe()
	s.log.Debug("订阅者加入", "dataset", dataset, "subs", e.Subscribers())
	start := time.Now()

	go s.pump(e, key, dataset, start)

	s.serveWithSub(w, r, key, dataset, e, sub)
}

// acquire 取一个准入槽（最长排队 2s，队列满直接拒绝）。失败时按 design
// 10.1 §4.3/§6.1 写 503 SlowDown 并返回 false；客户端已离开时不写响应。
func (s *Server) acquire(w http.ResponseWriter, r *http.Request, dataset string) (func(), bool) {
	actx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	release, err := s.scheduler.Acquire(actx, dataset)
	if err == nil {
		return release, true
	}
	if r.Context().Err() != nil {
		// 客户端已经离开，不再写响应，也不计入准入拒绝。
		return nil, false
	}
	reason := "wait_timeout"
	if errors.Is(err, sched.ErrQueueFull) {
		reason = "queue_full"
	}
	s.obs.Rejected()
	s.log.Warn("准入拒绝", "dataset", dataset, "reason", reason)
	s.writeS3Error(w, http.StatusServiceUnavailable, GatewayCodeSlowDown,
		"gateway is busy", false)
	return nil, false
}

// pump 是每个条目的唯一拉取协程：从源站读、写环形缓冲，结束时 Finish。
// pumpCtx 是服务级 baseCtx 的子 ctx；单个订阅者断开不会中止共享下载，
// 但所有订阅者都离开时 Entry 会取消它，底层源站 Read 随之返回（10.2 D10）。
func (s *Server) pump(e *merge.Entry, key, dataset string, start time.Time) {
	pumpCtx, cancel := context.WithCancel(s.baseCtx)
	defer cancel()
	e.SetCancel(cancel)

	rc, err := s.origin.Open(pumpCtx, key)
	if err != nil {
		e.Finish(err)
		s.logEntryEnd(e, dataset, key, start, err)
		return
	}
	defer rc.Close()

	src := &metadataReader{r: rc, obj: rc, e: e, log: s.log, dataset: dataset}
	err = e.Pump(pumpCtx, &countingReader{r: src, add: s.obs.Bytes})
	s.logEntryEnd(e, dataset, key, start, err)
	if err != nil && !errors.Is(err, context.Canceled) && e.Written() > 0 {
		// 写响应体后才失败：状态码已发出，只记录源站失败（§6.3）。
		resp := minio.ToErrorResponse(err)
		s.log.Warn("源站失败",
			"dataset", dataset,
			"status", resp.StatusCode,
			"code", resp.Code,
			"err", err,
			"err_class", corelog.Classify(err),
		)
	}
}

func (s *Server) logEntryEnd(e *merge.Entry, dataset, key string, start time.Time, err error) {
	s.log.Debug("条目结束",
		"dataset", dataset,
		"key", key,
		"written", e.Written(),
		"duration_ms", time.Since(start).Milliseconds(),
		"subs", e.Subscribers(),
		"err", err,
		"err_class", corelog.Classify(err),
	)
}

// serveAttached 把 Attach 命中的条目转成订阅者。条目可能在 Attach 与
// 订阅之间 Finish，并因订阅者为 0 立即归还缓冲，此时 TrySubscribe 返回
// false：必须按独立回源处理，绝不能写出空 200（design 10.1 §6.3）。
//
// held 非 nil 表示调用方已持有准入槽，直接交给独立回源；否则重新取槽，
// 语义与普通准入一致（2s 等待，失败回 503 SlowDown）。
func (s *Server) serveAttached(w http.ResponseWriter, r *http.Request, key, dataset string, e *merge.Entry, held func()) {
	sub, ok := e.TrySubscribe()
	if !ok {
		s.log.Debug("条目在订阅前结束", "dataset", dataset, "key", key)
		if held != nil {
			s.serveIndependent(w, r, key, dataset, held)
			return
		}
		release, ok := s.acquire(w, r, dataset)
		if !ok {
			return
		}
		s.serveIndependent(w, r, key, dataset, release)
		return
	}

	if held != nil {
		held()
	}
	s.obs.Merged(dataset)
	s.log.Debug("订阅者加入", "dataset", dataset, "subs", e.Subscribers())
	s.serveWithSub(w, r, key, dataset, e, sub)
}

// serveWithSub 等待第一个数据块或结束，然后持续把 [pos, written) 写给客户端。
// 踢出时 panic(http.ErrAbortHandler) 断开连接（design 10.1 §6.2；10.2 D9）。
func (s *Server) serveWithSub(w http.ResponseWriter, r *http.Request, key, dataset string, e *merge.Entry, sub *merge.Sub) {
	defer sub.Close()

	wroteHeader := false
	for {
		data, err := sub.Wait(r.Context())

		if len(data) > 0 {
			if !wroteHeader {
				// 不设 Content-Length，走 chunked（design 10.1 §6.1 第 6 步）。
				// 首次写字节前元数据已由拉取协程写入条目（10.2 D28）。
				s.setEntryHeaders(w.Header(), e)
				w.WriteHeader(http.StatusOK)
				wroteHeader = true
			}
			if _, werr := w.Write(data); werr != nil {
				return
			}
			// 立即冲刷，保证客户端能边下边收；也避免小响应被 bufio 扣住
			// 导致「订阅者等数据、拉取协程等订阅者」的假死。
			_ = http.NewResponseController(w).Flush()
		}

		if err == nil {
			continue
		}

		switch {
		case errors.Is(err, merge.ErrKicked):
			s.obs.Kicked()
			s.log.Warn("订阅者被踢",
				"dataset", dataset,
				"reason", "slow_consumer",
				"written", e.Written(),
				"pos", sub.Pos(),
			)
			panic(http.ErrAbortHandler)
		case errors.Is(err, merge.ErrClosed):
			if !wroteHeader {
				s.setEntryHeaders(w.Header(), e)
				w.WriteHeader(http.StatusOK)
			}
			return
		case r.Context().Err() != nil:
			return
		default:
			// 拉取协程失败：尚未开始响应时按源站错误渲染；
			// 已写出字节时只能截断（§6.3）。
			if !wroteHeader {
				s.writeOriginError(w, r, key, err)
			}
			return
		}
	}
}

// serveIndependent 处理「条目在途但已越过合并窗口」的块 GET：直接回源，
// 不建条目、不参与合并，结束后释放准入槽（design 10.1 §6.2）。
//
// minio 的 4xx/5xx 在首次 Read 才暴露，所以必须先读第一块再决定状态码：
// 首读就失败时把源站状态码与 Code 透传给客户端（§6.3），不能先写 200。
func (s *Server) serveIndependent(w http.ResponseWriter, r *http.Request, key, dataset string, release func()) {
	defer release()

	ctx := r.Context()
	rc, err := s.origin.Open(ctx, key)
	if err != nil {
		s.writeOriginError(w, r, key, err)
		return
	}
	defer rc.Close()

	s.obs.Owner(dataset)

	src := &countingReader{r: rc, add: s.obs.Bytes}
	first := make([]byte, 32*1024)
	var n int
	var rerr error
	for {
		n, rerr = src.Read(first)
		if n > 0 || rerr != nil {
			break
		}
		// Read 允许返回 (0, nil)：继续读，直到有字节或出错。
	}

	if n == 0 {
		if errors.Is(rerr, io.EOF) {
			// 空对象：源站成功但零字节。
			w.WriteHeader(http.StatusOK)
			return
		}
		s.writeOriginError(w, r, key, rerr)
		return
	}

	// 首读已收到 GET 响应头，Stat 从传输层记录里取值、不发新请求；失败只记
	// Debug，不能因此阻断已经成功的下载（10.2 D28）。
	if info, serr := rc.Stat(); serr != nil {
		s.log.Debug("读取源站元数据失败", "dataset", dataset, "err", serr)
	} else {
		setMetadataHeaders(w.Header(), info.ETag, info.LastModified)
	}

	w.WriteHeader(http.StatusOK)
	if _, werr := w.Write(first[:n]); werr != nil {
		return
	}
	if rerr != nil {
		if !errors.Is(rerr, io.EOF) {
			s.log.Warn("源站响应中断",
				"dataset", dataset,
				"err", rerr,
				"err_class", corelog.Classify(rerr),
			)
		}
		return
	}
	if _, err := io.Copy(w, src); err != nil {
		s.log.Warn("源站响应中断",
			"dataset", dataset,
			"err", err,
			"err_class", corelog.Classify(err),
		)
	}
}

// metadataReader 在第一次读到字节时补齐条目元数据，再把这次字节交给上层。
// 元数据来自这次 GET 的响应头，obj.Stat() 不再发请求（design 10.1 §5.2；10.2 D28）。
type metadataReader struct {
	r       io.Reader
	obj     *origin.Object
	e       *merge.Entry
	log     *slog.Logger
	dataset string
	set     bool
}

func (m *metadataReader) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if n > 0 && !m.set {
		m.set = true
		// 必须在字节交给 Entry.Pump 之前写好元数据，保证第一次 write 广播
		// 时订阅者已能读到 Last-Modified/ETag。
		if info, serr := m.obj.Stat(); serr != nil {
			m.log.Debug("读取源站元数据失败", "dataset", m.dataset, "err", serr)
		} else {
			m.e.SetMetadata(info.ETag, info.LastModified)
		}
	}
	return n, err
}

// countingReader 统计从源站读入的字节，供 60s 汇总的 bytes 字段。
type countingReader struct {
	r   io.Reader
	add func(int64)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.add(int64(n))
	}
	return n, err
}

// servePassthrough 直通非块键 GET：先 Stat 拿状态与长度，再 Open + io.Copy。
func (s *Server) servePassthrough(w http.ResponseWriter, r *http.Request, key string) {
	s.obs.Passthrough()
	ctx := r.Context()
	info, err := s.origin.Stat(ctx, key)
	if err != nil {
		s.writeOriginError(w, r, key, err)
		return
	}
	rc, err := s.origin.Open(ctx, key)
	if err != nil {
		s.writeOriginError(w, r, key, err)
		return
	}
	defer rc.Close()

	s.setObjectHeaders(w.Header(), info)
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, rc); err != nil {
		// 响应已开始，无法改状态码；中途失败按 10.1 §6.3 只记日志。
		s.log.Warn("源站响应中断", "dataset", datasetOf(key), "err", err, "err_class", corelog.Classify(err))
	}
}

// setObjectHeaders 回填源站的对象头；空值不写（design 10.1 §4.1）。
func (s *Server) setObjectHeaders(h http.Header, info origin.ObjectInfo) {
	if info.Size >= 0 {
		h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	}
	setMetadataHeaders(h, info.ETag, info.LastModified)
	if info.ContentType != "" {
		h.Set("Content-Type", info.ContentType)
	}
}

// setEntryHeaders 把条目记录的元数据回填到合并响应；不写 Content-Length，
// 合并路径保持 chunked（10.2 D28）。元数据可能尚未就位（空对象），此时跳过。
func (s *Server) setEntryHeaders(h http.Header, e *merge.Entry) {
	etag, lastModified := e.Metadata()
	setMetadataHeaders(h, etag, lastModified)
}

// setMetadataHeaders 只回填 ETag 与 Last-Modified，空值不写。
func setMetadataHeaders(h http.Header, etag string, lastModified time.Time) {
	if etag != "" {
		h.Set("ETag", etag)
	}
	if !lastModified.IsZero() {
		h.Set("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
	}
}

// writeOriginError 按 design 10.1 §4.2 映射源站错误：
// 有状态码与 Code 的原样透传；StatusCode==0（连不上/TLS/超时）回 504。
func (s *Server) writeOriginError(w http.ResponseWriter, r *http.Request, key string, err error) {
	if r.Context().Err() != nil {
		// 客户端已取消：不再写响应，避免把取消误报成源站故障。
		s.log.Debug("请求已取消，放弃错误响应", "dataset", datasetOf(key), "err", err)
		return
	}

	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == 0 {
		s.log.Warn("源站不可用", "dataset", datasetOf(key), "err", err, "err_class", corelog.Classify(err))
		s.writeS3Error(w, http.StatusGatewayTimeout, GatewayCodeOriginUnavailable,
			"origin request failed", false)
		return
	}

	code := resp.Code
	if code == "" {
		code = "InternalError"
	}
	msg := resp.Message
	if msg == "" {
		msg = resp.Error()
	}
	s.log.Warn("源站返回错误",
		"dataset", datasetOf(key),
		"status", resp.StatusCode,
		"code", code,
		"err", err,
		"err_class", corelog.Classify(err),
	)
	s.writeS3Error(w, resp.StatusCode, code, msg, false)
}

// writeS3Error 输出 S3 XML 错误体。gatewayUnavailable 为 true 时带
// x-traincache-gateway: unavailable（只表示网关这一跳坏了）。
func (s *Server) writeS3Error(w http.ResponseWriter, status int, code, message string, gatewayUnavailable bool) {
	body := s3ErrorBody(code, message)
	h := w.Header()
	h.Set("Content-Type", "application/xml")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	if gatewayUnavailable {
		h.Set(GatewayHeaderName, GatewayHeaderUnavailable)
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// s3ErrorBody 生成固定的 XML 错误体，Code/Message 中的 <>& 统一转义。
func s3ErrorBody(code, message string) []byte {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString("<Error><Code>")
	_ = xml.EscapeText(&b, []byte(code))
	b.WriteString("</Code><Message>")
	_ = xml.EscapeText(&b, []byte(message))
	b.WriteString("</Message></Error>")
	return b.Bytes()
}

// datasetOf 取对象键第一个 "/" 之前的部分（design 10.1 §5.1）；日志只写 dataset，
// 不写高基数的完整 key（§7.2）。
func datasetOf(key string) string {
	if i := strings.IndexByte(key, '/'); i > 0 {
		return key[:i]
	}
	return ""
}

// splitObjectPath 把 /<bucket>/<key> 拆成桶与键；键保留后续所有 "/"。
func splitObjectPath(path string) (bucket, key string) {
	path = strings.TrimPrefix(path, "/")
	if i := strings.IndexByte(path, '/'); i >= 0 {
		return path[:i], path[i+1:]
	}
	return path, ""
}

// isBlockKey 判断对象键是否以 <dataset>/blocks/ 开头（design 10.1 §4.1/§5.1）。
func isBlockKey(key string) bool {
	i := strings.IndexByte(key, '/')
	if i <= 0 {
		return false
	}
	return strings.HasPrefix(key[i:], blocksSegment)
}

// statusWriter 记录响应是否已经开始，供 panic 恢复判断能否改写状态码。
// Unwrap 让 http.ResponseController 仍能拿到底层 Flusher 等能力。
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// recoverPanic 把 panic 恢复成 503 unavailable；http.ErrAbortHandler 必须
// 原样放行，否则踢出无法断开连接（design 10.1 §6.2）。响应已开始时只记日志。
func (s *Server) recoverPanic(w *statusWriter) {
	rec := recover()
	if rec == nil {
		return
	}
	if rec == http.ErrAbortHandler {
		panic(rec)
	}
	s.log.Error("panic 恢复", "panic", rec, "stack", string(debug.Stack()))
	if w.status == 0 {
		s.writeS3Error(w, http.StatusServiceUnavailable, GatewayCodeUnavailable,
			"gateway internal error", true)
	}
}
