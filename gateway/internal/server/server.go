// Package server 实现网关的 HTTP 契约：探针、GET/HEAD 直通、块 GET 的合并
// 路径、错误响应与 SIGTERM 排水（design 9.1「网关 HTTP 契约」「错误响应」「取数与扇出」「部署与更新」）。
package server

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
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
	"traincache/gateway/internal/cache"
	"traincache/gateway/internal/merge"
	"traincache/gateway/internal/obs"
	"traincache/gateway/internal/origin"
	"traincache/gateway/internal/sched"
)

// 网关错误码与响应头（design 9.1「错误响应」「失败判断标准」冻结；9.2 D3：与
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
	codeInvalidRange     = "InvalidRange"

	blocksSegment = "/blocks/"
)

// 条目结束行的 from 字段取值：字节来自源站还是来自盘（design 9.1「日志与可观测性」）。
const (
	fromOrigin = "origin"
	fromCache  = "cache"
)

// Origin 是 server 对源站客户端的最小依赖（origin.Client 实现）。
type Origin interface {
	Stat(ctx context.Context, key string) (origin.ObjectInfo, error)
	Open(ctx context.Context, key string) (*origin.Object, error)
	// OpenRange 发起带 Range 头的 GET，闭区间 [start, end]；Range 进入源站签名请求。
	OpenRange(ctx context.Context, key string, start, end int64) (*origin.Object, error)
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

	// Cache 是磁盘缓存存储；为 nil 表示没有磁盘缓存。
	Cache *cache.Store
	// CacheEnabled 是磁盘缓存开关。Cache 为 nil 或该开关为 false 时网关
	// 不查盘、不写盘，行为与加缓存之前完全一致（9.2 D29）。
	CacheEnabled bool
}

// Server 持有路由、探针与排水状态。HTTP server 的 Shutdown 由 cmd 调用，
// 本类型只负责 ready 与在途计数（design 9.1「部署与更新」）。
type Server struct {
	origin Origin
	opts   Options
	log    *slog.Logger

	registry  *merge.Registry
	scheduler *sched.Scheduler
	obs       *obs.Logger

	// cacheStore 是磁盘缓存存储；cacheOn 是缓存开关的最终判定（存储非 nil
	// 且开关打开）。两者都不成立时网关不查盘、不写盘。
	cacheStore *cache.Store
	cacheOn    bool

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
// （design 9.1「部署与更新」的启动序）。
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
		origin:     org,
		opts:       opts,
		log:        log,
		registry:   opts.Registry,
		scheduler:  opts.Scheduler,
		obs:        opts.Obs,
		cacheStore: opts.Cache,
		cacheOn:    opts.CacheEnabled && opts.Cache != nil,
		baseCtx:    ctx,
		cancel:     cancel,
	}
}

// Handler 返回计入在途请求的 HTTP handler；每个请求 inflight.Add(1)/Done()。
// panic 统一恢复到 503 unavailable（design 9.1「错误响应」）。
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

// Inflight 返回当前在途请求数，供退出日志使用（design 9.1「日志与可观测性」）。
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

// route 按 design 9.1「网关 HTTP 契约」分发请求：探针、方法校验、桶校验，然后 GET/HEAD。
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

// serveHead 直通 HEAD，不回源字节（design 9.1「网关 HTTP 契约」；9.2 D1）。
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

// serveBlock 处理块键 GET 的合并路径（design 9.1「取数与扇出」）：
// 先 Attach 命中在途条目；不在途且开关打开时 Lookup 查盘；仍未命中则
// Acquire 占准入槽（排队 2s、队列 64，超时回 SlowDown）；拿到槽后再试一次
// Attach、再试一次 Lookup（并发请求可能已建条目或刚 rename 落盘）；两者都
// 没有才从源站回源建条目。
//
// 读盘命中与回源共用同一个准入槽、同一个扇出路径、同一个环形缓冲
// （9.2 D34）。
func (s *Server) serveBlock(w http.ResponseWriter, r *http.Request, key string) {
	// 带 Range 的块 GET 按「键+区间」进在途合并表与片段缓存；不带 Range 的
	// 请求走整块合并与整块缓存。
	if r.Header.Get("Range") != "" {
		s.serveBlockRange(w, r, key)
		return
	}

	dataset := datasetOf(key)

	s.obs.Get(dataset)

	// 1) 在途优先。
	if e, ok := s.registry.Attach(key); ok {
		s.serveAttached(w, r, key, dataset, e, nil)
		return
	}

	// 2) 不在途且开关打开时先查盘。命中与否都要占准入槽，读盘扇出与回源
	// 用同一把额度（9.2 D34）。
	var probed *cache.Object
	if s.cacheOn && !s.registry.InFlight(key) {
		probed = s.lookupCache(dataset, key)
	}

	// 3) 占准入槽。
	release, ok := s.acquire(w, r, dataset)
	if !ok {
		if probed != nil {
			_ = probed.Close()
		}
		return
	}

	// 4) 拿槽后再试一次 Attach 与一次 Lookup。
	s.createMu.Lock()
	if e, ok := s.registry.Attach(key); ok {
		s.createMu.Unlock()
		if probed != nil {
			_ = probed.Close()
		}
		s.serveAttached(w, r, key, dataset, e, release)
		return
	}
	if s.registry.InFlight(key) {
		// 条目在途但 written > C：不加入、也不新建同名条目，独立回源
		// （design 9.1「取数与扇出」；9.2 D8）。仍占一个准入槽。条目还在表里就
		// 意味着块还没 rename 落盘，这一分支不查盘。
		s.createMu.Unlock()
		if probed != nil {
			_ = probed.Close()
		}
		s.serveIndependent(w, r, key, dataset, release)
		return
	}

	obj := s.lookupCache(dataset, key)
	if obj == nil {
		// 首次探到的对象在第二次查盘时被淘汰了，但已打开的描述符仍能读到
		// 文件尾（design 9.1「取数与扇出」）。
		obj = probed
	}

	buf := s.registry.GetBuffer()
	e := s.registry.Create(key, buf)
	s.createMu.Unlock()

	if obj != nil {
		if probed != nil && probed != obj {
			_ = probed.Close()
		}
		s.serveCacheHit(w, r, key, dataset, e, obj, release)
		return
	}

	// 5) 都没有命中：回源建条目。
	e.SetRelease(release)
	s.obs.Owner(dataset)
	s.log.Debug("条目创建", "dataset", dataset, "key", key)

	sub := e.Subscribe()
	s.log.Debug("订阅者加入", "dataset", dataset, "subs", e.Subscribers())
	start := time.Now()

	go s.pump(e, key, dataset, start)

	s.serveWithSub(w, r, key, dataset, e, sub)
}

// serveBlockRange 处理带 Range 的块 GET：按「键+区间」进在途表与磁盘缓存。
//
// 片段键既是在途表的键也是缓存键。同一段的并发请求合并成一次回源，这一段的
// 字节落进盘上的片段缓存，后来者从盘上读（design 9.1「网关 HTTP 契约」；
// 9.2 D36、D37）。
func (s *Server) serveBlockRange(w http.ResponseWriter, r *http.Request, key string) {
	dataset := datasetOf(key)
	s.obs.Get(dataset)

	br, ok := parseByteRange(r.Header.Get("Range"))
	if !ok {
		s.writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, codeInvalidRange,
			"unsupported range", false)
		return
	}
	start, end := br.start, br.end
	total := int64(-1)
	if br.suffix || br.open {
		// 开区间与后缀区间依赖对象总长度：先 HEAD 一次再解析。
		info, err := s.origin.Stat(r.Context(), key)
		if err != nil {
			s.writeOriginError(w, r, key, err)
			return
		}
		total = info.Size
		start, end, ok = br.resolve(info.Size)
		if !ok {
			s.writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, codeInvalidRange,
				"requested range not satisfiable", false)
			return
		}
	}

	fragKey := cache.FragmentKey(key, start, end)

	// 1) 在途优先。
	if e, ok := s.registry.Attach(fragKey); ok {
		s.serveFragmentAttached(w, r, key, dataset, start, end, e, nil)
		return
	}

	// 2) 不在途且开关打开时先查盘。命中与否都要占准入槽。
	var probed *cache.Object
	if s.cacheOn && !s.registry.InFlight(fragKey) {
		probed = s.lookupFragmentCache(dataset, fragKey, start, end)
	}

	// 3) 占准入槽。
	release, ok := s.acquire(w, r, dataset)
	if !ok {
		if probed != nil {
			_ = probed.Close()
		}
		return
	}

	// 4) 拿槽后再试一次 Attach 与一次 Lookup。
	s.createMu.Lock()
	if e, ok := s.registry.Attach(fragKey); ok {
		s.createMu.Unlock()
		if probed != nil {
			_ = probed.Close()
		}
		s.serveFragmentAttached(w, r, key, dataset, start, end, e, release)
		return
	}
	if s.registry.InFlight(fragKey) {
		// 条目在途但 written > C：不加入、也不新建同名条目，独立回源。
		s.createMu.Unlock()
		if probed != nil {
			_ = probed.Close()
		}
		s.serveFragmentIndependent(w, r, key, dataset, fragKey, start, end, release)
		return
	}

	obj := s.lookupFragmentCache(dataset, fragKey, start, end)
	if obj == nil {
		// 首次探到的对象在第二次查盘时被淘汰了，但已打开的描述符仍能读到
		// 文件尾。
		obj = probed
	}

	buf := s.registry.GetBuffer()
	e := s.registry.Create(fragKey, buf)
	s.createMu.Unlock()

	if obj != nil {
		if probed != nil && probed != obj {
			_ = probed.Close()
		}
		s.serveFragmentCacheHit(w, r, key, dataset, start, end, e, obj, release)
		return
	}

	// 5) 都没有命中：回源建条目。
	e.SetRelease(release)
	e.SetRange(start, end, total)
	s.obs.Owner(dataset)
	s.log.Debug("条目创建", "dataset", dataset, "key", key,
		"range", fmt.Sprintf("%d-%d", start, end))

	sub := e.Subscribe()
	s.log.Debug("订阅者加入", "dataset", dataset, "subs", e.Subscribers())
	started := time.Now()

	go s.pumpRange(e, key, dataset, fragKey, start, end, started)

	s.serveFragmentWithSub(w, r, key, dataset, e, sub, start, end)
}

// serveFragmentAttached 把 Attach 命中的片段条目转成订阅者。条目可能在
// Attach 与订阅之间结束，此时转为独立回源，绝不能交付空响应。
func (s *Server) serveFragmentAttached(w http.ResponseWriter, r *http.Request, key, dataset string,
	start, end int64, e *merge.Entry, held func()) {
	sub, ok := e.TrySubscribe()
	if !ok {
		s.log.Debug("条目在订阅前结束", "dataset", dataset, "key", key)
		if held != nil {
			s.serveFragmentIndependent(w, r, key, dataset, cache.FragmentKey(key, start, end),
				start, end, held)
			return
		}
		release, ok := s.acquire(w, r, dataset)
		if !ok {
			return
		}
		s.serveFragmentIndependent(w, r, key, dataset, cache.FragmentKey(key, start, end),
			start, end, release)
		return
	}

	if held != nil {
		held()
	}
	s.obs.Merged(dataset)
	s.log.Debug("订阅者加入", "dataset", dataset, "subs", e.Subscribers())
	s.serveFragmentWithSub(w, r, key, dataset, e, sub, start, end)
}

// serveFragmentCacheHit 用盘上的片段当 owner 侧读手：条目按普通扇出路径供
// 订阅者读取，.meta 里的 ETag、Last-Modified 与对象总长回填进 206 响应。
func (s *Server) serveFragmentCacheHit(w http.ResponseWriter, r *http.Request, key, dataset string,
	start, end int64, e *merge.Entry, obj *cache.Object, release func()) {
	e.SetRelease(release)
	meta := obj.Metadata()
	e.SetMetadata(meta.ETag, meta.LastModified)
	total := meta.ObjectSize
	if total <= 0 {
		total = -1
	}
	e.SetRange(start, end, total)

	s.obs.CacheHit()
	s.log.Debug("读盘命中", "dataset", dataset, "key", key,
		"range", fmt.Sprintf("%d-%d", start, end))

	sub := e.Subscribe()
	s.log.Debug("订阅者加入", "dataset", dataset, "subs", e.Subscribers())
	started := time.Now()

	go s.pumpCache(e, key, dataset, started, obj)

	s.serveFragmentWithSub(w, r, key, dataset, e, sub, start, end)
}

// serveFragmentIndependent 处理「片段条目在途但已越过合并窗口」的请求：直接
// 带 Range 回源，不建条目、不参与合并，结束后释放准入槽。
//
// 开关打开时这条分支的片段同样落盘。
func (s *Server) serveFragmentIndependent(w http.ResponseWriter, r *http.Request, key, dataset, fragKey string,
	start, end int64, release func()) {
	defer release()

	ctx := r.Context()
	rc, err := s.origin.OpenRange(ctx, key, start, end)
	if err != nil {
		s.writeOriginError(w, r, key, err)
		return
	}
	defer rc.Close()

	s.obs.Owner(dataset)

	var info origin.ObjectInfo
	src := io.Reader(&countingReader{r: rc, add: s.obs.Bytes})

	var tee *cacheTee
	if s.cacheOn {
		if writer := s.beginCachePut(ctx, dataset, fragKey); writer != nil {
			tee = &cacheTee{
				s:       s,
				r:       src,
				w:       writer,
				dataset: dataset,
				key:     fragKey,
				meta: func() cache.Metadata {
					return cache.Metadata{
						ETag:         info.ETag,
						LastModified: info.LastModified,
						RangeStart:   start,
						RangeEnd:     end,
						ObjectSize:   info.Size,
					}
				},
				expect: end - start + 1,
			}
			src = tee
			// 正常收尾后是空操作；中途失败时删掉 .tmp，盘上不留半成品。
			defer tee.abort()
		}
	}

	first := make([]byte, 32*1024)
	var n int
	var rerr error
	for {
		n, rerr = src.Read(first)
		if n > 0 || rerr != nil {
			break
		}
	}

	if n == 0 {
		if errors.Is(rerr, io.EOF) {
			// 空区间：源站成功但零字节，仍回 206 与区间头。
			s.writeRangeHeaders(w.Header(), origin.ObjectInfo{}, start, end, -1)
			w.WriteHeader(http.StatusPartialContent)
			return
		}
		s.writeOriginError(w, r, key, rerr)
		return
	}

	// 首读已收到 GET 响应头，Stat 从传输层记录取值、不发新请求。
	if stat, serr := rc.Stat(); serr != nil {
		s.log.Debug("读取源站元数据失败", "dataset", dataset, "err", serr)
		s.writeRangeHeaders(w.Header(), origin.ObjectInfo{}, start, end, -1)
	} else {
		info = stat
		s.writeRangeHeaders(w.Header(), info, start, end, info.Size)
	}
	w.WriteHeader(http.StatusPartialContent)
	if _, werr := w.Write(first[:n]); werr != nil {
		return
	}
	if rerr != nil {
		if !errors.Is(rerr, io.EOF) {
			s.log.Warn("源站响应中断", "dataset", dataset, "err", rerr,
				"err_class", corelog.Classify(rerr))
			return
		}
		// 首读就是 EOF：这一段已读完，正常收尾落盘。
		if tee != nil {
			tee.finish()
		}
		return
	}
	if _, err := io.Copy(w, src); err != nil {
		s.log.Warn("源站响应中断", "dataset", dataset, "err", err,
			"err_class", corelog.Classify(err))
	}
	// 这一段读完：写 .meta 再 rename 成正式名。
	if tee != nil {
		tee.finish()
	}
}

// serveFragmentWithSub 等第一个数据块或结束，然后把 [pos, written) 写给客户端。
// 响应是 206，区间头按条目记录的区间写。踢出时 panic(http.ErrAbortHandler)
// 断开连接。
func (s *Server) serveFragmentWithSub(w http.ResponseWriter, r *http.Request, key, dataset string,
	e *merge.Entry, sub *merge.Sub, start, end int64) {
	defer sub.Close()

	wroteHeader := false
	for {
		data, err := sub.Wait(r.Context())

		if len(data) > 0 {
			if !wroteHeader {
				s.setFragmentEntryHeaders(w.Header(), e, start, end)
				w.WriteHeader(http.StatusPartialContent)
				wroteHeader = true
			}
			if _, werr := w.Write(data); werr != nil {
				return
			}
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
				s.setFragmentEntryHeaders(w.Header(), e, start, end)
				w.WriteHeader(http.StatusPartialContent)
			}
			return
		case r.Context().Err() != nil:
			return
		default:
			// 拉取协程失败：尚未开始响应时按源站错误渲染；已写出字节时只能截断。
			if !wroteHeader {
				s.writeOriginError(w, r, key, err)
			}
			return
		}
	}
}

// pumpRange 是片段条目的拉取协程：带 Range 从源站取这一段，写进环形缓冲，
// 同一批字节写进盘上的片段缓存。
func (s *Server) pumpRange(e *merge.Entry, key, dataset, fragKey string, start, end int64, started time.Time) {
	// 「条目结束」在条目广播结束之前落：日志先于客户端收到最后一个字节。
	e.SetOnFinish(func(err error) {
		s.logEntryEnd(e, dataset, key, started, err, fromOrigin)
	})
	pumpCtx, cancel := context.WithCancel(s.baseCtx)
	defer cancel()
	e.SetCancel(cancel)

	rc, err := s.origin.OpenRange(pumpCtx, key, start, end)
	if err != nil {
		s.logEntryEnd(e, dataset, key, started, err, fromOrigin)
		e.Finish(err)
		return
	}
	defer rc.Close()

	src := &rangeMetadataReader{r: rc, obj: rc, e: e, log: s.log, dataset: dataset,
		start: start, end: end}
	var reader io.Reader = &countingReader{r: src, add: s.obs.Bytes}

	var tee *cacheTee
	if s.cacheOn {
		if writer := s.beginCachePut(pumpCtx, dataset, fragKey); writer != nil {
			tee = &cacheTee{
				s:       s,
				r:       reader,
				w:       writer,
				dataset: dataset,
				key:     fragKey,
				meta: func() cache.Metadata {
					etag, lastModified := e.Metadata()
					_, _, total, _ := e.Range()
					return cache.Metadata{
						ETag:         etag,
						LastModified: lastModified,
						RangeStart:   start,
						RangeEnd:     end,
						ObjectSize:   total,
					}
				},
				auto:   true,
				expect: end - start + 1,
			}
			reader = tee
			// 正常收尾后是空操作；中途失败时删掉 .tmp，盘上不留半成品。
			defer tee.abort()
		}
	}

	err = e.Pump(pumpCtx, reader)
	if err != nil && !errors.Is(err, context.Canceled) && e.Written() > 0 {
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

// lookupFragmentCache 查盘上的片段。区间长度与元数据里的字节数不一致时按未命中
// 处理：短读的片段交给客户端会少字节。
func (s *Server) lookupFragmentCache(dataset, fragKey string, start, end int64) *cache.Object {
	obj := s.lookupCache(dataset, fragKey)
	if obj == nil {
		return nil
	}
	if obj.Metadata().Size != end-start+1 {
		_ = obj.Close()
		return nil
	}
	return obj
}

// setFragmentEntryHeaders 把条目记录的区间与元数据回填进片段响应。对象总长
// 优先用源站这次响应给的（条目里记着），命中缓存时用元数据里存的那一份；
// 都没有就回总长未知的区间。
func (s *Server) setFragmentEntryHeaders(h http.Header, e *merge.Entry, start, end int64) {
	h.Set("Accept-Ranges", "bytes")
	_, _, total, ok := e.Range()
	if ok && total >= 0 {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	} else {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, end))
	}
	h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	etag, lastModified := e.Metadata()
	setMetadataHeaders(h, etag, lastModified)
}

// rangeMetadataReader 在第一次读到字节时补齐条目元数据与对象总长，再把这次
// 字节交给上层。元数据来自这次 GET 的响应头，obj.Stat() 不再发请求。
//
// 对象总长从 Content-Range 头里取：源站回 206 时带着它，命中缓存时网关手里
// 没有源站响应，只能用片段元数据里存的那一份。
type rangeMetadataReader struct {
	r       io.Reader
	obj     *origin.Object
	e       *merge.Entry
	log     *slog.Logger
	dataset string
	start   int64
	end     int64
	set     bool
}

func (m *rangeMetadataReader) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if n > 0 && !m.set {
		m.set = true
		// 必须在字节交给 Entry.Pump 之前写好元数据，保证第一次 write 广播
		// 时订阅者已能读到 Last-Modified/ETag 与区间总长。
		info, serr := m.obj.Stat()
		if serr != nil {
			m.log.Debug("读取源站元数据失败", "dataset", m.dataset, "err", serr)
		} else {
			m.e.SetMetadata(info.ETag, info.LastModified)
			if total, ok := parseContentRangeTotal(info.ContentRange); ok {
				m.e.SetRange(m.start, m.end, total)
			}
		}
	}
	return n, err
}

// parseContentRangeTotal 从 Content-Range 头里取对象总长。格式是
// "bytes start-end/total"；总长是 * 或格式不对时返回 ok=false。
func parseContentRangeTotal(v string) (int64, bool) {
	if v == "" {
		return 0, false
	}
	i := strings.LastIndex(v, "/")
	if i < 0 {
		return 0, false
	}
	total, err := strconv.ParseInt(strings.TrimSpace(v[i+1:]), 10, 64)
	if err != nil || total < 0 {
		return 0, false
	}
	return total, true
}

// writeRangeHeaders 回填 Range 响应的头部：Accept-Ranges 恒为 bytes；206 时
// 保留源站的 Content-Range（缺失时按请求区间合成，总长度未知写 *）；
// Content-Length 取源站给出的区间长度，缺失时用闭区间长度。
func (s *Server) writeRangeHeaders(h http.Header, info origin.ObjectInfo, start, end, total int64) {
	h.Set("Accept-Ranges", "bytes")
	if rangeStatus(info) == http.StatusPartialContent {
		switch {
		case info.ContentRange != "":
			h.Set("Content-Range", info.ContentRange)
		case total > 0:
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		default:
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, end))
		}
	}
	length := end - start + 1
	if info.Size > 0 {
		length = info.Size
	}
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	setMetadataHeaders(h, info.ETag, info.LastModified)
	if info.ContentType != "" {
		h.Set("Content-Type", info.ContentType)
	}
}

// rangeStatus 返回透传的 HTTP 状态码：源站明确给出 200/206 时照抄，
// 元数据缺失时按 206 处理。
func rangeStatus(info origin.ObjectInfo) int {
	if info.StatusCode == http.StatusOK || info.StatusCode == http.StatusPartialContent {
		return info.StatusCode
	}
	return http.StatusPartialContent
}

// byteRange 是单区间 Range 头的解析结果。suffix 为 true 时 end 暂存后缀长度
// （bytes=-N）；open 为 true 表示 bytes=N-。
type byteRange struct {
	start, end int64
	suffix     bool
	open       bool
}

// parseByteRange 解析单区间 Range 头，支持 bytes=start-end、bytes=start-、
// bytes=-suffix 三种形式；多区间、其他单位或语法错误返回 ok=false。
func parseByteRange(v string) (byteRange, bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(v, prefix) {
		return byteRange{}, false
	}
	spec := strings.TrimSpace(strings.TrimPrefix(v, prefix))
	if spec == "" || strings.Contains(spec, ",") {
		return byteRange{}, false
	}
	if strings.HasPrefix(spec, "-") {
		n, err := strconv.ParseInt(strings.TrimPrefix(spec, "-"), 10, 64)
		if err != nil || n <= 0 {
			return byteRange{}, false
		}
		return byteRange{suffix: true, end: n}, true
	}
	i := strings.IndexByte(spec, '-')
	if i < 0 {
		return byteRange{}, false
	}
	start, err := strconv.ParseInt(spec[:i], 10, 64)
	if err != nil || start < 0 {
		return byteRange{}, false
	}
	rest := strings.TrimSpace(spec[i+1:])
	if rest == "" {
		return byteRange{start: start, open: true}, true
	}
	end, err := strconv.ParseInt(rest, 10, 64)
	if err != nil || end < start {
		return byteRange{}, false
	}
	return byteRange{start: start, end: end}, true
}

// resolve 按对象总长度把区间解析为闭区间 [start, end]；size <= 0 或区间
// 越界返回 ok=false。
func (br byteRange) resolve(size int64) (start, end int64, ok bool) {
	if size <= 0 {
		return 0, 0, false
	}
	switch {
	case br.suffix:
		n := br.end
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	case br.open:
		if br.start >= size {
			return 0, 0, false
		}
		return br.start, size - 1, true
	default:
		if br.start >= size || br.end >= size {
			return 0, 0, false
		}
		return br.start, br.end, true
	}
}

// lookupCache 查盘。命中返回打开的对象；未命中、开关关闭或出错返回 nil。
// 读命中会刷新 last-touch 表（9.2 D33）。
func (s *Server) lookupCache(dataset, key string) *cache.Object {
	if !s.cacheOn {
		return nil
	}
	obj, err := s.cacheStore.Lookup(key)
	if err != nil {
		if !errors.Is(err, cache.ErrNotFound) {
			s.log.Debug("读盘失败", "dataset", dataset, "key", key,
				"err", err, "err_class", corelog.Classify(err))
		}
		return nil
	}
	return obj
}

// serveCacheHit 用盘上的对象当 owner 侧读手：条目按普通扇出路径供订阅者
// 读取，.meta 里的 ETag 与 Last-Modified 回填进 200 响应（design 9.1「取数与扇出」
// 第 6 步；9.2 D32、D34）。
func (s *Server) serveCacheHit(w http.ResponseWriter, r *http.Request, key, dataset string,
	e *merge.Entry, obj *cache.Object, release func()) {
	e.SetRelease(release)
	meta := obj.Metadata()
	e.SetMetadata(meta.ETag, meta.LastModified)

	// 读盘命中单独记账，不计入 owners（owners 是真实源站 GET 数）。
	s.obs.CacheHit()
	s.log.Debug("读盘命中", "dataset", dataset, "key", key)

	sub := e.Subscribe()
	s.log.Debug("订阅者加入", "dataset", dataset, "subs", e.Subscribers())
	start := time.Now()

	go s.pumpCache(e, key, dataset, start, obj)

	s.serveWithSub(w, r, key, dataset, e, sub)
}

// acquire 取一个准入槽（最长排队 2s，队列满直接拒绝）。失败时按 design
// 9.1「Agent 侧后端选择」「取数与扇出」写 503 SlowDown 并返回 false；客户端已离开时不写响应。
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
// 但所有订阅者都离开时 Entry 会取消它，底层源站 Read 随之返回（9.2 D10）。
//
// 开关打开时，同一批字节也写进 BeginPut 打开的 .tmp（design 9.1「盘上布局与配额淘汰」）。
func (s *Server) pump(e *merge.Entry, key, dataset string, start time.Time) {
	// 「条目结束」在条目广播结束之前落：日志先于客户端收到最后一个字节。
	e.SetOnFinish(func(err error) {
		s.logEntryEnd(e, dataset, key, start, err, fromOrigin)
	})
	pumpCtx, cancel := context.WithCancel(s.baseCtx)
	defer cancel()
	e.SetCancel(cancel)

	rc, err := s.origin.Open(pumpCtx, key)
	if err != nil {
		s.logEntryEnd(e, dataset, key, start, err, fromOrigin)
		e.Finish(err)
		return
	}
	defer rc.Close()

	src := &metadataReader{r: rc, obj: rc, e: e, log: s.log, dataset: dataset}
	var reader io.Reader = &countingReader{r: src, add: s.obs.Bytes}

	var tee *cacheTee
	if s.cacheOn {
		if writer := s.beginCachePut(pumpCtx, dataset, key); writer != nil {
			tee = &cacheTee{
				s:       s,
				r:       reader,
				w:       writer,
				dataset: dataset,
				key:     key,
				meta: func() cache.Metadata {
					etag, lastModified := e.Metadata()
					return cache.Metadata{ETag: etag, LastModified: lastModified}
				},
				auto: true,
			}
			reader = tee
			// 正常收尾后是空操作；中途失败时删掉 .tmp，盘上不留半成品。
			defer tee.abort()
		}
	}

	err = e.Pump(pumpCtx, reader)
	if err != nil && !errors.Is(err, context.Canceled) && e.Written() > 0 {
		// 写响应体后才失败：状态码已发出，只记录源站失败（design 9.1「取数与扇出」）。
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

// pumpCache 是读盘条目的拉取协程：把盘上的字节读进环形缓冲，结束后关闭文件。
// 与回源共用同一个扇出路径（9.2 D34）。
func (s *Server) pumpCache(e *merge.Entry, key, dataset string, start time.Time, obj *cache.Object) {
	// 「条目结束」在条目广播结束之前落：日志先于客户端收到最后一个字节。
	e.SetOnFinish(func(err error) {
		s.logEntryEnd(e, dataset, key, start, err, fromCache)
	})
	pumpCtx, cancel := context.WithCancel(s.baseCtx)
	defer cancel()
	e.SetCancel(cancel)

	_ = e.Pump(pumpCtx, obj)
	_ = obj.Close()
}

func (s *Server) logEntryEnd(e *merge.Entry, dataset, key string, start time.Time, err error, from string) {
	s.log.Debug("条目结束",
		"dataset", dataset,
		"key", key,
		"written", e.Written(),
		"duration_ms", time.Since(start).Milliseconds(),
		"subs", e.Subscribers(),
		"from", from,
		"err", err,
		"err_class", corelog.Classify(err),
	)
}

// serveAttached 把 Attach 命中的条目转成订阅者。条目可能在 Attach 与
// 订阅之间 Finish，并因订阅者为 0 立即归还缓冲，此时 TrySubscribe 返回
// false：必须按独立回源处理，绝不能写出空 200（design 9.1「取数与扇出」）。
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
// 踢出时 panic(http.ErrAbortHandler) 断开连接（design 9.1「取数与扇出」；9.2 D9）。
func (s *Server) serveWithSub(w http.ResponseWriter, r *http.Request, key, dataset string, e *merge.Entry, sub *merge.Sub) {
	defer sub.Close()

	wroteHeader := false
	for {
		data, err := sub.Wait(r.Context())

		if len(data) > 0 {
			if !wroteHeader {
				// 不设 Content-Length，走 chunked（design 9.1「取数与扇出」）。
				// 首次写字节前元数据已由拉取协程写入条目（9.2 D28）。
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
			// 已写出字节时只能截断（design 9.1「取数与扇出」）。
			if !wroteHeader {
				s.writeOriginError(w, r, key, err)
			}
			return
		}
	}
}

// serveIndependent 处理「条目在途但已越过合并窗口」的块 GET：直接回源，
// 不建条目、不参与合并，结束后释放准入槽（design 9.1「取数与扇出」）。
//
// minio 的 4xx/5xx 在首次 Read 才暴露，所以必须先读第一块再决定状态码：
// 首读就失败时把源站状态码与 Code 透传给客户端（design 9.1「取数与扇出」），不能先写 200。
//
// 开关打开时这条分支的字节同样按「盘上布局与配额淘汰」的协议落盘。
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

	var info origin.ObjectInfo
	src := io.Reader(&countingReader{r: rc, add: s.obs.Bytes})

	var tee *cacheTee
	if s.cacheOn {
		if writer := s.beginCachePut(ctx, dataset, key); writer != nil {
			tee = &cacheTee{
				s:       s,
				r:       src,
				w:       writer,
				dataset: dataset,
				key:     key,
				meta:    func() cache.Metadata { return cache.Metadata{ETag: info.ETag, LastModified: info.LastModified} },
			}
			src = tee
			// 正常收尾后是空操作；中途失败时删掉 .tmp，盘上不留半成品。
			defer tee.abort()
		}
	}

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
	// Debug，不能因此阻断已经成功的下载（9.2 D28）。
	if stat, serr := rc.Stat(); serr != nil {
		s.log.Debug("读取源站元数据失败", "dataset", dataset, "err", serr)
	} else {
		info = stat
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
			return
		}
		// 首读就是 EOF：整块已读完，正常收尾落盘。
		if tee != nil {
			tee.finish()
		}
		return
	}
	if _, err := io.Copy(w, src); err != nil {
		s.log.Warn("源站响应中断",
			"dataset", dataset,
			"err", err,
			"err_class", corelog.Classify(err),
		)
		return
	}
	// 整块读完：写 .meta 再 rename 成正式名。
	if tee != nil {
		tee.finish()
	}
}

// cacheTee 把读到的同一批字节写进缓存的 .tmp（design 9.1「盘上布局与配额淘汰」）：
// 读到 EOF 视为正常收尾，写 .meta 再 rename 成正式名；中途失败删掉 .tmp，
// 盘上不留半成品。
//
// 写盘失败（配额放不下、磁盘满、键分段过长）不影响响应：字节照常交给上层，
// 只记一条写盘失败 Warn。
type cacheTee struct {
	s       *Server
	r       io.Reader
	w       *cache.Writer
	meta    func() cache.Metadata
	dataset string
	key     string
	// auto 为 true 时读到 EOF 立即收尾。回源条目路径用 true：条目的元数据
	// 在首次读字节时就已写好，rename 早于条目摘除（design 9.1「取数与扇出」）。
	// 独立回源路径用 false：元数据要等首读之后的 Stat，收尾由调用方触发。
	auto bool
	done bool
	eof  bool
	// expect 是这一份应有的字节数，0 表示不校验。片段按区间长度校验：读到的
	// 字节少于区间长度就是短读，短读的片段不进缓存。
	expect  int64
	written int64
}

// Read 读上层给的字节，并把这批字节写进 .tmp。
func (t *cacheTee) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.written += int64(n)
	if n > 0 && t.w != nil {
		if _, werr := t.w.Write(p[:n]); werr != nil {
			// 写盘失败：不再写盘，字节照常返回。
			t.abort()
			t.s.logCacheWriteFailure(t.dataset, t.key, "write", werr)
		}
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			t.eof = true
			if t.auto {
				t.finish()
			}
		} else {
			t.abort()
		}
	}
	return n, err
}

// finish 正常收尾：写 .meta 再 rename。失败只记一条写盘失败 Warn。
func (t *cacheTee) finish() {
	if t.w == nil || t.done {
		return
	}
	if t.expect > 0 && t.written != t.expect {
		// 短读：这一份不完整，进缓存会让后来的请求拿到少字节的响应。
		t.s.log.Debug("片段字节数与区间不符", "dataset", t.dataset, "key", t.key,
			"want", t.expect, "got", t.written)
		t.abort()
		return
	}
	w := t.w
	t.w = nil
	t.done = true

	if err := w.Close(t.meta()); err != nil {
		t.s.logCacheWriteFailure(t.dataset, t.key, "close", err)
	}
}

// abort 中途失败：删掉 .tmp。已经收尾时什么都不做。
func (t *cacheTee) abort() {
	if t.w == nil || t.done {
		return
	}
	w := t.w
	t.w = nil
	t.done = true

	if err := w.Abort(); err != nil {
		t.s.log.Debug("写盘中止失败", "dataset", t.dataset, "key", t.key,
			"err", err, "err_class", corelog.Classify(err))
	}
}

// logCacheWriteFailure 记一条写盘失败 Warn。写盘失败不改变响应。
func (s *Server) logCacheWriteFailure(dataset, key, reason string, err error) {
	s.log.Warn("写盘失败",
		"dataset", dataset,
		"key", key,
		"reason", reason,
		"err", err,
		"err_class", corelog.Classify(err),
	)
}

// beginCachePut 打开写盘句柄。写盘失败不影响响应：只记一条写盘失败 Warn，
// 字节照原路返回（design 9.1「盘上布局与配额淘汰」）。ctx 已取消导致的失败属于正常中止，
// 记 Debug，不算写盘失败。
func (s *Server) beginCachePut(ctx context.Context, dataset, key string) *cache.Writer {
	writer, err := s.cacheStore.BeginPut(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			s.log.Debug("写盘中止", "dataset", dataset, "key", key,
				"err", err, "err_class", corelog.Classify(err))
		} else {
			s.logCacheWriteFailure(dataset, key, "begin_put", err)
		}
		return nil
	}
	return writer
}

// metadataReader 在第一次读到字节时补齐条目元数据，再把这次字节交给上层。
// 元数据来自这次 GET 的响应头，obj.Stat() 不再发请求（design 9.1「网关 HTTP 契约」；9.2 D28）。
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
	// 直通也读了源站的字节，一并计入 bytes：这条列的口径是「从源站读入的字节」。
	if _, err := io.Copy(w, &countingReader{r: rc, add: s.obs.Bytes}); err != nil {
		// 响应已开始，无法改状态码；中途失败按 design 9.1「取数与扇出」只记日志。
		s.log.Warn("源站响应中断", "dataset", datasetOf(key), "err", err, "err_class", corelog.Classify(err))
	}
}

// setObjectHeaders 回填源站的对象头；空值不写（design 9.1「网关 HTTP 契约」）。
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
// 合并路径保持 chunked（9.2 D28）。元数据可能尚未就位（空对象），此时跳过。
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

// writeOriginError 按 design 9.1「错误响应」映射源站错误：
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

// datasetOf 取对象键第一个 "/" 之前的部分（design 9.1「网关 HTTP 契约」）；日志只写 dataset，
// 不写高基数的完整 key（design 9.1「日志与可观测性」）。
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

// isBlockKey 判断对象键是否以 <dataset>/blocks/ 开头（design 9.1「网关 HTTP 契约」）。
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
// 原样放行，否则踢出无法断开连接（design 9.1「取数与扇出」）。响应已开始时只记日志。
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
