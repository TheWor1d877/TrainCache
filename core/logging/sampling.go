package logging

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 采样与限流的固定参数(docs/8.1 design.md §3.3 冻结,暂不开放配置)。
const (
	controlRate  = 100.0
	controlBurst = 200.0
	agentRate    = 200.0
	agentBurst   = 400.0

	// dedupWindow 是相同 (component,event,node,task_id,reason) 的去重窗口。
	dedupWindow = 60 * time.Second
	// dedupSweepThreshold 达到后尝试清理过期去重键,抑制高频任务 ID 造成的无界增长。
	dedupSweepThreshold = 4096
)

// 采样率配置项(8.1 §3.3);非法值退默认,不阻断启动。
const (
	infoSampleRateEnv  = "TRAINCACHE_LOG_SAMPLE_RATE"
	debugSampleRateEnv = "TRAINCACHE_LOG_DEBUG_SAMPLE_RATE"
	defaultInfoRate    = 1.0
	defaultDebugRate   = 0.01
)

// Sampler 是进程内日志采样器:令牌桶限流、60s 去重窗口,以及按级别的
// atomic 丢弃计数。Warn/Error 不采样、不走去重、不消耗令牌(ADR-006)。
//
// now 与 rand 可注入,测试用固定时钟与随机源;生产分别取 time.Now 与
// math/rand.Float64。
type Sampler struct {
	component string
	infoRate  float64
	debugRate float64
	rate      float64
	burst     float64

	now  func() time.Time
	rand func() float64

	mu        sync.Mutex
	tokens    float64
	last      time.Time
	seen      map[string]dedupEntry
	lastSweep time.Time

	dropped [4]atomic.Int64
}

// dedupEntry 是单个去重键在当前窗口内的状态。
type dedupEntry struct {
	start      time.Time
	suppressed int64
}

// NewControlSampler 返回控制面采样器:100 行/秒、突发 200。
// component 是丢弃计数与指标 component 标签的取值。
func NewControlSampler(component string) *Sampler {
	return newSampler(component, controlRate, controlBurst,
		envSampleRate(infoSampleRateEnv, defaultInfoRate),
		envSampleRate(debugSampleRateEnv, defaultDebugRate))
}

// NewAgentSampler 返回 Agent 采样器:200 行/秒、突发 400。
func NewAgentSampler(component string) *Sampler {
	return newSampler(component, agentRate, agentBurst,
		envSampleRate(infoSampleRateEnv, defaultInfoRate),
		envSampleRate(debugSampleRateEnv, defaultDebugRate))
}

func newSampler(component string, rate, burst, infoRate, debugRate float64) *Sampler {
	now := time.Now
	return &Sampler{
		component: component,
		infoRate:  infoRate,
		debugRate: debugRate,
		rate:      rate,
		burst:     burst,
		now:       now,
		rand:      rand.Float64,
		tokens:    burst,
		last:      now(),
		seen:      make(map[string]dedupEntry),
	}
}

// Component 返回采样器绑定的日志 component,供指标 Collector 生成标签。
func (s *Sampler) Component() string {
	if s == nil {
		return ""
	}
	return s.component
}

// Dropped 返回指定 component 与级别累计被丢弃的条数。采样器只服务一个
// component,传入其他 component 时返回 0。
func (s *Sampler) Dropped(component string, level slog.Level) int64 {
	if s == nil || component != s.component {
		return 0
	}
	return s.dropped[levelIndex(level)].Load()
}

// NewSamplingHandler 包装 next:先做采样、去重与令牌桶判断,通过后才交给
// next 格式化与脱敏,因此这一步在读 path/chunk 等字段之前完成。
func NewSamplingHandler(next slog.Handler, s *Sampler) slog.Handler {
	if next == nil || s == nil {
		return next
	}
	return &samplingHandler{next: next, sampler: s}
}

type samplingHandler struct {
	next    slog.Handler
	sampler *Sampler
	// attrs 累积 logger.With 注入的字段,用于构造去重键(component/node/task_id/reason)。
	attrs []slog.Attr
}

func (h *samplingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *samplingHandler) Handle(ctx context.Context, r slog.Record) error {
	out, ok := h.sampler.filter(r, h.attrs)
	if !ok {
		return nil
	}
	return h.next.Handle(ctx, h.sampler.annotate(out, h.attrs))
}

func (h *samplingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)
	return &samplingHandler{next: h.next.WithAttrs(attrs), sampler: h.sampler, attrs: merged}
}

// WithGroup 只把分组交给 next;去重键按扁平字段名提取(公共字段都在顶层)。
func (h *samplingHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &samplingHandler{next: h.next.WithGroup(name), sampler: h.sampler, attrs: h.attrs}
}

// filter 返回应写出的记录;ok=false 表示丢弃。顺序:采样 → 去重 → 令牌桶。
func (s *Sampler) filter(r slog.Record, attrs []slog.Attr) (slog.Record, bool) {
	level := r.Level
	// Warn/Error 永不采样、永不因限流丢弃。
	if level >= slog.LevelWarn {
		return r, true
	}
	if !s.sample(level) {
		s.recordDrop(level)
		return r, false
	}
	now := s.now()
	suppressed, ok := s.dedup(dedupKey(r, attrs), now)
	if !ok {
		s.recordDrop(level)
		return r, false
	}
	if !s.take(now) {
		s.recordDrop(level)
		return r, false
	}
	if suppressed > 0 {
		return withSuppressed(r, suppressed), true
	}
	return r, true
}

func (s *Sampler) sample(level slog.Level) bool {
	rate := s.sampleRate(level)
	if rate >= 1 {
		return true
	}
	if rate <= 0 {
		return false
	}
	return s.rand() < rate
}

// sampleRate 返回该级别的采样率;Warn/Error 固定为 1,不受配置影响。
func (s *Sampler) sampleRate(level slog.Level) float64 {
	switch {
	case level < slog.LevelInfo:
		return s.debugRate
	case level < slog.LevelWarn:
		return s.infoRate
	default:
		return 1
	}
}

// sampledState 返回放行记录的采样标记与采样率:Warn/Error 固定 sampled=false、
// sample_rate=1.0;Info/Debug 只有在采样率小于 1 且记录已通过采样时才标记
// sampled=true,否则同样放 1.0,便于按比例估算真实量(8.1 §3.1、§3.3)。
func (s *Sampler) sampledState(level slog.Level) (bool, float64) {
	rate := s.sampleRate(level)
	if rate >= 1 {
		return false, 1.0
	}
	return true, rate
}

// annotate 在放行记录上补齐公共字段:
//   - sampled/sample_rate 标明采样状态;
//   - err 存在而 err_class 缺失时按通用分类补一条,保证两者成对出现。
//
// 去重带来的 suppressed 已在 filter 阶段附加,这里只复制并追加,不改动原记录。
func (s *Sampler) annotate(r slog.Record, attrs []slog.Attr) slog.Record {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(a)
		return true
	})
	if _, ok := lookupAttr(r, attrs, "err_class"); !ok {
		if errAttr, ok := lookupAttr(r, attrs, "err"); ok {
			if class := classifyAttr(errAttr.Value); class != "" {
				out.AddAttrs(slog.String("err_class", class))
			}
		}
	}
	sampled, rate := s.sampledState(r.Level)
	out.AddAttrs(slog.Bool("sampled", sampled), slog.Float64("sample_rate", rate))
	return out
}

// lookupAttr 在记录属性与 logger.With 注入的属性里查找顶层字段。
func lookupAttr(r slog.Record, attrs []slog.Attr, key string) (slog.Attr, bool) {
	var found slog.Attr
	var ok bool
	scan := func(a slog.Attr) bool {
		a.Value = a.Value.Resolve()
		if a.Key == key {
			found, ok = a, true
			return false
		}
		return true
	}
	r.Attrs(func(a slog.Attr) bool { return scan(a) })
	if ok {
		return found, true
	}
	for _, a := range attrs {
		if !scan(a) {
			break
		}
	}
	return found, ok
}

// classifyAttr 从 err 属性值取分类:error 值走 Classify;字符串 err 没有类型
// 信息,按 io 兜底(通用分类只认超时与取消)。
func classifyAttr(v slog.Value) string {
	v = v.Resolve()
	if err, ok := v.Any().(error); ok {
		return Classify(err)
	}
	if v.Kind() == slog.KindString && v.String() != "" {
		return ErrClassIO
	}
	return ""
}

func (s *Sampler) recordDrop(level slog.Level) {
	s.dropped[levelIndex(level)].Add(1)
}

// dedup 对 key 做 60s 窗口去重。窗口内重复返回 ok=false;窗口到期后的首条
// 放行,并带回上一窗口的 suppressed 计数。
func (s *Sampler) dedup(key string, now time.Time) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.seen[key]; ok {
		if now.Sub(entry.start) < dedupWindow {
			entry.suppressed++
			s.seen[key] = entry
			return 0, false
		}
		delete(s.seen, key)
		s.seen[key] = dedupEntry{start: now}
		return entry.suppressed, true
	}
	s.seen[key] = dedupEntry{start: now}
	s.sweepLocked(now)
	return 0, true
}

// sweepLocked 在键数达到阈值时清理已过窗口的条目,限制去重表增长。
func (s *Sampler) sweepLocked(now time.Time) {
	if len(s.seen) < dedupSweepThreshold || now.Sub(s.lastSweep) < dedupWindow {
		return
	}
	for key, entry := range s.seen {
		if now.Sub(entry.start) >= dedupWindow {
			delete(s.seen, key)
		}
	}
	s.lastSweep = now
}

// take 从令牌桶取一个令牌;桶满时按固定速率补充,时钟回拨时不计额度。
func (s *Sampler) take(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if elapsed := now.Sub(s.last).Seconds(); elapsed > 0 {
		s.tokens += elapsed * s.rate
		if s.tokens > s.burst {
			s.tokens = s.burst
		}
		s.last = now
	}
	if s.tokens >= 1 {
		s.tokens--
		return true
	}
	return false
}

// dedupKey 按 (component,event,node,task_id,reason) 构造去重键;event 取日志消息。
func dedupKey(r slog.Record, attrs []slog.Attr) string {
	var component, node, taskID, reason string
	var collect func(a slog.Attr)
	collect = func(a slog.Attr) {
		a.Value = a.Value.Resolve()
		if a.Value.Kind() == slog.KindGroup {
			for _, groupAttr := range a.Value.Group() {
				collect(groupAttr)
			}
			return
		}
		if a.Value.Kind() != slog.KindString {
			return
		}
		switch a.Key {
		case "component":
			component = a.Value.String()
		case "node":
			node = a.Value.String()
		case "task_id":
			taskID = a.Value.String()
		case "reason":
			reason = a.Value.String()
		}
	}
	for _, a := range attrs {
		collect(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		collect(a)
		return true
	})
	return strings.Join([]string{component, r.Message, node, taskID, reason}, "\x00")
}

// withSuppressed 复制记录并附加 suppressed 计数,避免修改调用方传入的记录。
func withSuppressed(r slog.Record, suppressed int64) slog.Record {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(a)
		return true
	})
	out.AddAttrs(slog.Int64("suppressed", suppressed))
	return out
}

// levelIndex 把任意 slog.Level 归到 DEBUG/INFO/WARN/ERROR 四档。
func levelIndex(level slog.Level) int {
	switch {
	case level < slog.LevelInfo:
		return 0
	case level < slog.LevelWarn:
		return 1
	case level < slog.LevelError:
		return 2
	default:
		return 3
	}
}

// envSampleRate 读取采样率;空值、非数字、NaN 或超出 [0,1] 都退默认值。
func envSampleRate(name string, def float64) float64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || value < 0 || value > 1 {
		return def
	}
	return value
}
