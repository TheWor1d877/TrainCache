package collector

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/core/cas"
)

// ---------------- 测试辅助 ----------------

const (
	testTimeout = 3 * time.Second        // eventually 的 deadline(须大于 ordersource 200ms 轮询)
	quietWindow = 150 * time.Millisecond // 负断言观察窗
)

// baseTime 是测试时钟零点(确定性时间;D24 示例 last_access 同量级)。
var baseTime = time.Unix(1763000000, 0)

// testFP 构造确定性指纹(16 字节同值)。
func testFP(b byte) cas.Fingerprint {
	var fp cas.Fingerprint
	for i := range fp {
		fp[i] = b
	}
	return fp
}

// fakeClock 是可注入时钟(并发安全:后台循环与测试主体同时读)。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Set(t time.Time) {
	f.mu.Lock()
	f.now = t
	f.mu.Unlock()
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

// testEnv 是采集层测试环境。
type testEnv struct {
	t     *testing.T
	c     *Collector
	m     *metrics.Registry
	clock *fakeClock
	dir   string // blockDir(cas 根目录)
}

// newTestEnv 构造测试环境:blockDir = 临时目录(建 blocks/ 子目录)、白名单 b-track、
// 注入假时钟并提速 inotify 轮询。包内周期变量在构造前改小、构造后立即恢复(Collector
// 构造时已固化),避免与后台 goroutine 并发读写。
func newTestEnv(t *testing.T, cfg CollectorConfig) *testEnv {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, blocksDirName), 0o755); err != nil {
		t.Fatalf("建 blocks 目录: %v", err)
	}
	m := metrics.New(metrics.Config{DatasetWhitelist: []string{"b-track"}, Node: "worker-01"})
	clk := &fakeClock{now: baseTime}

	oldWatch := inotifyPollInterval
	inotifyPollInterval = 5 * time.Millisecond
	c := NewCollector(dir, m, cfg)
	inotifyPollInterval = oldWatch

	c.clock = clk.Now
	return &testEnv{t: t, c: c, m: m, clock: clk, dir: dir}
}

// start 以可取消 ctx 启动后台循环(清理时取消;取消早于包变量恢复类清理)。
func (e *testEnv) start() {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	e.c.Start(ctx)
}

// heatOf 读指纹当前热度(白盒;表锁内)。
func heatOf(t *testing.T, c *Collector, fp cas.Fingerprint) float64 {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[fp]
	if !ok {
		t.Fatalf("热度表无指纹 %s", fp)
	}
	return e.heat
}

// entryOf 读指纹记录(白盒;表锁内;不存在返回零值 + false)。
func entryOf(c *Collector, fp cas.Fingerprint) (heatEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[fp]
	if !ok {
		return heatEntry{}, false
	}
	return *e, true
}

// touchOf 读指纹 t_touch。
func touchOf(t *testing.T, c *Collector, fp cas.Fingerprint) time.Time {
	t.Helper()
	e, ok := entryOf(c, fp)
	if !ok {
		t.Fatalf("热度表无指纹 %s", fp)
	}
	return e.lastTouch
}

// writeLocalBlock 在 blocks/<fp[0:2]hex>/<fp hex>.blk 落一个本地块文件(cas 布局)。
func writeLocalBlock(t *testing.T, blockDir string, fp cas.Fingerprint) string {
	t.Helper()
	hexs := fp.String()
	dir := filepath.Join(blockDir, blocksDirName, hexs[:2])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("建分片目录: %v", err)
	}
	path := filepath.Join(dir, hexs+".blk")
	if err := os.WriteFile(path, []byte("block"), 0o644); err != nil {
		t.Fatalf("写块文件: %v", err)
	}
	return path
}

// eventually 带 deadline 轮询 cond。
func eventually(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// mustEventually deadline 内等待 cond 为真,超时 Fatal。
func mustEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !eventually(t, cond) {
		t.Fatalf("等待超时(%v):%s", testTimeout, what)
	}
}

// ---------------- 热度:β 增量 / t_touch / 计数 ----------------

// TestOpenHeatBetaIncrement:OPEN 合并 h ← h + (1−h)·β(β 默认 0.15),t_touch 与访问计数同步。
func TestOpenHeatBetaIncrement(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fp := testFP(0x11)

	for i := 0; i < 3; i++ {
		env.clock.Advance(time.Minute)
		env.c.Open(fp, ordersource.ChunkRef{})
	}

	wantHeat := 1 - math.Pow(1-defaultHeatOpenBeta, 3)
	if got := heatOf(t, env.c, fp); math.Abs(got-wantHeat) > 1e-9 {
		t.Errorf("三次打开后 h = %v, want %v(β 增量)", got, wantHeat)
	}
	if got := touchOf(t, env.c, fp); !got.Equal(env.clock.Now()) {
		t.Errorf("t_touch = %v, want %v(最后打开)", got, env.clock.Now())
	}
	e, _ := entryOf(env.c, fp)
	if e.accessCount != 3 {
		t.Errorf("access_count = %d, want 3", e.accessCount)
	}
}

// TestOpenNoProviderNoPanic:未注入订单 provider 时打开事件仍计入热度与命中率。
func TestOpenNoProviderNoPanic(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fp := testFP(0x12)
	env.c.Open(fp, ordersource.ChunkRef{Path: "shard-000.tar", Chunk: 1})
	if got := env.c.HitRate(); got != 0 {
		t.Errorf("块不存在时命中率 = %v, want 0", got)
	}
	if _, ok := entryOf(env.c, fp); !ok {
		t.Error("打开事件应建立热度条目")
	}
}

// ---------------- 命中率:本地块交集 / 指标 / 阶段聚合 ----------------

// TestOpenHitMissCounts:本地块存在 → 命中;不存在 → 未命中;窗口命中率 = 命中占比。
func TestOpenHitMissCounts(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	present, absent := testFP(0x21), testFP(0x22)
	writeLocalBlock(t, env.dir, present)

	env.c.Open(present, ordersource.ChunkRef{})
	env.c.Open(absent, ordersource.ChunkRef{})
	env.c.Open(present, ordersource.ChunkRef{})

	if got := testutil.ToFloat64(env.m.CacheHits().WithLabelValues("other", "success", metrics.SourceUnknown, metrics.PhaseUnknown)); got != 2 {
		t.Errorf("cache_hits{other} = %v, want 2(白名单外归并 other)", got)
	}
	if got := testutil.ToFloat64(env.m.CacheMisses().WithLabelValues("other", "success", metrics.PhaseUnknown)); got != 1 {
		t.Errorf("cache_misses{other} = %v, want 1", got)
	}
	if got := env.c.HitRate(); math.Abs(got-2.0/3.0) > 1e-9 {
		t.Errorf("HitRate = %v, want 2/3", got)
	}
}

// TestOpenUsesProviderDatasetAndStage:provider 注入订单时 dataset 走白名单标签,
// 命中率按 StageID 聚合。
func TestOpenUsesProviderDatasetAndStage(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	order := testOrder("b-track", "epoch-2", 0, 1)
	env.c.SetOrderContextProvider(func(ref ordersource.ChunkRef, fp cas.Fingerprint) (string, string, ordersource.ConsumeOrder, bool) {
		return order.Dataset, order.StageID, order, true
	})
	present, absent := testFP(0x31), testFP(0x32)
	writeLocalBlock(t, env.dir, present)

	env.c.Open(present, ordersource.ChunkRef{Path: "shard-000.tar", Chunk: 0})
	env.c.Open(absent, ordersource.ChunkRef{Path: "shard-000.tar", Chunk: 1})

	if got := testutil.ToFloat64(env.m.CacheHits().WithLabelValues("b-track", "success", metrics.SourceUnknown, metrics.PhaseUnknown)); got != 1 {
		t.Errorf("cache_hits{b-track} = %v, want 1(白名单命中保留原名)", got)
	}
	if got := env.c.HitRateForStage("epoch-2"); got != 0.5 {
		t.Errorf("HitRateForStage(epoch-2) = %v, want 0.5", got)
	}
	if got := env.c.HitRateForStage("epoch-9"); got != 0 {
		t.Errorf("未知阶段命中率 = %v, want 0", got)
	}
}

// TestOrderProviderNotOK:provider 无上下文(空 dataset/stage、!ok)时:
// 标签归 other、阶段不聚合、交叉校准不参与。
func TestOrderProviderNotOK(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{CalibrateWindow: 2, CalibrateThreshold: 0.8})
	env.c.SetOrderContextProvider(func(ref ordersource.ChunkRef, fp cas.Fingerprint) (string, string, ordersource.ConsumeOrder, bool) {
		return "", "", ordersource.ConsumeOrder{}, false
	})
	var got []ordersource.ChunkRef
	env.c.SetAdvanceCallback(func(dataset string, ref ordersource.ChunkRef) { got = append(got, ref) })

	fp := testFP(0x33)
	env.c.Open(fp, ordersource.ChunkRef{Path: "shard-000.tar", Chunk: 0})
	env.c.Open(fp, ordersource.ChunkRef{Path: "shard-000.tar", Chunk: 0})

	if got := testutil.ToFloat64(env.m.CacheMisses().WithLabelValues("other", "success", metrics.PhaseUnknown)); got != 2 {
		t.Errorf("cache_misses{other} = %v, want 2(!ok 时标签归 other)", got)
	}
	if got := env.c.HitRateForStage("epoch-1"); got != 0 {
		t.Errorf("!ok 时阶段命中率 = %v, want 0(不聚合)", got)
	}
	if len(got) != 0 {
		t.Errorf("!ok 时校准回调收到 %v, want 空(无期望顺序)", got)
	}
}

// ---------------- 声明式接口:Touch / ListForEviction / Score ----------------

// TestTouchIdempotentAndCreates:Touch 幂等(不回退 t_touch);不存在则建 h=0 条目。
func TestTouchIdempotentAndCreates(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fp := testFP(0x41)

	env.c.Touch(fp, baseTime)
	e, ok := entryOf(env.c, fp)
	if !ok {
		t.Fatal("Touch 应建立条目")
	}
	if e.heat != 0 || e.accessCount != 0 {
		t.Errorf("新建条目 h=%v access_count=%d, want h=0 / 0", e.heat, e.accessCount)
	}
	if !e.lastTouch.Equal(baseTime) {
		t.Errorf("新建条目 t_touch = %v, want %v", e.lastTouch, baseTime)
	}

	env.c.Touch(fp, baseTime) // 同一 t:幂等
	if got := touchOf(t, env.c, fp); !got.Equal(baseTime) {
		t.Errorf("重复 Touch 后 t_touch = %v, want %v", got, baseTime)
	}

	env.c.Touch(fp, baseTime.Add(-time.Hour)) // 更早的 t:不回退
	if got := touchOf(t, env.c, fp); !got.Equal(baseTime) {
		t.Errorf("Touch 更早时间后 t_touch = %v, want %v(不回退)", got, baseTime)
	}

	later := baseTime.Add(time.Hour) // 更新的 t:前进
	env.c.Touch(fp, later)
	if got := touchOf(t, env.c, fp); !got.Equal(later) {
		t.Errorf("Touch 更新时间后 t_touch = %v, want %v", got, later)
	}
}

// TestListForEvictionSnapshot:全量快照含 Touch 建的 h=0 条目;调用方排序。
func TestListForEvictionSnapshot(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	hot, cold := testFP(0x51), testFP(0x52)
	env.c.Open(hot, ordersource.ChunkRef{})
	env.c.Touch(cold, baseTime.Add(time.Minute))

	entries := env.c.ListForEviction()
	if len(entries) != 2 {
		t.Fatalf("快照条目数 = %d, want 2", len(entries))
	}
	sort.Slice(entries, func(i, j int) bool { return string(entries[i].FP[:]) < string(entries[j].FP[:]) })
	if entries[0].FP != hot {
		t.Fatalf("排序后首条 = %s, want %s", entries[0].FP, hot)
	}
	if math.Abs(entries[0].Heat-defaultHeatOpenBeta) > 1e-9 {
		t.Errorf("热度条目 h = %v, want %v", entries[0].Heat, defaultHeatOpenBeta)
	}
	if entries[1].Heat != 0 {
		t.Errorf("Touch 条目 h = %v, want 0", entries[1].Heat)
	}
	if !entries[1].LastTouch.Equal(baseTime.Add(time.Minute)) {
		t.Errorf("Touch 条目 t_touch = %v, want %v", entries[1].LastTouch, baseTime.Add(time.Minute))
	}
}

// TestScoreIsMaxOfHeatAndFreshness:S = max(h, R),R = 0.5^(Δt/T_R)(T_R 默认 6h)。
func TestScoreIsMaxOfHeatAndFreshness(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	hotFP, freshFP, staleFP := testFP(0x61), testFP(0x62), testFP(0x63)

	env.clock.Set(baseTime)
	for i := 0; i < 20; i++ { // h 收敛到 ~1(多次打开)
		env.c.Open(hotFP, ordersource.ChunkRef{})
	}
	env.c.Touch(freshFP, baseTime)                    // R = 1
	env.c.Touch(staleFP, baseTime.Add(-12*time.Hour)) // R = 0.5^2 = 0.25,超龄

	if got, want := env.c.Score(freshFP), 1.0; math.Abs(got-want) > 1e-9 {
		t.Errorf("刚 touch 的条目 S = %v, want %v(max(0,1))", got, want)
	}
	if got, want := env.c.Score(staleFP), 0.25; math.Abs(got-want) > 1e-9 {
		t.Errorf("半衰期两倍的条目 S = %v, want %v", got, want)
	}
	if got := env.c.Score(hotFP); got <= 0.25 {
		t.Errorf("历史热块 S = %v, want > 0.25(h 侧保护)", got)
	}

	// 半衰期:Δt = 6h(T_R)→ R = 0.5。
	env.c.Touch(staleFP, baseTime.Add(-defaultHeatTouchHalfLife))
	if got, want := env.c.Score(staleFP), 0.5; math.Abs(got-want) > 1e-9 {
		t.Errorf("Δt=T_R 的条目 S = %v, want %v", got, want)
	}
	// 未知指纹 → 0。
	if got := env.c.Score(testFP(0x6f)); got != 0 {
		t.Errorf("未知指纹 S = %v, want 0", got)
	}
}

// TestScoreAtPureFunction:ScoreAt 与 Collector.Score 同口径(接线层/M6 复用)。
func TestScoreAtPureFunction(t *testing.T) {
	entry := EvictionEntry{Heat: 0.1, LastTouch: baseTime.Add(-3 * time.Hour)}
	want := math.Pow(0.5, 3.0/6.0)
	if got := ScoreAt(baseTime, entry, 6*time.Hour); math.Abs(got-want) > 1e-9 {
		t.Errorf("ScoreAt = %v, want %v(R 侧)", got, want)
	}
	entry.Heat = 0.9
	if got := ScoreAt(baseTime, entry, 6*time.Hour); got != 0.9 {
		t.Errorf("ScoreAt = %v, want 0.9(h 侧取强)", got)
	}
}

// ---------------- advance 事件:计数 + 回调共用 ----------------

// TestAdvanceCountsOnly:Advance 只做推进计数,不分发回调(回调只由交叉校准触发)。
func TestAdvanceCountsOnly(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})

	var got []ordersource.ChunkRef
	env.c.SetAdvanceCallback(func(dataset string, ref ordersource.ChunkRef) { got = append(got, ref) })
	env.c.Advance(3)
	env.c.Advance(4)
	env.c.SetAdvanceCallback(nil)
	env.c.Advance(5)

	if len(got) != 0 {
		t.Errorf("Advance 触发回调 %v, want 空(advance 只计数)", got)
	}
	env.c.advMu.Lock()
	n := env.c.advances
	env.c.advMu.Unlock()
	if n != 3 {
		t.Errorf("advance 计数 = %d, want 3", n)
	}
}

// ---------------- inotify 兜底:钩子为主的双源口径 ----------------

// TestInotifyFallbackGatedByHookSilence:钩子窗口内报告过打开事件时,inotify 打开事件不计入;
// 钩子沉默超过窗口后兜底生效(§5.4"钩子为主,inotify 兜底")。
func TestInotifyFallbackGatedByHookSilence(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	hookFP, watchFP := testFP(0x71), testFP(0x72)

	env.c.Open(hookFP, ordersource.ChunkRef{}) // 钩子主源活跃
	env.c.openFromWatch(watchFP, env.clock.Now())
	if _, ok := entryOf(env.c, watchFP); ok {
		t.Error("钩子活跃期间的 inotify 打开事件不应计入(双源去重)")
	}

	// 时钟推进到沉默窗之后(hookSilenceWindow 构造时固化为 c.hookSilence)。
	env.clock.Advance(env.c.hookSilence + time.Millisecond)
	env.c.openFromWatch(watchFP, env.clock.Now())
	if _, ok := entryOf(env.c, watchFP); !ok {
		t.Error("钩子沉默超过窗口后 inotify 兜底应计入打开事件")
	}
	// 未命中合计 2 次:hookFP 主源打开 1 次 + watchFP 兜底 1 次(两者均无本地块);
	// 抑制期间的 openFromWatch 未产生第 3 次。
	if got := testutil.ToFloat64(env.m.CacheMisses().WithLabelValues("other", "success", metrics.PhaseUnknown)); got != 2 {
		t.Errorf("cache_misses{other,success} = %v, want 2(主源 1 + 兜底 1,抑制期 0)", got)
	}
	if got := testutil.ToFloat64(env.m.CacheHits().WithLabelValues("other", "success", metrics.SourceUnknown, metrics.PhaseUnknown)); got != 0 {
		t.Errorf("cache_hits{other} = %v, want 0(无本地块)", got)
	}
}

// TestOpenCallbackNoDedup:兜底门控通过时每个 OPEN 事件回调一次;同一指纹调用两次
// 即回调两次(去重不在采集层,由上层消费位点的单调性负责)。
func TestOpenCallbackNoDedup(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fp := testFP(0x73)

	var got []cas.Fingerprint
	env.c.SetOpenCallback(func(v cas.Fingerprint) { got = append(got, v) })

	env.c.openFromWatch(fp, env.clock.Now())
	env.c.openFromWatch(fp, env.clock.Now())

	if len(got) != 2 || got[0] != fp || got[1] != fp {
		t.Errorf("回调收到 %v, want 同一指纹两次", got)
	}
}

// TestOpenCallbackGatedByHookSilence:钩子活跃(沉默窗内)时兜底被抑制,回调不触发;
// 沉默窗过后兜底生效,回调收到指纹。
func TestOpenCallbackGatedByHookSilence(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	hookFP, watchFP := testFP(0x74), testFP(0x75)

	var got []cas.Fingerprint
	env.c.SetOpenCallback(func(v cas.Fingerprint) { got = append(got, v) })

	env.c.Open(hookFP, ordersource.ChunkRef{}) // 钩子主源活跃
	env.c.openFromWatch(watchFP, env.clock.Now())
	if len(got) != 0 {
		t.Errorf("钩子活跃期回调收到 %v, want 空", got)
	}

	env.clock.Advance(env.c.hookSilence + time.Millisecond)
	env.c.openFromWatch(watchFP, env.clock.Now())
	if len(got) != 1 || got[0] != watchFP {
		t.Errorf("沉默窗过后回调收到 %v, want [%v]", got, watchFP)
	}
}

// TestOpenCallbackNilSafe:未注册回调(或显式 nil)时兜底打开事件不 panic。
func TestOpenCallbackNilSafe(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fp := testFP(0x76)

	env.c.openFromWatch(fp, env.clock.Now()) // 从未注册
	env.c.SetOpenCallback(func(cas.Fingerprint) {})
	env.c.SetOpenCallback(nil)
	env.c.openFromWatch(fp, env.clock.Now()) // 显式 nil
}

// TestParseBlockName:块文件名解析(cas 布局 <32 hex>.blk;临时文件与非法名不计数)。
func TestParseBlockName(t *testing.T) {
	fp := testFP(0xab)
	cases := []struct {
		name string
		want bool
	}{
		{fp.String() + ".blk", true},
		{fp.String() + ".tmp", false},
		{"tmp-123456", false},
		{"deadbeef.blk", false},
		{fp.String(), false},
		{"zz" + fp.String()[2:] + ".blk", false},
	}
	for _, tc := range cases {
		got, ok := parseBlockName(tc.name)
		if ok != tc.want {
			t.Errorf("parseBlockName(%q) ok = %v, want %v", tc.name, ok, tc.want)
		}
		if ok && got != fp {
			t.Errorf("parseBlockName(%q) = %s, want %s", tc.name, got, fp)
		}
	}
}

// ---------------- 订单与 provider 测试辅助 ----------------

// testOrder 构造带 chunks 的订单。
func testOrder(dataset, stage string, chunks ...int64) ordersource.ConsumeOrder {
	o := ordersource.ConsumeOrder{
		Dataset:    dataset,
		SnapshotID: "snap-20261015",
		StageID:    stage,
		Chunks:     make([]ordersource.ChunkRef, 0, len(chunks)),
	}
	for _, c := range chunks {
		o.Chunks = append(o.Chunks, ordersource.ChunkRef{Path: "shard-000.tar", Chunk: c})
	}
	return o
}
