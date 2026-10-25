package evictor

// 本文件按 6.2 §8 任务 5 五判据覆盖:触发线/危险线阈值行为(淘汰序与 S 升序、危险线
// R 清零、降至 k-Δ / k-2Δ 停下)、8 块硬保护豁免、pin 块 ErrInUse 跳过、Sweep 先于
// Evict 与 GC 打点;另覆盖配置默认值、除零/查询失败防御、Start 周期与 ctx 退出。
// 装置:真实 core cas.Store(t.TempDir)落块;Usage/Sweep/热度/保护全部注入假件
// (无真实文件系统利用率与 collector 依赖)。

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/core/cas"
)

// fakeUsage 是注入的利用率来源:每次调用按序取 values 中的比例(耗尽后保持末值),
// total 为固定总字节数;calls 记录调用次数(Start/取消断言用)。
type fakeUsage struct {
	mu     sync.Mutex
	total  uint64
	values []float64
	idx    int
	calls  int
	err    error
}

// set 重置序列(每次 RunOnce 前调用)。
func (f *fakeUsage) set(values ...float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values = values
	f.idx = 0
	f.calls = 0
	f.err = nil
}

func (f *fakeUsage) use() (uint64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return 0, 0, f.err
	}
	var v float64
	if len(f.values) > 0 {
		v = f.values[min(f.idx, len(f.values)-1)]
		f.idx++
	}
	return uint64(math.Round(v * float64(f.total))), f.total, nil
}

func (f *fakeUsage) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeHeat 是注入的热度来源(D31 窄接口):返回 entries 的快照(防排序改写测试数据),
// log 可选记录调用次序。
type fakeHeat struct {
	mu      sync.Mutex
	entries []EvictionEntry
	log     *[]string
	calls   int
}

func (f *fakeHeat) ListForEviction() []EvictionEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.log != nil {
		*f.log = append(*f.log, "list")
	}
	return slices.Clone(f.entries)
}

func (f *fakeHeat) set(entries ...EvictionEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = entries
}

func (f *fakeHeat) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeProtected 是注入的硬保护集合(D24:cursor + lookahead 16 块)。
type fakeProtected struct {
	fps []cas.Fingerprint
}

func (f *fakeProtected) ProtectedFPs() []cas.Fingerprint { return slices.Clone(f.fps) }

// fakeSweep 是注入的 GC 驱动:记录调用序,返回预设的 (blocks, objects, err),
// fn 为可选副作用(模拟 Sweep 已回收某块)。
type fakeSweep struct {
	mu      sync.Mutex
	log     *[]string
	blocks  int64
	objects int64
	err     error
	fn      func()
	calls   int
}

func (f *fakeSweep) sweep(ctx context.Context) (int64, int64, error) {
	f.mu.Lock()
	f.calls++
	if f.log != nil {
		*f.log = append(*f.log, "sweep")
	}
	fn, blocks, objects, err := f.fn, f.blocks, f.objects, f.err
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
	return blocks, objects, err
}

// evictFixture 是淘汰循环测试装置:真实 CAS + 假 Usage/Sweep/热度/保护 + 真实 metrics
// 注册表(白名单空 → dataset 归并 "other",断言标签即验证归并生效)。
type evictFixture struct {
	cs        *cas.Store
	m         *metrics.Registry
	usage     *fakeUsage
	heat      *fakeHeat
	protected *fakeProtected
	sweep     *fakeSweep
	order     []string // heat.list / sweep 的调用序记录
	ev        *Evictor
}

func newEvictFixture(t *testing.T) *evictFixture {
	t.Helper()
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	fx := &evictFixture{
		cs:        cs,
		m:         metrics.New(metrics.Config{}),
		usage:     &fakeUsage{total: 10000},
		heat:      &fakeHeat{},
		protected: &fakeProtected{},
		sweep:     &fakeSweep{},
	}
	fx.heat.log = &fx.order
	fx.sweep.log = &fx.order
	fx.ev = New(cs, fx.heat, fx.protected, fx.m, nil, Config{
		Dataset: "ds",
		Usage:   fx.usage.use,
		Sweep:   fx.sweep.sweep,
	})
	return fx
}

// putBlock 在真实 CAS 落一个块,返回指纹(淘汰断言对象)。
func (fx *evictFixture) putBlock(t *testing.T, content string) cas.Fingerprint {
	t.Helper()
	fp, _, err := fx.cs.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	return fp
}

// blockExists 判断块文件是否仍在本地(Open 成功即在;顺带校验内容未损坏)。
func (fx *evictFixture) blockExists(t *testing.T, fp cas.Fingerprint) bool {
	t.Helper()
	br, err := fx.cs.Open(fp)
	if err == nil {
		_ = br.Close()
		return true
	}
	if errors.Is(err, cas.ErrNotFound) {
		return false
	}
	t.Fatalf("cas.Open(%s): %v", fp, err)
	return false
}

func (fx *evictFixture) evictCount(result string) float64 {
	return testutil.ToFloat64(fx.m.EvictBlocks().WithLabelValues("other", result))
}

func (fx *evictFixture) skippedCount(result string) float64 {
	return testutil.ToFloat64(fx.m.EvictProtectedSkipped().WithLabelValues("other", result))
}

func (fx *evictFixture) gcSweepsCount(result string) float64 {
	return testutil.ToFloat64(fx.m.GCSweeps().WithLabelValues("other", result))
}

func (fx *evictFixture) gcBlocksRemoved() float64 {
	return testutil.ToFloat64(fx.m.GCBlocksRemoved().WithLabelValues("other", metrics.ResultSuccess))
}

func (fx *evictFixture) gcObjectsRemoved() float64 {
	return testutil.ToFloat64(fx.m.GCObjectsRemoved().WithLabelValues("other", metrics.ResultSuccess))
}

func (fx *evictFixture) usageGauge() float64 {
	return testutil.ToFloat64(fx.m.DiskUsageRatio().WithLabelValues("other"))
}

// entry 构造候选(h + "多少小时前触摸";R 半衰期 6h 便于推算 S)。
func entry(fp cas.Fingerprint, heat float64, touchedAgo time.Duration) EvictionEntry {
	return EvictionEntry{FP: fp, Heat: heat, LastTouch: time.Now().Add(-touchedAgo)}
}

// TestRunOnceIdleAndLines:util ≤ k 空转(利用率 gauge 照常刷新,不 Sweep 不取候选);
// util 恰为 M 仍走触发线(危险线须严格超 M)。
func TestRunOnceIdleAndLines(t *testing.T) {
	fx := newEvictFixture(t)
	fp := fx.putBlock(t, "idle-block")
	fx.heat.set(entry(fp, 0.1, time.Hour))
	fx.sweep.blocks, fx.sweep.objects = 3, 1

	// 1) util = 0.80 恰为 k:空转。
	fx.usage.set(0.80)
	fx.ev.RunOnce(context.Background())
	if got := fx.usageGauge(); got != 0.80 {
		t.Errorf("disk_usage_ratio = %v, want 0.80(空转轮照常刷新)", got)
	}
	if fx.sweep.calls != 0 || fx.heat.callCount() != 0 {
		t.Errorf("空转不应 Sweep/取候选(sweep=%d list=%d)", fx.sweep.calls, fx.heat.callCount())
	}
	if !fx.blockExists(t, fp) {
		t.Error("空转不应淘汰任何块")
	}

	// 2) util = 0.90 恰为 M:触发线(result=normal),非危险线;降至 ≤ k-Δ 即停。
	fx.usage.set(0.90, 0.74)
	fx.ev.RunOnce(context.Background())
	if got := fx.evictCount(metrics.ResultNormal); got != 1 {
		t.Errorf("evict_blocks{normal} = %v, want 1(util == M 走触发线)", got)
	}
	if got := fx.evictCount(metrics.ResultDanger); got != 0 {
		t.Errorf("evict_blocks{danger} = %v, want 0", got)
	}
	if fx.blockExists(t, fp) {
		t.Error("触发线轮过后块应被淘汰")
	}
	if got := fx.gcSweepsCount(metrics.ResultSuccess); got != 1 {
		t.Errorf("gc_sweeps{success} = %v, want 1(触发轮先 Sweep)", got)
	}
}

// TestRunOnceTriggerLineEvictsByScore:超 k → 按 S = max(h, R) 升序逐块 Evict,
// 降至 k-Δ(0.75)即停;输入序按 S 降序堆放,淘汰序正确即证明排序发生。
func TestRunOnceTriggerLineEvictsByScore(t *testing.T) {
	fx := newEvictFixture(t)
	// S(半衰期 6h):high≈1.0 > midH=0.7 > midL=0.5 > low1=0.1 > low2=0.05。
	high := fx.putBlock(t, "trig-high")
	midH := fx.putBlock(t, "trig-midh")
	midL := fx.putBlock(t, "trig-midl")
	low1 := fx.putBlock(t, "trig-low1")
	low2 := fx.putBlock(t, "trig-low2")
	fx.heat.set(
		entry(high, 0.90, 0),
		entry(midH, 0.70, 12*time.Hour),
		entry(midL, 0.20, 6*time.Hour),
		entry(low1, 0.10, 24*time.Hour),
		entry(low2, 0.05, 48*time.Hour),
	)

	// 0.85 触发;淘汰一块后 0.76(仍 > k-Δ=0.75,继续);再一块后 0.74 ≤ 0.75,停。
	fx.usage.set(0.85, 0.76, 0.74)
	fx.ev.RunOnce(context.Background())

	for _, tc := range []struct {
		name string
		fp   cas.Fingerprint
		want bool // 是否仍在本地
	}{
		{"low2(最低 S)", low2, false},
		{"low1(次低 S)", low1, false},
		{"midL(第三)", midL, true},
		{"midH", midH, true},
		{"high", high, true},
	} {
		if got := fx.blockExists(t, tc.fp); got != tc.want {
			t.Errorf("%s 存在 = %v, want %v(S 升序淘汰、降至 k-Δ 停下)", tc.name, got, tc.want)
		}
	}
	if got := fx.evictCount(metrics.ResultNormal); got != 2 {
		t.Errorf("evict_blocks{normal} = %v, want 2", got)
	}
	if got := fx.skippedCount(metrics.ResultNormal); got != 0 {
		t.Errorf("evict_protected_skipped{normal} = %v, want 0", got)
	}
	if got := fx.heat.callCount(); got != 1 {
		t.Errorf("ListForEviction 调用 = %d, want 1(每轮一次全量快照)", got)
	}
}

// TestRunOnceDangerLineClearsFreshness:超 M → R 清零只按 h 升序(高 R 低 h 的块不再
// 靠新鲜度兜底),激进淘汰至 k-2Δ;result=danger 与触发线区分。
func TestRunOnceDangerLineClearsFreshness(t *testing.T) {
	fx := newEvictFixture(t)
	// normal 序(S):C(0.3) → B(0.9) → A(1.0);danger 序(h):C(0.3) → A(0.6) → B(0.9)。
	// 第二轮淘汰 A(而非 B)即证明 R 已清零:低 h 高 R 的纯投机块 A(预取了还没被读)
	// 先于高 h 低 R 的 B 被牺牲——"激进"口径(D24;§7.6"按 h 升序")。
	fpA := fx.putBlock(t, "danger-a")
	fpB := fx.putBlock(t, "danger-b")
	fpC := fx.putBlock(t, "danger-c")
	fx.heat.set(
		entry(fpA, 0.6, 0),
		entry(fpB, 0.9, 48*time.Hour),
		entry(fpC, 0.3, 24*time.Hour),
	)

	// 0.95 超 M,目标 k-2Δ=0.70:0.72 仍 > 0.70 继续,0.70 ≤ 0.70 停。
	fx.usage.set(0.95, 0.72, 0.70)
	fx.ev.RunOnce(context.Background())

	if fx.blockExists(t, fpA) || fx.blockExists(t, fpC) {
		t.Error("danger 轮应淘汰 h 小的 C(0.3)、A(0.6)(R 清零、忽略 LastTouch)")
	}
	if !fx.blockExists(t, fpB) {
		t.Error("B(h=0.9)在 danger 轮应最后被淘汰,本例应保留")
	}
	if got := fx.evictCount(metrics.ResultDanger); got != 2 {
		t.Errorf("evict_blocks{danger} = %v, want 2", got)
	}
	if got := fx.evictCount(metrics.ResultNormal); got != 0 {
		t.Errorf("evict_blocks{normal} = %v, want 0", got)
	}
	if got := fx.skippedCount(metrics.ResultDanger); got != 0 {
		t.Errorf("evict_protected_skipped{danger} = %v, want 0", got)
	}
}

// TestRunOnceProtectedBlocksSkipped:8 块硬保护(cursor + lookahead)任何情况跳过并计
// protected_skipped;保护块按 S 最低堆放——若遇保护即停,后面的块永远淘汰不了。
func TestRunOnceProtectedBlocksSkipped(t *testing.T) {
	fx := newEvictFixture(t)
	protect := make([]cas.Fingerprint, 0, 8)
	entries := make([]EvictionEntry, 0, 10)
	for i := 0; i < 8; i++ {
		fp := fx.putBlock(t, fmt.Sprintf("prot-%d", i))
		protect = append(protect, fp)
		entries = append(entries, entry(fp, 0.01*float64(i+1), 48*time.Hour)) // S = h ≤ 0.08
	}
	ev1 := fx.putBlock(t, "prot-ev1")
	ev2 := fx.putBlock(t, "prot-ev2")
	entries = append(entries, entry(ev1, 0.5, 0), entry(ev2, 0.6, 0)) // S ≈ 1.0
	fx.heat.set(entries...)
	fx.protected.fps = protect

	// 利用率恒 0.85(降不到 k-Δ=0.75):全量候选走一遍——8 块跳过、2 块淘汰。
	fx.usage.set(0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85, 0.85)
	fx.ev.RunOnce(context.Background())

	for i, fp := range protect {
		if !fx.blockExists(t, fp) {
			t.Errorf("受保护块 %d 不应被淘汰", i)
		}
	}
	if fx.blockExists(t, ev1) || fx.blockExists(t, ev2) {
		t.Error("非保护块应被淘汰(跳过保护块后继续处理其余候选)")
	}
	if got := fx.skippedCount(metrics.ResultNormal); got != 8 {
		t.Errorf("evict_protected_skipped{normal} = %v, want 8", got)
	}
	if got := fx.evictCount(metrics.ResultNormal); got != 2 {
		t.Errorf("evict_blocks{normal} = %v, want 2", got)
	}
}

// TestRunOncePinnedBlockSkipped:pin > 0 的块 cas.Evict 返回 ErrInUse → 跳过并计
// protected_skipped;Unpin 后下一轮即可淘汰(跳过由 pin 驱动而非保护集合)。
func TestRunOncePinnedBlockSkipped(t *testing.T) {
	fx := newEvictFixture(t)
	pinned := fx.putBlock(t, "pin-block")
	other := fx.putBlock(t, "pin-other")
	fx.heat.set(entry(pinned, 0.1, 0), entry(other, 0.2, 0))
	if err := fx.cs.Pin(pinned); err != nil {
		t.Fatalf("cas.Pin: %v", err)
	}
	t.Cleanup(func() { fx.cs.Unpin(pinned) })

	fx.usage.set(0.85, 0.74)
	fx.ev.RunOnce(context.Background())
	if !fx.blockExists(t, pinned) || !fx.blockExists(t, other) {
		t.Fatal("首轮:pin 块被跳过、other 未及处理,两者都应仍在本地")
	}
	if got := fx.skippedCount(metrics.ResultNormal); got != 1 {
		t.Errorf("evict_protected_skipped{normal} = %v, want 1(pin 块)", got)
	}
	if got := fx.evictCount(metrics.ResultNormal); got != 0 {
		t.Errorf("evict_blocks{normal} = %v, want 0(util 已 ≤ k-Δ 停下)", got)
	}

	fx.cs.Unpin(pinned)
	fx.usage.set(0.85, 0.74)
	fx.ev.RunOnce(context.Background())
	if fx.blockExists(t, pinned) {
		t.Error("Unpin 后 pin 块应被淘汰")
	}
	if got := fx.evictCount(metrics.ResultNormal); got != 1 {
		t.Errorf("evict_blocks{normal} = %v, want 1(Unpin 后)", got)
	}
	if got := fx.skippedCount(metrics.ResultNormal); got != 1 {
		t.Errorf("evict_protected_skipped{normal} = %v, want 1(累计不变)", got)
	}
}

// TestRunOnceSweepBeforeEvict:每轮先 Sweep 再取候选淘汰(调用序断言 + 假 Sweep 先
// 收走最差块:若次序颠倒该块会被成功淘汰并计数);成功/失败分别打 GC 指标。
func TestRunOnceSweepBeforeEvict(t *testing.T) {
	t.Run("Sweep 先于取候选且成功打点", func(t *testing.T) {
		fx := newEvictFixture(t)
		worst := fx.putBlock(t, "sweep-worst")
		mid := fx.putBlock(t, "sweep-mid")
		best := fx.putBlock(t, "sweep-best")
		fx.heat.set(entry(worst, 0.1, 0), entry(mid, 0.2, 0), entry(best, 0.3, 0))
		fx.sweep.blocks, fx.sweep.objects = 7, 2
		fx.sweep.fn = func() { _ = fx.cs.Evict(worst) } // 模拟 Sweep 已回收最差块

		fx.usage.set(0.85, 0.74)
		fx.ev.RunOnce(context.Background())

		if got := fx.order; !slices.Equal(got, []string{"sweep", "list"}) {
			t.Errorf("调用序 = %v, want [sweep list](Sweep 先于取候选/淘汰)", got)
		}
		if fx.blockExists(t, worst) {
			t.Error("worst 应已被假 Sweep 收走")
		}
		if !fx.blockExists(t, mid) || !fx.blockExists(t, best) {
			t.Error("mid/best 不应被删(util 已 ≤ k-Δ 停下)")
		}
		if got := fx.evictCount(metrics.ResultNormal); got != 0 {
			t.Errorf("evict_blocks{normal} = %v, want 0(最差块已被 Sweep 收走,ErrNotFound 不计)", got)
		}
		if got := fx.gcSweepsCount(metrics.ResultSuccess); got != 1 {
			t.Errorf("gc_sweeps{success} = %v, want 1", got)
		}
		if got := fx.gcBlocksRemoved(); got != 7 {
			t.Errorf("gc_blocks_removed = %v, want 7", got)
		}
		if got := fx.gcObjectsRemoved(); got != 2 {
			t.Errorf("gc_objects_removed = %v, want 2", got)
		}
	})

	t.Run("Sweep 失败仅日志,淘汰继续", func(t *testing.T) {
		fx := newEvictFixture(t)
		fp := fx.putBlock(t, "sweep-fail")
		fx.heat.set(entry(fp, 0.1, 0))
		fx.sweep.err = errors.New("boom")

		fx.usage.set(0.85, 0.74)
		fx.ev.RunOnce(context.Background())

		if fx.blockExists(t, fp) {
			t.Error("Sweep 失败不应阻塞淘汰")
		}
		if got := fx.gcSweepsCount(metrics.ResultError); got != 1 {
			t.Errorf("gc_sweeps{error} = %v, want 1", got)
		}
		if got := fx.evictCount(metrics.ResultNormal); got != 1 {
			t.Errorf("evict_blocks{normal} = %v, want 1", got)
		}
	})
}

// TestRunOnceUsageDefenses:总量为 0 → 利用率按 0 计空转(除零防御);利用率查询失败
// → 仅日志空转,不淘汰。
func TestRunOnceUsageDefenses(t *testing.T) {
	t.Run("total 为 0", func(t *testing.T) {
		fx := newEvictFixture(t)
		fp := fx.putBlock(t, "zero-total")
		fx.heat.set(entry(fp, 0.1, 0))
		fx.usage.mu.Lock()
		fx.usage.total = 0
		fx.usage.mu.Unlock()

		fx.ev.RunOnce(context.Background())
		if got := fx.usageGauge(); got != 0 {
			t.Errorf("disk_usage_ratio = %v, want 0(除零防御)", got)
		}
		if !fx.blockExists(t, fp) {
			t.Error("total=0 应空转,不淘汰")
		}
	})

	t.Run("查询失败", func(t *testing.T) {
		fx := newEvictFixture(t)
		fp := fx.putBlock(t, "usage-err")
		fx.heat.set(entry(fp, 0.1, 0))
		fx.usage.set(0.85)
		fx.usage.mu.Lock()
		fx.usage.err = errors.New("statfs 挂")
		fx.usage.mu.Unlock()

		fx.ev.RunOnce(context.Background())
		if !fx.blockExists(t, fp) {
			t.Error("利用率查询失败应空转,不淘汰")
		}
		if got := fx.evictCount(metrics.ResultNormal); got != 0 {
			t.Errorf("evict_blocks{normal} = %v, want 0", got)
		}
	})
}

// TestNewDefaultsAndNilSources:零值/负值 Config 取默认(30s/80/5/90);BlockDir 非空且
// Usage nil → 取 statfs 默认实现;heat/protected/m 全 nil 可运行不 panic。
func TestNewDefaultsAndNilSources(t *testing.T) {
	cs, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}

	for _, cfg := range []Config{
		{},
		{Interval: -time.Second, TriggerPercent: -1, DeltaPercent: -1, CriticalPercent: -1},
	} {
		e := New(cs, nil, nil, nil, nil, cfg)
		if e.interval != 30*time.Second || e.trigger != 0.80 || e.delta != 0.05 || e.critical != 0.90 {
			t.Errorf("cfg=%+v 默认 = %v/%v/%v/%v, want 30s/0.80/0.05/0.90",
				cfg, e.interval, e.trigger, e.delta, e.critical)
		}
		if e.usage != nil {
			t.Error("Usage 与 BlockDir 均空时不应臆造利用率来源")
		}
	}

	// BlockDir 非空:Usage nil 时接线 statfs 默认实现(disk_test.go 另测实现本身)。
	e := New(cs, nil, nil, nil, nil, Config{BlockDir: t.TempDir()})
	if e.usage == nil {
		t.Fatal("BlockDir 非空时 Usage nil 应取 statfs 默认实现")
	}
	used, total, err := e.usage()
	if err != nil || total == 0 || used > total {
		t.Errorf("statfs 默认实现 = (%d, %d, %v), want total > 0 且 used ≤ total", used, total, err)
	}

	// heat/protected/Sweep/m 全 nil:高利用率也不 panic(无候选、不打点)。
	e2 := New(cs, nil, nil, nil, nil, Config{Usage: func() (uint64, uint64, error) { return 85, 100, nil }})
	e2.RunOnce(context.Background())
}

// TestStartPeriodicEvictionAndCtxStop:Start 非阻塞、按 Interval 周期检查;ctx 取消后
// 停止触发(取消后调用次数不再增长)。
func TestStartPeriodicEvictionAndCtxStop(t *testing.T) {
	fx := newEvictFixture(t)
	fp := fx.putBlock(t, "start-block")
	fx.heat.set(entry(fp, 0.1, 0))
	fx.usage.set(0.85) // 恒超 k:每 tick 调用一次利用率

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fx.ev.interval = 5 * time.Millisecond // 白盒加速:默认 30s 无法在用例内等待
	fx.ev.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for fx.blockExists(t, fp) {
		if time.Now().After(deadline) {
			t.Fatal("Start 后周期检查未在 deadline 内淘汰块")
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	time.Sleep(20 * time.Millisecond) // 取消后至多一轮在途 tick 落地
	n := fx.usage.callCount()
	time.Sleep(50 * time.Millisecond) // 覆盖 10 个 tick 周期
	if got := fx.usage.callCount(); got != n {
		t.Errorf("ctx 取消后仍有新检查(取消时 %d 次,现 %d 次),Start 未随 ctx 退出", n, got)
	}
}
