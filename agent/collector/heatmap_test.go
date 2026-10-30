package collector

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
)

// heatmap_test.go:热度衰减(§5.5/D24)、heatmap.json 原子 dump 与启动恢复(§6.4),以及
// inotify 兜底监听的真机端到端(§5.5"inotify 旁路监听缓存目录打开事件")。

// TestDecayFactorAndAge:周期衰减 h × factor;超过 HeatDecayAge 未 touch 直接置 0。
func TestDecayFactorAndAge(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fpA, fpB := testFP(0xb1), testFP(0xb2)
	env.c.Open(fpA, ordersource.ChunkRef{})
	env.c.Open(fpB, ordersource.ChunkRef{})

	env.clock.Advance(2 * time.Hour)
	env.c.decay()
	want := defaultHeatOpenBeta * defaultHeatDecayFactor
	if got := heatOf(t, env.c, fpA); math.Abs(got-want) > 1e-12 {
		t.Errorf("未超龄条目衰减后 h = %v, want %v(×%v)", got, want, defaultHeatDecayFactor)
	}

	env.clock.Advance(25 * time.Hour) // 累计 27h > HeatDecayAge(24h)
	env.c.decay()
	if got := heatOf(t, env.c, fpA); got != 0 {
		t.Errorf("超龄条目 h = %v, want 0", got)
	}
	if got := heatOf(t, env.c, fpB); got != 0 {
		t.Errorf("超龄条目 h = %v, want 0", got)
	}
}

// TestDecayCustomConfig:自定义衰减因子与超龄(≤0 取默认;>0 生效)。
func TestDecayCustomConfig(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{HeatDecayFactor: 0.25, HeatDecayAge: time.Hour})
	fp := testFP(0xb3)
	env.c.Open(fp, ordersource.ChunkRef{})

	env.clock.Advance(30 * time.Minute)
	env.c.decay()
	if got, want := heatOf(t, env.c, fp), defaultHeatOpenBeta*0.25; math.Abs(got-want) > 1e-12 {
		t.Errorf("自定义因子衰减后 h = %v, want %v", got, want)
	}

	env.clock.Advance(45 * time.Minute) // 累计 75min > 自定义 1h
	env.c.decay()
	if got := heatOf(t, env.c, fp); got != 0 {
		t.Errorf("超自定义龄条目 h = %v, want 0", got)
	}
}

// TestDecayLoopWiring:Start 起周期衰减循环(HeatDecayInterval 生效)。
func TestDecayLoopWiring(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{HeatDecayInterval: 20 * time.Millisecond})
	env.start()
	fp := testFP(0xb4)
	env.c.Open(fp, ordersource.ChunkRef{})

	mustEventually(t, "周期衰减循环生效", func() bool {
		h := heatOf(t, env.c, fp)
		return h > 0 && h < defaultHeatOpenBeta
	})
}

// TestDumpLoadRoundTrip:dump 原子落盘、JSON 格式(§6.4)与启动恢复(Load 还原热度/计数/t_touch)。
func TestDumpLoadRoundTrip(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fp1, fp2 := testFP(0xc1), testFP(0xc2)
	env.c.Open(fp1, ordersource.ChunkRef{})
	env.c.Open(fp1, ordersource.ChunkRef{}) // access_count = 2
	env.clock.Advance(time.Minute)
	env.c.Open(fp2, ordersource.ChunkRef{})

	wantHeat1 := heatOf(t, env.c, fp1)
	wantHeat2 := heatOf(t, env.c, fp2)

	if err := env.c.Dump(); err != nil {
		t.Fatalf("Dump: %v", err)
	}

	// 磁盘格式:version=1、dumped_at=RFC3339、entries 指纹键 32 hex。
	raw, err := os.ReadFile(filepath.Join(env.dir, heatmapFileName))
	if err != nil {
		t.Fatalf("读 heatmap.json: %v", err)
	}
	var f heatmapFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("解析 heatmap.json: %v", err)
	}
	if f.Version != heatmapVersion {
		t.Errorf("version = %d, want %d", f.Version, heatmapVersion)
	}
	if _, err := time.Parse(time.RFC3339, f.DumpedAt); err != nil {
		t.Errorf("dumped_at 非 RFC3339: %q (%v)", f.DumpedAt, err)
	}
	if len(f.Entries) != 2 {
		t.Fatalf("entries 数 = %d, want 2", len(f.Entries))
	}
	e1 := f.Entries[fp1.String()]
	if math.Abs(e1.Heat-wantHeat1) > 1e-12 || e1.AccessCount != 2 {
		t.Errorf("fp1 落盘记录 = %+v, want heat≈%v count=2", e1, wantHeat1)
	}
	if e1.LastAccess != baseTime.Unix() {
		t.Errorf("fp1 last_access = %d, want %d", e1.LastAccess, baseTime.Unix())
	}

	// 原子写:blockDir 无 tmp 残留。
	ents, err := os.ReadDir(env.dir)
	if err != nil {
		t.Fatalf("读 blockDir: %v", err)
	}
	for _, ent := range ents {
		if strings.HasPrefix(ent.Name(), heatmapFileName+".tmp-") {
			t.Errorf("Dump 残留临时文件 %s(原子写应为 临时文件+rename)", ent.Name())
		}
	}

	// 启动恢复:新 Collector.Load 还原(秒级时间戳)。
	c2 := NewCollector(env.dir, env.m, CollectorConfig{})
	if err := c2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	g1, ok := entryOf(c2, fp1)
	if !ok {
		t.Fatal("Load 后缺 fp1")
	}
	if math.Abs(g1.heat-wantHeat1) > 1e-12 || g1.accessCount != 2 {
		t.Errorf("恢复 fp1 = %+v, want heat≈%v count=2", g1, wantHeat1)
	}
	if !g1.lastTouch.Equal(baseTime) {
		t.Errorf("恢复 fp1 t_touch = %v, want %v", g1.lastTouch, baseTime)
	}
	g2, ok := entryOf(c2, fp2)
	if !ok || math.Abs(g2.heat-wantHeat2) > 1e-12 || !g2.lastTouch.Equal(baseTime.Add(time.Minute)) {
		t.Errorf("恢复 fp2 = %+v ok=%v, want heat≈%v t_touch=%v", g2, ok, wantHeat2, baseTime.Add(time.Minute))
	}
}

// TestLoadMissingFile:文件缺失(首次启动)→ 空表 + nil(容错口径)。
func TestLoadMissingFile(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	env.c.Open(testFP(0xc3), ordersource.ChunkRef{}) // 内存先有条目
	if err := env.c.Load(); err != nil {
		t.Fatalf("缺失文件 Load = %v, want nil", err)
	}
	env.c.mu.Lock()
	n := len(env.c.entries)
	env.c.mu.Unlock()
	if n != 0 {
		t.Errorf("缺失文件 Load 后条目数 = %d, want 0(以空表继续)", n)
	}
}

// TestLoadCorruptFile:损坏文件容错——整体非法 JSON → 空表 + nil;单条非法指纹跳过;
// version 非预期告警但按同字段读取。
func TestLoadCorruptFile(t *testing.T) {
	t.Run("非法 JSON", func(t *testing.T) {
		env := newTestEnv(t, CollectorConfig{})
		if err := os.WriteFile(filepath.Join(env.dir, heatmapFileName), []byte("{{{ not json"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := env.c.Load(); err != nil {
			t.Fatalf("损坏文件 Load = %v, want nil(容错)", err)
		}
		env.c.mu.Lock()
		n := len(env.c.entries)
		env.c.mu.Unlock()
		if n != 0 {
			t.Errorf("损坏文件 Load 后条目数 = %d, want 0", n)
		}
	})

	t.Run("单条非法指纹跳过且 version 宽容", func(t *testing.T) {
		env := newTestEnv(t, CollectorConfig{})
		fp := testFP(0xc4)
		body := `{"version":77,"dumped_at":"2026-10-15T00:00:00Z","entries":{` +
			`"` + strings.Repeat("g", 32) + `":{"heat":0.5},` +
			`"` + fp.String() + `":{"heat":0.5,"last_access":1763000000,"access_count":7}}}`
		if err := os.WriteFile(filepath.Join(env.dir, heatmapFileName), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := env.c.Load(); err != nil {
			t.Fatalf("Load = %v, want nil", err)
		}
		e, ok := entryOf(env.c, fp)
		if !ok {
			t.Fatal("合法条目未恢复")
		}
		if e.heat != 0.5 || e.accessCount != 7 || !e.lastTouch.Equal(baseTime) {
			t.Errorf("恢复条目 = %+v, want heat=0.5 count=7 t_touch=%v", e, baseTime)
		}
		env.c.mu.Lock()
		n := len(env.c.entries)
		env.c.mu.Unlock()
		if n != 1 {
			t.Errorf("条目数 = %d, want 1(非法指纹键跳过)", n)
		}
	})
}

// TestWatchBlocksCountsOpenEvent:inotify 端到端——Start 时既有分片目录的块文件被打开
// (O_RDONLY) → 兜底打开事件计入热度/命中率;tmp- 临时文件与非法名不计入。
func TestWatchBlocksCountsOpenEvent(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	fp := testFP(0xd1)
	path := writeLocalBlock(t, env.dir, fp) // Start 前落盘:分片目录在监听初始化时已存在
	env.start()

	// watch 挂载与测试循环异步:反复打开直至事件被处理(同一打开事件幂等可重复)。
	mustEventually(t, "inotify 打开事件计入", func() bool {
		openReadOnce(t, path)
		_, ok := entryOf(env.c, fp)
		return ok
	})
	if got := heatOf(t, env.c, fp); got <= 0 {
		t.Errorf("兜底打开后 h = %v, want > 0", got)
	}
	// 热度条目先于 noteOpen 的命中率/指标步骤落表,轮询等到明细齐备。
	mustEventually(t, "命中计入命中率(本地块存在)", func() bool { return env.c.HitRate() == 1 })
	if got := testutil.ToFloat64(env.m.CacheMisses().WithLabelValues("other", "success", metrics.PhaseUnknown)); got != 0 {
		t.Errorf("cache_misses{other} = %v, want 0(块在本地)", got)
	}
	time.Sleep(100 * time.Millisecond) // 消窗:等内核队列里的事件被监听循环处理完
	hits0 := testutil.ToFloat64(env.m.CacheHits().WithLabelValues("other", "success", metrics.SourceUnknown, metrics.PhaseUnknown))
	if hits0 < 1 {
		t.Fatalf("cache_hits{other} = %v, want ≥ 1", hits0)
	}

	// tmp- 临时文件(且名非法)打开不计入。
	tmpPath := filepath.Join(filepath.Dir(path), tmpPrefix+"abc123")
	if err := os.WriteFile(tmpPath, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	openReadOnce(t, tmpPath)
	time.Sleep(100 * time.Millisecond)
	env.c.mu.Lock()
	n := len(env.c.entries)
	env.c.mu.Unlock()
	if n != 1 {
		t.Errorf("tmp 文件打开后条目数 = %d, want 1(临时文件不计入)", n)
	}
	if got := testutil.ToFloat64(env.m.CacheHits().WithLabelValues("other", "success", metrics.SourceUnknown, metrics.PhaseUnknown)); got != hits0 {
		t.Errorf("tmp 文件打开后 cache_hits = %v, want %v(不变)", got, hits0)
	}
}

// TestWatchBlocksNewShardDir:Start 之后新建的分片子目录由其 CREATE 事件补挂 watch,
// 随后的打开事件被捕获(§5.5 inotify 兜底动态增删 watch)。
func TestWatchBlocksNewShardDir(t *testing.T) {
	env := newTestEnv(t, CollectorConfig{})
	env.start()
	fp := testFP(0xd2)
	path := writeLocalBlock(t, env.dir, fp) // Start 后:根 watch 收 CREATE → 补挂分片目录

	mustEventually(t, "新分片目录的打开事件被捕获", func() bool {
		openReadOnce(t, path)
		_, ok := entryOf(env.c, fp)
		return ok
	})
}

// openReadOnce 以只读方式打开并读一小段后关闭(模拟消费侧读块;仅打开即产生 IN_OPEN)。
func openReadOnce(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return // 文件可能尚未就绪(轮询调用方自行重试)
	}
	buf := make([]byte, 1)
	_, _ = f.Read(buf)
	_ = f.Close()
}
