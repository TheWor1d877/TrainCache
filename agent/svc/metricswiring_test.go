package svc

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/objectstore"
)

// metricswiring_test.go:M9/D48 证据指标接线(singleflight 仅等待者、限速值、
// reason/dataset 解析、命中 source/phase 判定)。

// gatedGetStore 在首次 Get 时关闭 started,并阻塞到 release,用于制造同 key 并发窗口。
type gatedGetStore struct {
	*objectstore.Mock
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (g *gatedGetStore) Get(ctx context.Context, key string, w io.Writer) error {
	g.once.Do(func() { close(g.started) })
	<-g.release
	return g.Mock.Get(ctx, key, w)
}

// gatherValueOK 从 Gather 结果读出指定 family + 标签集合的 counter/gauge 值;
// found=false 表示该子指标尚未出现(轮询场景使用,不得在此 fatal)。
func gatherValueOK(r *metrics.Registry, family string, want map[string]string) (float64, bool) {
	families, err := r.Prometheus().Gather()
	if err != nil {
		return 0, false
	}
	for _, mf := range families {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			if len(got) != len(want) {
				continue
			}
			match := true
			for k, v := range want {
				if got[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue(), true
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue(), true
			}
		}
	}
	return 0, false
}

// gatherValue 断言式读取:子指标缺失即失败(断言场景使用)。
func gatherValue(t *testing.T, r *metrics.Registry, family string, want map[string]string) float64 {
	t.Helper()
	v, ok := gatherValueOK(r, family, want)
	if !ok {
		t.Fatalf("family %q 无标签 %v 的子指标", family, want)
	}
	return v
}

// TestMetricsBandwidthLimit:wireMetrics 从 core/fetch 取实际生效的限速值(D48)。
func TestMetricsBandwidthLimit(t *testing.T) {
	dir := t.TempDir()
	cs, err := cas.New(dir, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	f := fetch.New(objectstore.NewMock(), cs, fetch.Options{MaxInflightBytes: 12345})
	r := metrics.New(metrics.Config{})
	ag := &Agent{Metrics: r, Fetcher: f}
	ag.wireMetrics()

	if got := gatherValue(t, r, "traincache_fetch_bandwidth_limit_bytes", map[string]string{"dataset": "other"}); got != 12345 {
		t.Errorf("bandwidth_limit = %v, want 12345", got)
	}
	if got := f.MaxInflightBytes(); got != 12345 {
		t.Errorf("Fetcher.MaxInflightBytes() = %d, want 12345", got)
	}
}

// TestMetricsSingleflightMergedOnlyWaiters:同 key 并发 Fetch 时仅等待者计数(leader 不计)。
func TestMetricsSingleflightMergedOnlyWaiters(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cs, err := cas.New(dir, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	base := objectstore.NewMock()
	key := "ds/blocks/p#0"
	content := "singleflight-content"
	if _, err := base.Put(ctx, key, strings.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("Mock.Put: %v", err)
	}
	gs := &gatedGetStore{Mock: base, started: make(chan struct{}), release: make(chan struct{})}
	f := fetch.New(gs, cs, fetch.Options{})
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"ds"}})
	ag := &Agent{Metrics: r, Fetcher: f}
	ag.wireMetrics()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	run := func() {
		defer wg.Done()
		_, err := f.Fetch(ctx, key, int64(len(content)))
		errs <- err
	}
	wg.Add(1)
	go run()
	<-gs.started // leader 已登记 inflight 并阻塞在下载
	wg.Add(1)
	go run() // 等待者

	deadline := time.Now().Add(3 * time.Second)
	for {
		merged, found := gatherValueOK(r, "traincache_fetch_singleflight_merged_total", map[string]string{"dataset": "ds"})
		if found && merged >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("等待者未登记 OnMerge 计数")
		}
		time.Sleep(time.Millisecond)
	}
	close(gs.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
	}
	if got := gatherValue(t, r, "traincache_fetch_singleflight_merged_total", map[string]string{"dataset": "ds"}); got != 1 {
		t.Errorf("merged = %v, want 1(仅等待者)", got)
	}
}

// TestMetricsDatasetFromBlockKey:D25 键 → dataset,非法布局忽略。
func TestMetricsDatasetFromBlockKey(t *testing.T) {
	cases := []struct {
		key     string
		dataset string
		ok      bool
	}{
		{"a-track/blocks/x.tar#3", "a-track", true},
		{"dataset/b/blocks/x", "dataset/b", true},
		{"no-blocks-layout", "", false},
		{"/blocks/x", "", false},
		{"a-track/blocks/", "", false},
	}
	for _, c := range cases {
		ds, ok := datasetFromBlockKey(c.key)
		if ds != c.dataset || ok != c.ok {
			t.Errorf("datasetFromBlockKey(%q) = (%q,%v), want (%q,%v)", c.key, ds, ok, c.dataset, c.ok)
		}
	}
}

// TestMetricsConsumptionAdvance:advance 帧 source=hook;inotify 来源可计;集合外忽略。
func TestMetricsConsumptionAdvance(t *testing.T) {
	r := metrics.New(metrics.Config{DatasetWhitelist: []string{"ds"}})
	ag := &Agent{Metrics: r}

	ag.reportConsumptionAdvance("ds", metrics.SourceHook)
	ag.reportConsumptionAdvance("ds", metrics.SourceInotify)
	ag.reportConsumptionAdvance("ds", metrics.SourceInotify)
	ag.reportConsumptionAdvance("ds", "bogus")

	if got := gatherValue(t, r, "traincache_consumption_advance_total", map[string]string{"dataset": "ds", "source": "hook"}); got != 1 {
		t.Errorf("hook advance = %v, want 1", got)
	}
	if got := gatherValue(t, r, "traincache_consumption_advance_total", map[string]string{"dataset": "ds", "source": "inotify"}); got != 2 {
		t.Errorf("inotify advance = %v, want 2", got)
	}
}

// TestMetricsCacheSourceAndPhase:D46 命中 source 判定(当前窗口 prefetch / 窗口外 cache /
// 无订单 unknown)与 phase 取值。
func TestMetricsCacheSourceAndPhase(t *testing.T) {
	chunks := make([]ordersource.ChunkRef, 0, prefetchLookahead+3)
	for i := 0; i < prefetchLookahead+3; i++ {
		chunks = append(chunks, phaseRef("p", int64(i)))
	}
	b := &datasetBundle{
		dataset:  "ds",
		hasOrder: true,
		lastOrder: ordersource.ConsumeOrder{
			Dataset: "ds",
			Chunks:  chunks,
		},
	}
	// Status.Cursor 尚未推进 → cursor 从订单头起算(prefetch 窗口 = 前 lookahead 块)。
	b.prefetcher = prefetch.New(nil, nil, nil, nil, prefetch.Config{})
	pm := &PrefetchManager{bundles: map[string]*datasetBundle{"ds": b}}
	phase := newPhaseTracker(2)
	ag := &Agent{Prefetch: pm, phase: phase}

	if got := ag.cacheSource("ds", cas.Fingerprint{}, chunks[0]); got != metrics.SourcePrefetch {
		t.Errorf("窗口内 source = %q, want prefetch", got)
	}
	if got := ag.cacheSource("ds", cas.Fingerprint{}, chunks[prefetchLookahead]); got != metrics.SourceCache {
		t.Errorf("窗口外 source = %q, want cache", got)
	}
	if got := ag.cacheSource("ds", cas.Fingerprint{}, phaseRef("other", 0)); got != metrics.SourceCache {
		t.Errorf("订单外已有块 source = %q, want cache", got)
	}
	if got := ag.cacheSource("nope", cas.Fingerprint{}, chunks[0]); got != metrics.SourceUnknown {
		t.Errorf("无预取记录 source = %q, want unknown", got)
	}
	if got := ag.cacheSource("", cas.Fingerprint{}, chunks[0]); got != metrics.SourceUnknown {
		t.Errorf("dataset 空 source = %q, want unknown", got)
	}

	if got := ag.cachePhase("ds"); got != metrics.PhaseStartup {
		t.Errorf("启动期无事件 phase = %q, want startup", got)
	}
	phase.OnStageSwitchFor("ds", "epoch-2")
	if got := ag.cachePhase("ds"); got != metrics.PhaseSwitch {
		t.Errorf("切换中 phase = %q, want switch", got)
	}
}

// TestMetricsRefWindowState:窗口判定的边界(订单头到 lookahead-1 为窗内)。
func TestMetricsRefWindowState(t *testing.T) {
	chunks := make([]ordersource.ChunkRef, 0, prefetchLookahead+2)
	for i := 0; i < prefetchLookahead+2; i++ {
		chunks = append(chunks, phaseRef("p", int64(i)))
	}
	b := &datasetBundle{
		hasOrder:  true,
		lastOrder: ordersource.ConsumeOrder{Chunks: chunks},
	}
	b.prefetcher = prefetch.New(nil, nil, nil, nil, prefetch.Config{})

	if known, in := b.refWindowState(chunks[0]); !known || !in {
		t.Errorf("首块 = (known=%v,inWindow=%v), want (true,true)", known, in)
	}
	if known, in := b.refWindowState(chunks[prefetchLookahead-1]); !known || !in {
		t.Errorf("窗口末块 = (known=%v,inWindow=%v), want (true,true)", known, in)
	}
	if known, in := b.refWindowState(chunks[prefetchLookahead]); !known || in {
		t.Errorf("窗口后第一块 = (known=%v,inWindow=%v), want (true,false)", known, in)
	}
	if known, in := b.refWindowState(phaseRef("absent", 0)); !known || in {
		t.Errorf("订单外块 = (known=%v,inWindow=%v), want (true,false)", known, in)
	}
	b.hasOrder = false
	if known, _ := b.refWindowState(chunks[0]); known {
		t.Error("无订单应 known=false")
	}
}
