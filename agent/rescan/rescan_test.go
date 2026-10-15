package rescan

// 本文件是 rescan 的白盒/集成测试(7.2 §5 任务 4 场景):契约路径用真实 core 组合
// (objectstore.Mock + cas.Store + fetch.Fetcher + index.Catalog)验证;坏块注入 = 写块后
// 翻转块文件数据区字节(cas.Open 报 ErrCorrupt);抽样与"全量阶段中间态"经非导出钩子
// (pickSample)与可门控注入的 catalogFor 观测,断言不依赖 goroutine 调度顺序;等待一律
// 带 deadline 轮询,不用裸 sleep。

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"traincache/agent/ordersource"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// testTimeout 是轮询/等待类断言的统一 deadline。
const testTimeout = 3 * time.Second

// testDataset 是多数用例的数据集名。
const testDataset = "ds"

// waitFor 带 deadline 轮询 cond(不用裸 sleep 断言;poll 间隔 2ms)。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("超时未满足: %s(deadline %s)", what, testTimeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// countingStore 包装 objectstore.Mock,记录 Get 调用的对象键,并支持按键门控(并发修复
// 用例强制下载窗口)。
type countingStore struct {
	*objectstore.Mock
	mu    sync.Mutex
	keys  []string
	gates map[string]chan struct{} // 键 → 该键的 Get 阻塞至 close
}

func newCountingStore() *countingStore { return &countingStore{Mock: objectstore.NewMock()} }

func (s *countingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	gate := s.gates[key]
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return s.Mock.Get(ctx, key, w)
}

func (s *countingStore) getKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.keys)
}

// gate 为 key 安装阻塞闸(先于任何 Get 调用;Get 将阻塞至返回的 channel 关闭)。
func (s *countingStore) gate(key string) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gates == nil {
		s.gates = make(map[string]chan struct{})
	}
	ch := make(chan struct{})
	s.gates[key] = ch
	return ch
}

var _ objectstore.Store = (*countingStore)(nil)

// blockGets 返回源站 Get 中属于数据块(D25 键,含 "/blocks/")的键;索引元数据
// (dataset/<name>/CURRENT、manifest、分片)的读取是 Catalog 正常行为,不计入"下载"。
func blockGets(s *countingStore) []string {
	var out []string
	for _, k := range s.getKeys() {
		if strings.Contains(k, "/blocks/") {
			out = append(out, k)
		}
	}
	return out
}

// fakeMetrics 记录 rescan 窄接口的调用(并发安全;进度取最后一次,隔离数取最后与峰值)。
type fakeMetrics struct {
	mu          sync.Mutex
	progress    []float64
	quarantined []int64
}

func (f *fakeMetrics) SetRescanProgress(v float64) {
	f.mu.Lock()
	f.progress = append(f.progress, v)
	f.mu.Unlock()
}

func (f *fakeMetrics) SetQuarantined(n int64) {
	f.mu.Lock()
	f.quarantined = append(f.quarantined, n)
	f.mu.Unlock()
}

func (f *fakeMetrics) lastProgress() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.progress) == 0 {
		return -1
	}
	return f.progress[len(f.progress)-1]
}

func (f *fakeMetrics) lastQuarantined() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.quarantined) == 0 {
		return -1
	}
	return f.quarantined[len(f.quarantined)-1]
}

func (f *fakeMetrics) maxQuarantined() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var max int64 = -1
	for _, n := range f.quarantined {
		if n > max {
			max = n
		}
	}
	return max
}

// blockID 定位一个测试块(dataset, path, chunk)。
type blockID struct {
	dataset string
	path    string
	chunk   int64
}

// blockSpec 描述一个测试块:内容、源站可达性(D25 键)、坏块注入与索引缺失模拟。
// 本地副本恒存在(cas.Put 落盘;rescan 全部场景均以本地块为前提)。
type blockSpec struct {
	dataset string
	path    string
	chunk   int64
	content string
	source  bool // 内容放入源站对象(D25 键),供修复下载
	corrupt bool // 落盘(与索引提交)后翻转数据区字节,注入坏块
	noIndex bool // 该 dataset 不提交索引(模拟 Catalog 不可用:ErrNoCurrent)
}

// env 是测试装置:计数/门控 Mock 源站 + 临时目录 CAS + fetch.Fetcher + 按需构造的 Catalog。
type env struct {
	t     *testing.T
	store *countingStore
	dir   string // cas 根目录(坏块注入用)
	cs    *cas.Store
	fetch *fetch.Fetcher
	cats  map[string]*index.Catalog // dataset → Catalog(仅扫描协程访问)
	fpOf  map[blockID]cas.Fingerprint
}

// newEnv 构造测试装置:逐块 cas.Put(Commit 验证要求块已先行落盘)→ 按 dataset Commit
// (noIndex 的 dataset 跳过)→ 放置源站对象 → 注入坏块(Commit 之后翻转字节)。
func newEnv(t *testing.T, specs []blockSpec) *env {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cs, err := cas.New(dir, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	env := &env{
		t:     t,
		store: newCountingStore(),
		dir:   dir,
		cs:    cs,
		cats:  make(map[string]*index.Catalog),
		fpOf:  make(map[blockID]cas.Fingerprint),
	}
	env.fetch = fetch.New(env.store, cs, fetch.Options{
		MaxInflightBytes: 1 << 20,
		MaxTasks:         4,
		MaxConnsPerHost:  2,
		MaxRetries:       1,
	})

	byDS := make(map[string][]index.Entry)
	order := make([]string, 0) // dataset 首见顺序(Commit 顺序确定)
	noIndex := make(map[string]bool)
	for _, sp := range specs {
		fp, n, err := cs.Put(ctx, strings.NewReader(sp.content))
		if err != nil {
			t.Fatalf("cas.Put(%s#%d): %v", sp.path, sp.chunk, err)
		}
		id := blockID{dataset: sp.dataset, path: sp.path, chunk: sp.chunk}
		env.fpOf[id] = fp
		if _, seen := byDS[sp.dataset]; !seen {
			order = append(order, sp.dataset)
		}
		byDS[sp.dataset] = append(byDS[sp.dataset], index.Entry{Path: sp.path, Chunk: sp.chunk, Size: n, Fingerprint: fp})
		if sp.noIndex {
			noIndex[sp.dataset] = true
		}
	}
	w := index.NewWriter(env.store, cs, index.Config{})
	for _, ds := range order {
		if noIndex[ds] {
			continue
		}
		if _, err := w.Commit(ctx, ds, byDS[ds]); err != nil {
			t.Fatalf("index Commit(%s): %v", ds, err)
		}
	}
	for _, sp := range specs {
		if !sp.source {
			continue
		}
		key := blockKey(sp.dataset, ordersource.ChunkRef{Path: sp.path, Chunk: sp.chunk})
		if _, err := env.store.Mock.Put(ctx, key, strings.NewReader(sp.content), int64(len(sp.content))); err != nil {
			t.Fatalf("源站 Put(%s): %v", key, err)
		}
	}
	for _, sp := range specs {
		if sp.corrupt {
			env.corruptBlock(blockID{dataset: sp.dataset, path: sp.path, chunk: sp.chunk})
		}
	}
	return env
}

// corruptBlock 翻转块文件数据区开头 4 字节(注入坏块;cas.Open 将报 ErrCorrupt)。
func (env *env) corruptBlock(id blockID) {
	env.t.Helper()
	hexs := env.fpOf[id].String()
	p := filepath.Join(env.dir, "blocks", hexs[:2], hexs+".blk")
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		env.t.Fatalf("打开块文件 %s: %v", p, err)
	}
	defer f.Close()
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, cas.HeaderSize); err != nil {
		env.t.Fatalf("读块数据区 %s: %v", p, err)
	}
	for i := range buf {
		buf[i] ^= 0xff
	}
	if _, err := f.WriteAt(buf, cas.HeaderSize); err != nil {
		env.t.Fatalf("写块数据区 %s: %v", p, err)
	}
}

// readBlock 打开块并读回全部数据(断言修复闭环)。
func (env *env) readBlock(fp cas.Fingerprint) (string, error) {
	br, err := env.cs.Open(fp)
	if err != nil {
		return "", err
	}
	defer br.Close()
	b, err := io.ReadAll(br)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// catalogFor 是默认注入:任意 dataset 返回绑定的 Catalog(有无 CURRENT 由 List 报错区分)。
func (env *env) catalogFor(_ context.Context, dataset string) (*index.Catalog, error) {
	cat, ok := env.cats[dataset]
	if !ok {
		cat = index.NewCatalog(env.store, dataset, index.Config{})
		env.cats[dataset] = cat
	}
	return cat, nil
}

// listIndexOf 在 cs.List() 输出中定位指纹下标(抽样注入钩子用)。
func listIndexOf(t *testing.T, env *env, fp cas.Fingerprint) int {
	t.Helper()
	fps, err := env.cs.List()
	if err != nil {
		t.Fatalf("cs.List: %v", err)
	}
	for i, got := range fps {
		if got == fp {
			return i
		}
	}
	t.Fatalf("块 %s 不在 cs.List() 输出中", fp)
	return -1
}

// startRescanner 启动扫描;t.Cleanup 兜底取消 ctx 并等待扫描协程退出(不泄漏)。
func startRescanner(t *testing.T, r *Rescanner, ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	r.Start(ctx)
	t.Cleanup(func() {
		cancel()
		<-r.runDone
	})
}

// waitDone 等待扫描进入 DONE。
func waitDone(t *testing.T, r *Rescanner) {
	t.Helper()
	waitFor(t, "Phase=DONE", func() bool { return r.Status().Phase == PhaseDone })
}

// assertRepairClosedLoop 断言修复闭环:块回到 blocks/ 且内容与源站一致、quarantine 归零、
// 数据块下载请求恰为一次且键为 D25 布局。
func assertRepairClosedLoop(t *testing.T, env *env, id blockID, wantContent string) {
	t.Helper()
	got, err := env.readBlock(env.fpOf[id])
	if err != nil {
		t.Fatalf("修复后 Open %s#%d: %v", id.path, id.chunk, err)
	}
	if got != wantContent {
		t.Fatalf("修复后内容 = %q, want %q", got, wantContent)
	}
	q, err := env.cs.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined: %v", err)
	}
	if len(q) != 0 {
		t.Fatalf("quarantine 未归零: %d 块", len(q))
	}
	want := []string{blockKey(id.dataset, ordersource.ChunkRef{Path: id.path, Chunk: id.chunk})}
	if gets := blockGets(env.store); !slices.Equal(gets, want) {
		t.Fatalf("数据块下载请求 = %v, want %v(恰一次修复下载,D25 键)", gets, want)
	}
}

// ---------------- 抽样选择与计数 ----------------

// TestSampleSizeRule:1% 四舍五入、非空至少 1 块、空列表 0(D35/§7.1.3)。
func TestSampleSizeRule(t *testing.T) {
	cases := []struct{ n, want int }{
		{0, 0}, {1, 1}, {49, 1}, {50, 1}, {100, 1}, {150, 2}, {1000, 10}, {10000, 100},
	}
	for _, c := range cases {
		if got := sampleSize(c.n); got != c.want {
			t.Errorf("sampleSize(%d) = %d, want %d", c.n, got, c.want)
		}
	}
}

// ---------------- 抽样阶段与计数 ----------------

// TestSamplingCompletesThenPhaseAdvances:抽样(1 块)全过后 Phase 推进 FULL_SCAN、计数
// 正确;随后全量扫描完成 Phase=DONE、Sampled/Verified 达总数;进度终值 1。
// 全量阶段以注入的 catalogFor 门控:抽样结束与映射构建之间是确定性可观测的中间态。
func TestSamplingCompletesThenPhaseAdvances(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "a.tar", content: "clean-a"},
		{dataset: testDataset, path: "b.tar", content: "clean-b"},
		{dataset: testDataset, path: "c.tar", content: "clean-c"},
	})
	m := &fakeMetrics{}
	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	catalogFor := func(ctx context.Context, ds string) (*index.Catalog, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return env.catalogFor(ctx, ds)
	}
	r := New(env.cs, env.fetch, env.store, catalogFor, m)
	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)

	select {
	case <-entered:
	case <-time.After(testTimeout):
		t.Fatal("等待进入全量阶段(映射构建)超时")
	}
	st := r.Status()
	if st.Phase != PhaseFullScan {
		t.Fatalf("抽样结束后 Phase = %q, want FULL_SCAN", st.Phase)
	}
	if st.Sampled != 1 || st.Verified != 1 || st.Quarantined != 0 || st.RepairQueue != 0 {
		t.Fatalf("抽样结束计数 = %+v, want Sampled=1 Verified=1 Quarantined=0 RepairQueue=0", st)
	}

	close(gate)
	waitDone(t, r)
	st = r.Status()
	if st.Sampled != 3 || st.Verified != 3 || st.Quarantined != 0 || st.RepairQueue != 0 {
		t.Fatalf("全量结束计数 = %+v, want Sampled=3 Verified=3 Quarantined=0 RepairQueue=0", st)
	}
	if got := m.lastProgress(); got != 1 {
		t.Fatalf("进度终值 = %v, want 1", got)
	}
}

// TestStartEmptyCASDone:空块目录直接 DONE,零计数,进度 1。
func TestStartEmptyCASDone(t *testing.T) {
	env := newEnv(t, nil)
	m := &fakeMetrics{}
	r := New(env.cs, env.fetch, env.store, env.catalogFor, m)
	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)

	waitDone(t, r)
	st := r.Status()
	if st.Sampled != 0 || st.Verified != 0 || st.Quarantined != 0 || st.RepairQueue != 0 {
		t.Fatalf("空目录计数 = %+v, want 全零", st)
	}
	if got := m.lastProgress(); got != 1 {
		t.Fatalf("空目录进度终值 = %v, want 1", got)
	}
}

// TestStartIdempotent:二次 Start 为 no-op(不重建扫描协程、不重复计数)。
func TestStartIdempotent(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "a.tar", content: "idem-a"},
		{dataset: testDataset, path: "b.tar", content: "idem-b"},
	})
	r := New(env.cs, env.fetch, env.store, env.catalogFor, &fakeMetrics{})
	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)
	waitDone(t, r)

	before := r.Status()
	rd := r.runDone
	r.Start(ctx) // 二次 no-op
	if r.runDone != rd {
		t.Fatal("二次 Start 不应重建扫描协程")
	}
	if got := r.Status(); got != before {
		t.Fatalf("二次 Start 后 Status = %+v, want 不变 %+v", got, before)
	}
}

// TestStartWithCanceledCtx:ctx 已取消时扫描不进行,Phase 停留 SAMPLING、计数为零,
// 协程立即退出(不挂起)。
func TestStartWithCanceledCtx(t *testing.T) {
	env := newEnv(t, []blockSpec{{dataset: testDataset, path: "a.tar", content: "cancel-a"}})
	r := New(env.cs, env.fetch, env.store, env.catalogFor, &fakeMetrics{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.Start(ctx)
	select {
	case <-r.runDone:
	case <-time.After(testTimeout):
		t.Fatal("已取消 ctx 下扫描协程应快速退出")
	}
	st := r.Status()
	if st.Phase != PhaseSampling || st.Sampled != 0 || st.Verified != 0 || st.Quarantined != 0 || st.RepairQueue != 0 {
		t.Fatalf("取消后 Status = %+v, want Phase=SAMPLING 计数全零", st)
	}
}

// ---------------- dataset 枚举 ----------------

// TestDatasetsOf:键 → 数据集名规则(首段;core 索引布局 dataset/<name>/... 取第二段;
// 顶层 blocks/、无 "/" 键、dataset/blocks/... 跳过;去重保序)。
func TestDatasetsOf(t *testing.T) {
	keys := []string{
		"", "" /* 占位防误读 */, "ds1/blocks/a.tar#0",
		"dataset/ds1/CURRENT",
		"dataset/ds2/snapshots/s1/manifest.json",
		"CURRENT",
		"blocks/orphan#1",
		"dataset/blocks/x#2",
		"dataset/dataset/CURRENT",
		"ds1/index-extra",
	}
	keys = keys[1:] // 去掉占位
	want := []string{"ds1", "ds2", "dataset"}
	if got := datasetsOf(keys); !slices.Equal(got, want) {
		t.Fatalf("datasetsOf = %v, want %v", got, want)
	}
	if got := datasetsOf(nil); len(got) != 0 {
		t.Fatalf("datasetsOf(nil) = %v, want 空", got)
	}
}

// ---------------- 修复闭环与计数口径 ----------------

// TestScanRepairsCorruptBlockInSample:坏块被抽样命中——Quarantine 入队(映射构建后),
// 修复闭环(重下→校验→RemoveQuarantined,ListQuarantined 归零);计数与 gauge 断言。
func TestScanRepairsCorruptBlockInSample(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "healthy.tar", content: "healthy-content"},
		{dataset: testDataset, path: "bad.tar", chunk: 3, content: "corrupt-content", corrupt: true, source: true},
	})
	badID := blockID{dataset: testDataset, path: "bad.tar", chunk: 3}
	if _, err := env.cs.Open(env.fpOf[badID]); !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("前置坏块 Open err = %v, want ErrCorrupt", err)
	}

	m := &fakeMetrics{}
	r := New(env.cs, env.fetch, env.store, env.catalogFor, m)
	idx := listIndexOf(t, env, env.fpOf[badID]) // n=2 → k=1:强制抽样命中坏块
	r.pickSample = func(n, k int) []int { return []int{idx} }

	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)
	waitDone(t, r)

	st := r.Status()
	if st.Sampled != 2 || st.Verified != 1 || st.Quarantined != 1 || st.RepairQueue != 0 {
		t.Fatalf("Status = %+v, want Sampled=2 Verified=1 Quarantined=1 RepairQueue=0", st)
	}
	assertRepairClosedLoop(t, env, badID, "corrupt-content")
	if got := m.maxQuarantined(); got != 1 {
		t.Fatalf("隔离 gauge 峰值 = %d, want 1", got)
	}
	if got := m.lastQuarantined(); got != 0 {
		t.Fatalf("隔离 gauge 终值 = %d, want 0(修复成功递减)", got)
	}
	if got := m.lastProgress(); got != 1 {
		t.Fatalf("进度终值 = %v, want 1", got)
	}
}

// TestScanRepairsCorruptBlockInFullScan:坏块未被抽样命中(抽样给了健康块)→ 全量扫描发现
// ErrCorrupt,同样 Quarantine 入队并在队列优先语义下修复闭环。
func TestScanRepairsCorruptBlockInFullScan(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "healthy.tar", content: "healthy-content"},
		{dataset: testDataset, path: "bad.tar", chunk: 3, content: "corrupt-content", corrupt: true, source: true},
	})
	badID := blockID{dataset: testDataset, path: "bad.tar", chunk: 3}
	healthyID := blockID{dataset: testDataset, path: "healthy.tar"}

	r := New(env.cs, env.fetch, env.store, env.catalogFor, &fakeMetrics{})
	idx := listIndexOf(t, env, env.fpOf[healthyID]) // 抽样命中健康块,坏块留给全量
	r.pickSample = func(n, k int) []int { return []int{idx} }

	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)
	waitDone(t, r)

	st := r.Status()
	if st.Sampled != 2 || st.Verified != 1 || st.Quarantined != 1 || st.RepairQueue != 0 {
		t.Fatalf("Status = %+v, want Sampled=2 Verified=1 Quarantined=1 RepairQueue=0", st)
	}
	assertRepairClosedLoop(t, env, badID, "corrupt-content")
}

// TestCorruptBlockQuarantineOnlyWithoutCatalog:Catalog 不可用(无 CURRENT / catalogFor
// 报错)的 dataset 其坏块只 Quarantine 不修复:留队不入、quarantine 保留、零下载。
func TestCorruptBlockQuarantineOnlyWithoutCatalog(t *testing.T) {
	for _, tc := range []struct {
		name       string
		catalogErr error
	}{
		{"no current", nil},
		{"catalogFor error", errors.New("索引不可用")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnv(t, []blockSpec{
				{dataset: "orphan", path: "x.tar", content: "orphan-corrupt", corrupt: true, source: true, noIndex: true},
			})
			id := blockID{dataset: "orphan", path: "x.tar"}
			fp := env.fpOf[id]

			catalogFor := env.catalogFor
			if tc.catalogErr != nil {
				catalogFor = func(context.Context, string) (*index.Catalog, error) { return nil, tc.catalogErr }
			}
			r := New(env.cs, env.fetch, env.store, catalogFor, &fakeMetrics{})
			ctx, cancel := context.WithCancel(context.Background())
			startRescanner(t, r, ctx, cancel)
			waitDone(t, r)

			st := r.Status()
			if st.Quarantined != 1 || st.RepairQueue != 0 {
				t.Fatalf("Status = %+v, want Quarantined=1 RepairQueue=0(只隔离不入队)", st)
			}
			q, err := env.cs.ListQuarantined()
			if err != nil {
				t.Fatalf("ListQuarantined: %v", err)
			}
			if len(q) != 1 || q[0] != fp {
				t.Fatalf("quarantine = %v, want [%s](留待读时路径修)", q, fp)
			}
			if gets := blockGets(env.store); len(gets) != 0 {
				t.Fatalf("不应产生修复下载: %v", gets)
			}
			if _, err := env.cs.Open(fp); !errors.Is(err, cas.ErrNotFound) {
				t.Fatalf("blocks/ 下坏块已隔离,Open err = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestQuarantineGaugeTracksCurrentCount:quarantine/ 既有块计入 gauge 初值(启动装载)、
// 修复递减;Status.Quarantined 只记本次扫描检测数(存量不算)。
func TestQuarantineGaugeTracksCurrentCount(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "pre.tar", content: "pre-quarantined", source: true},
		{dataset: testDataset, path: "bad.tar", content: "scan-detected", corrupt: true, source: true},
	})
	preID := blockID{dataset: testDataset, path: "pre.tar"}
	badID := blockID{dataset: testDataset, path: "bad.tar"}
	// 前置:pre.tar 已隔离(上次进程遗留)。
	env.corruptBlock(preID)
	if err := env.cs.Quarantine(env.fpOf[preID]); err != nil {
		t.Fatalf("前置 Quarantine: %v", err)
	}

	m := &fakeMetrics{}
	r := New(env.cs, env.fetch, env.store, env.catalogFor, m)
	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)
	waitDone(t, r)

	st := r.Status()
	if st.Sampled != 1 || st.Verified != 0 || st.Quarantined != 1 || st.RepairQueue != 0 {
		t.Fatalf("Status = %+v, want Sampled=1 Verified=0 Quarantined=1 RepairQueue=0", st)
	}
	// gauge:初值 1(遗留)→ 2(本次隔离)→ 1(本次修复);终值 1。
	if got := m.maxQuarantined(); got != 2 {
		t.Fatalf("隔离 gauge 峰值 = %d, want 2", got)
	}
	if got := m.lastQuarantined(); got != 1 {
		t.Fatalf("隔离 gauge 终值 = %d, want 1(遗留块仍在)", got)
	}
	q, err := env.cs.ListQuarantined()
	if err != nil {
		t.Fatalf("ListQuarantined: %v", err)
	}
	if len(q) != 1 || q[0] != env.fpOf[preID] {
		t.Fatalf("quarantine = %v, want [%s]", q, env.fpOf[preID])
	}
	// bad.tar 修复闭环(下载恰一次)。
	got, err := env.readBlock(env.fpOf[badID])
	if err != nil || got != "scan-detected" {
		t.Fatalf("bad.tar 修复后 = (%q, %v), want (scan-detected, nil)", got, err)
	}
	want := []string{blockKey(testDataset, ordersource.ChunkRef{Path: "bad.tar"})}
	if gets := blockGets(env.store); !slices.Equal(gets, want) {
		t.Fatalf("数据块下载请求 = %v, want %v", gets, want)
	}
}

// TestNilMetricsScanAndRepair:metrics 为 nil 时扫描与修复全程静默,不 panic。
func TestNilMetricsScanAndRepair(t *testing.T) {
	env := newEnv(t, []blockSpec{
		{dataset: testDataset, path: "n.tar", content: "nil-metrics", corrupt: true, source: true},
	})
	r := New(env.cs, env.fetch, env.store, env.catalogFor, nil)
	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)
	waitDone(t, r)
	assertRepairClosedLoop(t, env, blockID{dataset: testDataset, path: "n.tar"}, "nil-metrics")
}

// ---------------- 并发安全 ----------------

// TestStatusConcurrentWithScan:扫描进行中并发读 Status,-race 下的锁正确性。
func TestStatusConcurrentWithScan(t *testing.T) {
	specs := make([]blockSpec, 0, 16)
	for i := 0; i < 16; i++ {
		specs = append(specs, blockSpec{dataset: testDataset, path: fmt.Sprintf("c-%02d.tar", i), content: fmt.Sprintf("concurrent-%02d", i)})
	}
	env := newEnv(t, specs)
	r := New(env.cs, env.fetch, env.store, env.catalogFor, &fakeMetrics{})

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				st := r.Status()
				if st.Sampled < 0 || st.Verified < 0 || st.Quarantined < 0 || st.RepairQueue < 0 {
					t.Errorf("Status 出现负计数: %+v", st)
					return
				}
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	startRescanner(t, r, ctx, cancel)
	waitDone(t, r)
	close(stop)
	<-done
}
