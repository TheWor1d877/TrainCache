package svc

// capacity_wiring_test.go:字节维度容量的装配级探针(M5 结案前置)。
//
// evictor 包级测试(agent/evictor/bytes_test.go)直接调 OnStoreOpen/OnStorePut,覆盖不到
// 两条真契约:
//   - 回调是否挂在生产实际使用的那个 cas.Store 上(assemble 的单槽合并注册);
//   - 累计口径与磁盘口径是否对得上(Put 回调传纯数据字节,stat 见 128B 块头;
//     启动预扫播种的块再被首次 Open/Put 会不会重复计入)。
//
// 本文件用真实 objectstore.Mock + 真实 cas.Store + 真实 fetch.Fetcher 走生产写路径
// (fetch → cs.Put → notifyPut → Evictor.OnStorePut),断言四条不变量。

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/evictor"
	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/core/cas"
	"traincache/core/fetch"
	"traincache/core/objectstore"
)

// capBlockSize 是探针块的数据字节数(256KiB,远小于 fetch 额度上限 1MiB)。
const capBlockSize = 256 << 10

// capFixture 是字节维度探针装置:cas 根目录 = evictor BlockDir(与生产 CacheRoot 同构),
// 观察者经生产注册点 installStoreObservers 挂载。
type capFixture struct {
	cs    *cas.Store
	fetch *fetch.Fetcher
	store *objectstore.Mock
	reg   *metrics.Registry
	ev    *evictor.Evictor
	dir   string
}

// newCapFixture 构造探针装置。capacityBytes ≤ 0 = 关闭字节维度;usage 是磁盘维度桩
// (返回固定利用率,探针不受宿主磁盘水位干扰)。
func newCapFixture(t *testing.T, capacityBytes int64, usage func() (uint64, uint64, error)) *capFixture {
	t.Helper()
	dir := t.TempDir()
	store := objectstore.NewMock()
	cs, err := cas.New(dir, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	f := fetch.New(store, cs, fetch.Options{
		MaxInflightBytes: 1 << 20,
		MaxTasks:         4,
		MaxRetries:       1,
	})
	reg := metrics.New(metrics.Config{Node: "probe-node"})
	ev := evictor.New(cs, nil, nil, reg, nil, evictor.Config{
		BlockDir:           dir,
		CacheCapacityBytes: capacityBytes,
		Usage:              usage,
		Sweep:              func(context.Context) (int64, int64, error) { return 0, 0, nil },
	})
	ag := &Agent{
		Cfg:     Config{CacheRoot: dir, NodeName: nodeid.ID("probe-node"), Verify: VerifyXXH3},
		Metrics: reg,
		CS:      cs,
		Fetcher: f,
		Evictor: ev,
	}
	ag.installStoreObservers(cs)
	return &capFixture{cs: cs, fetch: f, store: store, reg: reg, ev: ev, dir: dir}
}

// idleUsage 是永不触发磁盘维度的利用率桩。
func idleUsage() (uint64, uint64, error) { return 1, 1000, nil }

// fetchBlock 把 size 字节内容放上源站并走生产下载路径写入本地 CAS,返回块指纹。
// 内容按 key 加盐:同内容的块指纹相同,会被 seenFPs 合法去重,装置就无法区分多块累计。
func fetchBlock(t *testing.T, fx *capFixture, key string, size int) cas.Fingerprint {
	t.Helper()
	content := key + strings.Repeat("k", size-len(key))
	if _, err := fx.store.Put(context.Background(), key,
		strings.NewReader(content), int64(size)); err != nil {
		t.Fatalf("源站 Put(%s): %v", key, err)
	}
	fp, err := fx.fetch.Fetch(context.Background(), key, int64(size))
	if err != nil {
		t.Fatalf("Fetch(%s): %v", key, err)
	}
	return fp
}

// dirBlocksBytes 统计 <dir>/blocks 下 .blk 文件的实际字节(与 evictor 启动预扫同口径)。
func dirBlocksBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(filepath.Join(dir, "blocks"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".blk") || strings.HasPrefix(name, "tmp-") {
			return nil
		}
		info, err := d.Info()
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("遍历 blocks/ 目录: %v", err)
	}
	return total
}

// gaugeBytes 读 traincache_cache_capacity_bytes_total{source="bytes"}。
func gaugeBytes(t *testing.T, fx *capFixture) float64 {
	t.Helper()
	return testutil.ToFloat64(fx.reg.CapacityBytes().WithLabelValues(metrics.CapacitySourceBytes))
}

// TestCapacityAccumulatesOnFetchPut:探针 1——生产写路径(fetch → cas.Put)必须在进程内
// 计入字节维度且 gauge 同步;后续 Open 经 seenFPs 去重不得二次计入。
func TestCapacityAccumulatesOnFetchPut(t *testing.T) {
	fx := newCapFixture(t, 64<<20, idleUsage)

	const nBlocks = 3
	var want int64
	fps := make([]cas.Fingerprint, 0, nBlocks)
	for i := 0; i < nBlocks; i++ {
		key := fmt.Sprintf("cap/blocks/%03d", i)
		fps = append(fps, fetchBlock(t, fx, key, capBlockSize))
		want += capBlockSize + cas.HeaderSize // 块文件 size = 数据 + 块头
	}

	if got := fx.ev.BytesAccum(); got != want {
		t.Errorf("Fetch %d 块后 BytesAccum = %d, want %d(进程内 Put 累计未生效)", nBlocks, got, want)
	}
	if g := gaugeBytes(t, fx); g != float64(want) {
		t.Errorf("capacity_bytes{bytes} = %v, want %d(gauge 未随累计刷新)", g, want)
	}

	for _, fp := range fps {
		br, err := fx.cs.Open(fp)
		if err != nil {
			t.Fatalf("cs.Open(%s): %v", fp, err)
		}
		_ = br.Close()
	}
	if got := fx.ev.BytesAccum(); got != want {
		t.Errorf("重复 Open 后 BytesAccum = %d, want %d(seenFPs 去重失效)", got, want)
	}
}

// TestCapacityAccumulationMatchesDiskTruth:探针 2——字节维度的账要等于目录里的真实占用。
// cas.Put 落盘是 128B 块头 + 数据,而 Put 观察者收到的是纯数据字节;差一个块头/块就是
// 账实不符(累计偏低 → 淘汰触发滞后)。
func TestCapacityAccumulationMatchesDiskTruth(t *testing.T) {
	fx := newCapFixture(t, 64<<20, idleUsage)

	const nBlocks = 4
	for i := 0; i < nBlocks; i++ {
		fetchBlock(t, fx, fmt.Sprintf("truth/blocks/%03d", i), capBlockSize)
	}
	disk, accum := dirBlocksBytes(t, fx.dir), fx.ev.BytesAccum()
	if accum != disk {
		t.Errorf("账实不符:BytesAccum=%d,目录实际=%d,差=%d(每块块头 %d 字节 × %d?)",
			accum, disk, disk-accum, cas.HeaderSize, nBlocks)
	}
}

// TestCapacityBootStatNotDoubleCounted:探针 3——进程重启后目录里已有块(启动预扫播种
// BytesAccum),这些块随后被本进程首次 Open 时不得再加一次。seenFPs 未随预扫播种即双计:
// 8GiB 目录读成 16GiB(M5 现场 boot=16,959,176,704 = 2×8,479,588,352 同形),且淘汰
// 每块只减一次 → 累计永远回不到真值。
func TestCapacityBootStatNotDoubleCounted(t *testing.T) {
	dir := t.TempDir()
	cs, err := cas.New(dir, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	reg := metrics.New(metrics.Config{Node: "probe-node"})

	// 上一个进程留下的块:此刻既没有 evictor,也没有观察者(内容按序号加盐,保证指纹互异)。
	ctx := context.Background()
	var fps []cas.Fingerprint
	for i := 0; i < 3; i++ {
		content := fmt.Sprintf("boot-%03d%s", i, strings.Repeat("b", capBlockSize-8))
		fp, _, err := cs.Put(ctx, strings.NewReader(content))
		if err != nil {
			t.Fatalf("预置块 %d: %v", i, err)
		}
		fps = append(fps, fp)
	}

	ev := evictor.New(cs, nil, nil, reg, nil, evictor.Config{
		BlockDir:           dir,
		CacheCapacityBytes: 64 << 20,
		Usage:              idleUsage,
	})
	boot := ev.BytesAccum()
	if disk := dirBlocksBytes(t, dir); boot != disk {
		t.Fatalf("装置前置:启动预扫 BytesAccum=%d 应等于目录=%d", boot, disk)
	}

	(&Agent{Metrics: reg, CS: cs, Evictor: ev}).installStoreObservers(cs)

	// 本进程首次读打开每个块(M5 现场:训练 Pod 起来后逐个命中本地副本)。
	for _, fp := range fps {
		br, err := cs.Open(fp)
		if err != nil {
			t.Fatalf("cs.Open(%s): %v", fp, err)
		}
		_ = br.Close()
	}
	if got := ev.BytesAccum(); got != boot {
		t.Errorf("首次 Open 后 BytesAccum=%d, want 仍为启动预扫值 %d(双计 +%d,%.2f 倍)",
			got, boot, got-boot, float64(got)/float64(boot))
	}
}

// TestCapacityReturnsToZeroAfterEviction:探针 4——淘汰后累计要与目录真值对齐,清空目录后
// 必须归零。累计加纯数据字节、扣文件 size(含块头)则口径不一致;预扫双计则减不干净。
// 两者都会让 gauge 停在非零残值上反复报 danger 空转(M5 现场:目录已空、gauge 仍报
// 8,479,588,352、连 5 个 tick danger)。
func TestCapacityReturnsToZeroAfterEviction(t *testing.T) {
	t.Run("部分淘汰:账实相符", func(t *testing.T) {
		const capacity = 768 << 10 // 3 个块文件的 size;Fetch 6 块必然越线
		fx := newCapFixture(t, capacity, idleUsage)

		for i := 0; i < 6; i++ {
			fetchBlock(t, fx, fmt.Sprintf("drain/blocks/%03d", i), capBlockSize)
		}
		if got := fx.ev.BytesAccum(); got <= capacity {
			t.Fatalf("装置前置:累计 %d 应超过容量 %d 才能触发淘汰", got, capacity)
		}
		fx.ev.RunOnce(context.Background())

		remaining := dirBlocksBytes(t, fx.dir)
		if got := fx.ev.BytesAccum(); got != remaining {
			t.Errorf("淘汰后 BytesAccum=%d,目录实际=%d,残留 %d 个块文件",
				got, remaining, len(blkFiles(t, fx.dir)))
		}
		if g := gaugeBytes(t, fx); g != float64(remaining) {
			t.Errorf("淘汰后 capacity_bytes{bytes} = %v, want %d", g, remaining)
		}
	})

	t.Run("清空淘汰:累计归零", func(t *testing.T) {
		// 容量 1 字节:danger 目标 = 0,本轮必须把块删空;删空后累计应恰为 0。
		fx := newCapFixture(t, 1, idleUsage)
		for i := 0; i < 3; i++ {
			fetchBlock(t, fx, fmt.Sprintf("zero/blocks/%03d", i), capBlockSize)
		}
		fx.ev.RunOnce(context.Background())

		if files := blkFiles(t, fx.dir); len(files) != 0 {
			t.Fatalf("装置前置:目标为 0 字节,块应删空,实际残留 %d 个", len(files))
		}
		if got := fx.ev.BytesAccum(); got != 0 {
			t.Errorf("目录已空但 BytesAccum = %d, want 0(累计与扣减不同源)", got)
		}
		if g := gaugeBytes(t, fx); g != 0 {
			t.Errorf("目录已空但 capacity_bytes{bytes} = %v, want 0", g)
		}
	})
}

// blkFiles 返回目录里的块文件路径(诊断用)。
func blkFiles(t *testing.T, dir string) []string {
	t.Helper()
	out, err := filepath.Glob(filepath.Join(dir, "blocks", "*", "*.blk"))
	if err != nil {
		t.Fatalf("枚举块文件: %v", err)
	}
	return out
}

// TestCapacityAccumulatesInProductionAssembly 走生产装配路径(assembleWith)断言
// 字节维度累计:上面的用例手搭 Agent 再调 installStoreObservers,覆盖不到
// Assemble 里「注册现场」与「实际写入所用的 cas.Store」是否同一个实例。
func TestCapacityAccumulatesInProductionAssembly(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeName:           nodeid.ID("asm-node"),
		CacheRoot:          filepath.Join(dir, "cache"),
		OrdersDir:          filepath.Join(dir, "orders"),
		Verify:             VerifyXXH3,
		CacheCapacityBytes: 64 << 20,
	}
	ag, err := assembleWith(objectstore.NewMock(), cfg)
	if err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	defer ag.Prefetch.Close()

	content := "asm" + strings.Repeat("q", 4096)
	_, n, err := ag.CS.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("assemble 后的 cas.Put: %v", err)
	}
	want := float64(n) + cas.HeaderSize
	got := testutil.ToFloat64(ag.Metrics.CapacityBytes().WithLabelValues(metrics.CapacitySourceBytes))
	if got != want {
		t.Errorf("生产装配下 capacity_bytes{bytes} = %v, want %v(观察者没挂在生产使用的 cas.Store 上)", got, want)
	}
}

// TestCapacityAccumulatesOnAssemblyFetch 与上一用例同构,但走下载路径
// (assembleWith → ag.Fetcher.Fetch → cas.Put),与真机预热的写入方式一致。
func TestCapacityAccumulatesOnAssemblyFetch(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeName:           nodeid.ID("asm-node"),
		CacheRoot:          filepath.Join(dir, "cache"),
		OrdersDir:          filepath.Join(dir, "orders"),
		Verify:             VerifyXXH3,
		CacheCapacityBytes: 64 << 20,
	}
	ag, err := assembleWith(objectstore.NewMock(), cfg)
	if err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	defer ag.Prefetch.Close()

	ctx := context.Background()
	const size = 1 << 20
	content := "asm-fetch" + strings.Repeat("w", size-9)
	key := "wd/blocks/shard-00000.tar#0"
	if _, err := ag.ObjectStore.Put(ctx, key, strings.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("源站 Put: %v", err)
	}
	if _, err := ag.Fetcher.Fetch(ctx, key, int64(len(content))); err != nil {
		t.Fatalf("Fetcher.Fetch: %v", err)
	}
	want := float64(len(content)) + cas.HeaderSize
	got := testutil.ToFloat64(ag.Metrics.CapacityBytes().WithLabelValues(metrics.CapacitySourceBytes))
	if got != want {
		t.Errorf("生产装配 + 下载路径下 capacity_bytes{bytes} = %v, want %v", got, want)
	}
}

// TestCapacitySurvivesMissOpenProbe 复现 M5 真机现场:预取窗口在块落地前就用 cas.Open
// 探就绪(estimateETA、回填、rescan 同形态),而 Open 前置观察者对缺失块同样触发。
// 缺失块 stat 不到 size,若这一步就占用 seenFPs 去重槽,随后本进程 Put 同一块会被当成
// 「已累计」跳过——冷缓存下几乎每个块都先被探测,累计永远停在启动预扫值,字节维度淘汰
// 不再触发(capacity_bytes{bytes} 恒为播种值、evict 增量 0)。
func TestCapacitySurvivesMissOpenProbe(t *testing.T) {
	ctx := context.Background()
	fx := newCapFixture(t, 64<<20, idleUsage)

	const size = capBlockSize
	key := "probe/blocks/000"
	content := key + strings.Repeat("k", size-len(key))

	// 块指纹在块落地前由索引侧给出:用另一份 cas.Store 预计算,不写进本装置目录。
	scratch, err := cas.New(t.TempDir(), cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("scratch cas.New: %v", err)
	}
	fp, _, err := scratch.Put(ctx, strings.NewReader(content))
	if err != nil {
		t.Fatalf("scratch 预计算指纹: %v", err)
	}

	if br, err := fx.cs.Open(fp); err == nil {
		_ = br.Close()
		t.Fatalf("装置前置:探针块 %s 不该已在缓存里", fp)
	}
	if got := fx.ev.BytesAccum(); got != 0 {
		t.Fatalf("装置前置:缺失块探测后 BytesAccum = %d, want 0", got)
	}

	if _, err := fx.store.Put(ctx, key, strings.NewReader(content), int64(size)); err != nil {
		t.Fatalf("源站 Put: %v", err)
	}
	if got, err := fx.fetch.Fetch(ctx, key, int64(size)); err != nil {
		t.Fatalf("Fetch: %v", err)
	} else if got != fp {
		t.Fatalf("Fetch 指纹 %s 与探测指纹 %s 不一致", got, fp)
	}

	want := int64(size) + cas.HeaderSize
	if got := fx.ev.BytesAccum(); got != want {
		t.Errorf("探测未命中后再落地:BytesAccum = %d, want %d(缺失块探测占了去重槽,漏记整块)", got, want)
	}
	if g := gaugeBytes(t, fx); g != float64(want) {
		t.Errorf("capacity_bytes{bytes} = %v, want %d(gauge 未反映落地块)", g, want)
	}

	// 落地后再探测同一块:仍只计一次。
	if br, err := fx.cs.Open(fp); err == nil {
		_ = br.Close()
	} else {
		t.Fatalf("cs.Open(%s): %v", fp, err)
	}
	if got := fx.ev.BytesAccum(); got != want {
		t.Errorf("命中 Open 后 BytesAccum = %d, want %d(去重失效)", got, want)
	}
}
