package fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/core/cas"
	"traincache/core/internal/digest"
	"traincache/core/objectstore"
)

// fetchResult 是单次 Fetch 的返回值快照，便于从 goroutine 回传。
type fetchResult struct {
	fp  cas.Fingerprint
	err error
}

// newTestCAS 在临时目录创建 xxh3 校验等级的 cas.Store，返回 store 与其目录。
func newTestCAS(t *testing.T) (*cas.Store, string) {
	t.Helper()
	dir := t.TempDir()
	cs, err := cas.New(dir, cas.VerifyXXH3)
	if err != nil {
		t.Fatalf("cas.New: %v", err)
	}
	return cs, dir
}

// fpOf 返回内容对应的 CAS 指纹。
func fpOf(content []byte) cas.Fingerprint {
	return cas.Fingerprint(digest.XXH3_128(content))
}

// seed 把 key -> content 放进内存 mock 对象存储。
func seed(t *testing.T, m *objectstore.Mock, key string, content []byte) {
	t.Helper()
	if _, err := m.Put(context.Background(), key, bytes.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

// waitFor 轮询 cond 直到为 true 或超时。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("waitFor: condition not met before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

// updateMax 原子地把 *dst 抬到 >= v。
func updateMax(dst *atomic.Int64, v int64) {
	for {
		cur := dst.Load()
		if v <= cur || dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

// fnStore 用自定义 Get 行为包装 objectstore.Mock（其余方法走内存 mock）。
type fnStore struct {
	*objectstore.Mock
	get func(ctx context.Context, key string, w io.Writer) error
}

func newFnStore(get func(ctx context.Context, key string, w io.Writer) error) *fnStore {
	return &fnStore{Mock: objectstore.NewMock(), get: get}
}

func (s *fnStore) Get(ctx context.Context, key string, w io.Writer) error {
	return s.get(ctx, key, w)
}

// slowStore 流式慢速 Get：每次写 chunkSize 字节后停 delay，并统计并发 Get 峰值。
type slowStore struct {
	*objectstore.Mock
	chunkSize int
	delay     time.Duration
	cur       atomic.Int64
	peak      atomic.Int64
}

func (s *slowStore) Get(ctx context.Context, key string, w io.Writer) error {
	n := s.cur.Add(1)
	defer s.cur.Add(-1)
	updateMax(&s.peak, n)

	var buf bytes.Buffer
	if err := s.Mock.Get(ctx, key, &buf); err != nil {
		return err
	}
	data := buf.Bytes()
	for off := 0; off < len(data); off += s.chunkSize {
		end := min(off+s.chunkSize, len(data))
		if _, err := w.Write(data[off:end]); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.delay):
		}
	}
	return nil
}

// listBlockFiles 返回 casDir/blocks 下所有块文件路径。
func listBlockFiles(casDir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(filepath.Join(casDir, "blocks"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".blk") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

// listTmpFiles 返回 casDir/blocks 下的临时文件（cas.Put 的 tmp-*；正常收尾不应残留）。
func listTmpFiles(casDir string) ([]string, error) {
	return filepath.Glob(filepath.Join(casDir, "blocks", "tmp-*"))
}

// readBlock 打开并读回块内容（顺带验证块可通过 cas.Open 校验）。
func readBlock(t *testing.T, cs *cas.Store, fp cas.Fingerprint) []byte {
	t.Helper()
	br, err := cs.Open(fp)
	if err != nil {
		t.Fatalf("open block %s: %v", fp, err)
	}
	defer br.Close()
	data, err := io.ReadAll(br)
	if err != nil {
		t.Fatalf("read block %s: %v", fp, err)
	}
	return data
}

// corruptBlockTmp 翻转 casDir/blocks 下唯一 tmp-* 文件数据区的一个字节（偏移 128+3）。
// 调用时机是 store.Get 把全部数据交付给 io.Pipe 之后、io.Pipe 关闭之前——但此时
// cas.Put 可能尚未把管道缓冲写进临时文件，故先轮询等文件长到 128+dataSize（数据区
// 写毕；此后 Put 只会回填 128B 块头并 rename，不触碰数据区）再翻转。翻转的字节因此
// 确定性地进入最终块文件，使随后的 cas.Open 必然报 ErrCorrupt（“Put 已 rename、
// Open 未开始”的等价注入点，见 M3 DECISIONS）。
func corruptBlockTmp(casDir string, dataSize int) error {
	matches, err := filepath.Glob(filepath.Join(casDir, "blocks", "tmp-*"))
	if err != nil {
		return err
	}
	if len(matches) != 1 {
		return fmt.Errorf("want exactly 1 tmp file, got %d: %v", len(matches), matches)
	}
	path := matches[0]
	wantSize := int64(cas.HeaderSize + dataSize)
	deadline := time.Now().Add(5 * time.Second)
	for {
		fi, err := os.Stat(path)
		if err == nil && fi.Size() >= wantSize {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("tmp file %s did not reach %d bytes", path, wantSize)
		}
		time.Sleep(time.Millisecond)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	const off = int64(cas.HeaderSize + 3) // 数据区第 4 字节；Put 之后不会再写该区域
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		return fmt.Errorf("read tmp data byte: %w", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], off); err != nil {
		return fmt.Errorf("flip tmp data byte: %w", err)
	}
	return nil
}

// waitNoLeak 等待 goroutine 数回落至 before 以内；超时打印全部栈并失败。
func waitNoLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if after := runtime.NumGoroutine(); after <= before {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutine leak: before=%d after=%d\n%s", before, runtime.NumGoroutine(), buf[:n])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestInflightByteLimit：并发下载多个大对象，实测（store 侧独立观测 + limiter 仪表）
// 峰值在途字节不超过 MaxInflightBytes（design.md §5.3）。
func TestInflightByteLimit(t *testing.T) {
	const (
		objSize = 16 << 10 // 每个对象 16 KiB
		nObjs   = 6
		chunk   = 4 << 10
	)
	limit := int64(40 << 10) // 40 KiB：至多 2 个对象同时在途
	ctx := context.Background()

	store := &slowStore{Mock: objectstore.NewMock(), chunkSize: chunk, delay: 2 * time.Millisecond}
	keys := make([]string, nObjs)
	contents := make([][]byte, nObjs)
	for i := range contents {
		keys[i] = fmt.Sprintf("obj/%d", i)
		contents[i] = bytes.Repeat([]byte{byte('A' + i)}, objSize)
		seed(t, store.Mock, keys[i], contents[i])
	}

	cs, _ := newTestCAS(t)
	// MaxConnsPerHost / MaxTasks 放宽，确保唯一约束来自字节信号量。
	f := New(store, cs, Options{MaxInflightBytes: limit, MaxConnsPerHost: 64, MaxTasks: 64, MaxRetries: 1})

	// 采样 limiter 的在途字节仪表，记录峰值。
	var peakGauge atomic.Int64
	stop := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(200 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				updateMax(&peakGauge, f.lim.inflightBytes())
			}
		}
	}()

	var wg sync.WaitGroup
	results := make([]fetchResult, nObjs)
	for i := 0; i < nObjs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i].fp, results[i].err = f.Fetch(ctx, keys[i], objSize)
		}(i)
	}
	wg.Wait()
	close(stop)
	<-samplerDone

	for i, r := range results {
		if r.err != nil {
			t.Fatalf("Fetch(%s): %v", keys[i], r.err)
		}
		if want := fpOf(contents[i]); r.fp != want {
			t.Fatalf("Fetch(%s) fp = %s, want %s", keys[i], r.fp, want)
		}
		if got := readBlock(t, cs, r.fp); !bytes.Equal(got, contents[i]) {
			t.Fatalf("block %s content mismatch", r.fp)
		}
	}

	// 独立观测（不依赖 limiter 自报）：并发 Get 峰值 = 2 = limit/objSize 向下取整，
	// 既证明限流生效，也证明测试确实制造了并发。
	if peak := store.peak.Load(); peak != 2 {
		t.Fatalf("peak concurrent Get = %d, want 2 (limit %d, objSize %d)", peak, limit, objSize)
	}
	// limiter 仪表峰值须 <= 上限，且如实计入了在途字节。
	if peak := peakGauge.Load(); peak > limit || peak < objSize {
		t.Fatalf("peak inflight gauge = %d, want in [%d, %d]", peak, objSize, limit)
	}
}

// TestMergeSameKey：并发 Fetch 同一 key 合并为一次实际下载——mock 计数 Get 恰好 1 次，
// 所有调用者拿到同一指纹与 nil 错误（design.md §5.3）。
func TestMergeSameKey(t *testing.T) {
	const nCallers = 8
	key := "dataset/ds/snapshots/1/shard-abcd.idx"
	content := bytes.Repeat([]byte("merge-me-"), 512)
	ctx := context.Background()

	ms := objectstore.NewMock()
	seed(t, ms, key, content)

	var calls atomic.Int64
	entered := make(chan struct{}, nCallers)
	release := make(chan struct{})
	store := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return ms.Get(ctx, k, w)
	})

	cs, _ := newTestCAS(t)
	f := New(store, cs, Options{}) // 全零值 -> design.md §8 默认值

	ownerDone := make(chan fetchResult, 1)
	go func() {
		fp, err := f.Fetch(ctx, key, int64(len(content)))
		ownerDone <- fetchResult{fp, err}
	}()
	<-entered // owner 已注册 in-flight 并阻塞在下载中

	results := make([]fetchResult, nCallers-1)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i].fp, results[i].err = f.Fetch(ctx, key, int64(len(content)))
		}(i)
	}
	// 等全部等待者登记到同一次 in-flight 下载（waiters 受 f.mu 保护，同包测试可读）。
	waitFor(t, 3*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		c := f.inflight[key]
		return c != nil && c.waiters == nCallers-1
	})

	close(release) // 放行唯一一次实际下载
	owner := <-ownerDone
	wg.Wait()

	want := fpOf(content)
	if owner.err != nil {
		t.Fatalf("owner Fetch: %v", owner.err)
	}
	if owner.fp != want {
		t.Fatalf("owner fp = %s, want %s", owner.fp, want)
	}
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("waiter %d: %v", i, r.err)
		}
		if r.fp != want {
			t.Fatalf("waiter %d fp = %s, want %s", i, r.fp, want)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("store.Get calls = %d, want exactly 1 (指令合并)", got)
	}
}

// TestRetryAfterCorrupt：第 1 次下载落的块 Open 校验失败 -> 自动 Remove + 退避重下，
// 第 2 次成功；块目录在退避窗口内应观测不到块（证明 Remove 发生在重下之前）。
func TestRetryAfterCorrupt(t *testing.T) {
	key := "dataset/ds/snapshots/1/chunk-0"
	content := bytes.Repeat([]byte("retry-after-corrupt-"), 256) // 4 KiB
	ctx := context.Background()

	ms := objectstore.NewMock()
	seed(t, ms, key, content)
	cs, casDir := newTestCAS(t)

	var calls atomic.Int64
	store := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		n := calls.Add(1)
		if err := ms.Get(ctx, k, w); err != nil {
			return err
		}
		if n == 1 { // 仅第一次下载后破坏临时块（Put rename 后即为损坏的块文件）
			if err := corruptBlockTmp(casDir, len(content)); err != nil {
				t.Errorf("corrupt tmp block: %v", err)
			}
		}
		return nil
	})

	// 观察块目录存在性：Remove 在 Open 校验失败后、第 2 次下载前发生，
	// 因此第一次重试的退避窗口（>= 100ms）内采样应大量为“无块”。
	var (
		mu           sync.Mutex
		absentSample int
	)
	stop := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if blocks, err := listBlockFiles(casDir); err == nil && len(blocks) == 0 {
					mu.Lock()
					absentSample++
					mu.Unlock()
				}
			}
		}
	}()

	f := New(store, cs, Options{}) // MaxRetries 默认 3，本用例只需 1 次重下
	fp, err := f.Fetch(ctx, key, int64(len(content)))
	close(stop)
	<-watchDone

	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if want := fpOf(content); fp != want {
		t.Fatalf("fp = %s, want %s", fp, want)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("store.Get calls = %d, want 2（1 次失败 + 1 次重下成功）", got)
	}
	if got := readBlock(t, cs, fp); !bytes.Equal(got, content) {
		t.Fatalf("recovered block content mismatch")
	}
	blocks, err := listBlockFiles(casDir)
	if err != nil || len(blocks) != 1 {
		t.Fatalf("block files after recovery = %v (err %v), want exactly 1", blocks, err)
	}
	if tmps, _ := listTmpFiles(casDir); len(tmps) != 0 {
		t.Fatalf("residual tmp files = %v, want none", tmps)
	}
	mu.Lock()
	absent := absentSample
	mu.Unlock()
	if absent < 5 {
		t.Fatalf("block absent samples = %d, want >= 5：Remove 未发生在第 2 次下载前?", absent)
	}
}

// TestRetryExhausted：每次下载落的块都被破坏 -> 重试耗尽的尝试全部经 Open 校验失败，
// 返回 ErrTooManyRetries，且不残留块文件与临时文件（design.md §7.5）。
func TestRetryExhausted(t *testing.T) {
	const maxRetries = 2
	key := "dataset/ds/snapshots/1/chunk-1"
	content := bytes.Repeat([]byte("always-corrupt-"), 256) // 4 KiB
	ctx := context.Background()

	ms := objectstore.NewMock()
	seed(t, ms, key, content)
	cs, casDir := newTestCAS(t)

	var calls atomic.Int64
	store := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		calls.Add(1)
		if err := ms.Get(ctx, k, w); err != nil {
			return err
		}
		if err := corruptBlockTmp(casDir, len(content)); err != nil { // 每次下载都破坏
			t.Errorf("corrupt tmp block: %v", err)
		}
		return nil
	})

	f := New(store, cs, Options{MaxRetries: maxRetries})
	_, err := f.Fetch(ctx, key, int64(len(content)))
	if !errors.Is(err, ErrTooManyRetries) {
		t.Fatalf("Fetch error = %v, want ErrTooManyRetries", err)
	}
	if got := calls.Load(); got != maxRetries+1 {
		t.Fatalf("store.Get calls = %d, want %d（1 次初试 + %d 次重下）", got, maxRetries+1, maxRetries)
	}
	if blocks, err := listBlockFiles(casDir); err != nil || len(blocks) != 0 {
		t.Fatalf("residual blocks = %v (err %v), want none", blocks, err)
	}
	if tmps, _ := listTmpFiles(casDir); len(tmps) != 0 {
		t.Fatalf("residual tmp files = %v, want none", tmps)
	}
}

// TestFetcherMaxTasksLimit：从 Fetcher.Fetch 入口验证 MaxTasks 任务数上限语义
// （docs/5.3 §5）——MaxTasks=2 并发 Fetch 5 个不同 key：mock Store.Get 里加门控
// （每个在途任务上报进入/离开并记录峰值并发），断言峰值恰为 2（既不超过 MaxTasks，
// 也确实制造了并发）；放行后 5 个 key 全部落盘成功、任务额度全部归还、无死锁。
// ctx 取消路径已由 TestCtxCancel 与 TestLimiterTaskSemaphore 覆盖，本用例不重复。
func TestFetcherMaxTasksLimit(t *testing.T) {
	const (
		nKeys    = 5
		maxTasks = 2
		chunk    = 4 << 10 // 每个对象 4 KiB，字节额度（默认 1 GiB）不构成约束
	)
	ctx := context.Background()

	// 每个 key 内容不同 -> 指纹不同 -> 5 次独立下载，不触发同 key 合并。
	contents := make([][]byte, nKeys)
	byFP := make(map[cas.Fingerprint][]byte, nKeys)
	ms := objectstore.NewMock()
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("obj/%d", i)
		contents[i] = bytes.Repeat([]byte{byte('A' + i)}, chunk)
		seed(t, ms, keys[i], contents[i])
		byFP[fpOf(contents[i])] = contents[i]
	}

	var (
		cur     atomic.Int64
		peak    atomic.Int64
		entered = make(chan struct{}, nKeys) // 带缓冲：上报不阻塞，取消后迟到事件可安全丢弃
		release = make(chan struct{})        // 门控：闭合并放行全部在途 Get，后续任务随额度释放自然流过
	)
	store := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		n := cur.Add(1)
		defer cur.Add(-1)
		updateMax(&peak, n)
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return ms.Get(ctx, k, w)
	})

	cs, casDir := newTestCAS(t)
	// 唯一约束是 MaxTasks=2；其余维度放宽（同 host "obj"，连接额度不构成限制）。
	f := New(store, cs, Options{MaxTasks: maxTasks, MaxConnsPerHost: 64})

	start := make(chan struct{})
	results := make(chan fetchResult, nKeys)
	for i := 0; i < nKeys; i++ {
		go func(i int) {
			<-start
			fp, err := f.Fetch(ctx, keys[i], int64(chunk))
			results <- fetchResult{fp, err}
		}(i)
	}
	close(start)

	// 恰好 maxTasks 个任务进入 Get 并被门控拦下：此时其余 goroutine 只能阻塞在
	// 任务额度上（阻塞者不可能进入 Get，故此刻 peak 亦不超 2）。等待超时即死锁/上限失效。
	for i := 0; i < maxTasks; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/%d tasks entered store.Get: MaxTasks 未放行足额任务或发生死锁", i, maxTasks)
		}
	}
	// 两个被拦下的任务同时在途且都尚未离开，peak=2 由原子记账确定性成立。
	if p := peak.Load(); p != maxTasks {
		t.Fatalf("gated peak concurrent store.Get = %d, want exactly %d", p, maxTasks)
	}

	close(release) // 放行全部在途下载；剩余任务按额度逐个进入直至完成

	deadline := time.After(30 * time.Second)
	done := 0
	for done < nKeys {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("Fetch: %v", r.err)
			}
			want, ok := byFP[r.fp]
			if !ok {
				t.Fatalf("Fetch fp = %s, want one of the %d seeded contents", r.fp, nKeys)
			}
			if got := readBlock(t, cs, r.fp); !bytes.Equal(got, want) {
				t.Fatalf("block %s content mismatch", r.fp)
			}
			done++
		case <-deadline:
			t.Fatalf("only %d/%d Fetch returned before deadline: deadlock?", done, nKeys)
		}
	}

	// 全程峰值并发恰为 2：既证明第 3 个任务在额度释放前无法进入下载，也证明测试确实制造了并发。
	if p := peak.Load(); p != maxTasks {
		t.Fatalf("peak concurrent store.Get = %d, want exactly %d (MaxTasks=%d)", p, maxTasks, maxTasks)
	}
	if got := f.lim.tasks.current(); got != 0 {
		t.Fatalf("tasks.current() = %d, want 0（任务额度全部归还）", got)
	}
	if got := f.lim.bytes.inflight(); got != 0 {
		t.Fatalf("inflight bytes after completion = %d, want 0", got)
	}
	blocks, err := listBlockFiles(casDir)
	if err != nil || len(blocks) != nKeys {
		t.Fatalf("block files = %v (err %v), want %d", blocks, err, nKeys)
	}
	if tmps, _ := listTmpFiles(casDir); len(tmps) != 0 {
		t.Fatalf("residual tmp files = %v, want none", tmps)
	}
}

// TestCtxCancel：ctx 取消可打断字节信号量等待与下载中途；退出后无信号量泄漏、
// 无残留块/临时文件、无 goroutine 泄漏。
func TestCtxCancel(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx := context.Background()

	// ---- 场景 1：字节额度被占满时，等待者在 Acquire 上阻塞，ctx 取消使其立即退出 ----
	content1 := bytes.Repeat([]byte("gate-"), 256)
	ms1 := objectstore.NewMock()
	seed(t, ms1, "a/obj", content1)
	seed(t, ms1, "b/obj", content1)

	gateEntered := make(chan struct{}, 4)
	gateRelease := make(chan struct{})
	var gateCalls atomic.Int64
	store1 := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		gateCalls.Add(1)
		select {
		case gateEntered <- struct{}{}:
		default:
		}
		select {
		case <-gateRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
		return ms1.Get(ctx, k, w)
	})

	cs1, _ := newTestCAS(t)
	f1 := New(store1, cs1, Options{MaxInflightBytes: int64(len(content1)), MaxTasks: 4, MaxConnsPerHost: 4})

	aDone := make(chan fetchResult, 1)
	go func() {
		fp, err := f1.Fetch(ctx, "a/obj", int64(len(content1)))
		aDone <- fetchResult{fp, err}
	}()
	<-gateEntered // A 已取走全部字节额度并进入下载

	bCtx, cancelB := context.WithCancel(ctx)
	bDone := make(chan fetchResult, 1)
	go func() {
		fp, err := f1.Fetch(bCtx, "b/obj", int64(len(content1)))
		bDone <- fetchResult{fp, err}
	}()
	// 等 B 取得任务额度并阻塞在字节 Acquire 上（额度被 A 占满）。
	waitFor(t, 2*time.Second, func() bool {
		return f1.lim.tasks.current() == 2 && f1.lim.bytes.inflight() == int64(len(content1))
	})
	cancelB()

	select {
	case r := <-bDone:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("waiter Fetch after cancel: err = %v, want context.Canceled", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Fetch blocked on byte Acquire did not return after ctx cancel")
	}
	if got := gateCalls.Load(); got != 1 {
		t.Fatalf("store.Get calls = %d, want 1：B 不应在字节额度不足时进入下载", got)
	}

	close(gateRelease) // 放行 A
	select {
	case r := <-aDone:
		if r.err != nil {
			t.Fatalf("owner Fetch: %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("owner Fetch did not finish")
	}
	if got := f1.lim.bytes.inflight(); got != 0 {
		t.Fatalf("inflight bytes after completion = %d, want 0（Release 无泄漏）", got)
	}

	// ---- 场景 2：下载中途（Get 正在流式写）取消：立即退出、无残留 ----
	content2 := bytes.Repeat([]byte("stream-"), 8192)
	ms2 := objectstore.NewMock()
	seed(t, ms2, "slow/obj", content2)
	cs2, casDir2 := newTestCAS(t)

	started := make(chan struct{}, 1)
	var startOnce sync.Once
	store2 := newFnStore(func(ctx context.Context, k string, w io.Writer) error {
		var buf bytes.Buffer
		if err := ms2.Get(ctx, k, &buf); err != nil {
			return err
		}
		data := buf.Bytes()
		for off := 0; off < len(data); off += 4096 {
			end := min(off+4096, len(data))
			startOnce.Do(func() { started <- struct{}{} })
			if _, err := w.Write(data[off:end]); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Millisecond):
			}
		}
		return nil
	})

	f2 := New(store2, cs2, Options{MaxInflightBytes: 1 << 20, MaxTasks: 4, MaxConnsPerHost: 4})
	cCtx, cancelC := context.WithCancel(ctx)
	cDone := make(chan fetchResult, 1)
	go func() {
		fp, err := f2.Fetch(cCtx, "slow/obj", int64(len(content2)))
		cDone <- fetchResult{fp, err}
	}()
	<-started
	time.Sleep(10 * time.Millisecond) // 让若干分片真正流入 Put
	cancelC()

	select {
	case r := <-cDone:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("mid-download cancel: err = %v, want context.Canceled", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Fetch did not return after mid-download ctx cancel")
	}
	if got := f2.lim.bytes.inflight(); got != 0 {
		t.Fatalf("inflight bytes after cancel = %d, want 0（Release 无泄漏）", got)
	}
	if blocks, err := listBlockFiles(casDir2); err != nil || len(blocks) != 0 {
		t.Fatalf("residual blocks after cancel = %v (err %v), want none", blocks, err)
	}
	if tmps, _ := listTmpFiles(casDir2); len(tmps) != 0 {
		t.Fatalf("residual tmp files after cancel = %v, want none", tmps)
	}

	waitNoLeak(t, before)
}
