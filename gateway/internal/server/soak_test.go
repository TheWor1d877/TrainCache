package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 长时稳态（T28）默认参数：缩短版由 CI/本机执行，4 小时版本由运维执行。
const (
	soakDefaultDuration    = 30 * time.Second
	soakDefaultConcurrency = 8
	soakObjectSize         = 1 << 20 // 1MiB，整块落在默认 10MiB 合并窗口内
	soakDatasetCount       = 3
	soakObjectsPerDataset  = 4
	soakSampleInterval     = 5 * time.Second
	soakHeapGrowthSlack    = 32 << 20 // 允许的额外堆增量：32MiB
)

// soakMemSample 是一次定期采样（仅测试内使用）。
type soakMemSample struct {
	at         time.Duration
	heapInuse  uint64
	heapAlloc  uint64
	goroutines int
}

// soakDuration 读取 GATEWAY_SOAK_DURATION；未设置、解析失败或非正值都退回 30s。
func soakDuration() time.Duration {
	raw := strings.TrimSpace(os.Getenv("GATEWAY_SOAK_DURATION"))
	if raw == "" {
		return soakDefaultDuration
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return soakDefaultDuration
	}
	return d
}

// soakConcurrency 读取 GATEWAY_SOAK_CONCURRENCY；未设置、解析失败或非正值都退回 8。
func soakConcurrency() int {
	raw := strings.TrimSpace(os.Getenv("GATEWAY_SOAK_CONCURRENCY"))
	if raw == "" {
		return soakDefaultConcurrency
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return soakDefaultConcurrency
	}
	return n
}

// sampleSoak 采样一次。先 GC 再 ReadMemStats，HeapInuse 反映的是活对象的稳态，
// 而不是尚未回收的临时分配。
func sampleSoak(at time.Duration) soakMemSample {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return soakMemSample{
		at:         at,
		heapInuse:  m.HeapInuse,
		heapAlloc:  m.HeapAlloc,
		goroutines: runtime.NumGoroutine(),
	}
}

func (s soakMemSample) String() string {
	return fmt.Sprintf("t=%s HeapInuse=%.1fMiB HeapAlloc=%.1fMiB goroutines=%d",
		s.at.Round(time.Second), float64(s.heapInuse)/(1<<20), float64(s.heapAlloc)/(1<<20), s.goroutines)
}

// TestSoakSteadyState 覆盖长时间运行的内存稳态（design 10.1 §8 K2/K3/K4/K6）：
//
//   - 真实 merge.Registry / sched.Scheduler / obs.Logger + httptest 假源站；
//   - 3 个 dataset × 每个 4 个对象，每个对象 1MiB，随机键并发 GET，重复命中触发合并；
//   - 每个响应校验长度与 sha256；
//   - 每 5s 采样 HeapInuse / HeapAlloc / NumGoroutine；
//   - 结束时断言堆不随请求数增长、goroutine 回到基线附近、在途表清空、无失败请求。
//
// 缩短版（默认 30s）用于回归；4 小时版本由运维用同一用例加长 GATEWAY_SOAK_DURATION 执行。
func TestSoakSteadyState(t *testing.T) {
	if testing.Short() {
		t.Skip("soak 用例在 -short 下跳过（需要真实跑满时长）")
	}

	duration := soakDuration()
	concurrency := soakConcurrency()

	// 预生成对象：键沿用 D25 布局 <dataset>/blocks/<path>#<chunk>，内容按 key 确定，
	// 便于在失败时复现；1MiB 保证整块落在默认合并窗口内（K1）。
	type soakObject struct {
		key    string
		target string
		data   []byte
		sum    [sha256.Size]byte
	}
	objects := make([]soakObject, 0, soakDatasetCount*soakObjectsPerDataset)
	byKey := make(map[string][]byte, cap(objects))
	for d := 0; d < soakDatasetCount; d++ {
		for i := 0; i < soakObjectsPerDataset; i++ {
			key := fmt.Sprintf("soak-ds-%d/blocks/obj-%d#0", d, i)
			data := soakObjectBytes(key)
			objects = append(objects, soakObject{
				key:    key,
				target: (&url.URL{Path: "/traincache/" + key}).EscapedPath(),
				data:   data,
				sum:    sha256.Sum256(data),
			})
			byKey[key] = data
		}
	}

	// 假源站按 key 返回对应字节。每次 Read 后睡 100µs，让同键并发请求有重叠窗口，
	// 真实触发 registry 的合并路径（而不是把对象一次性吐完）。
	org := &fakeOrigin{
		openFn: func(_ context.Context, key string) (io.ReadCloser, error) {
			data, ok := byKey[key]
			if !ok {
				return nil, fmt.Errorf("soak 假源站: 未知 key %q", key)
			}
			return io.NopCloser(&soakThrottledReader{data: data, delay: 100 * time.Microsecond}), nil
		},
	}
	s, ob := newMergeServer(t, org, soakObjectSize, 32)
	ts := httptest.NewServer(s.Handler())
	client := ts.Client()

	baselineG := runtime.NumGoroutine()
	samples := []soakMemSample{sampleSoak(0)}
	t.Logf("soak 开始: duration=%s concurrency=%d objects=%d baseline_goroutines=%d %s",
		duration, concurrency, len(objects), baselineG, samples[0])

	var (
		failures atomic.Int64
		errMu    sync.Mutex
		firstErr error
	)
	recordFailure := func(err error) {
		failures.Add(1)
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)*7919))
			for time.Now().Before(deadline) {
				o := objects[r.Intn(len(objects))]
				resp, err := client.Get(ts.URL + o.target)
				if err != nil {
					recordFailure(fmt.Errorf("GET %s: %w", o.key, err))
				} else {
					h := sha256.New()
					n, readErr := io.Copy(h, resp.Body)
					_ = resp.Body.Close()
					switch {
					case readErr != nil:
						recordFailure(fmt.Errorf("读 %s: %w", o.key, readErr))
					case resp.StatusCode != http.StatusOK:
						recordFailure(fmt.Errorf("GET %s 状态 = %d", o.key, resp.StatusCode))
					case n != int64(len(o.data)):
						recordFailure(fmt.Errorf("GET %s 长度 = %d, want %d", o.key, n, len(o.data)))
					case !bytes.Equal(h.Sum(nil), o.sum[:]):
						recordFailure(fmt.Errorf("GET %s sha256 不一致", o.key))
					}
				}
				// 请求间随机 0–2ms，避免所有 worker 完全同步。
				time.Sleep(time.Duration(r.Intn(2000)) * time.Microsecond)
			}
		}(w)
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	start := time.Now()
	ticker := time.NewTicker(soakSampleInterval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ticker.C:
			s := sampleSoak(time.Since(start))
			samples = append(samples, s)
			t.Logf("soak 采样 %s", s)
		case <-finished:
			break loop
		}
	}

	// 关闭客户端与服务端，等待连接处理协程退出后再比较 goroutine 数。
	ts.CloseClientConnections()
	client.CloseIdleConnections()
	ts.Close()

	settleDeadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baselineG+4 && time.Now().Before(settleDeadline) {
		time.Sleep(50 * time.Millisecond)
	}

	endG := runtime.NumGoroutine()
	sum := ob.Snapshot()
	t.Logf("soak 结束: gets=%d merged=%d owners=%d bytes=%d requests_failed=%d",
		sum.Gets, sum.Merged, sum.Owners, sum.Bytes, failures.Load())
	t.Logf("soak goroutine: baseline=%d end=%d", baselineG, endG)

	if n := failures.Load(); n != 0 {
		errMu.Lock()
		e := firstErr
		errMu.Unlock()
		t.Fatalf("soak 期间 %d 个请求失败，首个: %v", n, e)
	}

	// 最后 3 次采样（含初始采样的短时运行）的 HeapInuse 峰值不得超过首采样的
	// 2 倍 + 32MiB：请求量持续增长时，活堆不应随请求数上升。
	first := samples[0]
	tail := samples
	if len(tail) > 3 {
		tail = tail[len(tail)-3:]
	}
	var maxHeap uint64
	for _, sm := range tail {
		if sm.heapInuse > maxHeap {
			maxHeap = sm.heapInuse
		}
	}
	limit := 2*first.heapInuse + soakHeapGrowthSlack
	t.Logf("soak 堆断言: first=%.1fMiB max_last3=%.1fMiB limit=%.1fMiB",
		float64(first.heapInuse)/(1<<20), float64(maxHeap)/(1<<20), float64(limit)/(1<<20))
	if maxHeap > limit {
		t.Fatalf("HeapInuse 峰值 %.1fMiB 超过上限 %.1fMiB（首采样 %.1fMiB）",
			float64(maxHeap)/(1<<20), float64(limit)/(1<<20), float64(first.heapInuse)/(1<<20))
	}

	if endG > baselineG+4 {
		t.Fatalf("结束时 goroutine = %d，超过基线 %d + 4", endG, baselineG)
	}

	// Registry 只有 InFlight(key string) bool；"全部条目已摘除"等价于 Len()==0。
	// 请求全部读完后条目应已 Finish；给最后一条条目一点摘除时间。
	lenDeadline := time.Now().Add(2 * time.Second)
	for s.registry.Len() != 0 && time.Now().Before(lenDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := s.registry.Len(); got != 0 {
		t.Fatalf("结束时在途条目 = %d, want 0（条目未摘除，可能是缓冲/准入泄漏）", got)
	}
}

// soakObjectBytes 按 key 生成确定性的 1MiB 随机字节。
func soakObjectBytes(key string) []byte {
	var seed int64 = 1
	for i := 0; i < len(key); i++ {
		seed = seed*131 + int64(key[i])
	}
	r := rand.New(rand.NewSource(seed))
	b := make([]byte, soakObjectSize)
	_, _ = r.Read(b)
	return b
}

// soakThrottledReader 按 32KiB 分片返回对象，并在每次读取后短暂停顿，
// 让并发请求有机会挂到同一条目上（不改变字节内容）。
type soakThrottledReader struct {
	data  []byte
	off   int
	delay time.Duration
}

func (r *soakThrottledReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	if len(p) > 32<<10 {
		p = p[:32<<10]
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	return n, nil
}
