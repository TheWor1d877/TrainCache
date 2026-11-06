package warmup

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	traincachev1alpha1 "traincache/controller/api/v1alpha1"
	"traincache/controller/internal/agentclient"
	"traincache/controller/internal/gate"
)

func TestHasMinProgress(t *testing.T) {
	cases := []struct {
		name string
		pods []gate.PodGateState
		want bool
	}{
		{
			name: "all zero ready",
			pods: []gate.PodGateState{
				{Index: 0, AssignedNodes: []string{"n1"}, ReadyByNode: map[string]int32{"n1": 0}},
			},
			want: false,
		},
		{
			name: "first pod has ready > 0",
			pods: []gate.PodGateState{
				{Index: 0, AssignedNodes: []string{"n1"}, ReadyByNode: map[string]int32{"n1": 5}},
			},
			want: true,
		},
		{
			name: "second pod has ready > 0",
			pods: []gate.PodGateState{
				{Index: 0, AssignedNodes: []string{"n1"}, ReadyByNode: map[string]int32{"n1": 0}},
				{Index: 1, AssignedNodes: []string{"n2"}, ReadyByNode: map[string]int32{"n2": 1}},
			},
			want: true,
		},
		{
			name: "no assigned nodes",
			pods: []gate.PodGateState{
				{Index: 0, AssignedNodes: nil, ReadyByNode: nil},
			},
			want: false,
		},
		{
			name: "empty pods",
			pods: nil,
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := hasMinProgress(c.pods)
			if got != c.want {
				t.Errorf("hasMinProgress = %v, want %v", got, c.want)
			}
		})
	}
}

func TestShouldSkipGate(t *testing.T) {
	const GiB = int64(1024 * 1024 * 1024)

	cases := []struct {
		name                        string
		totalBytes                  int64
		cacheCapacityBytes          int64
		observedThroughput          float64
		networkBandwidthBytesPerSec int64
		want                        bool
		wantReason                  string
	}{
		// —— 条件 1: 数据集太小 ——
		{
			name:                        "small dataset skips",
			totalBytes:                  100 * 1024 * 1024, // 100MiB
			cacheCapacityBytes:          10 * GiB,
			observedThroughput:          0,
			networkBandwidthBytesPerSec: 0,
			want:                        true, // 100MiB < 10GiB/20=512MiB
			wantReason:                  skipReasonDatasetTooSmall,
		},
		{
			name:                        "dataset too big for small condition",
			totalBytes:                  1 * GiB,
			cacheCapacityBytes:          10 * GiB,
			observedThroughput:          0,
			networkBandwidthBytesPerSec: 0,
			want:                        false, // 1GiB > 512MiB
		},
		{
			name:                        "cacheCapacity 0 disables small condition",
			totalBytes:                  1 * GiB,
			cacheCapacityBytes:          0,
			observedThroughput:          0,
			networkBandwidthBytesPerSec: 0,
			want:                        false,
		},
		{
			name:                        "totalBytes 0 disables small condition",
			totalBytes:                  0,
			cacheCapacityBytes:          10 * GiB,
			observedThroughput:          0,
			networkBandwidthBytesPerSec: 0,
			want:                        false,
		},

		// —— 条件 2: 带宽够快 ——
		{
			name:                        "bandwidth fast enough skips",
			totalBytes:                  1 * GiB, // < 5GiB 阈值
			cacheCapacityBytes:          0,
			observedThroughput:          900 * 1024 * 1024,  // 900 MiB/s
			networkBandwidthBytesPerSec: 1000 * 1024 * 1024, // 1000 MiB/s
			want:                        true,               // 900 > 0.8*1000=800
			wantReason:                  skipReasonBandwidthFastEnough,
		},
		{
			name:                        "bandwidth just below threshold",
			totalBytes:                  1 * GiB,
			cacheCapacityBytes:          0,
			observedThroughput:          799 * 1024 * 1024,
			networkBandwidthBytesPerSec: 1000 * 1024 * 1024,
			want:                        false, // 799 < 800
		},
		{
			name:                        "network bandwidth 0 disables condition 2",
			totalBytes:                  1 * GiB,
			cacheCapacityBytes:          0,
			observedThroughput:          900 * 1024 * 1024,
			networkBandwidthBytesPerSec: 0,
			want:                        false,
		},
		{
			name:                        "observed throughput 0 disables condition 2",
			totalBytes:                  1 * GiB,
			cacheCapacityBytes:          0,
			observedThroughput:          0,
			networkBandwidthBytesPerSec: 1000 * 1024 * 1024,
			want:                        false,
		},
		{
			name:                        "dataset too big for bandwidth condition",
			totalBytes:                  10 * GiB, // > 5GiB 阈值
			cacheCapacityBytes:          0,
			observedThroughput:          900 * 1024 * 1024,
			networkBandwidthBytesPerSec: 1000 * 1024 * 1024,
			want:                        false,
		},

		// —— 两个条件都满足 ——
		{
			name:                        "both conditions trigger",
			totalBytes:                  100 * 1024 * 1024,
			cacheCapacityBytes:          10 * GiB,
			observedThroughput:          900 * 1024 * 1024,
			networkBandwidthBytesPerSec: 1000 * 1024 * 1024,
			want:                        true,
			wantReason:                  skipReasonDatasetTooSmall,
		},

		// —— 条件 1 命中、但条件 2 的数据集前提不成立 ——
		{
			name:                        "large dataset with huge cache keeps dataset_too_small",
			totalBytes:                  6 * GiB,   // > 5GiB，条件 2 的数据集前提不成立
			cacheCapacityBytes:          200 * GiB, // 200GiB/20=10GiB > 6GiB
			observedThroughput:          900 * 1024 * 1024,
			networkBandwidthBytesPerSec: 1000 * 1024 * 1024,
			want:                        true,
			wantReason:                  skipReasonDatasetTooSmall,
		},

		// —— 两个条件都不满足 ——
		{
			name:                        "neither condition",
			totalBytes:                  10 * GiB,
			cacheCapacityBytes:          10 * GiB,
			observedThroughput:          100 * 1024 * 1024,
			networkBandwidthBytesPerSec: 1000 * 1024 * 1024,
			want:                        false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reason, got := shouldSkipGate(c.totalBytes, c.cacheCapacityBytes,
				c.observedThroughput, c.networkBandwidthBytesPerSec)
			if got != c.want {
				t.Errorf("shouldSkipGate() = %v, want %v", got, c.want)
			}
			if reason != c.wantReason {
				t.Errorf("shouldSkipGate() reason = %q, want %q", reason, c.wantReason)
			}
		})
	}
}

// metricsScrapeServer 起一个假的 agent /metrics 端点：记录抓取次数，
// 每次抓取返回一个递增的 traincache_fetch_bytes_total，模拟回源字节持续增长。
func metricsScrapeServer(t *testing.T) (*atomic.Int64, *agentclient.MetricsFetcher) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "traincache_fetch_bytes_total{result=\"success\",reason=\"prefetch\"} %d\n", n*1000)
	}))
	t.Cleanup(srv.Close)
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	fetcher := &agentclient.MetricsFetcher{
		Resolver: agentclient.StaticResolver{"n1": "127.0.0.1:9410"},
		Port:     port,
	}
	return &calls, fetcher
}

// gateSkipLayout 造一个单分片布局，lastChunkSize 决定 dataSetTotalBytes 的结果。
func gateSkipLayout(lastChunkSize int64) []traincachev1alpha1.BlockLayout {
	return []traincachev1alpha1.BlockLayout{
		{Shard: "s000.tar", StartChunk: 0, Count: 1, LastChunkSize: lastChunkSize},
	}
}

// 门控 Released 后不再抓 agent /metrics：抓取只在 Hold 时发生。
func TestGateReleasedDoesNotScrapeAgentMetrics(t *testing.T) {
	task := fixtureTask(1, 8, [][]int32{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}})
	ds := fixtureDataSet(10, singleShardLayout(10), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1",
		progressResponse{status: bitmapResponse(3, 1, 8, []byte{0x00})},
		progressResponse{status: bitmapResponse(3, 2, 8, []byte{0b00111111})},
	)
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	scrapes, fetcher := metricsScrapeServer(t)
	r := &Reconciler{
		Client:                c,
		Agents:                pool,
		MetricsFetcher:        fetcher,
		NetworkBandwidthBytes: 1000 * 1024 * 1024,
	}

	// 第一轮 ready=0：门控 Hold，抓一次 /metrics 建立采样基线。
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if got := scrapes.Load(); got != 1 {
		t.Fatalf("scrapes after the hold round = %d, want 1", got)
	}
	if state := readyTask(t, c).Status.GateDecision.State; state != "" {
		t.Fatalf("gate state after the hold round = %q, want no decision yet", state)
	}

	// 第二轮 ready=6：门控 Released，这一轮不再发起 /metrics 请求。
	result, err := reconcileResult(t, r)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0 (gate released)", result.RequeueAfter)
	}
	if got := scrapes.Load(); got != 1 {
		t.Fatalf("scrapes after the release round = %d, want 1 (released rounds do not scrape)", got)
	}
}

// 数据集太小命中条件 1：日志与 status 的 reason 都是 dataset_too_small。
func TestGateSkipReasonDatasetTooSmall(t *testing.T) {
	const GiB = int64(1024 * 1024 * 1024)
	task := fixtureTask(1, 1, [][]int32{{0}})
	ds := fixtureDataSet(1, gateSkipLayout(1024), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, 1, []byte{0x00})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	scrapes, fetcher := metricsScrapeServer(t)
	var buf bytes.Buffer
	r := &Reconciler{
		Client:                c,
		Agents:                pool,
		MetricsFetcher:        fetcher,
		Log:                   slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		CacheCapacityBytes:    10 * GiB,
		NetworkBandwidthBytes: 1000 * 1024 * 1024,
	}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	if updated.Status.GateDecision.State != traincachev1alpha1.GateStateReleased {
		t.Fatalf("gate state = %q, want Released", updated.Status.GateDecision.State)
	}
	if updated.Status.GateDecision.Reason != skipReasonDatasetTooSmall {
		t.Fatalf("gate reason = %q, want %q", updated.Status.GateDecision.Reason, skipReasonDatasetTooSmall)
	}
	if !strings.Contains(buf.String(), `"reason":"`+skipReasonDatasetTooSmall+`"`) {
		t.Fatalf("missing skip reason in the log: %s", buf.String())
	}
	if got := scrapes.Load(); got != 1 {
		t.Fatalf("scrapes = %d, want 1", got)
	}
}

// 数据集够大、吞吐够快命中条件 2：日志与 status 的 reason 都是 bandwidth_fast_enough。
func TestGateSkipReasonBandwidthFastEnough(t *testing.T) {
	const GiB = int64(1024 * 1024 * 1024)
	task := fixtureTask(1, 1, [][]int32{{0}})
	// 1GiB 数据集：大于 cacheCapacity/20（512MiB），条件 1 不命中；小于 5GiB，条件 2 可判定。
	ds := fixtureDataSet(1, gateSkipLayout(GiB), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, 1, []byte{0x00})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	scrapes, fetcher := metricsScrapeServer(t)
	var buf bytes.Buffer
	now := time.Unix(1000, 0)
	r := &Reconciler{
		Client:                c,
		Agents:                pool,
		MetricsFetcher:        fetcher,
		Log:                   slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		CacheCapacityBytes:    10 * GiB,
		NetworkBandwidthBytes: 1000,
		Now:                   func() time.Time { return now },
	}

	// 第一轮只有采样基线，吞吐为 0，门控继续 Hold。
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if got := scrapes.Load(); got != 1 {
		t.Fatalf("scrapes after the first round = %d, want 1", got)
	}

	// 第二轮回源字节继续增长：1000 B/s > 0.8*1000 B/s，条件 2 命中。
	now = now.Add(time.Second)
	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	if updated.Status.GateDecision.State != traincachev1alpha1.GateStateReleased {
		t.Fatalf("gate state = %q, want Released", updated.Status.GateDecision.State)
	}
	if updated.Status.GateDecision.Reason != skipReasonBandwidthFastEnough {
		t.Fatalf("gate reason = %q, want %q", updated.Status.GateDecision.Reason, skipReasonBandwidthFastEnough)
	}
	if !strings.Contains(buf.String(), `"reason":"`+skipReasonBandwidthFastEnough+`"`) {
		t.Fatalf("missing skip reason in the log: %s", buf.String())
	}
}

// 数据集超过 5GiB 时条件 2 不成立：即使缓存足够大，reason 仍是 dataset_too_small。
func TestGateSkipReasonLargeDatasetIsNotBandwidthFastEnough(t *testing.T) {
	const GiB = int64(1024 * 1024 * 1024)
	task := fixtureTask(1, 1, [][]int32{{0}})
	ds := fixtureDataSet(1, gateSkipLayout(6*GiB), []string{"n1"})
	pool := newFakeProgressPool()
	pool.set("train/t1/p0", "n1", progressResponse{status: bitmapResponse(3, 1, 1, []byte{0x00})})
	c := fixtureClient(t, task, ds, fixtureNode("n1"), fixtureAgentPod("n1"), fixtureTargetPod(0))
	scrapes, fetcher := metricsScrapeServer(t)
	r := &Reconciler{
		Client:                c,
		Agents:                pool,
		MetricsFetcher:        fetcher,
		CacheCapacityBytes:    200 * GiB, // 200GiB/20=10GiB > 6GiB，条件 1 命中
		NetworkBandwidthBytes: 1000,
	}

	if _, err := reconcileResult(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	updated := readyTask(t, c)
	if updated.Status.GateDecision.State != traincachev1alpha1.GateStateReleased {
		t.Fatalf("gate state = %q, want Released", updated.Status.GateDecision.State)
	}
	if updated.Status.GateDecision.Reason != skipReasonDatasetTooSmall {
		t.Fatalf("gate reason = %q, want %q", updated.Status.GateDecision.Reason, skipReasonDatasetTooSmall)
	}
	if got := scrapes.Load(); got != 1 {
		t.Fatalf("scrapes = %d, want 1", got)
	}
}
