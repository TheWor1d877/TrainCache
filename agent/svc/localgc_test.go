package svc

// localgc_test.go:节点级本地副本回收循环(svc/localgc.go)的接线测试。
// 装置与 wiring_test 同款:assembleWith(注入 store, 临时目录 CAS);断言覆盖
// 周期循环起停、枚举失败整轮放弃、正常轮误删保护与指标、StartBackground 接线。

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// recordingStore 在 Mock 之上记录 List/Get 调用次数,并支持注入 List 失败:
// List 计数证明"本轮确实跑了枚举",Get 计数证明枚举失败时 LocalGC.Sweep 未被调用
// (Sweep 第一步就读索引对象;svc 循环之外没有别的 Get 来源)。
type recordingStore struct {
	*objectstore.Mock
	listErr error // 非 nil → 对象存储枚举失败
	lists   atomic.Int64
	gets    atomic.Int64
}

func (s *recordingStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.lists.Add(1)
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.Mock.List(ctx, prefix)
}

func (s *recordingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.gets.Add(1)
	return s.Mock.Get(ctx, key, w)
}

// newLocalGCAgent 与 newWiringAgent 同构,仅把 store 换成注入点(记录调用的假 store)。
func newLocalGCAgent(t *testing.T, store objectstore.Store) (*Agent, Config) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		NodeName:  nodeid.ID("worker-01"),
		CacheRoot: filepath.Join(dir, "cache"),
		OrdersDir: filepath.Join(dir, "orders"),
		Verify:    VerifyXXH3,
	}
	ag, err := assembleWith(store, cfg)
	if err != nil {
		t.Fatalf("assembleWith: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)
	return ag, cfg
}

// localGCBlocksRemovedOther 读 gc_blocks_removed_total{dataset="other"}(节点级回收
// 不归属数据集,空串标签经白名单归 other)。
func localGCBlocksRemovedOther(ag *Agent) float64 {
	return testutil.ToFloat64(ag.Metrics.GCBlocksRemoved().WithLabelValues("other", metrics.ResultSuccess))
}

// TestLocalGCPeriodicLoopStartAndStop:循环按 tick 起轮、ctx 取消后立即退出
// (done 通道关闭即退出证明;退出后枚举计数冻结)。
func TestLocalGCPeriodicLoopStartAndStop(t *testing.T) {
	spy := &recordingStore{Mock: objectstore.NewMock()}
	ag, _ := newLocalGCAgent(t, spy)
	gc := index.NewLocalGC(spy, ag.CS, index.Config{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ag.runLocalGC(ctx, gc, 10*time.Millisecond)
	}()
	waitFor(t, "循环按 tick 驱动清扫轮", func() bool { return spy.lists.Load() > 0 })

	cancel()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("ctx 取消后 runLocalGC 未退出")
	}
	// 循环已退出(单 goroutine 驱动计数),再等若干周期确认计数不再增长。
	base := spy.lists.Load()
	time.Sleep(50 * time.Millisecond)
	if got := spy.lists.Load(); got != base {
		t.Errorf("ctx 取消后仍在起轮: lists %d → %d", base, got)
	}
}

// TestLocalGCStartBackgroundHook:循环随 StartBackground 在 signal ctx 下启动。
func TestLocalGCStartBackgroundHook(t *testing.T) {
	spy := &recordingStore{Mock: objectstore.NewMock()}
	ag, _ := newLocalGCAgent(t, spy)
	ag.Cfg.LocalGCInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ag.StartBackground(ctx)
	waitFor(t, "StartBackground 启动本地副本清扫循环", func() bool { return spy.lists.Load() > 0 })
}

// TestLocalGCSkipsRoundOnEnumerateFailure:枚举失败整轮放弃——LocalGC.Sweep 不得
// 拿着子集跑(它会把它看不到的数据集的副本判成孤儿删掉)。
func TestLocalGCSkipsRoundOnEnumerateFailure(t *testing.T) {
	spy := &recordingStore{Mock: objectstore.NewMock()}
	ag, cfg := newLocalGCAgent(t, spy)
	ctx := context.Background()

	// 一块本地存在、不在任何快照里的副本;误跑 Sweep(数据集集为空)会直接删掉它。
	orphanFP, _, err := ag.CS.Put(ctx, strings.NewReader("orphan-copy"))
	if err != nil {
		t.Fatalf("cas.Put: %v", err)
	}
	spy.listErr = errors.New("objectstore unavailable")

	gc := index.NewLocalGC(spy, ag.CS, index.Config{})
	getsBefore := spy.gets.Load()
	ag.sweepLocalBlocks(ctx, gc)

	if spy.lists.Load() == 0 {
		t.Fatal("本轮未执行数据集枚举")
	}
	if got := spy.gets.Load(); got != getsBefore {
		t.Errorf("枚举失败时不得调用 LocalGC.Sweep(索引读取次数 %d → %d)", getsBefore, got)
	}
	if _, err := os.Stat(wiringBlockFile(cfg, orphanFP)); errors.Is(err, os.ErrNotExist) {
		t.Error("枚举失败本轮孤儿副本被删除(整轮放弃未生效)")
	}
	if got := localGCBlocksRemovedOther(ag); got != 0 {
		t.Errorf("gc_blocks_removed = %v, want 0(放弃轮不打点)", got)
	}
}

// TestLocalGCNormalRoundEvictsOrphanBlock:端到端一轮(临时目录真实 cas.Store)——
// 快照按 §11.4 提交为不含 gone.tar 后,该条目的本地副本在本轮被回收,块文件确已删除;
// 快照引用的副本保留;删除数计入 gc_blocks_removed_total{other}。
func TestLocalGCNormalRoundEvictsOrphanBlock(t *testing.T) {
	spy := &recordingStore{Mock: objectstore.NewMock()}
	ag, cfg := newLocalGCAgent(t, spy)
	ctx := context.Background()

	fps := commitWiringBlocks(t, ag, wiringDataset,
		wiringBlock{path: "keep.tar", content: "keep"},
		wiringBlock{path: "gone.tar", content: "gone"},
	)
	keepFP, goneFP := fps["keep.tar#0"], fps["gone.tar#0"]

	// 删除语义落点:提交不含 gone.tar 的新快照(CommitChanges dels,与 traincacheds rm 同款)。
	_, err := index.NewWriter(spy, ag.CS, index.Config{}).CommitChanges(ctx, wiringDataset, nil,
		[]index.Delete{{Path: "gone.tar", Chunk: 0}})
	if err != nil {
		t.Fatalf("index CommitChanges: %v", err)
	}

	gc := index.NewLocalGC(spy, ag.CS, index.Config{})
	ag.sweepLocalBlocks(ctx, gc)

	if _, err := ag.CS.Open(goneFP); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("gone.tar 本地副本应被回收: Open err = %v", err)
	}
	if _, err := os.Stat(wiringBlockFile(cfg, goneFP)); !os.IsNotExist(err) {
		t.Errorf("gone.tar 块文件应已删除: Stat err = %v", err)
	}
	if br, err := ag.CS.Open(keepFP); err != nil {
		t.Errorf("keep.tar 快照引用副本不应被删: %v", err)
	} else {
		_ = br.Close()
	}
	if got := localGCBlocksRemovedOther(ag); got != 1 {
		t.Errorf("gc_blocks_removed{other} = %v, want 1", got)
	}
}
