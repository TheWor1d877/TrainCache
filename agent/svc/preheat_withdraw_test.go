package svc

// preheat_withdraw_test.go:D62 取消即撤回。
//
// 宽预热的落选节点被 Cancel 时,窗口里多数块并不在预取队列里——调度协程出队不设额度
// 门,瞬间就把它们交给抓取 goroutine;真正的积压在 core fetch 的准入排队 chan 上
// (MaxTasks=8、后台占半)。所以撤回的落点是任务级下载作用域:Cancel 取消它,卡在
// 门口的块在 ctx 边界退出,一次传输都不该发生。
//
// 证伪:去掉 Cancel 里的 tk.cancelDownloads(),6 个等待者会在闸门打开后照常下载,
// arrived 停在 2 的断言直接变红(变成 8)。

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "traincache/agent/api/agentpb"
	"traincache/core/objectstore"
)

// gateStore 给数据块下载设一道闸:进到 Get 的块先记账,再等闸门打开或 ctx 取消。
// 还没过 core fetch 准入的块不会出现在这里,所以「进了几次 Get」就是实际传输次数。
type gateStore struct {
	*objectstore.Mock
	mu      sync.Mutex
	arrived int
	release chan struct{}
}

func (s *gateStore) Get(ctx context.Context, key string, w io.Writer) error {
	if strings.HasPrefix(key, testDataset+"/blocks/") {
		s.mu.Lock()
		s.arrived++
		s.mu.Unlock()
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Mock.Get(ctx, key, w)
}

// count 返回已到达闸口的块数。
func (s *gateStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.arrived
}

var _ objectstore.Store = (*gateStore)(nil)

// waitGateArrivals 有界轮询直到 n 个块到达闸口(不靠裸 sleep 定时序)。
func waitGateArrivals(t *testing.T, gate *gateStore, n int) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for gate.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("到达下载闸口的块数 = %d,想要 %d", gate.count(), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestPreheatCancelWithdrawsAdmissionWaiters:8 块窗口只有 2 块能进后台额度,其余 6 块
// 卡在准入门外;Cancel 之后 8 块全部退出(闸口仍只见过 2 块),任务落 ERROR、
// completed=0/failed=8、canceled=true。闸门到最后才打开,所以「2 块中止」是确定的。
func TestPreheatCancelWithdrawsAdmissionWaiters(t *testing.T) {
	gate := &gateStore{Mock: objectstore.NewMock(), release: make(chan struct{})}
	fx := newSvcFixtureWithStore(t, gate)

	specs := make([]svcBlockSpec, 0, 8)
	chunks := make([]*agentpb.ChunkRef, 0, 8)
	for i := 0; i < 8; i++ {
		path := "withdraw-" + string(rune('a'+i)) + ".tar"
		specs = append(specs, svcBlockSpec{path: path, content: "block-" + path, source: true})
		chunks = append(chunks, m3Chunk(path, 0))
	}
	fx.commitDataset(t, testDataset, specs)
	ctx := context.Background()

	if _, err := fx.tasks.Submit(ctx, m3Submit("t-withdraw", 3, chunks...)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitGateArrivals(t, gate, 2)
	time.Sleep(backfillQuietWindow) // 让可能漏网的等待者也走完一轮
	if n := gate.count(); n != 2 {
		t.Fatalf("闸口块数 = %d,想要 2(后台额度 = MaxTasks/2,其余应卡在准入门外)", n)
	}

	if _, err := fx.tasks.Cancel(ctx, m3Cancel("t-withdraw", 3)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	st := waitTaskState(t, fx.tasks, "t-withdraw", agentpb.PreheatStatus_ERROR)
	if !st.GetCanceled() {
		t.Fatal("canceled = false, want true")
	}
	if completed, failed := st.GetCompletedBlocks(), st.GetFailedBlocks(); completed != 0 || failed != 8 {
		t.Fatalf("completed=%d failed=%d, want 0/8(8 块都随作用域取消退出,含闸口内那 2 块)", completed, failed)
	}

	// 闸门打开:没有撤回的话,这里的等待者会照常排进来把整窗下完。
	close(gate.release)
	time.Sleep(backfillQuietWindow)
	if got := gate.count(); got != 2 {
		t.Fatalf("取消后又有 %d 块发起下载(只想要取消前已到闸口的 2 块)——撤回没落到准入排队上", got)
	}
}

// TestPreheatCancelReleasesDownloadScope:被撤的块不再占 P0 在途硬保护(§7.6),
// 任务出表(TTL/drop)也不留作用域。
func TestPreheatCancelReleasesDownloadScope(t *testing.T) {
	gate := &gateStore{Mock: objectstore.NewMock(), release: make(chan struct{})}
	fx := newSvcFixtureWithStore(t, gate)
	specs := []svcBlockSpec{
		{path: "scope-a.tar", content: "a", source: true},
		{path: "scope-b.tar", content: "b", source: true},
	}
	fx.commitDataset(t, testDataset, specs)
	ctx := context.Background()

	if _, err := fx.tasks.Submit(ctx, m3Submit("t-scope", 1,
		m3Chunk("scope-a.tar", 0), m3Chunk("scope-b.tar", 0))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitGateArrivals(t, gate, 1)
	if n := len(fx.pm.p0Refs()); n != 2 {
		t.Fatalf("P0 在途保护 = %d,想要 2(两块都已入队)", n)
	}

	if _, err := fx.tasks.Cancel(ctx, m3Cancel("t-scope", 1)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	close(gate.release)
	waitTaskState(t, fx.tasks, "t-scope", agentpb.PreheatStatus_ERROR)
	// 完成回调逐块解除保护;任务到终态时两块都已出表。
	if n := len(fx.pm.p0Refs()); n != 0 {
		t.Fatalf("P0 在途保护残留 %d 项,想要 0(否则落选窗口永久挡住淘汰)", n)
	}
}
