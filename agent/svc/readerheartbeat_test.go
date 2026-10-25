package svc

// readerheartbeat_test.go:R2(6.1 §5.10)读者心跳测试:心跳写出 claim;ctx 取消
// 写出 Withdraw claim(空 SnapID + PreviousSnapID);心跳间隔下限。

import (
	"context"
	"testing"
	"time"

	"traincache/core/index"
	"traincache/core/objectstore"
)

// readOneClaim 读取 dataset 下唯一 claim(无 claim 返回 nil)。
func readOneClaim(t *testing.T, store objectstore.Store, dataset string) *index.ReaderClaim {
	t.Helper()
	claims, err := index.ReadReaderClaims(context.Background(), store, dataset)
	if err != nil {
		t.Fatalf("ReadReaderClaims: %v", err)
	}
	if len(claims) == 0 {
		return nil
	}
	if len(claims) != 1 {
		t.Fatalf("claims = %d,期望 1", len(claims))
	}
	return &claims[0]
}

// TestReaderHeartbeatDeclareAndWithdraw:心跳声明当前 snapID;ctx 取消后写空
// SnapID + PreviousSnapID 的撤销 claim。
func TestReaderHeartbeatDeclareAndWithdraw(t *testing.T) {
	store := objectstore.NewMock()
	const ds = "hb"
	const node = "worker-01"
	reg := index.NewReaderRegistry(store, node, 2*time.Second)

	hb := &readerHeartbeat{
		reg:      reg,
		interval: 5 * time.Millisecond,
		snapshots: func() []datasetSnapshot {
			return []datasetSnapshot{{dataset: ds, snapID: "snap-1"}}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { hb.run(ctx); close(done) }()

	waitFor(t, "心跳写出 claim", func() bool {
		c := readOneClaim(t, store, ds)
		return c != nil && c.NodeID == node && c.SnapID == "snap-1"
	})

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("心跳未随 ctx 取消退出")
	}

	c := readOneClaim(t, store, ds)
	if c == nil {
		t.Fatalf("撤销后 claim 缺失(期望空声明)")
	}
	if c.SnapID != "" || c.PreviousSnapID != "snap-1" {
		t.Fatalf("撤销 claim = {snap:%q prev:%q},期望 {\"\",\"snap-1\"}", c.SnapID, c.PreviousSnapID)
	}
}

// TestHeartbeatInterval:间隔 = 刷新周期/2,下限 1s。
func TestHeartbeatInterval(t *testing.T) {
	reg := index.NewReaderRegistry(objectstore.NewMock(), "n", time.Second)
	cases := []struct {
		refresh time.Duration
		want    time.Duration
	}{
		{10 * time.Second, 5 * time.Second},
		{100 * time.Millisecond, heartbeatMinInterval},
	}
	for _, tc := range cases {
		hb := newReaderHeartbeat(reg, tc.refresh, func() []datasetSnapshot { return nil })
		if hb.interval != tc.want {
			t.Fatalf("refresh=%v interval=%v,期望 %v", tc.refresh, hb.interval, tc.want)
		}
	}
}
