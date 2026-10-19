package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"traincache/core/objectstore"
)

// readClaimObject 读取并解码 dataset 下某节点的 claim 对象。
func readClaimObject(t *testing.T, store objectstore.Store, dataset, nodeID string) ReaderClaim {
	t.Helper()
	data, err := getObject(context.Background(), store, ReaderKey(dataset, nodeID))
	if err != nil {
		t.Fatalf("read claim %q: %v", ReaderKey(dataset, nodeID), err)
	}
	var claim ReaderClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		t.Fatalf("decode claim: %v", err)
	}
	return claim
}

// TestReaderKeyEscapesNodeID：nodeID 经 url.PathEscape，"/" 等字符不破坏键布局。
func TestReaderKeyEscapesNodeID(t *testing.T) {
	got := ReaderKey("ds", "node/1")
	want := "dataset/ds/readers/" + url.PathEscape("node/1") + ".json"
	if got != want {
		t.Fatalf("ReaderKey = %q, want %q", got, want)
	}
	if want != "dataset/ds/readers/node%2F1.json" {
		t.Fatalf("url.PathEscape 语义变化: %q", want)
	}
}

// TestReaderRegistryDeclareSwitchAndExpiry：Declare 幂等；切换快照时写入 previous 与
// PreviousUntil；同一快照重复声明保留未过期的 previous，过期后清空。
func TestReaderRegistryDeclareSwitchAndExpiry(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMock()
	const dataset = "ds"
	base := time.Unix(1_700_000_000, 0)
	now := base
	reg := NewReaderRegistry(store, "node-a", 10*time.Second)
	reg.now = func() time.Time { return now }

	// 首次声明：previous 为空，PreviousUntil = now+grace。
	if err := reg.Declare(ctx, dataset, "s1"); err != nil {
		t.Fatalf("Declare(s1): %v", err)
	}
	c := readClaimObject(t, store, dataset, "node-a")
	if c.NodeID != "node-a" || c.SnapID != "s1" || c.PreviousSnapID != "" {
		t.Fatalf("首次 claim = %+v", c)
	}
	if !c.UpdatedAt.Equal(base) || !c.PreviousUntil.Equal(base.Add(10*time.Second)) {
		t.Fatalf("首次时间字段 = %+v", c)
	}

	// 切换快照：previous = s1，PreviousUntil = now+grace。
	now = base.Add(time.Second)
	if err := reg.Declare(ctx, dataset, "s2"); err != nil {
		t.Fatalf("Declare(s2): %v", err)
	}
	c = readClaimObject(t, store, dataset, "node-a")
	if c.SnapID != "s2" || c.PreviousSnapID != "s1" {
		t.Fatalf("切换后 claim = %+v", c)
	}
	wantUntil := now.Add(10 * time.Second)
	if !c.PreviousUntil.Equal(wantUntil) {
		t.Fatalf("PreviousUntil = %v, want %v", c.PreviousUntil, wantUntil)
	}

	// 同一快照重复声明：previous/PreviousUntil 保留至过期。
	now = base.Add(2 * time.Second)
	if err := reg.Declare(ctx, dataset, "s2"); err != nil {
		t.Fatalf("re-Declare(s2): %v", err)
	}
	c = readClaimObject(t, store, dataset, "node-a")
	if c.PreviousSnapID != "s1" || !c.PreviousUntil.Equal(wantUntil) {
		t.Fatalf("重复声明后 claim = %+v", c)
	}
	if !c.UpdatedAt.Equal(now) {
		t.Fatalf("UpdatedAt = %v, want %v", c.UpdatedAt, now)
	}

	// 过期后再声明同一快照：previous 清空。
	now = base.Add(20 * time.Second)
	if err := reg.Declare(ctx, dataset, "s2"); err != nil {
		t.Fatalf("re-Declare(s2) after expiry: %v", err)
	}
	c = readClaimObject(t, store, dataset, "node-a")
	if c.PreviousSnapID != "" || !c.PreviousUntil.IsZero() {
		t.Fatalf("过期后 claim = %+v", c)
	}
}

// TestReaderRegistryWithdraw：写入空 snap_id、last 作为 previous、宽限期与时间戳；
// 写入失败仍返回 nil；ctx 取消返回 ctx.Err()。
func TestReaderRegistryWithdraw(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMock()
	const dataset = "ds"
	base := time.Unix(1_700_000_000, 0)
	now := base
	reg := NewReaderRegistry(store, "node-a", 5*time.Second)
	reg.now = func() time.Time { return now }
	if err := reg.Declare(ctx, dataset, "s1"); err != nil {
		t.Fatalf("Declare: %v", err)
	}

	now = base.Add(time.Second)
	if err := reg.Withdraw(ctx, dataset); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	c := readClaimObject(t, store, dataset, "node-a")
	if c.SnapID != "" || c.PreviousSnapID != "s1" {
		t.Fatalf("Withdraw claim = %+v", c)
	}
	if !c.UpdatedAt.Equal(now) || !c.PreviousUntil.Equal(now.Add(5*time.Second)) {
		t.Fatalf("Withdraw 时间字段 = %+v", c)
	}

	// best-effort：写入失败不阻止撤销。
	failing := &failingPutStore{Store: store}
	reg2 := NewReaderRegistry(failing, "node-b", time.Second)
	if err := reg2.Withdraw(ctx, dataset); err != nil {
		t.Fatalf("Withdraw 写失败 = %v, want nil", err)
	}

	// ctx 取消返回 ctx.Err()。
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := reg.Withdraw(canceled, dataset); !errors.Is(err, context.Canceled) {
		t.Fatalf("Withdraw(canceled) = %v, want context.Canceled", err)
	}
}

// failingPutStore 让 Put 恒失败，用于验证 best-effort 语义。
type failingPutStore struct {
	objectstore.Store
}

func (*failingPutStore) Put(context.Context, string, io.Reader, int64) (string, error) {
	return "", errors.New("put failed")
}

// TestReadReaderClaims：空集合、多 claim、JSON 损坏报错。
func TestReadReaderClaims(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMock()
	const dataset = "ds"

	claims, err := ReadReaderClaims(ctx, store, dataset)
	if err != nil {
		t.Fatalf("空目录 ReadReaderClaims: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("空目录 claims = %d, want 0", len(claims))
	}

	for _, node := range []string{"n1", "n2"} {
		data, _ := json.Marshal(ReaderClaim{NodeID: node, SnapID: "s1", UpdatedAt: time.Unix(1, 0)})
		if _, err := store.Put(ctx, ReaderKey(dataset, node), bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("Put claim %s: %v", node, err)
		}
	}
	claims, err = ReadReaderClaims(ctx, store, dataset)
	if err != nil {
		t.Fatalf("ReadReaderClaims: %v", err)
	}
	if len(claims) != 2 {
		t.Fatalf("claims = %d, want 2", len(claims))
	}

	bad := []byte("{not-json")
	if _, err := store.Put(ctx, ReaderKey(dataset, "bad"), bytes.NewReader(bad), int64(len(bad))); err != nil {
		t.Fatalf("Put corrupt claim: %v", err)
	}
	if _, err := ReadReaderClaims(ctx, store, dataset); err == nil {
		t.Fatal("损坏 claim 未返回错误")
	}
}

// TestReaderRegistryConcurrent：并发 Declare/Withdraw 无数据竞争。
func TestReaderRegistryConcurrent(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMock()
	reg := NewReaderRegistry(store, "node-a", time.Second)
	reg.now = func() time.Time { return time.Unix(1_700_000_000, 0) }

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if i%2 == 0 {
					_ = reg.Declare(ctx, "ds", "s1")
				} else {
					_ = reg.Withdraw(ctx, "ds")
				}
			}
		}(i)
	}
	wg.Wait()
}
