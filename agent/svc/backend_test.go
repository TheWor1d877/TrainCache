package svc

// backend_test.go：后端选择器的降级语义（10.1 §4.4 / M2 验收）与 Assemble 默认行为。

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"traincache/agent/nodeid"
	"traincache/core/objectstore"
)

// fakeBackendStore 是可控制写入字节数与返回错误的 Store 假实现。
// writeN > 0 时先写 data[:writeN] 再返回 err，用于验证「已写字节为 0」边界。
type fakeBackendStore struct {
	mu     sync.Mutex
	data   []byte
	writeN int
	err    error

	gets, puts, deletes, etags, putIfMatches, lists int
}

var _ objectstore.Store = (*fakeBackendStore)(nil)

func (f *fakeBackendStore) Get(_ context.Context, _ string, w io.Writer) error {
	f.mu.Lock()
	f.gets++
	data, n, err := f.data, f.writeN, f.err
	f.mu.Unlock()
	if err == nil {
		n = len(data)
	}
	if n > len(data) {
		n = len(data)
	}
	if n > 0 {
		if _, werr := w.Write(data[:n]); werr != nil {
			return werr
		}
	}
	return err
}

func (f *fakeBackendStore) Put(_ context.Context, _ string, _ io.Reader, _ int64) (string, error) {
	f.mu.Lock()
	f.puts++
	f.mu.Unlock()
	return "etag", nil
}

func (f *fakeBackendStore) Delete(_ context.Context, _ string) error {
	f.mu.Lock()
	f.deletes++
	f.mu.Unlock()
	return nil
}

func (f *fakeBackendStore) GetETag(_ context.Context, _ string) (string, error) {
	f.mu.Lock()
	f.etags++
	f.mu.Unlock()
	return "etag", nil
}

func (f *fakeBackendStore) PutIfMatch(_ context.Context, _ string, _ io.Reader, _ int64, _ string) (string, error) {
	f.mu.Lock()
	f.putIfMatches++
	f.mu.Unlock()
	return "etag", nil
}

func (f *fakeBackendStore) List(_ context.Context, _ string) ([]string, error) {
	f.mu.Lock()
	f.lists++
	f.mu.Unlock()
	return nil, nil
}

func (f *fakeBackendStore) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets
}

// TestBackendStoreDisabled：未启用网关时六个方法全部走源站，且不构造状态机。
func TestBackendStoreDisabled(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	s := newBackendStore(origin, nil)
	if s.state != nil {
		t.Fatal("gateway=nil 时 state 应为 nil")
	}

	var buf bytes.Buffer
	if err := s.Get(context.Background(), "ds/blocks/a#0", &buf); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if buf.String() != "origin" {
		t.Errorf("Get 内容 = %q, want origin", buf.String())
	}
	if _, err := s.Put(context.Background(), "k", bytes.NewReader(nil), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(context.Background(), "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.GetETag(context.Background(), "k"); err != nil {
		t.Fatalf("GetETag: %v", err)
	}
	if _, err := s.PutIfMatch(context.Background(), "k", bytes.NewReader(nil), 0, ""); err != nil {
		t.Fatalf("PutIfMatch: %v", err)
	}
	if _, err := s.List(context.Background(), ""); err != nil {
		t.Fatalf("List: %v", err)
	}
	if origin.gets != 1 || origin.puts != 1 || origin.deletes != 1 ||
		origin.etags != 1 || origin.putIfMatches != 1 || origin.lists != 1 {
		t.Fatalf("源站调用次数 = %+v", origin)
	}
}

// TestBackendStoreGatewayOnlyForGet：启用后 Get 走网关，其余五个方法仍走源站。
func TestBackendStoreGatewayOnlyForGet(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	gateway := &fakeBackendStore{data: []byte("gateway")}
	s := newBackendStore(origin, gateway)

	var buf bytes.Buffer
	if err := s.Get(context.Background(), "ds/blocks/a#0", &buf); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if buf.String() != "gateway" {
		t.Errorf("Get 内容 = %q, want gateway", buf.String())
	}
	if gateway.getCount() != 1 || origin.getCount() != 0 {
		t.Fatalf("Get 调用次数 gateway=%d origin=%d, want 1/0", gateway.getCount(), origin.getCount())
	}

	if _, err := s.Put(context.Background(), "k", bytes.NewReader(nil), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(context.Background(), "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.GetETag(context.Background(), "k"); err != nil {
		t.Fatalf("GetETag: %v", err)
	}
	if _, err := s.PutIfMatch(context.Background(), "k", bytes.NewReader(nil), 0, ""); err != nil {
		t.Fatalf("PutIfMatch: %v", err)
	}
	if _, err := s.List(context.Background(), ""); err != nil {
		t.Fatalf("List: %v", err)
	}
	if origin.puts != 1 || origin.deletes != 1 || origin.etags != 1 ||
		origin.putIfMatches != 1 || origin.lists != 1 {
		t.Fatalf("非 Get 方法未全部走源站: %+v", origin)
	}
}

// TestBackendStoreZeroByteFallback：网关 0 字节失败 → 同次改走源站并返回源站结果；
// 状态切直连并上报 reason=unavailable。
func TestBackendStoreZeroByteFallback(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	gateway := &fakeBackendStore{err: objectstore.ErrGatewayUnavailable}
	s := newBackendStore(origin, gateway)

	var modes []int
	var reasons []string
	s.state.setObservers(func(m int) { modes = append(modes, m) }, func(r string) { reasons = append(reasons, r) })

	var buf bytes.Buffer
	if err := s.Get(context.Background(), "ds/blocks/a#0", &buf); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if buf.String() != "origin" {
		t.Errorf("降级后内容 = %q, want origin", buf.String())
	}
	if origin.getCount() != 1 {
		t.Errorf("源站调用次数 = %d, want 1", origin.getCount())
	}
	if s.state.snapshotMode() != 0 {
		t.Error("降级后 mode = 1, want 0")
	}
	if len(reasons) != 1 || reasons[0] != "unavailable" {
		t.Errorf("fallback reasons = %v, want [unavailable]", reasons)
	}
	if len(modes) != 1 || modes[0] != 0 {
		t.Errorf("mode 通知 = %v, want [0]", modes)
	}
}

// TestBackendStorePartialBytesNoFallback：网关已写 3 字节后失败 → 不改走源站，返回原错误。
func TestBackendStorePartialBytesNoFallback(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	gateway := &fakeBackendStore{data: []byte("gateway-bytes"), writeN: 3, err: objectstore.ErrGatewayUnavailable}
	s := newBackendStore(origin, gateway)

	var buf bytes.Buffer
	err := s.Get(context.Background(), "ds/blocks/a#0", &buf)
	if !errors.Is(err, objectstore.ErrGatewayUnavailable) {
		t.Fatalf("Get = %v, want ErrGatewayUnavailable", err)
	}
	if origin.getCount() != 0 {
		t.Errorf("已写字节非 0 时不应改走源站，源站调用 = %d", origin.getCount())
	}
	if buf.String() != "gat" {
		t.Errorf("已写内容 = %q, want gat", buf.String())
	}
}

// TestBackendStoreRejectedNoStateChange：SlowDown 不改状态，但 0 字节时同次改走源站。
func TestBackendStoreRejectedNoStateChange(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	gateway := &fakeBackendStore{err: objectstore.ErrGatewayRejected}
	s := newBackendStore(origin, gateway)

	fallbacks := 0
	s.state.setObservers(nil, func(string) { fallbacks++ })

	var buf bytes.Buffer
	if err := s.Get(context.Background(), "ds/blocks/a#0", &buf); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if buf.String() != "origin" {
		t.Errorf("降级后内容 = %q, want origin", buf.String())
	}
	if s.state.snapshotMode() != 1 {
		t.Error("reject 不应改变 mode，want 1")
	}
	if fallbacks != 0 {
		t.Errorf("reject 不应上报降级，实际 %d 次", fallbacks)
	}
}

// TestBackendStoreNotFoundNoFallback：ErrNotFound 不写状态、不改走源站。
func TestBackendStoreNotFoundNoFallback(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	gateway := &fakeBackendStore{err: objectstore.ErrNotFound}
	s := newBackendStore(origin, gateway)

	err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard)
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	if origin.getCount() != 0 {
		t.Errorf("ErrNotFound 不应改走源站，源站调用 = %d", origin.getCount())
	}
	if s.state.snapshotMode() != 1 {
		t.Error("ErrNotFound 不应改变 mode，want 1")
	}
}

// TestBackendStoreTransportTimeoutReason：连接层超时 → reason=timeout，0 字节改走源站。
func TestBackendStoreTransportTimeoutReason(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	gateway := &fakeBackendStore{err: &objectstore.TransportError{Err: context.DeadlineExceeded}}
	s := newBackendStore(origin, gateway)

	var reasons []string
	s.state.setObservers(nil, func(r string) { reasons = append(reasons, r) })

	var buf bytes.Buffer
	if err := s.Get(context.Background(), "ds/blocks/a#0", &buf); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(reasons) != 1 || reasons[0] != "timeout" {
		t.Errorf("fallback reasons = %v, want [timeout]", reasons)
	}
}

// TestBackendStoreTransportReason：非超时的连接层失败 → reason=transport。
func TestBackendStoreTransportReason(t *testing.T) {
	origin := &fakeBackendStore{data: []byte("origin")}
	gateway := &fakeBackendStore{err: &objectstore.TransportError{Err: errors.New("connection reset")}}
	s := newBackendStore(origin, gateway)

	var reasons []string
	s.state.setObservers(nil, func(r string) { reasons = append(reasons, r) })

	if err := s.Get(context.Background(), "ds/blocks/a#0", io.Discard); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(reasons) != 1 || reasons[0] != "transport" {
		t.Errorf("fallback reasons = %v, want [transport]", reasons)
	}
}

// testAssembleConfig 构造不依赖外部服务的 Assemble 配置（仓库外临时目录）。
func testAssembleConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	return Config{
		NodeName:         nodeid.ID("worker-01"),
		CacheRoot:        filepath.Join(dir, "cache"),
		OrdersDir:        filepath.Join(dir, "orders"),
		Verify:           VerifyXXH3,
		ObjstoreEndpoint: "127.0.0.1:9000",
		ObjstoreBucket:   "traincache",
	}
}

// TestAssembleWithoutGateway：默认配置（未设置 TRAINCACHE_GATEWAY_ENDPOINT）下
// Agent.ObjectStore 就是源站 S3Store，sourceState 为 nil（10.1 §1.5 / M2 验收）。
func TestAssembleWithoutGateway(t *testing.T) {
	cfg := testAssembleConfig(t)
	ag, err := Assemble(cfg)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)

	if ag.sourceState != nil {
		t.Error("未配置网关时 sourceState 应为 nil")
	}
	if _, ok := ag.ObjectStore.(*objectstore.S3Store); !ok {
		t.Errorf("ObjectStore 类型 = %T, want *objectstore.S3Store", ag.ObjectStore)
	}
}

// TestAssembleWithGateway：配置网关后 ObjectStore 是 backendStore，初始 mode=1。
func TestAssembleWithGateway(t *testing.T) {
	cfg := testAssembleConfig(t)
	cfg.GatewayEndpoint = "127.0.0.1:9413"
	ag, err := Assemble(cfg)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	t.Cleanup(ag.Prefetch.Close)

	bs, ok := ag.ObjectStore.(*backendStore)
	if !ok {
		t.Fatalf("ObjectStore 类型 = %T, want *backendStore", ag.ObjectStore)
	}
	if ag.sourceState == nil || ag.sourceState != bs.state {
		t.Fatal("Agent.sourceState 应指向选择器持有的状态机")
	}
	if got := ag.sourceState.snapshotMode(); got != 1 {
		t.Errorf("初始 mode = %d, want 1(配置网关)", got)
	}
}
