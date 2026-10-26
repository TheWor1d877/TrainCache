package svc

// 本文件是 M3 阶段 D 的 svc 集成测试(7.0 验收 1/5/8/9;docs/6.2 plan.md §5 任务 5):
// 在 svc 装配层把真实 core 组合(objectstore.Mock 的计数/门闩包装 + 临时 CAS +
// fetch.Fetcher + index.Writer/Catalog)与 svc 组件(OrderDiscovery/HandleOrder/
// PrefetchManager/PreheatTasks/Server)端到端串起来,覆盖:
//
//	验收 1(TestM3Acceptance1PreheatRPCProgress):bufconn 全链路上 SubmitPreheat →
//	  GetPreheatStatus 轮询:下载门闩期观察到 RUNNING(0/3),放行后 DONE(3/3);
//	验收 5(TestM3Acceptance5ScopedPreheatDownloads):路径预热只加载所需索引分片、
//	  指定块预热只下载选中块(计数 store 断言未选中分片/块的 Get 次数为 0);
//	验收 8(TestM3Acceptance8OrderStartsPrefetcher):发现循环收到 ConsumeOrder →
//	  ForOrder 启动 PersistentPrefetcher、安装首个阶段、块就绪推进游标;
//	验收 9(TestM3Acceptance9StageSwitchKWindow):阶段切换 → 新阶段开头 K 块在"零完成"
//	  前提下即刻进入启动窗口并拉起下载,K 之外的块待 K 块就绪后才入 P2 邻域。
//
// 等待一律有界轮询(复用 orders_test 的 waitFor / preheat_test 的 waitTaskState);
// 下载门闩(而非裸 sleep)制造可控时序,断言确定性优先,-race 安全。

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "traincache/agent/api/agentpb"
	"traincache/agent/metrics"
	"traincache/agent/ordersource"
	"traincache/agent/prefetch"
	"traincache/core/cas"
	"traincache/core/index"
	"traincache/core/objectstore"
)

// M3 集成用例的数据集名(各场景相互隔离;每用例独立 fixture,dataset 名不共享)。
const (
	m3PrefixDataset = "m3-prefix" // 验收 5:路径选择预热
	m3ChunkDataset  = "m3-chunk"  // 验收 5:指定块预热
	m3OrderDataset  = "m3-order"  // 验收 8/9:订单驱动预取
)

// ring 布局常量(design §6.2,与 ordersource 读端一致;测试内独立字面量防冻结布局漂移)。
const (
	m3RingHeaderSize = 64 // 头固定 64B
	m3SlotHeaderSize = 12 // 槽头:seq int64 + len int32
)

// m3CountingStore 包装 objectstore.Mock,为集成断言提供两类能力:
//   - Get 计数:按对象键累计被 Get 的次数——验收 1/5/8 的"零下载/门闩在途"断言依据;
//   - 下载门闩(gate):装载后的键在 Get 里先等 release 放行(或 ctx 取消)再读源站——
//     验收 1/9 借此把下载稳定停在"在途",从而在无任何块完成的前提下做确定性断言
//     (不需要裸 sleep)。
//
// 索引分片/CURRENT/manifest 的读取也计入计数;数据块断言按 D25 键(prefix
// "<dataset>/blocks/")筛出,不与其混淆。
type m3CountingStore struct {
	*objectstore.Mock

	mu    sync.Mutex
	gets  map[string]int           // 键 → 累计 Get 次数
	gates map[string]chan struct{} // 键 → 放行信号(release 关闭);nil = 无门闩
}

// newM3CountingStore 构造空计数/门闩源站。
func newM3CountingStore() *m3CountingStore {
	return &m3CountingStore{Mock: objectstore.NewMock(), gets: make(map[string]int)}
}

var _ objectstore.Store = (*m3CountingStore)(nil)

// Get 计数后按门闩等待放行,再委托 Mock 读取。
func (s *m3CountingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	s.gets[key]++
	gate := s.gates[key]
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Mock.Get(ctx, key, w)
}

// getCount 返回键的累计 Get 次数。
func (s *m3CountingStore) getCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

// keysWithPrefix 返回计数表中出现过的、以 prefix 开头的键(去重、字典序)。
func (s *m3CountingStore) keysWithPrefix(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.gets {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// gateKeys 为 keys 装载下载门闩(幂等:已装载的键复用原信号)。
func (s *m3CountingStore) gateKeys(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gates == nil {
		s.gates = make(map[string]chan struct{}, len(keys))
	}
	for _, k := range keys {
		if _, ok := s.gates[k]; !ok {
			s.gates[k] = make(chan struct{})
		}
	}
}

// release 关闭并按表清除全部门闩(幂等;release 后新装载的门闩仍会阻塞等待下一次放行)。
func (s *m3CountingStore) release() {
	s.mu.Lock()
	gates := s.gates
	s.gates = nil
	s.mu.Unlock()
	for _, g := range gates {
		close(g)
	}
}

// m3Fixture 是集成用例装置:svcFixture(真实 core 组合 + svc 组件,复用 preheat_test 的
// newSvcFixtureWithStore)叠加计数/门闩源站。
type m3Fixture struct {
	fx    *svcFixture
	store *m3CountingStore
}

// newM3Fixture 构造装置(源站 = 计数/门闩包装的 Mock;其余装配同 newSvcFixture)。
func newM3Fixture(t *testing.T) *m3Fixture {
	t.Helper()
	store := newM3CountingStore()
	return &m3Fixture{fx: newSvcFixtureWithStore(t, store), store: store}
}

// commit 落索引(同 preheat_test 的 commitDataset,额外按 maxPerShard 指定分片粒度:
// 小粒度让不同 path 前缀落入独立分片,验收 5 据此断言"只加载所需分片";0 = 默认粒度):
// 逐块 cas.Put → index.Writer.Commit → 按 specs 放源站对象(D25 键)/Evict 本地副本。
func (e *m3Fixture) commit(t *testing.T, dataset string, specs []svcBlockSpec, maxPerShard int) {
	t.Helper()
	ctx := context.Background()
	fps := make(map[prefetch.ChunkRef]cas.Fingerprint, len(specs))
	entries := make([]index.Entry, 0, len(specs))
	for _, sp := range specs {
		fp, n, err := e.fx.cs.Put(ctx, strings.NewReader(sp.content))
		if err != nil {
			t.Fatalf("cas.Put(%s#%d): %v", sp.path, sp.chunk, err)
		}
		fps[prefetch.ChunkRef{Path: sp.path, Chunk: sp.chunk}] = fp
		entries = append(entries, index.Entry{Path: sp.path, Chunk: sp.chunk, Size: n, Fingerprint: fp})
	}
	if _, err := index.NewWriter(e.fx.store, e.fx.cs, index.Config{MaxEntriesPerShard: maxPerShard}).Commit(ctx, dataset, entries); err != nil {
		t.Fatalf("index Commit(%s): %v", dataset, err)
	}
	for _, sp := range specs {
		ref := prefetch.ChunkRef{Path: sp.path, Chunk: sp.chunk}
		if sp.source {
			if _, err := e.fx.store.Put(ctx, blockKeyLiteral(dataset, ref), strings.NewReader(sp.content), int64(len(sp.content))); err != nil {
				t.Fatalf("源站 Put(%s#%d): %v", sp.path, sp.chunk, err)
			}
		}
		if !sp.local {
			if err := e.fx.cs.Evict(fps[ref]); err != nil {
				t.Fatalf("cas.Evict(%s#%d): %v", sp.path, sp.chunk, err)
			}
		}
	}
}

// loadPrefixMapping 按前缀加载索引映射(与 ForOrder 的 LoadPrefixes 同路径):模拟
// "订单已到达、相关分片已加载"的前置状态;此后命中块走本地、免下载。
func (e *m3Fixture) loadPrefixMapping(t *testing.T, dataset string, prefixes []string) {
	t.Helper()
	if err := e.fx.pm.bundleFor(dataset, nil).catalog.LoadPrefixes(context.Background(), prefixes); err != nil {
		t.Fatalf("LoadPrefixes(%v): %v", prefixes, err)
	}
}

// shardKeyContaining 返回 dataset 索引分片中内容包含 needle 的键:经嵌入 Mock 直读分片
// 字节(绕开计数与门闩),用于装置前置与"未加载分片"的定位断言。未找到即 Fatal。
func (e *m3Fixture) shardKeyContaining(t *testing.T, dataset, needle string) string {
	t.Helper()
	ctx := context.Background()
	keys, err := e.store.List(ctx, "dataset/"+dataset+"/snapshots/")
	if err != nil {
		t.Fatalf("List(分片): %v", err)
	}
	for _, k := range keys {
		if !strings.HasSuffix(k, ".idx") {
			continue
		}
		var buf bytes.Buffer
		if err := e.store.Mock.Get(ctx, k, &buf); err != nil {
			t.Fatalf("直读分片 %s: %v", k, err)
		}
		if strings.Contains(buf.String(), needle) {
			return k
		}
	}
	t.Fatalf("dataset %s 无含 %q 的索引分片(keys=%v)", dataset, needle, keys)
	return ""
}

// startDiscoveryLoop 起发现循环并接默认订单 handler(HandleOrder = M2 日志行 + 预取
// 接线):扫描周期由用例先经 setScanInterval 改小;Run 的 ctx 生命周期交给 t.Cleanup。
func (e *m3Fixture) startDiscoveryLoop(t *testing.T, dir string) {
	t.Helper()
	d := NewOrderDiscovery(dir, metrics.New(metrics.Config{}), HandleOrder(e.fx.pm))
	startDiscovery(t, d)
}

// m3Order 构造字段齐全的 ConsumeOrder(chunks 顺序即订单顺序)。
func m3Order(dataset, stageID string, k int, chunks ...ordersource.ChunkRef) ordersource.ConsumeOrder {
	return ordersource.ConsumeOrder{
		Dataset:        dataset,
		SnapshotID:     "snap-" + stageID,
		StageID:        stageID,
		ShuffleSeed:    11,
		BatchSize:      64,
		PrefetchChunks: k,
		Chunks:         chunks,
	}
}

// ---------------- ring 写端(测试专用,模拟 sidecar;布局与写序同 ordersource 读端) ----------------

// m3RingWriter 是本文件私有的最小 ring 写端:magic "TCO1" / version 1 / 头 64B /
// 槽头 12B(seq int64 + len int32)/ commit = 头 write_seq 最后写 / ftruncate 只增不缩。
// 与 orders_test 的 writeTestRing(整文件原子落位)不同,本写端可在同一 ring 上连续提交
// 多份 ConsumeOrder —— 验收 8/9 的"同一 sidecar 先发订单、再做阶段切换"场景需要
// write_seq 递增的追读路径。
type m3RingWriter struct {
	t         *testing.T
	f         *os.File
	seq       int64 // 已 commit 的 write_seq
	slotSize  int32
	slotCount int32
}

// newM3RingWriter 建 ring 文件并写初始头(write_seq=0,generation 恒 1),ftruncate 到
// 64+slot_size(写端"只增不缩"的起点)。文件随即对发现循环可见(合法头,无订单)。
func newM3RingWriter(t *testing.T, path string, slotSize, slotCount int32) *m3RingWriter {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("创建测试 ring %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	w := &m3RingWriter{t: t, f: f, slotSize: slotSize, slotCount: slotCount}
	w.writeHeader()
	if err := f.Truncate(m3RingHeaderSize + int64(slotSize)); err != nil {
		t.Fatalf("ftruncate 测试 ring: %v", err)
	}
	return w
}

// WriteOrder 提交一份订单:先写整槽、后写头(commit);返回 seq。
func (w *m3RingWriter) WriteOrder(o ordersource.ConsumeOrder) int64 {
	w.t.Helper()
	payload, err := json.Marshal(o)
	if err != nil {
		w.t.Fatalf("序列化 ConsumeOrder: %v", err)
	}
	if room := int(w.slotSize) - m3SlotHeaderSize; len(payload) > room {
		w.t.Fatalf("payload %dB 超出槽容量 %dB(用例请选更大的 slot_size)", len(payload), room)
	}
	w.seq++
	off := int64(m3RingHeaderSize) + ((w.seq-1)%int64(w.slotCount))*int64(w.slotSize)
	need := off + int64(w.slotSize)
	if st, err := w.f.Stat(); err != nil {
		w.t.Fatalf("fstat 测试 ring: %v", err)
	} else if st.Size() < need {
		if err := w.f.Truncate(need); err != nil {
			w.t.Fatalf("ftruncate 测试 ring: %v", err)
		}
	}
	slot := make([]byte, m3SlotHeaderSize+len(payload))
	binary.LittleEndian.PutUint64(slot[0:8], uint64(w.seq))
	binary.LittleEndian.PutUint32(slot[8:12], uint32(len(payload)))
	copy(slot[m3SlotHeaderSize:], payload)
	if _, err := w.f.WriteAt(slot, off); err != nil {
		w.t.Fatalf("写测试 ring 槽: %v", err)
	}
	w.writeHeader()
	return w.seq
}

// writeHeader 重写 64B 头(commit 标记最后写,由调用顺序保证)。
func (w *m3RingWriter) writeHeader() {
	w.t.Helper()
	var h [m3RingHeaderSize]byte
	copy(h[0:4], "TCO1")
	binary.LittleEndian.PutUint16(h[4:6], 1) // version
	binary.LittleEndian.PutUint64(h[8:16], 1)
	binary.LittleEndian.PutUint64(h[16:24], uint64(w.seq)) // commit 标记
	binary.LittleEndian.PutUint32(h[24:28], uint32(w.slotSize))
	binary.LittleEndian.PutUint32(h[28:32], uint32(w.slotCount))
	if _, err := w.f.WriteAt(h[:], 0); err != nil {
		w.t.Fatalf("写测试 ring 头: %v", err)
	}
}

// m3WaitRunning 轮询 GetPreheatStatus 直到观察到 RUNNING(deadline testTimeout);
// 期间出现 ERROR 即 Fatal(门闩期不应有块级失败)。
func m3WaitRunning(t *testing.T, pc agentpb.PreheatServiceClient, taskID string) *agentpb.PreheatStatus {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(testTimeout)
	for {
		st, err := pc.GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{TaskId: taskID})
		if err != nil {
			t.Fatalf("GetPreheatStatus: %v", err)
		}
		switch st.GetState() {
		case agentpb.PreheatStatus_QUEUED:
		case agentpb.PreheatStatus_RUNNING:
			return st
		default:
			t.Fatalf("门闩期任务状态 = %v, want QUEUED/RUNNING", st.GetState())
		}
		if time.Now().After(deadline) {
			t.Fatalf("超时未观察到 RUNNING")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// m3WaitDone 轮询 GetPreheatStatus 直到 DONE(deadline testTimeout);ERROR 即 Fatal。
func m3WaitDone(t *testing.T, pc agentpb.PreheatServiceClient, taskID string) *agentpb.PreheatStatus {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(testTimeout)
	for {
		st, err := pc.GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{TaskId: taskID})
		if err != nil {
			t.Fatalf("GetPreheatStatus: %v", err)
		}
		if st.GetState() == agentpb.PreheatStatus_DONE {
			return st
		}
		if st.GetState() == agentpb.PreheatStatus_ERROR {
			t.Fatalf("任务 ERROR(completed=%d failed=%d)", st.GetCompletedBlocks(), st.GetFailedBlocks())
		}
		if time.Now().After(deadline) {
			t.Fatalf("超时未 DONE: state=%v completed=%d/%d", st.GetState(), st.GetCompletedBlocks(), st.GetTotalBlocks())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestM3Acceptance1PreheatRPCProgress(验收 1:"手动调用 gRPC 接口能够下发预热任务并查询
// 进度"):bufconn 全链路上 SubmitPreheat 受理 → GetPreheatStatus 轮询到 RUNNING(0/3,
// 三块下载被门闩停在在途)→ 放行 → DONE(3/3)→ 块真实落本地。
func TestM3Acceptance1PreheatRPCProgress(t *testing.T) {
	env := newM3Fixture(t)
	specs := []svcBlockSpec{
		{path: "rpc-000.tar", chunk: 0, content: "m3-rpc-0", source: true},
		{path: "rpc-001.tar", chunk: 0, content: "m3-rpc-1", source: true},
		{path: "rpc-002.tar", chunk: 1, content: "m3-rpc-2", source: true},
	}
	env.commit(t, testDataset, specs, 0)

	// 三块下载装载门闩(放行前保持"在途"):RUNNING 观测与进度计数均为确定性断言。
	keys := make([]string, 0, len(specs))
	for _, sp := range specs {
		keys = append(keys, blockKeyLiteral(testDataset, prefetch.ChunkRef{Path: sp.path, Chunk: sp.chunk}))
	}
	env.store.gateKeys(keys...)
	t.Cleanup(env.store.release)

	conn := startServerWith(t, env.fx.agent())
	pc := agentpb.NewPreheatServiceClient(conn)
	ctx := context.Background()

	req := validSubmitReq()
	resp, err := pc.SubmitPreheat(ctx, req)
	if err != nil {
		t.Fatalf("SubmitPreheat: %v", err)
	}
	if !resp.GetAccepted() || resp.GetTaskId() != req.GetTaskId() {
		t.Fatalf("SubmitPreheat 响应 = %+v, want accepted=true task_id=%s", resp, req.GetTaskId())
	}

	// 门闩期:进度可查且状态为 RUNNING(块在途未完成,计数为 0)。
	running := m3WaitRunning(t, pc, req.GetTaskId())
	if running.GetTotalBlocks() != 3 || running.GetCompletedBlocks() != 0 || running.GetFailedBlocks() != 0 {
		t.Errorf("RUNNING 进度 = total %d / completed %d / failed %d, want 3/0/0",
			running.GetTotalBlocks(), running.GetCompletedBlocks(), running.GetFailedBlocks())
	}
	if running.GetNode() != "worker-01" || running.GetDataset() != testDataset {
		t.Errorf("RUNNING 标识 = node %q dataset %q, want worker-01/%s",
			running.GetNode(), running.GetDataset(), testDataset)
	}

	// 放行下载:任务在进度查询下走完 RUNNING → DONE,计数吻合。
	env.store.release()
	done := m3WaitDone(t, pc, req.GetTaskId())
	if done.GetTotalBlocks() != 3 || done.GetCompletedBlocks() != 3 || done.GetFailedBlocks() != 0 {
		t.Errorf("DONE 进度 = total %d / completed %d / failed %d, want 3/3/0",
			done.GetTotalBlocks(), done.GetCompletedBlocks(), done.GetFailedBlocks())
	}
	if done.GetStartedAt() == nil || done.GetUpdatedAt() == nil {
		t.Error("DONE 应带 started_at/updated_at")
	}
}

// TestM3Acceptance5ScopedPreheatDownloads(验收 5:"路径预热只加载所需的 Manifest 分片,
// 不要求加载整个 Manifest;指定块预热不会意外下载未选中的数据块"):
//   - 两个路径前缀(train-data / val-data)各 2 块、分片粒度 2 → 前缀各自成片;
//   - 路径预热:仅所需分片被加载(未选中分片 Get 次数为零增量),选中块本地命中零下载;
//   - 指定块预热:恰选中块被下载(D25 键),同前缀邻居与另一前缀块一律零下载。
func TestM3Acceptance5ScopedPreheatDownloads(t *testing.T) {
	env := newM3Fixture(t)

	// 路径用例与块用例各建独立 dataset(共享 fixture,状态互不串扰)。
	env.commit(t, m3PrefixDataset, []svcBlockSpec{
		{path: "train-data/t-000.tar", chunk: 0, content: "m3-train-0", local: true},
		{path: "train-data/t-001.tar", chunk: 1, content: "m3-train-1", local: true},
		{path: "val-data/v-000.tar", chunk: 0, content: "m3-val-0", source: true},
		{path: "val-data/v-001.tar", chunk: 1, content: "m3-val-1", source: true},
	}, 2)
	env.commit(t, m3ChunkDataset, []svcBlockSpec{
		{path: "train-data/c-000.tar", chunk: 0, content: "m3-chunk-sel", source: true},
		{path: "train-data/c-001.tar", chunk: 0, content: "m3-chunk-sib", source: true},
		{path: "val-data/c-000.tar", chunk: 0, content: "m3-chunk-val", source: true},
	}, 2)

	// 装置前置:路径用例的两个前缀确实分属独立分片(否则"只加载所需分片"无从断言)。
	trainShard := env.shardKeyContaining(t, m3PrefixDataset, "train-data/t-000.tar")
	valShard := env.shardKeyContaining(t, m3PrefixDataset, "val-data/v-000.tar")
	if trainShard == valShard {
		t.Fatalf("装置前置不成立:train/val 未落入独立分片(%q)", trainShard)
	}

	t.Run("路径预热只加载所需分片且不误下载未选中块", func(t *testing.T) {
		// 模拟订单先到:仅加载 train-data 前缀映射(选中块将本地命中,零下载)。
		env.loadPrefixMapping(t, m3PrefixDataset, []string{"train-data/"})
		baseTrain, baseVal := env.store.getCount(trainShard), env.store.getCount(valShard)

		req := validSubmitReq()
		req.TaskId = "task-prefix"
		req.Dataset = m3PrefixDataset
		req.Prefixes = []string{"train-data/"}
		if _, err := env.fx.tasks.Submit(context.Background(), req); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		st := waitTaskState(t, env.fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_DONE)
		if st.GetTotalBlocks() != 2 || st.GetCompletedBlocks() != 2 || st.GetFailedBlocks() != 0 {
			t.Fatalf("进度 = total %d / completed %d / failed %d, want 2/2/0",
				st.GetTotalBlocks(), st.GetCompletedBlocks(), st.GetFailedBlocks())
		}

		// 仅所需前缀的分片被加载;未选中前缀的分片分毫未动(验收 5 前半句)。
		if got := env.store.getCount(valShard); got != baseVal {
			t.Errorf("未选中前缀的分片被下载: Get(%s) = %d, want %d", valShard, got, baseVal)
		}
		if got := env.store.getCount(trainShard); got <= baseTrain {
			t.Errorf("所需前缀的分片未被加载: Get(%s) = %d, want > %d", trainShard, got, baseTrain)
		}
		// 四个数据块零下载:选中块本地命中;未选中块(可下载,source=true 兜底反例)未被触碰。
		for _, sp := range []svcBlockSpec{
			{path: "train-data/t-000.tar", chunk: 0},
			{path: "train-data/t-001.tar", chunk: 1},
			{path: "val-data/v-000.tar", chunk: 0},
			{path: "val-data/v-001.tar", chunk: 1},
		} {
			key := blockKeyLiteral(m3PrefixDataset, prefetch.ChunkRef{Path: sp.path, Chunk: sp.chunk})
			if got := env.store.getCount(key); got != 0 {
				t.Errorf("数据块被意外下载: Get(%s) = %d, want 0", key, got)
			}
		}
	})

	t.Run("指定块预热不意外下载未选中块", func(t *testing.T) {
		env.loadPrefixMapping(t, m3ChunkDataset, []string{"train-data/"})
		selected := prefetch.ChunkRef{Path: "train-data/c-000.tar", Chunk: 0}

		req := validSubmitReq()
		req.TaskId = "task-chunk"
		req.Dataset = m3ChunkDataset
		req.Prefixes = nil
		req.Chunks = []*agentpb.ChunkRef{{Path: selected.Path, Chunk: selected.Chunk}}
		if _, err := env.fx.tasks.Submit(context.Background(), req); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		st := waitTaskState(t, env.fx.tasks, req.GetTaskId(), agentpb.PreheatStatus_DONE)
		if st.GetTotalBlocks() != 1 || st.GetCompletedBlocks() != 1 || st.GetFailedBlocks() != 0 {
			t.Fatalf("进度 = total %d / completed %d / failed %d, want 1/1/0",
				st.GetTotalBlocks(), st.GetCompletedBlocks(), st.GetFailedBlocks())
		}

		// 选中块真实走下载路径(本地无副本),未选中块一律零下载(验收 5 后半句)。
		selKey := blockKeyLiteral(m3ChunkDataset, selected)
		if got := env.store.getCount(selKey); got == 0 {
			t.Errorf("选中块未被下载: Get(%s) = %d, want > 0", selKey, got)
		}
		for _, ref := range []prefetch.ChunkRef{
			{Path: "train-data/c-001.tar", Chunk: 0}, // 同前缀邻居(可下载反例)
			{Path: "val-data/c-000.tar", Chunk: 0},   // 另一前缀(可下载反例)
		} {
			key := blockKeyLiteral(m3ChunkDataset, ref)
			if got := env.store.getCount(key); got != 0 {
				t.Errorf("未选中块被意外下载: Get(%s) = %d, want 0", key, got)
			}
		}
		// 数据块级下载恰为选中块一个(按 D25 键全量筛出)。
		got := env.store.keysWithPrefix(m3ChunkDataset + "/blocks/")
		want := []string{selKey}
		if !slices.Equal(got, want) {
			t.Errorf("数据块下载集合 = %v, want %v(仅选中块)", got, want)
		}
	})
}

// TestM3Acceptance8OrderStartsPrefetcher(验收 8:"启动最小训练 Pod 和 OrderSource sidecar
// 后,Agent 能够接收 ConsumeOrder 并启动 PersistentPrefetcher"):发现循环轮询 orders 目录
// 发现 ring → 读端解析 ConsumeOrder → HandleOrder → ForOrder(LoadPrefixes + Start +
// OnStageSwitch)→ 阶段就位、块就绪、游标推进;全部块本地命中 → 零数据块下载。
func TestM3Acceptance8OrderStartsPrefetcher(t *testing.T) {
	env := newM3Fixture(t)
	env.commit(t, m3OrderDataset, []svcBlockSpec{
		{path: "stage1/c-000.tar", chunk: 0, content: "m3-s1-0", local: true},
		{path: "stage1/c-001.tar", chunk: 0, content: "m3-s1-1", local: true},
		{path: "stage1/c-002.tar", chunk: 1, content: "m3-s1-2", local: true},
	}, 0)

	setScanInterval(t, 10*time.Millisecond)
	dir := t.TempDir()
	env.startDiscoveryLoop(t, dir)
	w := newM3RingWriter(t, filepath.Join(dir, "pod-m3.ring"), 4096, 8)

	chunks := []ordersource.ChunkRef{
		{Path: "stage1/c-000.tar", Chunk: 0},
		{Path: "stage1/c-001.tar", Chunk: 0},
		{Path: "stage1/c-002.tar", Chunk: 1},
	}
	w.WriteOrder(m3Order(m3OrderDataset, "stage-1", 1, chunks...))

	// 接收 + 启动:预取器调度已启动(Started)且首个阶段已安装(Stage)。
	b := env.fx.pm.bundleFor(m3OrderDataset, nil)
	waitFor(t, "收到 ConsumeOrder → 预取器启动并安装首个阶段", func() bool {
		b.mu.Lock()
		started := b.started
		b.mu.Unlock()
		return started && b.prefetcher.Status().Stage == "stage-1"
	})
	// 块就绪:预取推进游标到订单末块(首块 P1、其余邻域窗口,全部本地命中)。
	waitFor(t, "首阶段块全部就绪(预取前沿推进到末块)", func() bool {
		return b.prefetcher.Status().PrefetchFrontier == chunks[len(chunks)-1]
	})
	// 本地命中场景:数据块级下载为零(D25 键无 Get)。
	if got := env.store.keysWithPrefix(m3OrderDataset + "/blocks/"); len(got) != 0 {
		t.Errorf("本地命中场景出现数据块下载: %v, want 无", got)
	}
}

// TestM3Acceptance9StageSwitchKWindow(验收 9:"阶段切换事件到达后,Agent 能够及时将新阶段
// 开头 K 个块加入高优先级预取队列"):同一 ring 追读第二份 ConsumeOrder(新 StageID、
// prefetch_chunks=K、chunks 顺序重排)。
//
// 观察手段(确定性,不用裸 sleep):新阶段三块本地均无副本(仅源站可达)且装载下载门闩,
// 于是"没有任何块完成(新阶段游标仍为 -1)"的状态由测试完全控制,可以断言:
//  1. 前 K=2 块(按新订单顺序)已被拉起下载 —— 启动窗口在切换时刻入队,不依赖完成推进;
//  2. 第 3 块零触碰 —— P2 邻域须待 K 块就绪后才开放(§7.3),即 K 块优先于其余块;
//  3. 放行后 K 块完成 → 邻域开放 → 第 3 块下载并完成,游标推进到新阶段末块。
//
// (P1 队列深度的字面量断言由 prefetch 单测 TestStageSwitchEnqueuesKWindowAsP1 覆盖;
// 本用例覆盖其端到端可观察后果。)
func TestM3Acceptance9StageSwitchKWindow(t *testing.T) {
	env := newM3Fixture(t)
	env.commit(t, m3OrderDataset, []svcBlockSpec{
		{path: "stage1/c-000.tar", chunk: 0, content: "m3-s1-a", local: true},
		{path: "stage1/c-001.tar", chunk: 0, content: "m3-s1-b", local: true},
		// 新阶段:仅源站可达(下载走门闩),顺序重排(非路径序:stage2/b → a → c)。
		{path: "stage2/b-000.tar", chunk: 0, content: "m3-s2-b", source: true},
		{path: "stage2/a-000.tar", chunk: 0, content: "m3-s2-a", source: true},
		{path: "stage2/c-000.tar", chunk: 0, content: "m3-s2-c", source: true},
	}, 0)

	setScanInterval(t, 10*time.Millisecond)
	dir := t.TempDir()
	env.startDiscoveryLoop(t, dir)
	w := newM3RingWriter(t, filepath.Join(dir, "pod-m3.ring"), 4096, 8)

	// 第一份订单(阶段一):两块本地命中,完成后再做阶段切换(稳定态切换)。
	stage1 := []ordersource.ChunkRef{{Path: "stage1/c-000.tar"}, {Path: "stage1/c-001.tar"}}
	w.WriteOrder(m3Order(m3OrderDataset, "stage-1", 1, stage1...))
	b := env.fx.pm.bundleFor(m3OrderDataset, nil)
	waitFor(t, "首阶段块全部就绪(预取前沿到末块)", func() bool {
		return b.prefetcher.Status().PrefetchFrontier == stage1[len(stage1)-1]
	})

	// 第二份订单(新阶段):K=2 启动窗口;chunks 顺序重排为 b → a → c。
	newOrder := []ordersource.ChunkRef{
		{Path: "stage2/b-000.tar", Chunk: 0},
		{Path: "stage2/a-000.tar", Chunk: 0},
		{Path: "stage2/c-000.tar", Chunk: 0},
	}
	window, rest := newOrder[:2], newOrder[2] // 前 K 块(启动窗口)vs K 之外的块
	winKeys, restKey :=
		[]string{
			blockKeyLiteral(m3OrderDataset, window[0]),
			blockKeyLiteral(m3OrderDataset, window[1]),
		},
		blockKeyLiteral(m3OrderDataset, rest)
	env.store.gateKeys(winKeys[0], winKeys[1], restKey)
	t.Cleanup(env.store.release)

	w.WriteOrder(m3Order(m3OrderDataset, "stage-2", 2, newOrder...))

	waitFor(t, "阶段切换事件已安装(Stage=stage-2)", func() bool {
		return b.prefetcher.Status().Stage == "stage-2"
	})
	// 断言 1:前 K 块在零完成前提下已被拉起下载(启动窗口 = 切换时刻入队)。
	waitFor(t, "新阶段开头 K 块即刻入队并拉起下载(P1 启动窗口)", func() bool {
		return env.store.getCount(winKeys[0]) >= 1 && env.store.getCount(winKeys[1]) >= 1
	})
	// 断言 2:此刻无任何块完成(预取前沿未建立),且 K 之外的块零触碰(P2 邻域未开放)。
	if got := b.prefetcher.Status().PrefetchFrontier; got != (ordersource.ChunkRef{}) {
		t.Errorf("门闩期不应有块完成: PrefetchFrontier = %+v, want 零值", got)
	}
	if got := env.store.getCount(restKey); got != 0 {
		t.Errorf("K 之外的块在启动窗口期被触碰: Get(%s) = %d, want 0", restKey, got)
	}

	// 断言 3:放行 → K 块完成 → 邻域开放 → 第 3 块下载完成,游标到新阶段末块。
	env.store.release()
	waitFor(t, "K 块就绪后剩余块进入邻域并完成(预取前沿到新阶段末块)", func() bool {
		return b.prefetcher.Status().PrefetchFrontier == rest
	})
	if got := env.store.getCount(restKey); got == 0 {
		t.Errorf("K 块就绪后剩余块未被预取: Get(%s) = %d, want > 0", restKey, got)
	}
}
