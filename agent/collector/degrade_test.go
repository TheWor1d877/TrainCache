package collector

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/ordersource"
)

// degrade_test.go:降级状态机(design §7.4、验收 10;decisions D29)——ring 写入停滞 30s
// 或文件消失 → degraded;代际/序号推进 → 自动退出。共 10 个场景。

// testNode 与 newTestEnv 的 metrics.Config.Node 一致(agent_degraded 打点核对用)。
const testNode = "worker-01"

// ringAndReader 建 ring、写首份订单并返回 (写端, 读端);等到首次推进可见。
func ringAndReader(t *testing.T) (*testRing, *ordersource.Reader) {
	t.Helper()
	w := newTestRing(t, t.TempDir(), 1)
	w.writeOrder(testOrder("b-track", "epoch-1", 0, 1))
	r := newTestReader(t, w.path)
	mustEventually(t, "ring 首次推进可见", func() bool { return !r.LastAdvance().IsZero() })
	return w, r
}

// 场景 1:ring 未装配(nil)→ 判降级;degradedAt 为纯函数,不产生状态转移。
func TestDegradedAtNilRing(t *testing.T) {
	env := newTestEnv(t, nil, CollectorConfig{})
	if !env.c.degradedAt(time.Now()) {
		t.Error("nil ring 应判降级")
	}
	if env.c.Degraded() {
		t.Error("degradedAt 不应改变状态(纯函数)")
	}
}

// 场景 2:ring 文件不存在(Reader.Missing)→ 判降级;LastAdvance 保持零值。
func TestDegradedAtMissingFile(t *testing.T) {
	ring := newTestReader(t, filepath.Join(t.TempDir(), "absent.ring"))
	mustEventually(t, "Missing 标记", ring.Missing)
	mustEventually(t, "LastAdvance 保持零值", func() bool { return ring.LastAdvance().IsZero() })

	env := newTestEnv(t, ring, CollectorConfig{})
	if !env.c.degradedAt(time.Now()) {
		t.Error("ring 文件缺失应判降级")
	}
}

// 场景 3:正常推进不降级;停滞阈值恰为 30s(§7.4 冻结),"严格大于"才降级(边界)。
func TestDegradedAtFreshAdvanceAndBoundary(t *testing.T) {
	_, ring := ringAndReader(t)
	env := newTestEnv(t, ring, CollectorConfig{})

	if got := env.c.stallThreshold; got != 30*time.Second {
		t.Fatalf("停滞阈值 = %v, want 30s(§7.4 冻结)", got)
	}
	la := ring.LastAdvance()
	if env.c.degradedAt(la) {
		t.Error("刚推进(Δ=0)不应降级")
	}
	if env.c.degradedAt(la.Add(30 * time.Second)) {
		t.Error("停滞恰为阈值 30s 不应降级(严格大于)")
	}
	if !env.c.degradedAt(la.Add(30*time.Second + time.Nanosecond)) {
		t.Error("停滞超阈值 1ns 应降级")
	}
}

// 场景 4:文件存在但头非法(从未推进,LastAdvance 零值)→ 判降级;Missing 仍为 false。
func TestDegradedAtNeverAdvancedBadHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.ring")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0xAB}, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	ring := newTestReader(t, path)
	time.Sleep(300 * time.Millisecond) // ≥1 个 poll 周期

	if ring.Missing() {
		t.Fatal("文件存在,Missing 应为 false")
	}
	if !ring.LastAdvance().IsZero() {
		t.Fatal("坏头不应记为推进(D29)")
	}
	env := newTestEnv(t, ring, CollectorConfig{})
	if !env.c.degradedAt(time.Now()) {
		t.Error("从未观察到推进应判降级")
	}
}

// 场景 5:停滞超过阈值(时间推进)→ 判降级。
func TestDegradedAtStallBeyondThreshold(t *testing.T) {
	_, ring := ringAndReader(t)
	env := newTestEnv(t, ring, CollectorConfig{})
	env.c.stallThreshold = 40 * time.Millisecond

	la := ring.LastAdvance()
	if env.c.degradedAt(la) {
		t.Fatal("刚推进即判停滞")
	}
	time.Sleep(60 * time.Millisecond)
	if !env.c.degradedAt(time.Now()) {
		t.Error("停滞超过阈值应判降级")
	}
}

// 场景 6+7:状态转移经 checkDegrade 生效——停滞 → degraded=1 打点;
// 写端推进(同一代际 write_seq 前进)→ 自动退出 degraded=0。
func TestDegradeTransitionMetricAndRecovery(t *testing.T) {
	w, ring := ringAndReader(t)
	env := newTestEnv(t, ring, CollectorConfig{})
	env.c.clock = time.Now // 状态机走真实时钟(与 ring LastAdvance 同源)
	env.c.stallThreshold = 40 * time.Millisecond

	env.c.checkDegrade()
	if env.c.Degraded() {
		t.Fatal("刚推进即被判降级")
	}
	if got := testutil.ToFloat64(env.m.AgentDegraded().WithLabelValues(testNode)); got != 0 {
		t.Errorf("agent_degraded = %v, want 0", got)
	}

	time.Sleep(60 * time.Millisecond)
	env.c.checkDegrade()
	if !env.c.Degraded() {
		t.Fatal("停滞超过阈值未进入降级")
	}
	if got := testutil.ToFloat64(env.m.AgentDegraded().WithLabelValues(testNode)); got != 1 {
		t.Errorf("降级后 agent_degraded = %v, want 1", got)
	}

	la := ring.LastAdvance()
	w.writeOrder(testOrder("b-track", "epoch-1", 2, 3)) // write_seq 前进
	mustEventually(t, "推进可观测", func() bool { return ring.LastAdvance().After(la) })
	env.c.checkDegrade()
	if env.c.Degraded() {
		t.Error("推进后未自动退出降级")
	}
	if got := testutil.ToFloat64(env.m.AgentDegraded().WithLabelValues(testNode)); got != 0 {
		t.Errorf("恢复后 agent_degraded = %v, want 0", got)
	}
}

// 场景 8:sidecar 重启(代际变化,write_seq 不变)也视为推进 → 退出降级(D29)。
func TestDegradeRecoveryOnGenerationChange(t *testing.T) {
	w, ring := ringAndReader(t)
	env := newTestEnv(t, ring, CollectorConfig{})
	env.c.clock = time.Now
	env.c.stallThreshold = 30 * time.Millisecond

	time.Sleep(50 * time.Millisecond)
	env.c.checkDegrade()
	if !env.c.Degraded() {
		t.Fatal("停滞未降级")
	}

	la := ring.LastAdvance()
	w.setGeneration(2) // 仅代际推进
	mustEventually(t, "代际变化可观测", func() bool { return ring.LastAdvance().After(la) })
	env.c.checkDegrade()
	if env.c.Degraded() {
		t.Error("新代际未退出降级")
	}
}

// 场景 9:Start 起的判定循环——停滞自动降级、推进自动恢复(双向闭环)。
func TestDegradeLoopStartAndRecover(t *testing.T) {
	w, ring := ringAndReader(t)
	env := newTestEnv(t, ring, CollectorConfig{})
	env.c.clock = time.Now
	env.c.stallThreshold = 150 * time.Millisecond
	env.start()

	if env.c.Degraded() {
		t.Fatal("启动即降级(ring 新近推进)")
	}
	mustEventually(t, "循环判定停滞 → 降级", env.c.Degraded)

	la := ring.LastAdvance()
	w.writeOrder(testOrder("b-track", "epoch-1", 4, 5))
	mustEventually(t, "推进可观测", func() bool { return ring.LastAdvance().After(la) })
	mustEventually(t, "循环判定推进 → 退出降级", func() bool { return !env.c.Degraded() })
}

// 场景 10:nil ring 时 Start 同步判定一次 → 启动后立即可见降级(无需等首个 tick)。
func TestDegradeStartupImmediateNilRing(t *testing.T) {
	env := newTestEnv(t, nil, CollectorConfig{})
	env.start()
	if !env.c.Degraded() {
		t.Fatal("nil ring 启动后应立即可见降级(Start 内同步判定)")
	}
	if got := testutil.ToFloat64(env.m.AgentDegraded().WithLabelValues(testNode)); got != 1 {
		t.Errorf("agent_degraded = %v, want 1", got)
	}
}
