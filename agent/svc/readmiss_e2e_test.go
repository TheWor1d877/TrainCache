package svc

// readmiss_e2e_test.go:M8/D44 read_miss 端到端(不做纯函数伪造)。
//
// 用真实 HookSock(Unix datagram)+ 最小可用 Agent 装配(真实 core:objectstore.Mock、
// 临时 CAS、fetch.Fetcher、PrefetchManager)直写 socket 发送 read_miss,断言:
//   - 块被 demand 补回本地且可读;
//   - traincache_hook_read_miss_total{reason="absent"} 增长(逐帧计数);
//   - 回源 reason=demand 的 fetch_requests_total 增长。
//
// 装置复用 m3Fixture(计数源站 + 真实索引提交),在 svc 包内运行,不依赖外部进程。

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"traincache/agent/metrics"
	"traincache/agent/nodeid"
	"traincache/core/cas"
)

// m8ReadMissDataset 是本用例独立 dataset(避免与其它 fixture 场景串扰)。
const m8ReadMissDataset = "m8-read-miss"

// newReadMissAgent 构造最小可用 Agent:真实 HookSock + 与 fixture 共享的
// PrefetchManager/CAS/指标(与 Assemble 同构的钩子 handler 接线)。
func newReadMissAgent(t *testing.T, env *m3Fixture) *Agent {
	t.Helper()
	cfg := Config{
		NodeName:           nodeid.ID("worker-01"),
		CacheRoot:          env.fx.cfg.CacheRoot,
		OrdersDir:          filepath.Join(t.TempDir(), "orders"),
		Verify:             VerifyXXH3,
		ReadStallMinWaitMS: stallMinWaitMS,
	}
	ag := &Agent{
		Cfg:         cfg,
		Metrics:     env.fx.reg,
		Prefetch:    env.fx.pm,
		CS:          env.fx.cs,
		ObjectStore: env.store,
	}
	ag.phase = newPhaseTracker(prefetchLookahead)
	env.fx.pm.phase = ag.phase // 与装配层一致:预取器切换回调可通知 phaseTracker
	ag.HookSock = New(hookSockPath(cfg), ag.Metrics, ag.hookHandlers())

	ctx, cancel := context.WithCancel(context.Background())
	if err := ag.HookSock.Start(ctx); err != nil {
		cancel()
		t.Fatalf("HookSock.Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = ag.HookSock.Close()
	})
	return ag
}

// TestHookReadMissEndToEndDemandBackfill:直写 Unix datagram 的 read_miss 触发
// demand 补块;块可读、帧计数与 reason=demand 回源计数同步增长。
func TestHookReadMissEndToEndDemandBackfill(t *testing.T) {
	env := newM3Fixture(t)
	ref := struct {
		path  string
		chunk int64
	}{path: "rm/c-000.tar", chunk: 0}
	env.commit(t, m8ReadMissDataset, []svcBlockSpec{{
		path: ref.path, chunk: ref.chunk, content: "m8-read-miss-block", source: true,
	}}, 0)
	// 模拟订单已到达:加载该 dataset 索引映射;此后 resolveRef/Lookup 可用。
	env.loadPrefixMapping(t, m8ReadMissDataset, []string{""})

	entry, found, err := env.fx.pm.bundleFor(m8ReadMissDataset, nil).catalog.Lookup(
		context.Background(), ref.path, ref.chunk)
	if err != nil || !found {
		t.Fatalf("索引查询 %s#%d: found=%v err=%v", ref.path, ref.chunk, found, err)
	}
	if _, err := env.fx.cs.Open(entry.Fingerprint); !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("装置前置失败:块应不在本地,Open err=%v", err)
	}

	ag := newReadMissAgent(t, env)
	c := dialHook(t, hookSockPath(ag.Cfg))

	missBefore := testutil.ToFloat64(env.fx.reg.HookReadMiss().WithLabelValues(metrics.ReasonAbsent))
	demandBefore := testutil.ToFloat64(env.fx.reg.FetchRequests().WithLabelValues(
		"other", metrics.ResultSuccess, metrics.FetchReasonDemand))

	sendHook(t, c, []byte(`{"type":"read_miss","path":"rm/c-000.tar","chunk":0,`+
		`"reason":"absent","ts":"2026-10-18T00:00:00Z"}`))

	waitFor(t, "hook_read_miss_total{reason=absent} 增长", func() bool {
		return testutil.ToFloat64(env.fx.reg.HookReadMiss().WithLabelValues(metrics.ReasonAbsent)) == missBefore+1
	})
	waitFor(t, "demand 补块后块可读", func() bool {
		br, err := env.fx.cs.Open(entry.Fingerprint)
		if err != nil {
			return false
		}
		_ = br.Close()
		return true
	})
	waitFor(t, "fetch_requests_total{reason=demand} 增长", func() bool {
		return testutil.ToFloat64(env.fx.reg.FetchRequests().WithLabelValues(
			"other", metrics.ResultSuccess, metrics.FetchReasonDemand)) > demandBefore
	})
}
