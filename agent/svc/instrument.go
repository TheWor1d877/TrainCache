package svc

// instrument.go:综合验收补齐的三处生产打点接线(M6 七线指标的调用点;此前 family 已全量
// 注册但回源/索引/延迟三线的 7 个 family 在生产代码零调用点):
//   - 回源线(②):埋点在被下载路径内,见 prefetch.PersistentPrefetcher.grabBlock;
//   - 索引线(③):本文件 measureLoadPrefixes —— 包住 agent 侧唯一真实发起 Catalog 刷新的
//     调用点(ForOrder 的 LoadPrefixes);
//   - 延迟线(⑥):埋点在各 RPC 入口,见 server.go 的 observeRequest;
//   - 订单到达 t_touch 刷新(§7.6 任务 4;D24 t_touch 三源之"新订单刷新"):本文件
//     SetOrderTouchHook/dispatchOrderTouch + wire.go onOrderTouch。
//
// 标签纪律沿用 metrics 打点 API 自身(DatasetLabel 归并、result 集合外忽略),本文件不
// 重复校验。

import (
	"context"
	"time"

	"traincache/agent/metrics"
	"traincache/agent/prefetch"
	"traincache/core/index"
)

// orderTouchHook 是"新 ConsumeOrder 到达 → 刷新订单内块 t_touch"的 svc 侧窄接口
// (§7.6 任务 4):dataset 归属 + 该 dataset 的 Catalog(供回调内解析指纹)+ 订单块引用。
// 在 ForOrder 调用链内同步调用(索引解析为纯内存查询),实现不得阻塞。
type orderTouchHook func(dataset string, cat *index.Catalog, refs []prefetch.ChunkRef)

// SetOrderTouchHook 注册新订单 t_touch 刷新钩子(装配期调用一次;传 nil 解除)。
// 回调路径原子读(与 p0Done/blockDone 同口径),调用链在订单处理 goroutine 内。
func (m *PrefetchManager) SetOrderTouchHook(fn orderTouchHook) {
	if fn == nil {
		m.orderTouch.Store(nil)
		return
	}
	m.orderTouch.Store(&fn)
}

// dispatchOrderTouch 把一次新订单的 t_touch 刷新转给已注册钩子(无注册则丢弃)。
func (m *PrefetchManager) dispatchOrderTouch(dataset string, cat *index.Catalog, refs []prefetch.ChunkRef) {
	if h := m.orderTouch.Load(); h != nil {
		(*h)(dataset, cat, refs)
	}
}

// measureLoadPrefixes 执行一次订单索引加载并打索引线指标(③):失败记一次
// traincache_index_refresh_total{dataset,error};成功路径的 success 计数由 Catalog
// OnSnapshotChange 统一计入(R1/7.1 §5.9:ForOrder 与 Follow 共用回调,避免重复计数),
// 本函数追加 traincache_index_refresh_duration_seconds{dataset,success}(签名无 result,
// 失败耗时不上报——避免以 result="success" 混入失败样本),并以 Catalog.ShardsLoaded()
// (5.3 D30:当前生效快照的 Manifest 分片数)上报 traincache_index_shards_loaded{dataset}
// (family 调用点即在此;此前恒 0 为 M6 遗留)。失败路径不更新该 gauge:失败不动旧映射,
// gauge 保持前次成功值(尚无成功加载则为注册初值 0),不被瞬时错误打成 0。
// 打点不改变调用语义:LoadPrefixes 的错误原样返回,调用方行为不变。
func (m *PrefetchManager) measureLoadPrefixes(ctx context.Context, dataset string, cat *index.Catalog, prefixes []string) error {
	start := time.Now()
	err := cat.LoadPrefixes(ctx, prefixes)
	if m.m == nil {
		return err
	}
	if err != nil {
		// 成功路径的 index_refresh_total 由 Catalog 的 OnSnapshotChange 统一计入
		// (R1:ForOrder 与 Follow 共用同一回调,避免同一轮加载重复计数);失败不切换、
		// 无回调,故在此计 error。
		m.m.IncIndexRefresh(dataset, metrics.ResultError)
		return err
	}
	m.m.ObserveIndexRefreshDuration(dataset, time.Since(start).Seconds())
	m.m.SetIndexShardsLoaded(dataset, int64(cat.ShardsLoaded()))
	return nil
}
