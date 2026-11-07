// Package svc wiring.go:Store 观察者注册(设计《回收与磁盘淘汰》D58 字节维度累计 +D49 自打开守卫)。
//
// cas.Store 的两类观察者都是单槽(atomic.Pointer),后注册者覆盖前者,所以全部回调必须在
// 一处合并注册:Open 前置槽同时供 selfOpen.Mark 与 evictor.OnStoreOpen,Put 后置槽供
// evictor.OnStorePut。evictor.New 不自行注册 Open 槽,避免覆盖装配层的合并回调。
package svc

import (
	"traincache/core/cas"
)

// installStoreObservers 把字节维度累计与自打开守卫挂到 cs 上。必须在任何写入方
// (fetch/prefetch/backfill)起跑之前调用一次;调用时 ag.Evictor 必须已构造。
func (ag *Agent) installStoreObservers(cs *cas.Store) {
	cs.SetOpenObserver(func(fp cas.Fingerprint) {
		ag.selfOpen.Mark(fp)
		ag.Evictor.OnStoreOpen(fp)
	})
	cs.SetPutObserver(func(fp cas.Fingerprint, size int64) {
		ag.Evictor.OnStorePut(fp, size)
	})
}
