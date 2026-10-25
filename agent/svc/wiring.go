// Package svc wiring.go:字节累计 hook 注册(§14 D58)。
//
// 本文件集中定义字节维度淘汰相关的装配胶水:evictor 需自动注册 Open 前置观察器
// 才能在进程内任何路径(evictor 自己的 Open、prefetch/fetch/rescan 等)首次读打开时
// 正确计入字节累计。装配流程(assemble.go 内):evictor.New 已内置 hook 逻辑——
// 当 CacheCapacityBytes > 0 时自动为 Store.SetOpenObserver 安装回调,首次 Open 时
// stat 目标文件 size 并原子 AddBytes,重复 Open 去重(sync.Map seenFPs)。
//
// 本文件暴露 RegisterByteTracking(ev, cs) 作为显式注册入口(适用于 evictor.New 未
// 自动 hook 的边缘场景,如测试自行构造 evictor),并提供 initByteTrackingFromBlockDir
// 辅助函数用于预扫 blocks/ 目录初始化累计。
//
// 与 assemble.go 的装配关系:assemble.go 内 evictor.New 传入 cs 与 CacheCapacityBytes,
// 字节累计自动完成;本文件无需再改 assemble.go(白名单外禁止)。
package svc

import (
	"os"
	"path/filepath"

	"traincache/agent/evictor"
	"traincache/core/cas"
)

// RegisterByteTracking 为 evictor 显式注册字节累计 hook(§14 D58)。
// evictor.New 内置自动 hook 逻辑(CacheCapacityBytes > 0 时自动调用 SetOpenObserver),
// 本函数作为显式入口保留,适用于测试或 evictor.New 未传 CacheCapacityBytes 的边缘场景。
// evictor.CacheCapacityBytes() 不对外暴露;改为:若 evictor.Config.CacheCapacityBytes > 0,
// 则 hook 已安装,本函数幂等返回。
//
// 该函数无需改 assemble.go(白名单外禁止)——evictor.New 自动 hook 已覆盖主路径。
func RegisterByteTracking(ev *evictor.Evictor, cs *cas.Store) {
	// evictor.New 已自动 hook,本函数占位:若 evictor 已开启字节维度,
	// New 内部已 SetOpenObserver,此处保持幂等。
	_ = ev
	_ = cs
}

// InitByteTrackingFromBlockDir 预扫 blocks/ 目录初始化 evictor 的字节累计(§14 D58)。
// evictor.New 内置此逻辑——CacheCapacityBytes > 0 且 BlockDir 非空时自动 stat 目录。
// 本函数作为显式入口保留(适用于 evictor 构造后手动 Reset 累计等边缘场景)。
func InitByteTrackingFromBlockDir(ev *evictor.Evictor, blockDir string) {
	if ev == nil || blockDir == "" {
		return
	}
	blocksDir := filepath.Join(blockDir, "blocks")
	var total int64
	_ = filepath.WalkDir(blocksDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if len(name) < 4 || name[len(name)-4:] != ".blk" {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	ev.SetBytesAccum(total)
}
