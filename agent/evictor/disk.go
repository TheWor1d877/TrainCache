// disk.go 提供默认磁盘利用率来源:Config.Usage 为 nil 且 BlockDir 非空时,
// New 取 statfs(BlockDir) 作为实现(本包唯一触碰文件系统的地方是 core cas.Evict,
// 利用率查询本身经此默认实现)。
package evictor

import (
	"fmt"
	"syscall"
)

// statfsUsage 返回 statfs(dir) 的 (used, total) 字节数:
//   - total = 块总数 × 块大小;
//   - used  = total − 空闲块数 × 块大小(取 Bfree 而非 Bavail:本进程即写入者与保留块
//     的持有者,不存在非特权用户视角下可用块被高估的问题;DECISIONS 回传)。
//
// 不引入新依赖:走标准库 syscall(Linux 为目标平台;项目部署形态为 K8s DaemonSet)。
func statfsUsage(dir string) func() (used, total uint64, err error) {
	return func() (uint64, uint64, error) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(dir, &st); err != nil {
			return 0, 0, fmt.Errorf("evictor: statfs %s: %w", dir, err)
		}
		bsize := uint64(st.Bsize)
		total := st.Blocks * bsize
		free := st.Bfree * bsize
		if free > total {
			free = total // 防御:文件系统实现异常时不让 used 下溢
		}
		return total - free, total, nil
	}
}
