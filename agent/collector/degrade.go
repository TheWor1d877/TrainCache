package collector

import (
	"context"
	"log/slog"
	"time"
)

// degrade.go:降级状态机(design §7.4、验收 10;decisions D29):
//
//	sidecar 退出(ring 写入停滞 30s 或文件消失)→ 置 degraded:停止依赖 OrderSource,
//	以 inotify 打开事件驱动热度/命中率统计,traincache_agent_degraded=1;
//	sidecar 恢复(新代际 write_seq 推进)→ 自动退出降级。
//
// 判据只取 ring 观测(ordersource.Reader):Missing()=文件消失;LastAdvance()=最近推进
// 时间(零值=从未推进)。时间源 clock 可注入(测试用),判定周期 1s(包内变量可改)。

// degradeStallThreshold 是 ring 写入停滞阈值(§7.4:30s;包内变量,NewCollector 构造时
// 固化为 Collector.stallThreshold,测试改包变量后构造即可覆盖)。
var degradeStallThreshold = 30 * time.Second

// degradeCheckInterval 是降级判定周期(§7.4 未冻结,取 1s;构造时固化,同上)。
var degradeCheckInterval = time.Second

// Degraded 报告当前是否处于降级模式(§7.4;并发安全)。
func (c *Collector) Degraded() bool {
	c.degMu.Lock()
	defer c.degMu.Unlock()
	return c.degraded
}

// degradeLoop 周期判定循环(ctx 取消退出)。
func (c *Collector) degradeLoop(ctx context.Context) {
	t := time.NewTicker(c.degradeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.checkDegrade()
		}
	}
}

// checkDegrade 执行一次判定并处理状态转移(时间注入 clock)。
func (c *Collector) checkDegrade() {
	c.applyDegrade(c.degradedAt(c.clock()))
}

// degradedAt 判定 now 时刻是否应处降级(纯函数,便于时间注入测试):
//   - ring 未装配 / 文件不存在(Reader.Missing)→ 降级;
//   - 从未观察到推进(LastAdvance 零值:文件从未出现或头始终非法)→ 降级;
//   - 最近一次推进距今超过停滞阈值(严格大于,恰为阈值不降级)→ 降级。
func (c *Collector) degradedAt(now time.Time) bool {
	if c.ring == nil {
		return true
	}
	if c.ring.Missing() {
		return true
	}
	la := c.ring.LastAdvance()
	if la.IsZero() {
		return true
	}
	return now.Sub(la) > c.stallThreshold
}

// applyDegrade 状态转移:仅在变化时更新状态、打点(agent_degraded 1/0)与日志。
func (c *Collector) applyDegrade(d bool) {
	c.degMu.Lock()
	if c.degraded == d {
		c.degMu.Unlock()
		return
	}
	c.degraded = d
	c.degMu.Unlock()

	if d {
		slog.Warn("collector: 进入降级模式(sidecar ring 停滞/文件消失),以 inotify 兜底",
			"block_dir", c.blockDir, "stall_threshold", c.stallThreshold)
		if c.m != nil {
			c.m.SetAgentDegraded(1)
		}
		return
	}
	slog.Info("collector: 退出降级模式(sidecar ring 恢复推进)", "block_dir", c.blockDir)
	if c.m != nil {
		c.m.SetAgentDegraded(0)
	}
}
