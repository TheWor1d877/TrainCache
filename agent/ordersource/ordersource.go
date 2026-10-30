// Package ordersource 是 OrderSource 的读端:以 mmap 只读方式追读 sidecar 写入的
// 共享内存 ring(design §5.3 类型与接口、§6.2 ring 布局;decisions D22 写端协议、
// D23 格式错误指标)。
//
// 纯读端、仅标准库:不依赖 core、不依赖 metrics(decisions D4:子模块互不依赖);
// 写端由 sidecar 承担(ordersrc/,M2 阶段 B)。ring 单写(sidecar)单读(Agent):
//
//   - commit 标记 = 头 write_seq 最后写:reader 只读 seq ≤ 头 write_seq 的槽,半写槽天然不可见;
//     槽索引 =(seq-1) mod slot_count;write_seq 跨代际续增(sidecar 重启读旧头 +1,新文件从 1 起);
//   - reader 落后不阻塞 writer:落后超过 slot_count 时直接跳至最新(阶段切换只关心最新顺序);
//   - writer_generation 变化 → reader 清当前订单、丢弃追读位置重读最新,新代际订单即使载荷
//     字节相同也重放一次;writer 重启由代际计数检测;跳读 / 损坏槽 / 空订单计入包级丢单计数
//     (design §7.4);
//   - 格式不符(magic / version / 几何非法)→ 忽略该文件并计入包级错误计数(D23)。
//
// Reader 由 svc 装配(发现 = svc 目录轮询 watchDir,见 D22);平台锁定 linux
// (syscall.Mmap 直接可用,项目 k8s-only,不写 build tag、不引 x/sys)。
package ordersource

import (
	"context"
	"errors"
)

// ChunkRef 是块引用(core Entry 寻址,design §5.1/§5.3;字段与 JSON 标签冻结)。
type ChunkRef struct {
	Path  string `json:"path"`
	Chunk int64  `json:"chunk"`
}

// ConsumeOrder 是本阶段的消费顺序(共享内存载荷,design §5.3/§6.1;字段与 JSON 标签冻结)。
type ConsumeOrder struct {
	Dataset        string     `json:"dataset"`
	SnapshotID     string     `json:"snapshot_id"`
	StageID        string     `json:"stage_id"`
	ShuffleSeed    int64      `json:"shuffle_seed"`
	BatchSize      int64      `json:"batch_size"`
	PrefetchChunks int        `json:"prefetch_chunks"` // 启动窗口大小 K
	Chunks         []ChunkRef `json:"chunks"`          // 本阶段消费顺序(有序块引用列表)
}

// OrderSource 是消费顺序来源(design §5.3):读端实现为共享内存 mmap Reader,
// 由 svc 装配。
type OrderSource interface {
	// CurrentOrder 返回当前阶段的消费顺序;尚无任何推送时返回 false。
	CurrentOrder() (ConsumeOrder, bool)
	// SubscribeStageSwitch 订阅阶段切换事件:channel 每收到一个事件 push 一份新 ConsumeOrder。
	// channel 永不 close(约定 8);ctx 取消 = 订阅终止;Reader 关闭后返回 ErrClosed。
	SubscribeStageSwitch(ctx context.Context) (<-chan ConsumeOrder, error)
}

// ErrClosed 表示 Reader 已关闭:Close 之后 SubscribeStageSwitch 返回该错误,
// CurrentOrder 返回 false。
var ErrClosed = errors.New("ordersource: Reader 已关闭")
