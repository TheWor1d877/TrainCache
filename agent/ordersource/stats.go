package ordersource

import "sync/atomic"

// 格式错误 kind(decisions D23:traincache_ordersource_format_errors_total 的 reason 标签取值)。
const (
	errKindMagic   = "magic"   // magic 读不出或不是 "TCO1"
	errKindVersion = "version" // version / 几何非法(D22 上限:slot_size∈[16,64MiB]、slot_count∈[1,1024])
)

// 包级累计计数(atomic):svc 每 tick 以绝对值同步到 Prometheus counter(D23)。
// 去重口径:同一文件同一坏头状态只计一次——初验失败计一次,
// 此后仅当文件重开 / 代际变化后的复验再失败再计(由 Reader.badKind 保证)。
var (
	formatErrorsMagic   atomic.Int64
	formatErrorsVersion atomic.Int64
)

// 丢单累计计数(design §7.4 / decisions D63):svc 每 tick 以绝对值同步到
// traincache_orders_dropped_total{reason}(reason ∈ {resync, corrupt, invalid})。
// 计数为节点级累计,不提供重置接口。
var (
	droppedResync  atomic.Int64 // 已接入 Reader 跳读最新时被跳过的序号数(仅正数)
	droppedCorrupt atomic.Int64 // 损坏槽被跳过的次数(同一槽只计一次)
	droppedInvalid atomic.Int64 // 解析成功但 chunks 为空的订单数
)

// DroppedTotals 返回丢单累计计数,顺序固定:
//   - resync:已接入 Reader 因代际变化 / 写端回退等原因跳读最新时跳过的序号数;
//     首次接入跳过的历史不计数(尚无消费者);
//   - corrupt:槽载荷 JSON 解析失败且被跳过的次数(同一槽只计一次);
//   - invalid:解析成功但 chunks 为空的订单数(仍更新当前订单)。
func DroppedTotals() (resync, corrupt, invalid int64) {
	return droppedResync.Load(), droppedCorrupt.Load(), droppedInvalid.Load()
}

// countDroppedResync 按跳过序号数累加 resync 丢单。
func countDroppedResync(skipped int64) {
	if skipped > 0 {
		droppedResync.Add(skipped)
	}
}

// countDroppedCorrupt 累加损坏槽跳过一次。
func countDroppedCorrupt() {
	droppedCorrupt.Add(1)
}

// countDroppedInvalid 累加空订单一次。
func countDroppedInvalid() {
	droppedInvalid.Add(1)
}

// FormatErrorTotals 返回格式错误累计计数(magic、version 两枚,D23)。
// 只统计 magic/version 两类;槽 len 非法与载荷 JSON 解析失败不计数(只记日志)。
func FormatErrorTotals() (magic, version int64) {
	return formatErrorsMagic.Load(), formatErrorsVersion.Load()
}

// countFormatError 按 kind 累加;未知 kind 忽略。
func countFormatError(kind string) {
	switch kind {
	case errKindMagic:
		formatErrorsMagic.Add(1)
	case errKindVersion:
		formatErrorsVersion.Add(1)
	}
}
