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
