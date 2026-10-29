package svc

// errclass.go:把 svc 日志里出现的错误映射到 err_class 枚举(docs/8.1 §3.1)。
// 这里只做分类,不改错误处理;调用点在 err 旁补 err_class,聚合与告警按类统计。

import (
	"errors"

	"traincache/core/cas"
	corelog "traincache/core/logging"
	"traincache/core/objectstore"
)

// errClass 把 svc 常见错误映射到固定枚举:读坏块 → corrupt,块/对象不存在 →
// not_found;其余(本地 I/O、上下文等)回退 core 的通用分类。
//
// 映射依据 backfill.go 实际出现的错误:cas.Open 与 RepairOne 返回 cas.ErrCorrupt/
// cas.ErrNotFound,fetch 回源透传 objectstore.ErrNotFound,其余是本地 I/O 失败。
func errClass(err error) string {
	switch {
	case errors.Is(err, cas.ErrCorrupt):
		return corelog.ErrClassCorrupt
	case errors.Is(err, cas.ErrNotFound), errors.Is(err, objectstore.ErrNotFound):
		return corelog.ErrClassNotFound
	}
	return corelog.Classify(err)
}
