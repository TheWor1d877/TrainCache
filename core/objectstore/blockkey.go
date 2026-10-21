package objectstore

import "strconv"

// BlockKey 返回数据块在对象存储中的源站键：
// <dataset>/blocks/<path>#<chunk>。
//
// 前置条件：dataset 与 path 均非空，且 path 不得包含 '#'；chunk 为 int64
// 十进制编码。函数不做任何转义，调用方负责保证路径满足上述约定。
//
// 该布局与 agent/prefetch 及 agent/rescan 中的块键格式逐字一致。
func BlockKey(dataset, path string, chunk int64) string {
	return dataset + "/blocks/" + path + "#" + strconv.FormatInt(chunk, 10)
}
