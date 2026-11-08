package plugin

import (
	"fmt"
	"path"
)

// 后端对象布局（design 10.1「数据格式」）。local 插件把同样的相对路径挂在 root 下，
// S3 插件把它拼在 prefix 后；两个实现共用这一组函数，避免键格式分叉。
//
//	<prefix>/<task_id>/step-<step>/rank-<rank>/files/<相对路径>
//	<prefix>/<task_id>/step-<step>/rank-<rank>/manifest.json
func taskPrefix(prefix, taskID string) string {
	return path.Join(prefix, taskID)
}

// StepPrefix 返回一个 step 的公共前缀，resolve 枚举目录时使用。
func StepPrefix(prefix, taskID string, step int64) string {
	return path.Join(taskPrefix(prefix, taskID), fmt.Sprintf("step-%d", step))
}

// RankPrefix 返回一个 rank 的目录前缀。
func RankPrefix(prefix, taskID string, step int64, rank int) string {
	return path.Join(StepPrefix(prefix, taskID, step), fmt.Sprintf("rank-%d", rank))
}

// FilesPrefix 返回文件对象的公共前缀；manifest 里的 path 相对于它。
func FilesPrefix(prefix, taskID string, step int64, rank int) string {
	return path.Join(RankPrefix(prefix, taskID, step, rank), "files")
}

// ManifestKey 是提交标记的键，最后写入。
func ManifestKey(prefix, taskID string, step int64, rank int) string {
	return path.Join(RankPrefix(prefix, taskID, step, rank), "manifest.json")
}

// FileKey 返回单个文件对象的键。rel 必须是 manifest 校验过的相对路径。
func FileKey(prefix, taskID string, step int64, rank int, rel string) string {
	return path.Join(FilesPrefix(prefix, taskID, step, rank), rel)
}
