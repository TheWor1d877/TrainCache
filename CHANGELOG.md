# Changelog说明
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。版本号与 git tag 逐字一致，带 `v`；最新版本在最上面。
只在发版时更新：`dev` 的日常提交不改本文件；`dev` 合并到 `main`、打 `vX.Y.Z` 之前，补该版本条目并填发布日期。
## 写法示例
下面的代码块只演示格式，不是发布记录。分组按改动性质从 Added / Changed / Deprecated / Removed / Fixed / Security 中选，没有就不写该组。
```markdown
## [v0.2.0] - 2026-12-01

### Added
- `WarmupTask` 新增按块预热开关 `spec.blockWarmup`，只预热训练首个窗口。字段契约见 `docs/9.2 实验协议.md` §C。

### Changed
- Agent 默认缓存上限从固定 1GiB 改为节点内存的 10%。

### Fixed
- 节点重启后同一块被重复预取，导致命中率统计偏高。
```

# Changelog正文

## [v0.1.0] - 2026-10-27

首个版本，包含控制面、节点面、缓存库与配套工具。

### Added

- 控制面：`DataSet`/`WarmupTask` CRD、预热编排 Controller、调度器评分插件、Pod 注入 webhook。
- 节点面：`traincache-agentd` DaemonSet、块级缓存、预热执行、gRPC 服务与 inotify 兜底。
- 缓存库：内容寻址存储、索引分片、S3 兼容对象存储读写与条件写。
- 工具：`traincache` CLI（prefetch / node status / status）、快照发布工具、kind e2e 与 bench 装置。
