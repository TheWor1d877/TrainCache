# Changelog说明
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。版本号与 git tag 逐字一致，带 `v`；最新版本在最上面。
只在发版时更新：`dev` 的日常提交不改本文件；`dev` 合并到 `main`、打 `vX.Y.Z` 之前，补该版本条目并填发布日期。
## 写法示例
下面的代码块只演示格式，不是发布记录。分组按改动性质从 Added / Changed / Deprecated / Removed / Fixed / Security 中选，没有就不写该组。
```markdown
## [v9.9.9] - 2026-12-01

### Added
- `WarmupTask` 新增按块预热开关 `spec.blockWarmup`，只预热训练首个窗口。字段契约见 `docs/9.2 实验协议.md` §C。

### Changed
- Agent 默认缓存上限从固定 1GiB 改为节点内存的 10%。

### Fixed
- 节点重启后同一块被重复预取，导致命中率统计偏高。
```

# Changelog正文

## [未发布]

### Added

- 训练侧 DataLoader 适配器 `agent/hooklib/`：包装 torch 的 `Dataset`/`IterableDataset`，发 advance/stage_switch/open_report/read_miss/read_stall 五帧，并按 `docs/8.1 design.md` §4.3 打 JSON 日志；webhook 注入 `TRAINCACHE_TASK`/`TRAINCACHE_POD_INDEX`/`TRAINCACHE_DATASET`/`TRAINCACHE_HOOK_SOCK`/`TRAINCACHE_GENERATION_AT_START`，参考清单把 `/run/traincache` 挂进训练容器。
- `traincacheds gc --orphan-blocks` 清理远端孤儿块：删除不被任何快照引用、且最后修改时间早于 `--orphan-grace`（默认 24h）的块；manifest 或分片读不通时整轮放弃删除。
- `traincacheds` 交付 `rm` 与 `gc`：`rm --path/--prefix` 经 `Writer.CommitChanges` 提交新快照完成删除，`gc` 触发一次 `Collector.Sweep` 回收旧快照；两者默认只预览，带 `--yes` 才执行。

### Changed

- Agent 的部署清单只剩 `controller/deploy/agent/daemonset.yaml` 一份，kind e2e 改为 apply 该清单后替换镜像引用；`agent/deploy/agentd.yaml` 删除，同名 DaemonSet 两套 request/limit 并存的问题消失。Agent 内存 limit 为 4Gi。


