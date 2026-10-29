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

## [v0.2.0] - 2026-10-29

### Added

- Agent 启动日志级别不再固定为 `info`，改由 `TRAINCACHE_LOG_LEVEL` 决定（默认 `info`）。
- 四个进程新增 localhost-only 的 `/debug/loglevel`：`GET` 读当前级别，`PUT` 改级别即时生效，进程重启回落到 env 值。监听地址由 `CONTROLLER_DEBUG_ADDR`（controller-manager / scheduler / webhook）与 `TRAINCACHE_DEBUG_ADDR`（agentd）给出，默认 `127.0.0.1:6060`，空串关闭。契约见 `docs/8.1 design.md` §7。
- Agent 快照跟随循环在切换成功后补一条 Info（`catalog: 快照切换成功`），与失败侧的 Warn 成对；此前只有失败出声，「没有新快照」和「读不到」在日志里分不开。

### Changed

- 控制面日志的 `assign` 字段由 `"p0=n0"` 字符串改为结构化对象（Pod → 节点数组），可按节点聚合；Agent RPC 的进程内重试循环去掉，失败日志不再带恒为 1 的 `attempt`，退避交给 controller-runtime。
- `CancelPreheat` 撤回该任务还没下完的块：受理预热任务时结一个下载作用域，取消即取消它，块在下一个 ctx 边界（准入排队、源站读、写盘）退出并按失败回调，任务落 `ERROR`、`completed` 保留取消前已落盘的块数。作用域只在任务出表时释放，`DONE` 不释放——DONE 后仍可能收到 `UpdatePreheat` 续窗。再次提交同一 `task_id` 仍视为新任务。契约见 `docs/6.1 design.md` 的 `CancelPreheat` 条目，决策记 `docs/6.3 decisions.md` D62。
- bench 与现勘清单的 MinIO 镜像从 `latest` 钉到固定 release tag（`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`，manifest digest 已 `docker pull` 验证），滚动升级不再带来行为漂移。

### Fixed

- 预热任务在同一窗口上反复取消重提、Agent 每次从块 0 重下，请求 1 GiB 实搬 3–4 GiB、warm 反慢于 cold。四层叠出来的，逐条见 issue #4–#8：
  - 「已经提交过什么」不再从 `status.podNodeReadiness` 反推，改成每轮 `GetPreheatStatus` 问 Agent；`PreheatStatus` 加 `canceled` 字段，被取消的任务不算持有，节点重新选中时会被重新派活而不是静默跳过。Agent 上没有该任务不再算永久拒绝，`Cancel` 收到它按幂等空操作成功。
  - GWAA 目标集合加在册保留（`Input.Held`）：在册且仍在保留带（`2*c_ij ≥ best`，低信号自动满足）的节点不因一块差距被换下，只换不加、停止节点不参与。
  - `Submit`/`Update` 成功后清除该配对在本进程的 `seq` 高水位，重投任务从 1 起的新上报不再被当成陈旧丢弃。
  - 宽预热落选节点的下载随 `CancelPreheat` 撤回（见 Changed）。


## [v0.1.0] - 2026-10-27

首个版本，包含控制面、节点面、缓存库与配套工具。

### Added

- 控制面：`DataSet`/`WarmupTask` CRD、预热编排 Controller、调度器评分插件、Pod 注入 webhook。
- 节点面：`traincache-agentd` DaemonSet、块级缓存、预热执行、gRPC 服务与 inotify 兜底。
- 缓存库：内容寻址存储、索引分片、S3 兼容对象存储读写与条件写。
- 工具：`traincache` CLI（prefetch / node status / status）、快照发布工具、kind e2e 与 bench 装置。
