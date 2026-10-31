# Changelog说明
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。版本号与 git tag 逐字一致，带 `v`；最新版本在最上面。
每完成一项用户可见的改动就补一条，写在 `## 未发布` 下，先不写版本号；发版时把该节标题改成 `## [vX.Y.Z] - 日期`。
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

## [v0.3.2] - 2026-10-31

### Changed

- 下载准入档位改名：`fetch.Priority` → `fetch.Admission`、`FetchWithPriority` → `FetchWithAdmission`、`PriorityForeground`/`PriorityBackground` → `AdmissionReading`（在读：需求读补块与坏块修复）/`AdmissionSpeculative`（预铺：预取含显式预热）。只影响 `traincache/core/fetch` 的导入方；半额额度、让路门、同 key 合并与回源指标口径不变。契约见 `docs/5.1 design.md` §5.3 与 `docs/6.3 decisions.md` D61。

## [v0.3.1] - 2026-10-30

### Added

- 控制面到 Agent 的四个 RPC 新增 gRPC metadata 三键 `x-traincache-request-id`/`x-traincache-task`/`x-traincache-generation`：Controller 客户端注入，Agent 拦截器把三者写进 context logger，两端日志可按同一次请求串联。契约见 `docs/8.1 design.md` §5。
- 钩子帧可携带任务身份 `warmup_task`/`pod`/`pod_index`/`dataset`/`generation`，缺省时行为与旧帧一致；`read_miss` 的补块日志带上这组字段。webhook 相应补注入 `TRAINCACHE_POD`。契约见 `docs/8.0 日志与监控.md` 与 `docs/6.1 design.md` §5.4。
- 日志采样与限流：`TRAINCACHE_LOG_SAMPLE_RATE`（Info，默认 1.0）与 `TRAINCACHE_LOG_DEBUG_SAMPLE_RATE`（Debug，默认 0.01）控制级别采样，配进程内令牌桶（控制面 100 行/秒、Agent 200 行/秒）与 60s 去重窗口；Warn/Error 不采样、不被丢弃。丢弃条数由新增的 `traincache_log_dropped_total{component,level}` 暴露。契约见 `docs/8.1 design.md` §3.3。
- 日志固定带 `sampled`/`sample_rate` 两个字段；带 `err` 的 Warn/Error 固定成对带 `err_class`（`invalid_spec`/`not_found`/`unavailable`/`permanent`/`corrupt`/`io`/`timeout`）。契约见 `docs/8.1 design.md` §3.1。
- Agent 高频路径改为 60s 汇总：逐块下载/校验每窗口一条 Info（块数、字节、失败数、P50/P99），缓存命中/未命中/read_miss 每窗口一条 Info（按 dataset 与 reason）。逐块明细降为 Debug 1/100 采样。契约见 `docs/8.1 design.md` §3.3 与 §4.2。
- observability 装置补齐告警与看板：五条 Prometheus 规则（门控 Hold 超时、Agent 不可达、read stall 超阈值、磁盘危险线、日志断流），每条带 `runbook_url` 指向 `docs/8.1 design.md` 附录 A；Grafana 增加控制平面、Agent、DataLoader 三层看板并从 ConfigMap 自动加载。
- 训练侧 DataLoader 适配器 `agent/hooklib/`：包装 torch 的 `Dataset`/`IterableDataset`，发 advance/stage_switch/open_report/read_miss/read_stall 五帧，并按 `docs/8.1 design.md` §4.3 打 JSON 日志；webhook 注入 `TRAINCACHE_TASK`/`TRAINCACHE_POD`/`TRAINCACHE_POD_INDEX`/`TRAINCACHE_DATASET`/`TRAINCACHE_HOOK_SOCK`/`TRAINCACHE_GENERATION_AT_START`，参考清单把 `/run/traincache` 挂进训练容器。
- `traincacheds gc --orphan-blocks` 清理远端孤儿块：删除不被任何快照引用、且最后修改时间早于 `--orphan-grace`（默认 24h）的块；manifest 或分片读不通时整轮放弃删除。
- `traincacheds` 交付 `rm` 与 `gc`：`rm --path/--prefix` 经 `Writer.CommitChanges` 提交新快照完成删除，`gc` 触发一次 `Collector.Sweep` 回收旧快照；两者默认只预览，带 `--yes` 才执行。

### Changed

- 节点分数注解从 Node 迁到训练 Pod，键名由 `traincache.io/scores` 改为 `traincache.io/node-scores`；controller 与 scheduler 的 `CONTROLLER_SCORE_ANNOTATION` 必须同值。放行在同一次 Pod patch 内写分数并摘 gate，Scheduler 不再有读不到分数的窗口。契约见 `docs/7.1 design.md` 与 `docs/PARAMETER-CONFIGURATION.md`（D49 取代 D27）。
- `read_stall` 归因关联窗口从固定 ±5s 改为可配：`TRAINCACHE_READ_STALL_WINDOW_MS`，默认 `5000`，表示以帧 `ts` 为中心前后各该毫秒数。契约见 `docs/PARAMETER-CONFIGURATION.md`。
- Agent 的部署清单只剩 `controller/deploy/agent/daemonset.yaml` 一份，kind e2e 改为 apply 该清单后替换镜像引用；`agent/deploy/agentd.yaml` 删除，同名 DaemonSet 两套 request/limit 并存的问题消失。Agent 内存 limit 为 4Gi。

## [v0.3.0] - 2026-10-29

### Added

- Agent 新增订单通道观测指标：`traincache_agent_ordersources{node,state}` 给出每路订单源处于 discovered / healthy / stalled，`traincache_agent_ordersource_ended_total{node,reason}` 记录订单源被收回（`missing_ttl` / `stale_file`），`traincache_orders_dropped_total{reason}` 记录跳读、损坏槽与空订单（`resync` / `corrupt` / `invalid`）。契约见 `docs/6.3 decisions.md` D63。

### Changed

- 订单通道降级改为观测语义：`traincache_agent_degraded{node}` 只在全部订单源都 stalled 时为 1，不再表示「停止推送订单」，降级期间订单照常下发；sidecar 退出或 ring 停止推进 30 秒后该路标 stalled，任一路恢复推进自动解除。inotify 只做统计，不计入恢复。

### Fixed

- 宽预热在训练 Pod 尚未创建时把 `PodSetMismatch` 置为 True，日志与告警被刷屏；现在 Pod 集为空只记 Debug 并清除该条件，只有部分不匹配才 Warn。
- 窗口是否变化改按 `window_blocks` 判定，不再比较 bitmap 字节数，字节数相同的不同窗口不再被当成同一窗口。

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
