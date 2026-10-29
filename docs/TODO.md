# docs 待办(小版本更新)

口径：docs 正文只写当前状态与契约，不写“未实现/待补”；缺口只登记未完成的任务，bug 级缺口登记为 issue，不在本文件排队。

## B. 上生产前必做

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| 8.1 §4.3；8.2 P2 | 训练侧钩子（DataLoader 适配器）生产实现与日志契约落地：包 Dataset/IterableDataset，发 advance/stage_switch/open_report/read_miss/read_stall，并按 8.1 §4.3 字段打日志；v0.1.0 至少交付参考适配器 | 真实训练没有客户端接 `agent.sock`；bench 用自己的 PyTorch loader，不影响 9.x 实验 |
| 6.1 §5.9；6.3 D50 | 快照发布方落地：由产生新 entries 的组件调用 `index.Writer.Commit` 并推进 CURRENT；Controller 现有职责不产生索引，Agent 不发布 | 新索引无法发布，只能读既有 CURRENT；发布链路的自开观测防护也无从验证 |
| 5.1 §11.1；`core/cmd/traincacheds/main.go` | 交付 `rm`/`gc` 子命令 | 数据集不能删、不能手动回收；CLI 明确报 `not implemented`，不会静默错删 |
| 5.1 §10.7；5.3 D31 ⑤ | 远端孤儿块清理策略 | 发布/Commit 失败后远端块只增不减；不影响读正确性，但存储持续涨 |
| 6.1 §8；`agent/deploy/agentd.yaml`、`controller/deploy/agent/daemonset.yaml` | 统一两份 Agent DaemonSet 清单 | 同名 `traincache-agentd` 两套 request/limit，部署结果取决于用哪份 |

## C. 做了更完善

本节含原 8.2 缺口清单中裁决为「做了更完善」的项：采样限流、Agent 汇总日志、钩子帧任务身份、告警与看板。

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| 6.2 §13 登记（M7）；6.3 D43；`agent/e2e/run-local.sh` | 4.2/4.3 带 MinIO 跑并转硬断言（MinIO 由 e2e 自带还是外部提供待定） | 没起 MinIO 时记 GAP 且最终 PASS，对象存储回归会被记成已知缺口 |
| 5.4 test.md；`core/objectstore/s3_integration_test.go` | S3 集成测试进 CI：起 MinIO、注入 `TC_S3_*` | 只有 mock 测试，真实 S3 分页与条件写回归发现不了 |
| 6.3 Q15 | `read_stall` 阈值（默认 100ms）、关联窗口（ts±5s）与上报频率标定；按 `(path,chunk)` 去重与全局令牌桶 | 默认值未验证，stall 归因可能偏 |
| 6.1 §8 | Agent 资源数值按 bench 校准 | 示例值可能过紧或过松，影响 OOM、成本与调度 |
| 8.1 §5 | gRPC metadata 三键 `x-traincache-request-id`/`x-traincache-task`/`x-traincache-generation`：Controller 客户端注入，Agent 拦截器写 context logger | controller 与 Agent 日志串不起来，排障靠 `task_id` 手工对 |
| 8.1 §5；6.1 §5.4；8.2 P2 | 钩子帧携带任务身份（`task_id`/`pod`/`dataset`/`generation`），与 gRPC metadata 三键一起把 controller → Agent → 训练侧三段日志串起来 | 同一次任务在训练侧日志里没有归属，只能按时间猜；三段日志对不上号 |
| 8.1 §7 | 采样与限流：令牌桶、去重窗口、`TRAINCACHE_LOG_SAMPLE_RATE`/`TRAINCACHE_LOG_DEBUG_SAMPLE_RATE` | 默认 Info、Debug 关闭时无影响；开 Debug 才可能打满磁盘 |
| 8.1 §4.2 | Agent 逐块下载/校验的 60s 汇总日志 | 逐块明细只有指标与 Debug 日志，排障要自己聚合 |
| 8.1 §4.2；`agent/svc/hooksock.go`、`backfill.go` | 缓存命中/未命中/`read_miss` 的汇总日志（Debug 明细采样可后置） | 命中率只有指标，单任务归因要手工对照 |
| 8.1 §3.1；8.5 §三 | 契约字段接线：`err_class` 与 `sampled`/`sample_rate` 由生产代码发射，`err` 与 `err_class` 成对 | 告警与聚合只能靠 `err` 原文匹配，采样比例无法核对 |
| 8.2 P3；8.1 §6/附录 A | 告警规则 + Grafana 三层看板 + runbook 链接：门控 Hold 超时、Agent 不可达、read stall 超阈值、磁盘危险线、日志采集断流 | 出问题只能人肉查，没有阈值与处置入口 |
| 7.0 inotify「辅助预热」 | 冷启动按 `heatmap.json` 预热历史热块；手动入口为 `traincache prefetch`，排期待裁决 | 冷启动命中率低、首 epoch 慢；功能不坏 |
全局e2e的位置问题

# 附加
标题：degraded 语义缺陷诊断与 v0.1.1 修正方向（v0.1.0）
定位（已定）：订单是提示，不是命令。训练正确性不依赖订单，丢单只损失预取效率；stall_ratio 已能暴露预取失效。因此 v0.1.1 按"语义修正"做，不做重放/ACK/游标持久化的状态机重构；inotify 兜底保持"只做统计"成立。
根本问题：把三件生命周期不同的事折叠成一个 bool —— 订单通道 liveness、读服务 availability、降级期数据 correctness。触发、兜底、恢复、可观测性全部建立在错误前提上。

v0.1.0 现状
单一 0/1 gauge，两个写者互相覆盖；无 mode/原因/持久化。
触发硬编码 30s/1s/200ms；"无 reader = 健康"，空目录、sidecar 缺失、重启清空全报 0。
降级只停 ring 推送；缓存/预取/gRPC/readiness/钩子均无降级分支。
inotify 兜底由 open_report 静默触发，只做统计，不产生订单、不切阶段。
任意一次推进即恢复；resync 跳游标不重放；重启状态清零。
Reader 永不回收，Collector 只绑首个 ring；重启后可能用旧 Pod 残留数据。
文档与实现不符，验收只断言 gauge。

五个根本缺陷
概念折叠：liveness/availability/correctness 共用一个 bool。
触发无身份与生命周期：无法区分训练结束/Pod 重建/sidecar 重启/节点抖动；Reader 无终态、无 lease、无回收。
兜底不是兜底：inotify 是弱化旁路，方向反了（有钩子时不启用，bench 全程启用）。
恢复不是重建：只翻标志位，不重放、不重建会话、不清理 Reader，丢单无计数。
无契约与仲裁：两个写者语义相反，测量侧从不读 gauge，降级可全程发生而跑分照常。

v0.1.1 方向（按"提示"定位）
A. per-source 状态机：Discovered→Healthy→Stalled→Ended，身份 (pod_uid, dataset, generation)，带 lastAdvance + lease，TTL 回收。
B. 派生聚合单一写者：none/healthy/partial/all_stalled，gauge 带 mode；Collector 不写 gauge、不绑单 ring。
C. 明确契约：读服务与订单通道解耦，订单停止只影响预取；fail-fast 做成独立配置项，不混入 degraded 语义；钩子同守此契约。
D. 恢复=追当前状态：新代际/文件重建时重放 CurrentOrder + StageSwitch，把会话追到当前阶段；resync 跳过记 orders_dropped_total{reason}；启动加 debounce（连续两个 poll 健康或 5s 宽限）。
E. inotify 兜底保持"只做统计"，不承担正确性恢复。

执行顺序：先改文档对齐定位（删"命令式"表述、修正 6.1 design 294/554-556 偏差）→ 再做 A/B/C 语义拆分 → 最后补 D 的追当前阶段与 debounce。

