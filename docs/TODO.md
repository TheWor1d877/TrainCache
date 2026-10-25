# docs 待办

## A. bug 级

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| 5.3 D32；8.1 §3 | 脱敏生产调用点接线：controller/agent 落日志前调 `Redact`/`RedactURL`；Agent 补哨兵 Secret 负向断言 | 凭证可能随错误信息或 URL 进日志与 Loki；当前生产路径零调用，只有定义与单测 |

## B. 上生产前必做

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| 8.1 §4.3 | DataLoader 生产实现与日志契约落地 | 真实训练没有客户端接 `agent.sock`；bench 用自己的 PyTorch loader，不影响 9.x 实验 |
| 5.1 §11.1；`core/cmd/traincacheds/main.go` | 交付 `rm`/`gc` 子命令 | 数据集不能删、不能手动回收；CLI 明确报 `not implemented`，不会静默错删 |
| 5.1 §10.7；5.3 D31 ⑤ | 远端孤儿块清理策略 | 发布/Commit 失败后远端块只增不减；不影响读正确性，但存储持续涨 |
| 6.1 §8；`agent/deploy/agentd.yaml`、`controller/deploy/agent/daemonset.yaml` | 统一两份 Agent DaemonSet 清单 | 同名 `traincache-agentd` 两套 request/limit，部署结果取决于用哪份 |

## C. 做了更完善

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| 6.2 §13 登记（M7）；6.3 D43；`agent/e2e/run-local.sh` | 4.2/4.3 带 MinIO 跑并转硬断言（MinIO 由 e2e 自带还是外部提供待定） | 没起 MinIO 时记 GAP 且最终 PASS，对象存储回归会被记成已知缺口 |
| 5.4 test.md；`core/objectstore/s3_integration_test.go` | S3 集成测试进 CI：起 MinIO、注入 `TC_S3_*` | 只有 mock 测试，真实 S3 分页与条件写回归发现不了 |
| 6.3 Q15 | `read_stall` 阈值（默认 100ms）、关联窗口（ts±5s）与上报频率标定；按 `(path,chunk)` 去重与全局令牌桶 | 默认值未验证，stall 归因可能偏 |
| 6.1 §8 | Agent 资源数值按 bench 校准 | 示例值可能过紧或过松，影响 OOM、成本与调度 |
| 8.1 §5 | gRPC metadata 三键 `x-traincache-request-id`/`x-traincache-task`/`x-traincache-generation`：Controller 客户端注入，Agent 拦截器写 context logger | controller 与 Agent 日志串不起来，排障靠 `task_id` 手工对 |
| 8.1 §7 | 采样与限流：令牌桶、去重窗口、`TRAINCACHE_LOG_SAMPLE_RATE`/`TRAINCACHE_LOG_DEBUG_SAMPLE_RATE` | 默认 Info、Debug 关闭时无影响；开 Debug 才可能打满磁盘 |
| 7.0 inotify「辅助预热」 | 冷启动按 `heatmap.json` 预热历史热块；手动入口为 `traincache prefetch`，排期待裁决 | 冷启动命中率低、首 epoch 慢；功能不坏 |

## D. 做不做都行

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| 5.3 D34 | 新增 `traincachepublish`，`trainpublish` 留兼容别名一个版本；同步 Makefile、bench 脚本与 design §10.3 的 usage 文本 | 命名一致性，无功能影响 |
| 8.1 §4.2 | Agent 逐块下载/校验的 60s 汇总日志 | 指标已有逐块计数，汇总日志是重复信息 |
| `controller/internal/controller/warmup/place.go:263`；8.1 §4.1 | gRPC 重试循环与 `attempt` 语义 | controller-runtime 已按 backoff 重试 `ErrAgentUnavailable`；剩下只是 `attempt` 恒 1 的日志准确性 |
| 8.1 §4.2；`agent/svc/hooksock.go`、`backfill.go` | 缓存命中/未命中/`read_miss` 的 Debug 采样日志 | 指标已覆盖，日志只是 Debug 便利 |
| 8.4 test.md；`agent/svc/follow.go` | 快照刷新成功路径补 Info，并与失败成对断言 | 失败已有 Warn，成功不打不影响排障 |
| 8.1 §7；8.3 §3 | 动态日志级别：载体在 localhost `/debug/loglevel` 与 ConfigMap 热加载之间二选一 | 改级别重启一次即可 |
