# docs 待办(小版本更新)

口径：docs 正文只写当前状态与契约，不写“未实现/待补”；所有缺口统一登记在本文件。

## A. bug 级

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| `docs/RELEASE-CONSTRAINTS.md` §4；`.github/workflows/e2e.yml` | 改镜像名与 registry 后未跑 e2e：kind 集群未重建，四个镜像未重编，只做了 `bash -n` | e2e 清单与脚本的前缀改动没有经过真实集群验证，发版前“必过”项处于未知状态 |
| `agent/cmd/traincache/main.go`；`docs/CLI 接口.md` §2 | `prefetch --chunks` 必填但不进请求：只做非空校验，实际按 `--path` 整个前缀预热 | 用户按块预热的请求变成整前缀预热，流量与缓存占用被放大，且 CLI 不回显这一差异 |
| `release.yml`、`release-smoke.yml`；`docs/RELEASE-CONSTRAINTS.md` §4 | release-smoke 未跑：需要先有 `v0.1.0` tag 与 GHCR 发布物 | 推镜像、digest 渲染、Release 资产与装机链路从未端到端执行，首次发版才暴露问题 |
| 5.3 D32；8.1 §3 | 脱敏生产调用点接线：controller/agent 落日志前调 `Redact`/`RedactURL`；Agent 补哨兵 Secret 负向断言 | 凭证可能随错误信息或 URL 进日志与 Loki；当前生产路径零调用，只有定义与单测 |

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

## D. 做不做都行

| 出处 | 事项 | 不做的后果 |
|------|------|------------|
| 5.3 D34 | 新增 `traincachepublish`，`trainpublish` 留兼容别名一个版本；同步 Makefile、bench 脚本与 design §10.3 的 usage 文本 | 命名一致性，无功能影响 |
| `controller/internal/controller/warmup/place.go:263`；8.1 §4.1 | gRPC 重试循环与 `attempt` 语义 | controller-runtime 已按 backoff 重试 `ErrAgentUnavailable`；剩下只是 `attempt` 恒 1 的日志准确性 |
| 8.4 test.md；`agent/svc/follow.go` | 快照刷新成功路径补 Info，并与失败成对断言 | 失败已有 Warn，成功不打不影响排障 |
| 8.1 §7；8.3 §3 | 动态日志级别：载体在 localhost `/debug/loglevel` 与 ConfigMap 热加载之间二选一 | 改级别重启一次即可 |
| 8.1 §4.1；`controller/internal/controller/warmup` | GWAA 日志 `assign` 从字符串改为结构化对象 | 分配结果只能人读，不能按节点/动作聚合 |
| 6.1 §5.12；6.2 task 4；`bench/k8s/minio.yaml` | 固定 MinIO 镜像做一次拉取验证 | 离线或受限网络首次 `bench up` 才暴露镜像不可拉 |


# 附加(大版本更新)

## 降级策略问题

根本缺陷不是某个 if 写错，而是把三件生命周期完全不同的事折叠成了一个 bool：

- 订单通道是否活着（liveness）
- agent 对训练侧读服务是否可用（availability）
- 降级期间数据是否仍然正确（correctness）

v0.1.0 用一个 traincache_agent_degraded gauge 同时表达这三件事，于是触发、兜底、恢复、可观测性全都建立在错误的前提上。今天那次 void 就是典型：agentd 刚重启 14 秒，订单通道还没稳定，但读服务照样开门，测量体拿不到数据 300 秒，而 bench 完全不看这个 gauge。

v0.1.0 实际是什么样

- 状态：只有一个 0/1 gauge，两个写者——Collector 在跳变时写，OrderDiscovery 每秒无条件写并覆盖前者。没有 mode、原因、per-source 状态，也没有持久化。
- 触发：单条 ring 30 秒没有 write_seq 推进、文件消失、或 header 从未有效。硬编码 30s/1s/200ms，不可配。
- 无 reader = 健康：订单目录为空、sidecar 从未出现、agentd 重启后目录被清空，全部报 0。只有「曾经有 ring 然后停了」才会报 1。
- 降级后：只停 ring 订单推送。缓存读写、预取、淘汰、gRPC、readiness 都没有降级分支；钩子的 advance/StageSwitch 也不受降级闸门约束。
- 兜底：inotify 不是由 degraded 触发，而是由钩子 open_report 静默 30 秒触发；它只推进游标和热度，不产生订单、不切换阶段、不更新预取窗口。
- 恢复：任意一次推进就把标志位翻回 0；resync 直接把游标跳到最新槽，中间订单不重放；agentd 重启后状态清零重算。
- 生命周期：Reader 永不删除，旧 Pod 的 ring 永久占位；Collector 只绑首个发现的 ring，永不换绑。agentd 重启后首个 ring 若是旧 Pod 残留，热度、命中率、指纹反查都会用陈旧订单。
- 文档偏差：docs/6.1 design.md 294 说「socket 缺席转降级」、554-556 说「不再依赖 OrderSource、改用 inotify 驱动」，实现都不是这样。验收 10 只断言 gauge，所以偏差一直没暴露。

五个根本缺陷
1. 概念折叠。liveness、availability、correctness 的恢复条件不同，却共用一个 bool。读服务本来不依赖订单，却被这个 bool 间接影响；订单通道坏了，读服务却继续静默服务。
2. 触发没有身份和生命周期。30 秒停滞无法区分「训练结束」「Pod 重建」「sidecar 重启」「节点抖动」。Reader 没有终态、没有 lease、没有回收，导致「全部训练结束」和「全部 sidecar 挂了」看起来一样。
3. 兜底不是兜底。inotify 是另一条弱化路径，不是主路径的恢复。它只能做统计，不能恢复预取；门控条件还是 open_report 静默而不是 ring 健康，方向反了——生产有钩子时它不启用，bench 没钩子时它全程启用。
4. 恢复不是重建。只翻标志位，不重放订单、不重建会话、不清理旧 Reader。丢单没有计数器，只有 Debug 日志。
5. 没有契约和仲裁。两个写者语义相反（无 reader 健康 vs nil ring 降级），测量侧从不读取 gauge，降级可以全程发生而跑分结果照常产出。

v0.1.1 的优化方向

先把「降级」拆成三个显式概念，再谈实现：

A. per-source 状态机
每个 order source 一个状态：Discovered → Healthy → Stalled → Ended。身份用 (pod_uid, dataset, generation)，带 lastAdvance 和 lease。Pod 消失 + TTL 到期进入 Ended 并回收 Reader，不再永久占位。

B. 派生聚合，单一写者
聚合结果不再是 bool，而是 none / healthy / partial / all_stalled。gauge 改成带 mode 标签或 enum，只由聚合层写一次。Collector 不再写 gauge，也不再绑定单条 ring。

C. 明确服务契约
读服务与订单通道解耦：订单停止只影响预取，不影响读。若要 fail-fast，单独做成配置项，不要混进 degraded 语义。钩子路径同样服从这个契约。

D. 恢复 = 重建会话
新代际或文件重建时，至少重放 CurrentOrder + StageSwitch；resync 跳过的订单记 orders_dropped_total{reason}。启动加 debounce（连续两个 poll 健康或 5 秒宽限），避免今天这种「刚重启就判降级」的假状态。

E. 止血项（不等 v0.1.1）
- bench 每轮开跑前等 agent 的 order source 进入 healthy（有 reader 推进且 gauge=0），最多等 N 秒；等不到就标 void。
- 测量期间抓 traincache_agent_degraded，非 0 直接写进 summary.csv 并标 void，不让降级污染数据。
- 无 reader 时 gauge 不要报 0，改成 unknown/none，至少日志里能看见。

需要你先定的一件事

订单在你的模型里是「提示」还是「命令」？

- 若是提示：训练正确性不依赖订单，丢单只损失预取效率。那么 v0.1.1 可以做轻量状态机 + 可观测性 + 恢复追当前阶段，inotify 兜底保持「只做统计」也成立。
- 若是命令：订单必须到达，那就需要重放、ACK、游标持久化，inotify 不能作为正确性兜底，整个降级语义要重写。

这个定位决定了 v0.1.1 是「语义修正」还是「状态机重构」。我倾向按「提示」定，因为训练循环本身不依赖订单，且 stall_ratio 已经把预取失效暴露出来了；但这是你的模型，先定这个再动文档和代码。



## 其他问题 
1. fetch 限流无优先级、无带宽预留（最严重）
   位置：core/fetch/limiter.go，prefetch 队列在 core/fetch 的调度侧。
   现象：prefetch 任务 8 个并发，需求读（P0 demand）与预取共用同一个 MaxInflightBytes=1GiB 的在途额度；预取把唯一 MinIO 出口占满时，需求读只能排队。
   数据（cold 探针）：首片 252MiB 串行块读耗 76.6s，独占 12.5MB/s 应约 21s；启动 80.0s 里 96% 是这段首片读；整轮取数 2.12GB 中 1.96GB（92%）归因 reason=prefetch。
   建议：有未完成的 P0 需求块时压低 prefetch 并发（如 8→2），或按字节比例给需求读保底带宽（如 50%）。预期首片 76.6s→约 21s，启动 80s→约 25s，优于 direct 的 36.9s。

2. gate tMax 硬编码 5m，与带宽档位无关
   位置：bench/compare/traincache/up.sh 的 GATE_TMAX。
   现象：预热字节 = K(128) × 8MiB × fanout(4) = 4GiB；100 mbit 单出口需约 344s，超过默认 300s，S-100 warm 三次进 Degraded。目前靠 TRAINCACHE_GATE_TMAX=7m 人工绕过。
   建议：默认值由「档位带宽 × fanout × K × chunk」推导，脚本自动算，不写死。

3. 全局衰减就绪度参数自相矛盾
   位置：up.sh 的 score.cutoff: 8 与 WARMUP_K=128；算法在 core/nodescore 的 Compute。
   现象：w(i) 在 i ≥ C 时为 0，C=8 意味着只统计前 8 块；衰减式 0.5^((i-K)/H) 的指数部分恒为负、段为空。所谓“全局衰减就绪度”实际退化成“前 8 块就绪度”。
   影响：只影响调度偏好（权重 10），不影响 gate 放行，所以没影响本轮数据有效性，但语义错了。
   建议：C ≥ K（如 144），并在 CRD 校验里约束或告警。

4. check_images 单次探测会误报
   位置：bench/compare/_common.sh 的 check_images。
   现象：镜像刚导入、containerd 元数据未完全同步时会报 traincache-worker 缺 controller-manager，up 直接失败；手工 crictl images -q 随即正常。cold 探针第一次 up_failed 就是这里。
   建议：重试 3 次、间隔 2s；或改判 ImagePullBackOff 事件而不是查 crictl。

5. 驱动脚本把作废行当成功
   位置：bench/ 下 M7 驱动脚本（现临时版已修，未沉淀）。
   现象：只看 run_bench 退出码，up_failed 行仍算 OK，导致连续作废行计入“已有行”，挡住重跑。
   建议：run_bench 输出含 void 行时返回非零；去重只认有效行。时脚本里。