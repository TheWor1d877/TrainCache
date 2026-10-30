# 参数配置

面向部署和提交训练任务的人。只列可改的参数、默认值和生效方式；内部实现见 [6.1 design](6.1%20design.md) 与 [7.1 design](7.1%20design.md)。

## 修改入口速查

先定改哪里，再看后文取默认值。

| 想改什么                                     | 改哪里                                                                                                                                                                      | 生效方式                                                        |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| 缓存字节上限 `cacheCapacityBytes`            | ConfigMap `traincache-config`（`controller/deploy/agent/configmap.yaml`）                                                                                                   | 改 key 后重启 Agent Pod，不重启不生效                           |
| 预取提前量 `prefetchLookahead`               | 同上                                                                                                                                                                        | 同上                                                            |
| 淘汰策略 `evictPolicy`                       | 同上                                                                                                                                                                        | 同上                                                            |
| 指标数据集白名单 `datasetMetricsWhitelist`   | 同上                                                                                                                                                                        | 同上                                                            |
| 对象存储端点、桶、AK/SK                      | Agent：`controller/deploy/agent/daemonset.yaml` 的 env，AK/SK 引自 Secret `traincache-objstore`；控制面：DataSet YAML 的 `spec.endpoint` / `spec.bucket` / `spec.secretRef` | apply 后 Agent Pod 重建；DataSet 改动对之后创建的任务生效       |
| 组件资源 requests/limits                     | `manager/deployment.yaml`、`webhook/deployment.yaml`、`scheduler/deployment.yaml`、`agent/daemonset.yaml`                                                                   | apply，Pod 重建                                                 |
| Agent 校验级别、缓存目录、监听端口           | Agent DaemonSet env                                                                                                                                                         | apply，Agent Pod 重建                                           |
| Agent 热度衰减与磁盘水位                     | Agent DaemonSet env 的 `TRAINCACHE_HEAT_*`                                                                                                                                  | apply，Agent Pod 重建                                           |
| Controller 与 Scheduler 参数                 | manager / webhook / scheduler 三个 Deployment 的 env                                                                                                                        | apply，Deployment 滚动                                          |
| 块大小                                       | `trainpublish --chunk-size`，只在空桶首次发布时写入 `_traincache/format.json`                                                                                               | 之后每次发布沿用；改值要清空桶或换新桶                          |
| DataSet 字段                                 | DataSet YAML                                                                                                                                                                | 重新 apply；发布新快照后同步 `spec.snapshotID` 与 `spec.shards` |
| 预热任务参数                                 | WarmupTask YAML 的 `spec`                                                                                                                                                   | 重新 apply；`dataSetRef` 与 `podCount` 不可改                   |
| 发布器并发、临时目录、校验开关               | `trainpublish` 命令行 flag                                                                                                                                                  | 下次发布生效                                                    |

集群运行时只有六个参数在 ConfigMap：`cacheCapacityBytes`、`prefetchLookahead`、`evictPolicy`、`datasetMetricsWhitelist`、`startupWindowBytes`、`maxFillFraction`。其余参数都不在 ConfigMap 里，往 ConfigMap 加 key 不会被读取。

长期修改要改仓库 YAML 再 apply。`kubectl edit` 只改集群副本，下次 apply 会被覆盖。

## 1. 配置分三层

| 层 | 改哪里 | 生效方式 |
|---|---|---|
| 集群运行时 | ConfigMap `traincache-config`、Deployment/DaemonSet | 改 ConfigMap 后滚动重启 Agent；改资源直接 apply |
| 数据集 | `trainpublish` 参数、DataSet CRD | 发布新快照；DataSet 重新 apply |
| 训练任务 | WarmupTask CRD | 重新 apply；`dataSetRef` 与 `podCount` 不可改 |

## 2. 集群运行时

### 2.1 ConfigMap traincache-config

位置 `controller/deploy/agent/configmap.yaml`，namespace `traincache`。Agent 读 `prefetchLookahead`、`evictPolicy`、`datasetMetricsWhitelist`、`cacheCapacityBytes`；Controller 读 `cacheCapacityBytes`、`startupWindowBytes`、`maxFillFraction`。

| key | 默认 | 说明 |
|---|---|---|
| `cacheCapacityBytes` | `"0"` | 本地缓存字节上限，必须 `>=0`。`0` 关闭字节维度，只按磁盘水位（80% 触发、90% 紧急）淘汰；填字节数（1GiB = `"1073741824"`）后，字节占用先到阈值就先淘汰 |
| `prefetchLookahead` | `"16"` | 节点侧游标之后持续预取的块数。调大提前量，预热流量和缓存占用随之增加。它不是 WarmupTask 的 `podDetails[].k` |
| `evictPolicy` | `"heat"` | 缓存淘汰顺序，取 `heat`、`lru`、`lfu`。非法值使 Agent 拒绝启动 |
| `datasetMetricsWhitelist` | `""` | 逗号分隔的 DataSet 名，只决定指标 `dataset` 标签是否保留原名；为空时全部归并 `other`。不影响 Agent 读哪个数据集 |
| `startupWindowBytes` | `"536870912"` | 启动窗口字节数，默认 512MiB，必须 `>0`。只用于自动算 K：`K_window = ceil(startupWindowBytes / chunkSize)` |
| `maxFillFraction` | `"0.125"` | 全部 Pod 的预热总量占缓存容量的上限，默认八分之一，取值必须在 `(0,0.25]`。按 `podCount` 均摊：`K_cache = floor(cacheCapacityBytes × maxFillFraction / (podCount × chunkSize))`；`cacheCapacityBytes=0` 时不参与计算 |

这个 ConfigMap 只放这六个 key。`cacheCapacityBytes` 由 Agent 与 Controller 共同读取，改它要同时重启两者。改仓库文件后 apply，再重启读取方：

```bash
kubectl apply -f controller/deploy/agent/configmap.yaml
kubectl -n traincache rollout restart ds/traincache-agentd
kubectl -n traincache rollout status ds/traincache-agentd
kubectl -n traincache rollout restart deploy/traincache-controller-manager
kubectl -n traincache rollout status deploy/traincache-controller-manager
```

只在集群里 `kubectl edit` 也能生效，但下次 apply 会被仓库文件覆盖。

六个 key 都必须存在。缺任一 key，对应读取方 Pod 停在 `CreateContainerConfigError`：Agent 读 `cacheCapacityBytes`、`prefetchLookahead`、`evictPolicy`、`datasetMetricsWhitelist`，manager 读 `cacheCapacityBytes`、`startupWindowBytes`、`maxFillFraction`；缺 `cacheCapacityBytes` 时两者都起不来。缺失不回退到默认值。

`datasetMetricsWhitelist` 只控制指标标签。清空后所有带 `dataset` 标签的指标都记到 `other`，按数据集拆的看板看不出差异；排查单个数据集时把名字加回来。

### 2.2 对象存储

Agent 在 `controller/deploy/agent/daemonset.yaml` 的 env 里配置；控制面在 DataSet CRD 里配置。两边必须指向同一个桶。

| 位置 | 参数 | 改哪里 | 默认 | 说明 |
|---|---|---|---|---|
| Agent | `TRAINCACHE_OBJSTORE_ENDPOINT` | DaemonSet env | `127.0.0.1:9000` | S3 兼容端点，不带 scheme |
| Agent | `TRAINCACHE_OBJSTORE_ACCESS_KEY` | DaemonSet env，值取自 Secret `traincache-objstore` 的 `accessKey` | 空 | 静态 AK，生产用 Secret 注入 |
| Agent | `TRAINCACHE_OBJSTORE_SECRET_KEY` | DaemonSet env，值取自 Secret `traincache-objstore` 的 `secretKey` | 空 | 静态 SK |
| Agent | `TRAINCACHE_OBJSTORE_BUCKET` | DaemonSet env | `traincache` | 单桶；须等于 `DataSet.spec.bucket`（`SubmitPreheat` 不带桶名，不一致时每个块读取都失败） |
| Agent | `TRAINCACHE_OBJSTORE_SECURE` | DaemonSet env | `false` | `true` 时用 HTTPS |
| Agent | `TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE` | DaemonSet env | `false` | 启动时探测后端条件写 |
| DataSet | `spec.endpoint` / `spec.bucket` / `spec.secretRef` | DataSet YAML | 无 | 控制面读这三个字段；Secret 字段 `accessKey`、`secretKey`，与 DataSet 同 namespace |

默认列是程序内置值。Agent 清单按集群覆盖成实际端点与桶名，以清单为准。

节点要能访问对象存储，并打上 `traincache.io/s3-access=true`。Controller 只在这个标签为 true 的节点里选预热目标，标签由管理员手工维护。

### 2.3 资源

四个组件各改自己的清单，按集群规模调整：

| 组件 | 改哪里 | requests | limits |
|---|---|---|---|
| controller-manager | `controller/deploy/manager/deployment.yaml` | 50m / 128Mi | 512Mi（无 CPU 上限） |
| webhook | `controller/deploy/webhook/deployment.yaml` | 50m / 64Mi | 256Mi（无 CPU 上限） |
| kube-scheduler | `controller/deploy/scheduler/deployment.yaml` | 100m / 128Mi | 512Mi（无 CPU 上限） |
| traincache-agentd | `controller/deploy/agent/daemonset.yaml` | 100m / 256Mi | 1Gi（无 CPU 上限） |

Agent 的缓存落在节点 hostPath `/var/cache/traincache`，不占容器内存；`limits.memory` 限制的是 Agent 进程本身。缓存大小由 `cacheCapacityBytes` 和磁盘水位控制。

### 2.4 Agent 高级参数

这些只写在 Agent DaemonSet env 里，不常用：

| 变量 | 改哪里 | 默认 | 说明 |
|---|---|---|---|
| `TRAINCACHE_EVICT_POLICY` | DaemonSet env | `heat` | 淘汰策略：`heat` / `lru` / `lfu`；非法值拒绝启动 |
| `TRAINCACHE_VERIFY` | DaemonSet env | `xxh3` | 校验级别：`xxh3` 或 `xxh3+sha256` |
| `TRAINCACHE_CACHE_ROOT` | DaemonSet env | `/var/cache/traincache` | 缓存根目录，必须与 DaemonSet 的 hostPath 挂载一致 |
| `TRAINCACHE_GRPC_ADDR` | DaemonSet env | `:9410` | Agent gRPC 监听地址 |
| `TRAINCACHE_METRICS_ADDR` | DaemonSet env | `:9411` | 指标监听地址 |
| `TRAINCACHE_ORDERS_DIR` | DaemonSet env | `/run/traincache/orders` | OrderSource ring 目录 |
| `TRAINCACHE_INDEX_REFRESH_INTERVAL` | DaemonSet env | `5s` | 快照跟随刷新周期 |
| `TRAINCACHE_READ_STALL_MIN_WAIT_MS` | DaemonSet env | `100` | `read_stall` 入直方图的最小等待毫秒 |
| `TRAINCACHE_READ_STALL_WINDOW_MS` | DaemonSet env | `5000` | `read_stall` 归因关联窗口，单位毫秒，以帧 `ts` 为中心前后各该值 |

热度与磁盘水位共 11 个 `TRAINCACHE_HEAT_*`，都改 DaemonSet env。默认值只在需要细调淘汰排序时改：

| 变量 | 改哪里 | 默认 |
|---|---|---|
| `TRAINCACHE_HEAT_DECAY_AGE` | DaemonSet env | `24h` |
| `TRAINCACHE_HEAT_DUMP_INTERVAL` | DaemonSet env | `30m` |
| `TRAINCACHE_HEAT_DECAY_INTERVAL` | DaemonSet env | `24h` |
| `TRAINCACHE_HEAT_DECAY_FACTOR` | DaemonSet env | `0.5` |
| `TRAINCACHE_HEAT_OPEN_BETA` | DaemonSet env | `0.15` |
| `TRAINCACHE_HEAT_TOUCH_HALF_LIFE` | DaemonSet env | `6h` |
| `TRAINCACHE_HEAT_CALIBRATE_WINDOW` | DaemonSet env | `100` |
| `TRAINCACHE_HEAT_CALIBRATE_THRESHOLD` | DaemonSet env | `0.8` |
| `TRAINCACHE_HEAT_DISK_TRIGGER_PERCENT` | DaemonSet env | `80` |
| `TRAINCACHE_HEAT_DISK_DELTA_PERCENT` | DaemonSet env | `5` |
| `TRAINCACHE_HEAT_DISK_CRITICAL_PERCENT` | DaemonSet env | `90` |

### 2.5 控制面参数

都在对应 Deployment 的 env 里，改完 apply：

| 变量 | 改哪里 | 部署清单值 | 说明 |
|---|---|---|---|
| `CONTROLLER_POLL_INTERVAL` | manager Deployment env | `1s` | Controller 轮询 Agent 状态的间隔 |
| `CONTROLLER_AGENT_PORT` | manager Deployment env | `9410` | Controller 连接 Agent 的 gRPC 端口，必须与 Agent 实际监听一致 |
| `CONTROLLER_AGENT_TIMEOUT` | manager Deployment env | `5s` | 单次 Agent 调用超时 |
| `CONTROLLER_SCORE_TTL` | manager Deployment env | `10m` | 调度分数有效期 |
| `SCHEDULER_SCORE_TTL` | scheduler Deployment env | `10m` | Scheduler 读取分数的有效期，必须与 Controller 一致 |
| `CONTROLLER_SCORE_ANNOTATION` | manager / scheduler Deployment env | `traincache.io/node-scores` | 训练 Pod 上的分数注解键，两端必须同值 |
| `CONTROLLER_MAX_CHUNKS` | manager Deployment env | `10000000` | 单任务块数上限 |
| `CONTROLLER_LOG_LEVEL` | manager / webhook / scheduler Deployment env | `info` | `debug` / `info` / `warn` / `error` |
| `CONTROLLER_LEADER_ELECTION` | manager Deployment env | `false` | 多副本时开 |
| `CONTROLLER_METRICS_ADDR` | manager / webhook / scheduler Deployment env | `:9412` | Prometheus 指标监听 |
| `CONTROLLER_HEALTH_ADDR` | manager / webhook / scheduler Deployment env | `:8081` | `healthz` / `readyz` 监听，探针按同名端口引用 |
| `CONTROLLER_WEBHOOK_ADDR` | webhook Deployment env | `:9443` | webhook server 监听，Service 443 转发到这里 |
| `CONTROLLER_NAMESPACE` | manager / webhook Deployment env | 空 | 只校验格式并记进启动日志，不改变任何监听范围：manager 始终 watch 全部 namespace，webhook 的准入范围由 `objectSelector` 的 `traincache.io/warmup: enabled` 决定；留空即可 |

manager、webhook、scheduler 三个 Deployment 分别在 `controller/deploy/manager/`、`webhook/`、`scheduler/` 下的 `deployment.yaml`。

`CONTROLLER_GATE_NAME` 与 `CONTROLLER_SCORE_ANNOTATION` 是 webhook、scheduler、训练 Pod 共用的约定，不要改。

## 3. 数据集参数

### 3.1 块大小

一个桶只有一个块大小。记录在桶根 `_traincache/format.json`：

```json
{"version":1,"chunkSize":67108864}
```

- 第一次往空桶发布时，用 `trainpublish --chunk-size` 写入；默认 64 MiB。
- 之后每次发布先读这个文件。不传 `--chunk-size` 就沿用；传了不同的值直接失败。
- 改块大小必须清空桶重灌，或换一个新桶。原地改会让旧块名和索引偏移对不上。
- 老桶里已有数据但没有这个文件时，第一次发布显式传历史块大小。

### 3.2 DataSet 字段

都改 DataSet YAML，改完重新 apply：

| 字段 | 改哪里 | 说明 |
|---|---|---|
| `spec.endpoint` / `spec.bucket` | DataSet YAML | 与 Agent 的 endpoint/bucket 一致 |
| `spec.datasetName` | DataSet YAML | 桶内前缀 `dataset/<name>/`；与发布器的 `--dataset` 一致 |
| `spec.secretRef` | DataSet YAML | 同 namespace 的 Secret，字段 `accessKey`、`secretKey` |
| `spec.snapshotID` | DataSet YAML | 空 = 当前 CURRENT；指定后固定读该快照 |
| `spec.chunkSize` | DataSet YAML | 必须等于桶的块大小 |
| `spec.shards` | DataSet YAML | 按发布产物的分片列表填：`name`、`count`、`lastChunkSize` |
| `spec.accessibleNodeSelector` | DataSet YAML | 在 `s3-access=true` 的节点里再收窄 |
| `spec.shuffle` | DataSet YAML | 数据集是否支持 shuffle，登记用 |

手写 DataSet 后用发布器校验 `datasetName` 与 `chunkSize`：

```bash
trainpublish --dataset coco --bucket traincache --check-dataset coco-dataset.yaml
```

### 3.3 发布器参数

都是 `trainpublish` 命令行 flag，不改清单，下次执行生效：

| 参数 | 改哪里 | 默认 | 说明 |
|---|---|---|---|
| `--chunk-size` | 命令行 | 64 MiB | 只在空桶首次发布生效 |
| `--concurrency` | 命令行 | `4` | 并发上传数 |
| `--work-dir` | 命令行 | 临时目录 | 切块临时目录 |
| `--keep-work-dir` | 命令行 | `false` | 排障时保留临时目录 |
| `--probe-conditional-write` | 命令行 | `false` | 发布前探测后端条件写 |
| `--check-dataset` | 命令行 | 空 | 只校验 DataSet，不上传 |

完整 flag 与环境变量见 [CLI 接口](CLI-INTERFACE.md) §10。

## 4. 训练任务参数

WarmupTask 在提交训练任务时创建，字段都在 `spec` 下，只改 WarmupTask YAML。「默认」列写「无」的是必填项。

| 字段 | 改哪里 | 默认 | 说明 |
|---|---|---|---|
| `dataSetRef.name` | WarmupTask YAML | 无 | 同 namespace 的 DataSet；创建后不可改 |
| `targetRef` | WarmupTask YAML | 无 | 目标 Job/StatefulSet/Pod |
| `podCount` | WarmupTask YAML | 无 | 训练 Pod 数；创建后不可改 |
| `preheatFanout` | WarmupTask YAML | `2` | 每个 Pod 最多预热几个节点。单节点候选时实际为 1 |
| `preheatFanoutMin` | WarmupTask YAML | `1` | 每个 Pod 期望的下限；实际不足时置 `MinUnmet` |
| `trainingType` | WarmupTask YAML | 无 | `epoch` / `shuffle` / `curriculum` / `static` |
| `podDetails[].index` | WarmupTask YAML | 无 | 0..podCount-1，必须连续 |
| `podDetails[].resources` | WarmupTask YAML | 无 | 用于节点容量筛选 |
| `podDetails[].initialConsumeFiles` | WarmupTask YAML | 空 | tar 分片名列表，如 `shard-000.tar`。全部 Pod 合起来覆盖全部分片、每个分片只出现一次；为空、未知分片、重复或没覆盖全部分片时整组回退默认均分，`totalChunks < podCount` 例外（见 §4.2） |
| `podDetails[].k` | WarmupTask YAML | 空 | 启动窗口块数，CRD 下限 0；0 或省略表示自动，按 §4.1 计算；显式有效范围 1..该 Pod 的 `W_i`，超出时置 `Condition InvalidSpec=False` 并拒绝。门控阈值按这个窗口算；它不是节点侧的 `prefetchLookahead`（默认 16） |
| `gate.threshold` | WarmupTask YAML | 无 | 就绪比例阈值，0..1 |
| `gate.gapMin` / `gate.slopeMin` | WarmupTask YAML | 无 | 边际收益阈值 |
| `gate.tMax` | WarmupTask YAML | 无 | 门控绝对超时 |
| `gate.minInterval` | WarmupTask YAML | 无 | 边际收益采样最小间隔 |
| `score.halfLife` / `score.cutoff` | WarmupTask YAML | 无 | 全局位置衰减评分的半衰期与截断距离，单位块 |
| `candidate.requireReady` | WarmupTask YAML | `false` | 只选 Ready 节点 |
| `candidate.ignoreNoSchedule` | WarmupTask YAML | `false` | 忽略 NoSchedule 污点 |
| `candidate.minFreeDiskBytes` | WarmupTask YAML | `0` | 候选节点最小剩余磁盘 |

`preheatFanout` 是上限，不是必须凑满：预热不占调度容量，一个候选节点只服务一个 Pod 的预热，所以要让每个 Pod 都有落点，候选节点数得 ≥ `podCount × preheatFanout`。不足时排在前面的 Pod 把节点认领干净，其余 Pod 记 `MinUnmet`、一个块都没被预热，它们的速率恒为 0 会把门控推向 `marginal_gain_insufficient` 提前放行。介意成本或候选节点少，就显式写 `preheatFanout: 1`。

### 4.1 启动窗口 K 的计算

`podDetails[].k` 为 0 或省略时，Controller 按下表算出每个 Pod 的 `K_i`：

| 符号 | 计算 |
|---|---|
| `W_i` | 该 Pod 实际分到的块数：填文件清单时为所列分片的块数之和，默认均分时为区间长度 |
| `K_window` | `ceil(startupWindowBytes / chunkSize)` |
| `K_cache` | `floor(cacheCapacityBytes × maxFillFraction / (podCount × chunkSize))` |
| `K_i` | `clamp(min(W_i, K_window, K_cache), 1, W_i)` |

`cacheCapacityBytes=0` 时不算 `K_cache`，只取 `clamp(min(W_i, K_window), 1, W_i)`。Controller 把算出的 K 写回 `spec.podDetails[].k`，随后 Requeue，并发 Normal 事件 `KDefaulted`。

例：8 MiB 块、1 GiB 缓存、1 个 Pod，`startupWindowBytes` 用默认 512MiB、`maxFillFraction` 用默认 0.125 → `K_window=64`、`K_cache=16`，`W_0` 足够时 `K_0=16`。

预热请求的块数是 `min(max(K_i, score.cutoff), W_i)`，不是 `K_i`。`score.cutoff` 大于 `K_i` 时启动前按 `cutoff` 展开，但不超过 `W_i`；流量与耗时随 `cutoff` 增长。启动阶段只想预热 `K_i` 块，就把 `score.cutoff` 设成 `K_i` 或更小。

显式填 `k` 时按填写值执行，不做钳制或截断。`k` 的 CRD 下限是 0，`0` 或省略表示自动；显式有效范围是 1..`W_i`，`k > W_i` 时 Controller 置 `Condition InvalidSpec=False` 并拒绝该任务。全部 Pod 的实际请求块数（`min(max(K_i, score.cutoff), W_i)`）合计超过 `cacheCapacityBytes × maxFillFraction` 时，Controller 发 Warning 事件 `KExceedsFillLimit`，任务照常执行。

### 4.2 消费文件清单

`podDetails[].initialConsumeFiles` 填 tar 分片名，不填块号。名字照抄 DataSet 的 `spec.shards[].name`，或用 `traincache ls --dataset <name>` 查看文件。

- 每个 Pod 只写自己的分片，块序按分片列出顺序展开。
- 全部 Pod 合起来必须覆盖 DataSet 的全部分片，每个分片只出现一次。
- 任一 Pod 缺失、写了未知分片、分片重复，或合起来没覆盖全部分片时，整组回退默认均分，`status.consumeOrderSource=Default`。
- `totalChunks < podCount` 时无法均分，Controller 置 `Condition InvalidSpec=False` 并拒绝，不回退。
- 没有"只写前几个再追加"的写法：本 Pod 的分片必须一次列全。

最小任务示例：

```yaml
apiVersion: traincache.io/v1alpha1
kind: WarmupTask
metadata:
  name: train-task
  namespace: team-a
spec:
  dataSetRef: {name: train-data}
  targetRef: {apiVersion: batch/v1, kind: Job, name: train-job}
  podCount: 1
  preheatFanout: 2
  preheatFanoutMin: 1
  trainingType: epoch
  podDetails:
    - index: 0
      resources:
        requests: {cpu: "1", memory: 2Gi}
  gate:
    threshold: 0.8
    gapMin: 0.01
    slopeMin: 0.0001
    tMax: 10m
    minInterval: 2s
  score:
    halfLife: 128
    cutoff: 8   # 64MiB 块时 K_window=8，cutoff 取同值，启动阶段不多预热
```

## 5. 不能改的部分

这些没有参数入口，改了会破坏数据或组件之间的约定：

- 对象键布局：`dataset/<name>/snapshots/<id>/`、`<name>/blocks/<path>#<chunk>`、`_traincache/format.json`。
- 块编号：按 shard 顺序累计，发布后固定。
- 已发布桶的块大小：写在 `_traincache/format.json`，发布器拒绝改值；只能清空桶重灌或换新桶。
- 已创建任务的 `dataSetRef` 与 `podCount`：API server 按 `self == oldSelf` 拒绝修改，要改只能删掉重建。
- gate 名 `traincache.io/warmup`、训练 Pod 上的分数注解 `traincache.io/node-scores`：webhook、scheduler、训练 Pod 共用。
- 镜像名与 tag：发布物决定，用户不通过参数改；离线部署用 `docker save` / `kind load`。

## 6. 改完后的检查

```bash
# 集群参数
kubectl -n traincache get cm traincache-config -o yaml
kubectl -n traincache rollout status ds/traincache-agentd

# 数据集
trainpublish --dataset <name> --bucket <bucket> --check-dataset <dataset.yaml>
kubectl -n <ns> get dataset <name> -o jsonpath='{.status.conditions}'

# 任务
kubectl -n <ns> get warmuptask <name> -o yaml
```
