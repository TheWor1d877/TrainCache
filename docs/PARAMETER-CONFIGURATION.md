# 参数配置

参数按落位对象分三层：集群运行时、数据集、训练任务。

## 三层落位

| 层 | 落位对象 | 改完怎么生效 |
|---|---|---|
| 集群运行时 | ConfigMap `traincache-config`、ConfigMap `traincache-scheduler-config`、ConfigMap `traincache-checkpoint-image`、Secret `traincache-objstore`、五份工作负载清单 | 改 ConfigMap 或 Secret 后重启读取它的工作负载，改资源直接 apply |
| 数据集 | `trainpublish` 参数、DataSet CRD | 发布新快照，DataSet 重新 apply |
| 训练任务 | WarmupTask CRD | 重新 apply，`dataSetRef` 与 `podCount` 不可改 |

## 落位判据

一个参数写在哪个对象里，按下面五问从上往下问，问到「是」就停。

| 顺序 | 问 | 落位 |
|---|---|---|
| 1 | 是凭证吗 | Secret + `secretKeyRef` |
| 2 | 是每个 Pod 各不相同的身份信息吗 | Downward API `fieldRef` |
| 3 | 换集群或换实验轮次要改吗 | 该工作负载自己 namespace 里的 ConfigMap + `configMapKeyRef` |
| 4 | 值与代码默认不同，但基本不动吗 | 同第 3 问的落位，写进 ConfigMap |
| 5 | 值等于代码默认 | 哪里都不写，默认只活在 `agent/svc/config.go`、`controller/internal/config/config.go`、`gateway/internal/config/config.go` |

五问各自的落位实例：

- 第 1 问的凭证是 `accessKey` 与 `secretKey`，写进 Secret `traincache-objstore`，由 Agent DaemonSet 与网关 Deployment 用 `secretKeyRef` 注入。
- 第 2 问的身份信息是节点名，DaemonSet 用 Downward API 的 `fieldRef` 取 `spec.nodeName`，环境变量名 `NODE_NAME`。
- 第 3 问的取值落在这些 ConfigMap 键上：端点、桶、TLS 开关、缓存容量、预取提前量、淘汰策略、白名单、自动 K 输入、网关到桶并发额度与缓存配额在 `traincache-config`；`controllerPollInterval`、`controllerScoreTTL` 同在 `traincache-config`；scheduler 的两个配置项在 kube-system 的 `traincache-scheduler-config`。
- 第 4 问的取值写进同一批 ConfigMap。`traincache-config` 里偏离代码默认的是 `objstoreEndpoint`、`controllerPollInterval`、`controllerScoreTTL`；kube-system 的 `traincache-scheduler-config` 里是 `schedulerScoreTTL` 与 `schedulerLogLevel`。
- 第 5 问的取值哪里都不写。监听端口、`TRAINCACHE_VERIFY`、`TRAINCACHE_CACHE_ROOT`、网关缓存根目录、11 个 `TRAINCACHE_HEAT_*`、`CONTROLLER_*` 的默认档都只活在代码里。

五份工作负载清单（Agent DaemonSet、manager、webhook、scheduler、gateway）因此没有任何一条 `value:` 字面量 env，取值全部走引用或走代码默认。引用还有一个方向要守：`configMapKeyRef` 只在本 Pod 的 namespace 里找对象，跨不过去，所以 kube-system 的 scheduler 用不了 `traincache-config`。这两条由 `hack/verify-config-placement.sh` 逐条校验，写错即失败。往 ConfigMap 加一项而同 namespace 的清单里没有对应引用时，进程读不到那一项。

第 5 问那批默认值要临时覆盖，用 `kubectl -n traincache set env ds/traincache-agentd TRAINCACHE_VERIFY=xxh3+sha256`，下次 `kubectl apply -k controller/deploy` 会把 DaemonSet 改回仓库那份；要长期覆盖，在 `traincache-config` 加一项、在 DaemonSet 加一条 `configMapKeyRef`，两处都要改。

## 术语

- 预热：训练 Pod 调度之前，把数据块提前拉进候选节点的缓存。WarmupTask 的 `preheatFanout` 决定每个 Pod 预热几个节点。
- 启动窗口 K：训练开始之前必须就绪的块数，落在 WarmupTask 的 `podDetails[].k`，算式在「启动窗口 K 的计算」一节。
- 门控：训练 Pod 放行前的准入检查，判定通过才摘掉 Pod 上的调度门。WarmupTask 的 `gate` 字段是它的配置，判定方是 controller-manager。
- 消费文件清单：每个训练 Pod 依次消费的 tar 分片名列表，落在 WarmupTask 的 `podDetails[].initialConsumeFiles`，规则在「消费文件清单」一节。
- 在途合并表：网关进程内记「正在下载的块」的一张表，同一块同时到达的请求在表里合并成一次回源。
- 节点分数：Controller 按节点算出的预热价值分，写在训练 Pod 的注解 `traincache.io/node-scores` 上，scheduler 读它给节点打分。

## 集群运行时

### ConfigMap traincache-config

位置 `controller/deploy/agent/configmap.yaml`，namespace `traincache`，21 个配置项。scheduler 的两个配置项在 kube-system 的 `traincache-scheduler-config`。

- `cacheCapacityBytes`，环境变量 `TRAINCACHE_CACHE_CAPACITY_BYTES`，默认 `"0"`，Agent 与 manager 读取，改完重启 Agent 与 manager。
  本地缓存字节上限，必须 `>=0`。`0` 关闭字节维度，只按磁盘水位（80% 触发、90% 紧急）淘汰；填字节数（1GiB = `"1073741824"`）后，字节占用先到阈值就先淘汰。manager 只用它算 `K_cache`，算式是 `floor(cacheCapacityBytes × maxFillFraction / (podCount × chunkSize))`。
- `prefetchLookahead`，环境变量 `TRAINCACHE_PREFETCH_LOOKAHEAD`，默认 `"16"`，Agent 读取，改完重启 Agent。
  节点侧游标之后持续预取的块数。调大提前量，预热流量和缓存占用随之增加。启动窗口块数落在 WarmupTask 的 `podDetails[].k`，算式是 `clamp(min(W_i, K_window, K_cache), 1, W_i)`。
- `evictPolicy`，环境变量 `TRAINCACHE_EVICT_POLICY`，默认 `"heat"`，Agent 读取，改完重启 Agent。
  缓存淘汰顺序，取 `heat`、`lru`、`lfu`。非法值使 Agent 拒绝启动。
- `datasetMetricsWhitelist`，环境变量 `TRAINCACHE_DATASET_WHITELIST`，默认 `""`，Agent 读取，改完重启 Agent。
  逗号分隔的 DataSet 名，只决定指标 `dataset` 标签是否保留原名；为空时全部合并成 `other`。取数与缓存不受它影响。
- `startupWindowBytes`，环境变量 `TRAINCACHE_STARTUP_WINDOW_BYTES`，默认 `"536870912"`，manager 读取，改完重启 manager。
  启动窗口字节数，默认 512MiB，必须 `>0`。只用于自动算 K，算式是 `ceil(startupWindowBytes / chunkSize)`。
- `maxFillFraction`，环境变量 `TRAINCACHE_MAX_FILL_FRACTION`，默认 `"0.125"`，manager 读取，改完重启 manager。
  全部 Pod 的预热总量占缓存容量的上限，默认八分之一，取值必须在 `(0,0.25]`。按 `podCount` 均摊，算式里是 `cacheCapacityBytes × maxFillFraction / (podCount × chunkSize)`；`cacheCapacityBytes=0` 时不参与计算。
- `objstoreEndpoint`，环境变量 `TRAINCACHE_OBJSTORE_ENDPOINT`，默认 `"minio.minio.svc.cluster.local:9000"`，Agent 与网关读取，改完重启 Agent 与网关。
  源站端点，不带 scheme。清单里是集群内 MinIO 的 Service 地址；接 COS/OSS/S3 时改这一处。
- `objstoreBucket`，环境变量 `TRAINCACHE_OBJSTORE_BUCKET`，默认 `"traincache"`，Agent 与网关读取，改完重启 Agent 与网关。
  单桶名，必须等于 `DataSet.spec.bucket`：`SubmitPreheat` 不带桶名，不一致时每个块读取都失败。bench 按数据集改写它。
- `objstoreSecure`，环境变量 `TRAINCACHE_OBJSTORE_SECURE`，默认 `"false"`，Agent 与网关读取，改完重启 Agent 与网关。
  `true` / `1` / `yes` 时用 HTTPS 连源站，其余取值（含空）走明文。
- `objstoreCompression`，环境变量 `TRAINCACHE_OBJSTORE_COMPRESSION`，默认 `"false"`，Agent 与网关读取，改完重启 Agent 与网关。
  传输压缩开关。`true` / `1` / `yes` 时 S3 客户端向源站声明 gzip，传输层解压后按原始字节计算指纹；关闭时不发 `Accept-Encoding`。带 Range 的请求不参与协商。训练数据多为已压缩格式，默认关闭。
- `fetchRangeParallel`，环境变量 `TRAINCACHE_FETCH_RANGE_PARALLEL`，默认 `"true"`，Agent 读取，改完重启 Agent。
  Range 并行下载开关，默认打开。打开时每个块拆成 4 条 Range GET 并行取，拼成完整块后算指纹落盘；关闭时每个块走一条整对象 GET。块大小不参与判定，非法值使 Agent 拒绝启动。
- `gatewayEndpoint`，环境变量 `TRAINCACHE_GATEWAY_ENDPOINT`，默认 `""`，Agent 读取，改完重启 Agent。
  合并回源网关地址，形如 `traincache-gateway.traincache.svc.cluster.local:9413`，不带 scheme；空表示全部直连源站。非空时只有块 GET 走网关，`Put`/`List`/`GetETag`/`PutIfMatch`/`Delete` 仍直连源站。
- `gatewaySecure`，环境变量 `TRAINCACHE_GATEWAY_SECURE`，默认 `"false"`，Agent 读取，改完重启 Agent。
  Agent 到网关是否 TLS，`true`/`1`/`yes` 为真，取值独立于 `objstoreSecure`，源站与网关的 TLS 各设一次。
- `gatewayBucketConcurrency`，环境变量 `TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY`，默认 `"64"`，网关读取，改完重启网关。
  到源站的并发下载上限，正整数；`0` 或非整数使网关拒绝启动。
- `gatewayBlockBufferBytes`，环境变量 `TRAINCACHE_GATEWAY_BLOCK_BUFFER_BYTES`，默认 `"10485760"`，网关读取，改完重启网关。
  每个在途块环形缓冲的字节数（默认 10MiB），正整数；非法值使网关拒绝启动。
- `gatewayCache`，环境变量 `TRAINCACHE_GATEWAY_CACHE`，默认 `"true"`，网关读取，改完重启网关。
  磁盘缓存开关，默认打开。`1`/`true`/`yes` 开，其余取值关；env 未设置也开。关掉后网关只做在途合并，不落盘。该关的四种场合：数据集小于节点盘、工作集大于配额、命中侧只有 HDD 或邻居写盘活跃、源站不计费且出口快于集群取速率。
- `gatewayCacheDatasetBytes`，环境变量 `TRAINCACHE_GATEWAY_CACHE_DATASET_BYTES`，默认 `"34359738368"`，网关读取，改完重启网关。
  单个 dataset 在网关盘上的驻写上限，默认 32GiB，正整数；非法值使网关拒绝启动。超过它先删这个 dataset 里 mtime 最老的块。
- `gatewayCacheTotalBytes`，环境变量 `TRAINCACHE_GATEWAY_CACHE_TOTAL_BYTES`，默认 `"68719476736"`，网关读取，改完重启网关。
  全局驻写上限，默认 64GiB，正整数且不小于 `gatewayCacheDatasetBytes`，否则网关拒绝启动。单 dataset 淘汰后仍超总上限，删最久没出现的 dataset 整个目录。
- `controllerPollInterval`，环境变量 `CONTROLLER_POLL_INTERVAL`，默认 `"1s"`，manager 读取，改完重启 manager。
  轮询 Agent 进度的间隔，代码默认 2s；清单写 1s，让门控放行延迟落在 bench 的测量窗口内。
- `controllerScoreTTL`，环境变量 `CONTROLLER_SCORE_TTL`，默认 `"10m"`，manager 读取，改完重启 manager。
  节点分数的有效期，代码默认 5m。
- `gateNetworkBandwidthBytes`，环境变量 `TRAINCACHE_GATE_NETWORK_BANDWIDTH`，默认 `"0"`，manager 读取，改完重启 manager。
  网络带宽上限，单位 bytes/sec。`0` 表示不启用「带宽够快就跳过门控」的判断；非零时门控把 agent 上报吞吐与该上限的 0.8 倍比较。取值必须 `>= 0`，负值使 controller 拒绝启动。改它要重启 controller-manager Deployment，不重启 Agent 与网关。

`objstoreCompression`、`fetchRangeParallel`、`gatewayCache` 三个键的值与代码默认相同，仍然写进这个 ConfigMap，是因为 Agent DaemonSet 与网关 Deployment 用 `configMapKeyRef` 引用它们：删掉键等于让引用它的 Pod 起不来。`objstoreSecure`、`gatewaySecure`、`gatewayEndpoint`、`objstoreBucket` 也被引用，缺键同样是启动失败。

### 影响面

改一个键要重启哪些工作负载，由它有几个读取方决定：Agent 与 manager 一起读的键要同时重启两方，只有一处读的只重启那一个。ConfigMap 的值不热生效，改完先 apply 再重启读取方。

网关的两个配额与 Agent 的 `cacheCapacityBytes` 落在同一块宿主盘上，两处之和要给节点盘留余量。

### 作用范围

网关只处理块 GET。带 Range 的 GET 既不进在途合并表也不进磁盘缓存，字节不写盘，直接透传源站。`CURRENT` 与 manifest 这两类快照指针的 GET 直接转发给源站，既不进在途表也不进缓存。合并与缓存只认 `<dataset>/blocks/` 前缀下的键。

### 改完怎么生效

改仓库文件后 apply，再重启读取方：

```bash
kubectl apply -f controller/deploy/agent/configmap.yaml
kubectl -n traincache rollout restart ds/traincache-agentd
kubectl -n traincache rollout status ds/traincache-agentd
kubectl -n traincache rollout restart deploy/traincache-controller-manager
kubectl -n traincache rollout status deploy/traincache-controller-manager
```

被引用的配置项都必须存在。缺任一项，引用它的 Pod 停在 `CreateContainerConfigError`，不会回退到代码默认值：Agent 缺 `objstoreEndpoint` 时访问不到对象存储，manager 缺 `cacheCapacityBytes` 时算不出自动 K。先 apply ConfigMap，再 apply 工作负载；`kubectl apply -k controller/deploy` 同时带上两者，只有单独 apply `controller/deploy/agent/daemonset.yaml` 才需要留意顺序。

### 对象存储接入

Agent 与网关的源站地址、桶名、TLS 开关写在 ConfigMap `traincache-config` 的 `objstoreEndpoint`、`objstoreBucket`、`objstoreSecure` 三个键；凭证走 Secret `traincache-objstore`，由 Agent DaemonSet 与网关 Deployment 用 `secretKeyRef` 注入。

- `TRAINCACHE_OBJSTORE_ENDPOINT`：落在 ConfigMap `traincache-config` 的 `objstoreEndpoint` 键，程序内置默认 `127.0.0.1:9000`。S3 兼容端点，不带 scheme。ConfigMap 里写的是集群内 MinIO 的 Service 地址。
- `TRAINCACHE_OBJSTORE_ACCESS_KEY`：落在 Secret `traincache-objstore` 的 `accessKey` 键，程序内置默认空。静态 AK，Agent 与网关共用同一把。
- `TRAINCACHE_OBJSTORE_SECRET_KEY`：落在 Secret `traincache-objstore` 的 `secretKey` 键，程序内置默认空。静态 SK，Agent 与网关共用同一把。
- `TRAINCACHE_OBJSTORE_BUCKET`：落在 ConfigMap `traincache-config` 的 `objstoreBucket` 键，程序内置默认 `traincache`。单桶。
- `TRAINCACHE_OBJSTORE_SECURE`：落在 ConfigMap `traincache-config` 的 `objstoreSecure` 键，程序内置默认 `false`。`true` 时用 HTTPS。
- `TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE`：清单未设，取代码默认 `false`。启动时探测后端条件写；临时开一次用 `kubectl set env`，长期开走「落位判据」里那条长期覆盖办法。

程序内置默认是代码里的值。ConfigMap 里按集群覆盖成实际端点与桶名，以 ConfigMap 为准。

节点要能访问对象存储，并打上 `traincache.io/s3-access=true`。Controller 只在这个标签为 true 的节点里选预热目标，标签由管理员手工维护。

### ConfigMap traincache-scheduler-config

位置 `controller/deploy/scheduler/kube-scheduler-config.yaml`，namespace `kube-system`，除 `scheduler.yaml` 这一项（同一对象里的 KubeSchedulerConfiguration，挂进 Pod 后作为调度器配置）外还有 scheduler 读的两个配置项。

- `schedulerScoreTTL`，默认 `"10m"`，scheduler 读取，改完重启 scheduler。
  scheduler 判定分数过期的有效期，代码默认 5m，必须与 `traincache-config` 的 `controllerScoreTTL` 同值。
- `schedulerLogLevel`，默认 `"debug"`，scheduler 读取，改完重启 scheduler。
  只有 scheduler 用 `debug`：每个 (pod,node) 决策打一行 `scheduler score`；manager 与 webhook 用代码默认 `info`。`controller/e2e/run.sh` 断言这行日志，改成 `info` 会让该项失败。

这两项单独放在 kube-system：`configMapKeyRef` 只在 Pod 自己的 namespace 找对象，scheduler 跑在 kube-system，引用 `traincache` 里的 ConfigMap 取不到值，Pod 停在 `CreateContainerConfigError`。`hack/verify-config-placement.sh` 逐条按 namespace 校验每条引用指向的对象，写错即失败。改值后重启 scheduler：

```bash
kubectl apply -f controller/deploy/scheduler/kube-scheduler-config.yaml
kubectl -n kube-system rollout restart deploy/traincache-scheduler
kubectl -n kube-system rollout status deploy/traincache-scheduler
```

### ConfigMap traincache-checkpoint-image

位置 `controller/deploy/webhook/checkpoint-image.yaml`，namespace `traincache`，一项。

- `checkpointImage`，默认 `ghcr.io/thewor1d877/traincache/traincache-checkpointd:dev`，webhook 读取，改完重启 webhook。
  注入到训练 Pod 的 checkpointd 镜像。开发清单写 `:dev`；发布清单由渲染脚本把它改成本次发布的 digest。接入方可用 Pod 注解 `traincache.io/checkpoint-image` 或自己的 ConfigMap 键覆盖它。

改这个值后重启 webhook：

```bash
kubectl apply -f controller/deploy/webhook/checkpoint-image.yaml
kubectl -n traincache rollout restart deploy/traincache-webhook
kubectl -n traincache rollout status deploy/traincache-webhook
```

### 工作负载清单与资源

各组件各改自己的清单，按集群规模调整：

| 组件 | 清单 | requests | limits |
|---|---|---|---|
| controller-manager | `controller/deploy/manager/deployment.yaml` | 50m / 128Mi | 512Mi（无 CPU 上限） |
| webhook | `controller/deploy/webhook/deployment.yaml` | 50m / 64Mi | 256Mi（无 CPU 上限） |
| kube-scheduler | `controller/deploy/scheduler/deployment.yaml` | 100m / 128Mi | 512Mi（无 CPU 上限） |
| traincache-agentd | `controller/deploy/agent/daemonset.yaml` | 100m / 256Mi | 4Gi（无 CPU 上限） |
| traincache-gateway | `controller/deploy/gateway/deployment.yaml` | 100m / 256Mi | 1.5Gi（无 CPU 上限） |

Agent 的缓存落在节点 hostPath `/var/cache/traincache`，不占容器内存；`limits.memory` 限制的是 Agent 进程本身。缓存大小由 `cacheCapacityBytes` 和磁盘水位控制。

网关的缓存根目录 `/var/lib/traincache-gateway/cache` 是代码常量，不进 ConfigMap，清单只负责把卷挂到这条路径。

### Agent 高级参数

这些参数的运行值取自 `agent/svc/config.go` 的默认值。覆盖办法走「落位判据」；改完重启 Agent DaemonSet 生效。

- `TRAINCACHE_EVICT_POLICY`，默认 `heat`。淘汰策略，取值与非法值的处理跟 `traincache-config` 的 `evictPolicy` 同一个键，DaemonSet 从该键注入。
- `TRAINCACHE_VERIFY`，默认 `xxh3`。校验级别：`xxh3` 或 `xxh3+sha256`。
- `TRAINCACHE_CACHE_ROOT`，默认 `/var/cache/traincache`。缓存根目录，必须与 DaemonSet 的 hostPath 挂载一致。
- `TRAINCACHE_GRPC_ADDR`，默认 `:9410`。Agent gRPC 监听地址，DaemonSet 的 `hostPort` 同值。
- `TRAINCACHE_METRICS_ADDR`，默认 `:9411`。指标监听地址，DaemonSet 的 `hostPort` 同值。
- `TRAINCACHE_ORDERS_DIR`，默认 `/run/traincache/orders`。OrderSource ring 目录。
- `TRAINCACHE_INDEX_REFRESH_INTERVAL`，默认 `5s`。快照跟随刷新周期。
- `TRAINCACHE_LOCAL_GC_INTERVAL`，默认 `10m`。Agent 清扫本节点本地孤儿块副本的周期。Agent 按节点一进程一份，粒度为进程级即节点级；调大则已删除条目与数据集的本地副本更晚被回收，调小则每轮索引读取流量增加。整轮清扫是 fail-safe 的：任一数据集的索引读不到或损坏时，这一轮不删任何块。删除的本地块数计进指标 `traincache_gc_blocks_removed_total`。
- `TRAINCACHE_READ_STALL_MIN_WAIT_MS`，默认 `100`。`read_stall` 入直方图的最小等待毫秒。
- `TRAINCACHE_READ_STALL_WINDOW_MS`，默认 `5000`。`read_stall` 归因关联窗口，单位毫秒，以帧 `ts` 为中心前后各该值。

热度与磁盘水位共 11 个 `TRAINCACHE_HEAT_*`，同样不写清单，只在需要细调淘汰排序时改：

- `TRAINCACHE_HEAT_DECAY_AGE`，默认 `24h`。
- `TRAINCACHE_HEAT_DUMP_INTERVAL`，默认 `30m`。
- `TRAINCACHE_HEAT_DECAY_INTERVAL`，默认 `24h`。
- `TRAINCACHE_HEAT_DECAY_FACTOR`，默认 `0.5`。
- `TRAINCACHE_HEAT_OPEN_BETA`，默认 `0.15`。
- `TRAINCACHE_HEAT_TOUCH_HALF_LIFE`，默认 `6h`。
- `TRAINCACHE_HEAT_CALIBRATE_WINDOW`，默认 `100`。
- `TRAINCACHE_HEAT_CALIBRATE_THRESHOLD`，默认 `0.8`。
- `TRAINCACHE_HEAT_DISK_TRIGGER_PERCENT`，默认 `80`。
- `TRAINCACHE_HEAT_DISK_DELTA_PERCENT`，默认 `5`。
- `TRAINCACHE_HEAT_DISK_CRITICAL_PERCENT`，默认 `90`。

### 控制面参数

manager、webhook、scheduler 三个进程的取值口径与 Agent 一致：等于 `controller/internal/config/config.go` 默认值的不写进清单，需要偏离默认的进 ConfigMap。manager 的偏离项在 `traincache-config`，scheduler 的偏离项在自己 namespace 的 `traincache-scheduler-config`；webhook 的六条取值全部等于默认，它另有一条 `CHECKPOINT_IMAGE`，值来自 `traincache-checkpoint-image` ConfigMap，装配值是镜像引用而不是运行参数。

- `CONTROLLER_POLL_INTERVAL`，生效值 `1s`，写在 ConfigMap `traincache-config` 的 `controllerPollInterval` 键，代码默认 2s。轮询 Agent 进度的间隔。
- `CONTROLLER_AGENT_PORT`，生效值 `9410`，代码默认。Controller 连接 Agent 的 gRPC 端口，必须与 Agent 实际监听一致。
- `CONTROLLER_AGENT_TIMEOUT`，生效值 `5s`，代码默认。单次 Agent 调用超时。
- `CONTROLLER_SCORE_TTL`，生效值 `10m`，写在 ConfigMap `traincache-config` 的 `controllerScoreTTL` 键，代码默认 5m。节点分数的有效期。
- `SCHEDULER_SCORE_TTL`，生效值 `10m`，写在 kube-system 的 ConfigMap `traincache-scheduler-config` 的 `schedulerScoreTTL` 键，代码默认 5m。scheduler 判定分数过期的有效期。
- `CONTROLLER_SCORE_ANNOTATION`，生效值 `traincache.io/node-scores`，代码默认。训练 Pod 上的分数注解键，三端共用。
- `CONTROLLER_MAX_CHUNKS`，生效值 `10000000`，代码默认。单任务块数上限。
- `CONTROLLER_LOG_LEVEL`，manager 与 webhook 是 `info`，scheduler 是 `debug`。scheduler 取 kube-system 的 ConfigMap `traincache-scheduler-config` 的 `schedulerLogLevel` 键，其余取代码默认。取值 `debug` / `info` / `warn` / `error`。
- `CONTROLLER_LEADER_ELECTION`，生效值 `false`，代码默认。多副本时开。
- `CONTROLLER_METRICS_ADDR`，生效值 `:9412`，代码默认。Prometheus 指标监听。
- `CONTROLLER_HEALTH_ADDR`，生效值 `:8081`，代码默认。`healthz` / `readyz` 监听，探针按同名端口引用。
- `CONTROLLER_WEBHOOK_ADDR`，生效值 `:9443`，代码默认。webhook server 监听，Service 443 转发到这里。
- `CONTROLLER_NAMESPACE`，生效值空，代码默认。只校验格式并记进启动日志，不改变任何监听范围：manager 始终 watch 全部 namespace，webhook 的准入范围由 `objectSelector` 的 `traincache.io/warmup: enabled` 决定；留空即可。
- `CHECKPOINT_IMAGE`，生效值是装配方给的值，来自 ConfigMap `traincache-checkpoint-image`，namespace `traincache`。注入到训练 Pod 的 checkpointd 镜像。

`CONTROLLER_GATE_NAME` 与 `CONTROLLER_SCORE_ANNOTATION` 是 webhook、scheduler、训练 Pod 三端共用的约定键，改动要三端同步。

### CLI 参数

`traincache` 命令读操作者 shell 里的环境变量，这些值不写进任何工作负载清单，也不进 ConfigMap。

- `TRAINCACHE_CONTROLLER_ENDPOINT`，默认 `traincache-controller-manager.traincache.svc:9412`，代码默认。CLI 按节点名解析 Agent 地址时访问的控制面地址，作用范围与 flag `--controller` 相同。集群内用默认值即可；集群外运行 CLI 时用这个环境变量或 `--controller` 指向可达地址。值等于代码默认时不写进任何 ConfigMap。

## 数据集参数

### 块大小

一个桶只有一个块大小。记录在桶根 `_traincache/format.json`：

```json
{"version":1,"chunkSize":67108864}
```

- 第一次往空桶发布时，用 `trainpublish --chunk-size` 写入；默认 64 MiB。
- 之后每次发布先读这个文件。不传 `--chunk-size` 就沿用；传了不同的值直接失败。
- 改块大小要清空桶重新发布，或换一个新桶。原地改会让旧块名和索引偏移对不上。
- 桶里已有数据但没有这个文件时，第一次发布显式传历史块大小。

### DataSet 字段

都改 DataSet YAML，改完重新 apply：

- `spec.endpoint` 与 `spec.bucket`：与 Agent 的 endpoint/bucket 一致。
- `spec.datasetName`：桶内前缀 `dataset/<name>/`；与发布器的 `--dataset` 一致。
- `spec.secretRef`：同 namespace 的 Secret，字段 `accessKey`、`secretKey`。
- `spec.snapshotID`：空表示当前 CURRENT；指定后固定读该快照。
- `spec.chunkSize`：必须等于桶的块大小。
- `spec.shards`：按发布产物的分片列表填：`name`、`count`、`lastChunkSize`。
- `spec.accessibleNodeSelector`：在 `s3-access=true` 的节点里再收窄。
- `spec.shuffle`：数据集是否支持 shuffle，登记用。

手写 DataSet 后用发布器校验 `datasetName` 与 `chunkSize`：

```bash
trainpublish --dataset coco --bucket traincache --check-dataset coco-dataset.yaml
```

### 发布器参数

都是 `trainpublish` 命令行 flag，不改清单，下次执行生效：

- `--dataset`：必填。目标 dataset 名，写入 `dataset/<name>/` 前缀，须与 DataSet 的 `spec.datasetName` 一致。
- `--endpoint`：默认取 `TRAINCACHE_OBJSTORE_ENDPOINT`，未设置时是 `127.0.0.1:9000`。S3 兼容端点，不带 scheme。
- `--access-key`：默认取 `TRAINCACHE_OBJSTORE_ACCESS_KEY`，未设置时为空。静态 AK。
- `--secret-key`：默认取 `TRAINCACHE_OBJSTORE_SECRET_KEY`，未设置时为空。静态 SK。
- `--bucket`：默认取 `TRAINCACHE_OBJSTORE_BUCKET`，未设置时是 `traincache`。单桶。
- `--secure`：默认取 `TRAINCACHE_OBJSTORE_SECURE`。用 TLS 连对象存储。
- `--chunk-size`：默认 64 MiB。只在空桶首次发布生效。
- `--concurrency`：默认 `4`。并发上传数。
- `--work-dir`：默认临时目录。切块临时目录。
- `--keep-work-dir`：默认 `false`。排障时保留临时目录。
- `--probe-conditional-write`：默认 `false`。发布前探测后端条件写。
- `--check-dataset`：默认空。只校验 DataSet，不上传。

## 训练任务参数

WarmupTask 在提交训练任务时创建，字段都在 `spec` 下，只改 WarmupTask YAML。下面标「必填」的是必填项。

- `dataSetRef.name`：必填。同 namespace 的 DataSet；创建后不可改。
- `targetRef`：必填。目标 Job/StatefulSet/Pod。
- `podCount`：必填。训练 Pod 数；创建后不可改。
- `preheatFanout`：默认 `2`。每个 Pod 最多预热几个节点。单节点候选时实际为 1。
- `preheatFanoutMin`：默认 `1`。每个 Pod 期望的下限；实际不足时置 `MinUnmet`。
- `trainingType`：必填。`epoch` / `shuffle` / `curriculum` / `static`。
- `podDetails[].index`：必填。0..podCount-1，必须连续。
- `podDetails[].resources`：必填。用于节点容量筛选。
- `podDetails[].initialConsumeFiles`：默认空。tar 分片名列表，如 `shard-000.tar`。全部 Pod 合起来覆盖全部分片、每个分片只出现一次；为空、未知分片、重复或没覆盖全部分片时整组回退默认均分。`totalChunks < podCount` 时不回退，Controller 置 `Condition InvalidSpec=False` 并拒绝。
- `podDetails[].k`：默认空。启动窗口块数，CRD 下限 0；0 或省略表示自动，算式是 `clamp(min(W_i, K_window, K_cache), 1, W_i)`；显式有效范围 1..该 Pod 的 `W_i`，超出时置 `Condition InvalidSpec=False` 并拒绝。门控阈值按这个窗口算；节点侧的持续预取块数是 `prefetchLookahead`，默认 16。
- `gate.threshold`：必填。就绪比例阈值，0..1。
- `gate.gapMin` 与 `gate.slopeMin`：必填。边际收益阈值。
- `gate.tMax`：必填。门控绝对超时。
- `gate.minInterval`：必填。边际收益采样最小间隔。
- `score.halfLife` 与 `score.cutoff`：必填。全局位置衰减评分的半衰期与截断距离，单位块。
- `candidate.requireReady`：默认 `false`。只选 Ready 节点。
- `candidate.ignoreNoSchedule`：默认 `false`。忽略 NoSchedule 污点。
- `candidate.minFreeDiskBytes`：默认 `0`。候选节点最小剩余磁盘。

`preheatFanout` 是每个 Pod 的预热节点数上限。预热不计入调度容量，一个候选节点只服务一个 Pod 的预热，所以要给 `podCount` 个 Pod 各分配 `preheatFanout` 个节点，候选节点数需 ≥ `podCount × preheatFanout`。候选不足时，排在前面的 Pod 取走候选节点，其余 Pod 记 `MinUnmet`，预热块数为 0。这些 Pod 的读速率恒为 0，门控指标 `marginal_gain_insufficient` 会提前放行。候选节点少或者不接受翻倍预热流量时，显式写 `preheatFanout: 1`。

### 启动窗口 K 的计算

`podDetails[].k` 为 0 或省略时，Controller 自动算出每个 Pod 的 `K_i`：

| 符号 | 计算 |
|---|---|
| `W_i` | 该 Pod 实际分到的块数：填文件清单时为所列分片的块数之和，默认均分时为区间长度 |
| `K_window` | `ceil(startupWindowBytes / chunkSize)` |
| `K_cache` | `floor(cacheCapacityBytes × maxFillFraction / (podCount × chunkSize))` |
| `K_i` | `clamp(min(W_i, K_window, K_cache), 1, W_i)` |

`cacheCapacityBytes=0` 时不算 `K_cache`，只取 `clamp(min(W_i, K_window), 1, W_i)`。Controller 把算出的 K 写回 `spec.podDetails[].k`，随后 Requeue，并发 Normal 事件 `KDefaulted`。

例：8 MiB 块、1 GiB 缓存、1 个 Pod，`startupWindowBytes` 用默认 512MiB、`maxFillFraction` 用默认 0.125，`K_window=64`、`K_cache=16`，`W_0` 足够时 `K_0=16`。

预热请求的块数是 `min(max(K_i, score.cutoff), W_i)`。`score.cutoff` 大于 `K_i` 时启动前按 `cutoff` 展开，上限是 `W_i`；流量与耗时随 `cutoff` 增长。启动阶段只想预热 `K_i` 块，就把 `score.cutoff` 设成 `K_i` 或更小。

显式填 `k` 时按填写值执行，不做钳制或截断。全部 Pod 的实际请求块数（`min(max(K_i, score.cutoff), W_i)`）合计超过 `cacheCapacityBytes × maxFillFraction` 时，Controller 发 Warning 事件 `KExceedsFillLimit`，任务照常执行。

### 消费文件清单

`podDetails[].initialConsumeFiles` 填 tar 分片名，块序按分片列出顺序展开。名字照抄 DataSet 的 `spec.shards[].name`，或用 `traincache ls --dataset <name>` 查看文件。

- 每个 Pod 写自己那批分片，一次列全。
- 全部 Pod 合起来必须覆盖 DataSet 的全部分片，每个分片只出现一次。
- 任一 Pod 缺失、写了未知分片、分片重复，或合起来没覆盖全部分片时，整组回退默认均分，`status.consumeOrderSource=Default`。
- `totalChunks < podCount` 时无法均分，Controller 置 `Condition InvalidSpec=False` 并拒绝，不回退。

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
