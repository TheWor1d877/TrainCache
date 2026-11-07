# Fluid 与 JuiceFS 机制现勘报告

## 结论与缺口判定

现勘回答三个问题，结论如下。

Fluid 没有预热与调度联动。Fluid 1.1.0-alpha.9 在 JuiceFSRuntime 上 webhook 零注入，注入前后 Pod spec 的 diff 为零；预热与调度完全独立，Pod 一创建就被调度，创建、调度与容器启动的时间戳在同一秒。

JuiceFS 社区版只有本地缓存。CE 是社区版，分布式缓存组、IORPC、RDMA 都是企业版（5.2/5.3+）的能力，有官方文档佐证。

对手没有阶段感知与按消费顺序持续预取的能力。三 epoch 换序读实验在三个切换点与三个观测点（客户端块级日志、Dataset cached 抬升、UFS 流量）上全部被动，观测窗口内零超前于读取的自动预取动作；手动 DataLoad 预热可行，只能由外部显式触发。

缺口成立，核心方向按预热与调度联动推进。阶段感知与按消费顺序持续预取列为缺口项，证据并入能力边界一节。

## 现勘环境

全部组件镜像都在宿主机上拉好，再用 `kind load` 送进集群：

| 组件 | 版本与形态 | 证据 |
|---|---|---|
| kind 集群 | 3 节点（control-plane + worker + worker2），k8s **v1.37.0**（containerd 2.3.4） | `../survey/evidence/kubectl-get-nodes.txt`、`../survey/evidence/kind-repair-log.txt` |
| Fluid | helm chart fluid-1.1.0-alpha.9，APP VERSION 1.1.0-36f0467 | `../survey/evidence/fluid-version.txt`、`../survey/evidence/fluid-install-helm.txt` |
| Fluid 组件 | dataset-controller、juicefsruntime-controller、fluid-webhook、fluidapp-controller、fluid-csi（DaemonSet×3）全部 Running | `../survey/evidence/fluid-pods.txt`、`../survey/evidence/fluid-pods-after-reinstall.txt` |
| JuiceFS 客户端 | ce-v1.3.0（挂载镜像 juicedata/mount:ce-v1.3.0） | `../survey/evidence/standalone-baseline.txt` 首行、`../survey/evidence/juicefsruntime-log-3epoch.txt` 的 mount 版本行 |
| JuiceFSRuntime | jfsdemo：tieredstore path=/dev/shm、MEM、quota **1Gi**（自 3Gi 下调） | `../survey/evidence/jfsruntime-describe.yaml` |
| 元数据服务 | redis:7-alpine（单实例，AOF 未开） | 集群内 `default/redis-*` |
| 对象存储 | MinIO（latest），bucket `train-data`，16×126MB tar 裸对象（1.97GiB） | `../survey/evidence/kind-repair-log.txt` |
| 训练 Pod | busybox `train-demo`，三 epoch 换序读，读序字面量内嵌 | `../survey/evidence/train-pod-3epoch.log` |

装置和探针阶段共用同一批 16×126MB tar 数据，种子固定可复现。

### 镜像装法与 Fluid 安装

宿主机到不了 docker hub，全部组件镜像先拉在宿主机，再用 `kind load` 送进集群：`redis:7-alpine`、`busybox:latest`，外加 9 个 Fluid/JuiceFS 镜像，即 7 个 fluidcloudnative:v1.1.0-36f0467 全套，加 juicefs-csi-driver:v0.32.0 与 mount:ce-v1.3.0。

官方 helm repo index 没收录 alpha 版本，从 GitHub release 下离线 tgz（`helm-chart-fluid-1.1.0-alpha.9`），`helm install` 到 fluid-system，带 `--set runtime.juicefs.enabled=true`。chart 的 appVersion 1.1.0-36f0467 与本地镜像 tag 一致，`helm list` 状态为 deployed。

16 个分片共 1.97GiB 通过 `kubectl port-forward` 通道上传，耗时 5s。之后照序 apply：redis、jfs-secret、dataset、juicefsruntime。JuiceFSRuntime 的 Worker 与 FUSE Ready，Dataset Bound，PVC Bound。

### 装置配额 1Gi 的意图与落空

Runtime 的缓存配额设 1Gi，低于数据集总量约 2GB，设计意图是让每次 epoch 切换都必然产生缓存 miss，给主动预取留出可观测的空间。实际节点 `/dev/shm` 只有 64MB，这个意图没有生效。

## 官方 demo 的链路与数据导入

Dataset `jfsdemo` 与 JuiceFSRuntime（同名绑定）先 format，元数据进 redis，数据块落 MinIO；再把裸对象导入 JuiceFS 文件系统；然后 DataLoad 做 warmup；最后训练 Pod 起起来读。

导入方式：`juicefs sync` 以 `jfs://` 协议直写文件系统，源 URI 为路径式 `minio://AK:SK@endpoint/bucket`。这个镜像里的客户端（ce-v1.3.0）已经移除了 `juicefs import` 命令，Fluid 的 PVC 只读、挂载写不可行，只能走 sync。

## webhook 注入前后 diff 为零

dry-run（绕过 webhook）和实际创建（经过 webhook）的 Pod spec 对比，见 `../survey/evidence/pod-before-injection.yaml` 与 `../survey/evidence/pod-after-injection.yaml`：

| 观察项 | 结果 |
|---|---|
| FUSE sidecar 容器注入 | **无**，容器列表里只有用户的 busybox；FUSE 由 Fluid CSI（node plugin）加 `jfsdemo-fuse` Pod（每节点一个）在 kubelet 层完成挂载，Pod 内只见普通 volumeMount |
| nodeAffinity 与 nodeSelector 注入 | **无**，这个版本与 JuiceFSRuntime 的组合下不注入 nodeAffinity 与 nodeSelector |
| 其他注入 | 仅 k8s 默认字段，即 serviceaccount projected volume 与默认 tolerations |

`JuiceFSRuntime.status` 里 Fluid 维护着 `cacheAffinity`（`../survey/evidence/runtime-status.txt`）：worker 节点被打上 `fluid.io/f-default-jfsdemo=true` 标签，亲和策略为 `preferredDuringSchedulingIgnoredDuringExecution`（weight 100），required 项为空，而且没有注入到任何 Pod。

Fluid 1.1 把 JuiceFS 访问全面 CSI 化之后，webhook sidecar 注入路径不再走，缓存亲和只剩 runtime status 里的软偏好加节点标签，不强制也不自动。粒度仍是二值信息，即节点有没有缓存 worker，没有块级进度。背景文档记的粗粒度二进制加事后注入里，注入环节在这个版本已不存在。

## Pod 不等 DataLoad

`../survey/evidence/timeline-notes.txt` 的三个时间戳来自 `kubectl get pod -o jsonpath`，全部在同一秒：

| 时刻（UTC） | 事件 |
|---|---|
| 08:33:39 | train-demo creationTimestamp |
| 08:33:39 | PodScheduled 条件生效 |
| 08:33:39 | startTime（容器启动） |

Pod 创建、调度到 `traincache-worker`、启动在同一秒完成，零等待。DataLoad 是之后手动创建的独立资源，10:00 和 16:26 各跑了一次，没有机制让 Pod 等预热完成。这套装置上数据已经在文件系统里，等数据由 FUSE 冷读承担，调度器不感知数据状态。

## 两次 warmup 的结果差异与 3.9G 污染数据

DataLoad 的 loader Job 在 worker Pod 里执行 `juicefs warmup <worker挂载点>`，两次运行结果不一样：

| 时间（UTC） | warmup 结果 | 证据 |
|---|---|---|
| 02:00:12（10:00 CST） | **0 files (0 B)** | `../survey/evidence/loader-jfs-load.log`、`../survey/evidence/dataload-timing.txt` |
| 08:26:55（16:26 CST） | **548 files (3.9 GiB)** | 执行时终端日志 |

10:00 那次 warmup 是 0 files，但同一脚本里 `ls` 挂载点能看到 shard-00000/00001.tar。两次差异与 worker 生命周期有关，jfsdemo-worker-0 在 12:38 重启过，16:26 的 warmup 跑在重启后的 worker 上。warmup 行为依赖 worker 挂载状态，失败时 DataLoad 仍会以 Complete 结束，10:00 那次 DataLoad 的 PHASE 是 Complete、DURATION 是 6s，见 `../survey/evidence/dataload-status.txt`。

污染数据来自导入时源 URI 拼重了，形如 `train-data//train-data/`，sync 把 JuiceFS 自己的数据块对象也镜像进了文件系统本身。文件系统内容是 16 个 tar（1.97GiB）加 `traincache-demo/chunks/` 垃圾镜像（1.97GiB），合计约 3.9G，见 `../survey/evidence/import-pod.log`。warmup 的 3.9GiB 因此超过 3Gi 缓存配额，热读被 LRU 淘汰打了折；多拉 2 倍数据，系统不上报任何错误，预热成本没有记录。垃圾镜像已随元数据重建消解。裸挂载基线的数字含这批污染数据，三 epoch 换序读实验在干净数据（16 分片，1.97GiB）上做。

## 训练 Pod 读数据

`../survey/evidence/train-pod.log` 显示，经 Fluid 挂载的 `/data` 能看到 16 个 shard-*.tar，`du -sh` 是 3.9G（含垃圾），读 tar 前 1MB 成功，从挂载到读取的完整链路可用。

## 三 epoch 换序读实验

本实验验证 Fluid 与 JuiceFS 会不会随训练阶段和读序变化，自动且超前于消费地预取下一批数据。装置与版本同现勘环境一节。

读序的构造方式：`train-demo` 对挂载点里 16 个 `shard-*.tar` 逐一 `dd`，每分片约 126MiB，每 epoch 合计约 2.0GiB。E0 升序；E1 按 `random.Random(17).sample(range(16),16)` 置换；E2 按 `random.Random(42)` 置换。两个置换离线生成后字面量嵌入 Pod 脚本，经 `sh -n` 校验。复核结果是 E1 与 E2 的实际读序与上述种子置换逐一一致，见 `../survey/evidence/train-pod-3epoch.log`。

观测窗口是 13:26:15Z 到 13:29:20Z，即 E2 完成加 2 分钟。

三个切换点：A 是窗口起点，即 E0 冷启动，消费端 fuse 客户端是新挂载、缓存为空；B 是 E0 到 E1，在 13:27:19.704Z；C 是 E1 到 E2，在 13:27:19.894Z。

三个观测点，采样节奏是 Dataset status 与 Runtime、控制器日志 10s 一轮，MinIO netns 计数与客户端 metrics 3s 一轮，切换点附近更密：

1. 客户端块级与预热日志，看 Runtime、FUSE、CSI 日志里的 warmup 与 prefetch 关键词，以及消费端 fuse 客户端（`:15506`）的缓存 miss 增量。
2. Dataset status 的 `cached` 抬升，看它会不会超前于 Pod 读取。
3. UFS 流量，看 MinIO Pod netns eth0 的 tx 计数，即 JuiceFS 从对象存储拉取的字节。

判读标准是：任一切换点出现任何超前于读取的预热或块获取动作就判主动，三者全无就判被动。

窗口内的时序：

| 时刻（UTC） | 事件 | 证据 |
|---|---|---|
| 13:27:15.5 | CSI NodeStage 与 NodePublish，由 train-demo 自身触发 | `../survey/evidence/fluid-runtime-log-3epoch.txt` |
| 13:27:16–17 | fuse 挂载完成，缓存为空，容量被 freeratio 压至 60,397,976B | `../survey/evidence/fluid-runtime-log-3epoch.txt` |
| 13:27:18.797 | train-demo 开始读，E0 升序 | `../survey/evidence/train-pod-3epoch.log` |
| 13:27:18.80–19.70 | E0 冷读 0.90s，分片 1.7-2.7GB/s；MinIO tx 增加 2,123,741,306B；fuse 客户端 miss 528 块、2.1197GB | `../survey/evidence/train-pod-3epoch.log`、`../survey/evidence/minio-traffic-counters.txt`、`../survey/evidence/juicefsruntime-log-3epoch.txt` |
| 13:27:19.704 | 切换点 B，E0 到 E1，seed17 置换 | `../survey/evidence/train-pod-3epoch.log` |
| 13:27:19.89 | E1 完成，约 0.19s，分片瞬时 11-15GB/s（页缓存命中，UFS 零流量） | `../survey/evidence/train-pod-3epoch.log` |
| 13:27:19.894 | 切换点 C，E1 到 E2，seed42 置换 | `../survey/evidence/train-pod-3epoch.log` |
| 13:27:20.074 | E2 完成，train-demo done | `../survey/evidence/train-pod-3epoch.log` |
| 13:28:11（采样） | Dataset `cached` 首次非空，54.44MiB、占 2.7%，lastUpdateTime 是 13:28:06Z | `../survey/evidence/dataset-status-samples.yaml` |
| 13:30:05 起（窗口外） | 手动 DataLoad 预热 | `../survey/evidence/dataload-manual-check.txt` |

三个观测点在三个切换点上的读数全部是被动行为。

客户端块级与预热日志这个观测点：切换点 A 上读前零自动预热动作，fuse 挂载（13:27:16-17Z）是 Pod 自身触发的挂载编排，不属于数据预取，528 块、2.1197GB 的 miss 全部由 E0 读取同步产生。切换点 B 上窗口内 warmup 与 prefetch 关键词命中 0 条，miss_bytes 此后不再增长。切换点 C 与 B 相同。证据是 `../survey/evidence/fluid-runtime-log-3epoch.txt` 与 `../survey/evidence/juicefsruntime-log-3epoch.txt`。

Dataset `cached` 抬升这个观测点：切换点 A 上首次非空是 13:28:11Z 的采样，晚于读取完成的 13:27:20.074Z，没有超前抬升。切换点 B 上切换后 13:27:30Z 与 13:27:51Z 两次采样的 `cached` 仍是空串。切换点 C 上 13:28:11Z 首次非空晚于两个切换点，此后到窗口结束保持 54.44MiB 不再增长。证据是 `../survey/evidence/dataset-status-samples.yaml`。

UFS 流量这个观测点：切换点 A 上窗口内唯一的批量流量是增加 2,123,741,306B，与 E0 冷读同窗（13:27:18.134Z 到 13:27:21.279Z），读前无流量。切换点 B 上切换点前后无 UFS 批量流量回升，相对增量约 0。切换点 C 与 B 相同，切换后到窗口结束只有约 30KB/s 级的后台抖动。证据是 `../survey/evidence/minio-traffic-counters.txt`。

手动 DataLoad 对照在窗口之外。手动预热可行，只能由外部显式发起。整卷预热是 19 个文件、2.0GiB，warmup 实测 1.0656s，DataLoad 约 6s 后 Complete。两分片子集是 shard-00013.tar 与 shard-00003.tar 共 253MiB，耗时 126.7ms。受控单分片探针是 126.4MiB，耗时 46.7ms，这 126.4MiB 由本地路径就地填入块缓存，两端 eth0 各只有约 9KB 抖动。两轮手动预热产生的 2.52GB 缓存填充在 MinIO pod 的 eth0 计数上不可见，因为走的是同节点路径，而 E0 的跨节点冷读增加 2.1237GB 清晰可见。这是装置级的观测口径差异，见 `../survey/evidence/dataload-manual-check.txt`。

判定结果是无阶段感知。三个切换点与三个观测点全部被动，观测窗口内预热与预取零动作，预热只能由外部显式触发的手动 DataLoad 完成。开源 Fluid 1.1.0-alpha.9 与 JuiceFS CE 1.3.0 这个组合没有按训练阶段或读序切换自动预取下一批数据的机制，也没有按消费顺序持续预取。

### 装置混杂与证据强度

页缓存吸收掉了切换点上可预热的缺失。E1 与 E2 每 epoch 只有约 0.19s 与约 0.18s，分片瞬时 11-15GB/s，UFS 零流量，数据全部命中内核页缓存，消费端在 JuiceFS 层没产生任何 miss。消费端 fuse 的 `read` ops 约 16207，约等于 E0 一次全量读取在 128KB 粒度下的次数，E1 与 E2 基本没经过 FUSE。UFS 流量这个观测点在切换点的区分力因此受限，即便真有按 miss 触发的预取，切换点也没有可被激励的 miss。E0 冷读只用 0.90s，分片 1.7-2.7GB/s，是 kind 内网直连 MinIO 把冷热差压缩了。

`/dev/shm` 只有 64MB。JuiceFS 客户端按 freeratio 把 1GiB 的声明配额压到 60,397,976B，约 57.6MB，欠配制造缓存 miss 的设计意图没有生效，有效容量连单个 126MiB 分片都装不下。证据是 `../survey/evidence/fluid-runtime-log-3epoch.txt` 的 Adjusted cache capacity 行。

证据强度分三档。窗口内零自动预热动作、零 warmup 与 prefetch 关键词、fuse 客户端 miss 全部由 E0 读取同步产生且此后不增长，属于日志级的强证据。UFS 流量在切换点被动这一条因页缓存混杂被削弱，手动预热对照还暴露了流量观测口径的盲区。Dataset `cached` 首次非空晚于读取完成只算弱佐证，Fluid 统计刷新滞后，基线时这个字段甚至是空串。

## JuiceFS CE 裸挂载基线

不经过 Fluid，用挂载镜像直接裸挂载同一个文件系统，装置见 `../survey/manifests/jfs-standalone.yaml`，结果见 `../survey/evidence/standalone-baseline.txt`：

| 项 | 记录 |
|---|---|
| 客户端版本 | juicefs **1.3.0**+2025-07-03.30190ca1 |
| 挂载参数 | `metaurl=redis://redis.default.svc.cluster.local:6379/0`，`--cache-dir /cache --cache-size 3072`，本地缓存模式 |
| 冷读（缓存空，首读 1.97GiB 有效数据） | **1037 ms**，约 1.9 GB/s，kind 集群内网直连 MinIO |
| warmup | 548 files（3.9 GiB，含垃圾镜像），**1193 ms** |
| 热读（重读） | **354 ms**，约 5.6 GB/s，本地缓存命中 |
| 缓存占用 | 2.9G，3.9GiB 数据超出 3GiB 配额，LRU 已淘汰部分 |

冷热差只有 2.9 倍。这套装置没有 tc 限速，集群内网太快，冷读已经接近内存速度，冷热差被压缩，探针阶段在限速装置下的冷热差是 817 倍。warmup 本身开销极小，是秒级通知式拉取，成本在缓存容量和重复预热的字节上。这张表是含污染数据的基线，污染已随 redis 元数据重建消解。

## 能力边界四方对比

预热与调度联动。开源 Fluid 无：webhook 注入 diff 为零，Pod 创建即调度且在同一秒，DataLoad 独立手动。ACK ack-fluid 有缓存亲和感知调度，调度器与缓存引擎交互；多级亲和调度 v1.0.5 起；FUSE 客户端亲和调度语义 v1.0.7 起。JuiceFS CE 与 EE 都无，两者的社区版与企业版都没有调度概念。

预热机制。开源 Fluid 的 DataLoad CRD 含 `schedule` 与 `target[].replicas` 字段（CRD 实证，fluid chart 里的 `data.fluid.io_dataloads.yaml`），warmup 实测可用。ACK ack-fluid 支持定时 DataLoad（Once 与 Cron，v1.0.2 起），target 可配副本数，预热资源限额 v1.0.6 起，定时回收 v1.0.5 起。JuiceFS CE 有 `juicefs warmup` 命令，实测可用。JuiceFS EE 是同一条命令，配合缓存组语义。

块级进度与就绪度门控。开源 Fluid 无，既没有 SchedulingGate 也没有块级就绪概念。ACK ack-fluid 未见，亲和调度仍停在有无缓存这一级。JuiceFS CE 与 EE 都无。

阶段感知与按消费顺序持续预取。开源 Fluid 无，三 epoch 换序读实测三个观测点与三个切换点全被动、零自动预热动作。JuiceFS CE 无，实测 `juicefs warmup` 仅手动触发，观测窗口内零自动预取。ACK ack-fluid 与 JuiceFS EE 未见，前者依据阿里云官方文档，后者依据 JuiceFS 企业版文档，缓存组与 IORPC 是数据放置语义。

缓存亲和注入。开源 Fluid 有 `status.cacheAffinity`，preferred weight 100、required 为空、未注入 Pod，实测。ACK ack-fluid 在 Serverless 场景做 FUSE Sidecar 注入（文档），标准集群走 CSI。JuiceFS CE 与 EE 都没有。

分布式缓存组。开源 Fluid 无。JuiceFS CE 无，只有本地缓存。JuiceFS EE 有，一致性哈希缓存组、IORPC（5.2+）、RDMA（5.3+，Beta）。ACK ack-fluid 一列无对应项。

加速运行时。开源 Fluid 提供开源 Runtime，含 Alluxio、JuiceFS、Jindo、Thin 等。ACK ack-fluid 是阿里云自研的 JindoRuntime 与 EFCRuntime，ECI 本地盘缓存 v1.0.2 起。JuiceFS CE 与 EE 都没有。

三条对照结论。

预热与调度联动，四方都没有做到块级。ACK ack-fluid 最接近，做的是亲和感知调度，粒度仍是节点有没有缓存，而且是商业版，没有块级进度门控，也没有按 Job 子集预热。

开源 Fluid CSI 化后 webhook 零注入，缓存亲和只剩 status 里的软偏好。TrainCache 的设计是 Job 即将调度时按需做块级预热，用 SchedulingGate 门控放行，预热超时就降级放行；四方里没有对应实现。

阶段感知与按消费顺序持续预取，四方中同样未见。开源 Fluid 与 JuiceFS CE 经三 epoch 换序读实测判定为无；ACK ack-fluid 与 JuiceFS EE 维持文档级判断，来源是官方文档，没有实测。

## 三个缺口判定

预热与调度不联动，成立且强化。注入 diff 为零，Pod 不等 DataLoad，ACK ack-fluid 只有粗粒度亲和且来自文档，无阶段感知属于这一缺口在运行期的延伸。

就绪度粒度错配，成立。cacheAffinity 是二值的，即节点有无 worker，没有该 Job 要读的分片就绪比例这个概念。

预热成本无人记账，成立。3.9G 污染事件多拉 2 倍字节且系统零提示，warmup 超配额静默 LRU。

无阶段感知这个判定是预热与调度不联动在运行期的延伸：调度期没有从 Job 到预热再到放行的联动，运行期也没有从读序或阶段切换到预取的接续。
