# P1 对手现勘报告(Fluid / JuiceFS 机制现勘)

- 日期:2026-10-09(2026-10-13 补做阶段感知实验,并轻修恢复装置后重装,见 §1.1)
- 环境:kind 集群 `traincache`(3 节点,k8s **v1.37.0**,containerd 2.3.4;节点镜像 kindest/node:v1.37.0,宿主机上现仅剩 dangling 状态,见 §6)
- 全部证据归档:`../survey/evidence`(本报告每条判断均标注对应证据文件)
- 版本:**开源 Fluid 1.1.0-alpha.9**(镜像 1.1.0-36f0467,helm chart fluid-1.1.0-alpha.9)、**JuiceFS CE 1.3.0**(客户端 1.3.0+2025-07-03.30190ca1)、JuiceFS CSI v0.32.0(本地镜像)、MinIO latest、redis 7-alpine

## 0. 摘要与闸门判定

这次现勘一共验证了三个问题,结论如下:

1. **Fluid 到底有没有做"预热—调度"联动?** 没有,而且比 P1 笔记预想的更彻底:开源 Fluid 1.1.0-alpha.9 在 JuiceFSRuntime 上**连 webhook 的粗粒度注入都消失了**(实测注入前后 diff 为零,见 §2.2),预热(DataLoad)和调度(Pod 一创建就被调度,时间戳同秒)完全独立。
2. **JuiceFS 社区版的能力边界在哪?** 确认:CE 只有本地缓存;分布式缓存组、IORPC、RDMA 都是企业版(5.2/5.3+)的能力,有官方文档佐证。
3. **对手有没有"阶段感知/按消费顺序持续预取"的能力?** **判定:没有**(见 §2.6):三 epoch 换序读实验在三个切换点 × 三个观测点(客户端块级日志、Dataset cached 抬升、UFS 流量)上全部被动,观测窗口内零超前于读取的自动预取动作;手动 DataLoad 预热可行,但必须外部显式触发。

**闸门判定:缺口成立 → 核心1 按"闭环"叙事推进**(P1 现勘闸门 ①)。对手"无阶段感知"同样成立(§2.6),"阶段感知/按消费顺序持续预取"列为缺口项,证据已并入 §4 的边界表。

## 1. 现勘环境与部署(动作1)

全部组件镜像都在宿主机上拉好,再用 `kind load` 送进集群:

| 组件           | 版本/形态                                                                                                                 | 证据                                                                                            |
|----------------|---------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| kind 集群      | 3 节点(control-plane + worker + worker2),k8s **v1.37.0**(containerd 2.3.4)                                                | `evidence/kubectl-get-nodes.txt`(集群在线可复核)、`evidence/kind-repair-log.txt`                |
| Fluid          | helm chart fluid-1.1.0-alpha.9,APP VERSION 1.1.0-36f0467                                                                  | `evidence/fluid-version.txt`、`evidence/fluid-install-helm.txt`                                 |
| Fluid 组件     | dataset-controller / juicefsruntime-controller / fluid-webhook / fluidapp-controller / fluid-csi(DaemonSet×3)全部 Running | `evidence/fluid-pods.txt`、`evidence/fluid-pods-after-reinstall.txt`                            |
| JuiceFS 客户端 | ce-v1.3.0(挂载镜像 juicedata/mount:ce-v1.3.0)                                                                             | `evidence/standalone-baseline.txt` 首行、`evidence/juicefsruntime-log-3epoch.txt`(mount 版本行) |
| JuiceFSRuntime | jfsdemo:tieredstore path=/dev/shm、MEM、quota **1Gi**(自 3Gi 下调,见 §1.1)                                                | `evidence/jfsruntime-describe.yaml`                                                             |
| 元数据引擎     | redis:7-alpine(单实例,AOF 未开)                                                                                           | 集群内 `default/redis-*`                                                                        |
| 对象存储       | MinIO(latest),bucket `train-data`,16×126MB tar 裸对象(1.97GiB)                                                            | 阶段二上传,2GB;2026-10-13 重传见 `evidence/kind-repair-log.txt`                                 |
| 训练 Pod       | busybox `train-demo`,三 epoch 换序读(读序字面量内嵌,见 §2.6)                                                              | `evidence/train-pod-3epoch.log`                                                                 |

装置和探针阶段共用同一批 16×126MB tar 数据(种子固定可复现)。

### 1.1 集群修复与重装(2026-10-13 轻修)

**现象与根因**:kind 节点容器重启之后,containerd 里残留着旧容器条目,造成 name reservation 冲突(kubelet 日志:"check if another CreateContainer request is in progress: name ... is reserved for ..."),kube-system 和 minio 命名空间里多个 Pod(修复前约 10 个)报 CreateContainerError,整个 Fluid 也不见了(没有 fluid-system ns、没有 CRD、helm list 为空)。

**修复选择**:我们选了**第一档轻修**,逐个节点删掉 Exited 容器条目,再重启 kubelet:

```
for n in traincache-control-plane traincache-worker traincache-worker2; do
  docker exec $n bash -c "crictl ps -a | grep Exited | cut -d' ' -f1 | xargs -r crictl rm -f"
  docker exec $n systemctl restart kubelet
done
```

一共删掉 12 个残留条目(control-plane 7 + worker 2 + worker2 3),修完之后 `kubectl get pods -A` = 23/23 Running。**没有走第二档重建**:重建得先把 kindest/node:v1.37.0 的镜像 tag 恢复回来(宿主机上现在只剩 dangling 状态,见 §6),而且有 docker hub 拉不通的风险。

**镜像**:docker hub 到不了,所以全部走 `kind load`(redis:7-alpine、busybox:latest,外加 9 个 Fluid/JuiceFS 镜像:7 个 fluidcloudnative:v1.1.0-36f0467 全套 + juicefs-csi-driver:v0.32.0 + mount:ce-v1.3.0)。

**Fluid 重装**:官方 helm repo index 没收录 alpha 版本(/tmp/fluid 的残留也早没了),所以改成从 GitHub release 直接下离线 tgz(`helm-chart-fluid-1.1.0-alpha.9`),再 `helm install` 到 fluid-system(带 `--set runtime.juicefs.enabled=true`);chart 的 appVersion 1.1.0-36f0467 和本地镜像 tag 一致,`helm list` 状态 deployed(`evidence/fluid-install-helm.txt`)。

**数据恢复**:MinIO 里的 train-data bucket 不见了(不是重建引起的,minio Pod 从 3d20h 前就一直存在),我们通过 `kubectl port-forward` 通道重传了 16 个分片共 1.97GiB(耗时 5s);之后照序 apply(redis → jfs-secret → dataset → juicefsruntime),JuiceFSRuntime 的 Worker/FUSE Ready、Dataset Bound、PVC Bound。

**装置改动:quota 3Gi→1Gi**。我们把 Runtime 的缓存配额从 3Gi 下调到 1Gi(低于数据集总量 ≈2GB),设计意图是**让每次 epoch 切换都必然产生缓存 miss**,好给"主动预取"留出可观测的发挥空间。这个改动的装置副作用见 §2.6(节点 /dev/shm 只有 64MB,配额名存实亡)。

## 2. 官方 demo 与证据抓取(动作2)

### 2.1 链路与数据导入

Dataset `jfsdemo` + JuiceFSRuntime(同名绑定)→ format(元数据进 redis,数据块落 MinIO)→ **数据导入**(裸对象→JuiceFS 文件系统)→ DataLoad(warmup)→ 训练 Pod。

导入方式(踩坑后定稿):`juicefs sync` 以 `jfs://` 协议直写文件系统,源 URI 为路径式 `minio://AK:SK@endpoint/bucket`。这个镜像里的客户端(ce-v1.3.0)已经移除了 `juicefs import` 命令;Fluid 的 PVC 只读,挂载写不可行。

### 2.2 发现一:webhook 注入前后 diff 为零

dry-run(绕过 webhook)和实际创建(经过 webhook)的 Pod spec 对比(`evidence/pod-before-injection.yaml` vs `pod-after-injection.yaml`):

| 观察项                           | 结果                                                                                                                                                 |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| FUSE sidecar 容器注入            | **无**——容器列表里只有用户的 busybox。FUSE 由 Fluid CSI(node plugin)+ `jfsdemo-fuse` Pod(每节点一个)在 kubelet 层完成挂载,Pod 内只见普通 volumeMount |
| nodeAffinity / nodeSelector 注入 | **无**——P1 笔记描述的"webhook 注入亲和"在这个版本 + Runtime 组合下不复现                                                                             |
| 其他注入                         | 仅 k8s 默认字段(serviceaccount projected volume、默认 tolerations)                                                                                   |

而 `JuiceFSRuntime.status` 里 Fluid 维护着 `cacheAffinity`(`evidence/runtime-status.txt`):worker 节点被打上 `fluid.io/f-default-jfsdemo=true` 标签,亲和策略为 `preferredDuringSchedulingIgnoredDuringExecution`(weight 100),**required 项为空,而且没有注入到任何 Pod**。

**解读**:Fluid 1.1 把 JuiceFS 访问全面 CSI 化之后,传统的 webhook sidecar 注入路径就不走了;缓存亲和退化成了 runtime status 里的"软偏好 + 节点标签",既不强制也不自动。跟 P1 笔记里的外部事实"粗粒度二进制 + 事后注入"比,注入这一环已经弱化了不少,但粒度问题还在——它只看"节点有没有缓存 worker"这种二值信息,看不到块级进度。

### 2.3 发现二:Pod 不等 DataLoad(时序证据)

`evidence/timeline-notes.txt`(全部同一秒,来自 `kubectl get pod -o jsonpath`):

| 时刻(UTC) | 事件                         |
|-----------|------------------------------|
| 08:33:39  | train-demo creationTimestamp |
| 08:33:39  | PodScheduled 条件生效        |
| 08:33:39  | startTime(容器启动)          |

Pod 从创建,到被调度到 `traincache-worker`,再到启动,**同一秒就完成,零等待**。DataLoad 是之后手动创建的独立资源(10:00 和 16:26 各跑了一次),**没有任何机制让 Pod 等预热完成**——"先被调度、再等数据"的 P1 论断成立(而且在这套装置上数据已经在文件系统里,连"等数据"都由 FUSE 冷读承担,调度器一无所知)。

### 2.4 发现三:两次 warmup 的结果差异与 3.9G 污染事件

DataLoad 的 loader Job 在 worker Pod 里执行 `juicefs warmup <worker挂载点>`,两次运行结果不一样:

| 时间(UTC)           | warmup 结果             | 证据                                                  |
|---------------------|-------------------------|-------------------------------------------------------|
| 02:00:12(10:00 CST) | **0 files (0 B)**       | `evidence/loader-jfs-load.log`、`dataload-timing.txt` |
| 08:26:55(16:26 CST) | **548 files (3.9 GiB)** | 执行时终端日志(用户记录)                              |

10:00 那次 warmup 是 0 files,但同一脚本里 `ls` 挂载点能看到 shard-00000/00001.tar——两次的差异跟 worker 生命周期有关(jfsdemo-worker-0 在 12:38 重启过,16:26 的 warmup 跑在重启后的 worker 上),**结论按实记录:warmup 行为依赖 worker 挂载状态,失败时 DataLoad 仍会以 Complete 结束**(10:00 那次 DataLoad PHASE=Complete、DURATION 6s,`evidence/dataload-status.txt`)。

**3.9G 污染事件(装置侧教训,如实记录)**:导入时源 URI 拼重了(`train-data//train-data/`),导致 sync 把 JuiceFS 自己的数据块对象也镜像进了文件系统本身,文件系统 = 16 tar(1.97GiB)+ `traincache-demo/chunks/` 垃圾镜像(1.97GiB)≈ 3.9G(`evidence/import-pod.log`)。这直接让 warmup 的 3.9GiB 超过了 3Gi 缓存配额、热读被 LRU 淘汰打了折——**这恰好是"预热成本无人记账"的装置侧缩影:多拉 2 倍数据,系统一点提示都没有**。垃圾目录的清理状态见 §6 遗留事项(2026-10-13 已随元数据重建消解)。

### 2.5 训练 Pod 读数据(端到端成立)

`evidence/train-pod.log`:经 Fluid 挂载的 `/data` 能看到 16 个 shard-*.tar,`du -sh` 是 3.9G(含垃圾),读 tar 前 1MB 成功——Fluid 链路端到端可用。

### 2.6 发现四:对手无阶段感知(三 epoch 换序读实验)

这一节回答一个新问题:**对手(Fluid + JuiceFS)到底有没有"阶段感知"——也就是随着训练读取的阶段/读序变化,它会自动、持续、超前于消费地去预取下一批数据吗?** 装置与版本同 §1(轻修恢复后重装,见 §1.1)。

**装置**:

- 读序怎么构造:`train-demo` 对挂载点里 16 个 `shard-*.tar` 逐一 `dd`(每分片≈126MiB,每 epoch 合计≈2.0GiB)。E0 升序;E1 按 `random.Random(17).sample(range(16),16)` 置换;E2 按 `random.Random(42)` 置换;两个置换离线生成后**字面量嵌入** Pod 脚本(经 `sh -n` 校验)。复核结果:E1/E2 的实际读序与上述种子置换逐一一致(`evidence/train-pod-3epoch.log`)。
- 观测窗口:13:26:15Z ~ 13:29:20Z(E2 完成 +2min)。
- 三个切换点:A = 窗口起点(E0 冷启动,消费端 fuse 客户端是新挂载、缓存空);B = E0→E1(13:27:19.704Z);C = E1→E2(13:27:19.894Z)。
- 三个观测点(采样节奏:Dataset status 与 Runtime/控制器日志 10s 一轮;MinIO netns 计数与客户端 metrics 3s 一轮,切换点附近更密):
  1. **客户端块级/预热日志**:Runtime/FUSE/CSI 日志里的 warmup/prefetch 关键词,以及消费端 fuse 客户端(:15506)的缓存 miss 增量;
  2. **Dataset status `cached` 抬升**:看它会不会超前于 Pod 读取;
  3. **UFS 流量**:MinIO Pod netns eth0 的 tx 计数(≈ JuiceFS 从对象存储拉取的字节)。
- 判读标准:任一切换点出现任何超前于读取的预热/块获取动作 → 判主动;三者全无 → 判被动。

**观测结果(全为被动行为)**:

窗口内时序:

| 时刻(UTC)           | 事件                                                                                     | 证据                                                                                                             |
|---------------------|------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| 13:27:15.5          | CSI NodeStage/NodePublish(train-demo 自身触发)                                           | `evidence/fluid-runtime-log-3epoch.txt`                                                                          |
| 13:27:16–17         | fuse 挂载完成(缓存空;容量被 freeratio 压至 60,397,976B)                                  | 同上                                                                                                             |
| 13:27:18.797        | train-demo 开始读(E0 升序)                                                               | `evidence/train-pod-3epoch.log`                                                                                  |
| 13:27:18.80–19.70   | E0 冷读:0.90s,分片 1.7-2.7GB/s;MinIO tx +2,123,741,306B;fuse 客户端 miss 528 块/2.1197GB | `evidence/train-pod-3epoch.log`、`evidence/minio-traffic-counters.txt`、`evidence/juicefsruntime-log-3epoch.txt` |
| 13:27:19.704        | 切换点 B:E0→E1(seed17 置换)                                                              | `evidence/train-pod-3epoch.log`                                                                                  |
| 13:27:19.89         | E1 完成:≈0.19s,分片瞬时 11-15GB/s(页缓存命中,UFS 零流量)                                 | 同上                                                                                                             |
| 13:27:19.894        | 切换点 C:E1→E2(seed42 置换)                                                              | 同上                                                                                                             |
| 13:27:20.074        | E2 完成、train-demo done                                                                 | 同上                                                                                                             |
| 13:28:11(采样)      | Dataset `cached` 首次非空:54.44MiB/2.7%(lastUpdateTime 13:28:06Z)                        | `evidence/dataset-status-samples.yaml`                                                                           |
| 13:30:05 起(窗口外) | 手动 DataLoad 预热(见下文对照)                                                           | `evidence/dataload-manual-check.txt`                                                                             |

三观测点 × 三切换点判定表(每格标注证据文件):

| 观测点                  | 切换点      | 观测结果(全部为被动)                                                                                                           | 证据                                                                              |
|-------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| ① 客户端块级/预热日志   | A:E0 冷启动 | 读前零自动预热动作;fuse 挂载(13:27:16-17Z)是 Pod 自身触发的挂载编排,不属于数据预取;528 块/2.1197GB miss 全部由 E0 读取同步产生 | `evidence/fluid-runtime-log-3epoch.txt`、`evidence/juicefsruntime-log-3epoch.txt` |
| ① 客户端块级/预热日志   | B:E0→E1     | 窗口内 warmup/prefetch 关键词命中 0 条;miss_bytes 此后不再增长                                                                 | 同上                                                                              |
| ① 客户端块级/预热日志   | C:E1→E2     | 同 B                                                                                                                           | 同上                                                                              |
| ② Dataset `cached` 抬升 | A:E0 冷启动 | 首次非空(13:28:11Z 采样)晚于读取完成(13:27:20.074Z),无超前抬升                                                                 | `evidence/dataset-status-samples.yaml`                                            |
| ② Dataset `cached` 抬升 | B:E0→E1     | 切换后 13:27:30Z/13:27:51Z 采样 `cached` 仍为空串                                                                              | 同上                                                                              |
| ② Dataset `cached` 抬升 | C:E1→E2     | 13:28:11Z 首次非空晚于两个切换点;此后至窗口结束保持 54.44MiB 不再增长                                                          | 同上                                                                              |
| ③ MinIO netns 流量      | A:E0 冷启动 | 窗口内唯一批量流量 +2,123,741,306B 与 E0 冷读同窗(13:27:18.134Z→13:27:21.279Z),读前无流量                                      | `evidence/minio-traffic-counters.txt`、`evidence/train-pod-3epoch.log`            |
| ③ MinIO netns 流量      | B:E0→E1     | 切换点前后无 UFS 批量流量回升,相对增量≈0                                                                                       | `evidence/minio-traffic-counters.txt`                                             |
| ③ MinIO netns 流量      | C:E1→E2     | 同 B;切换后至窗口结束仅 ~30KB/s 级后台抖动                                                                                     | 同上                                                                              |

**手动 DataLoad 对照(窗口外)**:手动预热**可行,但没有任何自动触发机制,必须外部显式发起**——① 整卷预热(19 files,2.0GiB):warmup 实测 **1.0656s**,DataLoad ~6s Complete;② 两分片子集(shard-00013.tar、shard-00003.tar,253MiB):**126.7ms**;③ 受控单分片探针(126.4MiB):**46.7ms**,而且这 126.4MiB 是由本地路径就地填入块缓存(两端 eth0 各仅 ~9KB 抖动)。两轮手动预热产生的 2.52GB 缓存填充,在 MinIO pod eth0 计数上不可见(同节点路径;正好和 E0 跨节点冷读 +2.1237GB 清晰可见形成对照),这是装置级的观测口径差异,我们如实记录(`evidence/dataload-manual-check.txt`)。

**判定:无阶段感知。** 三个切换点 × 三个观测点全部被动(见上表),观测窗口内预热/预取零动作;预热只能由外部显式触发(手动 DataLoad)。结论:开源 Fluid(1.1.0-alpha.9)+ JuiceFS CE 1.3.0 这个组合**不存在任何"按训练阶段/读序切换自动预取下一批数据"的机制**,更谈不上"按消费顺序持续预取"。

**装置混杂与证据强度限定(如实记录)**:

1. **页缓存吸收,切换点"没有可预热的缺失"**:E1/E2 每 epoch 只有 ≈0.19s / ≈0.18s、分片瞬时 11-15GB/s,零 UFS 流量——数据全部命中内核页缓存,消费端在 JuiceFS 层没产生任何 miss(消费端 fuse `read` ops≈16207,约等于 E0 一次全量读取在 128KB 粒度下的次数,E1/E2 基本没经过 FUSE)。所以观测点③在切换点的区分力受限:即便真有"按 miss 触发"的预取,切换点也没有可被激励的 miss。E0 冷读只用 0.90s(分片 1.7-2.7GB/s),也是因为 kind 内网直连 MinIO,冷热差被压缩了。
2. **/dev/shm 只有 64MB**:kind 节点的 /dev/shm 是 64MB,JuiceFS 客户端按 freeratio 把 1GiB 的声明配额又压到 **60,397,976B(≈57.6MB)**——"quota 3Gi→1Gi 欠配"的设计名存实亡(容量小到连单个 126MiB 分片都装不下);证据:`evidence/fluid-runtime-log-3epoch.txt`(Adjusted cache capacity 行)。
3. **证据强度分级**:日志级证据(窗口内零自动预热动作、零 warmup/prefetch 关键词;fuse 客户端 miss 全部由 E0 读取同步产生,而且此后不增长)属于**强**;"UFS 流量在切换点被动"这一条因为页缓存混杂而被**削弱**(手动预热对照还暴露了流量观测口径的盲区);Dataset `cached` 首次非空晚于读取完成只能算**弱佐证**(Fluid 统计刷新滞后,基线时这个字段甚至是空串)。
4. **(可选)补测方向**:放大 /dev/shm,或者改用 O_DIRECT 绕过页缓存,做一次"切换级纯 UFS 缺失"实验,就能把观测点③在切换点的区分力补满。

**实验后装置状态**:train-demo、手动 DataLoad(及其 loader Job)、import pod 都已清理;PVC/Dataset/Runtime 完好;未回收的 fuse pod 与节点标签、镜像 dangling 等见 §6。

## 3. 独立 JuiceFS CE 对照组与 warmup 基线(动作4)

不经过 Fluid,用挂载镜像直接裸挂载同一个文件系统(`../survey/manifests/jfs-standalone.yaml`),装置与结果(`evidence/standalone-baseline.txt`):

| 项                                 | 记录                                                                                                          |
|------------------------------------|---------------------------------------------------------------------------------------------------------------|
| 客户端版本                         | juicefs **1.3.0**+2025-07-03.30190ca1                                                                         |
| 挂载参数                           | `metaurl=redis://redis.default.svc.cluster.local:6379/0`,`--cache-dir /cache --cache-size 3072`(本地缓存模式) |
| 冷读(缓存空,首读 1.97GiB 有效数据) | **1037 ms**(≈1.9 GB/s,kind 集群内网直连 MinIO)                                                                |
| warmup                             | 548 files(3.9 GiB,含垃圾镜像),**1193 ms**                                                                     |
| 热读(重读)                         | **354 ms**(≈5.6 GB/s,本地缓存命中)                                                                            |
| 缓存占用                           | 2.9G(3.9GiB 数据超出 3GiB 配额,LRU 已淘汰部分)                                                                |

**基线解读**:① 冷热差只有 2.9×——这套装置没有 tc 限速,集群内网太快,冷读已经接近内存速度,冷热差被压缩了(探针阶段在限速装置下是 817×);② warmup 本身开销极小(秒级"通知式"拉取),真正的成本在缓存容量和重复预热的字节上;③ 这个数字是"含污染数据"的基线——污染数据后来随 redis 元数据重建消解了(§6),§2.6 的阶段感知实验就是在干净数据(16 分片,1.97GiB)上做的。

## 4. 能力边界表(动作3,四方对比)

每条判断都附版本号与证据来源(实测/CRD/官方文档):

| 能力                        | 开源 Fluid(实测 1.1.0-alpha.9,镜像 36f0467)                                                                                   | ACK ack-fluid(阿里云文档,版本标注)                                                          | JuiceFS CE(实测 1.3.0)                                        | JuiceFS EE(文档,5.2/5.3+)                                            |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|---------------------------------------------------------------|----------------------------------------------------------------------|
| 预热与调度联动              | **无**:webhook 注入 diff 为零(实测);Pod 创建即调度(实测同秒);DataLoad 独立手动                                                | 缓存亲和感知调度(调度器与缓存引擎交互);多级亲和调度 v1.0.5+;FUSE 客户端亲和调度语义 v1.0.7+ | 无(无调度概念)                                                | 无(无调度概念)                                                       |
| 预热机制                    | DataLoad CRD 含 `schedule`、`target[].replicas` 字段(CRD 实证,`/tmp/fluid/crds/data.fluid.io_dataloads.yaml`);warmup 实测可用 | 定时 DataLoad(Once/Cron,v1.0.2+);target 可配副本数;预热资源限额 v1.0.6+;定时回收 v1.0.5+    | `juicefs warmup` 命令(实测)                                   | 同命令,配合缓存组语义                                                |
| 块级进度/就绪度门控         | **无**(无 SchedulingGate、无块级就绪概念)                                                                                     | 未见(亲和调度仍为"有无缓存"级)                                                              | 无                                                            | 无                                                                   |
| 阶段感知/按消费顺序持续预取 | **无**(三 epoch 换序读实测:三观测点×三切换点全被动、零自动预热动作,§2.6)                                                      | 未见(维持原判断;依据阿里云官方文档,未见过"阶段/消费顺序"语义的预取特性)                     | **无**(实测:`juicefs warmup` 仅手动触发,观测窗口内零自动预取) | 未见(维持原判断;依据 JuiceFS 企业版文档,缓存组/IORPC 为数据放置语义) |
| 缓存亲和注入                | status.cacheAffinity(preferred weight 100,required 空,**未注入 Pod**,实测)                                                    | Serverless 场景 FUSE Sidecar 注入(文档);标准集群走 CSI                                      | —                                                             | —                                                                    |
| 分布式缓存组                | 无                                                                                                                            | —                                                                                           | **无(仅本地缓存)**                                            | **有**:一致性哈希缓存组、IORPC(5.2+)、RDMA(5.3+,Beta)                |
| 加速运行时                  | 开源 Runtime(Alluxio/JuiceFS/Jindo/Thin 等)                                                                                   | 阿里云自研 JindoRuntime、EFCRuntime;ECI 本地盘缓存 v1.0.2+                                  | —                                                             | —                                                                    |

**对照结论**:

1. "预热—调度"联动在**四方中没有任何一方做到块级**;ACK 做得最接近(亲和感知调度),但仍是"节点有没有缓存"的粗粒度,而且是商业版,不含块级进度门控,也不含按 Job 子集预热。
2. 开源 Fluid 的注入行为比 P1 笔记预想的更弱(CSI 化后 webhook 零注入)——TrainCache 的闭环设计("Job 即将调度→按需块级预热→SchedulingGate 门控→超时降级")放在这个对比里就是一块**真空地带**,谈不上重复造轮子。
3. **"阶段感知/按消费顺序持续预取"在四方中同样未见**:开源 Fluid 和 JuiceFS CE 经三 epoch 换序读实测判定为无(§2.6);ACK/EE 维持原文档级判断(来源为官方文档,没有实测)。

## 5. 缺口判定(P1 三个"缝")与叙事方向

| 缝                 | 判定            | 现勘证据                                                                                  |
|--------------------|-----------------|-------------------------------------------------------------------------------------------|
| ① 预热与调度不闭环 | **成立,且强化** | 注入 diff 为零;Pod 不等 DataLoad;ACK 仅有粗粒度亲和(文档);无阶段感知(§2.6,属于运行期延伸) |
| ② 就绪度粒度错配   | **成立**        | cacheAffinity 为二进制(节点有无 worker),没有"该 Job 要读的分片就绪比例"这个概念           |
| ③ 预热成本无人记账 | **成立**        | 3.9G 污染事件:多拉 2 倍字节,系统零提示;warmup 超配额静默 LRU                              |

**闸门判定:缺口被证据确认 → 核心1 按"闭环"叙事推进,不带假设开工。**

§2.6 的"无阶段感知"判定,是缝①在运行期语义上的延伸:对手既没有"Job→预热→调度"的闭环,也没有"读序/阶段切换→预取"的闭环。

## 6. 遗留事项

1. **未回收装置残留**(2026-10-13 新增,待用户决定):实验后 fuse pod `jfsdemo-fuse-2467z`(节点 traincache-worker)与节点标签 `fluid.io/f-default-jfsdemo=true` 仍未回收——fuse Pod 的清理策略是 OnRuntimeDeleted(NodeUnstageVolume 记录),不删 Runtime 就不会回收;如果需要手动摘标签:`kubectl label node traincache-worker fluid.io/f-default-jfsdemo-`。证据:`evidence/jfsruntime-describe.yaml`、`evidence/fluid-runtime-log-3epoch.txt`。
2. **kindest/node 镜像仅剩 dangling**(2026-10-13 新增):宿主机上 kindest/node:v1.37.0 的 RepoTags 已经丢了(只剩 image ID `sha256:24f646398ba8...`);以后要重建集群,必须先 `docker tag` 把镜像 tag 恢复回来,否则 kind 会尝试联网拉取(而 docker hub 不可达)。证据:`evidence/kind-repair-log.txt`。
3. **3.9G 污染数据已消解**(2026-10-13,原遗留事项 1 关闭):redis 元数据重建 + 裸对象重传(16 分片/1.97GiB)之后,垃圾镜像目录没有再出现;原先"删除 `traincache-demo/` 垃圾目录"那条命令不再需要执行——阶段感知实验(§2.6)就是在干净数据上做的。证据:`evidence/kind-repair-log.txt`。
4. **Fluid 仍是 alpha 版**(原遗留事项 2 延续):1.1.0-alpha.9(稳定线 1.0.8);2026-10-13 重装后还是这个版本(镜像 tag 1.1.0-36f0467)。本报告里所有开源 Fluid 结论都标注了 alpha 状态;如果要出正式结论,建议用 1.0.8 再测一遍(需要另拉 5 个镜像)。
5. **ACK 侧证据都是文档级**(沿用原遗留事项 3):没有在真实 ACK 集群里验证过(也无法访问);§4 边界表(含新增的"阶段感知"行)里 ACK 的判断都标注了来源为官方文档。
6. **JuiceFS CSI 独立对照组没有执行**(沿用原遗留事项 4):需要 5 个新镜像(mount ce-v1.4.0 + sig-storage sidecar×4);裸挂载对照组(§3)已经覆盖了"独立 JuiceFS 基线"这个目标。

## 参考来源

- [Fluid 官方文档](https://fluid-cloudnative.github.io/docs/core-concepts/architecture-and-concepts)
- [在 Fluid 中使用 JuiceFS(JuiceFS 文档)](https://juicefs.com/docs/zh/cloud/kubernetes/fluid/)
- [ack-fluid 组件介绍与发布记录(阿里云)](https://www.alibabacloud.com/help/en/ack/cloud-native-ai-suite/product-overview/ack-fluid)
- [JuiceFS IORPC 协议](https://juicefs.com/docs/cloud/guide/iorpc/)、[分布式缓存 RDMA 加速](https://www.juicefs.io/docs/zh/cloud/guide/distributed-cache-rdma/)
- [JuiceFS 云服务(企业版特性)](https://beta.juicefs.com/zh-cn/product/cloud-service)
- [JuiceFS CSI 安装文档](https://juicefs.com/docs/zh/csi/getting_started/)
