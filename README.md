# TrainCache

面向 Kubernetes 训练 Job 的数据流加速器。

工业界把数据预热做成启动时的一次性动作：预热完就撒手，训练 Pod 落地后靠 page cache 和回源读数据。epoch 中后期缓存失效、shuffle 打乱顺序、热子集被换出，都会再次出现 data stall，此时没有任何组件再帮它把数据提前铺好。TrainCache 把预热从启动动作扩展成贯穿训练全程的闭环，并把"预热了哪些块"接进调度决策。

## 解决什么

现有方案（以 Fluid 为代表）用两点回答"节点上有没有这个 Dataset"：Runtime 存在与否，以及事后注入的 nodeAffinity。它不知道缓存了哪几块、拉了多少比例，也不知道接下来这个 Job 要读哪些分片。预热 CRD 与调度决策不闭环，重复预热到多个候选节点的字节成本也没人记账。

TrainCache 在块粒度上补这三段：Job 提交时向候选节点按消费顺序预热它真正要读的块；块级就绪度决定何时放行 Pod、给哪个节点打分；Pod 落地后继续按训练实际消费顺序预取。

## 三个阶段

| 阶段 | 时机 | 触发方 | 失败兜底 |
|---|---|---|---|
| 启动预取 | Job 提交到 Pod 落节点 | Controller 用 SchedulingGate 挡住 Pod，按块预热候选节点 | 超过 `t_max` 去掉 gate，按普通调度放行 |
| 持续预取 | Pod 落地到训练结束 | 节点上的 Agent 自治 | 预取跟不上就退化成回源读，不影响正确性 |
| 阶段切换重预取 | epoch、shuffle、curriculum 切换 | 训练侧钩子上报事件 | 切换瞬间允许 stall 短暂回升，重新铺数据后回落 |

调度侧四步：筛选出数据可达、资源够的候选节点，宽预热前 N 个，启动窗口就绪度决定门控放行，全局位置衰减就绪度给剩下的节点打分。门控阈值 `M` 是启动延迟与选点质量之间的权衡。

读路径用 preheat 模型：Agent 把块落到节点本地盘 `/var/cache/traincache/blocks/`，训练 Pod 以 hostPath 只读挂载，直接读本地盘和 page cache，Agent 不在读 I/O 路径上，读延迟与平时一致。代价是读侧信号只能旁路采集，靠 DataLoader 钩子与 inotify 兜底。

## 组成

| 目录 | 内容 |
|---|---|
| `controller/` | 控制面：`DataSet`、`WarmupTask` 两个 CRD，预热编排 Controller，调度器评分插件，Pod 注入 webhook |
| `agent/` | 节点面：`traincache-agentd` DaemonSet，块级缓存、预热执行、gRPC 服务、消费顺序 sidecar |
| `core/` | 缓存库：内容寻址存储、索引分片、S3 兼容对象存储读写与条件写 |
| `bench/` | 实验装置：kind 集群、MinIO、tc 限速与五组对照跑分 |
| `docs/` | 设计、决策、实验协议与发布口径 |

数据源是任意 S3 兼容对象存储（MinIO、COS、OSS、S3）。控制面只记录 endpoint 与凭据引用，Agent 直连桶拉块。

## 快速开始

前置条件：Kubernetes 集群、kubectl、一个 S3 兼容桶。实验与 e2e 在 kind v1.37.0 上验证。

**1. 装控制面。** 用发布包里的 `install.yaml`，四个镜像按 digest 固定：

```bash
kubectl apply -f install.yaml
```

从源码开发时用 `kubectl apply -k controller/deploy`，镜像 tag 为 `dev`，需先 `kind load docker-image` 导入。webhook 依赖 `traincache` namespace 里的 TLS Secret `traincache-webhook-tls` 和 caBundle，需自备。

**2. 建桶与凭据。** `DataSet.spec` 写 `endpoint`（不带 scheme）、`bucket`、`secretRef`；Secret 的字段是 `accessKey` 与 `secretKey`，与 DataSet 同 namespace。桶的接入方式见 [云对象存储部署指南](docs/0.云对象存储部署指南.md)。

**3. 提交 Job。** webhook 给匹配 `WarmupTask` 的 Pod 注入 SchedulingGate，Controller 收 `WarmupTask` 后按块向候选节点预热；训练 Pod 自己以 hostPath 只读挂载 `/var/cache/traincache/blocks/`，样例见 `agent/deploy/train-pod.yaml`。运行时参数集中在 ConfigMap `traincache-config`，见[参数配置](docs/PARAMETER-CONFIGURATION.md)。

**4. 命令行。** 发布包含两个 linux/amd64 静态二进制：`traincache`（gRPC 客户端）与 `trainpublish`（快照发布器）。

```bash
sha256sum --ignore-missing -c SHA256SUMS
install -m 0755 traincache-linux-amd64 /usr/local/bin/traincache
install -m 0755 trainpublish-linux-amd64 /usr/local/bin/trainpublish
traincache help
```

CLI 默认连 `127.0.0.1:9410`，访问集群内 Agent 先做端口转发。参数见 [CLI 接口](docs/CLI-INTERFACE.md)。

## 实验

`bench/` 是本项目的正式实验装置：kind 起的 5 节点集群、MinIO 源站、tc 限速，七组态对照（direct 直读、JuiceFS 冷/热、Fluid 冷/热、TrainCache 冷/热）在同一负载形态下跑分。

```bash
make bench-up                        # 建 kind 集群、MinIO、控制面，挂 tc
make bench-status && make bench-doctor
make bench COND=<cond-name>          # 跑一个实验点
```

数据与产物一律落在 `~/tcd/`，仓库内只存脚本与清单。装置细节与条件清单见 [bench 装置说明](docs/4 bench装置说明.md) 与[实验协议](docs/9.2 实验协议.md)。

## 构建与测试

```bash
make verify        # build + vet + go test -race + gofmt，三个模块
make verify-metadata   # 版本口径、镜像名、文档相对链接
hack/build-images.sh   # 四个开发镜像（:dev），构建前先补基础镜像官方短名
make e2e           # kind 里的端到端
```

## 文档

| 文档 | 内容 |
|---|---|
| [1 TrainCache介绍与背景](docs/1 TrainCache介绍与背景.md) | 问题定义、外部事实、与 Fluid/JuiceFS 的定位差异 |
| [2 TrainCache三层架构](docs/2 TrainCache三层架构.md) | 控制面、节点面、数据源的分工与数据面约定 |
| [3 现勘报告](docs/3 现勘报告.md) | 现场勘查数据与缺口证据 |
| [4 bench装置说明](docs/4 bench装置说明.md) | 实验装置、条件清单、产物格式 |
| [5.x 缓存核心库](docs/5.1 design.md) | 设计、决策、测试 |
| [6.x Agent 服务化](docs/6.1 design.md) | 同上，对应节点面 |
| [7.x Controller & Scheduler](docs/7.1 design.md) | 同上，对应控制面 |
| [8.x 日志与监控](docs/8.1 design.md) | 指标、日志契约与使用说明 |
| [9.1 决策](docs/9.1 decisions.md) | 验收与实验口径的裁决 |
| [9.2 实验协议](docs/9.2 实验协议.md) | 预注册假设、口径、条件矩阵 |
| [发布约束](docs/RELEASE-CONSTRAINTS.md) | 版本、镜像、发布物与 CI 口径 |
