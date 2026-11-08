## 层1 k8s control plane

#### 资源预热

定义 DataSet 与 WarmupTask 两种 CRD，一个 Controller（reconcile）对比资源与期望状态，调谐这些 CRD。

manifest 引用数据子集与消费顺序 hint，声明需要预热哪些块以及顺序。Controller 向节点上的 Agent 下达预热命令，Agent 完成块预热后通过 `Dataset.status` 反馈给 Controller。

#### 筛选、门控与打分

筛选（粗粒度看数据）：从集群所有节点中排除没资格参与本次调度的节点，查看资源条件、数据可达性、节点健康状态、Job 亲和性，输出候选节点集合的前 N 个进入宽预热。

宽预热：Controller 按 Job 消费顺序向候选节点预热数据块。顺序严格按训练消费顺序（块 0, 1, 2, ...），粒度是块级别，Job 提交时开始，持续到门控放行（训练中继续）。

门控（中粒度看数据）：决定现在能不能放行 Pod 调度。Pod 上挂着 SchedulingGate，gate 在则 K8s 调度器不调度它，Pod 保持 Pending。判据是启动窗口就绪度：窗口内块就绪个数。启动窗口取前 W 个块，覆盖从训练开始到 GPU 打满的那段，这个数据按可选提供 + 默认推导 + 兜底三层设计。放行条件三选一，任意满足即移除 Gate：

（1）存在至少 M 个候选节点，其 S_startup ≥ 门控阈值
（2）边际收益 < 成本（下一个候选预估分差 < Δ_score）
（3）超时 t_max

M 是在启动延迟与选择质量之间的权衡。

打分（细粒度看数据）：放行后选哪个节点，用全局位置衰减就绪度，覆盖启动加全程，选对整体数据流加速最好的节点。窗口内权重为 1，窗口外按距离指数衰减，权重低于阈值就不再计算（防止数据用到时缓存已淘汰，同时省计算），选 S(node) 最高的节点。

#### 训练中的三阶段

| 阶段           | 时机                              | 触发方                         | 优先级                              | 失败兜底                                    |
|:---------------|:----------------------------------|:-------------------------------|:------------------------------------|:--------------------------------------------|
| 调度-启动预取  | Job 提交到 Pod 落节点             | Controller 通过 SchedulingGate | 高（阻塞门控）                      | 超时降级，去 gate 正常调度                  |
| 训练中持续预取 | Pod 调度完成到训练结束            | Agent 自治                     | 低（可被启动预取打断）              | 预取跟不上就退化成回源读，不影响正确性      |
| 阶段切换重预取 | epoch / shuffle / curriculum 切换 | DataLoader 钩子上报事件        | 中（高于持续预取，低于新 Job 启动） | 切换瞬间允许 stall 短暂回升，重新预取后回落 |

## 层2 节点层

#### 缓存Agent DaemonSet

Agent 以 DaemonSet 部署，每个节点一个，职责是接收 Controller 的预热指令、去源站下载数据、落到本地盘、回报进度。三类工作：启动时按阶段首批预取；训练中持续预取（按消费顺序、低优先级、可取消）；阶段切换重预取（按新顺序，降级旧阶段的在途请求）。

抓取指令三条约束：有界并发（按字节与信号量限制在途字节数）；多个相同指令合并；查本地缓存，命中就不触及数据源。

#### 训练Pod

训练 Pod 通过 hostPath 只读挂载 Agent 下载数据的目录 `/var/cache/traincache/`，直接读本地盘与 page cache，不经过 Agent，读路径零额外延迟（preheat 模型，见下文数据面约定）。

DataLoader 钩子（已插桩）上报实际打开的块、当前 epoch、shuffle seed。新的 epoch / shuffle / curriculum 切换事件通过 Unix Socket 通知 Agent。

#### 消费顺序的来源

训练 Pod 里跑一个轻量的消费顺序生成器 sidecar（或 DataLoader 内的钩子），按 Job 配置（dataset、epochs、shuffle_seed、batch_size、prefetch_factor）算出当前阶段按消费顺序排列的块列表，通过共享内存或 Unix socket 传给同节点的 Agent。

Agent 拿到顺序后，把阶段首批（默认前 K 块，K = batch_size × prefetch_factor）作为高优先级队列送进抓取管线，剩余块进低优先级持续预取队列。

## 层3 数据源

数据源使用 S3 对象存储。kind 集群开发演练时部署 MinIO 或其他 S3 兼容存储做测试，真实环境直接连腾讯云 COS / 阿里云 OSS / AWS S3，接入配置见部署指南的对象存储的环境变量一节。

合并回源网关常驻集群内，Agent 的块回源一律经它转发，网关契约见 9.1。自搭 MinIO 也挂在网关后面，网关与 MinIO 允许同节点，须是两个独立 Pod。

## 数据面约定

preheat 模型：Agent 把数据块落到 `/var/cache/traincache/<ds>/blocks/`，训练 Pod 通过 hostPath PV 只读挂载这个目录，读数据直接走内核 page cache，Agent 不参与读 I/O 路径，读路径没有额外延迟。代价是读侧信号只能靠旁路（DataLoader 钩子）采集。
