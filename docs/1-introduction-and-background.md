## TrainCache 介绍

TrainCache 是面向 K8s 训练 Job 全程的数据流加速器。工业界把数据预热做成启动时的一次性操作，预热完成后不再管理；训练 Pod 落到节点后，epoch 中后期出现的 data stall 没有组件处理。

TrainCache 把启动预取扩展到训练全程，分两段：

- 调度阶段：按 Job 实际消费的数据子集，以块粒度向候选节点预热，按细粒度就绪度门控并给作业打分；节点准备超时则自动降级。
- Pod 调度到节点后：Agent 按消费顺序持续预取后续块；epoch 切换或 shuffle 重排时按新顺序重新预取。

收益用 makespan 与分阶段曲线量化，对照组是真实部署的 Fluid 和 JuiceFS，同样负载下跑分。

#### 定位

- 对照：Fluid（Dataset/DataLoad/Runtime 加缓存亲和调度）与 JuiceFS 社区版作为真实对照组部署跑分。
- 测量：核心指标四个，Job 从提交到跑起第一个 step 的时间、整个 epoch 的 makespan、训练全程的 dataloader stall 占比、缓存命中率分阶段曲线。预热时重复搬运的字节计入成本；允许阴性结论，实验开始前预注册假设。
- 改进去向：现勘证实的缺口以测量数据支撑，向 Fluid 提 issue/PR。

范围边界：TrainCache 的交付物是预热、门控、打分与节点内持续预取，数据源保持 S3 对象存储，训练读取仍走本地盘与 page cache；功能验证在 CPU 训练循环上完成，不需要 GPU；epoch 是 Pod 内部循环，阶段切换作为节点内缓存策略实现，不进 K8s 调度路径。

## 外部事实

- Fluid 的缓存亲和度是粗粒度二进制加事后注入：Pod 引用了 Dataset A，节点装了 A 的 Runtime 就加分，没装不加分；Pod 创建后才由 webhook 往 spec 里加 nodeAffinity，Pod 自带 affinity/nodeSelector 时跳过注入。判定只看「节点上有没有这个 Dataset 的 runtime/缓存」，缓存了多少比例、哪些块不参与加分。
- 预热与调度互盲：DataLoad 得不到调度信息，拿不到「哪个 Job 需要哪些分片」，只能手动拉全量；webhook 注入亲和时只检查节点上有没有 Runtime，不检查数据拉了多少、拉了哪几块，可能把 Job 派到「Runtime 开着但数据只拉了 10%」的节点。工业方案的预热针对整个 Dataset 或某个 prefix，不针对具体 Job 要读的分片；预热是独立、手动创建的 CRD，调度不按 DataLoad 的块级进度门控。
- JuiceFS 社区版只有单机本地缓存，分布式缓存组（一致性哈希环、peer 间共享块）、IORPC、RDMA 都是企业版能力，实测边界见 3 现勘报告 §4。
- 2026 年训练数据加载的主流形态：数据打包成百 MB 级分片（webdataset tar 或 Mosaic MDS），确定性 shuffle（用种子控制）下顺序流式读取，不再是百万小文件的元数据压力场景。训练主负载是「百 MB 分片 + 真实 DataLoader」。
- 数据并行下的分片对应：整份数据被所有 Pod 合起来跑一遍，每个 Pod 只跑自己的分片，Pod 之间不重复。预热要按 Pod 分片对应到候选节点，每个候选节点只预热它要承接的那部分。

## TrainCache 要补的缺口

1. 训练全程的数据流没有组件加速：工业方案把预热当成启动时的一个动作，预热完之后训练 Pod 靠自己读 page cache 和回源。epoch 中后期 page cache 失效、shuffle 之后顺序被打乱、热子集被换出，都会再次造成 data stall，此时 Pod 已在节点上运行，没有组件再为它提前拉数据。
2. 预热与调度不联动：工业方案先手动 DataLoad、再靠二进制亲和选节点，这两步之间缺少「Job 即将调度 → 向哪几个候选节点预热它需要的那些块 → 块级就绪度决定何时放行/如何打分 → 超时如何降级」的控制回路。
3. 预热成本没有记录：向 2–3 个候选节点重复预热有 2–3 倍字节与带宽代价，什么情况下这笔代价值得，没有公开曲线。
