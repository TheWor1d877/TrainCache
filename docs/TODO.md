# TODO

按处理方式分组；组内按 [A]/[B]/[C]/[D] 分级排。每条写清落点与证据。

## 需要改代码（逻辑、清单、流程）

(thewor1d877)： [B] checkpoint 容量写满用例未跑：`checkpoint/e2e` 覆盖保存与恢复，没有在小容量卷上触发 ENOSPC；失败路径只由 sidecar 单测的后端错误注入覆盖，真实写满时的工作目录与日志行为未验

(thewor1d877)： [C] checkpoint 稳态 100 step 循环未跑：连续保存、缓存淘汰与句柄增长只有单测，缺长跑装置

(thewor1d877)： [C] checkpoint 的 webhook 注入未在集群里过准入：e2e 用手写 Pod 复刻注入形状（`checkpoint/e2e/manifests/pod.yaml`），契约测试覆盖 patch 内容，但第二条 MutatingWebhookConfiguration 与 CA 注入没有一起跑

(thewor1d877)： [B] 分布式缓存组未实现：`docs/2` 层2 与 9.0 singleflight 一节里的迟到请求第 2 层（集群内节点已有该块则就近取）仍是独立回源，错峰到达的请求付整份回源字节；`Entry.Node` 的跨节点取值语义随此方案定稿。该族从未跑过（`results/` 无 gateway 分区，`scores/` 无 `+gw` 格子）

(thewor1d877)： [B] 8.4 第二节与第五节：生产部署前把 Loki 与 Prometheus 的存储从 emptyDir 换成持久卷或对象存储，并把 retention 6h 调大

(thewor1d877)： [C] 5.1 §11.5：数据集删除后 Agent 本地副本的清理入口归 Agent 层

(thewor1d877)： [D] RELEASE-CONSTRAINTS §4.3：CI 建完 draft Release 后追加 `gh release edit <tag> --draft=false` 可自动转正，现需人工点 Publish

(thewor1d877)： [B] probes §4.7 的装置缺口：`num_workers>0` 时 worker 的切分模型（`split_by_worker` 交错）与到达时序未建模，`probes/probe_d/judge.py:135` 只记了 `islice(src, worker, None, num_workers)` 的交错规则，消费顺序重建对多进程不成立

(thewor1d877)： [C] docs/3 §2.6 的重跑条件：「切换点纯 UFS 缺失」实验的 1 GiB 欠配设计被 /dev/shm 64 MB 限制（freeratio 压至 ≈57.6 MB，见 `docs/3-field-survey.md:143`），重跑前需放大 /dev/shm 或改用 O_DIRECT

## 一次性动作（不改代码不改文档）

(thewor1d877)： [C] 宿主上 19 个 dangling 镜像待清理（一次性 `docker rmi` 动作，不改代码）。原条目的另两项已不成立：现勘集群 `kind-traincache` 现存 0 个 fuse/juicefs/fluid Pod；节点标签 `traincache.io/s3-access=true` 是设计要求（`controller/deploy/agent/daemonset.yaml:5` 注明由人维护），不是残留

## 需要你裁决

(thewor1d877)： [C] docs/0 §1：dataset 到 bucket 的一对一映射（独立桶）未设计，`Store` 接口签名保持不变，落点在 Controller 层

(thewor1d877)： [B] docs/8.1 §3.1 的 component 是冻结枚举，没有 checkpointd；日志已经用 component=checkpointd，采集端的枚举校验可能丢字段。需要确认是否按“只增不改”加一行并写 ADR
