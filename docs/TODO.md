(thewor1d877)： [A] compare 族的组态矩阵要补齐：juicefs 与 fluid 只锁 1 GiB 一个缓存配额点，TrainCache 的 ρ 轴没有同配额的对照档；要在不新开集群的前提下定出补齐的规模

(thewor1d877)： [B] JuiceFS 与 Fluid 的实际回源连接数未实测，跨组比较并行能力前需在集群上测 mount 进程到 MinIO 的连接数，命令 `nsenter --net=<minio-netns> ss -tn state established '( dport = :9000 )'`

(thewor1d877)： [C] e2e 族缺 checkpoint 续跑能力：`bench/scripts/load_train.py` 无 `--checkpoint-dir` 与 `--resume`，`bench/experiments/run.py` 无 e2e 分支，需先定存什么状态、在哪一步中断

(thewor1d877)： [C] `bench/chaos/inject/node_loss.sh` 只过了 `bash -n`，未在集群实跑，需在 chaos 族正式跑之前验证一轮

(thewor1d877)： [D] `~/tcd/bench/results/cold-verify.txt` 与 `~/tcd/bench/results/gateway/gw-100-r1-on-cache/` 是旧 COND 命名时代的残留，出图与报告都不读；清理需用户确认

(thewor1d877)： [C] 块级上报通道 `ReportBlocks` 两端都空：Agent 侧返回 Unimplemented，Controller 现设计改用轮询 `GetPreheatStatus` 拿进度、明确不使用该通道；要先裁决是保留（补 Agent 服务端与 Controller 接收端）还是从 proto 里删掉

(thewor1d877)： [C] 启动窗口块数仍是订单里的固定 `prefetch_chunks`：命中率输入 `Status.HitRate` 已就位，调整动作未实现，缺命中率目标区间与步长的数字，需先用稳态轮 bench 定口径

(thewor1d877)： [C] CLI 拨号仍要手工给 `--endpoint`：`--node` 只作请求校验字段，节点到 Agent 地址的映射随 Controller 就绪，现在无法只凭节点名连上指定节点

(thewor1d877)： [C] 根 `Makefile` 的 `e2e-k8s` 与 `e2e-local` 转给 `bench/Makefile`，那边没有同名目标，`make -n e2e-k8s` 直接报「没有规则可制作目标」；要么在 bench 侧补目标，要么删掉这两个转入口并让脚本自己当入口（改构建｜根 Makefile:75、bench/Makefile）

(thewor1d877)： [A] compare 族 warm 组的预热量不等量：juicefs 与 fluid 预热整棵 4 GiB 数据树、1 GiB 配额下各留 1 GiB 数据，traincache 的 WarmupTask 显式写 k、ρ=4 时是 16 块即 128 MiB；要裁决是否把 traincache 的 k 提到与基线等量，否则 warm 组读数按现口径读（bench/compare/run_bench.py 的 warmup_k_for、docs/4.2 的对照族一节）

(thewor1d877)： [B] juicefs 与 fluid 的缓存配额要随 COND 变才有 8 GiB 档：`JFS_CACHE_MB` 与 `FLUID_CACHE_GI` 已是 env 可覆盖，缺的是 run_bench 按 COND 下发（bench/compare/juicefs/up.sh:33、bench/compare/fluid/up.sh:32）

(thewor1d877)： [B] 缓存目录峰值超声明配额 2.26 倍（历史那一跑 2.57 GiB 对 1 GiB），4.3 全体判据的 1.39 倍上界在代码里没有强制点；候选成因是预取写的块没进淘汰候选集（collector.ListForEviction 只含被 Touch 或 Open 过的指纹）或 30 s 淘汰节拍跟不上写入，要先定位再定判据

(thewor1d877)： [C] rangepar 族的 tc 形态要先在 kind 上验一次：tbf 之下挂 fq maxrate、netem 挂 fq 的 leaf class、`sch_fq` 模块能加载、单流限速可测（一条连接与四条连接的速率差要能测出来），形态没挂对就跑 24 跑是白跑

(thewor1d877)： [C] `summary.csv` 缺单块下载时间这一列，agent 侧的 `traincache_prefetch_task_duration_seconds` 没被采集进产物；并行下载族要判「4 路并行本身生效」就得先加这一列的采集与落盘（改采集｜bench/compare/run_bench.py 的指标快照与 summary 行、bench/experiments/run.py 的 SUMMARY_FIELDS）

(thewor1d877)： [C] 探点行（`summary.csv` 的 `exploratory` 列）在 4.x 文档里没有落点：汇总把这类行标成 exploratory 且不参与组间判定，文档重写后没有对应段落，要定是补一段还是删掉这个分支
