(thewor1d877)： [B] JuiceFS 与 Fluid 的实际回源连接数未实测，跨组比较并行能力前需在集群上测 mount 进程到 MinIO 的连接数，命令 `nsenter --net=<minio-netns> ss -tn state established '( dport = :9000 )'`

(thewor1d877)： [C] `bench/chaos/inject/node_loss.sh` 只过了 `bash -n`，未在集群实跑，需在 chaos 族正式跑之前验证一轮

(thewor1d877)： [D] `~/tcd/bench/results/cold-verify.txt` 与 `~/tcd/bench/results/gateway/gw-100-r1-on-cache/` 是旧 COND 命名时代的残留，出图与报告都不读；清理需用户确认

(thewor1d877)： [C] 块级上报通道 `ReportBlocks` 两端都空：Agent 侧返回 Unimplemented，Controller 现设计改用轮询 `GetPreheatStatus` 拿进度、明确不使用该通道；要先裁决是保留（补 Agent 服务端与 Controller 接收端）还是从 proto 里删掉

(thewor1d877)： [C] 启动窗口块数仍是订单里的固定 `prefetch_chunks`：命中率输入 `Status.HitRate` 已就位，调整动作未实现，缺命中率目标区间与步长的数字，需先用稳态轮 bench 定口径

(thewor1d877)： [C] CLI 拨号仍要手工给 `--endpoint`：`--node` 只作请求校验字段，节点到 Agent 地址的映射随 Controller 就绪，现在无法只凭节点名连上指定节点

(thewor1d877)： [C] 根 `Makefile` 的 `e2e-k8s` 与 `e2e-local` 转给 `bench/Makefile`，那边没有同名目标，`make -n e2e-k8s` 直接报「没有规则可制作目标」；要么在 bench 侧补目标，要么删掉这两个转入口并让脚本自己当入口（改构建｜根 Makefile:75、bench/Makefile）

(thewor1d877)： [C] rangepar 族的 tc 形态要先在 kind 上验一次：tbf 之下挂 fq maxrate、netem 挂 fq 的 leaf class、`sch_fq` 模块能加载、单流限速可测（一条连接与四条连接的速率差要能测出来），形态没挂对就跑 24 跑是白跑

(thewor1d877)： [C] 集群 ConfigMap `traincache-config` 缺 `fetchRangeParallel`、`gateNetworkBandwidthBytes`、`objstoreCompression` 三个键，仓库清单里三个键都有，集群上是更早的版本，发镜像与下发 ConfigMap 时要一并对齐

(thewor1d877)： [C] `bench/.sudo-pass` 不存在，`cold_reset.sh` 的 drop_caches 要 sudo 密码，跑分前要先放好这个文件

(thewor1d877)： [B] 缓存峰值的 `cache_peak_ratio` 列要在当前代码上重测：旧产物没有这一列，且旧产物的 agent 镜像早于本轮改动，峰值超配额的成因（淘汰候选集只含被消费过的块）没在当前代码上验证过
