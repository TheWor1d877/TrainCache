(thewor1d877)： [A] compare 族的组态矩阵要重做：四个组态的取数路径结构不同，direct 每个 shard 一条连接、READ_WORKERS=2 封顶 2 条并发，juicefs 与 fluid 由 JuiceFS 客户端内部参数决定回源连接数且仓库未固定该数，traincache 由代码里 4 路 Range 决定，当前矩阵没有可比基数；要在不新开集群的前提下定出 TrainCache 与 Fluid/JuiceFS 同台比较的口径

(thewor1d877)： [B] JuiceFS 与 Fluid 的实际回源连接数未实测，跨组比较并行能力前需在集群上测 mount 进程到 MinIO 的连接数，命令 `nsenter --net=<minio-netns> ss -tn state established '( dport = :9000 )'`

(thewor1d877)： [C] e2e 族缺 checkpoint 续跑能力：`bench/scripts/load_train.py` 无 `--checkpoint-dir` 与 `--resume`，`bench/experiments/run.py` 无 e2e 分支，需先定存什么状态、在哪一步中断

(thewor1d877)： [C] `bench/experiments/report.py` 的版本字段按目录 mtime 推断，混跑新旧 COND 时会显示较旧的版本号，需改为读 COND 目录里的 version 文件

(thewor1d877)： [C] `bench/chaos/inject/node_loss.sh` 只过了 `bash -n`，未在集群实跑，需在 chaos 族正式跑之前验证一轮

(thewor1d877)： [D] `~/tcd/bench/results/cold-verify.txt` 与 `~/tcd/bench/results/gateway/gw-100-r1-on-cache/` 是旧 COND 命名时代的残留，出图与报告都不读，但 report.py 的版本字段会被它影响；清理需用户确认
