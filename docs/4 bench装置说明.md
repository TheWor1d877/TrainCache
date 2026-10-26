# bench 装置说明

## 快速开始

TrainCache 出新版本、只跑自身（cold 冷缓存 + warm 预热后）、不重跑其他对照：

```bash
cd bench
make bench-quick TIER=S RATE=1000 REPS=3
```

前置条件：`make bench-up` 已成功执行一次（该命令同时部署控制面与 agentd，前置与幂等保证见 §4.1），且当前没有运行中的 train-job。产物落 `~/tcd/bench/results/quick/<run_id>/`，含 `summary.csv`、`run.json`、`lock.json`；存在上一次 quick 产物时另出 `vs_last.md`，对比两次的启动/全程 makespan、stall 占比与字节成本。

`TIER` 只能是 `S`：M/L 档的参跑组不含 `traincache-warm`，凑不齐冷热两阶段。`RATE` 取 {100, 300, 1000}，`REPS` 取 3（S 档正式值），`TAG=...` 给 run_id 加后缀。全链起停见 §5。

## 1. 装置是什么

一套在单机（32 核 / 30 GiB 内存 / kind 5 节点集群，其中 4 个 worker）上复刻"分布式训练存储"的实验装置，做三件事：

| 要做的事 | 怎么做 | 关键数字 |
|---|---|---|
| 造冷热差 | 用 tc 把源站出站路径限到指定带宽档；本地缓存命中就绕开这条路 | 100mbit≈12.5MB/s，读 8GiB 光传输 640 秒 |
| 造负载 | 四轨合成数据：A 小文件元数据、B 8GiB webdataset tar、C 倾斜访问清单、D 2GiB 连续 epoch | 种子固定，逐字节可复现 |
| 同台跑分 | TrainCache 与 direct/juicefs/fluid 四个对手（展开七个组态）吃同一份轨迹、同一个带宽档、同一份缓存容量 | §4.4、§9 |

带宽是实验自变量（9.1 D31），rate 取 {100, 300, 1000} mbit，有效带宽 = min(名义 rate, 源站实测上限)。burst 按 rate 推导：100→1250k、300→3750k、1000→12500k——固定 1600k 在 1000mbit 下只剩 13ms 突发窗口。netem 全档位只挂固定 `delay 20ms`，不加随机抖动：逐包重抽时延会把包序打乱，300mbit 档实测吞吐从 35.6 MB/s（95% 线速）跌到 0.6 MB/s 以下，`TCPSackRecovery` 占已发段约 15%，档位差异被装置掩盖。

```text
bench/
├── Makefile          # 主实验入口（四轨+限速+冷态+verify）
├── config.py         # 唯一参数源：python3 config.py --sh KEY 查询
├── scripts/          # 数据生成、负载器、tc、冷态、verify、trainpublish 二进制
├── k8s/              # kind 集群配置 + MinIO manifest
├── compare/          # 五组对照装置（run_bench.py + 每组目录）
└── experiments/      # 条件化实验层（COND 契约、runner、统计、出图）
```

数据在 `~/tcd/bench/data/`，结果与日志在 `~/tcd/bench/results/`、`~/tcd/bench/logs/`、`~/tcd/bench/compare/logs/`，repo 内没有同名目录（§9）。

概述：装置把"冷读多慢、换个策略快多少、跟对手比赢在哪"变成同一条命令能重复测量的数字。

## 2. 文件与接口

### 2.1 主实验层（bench/ 根 + scripts/）

| 文件 | 作用 | 接口 |
|------|------|------|
| `Makefile` | 全部入口 | `bench-up`/`bench-down`/`bench-down-full`/`ctrl-up`/`ctrl-down`/`bench-status`/`bench-doctor`/`bench-quick TIER= RATE= REPS= [TAG=]`/`kind-up`/`k8s-up`/`data-up`/`upload`/`tc-up RATE= DELAY=`/`tc-down`/`bench-cold`/`bench-verify QUICK=`/`bench-data`/`bench-tc`/`selftest`/`track-d-data\|up\|verify\|down`/`e2e-local`/`e2e-k8s`/`bench COND= REPS=`/`bench-list` |
| `config.py` | 唯一参数源 | `python3 config.py --sh <KEY>`；所有脚本都从这里取值 |
| `scripts/synth_track_{a,b,c,d}.py` | 四轨数据生成 | 参数全在 config；幂等，已存在即跳过 |
| `scripts/bench_track_d.py` | D 轨负载器 + verify 判据 | `--epochs --seed --workers --out-dir`；`--selftest` 自检判据函数 `compare_epoch_medians`；模块级依赖 numpy/torch/webdataset（§7 第 19 条） |
| `scripts/bench_track_c.py` | C 轨双 Job 倾斜负载器 | 热集循环 Job + 全量扫描 Job，`--stagger-s` 控制两 Job 交错启动 |
| `scripts/tc_apply.sh` / `tc_teardown.sh` | 挂/拆限速 | `<rate> <delay> [burst] [--target minio\|source-d] [--dry-run] [--self-test]`；不给 burst 时按 rate 推导（`config.burst_for_rate`），netem 只挂固定 delay |
| `scripts/cold_reset.sh` | 主实验冷态规程 | 无参；sync + drop_caches + 重启 MinIO + 重挂 tc。对照层不用它，用 `run_bench.py` 自带的 `cold_reset()`（§4.4） |
| `scripts/verify_cold.sh` | 装置可信性 | `[--quick]`；两轮冷读相对差 ≤10% 判过 |
| `scripts/track_d_{up,verify,down}.sh` | D 轨编排 | 零参数，`--dry-run`；up=容器+欠配+tc+10 epoch，verify=两轮×3 epoch 对账，down=拆除但保留数据 |
| `scripts/upload_bench.py` | B 轨灌 MinIO | 无参；只追加不覆盖（D41）：桶里已有的 `shard-*.tar` 跳过，`UPLOAD_FORCE=1` 才重传；每次运行重设公开读 policy |
| `scripts/trainpublish` | 发布 TrainCache 块快照 | Go 二进制，源码在 `core/cmd/trainpublish`，重编 `/usr/local/go/bin/go build`；关键旗标 `-chunk-size`，实验取 8388608（§3） |
| `scripts/ctrl_up.sh` / `ctrl_down.sh` | 控制面起停 | up：命名空间、节点标签、`traincache-objstore`/webhook TLS 两个 Secret、caBundle、`kubectl apply -k controller/deploy`、agentd 配置、四个工作负载就绪；down：删控制面资源，保留 namespace/集群/数据，支持 `--dry-run` |
| `k8s/kind-config.yaml`、`k8s/minio.yaml` | 集群与源站基座 | kind 5 节点配置；MinIO 带 30Gi PVC，所以 `kind delete` 后源数据仍在宿主机（§7 第 6 条） |

### 2.2 五组对照层（compare/）

| 文件 | 作用 | 接口 / 说明 |
|------|------|-------------|
| `compare/Makefile` | 对照入口 | `compare-up/down GROUP=<组态>`（七个组态名，或 `direct`/`juicefs`/`fluid`/`traincache` 别名）、`compare-all-up/down`、`trace`（生成统一轨迹）、`check`、`help` |
| `compare/run_bench.py` | 跑分编排器 | `--reps N --groups <组...> --tier S\|M\|L --rate <档> --out <csv>`；`--tier` 缺省取环境变量 `CAPACITY_RATIO`（再缺省 S）；`--rate` 只从命令行取，S 档缺省 100mbit，M/L 档必填。档位一次决定 `N_TARS`、`EPOCHS`、每点次数、本档参跑组；越档的组态被过滤，从显式请求里挑出来时告警。7 个组名映射 4 个目录组（direct、juicefs-{cold,warm}、fluid-{cold,warm}、traincache-{cold,warm}），组名按 startswith 分流。`--selftest` 离线自检，不连集群：验模板可编译、字节账精确、超时分支计入统计、档位接线 |
| `compare/direct/` | 组 1：HTTP 直读 MinIO，零缓存 | `up.sh` 建 namespace/secret/trace ConfigMap 并挂 tc；无预热 |
| `compare/juicefs/` | 组 2/3：JuiceFS 冷/热 | `up.sh`：Redis + mount Pod，缓存走 hostPath `/var/cache/jfs`；`JFS_WARMUP=1` 时预热在 up.sh 内完成，15 分钟硬超时、超时整轮作废；预热前轮询 `/proc/mounts` 确认 `/mnt/jfs` 已挂载（Pod Ready 只表示容器已启动）；`--cache-size` **单位是 MiB** |
| `compare/fluid/` | 组 4/5：Fluid 冷/热 | `up.sh`：Dataset + JuiceFSRuntime + PVC，导入 Job 成功才继续、失败立即退出；`FLUID_PREHEAT=1` 时 DataLoad 在 up.sh 内等完成；`FLUID_CACHE_GI` 控容量 |
| `compare/traincache/` | 组 6/7：TrainCache 冷/热 | `up.sh` 四步幂等，逐步见 §4.2；warm 组 `TRAINCACHE_WARMUP_K=128`、cold 组 0 |
| 统一测量体 | 每组只有一个 train-job Pod | measure 脚本是主容器；traincache 组走 webhook gate（Pending 等待计入 startup），其余组普通调度；`t_submit` = 创建该 Pod 之前的时刻，五组同尺（`run_bench.py create_measure_pod()`） |

### 2.3 七组态资源与规模对齐

七组态吃同一份源数据、同一个 tc 档位、同一块 1 GiB 缓存容量，差异只在缓存层实现。每组一个训练 Pod（`train-job-0`），测量体与调度体合一。

| 组态 | 训练 Pod 副本 | 缓存介质与路径 | 容量控制量 | 元数据 | 数据通路 | 预热 | 调度 | 资源 request / limit | 命中率口径 |
|---|---|---|---|---|---|---|---|---|---|
| direct | 1 | 无缓存层 | — | — | HTTP GET MinIO `shard-*.tar`，按 8 MiB 分段 `read(n)`，只累加总字节 | 无 | 普通调度 | 500m、1Gi / 2 CPU、4Gi | 每次读皆回源，`hit_rate≡0` |
| juicefs-cold | 1 | 节点 hostPath `/var/cache/jfs` | `--cache-size 1024`（单位 MiB，=1 GiB） | Redis 7 `redis.juicefs.svc:6379` | FUSE `/mnt/jfs`（measure Pod 内自 mount，privileged） | 无，空缓存启动 | 普通调度 | 500m、1Gi / 2 CPU、6Gi | 近似：cold≈全回源 + 回源字节 |
| juicefs-warm | 1 | 同 juicefs-cold | 同 juicefs-cold | 同 juicefs-cold | 同 juicefs-cold | up.sh 内 `juicefs warmup /mnt/jfs/` 预热整档数据集，单独计时，15 分钟超时 | 普通调度 | 500m、1Gi / 2 CPU、6Gi | 近似：warm≈全命中 |
| fluid-cold | 1 | tieredstore HDD，节点 hostPath `/tmp/juicefs-cache` | `quota: 1Gi`（`FLUID_CACHE_GI=1`） | Redis 7 `redis.fluid.svc:6379`（独立实例） | PVC `jfs-demo` 挂 `/data` | `FLUID_PREHEAT=0`，不建 DataLoad | nodeAffinity 绑缓存 worker；JuiceFSRuntime `replicas: 1` | 500m、1Gi / 2 CPU、4Gi | 近似：cold≈全回源 + 回源字节 |
| fluid-warm | 1 | 同 fluid-cold | 同 fluid-cold | 同 fluid-cold | 同 fluid-cold | DataLoad `jfs-load`（target=`/`）预热整档数据集，15 分钟超时 | 同 fluid-cold | 500m、1Gi / 2 CPU、4Gi | 近似：warm≈全命中 |
| traincache-cold | 1（WarmupTask `podCount: 1`） | 节点 hostPath `/var/cache/traincache/blocks`（measure 只读挂载） | env `TRAINCACHE_CACHE_CAPACITY_BYTES=1073741824`（1 GiB） | DataSet CRD + MinIO 快照 `dataset/<name>/CURRENT` | gRPC 取块 → 本地块读；miss 等预取，超时 HTTP 回源 | k=0，不建 WarmupTask，gate 立即放行 | 普通调度 | 500m、1Gi / 2 CPU、4Gi | `traincache_fetch_bytes_total` run 窗口增量对账，回源字节计 miss |
| traincache-warm | 1（WarmupTask `podCount: 1`） | 同 traincache-cold | 同 traincache-cold | 同 traincache-cold | 同 traincache-cold | WarmupTask `compare-task` k=128（8 MiB × 128 = 1 GiB），`preheatFanout`=4（s3-access 节点数），15 分钟超时 | `traincache-scheduler` + webhook schedulingGate | 500m、1Gi / 2 CPU、4Gi | 同 traincache-cold |

命名空间：direct→`direct`；juicefs-cold/warm→`juicefs`；fluid-cold/warm→`fluid`（控制面 `fluid-system`）；traincache-cold/warm→`traincache`（控制面与负载同 ns）。两个 Redis 7 是彼此独立的实例。

限额与副本以 `run_bench.py` 的 Pod 模板和 `up.sh` 为准：CPU/内存 request 500m/1Gi、limit 2 CPU/4Gi；juicefs 组在 Pod 内跑 FUSE mount，内存 limit 放宽到 6Gi。

档位决定数据规模与 epoch 数：S 档 8 片 × 256 MiB = 2 GiB、3 epoch；M 档 32 × 256 MiB = 8 GiB、1 epoch；L 档 128 × 256 MiB = 32 GiB、1 epoch。S 档 compare-d 的 COND 写 `dataset.shards=8`、`shard_size_bytes=268435456`，不是 16 片 × 128 MiB。

### 2.4 条件化实验层（experiments/）

| 文件 | 作用 | 接口 / 说明 |
|------|------|-------------|
| `cond.py` + `docs/9.2 实验协议.md` §C | COND 契约 v1：一个 JSON = 一个实验点 | 校验字段全集并产出环境变量。`network.rate_mbit` ∈ {100,300,1000}；burst 按 rate 推导，值不符直接拒收；组名不在本档参跑集合（`CAPACITY_TIER_GROUPS`）内即拒收；`capacity_ratio`、`chunk_size_bytes`、`warmup_seconds` 都在契约里；netem 形态不在 COND 字段里（只有固定 delay，没有抖动开关）；占位符 digest/sha 一律拒收；`environment.images` 实际 9 键：agent、controller、scheduler、webhook、training、minio、juicefs、fluid、fluid-csi |
| `conds/*.json` | 正式 COND 集，共 34 个 | `compare-d-<组态>-<rate>` 17 个（七组态 × {100,1000}，加 direct/fluid-cold/traincache-cold 的 @300 档）、`compare-b-m-<组>-1000` 3 个、`compare-b-l-<组>-1000` 2 个、`policy-{heat,lru,lfu}` 3 个、`pareto2-lookahead{8,16,32}-{heat,lru,lfu}` 9 个 |
| `run.py` | 主实验 runner | 冷态 → 搭负载 → 采集 → 归档；失败保留现场并标 `void` |
| `run_policy.py` | 淘汰 × 预取实验执行体 | 注入 `TRAINCACHE_EVICT_POLICY`、`TRAINCACHE_PREFETCH_LOOKAHEAD` |
| `collect.py` | 离线汇总 | makespan 两边界、三笔字节成本、分阶段命中率 → `metrics.csv`/`collect.json`；`--selftest` |
| `stats.py` | 统计 | 平均数 + bootstrap 95% CI（样本有放回重抽 10000 次，取 2.5%/97.5% 分位） |
| `lock.py` | 环境清单 | 镜像 digest、kubectl 版本、tc 档位（rate/delay/burst）、`chunk_size_bytes`、`dataset_name`、种子，每个 run 目录落一份 `lock.json` |
| `plot.py` | 出图 | 五张旗舰图统一绘制，负收益区域照画 |
| `docs/9.2 实验协议.md` §0~§8 | 预注册协议 | 冻结前定稿假设、档位与判据；冻结后只允许往 §7 追加偏离说明 |

概述：三层各管一件事——主实验层造冷热差，对照层做同台跑分，实验层保证每个点条件可复现。

## 3. 参数总表（哪个旋钮影响什么）

| 旋钮 | 现值 | 写在哪 | 影响 |
|------|------|--------|------|
| `--rate / TC_DELAY` | 默认 100mbit / 20ms；正式跑分按 COND 的 `network.rate_mbit` 取 {100, 300, 1000} | 对照层由 `run_bench.py --rate` 取速率、`TC_DELAY` 取延迟，再以 `TC_RATE`/`TC_DELAY` 环境变量下发给 up.sh；`make bench-tc RATE= DELAY=` 走同一套 `tc_apply` | 回源管道粗细。有效带宽 = min(名义 rate, 源站实测上限)，上限见 §10 |
| `TC_BURST` | burst 按 rate 推导（§1）；netem 只挂 `delay 20ms`，没有抖动参数 | `config.burst_for_rate()`；`tc_apply.sh` 自己推导 burst；四个 `compare/*/up.sh` 只传 `rate` 与 `delay` | 突发窗口必须跟着 rate 走；抖动会把包序打乱、量不到档位差异，装置侧不提供该参数 |
| `TRACK_B_N_TARS` | 32（~264MB/个，≈8GiB） | `config.py` | 源数据生成规模。桶 `bench-b` 现有 128 个对象，够 L 档 32GiB 取用；追加不覆盖（9.1 D41） |
| `CAPACITY_RATIO` / `--tier` | S（缺省） | `run_bench.py` 环境变量 / 旗标；档位参数单点定义在 `experiments/cond.py` 的 `CAPACITY_TIER_*` | 一键决定 `N_TARS`、`EPOCHS`、每点名义次数（S=3、M=3、L=1）与本档参跑组（S 七组态、M 三组、L 两组，9.1 D30）。非法值退回 S 并打印告警 |
| `N_TARS`（对照） | S=8（≈2GiB）、M=32（8GiB）、L=128（32GiB） | `run_bench.py` 环境变量（缺省按 `CAPACITY_RATIO` 取） | 每组读的分片数 |
| `chunk-size`（实验块） | **8MiB**（生产默认 64MiB） | `trainpublish -chunk-size 8388608`，随产物归档 | 块是缓存与预取的最小单位；1GiB 缓存 = 128 块（9.1 D20） |
| `TRAINCACHE_CACHE_CAPACITY_BYTES` | 1GiB（`1073741824`） | `ctrl_up.sh` 的 `ensure_agent_config`（bench-up 与 traincache `up.sh` 共用）注入 agentd；读取在 `agent/svc/config.go` | TrainCache 缓存字节上限，淘汰按它触发 |
| `JFS_CACHE_MB` | 1024 | juicefs `up.sh` | JuiceFS 缓存上限；`--cache-size` 直接收 MiB 数值，**别再乘 1024** |
| `FLUID_CACHE_GI` | 1 | fluid `up.sh` | Fluid 缓存配额（Gi） |
| `TRAINCACHE_WARMUP_K` | warm=128 / cold=0，由「缓存容量 ÷ 块大小」在代码里自动推 | `run_bench.py` 环境变量 → `traincache/up.sh` → WarmupTask `podDetails[0].k` | 预热块数。**预热量 = k × chunk-size × fanout**：8MiB × 128 = 1GiB 前缀，只覆盖数据集开头 1 GiB，不是整档（9.1 D20/D19）；JuiceFS/Fluid 的 warm 组预热整档数据集，两者不可直接比字节数。换块大小按 `字节量 ÷ CHUNK_BYTES` 重算 |
| `TRAINCACHE_PREFETCH_LOOKAHEAD` | 16（默认） | `agent/svc/assemble.go`；`experiments/run_policy.py` 注入 | Agent 持续预取窗口；对照三档统一 16（9.1 D21） |
| `TRAINCACHE_PREFETCH_K` | 16（默认） | compare train-job 侧 ordersrc 消费窗口 | 训练侧消费深度，与 lookahead 是两个旋钮；对照三档统一 16 |
| `TRAINCACHE_EVICT_POLICY` | heat（默认）/ lru / lfu | env 注入，非法名拒绝启动 | 淘汰与预取实验用；对照实验一律 heat |
| `EPOCHS / STEPS_PER_EPOCH / BATCH / IMAGES_PER_TAR` | 3 / 400 / 32 / 1600 | `run_bench.py` 环境变量（与 `config.py` 的 D 轨口径对齐）；`EPOCHS` 缺省按档位取 {S:3, M:1, L:1}，显式给出时优先 | 对照训练形态：一个 epoch = 400 步 × 200ms 计算 = 80 秒，按轨迹过一遍全量。每步读量 r 由这四个数决定 |
| `COMPUTE_MS / READ_WORKERS / READ_QUEUE_DEPTH / READ_TIMEOUT_S` | 200 / 2 / 2 / 300 | `run_bench.py` 环境变量 → 下发进 measure 模板，不写死 | 每步计算量（决定 stall_ratio 有没有判别力，10.1 任务 6.15）与异步预取队列深度；队列只传字节数，常驻内存 O(1) |
| `TRAINCACHE_DATASET_WHITELIST / OBJSTORE_BUCKET` | compare-data-8m / bench-b | `ctrl_up.sh` 的 `ensure_agent_config` 运行时 `kubectl set env` | agentd 服务哪些数据集/桶。换 `DATASET_NAME`（如冒烟前缀）时白名单必须同步，否则 preheat 全失败（§7 第 13 条） |
| seed | 20261015（D 轨）等 | `config.py` / COND | 数据与 shuffle 可复现的根 |

概述：调任何旋钮之前先在这张表里找到它；表外还有参数，说明装置跑偏了。

## 4. 操作规程

### 4.1 从零拉起（新机器或集群重建后）

`make bench-up` 一条命令拉起全链，各步幂等：

```bash
cd bench
make bench-up
```

它依次执行 `kind-up`（kind 5 节点集群，镜像需已导入，见 §7 第 1 条）、`k8s-up`（MinIO 带 PVC）、`data-up`（四轨数据，含 `track-d-data`）、`upload`（B 轨灌 MinIO）、`ctrl-up`（控制面与 agentd）、`tc-up`（默认 100mbit/20ms，需要 sudo，走 `.sudo-pass`）、`bench-verify`（两轮冷读相对差 ≤10%）。首次约 1 小时（估算值，主要花在造数），已存在的步骤自动跳过。

`ctrl-up` 的前置与保证：

- 控制面镜像 manager/webhook/scheduler/agentd 统一用 `dev` 标签，镜像名为 `ghcr.io/thewor1d877/traincache/<组件>:dev`，必须已在 4 个 worker 的 containerd 中，`kind-up` 不预载。缺失时 `ctrl_up.sh` 直接失败并打印 `docker pull` 与 `kind load docker-image ... --name traincache`，不在脚本内重试拉取。
- webhook TLS Secret 缺失时自签生成，并把 CA 注入 MutatingWebhookConfiguration `traincache-pod-warmup` 的 caBundle。
- 凭证 Secret `traincache-objstore` 用 `config.py` 的 MinIO 凭证创建，已存在则跳过。
- worker 节点缺 `traincache.io/s3-access=true` 时自动补标签。
- 幂等：`controller/deploy` 的内容哈希记在 namespace 注解 `traincache.io/deploy-hash`；哈希未变且四个工作负载都在时不执行 apply。连续两次 `make ctrl-up` 不触发滚动重启，Pod UID 不变。agentd 的 WHITELIST/BUCKET/CACHE_CAPACITY_BYTES 只在现值与期望不符时 `kubectl set env`。

跑分进行中不要执行 `bench-up` 或 `ctrl-up`：首次部署或清单哈希变化时 apply 会触发一次 agentd 滚动重启，中断正在运行的任务。

`make bench-status` 只读打印集群、节点、MinIO、tc qdisc、数据目录与最近产物。

### 4.2 只起 TrainCache（不搭 juicefs/fluid）

用途：装置自检、淘汰与预取实验、traincache 组冒烟。不需要 Redis、不需要 helm、不需要 `make compare-all-up`——四组对照装置（direct/juicefs/fluid/traincache）里只起 `traincache` 一个。

```bash
cd bench
make kind-up k8s-up data-up upload           # 已就绪会自动跳过；tc 不用手工挂，up.sh 自己挂
make -C compare compare-up GROUP=traincache  # 只起 TrainCache 组
```

`traincache/up.sh` 自带四步，顺序就是脚本里的顺序，全部幂等：

| 步 | 干什么 | 对应代码 | 出问题先查 |
|---|---|---|---|
| 1 | 给所有 worker 节点打 `traincache.io/s3-access=true`（本机 4 个；节点数动态枚举，不写死） | `ctrl_up.sh` 的 `label_s3_nodes` | NoAccessibleNodes（§7 第 12 条） |
| 2 | 发布快照：`trainpublish -chunk-size 8388608` 把 `~/tcd/bench/data/track_b/tars/shard-*.tar` 切成 8MiB 块，上传成 `bench-b/dataset/compare-data-8m/`；`CURRENT` 已存在就跳过 | `publish_snapshot` | DataSet SnapshotNotFound（§7 第 11 条） |
| 3 | 控制面：与 `bench-up` 共用 `ctrl_up.sh`；清单哈希变化或四个工作负载缺失才 `kubectl apply -k controller/deploy`，随后等 rollout | `ctrl_up.sh` 的 `ensure_control_plane` | 镜像不在 4 节点 containerd 里 → ImagePullBackOff（§7 第 1 条） |
| 4 | 注入 agentd 白名单与缓存上限（`TRAINCACHE_DATASET_WHITELIST=compare-data-8m`、`TRAINCACHE_CACHE_CAPACITY_BYTES=1GiB`），建 DataSet/WarmupTask，最后挂 tc | `ctrl_up.sh` 的 `ensure_agent_config` | preheat 全失败（§7 第 13 条） |

单跑一轮冒烟（cold 档 k=0，不等预热）：

```bash
cd bench
CAPACITY_RATIO=S DATASET_NAME=compare-data-8m \
python3 compare/run_bench.py --reps 1 --groups traincache-cold \
        --out compare/smoke-traincache.csv
```

`--out` 的相对路径按 `RESULTS_DIR` 展开，上面的 CSV 落在 `~/tcd/bench/results/compare/`。

`--reps 1` 低于 S 档规定的 3 次，会打印一行警告；冒烟无所谓，正式跑分按 §4.4 的次数表。

只有 TrainCache 时能报的：makespan（启动/全程）、stall 序列、命中率对账、三笔字节成本、预热时长——绝对值都有。缺基线就不给跨对手的相对结论。冷读口径靠 `cold_reset()` 保证：四条清缓存命令都走 `sh -c`，glob 在节点容器内展开，删完再用 `ls -A` 逐个目录判空，非空或无法确认即终止本轮；`run_bench.py --selftest` 断言命令形态。

### 4.3 主实验（D 轨与淘汰/预取）

```bash
make -C bench track-d-up                       # D 轨 10 epoch 连跑（宿主机直跑，不占集群）
make -C bench bench COND=policy-lru REPS=3     # 跑一个实验点（N 次含冷态循环）
make -C bench bench-list                       # 有哪些 COND

.venv/bin/pip install -r bench/requirements.txt   # 分析层依赖装进项目根 .venv
.venv/bin/python bench/experiments/collect.py     # 离线汇总出 metrics.csv
.venv/bin/python bench/experiments/plot.py        # 出图
```

进度查看：`ls ~/tcd/bench/results/<run目录>` 逐轮增多；每步打点在 per-step jsonl，`tail -f` 即可。

### 4.4 五组对照跑分（七个组态）

M7 产物是五格，一格 = 容量档位 × 带宽档：

| 格 | tier | rate | 组态 | 每点次数 | 预计机时 |
|---|---|---|---|---|---|
| S-100 | S | 100mbit | 七组态 | 3 | 3.2 h |
| S-300 | S | 300mbit | direct / fluid-cold / traincache-cold | 3 | 0.7 h |
| S-1000 | S | 1000mbit | 七组态 | 3 | 1.6 h |
| M-1000 | M | 1000mbit | direct / fluid-cold / traincache-cold | 3 | 0.7 h |
| L-1000 | L | 1000mbit | traincache-cold / fluid-cold | 1 | 0.2 h |

合计 62 run，约 6.3 h（9.1 §12.3）。

一键跑五格，串行执行：

```bash
cd bench
make bench-m7           # 串行五格，约 6.3 h
make bench-m7 FORCE=1   # 忽略已完成格，全量重跑
```

`flock -n /tmp/traincache-m7.lock` 保证同一时刻只有一个 M7 实例，五格不并行。某格 `summary.csv` 已存在且非 void 数据行数达到「组数 × 次数」时跳过（`void=True` 的行一律不计入行数，含 `agent_degraded`、`up_failed`、`read_queue_timeout`、`read_error`、`suspect_page_cache`）；中断后重跑同一条命令，从第一个未完成的格续跑。任一格失败立即停止，已完成格产物保留。每格开始打印格名、组数与预计时长，结束打印该格 summary 路径。

单格复跑时显式给出 `--rate`、`--groups` 与独立的 `--out`；`--out` 的相对路径按 `RESULTS_DIR` 展开：

```bash
cd bench/compare
CAPACITY_RATIO=S python3 run_bench.py --tier S --rate 100mbit --reps 3 \
  --groups direct juicefs-cold juicefs-warm fluid-cold fluid-warm traincache-cold traincache-warm \
  --out m7/S-100/summary.csv
CAPACITY_RATIO=S python3 run_bench.py --tier S --rate 300mbit --reps 3 \
  --groups direct fluid-cold traincache-cold --out m7/S-300/summary.csv
CAPACITY_RATIO=S python3 run_bench.py --tier S --rate 1000mbit --reps 3 \
  --groups direct juicefs-cold juicefs-warm fluid-cold fluid-warm traincache-cold traincache-warm \
  --out m7/S-1000/summary.csv
CAPACITY_RATIO=M python3 run_bench.py --tier M --rate 1000mbit --reps 3 \
  --groups direct fluid-cold traincache-cold --out m7/M-1000/summary.csv
CAPACITY_RATIO=L python3 run_bench.py --tier L --rate 1000mbit --reps 1 \
  --groups traincache-cold fluid-cold --out m7/L-1000/summary.csv

make compare-up GROUP=juicefs-warm     # 只起某组环境（预热在 up 内完成并等待）
make compare-down GROUP=juicefs-warm   # 拆该组
```

M/L 档不传 `--rate` 时 runner 直接退出，因为这两档只有 @1000；S@300 不传 `--groups` 时也退出，因为该格只允许三组。runner 不按 COND 反查组合，格子由命令行给全。

每格产物落 `~/tcd/bench/results/m7/<格名>/`，含 `summary.csv` 与 `lock.json`。`summary.csv` 的 `rate` 列记本次生效的带宽档（如 `1000mbit`）；`warmup_basis` 列记预热字节口径（traincache-warm 为 `prefix_only(k*chunk*fanout)`，juicefs-warm/fluid-warm 为 `full_dataset(n_tars*shard_size)`，其余组留空）；`read_mode` 列只在 direct 组填 `http_get_read_segmented(8388608,total_bytes)`，表示按 8 MiB 分段读、只累加总字节。`lock.json` 的 `images_actual` 记本次实际使用的 training/juicefs 镜像引用（tag 级，docker 侧能取到 RepoDigest 时一并记入）；`docker_images` 保留 COND 冻结的 containerd manifest digest，两者不一致只记录、不阻止跑分；`direct_read_mode` 记 direct 组的读模式（transport/segment_bytes/counting）。

`GROUP=` 接受七个组态名，冷热只切环境变量、共用同一目录。

参跑组由档位自动过滤：S 档七组态，M 档 direct/fluid-cold/traincache-cold，L 档 traincache-cold/fluid-cold。每点次数取 `CAPACITY_TIER_REPS`（S=3、M=3、L=1，9.1 D32）；正式实验各格统一 3 次，不做符号翻转补测。L 档产物标 `exploratory`，不报 CI、不进正式表。

rep 之间的复用规则：cold 组进门一次 `cold_reset` + `group_up`，rep 之间只清缓存（`rep_cold_reset`），收尾一次 `group_down`；warm 组每个 rep 重建，因为 WarmupTask 停在 Released，复用控制面会让「标 warm 实为冷读」。

跑中要盯的东西，每条都可核验：

```bash
kubectl get pods -A -l app=train-job -o wide     # 当前哪个组在测、落在哪个节点
kubectl -n <组ns> get pod train-job-0 -o jsonpath='{.status.phase}'   # traincache 组先看 Pending（gate 卡着）→Running
kubectl logs -n <组ns> train-job-0 -f            # 每步的 wait_s/step_s 实时打点
kubectl -n traincache get warmuptask             # 预热进度
kubectl -n traincache get pods -o wide | grep agentd   # 4 worker 各一且 Ready
tc -s qdisc show dev eth0                        # 确认档位真的是这一格要的值（含 burst）
```

CSV 每组跑完重写一次，组内中断只丢当组；日志在 `~/tcd/bench/compare/logs/<组>.log`。

对照层的冷态和主实验不同：`run_bench.py` 自带的 `cold_reset()` 只删训练 Pod（`app=train-job`；agentd、Redis、CSI 等基础设施保留），再在四个 kind 节点上清 `/var/cache/jfs`、`/tmp/juicefs-cache`、`/var/cache/traincache/blocks`、`/var/cache/traincache/quarantine`，删完判空，**不重启 MinIO**，所以 tc 不会丢；hostPath 缓存不随 Pod 删除消失，必须显式清，目录不存在的组视为已空。traincache 组每个 rep 在删掉上一个测量 Pod 之后、创建新测量 Pod 之前复位 agentd：把 `traincache-agentd` 缩到 0（DaemonSet 没有 scale 子资源，代码改用不匹配的 `nodeSelector` 挂起）、等 Pod 全消失，逐节点清空 `/run/traincache/orders`、`/var/cache/traincache/blocks`、`/var/cache/traincache/quarantine` 并判空，再缩回 1，等 4 个 Pod Ready、`traincache_agent_degraded=0`、fetch 计数两次采样不增长（确认无 reader），且每个 Pod 内 `/run/traincache/agent.sock` 存在（滚动重启时旧 Pod 的 cleanup 会 unlink 共享 socket）；warm 组的复位排在 `up.sh` 预热之前，预热写入的块不被清掉，任一步失败该 rep 记 void/up_failed 并停止该组。复位避免控制面重启后 agentd 回放 `/run/traincache/orders` 下的历史 ring，也避免 `cold_reset()` 清块缓存时 agentd 仍在写、残留 `tmp-*` 让它退出。测量期间每 5s 采一次所有节点的 `traincache_agent_degraded`，测量 Pod 结束即停采样；出现 1 记 void(reason=`agent_degraded`)，产物新增 `agent_degraded_max` 列（0/1，其他组留空）。一旦手工重启 MinIO，必须重挂 tc，否则整轮跑在无限速链路上（§7 第 3 条）。

概述：M7 用 `make bench-m7` 串行跑五格，单格复跑用本节的显式命令，进度看 kubectl 实况。

## 5. 一键入口

`bench-*` 目标在 `bench/` 下执行；`compare-*` 目标在 `bench/compare/` 下执行。

| 动作 | 命令 | 说明 |
|------|------|------|
| 全链拉起 | `make bench-up` | kind → k8s → 四轨数据 → 上传 → 控制面与 agentd → tc → 冷读验证，全幂等 |
| 只起/校验控制面 | `make ctrl-up` | 同上表 §4.1 的 `ctrl-up`；清单未变时不 apply，不滚动重启 |
| 只停控制面 | `make ctrl-down` | 只删控制面与 Secret，保留集群与数据；`bench-down-full` 先拆 tc 再走这一步 |
| 全拆 | `make bench-down-full` | 先拆 tc，再删控制面 Deployment/DS/CRD/Secret；kind 集群、namespace 与 `~/tcd` 数据保留 |
| 新版本自跑 | `make bench-quick TIER=S RATE=1000 REPS=3 [TAG=...]` | 只跑 `traincache-cold` + `traincache-warm`，产物见本文 §9.1 |
| 查看状态 | `make bench-status` | 只读：集群、节点、MinIO、tc qdisc、数据目录、最近产物 |
| 体检 | `make bench-doctor` | 只读逐项 PASS/FAIL/WARN；有 FAIL 时退出 1 |
| 冷态 | `make bench-cold` | drop_caches + 重启 MinIO + 重挂 tc；对照层用 `run_bench.py` 的 `cold_reset()` |
| 冷读验证 | `make bench-verify` | 两轮冷读相对差 ≤10%；快检加 `QUICK=--quick`，`QUICK` 原样传给脚本 |
| 拆限速 | `make bench-down` | 只拆 tc，集群与数据保留；只拆限速不打印收尾提示用 `make tc-down` |
| 起对照组 | `make compare-up GROUP=<组态>` | 七个组态名任选，冷热只切环境变量；全起用 `make compare-all-up` |
| 拆对照组 | `make compare-down GROUP=<组态>` | 不带 `GROUP` 等价 `compare-all-down`，拆尽四组 |

单点实验与出图见 §4.3。

## 6. 每组冒烟判据

冒烟 = 起该组 → 低负载一轮 → 判定 → 拆该组清缓存。低负载口径是 `N_TARS=2 EPOCHS=1 STEPS_PER_EPOCH=4`。任何装置改动后，先跑受影响组的冒烟再谈正式实验。

| 组 | 通过判据 |
|----|----------|
| direct | train-job Running 到完成；四指标 CSV 行齐全；stall>0（限速生效） |
| juicefs-cold | 同上 + 缓存目录 `--cache-size` 生效（跑前四条缓存路径均为空，`cold_reset()` 判空通过） |
| juicefs-warm | up.sh 内 warmup 完成才创建 train-job；`du` 缓存非 0；startup 不含 warmup 时间 |
| fluid-cold / fluid-warm | PVC 挂载成功；warm 组 DataLoad 完成事件晚于 up、早于 t_submit |
| traincache-cold | k=0：WarmupTask 不创建，gate 立即放行，train-job 正常跑完 |
| traincache-warm | k=128（8MiB 块）：预热完成 → gate 放行；agentd 命中指标非零；落点节点的 blocks 目录有数据 |

全部组共判：`kubectl get events` 无 ImagePullBackOff/CrashLoop；产物含 run ID 并能关联 COND。

概述：冒烟是装置的体温计——低负载、快出结果、只回答"这组还能不能用"。

## 7. 故障速查表（现象 → 根因 → 处置；都是真实踩过的，勿回退）

| # | 现象 | 根因 | 处置 |
|---|------|------|------|
| 1 | 训练 Pod ImagePullBackOff | 离线环境拉不到 docker hub；k8s/containerd 不读 daemon.json 的镜像加速 | 宿主机 `docker pull` 后 `kind load docker-image` 导入；训练镜像用 `python:3.12-alpine`（slim 拉不到） |
| 2 | 宿主机访问 MinIO NodePort 不通 | kind 不映射 nodePort 到宿主机 | 走节点 InternalIP（`http://172.19.0.x:30090`），upload 脚本已自动探测 |
| 3 | 冷读结果忽快忽慢 | MinIO/源站重启后 tc qdisc 与匿名读 policy 都丢 | 冷态规程自动重挂；不复挂 = 跑在无限速链路上，该轮作废 |
| 4 | crictl 找不到 MinIO netns | 节点残留 NotReady 僵尸 sandbox（worker2 实测有一条） | `crictl pods --state Ready --name <pod> -q` 过滤，`tc_apply` 已改 |
| 5 | VM 重启后 mount "not formatted"、format 报 storage is not empty | 两组的 redis 都无持久化：重启把 JuiceFS 元数据带走，MinIO 里的卷对象前缀还在，CE format 拒绝 | juicefs 组 up.sh 自愈：format 失败且报 not empty → `clean_orphan_volume` 只清 `$JFS_VOL/` 前缀，再重 format。不用 `--force`：块键按 inode+chunk 命名，新元数据会读到上一轮的陈旧块。fluid 组仍手工 `kubectl delete ns fluid` 后重 up（down.sh 会清派生前缀）；kubeadm 证书过期先 `kubeadm certs renew all` |
| 6 | 全 API 401、Service 不通 | 宿主机时钟回拨，SA token 的 nbf 在未来 | 重启 Deployment 重签；坏透就 `cd bench && kind delete cluster && make bench-up`（数据在宿主机，重传 8 秒） |
| 7 | fluid-cold 等组跑出裸 Pod / NameError | 组名精确匹配漏改（7 组名 vs 4 目录） | `run_bench` 已改 startswith 分流；改组名时全局搜 `== "fluid"` 这类精确比较 |
| 8 | juicefs 缓存 1GiB 限制从不生效 | `--cache-size` 单位是 **MiB**，装置曾注释成 KiB、配了 1048576（=1TiB） | 配 1024；见 §3；历史 juicefs 结果因此作废过一轮 |
| 9 | warmup 了但测量组读不到缓存 | `warm.sh` 暖的是 jfs-mount 的 emptyDir，测量 Pod 用的是另一个 emptyDir | 缓存改 hostPath `/var/cache/jfs`；预热挪进 up.sh（9.1 D23/D24） |
| 10 | measure 的 startup 712 秒异常 | Pod 内 warmup 耗时被计进 t_submit→首步 | 预热外移到 group_up，并删掉 `TRAINCACHE_JOB_CREATED_AT="0"` 特判 |
| 11 | DataSet SnapshotNotFound | 快照没发布过（`CURRENT` 缺失） | up.sh 的 `publish_snapshot` 调 trainpublish；8MiB 块发布到 `compare-data-8m`（9.1 D41） |
| 12 | NoAccessibleNodes | worker 缺 s3-access 标签 | `ctrl_up.sh` 的 `label_s3_nodes` 自动打；重建集群后要重跑 |
| 13 | preheat 全失败 | agentd 的 WHITELIST/BUCKET 与实际数据集/桶不匹配 | `ctrl_up.sh` 的 `ensure_agent_config` 运行时 `kubectl set env`；换快照名要同步改（§3） |
| 14 | shard 数量算错、读一半没数据 | 按 128MB 假设算 count，实际约 264MB | 从对象 stat 读实际大小 |
| 15 | pkill 把 shell 自己杀了（exit 143）；pgrep 判「有没有在跑」永远返回真 | `pkill -f`/`pgrep -f` 的模式串出现在自己那条命令行里，等于自己匹配自己 | 找人再杀用 `pgrep -f 'run_bench\.py'`；判在跑用 `ps -eo cmd \| grep -E '^(bash\|/bin/bash) .*up\.sh'`；要改正在被调用的脚本，先写同目录临时文件再 `mv` 覆盖（同目录 rename 是原子的，老进程握着旧 inode 继续读旧内容，下一次调用才读到新内容） |
| 16 | juicefs-warm 与 juicefs-cold 读数几乎相同 | `juicefs warmup` 暖的是 jfs-mount 所在节点的 hostPath `/var/cache/jfs`；测量 Pod 没有共置约束，落到别台节点就是空缓存起跑，warm 退化成 cold | up.sh 回传 `[MOUNT_NODE]`，`run_bench` 给 juicefs 两组的测量 Pod 加 `kubernetes.io/hostname` nodeSelector（cold 同节点，warm/cold 差值才是同一台节点的比较）；回传缺失直接判 `up_failed` |
| 17 | `warmup_seconds` 同组内差出量级（2s vs 177s） | up.sh 把 `tc_apply` 排在预热之后：节点重启后 qdisc 已丢，这一档第一次预热走满速，之后各 rep 才被自己挂上的 tc 限住 | juicefs/fluid 的 `tc_apply` 已挪到预热之前——预热与测量走同一条限速链路，`warmup_seconds` 才是本档口径的搬运成本 |
| 18 | drop_caches / tc 需要 root 卡住 | sudo 交互不可用 | `bench/.sudo-pass`（gitignore、600 权限）；文档不展开内容 |
| 19 | python 依赖找不到（boto3/numpy 等） | 直接调系统 `python3` 时没有加载项目根 `.venv` 的 site-packages | 统一用 Makefile 入口（`PY`/`PYTHONPATH` 已在其中设定）；分析层依赖按 `bench/requirements.txt` 装入项目根 `.venv`，上传所需的 boto3 由 `make upload` 安装 |
| 20 | `docker update --memory 0` 没恢复 | 本机 docker 把 0 当"不变更" | 恢复用 128GiB 等效大值，记录在 env json 的 `restore_caveat` |
| 21 | D 轨（`make -C bench track-d-up`）跑不起来 | `bench_track_d.py` 要 numpy/torch/webdataset；`bench/requirements.txt` 只锁 numpy | 装齐三项：numpy 按 requirements，torch/webdataset 用用户级安装或训练镜像；脚本统一用 `$PY`（缺省 `/usr/bin/python3`），代码里已无硬编码解释器 |
| 22 | fluid 组报「等待 Dataset Bound 超时(>420s,当前 phase=NotBound)」，且失败严格按「一轮挂、下一轮过」交替 | `down.sh` 的 `delete dataset --wait` 只等 Dataset 对象本身，JuiceFSRuntime 是 Fluid 事后异步级联删的；下一次 up 见 Runtime 还在（带 deletionTimestamp）就按「已存在,跳过」不放新的，它随即被 GC 掉，Dataset 没有 Runtime 认领 → 永远 NotBound。证据：撞线轮 420s 一动不动，下一轮 10s 就 Bound（因为尸体已清干净）——是竞态，不是级联慢 | `_common.sh` 增 `resource_live`（存在且无 deletionTimestamp）与 `wait_absent`；up.sh 加 §3.5 删除屏障、down.sh 删 Dataset 后等 Runtime 删净；Bound 超时打现场快照（runtime 三相位 + Pod 列表）。420s 上限只当保险，不拿它掩盖竞态 |
| 23 | fluid-cold 的 startup 同格内乱跳（S-1000：1.31 / 1.36 / 5.57 s；S-100：12.1 / 44 / 47 s），最刺眼的是 1.31 s 比同格 direct 的 5.76 s 还快——1000mbit=125 MB/s 下不可能 | 冷态复位只 `rm -rf` 了**磁盘**缓存目录，但 cold 组是「up 一次、rep 间只删测量 Pod」，跨 rep 存活的 FUSE 挂载让内核里那批 fuse 文件的**干净页**留了下来；下一 rep 的首批读直接命中页缓存，**根本不过 tc 限速链路**。为什么是双峰而不是恒快：机器只有 32 GiB，内存吃紧时这些页被回收就得回源（44 s），宽松时命中（12 s）。探针证据（2 s 采样 MinIO 侧 tbf `Sent` + 四台节点的 `/tmp/juicefs-cache`）：每 rep 起跑前四台目录都是 0——「解链了但挂载进程握着 fd」那条猜测**证伪**，漏的是页缓存那一层 | `_clear_node_caches()` 尾部加 `_drop_host_page_caches()`：`sync; echo 3 > /proc/sys/vm/drop_caches`，sudo -S(bench/.sudo-pass) 优先、特权 docker 回退，两条都不通直接退出（宁可不跑也不出「看着是冷读其实是页缓存」的行）；口径与 D 轨 `track_d_up.sh` 冷态④一致。受影响的 4 行（S-100 `fluid-cold`/`juicefs-cold`、S-1000 `fluid-cold` 两行）在原表标 `void=True`、`reason=suspect_page_cache`，行与数值保留；重跑行并入同格后，S-1000 `fluid-cold` startup 均值 7.31 s（5.57/8.05/8.31），高于同格 `direct` 的 5.79 s，物理上不再倒挂 |
| 24 | fluid 组「warm 的 startup 比 cold 还慢」（S-1000：warm 8.2–8.7 s vs 假快的 cold 1.31–1.36 s）看着像矛盾 | fluid 两组只有 makespan / stall 三项是实测，`hit_rate` 与 `startup_warmup_bytes` 是 9.1 D25 的**近似口径**（`approx_cold_all_refetch` / `approx_warm_all_hit`，写死 0 与 1），不是量出来的命中；再叠上第 23 条那个假快的 cold 行，就成了「warm 更慢」的错觉 | 不改采集，改引用：报告里带 fluid 的命中类数字必须连着近似口径一起写，胜负判定只看实测三项。第 23 条重跑后 S-1000 `fluid-cold` startup 均值 7.31 s、`fluid-warm` 8.49 s，warm 仍不比 cold 快；该对比按实测三项照实报，不从命中率近似值推因果 |

概述：这张表是装置的全部病史，动环境之前先来对一遍。

## 8. 禁改清单（清理磁盘、重置实验时一律不动）

1. MinIO `bench-b/shard-*.tar`——五组共同源数据，删了等于重造数据。
2. 旧 `bench-b/compare-data/`（64MiB 块快照）——保留供历史产物复现（9.1 D41）。
3. kind 集群本体与 MinIO PVC——清理脚本不许 `kind delete`。
4. `~/tcd/bench/data/`、`~/tcd/probes/data/`——四轨数据与探针 tar 的实体（§9），一律不许删。四轨数据种子可再生成，但重生成很贵。
5. `~/tcd/bench/results/`、`~/tcd/bench/perf_results/` 的已归档产物——只追加，不覆盖，不删。
6. `bench/.sudo-pass` 内容——任何文档、日志、回复里都不出现。
7. `bench-down-full` 只删控制面资源，不执行 `kind delete`（第 3 条），不碰 `~/tcd`（第 4 条）；namespace `traincache` 与工作负载 ConfigMap 保留。

概述：这七条被破坏的代价是重造数据或泄漏凭证，比省下的磁盘贵得多。

## 9. 数据与产物的物理落点（硬性）

数据与产物一律实体落在 `~/tcd/`（＝`~/Project/TrainCache/traincachedata`），repo 内只存脚本与清单。`bench/config.py` 的 `DATA_ROOT` 直接指向数据层，repo 内没有同名的数据、结果、日志目录。目录分三层，IDE 与 Qoder 的工作根只允许指到代码层，不能指到容器层：

| 层 | 路径 | 装什么 | 实测规模 |
|---|---|---|---|
| 容器层 | `~/Project/TrainCache/` | 空壳，只用来隔出上下两层 | — |
| 代码层（repo） | `~/Project/TrainCache/traincache/`＝`~/tc` | Go/Python 源码、脚本、清单、docs、`.git` | 14,770 条 / 200M |
| 数据层 | `~/Project/TrainCache/traincachedata/`＝`~/tcd` | 四轨数据、探针 tar、运行结果 | 1,000,211 条 / 24G |

数据层按 repo 模块一一对应：

| 数据层路径 | 装什么 |
|---|---|
| `~/tcd/bench/data/` | 四轨数据：A 小文件、B tar 分片、C 清单、D shard |
| `~/tcd/bench/results/` | 主实验与对照产物树，`run_bench.py` 的 `--out` 相对路径以它为根 |
| `~/tcd/bench/results/quick/<run_id>/` | `bench-quick` 产物 |
| `~/tcd/bench/logs/` | 主实验脚本日志 |
| `~/tcd/bench/compare/logs/` | 四个目录组的 up/down 与跑分日志 |
| `~/tcd/bench/perf_results/` | `summary.csv`、`figures/`、`rerun.log` |
| `~/tcd/probes/data/` | 探针数据与 tar |

为什么把数据移出 repo：inotify 配额按 UID 计算（本机 `max_user_watches=524288`），IDE 的文件监听跟着工作根走。百万级数据文件留在 repo 内会把整个 UID 的 watch 预算吃光，之后任何程序调 `inotify_add_watch` 都返回 `No space left on device`。搬到数据层后实测整个 UID 只用 10,154 条（1.9%），配额未改。

`.venv/` 不搬：它是环境不是数据，`pip`/`uv` 的 `pyvenv.cfg` 与 IDE 解释器配置记的是绝对路径。监听成本可忽略。

### 9.1 产物结构

```text
~/tcd/bench/
├── results/
│   ├── cold-verify.txt              # 装置可信性
│   ├── track_d/                     # D 轨：epochs_meta.json、per_step jsonl、verify_*.json、sweep_*/
│   ├── policy/                      # 淘汰实验运行目录
│   ├── quick/<run_id>/              # bench-quick：summary.csv、run.json、lock.json、vs_last.md
│   ├── <COND 运行目录>/              # 主实验：run.json + metrics.csv + collect.json + lock.json
│   └── compare/                     # 对照产物：run_bench.py 的 --out CSV（一张扁表）
├── logs/                            # 主实验脚本日志
├── compare/logs/                    # 四个目录组的 up/down 与跑分日志
└── perf_results/                    # summary.csv、figures/、rerun.log
```

主实验每个 run 目录里，`run.json` 存 t_submit / 首末步 / per-step / 预热时长，`lock.json` 存镜像 digest 与 tc 档位，`metrics.csv` 存四个指标。对照 CSV 每行要能回答「哪一档、哪个组、第几次、哪个 COND」，档位与带宽档同时写进文件名（`S-100.csv`、`M-1000.csv`）。`void` 轮不进汇总（9.1 D14），`exploratory` 不进正式表。对照 CSV 的列定义以 `run_bench.py` 的 `write_csv()` 为准，主实验以 `collect.py`/`stats.py` 为准。

概述：产物结构是脚本、图、复跑三方共同的读取约定，路径与命名先定下来才有人读。

## 10. 实测基准（引用时连采法一起带）

- **100mbit/20ms 档冷读**：8GiB 两轮对账 710s / 710s，相对差 0.0%（理论传输 640s），产物 `~/tcd/bench/results/cold-verify.txt`。只有这一档有冷读对账记录；@300 与 @1000 跨档比较前先各跑一遍 `make bench-verify`，710s 不外推到别的档位。
- **源站吞吐上限 C ≈ 674 MB/s**（9 次测量的中心值，区间 629~755，约 5.4 Gbit/s）。采法：与 train-job 同路径，worker 节点上的临时 Pod 经 `nodeIP:30090` 读 `mc cat bench-b/shard-*.tar`，全程无 tc。C 远高于 125MB/s，所以 @1000 档独立存在，带宽三档 {100, 300, 1000} 全部保留。
- **指标灵敏度（D 轨两档扫点，各 1 epoch）**：吞吐 12.4→32.8MB/s、p90 wait 1.23→0.013s、mean stall 28.5%→10.5%，而 median stall 8.53%→8.38% 几乎不动——prefetch 把它吸收了。扫点看 mean/p90/吞吐，中位数只用来判形态（`~/tcd/bench/results/track_d/sweep_100m|300m/`）。
- **D 轨 verify 主判据**：epoch 级 `median_stall` ≤10%。20 步窗口的均值被重尾毛刺主导（相邻 epoch 逐点相对差实测 37%~56%），所以窗口曲线只作 `report_only`。终验两轮相对差 1.6% / 0.5%。
- **stall_ratio 口径**：measure 脚本用 `COMPUTE_MS=200ms` 控制计算量，读与计算并行（模拟异步 DataLoader：读到数据的同时在算上一步）。预取正常时 wait_s≈0 → stall_ratio≈0；预取失效时 wait_s 占满 step_s → stall_ratio≈1。串行"读完整片再算"的版本里读比算慢两个数量级，stall_ratio 恒≈1，没有判别力。
- **traincache 组命中率口径**：用 agent `traincache_fetch_bytes_total` 的 run 窗口增量对账，回源字节计入 miss 侧，`hit_rate = (总读字节 − 回源字节) / 总读字节`。"磁盘上读到就算 hit"会虚高——agent 运行期会悄悄回源补块，测量体看不见。JuiceFS/Fluid 仍按 warm≈全命中、cold≈全回源近似（9.1 D25）。测量 Pod 经 hostPath `/run/tc-hook/agent.sock`（节点 `/run/traincache/agent.sock`）向 agent 上报读路径：缺块时发 `read_miss`（reason=`absent`）触发 P0 补块，读块成功后发 `advance` 推进消费游标；帧里的 path/chunk 必须与当前订单一致，发送失败静默忽略，不影响读数与字节账。
- **对照冷读清缓存口径**：四条清理命令（JFS hostPath、Fluid tieredstore `/tmp/juicefs-cache`、traincache blocks、traincache quarantine）统一走 `sh -c`，让 glob 在节点容器内展开。`docker exec <node> rm -rf <path>/*` 不带 shell，通配符不展开，`rm` 收到字面路径、不存在又带 `-f`，会静默返回 0。删完逐个目录 `ls -A` 判空，有残留或 `docker exec` 无法执行就终止本轮，不带着上一轮缓存起跑。**两层都要清**：目录清了还不算冷态，跨 rep 存活的 FUSE 挂载会把文件页留在内核页缓存里，读就不走限速链路了；所以 `_clear_node_caches()` 收尾必须 `sync; echo 3 > /proc/sys/vm/drop_caches`（sudo -S 优先、特权 docker 回退，两条都不通则终止本轮），与 D 轨冷态④同口径。
- **测量体计时与内存**：计时用 `time.perf_counter()`，wall clock 会被 NTP 校正拽出离群步，只保留跨进程对齐用的纪元时间戳。预读队列只传字节数，常驻内存 O(1)，实际持有的字节数由计数 `logical_resident_bytes` 跟踪、峰值进产物 `peak_resident_bytes` 列。hits/misses 自增加锁，多 reader 线程并发读改写会丢计数。读队列超时那一步的等待整段计入 wait_s，并把该轮标 `void`（reason=`read_queue_timeout`），不静默出数。
- **`kindest/node:v1.28.15` 标签名不副实**：该标签实际指向 v1.37 镜像，版本以 `kubectl get nodes` 实测为准。

概述：这些数字是装置的地基，换环境后必须重测，不许直接引用。
