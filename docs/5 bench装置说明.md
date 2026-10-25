# 5 bench 装置说明书（手册版 v3，2026-10-25 校准）

> 这份文档是 `bench/` 实验装置的说明书：文件是什么、命令怎么敲、一键化做到哪、出了毛病查哪行。
> v3 相对 v2 的改动：带宽升成实验自变量（10.1 D31）、M/L 档参跑组 fluid 化（D30）、对照部分机时结构重排（D32）三处口径回写；新增 §4.2「只有 TrainCache 时怎么启动」；§3 参数总表与 §9 产物现状按磁盘代码逐条重核。
> 两类读者：维护这套装置的人（§2、§3），和将来迭代版本的我自己（§4 起）。
> 路径一律写相对路径 `bench/...`。`/home/howardhe/tc` 是指向项目根的符号链接，不是另一份拷贝，看到旧文档里的 tc/ 前缀就当项目根。
> 状态图例：**✅ 已实现**｜**🔧 改造中**｜**📝 规划待建**。本文只把已实现的当现状，规划项明确标注，不混。
> 纪律：文档是代码的声明式。以磁盘代码为准，发现对不上，当天改文档。

## 1. 装置是什么

一套在单机（32 核 / 30 GiB 内存 / 4 worker kind 集群）上复刻"分布式训练存储"的实验装置，做三件事：

1. **造冷热差**：tc 把源站出站路径限到指定档位（默认 100mbit/20ms，100Mbps≈12.5MB/s，读 8GB 光传输 640 秒），本地缓存命中绕开这条路，冷读慢热读快是真差不是假差。**带宽本身是实验自变量**（10.1 D31）：rate 取 {100, 300, 1000} mbit，burst 按 rate 推导，抖动只在临界档开。
2. **造负载**：四轨合成数据（A 小文件元数据、B 8GB webdataset tar、C 倾斜访问清单、D 2GiB 连续 epoch），固定种子、逐字节可复现。
3. **同台跑分**：TrainCache 与 direct/juicefs/fluid 五个受试者（展开是七个组态）吃同一份轨迹、同一个带宽档、同一个容量的缓存，一键起拆、一键出指标（§4.4、§9）。

```text
bench/
├── Makefile          # 主实验入口（四轨+限速+冷态+verify）
├── config.py         # 唯一参数源：python3 config.py --sh KEY 查询
├── scripts/          # 数据生成、负载器、tc、冷态、verify、trainpublish 二进制
├── k8s/              # kind 集群配置 + MinIO manifest
├── data/             # (gitignore) 四轨数据，种子固定可再生成
├── results/          # 归档：冷读验证、D 轨对账、扫点
├── compare/          # 五组对照装置（run_bench.py + 每组目录）
└── experiments/      # 条件化实验层（COND 契约、runner、统计、出图）
```

一句话：装置把"冷读多慢、换个策略快多少、跟对手比赢在哪"变成同一条命令能重复测量的数字。

## 2. 文件与接口（给维护者）

### 2.1 主实验层（bench/ 根 + scripts/）

| 文件 | 作用 | 接口 / 被谁调用 | 状态 |
|------|------|-----------------|------|
| `Makefile` | 全部入口 | `bench-up`/`bench-down`/`kind-up`/`k8s-up`/`data-up`/`upload`/`tc-up RATE= DELAY=`/`tc-down`/`bench-cold`/`bench-verify QUICK=`/`bench-data`/`bench-tc`/`track-d-data\|up\|verify\|down`/`e2e-local`/`e2e-k8s`/`bench COND= REPS=`/`bench-list` | ✅ |
| `config.py` | 唯一参数源 | `python3 config.py --sh <KEY>`；被所有脚本读 | ✅ |
| `scripts/synth_track_{a,b,c,d}.py` | 四轨数据生成 | 参数全在 config；幂等，已存在即跳过 | ✅ |
| `scripts/bench_track_d.py` | D 轨负载器 + verify 判据 | `--epochs --seed --workers --out-dir`；`--selftest` 自测判据函数 `compare_epoch_medians`。⚠ 模块级 import numpy/torch/webdataset，本机解释器已缺这些依赖 → 现在连 `--selftest` 都跑不动（§7 第 20 条） | 🔧 需重建依赖 |
| `scripts/bench_track_c.py` | C 轨双 Job 倾斜负载器 | M3/M5 用（热集循环 + 全量扫描） | ✅ |
| `scripts/tc_apply.sh` / `tc_teardown.sh` | 挂/拆限速 | `<rate> <delay> [burst] [--target minio\|source-d] [--jitter\|--no-jitter] [--tier S\|M\|L] [--dry-run] [--self-test]`；burst 不给时按 rate 推导（`config.burst_for_rate`），`--tier` 让脚本自己判该档要不要抖动 | ✅ |
| `scripts/cold_reset.sh` | 主实验冷态规程 | 无参；sync+drop_caches+重启 MinIO+重挂 tc。**对照层不用它**（run_bench 自带 `cold_reset()`，只清 Pod 与缓存目录、不重启 MinIO，见 §4.4） | ✅ |
| `scripts/verify_cold.sh` | 装置可信性 | `[--quick]`；两轮冷读相对差 ≤10% 判过 | ✅ |
| `scripts/track_d_{up,verify,down}.sh` | D 轨编排 | 零参数，`--dry-run`；up=容器+欠配+tc+10 epoch，verify=两轮×3 epoch 对账，down=拆除保留数据 | ✅ |
| `scripts/upload_bench.py` | B 轨灌 MinIO | 无参；每次全量重传（同键覆盖+重设公开读 policy），无"存在即跳过" | ✅ |
| `scripts/trainpublish` | 发布 TrainCache 块快照 | Go 二进制（源码 `core/cmd/trainpublish`，重编：`/usr/local/go/bin/go build`）；关键旗标 `-chunk-size`（实验取 8388608，见 §3） | ✅ |
| `k8s/kind-config.yaml`、`k8s/minio.yaml` | 集群与源站基座 | kind 5 节点配置；MinIO 带 30Gi PVC（必须，见 §7 第 6 条） | ✅ |

### 2.2 五组对照层（compare/）

| 文件 | 作用 | 接口 / 说明 | 状态 |
|------|------|-------------|------|
| `compare/Makefile` | 对照入口 | `compare-up/down GROUP=`、`compare-all-up/down`、`trace`（生成统一轨迹）、`check`、`help` | ✅ |
| `compare/run_bench.py` | 跑分编排器 | `--reps N --groups <组...> --tier S\|M\|L --out <csv>`；`--tier` 缺省取环境变量 `CAPACITY_RATIO`（再缺省 S）；档位决定 `N_TARS`/`reps`。7 个组名映射 4 个目录组（direct、juicefs-{cold,warm}、fluid-{cold,warm}、traincache-{cold,warm}）；`--selftest` 离线自检（不连集群，验模板可编译、字节账精确、超时分支计入统计）。**未闭合**：不按档位过滤参跑组（跑 M/L 仍会把 warm 组一起拖跑）、每 rep 仍 up/down、L 档不自动把 `EPOCHS` 降到 1（§3、§4.4） | ✅🔧 |
| `compare/direct/` | 组 1：HTTP 直读 MinIO，零缓存 | `up.sh` 建 ns/secret/trace ConfigMap+tc；无预热 | ✅ |
| `compare/juicefs/` | 组 2/3：JuiceFS 冷/热 | `up.sh`：Redis+mount Pod（缓存 hostPath `/var/cache/jfs`）；`JFS_WARMUP=1` 时预热在 up.sh 内完成（15 分钟硬超时，超时整轮作废）；`--cache-size` **单位是 MiB** | 🔧✅（合一改造已完成） |
| `compare/fluid/` | 组 4/5：Fluid 冷/热 | `up.sh`：Dataset+JuiceFSRuntime+PVC；`FLUID_PREHEAT=1` 时 DataLoad 在 up.sh 内等待完成；`FLUID_CACHE_GI` 控容量 | ✅ |
| `compare/traincache/` | 组 6/7：TrainCache 冷/热 | `up.sh`：打 `s3-access` 标签（`label_s3_nodes`）、发布快照（`publish_snapshot`，`trainpublish -chunk-size 8388608` → `dataset/compare-data-8m/`，CURRENT 已存在则跳过）、校验/补部署控制面、注入 agentd 配置（`ensure_agent_config`）、建 DataSet/WarmupTask（`TRAINCACHE_WARMUP_K`，warm=128/cold=0）、挂 tc；**sleep 占位 compare-job 已删** | ✅（控制面自装与 8MiB 口径已就位；正式跑分前仍需真发一次 `compare-data-8m` 并核对 agentd WHITELIST） |
| 统一测量体 | **每组只有一个 train-job Pod**：measure 脚本是主容器；traincache 组走 webhook gate（Pending 等待计入 startup），其余组普通调度；`t_submit` = 创建该 Pod 之前的时刻，五组同尺 | `run_bench.py create_measure_pod()` | ✅ |

### 2.3 条件化实验层（experiments/）

| 文件 | 作用 | 状态 |
|------|------|------|
| `cond.py` + `cond.schema.md` | COND 契约（v1）：一个 JSON = 一个实验点；解析器校验字段全集并产出环境变量。带宽档（`network.rate_mbit` ∈ {100,300,1000}）、burst（按 rate 推导，值不符直接拒收）、档位参跑组（`CAPACITY_TIER_GROUPS`，组名不在本档集合内即拒）都已进校验器；抖动**不是** COND 字段，由 `config.jitter_enabled(tier, rate)` 推导 | ✅（`capacity_ratio`、`chunk_size_bytes`、`warmup_seconds` 三字段已进契约与校验器；占位符 digest/sha 一律拒收） |
| `conds/*.json` | 正式 COND 集，共 34 个：`compare-d-<组态>-<rate>`（七组态 × {100,1000}，再加 direct/fluid-cold/traincache-cold 的 300 抖动格 = 17 个）、`compare-b-m-<组>-1000`（3 个）、`compare-b-l-<组>-1000`（2 个）、`policy-{heat,lru,lfu}`、`pareto2-lookahead{8,16,32}-{heat,lru,lfu}`；`policy-wtinylfu.json` 随 W-TinyLFU 撤档删除（10.1 D9） | ✅ |
| `run.py` | 主实验 runner：冷态→搭负载→采集→归档，失败标 `void` | ✅ |
| `run_policy.py` | M5 淘汰×预取实验执行体（注入 `TRAINCACHE_EVICT_POLICY`、`TRAINCACHE_PREFETCH_LOOKAHEAD`） | ✅ |
| `collect.py` | 离线汇总：makespan 两边界、三笔字节成本、分阶段命中率 → `metrics.csv`/`collect.json`；`--selftest` | ✅ |
| `stats.py` | 中位数 + bootstrap 95% CI（10000 次重采样） | ✅ |
| `lock.py` | 环境清单：镜像 digest、kubectl 版本、tc 档位、种子写进产物目录 | ✅ |
| `plot.py` | 五张旗舰图统一绘制 | ✅ |
| `preregister.md` | 预注册文件（冻结前允许追加假设；S/M/L 档位假设待写入，📝随 M6） | ✅ |

一句话：三层各管一件事——主实验层管"造冷热差"，对照层管"同台跑分"，实验层管"条件可复现"。

## 3. 参数总表（哪个旋钮影响什么）

| 旋钮 | 现值 | 写在哪 | 影响 |
|------|------|--------|------|
| `TC_RATE / TC_DELAY` | 默认 100mbit / 20ms；**正式跑分按 COND 的 `network.rate_mbit` 取 {100, 300, 1000}** | `config.py` 缺省值；`make bench-tc RATE= DELAY=`；对照层由 `TC_RATE`/`TC_DELAY` 环境变量经 up.sh 传给 tc_apply | 回源管道粗细。带宽是实验自变量（10.1 D31），不再有「对照三档不许动」的固定值；有效带宽 = min(名义 rate, MinIO 实测上限) |
| `TC_BURST` / 抖动 | burst 按 rate 推导：100→1250k、300→3750k、1000→12500k；抖动只在 S@300 与 M@1000 开（`delay 20ms 10ms distribution normal`） | `config.burst_for_rate()` / `config.jitter_enabled(tier, rate)`；`tc_apply.sh` 的 `--tier`/`--jitter` | 突发窗口必须跟着 rate 走，固定 1600k 在 1000mbit 下只剩 13ms。**现状缺口**：四个 `compare/*/up.sh` 只传 `rate delay`，没传 `--tier`，所以对照层跑不出抖动（§7 待改） |
| `TRACK_B_N_TARS` | 32（~264MB/个，≈8GiB） | `config.py` | 源数据规模 |
| `CAPACITY_RATIO` / `--tier` | S（缺省） | `run_bench.py` 环境变量 / 旗标；档位参数单点定义在 `experiments/cond.py` 的 `CAPACITY_TIER_*` | 一键决定 `N_TARS`、每点次数（S=3、M=3、L=1）与本档参跑组（S 七组态、M/L 三组/两组，D30）。非法值退回 S 并打印告警 |
| `N_TARS`（对照） | 8（≈2GiB，S 档）；M=32（8GiB）、L=128（32GiB） | `run_bench.py` 环境变量（缺省按 `CAPACITY_RATIO` 取） | 每组读的分片数；L 档的 128 片需要新增生成 24GiB 数据，只追加不覆盖 `bench-b`（Q12） |
| `chunk-size`（实验块） | **8MiB**（生产默认 64MiB） | `trainpublish -chunk-size 8388608`，随产物归档 | 块=缓存与预取最小单位；1GiB 缓存=128 块（10.1 D20） |
| `TRAINCACHE_CACHE_CAPACITY_BYTES` | 1GiB（`1073741824`） | traincache `up.sh` `ensure_agent_config` 注入 agentd；代码实现在 `agent/svc/config.go` | TrainCache 缓存字节上限，淘汰按它触发 |
| `JFS_CACHE_MB` | 1024 | juicefs `up.sh` | JuiceFS 缓存上限；`--cache-size` 直接收 MiB 数值，**别再乘 1024** |
| `FLUID_CACHE_GI` | 1 | fluid `up.sh` | Fluid 缓存配额（Gi） |
| `TRAINCACHE_WARMUP_K` | warm=128 / cold=0（128 = 缓存容量 ÷ 块大小，代码里自动推，不再写死 16） | `run_bench.py` 环境变量 → `traincache/up.sh` → WarmupTask `podDetails[0].k` | 预热块数。**预热量 = k × chunk-size**：8MiB 块 × 128 = 1GiB = 缓存容量整档（对 M 档 8GiB 数据即「预热覆盖 1/8」，10.1 D20/D19）；换块大小按 `字节量 ÷ CHUNK_BYTES` 重算 |
| `TRAINCACHE_PREFETCH_LOOKAHEAD` | 16（默认） | `agent/svc/assemble.go`；experiments/run_policy.py 注入 | Agent 持续预取窗口；对照三档统一 16（D21） |
| `TRAINCACHE_PREFETCH_K` | 16（默认） | compare train-job 侧 ordersrc 消费窗口 | 训练侧消费深度，与 lookahead 是两个旋钮；对照三档统一 16（D21，与 agent 侧 `prefetchLookahead=16` 一致） |
| `TRAINCACHE_EVICT_POLICY` | heat（默认）/lru/lfu（wtinylfu 档已撤档，10.1 D9） | env 注入，非法名拒启 | M4/M5 用；对照实验一律 heat |
| `EPOCHS / STEPS_PER_EPOCH / BATCH / IMAGES_PER_TAR` | 3 / 400 / 32 / 1600 | `run_bench.py` 环境变量（与 `config.py` 的 D 轨口径对齐） | 对照训练形态：一个 epoch 恰好 80 秒（400 步 × 200ms 计算）按轨迹过一遍全量——每步读量 r 就是由这四个数决定的（§3 第一行的带宽分区）。**缺口**：L 档目标 `EPOCHS=1` 还没跟 `CAPACITY_RATIO` 联动，跑 L 档要显式给 |
| `COMPUTE_MS / READ_WORKERS / READ_QUEUE_DEPTH / READ_TIMEOUT_S` | 200 / 2 / 2 / 300 | `run_bench.py` 环境变量 → 下发进 measure 模板，不写死 | 每步计算量（决定 stall_ratio 有没有判别力，10.1 任务 6.15）与异步预取队列深度；队列只传字节数，常驻内存 O(1) |
| `TRAINCACHE_DATASET_WHITELIST / OBJSTORE_BUCKET` | compare-data-8m / bench-b | `ensure_agent_config` 运行时 `kubectl set env` | agentd 服务哪些数据集/桶。正式口径已是 8MiB 快照 `compare-data-8m`；换 `DATASET_NAME`（如冒烟前缀）时白名单必须同步，否则 preheat 全失败（§7 第 13 条） |
| seed | 20261015（D 轨）等 | `config.py`/COND | 数据与 shuffle 可复现的根 |

一句话：调什么之前先在这张表里找到它，改表外没有的参数说明装置跑偏了。

## 4. 操作手册（给我自己：命令干了什么、进度怎么看）

### 4.1 从零拉起（新机器/集群重建后）

```bash
cd bench
make kind-up          # 建 kind 5 节点集群（镜像需已 docker 导入，见 §7）
make k8s-up           # 装 MinIO（带 PVC）
make data-up          # 生成四轨数据（首次约 1 小时；幂等，已存在跳过）
make upload           # B 轨灌 MinIO + 公开读 policy（全量重传）
make tc-up            # 挂限速 100mbit/20ms（需要 sudo，走 .sudo-pass）
make bench-verify     # 两轮冷读对账 ≤10%，过了才配谈实验
```

查状态：`kubectl get nodes` / `kubectl -n minio get pods` / `tc -s qdisc`。

### 4.2 只有 TrainCache 时怎么启动（不搭 juicefs/fluid）

用途：装置自检、M4/M5 的淘汰与预取实验、traincache 组冒烟，以及「先把自家跑通」的日常迭代。
不需要 Redis、不需要 helm、不需要 `make compare-all-up`——四个目录组里只起 `traincache` 一个。

```bash
cd bench
make kind-up k8s-up data-up upload           # 已就绪会自动跳过；tc 不用手工挂，up.sh 自己挂
make -C compare compare-up GROUP=traincache  # 只起 TrainCache 组
```

`traincache/up.sh` 自带四步，顺序就是脚本里的顺序，全部幂等：

| 步 | 干什么 | 对应代码 | 出问题先查 |
|---|---|---|---|
| 1 | 给所有 worker 节点打 `traincache.io/s3-access=true`（本机 4 个；节点数是动态枚举的，不写死） | `label_s3_nodes` | NoAccessibleNodes（§7 第 12 条） |
| 2 | 发布快照：`trainpublish -chunk-size 8388608` 把 `bench/data/track_b/tars/shard-*.tar` 切成 8MiB 块，上传成 `bench-b/dataset/compare-data-8m/`；`CURRENT` 已存在就跳过 | `publish_snapshot` | DataSet SnapshotNotFound（§7 第 11 条） |
| 3 | 控制面：controller-manager / webhook / scheduler / agentd 都在则跳过，缺则 `kubectl apply -k controller/deploy` 并等 rollout | `ensure_control_plane` | 镜像不在 4 节点 containerd 里 → ImagePullBackOff（§7 第 1 条） |
| 4 | 注入 agentd 白名单与缓存上限（`TRAINCACHE_DATASET_WHITELIST=compare-data-8m`、`TRAINCACHE_CACHE_CAPACITY_BYTES=1GiB`），建 DataSet/WarmupTask，最后挂 tc | `ensure_agent_config` | preheat 全失败（§7 第 13 条） |

单组跑一轮（cold 档 k=0，不等预热）：

```bash
CAPACITY_RATIO=S DATASET_NAME=compare-data-8m \
python3 bench/compare/run_bench.py --reps 1 --groups traincache-cold \
        --out bench/results/compare/smoke-traincache.csv
```

`--reps 1` 低于档位规定的 3 次，会打印一行 warn（冒烟无所谓，正式跑分按 §4.4 的次数表）；这条 warn 的合法值判定还没跟 D32 对齐，见 `bench/compare/resources.md` §7.4 第 35 条。

只有 TrainCache 时能报什么：makespan（启动/全程）、stall 序列、命中率对账、三笔字节成本、预热时长——绝对值都有；「比 Fluid 快多少」没有，缺基线不许编。冷读口径靠 `cold_reset()` 保证（四条清缓存命令都走 `sh -c`，glob 在容器内展开，由 `run_bench.py --selftest` 第 4 项断言）。

### 4.3 主实验（D 轨与淘汰/预取）

```bash
make track-d-up                       # D 轨 10 epoch 连跑（宿主机直跑，不占集群）
make bench COND=policy-lru REPS=7     # 跑一个实验点（N 次含冷态循环）
make bench-list                       # 有哪些 COND
python3 experiments/collect.py ...    # 离线汇总出 metrics.csv
python3 experiments/plot.py           # 出图
```

进度查看：`ls results/<run目录>` 逐轮增多；训练打点在 per-step jsonl，`tail -f` 即可。

### 4.4 五组对照跑分（七个组态）

```bash
cd bench/compare                    # 脚本在 repo 内；日志与产物经软链接落 ~/tcd
python3 run_bench.py --groups direct --reps 1 --out ../results/compare/smoke.csv   # 先单组试跑

# 正式跑分：一条命令 = 一个「档位 × 带宽档」格子（D31/D32）
CAPACITY_RATIO=S TC_RATE=100mbit  python3 run_bench.py --reps 3 --out ../results/compare/S-100.csv
CAPACITY_RATIO=S TC_RATE=1000mbit python3 run_bench.py --reps 3 --out ../results/compare/S-1000.csv
# M/L 档必须显式列组：runner 还没接档位过滤，漏写 --groups 会把 warm 组一起拖跑
CAPACITY_RATIO=M TC_RATE=1000mbit python3 run_bench.py --reps 3 \
    --groups direct fluid-cold traincache-cold --out ../results/compare/M-1000.csv
# L 档探点：EPOCHS=1 也没跟档位联动，要手工给
CAPACITY_RATIO=L TC_RATE=1000mbit EPOCHS=1 python3 run_bench.py --reps 1 \
    --groups traincache-cold fluid-cold --out ../results/compare/L-1000.csv

make compare-up GROUP=juicefs-warm     # 只起某组环境（预热在 up 内完成并等待）
make compare-down GROUP=...            # 拆该组
```

跑中要盯的东西（每条都可核验，别信"应该快了"）：

```bash
kubectl get pods -A -l app=train-job -o wide     # 当前哪个组在测、落在哪个节点
kubectl -n <组ns> get pod train-job-0 -o jsonpath='{.status.phase}'   # traincache 组先看 Pending（gate 卡着）→Running
kubectl logs -n <组ns> train-job-0 -f            # 每步的 wait_s/step_s 实时打点
kubectl -n traincache get warmuptask             # 预热进度
kubectl -n traincache get pods -o wide | grep agentd   # 4 worker 各一且 Ready
tc -s qdisc show dev eth0                        # 确认档位真的是这一格要的值（含 burst）
```

注意：**CSV 每组跑完重写一次**，组内中断只丢当组；日志在 `bench/compare/logs/<组>.log`（软链接指向 `~/tcd/bench/compare/logs/`）。

### 4.5 拆除

```bash
make compare-down          # 或 make compare-all-down（拆尽四组）
make bench-down / tc-down  # 主实验拆限速；集群与数据保留
```

一句话：拉起跑验用 4.1，只跑自家按 4.2，日常实验按轨选 4.3/4.4，进度永远用 kubectl 实况说话。

## 5. 一键化规程（目标形态，状态标注）

| 动作 | 命令 | 干什么 | 状态 |
|------|------|--------|------|
| 体检 | `make bench-doctor` | 一次跑完 §5.1 清单，输出逐项绿/红 | 📝 待建 |
| 冷态 | `make bench-cold` | 主实验用：drop_caches + 重启 MinIO + 重挂 tc。对照层不用它——`run_bench.py` 自带 `cold_reset()`，只删 Pod + 清三个缓存目录，**不重启 MinIO**（所以 tc 不会丢；一旦手工重启 MinIO，必须重挂 tc，否则整轮跑在无限速链路上） | ✅ |
| 开始 | `make bench COND=` / `run_bench.py` | 见 §4 | ✅ |
| 清场 | `make compare-all-down` + cold_reset | 组间零残留 | ✅ |
| 出指标 | `collect.py`（主）/ `--out` CSV（对照） | 统一四指标 + 档位 + 预热时长列 | ✅（tier/预热列 📝随 M6） |
| 出图 | `python3 experiments/plot.py` | 五张图，负收益区域照画 | ✅ |
| 存档 | `make bench-save` | 镜像 docker save + MinIO 对象清单快照 + CRD/配置 dump + 产物打包 | 📝 待建 |
| 恢复 | `make bench-restore` | 从存档反向重建（kind 对时钟回拨等很脆，见 §7 第 8 条，恢复能力是刚需） | 📝 待建 |

### 5.1 doctor 检查清单（待建脚本的验收单）

1. kind 集群 5 节点 Ready；`kubectl version` 与镜像匹配（§7 第 9 条）。
2. 训练镜像 `python:3.12-alpine`、控制面镜像（REGISTRY=dgl3i4ucowsgvvdwty.xuanyuan.run TAG=0.8.0）已在 4 节点 containerd 里。
3. MinIO 桶 `bench-b`：32 个 `shard-*.tar` 在位、可读；`compare-data/`（旧 64MiB 快照）与 `compare-data-8m/`（新）在位。
4. agentd DaemonSet 4/4 Ready，`TRAINCACHE_CACHE_CAPACITY_BYTES` 已注入，`traincache_agent_degraded` 指标为 0。
5. 4 个 worker 均有 `traincache.io/s3-access=true` 标签。
6. tc qdisc 在位且档位 = COND 声明值。
7. `df` 磁盘余量 ≥ 实验预算（L 档全量口径约需 100GiB，预算上限 200GiB，10.1 Q11）。
8. `/var/cache/traincache/blocks`、`/var/cache/jfs`、`/runtime-mnt` 三个缓存目录已按规程清空（或声明本轮不清）。

一句话：凡是"手动记得的下一步"，都要变成 doctor/save/restore 的脚本行。

## 6. 每组独立 smoke（M6 验收执行体）

接口定义（📝 待建）：`make compare-smoke GROUP=<组名>`，内部四步：起该组 → 低负载一轮（`N_TARS=2 EPOCHS=1 STEPS_PER_EPOCH=4`）→ 判定 → 拆该组清缓存。任何装置改动后，先跑受影响组的 smoke 再谈正式实验。

| 组 | 通过判据 |
|----|----------|
| direct | train-job Running 到完成；四指标 CSV 行齐全；stall>0（限速生效） |
| juicefs-cold | 同上 + 缓存目录 `--cache-size` 生效（跑前 `du /var/cache/jfs` = 0） |
| juicefs-warm | up.sh 内 warmup 完成才创建 train-job；`du` 缓存非 0；startup 不含 warmup 时间 |
| fluid-cold / fluid-warm | PVC 挂载成功；warm 组 DataLoad 完成事件晚于 up、早于 t_submit |
| traincache-cold | k=0：WarmupTask 不创建，gate 立即放行，train-job 正常跑完 |
| traincache-warm | k=128（8MiB 块）：预热完成 → gate 放行；agentd 命中指标非零；落点节点的 blocks 目录有数据 |

全部组共判：`kubectl get events` 无 ImagePullBackOff/CrashLoop；产物含 run ID 并能关联 COND。

一句话：smoke 是装置的体温计——低负载、快出结果、只回答"这组还能不能用"。

## 7. 故障速查表（现象 → 根因 → 处置；都是真实踩过的，勿回退）

| # | 现象 | 根因 | 处置 |
|---|------|------|------|
| 1 | 训练 Pod ImagePullBackOff | 离线环境拉不到 docker hub；k8s/containerd 不读 daemon.json 的镜像加速 | 镜像 `image:` 必须写加速域名全称；或 docker 拉好 `kind load image-archive` 导入；训练镜像用 `python:3.12-alpine`（slim 拉不到） |
| 2 | 宿主机访问 MinIO NodePort 不通 | kind 不映射 nodePort 到宿主机 | 走节点 InternalIP（`http://172.19.0.x:30090`），upload 脚本已自动探测 |
| 3 | 冷读结果忽快忽慢 | MinIO/源站重启后 tc qdisc 与匿名读 policy 都丢 | 冷态规程自动重挂；不复挂 = 跑在无限速链路，该轮作废 |
| 4 | crictl 找不到 MinIO netns | 节点残留 NotReady 僵尸 sandbox（worker2 实测有一条） | `crictl pods --state Ready --name <pod> -q` 过滤，tc_apply 已改 |
| 5 | VM 重启后 fluid mount "not formatted" + redis format FATAL | redis 无 AOF，元数据没了；MinIO 前缀非空拒绝重新 format | 不是代码 bug：`kubectl delete ns fluid` 后重 up（down.sh 会清派生前缀）；kubeadm 证书过期先 `kubeadm certs renew all` |
| 6 | 全 API 401、Service 不通 | 宿主机时钟回拨，SA token nbf 在未来 | 重启 Deployment 重签；坏透就 `kind delete cluster && make bench-up`（数据在宿主机，重传 8 秒） |
| 7 | fluid-cold 等组跑出裸 Pod/NameError | 组名精确匹配漏改（7 组名 vs 4 目录） | run_bench 已改 startswith 分流；改组名时全局搜 `== "fluid"` 这类精确比较 |
| 8 | juicefs 缓存 1GiB 限制从不生效 | `--cache-size` 单位是 **MiB**，装置曾注释成 KiB 配了 1048576（=1TiB） | 配 1024；见 §3；历史 juicefs 结果因此作废过一轮 |
| 9 | warmup 了但测量组读不到缓存 | `warm.sh` 暖的是 jfs-mount 的 emptyDir，测量 Pod 用另一个 emptyDir | 缓存改 hostPath `/var/cache/jfs`；预热挪进 up.sh（D23/D24） |
| 10 | measure 的 startup 712 秒异常 | Pod 内 warmup 耗时被计进 t_submit→首步 | 预热外移 group_up 后特判 `TRAINCACHE_JOB_CREATED_AT="0"` 已删 |
| 11 | DataSet SnapshotNotFound | 快照没发布过（CURRENT 缺失） | up.sh `publish_snapshot` 调 trainpublish；8MiB 块要发布到 `compare-data-8m`（Q12） |
| 12 | NoAccessibleNodes | worker 缺 s3-access 标签 | `label_s3_nodes` 自动打；重建集群后要重跑 |
| 13 | preheat 全失败 | agentd WHITELIST/BUCKET 与实际数据集/桶不匹配 | `ensure_agent_config` 运行时 `kubectl set env`；换快照名记得同步改（§3） |
| 14 | shard 数量算错、读一半没数据 | 按 128MB 假设算 count，实际 ~264MB | 从对象 stat 读实际大小 |
| 15 | pkill 把 shell 自己杀了（exit 143） | `pkill -f 'kubectl'` 匹配到自己的命令行 | 用 `pgrep -f 'run_bench\.py'` 精确找人再杀 |
| 16 | drop_caches/tc 需要 root 卡住 | sudo 交互不可用 | `bench/.sudo-pass`（gitignore、600 权限）；文档不展开内容 |
| 17 | python 依赖炸（boto3 找不到） | 系统 python3 是 3.14，`bench/.venv` 的 site-packages 是 3.12，且 `.venv/bin/python` 链到系统 3.14 已不可执行 | 装置统一 `/usr/bin/python3`（`bench/Makefile`、`bench/compare/Makefile` 的 `PY`），boto3 1.43.x 经 PYTHONPATH 指 `bench/.venv/lib/python3.12/site-packages`；本机没有 `/usr/bin/python3.12`，别照抄旧文档写它 |
| 18 | `docker update --memory 0` 没恢复 | 本机 docker 把 0 当"不变更" | 恢复用 128GiB 等效大值，记录在 env json `restore_caveat` |
| 19 | D 轨 env 留痕里的 `loader.cmd` 不可复现 | `track_d_up.sh` L240 把解释器硬写成 `/usr/bin/python3.12`，实际跑的是 `$PY=/usr/bin/python3`（L33、L329） | 留痕是失真值，复现按 `$PY scripts/bench_track_d.py`；接线修复待办见 `bench/compare/resources.md` §7.4 第 34 条 |
| 20 | D 轨（`make track-d-up`）当前跑不起来 | `bench_track_d.py` 要 numpy/torch/webdataset，探针期装在 python3.12 的用户级 site-packages（docs/3 §环境要点）；本机已没有 3.12，`~/.local/lib/python3.12` 也没了 | 重启 D 轨前先重建解释器+依赖（3.12 或 3.14 都行，装完把 `track_d_up.sh` L240 的硬编码一并改成 `$PY`）。对照层 `run_bench.py` 只用标准库，不受影响 |

一句话：这张表是装置的全部病史，动环境之前先来对一遍。

## 8. 禁改清单（清理磁盘、重置实验时一律不动）

1. MinIO `bench-b/shard-*.tar`——五组共同源数据（32 个 ~8GiB），删了等于重造数据。
2. 旧 `bench-b/compare-data/`（64MiB 块快照）——保留供历史产物复现（10.1 Q12）。
3. kind 集群本体与 MinIO PVC——清理脚本不许 `kind delete`。
4. `bench/data`、`probes/data`、`tmp_shards`——repo 内这三个路径是**软链接**，实体在 `~/tcd/`（见 §9.0）。四轨数据种子可再生成但重生成很贵，探针 tar 与 shard 是实测原料，一律不许删。动软链只许 `rm -f 路径`：不带尾斜杠、绝不加 `-r`——`rm -rf bench/data/` 会穿过软链把 14G 数据删干净。
5. `results/`、`perf_results/` 已归档产物——只追加不覆盖不删。新产物按 §9.0 落 `~/tcd/`，repo 内 `bench/` 路径只存脚本与清单。
6. `bench/.sudo-pass` 内容——任何文档、日志、回复里不出现。

## 9. 产物目录规范（一键出指标/出图的读取约定）

### 9.0 数据与产物的物理落点（硬性）

一句话：**数据与产物一律实体落在 `~/tcd/`，repo 内只存脚本与清单，原位留同名软链接指回去。**

目录分三层。IDE 与 Qoder 的工作根只允许指到「代码层」，不能指到容器层。

| 层 | 路径 | 装什么 | 实测规模 |
|---|---|---|---|
| 容器层 | `~/Project/TrainCache/` | 空壳，只用来隔出上下两层 | — |
| 代码层（repo） | `~/Project/TrainCache/traincache/`＝`~/tc` | Go/Python 源码、脚本、清单、docs、`.git` | 14,770 条 / 200M |
| 数据层 | `~/Project/TrainCache/traincachedata/`＝`~/tcd` | 四轨数据、探针 tar、shard 产物、运行结果 | 1,000,211 条 / 24G |

repo 内的软链接一览（新增数据按同样规矩办：数据层建同名目录 → repo 内 `ln -s` 相对路径）：

| repo 路径 | 指向（相对） | 实体大小 |
|---|---|---|
| `bench/data` | `../../traincachedata/bench/data` | 1,000,157 条 / 14G |
| `probes/data` | `../../traincachedata/probes/data` | 18 条 / 2.0G |
| `tmp_shards` | `../traincachedata/tmp_shards` | 32 个 tar / 7.9G |
| `perf_results`（repo 根） | `../traincachedata/perf_results` | summary.csv、figures/、rerun.log |
| `bench/results` | `../../traincachedata/results` | 主实验与对照产物树（`run_bench.py` 的默认 `--out` 就在这里） |
| `bench/logs` | `../../traincachedata/bench/logs` | 主实验脚本日志 |
| `bench/compare/logs` | `../../../traincachedata/bench/compare/logs` | 四个目录组的 up/down 日志（`_common.sh` 的 `LOG_DIR`） |

为什么要这么切：inotify 配额是按 UID 算的（本机 `max_user_watches=524288`），IDE 的文件监听跟着工作根走。数据留在 repo 内时，百万级文件把整个 UID 的 watch 预算吃光，任何程序调 `inotify_add_watch` 都返回 `No space left on device`——连截图工具改配置都失败，报的却是「无法捕获屏幕」。搬走之后实测整个 UID 只用 10,154 条（1.9%），`max_user_watches` 一个没改。

两个事实要说准，别记错：

1. 软链接**会被**监听器跟随，数据层 113 个目录里 108 个仍在 watch 集合内。省下预算的是「百万个文件不再逐个加 watch」，实测数据文件被监听数 0/1,000,098。
2. 因此别指望 `~/tcd` 完全不被扫，它只是从百万级降到百级。

`.gitignore` 的坑：软链接是 git 的对象类型 `120000`（文件），带尾斜杠的目录型模式匹配不到它。所以每个数据路径要写两行——`probes/data/` 之外还得有 `probes/data`，否则 7.9G 的 `tmp_shards` 会以 `??` 出现在 `git status` 里，一次 `git add -A` 就进库。

`venv/`、`bench/.venv/` **不搬**：它们是环境不是数据，`pip`/`uv` 的 `pyvenv.cfg` 与 IDE 解释器配置记的是绝对路径，搬出去只会让项目变复杂。监听成本实测可忽略——两个环境各贡献 1,121 个目录，合计占当前预算的零头。

### 9.1 产物结构

```text
~/tcd/                          # 软链接，实际落 ~/Project/TrainCache/traincachedata
├── results/                    # repo 内 bench/results 是指向它的软链接
│   ├── cold-verify.txt              # 装置可信性
│   ├── track_d/                     # D 轨：epochs_meta.json、per_step jsonl、verify_*.json、sweep_*/
│   ├── policy/                      # M4/M5 淘汰实验运行目录
│   ├── <COND 运行目录>/              # 主实验：run.json + metrics.csv + collect.json + env.lock
│   └── compare/                     # 对照产物（现状只有 smoke/summary.csv）
│       └── <tier S|M|L>/<组>/<repNN>/   # 📝 目标形态，runner 尚未实现
│           ├── run.json             # t_submit/t_first_step/t_last_step、per-step、预热时长
│           ├── metrics.csv          # 四指标 + tier + 预热时长列
│           ├── env.lock             # 镜像 digest、tc 档位、seed、chunk-size、cache 配置
│           └── status               # ok | void | exploratory（L 档探点标这个）
├── perf_results/               # repo 根 perf_results 是软链接：summary.csv、figures/、rerun.log
└── bench/compare/logs/         # 四个目录组的 up/down 日志（repo 内 bench/compare/logs 是软链接）
```

现状对账（2026-10-25 实地核过）：`run_bench.py` **只写一个扁平 CSV**（`--out`，缺省落 `bench/results/compare/summary.csv`），上面那棵 `<tier>/<组>/<repNN>/` 目录树、`env.lock`、`status` 文件都还没实现——档位与带宽档靠文件名表达（`S-100.csv`、`M-1000.csv`）。repo 内另有几棵历史产物树体量太小不动：`probes/results/` 2.6M/45 条、`controller/e2e/results/` 17M/60 条。今后新产物按本节约定直接写 `~/tcd/`。

约定：每行数据能回答「哪一档、哪个组、第几次、哪个 COND」四件事；`void` 轮不进汇总（10.1 D14）；`exploratory` 不进正式表。`--out` CSV 与 summary.csv 的列以 §2.3 collect/stats 实现为准。

一句话：产物结构先冻结，脚本才有的读、图才有的画、别人才能复跑。

## 10. 不可再生的实测基准（引用时注意出处）

- **默认档 100mbit/20ms**：冷读 8GiB 两轮对账 710s/710s、相对差 0.0%（理论传输 640s，`results/cold-verify.txt`）。
- **只有 100mbit 这一档有实测基准**：D31 把带宽升成自变量后，@300 与 @1000 两档既没有冷读对账、也没有 MinIO 吞吐上限实测。正式跑分的第一格必须是 10.1 任务 7.7 的前置探针（无 tc 状态下测源站上限 C），再按 `make bench-verify` 逐档重验冷读 ≤10%，然后才谈跨档比较。100mbit 的 710s 不能外推到别的档位。
- **指标灵敏度（D 轨两档扫点，各 1 epoch）**：吞吐 12.4→32.8MB/s、p90 wait 1.23→0.013s、mean stall 28.5%→10.5%，而 **median stall 8.53%→8.38% 几乎不动**（prefetch 吸收了）。结论：扫点用 mean/p90/吞吐，中位数只判形态（`results/track_d/sweep_100m|300m/`）。
- **stall_ratio 改造（2026-10-24）**：旧版 measure 脚本是串行"读 256MB → matmul"，stall_ratio = wait_s/step_s 恒≈1（读比算慢两个数量级）。改造后用 COMPUTE_MS 参数（200ms）控制计算量，并将读与计算并行（模拟异步 DataLoader：读到数据的同时在算上一步），stall_ratio 现在有判别力——预取好时 wait_s≈0 → stall_ratio≈0，预取失效时 wait_s 占满 step_s → stall_ratio≈1。**所有旧对照 run_bench.py 产物的 stall_ratio 作废**，需按改造后口径重跑。
- **命中率对账（2026-10-24）**：旧版 traincache 组的 hit_rate 是"磁盘上读到就算 hit"——agent 运行期悄悄回源补块，测量体并不知道，全部记成 hit 导致 hit_rate 虚高。改造后用 agent `traincache_fetch_bytes_total` 的 run 窗口增量做对账，回源字节单独计入 miss 侧；hit_rate = (总读字节 - 回源字节) / 总读字节。JuiceFS/Fluid 仍按 warm≈全命中 / cold≈全回源的模型近似（见 10.1 D25）。
- **对照冷读口径修复（2026-10-24）**：`cold_reset` 清 traincache 块缓存的两条命令原写成 `docker exec <node> rm -rf /var/cache/traincache/blocks/*`——docker exec 不带 shell，通配符不展开，`rm` 收到的是字面路径，路径不存在又带 `-f`，静默返回 0。**块缓存从来没被清过，traincache-cold 实际带着上一轮缓存起跑**，五组对照的冷读口径不成立。现在四条清理命令（JFS hostPath、Fluid tieredstore、traincache blocks、traincache quarantine）统一走 `sh -c`，在节点容器内展开 glob。修复前的 traincache 冷读轮次一律作废，需重跑。
- **测量体计时与内存修复（2026-10-24）**：预读队列原本存整片 bytes，常驻内存约 (队列深度+读线程+1)×256MiB ≈1.25GiB，逼近训练 Pod 的 1GiB memory request；现改成队列只传字节数，常驻内存 O(1)，真实加载器会持有的字节数改用纯计数 `logical_resident_bytes` 跟踪（峰值见产物 `peak_resident_bytes` 列），字节账逐字节不变。计时从 `time.time()` 换成 `time.perf_counter()`（wall clock 会被 NTP 校正拽出离群步），只保留跨进程对齐用的纪元时间戳。hits/misses 的自增加锁（多 reader 线程并发读改写会丢计数）。读队列超时那一步原先被 `continue` 跳过，step_total 分母偏小 → stall_ratio 偏乐观；现在整段等待计入 wait_s，并把该轮标 `void`（reason=`read_queue_timeout`），不静默出数。
- **D 轨 verify 判据降级史**：20 步窗口均值被重尾毛刺主导（相邻 epoch 逐点相对差实测 37%~56%），主判据降为 epoch 级 `median_stall` ≤10%；窗口曲线标 `report_only`。首轮 FAIL（max_rel_diff 0.593）→降级后 PASS→终验 PASS（1.6%/0.5%）。
- **kindest/node:v1.28.15 标签实指 v1.37 镜像**：版本以 `kubectl get nodes` 实测为准。
- **遗留矛盾**：Makefile 注释 B 轨生成"约 25 分钟"vs 旧文档"~50 分钟"，待实测统一。

一句话：这些数字是装置的地基，换环境后必须重测，不许直接引用。
