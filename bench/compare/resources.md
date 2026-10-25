# 对照组资源与规模对齐记录（10.1 任务 6.14 交付物）

> 结论先行：五组对照吃同一份数据、同一个 tc 档位、同一块缓存容量档，差异只在缓存层实现。
> 本文件口径以 `docs/10.0 核心能力验收与测试.md`「缓存容量档位」「块大小与限速口径」和
> `docs/10.1 plan.md` §8/§13（D19/D20/D21/D24/D25）为准；代码与 docs 不一致的项列在 §7
> 「待改代码项」，表格本身写 docs 口径。数据实体与产物落 `~/tcd/`，repo 内只留脚本与软链接。

## 1. 共享参数（各组一致）

| 参数 | 值 | 来源/口径 |
|---|---|---|
| 数据集源 | MinIO bucket `bench-b`，对象 `shard-*.tar` | 五组共同的源数据，只此一份（Q12） |
| tc 速率 | 带宽是实验自变量（D31）：rate ∈ {100, 300, 1000} mbit，随 COND 的 `network.rate_mbit` 落盘；有效带宽 = min(名义 rate, MinIO 实测上限) | `config.TC_RATE_MBITS`，挂在源站 MinIO Pod eth0 egress，对七个组态一视同仁；`config.TC_RATE` 只是缺省值 100mbit |
| tc 延迟 | 20 ms（临界档在此之上叠 10ms 标准差的正态抖动，由 `config.jitter_enabled(tier, rate)` 推导，全表命中 S@300 与 M@1000 两格） | `config.TC_DELAY`；抖动不是 COND 字段 |
| tc burst | 按 rate 推导：100→1250k、300→3750k、1000→12500k（≈0.1 秒传输量） | `config.burst_for_rate()`，`tc_apply.sh` 不给第三个位置参数时自动推；`config.TC_BURST=1600k` 只是旧缺省，禁止跨档复用 |
| 块大小 | 8 MiB（实验口径） | D20：trainpublish `-chunk-size` 8MiB。块是 TrainCache 缓存与预取的最小单位；1GiB 缓存按 8MiB 切有 128 块，预取保护窗口（lookahead 16 块）只占 1/8，淘汰统计不失真。生产默认 64MiB 不动 |
| lookahead | 16，三档统一 | D21 |
| trace seed | 20261015，shuffle=per-epoch | `bench/compare/trace.py`（6.12：五组读同一份 trace.json） |
| epochs | 3 | `run_bench.py` 默认 |
| 被测对象形态 | 每组一个 train-job Pod（`train-job-0`），测量体=调度体合一 | D22 |
| 预热口径 | 预热在 group_up 内完成并等待，不计入 makespan；超 15 分钟整轮作废 | D11/D23；三组 up.sh 超时均硬编码 900s |
| MinIO endpoint | `http://minio.minio.svc.cluster.local:9000` | `config.MINIO_ENDPOINT` |

## 2. 缓存容量档位（D19，10.0「缓存容量档位」）

真实训练里数据集通常是节点缓存的几十倍，单点跑分无法外推，所以按数据/缓存容量比扫三档：

| 档 | 数据 | 缓存 | 容量比 | 参跑组 | 每点次数 | 定位 |
|---|---|---|---|---|---|---|
| S | 2 GiB | 1 GiB | 1:2 | 全部七组态 | 3+3（@100 与 @1000 各 3 次，符号翻转格补到 5） | 正式结果 |
| M | 8 GiB | 1 GiB | 1:8 | direct、fluid-cold、traincache-cold | 3 | 正式结果，欠配校验 |
| L | 32 GiB | 1 GiB | 1:32 | traincache-cold、fluid-cold | 1 | 探点：标 `exploratory`，不报 CI，不进正式表 |

- warm 组只出现在 S 档：1:8 及以下预热至多覆盖 1/8 数据，优势缩小是算术事实，不陪跑。
- 对照实验的 1GiB 固定缓存与 C 轨淘汰实验的「数据集 30%」是两套口径，不混用（Q5）。
- 数据规模由 `N_TARS` × `SHARD_SIZE_MB` 控制（`run_bench.py` 默认 32×256MiB=8GiB，即 M 档
  数据量；256MiB 是单个 shard tar 的目标大小）。三档选组与次数已接入 COND 的 `capacity_ratio`
  与 `sampling.reps`，见 §7。
- 参跑组的 fluid 化（D30）：M/L 档的 `juicefs-cold` 换成 `fluid-cold`；S 档仍是全部七组态。
  Fluid 若触发 Q6 的两次环境性失败，M/L 该组落回 `juicefs-cold` 补跑，产物记
  `baseline_substituted`——这个标记必须落在 summary.csv 里，不能只写在跑分日志里。
- 采样次数按带宽分区（D32）：S 档 @100 ×3 + @1000 ×3，符号翻转格补到 5；
  M 档 @1000 ×3；L 档 @1000 ×1（探点）。3 次必须报均值 + CI，只报中位数是违规形态。

## 3. 五组资源与规模对齐表

| 项 | direct | juicefs-cold | juicefs-warm | fluid | traincache |
|---|---|---|---|---|---|
| 命名空间 | `direct` | `juicefs` | `juicefs` | `fluid`（控制面 `fluid-system`，helm 预装） | `traincache`（控制面与负载同 ns） |
| 缓存层 | 无（零缓存基线） | JuiceFS CE v1.3.0 单客户端 | 同左 | Fluid JuiceFSRuntime（replicas=1） | TrainCache agent DaemonSet |
| 缓存介质 | — | hostPath `/var/cache/jfs`（D24；emptyDir 会让预热成果随 Pod 销毁，warm 口径不成立） | 同左，warm 先 `juicefs warmup /mnt/jfs/` | tieredstore：mediumtype HDD、路径 `/tmp/juicefs-cache` | hostPath `/var/cache/traincache/blocks`（measure 只读挂载） |
| 缓存有效容量 | — | `--cache-size 1024`（**单位 MiB**，=1GiB） | 同左 | `quota: 1Gi` | `TRAINCACHE_CACHE_CAPACITY_BYTES=1073741824`（=1GiB，up.sh 强制校正） |
| 元数据 | — | Redis 7（`redis.juicefs.svc:6379`） | 同左 | Redis 7（`redis.fluid.svc:6379`，独立实例） | DataSet CRD + MinIO 快照 `dataset/<name>/CURRENT` |
| 数据通路 | HTTP GET MinIO | FUSE `/mnt/jfs`（measure Pod 内自 mount，privileged） | 同左 | PVC `jfs-demo` 挂 `/data` | gRPC 取块 → 本地块读，miss 等预取、超时 HTTP 回源 |
| 预热方式 | 无 | 无（空缓存启动） | up.sh 内 warmup，单独计时 | warm 走 DataLoad `jfs-load`（target=/），cold `FLUID_PREHEAT=0` | warm `WarmupTask compare-task` k=16、`preheatFanout`=全部 s3-access 节点（4，D26）；cold k=0 |
| 调度 | 普通 | 普通 | 普通 | nodeAffinity 绑缓存 worker | warm：`traincache-scheduler` + webhook schedulingGate；cold：普通 |
| 训练 Pod 副本 | 1 | 1 | 1 | 1 | 1（WarmupTask `podCount: 1`） |
| CPU request/limit | 500m / 2 | 500m / 2 | 500m / 2 | 500m / 2 | 500m / 2 |
| 内存 request/limit | 256Mi / 1Gi | 1Gi / 6Gi（Pod 内跑 FUSE mount，放宽） | 同左 | 256Mi / 1Gi | 256Mi / 1Gi |
| 块大小（docs 目标 8 MiB，D20） | 不适用（整文件 HTTP 流式读） | 不适用（按对象读；请求序列与 8 MiB trace 对齐） | 同左 | 不适用（同左） | 8 MiB：trainpublish `-chunk-size 8388608`（现状代码仍 64 MiB，见 §7-1） |
| tc 限速（带宽是自变量，D31） | rate 取 {100, 300, 1000} mbit，随 COND 落盘；burst 按 `≈ 0.1 × rate` 重算；只有 BW/r ∈ [1,2] 的临界档加 netem 抖动（S@300 与 M@1000 两格，**对照层尚未接线**，见 §7.3 第 30 条）。挂在源站 MinIO Pod eth0 egress，五组共用同一档位 | 同左 | 同左 | 同左 | 同左 |
| 命中率口径（D25） | 无缓存 → 每次读皆回源，hit_rate≡0 | 近似：cold≈全回源 + 回源字节【近似值】 | 近似：warm≈全命中【近似值】 | 近似：warm≈全命中 / cold≈全回源 + 回源字节【近似值】 | `traincache_fetch_bytes_total` run 窗口增量对账，回源字节计 miss（非近似；代码现状见 §7-5） |
| 训练镜像 | `python:3.12-alpine` | `juicedata/mount:ce-v1.3.0` | 同左 | `python:3.12-alpine` | `python:3.12-alpine` |

fluid 组 cold/warm 共用上列表格，差异仅 `FLUID_PREHEAT` 开关；traincache 组同理，差异仅 `TRAINCACHE_WARMUP_K`。
TrainCache 控制面镜像：`$REGISTRY/traincache/{controller-manager,webhook,kube-scheduler}:0.8.0`、`traincache-agentd:0.8.3`（2026-10-24 从当前源码重建，与冻结的 HEAD 对齐）；
淘汰策略未显式设置，走 agent 默认 `heat`（`TRAINCACHE_EVICT_POLICY`）。
辅助组件限额：Redis 100m/128Mi → 1/512Mi（juicefs、fluid 各一份）。

> 块大小只有 TrainCache 有物理块（`chunkSize`，缓存与预取的最小单位）；direct/JuiceFS/Fluid
> 无 TrainCache 块概念，五组对齐的是同一份按 8 MiB 粒度切分的 trace 请求序列（D20/6.13）。
> fluid 组 `up.sh` 头注释已改成 tieredstore HDD + 节点 hostPath（§7-12）。
> 限速口径从「三档统一 100 mbit」改成带宽自变量（D31）后，§1 的共享参数表与本文 §2/§3 都要按
> COND 的 `network.rate_mbit` 重算 burst；固定 1600k 跨档复用会失真。

## 4. 命中率口径（D25）

- JuiceFS/Fluid 拿不到块级命中指标，按模型近似：**warm≈全命中、cold≈全回源 + 回源字节**，
  报表里必须显式标注"近似值"，不许为对手编造指标。
- TrainCache 组须用 agent 指标 `traincache_fetch_bytes_total` 的 run 窗口增量对账：
  回源字节计 miss，`hit_rate = (总读 − 回源) / 总读`。旧版「磁盘上读到就算 hit」口径作废（§7-5）。

## 5. 数据共享与冷态

所有组读同一个 MinIO bucket `bench-b` 的 `shard-*.tar`（源数据不删不动）：

| 组 | 取数方式 |
|---|---|
| direct | HTTP 直读 `shard-*.tar` |
| juicefs / fluid | `juicefs sync` 从 MinIO 导入各自 JuiceFS 文件系统（Redis 元数据隔离） |
| traincache | trainpublish 把 shard 切块发布为快照 `dataset/<DATASET>/`；正式实验一律用 8MiB 快照 `compare-data-8m`（Q12），旧 64MiB 快照 `compare-data` 只留作历史产物复现 |

冷态：每轮跑前 `cold_reset()` 删各组 Pod（保留 Redis），并清节点 hostPath 缓存 `/var/cache/jfs`、`/var/cache/traincache/{blocks,quarantine}`；down 不删 MinIO 桶数据。
fluid 的 `/tmp/juicefs-cache` 已不再显式清（仅靠删 worker Pod 顺带带出），残留风险见 §7-8。

## 6. 产物与归档

- 跑分产物 `summary.csv` 落 `~/tcd/results/compare/`（repo 内 `bench/results` 是指向
  `~/tcd` 对应目录的软链接，实体在数据层，docs/5 §9 口径），须带 `tier` 列（§7-6）。
- 环境锁定：镜像 digest、tc 参数、seed 随产物归档，`make bench COND=xxx` 一键复现（D12）。

## 7. 代码现状与 docs 口径的差（2026-10-24 复核）

> 快照口径：本表截至 2026-10-24。上一版记录的全部差异（§7-1~12）已逐条落地；
> 下方按「已闭合」「本轮新发现并已修复」「仍未闭合」三段给出当前状态。

### 7.1 已闭合（原差异 → 现在的代码）

| # | 原差异 | 现在的代码 |
|---|---|---|
| 1 | 块大小仍 64MiB | `traincache/up.sh` 的 `CHUNK_BYTES=8388608`、`run_bench.py` 的 `CHUNK_SIZE=8388608`，trainpublish 按 `-chunk-size 8388608` 发布 |
| 2 | 快照名默认 `compare-data` | `DATASET` 默认 `compare-data-8m`，由 `traincache/up.sh` 在 up 阶段发布 |
| 3 | `WARMUP_K=16` 注释按 64MiB 算 | 改为 `WARMUP_K=128`（8MiB × 128 = 1GiB = 缓存容量），注释写明按 `字节量/CHUNK_BYTES` 重算 |
| 4 | measure 不消费 trace.json | `run_bench.py` 生成 trace 并下发，measure 按 trace 的 `shard_perm` 读分片，Pod 内校验 `n_tars`/`epochs` 与 trace 一致 |
| 5 | traincache 命中率是「读到就算 hit」 | 改用 `traincache_fetch_bytes_total` 的 run 窗口增量对账，产物列 `hit_rate` 与三笔字节成本同时落盘 |
| 6 | `--reps` 默认 7、无 tier | 档位表 S/M/L 名义次数 = 3/3/1（D32），来源单点是 `cond.CAPACITY_TIER_REPS`，runner 的 `TIER_PRESETS` 直接取它；`--help` 与顶部 docstring 原写「S=5」，2026-10-25 已改成 3/3/1。S 档符号翻转格补到 5 是**补测流程**（`CAPACITY_TIER_REPS_ALLOWED` 只声明允许值），runner 里没有自动加跑逻辑 |
| 7 | 数据集规模没写进 COND | `capacity_ratio` 进 COND 契约与校验器，三档的 `n_tars`/`bytes`/`ratio` 固化在 COND 里 |
| 8 | `cold_reset()` 不清 fluid 缓存 | 注释与代码对齐：Fluid tieredstore 落节点 hostPath `/tmp/juicefs-cache`，冷态显式清空 |
| 10 | `PREFETCH_K` 默认 8 | runner 侧与 measure 模板侧统一为 16（D21），与 `agent/svc/assemble.go` 的 `prefetchLookahead = 16` 一致 |
| 11 | `juicefs/up.sh` 头注释写 emptyDir | 改为 hostPath `/var/cache/jfs`（D24），预热目标注释同步 |
| 12 | `fluid/up.sh` 头注释写「内存缓存」 | 改为 tieredstore HDD、落节点 hostPath `/tmp/juicefs-cache`、配额 1GiB |

### 7.2 本轮新发现并已修复

| # | 问题 | 影响 | 修法 |
|---|---|---|---|
| 13 | `cold_reset()` 清 traincache 块缓存的两条命令没走 shell，glob 不展开、删除命令静默返回 0 | **块缓存从未清过**：traincache 冷读组一直带着上一轮缓存起跑，五组对照的冷态口径不成立 | 改成 `docker exec <节点> sh -c "…"` 形式，glob 在节点容器内展开 |
| 14 | 预读队列装的是分片全量 bytes，队列深度 × 分片大小决定常驻内存（≈1.25GiB） | 逼近 train-job Pod 的内存 request | 队列改传字节数，常驻内存与深度、分片大小解耦；新增产物列 `peak_resident_bytes` |
| 15 | `hits/misses` 是多 reader 线程共享的无锁自增 | 命中计数可能丢 | 计数加锁 |
| 16 | measure 里用 `time.time()` 计时 | NTP 校正即可让单步耗时离群 | 测量计时改 `time.perf_counter()`；`time.time()` 只留纪元校准点 |
| 17 | 读队列取空超时后 `continue`，该步既不计 wait 也不计 work | `stall_ratio` 分母偏小、结果偏乐观，且不置 void、不作废该轮 | 超时步计入统计并把该轮标 void，产物列 `queue_timeouts` |
| 18 | `juicefs/fluid` 把桶里全部 32 片（8GiB）同步进自己的文件系统 | S 档只消费 8 片（2GiB），数据集口径与容量比被撑大 4 倍；预热也多搬 4 倍字节 | 同步按本档位分片数收窄（`_common.sh` 的 `shard_include_args` 生成精确的 `--include` 模式） |

### 7.3 按 D30~D32 的改动（2026-10-24 晚；2026-10-25 实地复核完成度）

**已落地（磁盘代码可核）**

| # | 改动 | 落点 | 证据 |
|---|---|---|---|
| 23 | COND 组名词表对齐 | `bench/experiments/cond.py` | `VALID_GROUPS` 七值（冷热由组态名表达），`CAPACITY_TIER_GROUPS` 同步改成七组态口径 |
| 24a | 带宽进 COND | `cond.py`、`cond.schema.md`、`conds/` | `ALLOWED_NETWORK={rate_mbit,delay_ms,burst_k}`、`VALID_RATES=config.TC_RATE_MBITS`；34 个 COND 里对照部分 17 个带 `-100/-300/-1000` 后缀 |
| 25 | burst 与 rate 联动 | `bench/scripts/tc_apply.sh` | `derive_burst()` → `config.burst_for_rate`，`--self-test` 逐档断言 1250k/3750k/12500k |
| 26 | netem 抖动参数化 | `bench/scripts/tc_apply.sh` | `--jitter/--no-jitter/--tier`、`netem_args()` 输出 `delay 20ms 10ms distribution normal`；`config.jitter_enabled` 判定临界档 |
| 29 | 档位次数与参跑组进 COND 校验 | `cond.py` | `CAPACITY_TIER_REPS={S:3,M:3,L:1}`、`CAPACITY_TIER_REPS_ALLOWED`（S 允许 3/5）、组名不在本档集合即拒 |

**声明过但还没落地（原表把这几条写成「现在已按…」，属虚标，2026-10-25 撤回）**

| # | 改动 | 该落在哪 | 现状与证据 |
|---|---|---|---|
| 22 | 参跑组按档位过滤 | `run_bench.py` | 未做。`--groups` 缺省仍是全七组态（L1506），主循环 `for group in args.groups`（L1543）不看档位；跑 M/L 档会把 warm 组一起拖跑。临时办法：命令行显式列组（docs/5 §4.4） |
| 24b | 每档默认 rate 由 BW/r 决定 | `run_bench.py` / up.sh | 未做。runner 完全不认识 rate，只靠外层环境变量 `TC_RATE` 透传；`TC_RATE=1000mbit` 得人手写 |
| 27 | rep 复用控制面 | `run_bench.py` | 未做。`run_group()` 每 rep 都 `cold_reset()`+`group_up()`（L1247-1248），结束再 `group_down()`，省不下每次 ≈2 分钟的搭建 |
| 28 | 基线替补标记 `baseline_substituted` | `run_bench.py` + summary.csv | 未做。全仓搜不到该字符串 |
| 30 | 对照层抖动接线 | 四个 `compare/*/up.sh` | 未做。up.sh 只调 `tc_apply.sh "$TC_RATE" "$TC_DELAY"`，不传 `--tier`，所以 S@300 与 M@1000 两格实际跑不出抖动（tc_apply 的能力是白给的） |
| 31 | L 档 `EPOCHS=1` 跟档位联动 | `run_bench.py` | 未做。`TIER_PRESETS` 只有 n_tars/reps/dataset_bytes，`EPOCHS` 缺省 3，跑 L 档要手工给 |

### 7.4 仍未闭合

| # | 问题 | 说明 |
|---|---|---|
| 19 | Fluid tieredstore 路径仍是 `/tmp/juicefs-cache` | 与 juicefs 组的 `/var/cache/jfs` 不同口径。改路径要删掉已存在的 JuiceFSRuntime 再重新 apply，风险高，暂缓；本次只保证冷态显式清空 |
| 20 | warm 组预热范围三组不同 | juicefs-warm / fluid-warm 预热本档整个数据集（S 档 2GiB），traincache-warm 预热缓存容量前缀（k=128 块 = 1GiB）。命中率可比（都约一半分片在缓存里），但**预热字节成本口径不同**，出表时须标注 |
| 21 | MinIO 端观测到的响应体读取模式变了 | direct 组的 HTTP 读取从「一次性 read()」改成「分段 read(n)」，字节总数逐字节相同；若服务端按 read 次数计费/限流需复核 |
| 32 | `_common.sh` 的 `TC_BURST` 只用于打印 | 真实 burst 由 tc_apply 按 rate 推导，`group_header` 与 up.sh 日志里显示的 1600k 是失真值。改法：日志直接取 tc_apply 的推导值，或按 `config.burst_for_rate(TC_RATE)` 现算 |
| 33 | MinIO 吞吐上限 C 未实测 | 带宽档 {100,300,1000} 的有效性、L 档单 run 是 13 分钟还是 6 分钟，全押在 10.1 任务 7.7 的前置探针上。探针没出数之前，§12 机时表按保守值读 |
| 34 | D 轨 env 留痕的解释器是失真值，且 D 轨当前跑不起来 | `track_d_up.sh` L240 把 `loader.cmd` 硬写成 `/usr/bin/python3.12 …`，实际执行走 `$PY`（L33 缺省 `/usr/bin/python3`，L329 用 `$PY`）。本机已无 `/usr/bin/python3.12`（系统只有 3.14），`bench_track_d.py` 依赖的 numpy/torch/webdataset 原装在 3.12 用户级 site-packages（docs/3 §环境要点），现已不可导入 → D 轨需重建解释器与依赖；对照层 `run_bench.py` 只用标准库，不受影响。修复前产物里的 `loader.cmd` 一律按「记录值≠执行值」读 |
| 35 | `--reps` 与档位规定次数冲突时只打印警告，且 S 档补到 5 会误报警 | `run_bench.py` 的 warn 文案是「正式跑分须用 {tier_reps}」，取的是 `CAPACITY_TIER_REPS` 单值；D32 允许 S 档 {3,5}（`CAPACITY_TIER_REPS_ALLOWED`），所以 5 次这一合法值也会被 warn。改法：warn 条件用 `reps not in CAPACITY_TIER_REPS_ALLOWED[tier]`（行为改动，待确认后接线） |
| 36 | L 档 `exploratory` 标记只有常量，没有落地 | `run_bench.py` L172 定义 `TIER_EXPLORATORY = {"L"}`，全文再无引用；`summary.csv` 的 `fieldnames`（L1330，27 列）没有 status/exploratory 列，产物里区分不出探点与正式点。契约见 `bench/experiments/cond.schema.md`「sampling.reps」与 §7.4 引用处；接线时同时决定 status 列的取值（ok/void/exploratory/baseline_substituted），与 docs/5 §9.1 的 `status` 文件口径合并 |
