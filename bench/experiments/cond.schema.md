# COND 契约

> 作用：一个 COND 文件描述一个实验点；跑几次由 runner 决定，不由 COND 决定。
> 文件位置：`bench/experiments/conds/<name>.json`。字段名、枚举、必填项以本文为准；runner 遇到未知字段直接拒绝。

## 1. 命名规则

- 文件名与 `name` 字段一致，全小写，用 `-` 分隔，长度 ≤ 80。
- 推荐格式：`<figure>-<track>-bw<rate>-ratio<ratio>-fan<fanout>-ep<epochs>-<shuffle>`。
- 对照实验用 `<figure>[-<tier>]-<group>-<rate>`：`compare-d-<group>-<rate>`（S 档）、`compare-b-<tier>-<group>-<rate>`（如 `compare-b-m-direct-1000`）。
- 示例：`fig1-d-bw100-ratio2-fan2`、`fig2-b-bw300-size8-ep10`、`fig3-d-shuffle-per-epoch-ep3`。
- 同一个 `name` 在 `conds/` 下必须唯一；重跑同一个 COND 用 `reps` 与产物目录区分，不改名字。

## 2. 字段全集

### 2.1 顶层

| 字段 | 类型 | 必填 | 取值/约束 | 说明 |
|---|---|---|---|---|
| `schema_version` | string | 是 | 固定 `"1"` | 契约版本 |
| `name` | string | 是 | 小写 kebab，≤80 | 与文件名一致 |
| `figure` | string | 是 | `fig1`/`fig2`/`fig3`/`pareto1`/`pareto2`/`compare-d`/`compare-b`/`policy` | 这个点属于哪组实验 |
| `group` | string | 是 | `direct`/`juicefs-cold`/`juicefs-warm`/`fluid-cold`/`fluid-warm`/`traincache-cold`/`traincache-warm` | 受试组态；冷热由组态名表达（D30） |
| `track` | string | 是 | `B`/`C`/`D` | 数据轨；A 轨不进入本验收 |
| `dataset` | object | 是 | 见 2.2 | 数据源与规模 |
| `trace` | object | 是 | 见 2.3 | 请求轨迹 |
| `workload` | object | 是 | 见 2.4 | 训练负载 |
| `network` | object | 是 | 见 2.5 | tc 档位 |
| `cache` | object | 是 | 见 2.6 | 缓存容量与策略 |
| `prefetch` | object | 条件 | `group=traincache-cold`/`traincache-warm` 时必填 | 预取/门控/评分参数 |
| `sampling` | object | 是 | 见 2.8 | 重复次数与统计 |
| `environment` | object | 是 | 见 2.9 | 环境锁定 |
| `tags` | array[string] | 否 | 每个 tag ≤32 字符 | 检索用，不参与语义 |
| `notes` | string | 否 | ≤500 字符 | 人类备注，不参与计算 |

### 2.2 `dataset`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `name` | string | 是 | 非空 | 数据集名，同时是索引/缓存命名空间 |
| `source` | string | 是 | `minio`/`local-nginx` | 图一/图三/D 轨对照用 `local-nginx`；图二/B 轨对照用 `minio` |
| `endpoint` | string | 是 | URL | MinIO 或 nginx 端点 |
| `bucket` | string | 条件 | `source=minio` 时必填 | 桶名 |
| `bytes` | int64 | 是 | >0 | 数据集总字节 |
| `shards` | int | 是 | `B` 轨为 8/16/32/128（256 MiB 分片；128 片 = L 档 32 GiB，需新增生成）；`D` 轨中图一/pareto 为 16、`compare-d` 为 8；`C` 轨复用 B 轨 | tar 分片数 |
| `shard_size_bytes` | int64 | 是 | >0 | 单个 tar 的目标字节；B 轨与 `compare-d` 为 256 MiB，图一/pareto 的 D 轨约 128 MiB |
| `chunk_size_bytes` | int64 | 是 | 正式实验固定 `8388608`（8 MiB） | 实验块大小；块是缓存与预取的最小单位（D20） |
| `seed` | int | 是 | >0 | 数据生成种子；B/D 轨固定值 |

`bytes` 必须等于 `shards × shard_size_bytes` 的 ±1% 以内；末片可略小，runner 以实际文件大小为准并记录。

`chunk_size_bytes` 与生产默认的关系：生产默认块大小 67108864（64 MiB）不变；实验把块切成 8 MiB，只改 trainpublish 发布快照时的 `-chunk-size` 参数，不改生产代码。原因：1 GiB 缓存按 64 MiB 只有 16 块，预取保护窗口（lookahead 16 块）覆盖整个缓存，淘汰排序统计失真；按 8 MiB 有 128 块，窗口占 1/8（D20）。正式实验一律读 8 MiB 切块的 `compare-data-8m` 快照（trainpublish `-chunk-size 8388608` 发布，Q12）。

### 2.3 `trace`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `file` | string | 是 | 相对 `bench/experiments/` 的路径(如 `traces/track-c-32shard-per-epoch.json`) | 同档位各组态共用的请求轨迹 |
| `sha256` | string | 是 | 64 位小写 hex | 轨迹文件 hash；产物目录要复核 |
| `seed` | int | 是 | >0 | 轨迹生成种子；与 `dataset.seed` 分开记 |

### 2.4 `workload`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `epochs` | int | 是 | `1`/`3`/`10` | 训练 epoch 数 |
| `batch` | int | 是 | 固定 32 | 每步样本数 |
| `workers` | int | 是 | 固定 4 | DataLoader 进程数 |
| `steps_per_epoch` | int | 是 | 固定 400 | 每 epoch 步数 |
| `prefetch_factor` | int | 是 | 固定 2 | DataLoader 预取因子 |
| `compute_ms` | int | 是 | 固定 200 | 每步合成计算目标耗时（真实 PyTorch 循环仍用这个预算） |
| `shuffle` | string | 是 | `fixed`/`per-epoch`/`curriculum` | 消费顺序形态 |
| `curriculum` | object | 条件 | `shuffle=curriculum` 时必填 | 见下 |

`curriculum` 对象：

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `stages` | int | 是 | 固定 3 | stage 数 |
| `shard_fractions` | array[float] | 是 | `[1.0, 0.6667, 0.3333]` | 每个 stage 使用的分片比例 |
| `boundary_rule` | string | 是 | 固定 `equal-thirds` | 边界规则；精确语义见下 |

`equal-thirds` 的精确语义：`E<3` 时全部 epoch 属于 stage 0；否则 `e < floor(E/3)` 为 stage 0、`e < floor(2E/3)` 为 stage 1、其余为 stage 2。分片集合取 `P = Random(dataset.seed).permutation(n_tars)` 的嵌套前缀：stage 0/1/2 分别为 `P[:n]`、`P[:ceil(2n/3)]`、`P[:ceil(n/3)]`；stage 内顺序为 `P` 过滤到该前缀，stage 内所有 epoch 一致。`E=1` 时退化为 stage 0 全量。

### 2.5 `network`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `rate_mbit` | int | 是 | `100`/`300`/`1000` | tc tbf 速率；对照实验把它当自变量扫（D31） |
| `delay_ms` | int | 是 | 固定 20 | netem 延迟；全档位统一，避免与带宽混淆 |
| `burst_k` | int | 是 | 由 `rate_mbit` 推导：`100`→1250、`300`→3750、`1000`→12500 | tbf burst，单位 KB |

`rate_mbit` 是被扫的轴。取值集合单点定义在 `bench/config.py` 的 `TC_RATE_MBITS`，COND 只能取这三个值。

`burst_k` 由 `rate_mbit` 推导，口径是「约 0.1 秒的传输量」：`rate_mbit × 125 KB/s × 0.1 s`，即 100mbit → 1250k、300mbit → 3750k、1000mbit → 12500k。burst 固定不变会让突发窗口随带宽缩水（1600k 在 1000mbit 下只有 13ms），所以它必须跟着 rate 走。算式在 `bench/config.py` 的 `burst_for_rate`，runner 与校验器都从那里取。

抖动（netem delay variation）**不是 COND 字段**，由档位与 rate 推导：`config.jitter_enabled(tier, rate_mbit)`，当「链路带宽 ÷ 每步读量」落在 `[1,2]` 这个临界区间时开启。按实测每步读量 S 档 ≈26.8MB/s、M 档 ≈107.4MB/s、L 档 ≈429.5MB/s，比值 BW/r 分别是 S：0.47/1.40/4.66，M：0.12/0.35/1.16，L：0.03/0.09/0.29 —— 所以抖动恰好落在 S@300 与 M@1000 两处。COND 里不写抖动开关，避免出现互相矛盾的两个真相。

### 2.6 `cache`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `enabled` | bool | 是 | — | `direct` 为 false，其余为 true |
| `capacity_bytes` | int64 | 条件 | `enabled=true` 时必填，>0 | 缓存容量上限 |
| `ratio` | float | 条件 | `enabled=true` 时必填 | 数据集/缓存容量比；`capacity_bytes` 必须等于 `dataset.bytes / ratio` 的 ±1% |
| `capacity_ratio` | string | 条件 | `S`/`M`/`L`；仅 `figure=compare-d`/`compare-b` 必填，其余 figure 不填 | 对照实验的容量档位标签（D19）；同时决定该档的参跑组（D30） |
| `evict_policy` | string | 是 | `heat`/`lru`/`lfu`/`external` | TrainCache 的策略取 `heat`/`lru`/`lfu` 三档（D9）；`external` 表示由 JuiceFS/Fluid 自己管理，TrainCache 不应用 |
| `warmup_seconds` | int | 否 | ≥0，默认 0 | 本点预热预算秒数；预热时长单列、不计入 makespan（D11/D23） |
| `root` | string | 否 | 绝对路径 | TrainCache 缓存根；默认 `/var/cache/traincache` |

`ratio` 的档位：图一用 1/2/4；图二/图三用 2；C 轨倾斜场景用 3.3333，表示缓存容量为数据集字节的 30%。30% 是显式容量上限：`capacity_bytes = floor(dataset.bytes × 0.30)`，runner 以 `capacity_bytes` 为准，`ratio` 只作展示与交叉校验；稳态 `disk_usage_ratio` 是否落在 25%~35% 是验收检查，不是控制量。

`capacity_ratio`、`ratio`、`capacity_bytes` 的关系（只有一套容量口径，不允许出现两套定义）：

- `capacity_bytes` 是唯一的控制量（淘汰触发的容量上限）；`ratio = dataset.bytes ÷ capacity_bytes`，两者继续按校验规则 4 做 ±1% 交叉校验。
- `capacity_ratio` 只是对照实验的档位别名，不引入新容量语义。固定映射：`S`↔ratio 2（2 GiB 数据 / 1 GiB 缓存）、`M`↔ratio 8（8 GiB / 1 GiB）、`L`↔ratio 32（32 GiB / 1 GiB）；三档缓存都是同一个 1 GiB。
- 写了 `capacity_ratio` 时，`dataset.bytes`、`capacity_bytes`、`ratio` 三个值必须与档位映射自洽（±1%），runner 校验；不一致直接拒绝。
- `capacity_ratio` 还决定该档跑哪些组态（D30）：`S` 档七组态全跑，`M` 档跑 `direct`/`fluid-cold`/`traincache-cold`，`L` 档跑 `traincache-cold`/`fluid-cold`。`group` 不在所属档位的组态集合里，校验器直接拒绝。
- C 轨（`pareto2`/`policy`）用 `ratio=3.3333`，不填 `capacity_ratio`：S/M/L 档与 C 轨 30% 是两套场景口径，不混用（Q5）。

`warmup_seconds` 口径：预热外置于计时——所有组的预热在计时开始前完成并等待，超过 15 分钟整轮作废（D23）。冷组写 0 或不填；`juicefs-warm` 与 Fluid 热读组写预热预算（上限 900）。TrainCache 组写 0：它的「热」来自启动窗口把整块缓存灌满，属于 startup 阶段，计入 makespan，不是外置预热。实测预热耗时由 runner 记入产物 `summary.csv` 的 `warmup_seconds` 列，与 makespan 分列，不进成本以外的任何时间口径；预热搬的字节仍计入成本（预注册 §1）。

### 2.7 `prefetch`（仅 `group=traincache-cold`/`traincache-warm`）

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `lookahead` | int | 是 | `8`/`16`/`32` | 持续预取提前量；正式图默认 8，对照实验（S/M/L 三档）固定 16（D21/D28） |
| `window_k` | int | 是 | 固定 8 | D 轨消费窗口块数，经 `cond_to_track_d_args` 传 `bench_track_d.py --window-k`；不写入 WarmupTask，对照跑分链路不读该字段（对照侧消费窗口由 `run_bench.py` 的 `PREFETCH_K` 独立给出，固定 16） |
| `fanout` | int | 是 | `1`/`2`/`3` | 每 Pod 预热节点数上限 |
| `fanout_min` | int | 是 | 固定 1 | 预热节点数下限 |
| `gate` | object | 是 | 见下 | 门控三条件参数 |
| `score` | object | 是 | 见下 | 全局衰减评分参数 |

`gate` 对象：

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `threshold` | float | 固定 0.9 | 启动窗口就绪比例门槛 |
| `gap_min` | float | 固定 0.1 | 边际收益条件的 gap 下限 |
| `slope_min` | float | 固定 0.001 | 边际收益条件的 slope 下限 |
| `t_max_s` | int | 固定 300 | 门控绝对超时 |
| `min_interval_s` | int | 固定 1 | 斜率采样最小间隔 |

`score` 对象：

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `half_life` | float | 固定 4 | 窗口外权重半衰期（块数） |
| `cutoff` | int | 固定 8 | 权重截断距离（块数） |

### 2.8 `sampling`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `reps` | int | 是 | `3`/`7`；对照实验按容量档位，见下 | 本点最终采样次数 |
| `coarse_reps` | int | 是 | 固定 3 | 曲面粗扫次数 |
| `upgrade_rule` | string | 是 | `sign-change`/`none` | 曲面符号变化的判定规则（`sign-change` 只做判定，不追加采样） |
| `bootstrap_iters` | int | 是 | 固定 10000 | bootstrap 重采样次数 |

约束：`figure=pareto1`/`pareto2` 时 `reps=7`；`figure=policy` 与 `figure=fig1`/`fig2`/`fig3` 时 `reps=3`，曲面另带 `upgrade_rule=sign-change`。同一批曲面里，3 次的点只用于定位符号变化，不能写“赢/输”。

对照实验（`compare-d`/`compare-b`）的次数按容量档位走（D32）：

| 档位 | 允许的 `reps` | 说明 |
|---|---|---|
| `S` | 3 | 固定 3 次 |
| `M` | 3 | 固定 3 次 |
| `L` | 1 | 固定 1 次（探点，见下） |

`L` 档只跑 1 次，低于 3 次硬纪律，所以产物要标 `exploratory`，不进正式表、不报 CI。COND 里用 `capacity_ratio=L` 表达这个标记（`tags` 里另加一个 `exploratory` 便于检索，runner 以档位为准）。`run_bench.py` 的 `row_status()` 按 `TIER_EXPLORATORY` 给 L 档产物写 `status=exploratory`（作废轮写 `void`），`summary.csv` 的 `fieldnames` 含 `status` 列。

### 2.9 `environment`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `kind_workers` | int | 是 | 固定 4 | worker 节点数；加 control-plane 共 5 节点 |
| `k8s_version` | string | 是 | 非空 | 例如 `v1.31.0` |
| `images` | object | 是 | 见下 | 镜像 digest，不写 tag |
| `lock_file` | string | 是 | 相对路径 | runner 生成的环境清单 |

`images` 的键（实际 COND 全部 9 个）：

| 键 | 用途 |
|---|---|
| `agent` | Agent 镜像 |
| `controller` | controller-manager 镜像 |
| `webhook` | 准入 webhook 镜像 |
| `scheduler` | kube-scheduler 镜像 |
| `training` | 真实 PyTorch + webdataset 训练镜像 |
| `minio` | MinIO 镜像 |
| `juicefs` | JuiceFS CE 镜像 |
| `fluid` | Fluid webhook/controller 镜像 |
| `fluid-csi` | Fluid CSI 驱动镜像 |

镜像字段写 `repo@sha256:...` 形式，digest 由 runner 记入 `lock.json`。

## 3. 按实验类型的额外约束

| `figure` | 额外约束 |
|---|---|
| `fig1` | `track=D`；`epochs=1`；`shuffle=fixed`；`ratio∈{1,2,4}`；`fanout∈{1,2,3}`；`network.rate_mbit∈{100,300,1000}` |
| `fig2` | `track=B`；`dataset.shards∈{8,32}`；`shuffle=per-epoch`；`ratio=2`；`fanout=2`；`epochs∈{1,3,10}` |
| `fig3` | `track=D`；`ratio=2`；`fanout=2`；`shuffle∈{fixed,per-epoch,curriculum}`；`epochs∈{1,3,10}` |
| `pareto1` | `track=D`；`epochs=1`；`ratio=2`；`fanout∈{1,2,3}`；`reps=7` |
| `pareto2` | `track=C`；`ratio=3.3333`（缓存 30%）；`fanout=2`；`epochs=3`；`lookahead∈{8,16,32}`；`evict_policy∈{heat,lru,lfu}`；`reps=7`；不填 `capacity_ratio` |
| `compare-d` | `track=D`；S 档专用：`capacity_ratio=S`；`epochs=3`；`ratio=2`（2 GiB 数据 / 1 GiB 缓存）；`fanout=2`；`evict_policy=heat`；七组态各一个 COND（`direct`/`juicefs-cold`/`juicefs-warm`/`fluid-cold`/`fluid-warm`/`traincache-cold`/`traincache-warm`，D30）；`reps=3`（D32）；`rate_mbit∈{100,300,1000}` |
| `compare-b` | `track=B`；缓存固定 1 GiB，`epochs=1`，`evict_policy=heat`，`fanout=2`。M 档：`capacity_ratio=M`，`ratio=8`（8 GiB / 1 GiB），`rate_mbit=1000`，参跑组 `direct`/`fluid-cold`/`traincache-cold` 各一个 COND，`reps=3`（D32）。L 档：`capacity_ratio=L`，`ratio=32`（32 GiB / 1 GiB），`rate_mbit=1000`，参跑组 `traincache-cold`/`fluid-cold` 各一个 COND，`reps=1`，产物由 runner 依 `capacity_ratio=L` 自动标 `exploratory`（预注册 1.9；warm 组不陪跑 M/L） |
| `policy` | `track=C`；`ratio=3.3333`；`epochs=3`；`evict_policy` 三档（`heat`/`lru`/`lfu`）；`reps=3`；不填 `capacity_ratio` |

`compare-d` 扫两个轴：组态与带宽。基准带宽 100 mbit 跑全部七组态（轴 `group`）；300 mbit 只跑 `direct`/`fluid-cold`/`traincache-cold` 三组（临界档，`BW/r∈[1,2]`，开抖动）；1000 mbit 跑全部七组态。

`group` 不是 `traincache-cold`/`traincache-warm` 时，`prefetch` 不生效，但 COND 里仍要写 `fanout` 与 `window_k` 的对照值（用同一档位下 `traincache-cold` 的同名 COND 共享），runner 会拒绝缺失的对照。组态名里的 `cold`/`warm` 指冷读与热读两种状态，是组态名的一部分（`traincache-cold` 就是 TrainCache 冷读），不再靠别的字段表达。

## 4. 字段与消费方对照

| COND 字段 | 消费方 |
|---|---|
| `dataset.endpoint`/`bucket`/`name` | `dataset.name` → 环境变量 `TRAINCACHE_DATASET`（`cond_to_env`）；`endpoint`/`bucket` 在 COND 有定义，当前链路无消费者，`DataSet.spec.endpoint`/`bucket` 由 `traincache/up.sh` 的 heredoc 另行写入 |
| `dataset.shards[*]` | `DataSet.spec.shards`，`chunkSize = dataset.chunk_size_bytes ÷ 1MiB`；正式实验为 8（8 MiB，D20），DataSet CRD 指向 `compare-data-8m` 快照（Q12） |
| `dataset.chunk_size_bytes` | trainpublish `-chunk-size 8388608`（发布快照时生效）与 `DataSet.spec.chunkSize`；生产默认 67108864 不进 COND |
| `cache.capacity_ratio` | 校验器按档位校验 `dataset.bytes`/`capacity_bytes`/`ratio` 三者自洽；`L` 时产物标 `exploratory`（D19）。同一档位的参跑组由 `CAPACITY_TIER_GROUPS` 决定（D30），校验器与 runner 双侧生效：`run_bench.py` 的 `select_groups()` 取 `CAPACITY_TIER_GROUPS[tier]` 过滤，`--groups` 缺省即该档参跑组，显式传入也与档位求交集、越档组态被剔除。runner 的档位取自 `CAPACITY_RATIO` 环境变量或 `--tier`，不读 COND 实例，两者须由跑分前的对账保证一致；`summary.csv` 的 `tier` 列写这个值 |
| `cache.warmup_seconds` | runner `group_up` 阶段的预热预算（超时整轮作废，D23）；实测预热时长写入产物 `summary.csv` 的 `warmup_seconds` 列，不计入 makespan（D11） |
| `prefetch.fanout` | 对照链路由 `traincache/up.sh` 写死为 s3-access 节点数（`preheatFanout=$S3_NODE_CNT`）；COND 字段无消费者 |
| `prefetch.fanout_min` | 对照链路由 `traincache/up.sh` 写死为 s3-access 节点数（`preheatFanoutMin=$S3_NODE_CNT`）；COND 字段无消费者 |
| `prefetch.window_k` | `bench_track_d.py --window-k`（D 轨消费窗口）；不写 `WarmupTask.spec.podDetails[].k`。后者的 `k` 由 `TRAINCACHE_WARMUP_K` 经 `traincache/up.sh` 的 heredoc 写入，取值 = 缓存容量 ÷ 块大小（实验固定 128）；对照跑分链路不读本字段 |
| `prefetch.gate.threshold`/`gap_min`/`slope_min`/`t_max_s`/`min_interval_s` | 对照链路由 `traincache/up.sh` 写死：`threshold=0.9`、`gapMin=0.1`、`slopeMin=0.001`、`tMax=5m`、`minInterval=1s`；COND 字段无消费者 |
| `prefetch.score.half_life`/`cutoff` | 对照链路由 `traincache/up.sh` 写死：`halfLife=4`、`cutoff=8`；COND 字段无消费者 |
| `prefetch.lookahead` | 环境变量 `TRAINCACHE_PREFETCH_LOOKAHEAD` |
| `cache.evict_policy` | 环境变量 `TRAINCACHE_EVICT_POLICY` |
| `cache.capacity_bytes` | Agent 淘汰侧容量上限；JuiceFS 用 `--cache-size` |
| `network.rate_mbit`/`delay_ms` | `make bench-tc RATE=<rate>mbit DELAY=<delay>ms`；burst 取 `config.burst_for_rate(rate_mbit)`（1250k/3750k/12500k）；抖动由 `config.jitter_enabled(capacity_ratio, rate_mbit)` 决定，COND 不带该字段 |
| `workload.*` | 训练 Job 参数与 OrderSource sidecar 参数 |
| `workload.shuffle`/`curriculum` | OrderSource 生成顺序时的模式与 stage 配置 |
| `sampling.reps` | runner 的重复次数；`reps=3` 的点不参与最终胜负结论 |
| `environment.images` | kind manifest 的 image digest；runner 写入 `lock.json` |

## 5. 校验规则

runner 在开跑前逐条校验，任一失败直接拒绝，不进入执行：

1. JSON 可解析，`schema_version="1"`，无未知字段，必填字段非空。
2. `name` 与文件名一致，且在 `conds/` 下唯一。
3. `dataset.bytes` 与 `shards × shard_size_bytes` 误差 ≤1%。
4. `cache.enabled=true` 时，`capacity_bytes` 与 `dataset.bytes / ratio` 误差 ≤1%。
5. `cache.enabled=false` 只允许 `group=direct`。
6. `group=traincache-cold`/`traincache-warm` 时 `prefetch` 必填；其他组态 `prefetch` 可选，但对照字段必须齐。
7. `workload.shuffle=curriculum` 时 `curriculum` 必填，且 `stages=3`、`shard_fractions` 与冻结值逐字一致。
8. `sampling.reps` 取值：`fig1`/`fig2`/`fig3` 与 `policy` 为 3；`pareto1`/`pareto2` 必须为 7；`compare-*` 按容量档位走（D32）——`capacity_ratio=S` 时 3、`M` 时 3、`L` 时 1（`exploratory` 标记见 2.8）。
9. `trace.sha256` 是 64 位小写 hex；`environment.images` 每项是 `repo@sha256:...`；两者为全零或 `aaaa…` 占位符时拒绝执行。
10. 同一 `figure` 内，除被扫的自变量外，其余字段必须与基准 COND 一致。runner 用 `tags` 里的 `axis=<name>` 标出本点扫的轴。`compare-d` 扫两个轴：基准带宽点用 `axis=group`，非基准带宽点（300/1000 mbit）用 `axis=rate`；`compare-b` 只扫组态，用 `axis=group`。
11. `cache.capacity_ratio` 只允许出现在 `figure=compare-d`/`compare-b`；出现时 `dataset.bytes`、`capacity_bytes`、`ratio` 必须满足 S↔2、M↔8、L↔32 的档位映射（±1%），且缓存 1 GiB、数据 2/8/32 GiB。
12. `dataset.chunk_size_bytes` 正式实验必须为 8388608；`evict_policy` 只能是 `heat`/`lru`/`lfu`/`external`（D9）。
13. `group` 只能是 7 个组态名之一（`direct`/`juicefs-cold`/`juicefs-warm`/`fluid-cold`/`fluid-warm`/`traincache-cold`/`traincache-warm`）；COND 写了 `capacity_ratio` 时，`group` 还必须落在该档位的参跑组集合内（`S` 七组态、`M` 三组、`L` 两组），越组直接拒绝（D30）。
14. `network.rate_mbit` 只能是 100/300/1000；`network.burst_k` 必须等于该 rate 的推导值（1250/3750/12500）。抖动不出现在 COND 里，由 `config.jitter_enabled(capacity_ratio, rate_mbit)` 从档位与 rate 推导（D31）。

## 6. 完整示例

`bench/experiments/conds/fig1-d-bw100-ratio2-fan2.json`：

```json
{
  "schema_version": "1",
  "name": "fig1-d-bw100-ratio2-fan2",
  "figure": "fig1",
  "group": "traincache-cold",
  "track": "D",
  "dataset": {
    "name": "track-d",
    "source": "local-nginx",
    "endpoint": "http://127.0.0.1:9100",
    "bytes": 2147483648,
    "shards": 16,
    "shard_size_bytes": 134217728,
    "chunk_size_bytes": 8388608,
    "seed": 20261015
  },
  "trace": {
    "file": "traces/track-d-16shard-per-epoch.json",
    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "seed": 20261015
  },
  "workload": {
    "epochs": 1,
    "batch": 32,
    "workers": 4,
    "steps_per_epoch": 400,
    "prefetch_factor": 2,
    "compute_ms": 200,
    "shuffle": "fixed"
  },
  "network": {
    "rate_mbit": 100,
    "delay_ms": 20,
    "burst_k": 1250
  },
  "cache": {
    "enabled": true,
    "capacity_bytes": 1073741824,
    "ratio": 2.0,
    "evict_policy": "heat",
    "root": "/var/cache/traincache"
  },
  "prefetch": {
    "lookahead": 8,
    "window_k": 8,
    "fanout": 2,
    "fanout_min": 1,
    "gate": {
      "threshold": 0.9,
      "gap_min": 0.1,
      "slope_min": 0.001,
      "t_max_s": 300,
      "min_interval_s": 1
    },
    "score": {
      "half_life": 4,
      "cutoff": 8
    }
  },
  "sampling": {
    "reps": 3,
    "coarse_reps": 3,
    "upgrade_rule": "sign-change",
    "bootstrap_iters": 10000
  },
  "environment": {
    "kind_workers": 4,
    "k8s_version": "v1.31.0",
    "images": {
      "agent": "traincache-agent@sha256:0000000000000000000000000000000000000000000000000000000000000000",
      "controller": "traincache-controller@sha256:0000000000000000000000000000000000000000000000000000000000000000",
      "training": "traincache-train@sha256:0000000000000000000000000000000000000000000000000000000000000000",
      "minio": "minio/minio@sha256:0000000000000000000000000000000000000000000000000000000000000000",
      "juicefs": "juicefs/juicefs@sha256:0000000000000000000000000000000000000000000000000000000000000000",
      "fluid": "fluidcloudnative/fluid@sha256:0000000000000000000000000000000000000000000000000000000000000000"
    },
    "lock_file": "locks/fig1-d-bw100-ratio2-fan2.lock.json"
  },
  "tags": ["fig1", "axis=fanout", "bw100", "ratio2", "fan2"],
  "notes": "示例值；实际 sha256 与镜像 digest 由 runner 在跑分前填充。"
}
```

示例里的 `sha256` 和镜像 digest 是占位符，runner 校验到占位符会拒绝执行。
