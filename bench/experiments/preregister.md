# TrainCache 旗舰实验预注册

> 版本：v1（冻结候选，2026-10-22）
> 对应需求：`docs/9.0 核心能力验收与测试.md`；执行计划：`docs/9.1 plan.md`
> 冻结规则：第一次正式跑分前提交本文件并记录 git commit 与 sha256；之后只允许追加“偏离说明”，不允许回头改假设、档位或胜负判据。

## 0. 一句话

这份文件先把“量什么、怎么量、什么算赢、什么时候算输”写死，再看数据。三张旗舰图哪怕大半区域是负的，只要按本文件的口径如实报告，就算验收通过。

## 1. 五组对照与共同口径

五组受试者吃同一份请求轨迹、同一个 tc 档位、同一份数据：

| 组名 | 数据路径 | 缓存层 | 预热/预取 |
|---|---|---|---|
| `direct` | 训练 Pod 直接 HTTP 读 MinIO | 无 | 无 |
| `juicefs-cold` | JuiceFS CE 挂载 MinIO | JuiceFS 单机缓存，空缓存启动 | 无 |
| `juicefs-warm` | 同上 | JuiceFS 单机缓存，先做等价预热 | 预热完成后开始计时 |
| `fluid` | Fluid + JuiceFSRuntime + DataLoad + 亲和 | Fluid 管理的缓存 | 官方完整流程 |
| `traincache` | Agent 预热块 + 训练 Pod hostPath 只读挂载 | TrainCache Agent 缓存 | 启动门控 + 全程预取 |

`juicefs-warm` 的预热耗时单列，不计入 makespan；预热搬的字节仍计入成本。其余四组没有“预热时间不计”的例外。

### 1.1 makespan 边界

五组共用同一个训练脚本。脚本在关键点写 `run.json`，五个边界字段如下：

| 字段 | 定义 | 采集方式 |
|---|---|---|
| `t_submit` | 训练 Job 创建时刻 | runner 在创建 Job 前取当前时间，写入 Job 环境变量 `TRAINCACHE_JOB_CREATED_AT`；五组同一写法 |
| `t_pod_ready` | 训练 Pod Ready 时刻 | K8s Pod condition `Ready=True` 的 `lastTransitionTime` |
| `t_first_step` | 第一个 step 开始时刻 | 训练循环拿到第一个 batch 后、执行 forward 前打点 |
| `t_last_step` | 最后一个 epoch 最后一个 step 结束时刻 | 最后一个 optimizer step 返回后打点 |
| `t_origin_first_byte` | 源站收到该 run 第一个数据请求的时刻 | MinIO/nginx 访问日志；用于诊断，不作为 makespan 主口径 |

两个 makespan 定义：

- **启动 makespan** = `t_first_step - t_submit`。它包含调度、镜像拉取、Pod 启动、数据等待，直到第一步真正开始。
- **全程 makespan** = `t_last_step - t_submit`。它包含启动阶段；另外单列 `t_last_step - t_pod_ready` 作为“落地后阶段”，用于拆分启动收益与全程收益。

`t_first_step` 必须在 batch 就绪之后打点。如果在等 batch 之前打，数据 stall 会被漏掉，指标失真。`metadata.creationTimestamp` 只作交叉核对，不作为 `t_submit` 主口径。

### 1.2 stall 口径

每一步记录两个时间：

- `wait_s`：从本步开始等待 batch，到 batch 就绪的时长。
- `step_s`：batch 就绪到本步计算结束的时长。

`stall = wait_s / step_s`。聚合报 `mean` 与 `p90`，中位数只用于判断“是否处于 stall 形态”，不作为主要比较指标。epoch 0 前 10 步标记为 `warmup`，不进入 stall 聚合，但仍进入 makespan。

### 1.3 阶段标签

每步带一个 `phase` 标签，取值固定为 `startup`、`steady`、`switch`：

| 阶段 | 起点 | 终点 |
|---|---|---|
| `startup` | `t_submit` | `t_first_step` |
| `steady` | `t_first_step` | 第一次切换事件 |
| `switch` | 切换事件时刻 | 新窗口 K 块全部就绪的时刻；若未就绪，截到该 epoch 结束 |

每个 epoch 边界都算一次切换事件，三种 shuffle 策略都一样。curriculum 的 stage 边界额外算切换事件。`switch` 优先于 `steady`；两个窗口同时活跃时按 `switch` 记。`traincache` 启动窗口以 WarmupTask 的就绪状态（`startupWindowReady`/`gateDecision`）为准；epoch/stage 切换的训练侧 phase 用可观测代理：**新窗口 K 块对应的分片全部首次被训练侧打开的步**。代理晚于 Agent 的真实就绪时刻，会把少量 steady 预取归到 `switch`，报告里单列该口径差异；Agent 侧 `phase=switch` 的命中率仍由 `ready_bitmap` 事件驱动。

### 1.4 命中率口径

命中率的统一定义：**该块打开时没有触达源站，就算命中**。

- 训练脚本为每次块打开记录 `path`、`chunk`、`phase`、`source=cas|origin|unknown`。
- 源站侧（MinIO 访问日志或 nginx access log）记录每个块键的请求次数，用于交叉核对。
- `traincache` 组同时抓 `traincache_cache_hit_ratio{dataset,phase}`，与训练侧统计对账；两者差异超过 5% 时，以“训练侧打开事件 + 源站日志”为准，并在报告里说明。
- `direct` 组没有缓存层，命中率按 0% 记，并标注“无缓存层”；`juicefs-*` 与 `fluid` 用同样的“打开事件 + 源站日志”算法，不依赖各自厂商指标的语义。

### 1.5 三笔字节成本

三笔全部计入收益分母，不允许只报 makespan 不报成本：

| 账目 | 范围 | 统计方法 |
|---|---|---|
| 启动预取 | `t_submit` 到门控放行（`traincache`）或预热结束（`juicefs-warm`） | 对每个 Agent `instance` 的 `traincache_fetch_bytes_total` 求区间增量并求和；fanout 的重复下载已经体现在多副本计数器里，不再二次乘 fanout。另用 WarmupTask 的 `startupWindowReady × chunkSize` 给出独立估算，差异 >5% 时以计数器为准并说明 |
| 持续预取 | 门控放行/训练开始到训练结束 | 按 `phase=steady` 的时间窗对 `traincache_fetch_bytes_total` 求增量 |
| 阶段切换重预取 | 每个 `switch` 窗口 | 按 `phase=switch` 的时间窗求增量 |

`traincache_fetch_bytes_total` 现有标签是 `{dataset,result,reason}`，没有 `phase`。阶段拆分不新增标签，统一用 `run.json` 里的时间边界对 Prometheus 计数器做区间差分；差分边界与 15 秒抓取周期的误差单独报告。源站侧总出站字节作为交叉校验，两者差异超过 5% 时以源站为准。

`fanout` 的重复字节不隐藏：fanout=3 时启动预取字节按 3 倍计入，不做“有效字节”折算。

### 1.6 胜负判据

每组每个点报中位数与 bootstrap 95% 置信区间（10,000 次重采样）。比较两个组时，对“中位数差值”做同样的 bootstrap：

| 结论 | 条件（以 makespan 越低越好为例） |
|---|---|
| 赢 | 中位数改善 ≥ 5%，且差值 CI 上界 < 0 |
| 输 | 中位数恶化 ≥ 5%，且差值 CI 下界 > 0 |
| 相近 | 差值 CI 包含 0 |
| 可忽略 | CI 不包含 0，但中位数差异 < 5% |

命中率越高越好，方向反过来。图三的“回落时间”用 20% 作为赢/输阈值，其余规则相同。

最终报告里任何“赢/输”必须跑满该点规定的次数：曲面与淘汰矩阵每点 3 次，两张 Pareto 每点 7 次（D18）；五组对照按 1.9 的 S/M/L 分档是 3/3/1 次（D19 的次数口径由 D32 按带宽分区接管；D19 起，五组对照不再适用“7 次”要求）。

### 1.7 冷态与作废

每个 run 前做完整冷态：

- `traincache`：清空 Agent 缓存根目录，重启 Agent，drop page cache，重建 tc。
- `juicefs-*`：清空 JuiceFS 缓存目录，重启挂载客户端，drop page cache，重建 tc。
- `fluid`：删除 DataLoad 与 Runtime Pod，等待缓存目录清空；如果缓存目录无法可靠清空，该组标记 `cold-risk` 并在报告里说明，不能假装是冷态。
- `direct`：drop page cache，重建 tc，无需清缓存层。

单轮装置自检（`bench-verify` 或 `track-d-verify`）失败，整轮作废并重跑；不允许挑掉坏点，也不允许把失败轮换成“异常值”删掉。作废轮保留原始产物，标记 `void`。

### 1.8 环境与轨迹

- kind 集群：1 个 control-plane + 4 个 worker，共 5 个节点。对照组与主实验先同集群，资源吃紧再拆，拆组只允许整体迁移，不允许只迁一部分。
- 请求轨迹：同一 track+档位的七组态共用一份轨迹文件——S/D 用 `traces/track-compare-d-s8-per-epoch.json`，M/B 用 `traces/track-compare-b-m32-per-epoch.json`，L/B 用 `traces/track-compare-b-l128-per-epoch.json`，C 用 `traces/track-c-32shard-per-epoch.json`；记录分片顺序、seed、每步样本数，文件 hash 写进每个 COND 与产物目录。
- tc：速率按 COND，延迟统一 20 ms；`bench` 现有 300mbit/10ms 的扫点是另一套口径，不并入本预注册。
- 数据源：图一、图三与 D 轨五组对照用本地 nginx（`track_d`，2 GiB，16 个 tar）；图二与 B 轨对照用 MinIO 上的 B 轨子集（8 或 32 个 tar，2 GiB 或 8 GiB），同一源站便于只变数据集大小。
- 训练负载：真实 PyTorch + webdataset，CPU 训练循环，依赖由项目根 `.venv`（Python 3.14）提供——`bench/requirements.txt` 锁 numpy/pandas/matplotlib/boto3/webdataset，torch 由用户级安装或训练镜像经 `.pth` 暴露，图二/图三与 M5 的 C 轨两 Job 都直接跑它；合成 `bench_track_d.py` 只用于装置自检与粗扫，不进入正式五组对照的结论。对照层 M7 不受影响：`run_bench.py` 与它注入 train-job 的脚本只用标准库，复刻 D 轨负载形态（400 步 × 200ms + 分段读），不依赖上述训练脚本本体。
- 对照组不可用预案：M6 先做仅验可行性的 Fluid 冒烟（控制面与 DataLoad/Runtime 全 Ready、D 轨 400 步经 FUSE 跑通、组件额外占用 ≤6 CPU / 6 GiB）。两次失败且归因环境时记 `fluid-unavailable-on-kind`，图二/图三主基线降级为 `juicefs-warm`，Fluid 行标 `unavailable`，不补跑、不另起集群；manifest 自身错误修好后重试不计次数。可行性冒烟不产生性能数字，不属于“看到结果再调参”。

### 1.9 容量比三档 S/M/L 与预注册假设（七组态对照，D19/D30/D31/D32）

真实训练里数据集通常是节点缓存的几十倍以上，只在一个容量比下跑分，结论无法外推。七组态对照按数据/缓存容量比分三档，缓存三档固定同一个 1 GiB：

| 档 | 数据 | 缓存 | 容量比 | 参跑组 | 带宽 | 每点次数 | 定位 |
|---|---|---|---|---|---|---|---|
| S | 2 GiB | 1 GiB | 1:2 | 全部七组态 | @100 与 @1000（另有 @300 临界档三组） | 3（@100 与 @1000 各 3 次） | 正式结果，现状口径 |
| M | 8 GiB | 1 GiB | 1:8 | `direct`、`fluid-cold`、`traincache-cold` | @1000 | 3 | 正式结果，欠配校验 |
| L | 32 GiB | 1 GiB | 1:32 | `traincache-cold`、`fluid-cold` | @1000 | 1 | 探点，产物标 `exploratory` |

口径说明：

- COND 的 `group` 用七值词表，与跑分器的组态名一一对应：`direct`、`juicefs-cold`、`juicefs-warm`、`fluid-cold`、`fluid-warm`、`traincache-cold`、`traincache-warm`。冷热由组态名表达，不再靠「M/L 档的 `traincache` 就是冷启动」这类隐含约定。
- M 档与 L 档的基线用 `fluid-cold`（D30）：欠配校验必须压在最强工业基线上，只比 JuiceFS CE 会被质疑「绕过 Fluid」。`juicefs-cold`/`juicefs-warm` 仍留在 S 档七组态里。Fluid 若触发 Q6 的两次环境性失败，M/L 该组落回 `juicefs-cold` 补跑，产物记 `baseline_substituted`。
- warm 组（`juicefs-warm`、Fluid 热读）只出现在 S 档：1:8 及以下预热至多覆盖数据的 1/8，热读相对冷读的优势缩小是算术事实，不是新发现，不陪跑 M/L。
- L 档每点 1 次，低于 9.0 的“至少 3 次”硬纪律。因此 L 档标 `exploratory`（探点：只报单点值、不报 CI、不进正式表，只用来回答“M 的方向是否延续”），不支持任何正式结论。CI 指 1.6 的 bootstrap 95% 置信区间。
- 除容量档与带宽档外三档共用其余口径：同一轨迹、fanout=2、淘汰 `heat`、D16 训练负载、固定 delay 20 ms。
- **带宽是第二个自变量**（D31）：tc rate 取 {100, 300, 1000} mbit，随 COND 落盘。burst 按「约 0.1 秒的传输量」推导（1250k / 3750k / 12500k），不跨档复用固定值；只有「链路带宽 ÷ 每步读量」落在 [1,2] 的临界档启用 netem 抖动（`delay 20ms 10ms distribution normal`），其余档保持固定 delay。实测比值：S 档 0.47 / 1.40 / 4.66，M 档 0.12 / 0.35 / 1.16，L 档 0.03 / 0.09 / 0.29——临界档只有 S@300 与 M@1000 两处。
- 每点次数按带宽分区（D32）：S 档 @100 与 @1000 各 3 次；M 档 @1000 3 次；L 档 @1000 1 次。3 次必须报均值 + bootstrap CI，只报中位数不满足 9.0 的硬纪律。
- 块大小口径：正式实验一律用 8 MiB 块（8388608 字节）的 `compare-data-8m` 快照，生产默认 64 MiB 不变，旧 `compare-data` 快照只留作历史产物复现。块是缓存与预取的最小单位；1 GiB 缓存按 64 MiB 只有 16 块、按 8 MiB 有 128 块，后者淘汰排序统计才够用。
- L 档 32 GiB 数据已生成（128 个 256 MiB 分片，2026-10-24 追加生成约 24 GiB，只追加不覆盖 `bench-b` 源数据）；实际每份约 252.6 MiB，实际数据量约 31 GiB，比声明值小约 3%，容量比相应约为 1:31。三档偏差同向、不影响组间可比性。
- 跑分第一格先做前置探针：无 tc 状态下测 MinIO 实际吞吐上限。上限 < 125 MB/s 时 @1000 档失效，带宽轴缩为 {100, 上限} 两档，并回写 D31。**已实测（2026-10-25，§8.4）：C ≈ 674 MB/s，@1000 可达，三档独立，本条不触发。**

自变量：容量档 S(1:2) → M(1:8) → L(1:32)。假设按“自变量 → 预期正负区间 → 反例长什么样”写死：

| # | 假设：自变量 → 预期正负区间 | 反例长什么样 |
|---|---|---|
| HC.1 | 容量比越大，各组命中率越低、回源占比越高。预期：S→M 时三个冷组（`direct` 恒 0 除外）命中率单调下降、回源字节占比单调上升；即 M 档命中率 < S 档且回源占比 > S 档，`traincache` 与 `juicefs-cold` 两组都成立 | 命中率 S→M 不降甚至反升，或回源占比不升——先查产物与口径是否串档；确认后如实写“命中率与回源占比未随容量比单调变化” |
| HC.2 | 容量比越大，TrainCache 相对基线的收益应增大。预期：`traincache-cold` 相对 `direct`、相对 `fluid-cold` 的启动与全程 makespan 改善幅度，从 S 档的 X% 增大到 M 档的 Y%，要求 Y > X（S、M 两档数字都要在表里列出） | 两种反例：① S→M 收益不增反降（Y ≤ X）；② M 档 `traincache-cold` 与 `fluid-cold` 的 makespan 中位数差异落在差值 CI 内（CI 含 0，两组分不出快慢）。出现任一情形就如实写“容量比假设不成立”，不改档位 |
| HC.4 | 带宽越紧，TrainCache 相对基线的收益越大。预期：同一容量档内，链路受限区（@100，BW/r≈0.47）的 `traincache-cold` 启动与全程改善幅度大于预取有效区（@1000，BW/r≈4.66），即 X@100 > X@1000 | 反例：@1000 的改善幅度不小于 @100。此时如实写“收益与带宽无单调关系”；先查两档的 trace、chunk 与预热是否同口径，再下结论 |
| HC.5 | 抖动只影响方差、不影响均值。预期：临界档（S@300、M@1000）的 stall 占比与 makespan 均值与固定 delay 档同量级，但置信区间更宽 | 反例：临界档的均值明显偏离固定 delay 档（超出其置信区间）。此时如实写“临界档存在均值漂移”，并检查抖动参数是否真的只加在 netem 上 |
| HC.3 | M 的方向延续到 L。预期：L 档单点上 `traincache-cold` 不慢于 `fluid-cold`，且相对 S/M 的收益方向同号。探点不做 CI 判断 | L 档差异反号或接近 0。单点分不出噪声和趋势，结论只写“L 档方向不延续/无法判定”，不允许用来推翻或确认 HC.2 |

判据：HC.2 在 S→M 必须有明确的“证实/证伪”表述，写进报告表；缺组或 CI 跨 0 也照写。加档位、改次数只能作为下一轮新预注册，不动本节。

## 2. 图一：启动阶段盈亏曲面

### 2.1 自变量与档位

| 自变量 | 档位 | 说明 |
|---|---|---|
| 源站带宽 | 100 / 300 / 1000 mbit | 延迟固定 20 ms，burst 1600k |
| 数据集/缓存容量比 | 1 / 2 / 4 | 缓存容量 = 数据集字节 ÷ 比值；2 即现有 50% 欠配 |
| fanout | 1 / 2 / 3 | 每 Pod 预热节点数上限；受 4 个 worker 约束 |

共 27 个点。固定参数：D 轨 2 GiB（16 tar）、1 个 epoch、400 步/epoch、batch 32、workers 4、`K=8`、`threshold=0.9`、`gapMin=0.1`、`slopeMin=0.001`、`TMax=5m`、`halfLife=4`、`cutoff=8`、lookahead 8、淘汰策略 `heat`、固定 seed 顺序。

对照组“自由调度”= 同一个 Job 去掉 SchedulingGate，不做启动门控；预热与服务端逻辑保留，用于区分“门控的收益”和“预热的收益”。

### 2.2 预注册假设

| # | 假设 | 预期 |
|---|---|---|
| H1.1 | 低带宽、缓存装不下时，门控等待换来更短启动 makespan | 100 mbit、比值 ≥2、fanout 2/3 时，门控组启动 makespan 比自由调度改善 ≥10% |
| H1.2 | 带宽足够高时，门控的等待没有价值 | 1000 mbit、所有比值与 fanout 下，两组差异在 ±5% 内（相近） |
| H1.3 | fanout 的收益随带宽下降而变小 | 100 mbit 下 fanout=3 不比 fanout=1 更好，可能更差；300/1000 mbit 下 fanout=2 通常优于 1 |
| H1.4 | 存在明确的负收益区 | 1000 mbit、比值=1、fanout=3 附近，门控组启动 makespan 比自由调度差 ≥5% |

### 2.3 阴性结论的接受条件

如果正收益区为空，如实写“在本次档位范围内，启动门控从未在启动 makespan 上赢过自由调度”，并保留三张图的负结果。不得为了做出正区间临时加档位、改阈值或改 K；档位变更只能作为下一轮新预注册。

### 2.4 采样

27 个点每点 3 次，不做符号翻转补测；判据按 1.6 只看符号，不看效应大小。

## 3. 图二：全程 makespan 盈亏曲面

### 3.1 自变量与档位

| 自变量 | 档位 | 说明 |
|---|---|---|
| 源站带宽 | 100 / 300 / 1000 mbit | 延迟固定 20 ms |
| 数据集大小 | 2 GiB / 8 GiB | B 轨 8 或 32 个 tar，同一 MinIO 源 |
| epoch 数 | 1 / 3 / 10 | 每 epoch 400 步，batch 32 |

共 18 个点。固定参数：fanout=2、缓存容量=数据集 50%、`K=8`、门控参数同图一、lookahead 8、淘汰策略 `heat`、每 epoch 重排（`per-epoch`）。

基线是 `fluid` 全流程；`direct`、`juicefs-cold`、`juicefs-warm` 作为次级参考同表报告。纵轴是 TrainCache 全程 makespan 相对 Fluid 的中位数差值（负数 = 更快）。

### 3.2 预注册假设

| # | 假设 | 预期 |
|---|---|---|
| H2.1 | 多 epoch 场景 TrainCache 赢 Fluid | epoch ≥3 时，至少 2 个带宽档的全程 makespan 改善 ≥5%，且 CI 上界 < 0 |
| H2.2 | 单 epoch + 高带宽场景两者相近 | epoch=1、1000 mbit 时，差异在 ±5% 内 |
| H2.3 | 收益随 epoch 数与数据集大小增长 | 最大收益出现在 100 mbit、8 GiB、10 epoch；收益排序为 10 epoch > 3 epoch > 1 epoch |
| H2.4 | 启动收益不能替代全程收益 | 若启动 makespan 赢但全程 makespan 不赢，必须如实分列，不能把启动收益写进全程结论 |

### 3.3 阴性结论的接受条件

如果 TrainCache 只在启动阶段赢、全程与 Fluid 相近，报告写“全程收益未成立，收益主要来自启动阶段”。如果 10 epoch 也不赢，报告写“在本次档位内持续预取没有转化为全程 makespan 收益”，并给出字节成本与命中率曲线解释原因。

### 3.4 采样

18 个点每点 3 次，不做符号翻转补测。Fluid 基线在同一 COND 下跑同样的次数；不允许用历史 Fluid 数字替代当轮基线。

## 4. 图三：阶段切换 stall 回落曲线

### 4.1 自变量与档位

| 自变量 | 档位 | 说明 |
|---|---|---|
| epoch 数 | 1 / 3 / 10 | 决定切换次数 |
| shuffle 策略 | 固定 seed / 每 epoch 重排 / curriculum | 三种顺序形态 |

共 9 个点。固定参数：D 轨 2 GiB、100 mbit/20 ms、fanout=2、缓存 50%、`K=8`、门控参数同图一、lookahead 8、淘汰策略 `heat`。

三种 shuffle 的定义：

- `fixed`：所有 epoch 用同一个 seed，消费顺序逐 epoch 相同。
- `per-epoch`：seed+epoch，每 epoch 重新做分片置换与样本 shuffle。
- `curriculum`：3 个 stage，分片集合依次为全量、2/3、1/3。全局置换 `P = Random(seed).permutation(n_tars)`；stage 0/1/2 分别使用嵌套前缀 `P[:n]`、`P[:ceil(2n/3)]`、`P[:ceil(n/3)]`；stage 内顺序 = `P` 过滤到该前缀，stage 内所有 epoch 完全一致。stage 归属：`E<3` 时为 stage 0；否则 `e < floor(E/3)` 为 0、`e < floor(2E/3)` 为 1、其余为 2。每个 epoch 边界都是切换事件；stage 边界与 epoch 边界重合时只记一次，事件标 `kind=epoch+stage`。1 个 epoch 时 curriculum 退化为单个 stage（全量），仅用于保持矩阵完整，不参与“curriculum 优于固定 seed”的结论。

### 4.2 回落时间的定义

每个切换事件算一次回落时间：

1. 基线 = 切换前 20 步的 stall 均值。
2. 阈值 = `max(1.1 × 基线, 基线 + 5 个百分点)`。基线接近 0 时用加法项，避免阈值退化为 0。
3. 从切换后第 1 步开始，取 10 步滚动 stall 均值；第一次满足“滚动均值 ≤ 阈值，且之后连续 10 步都 ≤ 阈值”的步，记为回落点。
4. 到该 epoch 结束仍未满足，记右删失（right-censored），回落时间按 epoch 结束计，并单独报删失率。

纵轴报每个点的回落时间中位数与 95% CI；同时报删失率，避免用“没恢复”伪装成“恢复得快”。

### 4.3 预注册假设

| # | 假设 | 预期 |
|---|---|---|
| H3.1 | 每 epoch 重排时 TrainCache 回落明显快于 Fluid | epoch ≥3 时，回落时间中位数比 Fluid 低 ≥20%，且 CI 上界 < 0 |
| H3.2 | 固定 seed 时两者相近 | 固定 seed 下差异在 ±5% 内；切换本身不是瓶颈 |
| H3.3 | curriculum 比固定 seed 慢、比 Fluid 快 | curriculum 的回落时间介于两者之间；stage 越窄，切换瞬间 stall 峰值越高 |
| H3.4 | 切换成本随 epoch 数增加 | per-epoch 的回落时间随 epoch 数增加而增加；fixed 不明显增加 |

### 4.4 阴性结论的接受条件

如果每 epoch 重排下 TrainCache 没有更快回落，报告写“阶段切换不是本装置的瓶颈，或预取没赶上切换后的新窗口”，并给出新窗口 K 块就绪时刻与 ETA 的对照。允许这个阴性结论，不修改图三的档位。

### 4.5 采样

9 个点每点 3 次，不做符号翻转补测。

## 5. 次旗舰 Pareto

### 5.1 Pareto 一：启动收益 vs 重复预热字节

- 自变量：fanout 1/2/3。
- 固定：100 mbit/20 ms、D 轨 2 GiB、比值=2、1 epoch、其余参数同图一。
- 横轴：启动预取总字节（含 fanout 重复）；纵轴：启动 makespan 相对自由调度的改善。
- 假设：fanout=2 是拐点；fanout=3 的字节成本约为 3 倍，但启动收益不再同比例增加。
- 采样：3 个点各 7 次。

### 5.2 Pareto 二：全程收益 vs 持续预取总字节

- 自变量：lookahead 8/16/32 × 淘汰策略 `heat`/`lru`/`lfu`，3 策略 × 3 档 lookahead = 9 点（`docs/9.0` 为准，D28）。W-TinyLFU 已撤档（D9），不实现、不参赛。
- 固定：C 轨倾斜场景（Job A 循环读 10% 热分片，Job B 顺序扫全量，两 Job 并发）、缓存容量 30%（显式上限 `floor(dataset.bytes × 0.30)`，不是按 `disk_usage_ratio` 事后调）、100 mbit/20 ms、3 epoch。
- 横轴：持续推进预取总字节；纵轴：全程 makespan 相对 Fluid 的改善。另附 A 的命中率、A 的等待占比、B 挤占热数据次数。
- 假设：收益随 lookahead 上升但在某档后转平，拐点预期落在 16 附近（16 块 × 8 MiB = 128 MiB，占 1 GiB 缓存的 1/8；32 档时窗口占到 1/4，欠配压力明显加大）；顺序单 Job 下三档淘汰策略差异不显著；倾斜场景下三档才有可见差异（存在一对策略，A 命中率中位数差超出 CI）。允许阴性结论：顺序场景三档无显著差异，就如实写“排序策略对单 Job 顺序训练不关键”；倾斜场景三档差异也落在 CI 内，同样如实写。
- 采样：9 个点各 7 次；共享同一组基线时必须在产物里写明共享关系。

## 6. 允许的阴性结论

以下结论都可以写进报告，不需要为了“做出正结果”改档位：

- 启动门控在本次档位范围内从未赢过自由调度。
- TrainCache 只在启动阶段赢，全程 makespan 与 Fluid 相近。
- per-epoch 重排下阶段切换不是瓶颈，回落时间与 Fluid 相近。
- 顺序单 Job 下三档淘汰策略（`heat`/`lru`/`lfu`）没有显著差异，排序策略对单 Job 顺序训练不关键，真正关键的是预取调度与跨租户保护。
- fanout=3 的重复字节成本超过它带来的启动收益。

## 7. 偏离记录

正式开跑后，任何与本文件不一致的操作都必须追加到这里，写清日期、原因、影响的 COND 与 run ID。不允许删改上面的假设、档位和判据。

| 日期 | 偏离内容 | 原因 | 影响范围 |
|---|---|---|---|
| 2026-10-25 | 冻结前修正：COND `environment.images` 九键的值由 docker image ID 换成集群 containerd manifest digest；`fluid`/`fluid-csi` 的 tag 由 `v1.1.0-36f0467` 改成在役的 `v1.0.8-6533239` | 原值不是任何一侧可解析的 digest 形式，且 tag 与集群实际部署不符（见 §8.3）。属于记录错误修正，不改假设 | 34 个 COND 的环境字段；性能口径无影响 |
| 2026-10-25 | 冻结前修正：§9 机时估算 12~14 小时 → D32 的 7.2 小时；1.6 的五组次数口径 5/3/1 → 3/3/1（符号翻转格补 5） | D32 已裁决并被 1.9 表格接管，这两处是 D32 之前的正文残留 | 机时预算表；不影响任何假设与判据 |
| 2026-10-25 | 前置探针出数（9.1 §12.2）：C ≈ 674 MB/s，D31 的 `ceiling` 塌缩分支不触发；§12 L 档单 run 13 分钟 → ≈6 分钟 | 实测推翻「MinIO 上限约 45 MB/s」的保守前提 | 机时表 L 档行；带宽档 {100,300,1000} 全部保留 |
| 2026-10-25 | 补齐 MinIO 桶 `bench-b` 的分片（32 → 128 个对象，约 24 GiB），并把 `bench/scripts/upload_bench.py` 改成只追加不覆盖 | 桶里只有 32 个对象，L 档 `fluid-cold` 的 import Job 同步不到 32 GiB；同时该脚本原本对全部对象无条件 `upload_file`，重跑即覆盖源数据，与 Q12 冲突 | 数据面补齐；不改任何假设、档位与判据 |

## 8. 冻结记录

冻结锚点是轻量 tag `prereg-c1`。一次闭合：`preregister.md`、`cond.schema.md`、`conds/*.json`（34 个）、`traces/*.json`（4 个）在该 tag 指向的提交定稿。本文件的 sha256 不单独记录——记录自身哈希会改变文件内容，自引用无法闭合。

| 项 | 值 |
|---|---|
| 冻结日期 | 2026-10-25 |
| 冻结锚点 | tag `prereg-c1` |
| tag 指向 commit | `9d1a33c2d64dd04fe9318e4fb4a6b721895362eb` |
| 本文件 sha256 | 以 tag `prereg-c1` 中的版本为准，不单独记录 |
| `cond.schema.md` sha256（tag 版本） | `17e9b56bfa6d25bef8f8d2d35f2cd57fa9e2f9e3161de8ed3d58327e7c7826ac` |
| COND 集（34 个）合并 sha256 | `3f1927a4860923b4f91b6f4136fe0f294b955e6bf54d8bdbd0a9fcc614aae1d4` |
| `environment.images` 取值口径 | 见 §8.3；九个键逐个是集群 containerd 的 manifest digest，非人工编造 |
| COND 契约版本 | v1（`bench/experiments/cond.schema.md`） |

三个哈希都以 tag 为基准复算：

- 本文件：`git show prereg-c1:bench/experiments/preregister.md | sha256sum`
- `cond.schema.md`：`git show prereg-c1:bench/experiments/cond.schema.md | sha256sum`
- COND 集（34 个）：用 `git ls-tree -r --name-only prereg-c1 bench/experiments/conds/` 取路径并按路径排序；对每个文件跑 `git show prereg-c1:<path> | sha256sum` 得到 `<hex>`；把「`<hex>  <相对仓库根的路径>`」逐行拼接（两个空格分隔，行尾换行）；对拼接后的文本再算一次 sha256。

### 8.1 冻结清单（第一次正式跑分前，按顺序执行）

1. 确认 `bench/experiments/preregister.md`、`bench/experiments/cond.schema.md`、`bench/experiments/conds/*.json`（34 个）、`bench/experiments/traces/*.json`（4 个）内容为终稿。
2. 只提交上述冻结对象，打 tag `prereg-c1`，记下 tag 指向的 commit hash，填入上表。
3. 以 tag 为基准算 `cond.schema.md` 与 COND 集的哈希，填入上表。
4. 从集群取四个控制面镜像的 digest（agent、controller、training、minio；JuiceFS/Fluid 参跑组再补对应两项），填进上表，并替换各 COND 里的 `environment.images` 占位符。
5. 回填 `trace.sha256`：对 `bench/experiments/traces/` 实际使用的轨迹文件算 sha256，写进每个 COND 与产物目录。
6. 此后内容冻结：只允许往 §7 追加偏离说明（D12），不允许回头改假设、档位或胜负判据。

**执行状态（2026-10-25）**：1~3 是冻结动作本身，随提交与 tag `prereg-c1` 成立；哈希以 tag 为基准计算，回填本节不改变 tag。4 已成立，见 §8.3（九键全填，且纠正了两处错值）。5 已成立——34 个 COND 的 `trace.sha256` 与 `bench/experiments/traces/*.json` 的 `per_epoch` 稳定哈希逐一对上（口径是 `bench/compare/trace.py` 的 `trace_sha256()`，即对 `per_epoch` 数组做 `json.dumps(separators=(",",":"), sort_keys=True)` 后再 sha256；直接对轨迹整个文件算 sha256 得到的是另一个值，别拿错）。6 自 tag `prereg-c1` 起生效。

### 8.3 镜像 digest 取值口径（冻结时现取）

| COND 键 | 在役 repo:tag | containerd manifest digest |
|---|---|---|
| `agent` | `dgl3i4ucowsgvvdwty.xuanyuan.run/traincache/traincache-agentd:0.8.3` | `sha256:7c69317ad2c4a015e767b320e94d3856561ec3aa4073b5bd0905c189300ac9ea` |
| `controller` | `…/traincache/controller-manager:0.8.0` | `sha256:e0fd17cedaeadfc9c93ac190067c43dc01f9d7f3b8d4662e8131bf148f39f4b2` |
| `webhook` | `…/traincache/webhook:0.8.0` | `sha256:7bbf073986ee65bbbd9b7f3f4b2ba7947fcd4ee0fad33c74805d87de10748925` |
| `scheduler` | `…/traincache/kube-scheduler:0.8.0` | `sha256:6d3b06ebc2709d7751d54bf471d9cecbe11e15205b5fa315108cce7eb4bc6cba` |
| `training` | `python:3.12-alpine` | `sha256:d892f6cb770c805ca76e681cc356e88a535f0fcfe1ab6984ea7446f75dab783f` |
| `minio` | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | `sha256:2cde0f1131c931e16cb0b00c1c86353c80833dbc85630b97d3317ac8644c6fac` |
| `juicefs` | `juicedata/mount:ce-v1.3.0` | `sha256:81bbde1cd3e0fe6fbd393840fb7f4cfe321d259c78d01e7a975096c22567cb7f` |
| `fluid` | `fluidcloudnative/fluid-webhook:v1.0.8-6533239` | `sha256:11a24597a81d2e183ba6c05ec4cad93715eed838510d2a5d3313aa4dd07f67f4` |
| `fluid-csi` | `fluidcloudnative/fluid-csi:v1.0.8-6533239` | `sha256:df6c4d8c6f757f071d9444b91aa5763d9d754f3ac95f51339c04763cc5b1c875` |

取法：`docker exec <kind 节点> ctr -n k8s.io images ls`，取第二列 ref 与第三列 Digest（三个节点 `traincache-worker` / `worker2` / `control-plane` 上同名镜像的 Digest 完全一致，已交叉核过）。

九键的两处取值口径：

1. 表里填的是 containerd 的 manifest digest，不是 **docker image ID**（`docker images` 的 config digest，如 agent 的 `13f1ed…`）。`@sha256:` 在镜像引用里的语义是 manifest digest，两者不是同一个值。
2. `fluid` / `fluid-csi` 取 `v1.0.8-6533239`，这是 `fluid-system` 里实际在跑的版本（`kubectl -n fluid-system get pods -o jsonpath=…image`）。节点上另存的 v1.1.0 镜像未使用。

注意 kubelet 报的 `imageID` 是第三种值（形如 `docker.io/library/import-2026-10-24@sha256:f037d4…`），那是 kind `load docker-archive` 给导入镜像打的标记，不是内容摘要，别拿它当 digest。

同一个镜像名在 docker 侧还能问出第四种值——`RepoDigests`（如 `python@sha256:1b668…`、`…/pgsty/minio@sha256:b6bfe…`），那是上游 registry 的清单摘要，与 containerd 本地存的摘要**本就不同**（本地 build 的四个 traincache 镜像则根本没有 RepoDigests）。本轮选 containerd 那一侧，理由是它是三节点一致、且正是集群里在跑的字节。`lock.py` 因此把两路分开记：`docker_images` 取 COND 冻结值，`docker_repo_digests` 只做交叉核对，谁也不覆盖谁。顺带修掉一个真 bug：旧 `lock.py` 拿去掉 tag 的 repo 名去 `docker inspect`，`python:3.12-alpine` 会命中宿主上任意一个 `python` 镜像的 RepoDigest——记下来的是别的镜像。

### 8.4 前置探针实测（9.1 §12.2）

无 tc 状态下 MinIO 源站吞吐上限 **C ≈ 674 MB/s（median of 9，区间 629~755；≈5.4 Gbit/s）**，采法：与 train-job 同路径（worker 节点上临时 Pod 经 `nodeIP:30090` nodePort），`mc cat bench-b/shard-xxxxx.tar | wc -c` 计时。并行 2 连接聚合 1120/870 MB/s、并行 3 聚合 1207 MB/s——上限随连接数增长，说明 674 MB/s 是单连接读者上限而非源站出口上限。

结论按 D31 的判据落定：**C ≫ 125 MB/s，@1000mbit 档可达，{100, 300, 1000} 三档互相独立，无格子塌缩成 `ceiling`，BW/r 分区与抖动格（S@300、M@1000）无需按 C 重算**。§12 机时表里 L 档「按 MinIO 上限约 45 MB/s 取保守值 13 分钟」的前提被推翻，L 档单 run 应按链路 125 MB/s 取 ≈6 分钟（32 GiB ÷ 125 MB/s ≈ 4.4 分钟 + 冷态与采集）。

探针用过的临时 Pod 已删除，MinIO Pod eth0 的 tc 状态前后一致（`qdisc noqueue 0: root refcnt 2`，即无调控）。

### 8.2 冻结时刻仍未定的取值

| # | 事项 | 状态 |
|---|---|---|
| 1 | `compare-data-8m` 快照 | 尚未发布到 MinIO；首次 traincache 组 up 时由 `bench/compare/traincache/up.sh` 的 `publish_snapshot()` 幂等现场发布（S/M/L 档分别 8/32/128 个 shard） |

## 9. 机时粗估

单点时长按现有实测代入：D 轨 100 mbit 下 1 epoch 约 5.1 分钟、3 epoch 约 15 分钟、10 epoch 约 51 分钟；300 mbit 下 1 epoch 约 1.4 分钟；B 轨 8 GiB 冷读一轮约 710 秒（约 12 分钟）。冷态与采集每次另计 1~3 分钟。

| 实验 | 点数 | 每点次数 | 粗估 |
|---|---|---|---|
| 图一 | 27 | 3 | 6~12 小时 |
| 图二 | 18 | 3 | 30~50 小时 |
| 图三 | 9 | 3 | 12~20 小时 |
| 五组对照 S 档 | 7 | 3 | 约 4.2 小时 |
| 五组对照 M 档 | 3 | 3 | 约 5 小时 |
| 五组对照 L 档 | 2 | 1 | 约 1.5 小时；另加 32 GiB 数据生成灌入 1~2 小时 |
| Pareto 一 | 3 | 7 | 2~3 小时 |
| Pareto 二 | 9 | 7 | 11~19 小时 |
| 总计 | — | — | 约 75~120 小时 |

五组对照按 1.9 的 D19 分档（3/3/1），对照部分机时按 D32 的带宽分区结构是约 6.3 小时（`docs/9.1` §12）：rep 复用控制面，M 档只取 1000mbit 一档，L 档 1 次探点。W-TinyLFU 撤档后 Pareto 二从 16 点缩到 9 点（D28 把 lookahead 轴收敛为 8/16/32），机时按同口径等量下调。这是粗估，误差主要来自 8 GiB × 10 epoch 与 Fluid 组的冷态成本。M2 的 runner 会用实际单点时长替换本表；压缩只允许用“共享基线”，五组对照的次数按 D19，其余最终结论点不允许降低次数要求。
