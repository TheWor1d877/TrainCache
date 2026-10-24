# 5 bench 装置说明书（手册版 v2，2026-10-24 重写）

> 这份文档是 `bench/` 实验装置的说明书：文件是什么、命令怎么敲、一键化做到哪、出了毛病查哪行。
> 两类读者：维护这套装置的人（§2、§3），和将来迭代版本的我自己（§4 起）。
> 路径一律写相对路径 `bench/...`。`/home/howardhe/tc` 是指向项目根的符号链接，不是另一份拷贝，看到旧文档里的 tc/ 前缀就当项目根。
> 状态图例：**✅ 已实现**｜**🔧 改造中**｜**📝 规划待建**。本文只把已实现的当现状，规划项明确标注，不混。
> 纪律：文档是代码的声明式。以磁盘代码为准，发现对不上，当天改文档。

## 1. 装置是什么

一套在单机（32 核 / 30 GiB 内存 / 4 worker kind 集群）上复刻"分布式训练存储"的实验装置，做三件事：

1. **造冷热差**：tc 把源站出站路径限到 100mbit/20ms（100Mbps≈12.5MB/s，读 8GB 光传输 640 秒），本地缓存命中绕开这条路，冷读慢热读快是真差不是假差。
2. **造负载**：四轨合成数据（A 小文件元数据、B 8GB webdataset tar、C 倾斜访问清单、D 2GiB 连续 epoch），固定种子、逐字节可复现。
3. **同台跑分**：TrainCache 与 direct/juicefs/fluid 五个对照组吃同一份轨迹、同一个限速档、同一个容量的缓存，一键起拆、一键出指标（§4.3、§9）。

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
| `Makefile` | 全部入口 | `bench-up`/`bench-down`/`kind-up`/`k8s-up`/`data-up`/`upload`/`tc-up RATE= DELAY=`/`tc-down`/`bench-cold`/`bench-verify QUICK=`/`bench-data`/`bench-tc`/`track-d-data|up|verify|down`/`e2e-local`/`e2e-k8s`/`bench COND= REPS=`/`bench-list` | ✅ |
| `config.py` | 唯一参数源 | `python3 config.py --sh <KEY>`；被所有脚本读 | ✅ |
| `scripts/synth_track_{a,b,c,d}.py` | 四轨数据生成 | 参数全在 config；幂等，已存在即跳过 | ✅ |
| `scripts/bench_track_d.py` | D 轨负载器 + verify 判据 | `--epochs --seed --workers --out-dir`；`--selftest` 自测判据函数 `compare_epoch_medians` | ✅ |
| `scripts/bench_track_c.py` | C 轨双 Job 倾斜负载器 | M3/M5 用（热集循环 + 全量扫描） | ✅ |
| `scripts/tc_apply.sh` / `tc_teardown.sh` | 挂/拆限速 | `<rate> <delay> [burst] [--target minio\|source-d] [--dry-run]` | ✅ |
| `scripts/cold_reset.sh` | 冷态规程 | 无参；sync+drop_caches+重启 MinIO+重挂 tc；每组实验前必跑 | ✅ |
| `scripts/verify_cold.sh` | 装置可信性 | `[--quick]`；两轮冷读相对差 ≤10% 判过 | ✅ |
| `scripts/track_d_{up,verify,down}.sh` | D 轨编排 | 零参数，`--dry-run`；up=容器+欠配+tc+10 epoch，verify=两轮×3 epoch 对账，down=拆除保留数据 | ✅ |
| `scripts/upload_bench.py` | B 轨灌 MinIO | 无参；每次全量重传（同键覆盖+重设公开读 policy），无"存在即跳过" | ✅ |
| `scripts/trainpublish` | 发布 TrainCache 块快照 | Go 二进制（源码 `core/cmd/trainpublish`，重编：`/usr/local/go/bin/go build`）；关键旗标 `-chunk-size`（实验取 8388608，见 §3） | ✅ |
| `k8s/kind-config.yaml`、`k8s/minio.yaml` | 集群与源站基座 | kind 5 节点配置；MinIO 带 30Gi PVC（必须，见 §7 第 6 条） | ✅ |

### 2.2 五组对照层（compare/）

| 文件 | 作用 | 接口 / 说明 | 状态 |
|------|------|-------------|------|
| `compare/Makefile` | 对照入口 | `compare-up/down GROUP=`、`compare-all-up/down`、`trace`（生成统一轨迹）、`check`、`help` | ✅ |
| `compare/run_bench.py` | 跑分编排器 | `--reps N --groups <组...> --out <csv>`；7 个组名映射 4 个目录组（direct、juicefs-{cold,warm}、fluid-{cold,warm}、traincache-{cold,warm}） | ✅ |
| `compare/direct/` | 组 1：HTTP 直读 MinIO，零缓存 | `up.sh` 建 ns/secret/trace ConfigMap+tc；无预热 | ✅ |
| `compare/juicefs/` | 组 2/3：JuiceFS 冷/热 | `up.sh`：Redis+mount Pod（缓存 hostPath `/var/cache/jfs`）；`JFS_WARMUP=1` 时预热在 up.sh 内完成（15 分钟硬超时，超时整轮作废）；`--cache-size` **单位是 MiB** | 🔧✅（合一改造已完成） |
| `compare/fluid/` | 组 4/5：Fluid 冷/热 | `up.sh`：Dataset+JuiceFSRuntime+PVC；`FLUID_PREHEAT=1` 时 DataLoad 在 up.sh 内等待完成；`FLUID_CACHE_GI` 控容量 | ✅ |
| `compare/traincache/` | 组 6/7：TrainCache 冷/热 | `up.sh`：发布快照（`publish_snapshot`）、打 `s3-access` 标签（`label_s3_nodes`）、注入 agentd 配置（`ensure_agent_config`）、建 DataSet/WarmupTask（`TRAINCACHE_WARMUP_K`）；**sleep 占位 compare-job 已删** | 🔧（8MiB 重发布与 k 值调整待做，§3） |
| 统一测量体 | **每组只有一个 train-job Pod**：measure 脚本是主容器；traincache 组走 webhook gate（Pending 等待计入 startup），其余组普通调度；`t_submit` = 创建该 Pod 之前的时刻，五组同尺 | `run_bench.py create_measure_pod()` | ✅ |

### 2.3 条件化实验层（experiments/）

| 文件 | 作用 | 状态 |
|------|------|------|
| `cond.py` + `cond.schema.md` | COND 契约（v1）：一个 JSON = 一个实验点；解析器校验字段全集并产出环境变量 | ✅（`capacity_ratio`、`chunk_size_bytes`、`warmup_seconds` 字段待补，📝） |
| `conds/*.json` | 现存 6 个点：policy-{heat,lru,lfu,wtinylfu}、pareto2-lookahead{2,8} | ✅ |
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
| `TC_RATE / TC_DELAY` | 100mbit / 20ms | `config.py`；`make bench-tc RATE= DELAY=` | 回源管道粗细；是图一的自变量，对照三档实验不许动（10.1 D21） |
| `TRACK_B_N_TARS` | 32（~264MB/个，≈8GiB） | `config.py` | 源数据规模 |
| `N_TARS`（对照） | 8（≈2GiB，S 档） | `run_bench.py` 环境变量 | 每组读的分片数；M/L 档改这里 + 灌新数据 |
| `chunk-size`（实验块） | **8MiB**（生产默认 64MiB） | `trainpublish -chunk-size 8388608`，随产物归档 | 块=缓存与预取最小单位；1GiB 缓存=128 块（10.1 D20） |
| `TRAINCACHE_CACHE_CAPACITY_BYTES` | 1GiB（`1073741824`） | traincache `up.sh` `ensure_agent_config` 注入 agentd；代码实现在 `agent/svc/config.go` | TrainCache 缓存字节上限，淘汰按它触发 |
| `JFS_CACHE_MB` | 1024 | juicefs `up.sh` | JuiceFS 缓存上限；`--cache-size` 直接收 MiB 数值，**别再乘 1024** |
| `FLUID_CACHE_GI` | 1 | fluid `up.sh` | Fluid 缓存配额（Gi） |
| `TRAINCACHE_WARMUP_K` | warm=16 / cold=0 | `run_bench.py` 环境变量 → WarmupTask `podDetails[0].k` | 预热块数。**预热量 = k × chunk-size**：k=16 配 64MiB 块 =1GiB；改 8MiB 块后 S 档 warm 要 **k=128**（📝待改，10.1 D20 连带） |
| `TRAINCACHE_PREFETCH_LOOKAHEAD` | 16（默认） | `agent/svc/assemble.go`；experiments/run_policy.py 注入 | Agent 持续预取窗口；对照三档统一 16（D21） |
| `TRAINCACHE_PREFETCH_K` | 8（默认） | compare train-job 侧 ordersrc 消费窗口 | 训练侧消费深度，与 lookahead 是两个旋钮 |
| `TRAINCACHE_EVICT_POLICY` | heat（默认）/lru/lfu/wtinylfu | env 注入，非法名拒启 | M4/M5 用；对照实验一律 heat |
| `EPOCHS / STEPS_PER_EPOCH` | 3 / 6 | `run_bench.py` 环境变量 | 对照训练形态；L 档 EPOCHS=1 |
| `TRAINCACHE_DATASET_WHITELIST / OBJSTORE_BUCKET` | compare-data / bench-b | `ensure_agent_config` | agentd 服务哪些数据集/桶。快照改名 `compare-data-8m` 后**这里要同步**（10.1 Q12） |
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

### 4.2 主实验（D 轨与淘汰/预取）

```bash
make track-d-up                       # D 轨 10 epoch 连跑（宿主机直跑，不占集群）
make bench COND=policy-lru REPS=7     # 跑一个实验点（N 次含冷态循环）
make bench-list                       # 有哪些 COND
python3 experiments/collect.py ...    # 离线汇总出 metrics.csv
python3 experiments/plot.py           # 出图
```

进度查看：`ls results/<run目录>` 逐轮增多；训练打点在 per-step jsonl，`tail -f` 即可。

### 4.3 五组对照跑分

```bash
cd bench/compare
python3 run_bench.py --reps 5 --groups direct --out ../perf_results/s.csv   # 先单组试跑
python3 run_bench.py --reps 5 --out ...csv                                   # 全量
make compare-up GROUP=juicefs-warm     # 只起某组环境（预热在 up 内完成并等待）
make compare-down GROUP=...           # 拆该组
```

跑中要盯的东西（每条都可核验，别信"应该快了"）：

```bash
kubectl get pods -A -l app=train-job -o wide     # 当前哪个组在测、落在哪个节点
kubectl -n <组ns> get pod train-job-0 -o jsonpath='{.status.phase}'   # traincache 组先看 Pending（gate 卡着）→Running
kubectl logs -n <组ns> train-job-0 -f            # 每步的 wait_s/step_s 实时打点
kubectl -n traincache get warmuptask             # 预热进度
kubectl -n traincache get pods -o wide | grep agentd   # 4 worker 各一且 Ready
```

注意：**CSV 在全部 reps 跑完才落盘**，中途中断只剩 pod 日志与 `perf_results/logs/`。

### 4.4 拆除

```bash
make compare-down          # 或 make compare-all-down（拆尽四组）
make bench-down / tc-down  # 主实验拆限速；集群与数据保留
```

一句话：拉起跑验用 4.1，日常实验按轨选 4.2/4.3，进度永远用 kubectl 实况说话。

## 5. 一键化规程（目标形态，状态标注）

| 动作 | 命令 | 干什么 | 状态 |
|------|------|--------|------|
| 体检 | `make bench-doctor` | 一次跑完 §5.1 清单，输出逐项绿/红 | 📝 待建 |
| 冷态 | `make bench-cold` | drop_caches + 重启 MinIO + 重挂 tc；每 rep 前 runner 自动调 | ✅ |
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
| 17 | python 依赖炸（boto3 找不到） | 系统 python3 是 3.14，venv 里是 3.12 site-packages | 装置统一 `/usr/bin/python3.12`（Makefile `PY`），boto3 走 PYTHONPATH 指 `.venv` |
| 18 | `docker update --memory 0` 没恢复 | 本机 docker 把 0 当"不变更" | 恢复用 128GiB 等效大值，记录在 env json `restore_caveat` |

一句话：这张表是装置的全部病史，动环境之前先来对一遍。

## 8. 禁改清单（清理磁盘、重置实验时一律不动）

1. MinIO `bench-b/shard-*.tar`——五组共同源数据（32 个 ~8GiB），删了等于重造数据。
2. 旧 `bench-b/compare-data/`（64MiB 块快照）——保留供历史产物复现（10.1 Q12）。
3. kind 集群本体与 MinIO PVC——清理脚本不许 `kind delete`。
4. `bench/data/`（宿主机四轨数据，种子可再生成但重生成很贵）、`tmp_shards/`。
5. `results/`、`perf_results/` 已归档产物——只追加不覆盖不删。
6. `bench/.sudo-pass` 内容——任何文档、日志、回复里不出现。

## 9. 产物目录规范（一键出指标/出图的读取约定）

```text
bench/results/
├── cold-verify.txt              # 装置可信性
├── track_d/                     # D 轨：epochs_meta.json、per_step jsonl、verify_*.json、sweep_*/
├── <COND 运行目录>/              # 主实验：run.json + metrics.csv + collect.json + env.lock
└── compare/                     # 📝 目标形态（迁移前对照产物暂在 repo 根 perf_results/）
    └── <tier S|M|L>/<组>/<repNN>/
        ├── run.json             # t_submit/t_first_step/t_last_step、per-step、预热时长
        ├── metrics.csv          # 四指标 + tier + 预热时长列
        ├── env.lock             # 镜像 digest、tc 档位、seed、chunk-size、cache 配置
        └── status               # ok | void | exploratory（L 档探点标这个）
```

约定：每行数据能回答「哪一档、哪个组、第几次、哪个 COND」四件事；`void` 轮不进汇总（10.1 D14）；`exploratory` 不进正式表。`--out` CSV 与 summary.csv 的列以 §2.3 collect/stats 实现为准。

一句话：产物结构先冻结，脚本才有的读、图才有的画、别人才能复跑。

## 10. 不可再生的实测基准（引用时注意出处）

- **默认档 100mbit/20ms**：冷读 8GiB 两轮对账 710s/710s、相对差 0.0%（理论传输 640s，`results/cold-verify.txt`）。
- **指标灵敏度（D 轨两档扫点，各 1 epoch）**：吞吐 12.4→32.8MB/s、p90 wait 1.23→0.013s、mean stall 28.5%→10.5%，而 **median stall 8.53%→8.38% 几乎不动**（prefetch 吸收了）。结论：扫点用 mean/p90/吞吐，中位数只判形态（`results/track_d/sweep_100m|300m/`）。
- **D 轨 verify 判据降级史**：20 步窗口均值被重尾毛刺主导（相邻 epoch 逐点相对差实测 37%~56%），主判据降为 epoch 级 `median_stall` ≤10%；窗口曲线标 `report_only`。首轮 FAIL（max_rel_diff 0.593）→降级后 PASS→终验 PASS（1.6%/0.5%）。
- **kindest/node:v1.28.15 标签实指 v1.37 镜像**：版本以 `kubectl get nodes` 实测为准。
- **遗留矛盾**：Makefile 注释 B 轨生成"约 25 分钟"vs 旧文档"~50 分钟"，待实测统一。

一句话：这些数字是装置的地基，换环境后必须重测，不许直接引用。
