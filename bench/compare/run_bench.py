#!/usr/bin/env python3
"""五组对照跑分脚本（统一 train-job Pod 形态，预热先于计时）。

七组态各跑 N 次（次数按 9.1 D19/D32 分档：S=3（@100 与 @1000 各 3 次）
/ M=3 / L=1 探点，--reps 可显式覆盖；GROUPS 是五个受试者展开的七个组态），
收集 makespan / stall / 命中率 / 预热时长 / 三笔字节成本，汇总到 summary.csv。

统一形态（任务清单 #5）：
  五组全部创建一个 train-job Pod（label app=train-job）。
  traincache 组额外打 traincache.io/warmup=enabled label +
  schedulerName=traincache-scheduler，webhook 自动注入 schedulingGates，
  Pod 在 WarmupTask Released 前保持 Pending，Pending 时长天然计入 startup。
  其余组普通调度，直接 Running。

预热时机（任务清单 #2/#7，D23）：
  预热统一在 group_up 里完成、等完成、不计入 makespan（15 分钟超时整轮作废）。
  up.sh 里 tc 档位先于预热挂上：预热与测量走同一条限速链路。
  juicefs 两组的测量 Pod 共置到 jfs-mount 所在节点（CE 单机缓存只在那台）。
    direct        : 无预热（直读 MinIO，零缓存基线）
    juicefs-cold  : 无预热（JFS_WARMUP=0）
    juicefs-warm  : up.sh 内 juicefs warmup /mnt/jfs/（hostPath 缓存）
    fluid-cold    : 无预热（FLUID_PREHEAT=0）
    fluid-warm    : up.sh 内 DataLoad（Fluid 缓存）
    traincache-cold : 预热 k=0（无预热，WarmupTask 立即 Released）
    traincache-warm : up.sh 内 WarmupTask k=缓存容量/块大小（8MiB 块=128），
                      preheatFanout 拉满 s3-access 节点

6.12 统一轨迹：
  measure 主循环真正按 trace.json 消费（分片顺序、shuffle seed、每步样本数），
  不再用线性序 (epoch*steps+step)%n_tars。trace 由本脚本调 trace.py 的
  build_trace() 现场生成（同一 track+档位的七组态共用、逐字节一致），与 measure.py 一起装进
  train-job Pod 的 script ConfigMap（/scripts/trace.json），轨迹 sha256 随
  产物归档。规模统一到 D 轨口径：S 档 2GiB 数据（8 分片 × 256MiB、每分片
  1600 样本、batch 32 → 每 epoch 恰好 400 步、一遍全量）。

6.15 stall_ratio 改造（2026-10-24）：
  旧版 measure 是串行"读整片 tar → matmul"，读比算慢两个数量级，
  stall_ratio = wait_s/step_s 恒≈0.98，没有判别力——旧串行版本产物的
  stall_ratio 一律作废（docs/4 §10），需按本口径重跑。
  新版用 COMPUTE_MS=200ms 控制每步算力，读与计算并行（后台 reader 线程
  模拟 DataLoader 异步预取，把接下来若干分片读进内存队列，主循环从队头
  取数据 + 做固定算力）：预取有效（热缓存/预取跟上）→ stall_ratio 趋 0；
  预取失效（冷读跟不上算力）→ 队列常被抽空 → 趋 1。
  COMPUTE_MS / READ_WORKERS / READ_QUEUE_DEPTH 全部走 Pod 环境变量，
  模板内不写死。

6.16 命中率对账（D25）：
  traincache 组旧口径"块在磁盘存在即 hit"虚高（agent 运行期悄悄回源补块，
  测量体不知道），已作废。主口径改为训练侧 open-time（9.2 §1.4）：
  打开块时本地没有即算未命中，hit_rate = (总读字节 − demand_miss 字节) / 总读字节；
  agent 的 traincache_fetch_bytes_total run 窗口增量单独记 source_saving_ratio
  = (总读字节 − 回源字节) / 总读字节，衡量"省了多少源站流量"，与命中率分开。
  取数路径：优先 Prometheus（PROMETHEUS_URL 环境变量，复用
  experiments/collect.py 的查询函数），否则
  `kubectl get --raw ...pods/<agentd>:9411/proxy/metrics` 直接抓 agentd。
  JuiceFS/Fluid 无块级命中指标，仍按 warm≈全命中 / cold≈全回源 + 回源
  字节的模型近似，产物 hit_rate_basis 列显式标注近似性。
  summary 产物补三笔字节成本列：启动预热×fanout、持续预取、切换重预取（D5）。

9.2 口径重跑（2026-10-28）：
  ① 每步算力改墙钟：compute_work() 循环 matmul 直到实测 >= COMPUTE_MS，
     不再用固定重复次数（不同 Pod 实测 148–396ms，组间差异被算力噪声吃掉）；
     calibrate_compute_reps() 只留作估算值打印。
  ② t_first_step 打在本步算力开始之前（wait 之后），不再多算一步。
  ③ stall = 每步 wait_s/work_s 的 mean 与 p90，epoch0 前 10 步不进样本；
     旧的 Σwait/Σstep 不再写 RESULT。
  ④ 分阶段（startup/steady/switch）读字节与回源字节逐块累计：
     一个 epoch 前 k_shards 个分片（k 由 PREFETCH_K 折算）在 epoch0 记 startup、
     epoch>0 记 switch，其余 steady。
  ⑤ 快照缺片（[SNAPSHOT_MISS]）直接作废，不静默当 0 字节读。
  ⑥ 分阶段命中率（open-time 字节差分）、agent 分阶段交叉校验列、
     Pod Ready 落地时长、源站出站字节（MinIO cluster metrics）与预热字节实测一并落列。

流程（每组每轮）：
  1. cold_reset（删 Pod + 在节点上清 hostPath 缓存：traincache 块缓存、
     JuiceFS /var/cache/jfs、Fluid /tmp/juicefs-cache，删后判空）
  2. up.sh 搭建基础设施 + 预热（同步等完成 + 15min 超时，超时整轮作废）
  3. run_bench.py 创建 train-job Pod（t_submit = 创建前时刻；traincache 组
     创建前抓 fetch 计数器基线，运行期定时采样，结束后做窗口增量对账）
  4. 等待完成，解析 RESULT 行与 [EPOCH_BOUNDARY] 标记
  5. down.sh 拆除
  6. 写入 summary.csv（含 warmup_seconds / tier / status / baseline_substituted /
     trace_sha256 / 三笔字节成本 / hit_rate_basis 列）

D30/D32 接线（2026-10-25）：
  · 参跑组按档位过滤：S 档七组态全跑，M 档 direct/fluid-cold/traincache-cold，
    L 档 traincache-cold/fluid-cold；显式 --groups 与档位求交集，越档组态被剔除。
  · 冷组（含 direct，都无预热）跨 rep 复用控制面：进组前做一次全量 cold_reset +
    group_up，之后每 rep 只做轻冷态（删 train-job Pod + 清四个节点缓存目录），
    组末统一 down；warm 组每 rep 仍整轮 down/up，否则缓存回不了冷态、
    WarmupTask 会停在上一轮 Released（标 warm 实为冷读）。
  · epochs 随档位联动（S=3、M/L=1），与 conds/compare-*.json 的 workload.epochs 对齐。
  · Fluid 该组每轮都作废 → 落回同冷热档 JuiceFS 顶位，产物标 baseline_substituted。
  · status 列三态：ok / void（D14：void 不进汇总）/ exploratory（D19：L 档 1 次探点）。
  · D31 带宽格由 runner 现推并打印（rate/burst/BW÷r）；rate 只从 `--rate`
    取，再以 TC_RATE 环境变量连同 TC_DELAY 下发给 up.sh，
    shell 侧据此挂 tc（netem 只有固定 delay）。

用法：
  python3 bench/compare/run_bench.py --out compare/summary.csv
  # --out 相对路径按 bench/config.py 的 RESULTS_DIR(~/tcd/bench/results)展开,
  # 不落到 repo 的当前工作目录;绝对路径原样使用。
  python3 bench/compare/run_bench.py --tier M --dry-run        # 开跑前核对计划
  python3 bench/compare/run_bench.py --tier S --rate 300mbit   # S@300 档（BW/r=1.40）
  python3 bench/compare/run_bench.py --tier S --rate 1000mbit  # S@1000 档
  CAPACITY_RATIO=M python3 bench/compare/run_bench.py --rate 1000mbit  # M 档（3 组态 × 3 次）

环境变量（唯一口径集中在本文件顶部，模板/产物不各处写死）：
  CAPACITY_RATIO=S|M|L   容量档位（D19/D32：S=2GiB数据/1GiB缓存×3（@100 与
                         @1000 各 3 次）、
                         M=8GiB/1GiB×3、L=32GiB/1GiB×1 探点）
  N_TARS                 分片数（缺省按档位：S=8、M=32、L=128；
                         S 档 8×256MiB≈2GiB，docs/4 §3 对照口径）
  EPOCHS/STEPS_PER_EPOCH/BATCH/IMAGES_PER_TAR/SEED/SHUFFLE
                         统一轨迹参数（默认 3/400/32/1600/20261015/per-epoch，
                         与 bench/config.py D 轨语义对齐：一个 epoch 把全量
                         数据按轨迹顺序恰好过一遍）
  CHUNK_SIZE             实验块大小，默认 8388608（8MiB，9.1 D20；
                         生产默认 64MiB 不变）
  DATASET_NAME           TrainCache 快照名，默认 compare-data-8m（9.1 D41；
                         MinIO 内该快照由 trainpublish -chunk-size 8388608
                         发布属后续动作，本脚本只按新名引用）
  TRAINCACHE_CACHE_CAPACITY_BYTES  统一缓存容量，默认 1GiB
  TRAINCACHE_WARMUP_K    warm 组预热块数，显式写入 podDetails[].k；缺省 =
                         缓存容量/块大小（1GiB/8MiB=128，D20 重标度）。
                         cold 组用 0 作 bench 哨兵，跳过 WarmupTask；省略 k
                         时由 Controller 自动计算，不属于 bench 口径
  TRAINCACHE_PREFETCH_K  训练侧消费窗口块数（默认 16，docs/4 §3；D21 对照三档统一 16）。
                         agent 侧 lookahead 三档统一 16 块（D21）：8MiB 下
                         窗口 16×8MiB=128MiB≈缓存 1/8，无退化，不需重标度。
  COMPUTE_MS/READ_WORKERS/READ_QUEUE_DEPTH/READ_TIMEOUT_S
                         6.15 异步预取参数（200/2/2/300）
  COMPARE_SWITCH_WINDOW_S epoch 切换重预取的归因窗口（默认 60s）
  PROMETHEUS_URL         非空则 fetch 对账走 Prometheus，否则 kubectl raw 抓 agentd
  POD_TIMEOUT            单轮 Pod 超时（默认 1800s）
"""
import argparse
import ast
import contextlib
import csv
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

COMPARE_ROOT = Path(__file__).resolve().parent
BENCH_ROOT = COMPARE_ROOT.parent
SCRIPTS_DIR = BENCH_ROOT / "scripts"
CONDS_DIR = BENCH_ROOT / "experiments" / "conds"
# 6.12：复用对照层 trace 生成器；6.16：复用实验层 collect.py 的 Prometheus 查询
sys.path.insert(0, str(COMPARE_ROOT))
sys.path.insert(0, str(BENCH_ROOT / "experiments"))
import trace as trace_mod  # noqa: E402  (bench/compare/trace.py，遮蔽 stdlib trace)
# 容量档位口径与 cond.py 同源(D19/6.13/D32：单一参数点，不造第二套)
from cond import (CAPACITY_TIER_EPOCHS, CAPACITY_TIER_GROUPS,  # noqa: E402
                  CAPACITY_TIER_N_TARS, CAPACITY_TIER_REPS,
                  CAPACITY_TIER_REPS_ALLOWED, EXPLORATORY_TIERS,
                  tier_dataset_bytes)
# D31：带宽档的 burst 推导口径也取自 bench/config.py，shell 与 runner 同一套数
import config as bench_config  # noqa: E402

GROUPS = ["direct", "juicefs-cold", "juicefs-warm", "fluid-cold", "fluid-warm", "traincache-cold", "traincache-warm"]
# D31：@300 只有 S 档一个格子，只允许三组（与 M 档参跑组相同）
S300_GROUPS = ("direct", "fluid-cold", "traincache-cold")

GROUP_NS = {
    "direct": "direct",
    "juicefs-cold": "juicefs",
    "juicefs-warm": "juicefs",
    "fluid-cold": "fluid",
    "fluid-warm": "fluid",
    "traincache-cold": "traincache",
    "traincache-warm": "traincache",
}
GROUP_UP = {
    "direct": COMPARE_ROOT / "direct" / "up.sh",
    "juicefs-cold": COMPARE_ROOT / "juicefs" / "up.sh",
    "juicefs-warm": COMPARE_ROOT / "juicefs" / "up.sh",
    "fluid-cold": COMPARE_ROOT / "fluid" / "up.sh",
    "fluid-warm": COMPARE_ROOT / "fluid" / "up.sh",
    "traincache-cold": COMPARE_ROOT / "traincache" / "up.sh",
    "traincache-warm": COMPARE_ROOT / "traincache" / "up.sh",
}
GROUP_DOWN = {
    "direct": COMPARE_ROOT / "direct" / "down.sh",
    "juicefs-cold": COMPARE_ROOT / "juicefs" / "down.sh",
    "juicefs-warm": COMPARE_ROOT / "juicefs" / "down.sh",
    "fluid-cold": COMPARE_ROOT / "fluid" / "down.sh",
    "fluid-warm": COMPARE_ROOT / "fluid" / "down.sh",
    "traincache-cold": COMPARE_ROOT / "traincache" / "down.sh",
    "traincache-warm": COMPARE_ROOT / "traincache" / "down.sh",
}

TRAIN_IMAGE = os.environ.get("TRAIN_IMAGE", "python:3.12-alpine")
JUICEFS_IMAGE = os.environ.get("JUICEFS_IMAGE", "juicedata/mount:ce-v1.3.0")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "bench-b")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "traincache")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "traincache-probe")

# ---- 容量档位（9.1 D19/D32：S=2GiB数据/1GiB缓存×3、M=8GiB/1GiB×3、L=32GiB/1GiB×1 探点；
#      正式实验各格统一 3 次，L 档 1 次探点）
# 取值口径由 bench/experiments/cond.py 的 CAPACITY_TIER_* 常量单点定义 ----
TIER_PRESETS = {
    t: {"n_tars": CAPACITY_TIER_N_TARS[t], "reps": CAPACITY_TIER_REPS[t],
        "epochs": CAPACITY_TIER_EPOCHS[t], "dataset_bytes": tier_dataset_bytes(t)}
    for t in ("S", "M", "L")
}
# L 档 1 次低于 3 次硬纪律：产物标 exploratory，不进正式表（D19/9.0，单点取 cond.py）
TIER_EXPLORATORY = set(EXPLORATORY_TIERS)
TIER = os.environ.get("CAPACITY_RATIO", "S").strip().upper() or "S"
if TIER not in TIER_PRESETS:
    print(f"[cfg] 非法 CAPACITY_RATIO={TIER!r}（允许 S/M/L），按 S 处理")
    TIER = "S"

# 显式环境变量优先于档位联动（冒烟要跑 1 epoch 也走这里）；未显式给值时按档位取。
ENV_HAS_N_TARS = "N_TARS" in os.environ
ENV_HAS_EPOCHS = "EPOCHS" in os.environ
N_TARS = int(os.environ.get("N_TARS", str(TIER_PRESETS[TIER]["n_tars"])))
# TrainCache DataSet/快照名(6.13/Q12：正式实验一律 8MiB 快照 compare-data-8m;
# DATASET_NAME 环境变量可切换冒烟前缀,如 compare-data-8m-smoke。
# 注意:MinIO 里该快照由 trainpublish -chunk-size 8388608 发布属后续动作)
DATASET_NAME = os.environ.get("DATASET_NAME", "compare-data-8m")
# D32：epochs 缺省随档位联动（S=3 走 D 轨口径、M/L=1 走 B 轨探点），与
# conds/compare-*.json 的 workload.epochs 逐个对得上；显式 EPOCHS 环境变量优先。
EPOCHS = int(os.environ.get("EPOCHS", str(CAPACITY_TIER_EPOCHS[TIER])))
STEPS_PER_EPOCH = int(os.environ.get("STEPS_PER_EPOCH", "400"))   # 6.12：统一 D 轨口径
BATCH = int(os.environ.get("BATCH", "32"))
IMAGES_PER_TAR = int(os.environ.get("IMAGES_PER_TAR", "1600"))    # B 轨 256MiB tar 的样本数
TRACE_SEED = int(os.environ.get("SEED", "20261015"))
TRACE_SHUFFLE = os.environ.get("SHUFFLE", "per-epoch")
SHARD_SIZE_MB = int(os.environ.get("SHARD_SIZE_MB", "256"))

# ---- 块大小与缓存容量（6.13/D20：实验块 8MiB，生产 64MiB 不变）----
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "8388608"))
# 分段读粒度：测量脚本按此长度 read()，只累加长度、不保留字节。
READ_SEGMENT_BYTES = 8 << 20
CACHE_BYTES = int(os.environ.get("TRAINCACHE_CACHE_CAPACITY_BYTES", str(1 << 30)))
# warm 组预热块数（显式写入 CRD）：默认 k = 缓存容量 / 块大小（8MiB → 128）。
# 0 只作 bench 冷启动哨兵，不写进 CRD。
WARMUP_K = int(os.environ.get("TRAINCACHE_WARMUP_K",
                              str(max(1, CACHE_BYTES // CHUNK_SIZE))))
# 消费窗口块数：D21/任务 4.8 要求 lookahead 三档统一 16（8MiB 下窗口
# 16×8MiB=128MiB≈缓存 1/8，无退化）；旧默认 8 与 D21 不符，改为 16。
PREFETCH_K = int(os.environ.get("TRAINCACHE_PREFETCH_K", "16"))
# Agent 侧持续预取窗口。pareto2 的自变量是它，训练侧消费窗口 PREFETCH_K
# 保持 16 不动（docs/4 §3：两个旋钮）。缺省跟随 PREFETCH_K。
AGENT_PREFETCH_K = int(os.environ.get(
    "TRAINCACHE_PREFETCH_LOOKAHEAD", str(PREFETCH_K)))
# 预热节点数(D26):对照链路固定 2,scheduler 按就绪度选落点;s3-access 节点数只作上限
PREHEAT_FANOUT = int(os.environ.get("TRAINCACHE_PREHEAT_FANOUT", "2"))
PREHEAT_FANOUT_MIN = int(os.environ.get("TRAINCACHE_PREHEAT_FANOUT_MIN", "1"))
# 图一/图三的自变量：门控开关与数据源。gated 是默认形态；free 去掉
# SchedulingGate 与 WarmupTask 等待，只剩预取与服务端逻辑（9.2 §2.1）。
GATE_MODE = os.environ.get("TRAINCACHE_GATE_MODE", "gated").strip().lower()
DATA_SOURCE = os.environ.get("TRAINCACHE_DATA_SOURCE", "minio").strip().lower()
# 网关开关(D23)：只有 "1" 打开;其余取值（含空串/0/true）一律按无网关处理，
# 与 bench/scripts/ctrl_up.sh 的归一规则一致。
GATEWAY_ON = os.environ.get("TRAINCACHE_GATEWAY", "0").strip() == "1"


def gateway_flag(on=None):
    """summary.csv 的 gateway 列：1=经网关，0=agentd 直连源站。"""
    return "1" if (GATEWAY_ON if on is None else on) else "0"

EVICT_POLICY = os.environ.get("TRAINCACHE_EVICT_POLICY", "heat").strip().lower()
# 扫点脚本可显式指定本条产物的 COND 名（fig1/fig2/fig3/pareto1 的 COND
# 不叫 compare-*，_cond_name_for_cell 匹配不到）。
COND_NAME_OVERRIDE = os.environ.get("TRAINCACHE_COND_NAME", "").strip()
# 图系列(fig1/2/3、pareto1)由 run_figures.py 按 COND 驱动。它们的组态、
# 次数与带宽档不受 D30/D31/D32 档位约束,所以显式关掉越组过滤与
# 次数白名单;对照跑分与冒烟保持默认 0,行为不变。
FIGURE_MODE = os.environ.get("TRAINCACHE_FIGURE_MODE", "").strip() == "1"
# M5：倾斜双 Job（跑在同一个 measure Pod 里），只用于 pareto2/policy。
TILT = os.environ.get("TRAINCACHE_TILT", "").strip() == "1"

# ---- D31：带宽档（rate 是实验自变量，取 {100,300,1000} mbit）----
# 速率只从命令行 --rate 取：S 档缺省 100mbit，M/L 档必填。main() 解析后赋值，
# 再以 TC_RATE 环境变量下发给 up.sh；burst 不写死，由
# config.burst_for_rate 现推。
TC_RATE_EFF = None
TC_DELAY_EFF = os.environ.get("TC_DELAY", bench_config.TC_DELAY)

# ---- 6.15 异步预取参数（经 Pod env 下发给 measure 脚本，模板不写死）----
COMPUTE_MS = int(os.environ.get("COMPUTE_MS", "200"))
READ_WORKERS = int(os.environ.get("READ_WORKERS", "2"))
READ_QUEUE_DEPTH = int(os.environ.get("READ_QUEUE_DEPTH", "2"))
READ_TIMEOUT_S = float(os.environ.get("READ_TIMEOUT_S", "300"))

# ---- 6.16 命中率对账与三笔字节成本（D5/D25）----
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "").strip()
FETCH_SAMPLE_S = int(os.environ.get("FETCH_SAMPLE_S", "15"))
SWITCH_WINDOW_S = float(os.environ.get("COMPARE_SWITCH_WINDOW_S", "60"))
# 运行期 fetch 计数采样：目标周期与收尾等待。抓取要串行问四个 agentd，
# 单次可能超过周期，实际间隔以产物 sample_max_gap_s 为准。
SAMPLE_INTERVAL_S = float(os.environ.get("COMPARE_SAMPLE_INTERVAL_S", "5"))
SAMPLE_JOIN_TIMEOUT_S = float(os.environ.get("COMPARE_SAMPLE_JOIN_TIMEOUT_S", "30"))
AGENT_NS = "traincache"
AGENT_LABEL = "app=traincache-agentd"
AGENT_METRICS_PORT = 9411  # agentd /metrics（docs/8.3：TRAINCACHE_METRICS_ADDR=":9411"）
AGENT_DEGRADED_SAMPLE_S = 5  # 测量期间降级采样间隔
AGENTD_HIDE_TIMEOUT_S = 120  # 缩到 0 后等 Pod 全消失
AGENTD_READY_TIMEOUT_S = 180  # 缩回 1 后等全部 agentd Ready
AGENTD_CLEAN_TIMEOUT_S = 60  # 等 degraded=0 且 fetch 计数停增

POD_TIMEOUT = int(os.environ.get("POD_TIMEOUT", "1800"))

# 命中率口径标注（D25，产物列 hit_rate_basis 显式可见）
BASIS_TC_OPEN = "open_time_demand_miss_bytes(9.2 §1.4:打开时不在本地即未命中)"
BASIS_TC_SAVING = "origin_saving(1-agent_fetch_bytes/训练侧读字节,D25对账)"
BASIS_TC_UNAVAILABLE = "agent_fetch_unavailable(旧磁盘存在性口径已作废,本轮不报)"
BASIS_DIRECT = "exact_all_miss(直读全回源)"
BASIS_JFS_WARM = "approx_warm_all_hit(D25近似:warmup后读≈全命中)"
BASIS_JFS_COLD = "approx_cold_all_refetch(D25近似:冷读≈全回源)"
BASIS_FLUID_WARM = "approx_warm_all_hit(D25近似:DataLoad后读≈全命中)"
BASIS_FLUID_COLD = "approx_cold_all_refetch(D25近似:冷读≈全回源)"

# 预热量口径：traincache 只预热缓存容量大小的前缀，JuiceFS/Fluid 预热整档数据集。
WARMUP_BASIS_PREFIX = "prefix_only(k*chunk*fanout)"
WARMUP_BASIS_FULL = "full_dataset(n_tars*shard_size)"
# direct 组读法：HTTP GET 分段 read(n)，只累加总字节。
READ_MODE_DIRECT = f"http_get_read_segmented({READ_SEGMENT_BYTES},total_bytes)"


def run_cmd(cmd, env=None, timeout=300):
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def kubectl(args, namespace=None, timeout=120):
    cmd = ["kubectl"]
    if namespace:
        cmd += ["-n", namespace]
    cmd += args
    return run_cmd(cmd, timeout=timeout)


def _current_agent_config():
    """读 traincache-config 的三个 agent 键；读不到返回 {}。"""
    rc, out, _ = kubectl(["get", "configmap", "traincache-config", "-o", "json"],
                         namespace=AGENT_NS, timeout=30)
    if rc != 0:
        return {}
    try:
        return json.loads(out).get("data") or {}
    except ValueError:
        return {}


def ensure_agent_config():
    """把 traincache-config 的 agent 键对齐到本轮参数。

    agentd 与 controller-manager 都在启动时读这些值。cacheCapacityBytes 变化
    要重启 controller；三个键任一变化都要重启 agentd，后者由
    reset_traincache_agent() 逐轮完成。
    """
    want = {
        "cacheCapacityBytes": str(CACHE_BYTES),
        "prefetchLookahead": str(AGENT_PREFETCH_K),
        "evictPolicy": EVICT_POLICY,
    }
    cur = _current_agent_config()
    diff = {k: v for k, v in want.items() if cur.get(k) != v}
    if not diff:
        return
    pairs = ", ".join(f"{k} {cur.get(k) or '?'}→{v}" for k, v in diff.items())
    print(f"[cfg] 对齐 traincache-config: {pairs}", flush=True)
    patch = json.dumps({"data": diff})
    rc, _out, err = kubectl(["patch", "configmap", "traincache-config",
                             "--type", "merge", "-p", patch],
                            namespace=AGENT_NS, timeout=60)
    if rc != 0:
        raise SystemExit(f"[cfg] 写入 traincache-config 失败(rc={rc}): {err[:300]}")
    if "cacheCapacityBytes" not in diff:
        return
    rc, _out, err = kubectl(
        ["rollout", "restart", "deployment/traincache-controller-manager"],
        namespace=AGENT_NS, timeout=60)
    if rc != 0:
        raise SystemExit(f"[cfg] 重启 controller 失败(rc={rc}): {err[:300]}")
    rc, _out, err = kubectl(
        ["rollout", "status", "deployment/traincache-controller-manager",
         "--timeout=120s"], namespace=AGENT_NS, timeout=180)
    if rc != 0:
        raise SystemExit(f"[cfg] controller 重启未就绪(rc={rc}): {err[:300]}")


# 冷态清缓存落在哪些 kind 节点容器上（本机 4 worker；重建集群/换节点数时用环境变量覆盖）
KIND_WORKER_NODES = [n for n in os.environ.get(
    "KIND_WORKER_NODES",
    "traincache-worker,traincache-worker2,traincache-worker3,traincache-worker4"
).split(",") if n]


# 冷态要清的节点缓存目录：juicefs / fluid / traincache 各自的 hostPath。
_CACHE_DIRS = [
    "/var/cache/jfs",                    # juicefs 组 hostPath 缓存
    "/tmp/juicefs-cache",                # fluid 组 JuiceFSRuntime tieredstore
    "/var/cache/traincache/blocks",      # traincache 块缓存
    "/var/cache/traincache/quarantine",  # traincache 隔离块
]


def _node_cache_reset_cmds(node):
    """返回在某个节点容器内清缓存的命令列表（四条，一条清一个缓存目录）。

    四条都必须走 `sh -c`：docker exec 不带 shell 时通配符不展开，rm 收到的是
    字面路径 "/var/cache/traincache/blocks/*"，该路径不存在 + rm -f 静默返回 0
    ——缓存实际没被清掉，traincache-cold 会带着上一轮缓存起跑，冷读口径不成立。
    """
    return [["docker", "exec", node, "sh", "-c", "rm -rf " + d + "/*"]
            for d in _CACHE_DIRS]


def _node_cache_check_cmd(node):
    """返回在节点容器内检查四个缓存目录为空的命令（目录不存在视为空）。

    `ls -A` 含隐藏文件；有输出即表示仍有残留，冷读口径不成立。
    """
    script = ("for d in " + " ".join(_CACHE_DIRS) +
              '; do [ -d "$d" ] && ls -A "$d"; done')
    return ["docker", "exec", node, "sh", "-c", script]


def _drop_host_page_caches(tag):
    """冷态第二层：丢宿主机页缓存（口径同 D 轨 `track_d_up.sh` 的 ④）。

    清缓存目录只清了**磁盘**那一层：kind 节点上 Fluid/JuiceFS 的挂载进程跨 rep
    存活，内核里那批 FUSE 文件的干净页不会因为 `rm -rf` 缓存目录而失效，下一 rep
    的首批读直接命中页缓存、**根本不过 tc 限速链路**——实测 S-1000 fluid-cold 因此
    蹦出 1.31/1.36s，比同格 direct 的 5.76s 还快，冷读口径不成立。
    两条通道：sudo -S(bench/.sudo-pass) 优先，特权 docker 回退；都不通就退出，
    宁可不跑也不出"看着是冷读其实是页缓存"的行。
    """
    sync_then = "sync; echo 3 > /proc/sys/vm/drop_caches"
    pass_file = BENCH_ROOT / ".sudo-pass"
    if pass_file.exists():
        try:
            with pass_file.open() as fh:
                proc = subprocess.run(["timeout", "20", "sudo", "-S", "-p", "",
                                       "sh", "-c", sync_then],
                                      stdin=fh, capture_output=True, text=True,
                                      timeout=30, check=False)
            if proc.returncode == 0:
                print(f"[{tag}] 页缓存已丢弃(via sudo -S)", flush=True)
                return
        except (OSError, subprocess.SubprocessError):
            pass
    image = os.environ.get("TC_IMAGE", "kindest/node:v1.28.15")
    rc, _out, err = run_cmd(["docker", "run", "--rm", "--privileged",
                             "--entrypoint", "/bin/sh", image, "-c", sync_then],
                            timeout=90)
    if rc == 0:
        print(f"[{tag}] 页缓存已丢弃(via privileged-docker {image})", flush=True)
        return
    raise SystemExit(f"[{tag}] drop_caches 失败(sudo -S 与特权 docker 均不可用): "
                     f"{err[:200]}，冷读口径无法保证，退出")


def _clear_node_caches(tag):
    """清四个组在 kind 节点上的缓存目录（hostPath，不在 Pod 里），并丢宿主机页缓存。

    JuiceFS 组的缓存是 K8s 节点 hostPath /var/cache/jfs（见 create_measure_pod 的
    hostPath 挂载）；Fluid 组的 JuiceFSRuntime tieredstore 仍落在节点上的
    /tmp/juicefs-cache（fluid/up.sh 的 path，本次不改 path），冷态必须显式清，
    否则 cold 组带着上一轮缓存起跑。
    沿用本文件既有的"在节点上执行命令"机制(docker exec 进 kind 节点容器)，
    四条都在容器内用 sh -c 展开 glob，删除范围严格限定在目录内容本身。
    """
    for node in KIND_WORKER_NODES:
        for cmd in _node_cache_reset_cmds(node):
            rc, _out, err = run_cmd(cmd, timeout=30)
            if rc != 0:
                # 清缓存失败必须看得见：块缓存没清 = 冷读口径不成立
                print(f"[{tag}] WARNING: 节点 {node} 清缓存失败(rc={rc}): "
                      f"{' '.join(cmd)} {err[:200]}", flush=True)
        # 清后判空：hostPath 不随 Pod 删除消失，有残留就终止本轮。
        check = _node_cache_check_cmd(node)
        rc, out, err = run_cmd(check, timeout=30)
        if rc != 0:
            raise SystemExit(
                f"[{tag}] 节点 {node} 缓存检查失败(rc={rc}): {err[:200]}，"
                f"无法确认冷态，退出")
        leftover = out.strip()
        if leftover:
            raise SystemExit(
                f"[{tag}] 节点 {node} 缓存未清空，仍有残留: {leftover[:300]}，退出")
    _drop_host_page_caches(tag)


def cold_reset():
    print("[cold_reset] 清理训练 Pod 与缓存(保留 Redis 与控制面)...")
    for ns in set(GROUP_NS.values()):
        # 只删训练 Pod：`app!=redis` 会连带删掉 traincache 控制面，
        # agentd 重启后立刻回放 `/run/traincache/orders` 并写 `tmp-*`，
        # 与下面的清缓存判空打架，冷态复位随机会失败。
        kubectl(["delete", "pods", "-l", "app=train-job", "--grace-period=0", "--force"], namespace=ns, timeout=60)
    _clear_node_caches("cold_reset")
    time.sleep(5)


# ---------------------------------------------------------------------------
# traincache 组逐 rep 的 agentd 复位
# ---------------------------------------------------------------------------
# 订单 ring 与块缓存都在节点 hostPath 上：控制面重启后 agentd 会回放
# /run/traincache/orders 下所有残留 ring（每轮训练的 ring 不清理），按旧订单预取；
# cold_reset 清块缓存时 agentd 还在写，残留 tmp-* 会让它退出。所以每个 rep 先把
# agentd 停到 0（无写入），清空订单与块缓存，再拉起并自检干净。
_AGENTD_STATE_DIRS = [
    "/run/traincache/orders",
    "/var/cache/traincache/blocks",
    "/var/cache/traincache/quarantine",
]
_AGENTD_PAUSE_KEY = "traincache.io/bench-paused"
_AGENTD_SCALE_UNSUPPORTED = False
_AGENTD_PAUSE_NOTE_PRINTED = False


def _agentd_pod_names():
    """当前 agentd Pod 名列表；kubectl 失败返回 None。"""
    rc, out, _ = kubectl(["get", "pods", "-l", AGENT_LABEL,
                          "-o", "jsonpath={.items[*].metadata.name}"],
                         namespace=AGENT_NS, timeout=30)
    if rc != 0:
        return None
    return out.split()


def _agentd_pod_ready():
    """(Pod 总数, Ready 数)；kubectl 失败返回 None。"""
    rc, out, _ = kubectl(["get", "pods", "-l", AGENT_LABEL, "-o", "json"],
                         namespace=AGENT_NS, timeout=30)
    if rc != 0:
        return None
    try:
        items = json.loads(out).get("items", [])
    except ValueError:
        return None
    ready = 0
    for pod in items:
        conds = pod.get("status", {}).get("conditions", []) or []
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conds):
            ready += 1
    return len(items), ready


def _agentd_set_active(active):
    """把 agentd 拉到 active 个副本（0=停写，1=恢复）。

    DaemonSet 没有 scale 子资源，`kubectl scale ds` 会失败；首次失败后改用
    不匹配的 nodeSelector 挂起模板（恢复时只摘掉这个键），等效缩到 0/1。
    """
    global _AGENTD_SCALE_UNSUPPORTED, _AGENTD_PAUSE_NOTE_PRINTED
    if not _AGENTD_SCALE_UNSUPPORTED:
        rc, _out, _err = kubectl(
            ["scale", "ds/traincache-agentd", f"--replicas={1 if active else 0}"],
            namespace=AGENT_NS, timeout=60)
        if rc == 0:
            return
        _AGENTD_SCALE_UNSUPPORTED = True
        if not _AGENTD_PAUSE_NOTE_PRINTED:
            _AGENTD_PAUSE_NOTE_PRINTED = True
            print("[agent_reset] DaemonSet 不支持 kubectl scale，改用 nodeSelector 挂起",
                  flush=True)
    value = None if active else "true"
    patch = json.dumps({"spec": {"template": {"spec": {
        "nodeSelector": {_AGENTD_PAUSE_KEY: value}}}}})
    rc, _out, err = kubectl(["patch", "ds/traincache-agentd", "--type=merge",
                             "-p", patch], namespace=AGENT_NS, timeout=60)
    if rc != 0:
        raise SystemExit(f"[agent_reset] 无法{'恢复' if active else '挂起'} agentd"
                         f"(rc={rc}): {err[:300]}")


def _wait_agentd_hidden(timeout=AGENTD_HIDE_TIMEOUT_S):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        pods = _agentd_pod_names()
        if pods is not None and not pods:
            return
        time.sleep(2)
    raise SystemExit(f"[agent_reset] agentd 缩到 0 后 {timeout}s 内仍有 Pod 未消失，退出")


def _wait_agentd_ready(expected, timeout=AGENTD_READY_TIMEOUT_S):
    start = time.monotonic()
    last = None
    while time.monotonic() - start < timeout:
        state = _agentd_pod_ready()
        if state is not None:
            last = state
            if state[0] >= expected and state[1] >= expected:
                return
        time.sleep(2)
    raise SystemExit(f"[agent_reset] agentd 恢复后 {timeout}s 内未达到 {expected} 个 Ready"
                     f"（当前 {last}），退出")


def _agent_state_reset_cmd(node):
    """清节点上 agentd 的订单 ring 与块/隔离目录内容（保留目录本身）。"""
    script = ("for d in " + " ".join(_AGENTD_STATE_DIRS) +
              '; do [ -d "$d" ] && find "$d" -mindepth 1 -maxdepth 1 -exec rm -rf {} +; done; true')
    return ["docker", "exec", node, "sh", "-c", script]


def _agent_state_check_cmd(node):
    """判空三个目录（目录不存在视为空）。"""
    script = ("for d in " + " ".join(_AGENTD_STATE_DIRS) +
              '; do [ -d "$d" ] && ls -A "$d"; done; true')
    return ["docker", "exec", node, "sh", "-c", script]


def _clear_agent_node_state(tag):
    """逐节点清订单 ring 与 traincache 块缓存，清后判空；每个节点最多试 3 次。"""
    for node in KIND_WORKER_NODES:
        for attempt in range(3):
            rc, _out, err = run_cmd(_agent_state_reset_cmd(node), timeout=30)
            if rc != 0:
                print(f"[{tag}] WARNING: 节点 {node} 清 agent 状态失败(rc={rc}): "
                      f"{err[:200]}", flush=True)
            rc, out, err = run_cmd(_agent_state_check_cmd(node), timeout=30)
            if rc == 0 and not out.strip():
                break
            if attempt < 2:
                time.sleep(1)
        else:
            detail = out.strip() if rc == 0 else err[:200]
            raise SystemExit(f"[{tag}] 节点 {node} 的订单/块缓存未能清空"
                             f"（rc={rc}）: {detail[:300]}，退出")


def _wait_agentd_clean(expected, timeout=AGENTD_CLEAN_TIMEOUT_S):
    """确认 degraded=0 且没有 reader：fetch 计数两次采样不增长。"""
    start = time.monotonic()
    snap = None
    while time.monotonic() - start < timeout:
        snap = agent_degraded_snapshot()
        if snap is not None and snap["pods"] >= expected and snap["max"] == 0:
            break
        snap = None
        time.sleep(2)
    if snap is None:
        raise SystemExit(f"[agent_reset] agentd 恢复后 {timeout}s 内未确认 "
                         f"degraded=0（需 {expected} 个 Pod），退出")
    first = fetch_snapshot_via_kubectl()
    time.sleep(2)
    second = fetch_snapshot_via_kubectl()
    if first is None or second is None:
        raise SystemExit("[agent_reset] 无法采样 fetch 计数，不能确认无 reader，退出")
    after = agent_degraded_snapshot()
    if after is None or after["max"] != 0:
        raise SystemExit("[agent_reset] 复位后 agentd 再次降级，退出")
    if second["total"] > first["total"]:
        raise SystemExit(f"[agent_reset] 复位后 fetch 计数仍在增长"
                         f"（{first['total']}→{second['total']}），存在 reader，退出")


def _assert_agentd_sockets(expected):
    """每个 agentd Pod 内必须能看到 hook socket；滚动重启的旧 Pod 会 unlink 它。"""
    pods = _agentd_pod_names()
    if pods is None or len(pods) < expected:
        raise SystemExit(f"[agent_reset] agentd Pod 数不足（{pods}），无法检查 hook socket，退出")
    missing = []
    for pod in pods:
        rc, _out, _err = kubectl(
            ["exec", pod, "--", "test", "-S", "/run/traincache/agent.sock"],
            namespace=AGENT_NS, timeout=20)
        if rc != 0:
            missing.append(pod)
    if missing:
        raise SystemExit(f"[agent_reset] 以下 agentd Pod 缺少 "
                         f"/run/traincache/agent.sock: {', '.join(missing)}，退出")


def _assert_agent_metrics_families(expected):
    """每个 agentd Pod 的 /metrics 必须暴露回源字节指标族。

    source_saving_ratio 与三笔字节成本全靠它；新镜像若改了指标名，
    这里直接失败，比跑完才发现每一行都是空列/假 1.0 便宜。
    """
    texts = _agent_metrics_texts()
    if texts is None:
        raise SystemExit("[agent_reset] 抓不到任何 agentd /metrics，退出")
    missing = [pod for pod, txt in texts
               if not any(_FETCH_LINE_RE.match(line) for line in txt.splitlines())]
    if len(texts) < expected or missing:
        raise SystemExit(
            f"[agent_reset] agentd /metrics 缺少 traincache_fetch_bytes_total"
            f"（需 {expected} 个 Pod，抓到 {len(texts)}；缺: {', '.join(missing) or '无'}），"
            f"退出")


def reset_traincache_agent():
    """traincache 组每 rep 开跑前复位 agentd（只对 traincache 组调用）。

    顺序：停到 0 → 等 Pod 全消失 → 清订单 ring 与块/隔离目录并判空 →
    恢复 1 → 等 4 个 Pod Ready → 等 degraded=0 且 fetch 计数不增长 →
    校验每个 Pod 的 hook socket 存在。
    任一步失败抛 SystemExit，让该 rep 记 void/up_failed，不带着残余状态继续跑。
    """
    expected = len(KIND_WORKER_NODES)
    print(f"[agent_reset] 复位 traincache agentd（{expected} 个节点）...", flush=True)
    _agentd_set_active(False)
    try:
        _wait_agentd_hidden()
        _clear_agent_node_state("agent_reset")
    except BaseException:
        # 失败也必须把 agentd 拉回可用态，否则挂起状态会留到后续格。
        try:
            _agentd_set_active(True)
        except Exception as exc:
            print(f"[agent_reset] WARNING: 失败后恢复 agentd 也失败: {exc}", flush=True)
        raise
    _agentd_set_active(True)
    _wait_agentd_ready(expected)
    _wait_agentd_clean(expected)
    _assert_agentd_sockets(expected)
    _assert_agent_metrics_families(expected)
    print("[agent_reset] agentd 复位完成：degraded=0，fetch 计数停增，hook socket 与指标族就绪",
          flush=True)


# ---------------------------------------------------------------------------
# 6.12 统一轨迹：现场生成、校验、算 sha256（七组态按 track+档位共用一份）
# ---------------------------------------------------------------------------
def build_run_trace():
    """按 run_bench 的规模参数生成 trace dict（trace.py build_trace 唯一入口）。"""
    if BATCH <= 0 or IMAGES_PER_TAR <= 0:
        raise SystemExit("[trace] BATCH / IMAGES_PER_TAR 必须 >0")
    if IMAGES_PER_TAR % BATCH != 0:
        print(f"[trace] WARNING: images_per_tar({IMAGES_PER_TAR}) 不是 batch"
              f"({BATCH}) 的整数倍，每步样本数取整会有 1 步内误差")
    return trace_mod.build_trace(
        n_tars=N_TARS, seed=TRACE_SEED, shuffle=TRACE_SHUFFLE,
        epochs=EPOCHS, batch=BATCH, steps_per_epoch=STEPS_PER_EPOCH,
        images_per_tar=IMAGES_PER_TAR)


# JuiceFS mount Pod 所在节点，由 group_up 从 up.sh 输出回收；空=未起组。
JFS_MOUNT_NODE = ""


def group_up(group):
    """起组，并回收 up.sh 回传的两个装置事实：预热时长与 JuiceFS 挂载节点。

    JFS_MOUNT_NODE：CE 单机缓存只落在 mount Pod 所在节点，测量 Pod 必须共置，
    否则 warm 组暖好的块在另一台节点读不到（见 docs/4 §7 第 16 条）。
    """
    global JFS_MOUNT_NODE
    JFS_MOUNT_NODE = ""
    print(f"[up] {group}...")
    env = os.environ.copy()
    env["N_TARS"] = str(N_TARS)
    # D30/D31：把档位与带宽档显式下发给 up.sh —— burst 由 tc_apply 按 rate 推导，
    # netem 只有固定 delay 一种形态，两侧不再各处写死。
    env["CAPACITY_RATIO"] = TIER
    env["TC_RATE"] = TC_RATE_EFF
    env["TC_DELAY"] = TC_DELAY_EFF
    env["TRAIN_IMAGE"] = TRAIN_IMAGE
    env["MINIO_ACCESS_KEY"] = MINIO_ACCESS_KEY
    env["MINIO_SECRET_KEY"] = MINIO_SECRET_KEY
    env["RUN_BENCH"] = "1"
    # 6.13：块大小与缓存容量统一下发（DataSet CRD chunkSize、trainpublish
    # -chunk-size、WarmupTask k 全部以这里为唯一口径）
    env["CHUNK_BYTES"] = str(CHUNK_SIZE)
    env["TRAINCACHE_CACHE_CAPACITY_BYTES"] = str(CACHE_BYTES)
    # 统一缓存容量 1GiB(数据 2GiB+ > 缓存,模拟真实场景；D19 三档缓存固定 1GiB)
    if group.startswith("fluid"):
        env["FLUID_CACHE_GI"] = str(max(1, CACHE_BYTES >> 30))
        env["FLUID_PREHEAT"] = "1" if group == "fluid-warm" else "0"
    if group.startswith("juicefs"):
        env["JFS_CACHE_MB"] = str(CACHE_BYTES >> 20)  # --cache-size 单位是 MiB(docs/4 §3)
        # juicefs-warm:up.sh 里自动做 juicefs warmup;juicefs-cold:跳过
        env["JFS_WARMUP"] = "1" if group == "juicefs-warm" else "0"
    if group.startswith("traincache"):
        # warm 预热 k=WARMUP_K（8MiB 块 → 128 = 整缓存容量；6.13/D20 重标度）；
        # cold 传 0 作 bench 哨兵，up.sh 跳过 WarmupTask 创建
        env["TRAINCACHE_WARMUP_K"] = str(WARMUP_K) if group == "traincache-warm" else "0"
        env["TRAINCACHE_PREHEAT_FANOUT"] = str(PREHEAT_FANOUT)
        env["TRAINCACHE_PREHEAT_FANOUT_MIN"] = str(PREHEAT_FANOUT_MIN)
        env["DATASET"] = DATASET_NAME
        # D23：网关开关透传到 ctrl_up.sh，决定 gatewayEndpoint 与网关副本数
        env["TRAINCACHE_GATEWAY"] = "1" if GATEWAY_ON else "0"
    rc, out, err = run_cmd(["bash", str(GROUP_UP[group])], env=env, timeout=3600)
    if rc != 0:
        print(f"[up] {group} 失败(rc={rc}): {err[:800]}")
        return False, 0.0, "wall"
    # 解析 up.sh 输出中的预热时长标记
    warmup_seconds = 0.0
    warmup_basis = "wall"
    for line in out.splitlines() + err.splitlines():
        m = re.search(r"\[WARMUP_SECONDS\]\s+(\d+)", line)
        if m:
            warmup_seconds = float(m.group(1))
            continue
        m = re.search(r"\[WARMUP_BASIS\]\s+(\S+)", line)
        if m:
            warmup_basis = m.group(1)
            continue
        m = re.search(r"\[MOUNT_NODE\]\s+(\S+)", line)
        if m:
            JFS_MOUNT_NODE = m.group(1)
    if group.startswith("juicefs") and not JFS_MOUNT_NODE:
        print(f"[up] {group} 失败:up.sh 未回传 [MOUNT_NODE],无法共置", flush=True)
        return False, 0.0, warmup_basis
    if JFS_MOUNT_NODE:
        print(f"[up] {group} 挂载节点={JFS_MOUNT_NODE}(测量 Pod 将共置)")
    # direct / juicefs-cold / fluid-cold / traincache-cold 默认 0.0
    print(f"[up] {group} 预热时长={warmup_seconds}s (basis={warmup_basis})")
    return True, warmup_seconds, warmup_basis


def group_down(group):
    print(f"[down] {group}...")
    env = os.environ.copy()
    env["RUN_BENCH"] = "1"
    if group.startswith("traincache"):
        env["DATASET"] = DATASET_NAME
    run_cmd(["bash", str(GROUP_DOWN[group])], env=env, timeout=300)


def build_measure_common():
    """测量脚本公共段（五组共用）：时钟口径、计数器、轨迹消费。

    整段拆成 common / reader / loop 三块，是为了 --selftest 能把 reader 换成
    一个假的 read_shard（返回已知字节数），离线跑一遍主循环验字节账。
    """
    return r'''
import json, os, time, urllib.request, threading, subprocess, queue
from pathlib import Path

endpoint = "''' + MINIO_ENDPOINT.rstrip("/") + r'''"
bucket = "''' + MINIO_BUCKET + r'''"
n_tars = ''' + str(N_TARS) + r'''
epochs = ''' + str(EPOCHS) + r'''
steps = ''' + str(STEPS_PER_EPOCH) + r'''
batch = ''' + str(BATCH) + r'''
images_per_tar = ''' + str(IMAGES_PER_TAR) + r'''
group = os.environ.get("TRAINCACHE_GROUP", "direct")

# ---- 6.12 统一轨迹消费（七组态共用一份 trace.json，runner 现场生成并挂载）----
TRACE_PATH = os.environ.get("TRAINCACHE_TRACE_PATH", "/scripts/trace.json")
_trace = json.loads(Path(TRACE_PATH).read_text())
if (_trace["n_tars"] != n_tars or _trace["epochs"] != epochs
        or _trace["steps_per_epoch"] != steps or _trace["batch"] != batch):
    raise SystemExit("trace.json 与本次运行参数不一致: " + TRACE_PATH)
# 每个 epoch 的分片消费序列：shard_perm 按 repeats 循环展开。
# 一步消费 batch 个样本；分片(含 images_per_tar 样本)读一遍横跨
# images_per_tar/batch 步 —— 与 bench_track_d.py 的 epoch 语义对齐：
# 一个 epoch 恰好把该 stage 的数据按轨迹顺序过一遍。
epoch_occ = []
for _ep in _trace["per_epoch"]:
    _perm = _ep["shard_perm"]
    _rep = max(1, int(_ep.get("repeats", 1)))
    epoch_occ.append([_perm[_i % len(_perm)] for _i in range(len(_perm) * _rep)])

# ---- 时钟口径（2026-10-24 修）----
# 两把钟各管各的，别混用：
#   time.perf_counter() 单调钟：测时长用（wait_s / work_s / read_s / step_s）。
#       wall clock 会被 NTP 校正拽一下产生离群步，计时必须用单调钟。
#   now_epoch() 纪元时间戳：跨进程对齐用（t_submit 来自 runner 的 time.time()，
#       [EPOCH_BOUNDARY] ts 要和 runner 侧 fetch 采样同一把尺子）。
#       它由单调钟推算（起跑时对一次纪元），Pod 内 NTP 校正不会让它跳。
_t0_perf = time.perf_counter()
_t0_epoch = time.time()


def now_epoch():
    """纪元时间戳（秒），单调钟推算，跨进程对齐用。"""
    return _t0_epoch + (time.perf_counter() - _t0_perf)


_t_submit_env = os.environ.get("TRAINCACHE_JOB_CREATED_AT", "").strip()
t_submit = float(_t_submit_env) if _t_submit_env else now_epoch()
t_first_step = None
t_last_step = None
stall_total = 0
step_total = 0
read_bytes_total = 0
read_done = 0
hits = 0
misses = 0
# hits/misses 由多个 reader 线程并发自增：读改写非原子会丢计数，必须加锁。
counter_lock = threading.Lock()
# 队列只传"字节数"不传数据体 → 真实常驻内存 O(1)（与深度/分片大小无关）；
# 真实加载器会持有的字节数用纯计数器跟踪（不真持有 bytes），峰值见 RESULT。
logical_resident_bytes = 0
peak_resident_bytes = 0
# 读队列超时/读失败次数：>0 说明这轮读侧没跟上，该轮作废（见主循环分支）。
queue_timeouts = 0
read_errors = 0
measure_void = False
# 快照覆盖校验（9.2）：epoch_occ 里出现但 snapshot manifest 里没有的分片名。
# 非空时不能当 0 字节静默读，本轮直接作废（见主循环前的检查）。
snapshot_missing = False
# 分阶段字节账（9.2）：read_shard 按 phase 累计读到的字节；
# 回源分支（块不存在、走 HTTP 兜底）另计 origin 字节。
read_bytes_phase = {"startup": 0, "steady": 0, "switch": 0}
demand_miss_phase = {"startup": 0, "steady": 0, "switch": 0}

# 分段读粒度：读出来的字节只累加长度、不留在内存里（避免整片 256MiB 常驻）。
READ_SEGMENT_BYTES = ''' + str(READ_SEGMENT_BYTES) + r'''


def read_file_bytes(path):
    """分段读文件，返回总字节数（不保留内容）。"""
    n = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(READ_SEGMENT_BYTES)
            if not b:
                break
            n += len(b)
    return n


def http_stream_bytes(url):
    """分段读 HTTP 全量响应，返回总字节数（不保留内容）。"""
    n = 0
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=120) as r:
        while True:
            b = r.read(READ_SEGMENT_BYTES)
            if not b:
                break
            n += len(b)
    return n


def http_get(url, range_start=None, range_end=None):
    req = urllib.request.Request(url)
    if range_start is not None:
        req.add_header("Range", f"bytes={range_start}-{range_end}")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def add_resident(nbytes):
    """记一次"分片进了预读窗口"（纯计数，不持有 bytes）。"""
    global logical_resident_bytes, peak_resident_bytes
    with counter_lock:
        logical_resident_bytes += nbytes
        if logical_resident_bytes > peak_resident_bytes:
            peak_resident_bytes = logical_resident_bytes


def drop_resident(nbytes):
    """记一次"分片被主循环消费掉"。"""
    global logical_resident_bytes
    with counter_lock:
        logical_resident_bytes -= nbytes

# matmul compute unit (默认 64x64 单线程,~几十毫秒量级;标定按它累加出目标算力)
def matmul(n=64):
    import array
    a = array.array('f', [1.0]*n*n)
    b = array.array('f', [1.0]*n*n)
    c = array.array('f', [0.0]*n*n)
    for i in range(n):
        for j in range(n):
            s = 0.0
            for k in range(n):
                s += a[i*n+k] * b[k*n+j]
            c[i*n+j] = s
'''


def build_measure_reader(group):
    """组专属读路径：read_shard 返回"本片字节数"，不返回 bytes 对象。

    队列里只放字节数 → 常驻内存 O(1)；字节账与旧版一致（逐段累加长度，
    旧版是把整片 bytes 拼起来再取 len，两者总数相同）。
    """
    if group == "direct":
        return r'''
def read_shard(shard, phase):
    """直读 MinIO：分段读整片，只累加长度（不把 256MiB 读回来常驻内存）。"""
    global misses
    url = endpoint + "/" + bucket + "/shard-" + str(shard).zfill(5) + ".tar"
    nbytes = http_stream_bytes(url)
    with counter_lock:
        misses += 1  # 直读无缓存层，全部计回源
        read_bytes_phase[phase] = read_bytes_phase.get(phase, 0) + nbytes
    return nbytes
'''
    elif group in ("juicefs-cold", "juicefs-warm"):
        return r'''
def read_shard(shard, phase):
    global hits, misses
    p = "/mnt/jfs/shard-" + str(shard).zfill(5) + ".tar"
    nbytes = read_file_bytes(p)
    # JuiceFS 无法输出块级命中指标：按 D25 近似——warm 组 warmup 后读≈全命中，
    # cold 组≈全回源（分片粒度计数，产物 hit_rate_basis 列显式标注近似）。
    with counter_lock:
        read_bytes_phase[phase] = read_bytes_phase.get(phase, 0) + nbytes
        if os.environ.get("TRAINCACHE_GROUP") == "juicefs-warm":
            hits += 1
        else:
            misses += 1
    return nbytes
'''
    elif group.startswith("fluid"):
        return r'''
def read_shard(shard, phase):
    global hits, misses
    p = "/data/shard-" + str(shard).zfill(5) + ".tar"
    nbytes = read_file_bytes(p)
    # Fluid 同样无块级命中指标，D25 近似口径（见 JuiceFS 注释）。
    with counter_lock:
        read_bytes_phase[phase] = read_bytes_phase.get(phase, 0) + nbytes
        if os.environ.get("TRAINCACHE_GROUP") == "fluid-warm":
            hits += 1
        else:
            misses += 1
    return nbytes
'''
    elif group.startswith("traincache"):
        return r'''
import urllib.parse, subprocess, socket
CACHE_ROOT = "/var/cache/traincache/blocks"
ORDERS_DIR = "/run/traincache/orders"
POD_UID = os.environ.get("POD_UID", "unknown-pod")
DATASET = os.environ.get("TRAINCACHE_DATASET", "compare-data-8m")
# 6.13/D20：实验块 8MiB（run_bench.py 经 TRAINCACHE_CHUNK_SIZE 下发，勿写死）
CHUNK_SIZE = int(os.environ.get("TRAINCACHE_CHUNK_SIZE", "8388608"))
# D21：对照三档统一 lookahead=16（与 runner 侧 PREFETCH_K 默认值、agent/svc
# assemble.go 的 prefetchLookahead=16 三处对齐）。这里只是 env 缺失时的兜底，
# 正式跑分由 Pod env 下发（run_bench.py create_measure_pod）。
PREFETCH_K = int(os.environ.get("TRAINCACHE_PREFETCH_K", "16"))
# agentd hook：Unix datagram，行分隔 JSON；测量 Pod 经 hostPath 挂载节点
# /run/traincache 到 /run/tc-hook。发送失败静默忽略，不影响读数与字节账。
HOOK_SOCK = os.environ.get("TRAINCACHE_HOOK_SOCK", "/run/tc-hook/agent.sock")
_chunk_map = None
_order_written = False

def _hook_identity():
    """按 env 组装钩子帧可选任务身份（8.0 口径）；缺失的键不出现，旧帧逐字一致。"""
    ident = {}
    task = os.environ.get("TRAINCACHE_TASK")
    if task:
        ident["warmup_task"] = task
    pod = os.environ.get("TRAINCACHE_POD")
    if pod:
        ident["pod"] = pod
    try:
        ident["pod_index"] = int(os.environ.get("TRAINCACHE_POD_INDEX", ""))
    except ValueError:
        pass
    dataset = os.environ.get("TRAINCACHE_DATASET")
    if dataset:
        ident["dataset"] = dataset
    try:
        ident["generation"] = int(os.environ.get("TRAINCACHE_GENERATION", ""))
    except ValueError:
        pass
    return ident

_HOOK_IDENTITY = _hook_identity()

def _hook_send(frame):
    """best-effort 上报一帧；每次新建 socket，多读线程并发安全。

    帧身份字段仅在 env 有值时附在原有键之后；env 全缺省时与旧版帧内容逐字一致。
    """
    if _HOOK_IDENTITY:
        frame = dict(frame)
        frame.update(_HOOK_IDENTITY)
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            s.sendto((json.dumps(frame, separators=(",", ":")) + "\n").encode(),
                     HOOK_SOCK)
        finally:
            s.close()
    except OSError:
        pass

def _hook_advance(name, chunk_idx):
    _hook_send({"type": "advance", "path": name, "chunk": int(chunk_idx)})

def _hook_read_miss(name, chunk_idx):
    _hook_send({"type": "read_miss", "path": name, "chunk": int(chunk_idx),
                "reason": "absent"})

# Heartbeat:每 10s 重写当前 epoch 的订单(内容相同,ordersource 按字节去重不推送,
# 但 write_seq 推进避免 collector 因 ring 停滞 30s 降级到 inotify 兜底)。
_hb_stop = threading.Event()
_hb_epoch = [0]

def _heartbeat_loop():
    while not _hb_stop.is_set():
        try:
            _write_order(str(_hb_epoch[0]))
        except Exception:
            pass
        _hb_stop.wait(10)

def _load_snapshot():
    global _chunk_map
    if _chunk_map is not None:
        return _chunk_map
    ds = os.environ.get("TRAINCACHE_DATASET", "compare-data-8m")
    base = endpoint + "/" + bucket + "/dataset/" + ds
    cur = http_get(base + "/CURRENT").decode().strip().split()[0]
    manifest = json.loads(http_get(base + "/snapshots/" + cur + "/manifest.json"))
    m = {}
    for sh in manifest["shards"]:
        idx = json.loads(http_get(endpoint + "/" + bucket + "/" + sh["key"]))
        for e in idx:
            m.setdefault(e["path"], []).append((e["chunk"], e["fp"], e["size"]))
    for k in m:
        m[k].sort()
    _chunk_map = m
    return m

def _write_order(stage_id=""):
    """写入当前 epoch 的消费顺序到 ring 文件,触发 agentd 预取。
    6.12:顺序取 trace.json 该 epoch 的分片消费序列(perm 按 repeats 循环),
    不再是线性序;每个 epoch 调用一次(stage_id 不同),体现阶段切换重预取。"""
    chunks = []
    seen = set()
    snap = _load_snapshot()
    for shard in epoch_occ[int(stage_id)]:
        name = "shard-" + str(shard).zfill(5) + ".tar"
        if name in seen:
            continue
        seen.add(name)
        for chunk_idx, fp, size in snap.get(name, []):
            chunks.append(f"{name}:{chunk_idx}")
    try:
        subprocess.run([
            "python3", "/app/ordersrc.py",
            "--pod-uid", POD_UID,
            "--dataset", DATASET,
            "--chunks", ",".join(chunks),
            "--prefetch-chunks", str(PREFETCH_K),
            "--stage-id", stage_id,
        ], check=True, capture_output=True, timeout=30)
        print(f"[ordersrc] stage={stage_id} wrote {len(chunks)} chunks, K={PREFETCH_K}", flush=True)
    except Exception as e:
        print(f"[ordersrc] WARNING: write order failed: {e}", flush=True)

def read_shard(shard, phase):
    """traincache 读取路径：逐块读本地缓存，缺块等待 agentd 预取，超时兜底回源。
    返回本片字节数（累加各块长度，不把整片拼起来留在内存里）。
    注意：hits/misses 只是块级诊断(block_hit_rate_diag 列)——"块在磁盘存在即
    hit"口径会虚高(agent 运行期悄悄回源补块)，已作废(9.1 D25)；
    正式命中率由 runner 侧用 open-time 的 demand_miss 字节账计算。
    分阶段字节（9.2）：每个块都计入 read_bytes_phase[phase]；块不存在、走
    等待/回源分支时另计入 demand_miss_phase[phase]（按块 size 计）。"""
    global hits, misses
    name = "shard-" + str(shard).zfill(5) + ".tar"
    chunks = _load_snapshot().get(name, [])
    nbytes = 0
    n_hits = 0
    n_misses = 0
    for chunk_idx, fp, size in chunks:
        with counter_lock:
            read_bytes_phase[phase] = read_bytes_phase.get(phase, 0) + size
        blk = os.path.join(CACHE_ROOT, fp[:2], fp + ".blk")
        # 命中:直接读本地缓存
        if os.path.exists(blk):
            nbytes += read_file_bytes(blk)
            n_hits += 1
            _hook_advance(name, chunk_idx)
        else:
            # 未命中:先报 agent 触发 P0 demand 补块,再等持续预取(轮询,最多等 180s)
            with counter_lock:
                demand_miss_phase[phase] = demand_miss_phase.get(phase, 0) + size
            _hook_read_miss(name, chunk_idx)
            waited = 0.0
            while not os.path.exists(blk) and waited < 180:
                time.sleep(0.5)
                waited += 0.5
            if os.path.exists(blk):
                nbytes += read_file_bytes(blk)
                n_hits += 1
                _hook_advance(name, chunk_idx)
            else:
                # 超时 fallback:HTTP 回源(agentd 预取未跟上)
                url = endpoint + "/" + bucket + "/" + name
                start = chunk_idx * CHUNK_SIZE
                end = start + size - 1
                nbytes += len(http_get(url, start, end))
                n_misses += 1
                _hook_advance(name, chunk_idx)
    with counter_lock:
        hits += n_hits
        misses += n_misses
    return nbytes
'''
    return "\n"


def build_measure_loop():
    """主循环段：后台读线程预取 + 主循环取数算力 + 四个指标统计。"""
    return r'''
# ================= 并行测量模板参数(全部经 Pod env 下发,默认值见 run_bench.py) =================
COMPUTE_MS = int(os.environ.get("COMPUTE_MS", "200"))             # 每步目标算力耗时(毫秒)
READ_WORKERS = int(os.environ.get("READ_WORKERS", "2"))           # 后台读线程数(建议 1~2)
READ_QUEUE_DEPTH = int(os.environ.get("READ_QUEUE_DEPTH", "2"))   # 预读队列深度(接下来 N 个分片在窗口里)
READ_TIMEOUT_S = float(os.environ.get("READ_TIMEOUT_S", "300"))   # 队头等待上限(秒,防死锁)
# 队列元素只带"字节数"不带数据体 → 队列常驻内存 O(1)，与队列深度、分片大小
# 都无关（不再是 (深度+线程数+1)×分片大小 的常驻，那是旧版传 bytes 的账）。
# 真实加载器会持有的字节数(≈(深度+线程数+1)×分片大小)改用纯计数
# logical_resident_bytes 跟踪（不真持有 bytes），峰值见 RESULT 的
# peak_resident_bytes。旧串行版产物的 stall_ratio 口径作废
# （docs/4 §10）
# 口径(9.2)：
#   wait_s = 主循环从队列取"已读好分片"的阻塞等待(真 stall);
#   work_s = 墙钟算力(compute_work 循环 matmul 直到实测 >= COMPUTE_MS，取实测长度);
#   stall 样本 = 每步 wait_s / work_s，聚合取 mean 与 p90（不再用 Σwait/Σstep）。
#   epoch0 前 10 步是冷启动填充段，不进 stall 聚合。
#   预取有效(数据在本地缓存、读得快) -> stall 样本趋 0;
#   预取失效(要现回源) -> 队列常被抽空 -> 趋 1。

# 算力标定:先测 matmul 单位耗时(3 次取中位,抑噪),再算每步重复次数供日志参考。
# 实际算力按墙钟截断（COMPUTE_REPS 只作估算值打印，不再决定一步的长度）。
_matmul_unit_s = 0.0

def _one_matmul():
    t = time.perf_counter()
    matmul()
    return time.perf_counter() - t

def calibrate_compute_reps():
    global _matmul_unit_s
    samples = [_one_matmul() for _ in range(3)]   # 首次含解释器预热抖动,由中位数吸收
    _matmul_unit_s = sorted(samples)[len(samples) // 2]
    if _matmul_unit_s <= 0:
        _matmul_unit_s = 0.001
    return max(1, int(round(COMPUTE_MS / 1000.0 / _matmul_unit_s)))

COMPUTE_REPS = calibrate_compute_reps()
print("[calib] compute_ms=" + str(COMPUTE_MS) +
      " matmul_unit_ms=" + str(round(_matmul_unit_s * 1000, 3)) +
      " compute_reps=" + str(COMPUTE_REPS) +
      " read_workers=" + str(READ_WORKERS) +
      " queue_depth=" + str(READ_QUEUE_DEPTH), flush=True)

def compute_work():
    """墙钟算力:循环 matmul 直到实测耗时 >= COMPUTE_MS,返回实测耗时(秒)。

    固定重复次数在不同 Pod 上实测 148~396ms（组间差异被算力噪声吃掉），
    改为按墙钟截断后每步算力在步与步之间可比，stall/step 才归因到 I/O。
    至少执行一次 matmul。
    """
    budget = COMPUTE_MS / 1000.0
    t = time.perf_counter()
    while True:
        matmul()
        if time.perf_counter() - t >= budget:
            break
    return time.perf_counter() - t

# ================= 倾斜双 Job 模式(M5, TRAINCACHE_TILT=1) =================
# Job A 循环读前 10% 热分片,Job B 顺序扫全量。两者同进程并发,共用一个
# agentd 与节点 hostPath 缓存 —— B 的扫描会把 A 的热块挤出缓存。
# 记录:A 的块级命中率、A 的等待占比、B 挤占热块的次数。
if os.environ.get("TRAINCACHE_TILT", "") == "1":
    import random
    if not group.startswith("traincache"):
        raise SystemExit("TILT 只支持 traincache 组,当前 group=" + group)
    _n_hot = max(1, round(n_tars * 0.10))
    _hot = list(range(_n_hot))
    _full = list(range(n_tars))
    _stagger_s = float(os.environ.get("TILT_STAGGER_S", "5"))
    _max_a_cycles = int(os.environ.get("TILT_MAX_A_CYCLES", "100000"))
    _stop = threading.Event()
    _lock = threading.Lock()
    _a = {"hits": 0, "misses": 0, "evict": 0, "wait": 0.0, "step": 0.0,
          "reads": 0, "samples": []}
    _b = {"reads": 0}
    _hot_present = set()
    _b_done = threading.Event()

    def _chunk_count(shard):
        name = "shard-" + str(shard).zfill(5) + ".tar"
        return len(_load_snapshot().get(name, []))

    def _present_fps(shard):
        name = "shard-" + str(shard).zfill(5) + ".tar"
        out = set()
        for _ci, fp, _sz in _load_snapshot().get(name, []):
            if os.path.exists(os.path.join(CACHE_ROOT, fp[:2], fp + ".blk")):
                out.add(fp)
        return out

    def _job_a():
        global measure_void, queue_timeouts, read_errors, read_bytes_total, read_done
        rng = random.Random(int(os.environ.get("TILT_SEED", "20261015")))
        cycle = 0
        while not _stop.is_set() and cycle < _max_a_cycles:
            order = list(_hot)
            rng.shuffle(order)
            for shard in order:
                if _stop.is_set():
                    return
                t0 = time.perf_counter()
                present = _present_fps(shard)
                total_chunks = _chunk_count(shard)
                with _lock:
                    _a["evict"] += len(_hot_present - present)
                    _hot_present.update(present)
                    _a["hits"] += len(present)
                    _a["misses"] += max(0, total_chunks - len(present))
                try:
                    nb = read_shard(shard, "steady")
                except Exception as e:
                    with _lock:
                        read_errors += 1
                        measure_void = True
                    print("ERROR tilt job A shard=" + str(shard) + ": " + str(e),
                          flush=True)
                else:
                    with _lock:
                        read_bytes_total += nb
                        read_done += 1
                wait_s = time.perf_counter() - t0
                work_s = compute_work()
                step_s = wait_s + work_s
                with _lock:
                    _a["reads"] += 1
                    _a["wait"] += wait_s
                    _a["step"] += step_s
                    if work_s > 0:
                        _a["samples"].append(wait_s / step_s)
                if t_first_step is None:
                    _set_first_step()
            cycle += 1

    def _set_first_step():
        global t_first_step
        if t_first_step is None:
            t_first_step = now_epoch()

    def _job_b():
        global measure_void, queue_timeouts, read_errors, t_last_step
        global read_bytes_total, read_done
        if _stagger_s > 0:
            time.sleep(_stagger_s)
        for ep in range(epochs):
            if _stop.is_set():
                return
            _hb_epoch[0] = ep
            print("[EPOCH_BOUNDARY] epoch=" + str(ep) + " ts=" + str(now_epoch()) +
                  " at_step=" + str(ep * steps) + " kind=" +
                  ("epoch" if ep > 0 else "start"), flush=True)
            for shard in _full:
                try:
                    nb = read_shard(shard, "steady")
                except Exception as e:
                    with _lock:
                        read_errors += 1
                        measure_void = True
                    print("ERROR tilt job B shard=" + str(shard) + ": " + str(e),
                          flush=True)
                else:
                    with _lock:
                        read_bytes_total += nb
                        read_done += 1
                with _lock:
                    _b["reads"] += 1
                _set_first_step()
                t_last_step = now_epoch()
        _b_done.set()
        _stop.set()

    _hb_epoch[0] = 0
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    _write_order("0")
    time.sleep(2)  # 给 agentd 一点时间启动预取
    _th_a = threading.Thread(target=_job_a, daemon=True)
    _th_b = threading.Thread(target=_job_b, daemon=True)
    _th_a.start()
    _th_b.start()
    _th_b.join()
    _th_a.join(timeout=float(os.environ.get("TILT_A_JOIN_S", "120")))
    _hb_stop.set()

    _a_total = _a["hits"] + _a["misses"]
    _a_hit_ratio = _a["hits"] / _a_total if _a_total else ""
    _a_wait_ratio = _a["wait"] / _a["step"] if _a["step"] > 0 else ""
    _samples = sorted(_a["samples"])
    _a_wait_p90 = (_samples[(9 * len(_samples) + 9) // 10 - 1]
                   if _samples else "")
    _startup = (t_first_step - t_submit) if t_first_step else 0
    _total = (t_last_step - t_submit) if t_last_step else 0
    _hit = hits / (hits + misses) if (hits + misses) > 0 else 0
    _tilt_parts = [
        "startup_makespan=" + str(round(_startup, 3)),
        "total_makespan=" + str(round(_total, 3)),
        "stall_ratio=" + (str(round(_a_wait_ratio, 4)) if _a_wait_ratio != "" else ""),
        "stall_p90=" + (str(round(_a_wait_p90, 4)) if _a_wait_p90 != "" else ""),
        "stall_n=" + str(len(_samples)),
        "compute_actual_ms=",
        "compute_reps=" + str(COMPUTE_REPS),
        "hit_rate=" + str(round(_hit, 4)),
        "total_read_bytes=" + str(read_bytes_total),
        "reads=" + str(read_done),
        "hits=" + str(hits),
        "misses=" + str(misses),
        "void=" + str(1 if measure_void else 0),
        "snapshot_missing=" + str(1 if snapshot_missing else 0),
        "queue_timeouts=" + str(queue_timeouts),
        "read_errors=" + str(read_errors),
        "peak_resident_bytes=" + str(peak_resident_bytes),
        "a_hit_ratio=" + (str(round(_a_hit_ratio, 4)) if _a_hit_ratio != "" else ""),
        "a_wait_ratio=" + (str(round(_a_wait_ratio, 4)) if _a_wait_ratio != "" else ""),
        "b_evictions_of_hot=" + str(_a["evict"]),
        "a_steps=" + str(_a["reads"]),
        "b_steps=" + str(_b["reads"]),
        "tilt_hot_shards=" + str(_n_hot),
    ]
    for _p in ("startup", "steady", "switch"):
        _tilt_parts.append("read_bytes_" + _p + "=" + str(read_bytes_phase.get(_p, 0)))
        _tilt_parts.append("demand_miss_bytes_" + _p + "=" +
                           str(demand_miss_phase.get(_p, 0)))
    print("RESULT " + " ".join(_tilt_parts), flush=True)
    raise SystemExit(0)

def read_pipeline(shards, phase_seq, q, cursor, cursor_lock):
    """后台读线程:把 shards 里的分片读进有界队列 q。
    队列元素是 (shard, nbytes, err, read_s) —— 只带字节数不带数据体,
    队列常驻内存 O(1);真实加载器会持有的字节数由 add_resident 纯计数跟踪。
    队列满 -> put 阻塞,天然限流;每个分片恰好入队一次(字节数或异常)。
    phase_seq[i] 是该分片所属阶段(startup/switch/steady)，原样传给 read_shard
    做分阶段字节账。"""
    while True:
        with cursor_lock:
            i = cursor[0]
            if i >= len(shards):
                return
            cursor[0] = i + 1
        shard = shards[i]
        phase = phase_seq[i]
        r0 = time.perf_counter()
        try:
            nbytes = read_shard(shard, phase)
        except Exception as e:
            q.put((shard, 0, str(e), time.perf_counter() - r0))
        else:
            add_resident(nbytes)
            q.put((shard, nbytes, None, time.perf_counter() - r0))

# 分阶段判定(9.2)：一个 epoch 的前 k_shards 个分片是"预取窗口内的头段"——
# epoch0 记 startup（启动预热/首轮填充），epoch>0 记 switch（阶段切换重预取），
# 其余记 steady（持续预取）。窗口按块数折算成分片数：
#   chunks_per_shard = SHARD_SIZE_MB*1024*1024 // CHUNK_SIZE
#   k_shards = max(1, ceil(PREFETCH_K / chunks_per_shard))
_SHARD_BYTES = ''' + str(SHARD_SIZE_MB) + r''' * 1024 * 1024
_CHUNK_BYTES = int(os.environ.get("TRAINCACHE_CHUNK_SIZE", "''' + str(CHUNK_SIZE) + r'''"))
_PREFETCH_K_EFF = int(os.environ.get("TRAINCACHE_PREFETCH_K", "''' + str(PREFETCH_K) + r'''"))
_chunks_per_shard = max(1, _SHARD_BYTES // max(1, _CHUNK_BYTES))
_k_shards = max(1, (_PREFETCH_K_EFF + _chunks_per_shard - 1) // _chunks_per_shard)

# 快照覆盖校验(9.2)：epoch_occ 里要用到的分片必须都在 snapshot manifest 里。
# 缺片时不能当 0 块静默读成 0 字节（字节账与命中率都会被糊弄），本轮直接作废。
if group.startswith("traincache"):
    try:
        _snap_chk = _load_snapshot()
    except Exception as _e:
        snapshot_missing = True
        print("[SNAPSHOT_MISS] load snapshot failed: " + str(_e), flush=True)
    else:
        _seen_s = set()
        _wanted_s = []
        for _occ in epoch_occ:
            for _s in _occ:
                _nm = "shard-" + str(_s).zfill(5) + ".tar"
                if _nm in _seen_s:
                    continue
                _seen_s.add(_nm)
                if _nm not in _snap_chk:
                    _wanted_s.append(_nm)
        if _wanted_s:
            snapshot_missing = True
            print("[SNAPSHOT_MISS] " + str(len(_wanted_s)) + "/" + str(len(_seen_s)) +
                  " shards absent from snapshot: " + ",".join(_wanted_s[:5]), flush=True)

# stall 样本与墙钟算力样本（bad 步不进样本，但 wait 仍计入 stall_total 诊断账）
stall_samples = []
work_samples = []

for epoch in range(epochs):
    if snapshot_missing:
        measure_void = True
        break   # 快照缺片：直接落到末尾 RESULT（void=1），不跑空读数
    # TrainCache 组:每个 epoch 按 trace 顺序写消费订单,触发 agentd 阶段切换重预取
    if group.startswith("traincache"):
        _hb_epoch[0] = epoch
        if epoch == 0:
            threading.Thread(target=_heartbeat_loop, daemon=True).start()
        _write_order(str(epoch))
        time.sleep(2)  # 给 agentd 一点时间启动预取

    # 6.12: 本 epoch 的分片消费序列取 trace.json（不再是线性序）
    shards = epoch_occ[epoch]
    nocc = len(shards)
    # 9.2 分阶段序列：与 shards 等长，逐个分片传给 read_shard
    phase_seq = []
    for _i in range(nocc):
        if _i < _k_shards:
            phase_seq.append("startup" if epoch == 0 else "switch")
        else:
            phase_seq.append("steady")
    # epoch 切换标记:runner 侧把 prefetch 计数器增量归因到"切换重预取"窗口
    # ts 用纪元时间戳(与 runner 侧 fetch 采样同一把尺子)，但由单调钟推算
    print("[EPOCH_BOUNDARY] epoch=" + str(epoch) + " ts=" + str(now_epoch()) +
          " at_step=" + str(epoch * steps) + " kind=" +
          ("epoch" if epoch > 0 else "start"), flush=True)
    q = queue.Queue(maxsize=READ_QUEUE_DEPTH)
    cursor = [0]
    cursor_lock = threading.Lock()
    workers = []
    for _w in range(max(1, READ_WORKERS)):
        th = threading.Thread(target=read_pipeline,
                              args=(shards, phase_seq, q, cursor, cursor_lock), daemon=True)
        th.start()
        workers.append(th)

    cur_shard = None
    held_bytes = 0
    for step in range(steps):
        # 一步消费 batch 个样本；分片消费周期按 trace 顺序均摊到整步数，
        # 只在换分片时从队列取一次(取不到才是真 stall)
        occ = min((step * nocc) // steps, nocc - 1)
        want_shard = shards[occ]
        t0 = time.perf_counter()      # 单调钟：wait_s/work_s 的计时起点
        wait_s = 0.0
        bad = False
        if want_shard != cur_shard:
            try:
                shard, nbytes, err, read_s = q.get(timeout=READ_TIMEOUT_S)
            except queue.Empty:
                # 队头等满 READ_TIMEOUT_S 还没到数据：这一步不能"跳过"。
                # 跳过会让 stall 样本与字节账偏乐观（结论被带偏）；
                # 所以整段等待计入 wait_s（work_s=0：没数据就没算力），
                # 同时把本轮标 void（读侧没跟上，这轮的数不可信），并停止本轮测量。
                queue_timeouts += 1
                measure_void = True
                wait_s = time.perf_counter() - t0
                cur_shard = want_shard
                print("ERROR epoch=" + str(epoch) + " step=" + str(step) +
                      ": read queue empty for " + str(READ_TIMEOUT_S) + "s" +
                      " (本轮作废: read_queue_timeout)", flush=True)
                bad = True
            else:
                t1 = time.perf_counter()
                wait_s = t1 - t0
                if err is not None:
                    # 读失败：字节账不完整，这轮同样不可信 → 标 void 并计入统计。
                    read_errors += 1
                    measure_void = True
                    cur_shard = want_shard
                    print("ERROR epoch=" + str(epoch) + " step=" + str(step) +
                          " shard=" + str(shard) + ": " + err +
                          " (本轮作废: read_error)", flush=True)
                    bad = True
                else:
                    read_bytes_total += nbytes
                    read_done += 1
                    held_bytes = nbytes
                    drop_resident(nbytes)
                    cur_shard = want_shard
                    print("epoch=" + str(epoch) + " step=" + str(step) + " shard=" + str(shard) +
                          " bytes=" + str(held_bytes) +
                          " wait_s=" + str(round(wait_s, 3)) +
                          " read_s=" + str(round(read_s, 3)) +
                          " qsize=" + str(q.qsize()), flush=True)
        # t_first_step 边界（9.2）：本步 wait 已结束、算力尚未开始 → 不多算一步。
        if t_first_step is None:
            t_first_step = now_epoch()
        # 超时/读错的这一步照常计入统计（wait_s 记实、work_s=0），
        # 但 work_s=0 的步不进 stall 样本（除零）与算力均值。
        work_s = 0.0 if bad else compute_work()
        step_s = wait_s + work_s
        stall_total += wait_s
        step_total += step_s
        if not bad:
            work_samples.append(work_s)
            if work_s > 0 and not (epoch == 0 and step < 10):
                stall_samples.append(wait_s / step_s)
        t_last_step = now_epoch()
        # 图三需要逐步 stall：recovery.py 按 D40 从每步 wait_s/step_s 算回落。
        # 只打一行 JSON，runner 解析后写 per_step_*.jsonl（不落 Pod 日志文件）。
        print("[STEP] {\"epoch\":" + str(epoch) +
              ",\"step_in_epoch\":" + str(step) +
              ",\"global_step\":" + str(epoch * steps + step) +
              ",\"warmup\":" + ("true" if (epoch == 0 and step < 10) else "false") +
              ",\"wait_s\":" + str(round(wait_s, 6)) +
              ",\"step_s\":" + str(round(step_s, 6)) +
              ",\"phase\":\"" + phase_seq[occ] + "\"" +
              ",\"shard\":\"" + str(want_shard) + "\"}", flush=True)
        if bad:
            break

    if measure_void:
        break   # 本轮已作废：不再跑后面的 epoch，也不等后台读线程
                # （它们会阻塞在 q.put 上，白等 READ_TIMEOUT_S 没意义）

    for th in workers:
        th.join(timeout=READ_TIMEOUT_S)

# stall 聚合(9.2)：mean 与 p90 = 升序第 ceil(0.9*n)-1 个样本；无样本时留空。
stall_n = len(stall_samples)
if stall_n:
    stall_ratio = round(sum(stall_samples) / stall_n, 4)
    _sorted_stall = sorted(stall_samples)
    stall_p90 = round(_sorted_stall[(9 * stall_n + 9) // 10 - 1], 4)
else:
    stall_ratio = ""
    stall_p90 = ""
compute_actual_ms = round(sum(work_samples) / len(work_samples) * 1000, 1) if work_samples else ""
startup_makespan = (t_first_step - t_submit) if t_first_step else 0
total_makespan = (t_last_step - t_submit) if t_last_step else 0
# hit_rate: 这里的 hits/misses 是块级诊断计数(已作废)；traincache 的整轮命中率
# 由 reconcile_metrics 用 open-time 字节账覆盖；juicefs/fluid/direct 组是分片计数口径(D25 近似)。
hit_rate = hits / (hits + misses) if (hits + misses) > 0 else 0
if group.startswith("traincache"):
    _hb_stop.set()
_res_parts = [
    "startup_makespan=" + str(round(startup_makespan, 3)),
    "total_makespan=" + str(round(total_makespan, 3)),
    "stall_ratio=" + (str(stall_ratio) if stall_n else ""),
    "stall_p90=" + (str(stall_p90) if stall_n else ""),
    "stall_n=" + str(stall_n),
    "compute_actual_ms=" + (str(compute_actual_ms) if work_samples else ""),
    "compute_reps=" + str(COMPUTE_REPS),
    "hit_rate=" + str(round(hit_rate, 4)),
    "total_read_bytes=" + str(read_bytes_total),
    "reads=" + str(read_done),
    "hits=" + str(hits),
    "misses=" + str(misses),
    "void=" + str(1 if measure_void else 0),
    "snapshot_missing=" + str(1 if snapshot_missing else 0),
    "queue_timeouts=" + str(queue_timeouts),
    "read_errors=" + str(read_errors),
    "peak_resident_bytes=" + str(peak_resident_bytes),
]
for _p in ("startup", "steady", "switch"):
    _res_parts.append("read_bytes_" + _p + "=" + str(read_bytes_phase.get(_p, 0)))
if group.startswith("traincache"):
    for _p in ("startup", "steady", "switch"):
        _res_parts.append("demand_miss_bytes_" + _p + "=" +
                          str(demand_miss_phase.get(_p, 0)))
print("RESULT " + " ".join(_res_parts), flush=True)
'''


def build_measure_script(group):
    """生成组专属测量脚本（Python）。

    消费序列 = trace.json（6.12）；读算并行 = 后台 reader 线程 + 有界队列
    （6.15，模拟 DataLoader 异步预取）；队列只传字节数（常驻内存 O(1)）。
    """
    return build_measure_common() + build_measure_reader(group) + build_measure_loop()


def create_measure_pod(group, run_id, trace_json):
    """创建统一 train-job Pod（任务清单 #5）。

    五组全部创建 label app=train-job 的 Pod，名字固定 train-job-0（便于
    traincache WarmupTask targetRef=Pod/train-job-0 匹配、ResolvePodIndex
    从名字解析 index=0）。

    t_submit 在 kubectl apply 前取 time.time()，统一下发为
    TRAINCACHE_JOB_CREATED_AT，不再有 juicefs-warm 特判（任务清单 #6）。
    trace.json 与 measure.py 装进同一个 ConfigMap（6.12：七组态共用一份）。
    返回 (pod_name, t_submit)；失败返回 (None, 0.0)。t_submit 必须回传，
    run_one 用它算 t_pod_ready_s（9.2），且与 Pod 内看到的值严格相同。
    """
    ns = GROUP_NS[group]
    # Pod 名字固定:traincache 组 WarmupTask targetRef.Name=train-job-0
    # ResolvePodIndex 从 train-job-0 解析出 index=0
    pod_name = "train-job-0"
    script = build_measure_script(group)

    # 每次 run 前先删旧 Pod（上轮可能残留）
    kubectl(["delete", "pod", pod_name, "--grace-period=0", "--force"],
            namespace=ns, timeout=30)

    script_name = f"measure-script-{run_id}"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script)
        script_path = f.name
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(trace_json)
        trace_path = f.name
    kubectl(["delete", "configmap", script_name], namespace=ns, timeout=30)
    rc, out, err = kubectl(["create", "configmap", script_name,
                            "--from-file=measure.py=" + script_path,
                            "--from-file=trace.json=" + trace_path],
                           namespace=ns, timeout=30)
    os.unlink(script_path)
    os.unlink(trace_path)
    if rc != 0:
        print(f"[train-job] configmap 失败: {err[:300]}")
        return None, 0.0

    # 任务清单 #6:t_submit 统一在 Pod 创建前取;所有组共用同一口径
    t_submit = time.time()

    vol_mounts = [{"name": "script", "mountPath": "/scripts", "readOnly": True}]
    volumes = [{"name": "script",
                "configMap": {"name": script_name,
                              "items": [{"key": "measure.py", "path": "measure.py"},
                                        {"key": "trace.json", "path": "trace.json"}]}}]

    # ---- juicefs 组:Pod 内自己 mount JuiceFS,hostPath 共享预热缓存 ----
    if group in ("juicefs-cold", "juicefs-warm"):
        redis_url = f"redis://redis.{ns}.svc.cluster.local:6379/0"
        cache_size = str(CACHE_BYTES >> 20)  # MiB;--cache-size 单位是 MiB(docs/4 §3)
        image = JUICEFS_IMAGE
        cmd = ["/bin/sh", "-c"]
        # 任务清单 #7:删掉 measure Pod 内的 juicefs warmup 行,预热已由 up.sh 在外部完成
        args = [f"set -e\n"
                f"juicefs mount {redis_url} /mnt/jfs --cache-dir /cache --cache-size {cache_size} -d\n"
                f"for i in $(seq 1 30); do mountpoint -q /mnt/jfs && break; sleep 1; done\n"
                f"exec python3 /scripts/measure.py\n"]
        vol_mounts.append({"name": "cache", "mountPath": "/cache"})
        # 任务清单 #1:emptyDir → hostPath,与 jfs-mount Pod 共享预热后的数据
        volumes.append({"name": "cache",
                        "hostPath": {"path": "/var/cache/jfs", "type": "DirectoryOrCreate"}})
    # ---- fluid 组:挂 Fluid PVC,由 Fluid Runtime 提供数据 ----
    elif group.startswith("fluid"):
        image = TRAIN_IMAGE
        cmd = ["python3", "/scripts/measure.py"]
        args = None
        vol_mounts.append({"name": "fluid-data", "mountPath": "/data"})
        volumes.append({"name": "fluid-data",
                        "persistentVolumeClaim": {"claimName": "jfs-demo"}})
    # ---- traincache 组:挂 hostPath 缓存 + orders 目录 + ordersrc ConfigMap ----
    elif group.startswith("traincache"):
        image = TRAIN_IMAGE
        cmd = ["python3", "/scripts/measure.py"]
        args = None
        vol_mounts.append({"name": "cache-root", "mountPath": "/var/cache/traincache",
                           "readOnly": True})
        volumes.append({"name": "cache-root",
                        "hostPath": {"path": "/var/cache/traincache",
                                     "type": "Directory"}})
        vol_mounts.append({"name": "orders-dir", "mountPath": "/run/traincache/orders"})
        volumes.append({"name": "orders-dir",
                        "hostPath": {"path": "/run/traincache/orders",
                                     "type": "DirectoryOrCreate"}})
        # 读路径直接向 agentd 的 Unix datagram socket 上报 advance/read_miss
        vol_mounts.append({"name": "hook-dir", "mountPath": "/run/tc-hook"})
        volumes.append({"name": "hook-dir",
                        "hostPath": {"path": "/run/traincache",
                                     "type": "DirectoryOrCreate"}})
        vol_mounts.append({"name": "ordersrc", "mountPath": "/app/ordersrc.py",
                           "subPath": "ordersrc.py"})
        volumes.append({"name": "ordersrc",
                        "configMap": {"name": "traincache-ordersrc"}})
    # ---- direct 组:直读 MinIO,无额外 volume ----
    else:
        image = TRAIN_IMAGE
        cmd = ["python3", "/scripts/measure.py"]
        args = None

    container = {
        "name": "train",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "env": [
            # 任务清单 #6:统一下发 t_submit,删掉 juicefs-warm 的 "0" 特判
            {"name": "TRAINCACHE_JOB_CREATED_AT", "value": str(t_submit)},
            {"name": "TRAINCACHE_GROUP", "value": group},
            # 6.12:统一轨迹路径(trace.json 与 measure.py 同 ConfigMap)
            {"name": "TRAINCACHE_TRACE_PATH", "value": "/scripts/trace.json"},
            # 6.15:异步预取参数经 env 下发(不在模板里多处写死)
            {"name": "COMPUTE_MS", "value": str(COMPUTE_MS)},
            {"name": "READ_WORKERS", "value": str(READ_WORKERS)},
            {"name": "READ_QUEUE_DEPTH", "value": str(READ_QUEUE_DEPTH)},
            {"name": "READ_TIMEOUT_S", "value": str(READ_TIMEOUT_S)},
        ],
        "command": cmd,
        "volumeMounts": vol_mounts,
        # 队列只传字节数 → 预读常驻内存 O(1)；4Gi limit 是给 JuiceFS mount /
        # 本地块缓存留的余量 (资源与限额口径见 docs/4 §2.3)
        "resources": {"requests": {"cpu": "500m", "memory": "1Gi"},
                      "limits": {"cpu": "2", "memory": "4Gi"}},
    }
    # traincache 组需要 POD_UID 来写 ring 文件名
    if group.startswith("traincache"):
        container["env"].append({"name": "POD_UID",
                                 "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}})
        container["env"].append({"name": "TRAINCACHE_DATASET", "value": DATASET_NAME})
        # 6.13:实验块大小 8MiB 经 env 下发给回源区间计算
        container["env"].append({"name": "TRAINCACHE_CHUNK_SIZE", "value": str(CHUNK_SIZE)})
        container["env"].append({"name": "TRAINCACHE_PREFETCH_K", "value": str(PREFETCH_K)})
        if TILT:
            container["env"].append({"name": "TRAINCACHE_TILT", "value": "1"})
            container["env"].append({"name": "TILT_STAGGER_S",
                                     "value": os.environ.get("TILT_STAGGER_S", "5")})
            container["env"].append({"name": "TILT_SEED",
                                     "value": os.environ.get("TILT_SEED", "20261015")})
        # 测量体经 hostPath 访问 agentd hook socket（/run/tc-hook/agent.sock）
        container["env"].append({"name": "TRAINCACHE_HOOK_SOCK",
                                 "value": "/run/tc-hook/agent.sock"})
    if group in ("juicefs-cold", "juicefs-warm"):
        container["securityContext"] = {"privileged": True}
        # JuiceFS mount + 读缓存需要较大内存
        container["resources"] = {"requests": {"cpu": "500m", "memory": "1Gi"},
                                  "limits": {"cpu": "2", "memory": "6Gi"}}
    if args is not None:
        container["args"] = args

    # 统一 label:app=train-job;traincache-warm 额外打 webhook 需要的 label + scheduler
    pod_labels = {"app": "train-job"}
    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": ns,
                     "labels": pod_labels},
        "spec": {
            "restartPolicy": "Never",
            "containers": [container],
            "volumes": volumes,
        },
    }
    # JuiceFS CE 单机缓存只在 mount Pod 所在节点：测量 Pod 共置后，warm 暖好的块
    # 才是读得到的；cold 同节点，warm/cold 差值才是同一台节点上的比较。
    if group in ("juicefs-cold", "juicefs-warm") and JFS_MOUNT_NODE:
        pod_spec["spec"]["nodeSelector"] = {"kubernetes.io/hostname": JFS_MOUNT_NODE}
    # 任务清单 #5:traincache-warm 组走 traincache-scheduler + webhook gate
    # traincache-cold 不走 WarmupTask(bench 冷启动哨兵),普通调度
    # 图一对照组 GATE_MODE=free：预热照做，但 Pod 不带 gate 标签、走默认
    # 调度，用来把「门控的收益」从「预热的收益」里拆出来（9.2 §2.1）。
    if group == "traincache-warm" and GATE_MODE != "free":
        pod_labels["traincache.io/warmup"] = "enabled"
        pod_spec["spec"]["schedulerName"] = "traincache-scheduler"

    import yaml
    pod_yaml = yaml.safe_dump(pod_spec, default_flow_style=False)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(pod_yaml)
        tmp_path = f.name
    rc, out, err = kubectl(["apply", "-f", tmp_path], namespace=ns, timeout=30)
    os.unlink(tmp_path)
    if rc != 0:
        print(f"[train-job] 创建 Pod 失败: {err[:500]}")
        return None, 0.0
    print(f"[train-job] {group} Pod 已创建,label app=train-job, t_submit={t_submit:.3f}")
    # 9.2：t_submit 与 TRAINCACHE_JOB_CREATED_AT 必须是同一个值，回传给 run_one
    # 算 t_pod_ready_s（Pod Ready 条件时刻 − t_submit）。
    return pod_name, t_submit


def wait_pod_complete(ns, pod_name, timeout=POD_TIMEOUT, on_tick=None, tick_s=5):
    # 超时闸门用单调钟：wall clock 被 NTP 校正拽一下会让"等 Pod 完成"提前结束
    # （误判失败）或拖长。它不是测量量，但同样不能跳。
    start = time.monotonic()
    last_tick = 0.0
    while time.monotonic() - start < timeout:
        rc, out, _ = kubectl(["get", "pod", pod_name, "-o", "jsonpath={.status.phase}"], namespace=ns)
        phase = out.strip()
        if phase in ("Succeeded", "Failed"):
            rc, logs, _ = kubectl(["logs", pod_name], namespace=ns, timeout=120)
            return phase == "Succeeded", logs
        if on_tick is not None and time.monotonic() - last_tick >= tick_s:
            last_tick = time.monotonic()
            try:
                on_tick()
            except Exception as exc:  # 采样失败不影响跑分本身
                print(f"[tick] 采样失败: {exc}", flush=True)
        time.sleep(5)
    return False, ""


def pod_ready_epoch(ns, pod_name):
    """Pod Ready=True 的 lastTransitionTime 换算成纪元秒；未 Ready/拿不到返回 None。

    9.2：t_pod_ready_s = ready − t_submit 是从提交到"数据面可用"的落地时长，
    post_pod_s = total_makespan − t_pod_ready_s 是落地之后的训练净时长。
    Ready=False 也有 lastTransitionTime，必须过滤掉——Pod 结束后条件会转 False，
    若只看时间戳会把落地时刻记成结束时刻。
    kubectl 的 RFC3339 带 Z（fromisoformat 3.11 前不认），先换成 +00:00。
    """
    rc, out, _ = kubectl(
        ["get", "pod", pod_name, "-o",
         "jsonpath={range .status.conditions[?(@.type==\"Ready\")]}"
         "{.status} {.lastTransitionTime}{end}"],
        namespace=ns, timeout=30)
    if rc != 0:
        return None
    parts = out.strip().split()
    if len(parts) != 2 or parts[0] != "True":
        return None
    try:
        return datetime.fromisoformat(parts[1].replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 源站出站字节（9.2）：MinIO cluster metrics 的 s3 出站累计字节
# ---------------------------------------------------------------------------
MINIO_NODEPORT = int(os.environ.get("MINIO_NODEPORT", "30090"))
MINIO_METRICS_PATH = os.environ.get("MINIO_METRICS_PATH", "/minio/v2/metrics/cluster")
_EGRESS_LINE_RE = re.compile(
    r'^minio_s3_traffic_sent_bytes(?:\{[^}]*\})?\s+([0-9eE+.\-]+)')


def _node_internal_ip():
    """任取一个节点的 InternalIP；拿不到返回 None。"""
    rc, out, _ = kubectl(
        ["get", "nodes", "-o",
         "jsonpath={.items[0].status.addresses[?(@.type==\"InternalIP\")].address}"],
        timeout=30)
    if rc != 0 or not out.strip():
        return None
    return out.strip().split()[0]


def minio_egress_snapshot():
    """抓 MinIO 集群指标的 s3 出站字节总量；403/超时/无该指标一律返回 None。

    metrics 端点默认对匿名不开放（403），或 nodePort 不通时不能让整轮跑分炸掉，
    所以所有失败都收敛成 None（产物对应列留空）。
    """
    ip = _node_internal_ip()
    if not ip:
        return None
    url = f"http://{ip}:{MINIO_NODEPORT}{MINIO_METRICS_PATH}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            txt = r.read().decode("utf-8", "replace")
    except Exception as exc:  # 403/连接超时/解析失败都不该打断跑分
        print(f"[egress] MinIO metrics 抓取失败({url}): {exc}", flush=True)
        return None
    total = 0.0
    n_lines = 0
    for line in txt.splitlines():
        m = _EGRESS_LINE_RE.match(line)
        if not m:
            continue
        try:
            total += float(m.group(1))
        except ValueError:
            continue
        n_lines += 1
    if n_lines == 0:
        return None
    return {"ts": time.time(), "total": total, "series": n_lines}


# ---------------------------------------------------------------------------
# 6.16 traincache_fetch_bytes_total 对账（D25）——与 experiments/collect.py 同一指标
# ---------------------------------------------------------------------------
_FETCH_LINE_RE = re.compile(
    r'^traincache_fetch_bytes_total\{[^}]*reason="([^"]*)"[^}]*\}\s+([0-9eE+.\-]+)')


def _agent_metrics_texts():
    """kubectl --raw 抓所有 agentd Pod 的 :9411/metrics 原文。

    返回 [(pod, text)]；一个都抓不到返回 None。fetch 对账与降级采样共用。
    """
    rc, out, _ = kubectl(["get", "pods", "-l", AGENT_LABEL,
                          "-o", "jsonpath={.items[*].metadata.name}"],
                         namespace=AGENT_NS, timeout=30)
    if rc != 0 or not out.strip():
        return None
    texts = []
    for pod in out.split():
        rc, txt, _ = run_cmd(["kubectl", "get", "--raw",
                              f"/api/v1/namespaces/{AGENT_NS}/pods/{pod}:{AGENT_METRICS_PORT}/proxy/metrics"],
                             timeout=20)
        if rc == 0 and txt:
            texts.append((pod, txt))
    return texts or None


def fetch_snapshot_via_kubectl():
    """kubectl --raw 直接抓各节点 agentd :9411/metrics，按 reason 汇总回源字节。

    一条 fetch 行都没有时返回 None，不能返回 0：否则 base/final 都是 0，
    source_saving_ratio 会被算成 1.0、预取成本算成 0，指标名一变就出假数据。
    """
    texts = _agent_metrics_texts()
    if texts is None:
        return None
    by_reason = {}
    total = 0.0
    n_lines = 0
    for _pod, txt in texts:
        for line in txt.splitlines():
            m = _FETCH_LINE_RE.match(line)
            if not m:
                continue
            try:
                v = float(m.group(2))
            except ValueError:
                continue
            reason = m.group(1) or "other"
            by_reason[reason] = by_reason.get(reason, 0.0) + v
            total += v
            n_lines += 1
    if n_lines == 0:
        return None
    return {"ts": time.time(), "total": total, "by_reason": by_reason}


_DEGRADED_LINE_RE = re.compile(
    r'^traincache_agent_degraded(?:\{[^}]*\})?\s+([0-9eE+.\-]+)')


def agent_degraded_snapshot():
    """所有 agentd 的 traincache_agent_degraded 最大值与 fetch 总字节。

    返回 {"ts","max","pods","fetch_total"}；任一 Pod 抓不到指标或没有该行
    返回 None（此时无法断言未降级）。
    """
    texts = _agent_metrics_texts()
    if texts is None:
        return None
    max_v = 0
    fetch_total = 0.0
    for _pod, txt in texts:
        deg = None
        for line in txt.splitlines():
            m = _DEGRADED_LINE_RE.match(line)
            if m:
                deg = float(m.group(1))
                break
        if deg is None:
            return None
        max_v = max(max_v, int(deg))
        for line in txt.splitlines():
            m = _FETCH_LINE_RE.match(line)
            if m:
                fetch_total += float(m.group(2))
    return {"ts": time.time(), "max": max_v, "pods": len(texts),
            "fetch_total": fetch_total}


def fetch_snapshot_via_prometheus():
    """Prometheus 即时查询(复用 experiments/collect.py 的 prom_query)。失败或空结果返回 None。"""
    try:
        import collect
        res = collect.prom_query(PROMETHEUS_URL,
                                 "sum by (reason) (traincache_fetch_bytes_total)",
                                 datetime.now(timezone.utc))
        if not res:
            return None
        by_reason = {}
        total = 0.0
        for series in res:
            reason = series.get("metric", {}).get("reason", "other")
            v = float(series.get("value", [0, "0"])[1])
            by_reason[reason] = by_reason.get(reason, 0.0) + v
            total += v
        return {"ts": time.time(), "total": total, "by_reason": by_reason}
    except Exception as exc:
        print(f"[fetch] Prometheus 查询失败: {exc}", flush=True)
        return None


def fetch_snapshot():
    if PROMETHEUS_URL:
        snap = fetch_snapshot_via_prometheus()
        if snap is not None:
            return snap
    return fetch_snapshot_via_kubectl()


# ---------------------------------------------------------------------------
# 分阶段命中率（9.2）：traincache_cache_hits/misses_total 与 hit_ratio gauge
# ---------------------------------------------------------------------------
_CACHE_HITS_RE = re.compile(
    r'^traincache_cache_hits_total\{([^}]*)\}\s+([0-9eE+.\-]+)')
_CACHE_MISSES_RE = re.compile(
    r'^traincache_cache_misses_total\{([^}]*)\}\s+([0-9eE+.\-]+)')
_CACHE_RATIO_RE = re.compile(
    r'^traincache_cache_hit_ratio\{([^}]*)\}\s+([0-9eE+.\-]+)')
_PHASE_LABEL_RE = re.compile(r'phase="([^"]*)"')


def _phase_of(labels):
    """从标签串里取 phase；缺标签归到 unknown（不丢计数）。"""
    m = _PHASE_LABEL_RE.search(labels or "")
    return m.group(1) if m else "unknown"


def cache_counter_snapshot():
    """抓所有 agentd 的块级 hits/misses/hit_ratio，按 phase 汇总。

    9.2：hits/misses 是多 Pod 计数器（按 phase 求和），hit_ratio 是 gauge
    （多 Pod 取平均）。抓不到任何一条时返回 None，产物列留空。
    返回 {"ts","hits":{phase:n},"misses":{phase:n},"gauge":{phase:v}}。
    """
    texts = _agent_metrics_texts()
    if texts is None:
        return None
    hits = {}
    misses = {}
    gauge_vals = {}
    found = False
    for _pod, txt in texts:
        for line in txt.splitlines():
            m = _CACHE_HITS_RE.match(line)
            if m:
                try:
                    v = float(m.group(2))
                except ValueError:
                    continue
                ph = _phase_of(m.group(1))
                hits[ph] = hits.get(ph, 0.0) + v
                found = True
                continue
            m = _CACHE_MISSES_RE.match(line)
            if m:
                try:
                    v = float(m.group(2))
                except ValueError:
                    continue
                ph = _phase_of(m.group(1))
                misses[ph] = misses.get(ph, 0.0) + v
                found = True
                continue
            m = _CACHE_RATIO_RE.match(line)
            if m:
                try:
                    v = float(m.group(2))
                except ValueError:
                    continue
                ph = _phase_of(m.group(1))
                gauge_vals.setdefault(ph, []).append(v)
                found = True
    if not found:
        return None
    gauge = {ph: sum(v) / len(v) for ph, v in gauge_vals.items() if v}
    return {"ts": time.time(), "hits": hits, "misses": misses, "gauge": gauge}


def counter_delta(base, final):
    """两个 (total, by_reason) 快照的窗口增量；agentd 中途重启使 counter 归零
    时按 final 起算(保守近似)。返回 (Δtotal, Δby_reason) 或 (None, None)。"""
    if base is None or final is None:
        return None, None
    d_total = final["total"] - base["total"]
    if d_total < 0:
        d_total = final["total"]
    d_reason = {}
    for k, v in final["by_reason"].items():
        dv = v - base["by_reason"].get(k, 0.0)
        if dv < 0:
            dv = v
        d_reason[k] = dv
    return d_total, d_reason


def attribute_prefetch_bytes(samples, boundaries, switch_window_s):
    """把运行期 prefetch 计数增量归因：epoch 切换后 switch_window_s 秒内的
    增量计"切换重预取"，其余计"持续预取"。samples=[fetch_snapshot,...]，
    boundaries=[(epoch, ts),...]（epoch≥1 才构成切换）。"""
    pts = sorted((s["ts"], s["by_reason"].get("prefetch", 0.0)) for s in samples)
    if len(pts) < 2:
        return None, None
    switch_ts = [b[1] for b in boundaries if b[0] >= 1]
    total = 0.0
    switch_sum = 0.0
    for (_, a), (t2, b) in zip(pts, pts[1:]):
        inc = b - a if b >= a else b  # counter reset：从 0 起算
        total += inc
        if any(sw <= t2 < sw + switch_window_s for sw in switch_ts):
            switch_sum += inc
    return max(0.0, total - switch_sum), max(0.0, switch_sum)


def s3_fanout_count():
    """生效的预热 fanout(D26):固定 PREHEAT_FANOUT,节点数只作上限。"""
    rc, out, _ = kubectl(["get", "nodes", "-l", "traincache.io/s3-access=true",
                          "--no-headers"], timeout=30)
    if rc != 0 or not out.strip():
        return PREHEAT_FANOUT
    return min(PREHEAT_FANOUT, max(1, len(out.strip().splitlines())))


def parse_result(logs):
    """解析 RESULT 行（key=value 空格分隔，逐字段容错）。

    旧版用一条大正则卡死字段顺序与数量，加列就得同步改正则；改成先截
    `RESULT ...` 再按空格拆 k=v，缺字段/空值都落到默认值（数值缺省为空串），
    这样新增列不会让旧日志解析失败。
    """
    m = re.search(r"^RESULT (.*)$", logs, re.M)
    if not m:
        return None
    kv = {}
    for tok in m.group(1).split():
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        kv[k] = v
    if "startup_makespan" not in kv or "total_makespan" not in kv:
        return None

    def _num(key):
        v = kv.get(key, "")
        if v == "":
            return ""
        try:
            return float(v)
        except ValueError:
            return ""

    def _int(key):
        v = _num(key)
        return "" if v == "" else int(v)

    return {
        "startup_makespan": float(_num("startup_makespan") or 0.0),
        "total_makespan": float(_num("total_makespan") or 0.0),
        # 9.2：stall 按步 wait_s/step_s 聚合（mean + p90，epoch0 前 10 步不进样本）
        "stall_ratio": _num("stall_ratio"),
        "stall_p90": _num("stall_p90"),
        "stall_n": _int("stall_n") or 0,
        # 9.2：墙钟算力实测均值（ms）与标定出的 matmul 重复次数（只作估算值）
        "compute_actual_ms": _num("compute_actual_ms"),
        "compute_reps": _int("compute_reps"),
        "hit_rate": float(_num("hit_rate") or 0.0),
        "total_read_bytes": int(_num("total_read_bytes") or 0),
        "reads": int(_num("reads") or 0),
        "hits": int(_num("hits") or 0),
        "misses": int(_num("misses") or 0),
        # 2026-10-24 新增：测量体自报"这轮不可信"（读队列超时/读失败）与诊断量
        "measure_void": int(_num("void") or 0),
        # 9.2：快照缺片 → 本轮作废（reason=read_error）
        "snapshot_missing": int(_num("snapshot_missing") or 0),
        "queue_timeouts": int(_num("queue_timeouts") or 0),
        "read_errors": int(_num("read_errors") or 0),
        "peak_resident_bytes": int(_num("peak_resident_bytes") or 0),
        # 9.2 分阶段字节（非 traincache 组无 origin 口径 → 空串）
        "read_bytes_startup": _int("read_bytes_startup"),
        "read_bytes_steady": _int("read_bytes_steady"),
        "read_bytes_switch": _int("read_bytes_switch"),
        "demand_miss_bytes_startup": _int("demand_miss_bytes_startup"),
        "demand_miss_bytes_steady": _int("demand_miss_bytes_steady"),
        "demand_miss_bytes_switch": _int("demand_miss_bytes_switch"),
        # M5 倾斜双 Job（TILT=1）：A 的块级命中率/等待占比、B 挤占热块次数
        "a_hit_ratio": _num("a_hit_ratio"),
        "a_wait_ratio": _num("a_wait_ratio"),
        "b_evictions_of_hot": _int("b_evictions_of_hot"),
        "a_steps": _int("a_steps"),
        "b_steps": _int("b_steps"),
        "tilt_hot_shards": _int("tilt_hot_shards"),
    }


def parse_epoch_boundaries(logs):
    return [(int(e), float(ts)) for e, ts in
            re.findall(r"\[EPOCH_BOUNDARY\] epoch=(\d+) ts=([\d.]+)", logs)]


def parse_switch_events(logs, steps_per_epoch):
    """[EPOCH_BOUNDARY] → recovery.py 认的 switch_events[]。

    epoch 0 是起点不是切换；每个后续 epoch 边界记一次，at_step 取该 epoch
    第一步的全局步号。kind 由打点给出（curriculum 的 stage 重合时由 measure
    侧写成 epoch+stage，当前模板统一写 epoch）。
    """
    events = []
    for m in re.finditer(
            r"\[EPOCH_BOUNDARY\] epoch=(\d+) ts=([\d.]+)"
            r"(?: at_step=(\d+))?(?: kind=(\S+))?", logs):
        epoch = int(m.group(1))
        if epoch == 0:
            continue
        at_step = int(m.group(3)) if m.group(3) else epoch * steps_per_epoch
        events.append({
            "at": float(m.group(2)),
            "at_step": at_step,
            "to_epoch": epoch,
            "kind": m.group(4) or "epoch",
        })
    return events


def parse_step_records(logs):
    """[STEP] 行 → 每步记录；坏行跳过，不中断整轮解析。"""
    records = []
    for line in logs.splitlines():
        if not line.startswith("[STEP] "):
            continue
        try:
            records.append(json.loads(line[len("[STEP] "):]))
        except (ValueError, TypeError):
            continue
    return records


def write_run_artifacts(out_csv, group, rep, logs, trace_sha):
    """把逐步 stall 与切换事件写成 recovery.py 能直接吃的 run 目录。

    目录：<cell>/runs/<group>-r<rep>/per_step_1_w1.jsonl + run.json。
    与 summary.csv 并列，不覆盖旧产物。写完直接跑 D40 回落判据，
    有切换事件时落 recovery.json；无事件（1 epoch 或固定 seed 不退化的
    情况除外）不写。
    """
    run_dir = out_csv.parent / "runs" / f"{group}-r{rep:02d}"
    run_dir.mkdir(parents=True, exist_ok=True)
    records = parse_step_records(logs)
    if records:
        path = run_dir / "per_step_1_w1.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    _rate = re.search(r"(\d+)", str(TC_RATE_EFF or ""))
    cond_name = (COND_NAME_OVERRIDE or
                 _cond_name_for_cell(TIER, [group],
                                     int(_rate.group(1)) if _rate else 0)
                 or "")
    events = parse_switch_events(logs, STEPS_PER_EPOCH)
    run = {
        "schema_version": "1",
        "run_id": f"{group}-r{rep:02d}",
        "cond": cond_name,
        "group": group,
        "trace_sha256": trace_sha,
        "switch_events": events,
        "args": {"shuffle": TRACE_SHUFFLE, "epochs": EPOCHS,
                 "seed": TRACE_SEED, "steps_per_epoch": STEPS_PER_EPOCH},
    }
    with (run_dir / "run.json").open("w", encoding="utf-8") as f:
        json.dump(run, f, ensure_ascii=False, indent=1)
    if events:
        try:
            import recovery  # noqa: PLC0415
            result = recovery.analyze_run(run_dir)
            with (run_dir / "recovery.json").open("w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=1)
        except Exception as exc:  # noqa: BLE001
            print(f"[run] {group} rep={rep} recovery 分析失败: {exc}", flush=True)
    return run_dir, len(records)


def _cache_phase_ratio(cache_base, cache_final, phase):
    """一个 phase 的 run 窗口命中率：Δhits / (Δhits+Δmisses)。

    counter 回退（agentd 重启归零）按 end<base 时取 end 计，思路同 counter_delta；
    无样本或增量为 0 时返回空串（不写 0 冒充数据）。
    """
    if cache_base is None or cache_final is None:
        return ""
    h1 = cache_final["hits"].get(phase, 0.0)
    h0 = cache_base["hits"].get(phase, 0.0)
    dh = h1 - h0 if h1 >= h0 else h1
    m1 = cache_final["misses"].get(phase, 0.0)
    m0 = cache_base["misses"].get(phase, 0.0)
    dm = m1 - m0 if m1 >= m0 else m1
    tot = dh + dm
    if tot <= 0:
        return ""
    return round(min(1.0, max(0.0, dh / tot)), 4)


def reconcile_metrics(group, metrics, base_snap, samples, final_snap, boundaries,
                      cache_base=None, cache_final=None, pre_up_snap=None,
                      origin_egress=None):
    """6.16：把 D25 命中率口径与 D5 三笔字节成本写进 metrics 行。

    9.2 增补：预热字节实测、分阶段命中率、源站出站字节对账。
    """
    # ---- 第一笔：启动预热字节（模型值 + 实测值并列）----
    if group == "traincache-warm":
        fanout = s3_fanout_count()
        model_bytes = WARMUP_K * CHUNK_SIZE * fanout
        metrics["startup_warmup_model_bytes"] = model_bytes
        # 实测：group_up 前(pre_up_snap) → group_up 后(base_snap) 的 fetch 增量。
        # 抓不到任一端时退回模型值，并令两列相等（不写 0 冒充）。
        measured, _ = counter_delta(pre_up_snap, base_snap)
        metrics["startup_warmup_bytes"] = round(measured, 1) \
            if measured is not None else model_bytes
        if measured is None:
            metrics["startup_warmup_model_bytes"] = metrics["startup_warmup_bytes"]
        metrics["warmup_fanout"] = fanout
        metrics["warmup_basis"] = WARMUP_BASIS_PREFIX
    elif group in ("juicefs-warm", "fluid-warm"):
        # D25 近似：warmup/DataLoad 把整份数据集拉进节点缓存，计 1 份
        metrics["startup_warmup_bytes"] = N_TARS * SHARD_SIZE_MB * (1 << 20)
        metrics["startup_warmup_model_bytes"] = ""
        metrics["warmup_fanout"] = 1
        metrics["warmup_basis"] = WARMUP_BASIS_FULL
    else:
        metrics["startup_warmup_bytes"] = 0
        metrics["startup_warmup_model_bytes"] = ""
        metrics["warmup_fanout"] = 0
        metrics["warmup_basis"] = ""
    # direct 组直读 MinIO 的分段读法；其余组不走 HTTP 直读，留空。
    metrics["read_mode"] = READ_MODE_DIRECT if group == "direct" else ""
    # 源站出站字节（9.2）：run 窗口内 MinIO s3 出站累计增量
    metrics["origin_egress_bytes"] = round(origin_egress, 1) \
        if origin_egress is not None else ""

    if group.startswith("traincache"):
        # ---- traincache 组：fetch 字节 run 窗口增量对账（D25 补注）----
        fetch_d, reason_d = counter_delta(base_snap, final_snap)
        read_b = metrics.get("total_read_bytes", 0)
        miss_parts = [metrics.get("demand_miss_bytes_" + p)
                      for p in ("startup", "steady", "switch")]
        miss_b = sum(miss_parts) if all(isinstance(v, int) for v in miss_parts) else None
        metrics["fetch_bytes_run"] = round(fetch_d, 1) if fetch_d is not None else ""
        # 主口径（9.2 §1.4）：open-time，打开时不在本地即未命中
        if read_b > 0 and miss_b is not None:
            hit = 1.0 - miss_b / read_b
            metrics["hit_rate"] = round(min(1.0, max(0.0, hit)), 4)
            metrics["hit_rate_basis"] = BASIS_TC_OPEN
        else:
            # 字节账缺失时不回退旧"磁盘存在即 hit"口径(已作废)，命中率置空
            metrics["hit_rate"] = ""
            metrics["hit_rate_basis"] = BASIS_TC_UNAVAILABLE
        # 源站节省率（D25 对账列）：省下的源站流量，不等于命中率
        if fetch_d is not None and read_b > 0:
            save = 1.0 - fetch_d / read_b
            metrics["source_saving_ratio"] = round(min(1.0, max(0.0, save)), 4)
            metrics["source_saving_basis"] = BASIS_TC_SAVING
        else:
            metrics["source_saving_ratio"] = ""
            metrics["source_saving_basis"] = BASIS_TC_UNAVAILABLE
        metrics["block_hit_rate_diag"] = metrics["hit_rate_diag"]
        # ---- 9.2 分阶段命中率 ----
        # 主口径：训练侧 open-time（打开时块不在本地 = 未命中），与 §1.4 定义一致；
        # 三个 phase 由 read_bytes/demand_miss 差分得到，缺字节账时留空。
        # agent 的 hits/misses 按它的 phase 窗口计，作交叉校验（窗口内无读时留空）。
        for phase in ("startup", "steady", "switch"):
            rb = metrics.get("read_bytes_" + phase)
            dm = metrics.get("demand_miss_bytes_" + phase)
            metrics["hit_ratio_" + phase] = (
                round(max(0.0, min(1.0, 1.0 - dm / rb)), 4)
                if isinstance(rb, int) and rb > 0 and isinstance(dm, int) else "")
            metrics["hit_ratio_agent_" + phase] = _cache_phase_ratio(
                cache_base, cache_final, phase)
            gauge_v = ""
            if cache_final is not None:
                g = cache_final.get("gauge", {}).get(phase)
                if g is not None:
                    gauge_v = round(g, 4)
            metrics["hit_ratio_gauge_" + phase] = gauge_v
        # ---- 源站出站对账：训练侧 fetch 字节 vs MinIO 出站字节 ----
        if fetch_d is not None and origin_egress is not None:
            metrics["origin_reconcile_diff"] = round(fetch_d - origin_egress, 1)
        else:
            metrics["origin_reconcile_diff"] = ""
        # ---- 第二/三笔：持续预取 vs 切换重预取（按 reason=prefetch 归因）----
        # 归因序列补最终快照：否则最后一个采样点到训练结束的增量永远漏算。
        attr_samples = list(samples)
        if final_snap is not None:
            attr_samples.append(final_snap)
        sustained, switch = attribute_prefetch_bytes(attr_samples, boundaries, SWITCH_WINDOW_S)
        metrics["sustained_prefetch_bytes"] = round(sustained, 1) if sustained is not None else ""
        metrics["switch_reprefetch_bytes"] = round(switch, 1) if switch is not None else ""
        # reason 拆分：prefetch 归因与总 fetch 对账，缺口单列非预取回源。
        prefetch_d = reason_d.get("prefetch", 0.0) if reason_d is not None else None
        metrics["prefetch_bytes_run"] = round(prefetch_d, 1) if prefetch_d is not None else ""
        metrics["other_fetch_bytes_run"] = (
            round(fetch_d - prefetch_d, 1)
            if fetch_d is not None and prefetch_d is not None else "")
        # sustained + switch 应等于 prefetch 增量；对不上说明采样有缺口。
        if (sustained is not None and prefetch_d is not None
                and abs(sustained + switch - prefetch_d) > max(1.0, 0.01 * prefetch_d)):
            print(f"[warn] prefetch 归因与计数增量不一致: "
                  f"sustained+switch={sustained + switch:.0f} "
                  f"prefetch={prefetch_d:.0f} (samples={len(samples)})", flush=True)
    else:
        if group == "direct":
            metrics["hit_rate_basis"] = BASIS_DIRECT
        elif group == "juicefs-warm":
            metrics["hit_rate_basis"] = BASIS_JFS_WARM
        elif group == "juicefs-cold":
            metrics["hit_rate_basis"] = BASIS_JFS_COLD
        elif group == "fluid-warm":
            metrics["hit_rate_basis"] = BASIS_FLUID_WARM
        else:
            metrics["hit_rate_basis"] = BASIS_FLUID_COLD
        # JuiceFS/Fluid 无 agent 指标：持续/切换预取两列留空（近似口径见 basis 列）
        metrics["block_hit_rate_diag"] = ""
        metrics["sustained_prefetch_bytes"] = ""
        metrics["switch_reprefetch_bytes"] = ""
        metrics["fetch_bytes_run"] = ""
        # 非 traincache 组没有 agent fetch 指标，源站节省率与命中率统一走模型近似 basis
        metrics["source_saving_ratio"] = ""
        metrics["source_saving_basis"] = ""
        # 非 traincache 组没有 agent 分阶段命中率，三组列一律留空
        for phase in ("startup", "steady", "switch"):
            metrics["hit_ratio_" + phase] = ""
            metrics["hit_ratio_agent_" + phase] = ""
            metrics["hit_ratio_gauge_" + phase] = ""
        metrics["origin_reconcile_diff"] = ""
    metrics["hit_rate_diag"] = ""
    return metrics


def measure_void_reason(metrics):
    """测量体自报"这轮不可信"时的作废原因（写进 summary.csv 的 reason 列）。

    读队列超时 / 读失败 / 快照缺片都会让字节账与 stall 样本不可信，这轮不进结论。
    快照缺片复用 read_error（作废清单封闭，不新增词）。
    """
    if not metrics.get("measure_void"):
        return ""
    if metrics.get("snapshot_missing"):
        return "read_error"
    if metrics.get("queue_timeouts"):
        return "read_queue_timeout"
    if metrics.get("read_errors"):
        return "read_error"
    return "measure_void"


# ---------------------------------------------------------------------------
# D30/D32 接线：档位联动、按档位过滤、rep 复用控制面、status 列、Fluid 替补
# ---------------------------------------------------------------------------
def tc_cell(tier, rate_str=None):
    """把 D31 的带宽格落到具体参数：(rate_mbit, burst)。

    burst 从 config 现推（burst_for_rate），shell 侧与 runner 侧同一套数，
    不再各处写死 1600k。netem 只有固定时延，没有抖动开关。
    """
    rate_str = rate_str or TC_RATE_EFF
    m = re.search(r"\d+", rate_str)
    if not m:
        return None, bench_config.TC_BURST
    r = int(m.group())
    return r, bench_config.burst_for_rate(r)


def apply_tier(tier):
    """把档位落到 N_TARS/EPOCHS（D32 联动：S=3ep、M/L=1ep）。

    显式给过 N_TARS/EPOCHS 环境变量时以显式值为准（冒烟场景），不被档位覆盖。
    """
    global TIER, N_TARS, EPOCHS
    TIER = tier
    if not ENV_HAS_N_TARS:
        N_TARS = CAPACITY_TIER_N_TARS[tier]
    if not ENV_HAS_EPOCHS:
        EPOCHS = CAPACITY_TIER_EPOCHS[tier]


def select_groups(tier, requested=None):
    """按档位挑参跑组（D30），返回 (要跑的组态, 被剔除的组态)。

    requested 为空时取该档全部组态（按 GROUPS 顺序）；显式指定时与该档求交集，
    剔除项回传给调用方打印——M/L 档不该把 warm 组拖进来。
    """
    ordered = list(GROUPS) if not requested else list(requested)
    if FIGURE_MODE:
        return ordered, []
    allowed = CAPACITY_TIER_GROUPS[tier]
    return ([g for g in ordered if g in allowed],
            [g for g in (requested or []) if g not in allowed])


def row_status(tier, void=False):
    """产物 status 列：void 轮不进汇总（D14），L 档 1 次是符号探点（D19）。"""
    if void:
        return "void"
    if FIGURE_MODE:
        return "ok"
    return "exploratory" if tier in TIER_EXPLORATORY else "ok"


# D30：Fluid 整组起不来时落回同冷热档的 JuiceFS 顶位，产物标 baseline_substituted
SUBSTITUTE_GROUP = {"fluid-cold": "juicefs-cold", "fluid-warm": "juicefs-warm"}


def needs_substitution(results):
    """该组每一轮都作废 → 判装置级失败，需要替补组顶这个格子。"""
    return bool(results) and all(r.get("void") for r in results)


def stamp_rows(rows, group, substitute_for=""):
    """每行补组名与替补标记，供 baseline_substituted 列出表。"""
    for r in rows:
        r["group"] = r.get("group") or group
        r["baseline_substituted"] = substitute_for
    return rows


def rep_cold_reset(group):
    """rep 之间的轻冷态（D32 ①）：只删测量 Pod 与节点缓存，不 down/up 组件。

    和 cold_reset() 的差别是不动各命名空间的基础设施 Pod（Redis 元数据、挂载点、
    控制面都留着），省掉每轮 ≈2 分钟搭建；冷读口径仍成立——四个缓存目录全清、
    测量 Pod 重建。warm 组不走这里：缓存必须回冷态后重新预热，复用会让 WarmupTask
    停在上一轮 Released 的状态，跑成「标着 warm 实际冷读」。
    """
    print(f"[rep_reset] {group}: 删测量 Pod + 清节点缓存（保留基础设施）", flush=True)
    kubectl(["delete", "pods", "-l", "app=train-job", "--grace-period=0", "--force"],
            namespace=GROUP_NS[group], timeout=60)
    _clear_node_caches("rep_reset")
    time.sleep(5)


def run_group(group, reps, out_csv, trace_json, trace_sha, substitute_for=""):
    results = []
    is_tc = group.startswith("traincache")
    # D32 ①：cold 组（含 direct，都无预热）跨 rep 复用控制面；warm 组每 rep 整轮 down/up。
    reuse = not group.endswith("-warm")
    if reuse:
        cold_reset()                  # 进组前仍做一次全量冷态，保证干净起跑
        up_ok, group_warmup, group_warmup_basis = group_up(group)
        if not up_ok:
            print(f"[run] {group} 起组失败，{reps} 轮全记 up_failed", flush=True)
            return stamp_rows(
                [{"group": group, "rep": r, "void": True, "reason": "up_failed",
                  "warmup_seconds": 0.0, "trace_sha256": trace_sha}
                 for r in range(reps)], group, substitute_for)
    for rep in range(reps):
        run_id = f"{rep:02d}"
        print(f"\n{'='*60}")
        print(f"[run] group={group} rep={rep+1}/{reps}{'（复用控制面）' if reuse else ''}")
        print(f"{'='*60}", flush=True)

        # 9.2：预热前 fetch 计数基线（只有 traincache-warm 用，见下方 group_up 前）
        pre_up_snap = None
        if reuse:
            if rep:
                rep_cold_reset(group)
            if is_tc:
                # 控制面跨 rep 复用，也必须逐 rep 复位 agentd
                try:
                    reset_traincache_agent()
                except (SystemExit, Exception) as exc:
                    print(f"[run] {group} rep={rep} agentd 复位失败，本轮记 "
                          f"up_failed，停止该组：{exc}", flush=True)
                    results.append({"group": group, "rep": rep, "void": True,
                                    "reason": "up_failed",
                                    "warmup_seconds": group_warmup,
                                    "trace_sha256": trace_sha})
                    break
            warmup_seconds = group_warmup
            warmup_basis = group_warmup_basis
        else:
            cold_reset()
            if is_tc:
                # 必须在 up.sh 预热之前复位：复位会清块缓存，放在预热之后会把
                # warm 组刚写入的块清掉。
                try:
                    reset_traincache_agent()
                except (SystemExit, Exception) as exc:
                    print(f"[run] {group} rep={rep} agentd 复位失败，本轮记 "
                          f"up_failed，停止该组：{exc}", flush=True)
                    results.append({"group": group, "rep": rep, "void": True,
                                    "reason": "up_failed", "warmup_seconds": 0.0,
                                    "trace_sha256": trace_sha})
                    group_down(group)
                    break
            if group == "traincache-warm":
                # 预热前基线：agentd 已在跑（上一段 reset 已恢复并自检）
                pre_up_snap = fetch_snapshot_via_kubectl()
            up_ok, warmup_seconds, warmup_basis = group_up(group)
            if not up_ok:
                results.append({"group": group, "rep": rep, "void": True,
                                "reason": "up_failed", "warmup_seconds": warmup_seconds,
                                "trace_sha256": trace_sha})
                continue

        # 6.16:traincache 组在创建 train-job 前抓 fetch 计数器基线
        base_snap = fetch_snapshot() if is_tc else None
        cache_base = cache_counter_snapshot() if is_tc else None
        samples = [base_snap] if base_snap else []
        # 9.2：源站出站字节基线（MinIO cluster metrics；抓不到为 None）
        origin_base = minio_egress_snapshot()

        pod_name, t_submit = create_measure_pod(group, run_id, trace_json)
        if not pod_name:
            results.append({"group": group, "rep": rep, "void": True,
                            "reason": "pod_create_failed", "warmup_seconds": warmup_seconds,
                            "trace_sha256": trace_sha})
            if not reuse:
                group_down(group)
            continue

        ns = GROUP_NS[group]

        # 9.2：采样走独立线程，主循环只查 Pod 状态；抓取四个 agentd 的耗时
        # 不再拖长采样间隔。sample_times 用于报告实际节拍。
        sample_times = []
        stop_sample = threading.Event()
        sample_thread = None

        def _sample_loop():
            while not stop_sample.wait(SAMPLE_INTERVAL_S):
                t0 = time.time()
                snap = fetch_snapshot()
                if stop_sample.is_set():
                    break
                if snap is not None:
                    samples.append(snap)
                    sample_times.append(t0)

        # 测量期间每 5s 采一次 degraded；测量 Pod 一结束就停，避免训练结束
        # 30s 后 sidecar 退出导致的必然降级被误判进本轮。
        degraded = {"max": 0, "samples": 0}
        stop_degraded = threading.Event()
        degraded_thread = None
        if is_tc:
            sample_thread = threading.Thread(target=_sample_loop, daemon=True)
            sample_thread.start()
            def _degraded_loop():
                while not stop_degraded.wait(AGENT_DEGRADED_SAMPLE_S):
                    snap = agent_degraded_snapshot()
                    if stop_degraded.is_set():
                        break
                    if snap is not None:
                        degraded["samples"] += 1
                        degraded["max"] = max(degraded["max"], snap["max"])
            degraded_thread = threading.Thread(target=_degraded_loop, daemon=True)
            degraded_thread.start()

        # Ready 时刻必须在 Pod 存活期间轮询：结束后 Ready 条件会转 False，
        # 事后读 lastTransitionTime 拿到的是终止时刻。
        ready_seen = {"ts": None}

        def _tick():
            if ready_seen["ts"] is None:
                ts = pod_ready_epoch(ns, pod_name)
                if ts is not None:
                    ready_seen["ts"] = ts

        success, logs = wait_pod_complete(ns, pod_name, timeout=POD_TIMEOUT,
                                          on_tick=_tick, tick_s=5)
        stop_sample.set()
        if sample_thread is not None:
            sample_thread.join(timeout=SAMPLE_JOIN_TIMEOUT_S)
            if sample_thread.is_alive():
                print(f"[run] {group} rep={rep} WARNING: 采样线程未在 "
                      f"{SAMPLE_JOIN_TIMEOUT_S}s 内结束，归因只用已采到的样本", flush=True)
        # 归因读副本，避免采样线程收尾时 append 与 reconcile 迭代冲突。
        samples_for_attr = list(samples)
        stop_degraded.set()
        if degraded_thread is not None:
            degraded_thread.join(timeout=1)
        # 9.2：源站出站字节末值（与 origin_base 求 run 窗口增量）
        origin_final = minio_egress_snapshot()
        degraded_value = degraded["max"] if is_tc and degraded["samples"] else ""
        if is_tc and not degraded["samples"]:
            print(f"[run] {group} rep={rep} WARNING: 测量期间未采到 agentd 指标，"
                  f"agent_degraded_max 留空", flush=True)
        if not success:
            print(f"[run] {group} rep={rep} Pod 失败/超时")
            # 调试:打印 pod 状态与事件
            rc, desc, _ = kubectl(["describe", "pod", pod_name], namespace=ns, timeout=30)
            print(f"[debug] describe:\n{desc[:2000]}")
            rc, evts, _ = kubectl(["get", "events", "--field-selector",
                                   f"involvedObject.name={pod_name}",
                                   "--sort-by=.lastTimestamp"], namespace=ns, timeout=30)
            print(f"[debug] events:\n{evts[:1500]}")
            print(f"[debug] logs:\n{logs[:1500]}")
            results.append({"group": group, "rep": rep, "void": True,
                            "reason": "pod_failed", "warmup_seconds": warmup_seconds,
                            "trace_sha256": trace_sha,
                            "agent_degraded_max": degraded_value})
            if not reuse:
                group_down(group)
            continue

        metrics = parse_result(logs)
        if metrics is None:
            print(f"[run] {group} rep={rep} 无法解析结果")
            results.append({"group": group, "rep": rep, "void": True,
                            "reason": "parse_failed", "warmup_seconds": warmup_seconds,
                            "trace_sha256": trace_sha,
                            "agent_degraded_max": degraded_value})
        else:
            metrics["group"] = group
            metrics["rep"] = rep
            metrics["run_id"] = f"{group}-r{rep:02d}"
            metrics["epochs"] = EPOCHS
            metrics["shuffle"] = TRACE_SHUFFLE
            metrics["dataset_bytes"] = N_TARS * SHARD_SIZE_MB * 1024 * 1024
            metrics["prefetch_lookahead"] = AGENT_PREFETCH_K
            metrics["evict_policy"] = EVICT_POLICY
            metrics["gate_mode"] = GATE_MODE
            metrics["data_source"] = DATA_SOURCE
            # 图三：逐步 stall + 切换事件落盘，recovery.py 直接消费。
            _run_dir, _n_steps = write_run_artifacts(
                out_csv, group, rep, logs, trace_sha)
            if _n_steps:
                print(f"[run] {group} rep={rep} 逐步打点 {_n_steps} 步 → {_run_dir}",
                      flush=True)
            metrics["warmup_seconds"] = warmup_seconds
            metrics["warmup_time_basis"] = warmup_basis
            metrics["trace_sha256"] = trace_sha
            metrics["agent_degraded_max"] = degraded_value
            # 采样诊断：实际样本数与最大间隔，用于判断 switch 窗口是否被覆盖。
            metrics["sample_n"] = len(sample_times)
            _gaps = [b - a for a, b in zip(sample_times, sample_times[1:])]
            metrics["sample_max_gap_s"] = round(max(_gaps), 1) if _gaps else ""
            metrics["hit_rate_diag"] = metrics["hit_rate"]
            # 测量体自报不可信（读队列超时/读失败）→ 这轮作废，
            # 不进结论（否则 stall_ratio/字节账会被带偏）。
            reason = measure_void_reason(metrics)
            if reason:
                metrics["void"] = True
                metrics["reason"] = reason
                print(f"[run] {group} rep={rep} 测量体报 void"
                      f"({reason}, timeouts={metrics.get('queue_timeouts')},"
                      f" errors={metrics.get('read_errors')})，本轮作废", flush=True)
            else:
                metrics["void"] = False
            # 测量期间出现降级：功能打折（消费推进换 inotify 信号源），
            # 本轮不参与组间对比；已有更具体的作废原因时保留原原因。
            if is_tc and degraded_value == 1 and not metrics.get("void"):
                metrics["void"] = True
                metrics["reason"] = "agent_degraded"
                print(f"[run] {group} rep={rep} agentd 测量期间降级"
                      f"(agent_degraded_max=1)，本轮作废", flush=True)
            final_snap = fetch_snapshot() if is_tc else None
            cache_final = cache_counter_snapshot() if is_tc else None
            # 9.2：落地时长（t_pod_ready_s）与落地后训练净时长（post_pod_s）
            ready_ts = ready_seen["ts"] or pod_ready_epoch(ns, pod_name)
            if ready_ts is not None:
                metrics["t_pod_ready_s"] = round(ready_ts - t_submit, 3)
                total_ms = metrics.get("total_makespan")
                metrics["post_pod_s"] = round(total_ms - metrics["t_pod_ready_s"], 3) \
                    if isinstance(total_ms, (int, float)) else ""
            else:
                metrics["t_pod_ready_s"] = ""
                metrics["post_pod_s"] = ""
            # 源站出站增量：counter 回退（MinIO 重启）时按末值起算
            origin_egress = None
            if origin_base is not None and origin_final is not None:
                origin_egress = origin_final["total"] - origin_base["total"]
                if origin_egress < 0:
                    origin_egress = origin_final["total"]
            metrics = reconcile_metrics(
                group, metrics, base_snap, samples_for_attr, final_snap,
                parse_epoch_boundaries(logs),
                cache_base=cache_base, cache_final=cache_final,
                pre_up_snap=pre_up_snap, origin_egress=origin_egress)
            results.append(metrics)
            print(f"[run] {group} rep={rep} 结果: {metrics}", flush=True)

        if not reuse:
            group_down(group)
            time.sleep(10)
    if reuse:
        group_down(group)
    return stamp_rows(results, group, substitute_for)


def resolve_out_path(raw):
    """把 --out 落到数据层：相对路径按 bench/config.py 的 RESULTS_DIR 展开。

    产物一律实体写 ~/tcd/bench/results/，不在 repo 内 mkdir；绝对路径原样使用。
    """
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = bench_config.RESULTS_DIR / p
    return p.resolve()


def rate_label(rate_str):
    """把速率串归一到 `<n>mbit`，summary 的 rate 列只写这个形态。"""
    m = re.search(r"\d+", str(rate_str or ""))
    return f"{m.group()}mbit" if m else str(rate_str or "")


def _cond_name_for_cell(tier, groups, rate_mbit):
    """按档位/带宽/组态找对应的 compare COND，用于取冻结镜像 digest。"""
    # 命名见 9.2 §C3：S 档是 compare-d-<组态>-<rate>；
    # M/L 档是 compare-b-<m|l>-<组态>-<rate>。
    for g in groups:
        name = (f"compare-d-{g}-{rate_mbit}" if tier == "S"
                else f"compare-b-{tier.lower()}-{g}-{rate_mbit}")
        if (CONDS_DIR / f"{name}.json").is_file():
            return name
    for p in sorted(CONDS_DIR.glob("compare-*.json")):
        try:
            cond = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if (cond.get("network") or {}).get("rate_mbit") == rate_mbit and \
                (cond.get("cache") or {}).get("capacity_ratio") == tier:
            return p.stem
    return None


def _actual_image(ref):
    """实际使用的镜像引用；能取到 docker RepoDigest 时附上（tag 级为底线）。"""
    import lock as lock_mod  # noqa: PLC0415

    digest = lock_mod._docker_image_digest(ref)
    return {"ref": ref, "digest": digest,
            "digest_source": "docker_repo_digest" if digest else None}


def write_cell_lock(out_path, tier, groups, rate_mbit, rate_str):
    """每格写 lock.json：COND 冻结 digest + 本次实际镜像，不一致不阻止跑分。"""
    import lock as lock_mod  # noqa: PLC0415

    cond_name = _cond_name_for_cell(tier, groups, rate_mbit)
    if cond_name:
        os.environ["TRAINCACHE_COND"] = cond_name
    else:
        os.environ.pop("TRAINCACHE_COND", None)
    data = lock_mod.snapshot()
    data["cell"] = {"tier": tier, "rate": rate_label(rate_str),
                    "groups": list(groups)}
    data["images_actual"] = {
        "training": _actual_image(TRAIN_IMAGE),
        "juicefs": _actual_image(JUICEFS_IMAGE),
    }
    # direct 组读法：供服务端限流口径复核（分段 read(n)，按总字节计）。
    data["direct_read_mode"] = {
        "transport": "http",
        "segment_bytes": READ_SEGMENT_BYTES,
        "counting": "total_bytes",
    }
    path = out_path.parent / "lock.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8")
    print(f"[lock] 写入 {path}（COND={cond_name or '未匹配'}，"
          f"实际镜像 {TRAIN_IMAGE} / {JUICEFS_IMAGE}）")


# summary.csv 的列顺序（模块级：--selftest 也断言这里的列集合）
CSV_FIELDNAMES = ["group", "tier", "rate", "rep", "status", "void", "reason",
                  "baseline_substituted",
                  "warmup_seconds", "warmup_time_basis",
                  "startup_makespan", "total_makespan",
                  "stall_ratio", "hit_rate", "hit_rate_basis",
                  "source_saving_ratio", "source_saving_basis",
                  "total_read_bytes", "read_mode",
                  "startup_warmup_bytes", "warmup_fanout", "warmup_basis",
                  "sustained_prefetch_bytes", "switch_reprefetch_bytes",
                  "fetch_bytes_run", "prefetch_bytes_run", "other_fetch_bytes_run",
                  "block_hit_rate_diag", "sample_n", "sample_max_gap_s",
                  "queue_timeouts", "read_errors", "peak_resident_bytes",
                  "trace_sha256", "chunk_size_bytes", "cache_bytes",
                  "compute_ms", "read_workers", "read_queue_depth",
                  "agent_degraded_max",
                  # 9.2 协议新增列（拿不到值时留空，不写 0 冒充）
                  "t_pod_ready_s", "post_pod_s", "stall_p90",
                  "compute_actual_ms", "compute_reps",
                  "read_bytes_startup", "read_bytes_steady", "read_bytes_switch",
                  "demand_miss_bytes_startup", "demand_miss_bytes_steady",
                  "demand_miss_bytes_switch",
                  "hit_ratio_startup", "hit_ratio_steady", "hit_ratio_switch",
                  "hit_ratio_agent_startup", "hit_ratio_agent_steady",
                  "hit_ratio_agent_switch",
                  "hit_ratio_gauge_startup", "hit_ratio_gauge_steady",
                  "hit_ratio_gauge_switch",
                  "startup_warmup_model_bytes",
                  "origin_egress_bytes", "origin_reconcile_diff",
                  "prefetch_fanout", "measure_shards",
                  # 9.2 §R3：每个点可回溯到 COND 与 run ID；图一/图二/图三的
                  # 自变量列必须随产物落盘，否则出图只能靠目录名猜。
                  "cond", "run_id", "epochs", "shuffle",
                  "dataset_bytes", "cache_ratio",
                  "prefetch_lookahead", "evict_policy",
                  "gate_mode", "gateway", "data_source",
                  # M5 倾斜双 Job：A 命中率/等待占比、B 挤占热块次数
                  "a_hit_ratio", "a_wait_ratio", "b_evictions_of_hot",
                  "a_steps", "b_steps", "tilt_hot_shards"]


def write_csv(path, results):
    # 列：四指标 + warmup_seconds(任务#8) + tier/rate/trace_sha256(6.12/6.13/D31)
    #     + 三笔字节成本与命中率口径标注(6.16/D5/D25) + 实验参数归档(6.13/6.15)
    #     + status/baseline_substituted(D19/D30：探点与替补一眼可辨，void 不进汇总)
    #     + gateway(D23：1=经合并回源网关，0=直连源站)
    fieldnames = CSV_FIELDNAMES
    # 生效 fanout 与本次 measure 实际用到的分片数（N_TARS），逐行同值
    fanout_eff = s3_fanout_count()
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fieldnames}
            row["tier"] = row.get("tier") or TIER
            row["rate"] = row.get("rate") or rate_label(TC_RATE_EFF)
            row["status"] = row.get("status") or row_status(
                row["tier"], bool(r.get("void")))
            row["baseline_substituted"] = r.get("baseline_substituted") or ""
            row["chunk_size_bytes"] = row.get("chunk_size_bytes") or CHUNK_SIZE
            row["cache_bytes"] = row.get("cache_bytes") or CACHE_BYTES
            row["compute_ms"] = row.get("compute_ms") or COMPUTE_MS
            row["read_workers"] = row.get("read_workers") or READ_WORKERS
            row["read_queue_depth"] = row.get("read_queue_depth") or READ_QUEUE_DEPTH
            row["prefetch_fanout"] = r.get("prefetch_fanout") or fanout_eff
            row["measure_shards"] = r.get("measure_shards") or N_TARS
            dataset_bytes = int(r.get("dataset_bytes") or
                                N_TARS * SHARD_SIZE_MB * 1024 * 1024)
            row["cond"] = (r.get("cond") or COND_NAME_OVERRIDE or
                           _cond_name_for_cell(
                               TIER, [row["group"]],
                               int(str(row["rate"]).replace("mbit", "").strip()))
                           or "")
            row["run_id"] = r.get("run_id") or str(row["rep"]).zfill(2)
            row["epochs"] = r.get("epochs") or EPOCHS
            row["shuffle"] = r.get("shuffle") or TRACE_SHUFFLE
            row["dataset_bytes"] = dataset_bytes
            row["cache_ratio"] = round(
                dataset_bytes / max(1, int(row["cache_bytes"])), 4)
            row["prefetch_lookahead"] = r.get("prefetch_lookahead") or PREFETCH_K
            row["evict_policy"] = r.get("evict_policy") or EVICT_POLICY
            row["gate_mode"] = r.get("gate_mode") or GATE_MODE
            row["gateway"] = gateway_flag()
            row["data_source"] = r.get("data_source") or DATA_SOURCE
            w.writerow(row)


def selftest():
    """离线自检：不连集群、不起 Pod、不访问 MinIO。

    查七件事：
      1. 五组 measure 模板都能编译（模板是拼出来的字符串，语法错要到 Pod 里才炸）；
      2. 模板里的计时只用单调钟——time.time() 只允许出现在纪元对齐的校准点
         （now_epoch 靠它对一次，之后用 perf_counter 推算）；
      3. 字节账精确：换一个假的 read_shard（返回已知字节数）离线跑一遍主循环，
         total_read_bytes 必须 == reads × 每片字节数，且命中计数不丢；
      4. 节点缓存清理命令都走 sh -c（glob 必须在节点容器内展开，否则静默失败），
         清后判空命令形态正确；
      5. 读队列超时那一步的 wait 照实计入诊断账、本轮标 void（bad 步不进
         stall 样本，避免除零）；快照缺片的作废原因落 read_error；
      6. D30/D31/D32 接线的纯函数口径：按档位过滤参跑组、status 三态、
         档位 epochs 联动、带宽格的 burst 推导、Fluid 全废才顶位；
      7. D23 网关开关：summary.csv 的 gateway 列位次、取值映射 1/0、透传给 up.sh。
    """
    print("[selftest] run_bench.py 离线自检（不连集群/不访问 MinIO）")

    # ---- 1) 五组模板可编译  2) 计时只用单调钟 ----
    for g in GROUPS:
        src = build_measure_script(g)
        compile(src, "<measure:" + g + ">", "exec")
        wall = [n for n in ast.walk(ast.parse(src))
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and isinstance(n.func.value, ast.Name)
                and n.func.value.id == "time" and n.func.attr == "time"]
        if len(wall) != 1:
            raise AssertionError(f"{g}: 模板里 time.time() 调用 {len(wall)} 处，"
                                 f"应只有纪元对齐校准这 1 处")
    print("[selftest] 1) 五组模板编译通过  2) 模板计时只用单调钟")

    # ---- 3) 字节账：假 read_shard 跑一遍主循环 ----
    fake_bytes = 7 * 1024 * 1024 + 13   # 故意不是整 MiB，防止"凑整"掩盖偏差
    fake_reader = (
        "FAKE_SHARD_BYTES = " + str(fake_bytes) + "\n"
        "FAKE_CALLS = 0\n"
        "\n"
        "def read_shard(shard, phase):\n"
        "    global hits, misses, FAKE_CALLS\n"
        "    with counter_lock:\n"
        "        FAKE_CALLS += 1\n"
        "        hits += 1\n"
        "        read_bytes_phase[phase] = (read_bytes_phase.get(phase, 0)\n"
        "                                   + FAKE_SHARD_BYTES)\n"
        "    return FAKE_SHARD_BYTES\n"
    )
    # 读不动：队头等满 READ_TIMEOUT_S 也不来数据，用来验超时分支
    slow_reader = (
        "def read_shard(shard, phase):\n"
        "    time.sleep(2)\n"
        "    return 1024\n"
    )

    def _run_measure(reader_src, env_over):
        """离线跑一遍 measure 主循环，返回 (解析后的指标, 执行命名空间)。"""
        script = build_measure_common() + reader_src + build_measure_loop()
        env_backup = dict(os.environ)
        os.environ.update(env_over)
        buf = io.StringIO()
        ns = {"__name__": "__measure_selftest__"}
        try:
            with contextlib.redirect_stdout(buf):
                exec(compile(script, "<measure:selftest>", "exec"), ns)
        finally:
            os.environ.clear()
            os.environ.update(env_backup)
        line = re.search(r"^RESULT .*$", buf.getvalue(), re.M)
        if not line:
            raise AssertionError("自测：主循环没有打出 RESULT 行")
        metrics = parse_result(line.group(0))
        if metrics is None:
            raise AssertionError("自测：RESULT 行解析失败: " + line.group(0))
        return metrics, ns

    saved = {k: globals()[k] for k in ("EPOCHS", "STEPS_PER_EPOCH", "N_TARS")}
    # 2 个 epoch：epoch0 前 10 步按协议不进 stall 聚合，只有 epoch1 出样本
    globals()["EPOCHS"] = 2
    globals()["STEPS_PER_EPOCH"] = 4
    globals()["N_TARS"] = 2
    trace_path = None
    try:
        trace = build_run_trace()
        fd, trace_path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(trace, f)
        base_env = {
            "TRAINCACHE_GROUP": "direct",
            "TRAINCACHE_TRACE_PATH": trace_path,
            "TRAINCACHE_JOB_CREATED_AT": "",
            "COMPUTE_MS": "1",
            "READ_WORKERS": "2",
            "READ_QUEUE_DEPTH": "2",
            "READ_TIMEOUT_S": "10",
        }
        metrics, ns = _run_measure(fake_reader, base_env)
        timeout_metrics, timeout_ns = _run_measure(
            slow_reader, dict(base_env, READ_TIMEOUT_S="1"))
    finally:
        if trace_path and os.path.exists(trace_path):
            os.unlink(trace_path)
        globals().update(saved)

    calls = ns.get("FAKE_CALLS", 0)
    if metrics["reads"] <= 0:
        raise AssertionError("自测：主循环一次分片都没消费，测试无效")
    expect = metrics["reads"] * fake_bytes
    if metrics["total_read_bytes"] != expect:
        raise AssertionError(f"自测：字节账不符 reads={metrics['reads']} "
                             f"total_read_bytes={metrics['total_read_bytes']} 期望 {expect}")
    if metrics["hits"] != calls:
        raise AssertionError(f"自测：命中计数丢增量 hits={metrics['hits']} 读次数={calls}")
    if metrics["measure_void"] or metrics["queue_timeouts"]:
        raise AssertionError("自测：不该出现读队列超时/作废")
    # 9.2：stall 按步 wait_s/work_s 聚合；epoch0 前 10 步不进样本，
    # 自测跑 2 个 epoch（每 epoch 4 步），样本应来自 epoch1 且数量正确。
    if metrics["stall_n"] <= 0:
        raise AssertionError("自测：stall 样本为空（epoch0 前 10 步剔除后应有 epoch1 的样本）")
    if metrics["stall_n"] > 4:
        raise AssertionError(f"自测：stall 样本数不对 n={metrics['stall_n']}")
    if not isinstance(metrics["stall_ratio"], float):
        raise AssertionError(f"自测：stall_ratio 非数值 {metrics['stall_ratio']!r}")
    if not (0.0 <= metrics["stall_ratio"] <= 1.0):
        raise AssertionError("自测：stall_ratio 越界 " + str(metrics["stall_ratio"]))
    if not isinstance(metrics["stall_p90"], float) or \
            metrics["stall_p90"] < metrics["stall_ratio"]:
        raise AssertionError(f"自测：stall_p90 不对 p90={metrics['stall_p90']!r} "
                             f"mean={metrics['stall_ratio']!r}")
    # 9.2：算力按墙钟截断，实测均值不得低于本次下发的目标算力
    target_ms = float(base_env["COMPUTE_MS"])
    if not isinstance(metrics["compute_actual_ms"], float) or \
            metrics["compute_actual_ms"] < target_ms:
        raise AssertionError(f"自测：compute_actual_ms 未达目标 {metrics['compute_actual_ms']!r}"
                             f" < {target_ms}ms")
    # 9.2：分阶段字节账在 direct 组也要成立（每片按 phase 计一次）
    phase_sum = (metrics["read_bytes_startup"] + metrics["read_bytes_steady"]
                 + metrics["read_bytes_switch"])
    if phase_sum != metrics["total_read_bytes"]:
        raise AssertionError(f"自测：分阶段字节和不等于总读字节 {phase_sum} != "
                             f"{metrics['total_read_bytes']}")
    if min(metrics["read_bytes_startup"], metrics["read_bytes_steady"],
           metrics["read_bytes_switch"]) <= 0:
        raise AssertionError("自测：startup/steady/switch 三段没有都记到字节")
    if metrics["peak_resident_bytes"] <= 0:
        raise AssertionError("自测：常驻字节计数没生效")
    print(f"[selftest] 3) 字节账精确：reads={metrics['reads']} "
          f"total_read_bytes={metrics['total_read_bytes']} == reads×{fake_bytes}；"
          f"hits={metrics['hits']} 无丢计数；peak_resident_bytes="
          f"{metrics['peak_resident_bytes']}；stall n={metrics['stall_n']} "
          f"mean={metrics['stall_ratio']} p90={metrics['stall_p90']}；"
          f"compute_actual_ms={metrics['compute_actual_ms']}；"
          f"分阶段字节 startup/steady/switch="
          f"{metrics['read_bytes_startup']}/{metrics['read_bytes_steady']}/"
          f"{metrics['read_bytes_switch']}")

    # ---- 4) 节点缓存清理命令都走 sh -c ----
    for cmd in _node_cache_reset_cmds("dummy-node"):
        if cmd[:5] != ["docker", "exec", "dummy-node", "sh", "-c"]:
            raise AssertionError(f"节点清理命令没走 sh -c: {cmd}")
        if not cmd[5].startswith("rm -rf ") or not cmd[5].endswith("/*"):
            raise AssertionError(f"节点清理命令形态不对: {cmd}")
    check = _node_cache_check_cmd("dummy-node")
    if check[:5] != ["docker", "exec", "dummy-node", "sh", "-c"]:
        raise AssertionError(f"节点判空命令没走 sh -c: {check}")
    if "ls -A" not in check[5] or "[ -d " not in check[5]:
        raise AssertionError(f"节点判空命令形态不对: {check}")
    print("[selftest] 4) 节点缓存清理命令全部走 sh -c（glob 在容器内展开），清后判空命令形态正确")

    # ---- 5) 读队列超时：wait 照实计入 + 本轮标 void（bad 步不进 stall 样本）----
    # 超时那步 work_s=0，不能做 wait_s/work_s 的样本（除零），所以 stall_n=0、
    # stall_ratio 留空；但 wait_s 仍记进 stall_total 诊断账，本轮 void=1。
    if timeout_metrics["queue_timeouts"] != 1:
        raise AssertionError("自测：读队列超时应记 1 次")
    if not timeout_metrics["measure_void"]:
        raise AssertionError("自测：读队列超时时本轮应标 void")
    if timeout_metrics["reads"] != 0 or timeout_metrics["total_read_bytes"] != 0:
        raise AssertionError("自测：超时那轮不该有读取字节")
    if timeout_metrics["stall_n"] != 0 or timeout_metrics["stall_ratio"] != "":
        raise AssertionError(f"自测：超时那轮不该有 stall 样本 "
                             f"n={timeout_metrics['stall_n']} "
                             f"ratio={timeout_metrics['stall_ratio']!r}")
    if not timeout_ns.get("stall_total", 0) > 0:
        raise AssertionError("自测：超时那步的 wait_s 没记进诊断账")
    if measure_void_reason(timeout_metrics) != "read_queue_timeout":
        raise AssertionError("自测：作废原因映射不对: "
                             + measure_void_reason(timeout_metrics))
    if measure_void_reason(metrics) != "":
        raise AssertionError("自测：正常轮不该被标作废")
    # 快照缺片 → void=1 且 reason 落 read_error（作废清单封闭）
    if measure_void_reason({"measure_void": 1, "snapshot_missing": 1}) != "read_error":
        raise AssertionError("自测：快照缺片的作废原因不是 read_error")
    print(f"[selftest] 5) 超时分支：wait 计入诊断账(stall_total={timeout_ns['stall_total']:.3f}s)、"
          f"bad 步不进 stall 样本(n={timeout_metrics['stall_n']})，本轮 void=1 "
          f"reason=read_queue_timeout；快照缺片 reason=read_error")

    # ---- 6) D30/D31/D32 接线：档位过滤 / 探点状态 / 带宽格推导 / 替补判定 ----
    m_pick, m_drop = select_groups("M")
    if set(m_pick) != CAPACITY_TIER_GROUPS["M"] or m_drop:
        raise AssertionError(f"自测：M 档默认参跑组不对 picked={m_pick} dropped={m_drop}")
    l_pick, l_drop = select_groups("L", ["traincache-warm", "fluid-warm",
                                        "traincache-cold"])
    if l_pick != ["traincache-cold"] or l_drop != ["traincache-warm", "fluid-warm"]:
        raise AssertionError(f"自测：L 档越组态没被剔除 picked={l_pick} dropped={l_drop}")
    s_pick, _ = select_groups("S")
    if s_pick != GROUPS:
        raise AssertionError("自测：S 档应七组态全跑")
    if row_status("S") != "ok" or row_status("S", True) != "void":
        raise AssertionError("自测：S 档 status 映射不对")
    if row_status("L") != "exploratory" or row_status("L", True) != "void":
        raise AssertionError("自测：L 档探点/作废标记不对")
    if TIER_PRESETS["S"]["epochs"] != 3 or TIER_PRESETS["M"]["epochs"] != 1 \
            or TIER_PRESETS["L"]["epochs"] != 1:
        raise AssertionError("自测：档位 epochs 联动不对（S=3/M=1/L=1）")
    for t in ("S", "M", "L"):
        for r in (100, 300, 1000):
            got_r, got_burst = tc_cell(t, f"{r}mbit")
            if got_r != r or got_burst != bench_config.burst_for_rate(r):
                raise AssertionError(f"自测：D31 格子推导不对 {t}@{r} → "
                                     f"burst={got_burst}")
    if hasattr(bench_config, "jitter_enabled"):
        raise AssertionError("自测：config.jitter_enabled 应已撤除（抖动轴不在协议里）")
    all_void = [{"void": True}, {"void": True}]
    if not needs_substitution(all_void) or needs_substitution([{"void": True},
                                                              {"void": False}]) \
            or needs_substitution([]):
        raise AssertionError("自测：替补判据不对（全废→顶位；有任一成功轮→不顶；空→不顶）")
    stamped = stamp_rows([{"rep": 0}], "juicefs-cold", substitute_for="fluid-cold")
    if stamped[0]["group"] != "juicefs-cold" or \
            stamped[0]["baseline_substituted"] != "fluid-cold":
        raise AssertionError("自测：替补标记没落到行上")
    print(f"[selftest] 6) D30/D32 接线：M 档跑 {m_pick}（L 档剔除 {l_drop}）；"
          f"status ok/void/exploratory 三态、S/M/L epochs 3/1/1、"
          f"三档 burst 推导、抖动轴已撤、替补全废判据均自洽")

    # ---- 7) D23 网关开关：新列、取值映射与透传 ----
    if "gateway" not in CSV_FIELDNAMES:
        raise AssertionError("自测：summary.csv 缺 gateway 列")
    if CSV_FIELDNAMES.index("gateway") != CSV_FIELDNAMES.index("gate_mode") + 1:
        raise AssertionError("自测：gateway 列应紧跟 gate_mode")
    if gateway_flag(True) != "1" or gateway_flag(False) != "0":
        raise AssertionError(f"自测：gateway_flag 映射不对 "
                             f"{gateway_flag(True)!r}/{gateway_flag(False)!r}")
    if gateway_flag() != ("1" if GATEWAY_ON else "0"):
        raise AssertionError("自测：gateway 列没有跟随 TRAINCACHE_GATEWAY")
    import inspect  # noqa: PLC0415
    if 'env["TRAINCACHE_GATEWAY"]' not in inspect.getsource(group_up):
        raise AssertionError("自测：group_up 没有把网关开关透传给 up.sh")
    print(f"[selftest] 7) D23 网关开关：gateway 列紧随 gate_mode，"
          f"映射 1/0 正确，GATEWAY_ON={GATEWAY_ON} 已透传到 up.sh")
    print("run_bench selftest: PASS")


def main():
    parser = argparse.ArgumentParser(description="五组对照跑分")
    parser.add_argument("--reps", type=int, default=None,
                        help="每点采样次数；缺省按档位取 D19/D32 值（S=3/M=3/L=1）")
    parser.add_argument("--tier", choices=("S", "M", "L"), default=TIER,
                        help="容量档位(默认取 CAPACITY_RATIO 环境变量,缺省 S)")
    parser.add_argument("--rate", type=str, default=None,
                        help="带宽档（如 300mbit）；S 档缺省 100mbit，M/L 档必填")
    parser.add_argument("--groups", nargs="+", default=None,
                        help="要跑的组态；缺省按档位取（D30：S=七组态，M=direct/fluid-cold/"
                             "traincache-cold，L=traincache-cold/fluid-cold）。"
                             "显式指定时与该档求交集，越档的组态会被剔除")
    parser.add_argument("--out", type=str,
                        default=str(bench_config.RESULTS_DIR / "compare" / "summary.csv"))
    parser.add_argument("--selftest", action="store_true",
                        help="离线自检（不连集群、不起 Pod）后退出")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印本轮实验计划（组态×次数×epochs、D31 带宽格、trace sha），"
                             "不连集群、不起 Pod（开跑前核对用）")
    args = parser.parse_args()

    if args.selftest:
        selftest()
        return

    tier = args.tier
    # 速率只从 --rate 取：S 档缺省 100mbit；M/L 只有 @1000 格，不传会落到
    # 预注册外的档位，直接退出。
    global TC_RATE_EFF
    if args.rate is not None:
        TC_RATE_EFF = args.rate
    elif tier == "S":
        TC_RATE_EFF = bench_config.TC_RATE
    else:
        raise SystemExit(
            f"[cfg] {tier} 档只有 @1000 格，必须显式传 --rate"
            f"（如 --rate 1000mbit），退出")
    apply_tier(tier)                      # D32：档位联动 n_tars / epochs
    tier_reps = TIER_PRESETS[tier]["reps"]
    allowed_reps = CAPACITY_TIER_REPS_ALLOWED[tier]
    if args.reps is None:
        reps = tier_reps
    else:
        reps = args.reps
        if FIGURE_MODE:
            pass
        elif reps not in allowed_reps:
            print(f"[warn] --reps={reps} 不在 {tier} 档的合法次数 "
                  f"{sorted(allowed_reps)}（规定值 {tier_reps}）内；"
                  f"这轮产物只能当冒烟，不进正式表")
        elif reps != tier_reps:
            print(f"[note] --reps={reps} 是 {tier} 档的合法值"
                  f"（规定值 {tier_reps}，D32）")

    picked, dropped = select_groups(tier, args.groups)
    if dropped:
        print(f"[cfg] D30 档位过滤：{tier} 档剔除越档组态 {dropped}")
    if not picked:
        raise SystemExit(f"[cfg] {tier} 档没有可跑组态（请求 {args.groups}），退出")

    rate_mbit, burst_eff = tc_cell(tier, TC_RATE_EFF)
    if tier == "S" and rate_mbit == 300 and not FIGURE_MODE:
        if args.groups is None:
            raise SystemExit(
                "[cfg] S@300 只允许 direct/fluid-cold/traincache-cold 三组，"
                "必须显式传 --groups，退出")
        extra = [g for g in args.groups if g not in S300_GROUPS]
        if extra:
            raise SystemExit(
                f"[cfg] S@300 不接受组态 {extra}，只允许 {list(S300_GROUPS)}，退出")
    if rate_mbit not in bench_config.TC_RATE_MBITS:
        print(f"[warn] --rate={TC_RATE_EFF} 不在 D31 带宽轴 "
              f"{list(bench_config.TC_RATE_MBITS)} 内，这轮不是正式实验点")

    trace = build_run_trace()
    trace_sha = trace_mod.trace_sha256(trace)
    trace_json = json.dumps(trace, ensure_ascii=False, indent=2) + "\n"

    print(f"[cfg] tier={tier} n_tars={N_TARS}(数据集≈{N_TARS*SHARD_SIZE_MB}MiB) "
          f"epochs={EPOCHS} steps_per_epoch={STEPS_PER_EPOCH} batch={BATCH} "
          f"images_per_tar={IMAGES_PER_TAR} shuffle={TRACE_SHUFFLE} seed={TRACE_SEED}")
    print(f"[cfg] chunk={CHUNK_SIZE}B cache={CACHE_BYTES}B warmup_k(warm)={WARMUP_K} "
          f"prefetch_k={PREFETCH_K} agent_lookahead={AGENT_PREFETCH_K} "
          f"compute_ms={COMPUTE_MS} workers={READ_WORKERS} "
          f"depth={READ_QUEUE_DEPTH} dataset={DATASET_NAME}")
    print(f"[cfg] D31 带宽格: rate={TC_RATE_EFF} delay={TC_DELAY_EFF} "
          f"burst={burst_eff} netem=固定 delay "
          f"(BW/r={bench_config.bw_over_r(tier, rate_mbit):.2f}，"
          f"<1 链路受限、>2 计算主导)")
    print(f"[cfg] D32 核对: {tier} 档每点 {tier_reps} 次"
          f"（合法 {sorted(allowed_reps)}），本轮 reps={reps}；"
          f"参跑组态={picked}；trace sha256={trace_sha}")

    if args.dry_run:
        print(f"[dry-run] 计划：{len(picked)} 个组态 × {reps} 次 = "
              f"{len(picked) * reps} 轮 → " +
              " ".join(f"{g}×{reps}" for g in picked))
        print("[dry-run] 冷组（含 direct）跨 rep 复用控制面，warm 组每轮整组 down/up；"
              "Fluid 全轮作废时按 D30 落回 JuiceFS 同冷热档顶位")
        print(f"[dry-run] 未连集群、未起 Pod，产物 {args.out} 未写入")
        return

    out_path = resolve_out_path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # 图系列的缓存容量与策略逐 COND 变化：先把 ConfigMap 对齐，再起组。
    # 三档参数与默认值相同，这里是空操作。
    ensure_agent_config()
    write_cell_lock(out_path, tier, picked, rate_mbit, TC_RATE_EFF)

    all_results = []
    for group in picked:
        results = run_group(group, reps, out_path, trace_json, trace_sha)
        all_results.extend(results)
        write_csv(out_path, all_results)
        # D30：Fluid 整组起不来 → 落回同冷热档的 JuiceFS 顶位并标记
        sub = SUBSTITUTE_GROUP.get(group)
        if sub and needs_substitution(results):
            if sub in picked:
                print(f"[substitute] {group} 全轮作废，但 {sub} 本轮已单独在跑，"
                      f"不重复顶位；{group} 这一格按空缺处理")
            else:
                print(f"[substitute] {group} 全 {len(results)} 轮作废 → 用 {sub} 顶位"
                      f"（产物标 baseline_substituted={group}，D30）", flush=True)
                sub_rows = run_group(sub, reps, out_path, trace_json, trace_sha,
                                     substitute_for=group)
                all_results.extend(sub_rows)
                write_csv(out_path, all_results)

    print(f"\n[done] 结果写入 {out_path}")
    print(f"[done] 共 {len(all_results)} 条记录，{sum(1 for r in all_results if r.get('void'))} 条作废")


if __name__ == "__main__":
    main()
