#!/usr/bin/env python3
"""M7 五组对照跑分脚本（统一 train-job Pod 形态，预热先于计时）。

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
  stall_ratio 一律作废（docs/9.1 §8/6.15、docs/4 §10），需按本口径重跑。
  新版用 COMPUTE_MS=200ms 控制每步算力，读与计算并行（后台 reader 线程
  模拟 DataLoader 异步预取，把接下来若干分片读进内存队列，主循环从队头
  取数据 + 做固定算力）：预取有效（热缓存/预取跟上）→ stall_ratio 趋 0；
  预取失效（冷读跟不上算力）→ 队列常被抽空 → 趋 1。
  COMPUTE_MS / READ_WORKERS / READ_QUEUE_DEPTH 全部走 Pod 环境变量，
  模板内不写死。

6.16 命中率对账（D25）：
  traincache 组旧口径"块在磁盘存在即 hit"虚高（agent 运行期悄悄回源补块，
  测量体不知道），已作废。改造后用 agent 指标 traincache_fetch_bytes_total
  的 run 窗口增量对账：回源字节计 miss，
  hit_rate = (总读字节 − 回源字节) / 总读字节。
  取数路径：优先 Prometheus（PROMETHEUS_URL 环境变量，复用
  experiments/collect.py 的查询函数），否则
  `kubectl get --raw ...pods/<agentd>:9411/proxy/metrics` 直接抓 agentd。
  JuiceFS/Fluid 无块级命中指标，仍按 warm≈全命中 / cold≈全回源 + 回源
  字节的模型近似，产物 hit_rate_basis 列显式标注近似性。
  summary 产物补三笔字节成本列：启动预热×fanout、持续预取、切换重预取（D5）。

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
  · D31 带宽格由 runner 现推并打印（rate/burst/抖动/BW÷r）；rate 只从 `--rate`
    取，再以 TC_RATE 环境变量连同 TC_DELAY、CAPACITY_RATIO 下发给 up.sh，
    shell 侧据此挂 tc（临界格才开 netem 抖动）。

用法：
  python3 bench/compare/run_bench.py --out compare/summary.csv
  # --out 相对路径按 bench/config.py 的 RESULTS_DIR(~/tcd/bench/results)展开,
  # 不落到 repo 的当前工作目录;绝对路径原样使用。
  python3 bench/compare/run_bench.py --tier M --dry-run        # 开跑前核对计划
  python3 bench/compare/run_bench.py --tier S --rate 300mbit   # S@300 临界格（带抖动）
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
  DATASET_NAME           TrainCache 快照名，默认 compare-data-8m（9.1 Q12；
                         MinIO 内该快照由 trainpublish -chunk-size 8388608
                         发布属后续动作，本脚本只按新名引用）
  TRAINCACHE_CACHE_CAPACITY_BYTES  统一缓存容量，默认 1GiB
  TRAINCACHE_WARMUP_K    warm 组预热块数；缺省 = 缓存容量/块大小
                         （1GiB/8MiB=128；旧 64MiB 块时代为 16，D20 连带重标度）
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
# D31：带宽档的 burst/抖动推导口径也取自 bench/config.py，shell 与 runner 同一套数
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
# warm 组预热块数：k = 缓存容量 / 块大小（8MiB → 128；旧 64MiB 时代为 16）
WARMUP_K = int(os.environ.get("TRAINCACHE_WARMUP_K",
                              str(max(1, CACHE_BYTES // CHUNK_SIZE))))
# 消费窗口块数：D21/任务 4.8 要求 lookahead 三档统一 16（8MiB 下窗口
# 16×8MiB=128MiB≈缓存 1/8，无退化）；旧默认 8 与 D21 不符，改为 16。
PREFETCH_K = int(os.environ.get("TRAINCACHE_PREFETCH_K", "16"))

# ---- D31：带宽档（rate 是实验自变量，取 {100,300,1000} mbit）----
# 速率只从命令行 --rate 取：S 档缺省 100mbit，M/L 档必填。main() 解析后赋值，
# 再以 TC_RATE 环境变量下发给 up.sh；burst 与抖动不写死，由
# config.burst_for_rate / config.jitter_enabled 现推。
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
AGENT_NS = "traincache"
AGENT_LABEL = "app=traincache-agentd"
AGENT_METRICS_PORT = 9411  # agentd /metrics（docs/8.4：TRAINCACHE_METRICS_ADDR=":9411"）
AGENT_DEGRADED_SAMPLE_S = 5  # 测量期间降级采样间隔
AGENTD_HIDE_TIMEOUT_S = 120  # 缩到 0 后等 Pod 全消失
AGENTD_READY_TIMEOUT_S = 180  # 缩回 1 后等全部 agentd Ready
AGENTD_CLEAN_TIMEOUT_S = 60  # 等 degraded=0 且 fetch 计数停增

POD_TIMEOUT = int(os.environ.get("POD_TIMEOUT", "1800"))

# 命中率口径标注（D25，产物列 hit_rate_basis 显式可见）
BASIS_TC_RECON = "agent_fetch_bytes_reconciliation(D25)"
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


def _clear_node_caches(tag):
    """清四个组在 kind 节点上的缓存目录（hostPath，不在 Pod 里）。

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


def cold_reset():
    print("[cold_reset] 清理训练 Pod 与缓存(保留 Redis 元数据)...")
    for ns in set(GROUP_NS.values()):
        # 保留 Redis Pod(元数据引擎),其余全删以清空 emptyDir 缓存
        kubectl(["delete", "pods", "-l", "app!=redis", "--grace-period=0", "--force"], namespace=ns, timeout=60)
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
    print("[agent_reset] agentd 复位完成：degraded=0，fetch 计数停增，hook socket 就绪",
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


def group_up(group):
    print(f"[up] {group}...")
    env = os.environ.copy()
    env["N_TARS"] = str(N_TARS)
    # D30/D31：把档位与带宽档显式下发给 up.sh —— up.sh 用 CAPACITY_RATIO 组
    # tc_apply 的 --tier（只有临界格开 netem 抖动），burst 由 tc_apply 按 rate
    # 推导，两侧不再各处写死。
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
        # cold 预热 k=0(不预热,WarmupTask 快速 Released)
        env["TRAINCACHE_WARMUP_K"] = str(WARMUP_K) if group == "traincache-warm" else "0"
        env["DATASET"] = DATASET_NAME
    rc, out, err = run_cmd(["bash", str(GROUP_UP[group])], env=env, timeout=3600)
    if rc != 0:
        print(f"[up] {group} 失败(rc={rc}): {err[:800]}")
        return False, 0.0
    # 解析 up.sh 输出中的预热时长标记
    warmup_seconds = 0.0
    for line in out.splitlines() + err.splitlines():
        m = re.search(r"\[WARMUP_SECONDS\]\s+(\d+)", line)
        if m:
            warmup_seconds = float(m.group(1))
            break
    # direct / juicefs-cold / fluid-cold / traincache-cold 默认 0.0
    print(f"[up] {group} 预热时长={warmup_seconds}s")
    return True, warmup_seconds


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
def read_shard(shard):
    """直读 MinIO：分段读整片，只累加长度（不把 256MiB 读回来常驻内存）。"""
    global misses
    url = endpoint + "/" + bucket + "/shard-" + str(shard).zfill(5) + ".tar"
    nbytes = http_stream_bytes(url)
    with counter_lock:
        misses += 1  # 直读无缓存层，全部计回源
    return nbytes
'''
    elif group in ("juicefs-cold", "juicefs-warm"):
        return r'''
def read_shard(shard):
    global hits, misses
    p = "/mnt/jfs/shard-" + str(shard).zfill(5) + ".tar"
    nbytes = read_file_bytes(p)
    # JuiceFS 无法输出块级命中指标：按 D25 近似——warm 组 warmup 后读≈全命中，
    # cold 组≈全回源（分片粒度计数，产物 hit_rate_basis 列显式标注近似）。
    with counter_lock:
        if os.environ.get("TRAINCACHE_GROUP") == "juicefs-warm":
            hits += 1
        else:
            misses += 1
    return nbytes
'''
    elif group.startswith("fluid"):
        return r'''
def read_shard(shard):
    global hits, misses
    p = "/data/shard-" + str(shard).zfill(5) + ".tar"
    nbytes = read_file_bytes(p)
    # Fluid 同样无块级命中指标，D25 近似口径（见 JuiceFS 注释）。
    with counter_lock:
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

def _hook_send(frame):
    """best-effort 上报一帧；每次新建 socket，多读线程并发安全。"""
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

def read_shard(shard):
    """traincache 读取路径：逐块读本地缓存，缺块等待 agentd 预取，超时兜底回源。
    返回本片字节数（累加各块长度，不把整片拼起来留在内存里）。
    注意：hits/misses 只是块级诊断(block_hit_rate_diag 列)——"块在磁盘存在即
    hit"口径会虚高(agent 运行期悄悄回源补块)，2026-10-24 已作废(9.1 6.16/D25)；
    正式命中率由 runner 侧用 traincache_fetch_bytes_total run 窗口增量对账。"""
    global hits, misses
    name = "shard-" + str(shard).zfill(5) + ".tar"
    chunks = _load_snapshot().get(name, [])
    nbytes = 0
    n_hits = 0
    n_misses = 0
    for chunk_idx, fp, size in chunks:
        blk = os.path.join(CACHE_ROOT, fp[:2], fp + ".blk")
        # 命中:直接读本地缓存
        if os.path.exists(blk):
            nbytes += read_file_bytes(blk)
            n_hits += 1
            _hook_advance(name, chunk_idx)
        else:
            # 未命中:先报 agent 触发 P0 demand 补块,再等持续预取(轮询,最多等 180s)
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
# （docs/9.1 §8 6.15、docs/4 §10）
#
# 口径:wait_s = 主循环从队列取"已读好分片"的阻塞等待(真 stall);
#       work_s = 固定算力耗时(与后台读并行发生,不计入 stall);step_s = wait_s + work_s。
#       预取有效(数据在本地缓存、读得快) -> stall_ratio = Σwait/Σstep 趋 0;
#       预取失效(要现回源) -> 队列常被抽空 -> 趋 1。

# 算力标定:先测 matmul 单位耗时(3 次取中位,抑噪),再定每步重复次数,使 work_s 逼近 COMPUTE_MS。
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
    """固定算力:重复 COMPUTE_REPS 次 matmul,返回实际算力耗时(秒)。"""
    t = time.perf_counter()
    for _ in range(COMPUTE_REPS):
        matmul()
    return time.perf_counter() - t

def read_pipeline(shards, q, cursor, cursor_lock):
    """后台读线程:把 shards 里的分片读进有界队列 q。
    队列元素是 (shard, nbytes, err, read_s) —— 只带字节数不带数据体,
    队列常驻内存 O(1);真实加载器会持有的字节数由 add_resident 纯计数跟踪。
    队列满 -> put 阻塞,天然限流;每个分片恰好入队一次(字节数或异常)。"""
    while True:
        with cursor_lock:
            i = cursor[0]
            if i >= len(shards):
                return
            cursor[0] = i + 1
        shard = shards[i]
        r0 = time.perf_counter()
        try:
            nbytes = read_shard(shard)
        except Exception as e:
            q.put((shard, 0, str(e), time.perf_counter() - r0))
        else:
            add_resident(nbytes)
            q.put((shard, nbytes, None, time.perf_counter() - r0))

for epoch in range(epochs):
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
    # epoch 切换标记:runner 侧把 prefetch 计数器增量归因到"切换重预取"窗口
    # ts 用纪元时间戳(与 runner 侧 fetch 采样同一把尺子)，但由单调钟推算
    print("[EPOCH_BOUNDARY] epoch=" + str(epoch) + " ts=" + str(now_epoch()), flush=True)
    q = queue.Queue(maxsize=READ_QUEUE_DEPTH)
    cursor = [0]
    cursor_lock = threading.Lock()
    workers = []
    for _w in range(max(1, READ_WORKERS)):
        th = threading.Thread(target=read_pipeline,
                              args=(shards, q, cursor, cursor_lock), daemon=True)
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
                # 跳过会让 step_total 分母偏小 → stall_ratio 偏乐观（结论被带偏）；
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
        # 超时/读错的这一步照常计入统计（wait_s 记实、work_s=0），
        # 不允许"跳一步"把 stall_ratio 的分母做小。
        work_s = 0.0 if bad else compute_work()
        step_s = wait_s + work_s
        stall_total += wait_s
        step_total += step_s
        if t_first_step is None:
            t_first_step = now_epoch()
        t_last_step = now_epoch()
        if bad:
            break

    if measure_void:
        break   # 本轮已作废：不再跑后面的 epoch，也不等后台读线程
                # （它们会阻塞在 q.put 上，白等 READ_TIMEOUT_S 没意义）

    for th in workers:
        th.join(timeout=READ_TIMEOUT_S)

stall_ratio = stall_total / step_total if step_total > 0 else 0
startup_makespan = (t_first_step - t_submit) if t_first_step else 0
total_makespan = (t_last_step - t_submit) if t_last_step else 0
# hit_rate: traincache 组此处只是块级诊断口径(已作废,D25)，runner 用 fetch
# 字节对账覆盖；juicefs/fluid/direct 组是分片计数口径(D25 近似)。
hit_rate = hits / (hits + misses) if (hits + misses) > 0 else 0
if group.startswith("traincache"):
    _hb_stop.set()
print("RESULT startup_makespan=" + str(round(startup_makespan,3)) +
      " total_makespan=" + str(round(total_makespan,3)) +
      " stall_ratio=" + str(round(stall_ratio,4)) +
      " hit_rate=" + str(round(hit_rate,4)) +
      " total_read_bytes=" + str(read_bytes_total) +
      " reads=" + str(read_done) +
      " hits=" + str(hits) + " misses=" + str(misses) +
      " void=" + str(1 if measure_void else 0) +
      " queue_timeouts=" + str(queue_timeouts) +
      " read_errors=" + str(read_errors) +
      " peak_resident_bytes=" + str(peak_resident_bytes), flush=True)
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
        return None

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
    # 任务清单 #5:traincache-warm 组走 traincache-scheduler + webhook gate
    # traincache-cold 不走 WarmupTask(K=0 被 ValidateTask 拒绝),普通调度
    if group == "traincache-warm":
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
        return None
    print(f"[train-job] {group} Pod 已创建,label app=train-job, t_submit={t_submit:.3f}")
    return pod_name


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
    """kubectl --raw 直接抓各节点 agentd :9411/metrics，按 reason 汇总回源字节。"""
    texts = _agent_metrics_texts()
    if texts is None:
        return None
    by_reason = {}
    total = 0.0
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
    """Prometheus 即时查询(复用 experiments/collect.py 的 prom_query)。失败返回 None。"""
    try:
        import collect
        res = collect.prom_query(PROMETHEUS_URL,
                                 "sum by (reason) (traincache_fetch_bytes_total)",
                                 datetime.now(timezone.utc))
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
    """s3-access 标签节点数（WarmupTask fanout，D26/6.6：预热字节按实际 fanout 记总账）。"""
    rc, out, _ = kubectl(["get", "nodes", "-l", "traincache.io/s3-access=true",
                          "--no-headers"], timeout=30)
    if rc != 0:
        return 4
    return max(1, len(out.strip().splitlines()))


def parse_result(logs):
    m = re.search(r"RESULT startup_makespan=([\d.]+) total_makespan=([\d.]+) "
                  r"stall_ratio=([\d.]+) hit_rate=([\d.]+) "
                  r"total_read_bytes=(\d+) reads=(\d+) hits=(\d+) misses=(\d+)"
                  r"(?: void=(\d+))?(?: queue_timeouts=(\d+))?(?: read_errors=(\d+))?"
                  r"(?: peak_resident_bytes=(\d+))?", logs)
    if not m:
        return None
    return {
        "startup_makespan": float(m.group(1)),
        "total_makespan": float(m.group(2)),
        "stall_ratio": float(m.group(3)),
        "hit_rate": float(m.group(4)),
        "total_read_bytes": int(m.group(5)),
        "reads": int(m.group(6)),
        "hits": int(m.group(7)),
        "misses": int(m.group(8)),
        # 2026-10-24 新增：测量体自报"这轮不可信"（读队列超时/读失败）与诊断量
        "measure_void": int(m.group(9) or 0),
        "queue_timeouts": int(m.group(10) or 0),
        "read_errors": int(m.group(11) or 0),
        "peak_resident_bytes": int(m.group(12) or 0),
    }


def parse_epoch_boundaries(logs):
    return [(int(e), float(ts)) for e, ts in
            re.findall(r"\[EPOCH_BOUNDARY\] epoch=(\d+) ts=([\d.]+)", logs)]


def reconcile_metrics(group, metrics, base_snap, samples, final_snap, boundaries):
    """6.16：把 D25 命中率口径与 D5 三笔字节成本写进 metrics 行。"""
    # ---- 第一笔：启动预热字节 × 实际 fanout ----
    if group == "traincache-warm":
        fanout = s3_fanout_count()
        metrics["startup_warmup_bytes"] = WARMUP_K * CHUNK_SIZE * fanout
        metrics["warmup_fanout"] = fanout
        metrics["warmup_basis"] = WARMUP_BASIS_PREFIX
    elif group in ("juicefs-warm", "fluid-warm"):
        # D25 近似：warmup/DataLoad 把整份数据集拉进节点缓存，计 1 份
        metrics["startup_warmup_bytes"] = N_TARS * SHARD_SIZE_MB * (1 << 20)
        metrics["warmup_fanout"] = 1
        metrics["warmup_basis"] = WARMUP_BASIS_FULL
    else:
        metrics["startup_warmup_bytes"] = 0
        metrics["warmup_fanout"] = 0
        metrics["warmup_basis"] = ""
    # direct 组直读 MinIO 的分段读法；其余组不走 HTTP 直读，留空。
    metrics["read_mode"] = READ_MODE_DIRECT if group == "direct" else ""

    if group.startswith("traincache"):
        # ---- traincache 组：fetch 字节 run 窗口增量对账（D25 补注）----
        fetch_d, _reason_d = counter_delta(base_snap, final_snap)
        read_b = metrics.get("total_read_bytes", 0)
        metrics["fetch_bytes_run"] = round(fetch_d, 1) if fetch_d is not None else ""
        if fetch_d is not None and read_b > 0:
            hit = (read_b - fetch_d) / read_b
            metrics["hit_rate"] = round(min(1.0, max(0.0, hit)), 4)
            metrics["hit_rate_basis"] = BASIS_TC_RECON
        else:
            # 对账不可用时不回退旧"磁盘存在即 hit"口径(已作废)，命中率置空
            metrics["hit_rate"] = ""
            metrics["hit_rate_basis"] = BASIS_TC_UNAVAILABLE
        metrics["block_hit_rate_diag"] = metrics["hit_rate_diag"]
        # ---- 第二/三笔：持续预取 vs 切换重预取（按 reason=prefetch 归因）----
        sustained, switch = attribute_prefetch_bytes(samples, boundaries, SWITCH_WINDOW_S)
        metrics["sustained_prefetch_bytes"] = round(sustained, 1) if sustained is not None else ""
        metrics["switch_reprefetch_bytes"] = round(switch, 1) if switch is not None else ""
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
    metrics["hit_rate_diag"] = ""
    return metrics


def measure_void_reason(metrics):
    """测量体自报"这轮不可信"时的作废原因（写进 summary.csv 的 reason 列）。

    读队列超时 / 读失败都会让字节账与 stall_ratio 不可信，这轮不进结论。
    """
    if not metrics.get("measure_void"):
        return ""
    if metrics.get("queue_timeouts"):
        return "read_queue_timeout"
    if metrics.get("read_errors"):
        return "read_error"
    return "measure_void"


# ---------------------------------------------------------------------------
# D30/D32 接线：档位联动、按档位过滤、rep 复用控制面、status 列、Fluid 替补
# ---------------------------------------------------------------------------
def tc_cell(tier, rate_str=None):
    """把 D31 的带宽格落到具体参数：(rate_mbit, burst, 是否抖动)。

    burst 与抖动都从 config 现推（burst_for_rate / jitter_enabled），
    shell 侧与 runner 侧同一套数，不再各处写死 1600k。
    """
    rate_str = rate_str or TC_RATE_EFF
    m = re.search(r"\d+", rate_str)
    if not m:
        return None, bench_config.TC_BURST, False
    r = int(m.group())
    return r, bench_config.burst_for_rate(r), bench_config.jitter_enabled(tier, r)


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
    allowed = CAPACITY_TIER_GROUPS[tier]
    return ([g for g in ordered if g in allowed],
            [g for g in (requested or []) if g not in allowed])


def row_status(tier, void=False):
    """产物 status 列：void 轮不进汇总（D14），L 档 1 次是符号探点（D19）。"""
    if void:
        return "void"
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
        up_ok, group_warmup = group_up(group)
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
            up_ok, warmup_seconds = group_up(group)
            if not up_ok:
                results.append({"group": group, "rep": rep, "void": True,
                                "reason": "up_failed", "warmup_seconds": warmup_seconds,
                                "trace_sha256": trace_sha})
                continue

        # 6.16:traincache 组在创建 train-job 前抓 fetch 计数器基线
        base_snap = fetch_snapshot() if is_tc else None
        samples = [base_snap] if base_snap else []

        pod_name = create_measure_pod(group, run_id, trace_json)
        if not pod_name:
            results.append({"group": group, "rep": rep, "void": True,
                            "reason": "pod_create_failed", "warmup_seconds": warmup_seconds,
                            "trace_sha256": trace_sha})
            if not reuse:
                group_down(group)
            continue

        ns = GROUP_NS[group]

        def _sample():
            snap = fetch_snapshot()
            if snap is not None:
                samples.append(snap)

        # 测量期间每 5s 采一次 degraded；测量 Pod 一结束就停，避免训练结束
        # 30s 后 sidecar 退出导致的必然降级被误判进本轮。
        degraded = {"max": 0, "samples": 0}
        stop_degraded = threading.Event()
        degraded_thread = None
        if is_tc:
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

        success, logs = wait_pod_complete(ns, pod_name, timeout=POD_TIMEOUT,
                                          on_tick=_sample if is_tc else None,
                                          tick_s=FETCH_SAMPLE_S)
        stop_degraded.set()
        if degraded_thread is not None:
            degraded_thread.join(timeout=1)
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
            metrics["warmup_seconds"] = warmup_seconds
            metrics["trace_sha256"] = trace_sha
            metrics["agent_degraded_max"] = degraded_value
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
            metrics = reconcile_metrics(group, metrics, base_snap, samples,
                                        final_snap, parse_epoch_boundaries(logs))
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
    track = "d" if tier == "S" else "b"
    for g in groups:
        name = f"compare-{track}-{tier.lower()}-{g}-{rate_mbit}"
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


def write_csv(path, results):
    # 列：四指标 + warmup_seconds(任务#8) + tier/rate/trace_sha256(6.12/6.13/D31)
    #     + 三笔字节成本与命中率口径标注(6.16/D5/D25) + 实验参数归档(6.13/6.15)
    #     + status/baseline_substituted(D19/D30：探点与替补一眼可辨，void 不进汇总)
    fieldnames = ["group", "tier", "rate", "rep", "status", "void", "reason",
                  "baseline_substituted",
                  "warmup_seconds",
                  "startup_makespan", "total_makespan",
                  "stall_ratio", "hit_rate", "hit_rate_basis",
                  "total_read_bytes", "read_mode",
                  "startup_warmup_bytes", "warmup_fanout", "warmup_basis",
                  "sustained_prefetch_bytes", "switch_reprefetch_bytes",
                  "fetch_bytes_run", "block_hit_rate_diag",
                  "queue_timeouts", "read_errors", "peak_resident_bytes",
                  "trace_sha256", "chunk_size_bytes", "cache_bytes",
                  "compute_ms", "read_workers", "read_queue_depth",
                  "agent_degraded_max"]
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
            w.writerow(row)


def selftest():
    """离线自检：不连集群、不起 Pod、不访问 MinIO。

    查六件事：
      1. 五组 measure 模板都能编译（模板是拼出来的字符串，语法错要到 Pod 里才炸）；
      2. 模板里的计时只用单调钟——time.time() 只允许出现在纪元对齐的校准点
         （now_epoch 靠它对一次，之后用 perf_counter 推算）；
      3. 字节账精确：换一个假的 read_shard（返回已知字节数）离线跑一遍主循环，
         total_read_bytes 必须 == reads × 每片字节数，且命中计数不丢；
      4. 节点缓存清理命令都走 sh -c（glob 必须在节点容器内展开，否则静默失败），
         清后判空命令形态正确；
      5. 读队列超时那一步计入统计且本轮标 void（stall_ratio 分母不被跳步带小）；
      6. D30/D31/D32 接线的纯函数口径：按档位过滤参跑组、status 三态、
         档位 epochs 联动、带宽格的 burst/抖动推导、Fluid 全废才顶位。
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
        "def read_shard(shard):\n"
        "    global hits, misses, FAKE_CALLS\n"
        "    with counter_lock:\n"
        "        FAKE_CALLS += 1\n"
        "        hits += 1\n"
        "    return FAKE_SHARD_BYTES\n"
    )
    # 读不动：队头等满 READ_TIMEOUT_S 也不来数据，用来验超时分支
    slow_reader = (
        "def read_shard(shard):\n"
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
    globals()["EPOCHS"] = 1
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
        timeout_metrics, _ = _run_measure(
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
    if not (0.0 <= metrics["stall_ratio"] <= 1.0):
        raise AssertionError("自测：stall_ratio 越界 " + str(metrics["stall_ratio"]))
    if metrics["peak_resident_bytes"] <= 0:
        raise AssertionError("自测：常驻字节计数没生效")
    print(f"[selftest] 3) 字节账精确：reads={metrics['reads']} "
          f"total_read_bytes={metrics['total_read_bytes']} == reads×{fake_bytes}；"
          f"hits={metrics['hits']} 无丢计数；peak_resident_bytes="
          f"{metrics['peak_resident_bytes']}")

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

    # ---- 5) 读队列超时：这一步要计入统计 + 本轮标 void ----
    # 旧版是 continue 跳过这一步 → step_total 分母偏小 → stall_ratio 偏乐观；
    # 现在整段等待计入 wait_s（work_s=0），所以 stall_ratio 应趋 1 而不是 0。
    if timeout_metrics["queue_timeouts"] != 1:
        raise AssertionError("自测：读队列超时应记 1 次")
    if not timeout_metrics["measure_void"]:
        raise AssertionError("自测：读队列超时时本轮应标 void")
    if timeout_metrics["reads"] != 0 or timeout_metrics["total_read_bytes"] != 0:
        raise AssertionError("自测：超时那轮不该有读取字节")
    if timeout_metrics["stall_ratio"] < 0.9:
        raise AssertionError("自测：超时那步没计入统计，stall_ratio="
                             + str(timeout_metrics["stall_ratio"]) + "（旧版会被跳成 0）")
    if measure_void_reason(timeout_metrics) != "read_queue_timeout":
        raise AssertionError("自测：作废原因映射不对: "
                             + measure_void_reason(timeout_metrics))
    if measure_void_reason(metrics) != "":
        raise AssertionError("自测：正常轮不该被标作废")
    print(f"[selftest] 5) 超时分支计入统计：stall_ratio="
          f"{timeout_metrics['stall_ratio']}（含超时那步的等待），本轮 void=1 "
          f"reason=read_queue_timeout")

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
    cells = {("S", 300): True, ("M", 1000): True, ("S", 100): False,
             ("S", 1000): False, ("M", 100): False, ("L", 1000): False}
    for (t, r), want_jit in cells.items():
        got_r, got_burst, got_jit = tc_cell(t, f"{r}mbit")
        if got_r != r or got_jit != want_jit or \
                got_burst != bench_config.burst_for_rate(r):
            raise AssertionError(f"自测：D31 格子推导不对 {t}@{r} → "
                                 f"burst={got_burst} jitter={got_jit}")
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
          f"抖动格 S@300+M@1000、替补全废判据均自洽")
    print("run_bench selftest: PASS")


def main():
    parser = argparse.ArgumentParser(description="M7 五组对照跑分")
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
        if reps not in allowed_reps:
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

    rate_mbit, burst_eff, jitter_eff = tc_cell(tier, TC_RATE_EFF)
    if tier == "S" and rate_mbit == 300:
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
          f"prefetch_k={PREFETCH_K} compute_ms={COMPUTE_MS} workers={READ_WORKERS} "
          f"depth={READ_QUEUE_DEPTH} dataset={DATASET_NAME}")
    print(f"[cfg] D31 带宽格: rate={TC_RATE_EFF} delay={TC_DELAY_EFF} "
          f"burst={burst_eff} 抖动={'on' if jitter_eff else 'off'}"
          f"(BW/r={bench_config.bw_over_r(tier, rate_mbit):.2f}，"
          f"临界区 [1,2] 才开)")
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
