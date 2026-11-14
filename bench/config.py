#!/usr/bin/env python3
"""TrainCache bench 装置统一参数源(唯一参数点,bash/python 共用)。

bash 侧取值:python3 config.py --sh <KEY>
"""
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
# 数据与产物的唯一落点:repo 同级的 traincachedata,目录结构与 repo 模块一一对应。
# repo 内不放数据,也不放指向数据层的软链接。CI 用 TRAINCACHE_DATA_ROOT 改到 workspace 内。
DATA_ROOT = Path(os.environ.get("TRAINCACHE_DATA_ROOT") or (REPO_ROOT.parent / "traincachedata")).resolve()
DATA_DIR = DATA_ROOT / "bench" / "data"
DATASET_DIR = DATA_DIR / "dataset" / "tars"
TILT_DIR = DATA_DIR / "tilt"
TRAIN_DIR = DATA_DIR / "train" / "tars"
RESULTS_DIR = DATA_ROOT / "bench" / "results"
# 产物根下按族 ID 分片:results/<族>/<格子>/。族名与 COND 的 family 字段同源,
# 一个族一个目录,汇总与出图按族各自读自己的目录。
BENCH_FAMILIES = ("compare", "gateway", "evict", "prefetch", "warmup",
                  "switch", "chaos", "multiconsumer", "e2e", "rangepar")
LOGS_DIR = DATA_ROOT / "bench" / "logs"
COMPARE_LOGS_DIR = DATA_ROOT / "bench" / "compare" / "logs"
SUDO_PASS_FILE = ROOT / ".sudo-pass"


def family_results_dir(family: str) -> Path:
    """该族的产物根。族名拼错时立刻失败,避免把产物写到不存在的目录里。"""
    if family not in BENCH_FAMILIES:
        raise ValueError(f"未知族: {family!r}")
    return RESULTS_DIR / family


# ---- kind 集群 ----
CLUSTER_NAME = "traincache"
K8S_NS_MINIO = "minio"
MINIO_BUCKET = "bench-b"           # 装置专用桶(与现勘 train-data 隔离)
MINIO_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"
MINIO_NODEPORT = 30090             # 宿主机上传用(经节点 InternalIP 访问:kind 不映射 nodePort 到宿主机)
MINIO_ACCESS_KEY = "traincache"
MINIO_SECRET_KEY = "traincache-probe"

SEED = 20261006

# ---- 变量空间 ----
# 主轴 rho = 数据集字节 ÷ 缓存容量。数据集固定 4 GiB,换 rho 只改缓存容量这一个
# 环境变量,因此 rho 与数据集规模、epoch 数、采样次数互不影响。
RHO_VALUES = (0.5, 1.0, 2.0, 4.0, 8.0)
EPOCHS = 3                        # 全部格子 3 epoch,不再由档位推导
REPS = 3                          # 每格 3 次采样
E2E_REPS = 2                      # 端到端族按场景 2 次,只做二值判定
MAX_READERS = 4                   # 单格并发读端上限,与 kind 的 4 个 worker 对齐
EPOCH_STEPS = 400                 # 每 epoch 步数
COMPUTE_MS = 200                  # 每步计算耗时;只做相对比较,不与真实模型等价


def cache_bytes(rho: float) -> int:
    """缓存容量字节:数据集字节 ÷ rho。rho ≤ 1 数据装得下,> 1 每轮都要回源。"""
    return int(DATASET_BYTES / rho)


# ---- 主数据集:16 × 256 MiB = 4 GiB,固定不随实验变量变化 ----
DATASET_SHARDS = 16
DATASET_SHARD_BYTES = 1 << 28      # 256 MiB
DATASET_BYTES = DATASET_SHARDS * DATASET_SHARD_BYTES
DATASET_IMGS_PER_TAR = 1600        # 1600 × ~160KB ≈ 256MiB/tar
DATASET_IMG_SIZE = 512
DATASET_JPEG_QUALITY = 85
DATASET_TARGET_IMG_KB = 160        # σ 自动校准到该体积

# ---- 倾斜负载:热分片比例;数据复用主数据集,只生成清单 ----
TILT_HOT_RATIO = 0.10

# ---- 训练负载:单 Job 连续多 epoch,MinIO 块缓存源 ----
# 16×800 张 512px JPEG q85(~160KB/张)≈2GiB,落 train/tars/,经 trainpublish
# 发块到 MinIO;集群内训练 Pod 从 agentd 本地块缓存拼 tar 读取,每 epoch 换 seed。
TRAIN_N_TARS = 16
TRAIN_IMAGES_PER_TAR = 800
TRAIN_IMG_SIZE = 512
TRAIN_JPEG_QUALITY = 85
TRAIN_TARGET_IMG_KB = 160
TRAIN_SEED = 20261015
TRAIN_EPOCHS = 10                  # 负载器自身的缺省值;正式格子的 epoch 由 COND 注入
TRAIN_WORKERS = 4
TRAIN_BATCH = 32
TRAIN_STEPS_PER_EPOCH = EPOCH_STEPS
TRAIN_CACHE_MB = 1024              # 欠配:2GiB 数据集的 50%
TRAIN_WARMUP = 10                  # 仅 epoch0 前 10 步,口径与探针 C 一致
# 切换用的另一份训练数据(e2e checkpoint-version 场景阶段二读它):与基础数据
# (train/tars/)同形状、同规模、同等在用,只是种子不同 → 字节不同 → 块指纹不同,
# 用来验缓存失效判定。落在 train/alt/ 子目录,与 train/tars/ 平级。
TRAIN_DIR_ALT = DATA_DIR / "train" / "alt"
TRAIN_SEED_ALT = 20261016
TRAIN_RESULTS_DIR = RESULTS_DIR / "train"
TRAIN_TC_RATE = "100mbit"
TRAIN_TC_DELAY = "20ms"

# ---- 训练负载打点口径 ----
TRAIN_SHARD_BYTES = 128 << 20      # 单个 tar 目标字节(2GiB / 16)
TRAIN_CHUNK_BYTES = 64 << 20       # DataSet.spec.chunkSize
TRAIN_WINDOW_K = 8                 # 启动/切换窗口块数
TRAIN_BASELINE_STEPS = 20          # 回落基线:切换前 20 步
TRAIN_CURRICULUM_FRACTIONS = (1.0, 2.0 / 3.0, 1.0 / 3.0)

# ---- tc 限速档位(扫点实验的自变量)----
TC_RATE = "100mbit"
TC_BURST = "1600k"          # 旧固定值。tc 把 k 解析成 1024:1 638 400 字节,对 @100 的
                            # tbf 速率 12.5 MB/s 是 131 ms 突发窗口,对 @1000 只剩 13 ms
TC_LATENCY = "50ms"         # tbf 的 latency 决定队列长度,不覆盖 burst(容器内实测)
TC_DELAY = "20ms"

# ---- 带宽档口径(D31:带宽是实验自变量)----
# rate 的取值集合;COND 的 network.rate_mbit 只能取这些值。
TC_RATE_MBITS = (100, 300, 1000)


def burst_for_rate(rate_mbit: int) -> str:
    """burst 取「0.1 秒的传输量」:rate_mbit × 125 KB/s × 0.1 s,带 k 后缀交给 tc。

    100mbit → 1250k、300mbit → 3750k、1000mbit → 12500k。

    两处按十进制算会读错的地方:
    · tc 把 k 解析成 1024 不是 1000:1250k 挂上后是 1 280 000 字节,
      `tc qdisc show` 回读打印成 burst 1250Kb(KiB,不是 kilobit);对 @100 的
      tbf 速率 12.5 MB/s 突发窗口是 102 ms,对实测有效带宽 11.95 MB/s 是 107 ms。
    · 命令行同时给的 `latency 50ms` 不覆盖这个桶:同一容器内实测 latency 取
      5ms / 50ms / 500ms,回读的 burst 恒为命令行给的 1250Kb;latency 只定队列
      长度上限。
    `make bench-doctor` 把这两条钉住:从节点 netns 回读的 burst 折算成字节后
    必须等于本函数的返回值(±1%,容纳 iproute2 按 32 位字取整的零头,实测
    3750k 回读成 3839962b)。
    """
    return f"{round(rate_mbit * 125 * 0.1)}k"


# ---- 倾斜族的字节地板(读侧过滤 row_gate.py 的除数)----
# 倾斜负载走 @100 mbit 时回源是串行阻塞,所以「跑完 time ÷ 有效带宽」才是这一族
# 成立的物理下界;其余族的预热与计算重叠,同一比值普遍高于线速,不套这道地板。
# 每档实测有效带宽(MB/s)。@100 = 2026-11-02 集群内只读探针,同一对象四种
# 取法同值;@300 = 4.2 的机时账一节的 35.6(95% 线速)。表里没有的档位给不出地板,
# 读侧跳过第三条而不是猜一个值。
TILT_EFF_BW_MB_S = {100: 11.95, 300: 35.6}


# ---- 并行下载族的比值合格区间(4.3 的 rangepar·并行收益)----
# 键是链路条件档,值是「关臂 makespan ÷ 开臂 makespan」的 (下限, 上限)。
# 上限为 None 表示只管下限。l0 单流不限,两臂同速,比值应落在 1 附近。
RANGEPAR_RATIO = {
    "l0": (0.9, 1.1),
    "l1": (1.8, None),
    "l2": (1.8, None),
    "l3": (1.5, None),
}


def rate_mbit(value) -> int | None:
    """'100mbit' / 100 / '100' → 100;解析不出给 None。"""
    m = re.search(r"(\d+)", str(value or ""))
    return int(m.group(1)) if m else None


def tilt_floor_seconds(row: dict) -> float | None:
    """该行回源字节走完整整形链路所需的最短秒数;取不到给 None。

    按 load 字段判定是不是倾斜负载,不看 COND 名前缀:改名与新增族都不会漏判。
    """
    if row.get("load") != "tilt":
        return None
    bw = TILT_EFF_BW_MB_S.get(rate_mbit(row.get("rate")))
    try:
        fetch = float(row.get("fetch_bytes_run") or "")
    except ValueError:
        return None
    return fetch / (bw * 1e6) if bw and fetch > 0 else None


# ---- 冷态规程与验证 ----
COLD_VERIFY_RUNS = 2               # 过关标准:两次冷读对上
COLD_VERIFY_TOLERANCE = 0.10       # 两次冷读相对差 ≤10% 判"对上"
COLD_READ_IMAGE = "busybox:latest" # 集群内冷读执行者


def _sh():
    key = sys.argv[2]
    value = globals()[key]
    if isinstance(value, Path):
        value = str(value)
    elif isinstance(value, (list, tuple)):
        value = " ".join(str(v) for v in value)
    print(value)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--sh":
        _sh()
    else:
        print("用法: config.py --sh <KEY>")
        sys.exit(1)
