#!/usr/bin/env python3
"""TrainCache bench 装置统一参数源(唯一参数点,bash/python 共用)。

bash 侧取值:python3 config.py --sh <KEY>
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
# 数据与产物的唯一落点:repo 同级的 traincachedata,目录结构与 repo 模块一一对应。
# repo 内不放数据,也不放指向数据层的软链接。CI 用 TRAINCACHE_DATA_ROOT 改到 workspace 内。
DATA_ROOT = Path(os.environ.get("TRAINCACHE_DATA_ROOT") or (REPO_ROOT.parent / "traincachedata")).resolve()
DATA_DIR = DATA_ROOT / "bench" / "data"
TRACK_A_DIR = DATA_DIR / "track_a" / "files"
TRACK_B_DIR = DATA_DIR / "track_b" / "tars"
TRACK_C_DIR = DATA_DIR / "track_c"
RESULTS_DIR = DATA_ROOT / "bench" / "results"
LOGS_DIR = DATA_ROOT / "bench" / "logs"
COMPARE_LOGS_DIR = DATA_ROOT / "bench" / "compare" / "logs"
PERF_RESULTS_DIR = DATA_ROOT / "bench" / "perf_results"
SUDO_PASS_FILE = ROOT / ".sudo-pass"

# ---- kind 集群 ----
CLUSTER_NAME = "traincache"
K8S_NS_MINIO = "minio"
MINIO_BUCKET = "bench-b"           # 装置专用桶(与现勘 train-data 隔离)
MINIO_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"
MINIO_NODEPORT = 30090             # 宿主机上传用(经节点 InternalIP 访问:kind 不映射 nodePort 到宿主机)
MINIO_ACCESS_KEY = "traincache"
MINIO_SECRET_KEY = "traincache-probe"

# ---- 三轨数据 ----
SEED = 20261006

# A 轨:元数据压力臂(io_uring 战场,本地目录)
TRACK_A_N_FILES = 1_000_000        # 100 万个
TRACK_A_FILE_KB = 4
TRACK_A_SUBDIRS = 100              # 每子目录 1 万个文件

# B 轨:真实分片主臂(旗舰数字以此为准)
TRACK_B_N_TARS = 32                # 32 × 256MB ≈ 8GB
TRACK_B_IMGS_PER_TAR = 1600        # 1600 × ~160KB ≈ 256MB/tar
TRACK_B_IMG_SIZE = 512
TRACK_B_JPEG_QUALITY = 85
TRACK_B_TARGET_IMG_KB = 160        # σ 自动校准到该体积
TRACK_B_TAR_BYTES = 1 << 28        # 256MB(tar 目标大小,用于校验)

# C 轨:多 Job 倾斜(热分片比例;数据复用 B 轨,只生成清单)
TRACK_C_HOT_RATIO = 0.10

# ---- D 轨:全程收益专用(单 Job 连续 10 epoch,缓存欠配 50%,宿主机本地源)----
# 16×800 张 512px JPEG q85(~160KB/张)≈2GiB,落宿主机本地,经独立 nginx 容器
# traincache-source-d(端口 9100)供训练进程读取,不上传 MinIO;每 epoch 换 seed。
TRACK_D_N_TARS = 16
TRACK_D_IMAGES_PER_TAR = 800
TRACK_D_IMG_SIZE = 512
TRACK_D_JPEG_QUALITY = 85
TRACK_D_TARGET_IMG_KB = 160
TRACK_D_SEED = 20261015
TRACK_D_EPOCHS = 10
TRACK_D_WORKERS = 4
TRACK_D_BATCH = 32
TRACK_D_STEPS_PER_EPOCH = 400      # 16*800/32
TRACK_D_CACHE_MB = 1024            # 欠配:2GiB 数据集的 50%
TRACK_D_WARMUP = 10                # 仅 epoch0 前 10 步,口径与探针 C 一致
TRACK_D_SOURCE_CONTAINER = "traincache-source-d"
TRACK_D_SOURCE_PORT = 9100
TRACK_D_DIR = DATA_DIR / "track_d"                 # 宿主机数据目录(shard-*.tar)
TRACK_D_SOURCE_MOUNT = TRACK_D_DIR                 # docker -v 挂载源(需绝对路径)
TRACK_D_SOURCE_HTML = "track-d"                    # 容器内挂到 /usr/share/nginx/html/track-d
TRACK_D_URL = "http://127.0.0.1:9100/track-d/shard-%06d.tar"   # %06d 取 0..15
TRACK_D_RESULTS_DIR = RESULTS_DIR / "track_d"
TRACK_D_TC_RATE = "100mbit"
TRACK_D_TC_DELAY = "20ms"

# ---- M1 打点口径(预注册 §1.3/§4.1)----
TRACK_D_SHARD_BYTES = 128 << 20      # 单个 tar 目标字节(2GiB / 16)
TRACK_D_CHUNK_BYTES = 64 << 20       # DataSet.spec.chunkSize
TRACK_D_WINDOW_K = 8                 # 启动/切换窗口块数
TRACK_D_BASELINE_STEPS = 20          # 回落基线:切换前 20 步
TRACK_D_CURRICULUM_FRACTIONS = (1.0, 2.0 / 3.0, 1.0 / 3.0)

# ---- tc 限速档位(扫点实验的自变量)----
TC_RATE = "100mbit"
TC_BURST = "1600k"          # 旧固定值;跨带宽档复用会让突发窗口从 128ms 缩到 13ms(D31)
TC_LATENCY = "50ms"
TC_DELAY = "20ms"

# ---- 带宽档口径(D31:带宽是实验自变量)----
# rate 的取值集合;COND 的 network.rate_mbit 只能取这些值。
TC_RATE_MBITS = (100, 300, 1000)
# 每步读量 r 的推导口径:数据集字节 ÷ (每 epoch 步数 × 每步计算耗时)。
# S 档 2GiB/80s ≈ 26.8 MB/s、M 档 8GiB/80s ≈ 107 MB/s、L 档 32GiB/80s ≈ 429 MB/s。
TIER_STEPS_PER_EPOCH = 400
TIER_COMPUTE_MS = 200
TIER_DATA_BYTES = {"S": 2 << 30, "M": 8 << 30, "L": 32 << 30}


def tier_read_rate_bps(tier: str) -> float:
    """每步读量 r(字节/秒):数据集字节 ÷ (steps_per_epoch × compute 秒)。"""
    return TIER_DATA_BYTES[tier] / (TIER_STEPS_PER_EPOCH * TIER_COMPUTE_MS / 1000)


def bw_over_r(tier: str, rate_mbit: int) -> float:
    """链路带宽 ÷ 每步读量。<1 链路受限,>2 计算主导,[1,2] 临界。"""
    return rate_mbit * 125000.0 / tier_read_rate_bps(tier)


def burst_for_rate(rate_mbit: int) -> str:
    """burst ≈ 0.1 秒的传输量:rate_mbit × 125 KB/s × 0.1 s。

    100mbit → 1250k、300mbit → 3750k、1000mbit → 12500k。
    """
    return f"{round(rate_mbit * 125 * 0.1)}k"


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
