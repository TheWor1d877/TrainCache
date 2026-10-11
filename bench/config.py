#!/usr/bin/env python3
"""TrainCache bench 装置统一参数源(唯一参数点,bash/python 共用)。

bash 侧取值:python3 config.py --sh <KEY>
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
TRACK_A_DIR = DATA_DIR / "track_a" / "files"
TRACK_B_DIR = DATA_DIR / "track_b" / "tars"
TRACK_C_DIR = DATA_DIR / "track_c"
RESULTS_DIR = ROOT / "results"
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

# ---- tc 限速档位(扫点实验的自变量)----
TC_RATE = "100mbit"
TC_BURST = "1600k"
TC_LATENCY = "50ms"
TC_DELAY = "20ms"

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
