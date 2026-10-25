#!/usr/bin/env python3
"""TrainCache 可行性探针——统一参数源。

所有装置脚本(bash/python)从这里取参数,避免多份拷贝漂移。
bash 侧取值:python3 config.py --sh <KEY>
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# 数据与产物落运行时数据层(与 bench/config.py 同一口径、同一环境变量)。
DATA_ROOT = Path(os.environ.get("TRAINCACHE_DATA_ROOT") or (ROOT.parent.parent / "traincachedata")).resolve()
DATA_DIR = DATA_ROOT / "probes" / "data"   # 合成数据(2GB,种子固定可复现)
TARS_DIR = DATA_DIR / "tars"
CACHE_DIR = ROOT / ".cache"       # 运行时缓存(Agent 缓存目录等)
AGENT_CACHE = CACHE_DIR / "agent"  # 模拟 Agent 落盘目录(探针A 热读源)
RESULTS_DIR = DATA_ROOT / "probes" / "results"

# ---- 数据合成(探针A/B 共用同一批 tar)----
IMG_SIZE = 512
JPEG_QUALITY = 85
TARGET_IMG_KB = 160                # 合成噪声强度据此校准,保证真实解码成本与体积
N_TARS = 16
N_IMGS_PER_TAR = 800               # 800 × ~160KB ≈ 128MB / tar
SEED = 20261006

# ---- 源站(层3 数据源的裸机替身)----
# 原设计为容器 MinIO(公开读 bucket,http 直取);MinIO 镜像在所有可达镜像源
# (docker hub 直连/玄源/DaoCloud/1ms/quay/阿里云)均拉取失败,降级为 nginx 静态
# 服务:读路径语义等价(HTTP 字节流直取),tc 挂载点不变,URL 结构不变。
# kind 现勘阶段仍按文档部署真 MinIO(届时另解镜像)。
SOURCE_CONTAINER = "traincache-source"
SOURCE_IMAGE = "nginx:latest"      # 本地已有镜像,无需拉取
MINIO_PORT = 9000                  # 源站端口(参数名保留,URL 构建处引用)
BUCKET = "train-data"              # URL 前缀目录(nginx 下挂到 html/train-data)

# ---- tc 限速(装置核心:挂在源站容器 veth 的 host 侧)----
# 不能挂 docker0:bridge 二层转发不经过桥主设备 qdisc(见 tc_setup.sh 注释)
TC_DEV = "auto"                    # auto = 探测源站容器的 veth host 端
TC_RATE = "100mbit"
TC_BURST = "1600k"                 # ≈ 1s 令牌量
TC_LATENCY = "50ms"
TC_NETEM_DELAY = "20ms"

# ---- 探针A:冷热差 ----
PROBE_A_RUNS = 7                   # 冷/热各跑次数,取中位数
PROBE_A_CONCURRENCY = 4            # 模拟 Agent 的有界并发

# ---- 探针B:stall 指标 ----
PROBE_B_STEPS = 60                 # 每档测量步数
PROBE_B_WARMUP = 10                # 前 N 步剔除(管线预热)
PROBE_B_BATCH = 32                 # 32 × ~160KB ≈ 5MB / batch
PROBE_B_WORKERS = [0, 4, 8]        # 三档
PROBE_B_COMPUTE_SIZE = 1024        # 极小 matmul 边长(模拟真实 step 的 compute 量级)
PROBE_B_COMPUTE_ITERS = 8          # 重复次数:单线程单次 1024² ≈25ms,8 次 ≈200ms
                                   # (量级校准:3ms 时 stall 对 wait 过敏感,25ms 时
                                   #  热态 ~35% 仍压不到 15%,200ms 是"极小 compute
                                   #  替身"与判据灵敏度之间的落点)
PROBE_B_PREFETCH = 2               # DataLoader prefetch_factor
PROBE_B_SHARD_URL = "http://127.0.0.1:{port}/{bucket}/shard-{{00000..{last:05d}}}.tar"

# ---- 探针C:多 epoch 全程 stall(epoch0 中段/epoch1-2 切换后回升 ≥ 冷态基准 30%)----
PROBE_C_EPOCHS = 3                  # 训练循环连续 epoch 数
PROBE_C_SEED = 20261013             # 基种子;epoch e 有效种子 = seed + e
PROBE_C_WARMUP = 10                 # 仅 epoch 0 的前 N 步(计入步号、标记排除)
PROBE_C_WORKERS = [0, 4, 8]         # 三档(冷态)+ hot 组用 w4
PROBE_C_SWITCH_WINDOW = 20          # epoch 切换后的观察窗口步数 [0,20)
PROBE_C_MID_BEGIN = 1 / 3           # epoch 中段窗口起点(占 epoch 长的比例)
PROBE_C_MID_END = 2 / 3             # 中段窗口终点(400 步 → [133,266))
PROBE_C_GREEN_RATIO = 0.30          # 绿灯:窗口指标 ≥ 0.30 × 冷态基准
PROBE_C_TAIL_WAIT_S = 0.5           # w>0 档口径:wait_s ≥ 0.5 的步数占比

# ---- D 轨:全程缓存服务实验(复用 probe_c/train_multi_epoch.py 装置)----
TRACK_D_EPOCHS = 10                 # D 轨训练循环 epoch 数
TRACK_D_SEED = 20261014             # D 轨基种子
TRACK_D_WORKERS = [4]               # D 轨档位
TRACK_D_CACHE_MB = 1024             # 缓存服务容量(mb)
TRACK_D_WINDOW = 20                 # D 轨窗口步数
TRACK_D_MIN_R = 0.50                # D 轨判据:窗口相关/命中下限
TRACK_D_MIN_PAIRS = 6               # D 轨判据:最少配对数
TRACK_D_CV_MAX = 0.30               # D 轨判据:组内变异系数上限
TRACK_D_MEM_SAMPLE_S = 2            # D 轨:内存占用采样间隔(s)


def _sh():
    """bash 侧取值入口:python3 config.py --sh <KEY>"""
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
