#!/usr/bin/env python3
"""A 轨生成器:100 万个 4KB 小文件(元数据压力臂,io_uring 战场)。

- 本地目录,分 100 个子目录 × 1 万文件,避免单目录极端;
- 内容:种子派生的确定性随机字节(逐字节可复现,不存原图);
- 并行生成(32 进程),约 3-8 分钟;重跑幂等(先清后写)。
"""
import multiprocessing as mp
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


def gen_one(args):
    """生成一个子目录的全部文件。"""
    subdir_idx, n_per_dir, seed = args
    d = Path(config.TRACK_A_DIR) / f"{subdir_idx:03d}"
    d.mkdir(parents=True, exist_ok=True)
    nbytes = config.TRACK_A_FILE_KB * 1024
    for i in range(n_per_dir):
        # RandomState 要求种子 < 2**32,取模后碰撞概率极低且不影响确定性
        s = ((seed * 1000003 + subdir_idx) * 1000003 + i) % (2**32)
        rs = np.random.RandomState(s)
        data = rs.bytes(nbytes)
        (d / f"{i:05d}.dat").write_bytes(data)
    return subdir_idx


def main():
    n_sub = config.TRACK_A_SUBDIRS
    n_per = config.TRACK_A_N_FILES // n_sub
    print(f"A 轨:共 {n_sub}×{n_per}={n_sub*n_per} 个文件,"
          f"每个 {config.TRACK_A_FILE_KB}KB,"
          f"总 {n_sub*n_per*config.TRACK_A_FILE_KB/2**20:.1f} MiB")
    with mp.Pool(processes=32) as pool:
        for done in pool.imap_unordered(gen_one,
                                        [(s, n_per, config.SEED) for s in range(n_sub)]):
            print(f"[{done+1}/{n_sub}] 子目录完成", flush=True)
    print("A 轨生成完成")


if __name__ == "__main__":
    main()
