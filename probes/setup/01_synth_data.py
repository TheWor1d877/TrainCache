#!/usr/bin/env python3
"""合成 webdataset 风格 JPEG tar 分片(确定性、可复现,探针A/B 共用同一批)。

内容 = 渐变底 + 随机几何图形 + 高斯噪声:
- 纯色块会被 JPEG 过度压缩导致体积失真,噪声保证高频分量与真实解码成本;
- 噪声强度 σ 按目标体积(~160KB/张)自动校准;
- 全部随机源由固定种子派生,重跑得到逐字节一致的数据。
"""
import io
import random
import sys
import tarfile
import time
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


def _rs(shard: int, idx: int) -> np.random.RandomState:
    # RandomState 要求种子 < 2**32,取模后碰撞概率极低,且不影响确定性可复现
    seed = ((config.SEED * 1000003 + shard) * 1000003 + idx) % (2**32)
    return np.random.RandomState(seed)


def synth(shard: int, idx: int, sigma: float) -> Image.Image:
    """合成一张 512x512 RGB 图:渐变底 + 随机圆形/矩形 + 高斯噪声。"""
    n = config.IMG_SIZE
    rs = _rs(shard, idx)
    rng = random.Random(f"{config.SEED}:{shard}:{idx}")

    yy, xx = np.mgrid[0:n, 0:n] / (n - 1)
    img = (1 - yy[..., None]) * rs.rand(3) + yy[..., None] * rs.rand(3)

    for _ in range(rng.randint(2, 6)):
        color = rs.rand(3)
        if rng.random() < 0.5:  # 圆形
            cy, cx, rad = rng.randrange(n), rng.randrange(n), rng.randrange(n // 8, n // 3)
            mask = (yy - cy / n) ** 2 + (xx - cx / n) ** 2 < (rad / n) ** 2
        else:  # 矩形
            y0, y1 = sorted(rng.randrange(n) for _ in range(2))
            x0, x1 = sorted(rng.randrange(n) for _ in range(2))
            mask = np.zeros((n, n), bool)
            mask[y0:y1, x0:x1] = True
        img[mask] = color

    img += rs.normal(0, sigma, (n, n, 3))
    return Image.fromarray((np.clip(img, 0, 1) * 255).astype(np.uint8))


def jpeg_bytes(shard: int, idx: int, sigma: float) -> bytes:
    buf = io.BytesIO()
    synth(shard, idx, sigma).save(buf, "JPEG", quality=config.JPEG_QUALITY)
    return buf.getvalue()


def calibrate_sigma() -> float:
    """试合成几张,选体积最接近 TARGET_IMG_KB 的 σ(体积随 σ 近似单调)。"""
    target = config.TARGET_IMG_KB * 1024
    sigmas = [0.06, 0.12, 0.18, 0.24, 0.30]
    sizes = np.array([len(jpeg_bytes(0, 0, s)) for s in sigmas])
    i = int(np.argmin(np.abs(sizes - target)))

    if i in (0, len(sigmas) - 1):  # 目标落在采样范围外:警告并取边界
        print(f"警告:目标 {config.TARGET_IMG_KB}KB 超出 σ 采样范围,取边界值")
        return sigmas[i]

    best_s = sigmas[i]
    neighbor = sigmas[i + 1] if sizes[i] < target else sigmas[i - 1]
    mid = (best_s + neighbor) / 2
    size_mid = len(jpeg_bytes(0, 0, mid))
    final = mid if abs(size_mid - target) < abs(sizes[i] - target) else best_s
    print(f"σ 校准:目标 {config.TARGET_IMG_KB}KB → σ={final:.3f} "
          f"(实测 {len(jpeg_bytes(0, 0, final)) / 1024:.0f}KB/张)")
    return final


def main():
    config.TARS_DIR.mkdir(parents=True, exist_ok=True)
    sigma = calibrate_sigma()
    t0 = time.time()
    for shard in range(config.N_TARS):
        name = f"shard-{shard:05d}.tar"
        with tarfile.open(config.TARS_DIR / name, "w") as tf:
            for idx in range(config.N_IMGS_PER_TAR):
                data = jpeg_bytes(shard, idx, sigma)
                info = tarfile.TarInfo(f"shard-{shard:05d}_{idx:06d}.jpg")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        print(f"[{shard + 1}/{config.N_TARS}] {name} 完成,"
              f"累计 {(time.time() - t0) / 60:.1f} 分钟")
    total = sum(p.stat().st_size for p in config.TARS_DIR.glob("*.tar"))
    print(f"完成:{config.N_TARS} 个 tar,共 {total / 2**30:.2f} GiB "
          f"(约 {total / config.N_TARS / 2**20:.0f} MB/tar)")


if __name__ == "__main__":
    main()
