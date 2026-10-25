#!/usr/bin/env python3
"""B 轨生成器:合成 JPEG → webdataset 百 MB 级 tar 分片(旗舰数字的主力数据)。

合成纪律(零下载、真解码):
- 随机像素图 + 真实 JPEG 编码(质量/分辨率参数化),解码成本真实;
- 内容 = 渐变底 + 随机几何图形 + 高斯噪声(σ 自动校准到目标体积,防过度压缩);
- 固定种子,一键可重新生成逐字节一致的数据;
- 报告口径:合成数据,I/O 与解码路径真实,语义内容随机。

分片范围:
- 默认生成 config.TRACK_B_N_TARS(32)份,即 M 档 8GiB;
- L 档(32GiB)需要 128 份,多出的 96 份用 --start-shard 32 --count 96 追加生成。
  每个分片的随机种子只与自己的序号有关(config.SEED × 1000003 + shard),
  所以追加生成与一次生成 128 份逐字节一致;已存在的分片不会被覆盖。
"""
import argparse
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
    seed = ((config.SEED * 1000003 + shard) * 1000003 + idx) % (2**32)
    return np.random.RandomState(seed)


def synth(shard: int, idx: int, sigma: float) -> Image.Image:
    n = config.TRACK_B_IMG_SIZE
    rs = _rs(shard, idx)
    rng = random.Random(f"{config.SEED}:{shard}:{idx}")
    yy, xx = np.mgrid[0:n, 0:n] / (n - 1)
    img = (1 - yy[..., None]) * rs.rand(3) + yy[..., None] * rs.rand(3)
    for _ in range(rng.randint(2, 6)):
        color = rs.rand(3)
        if rng.random() < 0.5:
            cy, cx, rad = rng.randrange(n), rng.randrange(n), rng.randrange(n // 8, n // 3)
            mask = (yy - cy / n) ** 2 + (xx - cx / n) ** 2 < (rad / n) ** 2
        else:
            y0, y1 = sorted(rng.randrange(n) for _ in range(2))
            x0, x1 = sorted(rng.randrange(n) for _ in range(2))
            mask = np.zeros((n, n), bool)
            mask[y0:y1, x0:x1] = True
        img[mask] = color
    img += rs.normal(0, sigma, (n, n, 3))
    return Image.fromarray((np.clip(img, 0, 1) * 255).astype(np.uint8))


def jpeg_bytes(shard: int, idx: int, sigma: float) -> bytes:
    buf = io.BytesIO()
    synth(shard, idx, sigma).save(buf, "JPEG", quality=config.TRACK_B_JPEG_QUALITY)
    return buf.getvalue()


def calibrate_sigma() -> float:
    target = config.TRACK_B_TARGET_IMG_KB * 1024
    sigmas = [0.06, 0.12, 0.18, 0.24, 0.30]
    sizes = np.array([len(jpeg_bytes(0, 0, s)) for s in sigmas])
    i = int(np.argmin(np.abs(sizes - target)))
    if i in (0, len(sigmas) - 1):
        print(f"警告:目标 {config.TRACK_B_TARGET_IMG_KB}KB 超出采样范围,取边界")
        return sigmas[i]
    best_s = sigmas[i]
    neighbor = sigmas[i + 1] if sizes[i] < target else sigmas[i - 1]
    mid = (best_s + neighbor) / 2
    if abs(len(jpeg_bytes(0, 0, mid)) - target) < abs(sizes[i] - target):
        return mid
    return best_s


def main():
    ap = argparse.ArgumentParser(description="B 轨 tar 分片生成器")
    ap.add_argument("--start-shard", type=int, default=0,
                    help="起始分片序号(含);默认 0")
    ap.add_argument("--count", type=int, default=None,
                    help="生成分片数;默认 config.TRACK_B_N_TARS")
    args = ap.parse_args()

    config.TRACK_B_DIR.mkdir(parents=True, exist_ok=True)
    sigma = calibrate_sigma()
    print(f"σ={sigma:.3f}(单张 ~{len(jpeg_bytes(0,0,sigma))/1024:.0f}KB)")
    t0 = time.time()
    total = config.TRACK_B_N_TARS if args.count is None else args.count
    first = args.start_shard
    for shard in range(first, first + total):
        name = f"shard-{shard:05d}.tar"
        out = config.TRACK_B_DIR / name
        if out.exists():
            # 产物只追加不覆盖:已存在的分片跳过(重跑本脚本不会重写既有数据)
            print(f"[skip] {name} 已存在,跳过", flush=True)
            continue
        with tarfile.open(out, "w") as tf:
            for idx in range(config.TRACK_B_IMGS_PER_TAR):
                data = jpeg_bytes(shard, idx, sigma)
                info = tarfile.TarInfo(f"shard-{shard:05d}_{idx:06d}.jpg")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        print(f"[{shard+1}/{first+total}] {name} 完成,累计 {(time.time()-t0)/60:.1f} 分钟",
              flush=True)
    size = sum(p.stat().st_size for p in config.TRACK_B_DIR.glob("*.tar"))
    print(f"B 轨完成:{len(list(config.TRACK_B_DIR.glob('*.tar')))} 个 tar,共 {size/2**30:.2f} GiB")


if __name__ == "__main__":
    main()
