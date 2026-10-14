#!/usr/bin/env python3
"""D 轨生成器:合成 JPEG → webdataset tar 分片 ×16(≈2GiB,全程收益专用)。

D 轨(v4 新增,单 Job 连续 10 epoch、每 epoch 换 seed、缓存欠配 50%)专用
数据集:16 个 tar × 800 张 512px JPEG(q85,σ 校准到 ~160KB/张)≈2GiB,
落宿主机本地目录(config.TRACK_D_DIR,不上传 MinIO),由独立 nginx 容器
traincache-source-d(端口 9100,挂 /usr/share/nginx/html/track-d)供训练
进程读取。

与 B 轨(synth_track_b.py)同构(权威参考):
- 同一合成管线:种子派生 RandomState + random.Random(渐变色底 + 随机几何
  图形 + 高斯噪声,σ 自动校准到目标体积)+ 真实 JPEG 编码;
- 同一 tar 内结构:每样本恰好一个成员 `<key>.jpg`(webdataset 样本 = jpg),
  key = `shard-<分片号>_<样本号>.jpg`(分片号位数随 tar 名,本轨 %06d),
  tar 名 = shard-%06d.tar(000000..000015),key 前缀与所在 tar 同名;
  TarInfo 用默认值(mtime=0/uid=gid=0/mode=0644),无辅助成员;
- 同一取参方式:规模/尺寸/质量/目标体积/种子全部来自 bench/config.py 的
  TRACK_D_* 组(Makefile track-d-data 无参调用即默认全量生成)。

合成纪律(零下载、真解码):随机像素图 + 真实 JPEG 编码(质量/分辨率
参数化),解码成本真实;固定种子,一键可重新生成逐字节一致的数据;
报告口径:合成数据,I/O 与解码路径真实,语义内容随机。

幂等:目标分片已存在则默认跳过(可增量补齐缺失分片);--force 全量重生成。
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


def _rs(shard: int, idx: int, seed: int) -> np.random.RandomState:
    """样本级 RandomState:种子由 (seed, shard, idx) 确定性派生(同 B 轨)。"""
    s = ((seed * 1000003 + shard) * 1000003 + idx) % (2**32)
    return np.random.RandomState(s)


def synth(shard: int, idx: int, seed: int, size: int, sigma: float) -> Image.Image:
    """合成一张 size×size RGB 图:渐变底 + 随机圆形/矩形 + 高斯噪声。"""
    n = size
    rs = _rs(shard, idx, seed)
    rng = random.Random(f"{seed}:{shard}:{idx}")
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


def jpeg_bytes(shard: int, idx: int, seed: int, size: int, quality: int,
               sigma: float) -> bytes:
    buf = io.BytesIO()
    synth(shard, idx, seed, size, sigma).save(buf, "JPEG", quality=quality)
    return buf.getvalue()


def calibrate_sigma(seed: int, size: int, quality: int, target_kb: int) -> float:
    """试合成几张,选体积最接近 target_kb 的 σ(体积随 σ 近似单调)。"""
    target = target_kb * 1024
    sigmas = [0.06, 0.12, 0.18, 0.24, 0.30]
    sizes = np.array([
        len(jpeg_bytes(0, 0, seed, size, quality, s)) for s in sigmas
    ])
    i = int(np.argmin(np.abs(sizes - target)))
    if i in (0, len(sigmas) - 1):
        print(f"警告:目标 {target_kb}KB 超出 σ 采样范围,取边界值")
        return sigmas[i]
    best_s = sigmas[i]
    neighbor = sigmas[i + 1] if sizes[i] < target else sigmas[i - 1]
    mid = (best_s + neighbor) / 2
    if abs(len(jpeg_bytes(0, 0, seed, size, quality, mid)) - target) < abs(sizes[i] - target):
        return mid
    return best_s


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="D 轨数据生成器:合成 JPEG → webdataset tar ×N(默认 16×800"
                    " 张 512px q85≈2GiB,落宿主机本地,不上传 MinIO)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--out-dir", type=Path, default=config.TRACK_D_DIR,
                    help="输出目录(默认取 config.TRACK_D_DIR)")
    ap.add_argument("--n-tars", type=int, default=config.TRACK_D_N_TARS,
                    help="tar 分片数(默认取 config.TRACK_D_N_TARS)")
    ap.add_argument("--images-per-tar", type=int,
                    default=config.TRACK_D_IMAGES_PER_TAR,
                    help="每 tar 样本数(默认取 config.TRACK_D_IMAGES_PER_TAR)")
    ap.add_argument("--img-size", type=int, default=config.TRACK_D_IMG_SIZE,
                    help="图像边长 px(默认取 config.TRACK_D_IMG_SIZE)")
    ap.add_argument("--quality", type=int, default=config.TRACK_D_JPEG_QUALITY,
                    help="JPEG 质量(默认取 config.TRACK_D_JPEG_QUALITY)")
    ap.add_argument("--target-kb", type=int, default=config.TRACK_D_TARGET_IMG_KB,
                    help="单张目标体积 KB,σ 据此校准(默认取 config.TRACK_D_TARGET_IMG_KB)")
    ap.add_argument("--seed", type=int, default=config.TRACK_D_SEED,
                    help="固定种子(默认取 config.TRACK_D_SEED);同种子重跑逐字节一致")
    ap.add_argument("--force", action="store_true",
                    help="已存在的分片也重新生成(默认跳过已存在的分片)")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    names = [f"shard-{shard:06d}.tar" for shard in range(args.n_tars)]
    todo = [shard for shard, name in enumerate(names)
            if args.force or not (out_dir / name).exists()]
    skipped = args.n_tars - len(todo)
    if skipped:
        print(f"已存在 {skipped}/{args.n_tars} 个分片,默认跳过(重生成加 --force)")
    if not todo:
        size = sum((out_dir / name).stat().st_size for name in names)
        print(f"D 轨已完成:{args.n_tars} 个 tar,共 {size/2**30:.2f} GiB,无需生成")
        return
    sigma = calibrate_sigma(args.seed, args.img_size, args.quality, args.target_kb)
    one_kb = len(jpeg_bytes(0, 0, args.seed, args.img_size, args.quality, sigma)) / 1024
    print(f"σ={sigma:.3f}(单张 ~{one_kb:.0f}KB),输出 {out_dir}"
          f"(本次生成 {len(todo)}/{args.n_tars} 个分片)")
    t0 = time.time()
    for k, shard in enumerate(todo, 1):
        name = names[shard]
        with tarfile.open(out_dir / name, "w") as tf:
            for idx in range(args.images_per_tar):
                data = jpeg_bytes(shard, idx, args.seed, args.img_size,
                                  args.quality, sigma)
                info = tarfile.TarInfo(f"shard-{shard:06d}_{idx:06d}.jpg")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        print(f"[{k}/{len(todo)}] {name} 完成,累计 {(time.time()-t0)/60:.1f} 分钟",
              flush=True)
    size = sum((out_dir / name).stat().st_size for name in names)
    print(f"D 轨完成:{args.n_tars} 个 tar,共 {size/2**30:.2f} GiB")


if __name__ == "__main__":
    main()
