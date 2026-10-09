#!/usr/bin/env python3
"""探针B 核心装置:最小 CPU 训练循环 + 打点 wait_time / step_time。

stall 口径(主进程视角):
  每步:t0 = 上一步计算结束 → 等 batch 到手(t1)→ 极小 matmul(t2)
  wait  = t1 - t0   (DataLoader 等待数据的时间)
  step  = t2 - t0   (整步时间)
  stall = wait / step

--source http :冷态,从 MinIO 经 tc 限速流式读(webdataset http URL,顺序分片)
--source local:热态,从本地 tar 目录直读(模拟 hostPath 读路径)
"""
import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import torch
import webdataset as wds

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

SHARD_PATTERN = "shard-{{{:05d}..{:05d}}}.tar".format(0, config.N_TARS - 1)


def make_loader(source: str, workers: int):
    """webdataset 流式读 tar:decode+转 tensor 在 worker 进程并行,主进程只管消费。"""
    if source == "http":
        urls = (f"http://127.0.0.1:{config.MINIO_PORT}/"
                f"{config.BUCKET}/{SHARD_PATTERN}")
    else:
        urls = str(config.TARS_DIR / SHARD_PATTERN)

    def collate(samples):
        # 标准 collate:samples = 32 个 {"jpg": PIL, ...} dict,
        # 在 worker 内解码并堆成 (32, 3, H, W)
        xs = torch.stack([
            torch.from_numpy(np.array(s["jpg"])).float() / 255.0 for s in samples
        ])
        return {"jpg": xs.permute(0, 3, 1, 2)}

    # 不用 webdataset 的 .map/.batched:其 default_collation_fn 会把单张
    # (3,H,W) tensor 折叠成 dict-of-rows 的意外结构,改用标准 DataLoader batching
    ds = (wds.WebDataset(urls, shardshuffle=False)
          .shuffle(100)
          .decode("pil"))
    return wds.WebLoader(
        ds, batch_size=config.PROBE_B_BATCH, collate_fn=collate,
        num_workers=workers,
        prefetch_factor=config.PROBE_B_PREFETCH if workers > 0 else None,
        persistent_workers=workers > 0,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["http", "local"], required=True,
                    help="http=冷态(走 tc),local=热态(本地直读)")
    ap.add_argument("--workers", type=int, required=True)
    ap.add_argument("--steps", type=int, default=config.PROBE_B_STEPS)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    torch.manual_seed(config.SEED)
    # compute 量级校准:多核 BLAS 会把 1024² matmul 跑成 ~3ms,stall 对 wait 的
    # 微小变化过于敏感(0.6ms wait → 17% stall,13ms wait → 81% stall),装置判别力差。
    # 限单线程后 ~100ms,贴近真实 CPU 训练 step 的 compute 量级(相对真实模型仍是"极小")。
    torch.set_num_threads(1)
    n = config.PROBE_B_COMPUTE_SIZE
    A = torch.randn(n, n)
    B = torch.randn(n, n)

    it = iter(make_loader(args.source, args.workers))
    for _ in range(config.PROBE_B_WARMUP):  # 管线预热,不计
        next(it)

    records = []
    t2 = time.perf_counter()
    for step in range(args.steps):
        t0 = t2
        batch = next(it)          # t1:batch 到手(含排队与管线等待)
        t1 = time.perf_counter()
        for _ in range(config.PROBE_B_COMPUTE_ITERS):
            _ = A @ B             # 极小 compute 替身(量级校准见 config.py 注释)
        t2 = time.perf_counter()
        records.append({"step": step,
                        "wait_s": round(t1 - t0, 6),
                        "step_s": round(t2 - t0, 6)})
        if len(batch["jpg"]) != config.PROBE_B_BATCH:
            print(f"警告:第 {step} 步 batch 不满({len(batch['jpg'])}),", file=sys.stderr)

    waits = np.array([r["wait_s"] for r in records])
    steps = np.array([r["step_s"] for r in records])
    stalls = waits / steps
    out = {
        "source": args.source,
        "workers": args.workers,
        "batch": config.PROBE_B_BATCH,
        "steps": len(records),
        "median_stall": float(np.median(stalls)),
        "mean_stall": float(stalls.mean()),
        "p50_wait_s": float(np.median(waits)),
        "p90_wait_s": float(np.percentile(waits, 90)),
        "p50_step_s": float(np.median(steps)),
        "per_step": records,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    print(f"{args.source} w{args.workers}:中位 stall {out['median_stall']:.1%},"
          f"p50 wait {out['p50_wait_s'] * 1000:.0f}ms,"
          f"p50 step {out['p50_step_s'] * 1000:.0f}ms → {args.out}")


if __name__ == "__main__":
    main()
