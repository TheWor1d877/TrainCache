#!/usr/bin/env python3
"""探针C / D 轨共用的多 epoch 训练循环装置。

在探针B(probe_b/train.py)结构上移植,唯一重写点是数据装载:每 epoch 重建
loader(wds pipeline 在 worker 进程内的副本无法换 seed),分片级与样本级洗牌
逐 epoch 换种子,打点每个 epoch 内的 stall 时间序列。

口径(与探针B一致,主进程视角):
  每步:t0 = 上一步 compute 结束 → next(batch) t1 → 极小 matmul t2
  wait  = t1 - t0    step = t2 - t0    stall = wait / step
  epoch 边界不重置时钟:t0 总是"上一步 compute 结束",因此 epoch 1/2 首步的
  wait 天然包含 loader 重建 + worker 重启 + 换序后首批数据的等待 —— 切换
  代价正是本探针的测量对象(loader_build_s 单独留档便于对照)。

洗牌(seed 语义以 probe_d T1 实测为准,勿用"seed+1"旧说法):
  * 分片级:父进程 random.Random(seed + epoch) 生成 0..N_TARS-1 置换,按置换
    显式构造 URL 列表;WebDataset(shardshuffle=False) 的读取顺序 = 列表顺序;
  * 样本级:.shuffle(100, seed=seed + epoch);该 API 的有效种子就是传入值,
    同 seed 复跑逐位相同、异 seed 不同(wds 1.0.2 实测,见 probe_d)。

epoch 结构:epoch 0 的前 PROBE_C_WARMUP 步为 warmup(计入步号、标记排除);
epoch 1/2 步 0 起全部计入(切换首步 stall 正是测量对象)。epoch 长默认 =
N_TARS*N_IMGS_PER_TAR//PROBE_B_BATCH = 400 步(一遍完整数据)。

用法(探针C):
  python3.12 train_multi_epoch.py --source http --workers 4 \
      --out ../results/probe_c/c_cold_w4.json --env-file ../results/probe_c/env_cold_w4.json
D 轨显式传 --epochs/--steps-per-epoch/--seed(缺省取 config 的 PROBE_C_*)。
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import torch
import webdataset as wds

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


def shard_of_key(key: str) -> int:
    """从样本 __key__ 提分片号:"shard-00003_000000" / "shard-00003/xxx" → 3。"""
    head = key.split("/")[0]
    head = head.split("_")[0]
    return int(head.split("-")[-1])


def epoch_shard_perm(seed: int, epoch: int):
    """分片级置换:父进程 RNG,种子 = seed + epoch(确定性)。"""
    perm = list(range(config.N_TARS))
    random.Random(seed + epoch).shuffle(perm)
    return perm


def build_urls(source: str, perm):
    if source == "http":
        base = f"http://127.0.0.1:{config.MINIO_PORT}/{config.BUCKET}"
    else:
        base = str(config.TARS_DIR)
    return [f"{base}/shard-{i:05d}.tar" for i in perm]


def make_loader(source: str, workers: int, seed: int, epoch: int):
    """重建该 epoch 的 loader;返回 (loader, urls, perm)。"""
    perm = epoch_shard_perm(seed, epoch)
    urls = build_urls(source, perm)

    def collate(samples):
        # 与探针B 相同的 worker 内解码/转 tensor;额外带出分片号供打点
        xs = torch.stack([
            torch.from_numpy(np.array(s["jpg"])).float() / 255.0 for s in samples
        ])
        return {"jpg": xs.permute(0, 3, 1, 2),
                "shard": shard_of_key(samples[0]["__key__"])}

    ds = (wds.WebDataset(urls, shardshuffle=False)
          .shuffle(100, seed=seed + epoch)
          .decode("pil"))
    loader = wds.WebLoader(
        ds, batch_size=config.PROBE_B_BATCH, collate_fn=collate,
        num_workers=workers,
        prefetch_factor=config.PROBE_B_PREFETCH if workers > 0 else None,
        persistent_workers=workers > 0,
    )
    return loader, urls, perm


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["http", "local"], required=True,
                    help="http=冷态(走 tc),local=热态(本地直读)")
    ap.add_argument("--workers", type=int, required=True)
    ap.add_argument("--epochs", type=int, default=config.PROBE_C_EPOCHS)
    ap.add_argument("--steps-per-epoch", type=int, default=None,
                    help="缺省 = N_TARS*N_IMGS_PER_TAR//PROBE_B_BATCH(完整一遍数据)")
    ap.add_argument("--seed", type=int, default=config.PROBE_C_SEED)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--env-file", type=Path, default=None,
                    help="run_c.sh 写入的环境 JSON(tc 断言 / kind 负载检查结论)")
    args = ap.parse_args()

    steps_per_epoch = args.steps_per_epoch or (
        config.N_TARS * config.N_IMGS_PER_TAR // config.PROBE_B_BATCH)
    warmup_total = min(config.PROBE_C_WARMUP, steps_per_epoch // 2)
    if warmup_total != config.PROBE_C_WARMUP:
        print(f"警告:warmup 截断为 {warmup_total}(epoch 长 {steps_per_epoch})",
              file=sys.stderr)

    torch.manual_seed(args.seed)
    # compute 量级校准与探针B一致:单线程 1024² matmul ×8 ≈200ms,跨 epoch 复用
    torch.set_num_threads(1)
    n = config.PROBE_B_COMPUTE_SIZE
    A = torch.randn(n, n)
    B = torch.randn(n, n)

    records = []
    per_epoch = []
    t2 = None
    for epoch in range(args.epochs):
        t_build = time.perf_counter()
        loader, urls, perm = make_loader(args.source, args.workers, args.seed, epoch)
        it = iter(loader)
        loader_build_s = time.perf_counter() - t_build
        if t2 is None:
            t2 = time.perf_counter()  # epoch 0:时钟从 loader 建好后起(等首步)
        print(f"[epoch {epoch}] seed={args.seed + epoch} perm={perm} "
              f"build={loader_build_s:.3f}s", flush=True)

        measured = 0
        for k in range(steps_per_epoch):
            t0 = t2
            try:
                batch = next(it)
            except StopIteration:
                print(f"警告:epoch {epoch} 第 {k} 步数据流提前耗尽", file=sys.stderr)
                break
            t1 = time.perf_counter()
            for _ in range(config.PROBE_B_COMPUTE_ITERS):
                _ = A @ B             # 极小 compute 替身(量级校准见 config.py)
            t2 = time.perf_counter()
            warmup = epoch == 0 and k < warmup_total
            records.append({
                "epoch": epoch,
                "step_in_epoch": k,
                "global_step": len(records),
                "warmup": warmup,
                "wait_s": round(t1 - t0, 6),
                "step_s": round(t2 - t0, 6),
                "shard": batch["shard"],
            })
            if not warmup:
                measured += 1
            if len(batch["jpg"]) != config.PROBE_B_BATCH:
                print(f"警告:epoch {epoch} 第 {k} 步 batch 不满"
                      f"({len(batch['jpg'])}),", file=sys.stderr)

        ep = [r for r in records if r["epoch"] == epoch]
        meas = [r for r in ep if not r["warmup"]]
        meta = {
            "epoch": epoch,
            "epoch_seed": args.seed + epoch,
            "shard_perm": perm,
            "loader_build_s": round(loader_build_s, 6),
            "steps": len(meas),
            "warmup_steps": len(ep) - len(meas),
        }
        if meas:
            waits = np.array([r["wait_s"] for r in meas])
            stps = np.array([r["step_s"] for r in meas])
            stalls = waits / stps
            meta.update({
                "median_stall": float(np.median(stalls)),
                "mean_stall": float(stalls.mean()),
                "p90_wait_s": float(np.percentile(waits, 90)),
                "p50_step_s": float(np.median(stps)),
            })
        else:
            meta.update({"median_stall": None, "mean_stall": None,
                         "p90_wait_s": None, "p50_step_s": None})
        per_epoch.append(meta)
        print(f"[epoch {epoch}] 实测 {meta['steps']} 步 "
              f"median_stall={meta['median_stall']} "
              f"mean_stall={meta['mean_stall']}", flush=True)

        del it
        del loader  # 释放本 epoch worker,避免与下一 epoch 的 worker 叠加

    environment = {}
    env_path = args.env_file
    if env_path is None and "PROBE_C_ENV_FILE" in os.environ:
        env_path = Path(os.environ["PROBE_C_ENV_FILE"])
    if env_path is not None:
        p = Path(env_path)
        if p.exists():
            environment = json.loads(p.read_text())
            environment["env_file"] = str(p)
    environment["date"] = datetime.now().isoformat(timespec="seconds")
    environment["interpreter"] = sys.executable
    environment["note"] = ("epoch 之间未做任何清缓存/容器重启(测量纪律);"
                           "epoch1/2 首步 wait 含 loader 重建与 worker 重启")

    overall = [r for r in records if not r["warmup"]]
    out = {
        "source": args.source,
        "workers": args.workers,
        "epochs": args.epochs,
        "seed": args.seed,
        "steps_per_epoch": steps_per_epoch,
        "warmup_steps": warmup_total,
        "batch": config.PROBE_B_BATCH,
        "compute_iters": config.PROBE_B_COMPUTE_ITERS,
        "n_steps_total": len(records),
        "n_steps_measured": len(overall),
        "per_epoch": per_epoch,
        "per_step": records,
        "environment": environment,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    meds = [m["median_stall"] for m in per_epoch if m["median_stall"] is not None]
    print(f"{args.source} w{args.workers} {args.epochs} epoch:"
          f"中位 stall " + " / ".join(f"{m:.1%}" for m in meds)
          + f" → {args.out}")


if __name__ == "__main__":
    main()
