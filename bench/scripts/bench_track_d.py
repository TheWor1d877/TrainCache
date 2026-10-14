#!/usr/bin/env python3
"""bench D 轨负载器:单 Job 连续 N epoch、每 epoch 换 seed、缓存欠配 50% 的 stall 打点。

与探针 C 权威装置 probes/probe_c/train_multi_epoch.py 对齐(循环结构、每 epoch
重建 loader、seed 化 shuffle、per-step 打点、wds 1.0.2 语义);差异只在参数来源
与输出形态:

  * 参数缺省取 bench/config.py 的 TRACK_D_*:--url/--n-tars/--epochs/--seed/
    --workers/--batch/--steps-per-epoch/--warmup/--out-dir;
  * 输出 out-dir/per_step_<seed>_w<workers>.jsonl(每步一行,字段与 probe_c 的
    per_step 记录完全一致:epoch/step_in_epoch/global_step/warmup/wait_s/
    step_s/shard)+ out-dir/epochs_meta.json(epoch 级 meta:epoch_seed、
    shard_perm、loader_build_s、per-epoch stall 统计;每 epoch 覆盖写,可中途取)。

测量口径(勿改,逐项同 probe_c):
  * 每步:t0 = 上一步 compute 结束 → next(batch)=t1 → 1024² matmul×8(单线程)
    =t2;wait=t1-t0、step=t2-t0;epoch 边界不重置时钟(epoch≥1 首步 wait 含
    loader 重建 + worker 重启,切换代价即测量对象);compute 矩阵跨 epoch 复用;
  * 每 epoch 重建 loader:分片置换 = 父进程 random.Random(seed+epoch) 对
    0..n_tars-1 生成置换,按 --url 模板显式构造 URL 列表(WebDataset
    (shardshuffle=False) 的读取顺序 = 列表顺序);样本级 .shuffle(100,
    seed=seed+epoch)(wds 1.0.2 实测:有效种子 = 传入值,同 seed 复跑逐位相同);
  * epoch 0 前 --warmup 步为 warmup(计入步号、标记排除);epoch 长不足其两倍时
    截断到 steps_per_epoch//2(同 probe_c,保护微型 smoke 运行);
  * shard 号从样本 __key__ 提取("shard-00003_000000" / "shard-00003/xxx" → 3)。

本文件同时承载 bench verify 的判据函数,供 track_d_verify.sh 复用,并可在函数级
直接自测(相对差 5% → PASS、15% → FAIL,阈值 10%):

  * 主判据(epoch 级聚合对账):epoch_stall_stats / round_stall_stats /
    compare_epoch_medians —— 两轮 per-epoch median_stall 逐 epoch 相对差
    ≤ 阈值 且 两轮全轮 median_stall 相对差 ≤ 阈值,判"对上";
  * 报告项(不参与 verdict):window_curves / compare_window_curves —— 窗口
    stall 均值曲线逐点相对差。主对话裁决(2026-10-13):窗口均值被 wait 分布
    重尾毛刺主导,同一冷启内相邻 epoch 曲线逐点最大相对差已达 37%~56%,低于
    指标噪声底、无判别力,故降级为报告项(数值仍计算并写入 verify json)。

用法(装置内由 track_d_up.sh / track_d_verify.sh 调用,一般不需手敲):
  /usr/bin/python3.12 bench/scripts/bench_track_d.py --epochs 10 \
      --out-dir bench/results/track_d
smoke(自建微型 tar + 本地 http.server):
  /usr/bin/python3.12 bench/scripts/bench_track_d.py \
      --url http://127.0.0.1:9101/shard-%06d.tar --n-tars 2 --epochs 2 \
      --steps-per-epoch 4 --workers 0 --seed 7 --out-dir /tmp/track_d_smoke
"""
import argparse
import json
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

# ---- 与探针同口径的固定常量(量级校准依据见 probes/config.py 的 PROBE_B_*)----
COMPUTE_SIZE = 1024        # 1024² matmul 边长(单次≈25ms)
COMPUTE_ITERS = 8          # ×8 ≈200ms/步(单线程)
PREFETCH = 2               # WebLoader prefetch_factor(同 PROBE_B_PREFETCH)
SHUFFLE_BUFFER = 100       # 样本级 shuffle 缓冲(同 probe_c)
WINDOW = 20                # verify 窗口步数(报告项口径,同第一步)
VERIFY_TOL = 0.10          # verify 阈值缺省;实际由 track_d_verify.sh 传
                           # bench/config.py 的 COLD_VERIFY_TOLERANCE


def shard_of_key(key: str) -> int:
    """从样本 __key__ 提分片号:"shard-00003_000000" / "shard-00003/xxx" → 3。"""
    head = key.split("/")[0].split("_")[0]
    return int(head.split("-")[-1])


def shard_perm(seed: int, epoch: int, n_tars: int):
    """分片级置换:父进程 RNG,种子 = seed + epoch(确定性,同 probe_c)。"""
    perm = list(range(n_tars))
    random.Random(seed + epoch).shuffle(perm)
    return perm


def build_urls(url_tmpl: str, perm):
    """按置换显式构造 URL 列表(列表顺序 = wds 读取顺序)。"""
    return [url_tmpl % i for i in perm]


def make_loader(url_tmpl, n_tars, workers, batch, seed, epoch):
    """重建该 epoch 的 loader;返回 (loader, urls, perm)。"""
    perm = shard_perm(seed, epoch, n_tars)
    urls = build_urls(url_tmpl, perm)

    def collate(samples):
        # 与 probe_c 相同的 worker 内解码/转 tensor;额外带出分片号供打点
        xs = torch.stack([
            torch.from_numpy(np.array(s["jpg"])).float() / 255.0 for s in samples
        ])
        return {"jpg": xs.permute(0, 3, 1, 2),
                "shard": shard_of_key(samples[0]["__key__"])}

    ds = (wds.WebDataset(urls, shardshuffle=False)
          .shuffle(SHUFFLE_BUFFER, seed=seed + epoch)
          .decode("pil"))
    loader = wds.WebLoader(
        ds, batch_size=batch, collate_fn=collate, num_workers=workers,
        prefetch_factor=PREFETCH if workers > 0 else None,
        persistent_workers=workers > 0,
    )
    return loader, urls, perm


def stall(r):
    return r["wait_s"] / r["step_s"] if r["step_s"] > 0 else 0.0


def load_records(path):
    """读 per_step_*.jsonl → 记录列表(track_d_verify.sh 复用)。"""
    recs = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line:
            recs.append(json.loads(line))
    return recs


def window_curves(records, epochs, steps_per_epoch, window=WINDOW):
    """每 epoch 一条「窗口 stall 均值」曲线(verify 报告项口径,同第一步)。

    窗口 = [w*window, (w+1)*window) 步(每 epoch 步号从 0 起);值 = 窗口内
    stall(wait_s/step_s)均值;epoch0 前 warmup 步(记录带 warmup 标记)不计;
    不足一窗的余数步不入窗(与 probe_c/judge_track_d 同);空窗记 None。
    """
    meas = [r for r in records if not r.get("warmup")]
    n_windows = steps_per_epoch // window
    curves = []
    for e in range(epochs):
        ep = [r for r in meas if r["epoch"] == e]
        vals = []
        for w in range(n_windows):
            lo, hi = w * window, (w + 1) * window
            vv = [stall(r) for r in ep if lo <= r["step_in_epoch"] < hi]
            vals.append(float(np.mean(vv)) if vv else None)
        curves.append(vals)
    return curves


def rel_diff(a, b):
    """逐点相对差:|a-b| / max(a,b);两侧皆 0 记 0;任一 None → None(缺测)。"""
    if a is None or b is None:
        return None
    m = max(abs(a), abs(b))
    return 0.0 if m == 0 else abs(a - b) / m


def compare_window_curves(curves_a, curves_b, tol=VERIFY_TOL):
    """[报告项]两轮窗口曲线逐点相对差统计(不参与 verify verdict)。

    主对话裁决(2026-10-13):窗口均值被 wait 分布重尾毛刺主导,同一冷启内
    相邻 epoch 的曲线逐点最大相对差已达 37%~56%,低于指标噪声底、无判别力,
    故降级为报告项;数值(逐点相对差/max_rel_diff)继续写入 verify json,
    verdict 以 compare_epoch_medians(epoch 级聚合)为准。本函数返回结构不变
    {tol, per_epoch: [{epoch, curve_a, curve_b, rel_diffs, max_rel_diff}],
    max_rel_diff, n_points, n_missing, verdict, verdict_label},其中 verdict
    仅为统计副产品,verify 不再采用。
    """
    fail = {"tol": tol, "per_epoch": [], "max_rel_diff": None, "n_points": 0,
            "n_missing": None, "verdict": "FAIL", "verdict_label": "对不上"}
    if len(curves_a) != len(curves_b):
        return dict(fail, note=f"两轮曲线 epoch 数不一致:{len(curves_a)} vs {len(curves_b)}")
    per_epoch = []
    max_rel = None
    n_points = 0
    n_missing = 0
    for e, (ca, cb) in enumerate(zip(curves_a, curves_b)):
        if len(ca) != len(cb):
            return dict(fail, note=f"epoch {e} 窗口数不一致:{len(ca)} vs {len(cb)}")
        diffs = []
        for a, b in zip(ca, cb):
            d = rel_diff(a, b)
            diffs.append(d)
            if d is None:
                n_missing += 1
            else:
                n_points += 1
                max_rel = d if max_rel is None else max(max_rel, d)
        per_epoch.append({
            "epoch": e, "curve_a": ca, "curve_b": cb, "rel_diffs": diffs,
            "max_rel_diff": max((d for d in diffs if d is not None), default=None)})
    ok = n_points > 0 and max_rel is not None and max_rel <= tol
    return {"tol": tol, "per_epoch": per_epoch, "max_rel_diff": max_rel,
            "n_points": n_points, "n_missing": n_missing,
            "verdict": "PASS" if ok else "FAIL",
            "verdict_label": "对上" if ok else "对不上"}


def epoch_stall_stats(records, epochs):
    """逐 epoch stall 统计(epoch 级聚合判据的取数函数)。

    返回 [{epoch, n, median_stall, mean_stall}],epoch 从 0 到 epochs-1;
    n = 该 epoch 已测步数(epoch0 前 warmup 步不计);n=0 → median/mean = None。
    """
    meas = [r for r in records if not r.get("warmup")]
    out = []
    for e in range(epochs):
        vals = [stall(r) for r in meas if r["epoch"] == e]
        out.append({
            "epoch": e, "n": len(vals),
            "median_stall": float(np.median(vals)) if vals else None,
            "mean_stall": float(np.mean(vals)) if vals else None,
        })
    return out


def round_stall_stats(records):
    """全轮 stall 统计:{n, median_stall, mean_stall}。

    全轮 = 该轮全部已测步(warmup 步不计)合并;n=0 → median/mean = None。
    """
    vals = [stall(r) for r in records if not r.get("warmup")]
    return {
        "n": len(vals),
        "median_stall": float(np.median(vals)) if vals else None,
        "mean_stall": float(np.mean(vals)) if vals else None,
    }


def compare_epoch_medians(meds_a, meds_b, round_med_a, round_med_b, tol=VERIFY_TOL):
    """D 轨 verify 主判据(epoch 级聚合对账;窗口逐点对账已降级为报告项)。

    语义:PASS ⇔ ① 两轮 per-epoch median_stall 逐 epoch 相对差全部 ≤ tol,
    且 ② 两轮全轮 median_stall 相对差 ≤ tol(相对差口径同 rel_diff:
    |a-b|/max(a,b))。缺轮(任一轮无 per-epoch/全轮 median)、缺 epoch(两轮
    epoch 数不一致)或任一 epoch 缺测(None)→ FAIL(不判"对上")。

    入参:
      meds_a/meds_b:两轮逐 epoch median_stall(float/None 列表,经
        epoch_stall_stats 取);
      round_med_a/round_med_b:两轮全轮 median_stall(float,经
        round_stall_stats 取);
      tol:阈值(缺省 VERIFY_TOL;实际由 track_d_verify.sh 传 bench/config.py
        的 COLD_VERIFY_TOLERANCE)。
    返回:{tol, per_epoch: [{epoch, median_a, median_b, rel_diff}],
          max_rel_diff(逐 epoch 最大相对差),
          round_median: {round_a, round_b, rel_diff},
          verdict(PASS/FAIL), verdict_label(对上/对不上), note(不满足原因)}。
    """
    fail = {"tol": tol, "per_epoch": [], "max_rel_diff": None,
            "round_median": {"round_a": round_med_a, "round_b": round_med_b,
                             "rel_diff": rel_diff(round_med_a, round_med_b)},
            "verdict": "FAIL", "verdict_label": "对不上"}
    if not meds_a or not meds_b:
        return dict(fail, note="缺轮:任一轮无 per-epoch median_stall 数据")
    if len(meds_a) != len(meds_b):
        return dict(fail, note=f"缺 epoch:两轮 epoch 数不一致"
                               f"({len(meds_a)} vs {len(meds_b)})")
    if round_med_a is None or round_med_b is None:
        return dict(fail, note="缺轮:全轮 median_stall 缺测(任一轮无实测步)")
    per_epoch = []
    max_rel = None
    for e, (a, b) in enumerate(zip(meds_a, meds_b)):
        d = rel_diff(a, b)
        if d is None:
            return dict(fail, per_epoch=per_epoch, max_rel_diff=max_rel,
                        note=f"缺测:epoch {e} 的 median_stall 缺失(该 epoch 无实测步)")
        per_epoch.append({"epoch": e, "median_a": a, "median_b": b, "rel_diff": d})
        max_rel = d if max_rel is None else max(max_rel, d)
    rd = rel_diff(round_med_a, round_med_b)
    ok = max_rel is not None and max_rel <= tol and rd <= tol
    out = {"tol": tol, "per_epoch": per_epoch, "max_rel_diff": max_rel,
           "round_median": {"round_a": round_med_a, "round_b": round_med_b,
                            "rel_diff": rd},
           "verdict": "PASS" if ok else "FAIL",
           "verdict_label": "对上" if ok else "对不上"}
    if not ok:
        why = []
        if max_rel is None or max_rel > tol:
            why.append(f"逐 epoch 最大相对差 "
                       f"{'—' if max_rel is None else format(max_rel, '.1%')} > {tol:.0%}")
        if rd is None or rd > tol:
            why.append(f"全轮 median 相对差 "
                       f"{'—' if rd is None else format(rd, '.1%')} > {tol:.0%}")
        out["note"] = ";".join(why)
    return out


def write_meta(meta_path, args, warmup_total, per_step_path, per_epoch, records):
    """epochs_meta.json:epoch 级 meta 汇总(每 epoch 覆盖写,可中途取)。"""
    out = {
        "track": "D",
        "date": datetime.now().isoformat(timespec="seconds"),
        "interpreter": sys.executable,
        "url": args.url,
        "n_tars": args.n_tars,
        "epochs": args.epochs,
        "seed": args.seed,
        "workers": args.workers,
        "batch": args.batch,
        "steps_per_epoch": args.steps_per_epoch,
        "warmup_steps": warmup_total,
        "compute": {"size": COMPUTE_SIZE, "iters": COMPUTE_ITERS, "threads": 1},
        "shuffle_buffer": SHUFFLE_BUFFER,
        "prefetch_factor": PREFETCH,
        "n_steps_total": len(records),
        "n_steps_measured": sum(1 for r in records if not r["warmup"]),
        "per_step_path": str(per_step_path),
        "per_epoch": per_epoch,
        "note": ("epoch 之间不清缓存、不重启容器(测量对象);每 epoch 重建 loader + "
                 "分片/样本两级换 seed;epoch≥1 首步 wait 含切换代价"),
    }
    meta_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")


def main():
    ap = argparse.ArgumentParser(
        description="bench D 轨负载器:单 Job 连续 N epoch、每 epoch 换 seed 的 stall 打点"
                    "(口径同 probes/probe_c/train_multi_epoch.py)")
    ap.add_argument("--url", default=config.TRACK_D_URL,
                    help="tar URL 模板,含一个整数占位(缺省 config.TRACK_D_URL,"
                         "形如 http://127.0.0.1:9100/track-d/shard-NNNNNN.tar 的六位数字模板)")
    ap.add_argument("--n-tars", type=int, default=config.TRACK_D_N_TARS,
                    help="分片总数(缺省 config.TRACK_D_N_TARS)")
    ap.add_argument("--epochs", type=int, default=config.TRACK_D_EPOCHS,
                    help="连续 epoch 数(缺省 config.TRACK_D_EPOCHS)")
    ap.add_argument("--seed", type=int, default=config.TRACK_D_SEED,
                    help="基种子,epoch e 有效种子 = seed+e(缺省 config.TRACK_D_SEED)")
    ap.add_argument("--workers", type=int, default=config.TRACK_D_WORKERS,
                    help="WebLoader worker 数(缺省 config.TRACK_D_WORKERS)")
    ap.add_argument("--batch", type=int, default=config.TRACK_D_BATCH,
                    help="batch 大小(缺省 config.TRACK_D_BATCH)")
    ap.add_argument("--steps-per-epoch", type=int,
                    default=config.TRACK_D_STEPS_PER_EPOCH,
                    help="epoch 长(缺省 config.TRACK_D_STEPS_PER_EPOCH = "
                         "N_TARS*IMAGES_PER_TAR/BATCH 的一遍完整数据)")
    ap.add_argument("--warmup", type=int, default=config.TRACK_D_WARMUP,
                    help="epoch0 前 N 步标记 warmup 排除(缺省 config.TRACK_D_WARMUP)")
    ap.add_argument("--out-dir", type=Path, default=config.TRACK_D_RESULTS_DIR,
                    help="输出目录(缺省 config.TRACK_D_RESULTS_DIR)")
    args = ap.parse_args()

    if args.n_tars < 1 or args.epochs < 1 or args.batch < 1 \
            or args.steps_per_epoch < 1 or args.workers < 0:
        ap.error("参数需满足:n-tars≥1、epochs≥1、batch≥1、steps-per-epoch≥1、workers≥0")
    try:
        probe_url = args.url % (args.n_tars - 1)
    except (TypeError, ValueError) as e:
        ap.error(f"--url 需含一个整数占位(如 %06d 形式的模板):{e}")
    if probe_url == args.url:
        ap.error("--url 模板未使用占位(所有分片 URL 会相同),需含一个整数占位")

    warmup_total = min(args.warmup, args.steps_per_epoch // 2)
    if warmup_total != args.warmup:
        print(f"警告:warmup 截断为 {warmup_total}(epoch 长 {args.steps_per_epoch},"
              f"同 probe_c)", file=sys.stderr)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    per_step_path = args.out_dir / f"per_step_{args.seed}_w{args.workers}.jsonl"
    meta_path = args.out_dir / "epochs_meta.json"

    torch.manual_seed(args.seed)
    # compute 量级校准与 probe_c 一致:单线程 1024² matmul ×8 ≈200ms,跨 epoch 复用
    torch.set_num_threads(1)
    n = COMPUTE_SIZE
    A = torch.randn(n, n)
    B = torch.randn(n, n)

    print(f"D 轨负载:url={args.url} n_tars={args.n_tars} epochs={args.epochs} "
          f"seed={args.seed} workers={args.workers} batch={args.batch} "
          f"steps_per_epoch={args.steps_per_epoch} warmup={warmup_total}")
    print(f"输出:{per_step_path} / {meta_path}", flush=True)

    records = []
    per_epoch = []
    t2 = None
    with per_step_path.open("w") as fout:
        for epoch in range(args.epochs):
            t_build = time.perf_counter()
            loader, urls, perm = make_loader(
                args.url, args.n_tars, args.workers, args.batch, args.seed, epoch)
            it = iter(loader)
            loader_build_s = time.perf_counter() - t_build
            if t2 is None:
                t2 = time.perf_counter()  # epoch 0:时钟从 loader 建好后起(等首步)
            print(f"[epoch {epoch}] seed={args.seed + epoch} perm={perm} "
                  f"build={loader_build_s:.3f}s", flush=True)

            measured = 0
            for k in range(args.steps_per_epoch):
                t0 = t2
                try:
                    batch = next(it)
                except StopIteration:
                    print(f"警告:epoch {epoch} 第 {k} 步数据流提前耗尽"
                          f"(epoch 长 {args.steps_per_epoch} 超过数据可供给步数)",
                          file=sys.stderr)
                    break
                t1 = time.perf_counter()
                for _ in range(COMPUTE_ITERS):
                    _ = A @ B             # 极小 compute 替身(量级校准见探针 config)
                t2 = time.perf_counter()
                warmup = epoch == 0 and k < warmup_total
                rec = {
                    "epoch": epoch,
                    "step_in_epoch": k,
                    "global_step": len(records),
                    "warmup": warmup,
                    "wait_s": round(t1 - t0, 6),
                    "step_s": round(t2 - t0, 6),
                    "shard": batch["shard"],
                }
                records.append(rec)
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                fout.flush()
                if not warmup:
                    measured += 1
                if len(batch["jpg"]) != args.batch:
                    print(f"警告:epoch {epoch} 第 {k} 步 batch 不满"
                          f"({len(batch['jpg'])}/{args.batch})", file=sys.stderr)

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
            write_meta(meta_path, args, warmup_total, per_step_path,
                       per_epoch, records)

            del it
            del loader  # 释放本 epoch worker,避免与下一 epoch 的 worker 叠加

    meds = [m["median_stall"] for m in per_epoch if m["median_stall"] is not None]
    print(f"D 轨 {args.epochs} epoch w{args.workers}:中位 stall "
          + " / ".join(f"{m:.1%}" for m in meds)
          + f" → {per_step_path}")


if __name__ == "__main__":
    main()
