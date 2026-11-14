#!/usr/bin/env python3
"""bench train负载器:单 Job 连续 N epoch、每 epoch 换 seed、缓存欠配 50% 的 stall 打点。

循环结构:每 epoch 重建 loader、seed 化 shuffle、per-step 打点(wds 1.0.2 语义)。

  * 参数缺省取 bench/config.py 的 TRAIN_*:--n-tars/--epochs/--seed/
    --workers/--batch/--steps-per-epoch/--warmup/--out-dir;
  * 输出 out-dir/per_step_<seed>_w<workers>.jsonl(每步一行:epoch/
    step_in_epoch/global_step/warmup/wait_s/step_s/shard/shards/phase/stage)
    + out-dir/epochs_meta.json(epoch 级 meta:epoch_seed、shard_perm、
    loader_build_s、per-epoch stall 统计;每 epoch 覆盖写,可中途取)
    + out-dir/run.json(run 级边界:t_submit/t_first_step/t_last_step、
    switch_events、phase 口径;M1 新增)。

测量口径(勿改):
  * 每步:t0 = 上一步 compute 结束 → next(batch)=t1 → 1024² matmul×8(单线程)
    =t2;wait=t1-t0、step=t2-t0;epoch 边界不重置时钟(epoch≥1 首步 wait 含
    loader 重建 + worker 重启,切换代价即测量对象);compute 矩阵跨 epoch 复用;
  * 每 epoch 重建 loader:分片置换 = 父进程 random.Random(seed+epoch) 对
    0..n_tars-1 生成置换,显式构造 tcblk:// 分片 URL 列表(WebDataset
    (shardshuffle=False) 的读取顺序 = 列表顺序);样本级 .shuffle(100,
    seed=seed+epoch)(wds 1.0.2 实测:有效种子 = 传入值,同 seed 复跑逐位相同);
  * run 边界:t_submit 优先取环境变量 TRAINCACHE_JOB_CREATED_AT(runner 创建
    Job 前注入),缺失时回退脚本启动时刻并标记 script-start-fallback;
    t_first_step 在 epoch0 首个 batch 就绪后、compute 前;t_last_step 在最后
    一个成功 step 的 compute 结束后;
  * phase:startup 只存在于 run.json(t_submit→t_first_step,没有步记录);
    每个 epoch 边界记一次 switch,直到新窗口 K 块对应的分片全部被训练侧打开
    (可观测代理),否则截到该 epoch 结束;其余步为 steady。切换窗口的就绪
    时刻同时写进 run.json 的 switch_events,供 M1 采集器对账;
  * epoch 0 前 --warmup 步为 warmup(计入步号、标记排除);epoch 长不足其两倍时
    截断到 steps_per_epoch//2(保护微型 smoke 运行);
  * shard 号从样本 __key__ 提取("shard-00003_000000" / "shard-00003/xxx" → 3)。

数据面(e2e 块读,唯一模式):分片不读 HTTP URL,而是经 bench/e2e/
blockread.py 从节点本地块缓存拼回 tar 字节流(tcblk:// scheme 注入 webdataset
gopen);orders/hook 由 blockread 负责,每 epoch 写一次消费序订单(stage_id=
epoch)并 10s 心跳续写。

用法(e2e 族由 bench/experiments/run.py 在训练 Pod 内调用,一般不需手敲):
  /usr/bin/python3.12 bench/scripts/load_train.py --epochs 10 \
      --out-dir bench/results/train
块读 env(TRAINCACHE_ENDPOINT/BUCKET/DATASET、CACHE_ROOT 等)见 blockread.py;
本地冒烟见 bench/e2e/test_blockread.py --smoke(造假缓存 + 假 MinIO)。
"""
import argparse
import json
import math
import multiprocessing
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import torch
import webdataset as wds

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# 镜像里 load_train/blockread/config 平铺在 /app(脚本目录本就在 sys.path);
# 宿主机上 blockread 在 bench/e2e,需显式加入。
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "e2e"))
import config
import blockread

blockread.install()

# Python 3.14 把默认 start method 改成 forkserver:局部 collate 不可 pickle,
# 且全新 worker 进程继承不到 tcblk scheme 注册。改回 fork(3.12 语义),
# worker 经 fork 继承 opener 注册与快照缓存。
if "fork" in multiprocessing.get_all_start_methods():
    multiprocessing.set_start_method("fork", force=True)

# ---- 固定常量(compute 量级与 loader 口径,勿改)----
COMPUTE_SIZE = 1024        # 1024² matmul 边长(单次≈25ms)
COMPUTE_ITERS = 8          # ×8 ≈200ms/步(单线程)
PREFETCH = 2               # WebLoader prefetch_factor
SHUFFLE_BUFFER = 100       # 样本级 shuffle 缓冲
BASELINE_STEPS = config.TRAIN_BASELINE_STEPS  # 切换回落基线步数(4.3 按族的判据「switch·回落」)


def shard_of_key(key: str) -> int:
    """从样本 __key__ 提分片号:"shard-00003_000000" / "shard-00003/xxx" → 3。"""
    head = key.split("/")[0].split("_")[0]
    return int(head.split("-")[-1])


def shard_perm(seed: int, epoch: int, n_tars: int):
    """分片级置换:父进程 RNG,种子 = seed + epoch(确定性)。"""
    perm = list(range(n_tars))
    random.Random(seed + epoch).shuffle(perm)
    return perm


def build_urls(perm):
    """按置换显式构造块读 URL 列表(列表顺序 = wds 读取顺序)。"""
    return [blockread.shard_url(f"shard-{i:06d}.tar") for i in perm]


def utc_now() -> str:
    """当前 UTC 时刻,RFC3339 微秒精度(与 run.json 其他时刻同格式)。"""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z")


def parse_rfc3339(text: str) -> datetime:
    """解析 runner 注入的 RFC3339;失败抛 ValueError(由调用方决定回退)。"""
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def stage_of(epoch: int, epochs: int, fractions) -> int:
    """curriculum stage 归属(4.2 的阶段切换族一节):E<3 全 stage 0,否则 equal-thirds。"""
    if epochs < 3:
        return 0
    if epoch < epochs // 3:
        return 0
    if epoch < (2 * epochs) // 3:
        return 1
    return 2


def stage_shards(n_tars: int, fractions, stage: int) -> int:
    """stage 分片数:ceil(n_tars * fraction),至少 1。"""
    return max(1, math.ceil(n_tars * fractions[stage]))


def epoch_perm(seed: int, epoch: int, epochs: int, n_tars: int, shuffle: str,
               fractions):
    """该 epoch 的 (分片置换, stage)。

    fixed:所有 epoch 用 seed;per-epoch:seed+epoch;curriculum:全局置换由 seed
    定,stage 用嵌套前缀 P[:ceil(n*fraction)],stage 内顺序固定。
    """
    if shuffle == "fixed":
        return shard_perm(seed, 0, n_tars), 0
    if shuffle == "per-epoch":
        return shard_perm(seed, epoch, n_tars), 0
    stage = stage_of(epoch, epochs, fractions)
    order = shard_perm(seed, 0, n_tars)
    return order[:stage_shards(n_tars, fractions, stage)], stage


def window_shards(window_k: int, chunk_bytes: int, shard_bytes: int,
                  available: int) -> int:
    """新窗口 K 块对应的分片数(块按 shard 内切分;trainer 可见的代理)。

    ceil(K / ceil(shard/chunk)),至少 1,不超过该 stage 可用分片数;K=0 → 0。
    """
    if window_k <= 0:
        return 0
    chunks_per_shard = max(1, math.ceil(shard_bytes / chunk_bytes))
    return min(available, max(1, math.ceil(window_k / chunks_per_shard)))


def repeat_count(stage_count: int, images_per_tar: int, batch: int,
                 steps_per_epoch: int) -> int:
    """curriculum 窄 stage 需要循环几遍分片表才能填满固定 epoch 长度。"""
    if stage_count <= 0 or images_per_tar <= 0:
        return 1
    return max(1, math.ceil(steps_per_epoch * batch / (stage_count * images_per_tar)))


def make_loader(n_tars, workers, batch, seed, epoch, epochs, shuffle,
                fractions, images_per_tar, steps_per_epoch):
    """重建该 epoch 的 loader(块读唯一模式);返回 (loader, urls, perm, stage, repeats)。"""
    perm, stage = epoch_perm(seed, epoch, epochs, n_tars, shuffle, fractions)
    repeats = repeat_count(len(perm), images_per_tar, batch, steps_per_epoch)
    urls = build_urls(perm) * repeats
    # fixed / curriculum 的 stage 内顺序固定(seed 不随 epoch 变);per-epoch 换 seed。
    shuffle_seed = seed if shuffle in ("fixed", "curriculum") else seed + epoch

    def collate(samples):
        # worker 内解码/转 tensor;带出分片集合供窗口就绪判断
        xs = torch.stack([
            torch.from_numpy(np.array(s["jpg"])).float() / 255.0 for s in samples
        ])
        shards = sorted({shard_of_key(s["__key__"]) for s in samples})
        return {"jpg": xs.permute(0, 3, 1, 2),
                "shard": shards[0],
                "shards": shards}

    ds = (wds.WebDataset(urls, shardshuffle=False)
          .shuffle(SHUFFLE_BUFFER, seed=shuffle_seed)
          .decode("pil"))
    loader = wds.WebLoader(
        ds, batch_size=batch, collate_fn=collate, num_workers=workers,
        prefetch_factor=PREFETCH if workers > 0 else None,
        persistent_workers=workers > 0,
    )
    return loader, urls, perm, stage, repeats


def stall(r):
    return r["wait_s"] / r["step_s"] if r["step_s"] > 0 else 0.0


def args_dict(args):
    """argparse Namespace → JSON 可写字典(Path 转字符串)。"""
    return {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}


CHECKPOINT_NAME = "checkpoint.json"
CHECKPOINT_SCHEMA = "1"


def write_checkpoint(ckpt_dir: Path, state: dict) -> Path:
    """写中断点：epoch 进度、逐步记录、每 epoch 汇总。"""
    ckpt_dir = Path(ckpt_dir)
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    path = ckpt_dir / CHECKPOINT_NAME
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    return path


def read_checkpoint(ckpt_dir: Path) -> dict:
    """读中断点。文件不在或坏了就抛错，不静默从头跑。"""
    path = Path(ckpt_dir) / CHECKPOINT_NAME
    if not path.is_file():
        raise FileNotFoundError(f"checkpoint 不存在: {path}")
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("schema_version") != CHECKPOINT_SCHEMA:
        raise ValueError(f"checkpoint schema 不认识: {state.get('schema_version')!r}")
    return state


def checkpoint_state(records: list, per_epoch: list, next_epoch: int,
                     run: dict, args, warmup_total: int) -> dict:
    """组装中断点内容。

    存的是「训练进度」不是「模型权重」：已完成 epoch 的逐步记录、每 epoch
    汇总、下一个要跑的 epoch 序号，以及跨段要用的时刻（提交、首步、末步）。
    """
    return {
        "schema_version": CHECKPOINT_SCHEMA,
        "next_epoch": next_epoch,
        "records": records,
        "per_epoch": per_epoch,
        "switch_events": list(run.get("switch_events") or []),
        "t_submit": run.get("t_submit"),
        "t_first_step": run.get("t_first_step"),
        "t_last_step": run.get("t_last_step"),
        "interrupt_at": utc_now(),
        "args": args_dict(args),
        "warmup_total": warmup_total,
    }


def ts_delta_s(later: str | None, earlier: str | None) -> float | None:
    """两个 RFC3339 时刻的差（秒）。任一为空或不可解析返回 None。"""
    if not later or not earlier:
        return None
    try:
        dt_later = parse_rfc3339(str(later))
        dt_earlier = parse_rfc3339(str(earlier))
    except (ValueError, TypeError):
        return None
    return round((dt_later - dt_earlier).total_seconds(), 6)


def write_run_json(path, run):
    """run 级产物:先写初稿(t_first/t_last 为空),结束时覆盖为终稿。"""
    path.write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")


def switch_baseline(records, n=BASELINE_STEPS):
    """切换前 n 个实测步的 stall 基线(4.3 按族的判据「switch·回落」;warmup 不计)。"""
    vals = [stall(r) for r in records if not r.get("warmup")][-n:]
    if not vals:
        return {"baseline_n": 0, "baseline_stall": None,
                "insufficient_baseline": True}
    return {
        "baseline_n": len(vals),
        "baseline_stall": float(np.mean(vals)),
        "insufficient_baseline": len(vals) < n,
    }


def write_meta(meta_path, args, warmup_total, per_step_path, per_epoch, records):
    """epochs_meta.json:epoch 级 meta 汇总(每 epoch 覆盖写,可中途取)。"""
    out = {
        "date": utc_now(),
        "interpreter": sys.executable,
        "read_mode": "block",
        "n_tars": args.n_tars,
        "epochs": args.epochs,
        "seed": args.seed,
        "workers": args.workers,
        "batch": args.batch,
        "steps_per_epoch": args.steps_per_epoch,
        "warmup_steps": warmup_total,
        "shuffle": args.shuffle,
        "curriculum_fractions": list(config.TRAIN_CURRICULUM_FRACTIONS),
        "compute": {"size": COMPUTE_SIZE, "iters": COMPUTE_ITERS, "threads": 1},
        "shuffle_buffer": SHUFFLE_BUFFER,
        "prefetch_factor": PREFETCH,
        "n_steps_total": len(records),
        "n_steps_measured": sum(1 for r in records if not r["warmup"]),
        "per_step_path": str(per_step_path),
        "per_epoch": per_epoch,
        "note": ("epoch 之间不清缓存、不重启容器(测量对象);每 epoch 重建 loader;"
                 "fixed/curriculum 的 stage 内顺序固定,per-epoch 换 seed;"
                 "epoch≥1 首步 wait 含切换代价"),
    }
    meta_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")


def main():
    ap = argparse.ArgumentParser(
        description="bench train负载器:单 Job 连续 N epoch、每 epoch 换 seed 的 stall 打点")
    ap.add_argument("--n-tars", type=int, default=config.TRAIN_N_TARS,
                    help="分片总数(缺省 config.TRAIN_N_TARS)")
    ap.add_argument("--epochs", type=int, default=config.TRAIN_EPOCHS,
                    help="连续 epoch 数(缺省 config.TRAIN_EPOCHS)")
    ap.add_argument("--seed", type=int, default=config.TRAIN_SEED,
                    help="基种子(缺省 config.TRAIN_SEED)")
    ap.add_argument("--workers", type=int, default=config.TRAIN_WORKERS,
                    help="WebLoader worker 数(缺省 config.TRAIN_WORKERS)")
    ap.add_argument("--batch", type=int, default=config.TRAIN_BATCH,
                    help="batch 大小(缺省 config.TRAIN_BATCH)")
    ap.add_argument("--images-per-tar", type=int, default=config.TRAIN_IMAGES_PER_TAR,
                    help="每个 tar 的样本数,用于 curriculum 窄 stage 的循环数"
                         "(缺省 config.TRAIN_IMAGES_PER_TAR)")
    ap.add_argument("--steps-per-epoch", type=int,
                    default=config.TRAIN_STEPS_PER_EPOCH,
                    help="epoch 长(缺省 config.TRAIN_STEPS_PER_EPOCH = "
                         "N_TARS*IMAGES_PER_TAR/BATCH 的一遍完整数据)")
    ap.add_argument("--warmup", type=int, default=config.TRAIN_WARMUP,
                    help="epoch0 前 N 步标记 warmup 排除(缺省 config.TRAIN_WARMUP)")
    ap.add_argument("--shuffle", choices=("fixed", "per-epoch", "curriculum"),
                    default="per-epoch",
                    help="消费顺序形态(4.2 的阶段切换族一节;缺省 per-epoch,保持旧行为)")
    ap.add_argument("--window-k", type=int, default=config.TRAIN_WINDOW_K,
                    help="启动/切换窗口块数 K(缺省 config.TRAIN_WINDOW_K)")
    ap.add_argument("--chunk-bytes", type=int, default=config.TRAIN_CHUNK_BYTES,
                    help="DataSet 块粒度,用于 K 块→分片数换算")
    ap.add_argument("--shard-bytes", type=int, default=config.TRAIN_SHARD_BYTES,
                    help="单个 tar 的目标字节,用于 K 块→分片数换算")
    ap.add_argument("--out-dir", type=Path, default=config.TRAIN_RESULTS_DIR,
                    help="输出目录(缺省 config.TRAIN_RESULTS_DIR)")
    ap.add_argument("--checkpoint-dir", type=Path, default=None,
                    help="checkpoint 目录：写中断点、读续跑状态（e2e 族用）")
    ap.add_argument("--interrupt-after-epoch", type=int, default=-1,
                    help="跑完第 N 个 epoch 后写 checkpoint 并退出（-1 不中断）")
    ap.add_argument("--resume", action="store_true",
                    help="从 --checkpoint-dir 的 checkpoint 续跑")
    args = ap.parse_args()

    if args.resume and args.checkpoint_dir is None:
        ap.error("--resume 必须给 --checkpoint-dir")
    if args.interrupt_after_epoch >= args.epochs:
        ap.error("--interrupt-after-epoch 必须小于 --epochs")

    if args.n_tars < 1 or args.epochs < 1 or args.batch < 1 \
            or args.steps_per_epoch < 1 or args.workers < 0:
        ap.error("参数需满足:n-tars≥1、epochs≥1、batch≥1、steps-per-epoch≥1、workers≥0")
    if args.images_per_tar < 1 or args.window_k < 0 \
            or args.chunk_bytes < 1 or args.shard_bytes < 1:
        ap.error("参数需满足:images-per-tar≥1、window-k≥0、chunk-bytes≥1、shard-bytes≥1")

    warmup_total = min(args.warmup, args.steps_per_epoch // 2)
    if warmup_total != args.warmup:
        print(f"警告:warmup 截断为 {warmup_total}(epoch 长 {args.steps_per_epoch})",
              file=sys.stderr)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    per_step_path = args.out_dir / f"per_step_{args.seed}_w{args.workers}.jsonl"
    meta_path = args.out_dir / "epochs_meta.json"
    run_path = args.out_dir / "run.json"

    # ---- run 边界:提交时刻由 runner 注入;缺失时回退脚本启动并显式标记 ----
    started_at = utc_now()
    submit_env = os.environ.get("TRAINCACHE_JOB_CREATED_AT", "").strip()
    if submit_env:
        try:
            parse_rfc3339(submit_env)
            t_submit, t_submit_source = submit_env, "env"
        except ValueError:
            t_submit, t_submit_source = started_at, "invalid-env-fallback"
            print(f"警告:TRAINCACHE_JOB_CREATED_AT 无法解析({submit_env!r}),"
                  f"回退脚本启动时刻", file=sys.stderr)
    else:
        t_submit, t_submit_source = started_at, "script-start-fallback"
        print("警告:未注入 TRAINCACHE_JOB_CREATED_AT,run.json 的 t_submit "
              "回退为脚本启动时刻(仅限自检/粗扫,不用于正式对照)", file=sys.stderr)

    cond = os.environ.get("TRAINCACHE_COND", "").strip() or None
    run_id = os.environ.get("TRAINCACHE_RUN_ID", "").strip() or (
        f"{cond or 'train'}-s{args.seed}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{os.getpid()}")
    group = os.environ.get("TRAINCACHE_GROUP", "").strip() or None
    dataset = os.environ.get("TRAINCACHE_DATASET", "").strip() or "train"

    fractions = config.TRAIN_CURRICULUM_FRACTIONS
    startup_window_shards = window_shards(
        args.window_k, args.chunk_bytes, args.shard_bytes, args.n_tars)
    run = {
        "schema_version": "1",
        "run_id": run_id,
        "cond": cond,
        "group": group,
        "read_mode": "block",
        "dataset": dataset,
        "started_at": started_at,
        "finished_at": None,
        "t_submit": t_submit,
        "t_submit_source": t_submit_source,
        "t_pod_ready": None,
        "t_first_step": None,
        "t_last_step": None,
        "t_origin_first_byte": None,
        "args": args_dict(args),
        "phase_policy": {
            "phases": ["startup", "steady", "switch"],
            "startup": "t_submit → t_first_step(run 级;没有步记录)",
            "switch": "每个 epoch 边界起,至新窗口 K 块对应分片全部被训练侧打开;"
                      "未就绪截到 epoch 结束",
            "window_k": args.window_k,
            "window_shards": startup_window_shards,
            "chunk_bytes": args.chunk_bytes,
            "shard_bytes": args.shard_bytes,
            "baseline_steps": BASELINE_STEPS,
        },
        "switch_events": [],
        "n_steps_expected": args.epochs * args.steps_per_epoch,
        "n_steps_total": 0,
        "complete": False,
        "per_step_path": str(per_step_path),
        "note": "t_pod_ready/t_origin_first_byte 由 collect.py 从 K8s 与源站日志回填",
    }
    write_run_json(run_path, run)

    torch.manual_seed(args.seed)
    # compute 量级:单线程 1024² matmul ×8 ≈200ms/步,矩阵跨 epoch 复用
    torch.set_num_threads(1)
    n = COMPUTE_SIZE
    A = torch.randn(n, n)
    B = torch.randn(n, n)

    print(f"train负载(块读):dataset={blockread.DATASET} "
          f"endpoint={blockread.ENDPOINT}/{blockread.BUCKET} "
          f"cache_root={blockread.CACHE_ROOT} "
          f"n_tars={args.n_tars} epochs={args.epochs} "
          f"seed={args.seed} workers={args.workers} batch={args.batch} "
          f"steps_per_epoch={args.steps_per_epoch} warmup={warmup_total} "
          f"shuffle={args.shuffle} window_k={args.window_k}")
    print(f"输出:{per_step_path} / {meta_path} / {run_path}", flush=True)

    records = []
    per_epoch = []
    start_epoch = 0
    t_submit_run = t_submit
    if args.resume:
        state = read_checkpoint(args.checkpoint_dir)
        records = list(state.get("records") or [])
        per_epoch = list(state.get("per_epoch") or [])
        start_epoch = int(state.get("next_epoch") or 0)
        # 全程口径跨两段：t_submit 与 t_first_step 取第一段的值，
        # 中断到续跑跑出第一步的时长单独记在 run["resume"] 里。
        t_submit_run = state.get("t_submit") or t_submit
        run["t_submit"] = t_submit_run
        run["t_submit_source"] = "checkpoint-phase1"
        run["resumed"] = True
        run["switch_events"] = list(state.get("switch_events") or [])
        run["resume"] = {
            "checkpoint_next_epoch": start_epoch,
            "phase1_t_submit": state.get("t_submit"),
            "phase1_t_first_step": state.get("t_first_step"),
            "phase1_t_last_step": state.get("t_last_step"),
            "phase1_interrupt_at": state.get("interrupt_at"),
            "phase2_t_submit": t_submit,
            "phase2_t_first_step": None,
            "resume_submit_lag_s": None,
            "recovery_time_s": None,
        }
        print(f"[resume] 从 checkpoint 续跑：已完成 {len(records)} 步，"
              f"从 epoch {start_epoch} 接着跑", flush=True)
    if args.interrupt_after_epoch == 0:
        # 一个 epoch 都不跑就中断：写空 checkpoint 后退出（续跑段承担全部 epoch）
        state = checkpoint_state(records, per_epoch, 0, run, args, warmup_total)
        ckpt_path = write_checkpoint(args.checkpoint_dir, state)
        run["interrupted"] = True
        run["interrupt_reason"] = "interrupt-after-epoch-0"
        run["finished_at"] = utc_now()
        run["checkpoint_path"] = str(ckpt_path)
        write_run_json(run_path, run)
        print(f"[interrupt] 0 epoch 后中断，checkpoint → {ckpt_path}", flush=True)
        return
    t2 = None
    t_first_step = None
    t_first_step_run = (run["resume"]["phase1_t_first_step"]
                        if args.resume else None)
    t_last_step = None
    data_exhausted = False
    prev_stage = None
    interrupted = False
    # orders 心跳(10s 同代际续写);daemon 线程,中断路径随进程退出。
    blockread.start_heartbeat()
    with per_step_path.open("a" if args.resume else "w") as fout:
        for epoch in range(start_epoch, args.epochs):
            t_switch_wall = utc_now()  # 切换事件时刻 = 上一 epoch 结束/本 epoch 开始
            t_build = time.perf_counter()
            loader, urls, perm, stage, repeats = make_loader(
                args.n_tars, args.workers, args.batch, args.seed, epoch,
                args.epochs, args.shuffle, fractions, args.images_per_tar,
                args.steps_per_epoch)
            # 每 epoch 一份消费序订单(stage_id=epoch),先于数据读取下发预取。
            blockread.set_order(str(epoch),
                                [blockread.shard_name(u) for u in urls])
            it = iter(loader)
            loader_build_s = time.perf_counter() - t_build
            if t2 is None:
                t2 = time.perf_counter()  # epoch 0:时钟从 loader 建好后起(等首步)

            ws = window_shards(args.window_k, args.chunk_bytes, args.shard_bytes,
                               len(perm))
            switch = None
            switch_active = False
            switch_seen = set()
            if epoch > 0:
                kind = "epoch"
                if args.shuffle == "curriculum" and stage != prev_stage:
                    kind = "epoch+stage"
                switch = {
                    "kind": kind,
                    "from_epoch": epoch - 1,
                    "to_epoch": epoch,
                    "at": t_switch_wall,
                    "at_step": len(records),
                    "stage": stage,
                    "window_k": args.window_k,
                    "window_shards": ws,
                    "ready_at": None,
                    "ready_step": None,
                    "ready_source": "pending",
                    "censored": False,
                    **switch_baseline(records),
                }
                switch_active = args.window_k > 0
                if not switch_active:  # K=0:窗口立即就绪
                    switch.update({"ready_at": t_switch_wall,
                                   "ready_step": len(records),
                                   "ready_source": "k=0"})
            prev_stage = stage
            print(f"[epoch {epoch}] stage={stage} perm={perm} repeats={repeats} "
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
                    data_exhausted = True
                    break
                t1 = time.perf_counter()
                batch_wall = utc_now()
                if t_first_step is None:
                    # 4.3 启动与全程 makespan:batch 就绪后、forward 前
                    t_first_step = batch_wall
                    if t_first_step_run is None:
                        t_first_step_run = t_first_step
                for _ in range(COMPUTE_ITERS):
                    _ = A @ B             # 极小 compute 替身(量级见 COMPUTE_SIZE/COMPUTE_ITERS)
                t2 = time.perf_counter()
                t_last_step = utc_now()

                warmup = epoch == 0 and k < warmup_total
                phase = "steady"
                if switch_active:
                    phase = "switch"
                    switch_seen.update(batch["shards"])
                    if len(switch_seen) >= ws:
                        switch.update({"ready_at": batch_wall,
                                       "ready_step": len(records),
                                       "ready_source": "opened-shards"})
                        switch_active = False
                rec = {
                    "epoch": epoch,
                    "step_in_epoch": k,
                    "global_step": len(records),
                    "warmup": warmup,
                    "wait_s": round(t1 - t0, 6),
                    "step_s": round(t2 - t0, 6),
                    "shard": batch["shard"],
                    "shards": batch["shards"],
                    "phase": phase,
                    "stage": stage,
                }
                records.append(rec)
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                fout.flush()
                run["n_steps_total"] = len(records)
                run["t_first_step"] = t_first_step_run
                run["t_last_step"] = t_last_step
                if not warmup:
                    measured += 1
                if len(batch["jpg"]) != args.batch:
                    print(f"警告:epoch {epoch} 第 {k} 步 batch 不满"
                          f"({len(batch['jpg'])}/{args.batch})", file=sys.stderr)

            if switch is not None:
                switch["epoch_end_at"] = utc_now()
                switch["epoch_end_step"] = len(records) - 1 if records else None
                if switch["ready_step"] is None and switch["ready_source"] == "pending":
                    switch.update({"ready_source": "epoch-end", "censored": True})
                run["switch_events"].append(switch)

            ep = [r for r in records if r["epoch"] == epoch]
            meas = [r for r in ep if not r["warmup"]]
            meta = {
                "epoch": epoch,
                "epoch_seed": args.seed + epoch,
                "sample_shuffle_seed": (
                    args.seed if args.shuffle in ("fixed", "curriculum")
                    else args.seed + epoch),
                "stage": stage,
                "shard_perm": perm,
                "urls_repeats": repeats,
                "loader_build_s": round(loader_build_s, 6),
                "steps": len(meas),
                "warmup_steps": len(ep) - len(meas),
                "phase_counts": {
                    "steady": sum(1 for r in ep if r["phase"] == "steady"),
                    "switch": sum(1 for r in ep if r["phase"] == "switch"),
                },
                "switch": switch,
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
            run["finished_at"] = utc_now()
            write_run_json(run_path, run)

            del it
            del loader  # 释放本 epoch worker,避免与下一 epoch 的 worker 叠加
            if data_exhausted:
                break

            if 0 <= args.interrupt_after_epoch <= epoch + 1:
                # 跑够 --interrupt-after-epoch 个 epoch 就写 checkpoint 退出。
                # checkpoint 落在 --checkpoint-dir，续跑段用 --resume 接上。
                state = checkpoint_state(records, per_epoch, epoch + 1, run,
                                         args, warmup_total)
                ckpt_path = write_checkpoint(args.checkpoint_dir, state)
                run["interrupted"] = True
                run["interrupt_reason"] = f"interrupt-after-epoch-{args.interrupt_after_epoch}"
                run["finished_at"] = utc_now()
                run["checkpoint_path"] = str(ckpt_path)
                run["n_steps_total"] = len(records)
                run["complete"] = False
                write_run_json(run_path, run)
                print(f"[interrupt] 跑完 {epoch + 1} 个 epoch 后中断，"
                      f"checkpoint → {ckpt_path}", flush=True)
                return

    blockread.stop_heartbeat()
    run["finished_at"] = utc_now()
    if args.resume:
        # 全程口径：首步取第一段的，末步取本段的；续跑段的首步另记在 run["resume"]。
        run["t_first_step_phase2"] = t_first_step
        run["t_first_step"] = t_first_step_run or t_first_step
        run["resume"]["phase2_t_first_step"] = t_first_step
        run["resume"]["resume_submit_lag_s"] = ts_delta_s(
            run["resume"]["phase2_t_submit"], run["resume"]["phase1_t_last_step"])
        run["resume"]["recovery_time_s"] = ts_delta_s(
            t_first_step, run["resume"]["phase1_t_last_step"])
    else:
        run["t_first_step"] = t_first_step
    run["t_last_step"] = t_last_step
    run["n_steps_total"] = len(records)
    run["complete"] = (len(records) == args.epochs * args.steps_per_epoch
                       and not data_exhausted)
    if not run["complete"]:
        run["incomplete_reason"] = "data-exhausted" if data_exhausted else "short-run"
    write_run_json(run_path, run)

    meds = [m["median_stall"] for m in per_epoch if m["median_stall"] is not None]
    print(f"train {args.epochs} epoch w{args.workers}:中位 stall "
          + " / ".join(f"{m:.1%}" for m in meds)
          + f" → {per_step_path}")


if __name__ == "__main__":
    main()
