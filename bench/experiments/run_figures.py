#!/usr/bin/env python3
"""按 COND 执行图一/图二/图三/Pareto 一的对照层跑分。

每个 COND 对应一个 (figure, 档位, 组态) 点，执行器把它翻译成 run_bench.py
的环境变量与命令行参数。产物落 results/m8/<COND>/summary.csv，runs/ 下的
逐 step 记录与 recovery.json 由 run_bench.py 自己写。

与 M7 的区别：图系列不按 S/M/L 的 D30 组态白名单过滤，reps 由 COND 的
sampling.reps 决定；run_bench.py 用 TRAINCACHE_FIGURE_MODE=1 关掉这些约束。

用法:
  python3 bench/experiments/run_figures.py --list
  python3 bench/experiments/run_figures.py --cond fig1-100-ratio2-fan2-gated
  python3 bench/experiments/run_figures.py --figure fig1 --dry-run
  python3 bench/experiments/run_figures.py --figure all
"""
import argparse
import fcntl
import json
import os
import subprocess
import sys
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent
BENCH_ROOT = EXPERIMENTS_DIR.parent
sys.path.insert(0, str(EXPERIMENTS_DIR))
sys.path.insert(0, str(BENCH_ROOT))
import cond as cond_mod  # noqa: E402
import config as bench_config  # noqa: E402

CONDS_DIR = EXPERIMENTS_DIR / "conds"
RUNNER = BENCH_ROOT / "compare" / "run_bench.py"
OUT_ROOT = bench_config.RESULTS_DIR / "m8"
LOCK_FILE = Path("/tmp/traincache-figures.lock")
FIGURES = ("fig1", "fig2", "fig3", "pareto1")


def cond_env(c: dict) -> dict:
    """COND → run_bench 环境变量。"""
    ds = c["dataset"]
    wl = c["workload"]
    net = c["network"]
    cache = c["cache"]
    pf = c["prefetch"]
    tier = "S" if int(ds["shards"]) <= 8 else "M"
    env = os.environ.copy()
    env.update({
        "TRAINCACHE_FIGURE_MODE": "1",
        "TRAINCACHE_COND_NAME": c["name"],
        "CAPACITY_RATIO": tier,
        "N_TARS": str(ds["shards"]),
        "EPOCHS": str(wl["epochs"]),
        "STEPS_PER_EPOCH": str(wl["steps_per_epoch"]),
        "BATCH": str(wl["batch"]),
        "SHUFFLE": wl["shuffle"],
        "SEED": str(ds["seed"]),
        "DATASET_NAME": ds["name"],
        "MINIO_BUCKET": ds["bucket"],
        "TRAINCACHE_CACHE_CAPACITY_BYTES": str(cache["capacity_bytes"]),
        "TRAINCACHE_EVICT_POLICY": cache["evict_policy"],
        "TRAINCACHE_PREHEAT_FANOUT": str(pf["fanout"]),
        "TRAINCACHE_PREHEAT_FANOUT_MIN": str(pf["fanout_min"]),
        "TRAINCACHE_PREFETCH_K": str(pf["lookahead"]),
        "TRAINCACHE_GATE_MODE": pf["gate"]["mode"],
        "TC_RATE": f"{net['rate_mbit']}mbit",
        "TC_DELAY": f"{net['delay_ms']}ms",
    })
    return env


def cond_cmd(c: dict, out: Path) -> list[str]:
    tier = "S" if int(c["dataset"]["shards"]) <= 8 else "M"
    return [sys.executable, str(RUNNER),
            "--tier", tier,
            "--rate", f"{c['network']['rate_mbit']}mbit",
            "--groups", c["group"],
            "--reps", str(c["sampling"]["reps"]),
            "--out", str(out)]


def load_conds(selectors: list[str], figures: list[str]) -> list[dict]:
    out = []
    for p in sorted(CONDS_DIR.glob("*.json")):
        c = json.loads(p.read_text())
        if c.get("figure") not in FIGURES:
            continue
        if figures and c.get("figure") not in figures:
            continue
        if selectors and c["name"] not in selectors:
            continue
        out.append(c)
    return out


def done(c: dict, out: Path) -> bool:
    if not out.is_file():
        return False
    import csv
    with out.open(newline="") as f:
        rows = [r for r in csv.DictReader(f)
                if str(r.get("void", "")).lower() != "true"]
    return len(rows) >= int(c["sampling"]["reps"])


def main():
    ap = argparse.ArgumentParser(description="图系列 COND 执行器")
    ap.add_argument("--cond", nargs="+", default=None, help="只跑指定 COND 名")
    ap.add_argument("--figure", nargs="+", default=None,
                    choices=list(FIGURES) + ["all"], help="按图筛选")
    ap.add_argument("--list", action="store_true", help="只列出 COND,不执行")
    ap.add_argument("--dry-run", action="store_true", help="打印命令,不执行")
    ap.add_argument("--force", action="store_true", help="忽略已完成标记")
    args = ap.parse_args()

    figures = [] if (not args.figure or "all" in args.figure) else args.figure
    conds = load_conds(args.cond or [], figures)
    if not conds:
        raise SystemExit("没有匹配的 COND")
    if args.list:
        for c in conds:
            print(f"{c['figure']:8s} {c['name']:42s} group={c['group']:16s} "
                  f"reps={c['sampling']['reps']}")
        return

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    lock = LOCK_FILE.open("w")
    if not args.dry_run:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"已有图系列实例在跑（锁 {LOCK_FILE}）")

    failed = []
    for c in conds:
        out = OUT_ROOT / c["name"] / "summary.csv"
        if not args.force and done(c, out):
            print(f"[skip] {c['name']} 已完成")
            continue
        out.parent.mkdir(parents=True, exist_ok=True)
        cmd = cond_cmd(c, out)
        print(f"[run] {c['name']}  ({len(cmd)} args)")
        if args.dry_run:
            print("      " + " ".join(cmd))
            continue
        rc = subprocess.call(cmd, env=cond_env(c), cwd=str(BENCH_ROOT.parent))
        if rc != 0:
            print(f"[fail] {c['name']} rc={rc}")
            failed.append(c["name"])
    if failed:
        raise SystemExit(f"失败 {len(failed)} 个: {' '.join(failed)}")
    print(f"[done] {len(conds)} 个 COND")


if __name__ == "__main__":
    main()
