#!/usr/bin/env python3
"""按带宽降序重跑 compare 族的 traincache 组态，产物落独立结果树。

--group 按 COND 的 group 字段前缀过滤:traincache 冷热两组都跑,
traincache-cold 与 traincache-warm 各跑一组。同档内按 rho 升序,
先跑高带宽,便于在长时间重跑里尽早看到快档的结论。

产物默认落 <数据层>/bench/results/compare。完成判据同 bench/run.py:summary.csv 有效行数
(void 列非真)≥ reps,中断后重启自动跳过已完成格。
"""
import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BENCH_ROOT))
import config as bench_config  # noqa: E402

CONDS = BENCH_ROOT / "experiments" / "conds"
RUN_BENCH = BENCH_ROOT / "compare" / "run_bench.py"
DEFAULT_OUT = bench_config.RESULTS_DIR / "compare"


def done_rows(path: Path) -> int:
    if not path.is_file():
        return 0
    try:
        with path.open(newline="", encoding="utf-8") as f:
            return sum(1 for row in csv.DictReader(f)
                       if str(row.get("void", "")).strip().lower() not in ("true", "1"))
    except (OSError, csv.Error):
        return 0


def select_conds(group: str) -> list[dict]:
    conds = [json.loads(p.read_text())
             for p in sorted(CONDS.glob("compare-*.json"))]
    conds = [c for c in conds if c["group"].startswith(group)]
    conds.sort(key=lambda c: (-int(c["network"]["rate_mbit"]), float(c["cache"]["rho"])))
    return conds


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--group", default="traincache",
                    help="按 COND 的 group 前缀过滤(默认 traincache = 冷热两组)")
    ap.add_argument("--out-root", type=Path, default=DEFAULT_OUT,
                    help=f"产物根(默认 {DEFAULT_OUT})")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印顺序与完成情况,不拉子进程")
    args = ap.parse_args()

    conds = select_conds(args.group)
    if not conds:
        print(f"[empty] 没有 group 前缀为 {args.group} 的 compare 格", file=sys.stderr)
        return 1
    out_root = args.out_root
    print(f"[out] {out_root}", flush=True)
    print("[order] " + " → ".join(
        c["name"].replace("compare-", "") for c in conds), flush=True)

    failed = []
    skipped = 0
    for c in conds:
        name, group = c["name"], c["group"]
        rho = c["cache"]["rho"]
        rate = c["network"]["rate_mbit"]
        reps = int(c["sampling"]["reps"])
        if done_rows(out_root / name / "summary.csv") >= reps:
            print(f"[skip] {name} 有效采样≥{reps}，已完成", flush=True)
            skipped += 1
            continue
        if args.dry_run:
            print(f"[plan] {name} rho={rho} rate={rate} reps={reps}", flush=True)
            continue
        cmd = [sys.executable, str(RUN_BENCH),
               "--rho", str(rho), "--rate", f"{rate}mbit",
               "--group", group, "--reps", str(reps),
               "--out-root", str(out_root)]
        print(f"[run] {name} reps={reps}", flush=True)
        rc = subprocess.call(cmd, cwd=str(BENCH_ROOT.parent))
        if rc != 0:
            print(f"[fail] {name} rc={rc}", file=sys.stderr, flush=True)
            failed.append(name)

    print(f"[done] 跳过 {skipped}，失败 {len(failed)}", flush=True)
    if failed:
        print("失败格: " + " ".join(failed), file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
