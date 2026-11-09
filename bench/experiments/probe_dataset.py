#!/usr/bin/env python3
"""dataset 形态跑一遍 COND,可挂节点内探针。

跑一遍 compare 族的 dataset COND(集群内跑分,run_bench.py 出 summary.csv),
带 --probe 时另起一个 watcher 盯测量 Pod:Pod 起来就挂探针,Pod 结束 SIGINT
收尾,产物落 <格子>/probe/rep-NN/。同一 COND 跑两遍(一遍带 --probe,一遍不带),
两遍格子目录给 probe_overhead.py --form dataset 对账。

产物根在数据层(repos 外),repo 内只留这个脚本。
"""
import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent
BENCH_ROOT = EXPERIMENTS_DIR.parent
sys.path.insert(0, str(EXPERIMENTS_DIR))
sys.path.insert(0, str(BENCH_ROOT))
import cond as cond_mod  # noqa: E402
import config as bench_config  # noqa: E402
import probe as probe_mod  # noqa: E402

RUN_BENCH = BENCH_ROOT / "compare" / "run_bench.py"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="dataset 形态跑一遍 COND(可挂节点内探针)")
    ap.add_argument("--cond", required=True, help="COND 名(conds/<name>.json)")
    ap.add_argument("--reps", type=int, default=1, help="重复次数")
    ap.add_argument("--out-root", required=True, help="产物根(格子目录落它下面)")
    ap.add_argument("--cell", default=None, help="格子名(缺省取 COND 名)")
    ap.add_argument("--probe", action="store_true", help="挂节点内探针")
    ap.add_argument("--probe-out", default=None, help="探针产物落点(缺省 <格子>/probe)")
    ap.add_argument("--ns", default="traincache", help="测量 Pod 所在命名空间")
    ap.add_argument("--pod", default="train-job-0", help="测量 Pod 名")
    ap.add_argument("--probe-bin", default=str(probe_mod.DEFAULT_BIN),
                    help="traintrace 二进制")
    ap.add_argument("--timeout", type=float, default=5400.0, help="watcher 总超时秒数")
    args = ap.parse_args()

    cond = cond_mod.load_cond(args.cond)
    cell = args.cell or cond["name"]
    cell_dir = Path(args.out_root).resolve() / cell
    env = os.environ.copy()
    env.update(cond_mod.cond_to_env(cond))

    cmd = [sys.executable, str(RUN_BENCH)]
    cache = cond.get("cache") or {}
    if cache.get("enabled"):
        cmd += ["--rho", str(cache["rho"])]
    cmd += ["--rate", f"{cond['network']['rate_mbit']}mbit",
            "--group", cond["group"],
            "--reps", str(args.reps),
            "--out-root", str(Path(args.out_root).resolve()),
            "--cell", cell,
            "--family", cond["family"]]

    watcher = None
    if args.probe:
        probe_out = Path(args.probe_out) if args.probe_out else cell_dir / "probe"
        probe_out.mkdir(parents=True, exist_ok=True)
        watcher = threading.Thread(
            target=probe_mod.watch_pod,
            args=(args.ns, args.pod, probe_out, Path(args.probe_bin),
                  args.reps, args.timeout),
            daemon=True)
        watcher.start()
        time.sleep(2)  # watcher 先盯上,免得 Pod 起来得太快错过

    print(f"[probe_dataset] 跑 {cond['name']} reps={args.reps} cell={cell_dir}")
    rc = subprocess.run(cmd, env=env).returncode
    if watcher is not None:
        watcher.join(timeout=120)
    print(f"[probe_dataset] run_bench rc={rc} 格子目录 {cell_dir}")
    sys.exit(rc)


if __name__ == "__main__":
    main()
