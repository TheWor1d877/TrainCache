#!/usr/bin/env python3
"""bench 唯一实验入口：九个族 + status。

族 ID 同时是子命令名、COND 的 family 取值与产物目录名（results/<族>/<格子>/）。
一个格子由一个 COND 文件构成，格名即 COND 名；--rho、--rate、--group 与族专用选项
按 COND 字段过滤，没给就选该族全部格子。

执行一律走子进程，本文件不直接碰集群：
  compare        → compare/run_bench.py
  multiconsumer  → compare/run_bench.py（readers>1 时由其分派进 gateway_multi.py）
  chaos          → chaos/run.sh
  其余六族      → experiments/run.py（由它按 load 字段选执行体）

--dry-run 只打印计划（格名、COND 名、数据字节、缓存字节、rate、reps），不拉子进程、
不碰集群。status 扫产物根与 COND 目录，按族打印完成格子数与总格子数。
"""
import argparse
import csv
import json
import os
import re
import subprocess
import sys
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent
EXPERIMENTS_DIR = BENCH_ROOT / "experiments"
sys.path.insert(0, str(BENCH_ROOT))
sys.path.insert(0, str(EXPERIMENTS_DIR))
import cond as cond_mod  # noqa: E402
import config as bench_config  # noqa: E402

CONDS_DIR_DEFAULT = EXPERIMENTS_DIR / "conds"
RUN_BENCH = BENCH_ROOT / "compare" / "run_bench.py"
CHAOS_RUN = BENCH_ROOT / "chaos" / "run.sh"
RUN_CONDS = EXPERIMENTS_DIR / "run.py"

FAMILIES = bench_config.BENCH_FAMILIES
GATEWAY_ARM_RE = re.compile(r"^gateway-(g\d+)-")
MULTI_HOT_RE = re.compile(r"^multi-hot(\d+)-")


def _field(*keys):
    def get(cond):
        cur = cond
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur
    return get


def _name_tail(prefix):
    """COND 名里的场景/故障段：switch-per-epoch-rho8 → per-epoch。"""
    def get(cond):
        name = cond.get("name", "")
        if not name.startswith(prefix):
            return None
        return name[len(prefix):].rsplit("-rho", 1)[0]
    return get


def _gateway_arm(cond):
    match = GATEWAY_ARM_RE.match(cond.get("name", ""))
    return match.group(1) if match else None


def _window(cond):
    """预热窗口：gate=free 时 cache.warmup_seconds 写 0（窗口不生效），
    名义窗口记在 tags 的 window=NN 里；两张表都读，free 的三格才分得开。"""
    for tag in cond.get("tags") or []:
        if isinstance(tag, str) and tag.startswith("window="):
            try:
                return int(tag.split("=", 1)[1])
            except ValueError:
                return None
    return (cond.get("cache") or {}).get("warmup_seconds")


def _hot_pct(cond):
    """热比例按百分比整数比：0.10 → 10。"""
    match = MULTI_HOT_RE.match(cond.get("name", ""))
    return int(match.group(1)) if match else None


def _arm_tag(cond):
    """并行下载族的臂：tags 里的 arm=par-on / arm=par-off。"""
    for tag in cond.get("tags") or []:
        if isinstance(tag, str) and tag.startswith("arm="):
            return tag.split("=", 1)[1]
    return None


# 族专用选项。chaos 的故障名在文档里带下划线，COND 名里是 kebab，两边都收。
FAMILY_OPTIONS = {
    "compare": ("group",),
    "gateway": ("gateway",),
    "evict": ("evict_policy",),
    "prefetch": ("lookahead", "fanout"),
    "warmup": ("gate", "window"),
    "switch": ("scenario",),
    "chaos": ("fault",),
    "multiconsumer": ("hot_ratio",),
    "e2e": ("scenario",),
    "rangepar": ("arm",),
}

OPTION_GETTERS = {
    "group": _field("group"),
    "gateway": _gateway_arm,
    "evict_policy": _field("cache", "evict_policy"),
    "lookahead": _field("prefetch", "lookahead"),
    "fanout": _field("prefetch", "fanout"),
    "gate": _field("prefetch", "gate", "mode"),
    "window": _window,
    "scenario": lambda c: (_name_tail("switch-")(c) if c.get("family") == "switch"
                           else _name_tail("e2e-")(c)),
    "fault": _name_tail("chaos-"),
    "hot_ratio": _hot_pct,
    "arm": _arm_tag,
}

OPTION_TYPES = {
    "group": str, "gateway": str, "evict_policy": str,
    "lookahead": int, "fanout": int, "gate": str, "window": int,
    "scenario": str, "fault": str, "hot_ratio": float, "arm": str,
}


# 命令行写法 → COND 的规范写法。COND 一侧由 OPTION_GETTERS 直接给规范值，
# 只在这里把用户输入对齐一次，避免同一套归一规则作用两遍。
OPTION_QUERY = {
    "fault": lambda v: str(v).replace("_", "-"),
    "gateway": lambda v: str(v).strip().lower(),
    "hot_ratio": lambda v: int(round(float(v) * 100)),
    "arm": lambda v: str(v).strip().lower(),
}


def load_conds(conds_dir: Path) -> list[dict]:
    """读目录下全部 v4 COND；非 v4 的旧文件跳过（重生成期间目录里会有旧点）。"""
    out: list[dict] = []
    for path in sorted(Path(conds_dir).glob("*.json")):
        try:
            cond = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f"COND 读不了: {path}: {exc}")
        if not isinstance(cond, dict):
            raise SystemExit(f"COND 顶层不是对象: {path}")
        if cond.get("schema_version") != cond_mod.SCHEMA_VERSION:
            continue
        out.append(cond)
    return out


def select(conds: list[dict], family: str, args) -> list[dict]:
    """该族里命中全部筛选条件的格子。"""
    return [c for c in conds
            if c.get("family") == family and _matches(c, args)]


def _matches(cond: dict, args) -> bool:
    if getattr(args, "rho", None) is not None:
        rho = (cond.get("cache") or {}).get("rho")
        if rho is None or float(rho) != float(args.rho):
            return False
    if getattr(args, "rate", None) is not None:
        if int((cond.get("network") or {}).get("rate_mbit", -1)) != int(args.rate):
            return False
    for option in FAMILY_OPTIONS.get(args.command, ()):
        given = getattr(args, option, None)
        if given is None:
            continue
        want = OPTION_QUERY.get(option, lambda v: v)(given)
        if OPTION_GETTERS[option](cond) != want:
            return False
    return True


def reps_of(cond: dict, override: int | None) -> int:
    if override is not None:
        return int(override)
    return int(cond["sampling"]["reps"])


def grid_dir(cond: dict) -> Path:
    return bench_config.family_results_dir(cond["family"]) / cond["name"]


# ---------------------------------------------------------------------------
# 计划
# ---------------------------------------------------------------------------
def plan_line(cond: dict, reps: int) -> str:
    return (f"[plan] {cond['family']}/{cond['name']}"
            f"  cond={cond['name']}"
            f"  dataset={cond_mod.dataset_bytes(cond)}"
            f"  cache={cond_mod.cache_bytes(cond)}"
            f"  rate={cond['network']['rate_mbit']}"
            f"  reps={reps}")


# ---------------------------------------------------------------------------
# 执行
# ---------------------------------------------------------------------------
def _rate_args(cond: dict) -> list[str]:
    return ["--rate", f"{cond['network']['rate_mbit']}mbit"]


def _rho_args(cond: dict) -> list[str]:
    cache = cond.get("cache") or {}
    if not cache.get("enabled"):
        return []
    return ["--rho", str(cache["rho"])]


def _run_bench_cmd(cond: dict, reps: int) -> list[str]:
    """run_bench 的调用面：rho/rate/group/reps + 族产物根。

    compare 的格子名由 run_bench 按 COND 规则拼（docs/4.2 的 COND 契约一节）；非 compare 族没有
    族专用 CLI 轴，用 --cell 把 COND 名钉成格子名、--family 决定 summary 的 family 列。
    """
    family = cond["family"]
    cmd = [sys.executable, str(RUN_BENCH),
           *_rho_args(cond), *_rate_args(cond),
           "--group", cond["group"],
           "--reps", str(reps),
           "--out-root", str(bench_config.family_results_dir(family))]
    if family != "compare":
        cmd += ["--cell", cond["name"], "--family", family]
    return cmd


def _chaos_cmd(cond: dict) -> list[str]:
    """COND 名是 kebab，chaos/run.sh 的故障名是 snake。"""
    fault = cond["name"].split("-", 1)[1].rsplit("-rho", 1)[0].replace("-", "_")
    return ["bash", str(CHAOS_RUN), "--fault", fault,
            "--data-root", str(grid_dir(cond))]


def _multi_env(cond: dict) -> dict[str, str]:
    """多消费者族：读端数 2，反亲和按 COND 名里的放置方式定。"""
    env = cond_mod.cond_to_env(cond)
    env["TRAINCACHE_READERS"] = "2"
    env["TRAINCACHE_ANTI_AFFINITY"] = "0" if cond["name"].endswith("colocated") else "1"
    return env


def command_for(cond: dict, reps: int, conds_dir: Path) -> tuple[list[str], dict | None]:
    """一个格子 → (命令, 额外环境)。环境为 None 表示继承当前进程。"""
    family = cond["family"]
    if family == "chaos":
        return _chaos_cmd(cond), cond_mod.cond_to_env(cond)
    if family == "compare":
        return _run_bench_cmd(cond, reps), None
    if family == "multiconsumer":
        return _run_bench_cmd(cond, reps), _multi_env(cond)
    return ([sys.executable, str(RUN_CONDS),
             "--cond", cond["name"],
             "--reps", str(reps),
             "--conds-dir", str(conds_dir)], None)


def execute(cond: dict, reps: int, conds_dir: Path) -> bool:
    cmd, extra = command_for(cond, reps, conds_dir)
    env = os.environ.copy()
    if extra:
        env.update(extra)
    print(f"[run] {cond['family']}/{cond['name']} reps={reps}", flush=True)
    rc = subprocess.call(cmd, env=env, cwd=str(bench_config.REPO_ROOT))
    if rc != 0:
        print(f"[fail] {cond['family']}/{cond['name']} rc={rc}", file=sys.stderr)
        return False
    return True


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
def _done_rows(path: Path) -> int:
    if not path.is_file():
        return 0
    try:
        with path.open(newline="", encoding="utf-8") as f:
            return sum(1 for row in csv.DictReader(f)
                       if str(row.get("void", "")).strip().lower() not in ("true", "1"))
    except (OSError, csv.Error):
        return 0


def grid_done(cond: dict, reps: int) -> bool:
    """完成 = 该格的有效采样数够 reps。

    compare 等族把逐次采样写进格子根目录的 summary.csv；run_policy 与混沌族
    按轮落到格子下的子目录，一次采样一个子目录，子目录里分别是 summary.csv
    与 summary.txt。
    """
    root = grid_dir(cond)
    if _done_rows(root / "summary.csv") >= reps:
        return True
    rounds = [p for p in root.glob("*/") if p.is_dir()]
    csv_rows = sum(_done_rows(p / "summary.csv") for p in rounds)
    txt_rounds = sum(1 for p in rounds if (p / "summary.txt").is_file())
    return max(csv_rows, txt_rounds) >= reps


def cmd_status(conds_dir: Path) -> None:
    conds = load_conds(conds_dir)
    print(f"results: {bench_config.RESULTS_DIR}")
    print(f"conds:   {conds_dir}")
    total_done = total_all = 0
    for family in FAMILIES:
        grids = [c for c in conds if c.get("family") == family]
        done = sum(1 for c in grids if grid_done(c, int(c["sampling"]["reps"])))
        total_done += done
        total_all += len(grids)
        print(f"{family:14s} {done}/{len(grids)}")
    print(f"{'合计':12s} {total_done}/{total_all}")


def cmd_family(family: str, args) -> None:
    conds_dir = Path(args.conds_dir) if args.conds_dir else CONDS_DIR_DEFAULT
    picked = select(load_conds(conds_dir), family, args)
    if not picked:
        raise SystemExit(f"{family}: 没有匹配的格子（conds-dir={conds_dir}）")
    force = getattr(args, "force", False)
    if args.dry_run:
        for cond in picked:
            reps = reps_of(cond, args.reps)
            tag = "  （已完成，将跳过）" if not force and grid_done(cond, reps) else ""
            print(plan_line(cond, reps) + tag)
        return
    failed = []
    skipped = 0
    for cond in picked:
        reps = reps_of(cond, args.reps)
        if not force and grid_done(cond, reps):
            print(f"[skip] {cond['family']}/{cond['name']} 有效采样≥{reps}，已完成",
                  flush=True)
            skipped += 1
            continue
        if not execute(cond, reps, conds_dir):
            failed.append(cond["name"])
    if skipped:
        print(f"[skip] 共跳过 {skipped} 个已完成格（--force 可强制重跑）", flush=True)
    if failed:
        raise SystemExit(f"失败 {len(failed)} 格: {' '.join(failed)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--rho", type=float, default=None,
                        help="按 cache.rho 过滤（0.5/1/2/4/8）")
    parser.add_argument("--rate", type=int, default=None,
                        help="按 network.rate_mbit 过滤（100/300/1000）")
    parser.add_argument("--reps", type=int, default=None,
                        help="覆盖 COND 的 sampling.reps（缺省用 COND 值）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印计划表，不拉子进程、不碰集群")
    parser.add_argument("--force", action="store_true",
                        help="已完成格也重跑（默认按 summary.csv 有效行数跳过）")
    parser.add_argument("--conds-dir", type=Path, default=None,
                        help="COND 目录（默认 bench/experiments/conds）")


def _add_family_options(parser: argparse.ArgumentParser, family: str) -> None:
    for option in FAMILY_OPTIONS[family]:
        parser.add_argument(f"--{option.replace('_', '-')}",
                            dest=option, type=OPTION_TYPES[option], default=None,
                            help=f"按 COND 的 {option} 过滤")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="run.py", description="bench 唯一实验入口：十个族 + status")
    sub = ap.add_subparsers(dest="command", required=True)
    for family in FAMILIES:
        parser = sub.add_parser(family, help=f"{family} 族")
        _add_common(parser)
        _add_family_options(parser, family)
    parser = sub.add_parser("status", help="各族完成格子数")
    parser.add_argument("--conds-dir", type=Path, default=None)
    return ap


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.command == "status":
        cmd_status(Path(args.conds_dir) if args.conds_dir else CONDS_DIR_DEFAULT)
        return
    cmd_family(args.command, args)


if __name__ == "__main__":
    main()
