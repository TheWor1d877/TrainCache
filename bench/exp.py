#!/usr/bin/env python3
"""bench 实验统一入口:status / run / record。

产物落数据层的五个分区(scores / baselines / summary / figures / policy),分区由
`results_root()` 按族给出;点目录名一律是完整 COND 名(compare-groups 用格名)。
每族跑完按产物回填 docs/4.6 实验版本记录.md 的最后一行。
"""
import argparse
import csv
import fcntl
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent
EXPERIMENTS_DIR = BENCH_ROOT / "experiments"
sys.path.insert(0, str(EXPERIMENTS_DIR))
sys.path.insert(0, str(BENCH_ROOT))
import cond as cond_mod  # noqa: E402
import config as bench_config  # noqa: E402
import lock as lock_mod  # noqa: E402

CONDS_DIR = EXPERIMENTS_DIR / "conds"
RUNNER = BENCH_ROOT / "compare" / "run_bench.py"
REGISTRY = bench_config.REPO_ROOT / "docs" / "4.6 实验版本记录.md"
LOCK_FILE = Path("/tmp/traincache-exp.lock")

COMPARE_FAMILY = "compare-groups"
COND_FAMILIES = ("startup-surface", "total-makespan", "switch-recovery",
                 "pareto-startup")
TILT_FAMILIES = ("pareto-sustained", "evict-policy")
FAMILIES = (COMPARE_FAMILY,) + COND_FAMILIES + TILT_FAMILIES

# 族 ID 是 `--family` 与 `--point` 的寻址名，产物目录名用的是数据层分区 +
# 完整 COND 名（4.4 §C1），两套名字不同词（evict-policy 族的 COND 叫 policy-*，
# pareto-sustained 族叫 pareto2-*）。认领 COND、还原点名与算目录都以这张表为准；
# 漏了这张表，一族会认领不到任何 COND，点目录名还会退化成空串，
# 三个点写进同一个分区。
FAMILY_COND_PREFIX = {
    "startup-surface": "fig1",
    "total-makespan": "fig2",
    "switch-recovery": "fig3",
    "pareto-startup": "pareto1",
    "pareto-sustained": "pareto2",
    "evict-policy": "policy",
}


def cond_prefix(family: str) -> str:
    return FAMILY_COND_PREFIX.get(family, family)


def results_root(family: str) -> Path:
    """族 → 数据层分区，与 run_figures.py / run_policy.py / summarize.py 同一张表。

    图系列的点目录名带 `fig1-` 这类 COND 前缀，`plot.py` 靠它把曲面样本从五组对照
    里分出去；倾斜两族的每个 rep 独占一个 run 目录，产物不能并进一个 summary.csv。
    """
    if family == COMPARE_FAMILY:
        return bench_config.SCORES_RESULTS_DIR
    if family in TILT_FAMILIES:
        return bench_config.POLICY_RESULTS_DIR
    return bench_config.FIGURES_RESULTS_DIR


def point_of(family: str, cond: dict) -> str:
    """点名 = COND 名去掉该族的 COND 前缀（4.3 §5 的 `--point` 写法）。"""
    return cond["name"][len(cond_prefix(family)) + 1:]


def point_base(family: str, point: str) -> Path:
    """点目录 = 分区目录 + 完整 COND 名（点名加回前缀还原）。"""
    return results_root(family) / f"{cond_prefix(family)}-{point}"

COMPARE_CELLS = (("S-100", "S", 100), ("S-300", "S", 300),
                 ("S-1000", "S", 1000), ("M-1000", "M", 1000),
                 ("L-1000", "L", 1000))
GROUP_ORDER = ("direct", "juicefs-cold", "juicefs-warm", "fluid-cold",
               "fluid-warm", "traincache-cold", "traincache-warm")
S300_GROUPS = ("direct", "fluid-cold", "traincache-cold")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="microseconds").replace("+00:00", "Z")


def _valid_rows(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as f:
            return [r for r in csv.DictReader(f)
                    if str(r.get("void", "")).strip().lower() not in ("true", "1")]
    except (OSError, csv.Error):
        return []


def _tier_of(cond: dict) -> str:
    return "S" if int(cond["dataset"]["shards"]) <= 8 else "M"


def _run_tier(cond: dict, tilt: bool) -> str:
    """倾斜族沿用 M5 口径固定 --tier S;图系列的档位随分片数。"""
    return "S" if tilt else _tier_of(cond)


# ---------------------------------------------------------------------------
# 点清单
# ---------------------------------------------------------------------------
def _cond_families() -> dict[str, list[dict]]:
    out = {f: [] for f in COND_FAMILIES + TILT_FAMILIES}
    for path in sorted(CONDS_DIR.glob("*.json")):
        for family in out:
            if path.stem.startswith(f"{cond_prefix(family)}-"):
                out[family].append(cond_mod.load_cond(path.stem, CONDS_DIR))
                break
    return out


def _compare_groups(tier: str, cell: str) -> list[str]:
    if cell == "S-300":
        return list(S300_GROUPS)
    allowed = cond_mod.CAPACITY_TIER_GROUPS[tier]
    return [g for g in GROUP_ORDER if g in allowed]


def _compare_run_groups(tier: str, cell: str) -> list[str]:
    """exp.py 只跑 TrainCache 组;对照行来自只读的 baselines。"""
    return [g for g in _compare_groups(tier, cell)
            if g in cond_mod.TRAINCACHE_GROUPS]


# ---------------------------------------------------------------------------
# 完成度(完成 = 行数够 + 产物版本等于当前版本行)
# ---------------------------------------------------------------------------
def _stamp(base: Path) -> str | None:
    path = base / "version"
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8").strip()


def _write_stamp(base: Path, version: str) -> None:
    (base / "version").write_text(version + "\n", encoding="utf-8")


def compare_cell_done(cell: str, tier: str,
                      version: str | None) -> tuple[int, int]:
    """(已完成组数, 应跑组数)。TrainCache 行按版本,对照行取 baselines。"""
    groups = _compare_groups(tier, cell)
    reps = cond_mod.CAPACITY_TIER_REPS[tier]
    tc_rows = _valid_rows(results_root(COMPARE_FAMILY) / cell / "summary.csv")
    base_rows = _valid_rows(bench_config.BASELINE_RESULTS_DIR / cell / "summary.csv")
    tc_counts: dict[str, int] = {}
    for row in tc_rows:
        group = row.get("group", "")
        tc_counts[group] = tc_counts.get(group, 0) + 1
    base_counts: dict[str, int] = {}
    for row in base_rows:
        group = row.get("group", "")
        base_counts[group] = base_counts.get(group, 0) + 1
    stamped = _stamp(results_root(COMPARE_FAMILY) / cell) == version
    done = 0
    for group in groups:
        if group in cond_mod.TRAINCACHE_GROUPS:
            ok = stamped and tc_counts.get(group, 0) >= reps
        else:
            ok = base_counts.get(group, 0) >= reps
        done += bool(ok)
    return done, len(groups)


def point_done(family: str, point: str, reps: int,
               version: str | None) -> bool:
    base = point_base(family, point)
    if family in TILT_FAMILIES:
        return _tilt_done(base, version) >= reps
    return _stamp(base) == version and \
        len(_valid_rows(base / "summary.csv")) >= reps


def _tilt_done(base: Path, version: str | None = None) -> int:
    if not base.is_dir():
        return 0
    return sum(1 for d in base.iterdir()
               if d.is_dir() and _valid_rows(d / "summary.csv")
               and (version is None or _stamp(d) == version))


def family_state(family: str, version: str | None,
                 conds: dict | None = None) -> tuple[list[str], list[str]]:
    """(完成的点, 未完成的点)。"""
    if family == COMPARE_FAMILY:
        done, missing = [], []
        for cell, tier, _rate in COMPARE_CELLS:
            got, need = compare_cell_done(cell, tier, version)
            (done if got >= need else missing).append(cell)
        return done, missing
    conds = conds or _cond_families()
    if not conds[family]:
        return [], ["<无 COND>"]
    done, missing = [], []
    for cond in conds[family]:
        point = point_of(family, cond)
        if point_done(family, point, int(cond["sampling"]["reps"]), version):
            done.append(point)
        else:
            missing.append(point)
    return done, missing


# ---------------------------------------------------------------------------
# 环境与命令
# ---------------------------------------------------------------------------
def _cond_env(cond: dict, tilt: bool) -> dict[str, str]:
    ds, wl, net = cond["dataset"], cond["workload"], cond["network"]
    cache, pf = cond["cache"], cond["prefetch"]
    env = os.environ.copy()
    env.update({
        "TRAINCACHE_FIGURE_MODE": "1",
        "TRAINCACHE_COND_NAME": cond["name"],
        "CAPACITY_RATIO": _run_tier(cond, tilt),
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
        "TRAINCACHE_PREFETCH_K": "16" if tilt else str(pf["lookahead"]),
        "TC_RATE": f"{net['rate_mbit']}mbit",
        "TC_DELAY": f"{net['delay_ms']}ms",
    })
    mode = (pf.get("gate") or {}).get("mode")
    if mode:
        env["TRAINCACHE_GATE_MODE"] = mode
    if tilt:
        env["TRAINCACHE_TILT"] = "1"
        env["TRAINCACHE_PREFETCH_LOOKAHEAD"] = str(pf["lookahead"])
        env.update(cond_mod.cond_to_env(cond))
    return env


def _runner_cmd(tier: str, rate: int, groups: list[str], reps: int,
                out: Path) -> list[str]:
    return [sys.executable, str(RUNNER),
            "--tier", tier,
            "--rate", f"{rate}mbit",
            "--groups", *groups,
            "--reps", str(reps),
            "--out", str(out)]


def _exec(cmd: list[str], env: dict[str, str]) -> int:
    return subprocess.call(cmd, env=env, cwd=str(bench_config.REPO_ROOT))


# ---------------------------------------------------------------------------
# 执行
# ---------------------------------------------------------------------------
def run_compare_cell(cell: str, tier: str, rate: int, force: bool,
                     version: str) -> bool:
    got, need = compare_cell_done(cell, tier, version)
    if got >= need and not force:
        print(f"[skip] {COMPARE_FAMILY}/{cell} 已完成")
        return True
    groups = _compare_run_groups(tier, cell)
    reps = cond_mod.CAPACITY_TIER_REPS[tier]
    out = results_root(COMPARE_FAMILY) / cell / "summary.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update({
        "CAPACITY_RATIO": tier,
        "DATASET_NAME": "compare-data-8m",
        "TC_RATE": f"{rate}mbit",
        "TC_DELAY": f"{bench_config.TC_DELAY}",
    })
    cmd = _runner_cmd(tier, rate, groups, reps, out)
    print(f"[run] {COMPARE_FAMILY}/{cell} groups={len(groups)} reps={reps}")
    if _exec(cmd, env) != 0:
        return False
    _write_stamp(out.parent, version)
    return True


def run_cond_point(family: str, cond: dict, force: bool,
                   version: str) -> bool:
    point = point_of(family, cond)
    reps = int(cond["sampling"]["reps"])
    base = point_base(family, point)
    if point_done(family, point, reps, version) and not force:
        print(f"[skip] {family}/{point} 已完成")
        return True
    base.mkdir(parents=True, exist_ok=True)
    cmd = _runner_cmd(_tier_of(cond), cond["network"]["rate_mbit"],
                      [cond["group"]], reps, base / "summary.csv")
    print(f"[run] {family}/{point} reps={reps}")
    if _exec(cmd, _cond_env(cond, tilt=False)) != 0:
        return False
    _write_stamp(base, version)
    return True


def run_tilt_point(family: str, cond: dict, force: bool,
                   version: str) -> bool:
    point = point_of(family, cond)
    reps = int(cond["sampling"]["reps"])
    base = point_base(family, point)
    if point_done(family, point, reps, version) and not force:
        print(f"[skip] {family}/{point} 已完成")
        return True
    done = 0 if force else _tilt_done(base, version)
    base.mkdir(parents=True, exist_ok=True)
    for i in range(done, reps):
        run_id = (f"{cond['name']}-r{i + 1:02d}-"
                  f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
        run_dir = base / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        env = _cond_env(cond, tilt=True)
        env["TRAINCACHE_JOB_CREATED_AT"] = _now()
        env["TRAINCACHE_RUN_ID"] = run_id
        cmd = _runner_cmd("S", cond["network"]["rate_mbit"],
                          [cond["group"]], 1, run_dir / "summary.csv")
        print(f"[run] {family}/{point} rep={i + 1}/{reps}")
        if _exec(cmd, env) != 0:
            return False
        _write_stamp(run_dir, version)
        _write_lock(run_dir, cond)
    return True


def _write_lock(run_dir: Path, cond: dict) -> None:
    saved = {k: os.environ.get(k) for k in ("TRAINCACHE_COND", "DATASET_NAME")}
    os.environ["TRAINCACHE_COND"] = cond["name"]
    os.environ["DATASET_NAME"] = cond["dataset"]["name"]
    try:
        lock_mod.write_lock(run_dir / "lock.json")
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def run_family(family: str, points: list[str] | None, force: bool,
               version: str) -> bool:
    ok = True
    if family == COMPARE_FAMILY:
        for cell, tier, rate in COMPARE_CELLS:
            if points and cell not in points:
                continue
            ok = run_compare_cell(cell, tier, rate, force, version) and ok
        return ok
    conds = _cond_families()[family]
    if not conds:
        raise SystemExit(f"{family} 在 {CONDS_DIR} 下没有 COND")
    if points:
        known = {point_of(family, c) for c in conds}
        unknown = sorted(set(points) - known)
        if unknown:
            raise SystemExit(f"{family} 没有这些点: {' '.join(unknown)}")
    runner = run_tilt_point if family in TILT_FAMILIES else run_cond_point
    for cond in conds:
        point = point_of(family, cond)
        if points and point not in points:
            continue
        ok = runner(family, cond, force, version) and ok
    return ok


# ---------------------------------------------------------------------------
# 版本记录回填
# ---------------------------------------------------------------------------
def _registry_grid(path: Path) -> tuple[list[str], list[str], list[int]]:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("| 版本 |"):
            header_idx = i
            break
    if header_idx is None:
        raise SystemExit(f"{path} 找不到以 | 版本 | 开头的表头")
    header = [c.strip() for c in lines[header_idx].strip().strip("|").split("|")]
    row_idx = []
    i = header_idx + 2
    while i < len(lines) and lines[i].startswith("|"):
        row_idx.append(i)
        i += 1
    if not row_idx:
        raise SystemExit(f"{path} 没有版本行;先写一行,版本号放在第一格")
    return lines, header, row_idx


def _family_column(header: list[str], family: str) -> int:
    for i, cell in enumerate(header):
        if family in cell:
            return i
    raise SystemExit(f"4.6 表头没有 {family} 列")


def _last_row_cells(lines: list[str], row_idx: list[int]) -> list[str]:
    return [c.strip() for c in lines[row_idx[-1]].strip().strip("|").split("|")]


def check_registry() -> str:
    lines, _header, row_idx = _registry_grid(REGISTRY)
    version = _last_row_cells(lines, row_idx)[0]
    if not version:
        raise SystemExit("4.6 最后一行的版本号为空;先在最后一行的第一格写版本号")
    return version


def artifact_cell(family: str) -> str:
    """4.6 表格里这一族的产物路径：数据层分区 + 该族 COND 前缀，相对 `results/`。

    一族占一个 glob 而不是分区名：图系列四族同在 `figures/` 下，只写分区名
    四列会一模一样，反查不到点目录。
    """
    root = results_root(family).name
    if family == COMPARE_FAMILY:
        return f"`{root}/<格名>/`"
    return f"`{root}/{cond_prefix(family)}-*/`"


def record_family(family: str) -> None:
    lines, header, row_idx = _registry_grid(REGISTRY)
    cells = _last_row_cells(lines, row_idx)
    if not cells[0]:
        raise SystemExit("4.6 最后一行的版本号为空;先在最后一行的第一格写版本号")
    done, missing = family_state(family, cells[0])
    value = artifact_cell(family) if not missing else "未完成"
    col = _family_column(header, family)
    cells[col] = value
    ending = "\r\n" if lines[row_idx[-1]].endswith("\r\n") else "\n"
    lines[row_idx[-1]] = "| " + " | ".join(cells) + " |" + ending
    REGISTRY.write_text("".join(lines), encoding="utf-8")
    print(f"[record] {family}: {value}")


# ---------------------------------------------------------------------------
# 子命令
# ---------------------------------------------------------------------------
def cmd_status(family: str | None, detail: bool) -> None:
    conds = _cond_families()
    families = [family] if family else list(FAMILIES)
    lines, _header, row_idx = _registry_grid(REGISTRY)
    version = _last_row_cells(lines, row_idx)[0]
    print(f"版本 {version or '(未写)'}")
    for name in families:
        done, missing = family_state(name, version or None, conds)
        print(f"{name:16s} {len(done)}/{len(done) + len(missing)}"
              f"  产物 {results_root(name).name}/")
        if detail:
            for point in missing:
                print(f"  未完成 {point}")


def cmd_run(family: str | None, points: list[str] | None,
            dry_run: bool, force: bool) -> None:
    families = list(FAMILIES) if family in (None, "all") else [family]
    version = ""
    if not dry_run:
        version = check_registry()
        lock = LOCK_FILE.open("w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"已有 exp.py 在跑(锁 {LOCK_FILE})")
    failed = []
    for name in families:
        if dry_run:
            _print_plan(name, points if len(families) == 1 else None)
            continue
        if run_family(name, points if len(families) == 1 else None,
                      force, version):
            record_family(name)
        else:
            failed.append(name)
    if failed:
        raise SystemExit(f"失败 {len(failed)} 族: {' '.join(failed)}")


def _print_plan(family: str, points: list[str] | None = None) -> None:
    if family == COMPARE_FAMILY:
        for cell, tier, rate in COMPARE_CELLS:
            if points and cell not in points:
                continue
            out = results_root(COMPARE_FAMILY) / cell / "summary.csv"
            cmd = _runner_cmd(tier, rate, _compare_run_groups(tier, cell),
                              cond_mod.CAPACITY_TIER_REPS[tier], out)
            print(f"[plan] {family}/{cell}")
            print("       " + " ".join(cmd))
        return
    conds = _cond_families()[family]
    if points:
        known = {point_of(family, c) for c in conds}
        unknown = sorted(set(points) - known)
        if unknown:
            raise SystemExit(f"{family} 没有这些点: {' '.join(unknown)}")
    for cond in conds:
        point = point_of(family, cond)
        if points and point not in points:
            continue
        reps = int(cond["sampling"]["reps"])
        tilt = family in TILT_FAMILIES
        out = (point_base(family, point) / "<run>" / "summary.csv"
               if tilt else point_base(family, point) / "summary.csv")
        cmd = _runner_cmd(_run_tier(cond, tilt), cond["network"]["rate_mbit"],
                          [cond["group"]], 1 if tilt else reps, out)
        print(f"[plan] {family}/{point} reps={reps}")
        print("       " + " ".join(cmd))


def cmd_record(family: str | None) -> None:
    lock = LOCK_FILE.open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(f"已有 exp.py 在跑(锁 {LOCK_FILE})")
    for name in (FAMILIES if family in (None, "all") else [family]):
        record_family(name)


def cmd_selftest() -> None:
    """族↔COND 认领表、点名与产物分区的本地自检。

    命名一漂移，一族就认领不到任何 COND，点目录名退化成空串，几个点写进同一个
    分区；4.3 §5 的 `--point` 与 4.6 的完成判定都按族/点寻址，所以这条要进门禁。
    分区落位也在这里查：`plot.py` 按 `figures/fig*-*`、`summarize.py` 按
    `scores/<格名>` 读产物，目录写错分区的跑分是「跑完了但出不了图」，只有
    对照表会静默少行。
    """
    conds = _cond_families()
    claimed = {c["name"] for v in conds.values() for c in v}
    unowned = {p.stem for p in CONDS_DIR.glob("*.json")
               if not p.stem.startswith("compare-")} - claimed
    problems = []
    if unowned:
        problems.append(f"没有族认领的 COND: {sorted(unowned)}")
    for fam in COND_FAMILIES + TILT_FAMILIES:
        pts = [point_of(fam, c) for c in conds[fam]]
        if not pts:
            problems.append(f"{fam} 认领不到任何 COND")
        if any(not p for p in pts):
            problems.append(f"{fam} 有点名退化成空串")
        if len(set(pts)) != len(pts):
            problems.append(f"{fam} 点名重复: {sorted(pts)}")
        for c, p in zip(conds[fam], pts):
            if f"{cond_prefix(fam)}-{p}" != c["name"]:
                problems.append(f"{fam}: 点名 {p} 还原不出 COND 名 {c['name']}")
            want = (bench_config.POLICY_RESULTS_DIR if fam in TILT_FAMILIES
                    else bench_config.FIGURES_RESULTS_DIR)
            if point_base(fam, p).parent != want:
                problems.append(
                    f"{fam}: 点 {p} 的产物分区是 "
                    f"{point_base(fam, p).parent.name}/,应为 {want.name}/")
    expect = {COMPARE_FAMILY: bench_config.SCORES_RESULTS_DIR}
    expect.update({f: bench_config.FIGURES_RESULTS_DIR for f in COND_FAMILIES})
    expect.update({f: bench_config.POLICY_RESULTS_DIR for f in TILT_FAMILIES})
    for fam, want in expect.items():
        if results_root(fam) != want:
            problems.append(f"{fam} 映射到 {results_root(fam).name}/,应为 {want.name}/")
    if problems:
        raise SystemExit("exp.py 自检失败:\n  " + "\n  ".join(problems))
    print(f"exp.py 自检通过: {len(conds)} 族认领 {sum(len(v) for v in conds.values())} 个 COND，"
          "点命名与族前缀互逆、产物分区落位正确")


def main() -> None:
    parser = argparse.ArgumentParser(description="bench 实验统一入口")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("selftest", help="族↔COND 认领表与点名的本地自检")

    p_status = sub.add_parser("status", help="各族完成度")
    p_status.add_argument("--family", choices=FAMILIES)
    p_status.add_argument("-v", "--detail", action="store_true",
                          help="列出未完成的点")

    p_run = sub.add_parser("run", help="跑实验族")
    p_run.add_argument("family", choices=list(FAMILIES) + ["all"])
    p_run.add_argument("--point", nargs="+", default=None,
                       help="只跑这些点(仅单族)")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--force", action="store_true")

    p_record = sub.add_parser("record", help="按产物回填版本表")
    p_record.add_argument("family", choices=list(FAMILIES) + ["all"],
                          nargs="?", default="all")

    args = parser.parse_args()
    if args.command == "selftest":
        cmd_selftest()
    elif args.command == "status":
        cmd_status(args.family, args.detail)
    elif args.command == "run":
        if args.point and args.family == "all":
            raise SystemExit("--point 只用于单族")
        cmd_run(args.family, args.point, args.dry_run, args.force)
    else:
        cmd_record(args.family)


if __name__ == "__main__":
    main()
