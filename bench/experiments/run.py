#!/usr/bin/env python3
"""M2 实验 runner:冷态→tc→负载→采集→归档,重复采样。

流程(run_once):
  1. 创建产物目录 reps_dir/<run_id>/
  2. 冷态:cold_reset.sh
  3. 挂 tc:tc_apply.sh <rate> <delay> [--target source-d|minio]
  4. 注入 TRAINCACHE_JOB_CREATED_AT = 当前 RFC3339
  5. 跑负载:bench_track_d.py --out-dir <run_dir> ...
  6. 采集:collect.py --run-dir <run_dir>
  7. 写 lock.json(lock.py)
任一步失败:写 VOID 标记,返回目录路径(不抛异常)。
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# 同级导入 cond / lock / stats
sys.path.insert(0, str(Path(__file__).resolve().parent))
import cond as cond_mod  # noqa: E402
import lock as lock_mod  # noqa: E402
import recovery as recovery_mod  # noqa: E402
import stats as stats_mod  # noqa: E402

EXPERIMENTS_DIR = Path(__file__).resolve().parent
BENCH_ROOT = EXPERIMENTS_DIR.parent
SCRIPTS_DIR = BENCH_ROOT / "scripts"
RESULTS_DIR = BENCH_ROOT / "results"

COLD_RESET_SH = SCRIPTS_DIR / "cold_reset.sh"
TC_APPLY_SH = SCRIPTS_DIR / "tc_apply.sh"
BENCH_TRACK_D = SCRIPTS_DIR / "bench_track_d.py"
COLLECT_PY = EXPERIMENTS_DIR / "collect.py"

PY = sys.executable  # 与 Makefile 的 $(PY) 同口径(系统 3.12);此处用当前解释器

# ---- 汇总层产物列(任务 2.5:jsonl/metrics.csv → summary.csv)----
# 列名与对照装置 compare/run_bench.py 的 summary.csv 同口径,两边产物能直接对账。
SUMMARY_FIELDS = [
    "cond", "rep", "group", "tier", "exploratory", "void", "reason",
    "warmup_seconds",
    "startup_makespan", "total_makespan", "stall_ratio", "hit_rate",
    "startup_makespan_median", "startup_makespan_ci_low",
    "startup_makespan_ci_high",
    "total_makespan_median", "total_makespan_ci_low", "total_makespan_ci_high",
    "stall_ratio_median", "stall_ratio_ci_low", "stall_ratio_ci_high",
    "hit_rate_median", "hit_rate_ci_low", "hit_rate_ci_high",
    "startup_warmup_bytes", "warmup_fanout",
    "sustained_prefetch_bytes", "switch_reprefetch_bytes",
    "recovery_time_s", "recovery_censored", "recovery_insufficient_baseline",
]
# 四个指标(启动 makespan、全程 makespan、stall 占比、命中率)
SUMMARY_METRICS = ("startup_makespan", "total_makespan", "stall_ratio", "hit_rate")

BOOTSTRAP_ITERS = 10000   # 与 COND 的 sampling.bootstrap_iters 同口径
CI_SEED = 20261024        # 固定种子:同一批产物重算 CI 必须一致


def _rfc3339_now() -> str:
    """当前 UTC 时刻,RFC3339 微秒精度。"""
    return datetime.now(timezone.utc).isoformat(
        timespec="microseconds").replace("+00:00", "Z")


def _write_void(run_dir: Path, reason: str) -> None:
    """在产物目录写 VOID 标记文件,记录失败原因。"""
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "void": True,
        "reason": reason,
        "failed_at": _rfc3339_now(),
    }
    (run_dir / "VOID").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _run_cmd(cmd: list[str], env: dict | None = None,
             cwd: Path | None = None, timeout: int | None = None) -> tuple[int, str, str]:
    """执行命令(shell=False 列表形式),返回 (returncode, stdout, stderr)。"""
    proc = subprocess.run(
        cmd, capture_output=True, text=True, env=env, cwd=str(cwd) if cwd else None,
        timeout=timeout, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def _tc_target(cond: dict) -> str | None:
    """根据 dataset.source 决定 tc_apply.sh 的 --target。

    local-nginx(D 轨本地源站)→ source-d;minio(B/C 轨)→ 默认(minio,不传 --target)。
    """
    source = cond.get("dataset", {}).get("source")
    if source == "local-nginx":
        return "source-d"
    return None


# ---------------------------------------------------------------------------
# 汇总层:一轮产物 → summary.csv 的一行
# ---------------------------------------------------------------------------
def _to_float(value) -> float | None:
    """转 float;空值或非数返回 None。"""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value) -> datetime | None:
    """RFC3339 → aware UTC datetime;空值或不可解析返回 None。"""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _read_metrics_row(run_dir: Path) -> dict:
    """读一轮产物里的 metrics.csv(collect.py 写)第一行。"""
    path = Path(run_dir) / "metrics.csv"
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            return row
    return {}


def _read_jsonl_stall(run_dir: Path) -> float | None:
    """从 per_step_*.jsonl 算 stall 占比。

    口径:非 warmup 步的 mean(wait_s / step_s),与 bench/scripts/bench_track_d.py
    的 stall() 同源(docs/5 §10:扫点用 mean,中位数只判形态)。
    没有 jsonl 或没有可用步时返回 None。
    """
    vals = []
    for path in sorted(Path(run_dir).glob("per_step_*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("warmup"):
                continue
            step_s = _to_float(rec.get("step_s"))
            if not step_s or step_s <= 0:
                continue
            wait_s = _to_float(rec.get("wait_s")) or 0.0
            vals.append(wait_s / step_s)
    if not vals:
        return None
    return sum(vals) / len(vals)


def _hit_rate(row: dict) -> float | None:
    """命中率:三个阶段(startup/steady/switch)的 hits 与 misses 合计口径。

    无 Prometheus 序列时回退到 agent 的 gauge 值。
    """
    hits = misses = 0.0
    for phase in ("startup", "steady", "switch"):
        hits += _to_float(row.get(f"hit_hits_{phase}")) or 0.0
        misses += _to_float(row.get(f"hit_misses_{phase}")) or 0.0
    if hits + misses > 0:
        return hits / (hits + misses)
    for key in ("hit_ratio_gauge_steady", "hit_ratio_steady"):
        val = _to_float(row.get(key))
        if val is not None:
            return val
    return None


def _measured_warmup_seconds(run_dir: Path, row: dict) -> float:
    """实测预热耗时(秒)——不是 COND 里 cache.warmup_seconds 那个预算值。

    取 WarmupTask 自身时长(status.startedAt → 门控放行);拿不到时回退到
    启动窗口就绪时刻 − t_submit。没有预热任务的组记 0。
    """
    detail = Path(run_dir) / "collect.json"
    if detail.is_file():
        try:
            wt = json.loads(detail.read_text(encoding="utf-8")).get("warmup_task") or {}
        except json.JSONDecodeError:
            wt = {}
        started = _parse_ts(wt.get("t_warmup_started"))
        released = _parse_ts(wt.get("t_gate_release"))
        if started and released and released >= started:
            return round((released - started).total_seconds(), 3)
    ready = _parse_ts(row.get("t_startup_ready"))
    submit = _parse_ts(row.get("t_submit"))
    if ready and submit and ready >= submit:
        return round((ready - submit).total_seconds(), 3)
    return 0.0


def _recovery_stats(run_dir: Path) -> tuple[float | None, int, int]:
    """阶段切换回落统计(Q9 判据,生产者是 recovery.py)。

    返回 (回落时间中位数, 删失事件数, 基线不足事件数)。
    回落时间按「切换时刻→回落点」的实测 step_s 累加,取本轮所有成功回落事件的中位数;
    删失(到 epoch 末未回落)与基线不足(缺 20 步基线)单独计数,后者不进图三。
    产物缺 per_step_*.jsonl 或切换事件时返回 (None, 0, 0),不报错——不是每轮都有切换。
    """
    try:
        out = recovery_mod.analyze_run(run_dir)
    except (Exception, SystemExit) as exc:
        # recovery 的输入不满足契约时抛 RecoveryInputError(继承 SystemExit,不是 Exception),
        # 这里连 SystemExit 一起兜住:回落统计只是 summary 的三列附加信息,
        # 不该让整轮跑分失败。留一行告警,别把问题静默吞掉。
        print(f"[warn] {run_dir}: 回落统计不可用({exc})", file=sys.stderr)
        return None, 0, 0
    events = out.get("events") or []
    times = [e.get("recovery_time_s") for e in events
             if e.get("recovery_time_s") is not None]
    censored = sum(1 for e in events if e.get("status") == "censored")
    insufficient = sum(1 for e in events
                       if e.get("status") == "insufficient_baseline")
    med = stats_mod.median(times) if times else None
    return (round(float(med), 6) if med is not None else None,
            censored, insufficient)


def summarize_run(run_dir: Path, cond: dict, rep: int) -> dict:
    """把一轮产物汇成 summary.csv 的一行。

    四指标:启动 makespan / 全程 makespan / stall 占比 / 命中率。
    三笔字节成本:启动预热(计数器已含 fanout 重复,不再二次乘)+ 持续预取 +
    阶段切换重预取。tier 取 COND 的 cache.capacity_ratio;exploratory 在 L 档为真。
    """
    run_dir = Path(run_dir)
    row = _read_metrics_row(run_dir)
    tier = (cond.get("cache") or {}).get("capacity_ratio") or ""

    void_marker = run_dir / "VOID"
    void = void_marker.is_file() or row.get("void", "").strip().lower() == "true"
    reason = ""
    if void_marker.is_file():
        try:
            reason = (json.loads(void_marker.read_text(encoding="utf-8"))
                      or {}).get("reason", "")
        except json.JSONDecodeError:
            reason = ""
    recovery_s, recovery_censored, recovery_insufficient = _recovery_stats(run_dir)

    return {
        "cond": cond.get("name", "") or row.get("cond", ""),
        "rep": rep,
        "group": cond.get("group", "") or row.get("group", ""),
        "tier": tier,
        "exploratory": "true" if tier in cond_mod.EXPLORATORY_TIERS else "false",
        "void": "true" if void else "false",
        "reason": reason,
        "warmup_seconds": _measured_warmup_seconds(run_dir, row),
        "startup_makespan": _to_float(row.get("startup_makespan_s")),
        "total_makespan": _to_float(row.get("full_makespan_s")),
        "stall_ratio": _read_jsonl_stall(run_dir),
        "hit_rate": _hit_rate(row),
        "startup_warmup_bytes": _to_float(row.get("startup_fetch_bytes")),
        "warmup_fanout": (_to_float(row.get("fanout_actual"))
                          or _to_float(row.get("fanout_max"))),
        "sustained_prefetch_bytes": _to_float(row.get("sustained_fetch_bytes")),
        "switch_reprefetch_bytes": _to_float(row.get("switch_fetch_bytes")),
        "recovery_time_s": recovery_s,
        "recovery_censored": recovery_censored,
        "recovery_insufficient_baseline": recovery_insufficient,
    }


def _attach_ci(rows: list[dict]) -> None:
    """给每行补上本 COND 的中位数与 bootstrap 95% CI。

    void 轮不进统计(10.1 D14)。L 档只有 1 次采样,按预注册 1.9 只报单点值、
    不报 CI,CI 列留空。
    """
    valid = [r for r in rows if r.get("void") != "true"]
    exploratory = bool(valid) and all(r.get("exploratory") == "true" for r in valid)
    for metric in SUMMARY_METRICS:
        vals = [r[metric] for r in valid if r.get(metric) is not None]
        median = ci_low = ci_high = None
        if vals and not exploratory:
            median = round(stats_mod.median(vals), 6)
            low, high = stats_mod.bootstrap_ci(
                vals, iters=BOOTSTRAP_ITERS, seed=CI_SEED)
            ci_low, ci_high = round(low, 6), round(high, 6)
        for r in rows:
            r[f"{metric}_median"] = "" if median is None else median
            r[f"{metric}_ci_low"] = "" if ci_low is None else ci_low
            r[f"{metric}_ci_high"] = "" if ci_high is None else ci_high


def _csv_value(value):
    """写 CSV 前的取值:None → 空串,浮点去掉多余的尾零。"""
    if value is None:
        return ""
    if isinstance(value, float):
        return round(value, 6)
    return value


def write_summary(path: Path, rows: list[dict]) -> Path:
    """把各轮汇总行落盘成 summary.csv。返回写入路径。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _csv_value(row.get(k)) for k in SUMMARY_FIELDS})
    return path


def run_once(cond: dict, run_id: str, reps_dir: Path) -> Path:
    """跑一轮实验,返回本轮产物目录路径。

    任一步失败:写 VOID 标记,记录原因,返回目录路径(不抛异常)。
    """
    reps_dir = Path(reps_dir)
    run_dir = reps_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # ---- 2. 冷态 ----
    rc, out, err = _run_cmd(["bash", str(COLD_RESET_SH)])
    if rc != 0:
        reason = f"cold_reset 失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    print(f"[{run_id}] 冷态完成")

    # ---- 3. 挂 tc ----
    tc_args = cond_mod.cond_to_tc_args(cond)
    target = _tc_target(cond)
    tc_cmd = ["bash", str(TC_APPLY_SH)] + tc_args
    if target:
        tc_cmd += ["--target", target]
    rc, out, err = _run_cmd(tc_cmd)
    if rc != 0:
        reason = f"tc_apply 失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    print(f"[{run_id}] tc 已挂载: {' '.join(tc_args)}"
          f"{' --target ' + target if target else ''}")

    # ---- 4. 注入 TRAINCACHE_JOB_CREATED_AT + 合并 env ----
    job_created_at = _rfc3339_now()
    base_env = os.environ.copy()
    cond_env = cond_mod.cond_to_env(cond)
    base_env.update(cond_env)
    base_env["TRAINCACHE_JOB_CREATED_AT"] = job_created_at
    base_env["TRAINCACHE_RUN_ID"] = run_id

    # ---- 5. 跑负载 ----
    td_args = cond_mod.cond_to_track_d_args(cond)
    td_cmd = [PY, str(BENCH_TRACK_D)] + td_args + ["--out-dir", str(run_dir)]
    rc, out, err = _run_cmd(td_cmd, env=base_env)
    if rc != 0:
        reason = f"bench_track_d 失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    print(f"[{run_id}] 负载完成")

    # ---- 6. 采集 ----
    collect_cmd = [PY, str(COLLECT_PY), "--run-dir", str(run_dir)]
    rc, out, err = _run_cmd(collect_cmd, env=base_env)
    if rc != 0:
        reason = f"collect 失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    print(f"[{run_id}] 采集完成 → {run_dir / 'metrics.csv'}")

    # ---- 7. 写 lock.json ----
    try:
        lock_mod.write_lock(run_dir / "lock.json")
        print(f"[{run_id}] lock.json 已写")
    except Exception as exc:  # noqa: BLE001
        reason = f"lock 写入失败: {exc}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir

    return run_dir


def run_reps(cond_name: str, reps: int,
             conds_dir: Path | None = None) -> list[Path]:
    """跑 reps 轮。每轮前做冷态。cond_name 传给 load_cond。

    返回每轮产物目录列表。
    """
    cond = cond_mod.load_cond(cond_name, conds_dir)
    errors = cond_mod.validate_cond(cond)
    if errors:
        raise ValueError(f"COND {cond_name} 校验失败:\n" + "\n".join(errors))

    reps_dir = RESULTS_DIR / cond_name
    reps_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []
    rows: list[dict] = []
    for i in range(reps):
        run_id = f"{cond_name}-r{i+1:02d}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        print(f"\n===== 第 {i+1}/{reps} 轮: {run_id} =====")
        run_dir = run_once(cond, run_id, reps_dir)
        results.append(run_dir)
        rows.append(summarize_run(run_dir, cond, i + 1))
        void_marker = run_dir / "VOID"
        if void_marker.exists():
            print(f"[{run_id}] 本轮作废(VOID),继续下一轮", file=sys.stderr)

    # 汇总层:各轮产物 → summary.csv(任务 2.5)
    _attach_ci(rows)
    summary_path = write_summary(reps_dir / "summary.csv", rows)
    print(f"\n[summary] 已写 {summary_path}({len(rows)} 行)")
    return results


def main() -> None:
    ap = argparse.ArgumentParser(
        description="M2 实验 runner:冷态→tc→负载→采集→归档,重复采样")
    ap.add_argument("--cond", help="COND 名(conds/<name>.json)")
    ap.add_argument("--reps", type=int, default=7, help="重复次数(默认 7)")
    ap.add_argument("--conds-dir", type=Path, default=None,
                    help="COND 目录(默认 bench/experiments/conds)")
    ap.add_argument("--selftest", action="store_true",
                    help="跑汇总层自检(纯本地 mock,不碰集群)后退出")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return
    if not args.cond:
        ap.error("需要 --cond(或用 --selftest 跑汇总层自检)")

    run_reps(args.cond, args.reps, args.conds_dir)


# ---------------------------------------------------------------------------
# selftest:汇总层(jsonl/metrics.csv → summary.csv)纯本地自检
# ---------------------------------------------------------------------------
def _write_mock_run(run_dir: Path, startup_s: float, full_s: float,
                     hits: int, misses: int, void: bool = False) -> None:
    """造一轮 mock 产物:metrics.csv + per_step jsonl + collect.json。"""
    run_dir.mkdir(parents=True, exist_ok=True)
    fields = ["cond", "group", "void", "t_submit", "t_startup_ready",
              "startup_makespan_s", "full_makespan_s",
              "startup_fetch_bytes", "sustained_fetch_bytes",
              "switch_fetch_bytes", "fanout_actual",
              "hit_hits_startup", "hit_misses_startup",
              "hit_hits_steady", "hit_misses_steady",
              "hit_hits_switch", "hit_misses_switch"]
    row = {
        "cond": "mock-cond", "group": "traincache",
        "void": "true" if void else "false",
        "t_submit": "2026-10-24T00:00:00.000000Z",
        "t_startup_ready": "2026-10-24T00:00:30.000000Z",
        "startup_makespan_s": startup_s, "full_makespan_s": full_s,
        "startup_fetch_bytes": 100, "sustained_fetch_bytes": 200,
        "switch_fetch_bytes": 50, "fanout_actual": 2,
        "hit_hits_startup": hits, "hit_misses_startup": misses,
        "hit_hits_steady": hits, "hit_misses_steady": misses,
        "hit_hits_switch": hits, "hit_misses_switch": misses,
    }
    with (run_dir / "metrics.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerow(row)

    # 字段集要满足 recovery.py 的装载契约(含 global_step),否则回落统计读不了
    recs = [
        {"epoch": 0, "step_in_epoch": 0, "global_step": 0, "warmup": False,
         "phase": "startup", "wait_s": 0.1, "step_s": 0.2},
        {"epoch": 0, "step_in_epoch": 1, "global_step": 1, "warmup": False,
         "phase": "steady", "wait_s": 0.2, "step_s": 0.4},
        {"epoch": 0, "step_in_epoch": 2, "global_step": 2, "warmup": True,
         "phase": "warmup", "wait_s": 9.0, "step_s": 9.0},  # warmup 步不计入 stall
    ]
    with (run_dir / "per_step_1_w4.jsonl").open("w", encoding="utf-8") as f:
        for rec in recs:
            f.write(json.dumps(rec) + "\n")

    (run_dir / "collect.json").write_text(json.dumps({
        "warmup_task": {
            "t_warmup_started": "2026-10-24T00:00:00.000000Z",
            "t_gate_release": "2026-10-24T00:00:42.000000Z",
        },
    }), encoding="utf-8")

    # run.json:recovery.py 的切换事件来源。没有切换事件的轮次给空数组,
    # 这样回落统计走真实路径(而不是靠异常兜底),告警不会出现在正常产物上。
    (run_dir / "run.json").write_text(json.dumps({
        "run_id": "mock",
        "args": {"shuffle": "per-epoch", "epochs": 1, "seed": 20261024},
        "switch_events": [],
    }), encoding="utf-8")

    if void:
        (run_dir / "VOID").write_text(json.dumps(
            {"void": True, "reason": "mock 作废"}), encoding="utf-8")


def selftest() -> None:
    """汇总层自检:列名、四指标口径、CI、tier/exploratory、void 处理。"""
    cond_s = {"name": "mock-s", "group": "traincache",
              "cache": {"capacity_ratio": "S"}}
    cond_l = {"name": "mock-l", "group": "traincache",
              "cache": {"capacity_ratio": "L"}}

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_mock_run(root / "r1", 10.0, 100.0, 30, 10)
        _write_mock_run(root / "r2", 12.0, 110.0, 20, 10)
        _write_mock_run(root / "r3", 999.0, 999.0, 0, 0, void=True)

        rows = [summarize_run(root / "r1", cond_s, 1),
                summarize_run(root / "r2", cond_s, 2),
                summarize_run(root / "r3", cond_s, 3)]
        assert rows[0]["cond"] == "mock-s" and rows[0]["rep"] == 1
        assert rows[0]["tier"] == "S", rows[0]
        assert rows[0]["exploratory"] == "false", rows[0]
        assert rows[0]["group"] == "traincache"
        assert rows[0]["void"] == "false"
        # 实测预热时长 = WarmupTask startedAt → 门控放行 = 42s(不是 COND 预算)
        assert rows[0]["warmup_seconds"] == 42.0, rows[0]
        # 四指标
        assert rows[0]["startup_makespan"] == 10.0, rows[0]
        assert rows[0]["total_makespan"] == 100.0, rows[0]
        # stall = mean(wait_s/step_s),warmup 步被排除 → (0.5 + 0.5) / 2
        assert abs(rows[0]["stall_ratio"] - 0.5) < 1e-9, rows[0]
        # 命中率 = 三阶段 hits/(hits+misses) 合计
        assert abs(rows[0]["hit_rate"] - (90 / 120)) < 1e-9, rows[0]
        # 三笔字节成本
        assert rows[0]["startup_warmup_bytes"] == 100, rows[0]
        assert rows[0]["warmup_fanout"] == 2, rows[0]
        assert rows[0]["sustained_prefetch_bytes"] == 200, rows[0]
        assert rows[0]["switch_reprefetch_bytes"] == 50, rows[0]
        # void 轮标出来并带原因
        assert rows[2]["void"] == "true" and rows[2]["reason"], rows[2]

        _attach_ci(rows)
        # void 轮不进统计:中位数只由前两轮算 → (10+12)/2 = 11
        assert rows[0]["startup_makespan_median"] == 11.0, rows[0]
        assert rows[0]["startup_makespan_ci_low"] <= 11.0 <= \
            rows[0]["startup_makespan_ci_high"], rows[0]
        # 固定种子 → 两次算出的 CI 相同
        again = [summarize_run(root / "r1", cond_s, 1),
                 summarize_run(root / "r2", cond_s, 2)]
        _attach_ci(again)
        assert again[0]["startup_makespan_ci_low"] == \
            rows[0]["startup_makespan_ci_low"], (again[0], rows[0])

        out = write_summary(root / "summary.csv", rows)
        assert out.is_file()
        with out.open(newline="", encoding="utf-8") as f:
            got = list(csv.DictReader(f))
        assert list(got[0].keys()) == SUMMARY_FIELDS, list(got[0].keys())
        assert len(got) == 3, len(got)
        assert got[0]["cond"] == "mock-s" and got[0]["tier"] == "S"
        assert got[2]["void"] == "true"

        # L 档:标 exploratory 且不报 CI(预注册 1.9)
        lrow = summarize_run(root / "r1", cond_l, 1)
        assert lrow["tier"] == "L" and lrow["exploratory"] == "true", lrow
        _attach_ci([lrow])
        assert lrow["startup_makespan_ci_low"] == "" and \
            lrow["startup_makespan_ci_high"] == "", lrow

    print("run selftest: PASS")


if __name__ == "__main__":
    main()
