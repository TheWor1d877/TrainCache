#!/usr/bin/env python3
"""COND 执行体:一个 COND 一轮跑分,重复采样。

按 COND 的 load 字段选执行体,不看名字前缀:
  dataset → compare/run_bench.py(集群内跑分,由它写 summary.csv)
  tilt    → run_policy.py(倾斜双 Job,淘汰×预取)
  train   → scripts/load_train.py + collect.py(本文件的冷态→tc→负载→采集→归档流程)

流程(run_once):
  1. 创建产物目录 reps_dir/<run_id>/
  2. 冷态:cold_reset.sh
  3. 挂 tc:tc_apply.sh <rate> <delay> [--target source-d|minio]
  4. 注入 TRAINCACHE_JOB_CREATED_AT = 当前 RFC3339
  5. 跑负载:load_train.py --out-dir <run_dir> ...
  6. 采集:collect.py --run-dir <run_dir>
  7. 写 lock.json(lock.py)
任一步失败:写 VOID 标记,返回目录路径(不抛异常)。
"""
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
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
sys.path.insert(0, str(BENCH_ROOT))
import config as bench_config  # noqa: E402

COLD_RESET_SH = SCRIPTS_DIR / "cold_reset.sh"
TC_APPLY_SH = SCRIPTS_DIR / "tc_apply.sh"
LOAD_TRAIN = SCRIPTS_DIR / "load_train.py"
COLLECT_PY = EXPERIMENTS_DIR / "collect.py"
RUN_BENCH = BENCH_ROOT / "compare" / "run_bench.py"

PY = sys.executable  # 与 Makefile 的 $(PY) 同口径(系统 3.12);此处用当前解释器

# ---- e2e 族:两阶段跑分 ----
# 每个场景只差三处:中断在第几个 epoch、两阶段之间做什么动作、阶段二读哪个
# 数据版本。其余参数(数据集、epoch 数、带宽、缓存配额)与 COND 相同。
E2E_SCENARIOS = {
    "checkpoint-resume": {
        "interrupt_after_epoch": 1,   # 跑完第 1 个 epoch 写 checkpoint 中断
        "action": None,               # 两阶段之间不动集群
        "data_version": 1,            # 阶段二仍读版本 1
    },
    "checkpoint-version": {
        "interrupt_after_epoch": 1,
        "action": None,
        "data_version": 2,            # 阶段二读版本 2(同形状,种子不同)
    },
    "dual-restart": {
        "interrupt_after_epoch": 1,
        "action": "restart-agentd",   # 训练进程退出后重建 agentd
        "data_version": 1,
    },
}
E2E_AGENTD_NS = "traincache"
E2E_AGENTD_LABEL = "app=traincache-agentd"
E2E_AGENTD_READY_TIMEOUT_S = 300

# ---- 汇总层产物列(任务 2.5:jsonl/metrics.csv → summary.csv)----
# 列名与对照装置 compare/run_bench.py 的 summary.csv 同口径,两边产物能直接对账。
SUMMARY_FIELDS = [
    "cond", "rep", "group", "family", "load", "gateway", "rho",
    "exploratory", "void", "reason",
    "warmup_seconds",
    "startup_makespan", "total_makespan", "stall_ratio", "hit_rate",
    "startup_makespan_mean", "startup_makespan_ci_low",
    "startup_makespan_ci_high",
    "total_makespan_mean", "total_makespan_ci_low", "total_makespan_ci_high",
    "stall_ratio_mean", "stall_ratio_ci_low", "stall_ratio_ci_high",
    "hit_rate_mean", "hit_rate_ci_low", "hit_rate_ci_high",
    "startup_warmup_bytes", "warmup_fanout",
    "sustained_prefetch_bytes", "switch_reprefetch_bytes",
    "recovery_time_s", "recovery_censored", "recovery_insufficient_baseline",
    # e2e 族:两阶段恢复的专属列(其余族留空)
    "e2e_scenario", "e2e_phase1_epochs", "e2e_recovery_time_s",
    "e2e_resume_submit_lag_s", "e2e_fetch_errors_recovery", "e2e_complete",
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
    """tc_apply.sh 的 --target:train 负载读宿主机本地源站,其余走 MinIO。"""
    return "source-d" if cond.get("load") == "train" else None


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

    口径:非 warmup 步的 mean(wait_s / step_s),与 bench/scripts/load_train.py
    的 stall() 同源(4.3 stall:扫点用 mean,中位数只判形态)。
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

    返回 (回落时间平均数, 删失事件数, 基线不足事件数)。
    回落时间按「切换时刻→回落点」的实测 step_s 累加,取本轮所有成功回落事件的平均数;
    删失(到 epoch 末未回落)与基线不足(缺 20 步基线)单独计数,后者不进 fig3 的回落曲线。
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
    avg = stats_mod.mean(times) if times else None
    return (round(float(avg), 6) if avg is not None else None,
            censored, insufficient)


def _e2e_scenario(cond: dict) -> str:
    """COND 的 scenario 标签(e2e 族用)。没有标签返回空串。"""
    for tag in cond.get("tags") or []:
        text = str(tag)
        if text.startswith("scenario="):
            return text.split("=", 1)[1]
    return ""


def _e2e_resume_facts(run_dir: Path) -> dict:
    """读 run.json 的 resume 段:两阶段的时刻与恢复时长。

    非 e2e 轮没有这段,返回空 dict。
    """
    path = Path(run_dir) / "run.json"
    if not path.is_file():
        return {}
    try:
        run = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    resume = run.get("resume") or {}
    if not resume:
        return {}
    return {
        "phase1_t_last_step": resume.get("phase1_t_last_step"),
        "phase2_t_submit": resume.get("phase2_t_submit"),
        "phase2_t_first_step": resume.get("phase2_t_first_step"),
        "recovery_time_s": resume.get("recovery_time_s"),
        "resume_submit_lag_s": resume.get("resume_submit_lag_s"),
        "checkpoint_next_epoch": resume.get("checkpoint_next_epoch"),
        "complete": run.get("complete"),
    }


def summarize_run(run_dir: Path, cond: dict, rep: int) -> dict:
    """把一轮产物汇成 summary.csv 的一行。

    四指标:启动 makespan / 全程 makespan / stall 占比 / 命中率。
    三笔字节成本:启动预热(计数器已含 fanout 重复,不再二次乘)+ 持续预取 +
    阶段切换重预取。rho 取 COND 的 cache.rho;exploratory 看 COND 的 tags。
    """
    run_dir = Path(run_dir)
    row = _read_metrics_row(run_dir)
    cache = cond.get("cache") or {}
    rho = cache.get("rho") if cache.get("enabled") else ""
    exploratory = "exploratory" in (cond.get("tags") or [])

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
    resume_facts = _e2e_resume_facts(run_dir)
    scenario = _e2e_scenario(cond)

    return {
        "cond": cond.get("name", "") or row.get("cond", ""),
        "rep": rep,
        "group": cond.get("group", "") or row.get("group", ""),
        "family": cond.get("family", ""),
        "load": cond.get("load", ""),
        "gateway": "true" if cond.get("environment", {}).get("gateway") else "false",
        "rho": "" if rho is None else rho,
        "exploratory": "true" if exploratory else "false",
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
        "e2e_scenario": scenario,
        "e2e_phase1_epochs": resume_facts.get("checkpoint_next_epoch"),
        "e2e_recovery_time_s": resume_facts.get("recovery_time_s"),
        "e2e_resume_submit_lag_s": resume_facts.get("resume_submit_lag_s"),
        "e2e_fetch_errors_recovery": _to_float(row.get("fetch_errors_recovery")),
        "e2e_complete": ("true" if resume_facts.get("complete") else "false")
        if resume_facts else "",
    }


def _attach_ci(rows: list[dict]) -> None:
    """给每行补上本 COND 的平均数与 bootstrap 95% CI。

    void 轮不进统计(4.3 D14 异常轮整轮作废)。end-to-end 族每场景 2 次采样,只做二值判定,
    不报 CI,CI 列留空。
    """
    valid = [r for r in rows if r.get("void") != "true"]
    exploratory = bool(valid) and all(r.get("exploratory") == "true" for r in valid)
    for metric in SUMMARY_METRICS:
        vals = [r[metric] for r in valid if r.get(metric) is not None]
        avg = ci_low = ci_high = None
        if vals and not exploratory:
            avg = round(stats_mod.mean(vals), 6)
            low, high = stats_mod.bootstrap_ci(
                vals, iters=BOOTSTRAP_ITERS, seed=CI_SEED)
            ci_low, ci_high = round(low, 6), round(high, 6)
        for r in rows:
            r[f"{metric}_mean"] = "" if avg is None else avg
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
    td_args = cond_mod.cond_to_train_args(cond)
    td_cmd = ([PY, str(LOAD_TRAIN)] + td_args
              + ["--n-tars", str(cond["dataset"]["shards"]),
                 "--out-dir", str(run_dir)])
    rc, out, err = _run_cmd(td_cmd, env=base_env)
    if rc != 0:
        reason = f"load_train 失败(rc={rc}): {err.strip()[:500]}"
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


def run_reps(cond_name: str, reps: int | None = None,
             conds_dir: Path | None = None) -> list[Path]:
    """跑一个 COND;reps=None 取 COND 的 sampling.reps。

    按 load 选执行体:dataset 交给 run_bench.py,tilt 交给 run_policy.py,
    train 走本文件的逐轮流程(每轮前做冷态);e2e 族走两阶段流程。
    """
    cond = cond_mod.load_cond(cond_name, conds_dir)
    errors = cond_mod.validate_cond(cond)
    if errors:
        raise ValueError(f"COND {cond_name} 校验失败:\n" + "\n".join(errors))
    if reps is None:
        reps = int(cond["sampling"]["reps"])

    if cond.get("load") == "dataset":
        return _run_bench_cond(cond, reps)
    if cond.get("load") == "tilt":
        import run_policy  # noqa: PLC0415
        return run_policy.run_reps(cond_name, reps, conds_dir)
    if _e2e_scenario(cond):
        return _run_e2e_cond(cond, reps)
    return _run_train_cond(cond, reps)


def _axis_env(cond: dict) -> dict[str, str]:
    """族轴上 cond_to_env 没覆盖的旋钮。

    run_bench 在 import 时读 TRAINCACHE_PREHEAT_FANOUT，prefetch 族的 fanout 轴
    只能这样进去；其余轴（lookahead / gate / evict / rho）由 cond_to_env 给。
    """
    fanout = (cond.get("prefetch") or {}).get("fanout")
    return {} if fanout is None else {"TRAINCACHE_PREHEAT_FANOUT": str(fanout)}


def _run_bench_cond(cond: dict, reps: int) -> list[Path]:
    """dataset 负载:一格一次 run_bench.py,产物落在 results/<族>/<COND 名>/。"""
    root = bench_config.family_results_dir(cond["family"])
    cache = cond.get("cache") or {}
    cmd = [PY, str(RUN_BENCH)]
    if cache.get("enabled"):
        cmd += ["--rho", str(cache["rho"])]
    cmd += ["--rate", f"{cond['network']['rate_mbit']}mbit",
            "--group", cond["group"],
            "--reps", str(reps),
            "--out-root", str(root),
            "--cell", cond["name"],
            "--family", cond["family"]]
    env = os.environ.copy()
    env.update(cond_mod.cond_to_env(cond))
    env.update(_axis_env(cond))
    print(f"[run] {cond['name']} reps={reps}")
    rc, _out, err = _run_cmd(cmd, env=env)
    if rc != 0:
        raise SystemExit(f"run_bench 失败(rc={rc}): {err.strip()[:500]}")
    return [root]


def _run_train_cond(cond: dict, reps: int) -> list[Path]:
    """train 负载:逐轮冷态→tc→负载→采集,再汇成 summary.csv。"""
    cond_name = cond["name"]
    reps_dir = bench_config.family_results_dir(cond["family"]) / cond_name
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


# ---------------------------------------------------------------------------
# e2e 族:两阶段跑分
# ---------------------------------------------------------------------------
def _e2e_agentd_state() -> tuple[int, list[dict]]:
    """agentd DaemonSet 的期望 Pod 数与当前 Pod 列表。

    Pod 列表每项含 name / uid / ready。取不到就返回 (0, [])。
    """
    rc, out, _ = _run_cmd([
        "kubectl", "-n", E2E_AGENTD_NS, "get", "pods", "-l", E2E_AGENTD_LABEL,
        "-o", "custom-columns=NAME:.metadata.name,UID:.metadata.uid,"
              "READY:.status.containerStatuses[0].ready", "--no-headers"])
    pods: list[dict] = []
    if rc == 0:
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 3:
                pods.append({"name": parts[0], "uid": parts[1],
                             "ready": parts[2].lower() == "true"})
    rc2, out2, _ = _run_cmd([
        "kubectl", "-n", E2E_AGENTD_NS, "get", "ds", "traincache-agentd",
        "-o", "jsonpath={.status.desiredNumberScheduled}"])
    desired = 0
    if rc2 == 0 and out2.strip().isdigit():
        desired = int(out2.strip())
    return desired, pods


def _e2e_restart_agentd(timeout_s: int = E2E_AGENTD_READY_TIMEOUT_S) -> dict:
    """删掉 agentd Pod 让 DaemonSet 重建,等新 Pod 全部 Ready。

    返回 {ok, reason, t_start, t_ready, old_uids, new_uids}。超时或找不到
    Pod 时 ok=False,调用方把这一轮作废。
    """
    t_start = _rfc3339_now()
    desired, pods = _e2e_agentd_state()
    if not pods:
        return {"ok": False, "reason": f"命名空间 {E2E_AGENTD_NS} 里没有 agentd Pod",
                "t_start": t_start}
    old_uids = {p["uid"] for p in pods}
    for pod in pods:
        _run_cmd(["kubectl", "-n", E2E_AGENTD_NS, "delete", "pod", pod["name"],
                  "--grace-period=0", "--wait=false"])
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        want, current = _e2e_agentd_state()
        ready = [p for p in current if p["ready"] and p["uid"] not in old_uids]
        if want and len(ready) >= want:
            return {"ok": True, "reason": "", "t_start": t_start,
                    "t_ready": _rfc3339_now(),
                    "old_uids": sorted(old_uids),
                    "new_uids": sorted(p["uid"] for p in ready)}
        time.sleep(2)
    return {"ok": False, "reason": f"{timeout_s}s 内 agentd 没有全部 Ready",
            "t_start": t_start, "old_uids": sorted(old_uids)}


def _run_e2e_once(cond: dict, run_id: str, reps_dir: Path,
                  scenario: str) -> Path:
    """e2e 一轮:阶段一(跑 N 个 epoch 后中断)→ 场景动作 → 阶段二(续跑)→ 采集。

    两阶段共用同一个产物目录:阶段二的 run.json 记的是全程口径(提交时刻取
    阶段一),逐步记录追加在同一份 per_step_*.jsonl 上,阶段一的 run.json
    另存为 run_phase1.json 留档。
    """
    spec = E2E_SCENARIOS.get(scenario)
    if spec is None:
        raise ValueError(f"未知 e2e 场景: {scenario!r}")
    reps_dir = Path(reps_dir)
    run_dir = reps_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    ckpt_dir = run_dir / "ckpt"

    # 数据版本 2 的前置:分片不在就整轮作废,不进半截流程
    if spec["data_version"] == 2:
        shards = int(cond["dataset"]["shards"])
        missing = [i for i in range(shards)
                   if not (bench_config.TRAIN_DIR_V2 / f"shard-{i:06d}.tar").is_file()]
        if missing:
            reason = (f"数据版本 2 未就绪:{bench_config.TRAIN_DIR_V2} 缺 "
                      f"{len(missing)}/{shards} 个分片;先跑 make -C bench bench-data-v2")
            _write_void(run_dir, reason)
            print(f"[{run_id}] {reason}", file=sys.stderr)
            return run_dir

    # ---- 冷态 ----
    rc, out, err = _run_cmd(["bash", str(COLD_RESET_SH)])
    if rc != 0:
        reason = f"cold_reset 失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    print(f"[{run_id}] 冷态完成")

    # ---- 挂 tc ----
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
    print(f"[{run_id}] tc 已挂载: {' '.join(tc_args)}")

    base_env = os.environ.copy()
    base_env.update(cond_mod.cond_to_env(cond))
    base_env["TRAINCACHE_RUN_ID"] = run_id
    td_args = cond_mod.cond_to_train_args(cond)
    td_common = (td_args + ["--n-tars", str(cond["dataset"]["shards"]),
                            "--out-dir", str(run_dir),
                            "--checkpoint-dir", str(ckpt_dir)])

    # ---- 阶段一:跑够 N 个 epoch 就写 checkpoint 中断 ----
    env1 = dict(base_env)
    env1["TRAINCACHE_JOB_CREATED_AT"] = _rfc3339_now()
    phase1_cmd = ([PY, str(LOAD_TRAIN)] + td_common
                  + ["--interrupt-after-epoch", str(spec["interrupt_after_epoch"])])
    rc, out, err = _run_cmd(phase1_cmd, env=env1)
    if rc != 0:
        reason = f"阶段一失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    if not (ckpt_dir / "checkpoint.json").is_file():
        reason = "阶段一跑完了但没有 checkpoint,续跑无从接起"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    shutil.copyfile(run_dir / "run.json", run_dir / "run_phase1.json")
    print(f"[{run_id}] 阶段一完成({spec['interrupt_after_epoch']} 个 epoch 后中断)")

    # ---- 场景动作(两阶段之间)----
    action_facts: dict = {}
    if spec["action"] == "restart-agentd":
        action_facts = _e2e_restart_agentd()
        (run_dir / "e2e_action.json").write_text(
            json.dumps(action_facts, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8")
        if not action_facts.get("ok"):
            reason = f"agentd 重启失败: {action_facts.get('reason', '')}"
            _write_void(run_dir, reason)
            print(f"[{run_id}] {reason}", file=sys.stderr)
            return run_dir
        print(f"[{run_id}] agentd 已重建并 Ready")

    # ---- 阶段二:从 checkpoint 续跑 ----
    env2 = dict(base_env)
    env2["TRAINCACHE_JOB_CREATED_AT"] = _rfc3339_now()
    phase2_cmd = [PY, str(LOAD_TRAIN)] + td_common + ["--resume"]
    if spec["data_version"] == 2:
        phase2_cmd += ["--url", bench_config.TRAIN_URL_V2]
    rc, out, err = _run_cmd(phase2_cmd, env=env2)
    if rc != 0:
        reason = f"阶段二失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    print(f"[{run_id}] 阶段二完成(续跑到第 {cond['workload']['epochs']} 个 epoch)")

    # ---- 采集 ----
    rc, out, err = _run_cmd([PY, str(COLLECT_PY), "--run-dir", str(run_dir)],
                            env=env2)
    if rc != 0:
        reason = f"collect 失败(rc={rc}): {err.strip()[:500]}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir
    print(f"[{run_id}] 采集完成 → {run_dir / 'metrics.csv'}")

    # ---- 场景记录 + lock ----
    (run_dir / "e2e.json").write_text(json.dumps({
        "scenario": scenario,
        "interrupt_after_epoch": spec["interrupt_after_epoch"],
        "data_version": spec["data_version"],
        "action": spec["action"],
        "action_facts": action_facts,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    try:
        lock_mod.write_lock(run_dir / "lock.json")
        print(f"[{run_id}] lock.json 已写")
    except Exception as exc:  # noqa: BLE001
        reason = f"lock 写入失败: {exc}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir

    return run_dir


def _run_e2e_cond(cond: dict, reps: int) -> list[Path]:
    """e2e 族:逐轮跑两阶段流程,再汇成 summary.csv。"""
    cond_name = cond["name"]
    scenario = _e2e_scenario(cond)
    reps_dir = bench_config.family_results_dir(cond["family"]) / cond_name
    reps_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []
    rows: list[dict] = []
    for i in range(reps):
        run_id = f"{cond_name}-r{i+1:02d}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        print(f"\n===== 第 {i+1}/{reps} 轮({scenario}): {run_id} =====")
        run_dir = _run_e2e_once(cond, run_id, reps_dir, scenario)
        results.append(run_dir)
        rows.append(summarize_run(run_dir, cond, i + 1))
        if (run_dir / "VOID").exists():
            print(f"[{run_id}] 本轮作废(VOID),继续下一轮", file=sys.stderr)

    _attach_ci(rows)
    summary_path = write_summary(reps_dir / "summary.csv", rows)
    print(f"\n[summary] 已写 {summary_path}({len(rows)} 行)")
    return results


def main() -> None:
    ap = argparse.ArgumentParser(
        description="M2 实验 runner:冷态→tc→负载→采集→归档,重复采样")
    ap.add_argument("--cond", help="COND 名(conds/<name>.json)")
    ap.add_argument("--reps", type=int, default=None,
                    help="重复次数(缺省取 COND 的 sampling.reps)")
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


def _write_mock_e2e_run(run_dir: Path, recovery_s: float, submit_lag_s: float,
                        fetch_errors: float, complete: bool = True) -> None:
    """造一轮 e2e mock 产物:两阶段跑完的 run.json + metrics.csv。"""
    run_dir.mkdir(parents=True, exist_ok=True)
    fields = ["cond", "group", "void", "t_submit", "startup_makespan_s",
              "full_makespan_s", "fetch_errors_total", "fetch_errors_recovery"]
    row = {
        "cond": "e2e-mock", "group": "traincache-cold", "void": "false",
        "t_submit": "2026-10-24T00:00:00.000000Z",
        "startup_makespan_s": 10.0, "full_makespan_s": 100.0,
        "fetch_errors_total": fetch_errors, "fetch_errors_recovery": fetch_errors,
    }
    with (run_dir / "metrics.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerow(row)
    (run_dir / "run.json").write_text(json.dumps({
        "run_id": "mock-e2e",
        "complete": complete,
        "resume": {
            "checkpoint_next_epoch": 1,
            "phase1_t_last_step": "2026-10-24T00:01:00.000000Z",
            "phase2_t_submit": "2026-10-24T00:01:05.000000Z",
            "phase2_t_first_step": "2026-10-24T00:01:20.000000Z",
            "resume_submit_lag_s": submit_lag_s,
            "recovery_time_s": recovery_s,
        },
    }), encoding="utf-8")


def selftest() -> None:
    """汇总层自检:列名、四指标口径、CI、rho/exploratory、void 处理。"""
    cond_s = {"name": "mock-s", "group": "traincache",
              "family": "compare", "load": "train",
              "cache": {"enabled": True, "rho": 2.0},
              "environment": {"gateway": False}}
    cond_l = {"name": "mock-l", "group": "traincache",
              "family": "compare", "load": "train",
              "cache": {"enabled": True, "rho": 8.0},
              "environment": {"gateway": False},
              "tags": ["exploratory"]}

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_mock_run(root / "r1", 10.0, 100.0, 30, 10)
        _write_mock_run(root / "r2", 12.0, 110.0, 20, 10)
        _write_mock_run(root / "r3", 999.0, 999.0, 0, 0, void=True)

        rows = [summarize_run(root / "r1", cond_s, 1),
                summarize_run(root / "r2", cond_s, 2),
                summarize_run(root / "r3", cond_s, 3)]
        assert rows[0]["cond"] == "mock-s" and rows[0]["rep"] == 1
        assert rows[0]["rho"] == 2.0, rows[0]
        assert rows[0]["family"] == "compare" and rows[0]["load"] == "train", rows[0]
        assert rows[0]["gateway"] == "false", rows[0]
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
        # void 轮不进统计:平均数只由前两轮算 → (10+12)/2 = 11
        assert rows[0]["startup_makespan_mean"] == 11.0, rows[0]
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
        assert got[0]["cond"] == "mock-s" and got[0]["rho"] == "2.0"
        assert got[2]["void"] == "true"

        # tags 带 exploratory 的格子只报单点值、不报 CI
        lrow = summarize_run(root / "r1", cond_l, 1)
        assert lrow["rho"] == 8.0 and lrow["exploratory"] == "true", lrow
        _attach_ci([lrow])
        assert lrow["startup_makespan_ci_low"] == "" and \
            lrow["startup_makespan_ci_high"] == "", lrow

        # e2e 两阶段:恢复时长、提交间隔、恢复窗口错误数、是否跑完
        cond_e = {"name": "e2e-checkpoint-resume", "group": "traincache-cold",
                  "family": "e2e", "load": "train",
                  "cache": {"enabled": True, "rho": 4.0},
                  "environment": {"gateway": False},
                  "tags": ["e2e", "scenario=checkpoint-resume"]}
        _write_mock_e2e_run(root / "e1", 20.0, 5.0, 0.0)
        _write_mock_e2e_run(root / "e2", 20.0, 5.0, 2.0, complete=False)
        erow = summarize_run(root / "e1", cond_e, 1)
        assert erow["e2e_scenario"] == "checkpoint-resume", erow
        assert erow["e2e_phase1_epochs"] == 1, erow
        assert erow["e2e_recovery_time_s"] == 20.0, erow
        assert erow["e2e_resume_submit_lag_s"] == 5.0, erow
        assert erow["e2e_fetch_errors_recovery"] == 0.0, erow
        assert erow["e2e_complete"] == "true", erow
        erow2 = summarize_run(root / "e2", cond_e, 2)
        assert erow2["e2e_complete"] == "false", erow2
        assert erow2["e2e_fetch_errors_recovery"] == 2.0, erow2
        # 非 e2e 轮的 e2e 列留空
        assert rows[0]["e2e_scenario"] == "" and rows[0]["e2e_complete"] == "", rows[0]

    print("run selftest: PASS")


if __name__ == "__main__":
    main()
