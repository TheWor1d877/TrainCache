#!/usr/bin/env python3
"""COND 执行体:一个 COND 一轮跑分,重复采样。

按 COND 的 load 字段选执行体,不看名字前缀:
  dataset → compare/run_bench.py(集群内跑分,由它写 summary.csv)
  tilt    → run_policy.py(倾斜双 Job,淘汰×预取)
  train   → e2e 族两阶段流程(集群内训练 Pod 读 agentd 块缓存)

train 负载没有宿主机直读路径:agentd 缓存是节点本地盘,只有同节点的 Pod 读得到,
所以 train 必须带 scenario=* 标签走 e2e 族,否则报错。

e2e 族每轮:
  冷态复位(agentd 停写→清订单/块缓存→恢复 + cold_reset.sh)→ 挂 tc(MinIO)
  → trainpublish 发块到 MinIO → DataSet CR 就绪 → WarmupTask 预热并等 gate
  Released → 提取 WARM_NODE → 阶段一训练 Pod(trainer 镜像,块读,钉预热节点,
  跑 N epoch 写 checkpoint 中断)→ 场景动作 → 阶段二 Pod(同节点同 checkpoint
  续跑,switch_dataset 切 alt DataSet)→ collect.py(Prometheus 按真实 DataSet 名)。
任一步失败:写 VOID 标记,返回目录路径(不抛异常)。
"""
import argparse
import base64
import csv
import io
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

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
COLLECT_PY = EXPERIMENTS_DIR / "collect.py"
RUN_BENCH = BENCH_ROOT / "compare" / "run_bench.py"

PY = sys.executable  # 与 Makefile 的 $(PY) 同口径(系统 3.12);此处用当前解释器

# ---- e2e 族:两阶段跑分 ----
# 每个场景只差三处:中断在第几个 epoch、两阶段之间做什么动作、阶段二是否切到
# 另一份训练数据。其余参数(数据集、epoch 数、带宽、缓存配额)与 COND 相同。
E2E_SCENARIOS = {
    "checkpoint-resume": {
        "interrupt_after_epoch": 1,   # 跑完第 1 个 epoch 写 checkpoint 中断
        "action": None,               # 两阶段之间不动集群
        "switch_dataset": False,      # 阶段二仍读基础数据
    },
    "checkpoint-version": {
        "interrupt_after_epoch": 1,
        "action": None,
        "switch_dataset": True,       # 阶段二切到另一份数据(同形状,种子不同)
    },
    "dual-restart": {
        "interrupt_after_epoch": 1,
        "action": "restart-agentd",   # 训练进程退出后重建 agentd
        "switch_dataset": False,
    },
}
E2E_AGENTD_NS = "traincache"
E2E_AGENTD_LABEL = "app=traincache-agentd"
E2E_AGENTD_READY_TIMEOUT_S = 300

# ---- e2e 集群链路参数(口径与 compare/traincache/up.sh、run_bench.py 对齐)----
E2E_NS = "traincache"
# 专用快照名:train/tars 与 compare 的 dataset-4gib 是两份数据,不能共用一个
# MinIO 快照前缀(发布会互相覆盖)。alt 数据独立发一份,阶段二切它。
E2E_DATASET = os.environ.get("E2E_DATASET", "train-e2e")
E2E_CHUNK_BYTES = 8388608               # 本实验块粒度 8MiB(生产缺省 64MiB),与 up.sh/COND 同口径
E2E_TRAIN_IMAGE = os.environ.get("E2E_TRAIN_IMAGE", "traincache-trainer:dev")
# Pod 名固定:WarmupTask targetRef 按名匹配,ResolvePodIndex 从 -0 后缀解析 index=0
E2E_POD_NAME = "e2e-train-0"
E2E_WARMUP_NAME = "e2e-task"
E2E_POD_TIMEOUT_S = int(os.environ.get("E2E_POD_TIMEOUT_S", "1800"))
E2E_WARMUP_TIMEOUT_S = 900              # up.sh 的 TRAINCACHE_WARMUP_TIMEOUT 同值
E2E_DATASET_READY_TIMEOUT_S = 180       # up.sh 90×2s 同值
E2E_GATE_TMAX_DEFAULT = "10m"           # up.sh 全档统一 10m
E2E_HOST_ROOT = "/var/tmp/traincache-e2e"   # WARM_NODE 上 out/ckpt 的 hostPath 根
E2E_MINIO_NS = "minio"
E2E_OBJSTORE_SECRET = "traincache-objstore"
E2E_AGENTD_PAUSE_KEY = "traincache.io/bench-paused"
# 冷态要清的节点状态:订单 ring + 块缓存 + 隔离区(与 run_bench 同口径)
E2E_NODE_STATE_DIRS = ("/run/traincache/orders",
                       "/var/cache/traincache/blocks",
                       "/var/cache/traincache/quarantine")
# 冷态清缓存落在哪些 kind 节点容器上(与 run_bench.py 同缺省,可用环境变量覆盖)
KIND_WORKER_NODES = [n for n in os.environ.get(
    "KIND_WORKER_NODES",
    "traincache-worker,traincache-worker2,traincache-worker3,traincache-worker4"
).split(",") if n]
TRAINPUBLISH_BIN = SCRIPTS_DIR / "trainpublish"
# 训练 Pod 日志里的产物回传段(base64 tar.gz;完成态 Pod 不能 exec/kubectl cp,
# 日志是唯一不依赖运行中容器的取回通道)
E2E_ART_BEGIN = "===E2E-ARTIFACTS-BEGIN==="
E2E_ART_END = "===E2E-ARTIFACTS-END==="
E2E_CKPT_BEGIN = "===E2E-CKPT-BEGIN==="
E2E_CKPT_END = "===E2E-CKPT-END==="

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
             cwd: Path | None = None, timeout: int | None = None,
             input_text: str | None = None) -> tuple[int, str, str]:
    """执行命令(shell=False 列表形式),返回 (returncode, stdout, stderr)。"""
    proc = subprocess.run(
        cmd, capture_output=True, text=True, env=env, cwd=str(cwd) if cwd else None,
        timeout=timeout, check=False, input=input_text)
    return proc.returncode, proc.stdout, proc.stderr


def _kubectl(args: list[str], ns: str | None = None, timeout: int = 120,
             input_text: str | None = None) -> tuple[int, str, str]:
    """kubectl 列表形式调用(口径同 run_bench.kubectl)。"""
    cmd = ["kubectl"]
    if ns:
        cmd += ["-n", ns]
    cmd += args
    return _run_cmd(cmd, timeout=timeout, input_text=input_text)


def _tc_target(cond: dict) -> str | None:
    """tc_apply.sh 的 --target:全部负载(含 e2e/train)都经 MinIO 取数,
    返回 None 即缺省 MinIO egress 挂点;宿主机本地源站挂点已废弃。"""
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


def run_reps(cond_name: str, reps: int | None = None,
             conds_dir: Path | None = None) -> list[Path]:
    """跑一个 COND;reps=None 取 COND 的 sampling.reps。

    按 load 选执行体:dataset 交给 run_bench.py,tilt 交给 run_policy.py,
    train 走 e2e 族两阶段流程(集群内训练 Pod 读 agentd 块缓存)。
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
    if cond.get("load") == "train":
        raise ValueError(
            f"COND {cond_name}: train 负载必须带 e2e scenario 标签"
            "(训练在集群内 Pod 读 agentd 块缓存,没有宿主机直读路径)")
    raise ValueError(f"COND {cond_name}: 未知 load {cond.get('load')!r}")


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


# ---------------------------------------------------------------------------
# e2e 族:集群内完整生产链路
# (trainpublish → DataSet → WarmupTask/gate → 训练 Pod 块读 → checkpoint 两阶段)
# 控制面 YAML 与节点提取口径照 bench/compare/traincache/up.sh,
# 训练 Pod 形态照 bench/compare/run_bench.py 的 create_measure_pod。
# ---------------------------------------------------------------------------
def _e2e_node_ip() -> str | None:
    """任取一个节点的 InternalIP(trainpublish 经 nodePort 上传用)。"""
    rc, out, _ = _kubectl(
        ["get", "nodes", "-o",
         'jsonpath={.items[0].status.addresses[?(@.type=="InternalIP")].address}'],
        timeout=30)
    if rc != 0 or not out.strip():
        return None
    return out.strip().split()[0]


def _e2e_minio_pod() -> str | None:
    rc, out, _ = _kubectl(
        ["get", "pod", "-l", "app=minio",
         "-o", "jsonpath={.items[0].metadata.name}"],
        ns=E2E_MINIO_NS, timeout=30)
    return out.strip() if rc == 0 and out.strip() else None


def _e2e_mc_cat(minio_pod: str, key: str) -> str | None:
    rc, out, _ = _kubectl(
        ["exec", minio_pod, "--", "mc", "cat",
         f"local/{bench_config.MINIO_BUCKET}/{key}"],
        ns=E2E_MINIO_NS, timeout=120)
    return out if rc == 0 else None


def _e2e_snapshot_shard_count(dataset: str) -> int:
    """快照覆盖的 tar 数(口径同 up.sh snapshot_shard_count)。

    manifest 的 shards[] 是 idx 文件(trainpublish 把多个 tar 打进同一个 idx),
    必须数 idx 里的 distinct path,不能数 manifest 条目。
    """
    pod = _e2e_minio_pod()
    if not pod:
        return 0
    cur = (_e2e_mc_cat(pod, f"dataset/{dataset}/CURRENT") or "").split()
    if not cur:
        return 0
    man_text = _e2e_mc_cat(pod, f"dataset/{dataset}/snapshots/{cur[0]}/manifest.json")
    if not man_text:
        return 0
    try:
        keys = [s["key"] for s in json.loads(man_text).get("shards", [])]
    except (ValueError, KeyError, TypeError):
        return 0
    paths: set[str] = set()
    dec = json.JSONDecoder()
    for key in keys:
        text = _e2e_mc_cat(pod, key) or ""
        i = 0
        while i < len(text):
            while i < len(text) and text[i].isspace():
                i += 1
            if i >= len(text):
                break
            try:
                obj, i = dec.raw_decode(text, i)
            except ValueError:
                break
            for e in obj if isinstance(obj, list) else []:
                if isinstance(e, dict) and e.get("path"):
                    paths.add(e["path"])
    return len(paths)


def _e2e_publish(dataset: str, tar_dir: Path, n_tars: int) -> tuple[bool, str]:
    """trainpublish 把本地 tar 发块到 MinIO(参数口径同 up.sh publish_snapshot)。

    快照已覆盖全部分片就跳过发布(数据不可变,跨 rep 复用)。
    """
    tars = [tar_dir / f"shard-{i:06d}.tar" for i in range(n_tars)]
    missing = [p.name for p in tars if not p.is_file()]
    if missing:
        return False, (f"{tar_dir} 缺 {len(missing)}/{n_tars} 个分片"
                       f"(如 {missing[0]});先跑 make -C bench bench-data")
    pod = _e2e_minio_pod()
    if not pod:
        return False, "MinIO Pod 未就绪,无法发布/核对快照"
    _kubectl(["exec", pod, "--", "mc", "alias", "set", "local",
              "http://localhost:9000", bench_config.MINIO_ACCESS_KEY,
              bench_config.MINIO_SECRET_KEY], ns=E2E_MINIO_NS, timeout=60)
    have = _e2e_snapshot_shard_count(dataset)
    if have >= n_tars:
        return True, f"快照已覆盖 {have} 个分片(need {n_tars}),跳过发布"

    if not os.access(TRAINPUBLISH_BIN, os.X_OK):
        repo_root = BENCH_ROOT.parent
        if not (repo_root / "core" / "cmd" / "trainpublish").is_dir():
            return False, f"找不到 trainpublish 源码({repo_root})"
        print(f"[e2e] 构建 trainpublish → {TRAINPUBLISH_BIN}")
        env = os.environ.copy()
        env["PATH"] = env.get("PATH", "") + ":/usr/local/go/bin"
        rc, _, err = _run_cmd(
            ["go", "build", "-o", str(TRAINPUBLISH_BIN), "./core/cmd/trainpublish"],
            env=env, cwd=repo_root, timeout=600)
        if rc != 0:
            return False, f"trainpublish 构建失败: {err.strip()[:300]}"
    node_ip = _e2e_node_ip()
    if not node_ip:
        return False, "取不到节点 InternalIP,trainpublish 无法经 nodePort 上传"
    # work-dir 落数据层(缺省 /tmp 是 tmpfs,全量快照会写爆;口径同 up.sh)
    work_dir = bench_config.DATA_DIR / "_trainpublish"
    work_dir.mkdir(parents=True, exist_ok=True)
    cmd = [str(TRAINPUBLISH_BIN),
           "--dataset", dataset,
           "--endpoint", f"{node_ip}:{bench_config.MINIO_NODEPORT}",
           "--access-key", bench_config.MINIO_ACCESS_KEY,
           "--secret-key", bench_config.MINIO_SECRET_KEY,
           "--bucket", bench_config.MINIO_BUCKET,
           "--chunk-size", str(E2E_CHUNK_BYTES),
           "--concurrency", "8",
           "--work-dir", str(work_dir)] + [str(p) for p in tars]
    print(f"[e2e] 发布 {n_tars} 个 shard → dataset={dataset} "
          f"(endpoint={node_ip}:{bench_config.MINIO_NODEPORT})")
    rc, _, err = _run_cmd(cmd, timeout=1800)
    if rc != 0:
        return False, f"trainpublish 失败(rc={rc}): {err.strip()[:500]}"
    have = _e2e_snapshot_shard_count(dataset)
    if have < n_tars:
        return False, f"快照只发布 {have}/{n_tars} 个分片"
    return True, f"发布完成({have} 个分片)"


def _e2e_apply_dataset(name: str, tar_dir: Path, n_tars: int) -> tuple[bool, str]:
    """apply DataSet CR 并等 Ready(shards 按实际文件大小算块数,口径同 up.sh)。"""
    shards = []
    for i in range(n_tars):
        f = tar_dir / f"shard-{i:06d}.tar"
        size = f.stat().st_size
        count = (size + E2E_CHUNK_BYTES - 1) // E2E_CHUNK_BYTES
        last = size % E2E_CHUNK_BYTES or E2E_CHUNK_BYTES
        shards.append({"name": f.name, "count": count, "lastChunkSize": last})
    obj = {
        "apiVersion": "traincache.io/v1alpha1",
        "kind": "DataSet",
        "metadata": {"name": name, "namespace": E2E_NS},
        "spec": {
            # agentd 在集群内访问 MinIO,用服务 DNS(去掉 http:// 前缀,同 up.sh)
            "endpoint": bench_config.MINIO_ENDPOINT.split("://", 1)[-1].rstrip("/"),
            "bucket": bench_config.MINIO_BUCKET,
            "datasetName": name,
            "secretRef": {"name": E2E_OBJSTORE_SECRET},
            "chunkSize": E2E_CHUNK_BYTES,
            "shards": shards,
            "shuffle": False,
        },
    }
    rc, _, err = _kubectl(["apply", "-f", "-"], ns=E2E_NS,
                          input_text=yaml.safe_dump(obj, default_flow_style=False))
    if rc != 0:
        return False, f"DataSet/{name} apply 失败: {err.strip()[:300]}"
    # 用 datasets.traincache.io 避免与 Fluid 的 datasets.data.fluid.io 冲突
    deadline = time.monotonic() + E2E_DATASET_READY_TIMEOUT_S
    while time.monotonic() < deadline:
        rc, out, _ = _kubectl(
            ["get", "datasets.traincache.io", name, "-o",
             'jsonpath={.status.conditions[?(@.type=="Ready")].status}'],
            ns=E2E_NS, timeout=30)
        if rc == 0 and out.strip() == "True":
            return True, f"DataSet/{name} Ready({n_tars} shards)"
        time.sleep(2)
    return False, f"DataSet/{name} {E2E_DATASET_READY_TIMEOUT_S}s 内未 Ready"


def _e2e_warmup_k(cond: dict, tar_dir: Path, n_tars: int) -> int:
    """预热块数 = 缓存容量 ÷ 块大小,上限是数据集块数(口径同 run_bench.warmup_k_for)。"""
    total_blocks = 0
    for i in range(n_tars):
        size = (tar_dir / f"shard-{i:06d}.tar").stat().st_size
        total_blocks += (size + E2E_CHUNK_BYTES - 1) // E2E_CHUNK_BYTES
    cache = cond.get("cache") or {}
    cap = cond_mod.cache_bytes(cond) if cache.get("enabled") else (1 << 30)
    return min(max(1, cap // E2E_CHUNK_BYTES), max(1, total_blocks))


def _e2e_warm_node(wt: dict) -> str:
    """预热节点:优先 status.podNodeReadiness[].nodeName,回退 status.assignment
    (口径同 up.sh 的 [WARM_NODE] 提取)。"""
    status = wt.get("status") or {}
    nodes = [e.get("nodeName") for e in status.get("podNodeReadiness") or []
             if e.get("nodeName")]
    if not nodes:
        assignment = status.get("assignment")
        if isinstance(assignment, str) and assignment.strip():
            nodes = [assignment.strip()]
        elif isinstance(assignment, list):
            nodes = [str(a) for a in assignment if a]
    return nodes[0] if nodes else ""


def _e2e_apply_warmup(dataset: str, k: int, tmax: str) -> tuple[bool, str, dict]:
    """apply compare-task 式样的 WarmupTask,等 Released,回传预热节点与事实。

    gate threshold 0.9 / tMax 缺省 10m / fanout 1 / podCount 1,字段与 up.sh 一致。
    """
    _kubectl(["delete", "warmuptask", E2E_WARMUP_NAME,
              "--ignore-not-found", "--wait=true"], ns=E2E_NS, timeout=120)
    obj = {
        "apiVersion": "traincache.io/v1alpha1",
        "kind": "WarmupTask",
        "metadata": {"name": E2E_WARMUP_NAME, "namespace": E2E_NS},
        "spec": {
            "dataSetRef": {"name": dataset},
            "targetRef": {"apiVersion": "v1", "kind": "Pod", "name": E2E_POD_NAME},
            "podCount": 1,
            "preheatFanoutMin": 1,
            "preheatFanout": 1,
            "affinity": "Any",
            "trainingType": "static",
            "podDetails": [{"index": 0, "k": k,
                            "resources": {"requests": {"cpu": "500m",
                                                       "memory": "256Mi"}}}],
            "gate": {"threshold": 0.9, "gapMin": 0.1, "slopeMin": 0.001,
                     "tMax": tmax, "minInterval": "1s"},
            "score": {"halfLife": 4, "cutoff": 8},
            "candidate": {"requireReady": True, "ignoreNoSchedule": False},
        },
    }
    rc, _, err = _kubectl(["apply", "-f", "-"], ns=E2E_NS,
                          input_text=yaml.safe_dump(obj, default_flow_style=False))
    if rc != 0:
        return False, f"WarmupTask apply 失败: {err.strip()[:300]}", {}
    print(f"[e2e] WarmupTask/{E2E_WARMUP_NAME} 已创建(k={k}, tMax={tmax}),等 Released")
    deadline = time.monotonic() + E2E_WARMUP_TIMEOUT_S
    wt: dict = {}
    while time.monotonic() < deadline:
        rc, out, _ = _kubectl(["get", "warmuptask", E2E_WARMUP_NAME, "-o", "json"],
                              ns=E2E_NS, timeout=30)
        if rc == 0:
            try:
                wt = json.loads(out)
            except ValueError:
                wt = {}
        status = wt.get("status") or {}
        phase = status.get("phase") or ""
        gate_state = (status.get("gateDecision") or {}).get("state") or ""
        if phase == "Released" or gate_state == "Released":
            node = _e2e_warm_node(wt)
            if not node:
                return False, "WarmupTask 已 Released 但读不到预热节点", {}
            facts = {
                "warm_node": node,
                "k": k,
                "tmax": tmax,
                "phase": phase,
                "gate_state": gate_state,
                "started_at": status.get("startedAt"),
                "decided_at": (status.get("gateDecision") or {}).get("decidedAt"),
            }
            return True, f"Released,预热节点={node}", facts
        if phase in ("Failed", "Degraded"):
            return False, f"WarmupTask 异常(phase={phase} gate={gate_state})", {}
        time.sleep(2)
    return False, f"WarmupTask {E2E_WARMUP_TIMEOUT_S}s 内未 Released", {}


def _e2e_train_pod(run_id: str, cond: dict, dataset: str, warm_node: str,
                   train_args: list[str], job_created_at: str) -> dict:
    """阶段训练 Pod(形态照 create_measure_pod,镜像换 trainer、命令换 load_train)。

    与 create_measure_pod 的关键差异:
      * /dev/shm 挂 Memory emptyDir(torch DataLoader worker 走 /dev/shm,
        容器缺省 64MB 会挂);
      * out/ckpt 挂 WARM_NODE hostPath,两阶段同节点同目录共享 checkpoint;
      * 命令包一层 sh:训练结束后把 out 目录 tar+base64 打进日志(完成态 Pod
        不能 exec,kubectl cp 取不回产物),checkpoint 存在性与内容一并回传。
    gate 说明:不显式写 schedulingGates——webhook 只对「未 Released 的 Ready
    任务」注入 traincache.io/warmup gate;本流程先等 Released 再造 Pod(与
    compare 相同),此时注入与控制器 removeGates 都已终态,显式挂 gate 会让
    Pod 永久 Pending。label + schedulerName 照 create_measure_pod 保留。
    """
    host_out = f"{E2E_HOST_ROOT}/{run_id}/out"
    host_ckpt = f"{E2E_HOST_ROOT}/{run_id}/ckpt"
    wrapper = (
        f"python3 /app/load_train.py {shlex.join(train_args)}\n"
        "rc=$?\n"
        'if [ "$rc" -ne 0 ]; then exit "$rc"; fi\n'
        'if [ -f /ckpt/checkpoint.json ]; then\n'
        '  echo "[e2e-ckpt] present bytes=$(wc -c < /ckpt/checkpoint.json)"\n'
        "else\n"
        '  echo "[e2e-ckpt] missing"\n'
        "fi\n"
        f'echo "{E2E_ART_BEGIN}"\n'
        "tar czf - -C /out . | base64 -w 76\n"
        f'echo "{E2E_ART_END}"\n'
        'if [ -f /ckpt/checkpoint.json ]; then\n'
        f'  echo "{E2E_CKPT_BEGIN}"\n'
        "  base64 -w 76 /ckpt/checkpoint.json\n"
        f'  echo "{E2E_CKPT_END}"\n'
        "fi\n"
    )
    env = [
        {"name": "PYTHONUNBUFFERED", "value": "1"},
        {"name": "TRAINCACHE_DATASET", "value": dataset},
        {"name": "TRAINCACHE_ENDPOINT", "value": bench_config.MINIO_ENDPOINT},
        {"name": "TRAINCACHE_BUCKET", "value": bench_config.MINIO_BUCKET},
        {"name": "TRAINCACHE_CHUNK_SIZE", "value": str(E2E_CHUNK_BYTES)},
        {"name": "TRAINCACHE_PREFETCH_K",
         "value": str((cond.get("prefetch") or {}).get("lookahead", 16))},
        {"name": "CACHE_ROOT", "value": "/var/cache/traincache/blocks"},
        {"name": "ORDERS_DIR", "value": "/run/traincache/orders"},
        {"name": "TRAINCACHE_HOOK_SOCK", "value": "/run/tc-hook/agent.sock"},
        {"name": "POD_UID",
         "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
        {"name": "TRAINCACHE_JOB_CREATED_AT", "value": job_created_at},
        {"name": "TRAINCACHE_RUN_ID", "value": run_id},
        {"name": "TRAINCACHE_COND", "value": cond.get("name", "")},
        {"name": "TRAINCACHE_GROUP", "value": cond.get("group", "")},
    ]
    container = {
        "name": "train",
        "image": E2E_TRAIN_IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "command": ["sh", "-c"],
        "args": [wrapper],
        "env": env,
        "volumeMounts": [
            {"name": "cache-root", "mountPath": "/var/cache/traincache",
             "readOnly": True},
            {"name": "orders-dir", "mountPath": "/run/traincache/orders"},
            {"name": "hook-dir", "mountPath": "/run/tc-hook"},
            {"name": "out-dir", "mountPath": "/out"},
            {"name": "ckpt-dir", "mountPath": "/ckpt"},
            {"name": "dshm", "mountPath": "/dev/shm"},
        ],
        "resources": {"requests": {"cpu": "500m", "memory": "1Gi"},
                      "limits": {"cpu": "2", "memory": "4Gi"}},
    }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": E2E_POD_NAME,
            "namespace": E2E_NS,
            "labels": {"app": "train-job", "traincache.io/warmup": "enabled"},
        },
        "spec": {
            "restartPolicy": "Never",
            "schedulerName": "traincache-scheduler",
            "nodeSelector": {"kubernetes.io/hostname": warm_node},
            "containers": [container],
            "volumes": [
                {"name": "cache-root",
                 "hostPath": {"path": "/var/cache/traincache", "type": "Directory"}},
                {"name": "orders-dir",
                 "hostPath": {"path": "/run/traincache/orders",
                              "type": "DirectoryOrCreate"}},
                # agentd 的 hook socket 在节点 /run/traincache/agent.sock,
                # 挂到 /run/tc-hook 后 Pod 内即 /run/tc-hook/agent.sock(同 compare)
                {"name": "hook-dir",
                 "hostPath": {"path": "/run/traincache",
                              "type": "DirectoryOrCreate"}},
                {"name": "out-dir",
                 "hostPath": {"path": host_out, "type": "DirectoryOrCreate"}},
                {"name": "ckpt-dir",
                 "hostPath": {"path": host_ckpt, "type": "DirectoryOrCreate"}},
                # torch DataLoader worker 经 /dev/shm 传张量,缺省 64MB 会挂
                {"name": "dshm",
                 "emptyDir": {"medium": "Memory", "sizeLimit": "2Gi"}},
            ],
        },
    }


def _e2e_wait_pod(timeout_s: int) -> tuple[str, str]:
    """等训练 Pod 到终态,返回 (phase, logs);超时返回 ("Timeout", 尾部日志)。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rc, out, err = _kubectl(
            ["get", "pod", E2E_POD_NAME, "-o", "jsonpath={.status.phase}"],
            ns=E2E_NS, timeout=30)
        if rc != 0 and "NotFound" in err:
            return "Deleted", ""
        phase = out.strip()
        if phase in ("Succeeded", "Failed"):
            _, logs, _ = _kubectl(["logs", E2E_POD_NAME], ns=E2E_NS, timeout=120)
            return phase, logs
        time.sleep(5)
    _, logs, _ = _kubectl(["logs", E2E_POD_NAME, "--tail=200"],
                          ns=E2E_NS, timeout=60)
    return "Timeout", logs


def _e2e_extract_artifacts(logs: str, run_dir: Path) -> tuple[bool, str]:
    """从 Pod 日志取回 out 目录(tar.gz base64)与 checkpoint,落 run_dir。

    完成态 Pod 不能 exec(kubectl cp 依赖运行中容器),日志回传是唯一不依赖
    容器存活的取回通道。只解普通文件成员,拒绝绝对路径与 .. 越界。
    """
    lines = logs.splitlines()

    def section(begin: str, end: str) -> str | None:
        try:
            i = lines.index(begin)
            j = lines.index(end, i + 1)
        except ValueError:
            return None
        return "".join(lines[i + 1:j])

    art = section(E2E_ART_BEGIN, E2E_ART_END)
    if art is None:
        return False, "Pod 日志缺产物回传段(训练可能未正常收尾)"
    try:
        blob = base64.b64decode(art)
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                name = os.path.normpath(member.name)
                if os.path.isabs(name) or name.startswith(".."):
                    continue
                dest = run_dir / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                src = tf.extractfile(member)
                if src is None:
                    continue
                with dest.open("wb") as fout:
                    shutil.copyfileobj(src, fout)
    except (ValueError, tarfile.TarError, OSError) as exc:
        return False, f"产物解包失败: {exc}"
    ckpt = section(E2E_CKPT_BEGIN, E2E_CKPT_END)
    if ckpt is not None:
        try:
            (run_dir / "ckpt").mkdir(parents=True, exist_ok=True)
            (run_dir / "ckpt" / "checkpoint.json").write_bytes(base64.b64decode(ckpt))
        except (ValueError, OSError) as exc:
            return False, f"checkpoint 解包失败: {exc}"
    return True, ""


def _e2e_run_phase(cond: dict, run_id: str, run_dir: Path, phase: int,
                   dataset: str, warm_node: str,
                   extra_args: list[str]) -> tuple[bool, str]:
    """造一个阶段训练 Pod,等完成,日志与产物落 run_dir。返回 (ok, reason)。"""
    n_tars = int(cond["dataset"]["shards"])
    # --chunk-bytes 显式给 8MiB:load_train 用它算 K 块→分片窗口
    # (window_shards),进而定 switch 的 ready_at。config.TRAIN_CHUNK_BYTES
    # 缺省 64MiB 是生产块粒度;本实验发块用 8MiB,不显式覆盖会把窗口放大 8×,
    # 污染切换段回源口径。
    train_args = (cond_mod.cond_to_train_args(cond)
                  + ["--n-tars", str(n_tars),
                     "--chunk-bytes", str(E2E_CHUNK_BYTES),
                     "--out-dir", "/out", "--checkpoint-dir", "/ckpt"]
                  + extra_args)
    job_created_at = _rfc3339_now()
    pod = _e2e_train_pod(run_id, cond, dataset, warm_node, train_args, job_created_at)
    _kubectl(["delete", "pod", E2E_POD_NAME, "--ignore-not-found",
              "--grace-period=0", "--force", "--wait=true"], ns=E2E_NS, timeout=120)
    rc, _, err = _kubectl(["apply", "-f", "-"], ns=E2E_NS,
                          input_text=yaml.safe_dump(pod, default_flow_style=False))
    if rc != 0:
        return False, f"阶段{phase} Pod 创建失败: {err.strip()[:300]}"
    print(f"[{run_id}] 阶段{phase} Pod 已提交(dataset={dataset}, node={warm_node}, "
          f"t_submit={job_created_at})")
    phase_state, logs = _e2e_wait_pod(E2E_POD_TIMEOUT_S)
    (run_dir / f"phase{phase}.log").write_text(logs, encoding="utf-8")
    if phase_state != "Succeeded":
        _, state, _ = _kubectl(
            ["get", "pod", E2E_POD_NAME, "-o",
             "jsonpath={range .status.containerStatuses[*]}{.name} "
             "restarts={.restartCount} exit={.state.terminated.exitCode} "
             "reason={.state.terminated.reason}{end}"], ns=E2E_NS, timeout=30)
        (run_dir / f"phase{phase}_state.txt").write_text(
            f"phase={phase_state}\ncontainer_state: {state.strip()}\n",
            encoding="utf-8")
        return False, (f"阶段{phase} Pod {phase_state}"
                       f"(现场见 phase{phase}.log / phase{phase}_state.txt)")
    ok, reason = _e2e_extract_artifacts(logs, run_dir)
    if not ok:
        return False, f"阶段{phase} {reason}"
    if not (run_dir / "run.json").is_file():
        return False, f"阶段{phase} 产物缺 run.json"
    return True, ""


def _e2e_agentd_set_active(active: bool) -> None:
    """挂起/恢复 agentd DaemonSet(nodeSelector 挂起,口径同 run_bench)。"""
    value = None if active else "true"
    patch = json.dumps({"spec": {"template": {"spec": {
        "nodeSelector": {E2E_AGENTD_PAUSE_KEY: value}}}}})
    rc, _, err = _kubectl(["patch", "ds/traincache-agentd", "--type=merge",
                           "-p", patch], ns=E2E_AGENTD_NS, timeout=60)
    if rc != 0:
        raise RuntimeError(f"无法{'恢复' if active else '挂起'} agentd: "
                           f"{err.strip()[:300]}")


def _e2e_agentd_counts() -> tuple[int, int]:
    """agentd (Pod 总数, Ready 数);kubectl 失败返回 (0, 0)。"""
    rc, out, _ = _kubectl(["get", "pods", "-l", E2E_AGENTD_LABEL, "-o", "json"],
                          ns=E2E_AGENTD_NS, timeout=30)
    if rc != 0:
        return 0, 0
    try:
        items = json.loads(out).get("items", [])
    except ValueError:
        return 0, 0
    ready = sum(1 for p in items
                if any(c.get("type") == "Ready" and c.get("status") == "True"
                       for c in p.get("status", {}).get("conditions", []) or []))
    return len(items), ready


def _e2e_reset_node_state() -> tuple[bool, str]:
    """冷态第一层:停 agentd(无写入者)→ 清各节点订单 ring 与块/隔离缓存 →
    判空(5s 落定复查)→ 恢复并等 Ready。口径同 run_bench.reset_traincache_agent
    的停写→清→判空→恢复骨架;复位必须在清缓存前,否则在途预取会污染判空。
    """
    expected = len(KIND_WORKER_NODES)
    try:
        _e2e_agentd_set_active(False)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            total, _ = _e2e_agentd_counts()
            if total == 0:
                break
            time.sleep(2)
        else:
            return False, "agentd 挂起后 120s 内 Pod 未全部消失"
        dirs = " ".join(E2E_NODE_STATE_DIRS)
        reset_cmd = (f"for d in {dirs}; do "
                     '[ -d "$d" ] && find "$d" -mindepth 1 -maxdepth 1 '
                     "-exec rm -rf {} +; done; true")
        check_cmd = f"for d in {dirs}; do [ -d \"$d\" ] && ls -A \"$d\"; done; true"
        for node in KIND_WORKER_NODES:
            for attempt in range(3):
                rc, _, err = _run_cmd(["docker", "exec", node, "sh", "-c", reset_cmd],
                                      timeout=60)
                if rc != 0:
                    print(f"[e2e] WARNING: 节点 {node} 清状态失败: {err.strip()[:200]}",
                          file=sys.stderr)
                rc, out, _ = _run_cmd(["docker", "exec", node, "sh", "-c", check_cmd],
                                      timeout=60)
                if rc == 0 and not out.strip():
                    # 判空可能落在写入间隙:等 5s 复查,真有活写入者必须看得见
                    time.sleep(5)
                    rc2, out2, _ = _run_cmd(
                        ["docker", "exec", node, "sh", "-c", check_cmd], timeout=60)
                    if rc2 == 0 and not out2.strip():
                        break
                if attempt < 2:
                    time.sleep(5)
            else:
                return False, f"节点 {node} 的订单/块缓存未能清空,冷态不成立"
    except RuntimeError as exc:
        return False, str(exc)
    finally:
        try:
            _e2e_agentd_set_active(True)
        except RuntimeError as exc:
            print(f"[e2e] WARNING: 恢复 agentd 失败: {exc}", file=sys.stderr)
    deadline = time.monotonic() + E2E_AGENTD_READY_TIMEOUT_S
    while time.monotonic() < deadline:
        total, ready = _e2e_agentd_counts()
        if total >= expected and ready >= expected:
            return True, ""
        time.sleep(2)
    return False, f"agentd 恢复后 {E2E_AGENTD_READY_TIMEOUT_S}s 内未全部 Ready"


def _run_e2e_once(cond: dict, run_id: str, reps_dir: Path,
                  scenario: str) -> Path:
    """e2e 一轮:冷态 → tc → 发块/DataSet/WarmupTask → 阶段一(中断)→ 场景动作
    → 阶段二(续跑)→ 采集。

    两阶段共用同一个产物目录与 WARM_NODE 上同一份 out/ckpt hostPath:阶段二的
    run.json 记全程口径(t_submit 取阶段一),per_step 追加在同一份 jsonl 上,
    阶段一的 run.json 另存 run_phase1.json 留档。
    """
    spec = E2E_SCENARIOS.get(scenario)
    if spec is None:
        raise ValueError(f"未知 e2e 场景: {scenario!r}")
    reps_dir = Path(reps_dir)
    run_dir = reps_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    n_tars = int(cond["dataset"]["shards"])
    dataset = E2E_DATASET
    dataset_alt = f"{E2E_DATASET}-alt"

    def void(reason: str) -> Path:
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        _kubectl(["delete", "pod", E2E_POD_NAME, "--ignore-not-found",
                  "--grace-period=0", "--force"], ns=E2E_NS, timeout=60)
        return run_dir

    # 数据前置:基础与(切换场景的)alt 分片不在就整轮作废,不进半截流程
    for tar_dir, hint in [(bench_config.TRAIN_DIR, "make -C bench bench-data")] + (
            [(bench_config.TRAIN_DIR_ALT, "make -C bench bench-data-alt")]
            if spec["switch_dataset"] else []):
        missing = [i for i in range(n_tars)
                   if not (tar_dir / f"shard-{i:06d}.tar").is_file()]
        if missing:
            return void(f"训练数据未就绪:{tar_dir} 缺 {len(missing)}/{n_tars} "
                        f"个分片;先跑 {hint}")

    # ---- 冷态:先停 agentd 清订单/块缓存,再 cold_reset.sh(页缓存+MinIO+tc)----
    ok, reason = _e2e_reset_node_state()
    if not ok:
        return void(f"冷态复位失败: {reason}")
    rc, out, err = _run_cmd(["bash", str(COLD_RESET_SH)])
    if rc != 0:
        return void(f"cold_reset 失败(rc={rc}): {err.strip()[:500]}")
    print(f"[{run_id}] 冷态完成(agentd 已复位,节点订单/块缓存已清)")

    # ---- 挂 tc:e2e 走 MinIO egress(_tc_target 对 train 也返回 None)----
    tc_args = cond_mod.cond_to_tc_args(cond)
    target = _tc_target(cond)
    tc_cmd = ["bash", str(TC_APPLY_SH)] + tc_args
    if target:
        tc_cmd += ["--target", target]
    rc, out, err = _run_cmd(tc_cmd)
    if rc != 0:
        return void(f"tc_apply 失败(rc={rc}): {err.strip()[:500]}")
    print(f"[{run_id}] tc 已挂载: {' '.join(tc_args)}(MinIO egress)")

    # ---- 发块 + DataSet CR(基础;切换场景另发 alt)----
    publish_facts: dict = {}
    ok, detail = _e2e_publish(dataset, bench_config.TRAIN_DIR, n_tars)
    if not ok:
        return void(f"基础数据发布失败: {detail}")
    publish_facts[dataset] = detail
    ok, detail = _e2e_apply_dataset(dataset, bench_config.TRAIN_DIR, n_tars)
    if not ok:
        return void(detail)
    print(f"[{run_id}] {detail}")
    if spec["switch_dataset"]:
        ok, detail = _e2e_publish(dataset_alt, bench_config.TRAIN_DIR_ALT, n_tars)
        if not ok:
            return void(f"切换数据发布失败: {detail}")
        publish_facts[dataset_alt] = detail
        ok, detail = _e2e_apply_dataset(dataset_alt, bench_config.TRAIN_DIR_ALT, n_tars)
        if not ok:
            return void(detail)
        print(f"[{run_id}] {detail}")

    # ---- WarmupTask:预热 + gate 放行,提取 WARM_NODE ----
    cond_env = cond_mod.cond_to_env(cond)
    tmax = cond_env.get("TRAINCACHE_GATE_TMAX") or E2E_GATE_TMAX_DEFAULT
    k = _e2e_warmup_k(cond, bench_config.TRAIN_DIR, n_tars)
    ok, detail, warm_facts = _e2e_apply_warmup(dataset, k, tmax)
    if not ok:
        return void(f"预热失败: {detail}")
    warm_node = warm_facts["warm_node"]
    print(f"[{run_id}] 预热 {detail}(k={k})")

    base_env = os.environ.copy()
    base_env.update(cond_env)
    base_env["TRAINCACHE_RUN_ID"] = run_id

    # ---- 阶段一:跑够 N 个 epoch 写 checkpoint 中断 ----
    ok, reason = _e2e_run_phase(
        cond, run_id, run_dir, 1, dataset, warm_node,
        ["--interrupt-after-epoch", str(spec["interrupt_after_epoch"])])
    if not ok:
        return void(reason)
    if not (run_dir / "ckpt" / "checkpoint.json").is_file():
        return void("阶段一跑完了但没有 checkpoint,续跑无从接起")
    shutil.copyfile(run_dir / "run.json", run_dir / "run_phase1.json")
    print(f"[{run_id}] 阶段一完成({spec['interrupt_after_epoch']} 个 epoch 后中断,"
          f"checkpoint 已留档)")

    # ---- 场景动作(两阶段之间)----
    action_facts: dict = {}
    if spec["action"] == "restart-agentd":
        action_facts = _e2e_restart_agentd()
        (run_dir / "e2e_action.json").write_text(
            json.dumps(action_facts, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8")
        if not action_facts.get("ok"):
            return void(f"agentd 重启失败: {action_facts.get('reason', '')}")
        print(f"[{run_id}] agentd 已重建并 Ready")

    # ---- 阶段二:同节点同 checkpoint 续跑;切换场景读 alt DataSet ----
    phase2_dataset = dataset_alt if spec["switch_dataset"] else dataset
    ok, reason = _e2e_run_phase(cond, run_id, run_dir, 2, phase2_dataset,
                                warm_node, ["--resume"])
    if not ok:
        return void(reason)
    print(f"[{run_id}] 阶段二完成(续跑到第 {cond['workload']['epochs']} 个 epoch)")

    # ---- 采集(Prometheus 按真实 DataSet 名;Pod 仍在,collect 取 Pod 事实)----
    collect_cmd = [PY, str(COLLECT_PY), "--run-dir", str(run_dir),
                   "--warmup-task", E2E_WARMUP_NAME, "--pod", E2E_POD_NAME,
                   "--chunk-bytes", str(E2E_CHUNK_BYTES)]
    if os.environ.get("PROMETHEUS_URL", "").strip():
        collect_cmd += ["--prometheus-url", os.environ["PROMETHEUS_URL"].strip(),
                        "--dump-series"]
    rc, out, err = _run_cmd(collect_cmd, env=base_env)
    _kubectl(["delete", "pod", E2E_POD_NAME, "--ignore-not-found",
              "--grace-period=0", "--force"], ns=E2E_NS, timeout=60)
    if rc != 0:
        return void(f"collect 失败(rc={rc}): {err.strip()[:500]}")
    print(f"[{run_id}] 采集完成 → {run_dir / 'metrics.csv'}")

    # ---- 场景记录 + lock ----
    (run_dir / "e2e.json").write_text(json.dumps({
        "scenario": scenario,
        "interrupt_after_epoch": spec["interrupt_after_epoch"],
        "switch_dataset": spec["switch_dataset"],
        "action": spec["action"],
        "action_facts": action_facts,
        "dataset": dataset,
        "dataset_phase2": phase2_dataset,
        "warm_node": warm_node,
        "warmup": warm_facts,
        "publish": publish_facts,
        "image": E2E_TRAIN_IMAGE,
        "pod_name": E2E_POD_NAME,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    try:
        lock_mod.write_lock(run_dir / "lock.json")
        print(f"[{run_id}] lock.json 已写")
    except Exception as exc:  # noqa: BLE001
        return void(f"lock 写入失败: {exc}")

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
        description="COND 执行体:按 load 分派(dataset/tilt/train e2e),重复采样")
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
