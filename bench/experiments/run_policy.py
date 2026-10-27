#!/usr/bin/env python3
"""M5 实验 runner:淘汰与预取协同实验(C 轨倾斜场景)。

流程(run_once):
  1. 创建产物目录 results/policy/<cond_name>/<run_id>/
  2. 注入 COND 环境变量(TRAINCACHE_EVICT_POLICY /
     TRAINCACHE_PREFETCH_LOOKAHEAD / TRAINCACHE_CACHE_CAPACITY_BYTES)
  3. 真实模式:调对照层 run_bench.py,带 TRAINCACHE_TILT=1。
     冷态、tc、measure Pod、采集都由它完成;倾斜双 Job 跑在同一个 Pod 里,
     走真实 agent 与 hostPath 缓存。
  4. selftest:mock 下载,不访问网络
  5. 写 lock.json(lock.py)

产物目录隔离:bench/results/policy/<cond_name>/<run_id>/
selftest 完全 mock,不访问网络、不启动真实训练。
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

# 同级导入 cond / lock
sys.path.insert(0, str(Path(__file__).resolve().parent))
import cond as cond_mod  # noqa: E402
import lock as lock_mod  # noqa: E402

EXPERIMENTS_DIR = Path(__file__).resolve().parent
BENCH_ROOT = EXPERIMENTS_DIR.parent
SCRIPTS_DIR = BENCH_ROOT / "scripts"
sys.path.insert(0, str(BENCH_ROOT))
import config as bench_config  # noqa: E402

RESULTS_DIR = bench_config.RESULTS_DIR
POLICY_RESULTS_DIR = RESULTS_DIR / "policy"

BENCH_TRACK_C = SCRIPTS_DIR / "bench_track_c.py"
RUN_BENCH = BENCH_ROOT / "compare" / "run_bench.py"

PY = sys.executable

# bench_track_c 模块(selftest 时直接复用其 Downloader / run_job_a /
# run_job_b / compute_eviction_stats,保证 mock 逻辑与真实一致)
_btc = None


def _import_btc():
    """延迟导入 bench_track_c,避免污染 sys.path。"""
    global _btc
    if _btc is not None:
        return _btc
    sys.path.insert(0, str(SCRIPTS_DIR))
    import bench_track_c as btc  # noqa: E402
    _btc = btc
    return btc


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


def _run_cmd(cmd: list, env: dict | None = None,
             cwd: Path | None = None, timeout: int | None = None
             ) -> tuple[int, str, str]:
    """执行命令(shell=False 列表形式),返回 (returncode, stdout, stderr)。"""
    proc = subprocess.run(
        cmd, capture_output=True, text=True, env=env,
        cwd=str(cwd) if cwd else None, timeout=timeout, check=False)
    return proc.returncode, proc.stdout, proc.stderr


# ---------------------------------------------------------------------------
# Mock 实验(selftest 专用,完全复用 bench_track_c 的组件)
# ---------------------------------------------------------------------------
def _run_mock_track_c(cond: dict, run_dir: Path, env: dict) -> tuple[dict, dict]:
    """用 mock 模式跑一轮 C 轨双 Job 实验,产物写入 run_dir。

    复用 bench_track_c.Downloader / run_job_a / run_job_b /
    compute_eviction_stats,保证逻辑与真实一致。
    返回 (run_obj, eviction_stats)。
    """
    btc = _import_btc()
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    # 缓存目录隔离在本轮产物下,避免与其他实验混淆
    cache_dir = run_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # 合成清单:8 个分片,其中 2 个热分片(25%,比真实 10% 更易观测挤占)
    all_shards = [f"shard-{i:06d}.tar" for i in range(8)]
    hot_shards = all_shards[:2]
    full_shards = all_shards

    # 容量:只够放 3 个分片(37.5%),保证 B 扫描时会挤出 A 的热分片
    shard_bytes = 1 << 20  # 1MiB(selftest 用小体积,省时间)
    capacity_bytes = 3 * shard_bytes

    dl = btc.Downloader(
        "mock", url_tmpl="http://x/shard-%06d.tar",
        cache_dir=cache_dir, capacity_bytes=capacity_bytes,
        shard_bytes=shard_bytes, mock_io_s=0.005)

    stop_event = threading.Event()
    a_path = run_dir / "per_step_a.jsonl"
    b_path = run_dir / "per_step_b.jsonl"

    # A 跑 15 轮热分片;B 跑 2 轮全量;B 延迟 0.01s 启动,保证并发交错
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_a = pool.submit(
            btc.run_job_a, "A", hot_shards, 15, 1, dl, a_path, 1234,
            stop_event)
        time.sleep(0.01)
        fut_b = pool.submit(
            btc.run_job_b, "B", full_shards, 2, 1, dl, b_path, stop_event)
        sa = fut_a.result()
        sb = fut_b.result()

    # ---- run.json ----
    first_steps = [t for t in (sa.get("first_step_at"),
                               sb.get("first_step_at")) if t]
    last_steps = [t for t in (sa.get("last_step_at"),
                              sb.get("last_step_at")) if t]
    run_obj = {
        "schema_version": "1",
        "run_id": env.get("TRAINCACHE_RUN_ID"),
        "cond": env.get("TRAINCACHE_COND"),
        "group": env.get("TRAINCACHE_GROUP"),
        "track": "C",
        "started_at": btc.utc_now(),
        "finished_at": btc.utc_now(),
        "t_submit": env.get("TRAINCACHE_JOB_CREATED_AT", btc.utc_now()),
        "t_submit_source": "env",
        "t_first_step": min(first_steps) if first_steps else None,
        "t_last_step": max(last_steps) if last_steps else None,
        "switch_events": (sa.get("switch_events", [])
                          + sb.get("switch_events", [])),
        "phase_policy": {
            "phases": ["startup", "steady", "switch", "scan"],
        },
        "capacity_bytes": capacity_bytes,
        "hot_shard_count": len(hot_shards),
        "full_shard_count": len(full_shards),
        "n_steps_a": sa["n_steps"],
        "n_steps_b": sb["n_steps"],
        "selftest": True,
        "evict_policy": cond["cache"]["evict_policy"],
        "lookahead": cond.get("prefetch", {}).get("lookahead"),
    }
    (run_dir / "run.json").write_text(
        json.dumps(run_obj, ensure_ascii=False, indent=2) + "\n")

    # ---- eviction_stats.json ----
    ev = btc.compute_eviction_stats(sa, len(hot_shards), len(full_shards))
    (run_dir / "eviction_stats.json").write_text(
        json.dumps(ev, ensure_ascii=False, indent=2) + "\n")

    return run_obj, ev


# ---------------------------------------------------------------------------
# 单轮执行
# ---------------------------------------------------------------------------
def run_once(cond: dict, run_id: str, reps_dir: Path,
             selftest: bool = False) -> Path:
    """跑一轮实验,返回本轮产物目录路径。

    selftest=True:完全 mock,跳过 cold_reset / tc_apply / collect,
    用 bench_track_c 的 mock 下载器生成产物。
    任一步失败:写 VOID 标记,记录原因,返回目录路径(不抛异常)。
    """
    reps_dir = Path(reps_dir)
    run_dir = reps_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # ---- 注入环境变量 ----
    job_created_at = _rfc3339_now()
    base_env = os.environ.copy()
    cond_env = cond_mod.cond_to_env(cond)
    base_env.update(cond_env)
    base_env["TRAINCACHE_JOB_CREATED_AT"] = job_created_at
    base_env["TRAINCACHE_RUN_ID"] = run_id

    if not selftest:
        # ---- 2~4. 冷态/挂 tc/跑负载统一交给对照层 run_bench.py ----
        # 倾斜双 Job 跑在同一个 measure Pod 里（TRAINCACHE_TILT=1），
        # 走真实 TrainCache agent 与 hostPath 缓存。
        env = base_env.copy()
        env.update({
            "TRAINCACHE_FIGURE_MODE": "1",
            "TRAINCACHE_TILT": "1",
            "TRAINCACHE_COND_NAME": cond["name"],
            # 两个旋钮分开：训练侧消费窗口固定 16，Agent 侧 lookahead 取 COND。
            "TRAINCACHE_PREFETCH_K": "16",
            "TRAINCACHE_PREFETCH_LOOKAHEAD": str(cond["prefetch"]["lookahead"]),
            "N_TARS": str(cond["dataset"]["shards"]),
            "EPOCHS": str(cond["workload"]["epochs"]),
            "STEPS_PER_EPOCH": str(cond["workload"]["steps_per_epoch"]),
            "BATCH": str(cond["workload"]["batch"]),
            "SHUFFLE": cond["workload"]["shuffle"],
            "SEED": str(cond["dataset"]["seed"]),
            "DATASET_NAME": cond["dataset"]["name"],
            "MINIO_BUCKET": cond["dataset"]["bucket"],
            "TC_RATE": f"{cond['network']['rate_mbit']}mbit",
            "TC_DELAY": f"{cond['network']['delay_ms']}ms",
        })
        rb_cmd = [PY, str(RUN_BENCH),
                  "--tier", "S",
                  "--rate", f"{cond['network']['rate_mbit']}mbit",
                  "--groups", cond["group"],
                  "--reps", "1",
                  "--out", str(run_dir / "summary.csv")]
        rc, out, err = _run_cmd(rb_cmd, env=env)
        if rc != 0:
            reason = f"run_bench 失败(rc={rc}): {err.strip()[:500]}"
            _write_void(run_dir, reason)
            print(f"[{run_id}] {reason}", file=sys.stderr)
            return run_dir
        print(f"[{run_id}] 倾斜双 Job 完成")
    else:
        # ---- selftest:mock 实验 ----
        print(f"[{run_id}] selftest:mock 模式,跳过 cold_reset/tc_apply")
        try:
            run_obj, ev = _run_mock_track_c(cond, run_dir, base_env)
        except Exception as exc:  # noqa: BLE001
            reason = f"mock 实验失败: {exc}"
            _write_void(run_dir, reason)
            print(f"[{run_id}] {reason}", file=sys.stderr)
            return run_dir
        print(f"[{run_id}] selftest mock 完成:"
              f" A hits={ev['a_hits']} misses={ev['a_misses']} "
              f"evictions={ev['b_evictions_of_hot']} "
              f"hit_ratio={ev['a_hit_ratio']:.2%}")

    # ---- 5. 采集 ----
    # 真实模式的指标由 run_bench.py 直接写 summary.csv；mock 模式已在上面
    # 落 eviction_stats.json，不再跑 collect.py。

    # ---- 6. 写 lock.json ----
    try:
        lock_mod.write_lock(run_dir / "lock.json")
        print(f"[{run_id}] lock.json 已写")
    except Exception as exc:  # noqa: BLE001
        reason = f"lock 写入失败: {exc}"
        _write_void(run_dir, reason)
        print(f"[{run_id}] {reason}", file=sys.stderr)
        return run_dir

    return run_dir


# ---------------------------------------------------------------------------
# 多轮执行
# ---------------------------------------------------------------------------
def run_reps(cond_name: str, reps: int | None = None,
             conds_dir: Path | None = None,
             selftest: bool = False) -> list[Path]:
    """跑 reps 轮。cond_name 传给 load_cond。

    reps=None 时取 COND 的 sampling.reps(D18:淘汰矩阵每点 3 次)。
    selftest=True 时:reps 强制为 1,使用 mock 模式。
    返回每轮产物目录列表。
    """
    cond = cond_mod.load_cond(cond_name, conds_dir)
    errors = cond_mod.validate_cond(cond)
    if errors:
        raise ValueError(f"COND {cond_name} 校验失败:\n"
                         + "\n".join(errors))

    if selftest:
        reps = 1
    elif reps is None:
        reps = cond["sampling"]["reps"]

    reps_dir = POLICY_RESULTS_DIR / cond_name
    reps_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []
    for i in range(reps):
        run_id = (f"{cond_name}-r{i+1:02d}-"
                  f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
        print(f"\n===== 第 {i+1}/{reps} 轮: {run_id} =====")
        run_dir = run_once(cond, run_id, reps_dir, selftest=selftest)
        results.append(run_dir)
        void_marker = run_dir / "VOID"
        if void_marker.exists():
            print(f"[{run_id}] 本轮作废(VOID),继续下一轮", file=sys.stderr)
    return results


# ---------------------------------------------------------------------------
# selftest:跑一个策略的 mock 实验并校验产物
# ---------------------------------------------------------------------------
def selftest(cond_name: str = "policy-heat") -> int:
    """跑一个策略的 mock 实验,校验产物齐全且字段正确。返回 0=通过。"""
    print(f"run_policy selftest: cond={cond_name}")

    # 1. COND 能加载且校验通过
    cond = cond_mod.load_cond(cond_name)
    errs = cond_mod.validate_cond(cond)
    assert not errs, f"COND 校验失败: {errs}"
    print(f"  COND 加载与校验通过: evict_policy={cond['cache']['evict_policy']} "
          f"lookahead={cond['prefetch']['lookahead']} "
          f"capacity_bytes={cond['cache']['capacity_bytes']}")

    # 2. cond_to_env 正确设置环境变量
    env = cond_mod.cond_to_env(cond)
    assert env["TRAINCACHE_EVICT_POLICY"] == cond["cache"]["evict_policy"], \
        f"TRAINCACHE_EVICT_POLICY 错误: {env.get('TRAINCACHE_EVICT_POLICY')}"
    assert env["TRAINCACHE_PREFETCH_LOOKAHEAD"] == str(
        cond["prefetch"]["lookahead"]), \
        f"TRAINCACHE_PREFETCH_LOOKAHEAD 错误"
    assert env["TRAINCACHE_CACHE_CAPACITY_BYTES"] == str(
        cond["cache"]["capacity_bytes"]), \
        f"TRAINCACHE_CACHE_CAPACITY_BYTES 错误"
    print(f"  环境变量映射正确: "
          f"TRAINCACHE_EVICT_POLICY={env['TRAINCACHE_EVICT_POLICY']} "
          f"TRAINCACHE_PREFETCH_LOOKAHEAD={env['TRAINCACHE_PREFETCH_LOOKAHEAD']} "
          f"TRAINCACHE_CACHE_CAPACITY_BYTES={env['TRAINCACHE_CACHE_CAPACITY_BYTES']}")

    # 3. 跑 mock 实验
    results = run_reps(cond_name, reps=1, selftest=True)
    assert len(results) == 1
    run_dir = results[0]

    # 4. 校验产物文件齐全
    expected = ["run.json", "per_step_a.jsonl", "per_step_b.jsonl",
                "eviction_stats.json"]
    for fname in expected:
        fpath = run_dir / fname
        assert fpath.is_file(), f"selftest 失败:缺少产物 {fpath}"
    print(f"  产物齐全: {', '.join(expected)}")

    # 5. 校验产物内容格式
    run_obj = json.loads((run_dir / "run.json").read_text())
    assert run_obj["track"] == "C"
    assert run_obj["cond"] == cond_name
    assert run_obj["selftest"] is True
    assert run_obj["evict_policy"] == cond["cache"]["evict_policy"]

    a_recs = [json.loads(ln) for ln
              in (run_dir / "per_step_a.jsonl").read_text().splitlines()
              if ln.strip()]
    assert a_recs, "per_step_a.jsonl 为空"
    for r in a_recs:
        assert {"step", "wait_s", "step_s", "shard", "phase", "hit"}.issubset(
            r.keys()), f"per_step_a 字段缺失: {r.keys()}"

    b_recs = [json.loads(ln) for ln
              in (run_dir / "per_step_b.jsonl").read_text().splitlines()
              if ln.strip()]
    assert b_recs, "per_step_b.jsonl 为空"
    for r in b_recs:
        assert {"step", "wait_s", "step_s", "shard", "phase"}.issubset(
            r.keys()), f"per_step_b 字段缺失: {r.keys()}"

    ev = json.loads((run_dir / "eviction_stats.json").read_text())
    for k in ("a_hits", "a_misses", "a_hit_ratio", "a_wait_ratio",
              "b_evictions_of_hot", "hot_shard_count", "full_shard_count"):
        assert k in ev, f"eviction_stats.json 缺字段 {k}"
    # selftest 容量仅 3 片、B 扫 8 片,应观测到挤占
    assert ev["b_evictions_of_hot"] > 0, \
        "selftest 应观测到挤占(b_evictions_of_hot > 0)"

    print(f"  产物格式正确: A steps={len(a_recs)} B steps={len(b_recs)} "
          f"evictions={ev['b_evictions_of_hot']} "
          f"hit_ratio={ev['a_hit_ratio']:.2%}")

    # 6. 产物目录隔离:路径包含 policy/<cond_name>/<run_id>
    assert "policy" in run_dir.parts, f"产物目录未隔离到 policy/: {run_dir}"
    assert cond_name in run_dir.parts, \
        f"产物目录未包含 cond_name: {run_dir}"
    print(f"  产物目录隔离正确: {run_dir}")

    print(f"\nrun_policy selftest: PASS (产物目录: {run_dir})")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="M5 实验 runner:淘汰与预取协同实验(C 轨倾斜场景)")
    ap.add_argument("--cond", default="policy-heat",
                    help="COND 名(conds/<name>.json,默认 policy-heat)")
    ap.add_argument("--reps", type=int, default=None,
                    help="重复次数(默认取 COND 的 sampling.reps;"
                         "显式传入时以命令行为准;selftest 时强制为 1)")
    ap.add_argument("--conds-dir", type=Path, default=None,
                    help="COND 目录(默认 bench/experiments/conds)")
    ap.add_argument("--selftest", action="store_true",
                    help="自检测试:mock 模式跑一个策略,不访问网络")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(selftest(args.cond))

    run_reps(args.cond, args.reps, args.conds_dir, selftest=False)


if __name__ == "__main__":
    main()
