#!/usr/bin/env python3
"""探针开销对账:开/关两遍 run 目录 → wait_s P99 差值(主口径)+ 不变量校验。

主口径取两遍 per_step_*.jsonl 里 wait_s 的 P99 差值,单位 ms。wait_s 是取数等待
时长(load_train.py 写的一行一条),探针挂在块 I/O 与缺页路径上,代价落在这一项上。
load 侧不改,统计全在这里补。

交叉核对用 metrics.csv 的 full_makespan_s 差值与全轮 median_stall 差值。
不变量用 total_fetch_bytes 与 hit_ratio_steady:探针只读,这两项两遍之间应当
基本一致;变了说明两遍不可比,开销数据作废。
"""
import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stats as stats_mod  # noqa: E402


def load_records(run_dir: Path) -> list[dict]:
    """读 run 目录里全部 per_step_*.jsonl 的记录。"""
    recs: list[dict] = []
    for path in sorted(run_dir.glob("per_step_*.jsonl")):
        for line in path.read_text().splitlines():
            line = line.strip()
            if line:
                recs.append(json.loads(line))
    return recs


def stall(rec: dict) -> float:
    """单步 stall:wait_s / step_s(口径同 load_train.py 的 stall)。"""
    step_s = rec.get("step_s") or 0.0
    return rec.get("wait_s", 0.0) / step_s if step_s > 0 else 0.0


def read_metrics_csv(run_dir: Path) -> dict:
    """读 metrics.csv 的第一行(collect.py 的输出)。没有文件返回空字典。"""
    path = run_dir / "metrics.csv"
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            return dict(row)
    return {}


def _num(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def summarize(run_dir: Path) -> dict:
    """一轮 run 的取数侧摘要:wait_s P99、median_stall、步数、metrics.csv 关键列。"""
    run_dir = Path(run_dir)
    recs = load_records(run_dir)
    meas = [r for r in recs if not r.get("warmup")]
    waits = [float(r.get("wait_s") or 0.0) for r in meas]
    metrics = read_metrics_csv(run_dir)
    probe_metrics = run_dir / "metrics.json"
    return {
        "run_dir": str(run_dir),
        "n_steps_total": len(recs),
        "n_steps_measured": len(meas),
        "wait_s_p99_ms": stats_mod.p99(waits) * 1000.0 if waits else None,
        "wait_s_mean_ms": stats_mod.mean(waits) * 1000.0 if waits else None,
        "median_stall": stats_mod.median([stall(r) for r in meas]) if meas else None,
        "full_makespan_s": _num(metrics.get("full_makespan_s")),
        "total_fetch_bytes": _num(metrics.get("total_fetch_bytes")),
        "hit_ratio_steady": _num(metrics.get("hit_ratio_steady")),
        "probe_metrics_present": probe_metrics.is_file(),
    }


def compare(off: dict, on: dict, fetch_tol_pct: float, hit_tol: float) -> dict:
    """两遍摘要 → 差值与不变量校验。"""
    def delta(key: str) -> float | None:
        a, b = off.get(key), on.get(key)
        if a is None or b is None:
            return None
        return b - a

    invariants: dict[str, dict] = {}
    fb_off, fb_on = off.get("total_fetch_bytes"), on.get("total_fetch_bytes")
    if fb_off is None or fb_on is None:
        invariants["total_fetch_bytes"] = {"ok": None, "reason": "metrics.csv 缺该列或值为空"}
    else:
        rel = 0.0 if fb_off == 0 else abs(fb_on - fb_off) / fb_off * 100.0
        invariants["total_fetch_bytes"] = {
            "off": fb_off, "on": fb_on, "rel_diff_pct": rel, "ok": rel <= fetch_tol_pct,
        }
    hr_off, hr_on = off.get("hit_ratio_steady"), on.get("hit_ratio_steady")
    if hr_off is None or hr_on is None:
        invariants["hit_ratio_steady"] = {"ok": None, "reason": "metrics.csv 缺该列或值为空"}
    else:
        diff = abs(hr_on - hr_off)
        invariants["hit_ratio_steady"] = {
            "off": hr_off, "on": hr_on, "abs_diff": diff, "ok": diff <= hit_tol,
        }

    checked = [v["ok"] for v in invariants.values() if v["ok"] is not None]
    comparable = all(checked) if checked else None
    return {
        "wait_s_p99_delta_ms": delta("wait_s_p99_ms"),
        "full_makespan_s_delta": delta("full_makespan_s"),
        "median_stall_delta": delta("median_stall"),
        "invariants": invariants,
        "comparable": comparable,
    }


def _fmt(value, unit: str = "", digits: int = 3) -> str:
    if value is None:
        return "不可核对"
    return f"{value:.{digits}f}{unit}"


def print_report(off: dict, on: dict, cmp_result: dict) -> None:
    print("== 探针开销对账 ==")
    print(f"关探针: {off['run_dir']}")
    print(f"  wait_s P99 = {_fmt(off['wait_s_p99_ms'], ' ms')}  步数 = {off['n_steps_measured']}"
          f"  median_stall = {_fmt(off['median_stall'])}")
    print(f"开探针: {on['run_dir']}")
    print(f"  wait_s P99 = {_fmt(on['wait_s_p99_ms'], ' ms')}  步数 = {on['n_steps_measured']}"
          f"  median_stall = {_fmt(on['median_stall'])}")
    print(f"  traintrace 指标文件: {'有' if on['probe_metrics_present'] else '无'}")
    print("== 差值 ==")
    print(f"  wait_s P99 差值(主口径) = {_fmt(cmp_result['wait_s_p99_delta_ms'], ' ms')}")
    print(f"  full_makespan_s 差值(交叉核对) = {_fmt(cmp_result['full_makespan_s_delta'], ' s')}")
    print(f"  median_stall 差值(交叉核对) = {_fmt(cmp_result['median_stall_delta'])}")
    print("== 不变量 ==")
    for name, item in cmp_result["invariants"].items():
        if item["ok"] is None:
            print(f"  {name}: 不可核对({item.get('reason')})")
        else:
            print(f"  {name}: {'一致' if item['ok'] else '不一致'}"
                  f"  {json.dumps({k: v for k, v in item.items() if k != 'ok'}, ensure_ascii=False)}")
    if cmp_result["comparable"] is True:
        print("判定:两遍可比,开销数据可用")
    elif cmp_result["comparable"] is False:
        print("判定:不变量不一致,两遍不可比,开销数据作废")
    else:
        print("判定:不变量列缺失,可比性未核对")


def _write_mock_run(run_dir: Path, waits: list[float], step_s: float,
                    fetch_bytes: float | None, hit_ratio: float | None) -> None:
    """造一轮 mock 产物:per_step jsonl + 可选 metrics.csv。"""
    run_dir.mkdir(parents=True, exist_ok=True)
    with (run_dir / "per_step_1_w4.jsonl").open("w", encoding="utf-8") as f:
        for i, w in enumerate(waits):
            f.write(json.dumps({"epoch": 0, "step_in_epoch": i, "global_step": i,
                                 "warmup": False, "phase": "steady",
                                 "wait_s": w, "step_s": step_s}) + "\n")
    fields = ["cond", "void", "full_makespan_s", "total_fetch_bytes", "hit_ratio_steady"]
    row = {"cond": "mock", "void": "false", "full_makespan_s": 10.0,
           "total_fetch_bytes": fetch_bytes, "hit_ratio_steady": hit_ratio}
    with (run_dir / "metrics.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerow(row)


def selftest() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        off_dir = root / "off"
        on_dir = root / "on"
        _write_mock_run(off_dir, [0.1] * 100, 0.2, 1000.0, 0.5)
        _write_mock_run(on_dir, [0.1] * 99 + [0.3], 0.2, 1000.0, 0.5)
        off = summarize(off_dir)
        on = summarize(on_dir)
        assert off["n_steps_measured"] == 100
        assert on["n_steps_measured"] == 100
        assert off["wait_s_p99_ms"] is not None and on["wait_s_p99_ms"] is not None
        assert on["wait_s_p99_ms"] > off["wait_s_p99_ms"]
        result = compare(off, on, fetch_tol_pct=1.0, hit_tol=0.02)
        assert result["wait_s_p99_delta_ms"] > 0
        assert result["comparable"] is True
        assert result["invariants"]["total_fetch_bytes"]["ok"] is True
        assert result["invariants"]["hit_ratio_steady"]["ok"] is True

        # 不变量越界 → 判定不可比
        _write_mock_run(on_dir, [0.1] * 100, 0.2, 2000.0, 0.5)
        bad = compare(summarize(off_dir), summarize(on_dir), fetch_tol_pct=1.0, hit_tol=0.02)
        assert bad["comparable"] is False

        # 缺 metrics.csv → 不变量不可核对
        _write_mock_run(on_dir, [0.1] * 100, 0.2, None, None)
        (on_dir / "metrics.csv").unlink()
        missing = compare(summarize(off_dir), summarize(on_dir), fetch_tol_pct=1.0, hit_tol=0.02)
        assert missing["comparable"] is None

    print("probe_overhead selftest: PASS")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="探针开销对账:开/关两遍 run 目录 → wait_s P99 差值 + 不变量校验")
    ap.add_argument("--off", type=Path, help="没挂探针那遍的 run 目录")
    ap.add_argument("--on", type=Path, help="挂了探针那遍的 run 目录")
    ap.add_argument("--out", type=Path, help="结果 JSON 路径(缺省打印到终端)")
    ap.add_argument("--fetch-tol-pct", type=float, default=1.0,
                    help="total_fetch_bytes 相对差容差(%%)")
    ap.add_argument("--hit-tol", type=float, default=0.02,
                    help="hit_ratio_steady 绝对差容差")
    ap.add_argument("--selftest", action="store_true", help="跑纯本地自检后退出")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return
    if not args.off or not args.on:
        ap.error("需要 --off 与 --on(或用 --selftest 跑自检)")

    off = summarize(args.off)
    on = summarize(args.on)
    result = compare(off, on, args.fetch_tol_pct, args.hit_tol)
    print_report(off, on, result)
    payload = {"off": off, "on": on, **result}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8")
        print(f"结果已写: {args.out}")


if __name__ == "__main__":
    main()