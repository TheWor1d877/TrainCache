#!/usr/bin/env python3
"""淘汰矩阵(族 `evict-policy`)的三档七列出表入口(4.4 §5.3)。

`plot.py` 的图函数只吃 `fig1-/fig2-/fig3-/pareto1-/pareto2-` 前缀,
`summarize.py` 只读 `scores/` 与 `baselines/`,`policy/policy-*` 过去没有消费者。
本脚本读 `results/policy/policy-<策略>/<run ID>/summary.csv`,过 `row_gate.py`
的三道过滤,按 §5.3 固定的七列出 `results/summary/policy.csv` 与 `policy.md`,
并配 §1.10 末条的「预注册假设 vs 实测 vs 判定」三栏。

每档另给一条 §1.7 的判定:淘汰是否生效、热分片有没有被逐、目录峰值有没有
压在声明容量之上。判据一、判据二不过的那一档只作诊断,不进结论;判据三按
§1.7 原文不作废轮次,越界的档仍在结论里、判定栏带越界旁注。

用法:
  .venv/bin/python bench/experiments/policy_table.py
  .venv/bin/python bench/experiments/policy_table.py --root <results/policy> --out-dir <目录>
  .venv/bin/python bench/experiments/policy_table.py --selftest
"""
import argparse
import csv
import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(BENCH / "experiments"))
import config  # noqa: E402
import row_gate  # noqa: E402

POLICY_CONDS = ("policy-heat", "policy-lru", "policy-lfu")
# §5.3 固定的七列;a_steps/tilt_a_budget 是一列两个字段
METRICS = ("hit_rate", "a_steps", "tilt_a_budget", "b_evictions_of_hot",
           "stall_ratio", "total_makespan", "evict_blocks_run",
           "cache_dir_peak_bytes")
EVICT_INTERVAL_S = 30          # agent/evictor/evictor.go 的 defaultInterval
SAMPLE_PERIOD_NOMINAL_S = 5.0  # run_bench.py 的 SAMPLE_INTERVAL_S
PEAK_SLACK_MAX = 1.16          # §1.7 判据三的标称上界(节拍 30 s + 采样 5 s)


def _f(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def peak_bound(row: dict) -> float:
    """§1.7 判据三的上界,用产物里的实际采样节拍回代。

    式子 (淘汰节拍 + 采样周期) × 有效带宽 ÷ 声明容量,再对 1 取和;乘的是
    本轮档位的有效带宽,不是标称线速。
    """
    cap = _f(row.get("cache_bytes"))
    bw = config.TILT_EFF_BW_MB_S.get(config.rate_mbit(row.get("rate")))
    period = _f(row.get("cache_dir_peak_period_s")) or SAMPLE_PERIOD_NOMINAL_S
    if not cap or not bw:
        return PEAK_SLACK_MAX
    return 1 + (EVICT_INTERVAL_S + period) * bw * 1e6 / cap


def evidence_verdict(row: dict) -> str:
    """§1.7 三条判据的逐档判定;通过给「进结论」,判据三越界的带诊断旁注。"""
    evicted = _f(row.get("evict_blocks_run"))
    hot = _f(row.get("b_evictions_of_hot"))
    peak = _f(row.get("cache_dir_peak_bytes"))
    cap = _f(row.get("cache_bytes"))
    if evicted is None or hot is None or peak is None or cap is None:
        return "缺淘汰证据"
    if evicted <= 0:
        return "只作诊断:淘汰未触发(判据一)"
    if hot > 0:
        return f"只作诊断:热分片被逐 {hot:.0f} 次(判据二)"
    ratio = peak / cap
    bound = peak_bound(row)
    if ratio > bound:
        return (f"进结论(诊断:目录峰值 {ratio:.3f} > 上界 {bound:.3f},"
                "判据三只作诊断)")
    return "进结论"


def in_conclusion(row: dict) -> bool:
    """判据三越界的档仍进结论,所以按前缀判,不写全等。"""
    return str(row.get("verdict", "")).startswith("进结论")


def read_policy_runs(root: Path, version: str | None,
                     rejected: list[dict] | None = None) -> dict[str, list[dict]]:
    """{COND: [通过过滤的 run 行]},按 §5.3 的三档顺序。

    三档全缺和没有这个点目录是一回事，给空表比 KeyError 更适合出表；
    缺哪一档由 `build_table` 的「无数据」行报出来。
    """
    out: dict[str, list[dict]] = {c: [] for c in POLICY_CONDS}
    root = Path(root)
    for cond in POLICY_CONDS:
        for summary in sorted((root / cond).glob("*/summary.csv")):
            rows, rej = row_gate.read_gated(summary, version)
            if rej and rejected is not None:
                rejected.extend(rej)
            out[cond].extend(rows)
    return out


def build_table(runs: dict[str, list[dict]]) -> list[dict]:
    rows = []
    for cond in POLICY_CONDS:
        group = runs.get(cond) or []
        row = {"cond": cond, "n": len(group)}
        row.update({m: _mean([_f(r.get(m)) for r in group]) for m in METRICS})
        first = group[0] if group else {}
        row["cache_bytes"] = _f(first.get("cache_bytes"))
        row["peak_ratio"] = ((row["cache_dir_peak_bytes"] / row["cache_bytes"])
                             if row.get("cache_dir_peak_bytes")
                             and row.get("cache_bytes") else None)
        row["verdict"] = evidence_verdict(first) if group else "无数据"
        rows.append(row)
    return rows


def spread(a, b) -> float | None:
    """两档均值的相对极差;取不到给 None。"""
    if a is None or b is None or max(abs(a), abs(b)) == 0:
        return None
    return abs(a - b) / max(abs(a), abs(b))


def hypothesis_table(table: list[dict], threshold: float = 0.05) -> list[dict]:
    """§5.3 的预注册假设 vs 实测:三档两两比较的最大相对差。"""
    premise = f"三档差异 ≤{threshold:.0%}(§5.3;每点 1 次采样,只判方向)"
    out = []
    for metric in ("hit_rate", "total_makespan", "stall_ratio",
                   "b_evictions_of_hot", "evict_blocks_run"):
        conds = [r for r in table if r.get(metric) is not None
                 and in_conclusion(r)]
        if len(conds) < 2:
            out.append({"metric": metric, "premise": premise,
                        "observed": "进结论的档不足两个",
                        "verdict": "样本不足"})
            continue
        worst, worst_pair = 0.0, ""
        for i in range(len(conds)):
            for j in range(i + 1, len(conds)):
                s = spread(conds[i][metric], conds[j][metric])
                if s is not None and s > worst:
                    worst, worst_pair = s, f"{conds[i]['cond']} vs {conds[j]['cond']}"
        observed = f"最大相对差 {worst:.1%}（{worst_pair}）"
        verdict = ("与假设一致" if worst <= threshold
                   else "方向性偏离（单次采样，不判胜负）")
        out.append({"metric": metric, "premise": premise,
                    "observed": observed, "verdict": verdict})
    return out


def _fmt(value, fmt="{:.4f}") -> str:
    return fmt.format(value) if isinstance(value, (int, float)) else "—"


def render_markdown(table: list[dict], hyp: list[dict],
                    rejected: list[dict]) -> str:
    lines = ["# 淘汰矩阵（族 `evict-policy`，§5.3 三档七列）", ""]
    lines.append("| COND | n | hit_rate | a_steps/tilt_a_budget | "
                 "b_evictions_of_hot | stall_ratio | total_makespan | "
                 "evict_blocks_run | cache_dir_peak_bytes | 峰值/声明容量 | §1.7 判定 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in table:
        lines.append(
            f"| `{r['cond']}` | {r['n']} | {_fmt(r.get('hit_rate'))} | "
            f"{_fmt(r.get('a_steps'), '{:.0f}')}/"
            f"{_fmt(r.get('tilt_a_budget'), '{:.0f}')} | "
            f"{_fmt(r.get('b_evictions_of_hot'), '{:.0f}')} | "
            f"{_fmt(r.get('stall_ratio'))} | "
            f"{_fmt(r.get('total_makespan'), '{:.1f}')} | "
            f"{_fmt(r.get('evict_blocks_run'), '{:.0f}')} | "
            f"{_fmt(r.get('cache_dir_peak_bytes'), '{:.0f}')} | "
            f"{_fmt(r.get('peak_ratio'), '{:.3f}')} | {r['verdict']} |")
    lines += ["", "## 预注册假设 vs 实测", "",
              "| 指标 | 预注册假设 | 实测 | 判定 |",
              "|---|---|---|---|"]
    for h in hyp:
        lines.append(f"| `{h['metric']}` | {h['premise']} | {h['observed']} | "
                     f"{h['verdict']} |")
    if rejected:
        lines += ["", f"读侧排除 {len(rejected)} 行"
                      "（拒因分布见 `results/summary/rejected.csv`）。"]
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="淘汰矩阵三档七列出表")
    ap.add_argument("--root", type=Path, default=None,
                    help="淘汰矩阵分区(默认 results/policy)")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="出表目录(默认 results/summary)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return 0

    root = args.root or config.RESULTS_DIR / "policy"
    out_dir = args.out_dir or config.RESULTS_DIR / "summary"
    if not root.is_dir():
        print(f"[err] 分区不存在: {root}", file=sys.stderr)
        return 1
    version = row_gate.current_version()
    rejected: list[dict] = []
    runs = read_policy_runs(root, version, rejected)
    row_gate.print_rejects(rejected, config.RESULTS_DIR)
    table = build_table(runs)
    hyp = hypothesis_table(table)

    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "policy.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["cond", "n", "cache_bytes",
                                          "peak_ratio", "verdict", *METRICS])
        w.writeheader()
        w.writerows(table)
    (out_dir / "policy.md").write_text(
        render_markdown(table, hyp, rejected), encoding="utf-8")
    if rejected:
        row_gate.write_reject_log(config.RESULTS_DIR / "summary" / "rejected.csv",
                                  rejected)
    print(f"[out] {out_dir}/policy.csv、policy.md"
          f"（{sum(1 for r in table if in_conclusion(r))}/3 档进结论）")
    return 0


def selftest() -> None:
    """一行合格 run 出表、七列齐、两条淘汰证据非空；缺陷期行被拒。"""
    import tempfile

    # 2026-11-02 取证实跑的读数(~/tcd/probes/m5-gate-20261102/policy/policy-heat/
    # policy-heat-r01-20261102T113658Z/summary.csv)，按 §5.3 的列抄在这里。
    probe = {"group": "traincache-cold", "cond": "policy-heat",
             "rate": "100mbit", "void": "False", "status": "ok",
             "hit_rate": "0.9409", "a_steps": "1200", "tilt_a_budget": "1200",
             "b_evictions_of_hot": "0", "stall_ratio": "0.4762",
             "total_makespan": "5510.067", "fetch_bytes_run": "25200000000",
             "evict_blocks_run": "2793", "cache_dir_peak_bytes": "2935277824",
             "cache_bytes": "2576980377", "cache_dir_peak_period_s": "30.0"}

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "policy"
        run_dir = root / "policy-heat" / "policy-heat-r01-20261102T113658Z"
        run_dir.mkdir(parents=True)
        (run_dir / "version").write_text("v-selftest\n", encoding="utf-8")
        with (run_dir / "summary.csv").open("w", newline="",
                                            encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(probe.keys()))
            w.writeheader()
            w.writerow(probe)

        rejected: list[dict] = []
        runs = read_policy_runs(root, "v-selftest", rejected)
        table = build_table(runs)
        assert rejected == [] and len(table) == 3
        heat = table[0]
        assert heat["cond"] == "policy-heat" and heat["n"] == 1
        assert all(m in heat for m in METRICS), "七列不齐"
        assert heat["evict_blocks_run"] == 2793.0
        assert heat["cache_dir_peak_bytes"] == 2935277824.0
        assert heat["verdict"] == "进结论", heat["verdict"]
        # 峰值 1.139 对 30 s 实测节拍回代的上界 (30+30)×11.95 MB/s ÷ 2 576 980 377
        assert abs(heat["peak_ratio"] - 1.139) < 0.001, heat["peak_ratio"]
        assert abs(peak_bound(probe) - (1 + 60 * 11.95e6 / 2576980377)) < 1e-9
        assert table[1]["verdict"] == "无数据" and table[1]["n"] == 0

        md = render_markdown(table, hypothesis_table(table), [])
        for col in ("hit_rate", "a_steps/tilt_a_budget", "b_evictions_of_hot",
                    "stall_ratio", "total_makespan", "evict_blocks_run",
                    "cache_dir_peak_bytes"):
            assert col in md, f"表头缺列 {col}"

        # 缺陷期行(2026-11-01)缺 §1.7 三列，第一道就被拒，不进表
        bad = {"group": "traincache-cold", "cond": "policy-lru",
               "rate": "100mbit", "void": "False", "status": "ok",
               "hit_rate": "0.9", "total_makespan": "824.36",
               "fetch_bytes_run": "13146929152"}
        legacy_dir = root / "policy-lru" / "policy-lru-r01-20261101T051843Z"
        legacy_dir.mkdir(parents=True)
        # 戳给成当前版本，拒因才能落在缺证据列而不是缺戳
        (legacy_dir / "version").write_text("v-selftest\n", encoding="utf-8")
        with (legacy_dir / "summary.csv").open("w", newline="",
                                               encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(bad.keys()))
            w.writeheader()
            w.writerow(bad)
        rejected = []
        runs = read_policy_runs(root, "v-selftest", rejected)
        assert len(rejected) == 1, rejected
        assert rejected[0]["reason"] == "missing_eviction_evidence"
        assert runs["policy-lru"] == []
        assert build_table(runs)[1]["verdict"] == "无数据"

    # 取证跑那条 run 在数据层外，本机有就当真实输入再走一遍：
    # 落戳的那条进表，没落戳的那条被第一道挡掉。
    probe_root = Path.home() / "tcd/probes/m5-gate-20261102/policy"
    if probe_root.is_dir():
        rejected = []
        runs = read_policy_runs(probe_root, "probe", rejected)
        assert len(runs["policy-heat"]) == 1, runs
        assert [r["reason"] for r in rejected] == ["no_version_stamp"], rejected
        assert build_table(runs)[0]["verdict"] == "进结论"
    print("policy_table selftest PASS")


if __name__ == "__main__":
    sys.exit(main())
