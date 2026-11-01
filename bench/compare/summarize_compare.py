#!/usr/bin/env python3
"""五格对照汇总：按 (格子, 组态) 报平均数与 bootstrap 95% CI，并给组间判定。

口径来源 docs/9.2：§1.6 平均数 + 10,000 次重采样的 95% CI 与胜负判据，
§1.7 作废轮不进汇总；顶替行（`baseline_substituted` 非空）同样剔除。

用法：
    .venv/bin/python bench/compare/summarize_compare.py [--root DIR] [--fallback DIR]
    .venv/bin/python bench/compare/summarize_compare.py --selftest

`--fallback` 只补 root 里缺失的非 traincache 组，合并行标 `source=reused`。
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import numpy as np

BENCH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(BENCH / "experiments"))
import config  # noqa: E402
import stats  # noqa: E402

# (列名, 方向)。low=越低越好, high=越高越好, cost=成本列只报数不判定
METRICS = [
    ("startup_makespan", "low"),
    ("total_makespan", "low"),
    ("post_pod_s", "low"),
    ("stall_ratio", "low"),
    ("stall_p90", "low"),
    ("hit_rate", "high"),
    ("source_saving_ratio", "high"),
    ("hit_ratio_startup", "high"),
    ("hit_ratio_steady", "high"),
    ("hit_ratio_switch", "high"),
    ("startup_warmup_bytes", "cost"),
    ("sustained_prefetch_bytes", "cost"),
    ("switch_reprefetch_bytes", "cost"),
    ("warmup_seconds", "cost"),
]
DIRECTION = dict(METRICS)
GROUP_ORDER = ["direct", "juicefs-cold", "juicefs-warm",
               "fluid-cold", "fluid-warm", "traincache-cold", "traincache-warm"]
CELL_ORDER = ["S-100", "S-300", "S-1000", "M-1000", "L-1000"]
BASELINES = {
    "traincache-cold": ["direct", "fluid-cold"],
    "traincache-warm": ["juicefs-warm", "fluid-warm"],
}
VERDICT_METRICS = ["startup_makespan", "total_makespan", "stall_ratio", "hit_rate"]
TABLE_A = ["startup_makespan", "total_makespan", "post_pod_s", "stall_ratio", "stall_p90"]
TABLE_B = ["hit_rate", "source_saving_ratio",
           "hit_ratio_startup", "hit_ratio_steady", "hit_ratio_switch",
           "hit_ratio_agent_startup", "hit_ratio_agent_steady",
           "hit_ratio_agent_switch",
           "startup_warmup_bytes", "sustained_prefetch_bytes",
           "switch_reprefetch_bytes", "warmup_seconds"]


def _nums(rows: list[dict], metric: str) -> list[float]:
    out = []
    for r in rows:
        v = r.get(metric)
        if v in (None, ""):
            continue
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            continue
    return out


def _read_root(root: Path) -> dict[tuple[str, str], list[dict]]:
    cells: dict[tuple[str, str], list[dict]] = {}
    for path in sorted(root.rglob("summary.csv")):
        parts = path.relative_to(root).parts
        # `_topup*` 是作废轮的替补，算独立有效样本；其余下划线目录
        #（_summary/_probe/_r3/_v010*/_rerun*/_smoke*）是汇总或中间产物，不取。
        hidden = [p for p in parts if p.startswith("_") or p == "quick"]
        if any(not p.startswith("_topup") for p in hidden):
            continue
        if not parts or parts[0] not in CELL_ORDER:
            continue
        cell = parts[0]
        for row in csv.DictReader(open(path)):
            if (row.get("void") or "False").strip().lower() in ("true", "1"):
                continue
            if row.get("baseline_substituted"):
                continue
            if row.get("group") not in GROUP_ORDER:
                continue
            # D23：gateway=1 的行走 +gw 格子，与无网关的同名格子分开统计
            if (row.get("gateway") or "").strip().lower() in ("1", "true"):
                cell_key = f"{cell}+gw"
            else:
                cell_key = cell
            cells.setdefault((cell_key, row["group"]), []).append(row)
    return cells


def read_cells(root: Path, fallback: Path | None = None) -> dict[tuple[str, str], list[dict]]:
    """返回 {(格子, 组态): [有效行]}；下划线目录（汇总/临时）不取。

    `fallback` 只补非 traincache 组：本轮重跑只做 traincache 两组态，
    direct/juicefs/fluid 沿用既有产物（它们不读 TrainCache 快照）。同一
    (格子, 组态) 在 root 已有行时一律用 root，fallback 不覆盖。
    """
    cells = _read_root(root)
    if fallback is None or fallback == root or not fallback.is_dir():
        return cells
    for (cell, group), rows in _read_root(fallback).items():
        if group.startswith("traincache") or (cell, group) in cells:
            continue
        for r in rows:
            r["_source"] = "reused"
        cells[(cell, group)] = rows
    return cells


def _cell_order(observed) -> list[str]:
    """CELL_ORDER 在前，其余观测到的格子（如 `S-100+gw`）按字典序追加。"""
    extra = sorted({c for c in observed if c not in CELL_ORDER})
    return CELL_ORDER + extra


def build_table(cells: dict[tuple[str, str], list[dict]]) -> list[dict]:
    rows = []
    for cell in _cell_order({cell for cell, _g in cells}):
        for group in GROUP_ORDER:
            picked = cells.get((cell, group))
            if picked is None:
                continue
            row = {"cell": cell, "group": group,
                   # `S-100+gw` 与基础格子同规则：L 档仍是探点
                   "status": "exploratory" if cell.startswith("L-") else "ok",
                   "source": "reused" if all(
                       r.get("_source") == "reused" for r in picked) else "new"}
            for m, _d in METRICS:
                vals = _nums(picked, m)
                row[f"{m}_n"] = len(vals)
                row[f"{m}_mean"] = round(stats.mean(vals), 4) if vals else ""
                if len(vals) >= 2:
                    lo, hi = stats.bootstrap_ci(vals, seed=7)
                    row[f"{m}_ci_low"], row[f"{m}_ci_high"] = round(lo, 4), round(hi, 4)
                else:
                    row[f"{m}_ci_low"] = row[f"{m}_ci_high"] = ""
            rows.append(row)
    return rows


def _diff_ci(a: list[float], b: list[float], iters: int = 10000,
             seed: int = 7) -> tuple[float, float]:
    """两组平均数差值的 bootstrap 95% CI（各自有放回重采样）。"""
    aa, bb = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    rng = np.random.default_rng(seed)
    da = aa[rng.integers(0, aa.size, size=(iters, aa.size))].mean(axis=1)
    db = bb[rng.integers(0, bb.size, size=(iters, bb.size))].mean(axis=1)
    diff = da - db
    return (float(np.percentile(diff, 2.5)), float(np.percentile(diff, 97.5)))


def _verdict(direction: str, mg: float, mb: float,
             lo: float, hi: float) -> str:
    if mb == 0:
        return "n/a"
    gain = (mb - mg) / abs(mb) if direction == "low" else (mg - mb) / abs(mb)
    if direction == "low":
        if gain >= 0.05 and hi < 0:
            return "赢"
        if gain <= -0.05 and lo > 0:
            return "输"
    else:
        if gain >= 0.05 and lo > 0:
            return "赢"
        if gain <= -0.05 and hi < 0:
            return "输"
    if lo <= 0 <= hi:
        return "相近"
    return "可忽略"


def build_verdict(cells: dict[tuple[str, str], list[dict]]) -> list[dict]:
    out = []
    for cell in _cell_order({cell for cell, _g in cells}):
        if cell.startswith("L-"):
            continue  # 探点不做判定（9.2 §1.9）
        for group, bases in BASELINES.items():
            grows = cells.get((cell, group))
            if not grows:
                continue
            for base in bases:
                brows = cells.get((cell, base))
                if not brows:
                    continue
                for m in VERDICT_METRICS:
                    gv, bv = _nums(grows, m), _nums(brows, m)
                    if len(gv) < 2 or len(bv) < 2:
                        out.append({"cell": cell, "group": group, "baseline": base,
                                    "metric": m, "verdict": "n<2"})
                        continue
                    mg, mb = stats.mean(gv), stats.mean(bv)
                    lo, hi = _diff_ci(gv, bv)
                    out.append({
                        "cell": cell, "group": group, "baseline": base, "metric": m,
                        "group_mean": round(mg, 4), "baseline_mean": round(mb, 4),
                        "diff": round(mg - mb, 4), "diff_ci_low": round(lo, 4),
                        "diff_ci_high": round(hi, 4),
                        "gain_pct": round((mb - mg) / abs(mb) * 100, 2) if mb else "",
                        "verdict": _verdict(DIRECTION[m], mg, mb, lo, hi),
                    })
    return out


def _cells_txt(row: dict, metric: str) -> str:
    mean = row.get(f"{metric}_mean", "")
    if mean == "":
        return "—"
    n = row.get(f"{metric}_n", 0)
    if n < 2:
        return f"{float(mean):.3f} (n={n})"
    return (f"{float(mean):.3f} "
            f"[{float(row[f'{metric}_ci_low']):.3f}, "
            f"{float(row[f'{metric}_ci_high']):.3f}] (n={n})")


def _render(rows: list[dict], metrics: list[str], title: str) -> list[str]:
    lines = [f"### {title}", "",
             "| 格子 | 组态 | " + " | ".join(metrics) + " |",
             "|---|---|" + "---|" * len(metrics)]
    index = {(r["cell"], r["group"]): r for r in rows}
    for cell in _cell_order({r["cell"] for r in rows}):
        for group in GROUP_ORDER:
            r = index.get((cell, group))
            if not r:
                continue
            txt = [_cells_txt(r, m) for m in metrics]
            tag = " 〔探点〕" if r["status"] == "exploratory" else ""
            gname = group + ("〔沿用〕" if r.get("source") == "reused" else "")
            lines.append(f"| {cell}{tag} | {gname} | " + " | ".join(txt) + " |")
    return lines


def render_markdown(rows: list[dict], verdict: list[dict]) -> str:
    lines = ["## 五格对照汇总", ""]
    if any(r.get("source") == "reused" for r in rows):
        lines += ["注：`〔沿用〕` 组来自既有对照产物（本轮只重跑 traincache 两组态，"
                  "direct/juicefs/fluid 不读快照，口径未变）；其余为重跑结果。", ""]
    if any(str(r.get("cell", "")).endswith("+gw") for r in rows):
        lines += ["注：格子名带 `+gw` 表示该格有网关（`summary.csv` 的 `gateway=1`），"
                  "与同名基础格子分别统计、不混在一起。", ""]
    lines += _render(rows, TABLE_A, "makespan 与 stall")
    lines += [""]
    lines += _render(rows, TABLE_B, "命中率与字节成本")
    lines += ["", "### 组间判定（S/M 档；改善 ≥5% 且差值 CI 不跨 0）", "",
              "| 格子 | 组态 | 基线 | 指标 | 组均值 | 基线均值 | 差值 95% CI | 改善 | 判定 |",
              "|---|---|---|---|---|---|---|---|---|"]
    for v in verdict:
        if "verdict" not in v:
            continue
        if v["verdict"] == "n<2":
            lines.append(f"| {v['cell']} | {v['group']} | {v['baseline']} | "
                         f"{v['metric']} | — | — | — | — | n<2 |")
            continue
        gain = f"{v['gain_pct']:+.1f}%" if v["gain_pct"] != "" else "—"
        lines.append(
            f"| {v['cell']} | {v['group']} | {v['baseline']} | {v['metric']} | "
            f"{v['group_mean']:.3f} | {v['baseline_mean']:.3f} | "
            f"[{v['diff_ci_low']:.3f}, {v['diff_ci_high']:.3f}] | {gain} | {v['verdict']} |")
    return "\n".join(lines) + "\n"


def _flatten(rows: list[dict]) -> list[dict]:
    keys = ["cell", "group", "status", "source"]
    for m, _d in METRICS:
        keys += [f"{m}_n", f"{m}_mean", f"{m}_ci_low", f"{m}_ci_high"]
    return [{k: r.get(k, "") for k in keys} for r in rows]


def selftest() -> None:
    cells = {
        ("S-100", "direct"): [{"startup_makespan": "10"}, {"startup_makespan": "12"},
                              {"startup_makespan": "11"}],
        ("S-100", "traincache-cold"): [{"startup_makespan": "8"}, {"startup_makespan": "8.2"},
                                       {"startup_makespan": "8.1"}],
        ("L-1000", "fluid-cold"): [{"total_makespan": "5"}],
    }
    rows = build_table(cells)
    r0 = [r for r in rows if r["cell"] == "S-100" and r["group"] == "direct"][0]
    assert r0["startup_makespan_n"] == 3 and abs(r0["startup_makespan_mean"] - 11.0) < 1e-9
    lr = [r for r in rows if r["cell"] == "L-1000"][0]
    assert lr["status"] == "exploratory"
    # 差值 CI：明显更低的组应判赢
    verdict = build_verdict(cells)
    hit = [v for v in verdict if v.get("metric") == "startup_makespan"
           and v["baseline"] == "direct" and v["group"] == "traincache-cold"]
    assert hit and hit[0]["verdict"] in ("赢", "相近", "可忽略"), hit
    assert _verdict("low", 8.0, 10.0, -3.0, -1.0) == "赢"
    assert _verdict("low", 10.0, 10.0, -1.0, 1.0) == "相近"
    assert _verdict("high", 0.9, 0.5, 0.1, 0.5) == "赢"

    # fallback：非 traincache 组沿用旧根；traincache 的旧行绝不回流
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        new_root, old_root = Path(td) / "new", Path(td) / "old"
        def _write(root, cell, group, vals):
            p = root / cell / "tag1" / group / "summary.csv"
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["group", "rep", "void",
                                                  "startup_makespan"])
                w.writeheader()
                for i, v in enumerate(vals):
                    w.writerow({"group": group, "rep": i, "void": "false",
                                "startup_makespan": v})
        _write(new_root, "S-100", "traincache-cold", [5, 5, 5])
        _write(old_root, "S-100", "traincache-cold", [99, 99, 99])
        _write(old_root, "S-100", "direct", [10, 11, 12])
        _write(old_root, "S-100", "fluid-cold", [9, 9, 9])
        p = old_root / "S-100" / "_topup" / "fluid-cold" / "summary.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["group", "rep", "void",
                                              "startup_makespan"])
            w.writeheader()
            w.writerow({"group": "fluid-cold", "rep": 0, "void": "false",
                        "startup_makespan": "8"})
        merged = read_cells(new_root, old_root)
        tc = merged[("S-100", "traincache-cold")]
        assert [r["startup_makespan"] for r in tc] == ["5", "5", "5"], tc
        fc = merged[("S-100", "fluid-cold")]
        assert len(fc) == 4, fc  # 主表 3 行 + _topup 替补 1 行
        assert merged[("S-100", "direct")][0].get("_source") == "reused"
        tbl = {(r["cell"], r["group"]): r for r in build_table(merged)}
        assert tbl[("S-100", "traincache-cold")]["source"] == "new"
        assert tbl[("S-100", "direct")]["source"] == "reused"
        assert abs(tbl[("S-100", "direct")]["startup_makespan_mean"] - 11.0) < 1e-9

        # D23：同一 cell 下 gateway=1 与 0 各自独立成行（+gw 格子），均值不混
        gw_root = Path(td) / "gw"
        gp = gw_root / "S-100" / "tag1" / "traincache-cold" / "summary.csv"
        gp.parent.mkdir(parents=True, exist_ok=True)
        with open(gp, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["group", "rep", "void", "gateway",
                                              "startup_makespan"])
            w.writeheader()
            for i, v in enumerate(["10", "10"]):
                w.writerow({"group": "traincache-cold", "rep": i, "void": "false",
                            "gateway": "0", "startup_makespan": v})
            for i, v in enumerate(["4", "6"]):
                w.writerow({"group": "traincache-cold", "rep": i, "void": "false",
                            "gateway": "1", "startup_makespan": v})
        gw_cells = _read_root(gw_root)
        assert set(gw_cells) == {("S-100", "traincache-cold"),
                                 ("S-100+gw", "traincache-cold")}, gw_cells
        gw_tbl = {(r["cell"], r["group"]): r for r in build_table(gw_cells)}
        assert len(gw_tbl) == 2, gw_tbl
        assert abs(gw_tbl[("S-100", "traincache-cold")]["startup_makespan_mean"]
                   - 10.0) < 1e-9
        assert abs(gw_tbl[("S-100+gw", "traincache-cold")]["startup_makespan_mean"]
                   - 5.0) < 1e-9
        gw_md = render_markdown(list(gw_tbl.values()), [])
        assert "S-100+gw" in gw_md and "`+gw`" in gw_md, gw_md
    print("summarize_compare selftest PASS")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=None, help="本轮结果根目录")
    ap.add_argument("--fallback", type=Path, default=None,
                    help="非 traincache 组的既有结果根（默认 m7）")
    ap.add_argument("--no-fallback", action="store_true")
    ap.add_argument("--out-dir", type=Path, default=None)
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return 0

    root = args.root or config.RESULTS_DIR / "m7r3"
    out_dir = args.out_dir or root / "_summary"
    if not root.is_dir():
        print(f"[err] 结果目录不存在: {root}", file=sys.stderr)
        return 1
    fallback = None if args.no_fallback else (
        args.fallback or config.RESULTS_DIR / "m7")
    cells = read_cells(root, fallback)
    rows = build_table(cells)
    if not rows:
        print(f"[err] {root} 下没有有效行", file=sys.stderr)
        return 1
    verdict = build_verdict(cells)

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "m7_baseline.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(_flatten(rows)[0].keys()))
        w.writeheader()
        w.writerows(_flatten(rows))
    if verdict:
        with open(out_dir / "m7_verdict.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(verdict[0].keys()))
            w.writeheader()
            w.writerows(verdict)
    md = render_markdown(rows, verdict)
    (out_dir / "m7_baseline.md").write_text(md)
    print(md)
    print(f"[out] {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
