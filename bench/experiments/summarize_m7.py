#!/usr/bin/env python3
"""M7 五格基线汇总：按 (格子, 组态) 报平均数与 bootstrap 95% CI。

口径来源：docs/9.2 §1.6（平均数 + 10,000 次重采样的 95% CI）与 D14
（作废轮不进汇总）。顶替行（`baseline_substituted` 非空）默认剔除——它是
Fluid 起不来时拿 JuiceFS 顶这个格的记录，不是该组态自己的测量。

用法：
    .venv/bin/python bench/experiments/summarize_m7.py            # 打印 + 落 CSV/MD
    .venv/bin/python bench/experiments/summarize_m7.py --selftest
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(BENCH / "experiments"))
import config  # noqa: E402
import stats  # noqa: E402

# 四个正式指标（docs/9.2 §1.2）：makespan/stall 越低越好，hit_rate 越高越好
METRICS = ["startup_makespan", "total_makespan", "stall_ratio", "hit_rate"]
GROUP_ORDER = ["direct", "juicefs-cold", "juicefs-warm",
               "fluid-cold", "fluid-warm", "traincache-cold", "traincache-warm"]
CELL_ORDER = ["S-100", "S-300", "S-1000", "M-1000", "L-1000"]


def _nums(rows: list[dict], metric: str) -> list[float]:
    out = []
    for r in rows:
        try:
            out.append(float(r[metric]))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def read_cells(root: Path) -> dict[tuple[str, str], list[dict]]:
    """返回 {(格子, 组态): [有效行]}；_topup* 并入本格，其余下划线与 quick 产物不取。"""
    cells: dict[tuple[str, str], list[dict]] = {}
    for path in sorted(root.rglob("summary.csv")):
        parts = path.relative_to(root).parts
        if any((p.startswith("_") and not p.startswith("_topup"))
               or p == "quick" for p in parts):
            continue
        cell = parts[0]
        for row in csv.DictReader(open(path)):
            if (row.get("void") or "False").strip().lower() == "true":
                continue
            if row.get("baseline_substituted"):
                continue
            if row.get("group") not in GROUP_ORDER:
                continue
            cells.setdefault((cell, row["group"]), []).append(row)
    return cells


def build_table(cells: dict[tuple[str, str], list[dict]]) -> list[dict]:
    rows = []
    for cell in CELL_ORDER:
        for group in GROUP_ORDER:
            picked = cells.get((cell, group))
            if picked is None:
                continue
            row = {"cell": cell, "group": group, "n": len(picked),
                   "status": "exploratory" if cell.startswith("L-") else "ok"}
            for m in METRICS:
                vals = _nums(picked, m)
                lo, hi = stats.bootstrap_ci(vals, seed=7) if len(vals) >= 2 else ("", "")
                row[f"{m}_mean"] = stats.mean(vals) if vals else ""
                row[f"{m}_ci_low"], row[f"{m}_ci_high"] = lo, hi
            rows.append(row)
    return rows


def render_markdown(rows: list[dict]) -> str:
    lines = ["| 格子 | 组态 | " + " | ".join(METRICS) + " |",
             "|---|---|" + "---|" * len(METRICS)]
    index = {(r["cell"], r["group"]): r for r in rows}
    for cell in CELL_ORDER:
        for group in GROUP_ORDER:
            r = index.get((cell, group))
            if not r:
                continue
            txt = []
            for m in METRICS:
                if r[f"{m}_mean"] == "":
                    txt.append("—")
                elif r["n"] < 2:
                    txt.append(f"{float(r[f'{m}_mean']):.3f} (n=1)")
                else:
                    txt.append(f"{float(r[f'{m}_mean']):.3f} "
                               f"[{float(r[f'{m}_ci_low']):.3f}, "
                               f"{float(r[f'{m}_ci_high']):.3f}] (n={r['n']})")
            tag = " 〔探点〕" if r["status"] == "exploratory" else ""
            lines.append(f"| {cell}{tag} | {group} | " + " | ".join(txt) + " |")
    return "\n".join(lines) + "\n"


def selftest() -> None:
    cells = {("S-100", "direct"): [{"startup_makespan": "10"},
                                   {"startup_makespan": "20"},
                                   {"startup_makespan": "30"}]}
    rows = build_table(cells)
    assert len(rows) == 1 and rows[0]["n"] == 3, rows
    assert abs(rows[0]["startup_makespan_mean"] - 20.0) < 1e-9, rows[0]
    assert rows[0]["status"] == "ok"
    assert build_table({("L-1000", "fluid-cold"): [{"total_makespan": "5"}]})[0]["status"] == "exploratory"
    print("summarize_m7 selftest PASS")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=None, help="m7 结果根目录")
    ap.add_argument("--out-dir", type=Path, default=None)
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return 0

    root = args.root or config.RESULTS_DIR / "m7"
    out_dir = args.out_dir or root / "_summary"
    if not root.is_dir():
        print(f"[err] 结果目录不存在: {root}", file=sys.stderr)
        return 1
    rows = build_table(read_cells(root))
    if not rows:
        print(f"[err] {root} 下没有有效行", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "m7_baseline.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    md = render_markdown(rows)
    (out_dir / "m7_baseline.md").write_text(md)
    print(md)
    print(f"[out] {out_dir / 'm7_baseline.csv'}  {out_dir / 'm7_baseline.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
