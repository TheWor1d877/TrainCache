#!/usr/bin/env python3
"""探针A 判定:冷/热各 7 轮,绿灯三条件。

1. 冷/热中位延迟差 ≥5×(同时报中位吞吐比,一个数量级即 10×);
2. 两组 bootstrap 95% CI 不重叠;
3. 组内变异系数 CV ≤ 15%。

输出 verdict.json + 人读中文报告,并打印到 stdout。
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bootstrap import cv, pct_bootstrap_ci

RES = Path(__file__).resolve().parents[1] / "results" / "probe_a"


def load(kind: str):
    rows = []
    for p in sorted(RES.glob(f"{kind}_*.json")):
        rows.append(json.loads(p.read_text()))
    if not rows:
        sys.exit(f"没有 {kind} 结果:先跑对应 run_{kind}.sh")
    return [r["seconds"] for r in rows], [r["mbps"] for r in rows]


def main():
    cold_t, cold_m = load("cold")
    hot_t, hot_m = load("hot")

    cold_ci = pct_bootstrap_ci(cold_t)
    hot_ci = pct_bootstrap_ci(hot_t)
    ratio = cold_ci[2] / hot_ci[2]
    cv_c, cv_h = cv(cold_t), cv(hot_t)
    ci_overlap = not (cold_ci[1] < hot_ci[0] or hot_ci[1] < cold_ci[0])

    ok1 = ratio >= 5
    ok2 = not ci_overlap
    ok3 = cv_c <= 0.15 and cv_h <= 0.15
    verdict = ok1 and ok2 and ok3

    def ci(s):
        return f"{s[0]:.1f} ~ {s[1]:.1f}"

    lines = [
        "====== 探针A 判定:冷热差稳定可复现 ======",
        f"冷读(n={len(cold_t)}):中位耗时 {cold_ci[2]:.1f}s,中位吞吐 {sorted(cold_m)[len(cold_m)//2]:.1f} MB/s,"
        f"95% CI [{ci(cold_ci)}]s,CV {cv_c:.1%}",
        f"热读(n={len(hot_t)}):中位耗时 {hot_ci[2]:.3f}s,中位吞吐 {sorted(hot_m)[len(hot_m)//2]:.1f} MB/s,"
        f"95% CI [{ci(hot_ci)}]s,CV {cv_h:.1%}",
        f"冷/热中位耗时比 = {ratio:.1f}×(吞吐比 = {sorted(hot_m)[len(hot_m)//2] / max(sorted(cold_m)[len(cold_m)//2], 1e-9):.1f}×)",
        f"[{'过' if ok1 else '红灯'}] 条件1:中位差 ≥5×",
        f"[{'过' if ok2 else '红灯'}] 条件2:95% CI 不重叠",
        f"[{'过' if ok3 else '红灯'}] 条件3:两组 CV ≤15%(冷 {cv_c:.1%},热 {cv_h:.1%})",
        "",
        f"判定:{'绿灯 —— 可进入探针B 与后续现勘' if verdict else '红灯 —— 停止开工,按 design.md 红灯排查清单复盘装置'}",
    ]
    report = "\n".join(lines)
    print(report)

    RES.mkdir(parents=True, exist_ok=True)
    (RES / "verdict.json").write_text(json.dumps({
        "verdict": "PASS" if verdict else "FAIL",
        "ok": {"median_ratio_ge_5x": ok1, "ci_no_overlap": ok2, "cv_le_15pct": ok3},
        "cold": {"median_s": cold_ci[2], "ci_s": list(cold_ci[:2]), "cv": cv_c,
                 "runs_s": cold_t},
        "hot": {"median_s": hot_ci[2], "ci_s": list(hot_ci[:2]), "cv": cv_h,
                "runs_s": hot_t},
        "ratio": ratio,
    }, ensure_ascii=False, indent=2) + "\n")
    print(f"\n已写 verdict.json → {RES / 'verdict.json'}")
    sys.exit(0 if verdict else 1)


if __name__ == "__main__":
    main()
