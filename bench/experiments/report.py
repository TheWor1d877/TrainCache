#!/usr/bin/env python3
"""生成结果报告页：决策表、每族图与逐格读数。只读 results/，不访问集群。"""
from __future__ import annotations

import html
import json
import re
import sys
from pathlib import Path

import numpy as np

BENCH = Path(__file__).resolve().parent.parent
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import plot_families as pf  # noqa: E402

OUT = config.RESULTS_DIR / "report" / "index.html"
FIG_ROOT = config.RESULTS_DIR / "figures"
BASELINES = ["direct", "juicefs-cold", "juicefs-warm", "fluid-cold", "fluid-warm"]
FAMILY_CN = {
    "compare": "对照族", "gateway": "网关族", "evict": "淘汰策略族",
    "prefetch": "预取参数族", "warmup": "预热族", "switch": "阶段切换族",
    "chaos": "混沌族", "multiconsumer": "多消费者族", "e2e": "端到端族",
    "rangepar": "并行下载族",
}
FAMILY_Q = {
    "compare": "缓存摘掉源站字节后，启动与全程各快多少；ρ 从 0.5 到 8 在哪一段收益消失",
    "gateway": "网关合并与两级缓存各贡献多少",
    "evict": "heat、lru、lfu 三种算法在 ρ 两端能否分辨",
    "prefetch": "lookahead 增到多少转平；fanout 增加带来多少重复字节",
    "warmup": "门控等待预热值不值；窗口多长合适",
    "switch": "阶段切换后 stall 多久回落；哪些事件到 epoch 末仍未回落",
    "chaos": "注入故障后三条状态线能否回到正常读数",
    "multiconsumer": "热比例与读端放置对命中率和耗时的影响",
    "e2e": "中断后从 checkpoint 续跑，缓存与数据加载是否仍然有效",
    "rangepar": "把每个块拆成 4 条 Range GET 并行取，在什么链路条件下端到端变快",
}
FAMILY_CRITERION = {
    "compare": "4.3 按族的判据「compare·组间胜负」",
    "gateway": "4.3 按族的判据「gateway·复制读合并」「gateway·臂间比较」",
    "evict": "4.3 按族的判据「evict·策略可分辨」「evict·策略不可分辨」",
    "prefetch": "4.3 按族的判据「prefetch·转平」「prefetch·重复字节」",
    "warmup": "4.3 按族的判据「warmup·门控有效」「warmup·门控无价值」「warmup·门控负收益」",
    "switch": "4.3 按族的判据「switch·回落」「switch·差异」「switch·右删失」",
    "chaos": "4.3 混沌族判据",
    "multiconsumer": "4.3 按族的判据「multiconsumer·热比例效应」",
    "e2e": "4.3 按族的判据「e2e·恢复成功」",
    "rangepar": "4.3 按族的判据「rangepar·并行收益」「rangepar·字节守恒」「rangepar·命中不退化」",
}
FAMILY_FIGURES = {
    "compare": ["compare-groups-rho4", "compare-rho"],
    "gateway": ["gateway-makespan", "gateway-source-bytes"],
    "evict": ["evict-makespan", "evict-hot-evictions"],
    "prefetch": ["prefetch-lookahead"],
    "warmup": ["warmup-startup"],
    "switch": ["switch-recovery"],
    "multiconsumer": ["multiconsumer-hit", "multiconsumer-makespan"],
    "chaos": ["chaos-results"],
    "e2e": ["e2e-makespan", "e2e-read-errors"],
    "rangepar": ["rangepar-makespan", "rangepar-speedup"],
}
FAMILY_METRICS = {
    "compare": ["startup_makespan", "total_makespan", "hit_rate", "fetch_bytes_run"],
    "gateway": ["total_makespan", "fetch_bytes_run", "gateway_merged"],
    "evict": ["total_makespan", "hit_rate", "b_evictions_of_hot"],
    "prefetch": ["total_makespan", "fetch_bytes_run", "hit_rate"],
    "warmup": ["startup_makespan", "total_makespan", "hit_rate"],
    "switch": ["recovery_time_s", "recovery_censored", "total_makespan"],
    "multiconsumer": ["hit_rate", "total_makespan"],
    "e2e": ["total_makespan", "read_errors"],
    "rangepar": ["total_makespan", "fetch_bytes_run", "hit_rate"],
}


def _vals(rows, metric):
    out = []
    for row in rows:
        value = pf._num(row.get(metric))
        if value is not None:
            out.append(value)
    return out


def _mean(values):
    return sum(values) / len(values) if values else None


def _median(values):
    return float(np.median(values)) if values else None


def _diff_ci(a, b, iters=10000, seed=7):
    aa, bb = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    rng = np.random.default_rng(seed)
    da = aa[rng.integers(0, aa.size, size=(iters, aa.size))].mean(axis=1)
    db = bb[rng.integers(0, bb.size, size=(iters, bb.size))].mean(axis=1)
    diff = da - db
    return float(np.percentile(diff, 2.5)), float(np.percentile(diff, 97.5))


def _cmp_low(group_rows, base_rows, metric):
    """越低越好的指标：返回 (判定, 改善百分比) 或 None。"""
    gv, bv = _vals(group_rows, metric), _vals(base_rows, metric)
    if len(gv) < 2 or len(bv) < 2:
        return None
    mg, mb = _mean(gv), _mean(bv)
    if not mb:
        return None
    low, high = _diff_ci(gv, bv)
    gain = (mb - mg) / abs(mb) * 100
    if gain >= 5 and high < 0:
        return "赢", gain
    if gain <= -5 and low > 0:
        return "输", gain
    if low <= 0 <= high:
        return "相近", gain
    return "可忽略", gain


def _rel_diff(a, b):
    if not a or not b:
        return 0.0
    return abs(a - b) / max(abs(a), abs(b)) * 100


def verdict_compare(rows):
    if not rows:
        return "未跑"
    facts = []
    for rate in sorted({r.get("rate") for r in rows}, key=config.rate_mbit):
        at4 = [r for r in rows if abs((pf._rho(r) or 0) - 4) < 1e-9
               and r.get("rate") == rate]
        base = {g: _mean(_vals([r for r in at4 if r.get("group") == g], "total_makespan"))
                for g in BASELINES}
        base = {g: v for g, v in base.items() if v is not None}
        if not base:
            continue
        best = min(base, key=base.get)
        for group in ("traincache-cold", "traincache-warm"):
            grow = [r for r in at4 if r.get("group") == group]
            result = _cmp_low(grow, [r for r in at4 if r.get("group") == best],
                              "total_makespan")
            if result:
                facts.append(f"{pf._rate_fmt(rate)} ρ=4：{group} 对 {best} "
                             f"{result[0]} {abs(result[1]):.1f}%")
    return "；".join(facts) or "样本不足"


def verdict_gateway(rows):
    if not rows:
        return "未跑"
    arms = {pf._arm(r) for r in rows}
    if not {"g0", "g1", "g2"} <= arms:
        return "样本不足"
    def mean_arm(arm, metric):
        return _mean(_vals([r for r in rows if pf._arm(r) == arm], metric))
    facts = []
    for arm, base in (("g2", "g1"), ("g2", "g0")):
        result = _cmp_low([r for r in rows if pf._arm(r) == arm],
                          [r for r in rows if pf._arm(r) == base], "total_makespan")
        if result:
            facts.append(f"{arm.upper()} 对 {base.upper()} makespan "
                         f"{result[0]} {abs(result[1]):.1f}%")
    merged = mean_arm("g3", "gateway_merged")
    if merged is not None:
        facts.append(f"G3 合并次数均值 {merged:.0f}")
    return "；".join(facts) or "样本不足"


def verdict_evict(rows):
    if not rows:
        return "未跑"
    facts = []
    for rho in pf._rho_sorted({pf._rho_key(r) for r in rows}):
        sub = [r for r in rows if pf._rho_key(r) == rho]
        policies = sorted({pf._policy(r) for r in sub})
        if len(policies) < 2:
            continue
        worst = 0.0
        for i, left in enumerate(policies):
            for right in policies[i + 1:]:
                for metric in ("total_makespan", "hit_rate", "b_evictions_of_hot"):
                    a = _mean(_vals([r for r in sub if pf._policy(r) == left], metric))
                    b = _mean(_vals([r for r in sub if pf._policy(r) == right], metric))
                    worst = max(worst, _rel_diff(a, b))
        facts.append(f"ρ={rho}：三项最大相对差 {worst:.1f}%，"
                     + ("可分辨" if worst >= 5 else "不可分辨"))
    return "；".join(facts) or "样本不足"


def verdict_prefetch(rows):
    if not rows:
        return "未跑"
    facts = []
    for rho in pf._rho_sorted({pf._rho_key(r) for r in rows}):
        for fan in sorted({int(pf._num(r.get("prefetch_fanout")) or 0) for r in rows}):
            points = []
            for lookahead in sorted({int(pf._num(r.get("prefetch_lookahead")) or 0)
                                     for r in rows}):
                picked = [r for r in rows if pf._rho_key(r) == rho
                          and int(pf._num(r.get("prefetch_fanout")) or 0) == fan
                          and int(pf._num(r.get("prefetch_lookahead")) or 0) == lookahead]
                value = _mean(_vals(picked, "total_makespan"))
                if value is not None:
                    points.append((lookahead, value))
            plateau = None
            for (la0, m0), (la1, m1) in zip(points, points[1:]):
                if m0 and (m0 - m1) / m0 < 0.05:
                    plateau = (la0, la1)
                    break
            if plateau:
                facts.append(f"ρ={rho} fanout={fan}：{plateau[0]}→{plateau[1]} 块转平")
    return "；".join(facts) or "未见转平点"


def verdict_warmup(rows):
    if not rows:
        return "未跑"
    windows = sorted({pf._window(r) for r in rows if pf._window(r) is not None})
    if not windows:
        return "样本不足"
    window = windows[-1]
    facts = []
    for rho in pf._rho_sorted({pf._rho_key(r) for r in rows}):
        def mean_gate(gate):
            return _mean(_vals([r for r in rows if pf._rho_key(r) == rho
                                and pf._window(r) == window
                                and pf._gate(r) == gate], "startup_makespan"))
        gated, free = mean_gate("gated"), mean_gate("free")
        if gated is None or free is None or not gated:
            continue
        gain = (gated - free) / gated * 100
        label = "有效" if gain >= 10 else ("无价值" if abs(gain) <= 5 else "负收益")
        facts.append(f"ρ={rho} 窗口 {window}s：gated 比 free 慢 {gain:.1f}%，{label}")
    return "；".join(facts) or "样本不足"


def verdict_switch(rows):
    if not rows:
        return "未跑"
    facts = []
    for scenario in ("fixed", "per-epoch", "curriculum", "curriculum-narrow"):
        picked = [r for r in rows if pf._scenario(r) == scenario]
        value = _mean(_vals(picked, "recovery_time_s"))
        censored = sum(int(pf._num(r.get("recovery_censored")) or 0) for r in picked)
        if value is not None:
            facts.append(f"{scenario}：回落 {value:.1f}s，删失 {censored}")
    return "；".join(facts) or "样本不足"


def verdict_multiconsumer(rows):
    if not rows:
        return "未跑"
    facts = []
    for placement in ("spread", "colocated"):
        def mean_hot(pct):
            return _mean(_vals([r for r in rows if pf._placement(r) == placement
                                and pf._hot(r) == pct], "hit_rate"))
        low, high = mean_hot("10%"), mean_hot("50%")
        if low is not None and high is not None:
            facts.append(f"{placement}：热比例 50% 比 10% 命中率 "
                         f"{(high - low) * 100:+.1f}pp")
    return "；".join(facts) or "样本不足"


# 并行下载族的比值合格区间在 bench/config.py(与出图共用一份)。
RANGEPAR_CELL_RE = re.compile(r"^rangepar-(l\d+)-(par-on|par-off)$")


def _rangepar_parts(row):
    """COND 名 rangepar-<档>-<臂> → (档, 臂)。"""
    match = RANGEPAR_CELL_RE.match(pf._cell(row))
    if not match:
        return None, None
    return match.group(1), match.group(2)


def verdict_rangepar(rows):
    """并行下载族:同档两臂的总耗时比,外加字节守恒与命中不退化两条约束。"""
    if not rows:
        return "未跑"
    facts = []
    tiers = sorted({t for t, _ in map(_rangepar_parts, rows) if t})
    for tier in tiers:
        on = [r for r in rows if _rangepar_parts(r) == (tier, "par-on")]
        off = [r for r in rows if _rangepar_parts(r) == (tier, "par-off")]
        on_ms = _median(_vals(on, "total_makespan"))
        off_ms = _median(_vals(off, "total_makespan"))
        if not on_ms or not off_ms:
            continue
        ratio = off_ms / on_ms
        low, high = config.RANGEPAR_RATIO.get(tier, (None, None))
        if low is not None and ratio < low:
            mark = f"未达下限 {low}"
        elif high is not None and ratio > high:
            mark = f"超出上限 {high}"
        else:
            mark = "在合格区间"
        facts.append(f"{tier}：关臂/开臂 总耗时 {ratio:.2f}×（{mark}）")
        for metric in ("origin_egress_bytes", "fetch_bytes_run"):
            ov = _mean(_vals(on, metric))
            fv = _mean(_vals(off, metric))
            if ov and fv:
                delta = abs(ov - fv) / max(ov, fv) * 100
                if delta >= 2:
                    facts.append(f"{tier} 字节不守恒（{metric} 差 {delta:.1f}%）")
        oh = _mean(_vals(on, "hit_rate"))
        fh = _mean(_vals(off, "hit_rate"))
        if oh is not None and fh is not None and oh < fh - 0.02:
            facts.append(f"{tier} 命中退化（{(oh - fh) * 100:+.1f}pp）")
    return "；".join(facts) or "样本不足"


def chaos_results():
    root = config.family_results_dir("chaos")
    out = []
    if not root.is_dir():
        return out
    for path in sorted(root.glob("*/*/summary.txt")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].startswith(("PASS", "FAIL")):
                out.append((parts[0], parts[1], path.parent.parent.name))
    return out


def verdict_chaos():
    results = chaos_results()
    if not results:
        return "未跑"
    passed = sum(1 for _f, status, _r in results if status == "PASS")
    return f"PASS {passed}/{len(results)}"


def verdict_e2e(rows):
    if not rows:
        return "未跑"
    conds = sorted({pf._cell(r) for r in rows})
    errors = sum(int(pf._num(r.get("read_errors")) or 0) for r in rows)
    return f"运行完成 {len(conds)}/3 个场景，read_errors 合计 {errors}"


VERDICTS = {
    "compare": verdict_compare,
    "gateway": verdict_gateway,
    "evict": verdict_evict,
    "prefetch": verdict_prefetch,
    "warmup": verdict_warmup,
    "switch": verdict_switch,
    "multiconsumer": verdict_multiconsumer,
    "e2e": verdict_e2e,
    "rangepar": verdict_rangepar,
}


def _fmt(value):
    if value is None:
        return "—"
    if abs(value) >= 1e6:
        return f"{value:.3g}"
    return f"{value:.3f}"


def _cell_table(family, rows):
    if not rows:
        return "<p>没有产物。</p>"
    metrics = FAMILY_METRICS[family]
    by_cond = {}
    for row in rows:
        by_cond.setdefault(pf._cell(row), []).append(row)
    head = "".join(f"<th>{html.escape(m)}</th>" for m in metrics)
    body = []
    for cond in sorted(by_cond):
        picked = by_cond[cond]
        cells = []
        for metric in metrics:
            value = _mean(_vals(picked, metric))
            n = len(_vals(picked, metric))
            cells.append(f"<td>{_fmt(value)}<span class=dim> (n={n})</span></td>")
        body.append(f"<tr><td>{html.escape(cond)}</td>{''.join(cells)}</tr>")
    return ("<table><thead><tr><th>格子</th>" + head + "</tr></thead><tbody>"
            + "".join(body) + "</tbody></table>")


def _figure(family):
    parts = []
    for name in FAMILY_FIGURES.get(family, []):
        path = FIG_ROOT / f"{name}.zh.png"
        if path.is_file():
            parts.append(f'<figure><img src="../figures/{name}.zh.png" '
                         f'alt="{html.escape(name)}">'
                         f"<figcaption>{html.escape(name)}</figcaption></figure>")
    return "".join(parts) or "<p>没有图。</p>"


def _completion(rows_by_family):
    conds_dir = BENCH / "experiments" / "conds"
    total = done = 0
    if not conds_dir.is_dir():
        return 0, 0
    for path in sorted(conds_dir.glob("*.json")):
        try:
            cond = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if cond.get("schema_version") != "4":
            continue
        total += 1
        reps = int((cond.get("sampling") or {}).get("reps") or 3)
        count = sum(1 for row in rows_by_family.get(cond["family"], [])
                    if pf._cell(row) == cond["name"])
        if count >= reps:
            done += 1
    return done, total


CSS = """
body{font-family:"Noto Sans CJK SC","DejaVu Sans",sans-serif;color:#1a1a1a;
margin:0 auto;max-width:1100px;padding:32px 24px 64px;line-height:1.6}
h1{font-size:24px;margin:0 0 4px}h2{font-size:18px;margin:36px 0 8px;
border-bottom:1px solid #e5e7eb;padding-bottom:6px}
p{color:#374151}.meta{color:#6b7280;font-size:13px}
table{border-collapse:collapse;width:100%;font-size:13px;margin:12px 0}
th,td{border-bottom:1px solid #e5e7eb;padding:6px 8px;text-align:left;
vertical-align:top}th{color:#4b5563;font-weight:600}
.dim{color:#9ca3af;font-size:12px}
figure{margin:16px 0}img{max-width:100%;border:1px solid #ececec}
figcaption{color:#6b7280;font-size:12px;margin-top:4px}
"""


def _chaos_table(results):
    """混沌族的逐轮判定表：故障、结果、轮次目录。"""
    if not results:
        return "<p>没有产物。</p>"
    rows = "".join(
        f"<tr><td>{html.escape(fault)}</td><td>{html.escape(status)}</td>"
        f"<td>{html.escape(run)}</td></tr>" for fault, status, run in results)
    return ("<table><thead><tr><th>故障</th><th>结果</th><th>轮次目录</th>"
            f"</tr></thead><tbody>{rows}</tbody></table>")


def build() -> Path:
    rows_by_family = {family: pf.FAMILY_ROWS.get(family, pf.family_rows)(family)
                      for family in config.BENCH_FAMILIES}
    done, total = _completion(rows_by_family)
    verdicts = {family: VERDICTS[family](rows_by_family[family])
                for family in VERDICTS}
    verdicts["chaos"] = verdict_chaos()

    table_rows = []
    sections = []
    chaos = chaos_results()
    for family in config.BENCH_FAMILIES:
        rows = rows_by_family[family]
        grids = len({pf._cell(row) for row in rows})
        samples = f"{grids} 格 / {len(rows)} 行"
        table_rows.append(
            "<tr>"
            f"<td>{html.escape(FAMILY_CN[family])}</td>"
            f"<td>{html.escape(FAMILY_Q[family])}</td>"
            f"<td>{html.escape(verdicts[family])}</td>"
            f"<td>{html.escape(FAMILY_CRITERION[family])}</td>"
            f"<td>{samples}</td>"
            "</tr>")
        section = (
            f"<h2 id=\"{family}\">{html.escape(FAMILY_CN[family])}"
            f"（{html.escape(family)}）</h2>"
            f"<p class=meta>判据：{html.escape(FAMILY_CRITERION[family])}；"
            f"样本：{samples}</p>"
            + _figure(family))
        # 混沌族的产物是每轮一份 summary.txt，没有 summary.csv 的逐格读数表。
        section += (_chaos_table(chaos) if family == "chaos"
                    else _cell_table(family, rows))
        sections.append(section)

    doc = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>bench 实验结果报告</title><style>{CSS}</style></head><body>
<h1>bench 实验结果报告</h1>
<p class=meta>产物根 {html.escape(str(config.RESULTS_DIR))}；
完成格子 {done}/{total}</p>
<h2>决策表</h2>
<table><thead><tr><th>族</th><th>要回答的问题</th><th>结论</th><th>判据</th>
<th>样本</th></tr></thead><tbody>{''.join(table_rows)}</tbody></table>
{''.join(sections)}
</body></html>
"""
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(doc, encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"report: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
