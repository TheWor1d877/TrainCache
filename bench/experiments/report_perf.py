#!/usr/bin/env python3
"""性能报告生成器：results + e2e 序列 → 自包含 ECharts 报告页 + README 静态 PNG。

输出落 docs/perf-report/（index.html + fig/*.png），不直接放 docs 根。
--mock 用假数据生成同构报告，供风格验收；真实模式读数据层，不访问集群。

用法:
  python3 bench/experiments/report_perf.py --mock
  python3 bench/experiments/report_perf.py --results-root <数据层>/bench/results
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH = HERE.parent
REPO = BENCH.parent
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(HERE))

import config  # noqa: E402

OUT_DEFAULT = REPO / "docs" / "perf-report"
ECHARTS_JS = HERE / "assets" / "echarts.min.js"

RHOS = [0.5, 1, 2, 4, 8]
RATES = [1000, 300, 100]
WARM_RATES = [1000, 300]
C = {"blue": "#3b6fd4", "purple": "#7a5cd6", "orange": "#e8963c",
     "teal": "#3aa6a6", "rose": "#c95f81", "ink": "#1f2328",
     "mut": "#5a616e", "grid": "#eceff3"}
RATE_COLORS = {1000: C["blue"], 300: C["orange"], 100: C["rose"]}
GROUP_COLORS = {"direct": "#6f6f6f", "juicefs-cold": "#6fa9e0",
                "juicefs-warm": "#1f5f9e", "fluid-cold": "#f0a85a",
                "fluid-warm": "#c96a1c", "traincache-cold": "#a973d1",
                "traincache-warm": "#5b2a82"}
PHASE_COLORS = {"startup": "#fdeedd", "steady": "#eef4fd", "switch": "#f6e8f0"}


def rho_name(r):
    return ("rho%g" % r).replace(".", "")


def cell_name(group, rho, rate):
    return f"compare-{group}-{rho_name(rho)}-{rate}"


# ---------------------------------------------------------------------------
# 假数据：物理形状对齐真实跑分（冷读命中率低、带宽越低 makespan 越长、
# 切换处有等待尖峰、e2e 中断处序列断档），只用于风格验收，不进真实报告。
# ---------------------------------------------------------------------------

def _mock_steps(rng, n_steps, cold, rate):
    steps = []
    epoch_len = n_steps // 3
    base_wait = 0.02 if not cold else {1000: 0.05, 300: 0.25, 100: 1.2}[rate]
    for i in range(n_steps):
        epoch, sie = divmod(i, epoch_len)
        phase = "startup" if i < 20 else ("switch" if sie < 12 and epoch > 0 else "steady")
        wait = max(0.0, rng.gauss(base_wait, base_wait * 0.4))
        if phase == "startup":
            wait = base_wait * (3 if i < 3 else 1)
        if phase == "switch":
            wait = base_wait * rng.uniform(4, 9) * (1 - sie / 12)
        steps.append({"global_step": i, "epoch": epoch, "step_in_epoch": sie,
                      "phase": phase, "shard": str(rng.randrange(16)),
                      "wait_s": round(wait, 4),
                      "step_s": round(0.21 + wait, 4)})
    return steps


def _mock_summary(rng, group, rho, rate, rep):
    cold = group.endswith("cold")
    rate_f = {1000: 1.0, 300: 2.6, 100: 7.5}[rate]
    base = 240 * rate_f * (1 + 0.04 * math.log2(rho + 1))
    total = base * rng.uniform(0.97, 1.03)
    hit = rng.uniform(0.08, 0.14) if cold else rng.uniform(0.93, 0.99)
    read_bytes = 12717999872
    miss_b = int(read_bytes * (1 - hit))
    fetch_b = int(miss_b * rng.uniform(0.98, 1.02))
    origin_b = int(fetch_b * rng.uniform(0.99, 1.01))
    return {
        "group": group, "rho": rho, "rate": str(rate), "rep": rep, "void": False,
        "startup_makespan": round(20 * rate_f * rng.uniform(0.9, 1.1), 2),
        "total_makespan": round(total, 2),
        "stall_ratio": round(min(0.6, (1 - hit) * 0.35 * rate_f / 3), 4),
        "stall_p90": round(rng.uniform(0.5, 4) * rate_f / 3, 3),
        "queue_timeouts": int(max(0, round((rate_f - 2) * 1.6 * rho / 4 * rng.uniform(0.5, 1.5)))),
        "read_errors": 0,
        "hit_rate": round(hit, 4),
        "source_saving_ratio": round(hit * rng.uniform(0.95, 1.0), 4),
        "total_read_bytes": read_bytes,
        "read_bytes_startup": int(read_bytes * 0.02),
        "read_bytes_steady": int(read_bytes * 0.93),
        "read_bytes_switch": int(read_bytes * 0.05),
        "demand_miss_bytes_startup": int(read_bytes * 0.02),
        "demand_miss_bytes_steady": int(miss_b * 0.93),
        "demand_miss_bytes_switch": int(miss_b * 0.05),
        "hit_ratio_startup": 0.0 if cold else round(hit, 4),
        "hit_ratio_steady": round(hit, 4),
        "hit_ratio_switch": round(hit * 0.3, 4) if cold else round(hit, 4),
        "hit_ratio_agent_steady": round(min(0.999, hit + 0.85), 4) if cold else round(hit, 4),
        "block_hit_rate_diag": round(min(0.999, hit + 0.88), 4) if cold else round(hit, 4),
        "origin_egress_bytes": origin_b, "fetch_bytes_run": fetch_b,
        "origin_reconcile_diff": origin_b - fetch_b,
        "evict_blocks_run": int(1200 * rho / 4 * rng.uniform(0.8, 1.2)) if cold else 0,
        "cache_peak_ratio": round(min(1.45, 1.0 + 0.1 * rho) * rng.uniform(0.95, 1.05), 3),
        "prefetch_bytes_run": 0 if cold else int(read_bytes * 0.2),
        "prefetch_task_duration_p50_s": round(rng.uniform(0.8, 1.4), 3),
        "prefetch_task_duration_p90_s": round(rng.uniform(1.6, 2.4), 3),
        "warmup_seconds": 0.0 if cold else round(rng.uniform(30, 90), 1),
        "sample_n": 194, "sample_max_gap_s": round(rng.uniform(4.5, 6.5), 1),
        "dataset_bytes": 4294967296, "epochs": 3,
    }


def _mock_recovery(rng, rate):
    events = []
    for ep in (1, 2):
        rec = rng.uniform(1.5, 6) * {1000: 1, 300: 2.2, 100: 5}[rate]
        events.append({"kind": "epoch", "to_epoch": ep, "status": "recovered",
                       "censored": False, "recovery_time_s": round(rec, 3),
                       "recovery_step": rng.randrange(5, 15)})
    return events


def _mock_series(rng, scenario, t0, dur):
    """e2e 原始序列（5s 采样）：中断处断档，重启后预取游标回退重走。"""
    ts = list(range(0, dur, 5))
    interrupt = int(dur * 0.45)
    resume = interrupt + {"checkpoint-resume": 25, "checkpoint-version": 40,
                          "dual-restart": 55}[scenario]
    q = {}

    def add(name, fn, labels=None):
        vals = []
        for t in ts:
            if interrupt < t < resume:
                continue
            vals.append([t0 + t, round(fn(t), 4)])
        q[name] = {"series": [{"metric": labels or {}, "values": vals}]}

    cursor = [0]

    def cursor_fn(t):
        if t > resume:
            cursor[0] = max(0, cursor[0] - 30)
        cursor[0] += 5 * 2.2
        return min(cursor[0], 512)
    add("prefetch_cursor", cursor_fn)
    add("prefetch_lead_blocks", lambda t: 16 + 4 * math.sin(t / 40))
    add("prefetch_queue_depth",
        lambda t: max(0, 6 + 5 * math.sin(t / 25) + (12 if resume < t < resume + 60 else 0)))
    ev = [0]

    def evict_fn(t):
        if t > 200:
            ev[0] += 5 * 1.8
        return ev[0]
    add("evict_blocks", evict_fn)
    add("evict_protected_skipped", lambda t: max(0, (t - 200) * 0.15))
    fb = {"demand": [0], "prefetch": [0], "other": [0]}

    def fetch_fn(t, reason):
        rate_b = {"demand": 8e6, "prefetch": 22e6, "other": 1e6}[reason]
        if resume < t < resume + 90 and reason == "demand":
            rate_b *= 3
        fb[reason][0] += 5 * rate_b
        return fb[reason][0]

    # 三个 reason 各自一条曲线；不能复用 add()，否则同名查询互相覆盖。
    q["fetch_bytes"] = {"series": [
        {"metric": {"reason": reason},
         "values": [[t0 + t, round(fetch_fn(t, reason), 4)] for t in ts
                    if not (interrupt < t < resume)]}
        for reason in fb]}
    add("fetch_inflight_bytes", lambda t: 3e7 * (1 + 0.5 * math.sin(t / 30)))
    hits = [0]

    def hits_fn(t):
        hits[0] += 5 * (30 if t < interrupt else 55)
        return hits[0]
    add("cache_hits", hits_fn)
    add("cache_misses", lambda t: max(0, t * 0.8))
    add("cache_hit_ratio", lambda t: 0.97 if t > 120 else 0.6)
    add("disk_usage_ratio",
        lambda t: min(1.0, t / 260) - (0.25 if t > resume + 120 else 0))
    add("read_stall_seconds", lambda t: 0.4 + (3 if resume < t < resume + 60 else 0))
    sent = [0]

    def sent_fn(t):
        sent[0] += 5 * 3.1e7
        return sent[0]
    q["minio_sent_bytes"] = {"series": [{"metric": {}, "values":
                                        [[t0 + t, sent_fn(t)] for t in ts]}]}
    return q


def mock_data():
    rng = random.Random(20261112)
    cells = []
    for group, rates in (("traincache-cold", RATES), ("traincache-warm", WARM_RATES)):
        for rate in rates:
            for rho in RHOS:
                reps = [_mock_summary(rng, group, rho, rate, r) for r in range(3)]
                cells.append({
                    "name": cell_name(group, rho, rate), "group": group,
                    "rho": rho, "rate": rate, "reps": reps,
                    "steps": _mock_steps(rng, 399, group.endswith("cold"), rate),
                    "recovery": _mock_recovery(rng, rate),
                })
    e2e = []
    for sc in ("checkpoint-resume", "checkpoint-version", "dual-restart"):
        reps = []
        for r in range(2):
            t0 = 1794470000
            reps.append({
                "run_id": f"e2e-{sc}-r0{r}",
                "total_makespan": round(480 + r * 15 + rng.uniform(-10, 10), 1),
                "recovery_time_s": round({"checkpoint-resume": 25,
                                          "checkpoint-version": 40,
                                          "dual-restart": 55}[sc] * rng.uniform(0.9, 1.1), 1),
                "fetch_errors_recovery": 0,
                "steps": _mock_steps(rng, 399, True, 1000),
                "series": _mock_series(rng, sc, t0, 600),
                "t0": t0, "interrupt": 270, "resume": 270 + 25,
            })
        e2e.append({"scenario": sc, "reps": reps})
    families = _mock_families(rng)
    return {"cells": cells, "e2e": e2e, "families": families, "mock": True}


def _mock_families(rng):
    fam = {}
    fam["gateway"] = [{"arm": a, "total_makespan": v, "source_bytes": b}
                      for a, v, b in (("g0", 520, 12.7e9), ("g1", 410, 6.1e9),
                                      ("g2", 395, 5.8e9))]
    fam["evict"] = [{"policy": p, "rho": r,
                     "total_makespan": 400 * (1 + 0.15 * i) * rng.uniform(0.95, 1.05),
                     "b_evictions_of_hot": n}
                    for i, (p, n) in enumerate((("heat", 3), ("lru", 41), ("lfu", 27)))
                    for r in (1, 8)]
    fam["prefetch"] = [{"lookahead": la, "fanout": f, "rho": r,
                        "total_makespan": 420 - la * 1.5 + f * 8 + r * 3}
                       for la in (8, 16, 32) for f in (1, 2, 3) for r in (1, 8)]
    fam["warmup"] = [{"window": w, "gate": g, "rho": r,
                      "startup_makespan": (60 if g == "gated" else 25) + w * 0.1}
                     for w in (10, 60, 300) for g in ("gated", "free") for r in (1, 8)]
    fam["switch"] = [{"scenario": s, "recovery_time_s": v}
                     for s, v in (("fixed", 0.5), ("per-epoch", 4.2),
                                  ("curriculum", 6.8), ("curriculum-narrow", 3.1))]
    fam["multiconsumer"] = [{"hot": h, "placement": p, "hit_rate": hr,
                             "total_makespan": mk}
                            for h, hr, mk in (("10%", 0.62, 480), ("50%", 0.81, 430))
                            for p in ("spread", "colocated")]
    fam["rangepar"] = [{"level": l, "par": p, "total_makespan": mk}
                       for l in ("l0", "l1", "l2", "l3")
                       for p, mk in (("off", 500), ("on", 470))]
    fam["chaos"] = [{"fault": f, "rho": r, "status": s, "n": n}
                    for f in ("kill-agent", "minio-down", "tamper-block")
                    for r in (1, 8) for s, n in (("PASS", 5), ("FAIL", 0))]
    return fam


# ---------------------------------------------------------------------------
# 真实数据读取
# ---------------------------------------------------------------------------

def _read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _f(row, key, default=None):
    v = row.get(key, "")
    if v in ("", None):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def load_real(results_root):
    cells = []
    compare = results_root / "compare"
    for cell_dir in sorted(compare.glob("compare-traincache-*")):
        summaries = _read_csv(cell_dir / "summary.csv") if (cell_dir / "summary.csv").is_file() else []
        rows = [r for r in summaries if str(r.get("void")).lower() != "true"]
        if not rows:
            continue
        group = rows[0]["group"]
        rho = _f(rows[0], "rho")
        rate = int(_f(rows[0], "rate", "0").replace("mbit", ""))
        runs = sorted((cell_dir / "runs").glob("*"))
        steps, recovery = [], []
        if runs:
            sp = runs[0] / "per_step_1_w1.jsonl"
            if sp.is_file():
                steps = [json.loads(l) for l in sp.read_text().splitlines() if l.strip()]
            rp = runs[0] / "recovery.json"
            if rp.is_file():
                recovery = json.loads(rp.read_text()).get("events", [])
        cells.append({"name": cell_dir.name, "group": group, "rho": rho,
                      "rate": rate, "reps": rows, "steps": steps,
                      "recovery": recovery})
    e2e = []
    e2e_root = config.RESULTS_DIR / "e2e"
    for cond_dir in sorted(e2e_root.glob("e2e-*")) if e2e_root.is_dir() else []:
        reps = []
        for run_dir in sorted(cond_dir.glob("e2e-*-r*")):
            sj = run_dir / "series.json"
            rj = run_dir / "run.json"
            if not (sj.is_file() and rj.is_file()):
                continue
            run = json.loads(rj.read_text())
            series = json.loads(sj.read_text())
            steps = []
            for sp in run_dir.glob("per_step_*.jsonl"):
                steps = [json.loads(l) for l in sp.read_text().splitlines() if l.strip()]
                break
            reps.append({"run_id": run_dir.name, "series_raw": series,
                         "steps": steps, "run": run})
        if reps:
            e2e.append({"scenario": cond_dir.name.removeprefix("e2e-"), "reps": reps})
    families = load_real_families()
    return {"cells": cells, "e2e": e2e, "families": families, "mock": False}


def load_real_families():
    fam = {}
    for name in ("gateway", "evict", "prefetch", "warmup", "switch",
                 "multiconsumer", "rangepar"):
        root = config.family_results_dir(name)
        rows = []
        if root.is_dir():
            for sp in sorted(root.glob("*/summary.csv")):
                rows += [r for r in _read_csv(sp)
                         if str(r.get("void")).lower() != "true"]
        if rows:
            fam[name] = rows
    chaos = []
    croot = config.family_results_dir("chaos")
    if croot.is_dir():
        for sp in sorted(croot.glob("*/*/summary.txt")):
            for line in sp.read_text().splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[1].startswith(("PASS", "FAIL")):
                    chaos.append({"cond": sp.parent.parent.name, "fault": parts[0],
                                  "status": "PASS" if parts[1].startswith("PASS") else "FAIL"})
    if chaos:
        fam["chaos"] = chaos
    return fam

# ---------------------------------------------------------------------------
# ECharts option 构建
# ---------------------------------------------------------------------------

def _opt(**kw):
    o = {"textStyle": {"fontFamily": "Helvetica Neue,Arial,PingFang SC,Microsoft YaHei,sans-serif",
                       "color": C["ink"], "fontSize": 12},
         "tooltip": {"trigger": "axis", "confine": True},
         "grid": {"left": 60, "right": 24, "top": 42, "bottom": 40},
         "animation": False}
    o.update(kw)
    return o


def _x(name, data, **kw):
    a = {"type": "category", "data": data, "name": name,
         "nameLocation": "middle", "nameGap": 28,
         "axisLine": {"lineStyle": {"color": "#c7c9cc"}},
         "axisLabel": {"color": C["mut"]}}
    a.update(kw)
    return a


def _y(name, **kw):
    a = {"type": "value", "name": name,
         "nameTextStyle": {"color": C["mut"], "align": "left"},
         "splitLine": {"lineStyle": {"color": C["grid"]}},
         "axisLabel": {"color": C["mut"]}}
    a.update(kw)
    return a


def _legend(**kw):
    l = {"top": 6, "textStyle": {"color": C["mut"], "fontSize": 11.5},
         "itemWidth": 14, "itemHeight": 8}
    l.update(kw)
    return l


def _s(name, data, color, kind="bar", **kw):
    s = {"name": name, "type": kind, "data": data,
         "itemStyle": {"color": color}, "emphasis": {"focus": "series"}}
    if kind == "line":
        s.update({"showSymbol": False, "lineStyle": {"width": 2, "color": color},
                  "itemStyle": {"color": color}})
    if kind == "bar":
        s.update({"barMaxWidth": 26, "itemStyle": {"color": color, "borderRadius": [2, 2, 0, 0]}})
    s.update(kw)
    return s


def fig(fid, title, note, option, h=380):
    return {"id": fid, "title": title, "note": note, "option": option, "h": h}


def _cells(data, group):
    return [c for c in data["cells"] if c["group"] == group]


def _avg(rep_rows, key):
    vals = [_f(r, key) for r in rep_rows]
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def _cell_by(cells, rho, rate):
    for c in cells:
        if c["rho"] == rho and c["rate"] == rate:
            return c
    return None


def _fmt_mb(v):
    return None if v is None else round(v / 1e6, 1)


# ---- 主结果 ----

def fig_makespan_grid(data):
    grids, xaxes, yaxes, series = [], [], [], []
    for i, group in enumerate(("traincache-cold", "traincache-warm")):
        cells = _cells(data, group)
        rates = RATES if group.endswith("cold") else WARM_RATES
        grids.append({"left": "7%", "right": "7%", "top": 46 + i * 210, "height": 148})
        xaxes.append(_x("ρ（缓存容量 / 数据集）", [str(r) for r in RHOS], gridIndex=i))
        yaxes.append(_y("全程耗时 (s)", gridIndex=i))
        for rate in rates:
            vals = []
            for rho in RHOS:
                c = _cell_by(cells, rho, rate)
                vals.append(round(_avg(c["reps"], "total_makespan"), 1) if c else None)
            series.append(_s(f"{rate}mbit", vals, RATE_COLORS[rate], "line",
                             xAxisIndex=i, yAxisIndex=i))
    return fig("makespan-grid", "全程耗时随带宽与缓存容量比的变化",
               "每个数据点为 3 次重复的均值；上图为冷缓存（三档带宽），下图为预热（两档带宽）；ρ 为缓存容量与数据集大小之比。",
               _opt(grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    legend=_legend()), h=480)


def fig_source_saving(data):
    grids, xaxes, yaxes, series = [], [], [], []
    for i, group in enumerate(("traincache-cold", "traincache-warm")):
        cells = _cells(data, group)
        rates = RATES if group.endswith("cold") else WARM_RATES
        grids.append({"left": "7%", "right": "7%", "top": 46 + i * 210, "height": 148})
        xaxes.append(_x("ρ", [str(r) for r in RHOS], gridIndex=i))
        yaxes.append(_y("源站节省率", gridIndex=i, max=1))
        for rate in rates:
            vals = []
            for rho in RHOS:
                c = _cell_by(cells, rho, rate)
                vals.append(round(_avg(c["reps"], "source_saving_ratio"), 4) if c else None)
            series.append(_s(f"{rate}mbit", vals, RATE_COLORS[rate], "line",
                             xAxisIndex=i, yAxisIndex=i))
    return fig("source-saving", "源站节省率随缓存容量比的变化",
               "节省率 = 1 − agent 回源字节 / 训练侧读字节（D25 对账口径）；上图冷缓存，下图预热；冷缓存的节省来自块级命中，预热的节省来自预热阶段。",
               _opt(grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    legend=_legend()), h=480)


def fig_stall_struct(data):
    cells = _cells(data, "traincache-cold")
    metrics = [("stall_ratio", "等待占比"), ("stall_p90", "等待 p90 (s)"),
               ("queue_timeouts", "队列超时次数")]
    grids, xaxes, yaxes, series = [], [], [], []
    for i, (key, label) in enumerate(metrics):
        grids.append({"left": 60 + i * 340, "width": 280, "top": 60, "height": 198})
        xaxes.append(_x("ρ", [str(r) for r in RHOS], gridIndex=i))
        yaxes.append(_y(label, gridIndex=i))
        for rate in RATES:
            vals = []
            for rho in RHOS:
                c = _cell_by(cells, rho, rate)
                vals.append(round(_avg(c["reps"], key), 4) if c else None)
            series.append(_s(f"{rate}mbit", vals, RATE_COLORS[rate], "line",
                             xAxisIndex=i, yAxisIndex=i))
    return fig("stall-struct", "冷缓存场景等待结构：等待占比、长尾分位与队列超时",
               "三幅子图共用 ρ 轴：占比刻画等待的普遍性，p90 刻画长尾，队列超时次数刻画准入控制的触发条件。",
               _opt(grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    legend=_legend()), h=320)


def fig_hit_triple(data):
    grids, xaxes, yaxes, series = [], [], [], []
    keys = [("hit_rate", "demand（打开时不在本地即未命中）", C["blue"]),
            ("hit_ratio_agent_steady", "agent gauge（steady）", C["teal"]),
            ("block_hit_rate_diag", "block 诊断口径", C["orange"])]
    for i, group in enumerate(("traincache-cold", "traincache-warm")):
        cells = _cells(data, group)
        grids.append({"left": "7%", "right": "7%", "top": 46 + i * 210, "height": 148})
        xaxes.append(_x("ρ", [str(r) for r in RHOS], gridIndex=i))
        yaxes.append(_y("命中率（steady，跨带宽均值）", gridIndex=i, max=1))
        for key, label, color in keys:
            vals = []
            for rho in RHOS:
                cs = [c for c in cells if c["rho"] == rho]
                vv = [_avg(c["reps"], key) for c in cs]
                vv = [v for v in vv if v is not None]
                vals.append(round(sum(vv) / len(vv), 4) if vv else None)
            series.append(_s(label, vals, color, "line", xAxisIndex=i, yAxisIndex=i))
    return fig("hit-triple", "缓存命中率的三种口径对照",
               "demand 为训练侧真实等待口径（打开时不在本地即计未命中）；agent gauge 与 block 诊断为装置自报口径，系统性偏高属口径差异，非数据错误。",
               _opt(grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    legend=_legend()), h=480)


def fig_phase_bytes(data):
    cells = _cells(data, "traincache-cold")
    phases = [("startup", "启动"), ("steady", "稳态"), ("switch", "切换")]
    grids, xaxes, yaxes, series = [], [], [], []
    for i, (ph, label) in enumerate(phases):
        grids.append({"left": 60 + i * 340, "width": 280, "top": 60, "height": 198})
        xaxes.append(_x("ρ", [str(r) for r in RHOS], gridIndex=i))
        yaxes.append(_y(f"{label}读字节 (MB)", gridIndex=i))
        for rate in RATES:
            hit_v, miss_v = [], []
            for rho in RHOS:
                c = _cell_by(cells, rho, rate)
                if not c:
                    hit_v.append(None); miss_v.append(None); continue
                rd = _avg(c["reps"], f"read_bytes_{ph}") or 0
                ms = _avg(c["reps"], f"demand_miss_bytes_{ph}") or 0
                hit_v.append(_fmt_mb(max(0, rd - ms)))
                miss_v.append(_fmt_mb(ms))
            series.append(_s(f"{rate} 本地命中", hit_v, RATE_COLORS[rate], "bar",
                             stack=f"r{rate}", xAxisIndex=i, yAxisIndex=i))
            series.append(_s(f"{rate} 回源", miss_v, "#d9b8c4", "bar",
                             stack=f"r{rate}", xAxisIndex=i, yAxisIndex=i))
    return fig("phase-bytes", "冷缓存场景三相位字节流向分解",
               "各相位读字节拆分为本地命中与 demand miss 两部分；切换相位的回源占比直接反映切换代价。",
               _opt(grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    legend=_legend()), h=320)


def fig_cache_pressure(data):
    cells = _cells(data, "traincache-cold")
    series = []
    for rate in RATES:
        peak, evict = [], []
        for rho in RHOS:
            c = _cell_by(cells, rho, rate)
            peak.append(round(_avg(c["reps"], "cache_peak_ratio"), 3) if c else None)
            evict.append(round(_avg(c["reps"], "evict_blocks_run")) if c else None)
        series.append(_s(f"{rate}mbit 峰值/容量", peak, RATE_COLORS[rate], "line"))
        series.append(_s(f"{rate}mbit 淘汰块数", evict, RATE_COLORS[rate], "bar",
                         yAxisIndex=1, opacity=0.35))
    return fig("cache-pressure", "冷缓存场景缓存压力：峰值占用比与淘汰块数",
               "峰值与容量之比大于 1 表示运行期确实发生淘汰；淘汰块数随 ρ 的走势用于判断容量是否为瓶颈。",
               _opt(grid={"left": "8%", "right": 76, "top": 76, "bottom": 56},
                    xAxis=_x("ρ", [str(r) for r in RHOS]),
                    yAxis=[_y("峰值 / 容量"), _y("淘汰块数", nameGap=16, splitLine={"show": False})],
                    series=series, legend=_legend(),
                    tooltip={"trigger": "axis", "confine": True}), h=400)


def fig_prefetch_efficacy(data):
    cells = _cells(data, "traincache-warm")
    prefetch, demand, p50, p90 = [], [], [], []
    for rho in RHOS:
        cs = [c for c in cells if c["rho"] == rho]
        prefetch.append(_fmt_mb(sum(_avg(c["reps"], "prefetch_bytes_run") or 0 for c in cs) / len(cs)))
        demand.append(_fmt_mb(sum(_avg(c["reps"], "demand_miss_bytes_steady") or 0 for c in cs) / len(cs)))
        p50.append(round(sum(_avg(c["reps"], "prefetch_task_duration_p50_s") or 0 for c in cs) / len(cs), 3))
        p90.append(round(sum(_avg(c["reps"], "prefetch_task_duration_p90_s") or 0 for c in cs) / len(cs), 3))
    return fig("prefetch-efficacy", "预热场景预取有效性：预取字节与稳态 demand miss",
               "预取字节显著高于 demand miss 表示预取领先于需求发生；任务时长 p50/p90 读右轴。",
               _opt(grid={"left": "8%", "right": 76, "top": 76, "bottom": 56},
                    xAxis=_x("ρ", [str(r) for r in RHOS]),
                    yAxis=[_y("字节 (MB)"), _y("任务时长 (s)", nameGap=16, splitLine={"show": False})],
                    series=[_s("预取字节", prefetch, C["purple"]),
                            _s("稳态 demand miss", demand, C["orange"]),
                            _s("任务时长 p50", p50, C["teal"], "line", yAxisIndex=1),
                            _s("任务时长 p90", p90, C["rose"], "line", yAxisIndex=1)],
                    legend=_legend()), h=380)


def fig_warm_cost(data):
    cold = _cells(data, "traincache-cold")
    warm = _cells(data, "traincache-warm")
    cats, warmup, saving = [], [], []
    for rate in WARM_RATES:
        for rho in RHOS:
            wc = _cell_by(warm, rho, rate)
            cc = _cell_by(cold, rho, rate)
            if not (wc and cc):
                continue
            cats.append(f"ρ{rho}/{rate}")
            warmup.append(round(_avg(wc["reps"], "warmup_seconds"), 1))
            saving.append(round(_avg(cc["reps"], "startup_makespan")
                                - _avg(wc["reps"], "startup_makespan"), 1))
    return fig("warm-cost", "预热成本与收益：预热时长与启动耗时节省",
               "节省 = 同格冷启动启动耗时 − 预热启动耗时；柱高低于折线时预热不划算。",
               _opt(xAxis=_x("格", cats, axisLabel={"rotate": 35}),
                    yAxis=_y("秒"),
                    series=[_s("预热时长", warmup, C["orange"]),
                            _s("启动节省", saving, C["teal"], "line")],
                    legend=_legend()), h=380)


# ---- 时间展开 ----

def _phase_areas(steps):
    areas, cur, start = [], None, 0
    for i, s in enumerate(steps):
        ph = s.get("phase", "steady")
        if ph != cur:
            if cur is not None:
                areas.append([{"xAxis": start, "itemStyle": {"color": PHASE_COLORS.get(cur, "#fff")}},
                              {"xAxis": i - 1}])
            cur, start = ph, i
    if cur is not None:
        areas.append([{"xAxis": start, "itemStyle": {"color": PHASE_COLORS.get(cur, "#fff")}},
                      {"xAxis": len(steps) - 1}])
    return areas


def fig_timeline_sm(data, group, fid, title):
    cells = _cells(data, group)
    rates = RATES if group.endswith("cold") else WARM_RATES
    ncols, nrows = len(RHOS), len(rates)
    grids, xaxes, yaxes, series, titles = [], [], [], [], []
    gw, gh = 15.5, 22
    for ri, rate in enumerate(rates):
        for ci, rho in enumerate(RHOS):
            i = ri * ncols + ci
            c = _cell_by(cells, rho, rate)
            left, top = 4 + ci * 19.5, 8 + ri * 30
            grids.append({"left": f"{left}%", "top": f"{top}%",
                          "width": f"{gw}%", "height": f"{gh}%"})
            titles.append({"text": f"ρ{rho} · {rate}mbit",
                           "left": f"{left}%", "top": f"{top - 5}%",
                           "textStyle": {"fontSize": 11, "color": C["mut"], "fontWeight": 600}})
            steps = c["steps"] if c else []
            xaxes.append({"type": "value", "gridIndex": i, "min": 0,
                          "max": len(steps), "axisLabel": {"color": C["mut"], "fontSize": 9},
                          "splitLine": {"show": False}})
            yaxes.append({"type": "value", "gridIndex": i, "name": "wait(s)" if ci == 0 else "",
                          "nameTextStyle": {"fontSize": 9, "color": C["mut"]},
                          "axisLabel": {"color": C["mut"], "fontSize": 9},
                          "splitLine": {"lineStyle": {"color": C["grid"]}}})
            series.append({"name": c["name"] if c else "", "type": "line",
                           "data": [[s["global_step"], s["wait_s"]] for s in steps],
                           "showSymbol": False, "lineStyle": {"width": 1.2, "color": C["blue"]},
                           "xAxisIndex": i, "yAxisIndex": i,
                           "markArea": {"silent": True, "data": _phase_areas(steps)}})
    note = ("每格一条代表 rep 的逐步等待曲线，底色为相位（橙：启动，蓝：稳态，粉：切换）；"
            "小倍数按 ρ 排列，切换尖峰的高度差即带宽代价。")
    return fig(fid, title, note,
               _opt(title=titles, grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    tooltip={"trigger": "axis", "confine": True}),
               h=240 * nrows + 80)


def fig_shard_heatmap(data):
    cells = _cells(data, "traincache-cold")
    grids, xaxes, yaxes, series, titles, vmaps = [], [], [], [], [], []
    bucket = 8
    for i, rate in enumerate(RATES):
        c = _cell_by(cells, 4, rate)
        steps = c["steps"] if c else []
        nb = (len(steps) + bucket - 1) // bucket
        counts = {}
        for s in steps:
            key = (s["global_step"] // bucket, int(s["shard"]))
            counts[key] = counts.get(key, 0) + 1
        grids.append({"left": 60 + i * 340, "width": 280, "top": 50, "height": 220})
        titles.append({"text": f"ρ4 · {rate}mbit", "left": 60 + i * 340, "top": 26,
                       "textStyle": {"fontSize": 12, "color": C["mut"], "fontWeight": 600}})
        xaxes.append({"type": "category", "data": [str(b * bucket) for b in range(nb)],
                      "gridIndex": i, "name": "step", "nameLocation": "middle", "nameGap": 26,
                      "axisLabel": {"color": C["mut"], "fontSize": 9}})
        yaxes.append({"type": "category", "data": [str(s) for s in range(16)],
                      "gridIndex": i, "axisLabel": {"color": C["mut"], "fontSize": 9}})
        series.append({"type": "heatmap", "xAxisIndex": i, "yAxisIndex": i,
                       "data": [[b, sh, n] for (b, sh), n in sorted(counts.items())],
                       "itemStyle": {"borderColor": "#fff", "borderWidth": 0.5}})
    vmaps = [{"min": 0, "max": bucket, "calculable": False, "orient": "horizontal",
              "left": "center", "bottom": 0, "itemWidth": 12, "itemHeight": 80,
              "textStyle": {"color": C["mut"], "fontSize": 10},
              "inRange": {"color": ["#eef4fd", "#3b6fd4", "#1d3f7a"]},
              "seriesIndex": [0, 1, 2]}]
    return fig("shard-heatmap", "分片访问热力图（ρ = 4，三个带宽档）",
               "横轴为训练步（每 8 步聚合为一格），纵轴为分片号；per-epoch shuffle 使每个 epoch 的访问顺序重排，切换后的集中重访问清晰可见。",
               _opt(title=titles, grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    visualMap=vmaps, tooltip={"show": False}), h=340)


def fig_recovery_summary(data):
    cats, per = [], {r: [] for r in RATES}
    for c in data["cells"]:
        # 冷/预热同 ρ 同带宽的格名会重复，必须带分组前缀，否则散点挤到同一类目上
        g = "冷" if c["group"].endswith("cold") else "预热"
        key = f"{g}·ρ{c['rho']}/{c['rate']}"
        cats.append(key)
        for ev in c["recovery"]:
            t = ev.get("recovery_time_s")
            if t is None:
                continue
            per.setdefault(c["rate"], []).append(
                {"value": [key, t],
                 "symbol": "triangle" if ev.get("censored") else "circle",
                 "symbolSize": 9})
    series = [{"name": f"{r}mbit", "type": "scatter", "data": pts,
               "itemStyle": {"color": RATE_COLORS.get(r, C["blue"])}, "symbolSize": 9}
              for r, pts in per.items() if pts]
    return fig("recovery-summary", "epoch 切换恢复时长分布",
               "每个点对应一次 epoch 切换（recovery.json 口径：切换时刻至回落点）；三角形为删失样本"
               "（未回落到阈值内）；横轴按冷缓存、预热分组排列，颜色区分带宽档。",
               _opt(grid={"left": "8%", "right": "6%", "top": 56, "bottom": 96},
                    xAxis=_x("实验格", cats, axisLabel={"rotate": 45, "fontSize": 9, "interval": 0}),
                    yAxis=_y("恢复时长 (s)"),
                    series=series, legend=_legend(),
                    tooltip={"trigger": "item", "confine": True}), h=440)


# ---- 数据质量 ----

def fig_d25_residual(data):
    cats, vals, colors = [], [], []
    for c in data["cells"]:
        d = _avg(c["reps"], "origin_reconcile_diff")
        cats.append(c["name"].removeprefix("compare-traincache-"))
        vals.append(round((d or 0) / 1e6, 2))
        colors.append(C["teal"] if abs(d or 0) < 0.02 * 1.1e10 else C["rose"])
    return fig("d25-residual", "源站出站字节与 agent 回源的 D25 对账残差",
               "残差应接近 0；超过 2% 读字节的实验格标红。该图是测量装置自身的误差台账，不反映系统行为。",
               _opt(xAxis=_x("格", cats, axisLabel={"rotate": 45, "fontSize": 9}),
                    yAxis=_y("残差 (MB)"),
                    series=[{"type": "bar", "data": [
                        {"value": v, "itemStyle": {"color": co}}
                        for v, co in zip(vals, colors)], "barMaxWidth": 18}],
                    tooltip={"trigger": "axis", "confine": True}), h=380)


def fig_sampling_quality(data):
    cats = [c["name"].removeprefix("compare-traincache-") for c in data["cells"]]
    gaps = [round(_avg(c["reps"], "sample_max_gap_s") or 0, 1) for c in data["cells"]]
    voids = [sum(1 for r in c["reps"] if str(r.get("void")).lower() == "true")
             for c in data["cells"]]
    return fig("sampling-quality", "采样质量：最大采样间隔与 void 记录数",
               "间隔应接近 5s 采样周期，虚线为 1.5 倍周期；void 计数非零的实验格已列入重跑清单。",
               _opt(xAxis=_x("格", cats, axisLabel={"rotate": 45, "fontSize": 9}),
                    yAxis=[_y("最大间隔 (s)"), _y("void 数", splitLine={"show": False})],
                    series=[_s("sample_max_gap", gaps, C["blue"], "bar",
                               markLine={"silent": True, "symbol": "none",
                                         "lineStyle": {"type": "dashed", "color": C["rose"]},
                                         "data": [{"yAxis": 7.5}]}),
                            _s("void", voids, C["rose"], "line", yAxisIndex=1)],
                    legend=_legend()), h=380)


# ---- e2e ----

def _norm_series(rep):
    """mock 与真实 series.json 统一成 {name: [{labels, points:[(t_rel, v)]}]}。"""
    raw = rep.get("series") or rep.get("series_raw", {}).get("queries", {})
    t0 = rep.get("t0")
    if t0 is None:
        t0 = _parse_ts(rep.get("series_raw", {}).get("t_submit")) or 0
    out = {}
    for name, q in raw.items():
        ss = []
        for s in q.get("series", []):
            pts = [(float(t) - t0, float(v)) for t, v in s.get("values", [])]
            ss.append({"labels": s.get("metric", {}), "points": pts})
        out[name] = ss
    return out, t0


def _parse_ts(v):
    if not v:
        return None
    from datetime import datetime
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _rate(pts, gap=15):
    """计数器→速率；断档（中断窗口）处返回 None 断开折线。"""
    out = []
    for (t1, v1), (t2, v2) in zip(pts, pts[1:]):
        dt = t2 - t1
        if dt <= 0 or dt > gap or v2 < v1:
            out.append([t2, None])
        else:
            out.append([t2, round((v2 - v1) / dt, 3)])
    return out


def _steps_timed(steps):
    t, out = 0.0, []
    for s in steps:
        t += s.get("step_s", 0.2)
        out.append([round(t, 2), s.get("wait_s", 0)])
    return out


def fig_e2e_bars(data):
    scs = [e["scenario"] for e in data["e2e"]]
    mk = [round(sum(r["total_makespan"] for r in e["reps"]) / len(e["reps"]), 1)
          for e in data["e2e"]]
    rec = [round(sum(r["recovery_time_s"] for r in e["reps"]) / len(e["reps"]), 1)
           for e in data["e2e"]]
    err = [sum(r["fetch_errors_recovery"] for r in e["reps"]) for e in data["e2e"]]
    cn = {"checkpoint-resume": "checkpoint 续跑", "checkpoint-version": "数据版本切换",
          "dual-restart": "双重重启"}
    cats = [cn.get(s, s) for s in scs]
    return [fig("e2e-makespan-recovery", "端到端场景：全程耗时与恢复时长",
                f"柱为两阶段合计的全程耗时（2 次重复均值，读左轴），折线为中断至恢复出步的恢复时长（读右轴）；"
                f"恢复窗口内源站请求错误数三个场景合计 {sum(err)} 次，未单列成图。",
                _opt(grid={"left": "7%", "right": "7%", "top": 78, "bottom": 56},
                     xAxis=_x("场景", cats),
                     yAxis=[_y("全程耗时 (s)"), _y("恢复时长 (s)", splitLine={"show": False})],
                     series=[_s("全程耗时", mk, C["blue"]),
                             _s("恢复时长", rec, C["orange"], "line", yAxisIndex=1,
                                showSymbol=True, symbolSize=8)],
                     legend=_legend(), tooltip={"trigger": "axis", "confine": True}), h=380)]


def fig_e2e_overview(data, e, idx):
    rep = e["reps"][0]
    series_map, t0 = _norm_series(rep)
    interrupt, resume = rep.get("interrupt", 270), rep.get("resume", 295)
    def _ser(name, label, color, kind="line"):
        """series_map[name] 是 [{labels, points}] 列表；每个实体拆成一条曲线。"""
        for s in series_map.get(name, []):
            tag = next((str(v) for k, v in s["labels"].items()
                        if k in ("reason", "item", "pod") and v), "")
            yield (f"{label}:{tag}" if tag else label, s["points"], color, kind)

    rows = [
        ("训练侧 wait (s)", [("", _steps_timed(rep["steps"]), C["blue"], "line")]),
        ("预取队列/lead (块)", list(_ser("prefetch_queue_depth", "queue_depth", C["purple"]))
                            + list(_ser("prefetch_lead_blocks", "lead_blocks", C["teal"]))),
        ("淘汰速率 (块/s)", list(_ser("evict_blocks", "evict", C["rose"], "rate"))),
        ("回源速率 (MB/s)", list(_ser("fetch_bytes", "fetch", C["blue"], "rate"))),
        ("缓存占用/命中率", list(_ser("disk_usage_ratio", "disk_usage", C["teal"]))
                          + list(_ser("cache_hit_ratio", "hit_ratio", C["blue"]))),
        ("MinIO 出站 (MB/s)", list(_ser("minio_sent_bytes", "sent", C["orange"], "rate"))),
    ]
    _pal = (C["blue"], C["purple"], C["orange"], C["teal"], C["rose"])
    for i, row in enumerate(rows):
        rows[i] = (row[0], [(n, p, _pal[j % len(_pal)] if n.startswith("fetch:") else c, k)
                            for j, (n, p, c, k) in enumerate(row[1])])
    grids, xaxes, yaxes, series, titles = [], [], [], [], []
    n = len(rows)
    for i, (label, ss) in enumerate(rows):
        top = 6 + i * 15.2
        grids.append({"left": 70, "right": 30, "top": f"{top}%", "height": "9.8%"})
        titles.append({"text": label, "left": 70, "top": f"{top - 4.3}%",
                       "textStyle": {"fontSize": 11, "color": C["mut"], "fontWeight": 600}})
        xaxes.append({"type": "value", "gridIndex": i, "min": 0,
                      "axisLabel": {"show": i == n - 1, "color": C["mut"], "fontSize": 10},
                      "name": "t (s)" if i == n - 1 else "", "nameLocation": "middle", "nameGap": 26,
                      "splitLine": {"show": False}})
        yaxes.append({"type": "value", "gridIndex": i,
                      "axisLabel": {"color": C["mut"], "fontSize": 9},
                      "splitLine": {"lineStyle": {"color": C["grid"]}}})
        for name, pts, color, kind in ss:
            if kind == "rate":
                pts = _rate(pts)
                if "MB" in label:
                    pts = [[t, None if v is None else round(v / 1e6, 2)] for t, v in pts]
            series.append({"name": name, "type": "line", "data": pts,
                           "showSymbol": False, "lineStyle": {"width": 1.5, "color": color},
                           "xAxisIndex": i, "yAxisIndex": i,
                           "markLine": {"silent": True, "symbol": "none",
                                        "lineStyle": {"type": "dashed", "color": "#999"},
                                        "label": {"show": i == 0, "fontSize": 9},
                                        "data": [{"xAxis": interrupt, "name": "中断"},
                                                 {"xAxis": resume, "name": "恢复"}]}})
    return fig(f"e2e-overview-{idx}", f"端到端系统行为总览：{e['scenario']}（代表轮次）",
               "六个层面共用时间轴：训练侧等待、预取队列与 lead、淘汰速率、回源速率、缓存占用与命中率、源站出站；"
               "虚线为中断与恢复时刻，断档为进程中断窗口；计数器已做差分转为速率，底部滑尺可缩放。",
               _opt(title=titles, grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    axisPointer={"link": [{"xAxisIndex": "all"}]},
                    dataZoom=[{"type": "slider", "xAxisIndex": list(range(n)),
                               "bottom": 4, "height": 18},
                              {"type": "inside", "xAxisIndex": list(range(n))}],
                    legend=_legend(show=False),
                    tooltip={"trigger": "axis", "confine": True}), h=760)


def fig_e2e_zoom(data):
    grids, xaxes, yaxes, series, titles = [], [], [], [], []
    for i, e in enumerate(data["e2e"]):
        rep = e["reps"][0]
        smap, _ = _norm_series(rep)
        interrupt, resume = rep.get("interrupt", 270), rep.get("resume", 295)
        lo, hi = interrupt - 60, resume + 120
        grids.append({"left": 86, "right": 70, "top": 40 + i * 150, "height": 100})
        titles.append({"text": e["scenario"], "left": 70, "top": 18 + i * 150,
                       "textStyle": {"fontSize": 12, "color": C["mut"], "fontWeight": 600}})
        xaxes.append({"type": "value", "gridIndex": i, "min": lo, "max": hi,
                      "axisLabel": {"color": C["mut"], "fontSize": 10},
                      "name": "t (s)", "nameLocation": "middle", "nameGap": 26})
        yaxes.append({"type": "value", "gridIndex": i,
                      "axisLabel": {"color": C["mut"], "fontSize": 9},
                      "splitLine": {"lineStyle": {"color": C["grid"]}}})
        demand = next((s for s in smap.get("fetch_bytes", [])
                       if s["labels"].get("reason") == "demand"), None)
        if demand:
            pts = [[t, None if v is None else round(v / 1e6, 2)]
                   for t, v in _rate(demand["points"])]
            series.append({"name": "demand 回源 MB/s", "type": "line", "data": pts,
                           "showSymbol": False, "lineStyle": {"width": 1.8, "color": C["blue"]},
                           "xAxisIndex": i, "yAxisIndex": i,
                           "markArea": {"silent": True, "itemStyle": {"color": "#f6e8f0"},
                                        "data": [[{"xAxis": interrupt}, {"xAxis": resume}]]}})
        q = smap.get("prefetch_queue_depth", [])
        if q:
            series.append({"name": "预取队列", "type": "line",
                           "data": q[0]["points"], "showSymbol": False,
                           "lineStyle": {"width": 1.5, "color": C["purple"], "type": "dashed"},
                           "xAxisIndex": i, "yAxisIndex": i})
    return fig("e2e-zoom", "中断与恢复窗口的放大视图（三个场景）",
               "横轴截取中断前 60s 至恢复后 120s；粉色底纹为中断窗口；demand 回源速率的尖峰与预取队列的重建过程逐点可读，是恢复判据的直接证据。",
               _opt(title=titles, grid=grids, xAxis=xaxes, yAxis=yaxes, series=series,
                    legend=_legend()), h=520)


# ---- 分族结果（已有口径搬入） ----

import re  # noqa: E402


def _norm_family(name, rows):
    """真实 summary 行 → 与 mock 同构的键。"""
    out = []
    for r in rows:
        cond = str(r.get("cond") or r.get("_cell") or "")
        d = dict(r)
        # cond 解析出的键优先；解析不到时沿用行里已有的键（mock 行直接给键），最后退回 cond。
        def keep(key, val):
            if val not in (None, "") or not d.get(key):
                d[key] = val

        if name == "gateway":
            m = re.search(r"gateway-(g\d)", cond)
            keep("arm", m.group(1) if m else cond)
            d["source_bytes"] = (d.get("source_bytes")
                                 or _f(r, "origin_egress_bytes") or _f(r, "fetch_bytes_run"))
        elif name == "evict":
            m = re.search(r"evict-(\w+)", cond)
            keep("policy", m.group(1) if m else cond)
        elif name == "prefetch":
            d["lookahead"] = _f(r, "prefetch_lookahead") or d.get("lookahead")
            d["fanout"] = _f(r, "prefetch_fanout") or d.get("fanout")
        elif name == "warmup":
            m = re.search(r"warmup-(\w+)-w(\d+)", cond)
            if m:
                d["gate"], d["window"] = m.group(1), int(m.group(2))
            else:
                keep("gate", d.get("gate") or cond)
                d["window"] = d.get("window") or 0
        elif name == "switch":
            m = re.search(r"switch-(.+?)-rho", cond)
            keep("scenario", m.group(1) if m else cond)
            d["recovery_time_s"] = _f(r, "e2e_recovery_time_s") or _f(r, "recovery_time_s")
        elif name == "multiconsumer":
            m = re.search(r"multi-hot(\d+)-(\w+)", cond)
            keep("hot", f"{m.group(1)}%" if m else cond)
            keep("placement", m.group(2) if m else "")
        elif name == "rangepar":
            m = re.search(r"rangepar-(l\d)-par-(\w+)", cond)
            if m:
                d["level"], d["par"] = m.group(1), m.group(2)
            else:
                keep("level", cond)
                keep("par", d.get("par") or "")
        d["rho"] = _f(r, "rho")
        out.append(d)
    return out


def _fam_bar(fid, title, note, rows, xkey, ykey, series_key=None, ylabel="", h=320):
    xs = sorted({str(r.get(xkey)) for r in rows})
    if series_key:
        ss = sorted({str(r.get(series_key)) for r in rows})
        series = []
        for j, sv in enumerate(ss):
            vals = []
            for xv in xs:
                vv = [_f(r, ykey) for r in rows
                      if str(r.get(xkey)) == xv and str(r.get(series_key)) == sv]
                vv = [v for v in vv if v is not None]
                vals.append(round(sum(vv) / len(vv), 2) if vv else None)
            series.append(_s(sv, vals, list(RATE_COLORS.values())[j % 3]))
        opt = _opt(xAxis=_x(xkey, xs), yAxis=_y(ylabel), series=series, legend=_legend())
    else:
        vals = []
        for xv in xs:
            vv = [_f(r, ykey) for r in rows if str(r.get(xkey)) == xv]
            vv = [v for v in vv if v is not None]
            vals.append(round(sum(vv) / len(vv), 2) if vv else None)
        opt = _opt(xAxis=_x(xkey, xs), yAxis=_y(ylabel),
                   series=[_s(ylabel, vals, C["blue"])])
    return fig(fid, title, note, opt, h=h)


def fig_families(data):
    fam = data["families"]
    figs = []

    def rows(name):
        return _norm_family(name, fam.get(name, []))

    def col(rws, xk, yk, scale=1.0):
        """按 xk 分组取 yk 均值，返回（类目列表，数值列表）。"""
        xs = sorted({str(x.get(xk)) for x in rws})
        vals = []
        for xv in xs:
            vv = [_f(x, yk) for x in rws if str(x.get(xk)) == xv]
            vv = [v for v in vv if v is not None]
            vals.append(round(sum(vv) / len(vv) * scale, 3) if vv else None)
        return xs, vals

    if fam.get("gateway"):
        r = rows("gateway")
        arms, mk = col(r, "arm", "total_makespan")
        _, egress = col(r, "arm", "source_bytes", 1e-9)
        figs.append(fig("gateway-overview", "网关族：三档网关配置的全程耗时与源站出站字节",
                        "柱为全程耗时（读左轴），折线为源站出站字节（读右轴）；g0 为无网关基线，"
                        "g1、g2 为两级合并回源配置。",
                        _opt(grid={"left": "7%", "right": "7%", "top": 78, "bottom": 52},
                             xAxis=_x("网关配置", arms),
                             yAxis=[_y("全程耗时 (s)"), _y("源站出站 (GB)", splitLine={"show": False})],
                             series=[_s("全程耗时", mk, C["blue"]),
                                     _s("源站出站", egress, C["orange"], "line", yAxisIndex=1,
                                        showSymbol=True, symbolSize=8)],
                             legend=_legend(), tooltip={"trigger": "axis", "confine": True}), h=360))
    if fam.get("evict"):
        r = rows("evict")
        rhos = sorted({_f(x, "rho") for x in r if _f(x, "rho") is not None})
        policies = sorted({str(x.get("policy")) for x in r})
        series = []
        for j, p in enumerate(policies):
            vals = []
            for rho in rhos:
                vv = [_f(x, "total_makespan") for x in r
                      if str(x.get("policy")) == p and _f(x, "rho") == rho]
                vv = [v for v in vv if v is not None]
                vals.append(round(sum(vv) / len(vv), 1) if vv else None)
            series.append(_s(p, vals, list(RATE_COLORS.values())[j % 3], "line",
                             showSymbol=True, symbolSize=7))
        figs.append(fig("evict-makespan", "淘汰族：三种淘汰算法的全程耗时随缓存容量比的变化",
                        "三条曲线分别为 heat、LRU、LFU；ρ 由 1 增至 8 时缓存不再是瓶颈，算法间的差异应随之收窄。",
                        _opt(grid={"left": "8%", "right": "8%", "top": 70, "bottom": 52},
                             xAxis=_x("ρ", [f"{v:g}" for v in rhos]),
                             yAxis=_y("全程耗时 (s)"), series=series, legend=_legend(),
                             tooltip={"trigger": "axis", "confine": True}), h=340))
        rhos_axis = sorted({str(_f(x, "rho")) for x in r if _f(x, "rho") is not None})
        hot_series = []
        for j, rho in enumerate(rhos_axis):
            vals = []
            for p in policies:
                vv = [_f(x, "b_evictions_of_hot") for x in r
                      if str(x.get("policy")) == p and str(_f(x, "rho")) == rho]
                vv = [v for v in vv if v is not None]
                vals.append(round(sum(vv) / len(vv), 1) if vv else None)
            hot_series.append(_s(f"ρ={rho}", vals, list(RATE_COLORS.values())[j % 3]))
        figs.append(fig("evict-hot", "淘汰族：热块被挤出次数",
                        "热块被挤出意味着淘汰算法误伤正在使用的数据；数值越低越好，按淘汰算法分组、ρ 分色。",
                        _opt(grid={"left": "8%", "right": "8%", "top": 70, "bottom": 52},
                             xAxis=_x("淘汰算法", policies), yAxis=_y("被挤出次数"),
                             series=hot_series, legend=_legend(),
                             tooltip={"trigger": "axis", "confine": True}), h=320))
    if fam.get("prefetch"):
        r = rows("prefetch")
        las = sorted({_f(x, "lookahead") for x in r if _f(x, "lookahead") is not None})
        fans = sorted({_f(x, "fanout") for x in r if _f(x, "fanout") is not None})
        rhos = sorted({_f(x, "rho") for x in r if _f(x, "rho") is not None})
        grids, xaxes, yaxes, series, titles = [], [], [], [], []
        ncol = max(1, len(rhos))
        gw = (100 - 12) / ncol - 4
        for i, rho in enumerate(rhos):
            left = 7 + i * (gw + 4)
            grids.append({"left": f"{left}%", "width": f"{gw}%", "top": 84, "height": 188})
            titles.append({"text": f"ρ={rho:g}", "left": f"{left}%", "top": 44,
                           "textStyle": {"fontSize": 12, "color": C["mut"], "fontWeight": 600}})
            xaxes.append(_x("lookahead（块）", [str(int(v)) for v in las], gridIndex=i))
            yaxes.append({"type": "value", "gridIndex": i,
                          "axisLabel": {"color": C["mut"], "fontSize": 10},
                          "splitLine": {"lineStyle": {"color": C["grid"]}}})
            for j, fan in enumerate(fans):
                vals = []
                for la in las:
                    vv = [_f(x, "total_makespan") for x in r
                          if _f(x, "lookahead") == la and _f(x, "fanout") == fan
                          and _f(x, "rho") == rho]
                    vv = [v for v in vv if v is not None]
                    vals.append(round(sum(vv) / len(vv), 1) if vv else None)
                series.append(_s(f"fanout={fan:g}", vals,
                                 list(RATE_COLORS.values())[j % 3], "line",
                                 xAxisIndex=i, yAxisIndex=i))
        figs.append(fig("prefetch-lookahead", "预取族：预取窗口与 fanout 对全程耗时的影响",
                        "纵轴为全程耗时（s），按 ρ 分面；预取窗口越大预取越超前，fanout 决定并行回源的并发度。",
                        _opt(title=titles, grid=grids, xAxis=xaxes, yAxis=yaxes,
                             series=series, legend=_legend()), h=320))
    if fam.get("warmup"):
        r = rows("warmup")
        figs.append(_fam_bar("warmup-startup", "预热族：预热窗口与门控策略对启动耗时的影响",
                             "gated 为门控放行后出步，free 为自由读取；窗口越长启动越慢，稳态越稳定。",
                             r, "window", "startup_makespan", "gate", "启动耗时 (s)"))
    if fam.get("switch"):
        r = rows("switch")
        cats, rec = col(r, "scenario", "recovery_time_s")
        figs.append(fig("switch-recovery", "切换族：四种访问顺序的恢复时长",
                        "横轴为恢复时长，纵轴为访问顺序（fixed、per-epoch、curriculum、curriculum-narrow）。",
                        _opt(grid={"left": "22%", "right": "10%", "top": 56, "bottom": 56},
                             xAxis={"type": "value", "name": "恢复时长 (s)", "nameLocation": "middle",
                                    "nameGap": 30, "axisLabel": {"color": C["mut"]},
                                    "splitLine": {"lineStyle": {"color": C["grid"]}}},
                             yAxis={"type": "category", "data": cats,
                                    "axisLabel": {"color": C["mut"]}, "axisTick": {"show": False},
                                    "axisLine": {"show": False}},
                             series=[_s("恢复时长", rec, C["teal"])],
                             tooltip={"trigger": "axis", "confine": True}), h=320))
    if fam.get("multiconsumer"):
        r = rows("multiconsumer")
        for x in r:
            x["_k"] = f"{x.get('hot')}·{x.get('placement')}"
        cats, hit = col(r, "_k", "hit_rate")
        _, mk = col(r, "_k", "total_makespan")
        figs.append(fig("multi-overview", "多消费者族：热比例与放置策略对命中率、全程耗时的影响",
                        "柱为 steady 命中率（读左轴），折线为全程耗时（读右轴）；spread 表示读端分散至各节点，"
                        "colocated 表示集中放置；横轴为热数据比例与放置方式的组合。",
                        _opt(grid={"left": "7%", "right": "7%", "top": 78, "bottom": 56},
                             xAxis=_x("热比例·放置方式", cats),
                             yAxis=[_y("命中率", max=1), _y("全程耗时 (s)", splitLine={"show": False})],
                             series=[_s("命中率", hit, C["teal"]),
                                     _s("全程耗时", mk, C["orange"], "line", yAxisIndex=1,
                                        showSymbol=True, symbolSize=8)],
                             legend=_legend(), tooltip={"trigger": "axis", "confine": True}), h=360))
    if fam.get("rangepar"):
        r = rows("rangepar")
        levels = sorted({str(x.get("level")) for x in r})
        pars = sorted({str(x.get("par")) for x in r})
        series, speedup = [], []
        for j, p in enumerate(pars):
            vals = []
            for lv in levels:
                vv = [_f(x, "total_makespan") for x in r
                      if str(x.get("level")) == lv and str(x.get("par")) == p]
                vv = [v for v in vv if v is not None]
                vals.append(round(sum(vv) / len(vv), 1) if vv else None)
            series.append(_s(f"并行 {p}", vals,
                             C["blue"] if p == "off" else C["teal"]))
        for lv in levels:
            off = [_f(x, "total_makespan") for x in r
                   if str(x.get("level")) == lv and str(x.get("par")) == "off"]
            on = [_f(x, "total_makespan") for x in r
                  if str(x.get("level")) == lv and str(x.get("par")) == "on"]
            off = [v for v in off if v]; on = [v for v in on if v]
            speedup.append(round(sum(off) / len(off) / (sum(on) / len(on)), 3)
                           if off and on else None)
        series.append(_s("加速比（off/on）", speedup, C["orange"], "line", yAxisIndex=1,
                         showSymbol=True, symbolSize=8,
                         markLine={"silent": True, "symbol": "none",
                                   "lineStyle": {"type": "dashed", "color": C["mut"]},
                                   "label": {"fontSize": 10, "formatter": "1.0"},
                                   "data": [{"yAxis": 1.0}]}))
        figs.append(fig("rangepar-overview", "并行 Range 族：限速档位下并行开关的耗时与加速比",
                        "柱为全程耗时（读左轴，off/on 两档），折线为 off/on 的加速比（读右轴），虚线为 1.0；"
                        "tbf 为聚合限速，加速比接近 1 说明并行 Range 不改变总耗时。",
                        _opt(grid={"left": "7%", "right": "7%", "top": 78, "bottom": 56},
                             xAxis=_x("限速档位", levels),
                             yAxis=[_y("全程耗时 (s)"),
                                    _y("加速比", splitLine={"show": False}, min=0, max=1.5)],
                             series=series, legend=_legend(),
                             tooltip={"trigger": "axis", "confine": True}), h=360))
    if fam.get("chaos"):
        r = fam["chaos"]
        faults = sorted({x["fault"] for x in r})
        rhos = sorted({str(x.get("rho", "")) for x in r})
        grids, xaxes, yaxes, series, titles = [], [], [], [], []
        for i, rho in enumerate(rhos):
            grids.append({"left": 60 + i * 340, "width": 280, "top": 42, "height": 200})
            titles.append({"text": f"ρ={rho}", "left": 60 + i * 340, "top": 20,
                           "textStyle": {"fontSize": 12, "color": C["mut"], "fontWeight": 600}})
            xaxes.append(_x("故障", faults, gridIndex=i))
            yaxes.append(_y("轮次数", gridIndex=i))
            for status, color in (("PASS", "#2f8f6b"), ("FAIL", "#c0392b")):
                vals = [sum(1 for x in r if x["fault"] == f
                            and str(x.get("rho", "")) == rho and x["status"] == status)
                        for f in faults]
                series.append(_s(status, vals, color, "bar", stack="s",
                                 xAxisIndex=i, yAxisIndex=i))
        figs.append(fig("chaos-results", "混沌族：各故障注入场景的判定结果",
                        "PASS/FAIL 按轮次计数，判定标准见 docs/4.7。",
                        _opt(title=titles, grid=grids, xAxis=xaxes, yAxis=yaxes,
                             series=series, legend=_legend()), h=320))
    return figs


# ---------------------------------------------------------------------------
# 组装与 HTML
# ---------------------------------------------------------------------------

CSS = """
:root{--bg:#f6f7f9;--ink:#1f2328;--mut:#5a616e;--line:#e3e6eb;--grid:#eceff3;
  --c1:#3b6fd4;--c2:#7a5cd6;--c3:#e8963c;--c4:#3aa6a6;--c5:#c95f81}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{margin:0;background:var(--bg);color:var(--ink);
  font-family:"Helvetica Neue",Arial,"PingFang SC","Microsoft YaHei",sans-serif}
nav{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:18px;
  padding:11px 36px;background:rgba(255,255,255,.92);backdrop-filter:blur(8px);
  border-bottom:1px solid var(--line);flex-wrap:wrap}
nav .brand{font-size:13px;font-weight:700}
nav a{color:var(--mut);text-decoration:none;font-size:12.5px}
nav a:hover{color:var(--c1)}
#progress{position:absolute;left:0;bottom:-1px;height:2px;width:0;background:var(--c1)}
header{padding:30px 36px 4px}
header h1{margin:0;font-size:24px;font-weight:700;
  font-family:Georgia,"Times New Roman","Songti SC",serif}
header .sub{margin-top:8px;font-size:12.5px;color:var(--mut)}
.badge{display:inline-block;background:var(--c3);color:#fff;font-weight:700;
  padding:2px 10px;border-radius:4px;margin-left:10px;font-size:12px;vertical-align:middle}
.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:14px;padding:20px 36px 0}
.kpi{position:relative;background:#fff;border:1px solid var(--line);border-radius:8px;
  padding:14px 16px 12px;box-shadow:0 1px 3px rgba(30,40,90,.05)}
.kpi::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;
  border-radius:8px 0 0 8px;background:var(--c1)}
.kpi:nth-child(2)::before{background:var(--c2)}
.kpi:nth-child(3)::before{background:var(--c3)}
.kpi:nth-child(4)::before{background:var(--c4)}
.kpi:nth-child(5)::before{background:var(--c5)}
.kpi .v{font-size:25px;font-weight:700;font-variant-numeric:tabular-nums}
.kpi .l{font-size:12px;color:var(--mut);margin-top:5px;line-height:1.45}
.card{background:#fff;border:1px solid var(--line);border-radius:8px;margin:20px 36px;
  padding:16px 20px;box-shadow:0 1px 3px rgba(30,40,90,.05)}
.card h2{font-size:15.5px;margin:0 0 4px;font-weight:650}
.card h2 .fno{color:var(--c1);font-family:Georgia,serif;margin-right:8px}
.card p.note{font-size:12.5px;color:var(--mut);margin:0 0 10px;line-height:1.6}
.chart{width:100%}
section>h2.sec{margin:34px 36px 0;font-size:19px;font-weight:700;
  font-family:Georgia,"Times New Roman","Songti SC",serif;
  padding-bottom:8px;border-bottom:2px solid var(--line)}
table.dt{border-collapse:collapse;font-size:12.5px;width:100%;font-variant-numeric:tabular-nums}
table.dt th{color:var(--mut);font-weight:600;text-align:left;padding:6px 10px;
  border-bottom:1.5px solid var(--line)}
table.dt td{padding:5px 10px;border-bottom:1px solid var(--grid)}
footer{margin:40px 36px 30px;font-size:12px;color:var(--mut)}
@media (max-width:900px){.kpis{grid-template-columns:repeat(2,1fr)}
  .card,section>h2.sec,header,.kpis{margin-left:14px;margin-right:14px;padding-left:14px}}
"""

JS = """
const FIGS = __FIGS__;
const charts = [];
for (const f of FIGS) {
  const el = document.getElementById('chart-' + f.id);
  if (!el) continue;
  el.style.height = f.h + 'px';
  const ch = echarts.init(el);
  ch.setOption(f.option);
  charts.push(ch);
}
window.addEventListener('resize', () => charts.forEach(c => c.resize()));
const prog = document.getElementById('progress');
window.addEventListener('scroll', () => {
  const h = document.documentElement;
  prog.style.width = (h.scrollTop / (h.scrollHeight - h.clientHeight) * 100) + '%';
});
"""


def build_sections(data):
    secs = []
    secs.append(("main", "一、主结果", [
        fig_makespan_grid(data), fig_source_saving(data), fig_stall_struct(data),
        fig_hit_triple(data), fig_phase_bytes(data), fig_cache_pressure(data),
        fig_prefetch_efficacy(data), fig_warm_cost(data)]))
    secs.append(("timeline", "二、系统行为按时间展开", [
        fig_timeline_sm(data, "traincache-cold", "timeline-cold",
                        "冷缓存场景逐步等待时间线（15 格）"),
        fig_timeline_sm(data, "traincache-warm", "timeline-warm",
                        "预热场景逐步等待时间线（10 格）"),
        fig_shard_heatmap(data), fig_recovery_summary(data)]))
    secs.append(("quality", "三、数据质量", [
        fig_d25_residual(data), fig_sampling_quality(data)]))
    e2e_figs = fig_e2e_bars(data)
    for i, e in enumerate(data["e2e"]):
        e2e_figs.append(fig_e2e_overview(data, e, i))
    e2e_figs.append(fig_e2e_zoom(data))
    secs.append(("e2e", "四、端到端场景（Prometheus 全程序列）", e2e_figs))
    fam_figs = fig_families(data)
    if fam_figs:
        secs.append(("families", "五、分族实验结果", fam_figs))
    return secs


def kpis(data):
    cells = data["cells"]
    cold = _cells(data, "traincache-cold")
    warm = _cells(data, "traincache-warm")
    n_valid = sum(len(c["reps"]) for c in cells)
    best = max((_avg(c["reps"], "source_saving_ratio") or 0) for c in warm) if warm else 0
    c100 = [c for c in cold if c["rate"] == 100]
    stall = sum(_avg(c["reps"], "stall_ratio") or 0 for c in c100) / len(c100) if c100 else 0
    rec = [ev.get("recovery_time_s") for c in cells for ev in c["recovery"]
           if ev.get("recovery_time_s") is not None]
    return [
        (f"{len(cells)}", "实验格（cold 15 + warm 10）"),
        (f"{n_valid}", "有效 rep（void 已剔除）"),
        (f"{best:.0%}", "warm 最高源站节省率"),
        (f"{stall:.1%}", "cold 100mbit 平均等待占比"),
        (f"{(sum(rec)/len(rec) if rec else 0):.1f}s", "epoch 切换平均恢复时长"),
    ]


def render_html(data, secs):
    figs_flat = [f for _, _, fs in secs for f in fs]
    nav = "".join(f'<a href="#sec-{sid}">{name.split("、")[1]}</a>'
                  for sid, name, _ in secs)
    badge = '<span class="badge">假数据演示</span>' if data["mock"] else ""
    kpi_html = "".join(f'<div class="kpi"><div class="v">{v}</div><div class="l">{l}</div></div>'
                       for v, l in kpis(data))
    body = []
    n = 0
    for sid, name, fs in secs:
        body.append(f'<section id="sec-{sid}"><h2 class="sec">{name}</h2>')
        for f in fs:
            n += 1
            body.append(
                f'<div class="card"><h2><span class="fno">Fig. {n}</span>{f["title"]}</h2>'
                f'<p class="note">{f["note"]}</p>'
                f'<div class="chart" id="chart-{f["id"]}"></div></div>')
        body.append("</section>")
    setup_card = """
<div class="card"><h2>实验设置</h2>
<table class="dt">
<tr><th>项</th><th>值</th></tr>
<tr><td>数据集</td><td>16 × 256 MiB = 4 GiB，3 epoch，per-epoch shuffle</td></tr>
<tr><td>集群</td><td>kind 4 节点，MinIO 源站，tbf 聚合限速</td></tr>
<tr><td>缓存</td><td>ρ ∈ {0.5, 1, 2, 4, 8}，heat 淘汰，lookahead 16 块</td></tr>
<tr><td>带宽</td><td>cold 1000/300/100 mbit，warm 1000/300 mbit</td></tr>
<tr><td>完成判据</td><td>每格有效 rep ≥ 3，void 行剔除</td></tr>
<tr><td>e2e 序列</td><td>Prometheus 5s 采样，仅 e2e 期间部署，原始序列落 series.json</td></tr>
</table></div>"""
    figs_json = json.dumps(figs_flat, ensure_ascii=False)
    js = JS.replace("__FIGS__", figs_json)
    echarts = ECHARTS_JS.read_text()
    return f"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TrainCache 性能报告</title>
<style>{CSS}</style></head>
<body>
<nav><span class="brand">TrainCache 性能报告</span>{nav}<div id="progress"></div></nav>
<header><h1>TrainCache 缓存层性能实测报告{badge}</h1>
<div class="sub">compare 25 格 + e2e 3 场景 · 口径见各图注</div></header>
<div class="kpis">{kpi_html}</div>
{setup_card}
{''.join(body)}
<footer>生成器 bench/experiments/report_perf.py · 图注即口径 · 静态精选见 README</footer>
<script>{echarts}</script>
<script>{js}</script>
</body></html>"""


def export_png(data, out_dir):
    """README 静态精选：与报告同数据的 matplotlib 版。"""
    import plot_style
    plt = plot_style.setup("zh")
    fig_dir = out_dir / "fig"
    fig_dir.mkdir(parents=True, exist_ok=True)

    cold = _cells(data, "traincache-cold")
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    for ax, group in zip(axes, ("traincache-cold", "traincache-warm")):
        cells = _cells(data, group)
        rates = RATES if group.endswith("cold") else WARM_RATES
        for rate in rates:
            vals = []
            for rho in RHOS:
                c = _cell_by(cells, rho, rate)
                vals.append(_avg(c["reps"], "total_makespan") if c else None)
            ax.plot(RHOS, vals, marker="o", label=f"{rate}mbit",
                    color=RATE_COLORS[rate])
        ax.set_title("cold" if group.endswith("cold") else "warm")
        ax.set_xlabel("ρ")
        ax.set_ylabel("全程 makespan (s)")
        ax.legend()
    fig.tight_layout()
    fig.savefig(fig_dir / "makespan-grid.png", dpi=plot_style.SAVE_DPI)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 3.2))
    for rate in RATES:
        vals = []
        for rho in RHOS:
            c = _cell_by(cold, rho, rate)
            vals.append(_avg(c["reps"], "hit_rate") if c else None)
        ax.plot(RHOS, vals, marker="o", label=f"{rate}mbit", color=RATE_COLORS[rate])
    ax.set_xlabel("ρ")
    ax.set_ylabel("命中率（demand 口径）")
    ax.set_title("cold 命中率")
    ax.legend()
    fig.tight_layout()
    fig.savefig(fig_dir / "hit-rate.png", dpi=plot_style.SAVE_DPI)
    plt.close(fig)
    print(f"png: {fig_dir}")


def main():
    ap = argparse.ArgumentParser(description="性能报告生成器")
    ap.add_argument("--results-root", type=Path,
                    default=config.RESULTS_DIR)
    ap.add_argument("--out", type=Path, default=OUT_DEFAULT)
    ap.add_argument("--mock", action="store_true", help="假数据演示模式")
    ap.add_argument("--png", action="store_true", help="同时导出 README 静态 PNG")
    args = ap.parse_args()

    data = mock_data() if args.mock else load_real(args.results_root)
    if not data["cells"]:
        raise SystemExit("无 compare 产物；检查 --results-root 或用 --mock")
    secs = build_sections(data)
    n_figs = sum(len(fs) for _, _, fs in secs)
    args.out.mkdir(parents=True, exist_ok=True)
    html = render_html(data, secs)
    (args.out / "index.html").write_text(html, encoding="utf-8")
    print(f"report: {args.out / 'index.html'} ({n_figs} 图, {len(html)/1e6:.1f} MB)")
    if args.png:
        export_png(data, args.out)


if __name__ == "__main__":
    main()
