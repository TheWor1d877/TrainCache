#!/usr/bin/env python3
"""按族出图：读 results/<族>/<COND>/summary.csv，产出中英两套 PNG 与聚合 CSV。

只读数据层，不访问集群。图落 results/figures/，聚合值落 figures/data/。
某族没有产物时跳过，stderr 打印图名。
"""
from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parent.parent
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import plot_style  # noqa: E402
import stats  # noqa: E402

FIG_ROOT = config.RESULTS_DIR / "figures"
DATA_DIR = FIG_ROOT / "data"
LANGS = ("zh", "en")
LANG = "zh"

GROUP_ORDER = ["direct", "juicefs-cold", "juicefs-warm",
               "fluid-cold", "fluid-warm", "traincache-cold", "traincache-warm"]
GROUP_COLORS = {
    "direct": "#6f6f6f",
    "juicefs-cold": "#6fa9e0", "juicefs-warm": "#1f5f9e",
    "fluid-cold": "#f0a85a", "fluid-warm": "#c96a1c",
    "traincache-cold": "#a973d1", "traincache-warm": "#5b2a82",
}
SERIES_COLORS = ["#1f5f9e", "#a973d1", "#c96a1c", "#5b2a82", "#2f8f6b"]
SCENARIO_COLORS = {
    "fixed": "#2f6db5", "per-epoch": "#6c3390",
    "curriculum": "#d97b29", "curriculum-narrow": "#a973d1",
}
WRITTEN: set[str] = set()


def t(zh: str, en: str) -> str:
    return zh if LANG == "zh" else en


def _num(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _cell(row: dict) -> str:
    return str(row.get("cond") or row.get("_cell") or "")


def _rho(row: dict) -> float | None:
    return _num(row.get("rho"))


def _rate(row: dict) -> str:
    return str(row.get("rate") or "")


def family_rows(family: str) -> list[dict]:
    """该族全部有效采样行。status=ok、void=false、无基线顶替。"""
    root = config.family_results_dir(family)
    rows: list[dict] = []
    if not root.is_dir():
        return rows
    for path in sorted(root.rglob("summary.csv")):
        rel = path.relative_to(root).parts
        if any(part.startswith("_") for part in rel):
            continue
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if str(row.get("family") or "") != family:
                    continue
                # status 列缺省按 ok 认：训练负载那支写手不落这一列，
                # 作废轮由 void 列单独拦。
                status = str(row.get("status") or "").strip().lower()
                if status and status != "ok":
                    continue
                if str(row.get("void") or "").strip().lower() in ("true", "1"):
                    continue
                if str(row.get("baseline_substituted") or "").strip():
                    continue
                row["_cell"] = rel[0]
                rows.append(row)
    return rows


def _collect(rows, metric, facet, x, series):
    """(分面, 横轴, 分组) → (均值, CI 下界, CI 上界, 样本数)。"""
    buckets: dict[tuple, list[float]] = {}
    for row in rows:
        value = _num(row.get(metric))
        if value is None:
            continue
        buckets.setdefault((facet(row), x(row), series(row)), []).append(value)
    out = {}
    for key, values in buckets.items():
        mean = sum(values) / len(values)
        if len(values) > 1:
            low, high = stats.bootstrap_ci(values, seed=7)
        else:
            low = high = mean
        out[key] = (mean, low, high, len(values))
    return out


def _dump_csv(fname: str, data: dict) -> None:
    if fname in WRITTEN:
        return
    WRITTEN.add(fname)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{fname}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["facet", "x", "series", "n", "mean", "ci_low", "ci_high"])
        for (facet, x, series), (mean, low, high, n) in sorted(data.items(), key=str):
            writer.writerow([facet, x, series, n,
                             round(mean, 6), round(low, 6), round(high, 6)])


def _label_text(value) -> str:
    """柱顶标注的数值文本：整数不带小数点，位数随量级走。"""
    if value is None:
        return ""
    number = float(value)
    if number == int(number):
        return f"{int(number)}"
    if abs(number) >= 100:
        return f"{number:.0f}"
    if abs(number) >= 10:
        return f"{number:.1f}"
    return f"{number:.2f}"


def _render(data, fname, title, ylabel, xlabel, kind="bar",
            facet_order=None, x_order=None, series_order=None,
            colors=None, color_by="series", facet_fmt=None, x_fmt=None,
            series_fmt=None, show_legend=True, value_labels=False,
            hlines=None, stack=False, notes=None, xpad=None):
    """画一张图并落盘。

    `value_labels` 打开时柱顶标数值；`hlines` 是 {分面: [(y, 文字)]}，
    画参考线并在右侧标注；`stack` 把同一横轴位置的多个系列叠成一根柱。
    """
    if not data:
        print(f"[skip] {fname}: 无数据", file=sys.stderr)
        return
    _dump_csv(fname, data)
    facets = facet_order or sorted({k[0] for k in data}, key=str)
    xs = x_order or sorted({k[1] for k in data}, key=lambda v: (str(type(v)), v))
    series = series_order if series_order is not None else sorted(
        {k[2] for k in data}, key=str)
    ncols = min(3, len(facets))
    nrows = math.ceil(len(facets) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.7 * ncols, 3.2 * nrows),
                             squeeze=False)
    flat = axes.ravel()
    for index, facet in enumerate(facets):
        ax = flat[index]
        local_xs = [x for x in xs
                    if any(k[0] == facet and k[1] == x for k in data)]
        ymax = 0.0
        if kind == "bar":
            width = 0.6 if stack else 0.8 / max(1, len(series))
            bottoms = [0.0] * len(local_xs)
            for si, s in enumerate(series):
                positions, means, lows, highs, bases, idxs, xs_drawn = \
                    [], [], [], [], [], [], []
                for xi, x in enumerate(local_xs):
                    point = data.get((facet, x, s))
                    if point is None:
                        continue
                    positions.append(xi if stack else xi + (si - (len(series) - 1) / 2) * width)
                    means.append(point[0])
                    lows.append(point[0] - point[1])
                    highs.append(point[2] - point[0])
                    bases.append(bottoms[xi])
                    idxs.append(xi)
                    xs_drawn.append(x)
                if not positions:
                    continue
                if color_by == "x":
                    bar_colors = [(colors or {}).get(x, "#4b5563") for x in xs_drawn]
                else:
                    bar_colors = (colors or {}).get(s)
                if stack:
                    ax.bar(positions, means, width, bottom=bases,
                           label=series_fmt(s) if series_fmt else str(s),
                           color=bar_colors, zorder=2)
                else:
                    ax.bar(positions, means, width,
                           label=series_fmt(s) if series_fmt else str(s),
                           color=bar_colors, yerr=[lows, highs], capsize=2,
                           error_kw={"elinewidth": 0.8, "ecolor": plot_style.MUTED},
                           zorder=2)
                for xi, mean, high, base in zip(idxs, means, highs, bases):
                    tops = base + mean
                    top = tops + (high if not stack else 0)
                    ymax = max(ymax, top)
                    if stack:
                        bottoms[xi] = tops
                    if value_labels:
                        if stack:
                            # 堆叠柱的标注写在段内，字色取白以免压在深色上读不出。
                            ax.annotate(_label_text(mean),
                                        xy=(xi, base + mean / 2), ha="center",
                                        va="center", fontsize=8, color="white",
                                        zorder=3)
                        else:
                            ax.annotate(_label_text(tops),
                                        xy=(positions[idxs.index(xi)], top),
                                        xytext=(0, 4), textcoords="offset points",
                                        ha="center", fontsize=8,
                                        color=plot_style.MUTED, zorder=3)
        else:
            for si, s in enumerate(series):
                positions, means, lows, highs = [], [], [], []
                for xi, x in enumerate(local_xs):
                    point = data.get((facet, x, s))
                    if point is None:
                        continue
                    positions.append(xi)
                    means.append(point[0])
                    lows.append(point[0] - point[1])
                    highs.append(point[2] - point[0])
                ax.errorbar(positions, means, yerr=[lows, highs], marker="o",
                            markersize=3.5, linewidth=1.4,
                            color=(colors or {}).get(s),
                            label=series_fmt(s) if series_fmt else str(s),
                            capsize=2, elinewidth=0.8, ecolor=plot_style.MUTED,
                            zorder=2)
                if means:
                    ymax = max(ymax, max(means) + max(highs))
        for spec in (hlines or {}).get(facet, []):
            y, label = spec[0], spec[1]
            band_top = spec[2] if len(spec) > 2 else None
            if band_top is not None and band_top > y:
                ax.axhspan(y, band_top, color="#eef2f7", zorder=0)
            ax.axhline(y, color="#9aa0a6", linestyle=(0, (4, 3)), linewidth=0.9,
                       zorder=1)
            if label:
                ax.annotate(label, xy=(0.02, y), xycoords=("axes fraction", "data"),
                            xytext=(0, 3), textcoords="offset points", ha="left",
                            fontsize=8, color=plot_style.MUTED, zorder=3,
                            bbox=dict(facecolor="white", edgecolor="none",
                                      alpha=0.85, pad=1.0))
        note = (notes or {}).get(facet)
        if note:
            ax.annotate(note, xy=(0.03, 0.96), xycoords="axes fraction",
                        ha="left", va="top", fontsize=8.5, color=plot_style.MUTED,
                        zorder=3)
        ax.set_xticks(range(len(local_xs)))
        labels = [x_fmt(x) if x_fmt else str(x) for x in local_xs]
        ax.set_xticklabels(labels, rotation=25 if max(map(len, labels), default=0) > 6 else 0,
                           ha="right" if max(map(len, labels), default=0) > 6 else "center")
        if xpad is not None and local_xs:
            ax.set_xlim(-xpad, len(local_xs) - 1 + xpad)
        ax.set_title(facet_fmt(facet) if facet_fmt else str(facet))
        ax.grid(axis="y")
        if kind == "bar":
            ax.set_ylim(bottom=0)
            top = ax.get_ylim()[1]
            if ymax > top:
                ax.set_ylim(0, ymax)
            if value_labels or (show_legend and len(series) > 1):
                ax.set_ylim(0, ax.get_ylim()[1] * 1.22)
        else:
            ax.margins(y=0.22)
        if index % ncols == 0:
            ax.set_ylabel(ylabel)
        if index >= len(facets) - ncols:
            ax.set_xlabel(xlabel)
    for index in range(len(facets), len(flat)):
        flat[index].set_visible(False)
    if show_legend and len(series) > 1:
        handles, labels_ = [], []
        for ax in flat[:len(facets)]:
            got_handles, got_labels = ax.get_legend_handles_labels()
            for handle, label in zip(got_handles, got_labels):
                if label not in labels_:
                    handles.append(handle)
                    labels_.append(label)
        if handles:
            fig.legend(handles, labels_, loc="upper left",
                       bbox_to_anchor=(0.01, 0.93), ncol=min(len(labels_), 4),
                       frameon=False, borderaxespad=0.0)
    fig.suptitle(title, fontsize=12, x=0.01, ha="left", y=0.995)
    fig.tight_layout(rect=(0, 0, 1, 0.86 if nrows == 1 else 0.9))
    plot_style.bolden(fig)
    FIG_ROOT.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG_ROOT / f"{fname}.{LANG}.png", dpi=plot_style.SAVE_DPI,
                bbox_inches="tight", pad_inches=0.08)
    plt.close(fig)


def _gate(row):
    return str(row.get("gate_mode") or "")


def _policy(row):
    return str(row.get("evict_policy") or "")


def _window(row):
    match = re.search(r"-w(\d+)-rho", _cell(row))
    return int(match.group(1)) if match else None


def _arm(row):
    match = re.match(r"gateway-(g\d+)-", _cell(row))
    return match.group(1) if match else "?"


def _scenario(row):
    match = re.match(r"switch-(.+)-rho", _cell(row))
    return match.group(1) if match else "?"


def _hot(row):
    match = re.match(r"multi-hot(\d+)-", _cell(row))
    return f"{int(match.group(1))}%" if match else "?"


def _placement(row):
    cell = _cell(row)
    if cell.endswith("colocated"):
        return "colocated"
    if cell.endswith("spread"):
        return "spread"
    return "?"


def _rangepar_parts(row):
    """COND 名 rangepar-<档>-<臂> → (档, 臂)。"""
    match = re.match(r"^rangepar-(l\d+)-(par-on|par-off)$", _cell(row))
    if not match:
        return "?", "?"
    return match.group(1), match.group(2)


def _e2e_scenario(row):
    """COND 名 e2e-<场景> → 场景名。"""
    match = re.match(r"^e2e-(.+)$", _cell(row))
    return match.group(1) if match else "?"


def _chaos_rho(cond_name):
    """COND 名 chaos-<故障>-rho<ρ> → ρ 标签。"""
    match = re.search(r"-rho(\d+)$", cond_name)
    return f"ρ={match.group(1)}" if match else "?"


RANGEPAR_TIER_CN = {
    "l0": "l0 单流不限",
    "l1": "l1 单流 32 Mbit",
    "l2": "l2 单流 16 Mbit",
    "l3": "l3 高时延丢包",
}
RANGEPAR_TIER_EN = {
    "l0": "l0 no per-conn cap",
    "l1": "l1 32 Mbit per conn",
    "l2": "l2 16 Mbit per conn",
    "l3": "l3 high delay and loss",
}
CHAOS_FAULT_CN = {
    "kill_agent": "agentd 被杀",
    "minio_down": "源站下线",
    "tamper_block": "块被改坏",
    "netem_degrade": "源站降速",
    "netem_degrade_rate": "源站降速加限速",
    "stop_ordersrc": "订单写端停摆",
    "node_loss": "节点掉线",
}
E2E_SCENARIO_CN = {
    "checkpoint-resume": "断点续跑",
    "checkpoint-version": "换数据版本后恢复",
    "dual-restart": "训练与 agentd 同时重启",
}


def _rate_key(row):
    return _rate(row)


def _rho_key(row):
    value = _rho(row)
    return f"{value:g}" if value is not None else "?"


def _rho_sorted(values):
    return sorted((v for v in values if v != "?"), key=float)


def _rate_fmt(value):
    mbit = config.rate_mbit(value)
    return f"{mbit} mbit" if mbit else str(value)


def _rho_fmt(value):
    return f"ρ={value}" if LANG == "zh" else f"rho={value}"


def _rho_mark(value):
    return f"ρ={value}" if LANG == "zh" else f"rho={value}"


def _rangepar_arm_label(value):
    """臂名：par-on 是 Range 并行打开，par-off 是关掉。"""
    if LANG == "zh":
        return {"par-on": "并行开", "par-off": "并行关"}.get(value, str(value))
    return {"par-on": "parallel on", "par-off": "parallel off"}.get(value, str(value))


def plot_compare(rows):
    at4 = [r for r in rows if _rho(r) is not None and abs(_rho(r) - 4.0) < 1e-9]
    data = _collect(at4, "total_makespan", _rate_key,
                    lambda r: str(r.get("group") or ""), lambda r: "all")
    facets = sorted({k[0] for k in data}, key=config.rate_mbit)
    xs = [g for g in GROUP_ORDER if any(k[1] == g for k in data)]
    _render(data, "compare-groups-rho4",
            t("对照族：ρ=4 各带宽下的全程耗时", "compare: total makespan at rho=4"),
            t("全程 makespan (s)", "total makespan (s)"),
            t("组态", "group"), facet_order=facets, x_order=xs,
            series_order=["all"], colors=GROUP_COLORS, color_by="x",
            facet_fmt=_rate_fmt, show_legend=False)
    tc = [r for r in rows if str(r.get("group") or "").startswith("traincache")]
    data = _collect(tc, "total_makespan", _rate_key, _rho_key,
                    lambda r: str(r.get("group") or ""))
    facets = sorted({k[0] for k in data}, key=config.rate_mbit)
    rhos = _rho_sorted({k[1] for k in data})
    _render(data, "compare-rho",
            t("对照族：全程耗时随 ρ", "compare: total makespan against rho"),
            t("全程 makespan (s)", "total makespan (s)"),
            t("ρ", "rho"), kind="line", facet_order=facets, x_order=rhos,
            series_order=["traincache-cold", "traincache-warm"],
            colors=GROUP_COLORS, facet_fmt=_rate_fmt, x_fmt=lambda v: v)


def _gateway_source_bytes(row):
    """判据用的源站字节：G2 取网关记的源站字节（数据面经网关从源站拉到的
    字节），G0 与 G1 取 MinIO 出站。两边都只算 Get 路径的流量，
    Agent 元数据路径直连源站的部分不在口径内。"""
    if _arm(row) == "g2":
        return _num(row.get("gateway_bytes"))
    return _num(row.get("origin_egress_bytes"))


def plot_gateway(rows):
    xs = sorted({_arm(r) for r in rows})
    rows = [dict(r, source_bytes=_gateway_source_bytes(r)) for r in rows]
    for metric, fname, ylabel in (
        ("total_makespan", "gateway-makespan", t("全程 makespan (s)", "total makespan (s)")),
        ("source_bytes", "gateway-source-bytes", t("源站字节 (B)", "source bytes (B)")),
    ):
        data = _collect(rows, metric, lambda r: "", _arm, lambda r: "all")
        _render(data, fname,
                t("网关族：三个臂的耗时与源站字节", "gateway: makespan and source bytes by arm"),
                ylabel, t("网关臂", "gateway arm"), x_order=xs,
                series_order=["all"], colors={g: "#1f5f9e" for g in xs},
                color_by="x", show_legend=False)


def plot_evict(rows):
    xs = ["heat", "lru", "lfu"]
    present = {_policy(r) for r in rows}
    xs = [x for x in xs if x in present]
    rhos = _rho_sorted({_rho_key(r) for r in rows})
    colors = {r: c for r, c in zip(rhos, SERIES_COLORS)}
    for metric, fname, ylabel in (
        ("total_makespan", "evict-makespan", t("全程 makespan (s)", "total makespan (s)")),
        ("b_evictions_of_hot", "evict-hot-evictions",
         t("热块被挤出次数", "hot-block evictions")),
    ):
        data = _collect(rows, metric, lambda r: "", _policy, _rho_key)
        if not data:
            print(f"[skip] {fname}: 无数据", file=sys.stderr)
            continue
        _render(data, fname,
                t("淘汰族：三种算法在 ρ 两端的表现",
                  "evict: three policies at both ends of rho"),
                ylabel, t("淘汰算法", "eviction policy"), x_order=xs,
                series_order=rhos, colors=colors, series_fmt=_rho_mark)


def plot_prefetch(rows):
    xs = sorted({int(_num(r.get("prefetch_lookahead")) or 0) for r in rows})
    rhos = _rho_sorted({_rho_key(r) for r in rows})
    fans = sorted({int(_num(r.get("prefetch_fanout")) or 0) for r in rows})
    data = _collect(rows, "total_makespan", _rho_key,
                    lambda r: int(_num(r.get("prefetch_lookahead")) or 0),
                    lambda r: int(_num(r.get("prefetch_fanout")) or 0))
    _render(data, "prefetch-lookahead",
            t("预取族：窗口与 fanout 对全程耗时的影响",
              "prefetch: makespan against lookahead and fanout"),
            t("全程 makespan (s)", "total makespan (s)"),
            t("lookahead（块）", "lookahead (chunks)"), kind="line",
            facet_order=rhos, x_order=xs, series_order=fans,
            colors={f: c for f, c in zip(fans, SERIES_COLORS)},
            facet_fmt=_rho_fmt, series_fmt=lambda v: f"fanout={v}")


def plot_warmup(rows):
    xs = sorted({_window(r) for r in rows if _window(r) is not None})
    rhos = _rho_sorted({_rho_key(r) for r in rows})
    gates = ["gated", "free"]
    gates = [g for g in gates if any(_gate(r) == g for r in rows)]
    data = _collect(rows, "startup_makespan", _rho_key, _window, _gate)
    _render(data, "warmup-startup",
            t("预热族：门控与窗口对启动耗时的影响",
              "warmup: startup makespan by gate and window"),
            t("启动 makespan (s)", "startup makespan (s)"),
            t("窗口（s）", "window (s)"), facet_order=rhos, x_order=xs,
            series_order=gates,
            colors=dict(zip(gates, SERIES_COLORS)), facet_fmt=_rho_fmt,
            x_fmt=lambda v: str(v))


def plot_switch(rows):
    xs = ["fixed", "per-epoch", "curriculum", "curriculum-narrow"]
    present = {_scenario(r) for r in rows}
    xs = [x for x in xs if x in present]
    rhos = _rho_sorted({_rho_key(r) for r in rows})
    data = _collect(rows, "recovery_time_s", lambda r: "", _scenario, _rho_key)
    _render(data, "switch-recovery",
            t("阶段切换族：切换后的回落时间",
              "switch: recovery time after a phase change"),
            t("回落时间 (s)", "recovery time (s)"),
            t("场景", "scenario"), x_order=xs, series_order=rhos,
            colors=dict(zip(rhos, SERIES_COLORS)), series_fmt=_rho_mark)


def plot_multiconsumer(rows):
    xs = ["10%", "50%"]
    present = {_hot(r) for r in rows}
    xs = [x for x in xs if x in present]
    placements = ["spread", "colocated"]
    placements = [p for p in placements if any(_placement(r) == p for r in rows)]
    for metric, fname, ylabel in (
        ("hit_rate", "multiconsumer-hit", t("命中率", "hit rate")),
        ("total_makespan", "multiconsumer-makespan",
         t("全程 makespan (s)", "total makespan (s)")),
    ):
        data = _collect(rows, metric, lambda r: "", _hot, _placement)
        _render(data, fname,
                t("多消费者族：热比例与读端放置",
                  "multiconsumer: hot ratio and reader placement"),
                ylabel, t("热比例", "hot ratio"), x_order=xs,
                series_order=placements,
                colors=dict(zip(placements, SERIES_COLORS)))


def plot_rangepar(rows):
    """并行下载族：同档两臂的耗时，以及关臂与开臂的耗时比。"""
    buckets: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        tier, arm = _rangepar_parts(row)
        value = _num(row.get("total_makespan"))
        if tier == "?" or value is None:
            continue
        buckets.setdefault((tier, arm), []).append(value)
    tiers = sorted({tier for tier, _arm in buckets})
    if not tiers:
        print("[skip] rangepar: 无可比读数", file=sys.stderr)
        return
    arms = ["par-off", "par-on"]

    data = {}
    for tier in tiers:
        for arm in arms:
            values = buckets.get((tier, arm))
            if not values:
                continue
            mean = sum(values) / len(values)
            if len(values) > 1:
                low, high = stats.bootstrap_ci(values, seed=7)
            else:
                low = high = mean
            data[("", tier, arm)] = (mean, low, high, len(values))
    _render(data, "rangepar-makespan",
            t("并行下载族：同一链路档下两臂的全程耗时",
              "rangepar: total makespan of both arms per link tier"),
            t("全程 makespan (s)", "total makespan (s)"),
            t("链路条件档", "link tier"), facet_order=[""], x_order=tiers,
            series_order=arms, colors={"par-off": "#9aa0a6", "par-on": "#2f6db5"},
            series_fmt=_rangepar_arm_label,
            x_fmt=lambda v: (RANGEPAR_TIER_CN if LANG == "zh" else RANGEPAR_TIER_EN).get(v, v),
            value_labels=True)

    # 比值图：每个档一格，柱高是关臂耗时除以开臂耗时，虚线是该档的合格区间。
    ratio = {}
    bounds = {}
    notes = {}
    for tier in tiers:
        on, off = buckets.get((tier, "par-on")), buckets.get((tier, "par-off"))
        if not on or not off:
            continue
        on_median, off_median = stats.median(on), stats.median(off)
        if not on_median:
            continue
        value = off_median / on_median
        ratio[(tier, "比值", "all")] = (value, value, value, min(len(on), len(off)))
        low, high = config.RANGEPAR_RATIO.get(tier, (None, None))
        marks = []
        if low is not None and high is not None:
            marks.append((low, None, high))
            marks.append((high, None, None))
            notes[tier] = t(f"合格区间 {low}–{high}", f"qualified {low}–{high}")
        elif low is not None:
            marks.append((low, None, None))
            notes[tier] = t(f"合格下限 {low}", f"lower bound {low}")
        bounds[tier] = marks
    if ratio:
        _render(ratio, "rangepar-speedup",
                t("并行下载族：关臂耗时除以开臂耗时",
                  "rangepar: off-arm makespan over on-arm makespan"),
                t("耗时比", "makespan ratio"), "",
                facet_order=[tier for tier in tiers if tier in ratio],
                x_order=["比值"], series_order=["all"],
                colors={"all": "#2f6db5"}, facet_fmt=lambda v: (
                    RANGEPAR_TIER_CN if LANG == "zh" else RANGEPAR_TIER_EN).get(v, v),
                x_fmt=lambda v: "", hlines=bounds, notes=notes, xpad=0.9,
                value_labels=True)


def plot_e2e(rows):
    """端到端族：三个恢复场景的全程耗时与读错误数。"""
    scenarios = []
    for row in rows:
        scenario = _e2e_scenario(row)
        if scenario != "?" and scenario not in scenarios:
            scenarios.append(scenario)
    if not scenarios:
        print("[skip] e2e: 无产物", file=sys.stderr)
        return
    data = _collect(rows, "total_makespan", lambda r: "", _e2e_scenario,
                    lambda r: "all")
    _render(data, "e2e-makespan",
            t("端到端族：三个恢复场景的全程耗时",
              "e2e: total makespan per recovery scenario"),
            t("全程 makespan (s)", "total makespan (s)"),
            t("恢复场景", "recovery scenario"), facet_order=[""], x_order=scenarios,
            series_order=["all"], colors={"all": "#2f6db5"}, show_legend=False,
            x_fmt=lambda v: (E2E_SCENARIO_CN.get(v, v) if LANG == "zh" else v),
            value_labels=True)
    data = _collect(rows, "e2e_recovery_time_s", lambda r: "", _e2e_scenario,
                    lambda r: "all")
    if data:
        _render(data, "e2e-recovery",
                t("端到端族：各场景从中断到恢复出步的时长",
                  "e2e: time from interruption to first step after resume"),
                t("恢复时长 (s)", "recovery time (s)"),
                t("恢复场景", "recovery scenario"), facet_order=[""],
                x_order=scenarios, series_order=["all"],
                colors={"all": "#8a5a2b"}, show_legend=False,
                x_fmt=lambda v: (E2E_SCENARIO_CN.get(v, v) if LANG == "zh" else v),
                value_labels=True)
    data = _collect(rows, "e2e_fetch_errors_recovery", lambda r: "", _e2e_scenario,
                    lambda r: "all")
    if data:
        _render(data, "e2e-fetch-errors",
                t("端到端族：各场景恢复窗口内的回源请求错误数",
                  "e2e: origin fetch errors in the recovery window"),
                t("错误数", "fetch errors"),
                t("恢复场景", "recovery scenario"), facet_order=[""],
                x_order=scenarios, series_order=["all"],
                colors={"all": "#c0392b"}, show_legend=False,
                x_fmt=lambda v: (E2E_SCENARIO_CN.get(v, v) if LANG == "zh" else v),
                value_labels=True)


def chaos_rows(family: str = "chaos") -> list[dict]:
    """混沌族的逐轮结果：results/chaos/<格子>/<轮次>/summary.txt 里的 PASS/FAIL。"""
    root = config.family_results_dir(family)
    rows: list[dict] = []
    if not root.is_dir():
        return rows
    for path in sorted(root.glob("*/*/summary.txt")):
        cond = path.parent.parent.name
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].startswith(("PASS", "FAIL")):
                rows.append({"cond": cond, "_cell": cond, "run": path.parent.name,
                             "fault": parts[0],
                             "status": "PASS" if parts[1].startswith("PASS") else "FAIL"})
    return rows


def plot_chaos(rows):
    """混沌族：每个故障在各轮里的 PASS/FAIL 计数。"""
    counts: dict[tuple[str, str, str], int] = {}
    for row in rows:
        key = (_chaos_rho(row["cond"]), row["fault"], row["status"])
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        print("[skip] chaos: 无产物", file=sys.stderr)
        return
    data = {key: (count, count, count, count) for key, count in counts.items()}
    facets = sorted({key[0] for key in data})
    faults = sorted({key[1] for key in data})
    _render(data, "chaos-results",
            t("混沌族：各故障在各轮里的判定结果",
              "chaos: verdict per fault at each rho"),
            t("轮次数", "rounds"), t("故障", "fault"),
            facet_order=facets, x_order=faults, series_order=["PASS", "FAIL"],
            colors={"PASS": "#2f8f6b", "FAIL": "#c0392b"}, stack=True,
            x_fmt=lambda v: (CHAOS_FAULT_CN.get(v, v) if LANG == "zh" else v),
            value_labels=True)


FAMILY_PLOTS = {
    "compare": plot_compare,
    "gateway": plot_gateway,
    "evict": plot_evict,
    "prefetch": plot_prefetch,
    "warmup": plot_warmup,
    "switch": plot_switch,
    "multiconsumer": plot_multiconsumer,
    "chaos": plot_chaos,
    "e2e": plot_e2e,
    "rangepar": plot_rangepar,
}

# 混沌族的产物是 summary.txt,不是 summary.csv,单独取行。
FAMILY_ROWS = {"chaos": chaos_rows}


def generate(langs=LANGS) -> dict[str, list[dict]]:
    global plt, LANG
    rows_by_family = {family: FAMILY_ROWS.get(family, family_rows)(family)
                      for family in FAMILY_PLOTS}
    for lang in langs:
        plt = plot_style.setup(lang)
        LANG = lang
        for family, plot in FAMILY_PLOTS.items():
            rows = rows_by_family[family]
            if not rows:
                print(f"[skip] {family}: 无产物", file=sys.stderr)
                continue
            plot(rows)
    return rows_by_family


def main() -> int:
    ap = argparse.ArgumentParser(description="按族出图")
    ap.add_argument("--lang", choices=("both", "zh", "en"), default="both")
    args = ap.parse_args()
    langs = LANGS if args.lang == "both" else (args.lang,)
    generate(langs)
    print(f"figures: {FIG_ROOT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
