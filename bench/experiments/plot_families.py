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
                if str(row.get("status") or "").strip().lower() != "ok":
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


def _render(data, fname, title, ylabel, xlabel, kind="bar",
            facet_order=None, x_order=None, series_order=None,
            colors=None, color_by="series", facet_fmt=None, x_fmt=None,
            series_fmt=None, show_legend=True):
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
        if kind == "bar":
            width = 0.8 / max(1, len(series))
            for si, s in enumerate(series):
                positions, means, lows, highs, bars = [], [], [], [], []
                for xi, x in enumerate(local_xs):
                    point = data.get((facet, x, s))
                    if point is None:
                        continue
                    positions.append(xi + (si - (len(series) - 1) / 2) * width)
                    means.append(point[0])
                    lows.append(point[0] - point[1])
                    highs.append(point[2] - point[0])
                    bars.append(x)
                if color_by == "x":
                    bar_colors = [(colors or {}).get(x, "#4b5563") for x in bars]
                else:
                    bar_colors = (colors or {}).get(s)
                ax.bar(positions, means, width, label=series_fmt(s) if series_fmt else str(s),
                       color=bar_colors, yerr=[lows, highs], capsize=2,
                       error_kw={"elinewidth": 0.8, "ecolor": plot_style.MUTED})
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
                            capsize=2, elinewidth=0.8, ecolor=plot_style.MUTED)
        ax.set_xticks(range(len(local_xs)))
        labels = [x_fmt(x) if x_fmt else str(x) for x in local_xs]
        ax.set_xticklabels(labels, rotation=25 if max(map(len, labels), default=0) > 6 else 0,
                           ha="right" if max(map(len, labels), default=0) > 6 else "center")
        ax.set_title(facet_fmt(facet) if facet_fmt else str(facet))
        ax.grid(axis="y")
        if index % ncols == 0:
            ax.set_ylabel(ylabel)
        if index >= len(facets) - ncols:
            ax.set_xlabel(xlabel)
        if show_legend and len(series) > 1:
            ax.legend(frameon=False, loc="best")
    for index in range(len(facets), len(flat)):
        flat[index].set_visible(False)
    fig.suptitle(title, fontsize=12)
    fig.tight_layout()
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


def plot_gateway(rows):
    xs = sorted({_arm(r) for r in rows})
    for metric, fname, ylabel in (
        ("total_makespan", "gateway-makespan", t("全程 makespan (s)", "total makespan (s)")),
        ("fetch_bytes_run", "gateway-source-bytes", t("回源字节 (B)", "source bytes (B)")),
    ):
        data = _collect(rows, metric, lambda r: "", _arm, lambda r: "all")
        _render(data, fname,
                t("网关族：四个臂的耗时与回源量", "gateway: makespan and source bytes by arm"),
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


FAMILY_PLOTS = {
    "compare": plot_compare,
    "gateway": plot_gateway,
    "evict": plot_evict,
    "prefetch": plot_prefetch,
    "warmup": plot_warmup,
    "switch": plot_switch,
    "multiconsumer": plot_multiconsumer,
}


def generate(langs=LANGS) -> dict[str, list[dict]]:
    global plt, LANG
    rows_by_family = {family: family_rows(family) for family in FAMILY_PLOTS}
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
