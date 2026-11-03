#!/usr/bin/env python3
"""实验出图脚本。

读取 bench/results/compare/summary.csv 与 bench/results/experiments/ 下的
metrics.csv,产出预注册(docs/4.4 §2~§5)定义的六张图:

  fig1  启动阶段盈亏(带宽 × 缓存比 × fanout,自由/门控逐对)
  fig2  全程 makespan 绝对值(带宽 × epoch,8 GiB 数据集,附计算与传输下界)
  fig3  阶段切换 stall 回落时间(epoch × shuffle,点条 + 均值)
  pareto1  启动收益 vs 重复预热字节(fanout 1/2/3)
  pareto2  全程收益 vs 持续预取字节(lookahead 8/16/32 × 淘汰策略 heat,
           纵轴相对自身基线 lookahead=8+heat)

七组态对照(summary.csv)额外产出 3 张图,按格子(S-100…L-1000)分面:
  startup_makespan / total_makespan / stall_ratio

命中率不单独出图:direct 按定义恒为 0,juicefs/fluid 的冷热两态按定义恒为
0/1(D25 近似),与 traincache 的字节账口径不可比,画出来只是一排端点。

每张图对应的聚合值同时写到 figures/data/<图名>.csv。

用法:
  python3 bench/experiments/plot.py
      # 零参:自动扫描 RESULTS_DIR 下所有含 summary.csv 的目录
      # (run.py 把每个 COND 写到 results/<COND>/,这里能直接读到)
  python3 bench/experiments/plot.py --summary <数据层>/bench/results/compare/summary.csv \\
      --metrics-dir <目录> --out-dir <数据层>/bench/results/figures

默认输出目录按 bench/config.py 的 DATA_ROOT 解析到运行时数据层,与当前工作目录无关。
"""
import argparse
import csv
import json
import re
import sys
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import FormatStrFormatter, PercentFormatter

# 统一版式:中文字体走 Noto Sans CJK SC,取消上右边框,网格压在图形之下。
# 这些是 rcParams 的全局默认值,各图函数不再逐个设置字号与边框。
INK = "#1a1a1a"
MUTED = "#6b7280"
GRID = "#ececec"
SAVE_DPI = 200
# 标题与轴标签另起真实 Bold 字面:Noto Sans CJK SC Medium 家族里没有 bold,
# 在该家族上设 weight=bold 会被回退成更细的 Regular。
BOLD_FAMILY = "Noto Sans CJK SC"

plt.rcParams.update({
    # Medium 字重:Regular 在 200 dpi 下显细,观感像课件
    "font.sans-serif": ["Noto Sans CJK SC Medium", "Noto Sans CJK SC",
                        "Noto Sans CJK JP", "DejaVu Sans"],
    "axes.unicode_minus": False,
    "font.size": 10.5,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "xtick.labelsize": 9.5,
    "ytick.labelsize": 9.5,
    "legend.fontsize": 9.5,
    "axes.edgecolor": "#c7c9cc",
    "axes.linewidth": 0.7,
    "axes.labelcolor": "#374151",
    # 标题左对齐、不跳字号,读起来是报告而非课件
    "axes.titlelocation": "left",
    "axes.titleweight": "normal",
    "axes.titlepad": 7,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.axisbelow": True,
    "xtick.color": "#9aa0a6",
    "ytick.color": "#9aa0a6",
    "xtick.labelcolor": "#4b5563",
    "ytick.labelcolor": "#4b5563",
    "xtick.major.size": 3,
    "ytick.major.size": 3,
    "xtick.major.width": 0.7,
    "ytick.major.width": 0.7,
    "grid.color": GRID,
    "grid.linewidth": 0.6,
    "figure.facecolor": "white",
    "savefig.facecolor": "white",
})


def _bolden(fig):
    """标题、轴标签、图例改用真实 Bold 字面,避免 200 dpi 下笔画发虚。"""
    for ax in fig.axes:
        for t in (ax.title, ax._left_title, ax._right_title,
                  ax.xaxis.label, ax.yaxis.label):
            t.set_fontfamily(BOLD_FAMILY)
            t.set_fontweight("bold")
        leg = ax.get_legend()
        if leg is not None:
            for t in leg.get_texts():
                t.set_fontfamily(BOLD_FAMILY)
                t.set_fontweight("bold")
    for leg in fig.legends:
        for t in leg.get_texts():
            t.set_fontfamily(BOLD_FAMILY)
            t.set_fontweight("bold")
    if fig._suptitle is not None:
        fig._suptitle.set_fontfamily(BOLD_FAMILY)
        fig._suptitle.set_fontweight("bold")


def _save(fig, path: Path):
    """统一落盘:白底、200 dpi、裁掉空白边。"""
    _bolden(fig)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=SAVE_DPI, bbox_inches="tight", pad_inches=0.08)
    plt.close(fig)
    print(f"  saved {path.name}")


def _write_csv(path: Path, header: list[str], rows: list[list]):
    """把图上用到的聚合值写到 figures/data/,图与数可逐格核对。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"  saved data/{path.name}")


# 对照装置分组固定顺序与配色(与 bench/compare/run_bench.py 的 GROUPS 同口径:
# 五个对照组拆成 cold/warm 两态,共 7 个具体组名)
GROUP_ORDER = ["direct", "juicefs-cold", "juicefs-warm",
               "fluid-cold", "fluid-warm",
               "traincache-cold", "traincache-warm"]
GROUP_LABELS = {
    "direct": "Direct",
    "juicefs-cold": "JuiceFS 冷",
    "juicefs-warm": "JuiceFS 热",
    "fluid-cold": "Fluid 冷",
    "fluid-warm": "Fluid 热",
    "traincache-cold": "TrainCache 冷",
    "traincache-warm": "TrainCache 热",
}
# 对照格子固定顺序。每格带宽与容量不同,七组态只在本格内可比,跨格聚合会把
# 100 mbit 与 1000 mbit 的耗时混进同一根柱子(summarize.py 也按格子分表)。
CELL_ORDER = ["S-100", "S-300", "S-1000", "M-1000", "L-1000"]
# 冷/热两态用同一色相的两档明度:冷=浅,热=深。三个受试者各占一个色相,
# direct 保持中性灰,图里不用再看图例辨认组别。
GROUP_COLORS = {
    "direct": "#6f6f6f",
    "juicefs-cold": "#6fa9e0",
    "juicefs-warm": "#1f5f9e",
    "fluid-cold": "#f0a85a",
    "fluid-warm": "#c96a1c",
    "traincache-cold": "#a973d1",
    "traincache-warm": "#5b2a82",
}


def _data_root() -> Path:
    """产物根目录:与其余 bench 脚本同口径,由 bench/config.py 的 DATA_ROOT 定义。"""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import config  # noqa: PLC0415

    return config.DATA_ROOT


DATA_ROOT = _data_root()
RESULTS_DIR = DATA_ROOT / "bench" / "results"

# 读侧三道过滤(4.4 §1.7、§7.4):缺陷期的行既进不了图也不该进表
sys.path.insert(0, str(Path(__file__).resolve().parent))
from row_gate import (current_version, print_rejects,  # noqa: E402
                      read_gated, write_reject_log)

# 只有这三个一级目录是七组对照的样本:scores 本轮 TrainCache 行、baselines 既有
# 对照组行、compare 是旧的单文件产物。其余一级目录(figures/policy/quick/
# track_d/summary)装的是曲面点、倾斜双 Job 与自检产物,不是对照样本。
COMPARE_PARTITIONS = ("scores", "baselines", "compare")


# ---------------------------------------------------------------------------
# 数据读取
# ---------------------------------------------------------------------------
def read_summary(path: Path) -> list[dict]:
    """读 run_bench.py 产出的 summary.csv。"""
    rows = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def _num(value, default=0.0):
    """把 '100mbit' / '1.5' / '' 统一成数字；取不到给 default。"""
    if value is None or value == "":
        return default
    m = re.search(r"-?\d+(?:\.\d+)?", str(value))
    return float(m.group()) if m else default


def adapt_summary_rows(rows: list[dict]) -> list[dict]:
    """把 run_bench.py 的 summary.csv 行映射成六张图认的字段名。

    run_bench 写的是 rate/cache_ratio/startup_makespan/total_makespan/
    prefetch_fanout/dataset_bytes；图函数按 rate_mbit/cache_ratio/
    startup_makespan_s/full_makespan_s/fanout_actual/dataset_gb 取数。
    组态名统一成 traincache/fluid/juicefs/direct 四类，冷热信息留在 raw_group。
    """
    out = []
    for r in rows:
        if str(r.get("void", "")).lower() == "true":
            continue
        raw = (r.get("group") or "").strip()
        if raw.startswith("traincache"):
            group = "traincache"
        elif raw.startswith("fluid"):
            group = "fluid"
        elif raw.startswith("juicefs"):
            group = "juicefs"
        else:
            group = raw
        dataset_bytes = _num(r.get("dataset_bytes"))
        row = dict(r)
        row.update({
            "raw_group": raw,
            "group": group,
            "rate_mbit": int(_num(r.get("rate"))),
            "fanout_actual": int(_num(r.get("prefetch_fanout") or
                                     r.get("warmup_fanout"))),
            "cache_ratio": _num(r.get("cache_ratio")),
            "startup_makespan_s": _num(r.get("startup_makespan")),
            "full_makespan_s": _num(r.get("total_makespan")),
            "dataset_gb": dataset_bytes / (1 << 30) if dataset_bytes else 0.0,
            "epochs": int(_num(r.get("epochs"))),
            "shuffle": r.get("shuffle") or "",
            "gate_mode": r.get("gate_mode") or "gated",
            "evict_policy": r.get("evict_policy") or "heat",
            "lookahead": int(_num(r.get("prefetch_lookahead"))),
        })
        out.append(row)
    return out


def read_metrics_dir(root: Path) -> list[dict]:
    """递归读取 experiments 产物目录下所有 metrics.csv。"""
    rows = []
    if not root.is_dir():
        return rows
    for p in sorted(root.rglob("metrics.csv")):
        with p.open(newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                r["_source"] = str(p)
                rows.append(r)
    return rows


def read_recovery_runs(root: Path) -> list[dict]:
    """扫描 run_bench 的 runs/*/recovery.json,把每次切换事件摊成一行。

    fig3(switch-recovery)的样本单位是「一次阶段切换」:同一 run 在 epoch 1..N 各切一次,
    每次都有自己的回落时间。run.json 提供 cond/epochs/shuffle,recovery.json
    提供每个事件的 status 与 recovery_time_s。
    """
    rows = []
    if not root.is_dir():
        return rows
    for run_json in sorted(root.rglob("runs/*/run.json")):
        run_dir = run_json.parent
        rec_path = run_dir / "recovery.json"
        if not rec_path.is_file():
            continue
        try:
            meta = json.loads(run_json.read_text())
            rec = json.loads(rec_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        args = meta.get("args") or {}
        for ev in rec.get("events") or []:
            rows.append({
                "cond": meta.get("cond") or "",
                "run_id": meta.get("run_id") or run_dir.name,
                "epochs": args.get("epochs") or 0,
                "shuffle": args.get("shuffle") or "",
                "to_epoch": ev.get("to_epoch"),
                "status": ev.get("status"),
                "recovery_time_s": ev.get("recovery_time_s"),
                "_source": f"{rec_path}#{ev.get('to_epoch')}",
            })
    return rows


def discover_result_dirs(root: Path) -> list[Path]:
    """返回 root 下所有直接含 summary.csv 的目录(按路径排序)。

    run.py 的产物落 results/<COND>/summary.csv,run_bench.py 的产物落
    results/compare/summary.csv,quick 产物落 results/quick/<run_id>/summary.csv;
    三者都在这里被扫到。
    """
    if not root.is_dir():
        return []
    dirs = {p.parent for p in root.rglob("summary.csv")}
    # 补跑行在 summary.all.csv(merge_compare.py 生成);有合并表时只读它
    dirs |= {p.parent for p in root.rglob("summary.all.csv")}
    return sorted(dirs)


def _summary_path(directory: Path) -> Path:
    """同目录优先 summary.all.csv,否则 summary.csv。"""
    merged = directory / "summary.all.csv"
    return merged if merged.is_file() else directory / "summary.csv"


def mean_ci(vals: list[float], n_boot: int = 10000, seed: int = 42):
    """返回 (点估计, 2.5% 分位, 97.5% 分位);空返回 (0,0,0)。

    点估计用均值;区间用 bootstrap 重采样。固定 seed 让同一份数据每次出图一致。
    """
    if not vals:
        return 0.0, 0.0, 0.0
    arr = np.asarray(vals, dtype=float)
    m = float(np.mean(arr))
    if arr.size < 2:
        return m, m, m
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, arr.size, size=(n_boot, arr.size))
    boot = arr[idx].mean(axis=1)
    lo, hi = np.percentile(boot, [2.5, 97.5])
    return m, float(lo), float(hi)


def cell_summary(rows: list[dict], cell: str, metric: str) -> dict:
    """按 (格子, 组态) 聚合某指标 → {group: (mean, lo, hi, n)}。

    格子是 S-100 这类容量/带宽组合。七组态只在同格内可比:跨格聚合会把
    100 mbit 的 400 s 与 1000 mbit 的 100 s 混进同一根柱子。
    """
    out = {}
    for g in GROUP_ORDER:
        vals = []
        for r in rows:
            if r.get("_cell") != cell:
                continue
            # summary.csv 的组名是 traincache-cold 这类全名;adapt 之后 group 被
            # 归一成 traincache/fluid/juicefs,原名留在 raw_group。
            if (r.get("raw_group") or r.get("group")) != g:
                continue
            if str(r.get("void", "")).lower() == "true":
                continue
            try:
                vals.append(float(r.get(metric)))
            except (TypeError, ValueError):
                continue
        out[g] = (*mean_ci(vals), len(vals))
    return out


def _cells(rows: list[dict]) -> list[str]:
    """出现过的格子,按 CELL_ORDER 在前、其余按字典序追加。"""
    seen = {r.get("_cell") for r in rows if r.get("_cell")}
    ordered = [c for c in CELL_ORDER if c in seen]
    return ordered + sorted(seen - set(ordered))


def rate_ci(base: list[float], other: list[float], n_boot: int = 10000,
            seed: int = 42):
    """100 × (base - other) / base 的点估计与 bootstrap 95% 区间。

    两组样本各自重采样,用于 Pareto 图纵轴的相对改善;两样本都不足 1 条时
    返回 nan。
    """
    if not base or not other:
        return float("nan"), float("nan"), float("nan")
    b = np.asarray(base, dtype=float)
    o = np.asarray(other, dtype=float)
    point = 100.0 * (b.mean() - o.mean()) / b.mean()
    rng = np.random.default_rng(seed)
    bi = rng.integers(0, b.size, size=(n_boot, b.size))
    oi = rng.integers(0, o.size, size=(n_boot, o.size))
    bm = b[bi].mean(axis=1)
    boots = 100.0 * (bm - o[oi].mean(axis=1)) / bm
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return float(point), float(lo), float(hi)


def _pct(v: float) -> str:
    """0.0015 → 0.15%,0.529 → 52.9%。小值多留一位小数,避免一律显示成 0.0%。"""
    p = v * 100.0
    return f"{p:.2f}%" if p < 0.1 else f"{p:.1f}%"


def _facet_axes(n: int, ncols: int = 3, panel_w: float = 3.05,
                panel_h: float = 2.85):
    """n 个格子的小多图;最后一行不足 ncols 面时按面数拉宽,撑满整幅宽度。

    直接开 nrows×ncols 网格会在右下角留出空面;居中摆放又会在末行左右两侧
    留白。五格时末行两面各占 3 列(上排每面 2 列),整幅宽度不留缺口。
    """
    nrows = int(np.ceil(n / ncols))
    fig = plt.figure(figsize=(panel_w * ncols, panel_h * nrows + 1.0),
                     layout="constrained")
    grid = fig.add_gridspec(nrows, ncols * 2)
    n_last = n - (nrows - 1) * ncols
    axes = []
    first = None
    for i in range(n):
        row, col = divmod(i, ncols)
        if row == nrows - 1 and n_last < ncols:
            span = int(ncols * 2 / n_last)
            start = col * span
        else:
            span, start = 2, col * 2
        ax = fig.add_subplot(grid[row, start: start + span], sharey=first)
        if first is None:
            first = ax
        axes.append(ax)
    return fig, axes


def _label_cells(flat: list, n: int, ncols: int = 3):
    """每行最左一面给纵轴组态标签,其余面隐藏刻度文字,避免重复七次。"""
    for ax in flat:
        ax.tick_params(labelleft=False)
    for row in range(int(np.ceil(n / ncols))):
        idx = row * ncols
        if idx < n:
            flat[idx].set_yticks(range(len(GROUP_ORDER)))
            flat[idx].set_yticklabels([GROUP_LABELS[g] for g in GROUP_ORDER])
            for lbl, g in zip(flat[idx].get_yticklabels(), GROUP_ORDER):
                lbl.set_color(INK if g.startswith("traincache") else MUTED)
            flat[idx].tick_params(labelleft=True)


# ---------------------------------------------------------------------------
# 七组态对照(按格子分面)
# ---------------------------------------------------------------------------
def plot_compare_bars(rows: list[dict], cells: list[str], metric: str,
                      xlabel: str, title: str, out_path: Path,
                      percent: bool = False):
    """每格一面,横向条形图:一行一个组态,条长=均值,右侧标数值。

    条形从 0 起算,倍数关系一眼可见;各格量级不同,每面单独定横轴上限。
    """
    summ = {c: cell_summary(rows, c, metric) for c in cells}
    live = [v for s in summ.values() for v in s.values() if v[3] > 0]
    if not live:
        print(f"  {out_path.name}: 无数据,跳过")
        return
    slots = {g: i for i, g in enumerate(GROUP_ORDER)}

    fig, axes = _facet_axes(len(cells))
    for i, (ax, cell) in enumerate(zip(axes, cells)):
        s = summ[cell]
        present = [g for g in GROUP_ORDER if s[g][3] > 0]
        if not present:
            ax.set_title(f"({chr(97 + i)}) {cell}（无有效行）")
            continue
        scale = 100.0 if percent else 1.0
        top = max(s[g][0] for g in present) * scale
        for g in present:
            v = s[g][0] * scale
            color = GROUP_COLORS[g]
            ax.barh(slots[g], v, height=0.62, color=color, alpha=0.95,
                    linewidth=0, zorder=2)
            tag = _pct(s[g][0]) if percent else f"{s[g][0]:.1f}"
            ax.annotate(tag, (v, slots[g]), textcoords="offset points",
                        xytext=(5, 0), ha="left", va="center",
                        fontsize=9.5, color=INK)
        probe = "，探点 n=1" if max(s[g][3] for g in present) == 1 else ""
        ax.set_title(f"({chr(97 + i)}) {cell}{probe}")
        if i % 3 == 0:
            ax.set_xlabel(xlabel)
        ax.set_xlim(0, top * 1.5)
        ax.set_ylim(len(GROUP_ORDER) - 0.45, -0.55)
        ax.grid(axis="x")
        if percent:
            ax.xaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))

    _label_cells(axes, len(cells))
    fig.suptitle(title, x=0.005, ha="left", fontsize=11)
    _save(fig, out_path)
    rows_out = [[c, g, summ[c][g][3], f"{summ[c][g][0]:.3f}",
                 f"{summ[c][g][1]:.3f}", f"{summ[c][g][2]:.3f}"]
                for c in cells for g in GROUP_ORDER if summ[c][g][3]]
    _write_csv(out_path.parent / "data" / f"{out_path.stem}.csv",
               ["cell", "group", "n", "mean", "ci_low", "ci_high"], rows_out)


def plot_compare_all(rows: list[dict], out_dir: Path):
    """产出七组态对照的 3 张图,每张按格子分面。"""
    cells = _cells(rows)
    if not cells:
        print("  compare: 无带格子标记的行,跳过")
        return
    plot_compare_bars(rows, cells, "startup_makespan_s",
                      "启动 makespan（s）", "七组态启动 makespan",
                      out_dir / "compare_startup.png")
    plot_compare_bars(rows, cells, "full_makespan_s",
                      "全程 makespan（s）", "七组态全程 makespan",
                      out_dir / "compare_total.png")
    plot_compare_bars(rows, cells, "stall_ratio", "stall 占比（%）",
                      "七组态 stall 占比",
                      out_dir / "compare_stall.png", percent=True)


# ---------------------------------------------------------------------------
# 启动门控盈亏(startup-surface / fig1)
# ---------------------------------------------------------------------------
def plot_fig1(metrics: list[dict], out_dir: Path):
    """启动 makespan:自由调度与门控成对,按 fanout 分三面。

    横轴为启动 makespan（s，对数轴），每行一个带宽 × 缓存比组合。灰空心点是
    自由调度，实心点落在门控：蓝=更快，橙=更慢，两点之间的连线就是改善量。
    3D 曲面会互相遮挡，热力图把 27 个格子的正负与量级压成色块，这里换成
    点线，绝对位置和方向同时可见。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    rates = [100, 300, 1000]
    ratios = [1, 2, 4]
    fanouts = [1, 2, 3]
    faster, slower = "#1f6fb2", "#d97b29"

    # 筛选 fig1 的 traincache 组 run(需含 fanout/rate/ratio 元数据)
    fig1_rows = [r for r in metrics if r.get("cond", "").startswith("fig1-")]
    if not fig1_rows:
        print("  fig1: 无数据,跳过")
        return

    # 按 (fanout, rate, ratio) 分组，门控与自由调度各自取均值后算改善%
    grid = {}
    for r in fig1_rows:
        try:
            fo = int(r.get("fanout_actual") or 0)
            rate = int(r.get("rate_mbit") or 0)
            ratio = float(r.get("cache_ratio") or 0)
            sm = float(r.get("startup_makespan_s"))
        except (TypeError, ValueError):
            continue
        cell = grid.setdefault((fo, rate, ratio), {"gated": [], "free": []})
        cell.setdefault(r.get("gate_mode") or "gated", []).append(sm)

    pairs = []
    for cell in grid.values():
        gated, _, _ = mean_ci(cell.get("gated", []))
        free, _, _ = mean_ci(cell.get("free", []))
        if gated and free:
            pairs.append(100.0 * (free - gated) / free)
    if not pairs:
        print("  fig1: 缺成对数据,跳过")
        return
    # 发散柱:一根柱一个组合,长度=相对改善%,正负一眼可辨,-443.9% 截断到
    # -118 并在标签上标越界符号。
    xmin, xmax = -120.0, 110.0

    fig, axes = plt.subplots(1, len(fanouts), figsize=(9.6, 4.4), sharey=True,
                             layout="constrained")
    axes = np.atleast_1d(axes)
    csv_rows = []
    for k, (ax, fo) in enumerate(zip(axes, fanouts)):
        for i, ratio in enumerate(ratios):
            for j, rate in enumerate(rates):
                y = i * len(rates) + j
                cell = grid.get((fo, rate, ratio), {})
                gated, _, _ = mean_ci(cell.get("gated", []))
                free, _, _ = mean_ci(cell.get("free", []))
                if not (gated and free):
                    ax.text(xmin + 4, y, "缺测", va="center", fontsize=9,
                            color=MUTED)
                    continue
                gain = 100.0 * (free - gated) / free
                color = faster if gain >= 0 else slower
                clipped = gain < xmin + 2
                shown = max(gain, xmin + 2)
                ax.barh(y, shown, height=0.64, left=0, color=color, alpha=0.92,
                        linewidth=0, zorder=2)
                if clipped:
                    ax.annotate(f"{gain:+.1f}% ◀", (xmin + 3, y),
                                ha="left", va="center", fontsize=9.5,
                                color="white")
                else:
                    ax.annotate(f"{gain:+.1f}%", (shown, y),
                                textcoords="offset points",
                                xytext=(5 if gain >= 0 else -5, 0),
                                ha="left" if gain >= 0 else "right",
                                va="center", fontsize=9.5, color=color)
                csv_rows.append([fo, rate, ratio, len(cell.get("gated", [])),
                                 len(cell.get("free", [])), f"{gated:.3f}",
                                 f"{free:.3f}", f"{gain:.2f}"])
        ax.axvline(0, color="#8b8f94", lw=1.0, zorder=3)
        ax.set_xlim(xmin, xmax)
        ax.set_xticks([-100, -50, 0, 50, 100])
        ax.set_xticklabels(["−100", "−50", "0", "+50", "+100"])
        ax.set_ylim(len(ratios) * len(rates) - 0.5, -0.55)
        ax.set_xlabel("相对自由调度的启动 makespan 变化（%）")
        ax.set_title(f"({chr(97 + k)}) fanout={fo}")
        ax.grid(axis="x")

    axes[0].set_yticks(range(len(ratios) * len(rates)))
    axes[0].set_yticklabels([f"{rate} mbit · ×{ratio}"
                             for ratio in ratios for rate in rates])
    for ax in axes[1:]:
        ax.tick_params(labelleft=False)
    handles = [Patch(facecolor=faster, label="门控更快"),
               Patch(facecolor=slower, label="门控更慢")]
    fig.legend(handles=handles, loc="outside lower center", ncol=2,
               frameon=False, handletextpad=0.5, columnspacing=1.8,
               handlelength=1.2)
    fig.suptitle("启动门控盈亏", x=0.005, ha="left", fontsize=12)
    _save(fig, out_dir / "fig1_startup_surface.png")
    _write_csv(out_dir / "data" / "fig1_startup_surface.csv",
               ["fanout", "rate_mbit", "cache_ratio", "n_gated", "n_free",
                "gated_s", "free_s", "gain_pct"], csv_rows)


# ---------------------------------------------------------------------------
# 全程时长随带宽与 epoch(total-makespan / fig2)
# ---------------------------------------------------------------------------
def plot_fig2(metrics: list[dict], out_dir: Path):
    """全程 makespan 绝对值(s):X=epoch 数,每个 epoch 三根柱,一档带宽一根。

    数据只有 8 GiB 数据集、traincache-cold(协议 §3.1)。柱高是实测值,柱顶
    标数值;epoch 档按分类等距排布,1/3/6 的数值差不撑开间距。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    rates = [100, 300, 1000]
    epochs = [1, 3, 6]
    # 有效带宽 MB/s,取 docs/4.4 实验协议.md §7.4 的实测口径(100/300/1000 mbit 档)
    eff_mbps = {100: 13.4, 300: 35.0, 1000: 61.0}
    compute_s_per_epoch = 80.0
    # 带宽是序数,取蓝→紫的饱和色阶,三档一眼可分且不出现红绿相邻
    colors = {100: "#3fa9e0", 300: "#1f5f9e", 1000: "#5b2a82"}

    fig2_rows = [r for r in metrics
                 if r.get("cond", "").startswith("fig2-")
                 and r.get("group") == "traincache"]
    if not fig2_rows:
        print("  fig2: 无数据,跳过")
        return

    grid = {}
    dataset_bytes = 0.0
    for r in fig2_rows:
        try:
            rate = int(r.get("rate_mbit") or 0)
            ep = int(r.get("epochs") or 0)
            fm = float(r.get("full_makespan_s"))
        except (TypeError, ValueError):
            continue
        grid.setdefault((rate, ep), []).append(fm)
        dataset_bytes = max(dataset_bytes, _num(r.get("dataset_bytes")))

    fig, ax = plt.subplots(figsize=(7.2, 4.2), layout="constrained")
    base = 70.0
    # epoch 按等距分类位置排布,1/3/6 的数值差不再把图拉开
    pos = {ep: i for i, ep in enumerate(epochs)}
    offs = {100: -0.28, 300: 0.0, 1000: 0.28}
    for rate in rates:
        xs, ys = [], []
        for ep in epochs:
            med, _, _ = mean_ci(grid.get((rate, ep), []))
            if med:
                xs.append(pos[ep] + offs[rate])
                ys.append(med)
        if not ys:
            continue
        ax.bar(xs, [y - base for y in ys], bottom=base, width=0.26,
               color=colors[rate], linewidth=0, zorder=3,
               label=f"{rate} mbit")
        for x, y in zip(xs, ys):
            ax.annotate(f"{y:.0f}", (x, y), textcoords="offset points",
                        xytext=(0, 5), ha="center", va="bottom",
                        fontsize=9.5, color=INK)

    ax.set_yscale("log")
    ax.set_ylim(base, 1600)
    ax.set_xlim(-0.55, len(epochs) - 0.45)
    ax.set_xticks(range(len(epochs)))
    ax.set_xticklabels([str(ep) for ep in epochs])
    ax.set_xlabel("epoch 数")
    ax.set_ylabel("全程 makespan（s，对数轴）")
    ax.set_title("全程 makespan")
    ax.legend(loc="upper left", ncol=3, frameon=False, handlelength=1.2,
              columnspacing=1.0)
    ax.grid(axis="y", which="major")
    ax.grid(axis="y", which="minor", linewidth=0.4)
    _save(fig, out_dir / "fig2_total_makespan.png")
    csv_rows = []
    for rate in rates:
        for ep in epochs:
            samples = grid.get((rate, ep), [])
            if not samples:
                continue
            med, lo, hi = mean_ci(samples)
            ref = dataset_bytes * ep / (eff_mbps[rate] * 1e6) if dataset_bytes else ""
            csv_rows.append([rate, ep, len(samples), f"{med:.1f}", f"{lo:.1f}",
                             f"{hi:.1f}", f"{ref:.1f}" if ref else "",
                             compute_s_per_epoch * ep])
    _write_csv(out_dir / "data" / "fig2_total_makespan.csv",
               ["rate_mbit", "epochs", "n", "makespan_s", "ci_low", "ci_high",
                "transport_bound_s", "compute_bound_s"], csv_rows)


# ---------------------------------------------------------------------------
# 阶段切换后的 stall 回落(switch-recovery / fig3)
# ---------------------------------------------------------------------------
def plot_fig3(metrics: list[dict], out_dir: Path):
    """回落时间(epoch × shuffle)。epoch 档从数据里取,一行一格。

    六格一共只有十几个样本,竖排成两列会把横轴撑成一整条空白;改成横轴是
    回落时间、纵轴六格:单次切换按 x 抖动,均值与 95% CI 横着画。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    shuffles = ["fixed", "per-epoch", "curriculum"]
    colors = {"fixed": "#2f6db5", "per-epoch": "#6c3390",
              "curriculum": "#d97b29"}

    fig3_rows = [r for r in metrics if r.get("cond", "").startswith("fig3-")]
    if not fig3_rows:
        print("  fig3: 无数据,跳过")
        return
    epochs = sorted({int(r.get("epochs") or 0) for r in fig3_rows
                     if int(r.get("epochs") or 0) > 0})

    grid = {}
    censored = {}
    for r in fig3_rows:
        try:
            ep = int(r.get("epochs") or 0)
            sh = r.get("shuffle") or ""
        except (TypeError, ValueError):
            continue
        if sh not in shuffles:
            continue
        key = (sh, ep)
        if r.get("status") == "recovered":
            rt = r.get("recovery_time_s")
            if rt is not None:
                grid.setdefault(key, []).append(float(rt))
        elif r.get("status") == "censored":
            censored[key] = censored.get(key, 0) + 1

    # 删失率单独落盘:没恢复的切换若混进均值,会把"恢复慢"伪装成"恢复快"。
    rates = {}
    for sh in shuffles:
        for ep in epochs:
            n_rec = len(grid.get((sh, ep), []))
            n_cen = censored.get((sh, ep), 0)
            rates[f"{sh}/ep{ep}"] = {
                "recovered": n_rec, "censored": n_cen,
                "censored_ratio": round(n_cen / (n_rec + n_cen), 4) if (n_rec + n_cen) else None,
            }
    (out_dir / "fig3_censoring.json").write_text(
        json.dumps(rates, ensure_ascii=False, indent=2))

    sample = [v for vals in grid.values() for v in vals]
    if not sample:
        print("  fig3: 无回落样本,跳过")
        return
    # 只保留有回落样本的 epoch 档,避免出现没有点的刻度
    epochs = sorted({ep for (_, ep), vals in grid.items() if vals})
    rows_plotted = [(sh, ep, grid[(sh, ep)]) for sh in shuffles for ep in epochs
                    if grid.get((sh, ep))]
    lo_v, hi_v = min(sample), max(sample)
    pad = max(0.006, (hi_v - lo_v) * 0.10)

    fig, ax = plt.subplots(figsize=(7.0, 3.05), layout="constrained")
    rng = np.random.default_rng(7)
    csv_rows = []
    for i, (sh, ep, vals) in enumerate(rows_plotted):
        ax.scatter(vals, i + rng.uniform(-0.14, 0.14, len(vals)), s=30,
                   color=colors[sh], alpha=0.55, linewidths=0, zorder=2)
        med, lo, hi = mean_ci(vals)
        ax.plot(med, i, "o", markersize=11, color=colors[sh], mec="white",
                mew=1.5, zorder=4)
        ax.annotate(f"{med:.2f} s", (med, i), textcoords="offset points",
                    xytext=(10, 0), va="center", ha="left", fontsize=10,
                    color=INK)
        csv_rows.append([sh, ep, len(vals), f"{med:.3f}", f"{lo:.3f}",
                         f"{hi:.3f}", censored.get((sh, ep), 0)])

    ax.set_yticks(range(len(rows_plotted)))
    ax.set_yticklabels([f"{sh} · ep{ep}" for sh, ep, _ in rows_plotted])
    for lbl, (sh, _, _) in zip(ax.get_yticklabels(), rows_plotted):
        lbl.set_color(colors[sh])
    ax.set_ylim(len(rows_plotted) - 0.45, -0.55)
    ax.set_xlim(lo_v - pad, hi_v + pad + 0.03)
    ax.set_xlabel("回落时间（s）")
    ax.grid(axis="x")
    ax.xaxis.set_major_formatter(FormatStrFormatter("%.2f"))
    ax.set_title("stall 回落时间")
    handles = [Line2D([], [], marker="o", linestyle="", markersize=6,
                      mfc="#9aa0a6", mec="none", label="单次切换"),
               Line2D([], [], marker="o", linestyle="", markersize=11,
                      mfc="#6b7280", mec="white", label="均值")]
    fig.legend(handles=handles, loc="outside lower center", ncol=2,
               frameon=False, handletextpad=0.5, columnspacing=1.8)
    _save(fig, out_dir / "fig3_recovery.png")
    _write_csv(out_dir / "data" / "fig3_recovery.csv",
               ["shuffle", "epochs", "n", "mean_s", "ci_low", "ci_high",
                "censored"], csv_rows)


# ---------------------------------------------------------------------------
# 启动收益对启动预热字节(pareto-startup / pareto1)
# ---------------------------------------------------------------------------
def plot_pareto1(metrics: list[dict], out_dir: Path):
    """X=启动预取总字节(含 fanout 重复), Y=启动 makespan 改善%。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    p1_rows = [r for r in metrics if r.get("cond", "").startswith("pareto1-")]
    if not p1_rows:
        print("  pareto1: 无数据,跳过")
        return

    by_fanout = {}
    for r in p1_rows:
        try:
            fo = int(r.get("fanout_actual") or 0)
            sb = float(r.get("startup_warmup_bytes") or 0)
            sm = float(r.get("startup_makespan_s") or 0)
        except (TypeError, ValueError):
            continue
        cell = by_fanout.setdefault(fo, {"bytes": [], "sm": [], "free": []})
        if (r.get("gate_mode") or "gated") == "free":
            cell["free"].append(sm)
        else:
            cell["bytes"].append(sb)
            cell["sm"].append(sm)

    fo_colors = {}
    points = []
    for fo in sorted(by_fanout):
        cell = by_fanout[fo]
        if not (cell["bytes"] and cell["sm"] and cell["free"]):
            continue
        bx, blo, bhi = mean_ci(cell["bytes"])
        imp, ilo, ihi = rate_ci(cell["free"], cell["sm"])
        points.append((bx / 1e9, imp, (bx - blo) / 1e9, (bhi - bx) / 1e9,
                       imp - ilo, ihi - imp, fo))
    if not points:
        print("  pareto1: 缺 gated/free 成对数据,跳过")
        return
    points.sort(key=lambda p: p[0])
    # fanout 是序数,蓝→紫→橙三档高饱和,避免同色阶分不清
    palette = ["#1f5f9e", "#a973d1", "#c96a1c", "#5b2a82"]
    fo_colors = {p[6]: palette[i % len(palette)] for i, p in enumerate(points)}

    fig, ax = plt.subplots(figsize=(6.4, 4.0), layout="constrained")
    ax.plot([p[0] for p in points], [p[1] for p in points], color="#9CA3AF",
            lw=1.0, zorder=1)
    for x, y, xlo, xhi, ylo, yhi, fo in points:
        ax.plot(x, y, marker="o", markersize=10, color=fo_colors[fo],
                mec="white", mew=1.2, zorder=4)
        ax.annotate(f"fanout={fo}\n{y:+.1f}%", (x, y), textcoords="offset points",
                    xytext=(12, 4), fontsize=9, color=INK, linespacing=1.4)
    ax.axhline(0, color="#9CA3AF", linestyle="--", linewidth=0.8, zorder=0)
    # 纵轴范围按点与零线留白,不再按置信区间撑开
    y_lo = min(0.0, min(p[1] for p in points))
    y_hi = max(0.0, max(p[1] for p in points))
    pad = 0.12 * max(4.0, y_hi - y_lo)
    ax.set_ylim(y_lo - pad, y_hi + pad)
    ax.set_xlabel("启动预取字节（GB，含 fanout 重复读入）")
    ax.set_ylabel("启动 makespan 改善（%）")
    ax.set_title("启动收益与启动预取字节")
    ax.grid(axis="both")
    _save(fig, out_dir / "pareto1_startup_bytes.png")
    csv_rows = []
    for x, y, xlo, xhi, ylo, yhi, fo in points:
        cell = by_fanout[fo]
        csv_rows.append([fo, len(cell["sm"]), len(cell["free"]),
                         f"{x:.3f}", f"{x - xlo:.3f}", f"{x + xhi:.3f}",
                         f"{y:.2f}", f"{y - ylo:.2f}", f"{y + yhi:.2f}"])
    _write_csv(out_dir / "data" / "pareto1_startup_bytes.csv",
               ["fanout", "n_gated", "n_free", "warmup_gb", "gb_ci_low",
                "gb_ci_high", "improve_pct", "pct_ci_low", "pct_ci_high"],
               csv_rows)


# ---------------------------------------------------------------------------
# 全程收益对持续预取字节(pareto-sustained / pareto2)
# ---------------------------------------------------------------------------
def _prefetch_bytes(r):
    """持续预取总字节:优先 prefetch_bytes_run,退回 sustained_prefetch_bytes。"""
    for key in ("prefetch_bytes_run", "sustained_prefetch_bytes"):
        v = r.get(key)
        if v not in (None, ""):
            return float(v)
    return None


def plot_pareto2(metrics: list[dict], out_dir: Path):
    """X=持续预取总字节, Y=全程 makespan 改善%。按 lookahead 着色,淘汰策略分 marker。

    基线是自身最小预取档(lookahead=8 + heat):纵轴表示相对该档的改善,
    同档的点恒为 0。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    p2_rows = [r for r in metrics if r.get("cond", "").startswith("pareto2-")]
    if not p2_rows:
        print("  pareto2: 无数据,跳过")
        return

    policies = sorted({r.get("evict_policy") for r in p2_rows
                       if r.get("group") == "traincache" and r.get("evict_policy")})
    markers = {"heat": "o", "lru": "s", "lfu": "^"}
    base_rows = [r for r in p2_rows
                 if int(r.get("lookahead") or 0) == 8
                 and r.get("evict_policy") == "heat"
                 and r.get("group") == "traincache"]
    base_fm = [float(r["full_makespan_s"]) for r in base_rows
               if r.get("full_makespan_s")]
    if not base_fm:
        print("  pareto2: 缺 lookahead=8 + heat 基线,跳过")
        return
    baseline_fm = float(np.mean(base_fm))

    lookaheads = sorted({int(r.get("lookahead") or 0) for r in p2_rows
                         if int(r.get("lookahead") or 0) > 0})
    ramp = ["#1f5f9e", "#a973d1", "#c96a1c", "#5b2a82"]
    la_colors = {la: ramp[i % len(ramp)] for i, la in enumerate(lookaheads)}

    fig, ax = plt.subplots(figsize=(6.4, 4.0), layout="constrained")
    mean_points = []
    ys_all = []
    csv_rows = []
    for la in lookaheads:
        for pol in policies:
            rows = [r for r in p2_rows
                    if int(r.get("lookahead") or 0) == la
                    and r.get("evict_policy") == pol
                    and r.get("group") == "traincache"]
            if not rows:
                continue
            bx, blo, bhi = mean_ci([_prefetch_bytes(r) for r in rows
                                    if _prefetch_bytes(r) is not None])
            fm = [float(r["full_makespan_s"]) for r in rows
                  if r.get("full_makespan_s")]
            if not (bx and fm):
                continue
            imp, ilo, ihi = rate_ci(base_fm, fm)
            # 单次运行的小空心点:两点一档时,离散度只能靠它看出来。
            # 基线档(8+heat)按定义恒为 0,四个空心点会叠成一个黑团,不画。
            if not (la == 8 and pol == "heat"):
                for r in rows:
                    sb = _prefetch_bytes(r)
                    fm_i = r.get("full_makespan_s")
                    if sb is None or fm_i is None:
                        continue
                    y_i = 100.0 * (baseline_fm - float(fm_i)) / baseline_fm
                    ys_all.append(y_i)
                    ax.plot(sb / 1e9, y_i, marker=markers.get(pol, "o"), ms=6,
                            mfc="none", mec=la_colors[la], mew=1.0, alpha=0.7,
                            zorder=2)
            ys_all.append(imp)
            ax.plot(bx / 1e9, imp, marker=markers.get(pol, "o"), ms=10,
                    mfc=la_colors[la], mec="white", mew=1.2, zorder=4)
            mean_points.append((bx / 1e9, imp, la, pol))
            csv_rows.append([la, pol, len(rows), f"{bx / 1e9:.3f}",
                             f"{blo / 1e9:.3f}", f"{bhi / 1e9:.3f}",
                             f"{imp:.2f}", f"{ilo:.2f}", f"{ihi:.2f}",
                             f"{np.mean(fm):.1f}", f"{baseline_fm:.1f}"])

    mean_points.sort(key=lambda p: p[0])
    ax.plot([p[0] for p in mean_points], [p[1] for p in mean_points],
            color="#9CA3AF", lw=1.0, zorder=1)
    for x, y, la, pol in mean_points:
        tag = "（基线）" if la == 8 else f"（{y:+.1f}%）"
        ax.annotate(f"lookahead={la}{tag}", (x, y), textcoords="offset points",
                    xytext=(12, 13), va="bottom", fontsize=9, color=INK)

    # 基线点的标签往上偏 13 pt,顶部留出空间,避免压到标题
    ax.set_ylim(min(ys_all) - 1.5, 6.0)

    if len(policies) > 1:
        handles = [Line2D([], [], marker=markers.get(p, "o"), linestyle="",
                          mfc="white", mec="#374151", label=p) for p in policies]
        ax.legend(handles=handles, frameon=False, loc="lower left")

    ax.axhline(0, color="#9CA3AF", linestyle="--", linewidth=0.8, zorder=0)
    ax.set_xlabel("持续预取字节（GB）")
    ax.set_ylabel("全程 makespan 改善（%，基线 lookahead=8）")
    ax.set_title("全程收益与持续预取字节")
    ax.grid(axis="both")
    _save(fig, out_dir / "pareto2_sustained_bytes.png")
    _write_csv(out_dir / "data" / "pareto2_sustained_bytes.csv",
               ["lookahead", "evict", "n", "prefetch_gb", "gb_ci_low",
                "gb_ci_high", "improve_pct", "pct_ci_low", "pct_ci_high",
                "makespan_s", "baseline_s"], csv_rows)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="实验出图")
    ap.add_argument("--summary", type=Path, default=None,
                    help="五组对照 summary.csv;零参时自动扫描 RESULTS_DIR")
    ap.add_argument("--metrics-dir", type=Path, default=None,
                    help="experiments 产物目录(含 metrics.csv);零参时自动扫描 RESULTS_DIR")
    ap.add_argument("--out-dir", type=Path,
                    default=RESULTS_DIR / "figures",
                    help="图片输出目录(缺省取数据层 results/figures)")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # 零参:扫描 results/ 下所有含 summary.csv 的目录。只给其中一个旗标时,
    # 另一个仍按旧缺省(compare/summary.csv 或 results/experiments),保持兼容。
    auto = args.summary is None and args.metrics_dir is None
    if auto:
        result_dirs = discover_result_dirs(RESULTS_DIR)
        summary_paths = [_summary_path(d) for d in result_dirs]
        metrics_dirs = result_dirs
        print(f"[auto] {RESULTS_DIR} 下发现 {len(summary_paths)} 个含 summary.csv 的目录")
    else:
        summary_paths = ([args.summary] if args.summary is not None
                         else [RESULTS_DIR / "compare" / "summary.csv"])
        metrics_dirs = ([args.metrics_dir] if args.metrics_dir is not None
                        else [RESULTS_DIR / "experiments"])

    # 五组对照
    version = current_version()
    all_rejected = []
    compare_rows = []
    for path in summary_paths:
        if not path.is_file():
            print(f"[compare] {path} 不存在,跳过")
            continue
        # 自动扫描时只收对照分区:scores/ 是本轮 TrainCache 行,baselines/ 是既有
        # 对照组行(4.3 §5)。figures/ 与 policy/ 下的点目录名带 fig*/pareto2-*
        # 前缀由下面的曲面函数单独消费,而淘汰矩阵的行名是 policy-<策略>-rNN-*;
        # 按前缀黑名单挡不住它,会把 C 轨样本混进七组对照柱状图。
        if auto and path.relative_to(RESULTS_DIR).parts[0] not in COMPARE_PARTITIONS:
            print(f"[compare] 跳过非对照分区 {path.parent}")
            continue
        print(f"[compare] 读取 {path}")
        rows, rejected = read_gated(path, version)
        print_rejects(rejected, RESULTS_DIR)
        all_rejected.extend(rejected)
        mapped = adapt_summary_rows(rows)
        # 格子名取自产物目录(S-100 / M-1000 …),七组态只在本格内可比
        for r in mapped:
            r["_cell"] = path.parent.name
        print(f"  共 {len(rows) + len(rejected)} 条,有效 {len(mapped)} 条,"
              f"排除 {len(rejected)} 条")
        compare_rows.extend(mapped)
    if compare_rows:
        plot_compare_all(compare_rows, args.out_dir)
    else:
        print("[compare] 没有可读的 summary.csv,跳过五组对照图")

    # 六张图(数据来自各结果目录下的 metrics.csv)
    metrics = []
    seen_sources = set()
    # 图系列产物:每个 COND 一份 summary.csv,cond 列带 fig*/pareto* 前缀
    for directory in metrics_dirs:
        if not directory.name.startswith(
                ("fig1-", "fig2-", "fig3-", "pareto1-", "pareto2-")):
            continue
        path = _summary_path(directory)
        if not path.is_file():
            continue
        if str(path) in seen_sources:
            continue
        seen_sources.add(str(path))
        rows, rejected = read_gated(path, version)
        print_rejects(rejected, RESULTS_DIR)
        all_rejected.extend(rejected)
        metrics.extend(adapt_summary_rows(rows))
    for directory in metrics_dirs:
        for row in read_metrics_dir(directory):
            source = row.get("_source")
            if source in seen_sources:
                continue
            seen_sources.add(source)
            metrics.extend(adapt_summary_rows([row]))
    # stall 回落时间不在 metrics.csv,来自 runs/*/recovery.json
    for directory in metrics_dirs:
        for row in read_recovery_runs(directory):
            source = row.get("_source")
            if source in seen_sources:
                continue
            seen_sources.add(source)
            metrics.append(row)
    print(f"[experiments] 读取 {len(metrics)} 条 metrics/回落样本")
    plot_fig1(metrics, args.out_dir)
    plot_fig2(metrics, args.out_dir)
    plot_fig3(metrics, args.out_dir)
    plot_pareto1(metrics, args.out_dir)
    plot_pareto2(metrics, args.out_dir)

    print(f"\n[done] 图片输出到 {args.out_dir}")
    if all_rejected:
        reject_csv = RESULTS_DIR / "summary" / "rejected.csv"
        write_reject_log(reject_csv, all_rejected)
        print(f"[reject] 合计排除 {len(all_rejected)} 条,明细见 {reject_csv}")


if __name__ == "__main__":
    main()
