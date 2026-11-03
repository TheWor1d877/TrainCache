#!/usr/bin/env python3
"""实验出图脚本。

读取 bench/results/compare/summary.csv 与 bench/results/experiments/ 下的
metrics.csv,产出预注册(docs/4.4 §2~§5)定义的六张图:

  fig1  启动阶段盈亏曲面(带宽 × 缓存比 × fanout → 启动 makespan 改善%)
  fig2  全程 makespan 绝对值(带宽 × epoch,8 GiB 数据集,附计算与传输下界)
  fig3  阶段切换 stall 回落曲线(epoch × shuffle → 回落时间平均数)
  pareto1  启动收益 vs 重复预热字节(fanout 1/2/3)
  pareto2  全程收益 vs 持续预取字节(lookahead 8/16/32 × 淘汰策略 heat,
           纵轴相对自身基线 lookahead=8+heat)

五组对照(summary.csv)额外产出 4 张柱状对比图:
  startup_makespan / total_makespan / stall_ratio / hit_rate

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
from matplotlib import cm

# 中文字体配置(避免中文显示为方框)
plt.rcParams["font.sans-serif"] = ["Noto Sans CJK SC", "Noto Sans CJK JP",
                                    "WenQuanYi Zen Hei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

# 对照装置分组固定顺序与配色(与 bench/compare/run_bench.py 的 GROUPS 同口径:
# 五个对照组拆成 cold/warm 两态,共 7 个具体组名)
GROUP_ORDER = ["direct", "juicefs-cold", "juicefs-warm",
               "fluid-cold", "fluid-warm",
               "traincache-cold", "traincache-warm"]
GROUP_LABELS = {
    "direct": "Direct",
    "juicefs-cold": "JuiceFS\n(cold)",
    "juicefs-warm": "JuiceFS\n(warm)",
    "fluid-cold": "Fluid\n(cold)",
    "fluid-warm": "Fluid\n(warm)",
    "traincache-cold": "TrainCache\n(cold)",
    "traincache-warm": "TrainCache\n(warm)",
}
GROUP_COLORS = {
    "direct": "#9ca3af",
    "juicefs-cold": "#60a5fa",
    "juicefs-warm": "#3b82f6",
    "fluid-cold": "#fbbf24",
    "fluid-warm": "#f59e0b",
    "traincache-cold": "#34d399",
    "traincache-warm": "#10b981",
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


def group_summary(rows: list[dict], metric: str) -> dict:
    """按 group 聚合某指标 → {group: (mean, lo, hi, n)}。"""
    out = {}
    for g in GROUP_ORDER:
        vals = []
        for r in rows:
            # summary.csv 的组名是 traincache-cold 这类全名;adapt 之后 group 被
            # 归一成 traincache/fluid/juicefs,原名留在 raw_group。
            if (r.get("raw_group") or r.get("group")) != g:
                continue
            if r.get("void", "").lower() == "true":
                continue
            v = r.get(metric)
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                continue
        m, lo, hi = mean_ci(vals)
        out[g] = (m, lo, hi, len(vals))
    return out


# ---------------------------------------------------------------------------
# 五组对照柱状图
# ---------------------------------------------------------------------------
def plot_group_bar(rows: list[dict], metric: str, ylabel: str, title: str,
                   out_path: Path, ylim=None):
    """五组对照柱状图 + bootstrap 95% CI 误差棒。"""
    summ = group_summary(rows, metric)
    # 只画有有效样本的组:group_summary 会为 GROUP_ORDER 里的每个组都留一条记录,
    # 没有样本的组画出来是 0 高度的柱子,会被误读成「该组耗时为 0」。
    groups = [g for g in GROUP_ORDER if summ[g][3] > 0]
    means = [summ[g][0] for g in groups]
    ci_low = [summ[g][1] for g in groups]
    ci_high = [summ[g][2] for g in groups]
    ns = [summ[g][3] for g in groups]
    errs_low = [m - lo for m, lo in zip(means, ci_low)]
    errs_high = [hi - m for m, hi in zip(means, ci_high)]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    x = np.arange(len(groups))
    bars = ax.bar(x, means, yerr=[errs_low, errs_high], capsize=4,
                  color=[GROUP_COLORS[g] for g in groups], edgecolor="white",
                  linewidth=0.8, width=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels([GROUP_LABELS[g] for g in groups], fontsize=9)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_title(title, fontsize=11, pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3, linestyle="--")

    # 标注 n
    for bar, n in zip(bars, ns):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"n={n}", ha="center", va="bottom", fontsize=7, color="#555")
    if ylim:
        ax.set_ylim(ylim)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  saved {out_path.name}")


def plot_compare_all(rows: list[dict], out_dir: Path):
    """产出五组对照的 4 张柱状图。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    plot_group_bar(rows, "startup_makespan", "Startup makespan (s)",
                   "五组对照:启动 makespan",
                   out_dir / "compare_startup.png")
    plot_group_bar(rows, "total_makespan", "Total makespan (s)",
                   "五组对照:全程 makespan",
                   out_dir / "compare_total.png")
    plot_group_bar(rows, "stall_ratio", "Stall ratio",
                   "五组对照:stall 占比",
                   out_dir / "compare_stall.png", ylim=(0, 1.05))
    plot_group_bar(rows, "hit_rate", "Cache hit rate",
                   "五组对照:缓存命中率",
                   out_dir / "compare_hitrate.png", ylim=(0, 1.05))


# ---------------------------------------------------------------------------
# 启动门控盈亏曲面(startup-surface / fig1)
# ---------------------------------------------------------------------------
def plot_fig1(metrics: list[dict], out_dir: Path):
    """启动 makespan 改善% = 100 × (自由调度 - 门控) / 自由调度。

    X=带宽(100/300/1000), Y=缓存比(1/2/4), Z=改善%。
    fanout=1/2/3 分三个子图。数据缺失区域留空。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    rates = [100, 300, 1000]
    ratios = [1, 2, 4]
    fanouts = [1, 2, 3]

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

    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5), subplot_kw={"projection": "3d"})
    for ax, fo in zip(axes, fanouts):
        X, Y, Z = [], [], []
        for rate in rates:
            for ratio in ratios:
                cell = grid.get((fo, rate, ratio), {})
                gated, _, _ = mean_ci(cell.get("gated", []))
                free, _, _ = mean_ci(cell.get("free", []))
                if gated and free:
                    gain = 100.0 * (free - gated) / free
                else:
                    gain = np.nan
                X.append(rate)
                Y.append(ratio)
                Z.append(gain)
        if not np.isfinite(Z).any():
            ax.set_title(f"fanout={fo}\n(无数据)")
            continue
        Xa = np.array(X).reshape(3, 3)
        Ya = np.array(Y).reshape(3, 3)
        Za = np.array(Z).reshape(3, 3)
        surf = ax.plot_surface(Xa, Ya, Za, cmap=cm.RdYlGn_r, alpha=0.85,
                               edgecolor="gray", linewidth=0.3)
        ax.set_xlabel("带宽 (mbit)")
        ax.set_ylabel("缓存比")
        ax.set_zlabel("启动改善 %")
        ax.set_title(f"fanout={fo}")
        fig.colorbar(surf, ax=ax, shrink=0.5)
    fig.suptitle("启动门控盈亏曲面:门控 vs 自由调度(带宽 × 缓存比 × fanout)", fontsize=12)
    fig.tight_layout()
    fig.savefig(out_dir / "fig1_startup_surface.png", dpi=150)
    plt.close(fig)
    print("  saved fig1_startup_surface.png")


# ---------------------------------------------------------------------------
# 全程时长随带宽与 epoch(total-makespan / fig2)
# ---------------------------------------------------------------------------
def plot_fig2(metrics: list[dict], out_dir: Path):
    """全程 makespan 绝对值(s):X=epoch 数,每条线一档带宽。

    数据只有 8 GiB 数据集、traincache-cold(协议 §3.1);参考线两条:
    计算下界 80 s/epoch,传输下界 = 读字节 ÷ 有效带宽。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    rates = [100, 300, 1000]
    epochs = [1, 3, 6]
    # 有效带宽 MB/s,取 docs/4.4 实验协议.md §7.4 的实测口径(100/300/1000 mbit 档)
    eff_mbps = {100: 13.4, 300: 35.0, 1000: 61.0}
    compute_s_per_epoch = 80.0
    colors = {100: "#60a5fa", 300: "#10b981", 1000: "#f59e0b"}

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

    fig, ax = plt.subplots(figsize=(8, 5))
    for rate in rates:
        ys = []
        for ep in epochs:
            med, _, _ = mean_ci(grid.get((rate, ep), []))
            ys.append(med if med else np.nan)
        ax.plot(epochs, ys, marker="o", linewidth=1.8, color=colors[rate],
                label=f"{rate} mbit")
        if dataset_bytes:
            ref = [dataset_bytes * ep * 1.0 / (eff_mbps[rate] * 1e6)
                   for ep in epochs]
            ax.plot(epochs, ref, linestyle="--", linewidth=1.0,
                    color=colors[rate], alpha=0.45)

    ax.plot(epochs, [compute_s_per_epoch * ep for ep in epochs],
            linestyle=":", linewidth=1.2, color="black", alpha=0.6,
            label="计算下界 (80 s/epoch)")

    ax.set_xlabel("epoch 数")
    ax.set_ylabel("全程 makespan (s)")
    ax.set_title("全程 makespan(8 GiB,traincache-cold)")
    ax.legend()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(alpha=0.3, linestyle="--")
    ax.set_xticks(epochs)
    fig.tight_layout()
    fig.savefig(out_dir / "fig2_total_makespan.png", dpi=150)
    plt.close(fig)
    print("  saved fig2_total_makespan.png")


# ---------------------------------------------------------------------------
# 阶段切换后的 stall 回落(switch-recovery / fig3)
# ---------------------------------------------------------------------------
def plot_fig3(metrics: list[dict], out_dir: Path):
    """回落时间平均数(epoch × shuffle)。三组 shuffle 三条线。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    epochs = [1, 3, 10]
    shuffles = ["fixed", "per-epoch", "curriculum"]
    colors = {"fixed": "#60a5fa", "per-epoch": "#10b981", "curriculum": "#f59e0b"}

    fig3_rows = [r for r in metrics if r.get("cond", "").startswith("fig3-")]
    if not fig3_rows:
        print("  fig3: 无数据,跳过")
        return

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

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for sh in shuffles:
        ys, err_lo, err_hi = [], [], []
        for ep in epochs:
            samples = grid.get((sh, ep), [])
            if not samples:
                ys.append(np.nan)
                err_lo.append(0.0)
                err_hi.append(0.0)
                continue
            med, lo, hi = mean_ci(samples)
            ys.append(med)
            err_lo.append(max(0.0, med - lo))
            err_hi.append(max(0.0, hi - med))
        ax.errorbar(epochs, ys, yerr=[err_lo, err_hi],
                    marker="o", capsize=3, label=sh, color=colors[sh], linewidth=1.5)
    ax.set_xlabel("epoch 数")
    ax.set_ylabel("回落时间 (s)")
    ax.set_title("阶段切换 stall 回落时间")
    ax.legend(title="shuffle")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(alpha=0.3, linestyle="--")
    ax.set_xticks(epochs)
    fig.tight_layout()
    fig.savefig(out_dir / "fig3_recovery.png", dpi=150)
    plt.close(fig)
    print("  saved fig3_recovery.png")


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

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for fo in sorted(by_fanout):
        cell = by_fanout[fo]
        b_med, _, _ = mean_ci(cell["bytes"])
        sm_med, _, _ = mean_ci(cell["sm"])
        free_med, _, _ = mean_ci(cell["free"])
        if not (b_med and sm_med and free_med):
            continue
        improve = 100 * (free_med - sm_med) / free_med
        ax.scatter(b_med / 1e9, improve, s=120, zorder=3,
                   label=f"fanout={fo}")
        ax.annotate(f"fanout={fo}", (b_med / 1e9, improve),
                    textcoords="offset points", xytext=(8, 5), fontsize=9)
    ax.set_xlabel("启动预取字节 (GB)")
    ax.set_ylabel("启动 makespan 改善 (%)")
    ax.set_title("启动收益 vs 重复预热字节")
    ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(alpha=0.3, linestyle="--")
    fig.tight_layout()
    fig.savefig(out_dir / "pareto1_startup_bytes.png", dpi=150)
    plt.close(fig)
    print("  saved pareto1_startup_bytes.png")


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

    lookaheads = [8, 16, 32]
    policies = ["heat"]
    markers = {"heat": "o", "lru": "s", "lfu": "^"}
    cmap = cm.viridis
    la_colors = {la: cmap(i / (len(lookaheads) - 1)) for i, la in enumerate(lookaheads)}

    base_rows = [r for r in p2_rows
                 if int(r.get("lookahead") or 0) == 8
                 and r.get("evict_policy") == "heat"
                 and r.get("group") == "traincache"]
    baseline_fm = mean_ci([float(r["full_makespan_s"]) for r in base_rows
                           if r.get("full_makespan_s")])[0]
    if not baseline_fm:
        print("  pareto2: 缺 lookahead=8 + heat 基线,跳过")
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    for la in lookaheads:
        for pol in policies:
            rows = [r for r in p2_rows
                    if int(r.get("lookahead") or 0) == la
                    and r.get("evict_policy") == pol
                    and r.get("group") == "traincache"]
            if not rows:
                continue
            sb_med, _, _ = mean_ci([_prefetch_bytes(r) for r in rows
                                    if _prefetch_bytes(r) is not None])
            fm_med, _, _ = mean_ci([float(r["full_makespan_s"])
                                    for r in rows if r.get("full_makespan_s")])
            improve = (100 * (baseline_fm - fm_med) / baseline_fm) if fm_med else np.nan
            ax.scatter(sb_med / 1e9, improve, s=100, marker=markers[pol],
                       color=la_colors[la], edgecolor="black", linewidth=0.5,
                       zorder=3)

    # 图例
    from matplotlib.lines import Line2D
    la_handles = [Line2D([0], [0], marker="o", color="w", markerfacecolor=la_colors[la],
                          markersize=8, label=f"lookahead={la}") for la in lookaheads]
    pol_handles = [Line2D([0], [0], marker=markers[p], color="w", markerfacecolor="gray",
                          markeredgecolor="black", markersize=8, label=p)
                   for p in policies]
    leg1 = ax.legend(handles=la_handles, title="lookahead", loc="upper left",
                     fontsize=8, title_fontsize=9)
    ax.add_artist(leg1)
    ax.legend(handles=pol_handles, title="evict", loc="lower right",
              fontsize=8, title_fontsize=9)

    ax.set_xlabel("持续预取字节 (GB)")
    ax.set_ylabel("全程 makespan 相对自身基线改善 (%)")
    ax.set_title("全程收益 vs 持续预取字节(基线 lookahead=8+heat)")
    ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(alpha=0.3, linestyle="--")
    fig.tight_layout()
    fig.savefig(out_dir / "pareto2_sustained_bytes.png", dpi=150)
    plt.close(fig)
    print("  saved pareto2_sustained_bytes.png")


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
