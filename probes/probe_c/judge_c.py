#!/usr/bin/env python3
"""探针C 判定:多 epoch 全程 stall 的"回升 ≥ 冷态基准 30%"分口径判据。

口径(与 config.py 参数对齐,冷态基准从 probe_b 的 per_step 动态计算,不硬编码):
  * w0 档:窗口指标 = 窗口内 stall(median) ;基准 = cold_w0 全程 per_step 的
    median stall(约 71.7%);阈值 = 0.30 × 基准;
  * w4/w8 档:窗口指标 = 窗口内 wait_s ≥ PROBE_C_TAIL_WAIT_S(0.5)的步数占比;
    基准 = cold_wX 全程 per_step 同口径占比;阈值 = 0.30 × 基准;
  * 四窗口:A=epoch0 中段 [133,266)、B=epoch1 切换后 [0,20)、
    C=epoch2 切换后 [0,20)、D=epoch1 中段;
  * 绿灯:cold_w4 的 A、B ≥ 0.30×基准(cold_w4)且 cold_w0 的 A、B(median 口径)
    ≥ 0.30×71.7%=21.5%;cold_w8 仅报告;
  * hot_w4 全窗口指标 < 阈值(装置判别力哨兵)。hot_w4 自身基准占比为 0,
    "0.30×自身基准"退化恒不可达,因此哨兵取与冷组同一标尺(0.30×cold_w4 基准):
    冷态必须越过的门槛,hot 必须压在门下 —— 否则装置区分不出冷热,判装置污染,
    verdict 标 FAIL 并说明。

窗口指标的 95% CI 用 bootstrap.pct_bootstrap_ci(seed=42)给出。
输出 verdict.json(含中文报告全文)并打印报告;绿灯 exit 0,否则 exit 1。
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
from bootstrap import pct_bootstrap_ci

RATIO = config.PROBE_C_GREEN_RATIO
TAIL = config.PROBE_C_TAIL_WAIT_S
SWITCH = config.PROBE_C_SWITCH_WINDOW


def load(path: Path):
    if not path.exists():
        sys.exit(f"缺少结果 {path}:先跑 bash probe_c/run_c.sh / probe_b/run.sh")
    return json.loads(path.read_text())


def stall(r):
    return r["wait_s"] / r["step_s"] if r["step_s"] > 0 else 0.0


def measured(rs):
    return [r for r in rs if not r.get("warmup")]


def window(rs, epoch, lo, hi):
    return [r for r in measured(rs)
            if r["epoch"] == epoch and lo <= r["step_in_epoch"] < hi]


def eval_window(rs, mode):
    """返回 (指标, 95%CI, n);mode ∈ {median, tail}。"""
    vals = np.array([stall(r) for r in rs] if mode == "median"
                    else [1.0 if r["wait_s"] >= TAIL else 0.0 for r in rs])
    if len(vals) == 0:
        return None, None, 0
    stat = np.median if mode == "median" else np.mean
    lo, hi, point = pct_bootstrap_ci(vals, stat=stat, n_resamples=2000,
                                     alpha=0.05, seed=42)
    return float(point), [lo, hi], len(vals)


def tail_frac(per_step):
    return float(np.mean([1.0 if r["wait_s"] >= TAIL else 0.0 for r in per_step]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe-c-dir", type=Path,
                    default=config.RESULTS_DIR / "probe_c")
    ap.add_argument("--probe-b-dir", type=Path,
                    default=config.RESULTS_DIR / "probe_b")
    args = ap.parse_args()

    # ---------------- 冷态基准(probe_b per_step 动态计算)----------------
    pb = {tag: load(args.probe_b_dir / f"{tag}.json")["per_step"]
          for tag in ("cold_w0", "cold_w4", "cold_w8", "hot_w4")}
    base_w0_med = float(np.median([stall(r) for r in pb["cold_w0"]]))
    base_tail = {tag: tail_frac(pb[tag]) for tag in pb}
    thr = {
        "cold_w0": RATIO * base_w0_med,
        "cold_w4": RATIO * base_tail["cold_w4"],
        "cold_w8": RATIO * base_tail["cold_w8"],
        # hot 哨兵:自身基准占比 0 → 0.30×0 恒不可达,改用与冷组同一标尺
        "hot_w4": RATIO * base_tail["cold_w4"],
    }

    # ---------------- 四组结果与四窗口 ----------------
    groups = {}
    for tag in ("cold_w0", "cold_w4", "cold_w8", "hot_w4"):
        d = load(args.probe_c_dir / f"c_{tag}.json")
        rs = d["per_step"]
        mode = "median" if tag == "cold_w0" else "tail"
        steps = d.get("steps_per_epoch") or (
            config.N_TARS * config.N_IMGS_PER_TAR // config.PROBE_B_BATCH)
        lo = int(steps * config.PROBE_C_MID_BEGIN)
        hi = int(steps * config.PROBE_C_MID_END)
        win_def = [("A", 0, lo, hi), ("B", 1, 0, SWITCH),
                   ("C", 2, 0, SWITCH), ("D", 1, lo, hi)]
        wins = {}
        for wid, ep, wlo, whi in win_def:
            wr = window(rs, ep, wlo, whi)
            m, ci, n = eval_window(wr, mode)
            wins[wid] = {"epoch": ep, "range": [wlo, whi], "n": n,
                         "metric": m, "ci95": ci,
                         "threshold": thr[tag],
                         "pass_ge_thr": (m is not None and m >= thr[tag]),
                         "ci_minus_thr": (ci[0] - thr[tag]) if ci else None}
        groups[tag] = {"mode": mode, "workers": d["workers"], "windows": wins,
                       "windows_def": {w: [ep, wlo, whi] for w, ep, wlo, whi in win_def},
                       "per_epoch": d["per_epoch"],
                       "path": str((args.probe_c_dir / f"c_{tag}.json").resolve()),
                       "environment": d.get("environment", {})}

    # ---------------- 判定 ----------------
    ok = {
        "cold_w4_A": groups["cold_w4"]["windows"]["A"]["pass_ge_thr"],
        "cold_w4_B": groups["cold_w4"]["windows"]["B"]["pass_ge_thr"],
        "cold_w0_A": groups["cold_w0"]["windows"]["A"]["pass_ge_thr"],
        "cold_w0_B": groups["cold_w0"]["windows"]["B"]["pass_ge_thr"],
        "hot_sentinel": all(not groups["hot_w4"]["windows"][w]["pass_ge_thr"]
                            for w in "ABCD"),
    }
    cold_green = (ok["cold_w4_A"] and ok["cold_w4_B"]
                  and ok["cold_w0_A"] and ok["cold_w0_B"])
    verdict = cold_green and ok["hot_sentinel"]

    # ---------------- 中文报告 ----------------
    L = []
    L.append("====== 探针C 判定:多 epoch 全程 data stall(回升 ≥ 冷态基准 30%)======")
    L.append(f"口径:w0=窗口 stall 中位数;w4/w8/hot=窗口内 wait_s≥{TAIL}s 步数占比;"
             f"阈值={RATIO:.0%}×冷态基准(probe_b per_step 动态算)")
    L.append(f"窗口:A=epoch0 中段[{groups['cold_w4']['windows']['A']['range'][0]},"
             f"{groups['cold_w4']['windows']['A']['range'][1]})  B=epoch1 切换后[0,{SWITCH})"
             f"  C=epoch2 切换后[0,{SWITCH})  D=epoch1 中段")
    L.append("")
    L.append("-- 逐 epoch 中位/均值 stall --")
    for tag in ("cold_w0", "cold_w4", "cold_w8", "hot_w4"):
        cells = []
        for m in groups[tag]["per_epoch"]:
            ms = m["median_stall"]; mn = m["mean_stall"]
            cells.append("epoch%d: %s / %s" % (
                m["epoch"],
                f"{ms:.1%}" if ms is not None else "—",
                f"{mn:.1%}" if mn is not None else "—"))
        L.append(f"  {tag:>7} ({groups[tag]['mode']} 口径): " + "; ".join(cells))
    L.append("")
    L.append("-- 四窗口指标 vs 阈值 --")
    L.append(f"  {'组':>7} {'指标':>4} {'n':>4} {'窗口指标':>9} {'95%CI':>19} {'阈值':>8} 判定")
    for tag in ("cold_w0", "cold_w4", "cold_w8", "hot_w4"):
        w = groups[tag]["windows"]
        gate = ("A,B 门槛" if tag in ("cold_w0", "cold_w4")
                else ("仅报告" if tag == "cold_w8" else "哨兵 <阈值"))
        for wid in "ABCD":
            c = w[wid]
            m = f"{c['metric']:.1%}" if c["metric"] is not None else "—"
            ci = (f"[{c['ci95'][0]:.1%},{c['ci95'][1]:.1%}]" if c["ci95"] else "—")
            if tag == "hot_w4":
                mark = "哨兵过" if not c["pass_ge_thr"] else "哨兵FAIL"
            else:
                mark = "达标" if c["pass_ge_thr"] else "未达"
                if tag == "cold_w8":
                    mark += "(仅报告)"
            L.append(f"  {tag:>7} {('中位' if groups[tag]['mode']=='median' else '尾占比'):>4} "
                     f"{c['n']:>4} {m:>9} {ci:>19} {c['threshold']:>8.1%} {mark}  [{gate}]")
    L.append("")
    L.append(f"基准:cold_w0 median_stall={base_w0_med:.1%};"
             f"tail 占比 cold_w4={base_tail['cold_w4']:.1%}、"
             f"cold_w8={base_tail['cold_w8']:.1%}、hot_w4={base_tail['hot_w4']:.1%}")
    L.append(f"阈值:cold_w0={thr['cold_w0']:.1%};cold_w4={thr['cold_w4']:.1%};"
             f"cold_w8={thr['cold_w8']:.1%};hot 哨兵={thr['hot_w4']:.1%}"
             f"(=0.30×cold_w4 基准,冷热同一标尺)")
    L.append("")
    L.append(f"[{'过' if ok['cold_w4_A'] else '红灯'}] cold_w4 窗口A(epoch0 中段)达标")
    L.append(f"[{'过' if ok['cold_w4_B'] else '红灯'}] cold_w4 窗口B(epoch1 切换后)回升达标")
    L.append(f"[{'过' if ok['cold_w0_A'] else '红灯'}] cold_w0 窗口A(median 口径)")
    L.append(f"[{'过' if ok['cold_w0_B'] else '红灯'}] cold_w0 窗口B(median 口径)")
    L.append(f"[{'过' if ok['hot_sentinel'] else '红灯'}] hot_w4 装置判别力哨兵"
             f"(四窗口全部 < 阈值)")
    L.append("")
    if verdict:
        L.append("判定:绿灯 —— 全程 data stall 真实存在:epoch0 中段与 epoch1 切换后")
        L.append("stall 均回升到冷态基准的 30% 以上,且 hot 对照无此信号。")
    elif not ok["hot_sentinel"]:
        L.append("判定:红灯(装置污染)—— hot_w4 出现 ≥ 阈值的 stall 信号,装置")
        L.append("对冷/热失去判别力,本轮冷组结论不可用;需排查环境(宿主负载、tc 状态)")
        L.append("后重跑四组。")
    else:
        L.append("判定:红灯 —— 叙事收缩分支:'全程 data stall 真实存在'的强主张不成立。")
        L.append("按实测收缩为:仅在达标窗口观察到回升(未达标窗口:"
                 + ",".join(f"{t}-{w}" for t in ("cold_w0", "cold_w4")
                            for w in "AB"
                            if not groups[t]["windows"][w]["pass_ge_thr"])
                 + ");结论照实报告,不作装置外解释。")
    L.append("")
    L.append("per_step 留档:")
    for tag in ("cold_w0", "cold_w4", "cold_w8", "hot_w4"):
        L.append(f"  {groups[tag]['path']}")
    report = "\n".join(L)
    print(report)

    out = {
        "verdict": "PASS" if verdict else "FAIL",
        "green_ratio": RATIO, "tail_wait_s": TAIL, "switch_window": SWITCH,
        "ok": ok,
        "baselines": {"cold_w0_median_stall": base_w0_med,
                      "tail_frac": base_tail},
        "thresholds": thr,
        "groups": groups,
        "report": report,
    }
    out_path = args.probe_c_dir / "verdict.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    print(f"\n已写 verdict.json → {out_path}")
    sys.exit(0 if verdict else 1)


if __name__ == "__main__":
    main()
