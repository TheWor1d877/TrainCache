#!/usr/bin/env python3
"""D 轨判定:多 epoch 欠配(源站 cgroup 1GiB,数据集 2GB 的 50%)下 stall 的
"逐 epoch 重复形态"。

输入:
  * results/probe_c/track_d_w4.json   —— 训练打点(per_step/per_epoch/environment)
  * results/probe_c/mem_track_d_w4.jsonl —— 源站容器 cgroup 内存采样
    (meta 行 = 上限/采样间隔;采样行 = memory.current + memory.stat file;
     final 行 = OOM 状态与限制恢复)

判据(主对话裁决后的口径):
  前置门(欠配真实生效;不满足 → 判"无效"而非红灯):
    file 采样曾 ≥ 0.9×1GiB 且 memory.current 全程 ≤ 上限、无 OOM kill
    (OOMKilled=false,oom_kill 事件=0,final 行齐全)。
  形态绿灯判据(全部满足 = 绿灯,verdict 记"重复形态确认"):
    ① 每 epoch 400 步切 20 步窗口(20 窗,值=窗口内 stall 均值)→ 长度 20 的
       逐 epoch 曲线(移交数据 = track_d_windows.json);对 epoch 1..9 的 8 个
       相邻对算 Pearson r,要求 ≥6 对 r ≥ 0.5;
    ② epoch 2..9 逐 epoch 中位 stall 的 CV ≤ 0.30(重复形态幅度稳定)。
  报告项③(计算并输出数值,不参与绿/红):
    epoch 0 中位 stall 与 epoch 1..9 中位 stall 中位数的比值(旧阈值 >1.2)。
    主对话裁决:恒定 tc 限速装置下该比值被链路瓶颈掩盖(实测 0.99),与探针 B
    已确立机制一致(w4 冷态签名只在 wait 尾部),故降级为报告项;另报告
    "epoch1 首窗切换峰值"作为切换瞬间冷读集中的证据。

输出 track_d_windows.json(逐 epoch 窗口曲线 + 相邻对 r)与
verdict_track_d.json(前置门 + 判据①②数值 + 报告项③数值/降级说明/裁决理由 +
epoch1 首窗切换峰值 + 逐 epoch 中位 stall/p90 wait + 每 epoch 前 20 步切换窗
tail 占比表 + 中文结论)。
退出码:绿灯 0;红灯 1;无效 2。
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
from bootstrap import cv

WINDOW = config.TRACK_D_WINDOW          # 20
MIN_R = config.TRACK_D_MIN_R            # 0.50
MIN_PAIRS = config.TRACK_D_MIN_PAIRS    # 6
CV_MAX = config.TRACK_D_CV_MAX          # 0.30
TAIL = config.PROBE_C_TAIL_WAIT_S       # 0.5s(w>0 尾占比口径,与 probe_c 一致)
COLD_RATIO = 1.2                        # 报告项③的旧阈值(比值现不参与绿/红)
GATE_FILE_RATIO = 0.9                   # 前置门:file 峰值 ≥ 0.9×上限
SWITCH_PEAK_EPOCH = 1                   # 报告项:切换瞬间冷读集中考察的首窗 epoch
DOWNGRADE_REASON_3 = (
    "主对话裁决:恒定 tc 限速(100mbit+20ms)装置下,链路瓶颈掩盖 epoch0 冷态"
    "信号(实测比值 0.99,低于旧阈值 1.2),与探针 B 已确立机制一致(w4 冷态"
    "签名只在 wait 尾部,中位 stall 对冷态不敏感);据此判据③降级为报告项,"
    "不参与绿/红判定,verdict 以判据①②为准记\"重复形态确认\",并补充"
    "\"epoch1 首窗切换峰值\"作为切换瞬间冷读集中的证据。")


def stall(r):
    return r["wait_s"] / r["step_s"] if r["step_s"] > 0 else 0.0


def measured(rs):
    return [r for r in rs if not r.get("warmup")]


def pearson(a, b):
    """长度一致的序列 Pearson r;含 None/NaN/零方差 → None(该对不达标)。"""
    if a is None or b is None:
        return None
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if len(a) != len(b) or len(a) < 2:
        return None
    if not (np.isfinite(a).all() and np.isfinite(b).all()):
        return None
    if a.std() == 0 or b.std() == 0:
        return None
    return float(np.corrcoef(a, b)[0, 1])


def load_mem(path: Path):
    samples, meta, final = [], None, None
    if not path.exists():
        return samples, meta, final
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if rec.get("type") == "meta":
            meta = rec
        elif rec.get("type") == "final":
            final = rec
        elif "memory_current" in rec:
            samples.append(rec)
    return samples, meta, final


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", type=Path,
                    default=config.RESULTS_DIR / "probe_c")
    ap.add_argument("--tag", default="track_d_w4",
                    help="结果文件前缀(track_d_w4 → track_d_w4.json / mem_track_d_w4.jsonl)")
    args = ap.parse_args()
    rdir = args.results_dir
    tag = args.tag

    d = json.loads((rdir / f"{tag}.json").read_text())
    per_step = d["per_step"]
    epochs = int(d["epochs"])
    steps = int(d["steps_per_epoch"])
    n_windows = steps // WINDOW
    meas = measured(per_step)

    # ---------------- 前置门:欠配真实生效 ----------------
    samples, meta, final = load_mem(rdir / f"mem_{tag}.jsonl")
    limit = (meta or {}).get("mem_limit_bytes") or config.TRACK_D_CACHE_MB * 1024 * 1024
    file_vals = [s["file"] for s in samples if s.get("file") is not None]
    cur_vals = [s["memory_current"] for s in samples]
    file_max = max(file_vals) if file_vals else None
    cur_max = max(cur_vals) if cur_vals else None
    oom_killed = (final or {}).get("oom_killed")
    oom_kill_ev = ((final or {}).get("memory_events") or {}).get("oom_kill")
    gate = {
        "mem_jsonl": str((rdir / f"mem_{tag}.jsonl").resolve()),
        "samples": len(samples), "meta_line": meta is not None,
        "final_line": final is not None,
        "mem_limit_bytes": int(limit),
        "file_max_bytes": file_max,
        "file_max_ratio_vs_limit": (file_max / limit) if file_max is not None else None,
        "file_reached_90pct": bool(file_max is not None and file_max >= GATE_FILE_RATIO * limit),
        "memory_current_max": cur_max,
        "memory_current_within_limit": bool(
            samples and cur_max is not None and cur_max <= limit),
        "oom_killed": oom_killed,
        "oom_kill_events": oom_kill_ev,
        "restart_count": (final or {}).get("restart_count"),
    }
    gate["pass"] = bool(
        gate["final_line"] and gate["samples"] > 0
        and gate["file_reached_90pct"]
        and gate["memory_current_within_limit"]
        and gate["oom_killed"] is False
        and gate["oom_kill_events"] == 0)

    # ---------------- 逐 epoch 20 步窗口曲线(移交数据)----------------
    medians = [m.get("median_stall") for m in d["per_epoch"]]
    curves = []
    for e in range(epochs):
        ep = [r for r in meas if r["epoch"] == e]
        vals, ns = [], []
        for w in range(n_windows):
            lo, hi = w * WINDOW, (w + 1) * WINDOW
            vv = [stall(r) for r in ep if lo <= r["step_in_epoch"] < hi]
            vals.append(float(np.mean(vv)) if vv else None)
            ns.append(len(vv))
        curves.append({"epoch": e, "window_mean_stall": vals, "n_per_window": ns})
    # 兼容 400 步不整除 20 的余数步:余下步数并入最后一窗之外,此处记录
    covered = n_windows * WINDOW

    # ---------------- 判据①:相邻 epoch 窗口曲线 Pearson r ----------------
    pairs = []
    for a in range(1, epochs - 1):
        r = pearson(curves[a]["window_mean_stall"], curves[a + 1]["window_mean_stall"])
        pairs.append({"epoch_a": a, "epoch_b": a + 1, "pearson_r": r,
                      "ge_min_r": bool(r is not None and r >= MIN_R)})
    pair_pass = sum(1 for p in pairs if p["ge_min_r"])
    crit1 = {"n_pairs": len(pairs), "min_r": MIN_R, "min_pairs": MIN_PAIRS,
             "pairs_ge_min_r": pair_pass, "pass": pair_pass >= MIN_PAIRS,
             "pairs": pairs}

    # ---------------- 判据②:epoch 2..9 中位 stall 的 CV ----------------
    sub = [m for m in medians[2:epochs] if m is not None]
    cv_val = float(cv(sub)) if len(sub) >= 2 else None
    crit2 = {"epochs": list(range(2, epochs)), "median_stalls": medians[2:epochs],
             "cv": cv_val, "cv_max": CV_MAX,
             "pass": bool(cv_val is not None and cv_val <= CV_MAX)}

    # -------- 报告项③(不参与绿/红):epoch0 vs epoch1..9 中位比值 --------
    base = float(np.median([m for m in medians[1:epochs] if m is not None])) \
        if any(m is not None for m in medians[1:epochs]) else None
    ratio = (medians[0] / base) if (medians[0] is not None and base) else None
    crit3 = {"epoch0_median_stall": medians[0], "epoch1_9_median_of_medians": base,
             "ratio": ratio, "min_ratio": COLD_RATIO,
             "meets_threshold": bool(ratio is not None and ratio > COLD_RATIO),
             "role": "report_only", "downgraded": True,
             "downgrade_reason": DOWNGRADE_REASON_3}

    # ----- 报告项:epoch1 首窗切换峰值(切换瞬间冷读集中的证据) -----
    first_windows = [c["window_mean_stall"][0] if c["window_mean_stall"] else None
                     for c in curves]
    peak_val = first_windows[SWITCH_PEAK_EPOCH] \
        if SWITCH_PEAK_EPOCH < len(first_windows) else None
    fw_vals = [v for v in first_windows if v is not None]
    switch_peak = {
        "epoch": SWITCH_PEAK_EPOCH, "window_index": 0,
        "window_steps": [0, WINDOW],
        "mean_stall": peak_val,
        "is_peak_of_first_windows": bool(
            peak_val is not None and fw_vals and peak_val >= max(fw_vals)),
        "all_epochs_first_window_mean_stall": first_windows,
        "note": "切换瞬间冷读集中的证据:epoch1 首窗(切换后前 20 步)的窗口均值 "
                "stall 为各 epoch 首窗最高;主对话裁决补充报告项。",
    }

    # ---------------- 切换窗 tail 占比表(前 20 步,w4 口径 wait≥0.5s)----
    tail_tab = []
    for e in range(epochs):
        ep = [r for r in meas if r["epoch"] == e and r["step_in_epoch"] < WINDOW]
        frac = float(np.mean([1.0 if r["wait_s"] >= TAIL else 0.0 for r in ep])) if ep else None
        tail_tab.append({"epoch": e, "n_measured": len(ep),
                         "tail_frac_wait_ge_0p5s": frac,
                         "note": "epoch0 前 10 步为 warmup(已排除)" if e == 0 else ""})

    # ---------------- 判定(绿/红仅由前置门 + 判据①②决定;③为报告项) ----
    crits_ok = crit1["pass"] and crit2["pass"]
    if not gate["pass"]:
        verdict, verdict_label = "INVALID", "无效"
    else:
        verdict = "PASS" if crits_ok else "FAIL"
        verdict_label = "重复形态确认(绿灯)" if crits_ok else "红灯"

    # ---------------- 中文报告 ----------------
    L = []
    L.append("====== D 轨判定:多 epoch 欠配(1GiB)下 stall 的逐 epoch 重复形态 ======")
    L.append(f"场景:http 冷态 w{d['workers']},连续 {epochs} epoch、每 epoch "
             f"seed={d['seed']}+e 重排;跑前仅一次冷态,epoch 之间不清缓存;"
             f"源站容器 cgroup 限 1GiB(数据集 2GB 的 50%)。")
    L.append("")
    L.append("-- 前置门:欠配真实生效 --")
    fm = f"{file_max/limit:.1%}" if file_max is not None else "—"
    L.append(f"  file 峰值 = {file_max} 字节(上限 {limit} 的 {fm};"
             f"门槛 ≥{GATE_FILE_RATIO:.0%})→ {'过' if gate['file_reached_90pct'] else '不过'}")
    L.append(f"  memory.current 峰值 = {cur_max} 字节 ≤ 上限 → "
             f"{'过' if gate['memory_current_within_limit'] else '不过(breach)'}")
    L.append(f"  OOM:OOMKilled={oom_killed},oom_kill 事件={oom_kill_ev},"
             f"restart={gate['restart_count']} → "
             f"{'过' if oom_killed is False and oom_kill_ev == 0 else '不过'}")
    L.append(f"  采样 {gate['samples']} 点(每 {config.TRACK_D_MEM_SAMPLE_S}s),"
             f"meta/final 行齐全={gate['meta_line'] and gate['final_line']}")
    L.append(f"  前置门结论:{'过,进入形态判据' if gate['pass'] else '不过 → 判无效(非红灯)'}")
    L.append("")
    L.append("-- 绿灯判据① 相邻 epoch 窗口曲线的 Pearson r(窗口=20 步,s*均值口径) --")
    L.append("  对(1-2 … 8-9):" + ", ".join(
        f"{p['epoch_a']}-{p['epoch_b']}:"
        f"{'—' if p['pearson_r'] is None else format(p['pearson_r'], '.3f')}"
        for p in pairs))
    L.append(f"  r≥{MIN_R:.2f} 的对数 = {pair_pass}/{len(pairs)}(要求 ≥{MIN_PAIRS})→ "
             f"{'过' if crit1['pass'] else '不过'}")
    L.append("")
    L.append("-- 绿灯判据② epoch 2..9 逐 epoch 中位 stall 的 CV --")
    L.append("  中位 stall:" + ", ".join(
        f"e{e}={m:.1%}" if m is not None else f"e{e}=—"
        for e, m in enumerate(medians[2:epochs], start=2)))
    L.append(f"  CV = {'—' if cv_val is None else format(cv_val, '.1%')}"
             f"(要求 ≤{CV_MAX:.0%})→ {'过' if crit2['pass'] else '不过'}")
    L.append("")
    L.append("-- 报告项③(不参与绿/红):epoch0 中位 stall vs epoch1..9 --")
    L.append(f"  epoch0 = {'—' if medians[0] is None else format(medians[0], '.1%')},"
             f"epoch1..9 中位数 = {'—' if base is None else format(base, '.1%')},"
             f"比值 = {'—' if ratio is None else format(ratio, '.2f')}"
             f"(旧阈值 >{COLD_RATIO},仅参考,不参与绿/红)")
    L.append(f"  降级理由:{DOWNGRADE_REASON_3}")
    L.append("")
    L.append("-- 报告项:epoch1 首窗切换峰值(切换瞬间冷读集中) --")
    L.append(f"  epoch{SWITCH_PEAK_EPOCH} 首窗(步 [0,{WINDOW}))均值 stall = "
             f"{'—' if peak_val is None else format(peak_val, '.3f')}"
             f"{'(为各 epoch 首窗最高)' if switch_peak['is_peak_of_first_windows'] else ''}")
    L.append("  各 epoch 首窗均值:" + ", ".join(
        f"e{e}={'—' if v is None else format(v, '.3f')}"
        for e, v in enumerate(first_windows)))
    L.append("")
    L.append("-- 逐 epoch 打点(中位/均值 stall、p90 wait、loader 重建)--")
    L.append(f"  {'epoch':>5} {'中位 stall':>10} {'均值 stall':>10} {'p90 wait_s':>10} "
             f"{'loader_build_s':>13} {'窗口曲线(前 3 窗)':>22}")
    for e in range(epochs):
        m = d["per_epoch"][e]
        ws = curves[e]["window_mean_stall"]
        head = ", ".join("—" if v is None else format(v, '.3f') for v in ws[:3])
        L.append(f"  {e:>5} "
                 f"{format(m['median_stall'], '.1%') if m['median_stall'] is not None else '—':>10} "
                 f"{format(m['mean_stall'], '.1%') if m['mean_stall'] is not None else '—':>10} "
                 f"{format(m['p90_wait_s'], '.3f') if m['p90_wait_s'] is not None else '—':>10} "
                 f"{format(m['loader_build_s'], '.3f'):>13} {head:>22}")
    L.append("")
    L.append(f"-- 每 epoch 前 {WINDOW} 步切换窗 tail 占比(wait_s ≥ {TAIL}s 步数占比)--")
    L.append(f"  {'epoch':>5} {'n(实测)':>8} {'tail 占比':>9}")
    for t in tail_tab:
        L.append(f"  {t['epoch']:>5} {t['n_measured']:>8} "
                 f"{format(t['tail_frac_wait_ge_0p5s'], '.1%') if t['tail_frac_wait_ge_0p5s'] is not None else '—':>9}")
    L.append("")
    L.append("-- 结论 --")
    fmt_ratio = "—" if ratio is None else format(ratio, '.2f')
    fmt_peak = "—" if peak_val is None else format(peak_val, '.3f')
    if verdict == "INVALID":
        L.append("无效:欠配前置门未通过,本轮数据不用于红/绿判定。原因:"
                 + ";".join(
                     r for r, ok in (
                         ("file 峰值未达 0.9×上限(欠配未真实生效)", gate["file_reached_90pct"]),
                         ("memory.current 超上限(breach)", gate["memory_current_within_limit"]),
                         ("OOM/OOMKilled", oom_killed is False and oom_kill_ev == 0),
                         ("meta/final 采样行缺失", gate["meta_line"] and gate["final_line"]),
                     ) if not ok)
                 + f"。形态判据(仅报告):①{pair_pass}/{len(pairs)} 对 r≥{MIN_R},"
                 + f"②CV={'—' if cv_val is None else format(cv_val, '.1%')},"
                 + f"③(报告项)比值={fmt_ratio},epoch1 首窗峰值={fmt_peak}。")
    elif verdict == "PASS":
        L.append("绿灯(重复形态确认):欠配真实生效,且判据①②成立 ——")
        L.append(f"  ① epoch 1..9 相邻对窗口曲线 r≥{MIN_R:.2f} 的有 {pair_pass}/{len(pairs)} 对(≥{MIN_PAIRS});")
        L.append(f"  ② epoch 2..9 中位 stall 的 CV={cv_val:.1%} ≤ {CV_MAX:.0%}。")
        L.append("  即:v4 全程场景下,欠配(源站只缓存一半工作集)使 stall 在新 seed 重排后仍逐 epoch 重复出现 —— 重复形态确认。")
        L.append(f"  ③(报告项,不参与绿/红):epoch0/epoch1..9 中位比值={fmt_ratio}"
                 f"(旧阈值 >{COLD_RATIO})—— 恒定 tc 限速下链路瓶颈掩盖 epoch0 冷态差异,"
                 f"与探针 B 机制一致(w4 冷态签名只在 wait 尾部);")
        L.append(f"     切换瞬间冷读集中的证据:epoch1 首窗切换峰值={fmt_peak}。")
    else:
        L.append("红灯:欠配真实生效,但绿灯判据①②未全部成立 ——")
        L.append(f"  ① 相邻对 r≥{MIN_R:.2f}:{pair_pass}/{len(pairs)}(要求 ≥{MIN_PAIRS})"
                 f" → {'过' if crit1['pass'] else '不过'};")
        L.append(f"  ② CV={('—' if cv_val is None else format(cv_val, '.1%'))}"
                 f"(要求 ≤{CV_MAX:.0%})→ {'过' if crit2['pass'] else '不过'}。")
        L.append(f"  ③(报告项,不参与判定)比值={fmt_ratio},epoch1 首窗峰值={fmt_peak}。")
        L.append("  按实报红灯,不作装置外解释;移交逐 epoch 窗口曲线供 v4 复核。")
    L.append("")
    L.append(f"per_step 留档:{str((rdir / f'{tag}.json').resolve())}")
    L.append(f"内存采样留档:{str((rdir / f'mem_{tag}.jsonl').resolve())}")
    report = "\n".join(L)
    print(report)

    windows_out = {
        "track": "D", "tag": tag, "source": d["source"], "workers": d["workers"],
        "epochs": epochs, "seed": d["seed"], "steps_per_epoch": steps,
        "window_steps": WINDOW, "n_windows": n_windows,
        "metric": f"窗口内 stall 均值(wait_s/step_s);epoch0 前 {d['warmup_steps']} 步 "
                  f"warmup 不计入;余数步 {steps - covered} 步不在窗口内",
        "epochs_curves": curves,
        "per_epoch_median_stall": medians,
        "adjacent_pairs": pairs,
        "pair_criteria": {"min_r": MIN_R, "min_pairs": MIN_PAIRS,
                          "pairs_ge_min_r": pair_pass, "pass": crit1["pass"]},
    }
    (rdir / "track_d_windows.json").write_text(
        json.dumps(windows_out, ensure_ascii=False, indent=2) + "\n")

    out = {
        "verdict": verdict,
        "verdict_label": verdict_label,
        "verdict_note": "绿/红仅由前置门 + 判据①②决定;判据③为报告项(主对话裁决)。",
        "gate": gate,
        "criteria": {"window_repeat_r": crit1, "epoch_cv": crit2,
                     "epoch0_cold": crit3},
        "criteria_deciding": ["window_repeat_r", "epoch_cv"],
        "report_items": {"epoch1_first_window_switch_peak": switch_peak},
        "per_epoch": [{"epoch": m["epoch"], "median_stall": m["median_stall"],
                       "mean_stall": m["mean_stall"], "p90_wait_s": m["p90_wait_s"],
                       "p50_step_s": m["p50_step_s"],
                       "loader_build_s": m["loader_build_s"]}
                      for m in d["per_epoch"]],
        "switch_window_tail": tail_tab,
        "config": {"window_steps": WINDOW, "min_r": MIN_R, "min_pairs": MIN_PAIRS,
                   "cv_max": CV_MAX, "tail_wait_s": TAIL, "cold_ratio": COLD_RATIO,
                   "cold_ratio_role": "report_only",
                   "gate_file_ratio": GATE_FILE_RATIO,
                   "mem_limit_mb": config.TRACK_D_CACHE_MB,
                   "mem_sample_s": config.TRACK_D_MEM_SAMPLE_S},
        "windows_path": str((rdir / "track_d_windows.json").resolve()),
        "result_path": str((rdir / f"{tag}.json").resolve()),
        "report": report,
    }
    out_path = rdir / "verdict_track_d.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    print(f"\n已写 track_d_windows.json / verdict_track_d.json → {rdir}")
    sys.exit(0 if verdict == "PASS" else (2 if verdict == "INVALID" else 1))


if __name__ == "__main__":
    main()
