#!/usr/bin/env python3
"""Q9 阶段切换「回落稳态」判据的生产者(docs/10.1 §14 Q9,2026-10-22 已裁决)。

判据(照 Q9 原文逐条实现,口径不做任何改动):
  * 基线 = 切换前 20 步 stall 均值(warmup 步不计);
  * 阈值 = max(1.1×基线, 基线 + 5 个百分点);
  * 切换后 10 步滚动均值首次 ≤ 阈值、且之后连续 10 步(滚动均值序列的随后
    10 个窗口值)保持 ≤ 阈值 → 记为回落点;
    recovery_time_s = 切换时刻到回落点的时间(用切换后逐实测步 step_s 累加;
    per_step jsonl 无绝对时间戳,步时长累加即口径内的经过时间);
  * 到 epoch 末仍未满足 → 右删失(censored=True),单独统计删失率
    (删失率 = 删失数 / (回落数 + 删失数),基线不足事件不进分母);
  * 切换前不足 20 步实测 → status=insufficient_baseline,不进图三。

输入契约(D 轨 run 目录,由 bench/scripts/bench_track_d.py 写入):
  <run_dir>/per_step_*.jsonl  每步一行:epoch / step_in_epoch / global_step /
      warmup / wait_s / step_s / shard / shards / phase(steady|switch)/ stage
      (stall = wait_s/step_s;startup 相位只存在于 run.json,没有步记录)
  <run_dir>/run.json          switch_events[]:kind / from_epoch / to_epoch /
      at(UTC 墙钟)/ at_step(records 下标)/ ready_* / censored / baseline_*;
      args.shuffle / args.epochs / run_id(优先读此)
  <run_dir>/epochs_meta.json  兜底:per_epoch[i]["switch"](同结构,老 M1
      打点若未写 run.json 则从这里取切换事件)
  历史旧格式打点(行缺 phase、事件缺 at_step)会明确报错,不做静默降级。

输出:默认 <run_dir>/recovery_q9.json(--out 可改);plot.py 图三消费其中的
recovery_time_s(recovered 事件)与删失率。

用法:
  python3 bench/experiments/recovery.py RUN_DIR [RUN_DIR...] [--out FILE]
  python3 bench/experiments/recovery.py --selftest
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

BASELINE_STEPS = 20      # 切换前基线步数(Q9)
ROLL_WINDOW = 10         # 切换后滚动均值窗口步数(Q9)
HOLD_STEPS = 10          # 首次 ≤ 阈值后需保持的步数(Q9)
REL_FACTOR = 1.1         # 阈值系数(Q9:max(1.1×基线, …))
ABS_MARGIN = 0.05        # 基线 + 5 个百分点(stall 为 wait_s/step_s 比例,
                         # 5pp = 0.05;Q9:max(…, 基线+5pp))


class RecoveryInputError(SystemExit):
    """输入产物不满足契约:报错退出,不做静默降级。"""


def stall(rec):
    return rec["wait_s"] / rec["step_s"] if rec["step_s"] > 0 else 0.0


# ---------------------------------------------------------------------------
# 判据核心
# ---------------------------------------------------------------------------
def threshold(baseline):
    return max(REL_FACTOR * baseline, baseline + ABS_MARGIN)


def analyze_switch(pre, post):
    """对单个切换事件跑 Q9 判据。

    pre:  切换前实测步列表(warmup 已剔除;取末尾 BASELINE_STEPS 做基线);
    post: 切换后、本 epoch 内的实测步列表(同 run 记录字典)。
    返回事件结果 dict(status: recovered | censored | insufficient_baseline)。
    """
    base_vals = [stall(r) for r in pre[-BASELINE_STEPS:]]
    out = {
        "baseline_n": len(base_vals),
        "baseline_stall": None,
        "threshold": None,
        "status": None,
        "censored": False,
        "recovery_step": None,      # 回落点在 post 中的下标(窗口末步)
        "recovery_global_step": None,
        "recovery_time_s": None,
        "n_post_steps": len(post),
    }
    if len(base_vals) < BASELINE_STEPS:      # 切换前不足 20 步 → 不进图三
        out["status"] = "insufficient_baseline"
        return out
    baseline = sum(base_vals) / len(base_vals)
    thr = threshold(baseline)
    out["baseline_stall"] = baseline
    out["threshold"] = thr

    seq = [stall(r) for r in post]
    n = len(seq)
    if n < ROLL_WINDOW + HOLD_STEPS + 1:     # 凑不满「首窗口 + 随后 10 步保持」
        out["status"] = "censored"           # → 到 epoch 末必右删失
        out["censored"] = True
        return out
    roll = [sum(seq[i:i + ROLL_WINDOW]) / ROLL_WINDOW for i in range(n - ROLL_WINDOW + 1)]
    for i, m in enumerate(roll):
        if m > thr:
            continue
        # 之后连续 HOLD_STEPS 步(滚动均值序列)须保持 ≤ 阈值且窗口可取满
        nxt = roll[i + 1:i + 1 + HOLD_STEPS]
        if len(nxt) < HOLD_STEPS or any(v > thr for v in nxt):
            continue
        k = i + ROLL_WINDOW - 1              # 回落点 = 首个满足窗口的末步
        out.update({
            "status": "recovered",
            "recovery_step": k,
            "recovery_global_step": post[k]["global_step"],
            # 切换时刻 → 回落点:切换后逐实测步 step_s 累加
            "recovery_time_s": round(sum(r["step_s"] for r in post[:k + 1]), 6),
        })
        return out
    out["status"] = "censored"               # 到 epoch 末仍未满足 → 右删失
    out["censored"] = True
    return out


def summarize(events):
    """删失率单独统计:分母只含可判定的事件(回落 + 删失),基线不足不进分母。"""
    rec = sum(1 for e in events if e["status"] == "recovered")
    cen = sum(1 for e in events if e["status"] == "censored")
    ins = sum(1 for e in events if e["status"] == "insufficient_baseline")
    denom = rec + cen
    return {
        "n_events": len(events),
        "recovered": rec,
        "censored": cen,
        "insufficient_baseline": ins,
        "censoring_rate": (cen / denom) if denom else None,
    }


# ---------------------------------------------------------------------------
# run 目录装载(输入契约校验)
# ---------------------------------------------------------------------------
def load_records(run_dir):
    files = sorted(Path(run_dir).glob("per_step_*.jsonl"))
    if len(files) != 1:
        raise RecoveryInputError(
            f"{run_dir}: per_step_*.jsonl 数量 = {len(files)}(应恰好 1)")
    recs = [json.loads(l) for l in files[0].read_text().splitlines() if l.strip()]
    if not recs:
        raise RecoveryInputError(f"{run_dir}: {files[0].name} 为空")
    need = {"epoch", "step_in_epoch", "global_step", "warmup", "wait_s", "step_s"}
    miss = [r for r in recs if not need.issubset(r)]
    if miss:
        raise RecoveryInputError(
            f"{run_dir}: {files[0].name} 有 {len(miss)} 行缺字段 {sorted(need)}")
    if not any("phase" in r for r in recs):
        raise RecoveryInputError(
            f"{run_dir}: per_step 打点缺 phase 标签(startup/steady/switch)。"
            "这是旧格式产物,请用含 M1 打点的 bench_track_d.py 重跑 D 轨后再执行 recovery。")
    return recs


def load_switch_events(run_dir):
    """优先 run.json.switch_events;缺 run.json 时兜底 epochs_meta.json 的
    per_epoch[i].switch。返回 (events, meta)。"""
    rd = Path(run_dir)
    meta = {}
    events = None
    run_json = rd / "run.json"
    if run_json.is_file():
        run = json.loads(run_json.read_text())
        events = run.get("switch_events") or []
        args = run.get("args") or {}
        meta = {"run_id": run.get("run_id"), "shuffle": args.get("shuffle"),
                "epochs": args.get("epochs"), "seed": args.get("seed")}
    else:
        meta_json = rd / "epochs_meta.json"
        if not meta_json.is_file():
            raise RecoveryInputError(
                f"{run_dir}: 缺 run.json 与 epochs_meta.json,无切换事件来源")
        mm = json.loads(meta_json.read_text())
        per_epoch = mm.get("per_epoch", mm) if isinstance(mm, dict) else mm
        if isinstance(per_epoch, dict):
            per_epoch = per_epoch.get("per_epoch", [])
        events = [ep["switch"] for ep in per_epoch
                  if isinstance(ep, dict) and ep.get("switch")]
        meta = {"run_id": None, "shuffle": (mm.get("shuffle") if isinstance(mm, dict) else None),
                "epochs": mm.get("epochs") if isinstance(mm, dict) else None,
                "seed": mm.get("seed") if isinstance(mm, dict) else None}
    for e in events:
        if "at_step" not in e or "to_epoch" not in e:
            raise RecoveryInputError(
                f"{run_dir}: switch 事件缺 at_step/to_epoch(旧格式打点),"
                "请用含 M1 打点的 bench_track_d.py 重跑后执行 recovery")
    return events, meta


def analyze_run(run_dir):
    records = load_records(run_dir)
    events, meta = load_switch_events(run_dir)
    results = []
    for ev in events:
        at_step = ev["at_step"]
        to_epoch = ev["to_epoch"]
        pre = [r for r in records if not r["warmup"] and r["global_step"] < at_step]
        post = [r for r in records if not r["warmup"]
                and r["global_step"] >= at_step and r["epoch"] == to_epoch]
        r = analyze_switch(pre, post)
        r.update({"kind": ev.get("kind"), "from_epoch": ev.get("from_epoch"),
                  "to_epoch": to_epoch, "at": ev.get("at")})
        results.append(r)
    return {
        "schema": "q9-recovery-1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_dir": str(run_dir),
        "meta": meta,
        "params": {"baseline_steps": BASELINE_STEPS, "roll_window": ROLL_WINDOW,
                   "hold_steps": HOLD_STEPS, "rel_factor": REL_FACTOR,
                   "abs_margin_pp": 5.0,
                   "threshold_rule": "max(1.1×基线, 基线+5pp)",
                   "recovery_time_s": "切换时刻→回落点,按切换后实测步 step_s 累加"},
        "events": results,
        "summary": summarize(results),
    }


# ---------------------------------------------------------------------------
# 自检(判据函数级 + 装载级;不写任何产物到数据层)
# ---------------------------------------------------------------------------
def _mk(epoch, k, g, wait, step, warmup=False, phase="steady"):
    return {"epoch": epoch, "step_in_epoch": k, "global_step": g,
            "warmup": warmup, "wait_s": wait, "step_s": step, "phase": phase}


def _seq_records(vals):
    """vals = [(wait_s, step_s)],global_step 从 0 起。"""
    return [_mk(1, i, i, w, s, phase=("switch" if i < 10 else "steady"))
            for i, (w, s) in enumerate(vals)]


def _pre_post(good=0.10, bad=0.90, n_pre=BASELINE_STEPS, post=None):
    pre = _seq_records([(good, 1.0)] * n_pre)  # 基线 stall=0.10, thr=0.15
    seq = post if post is not None else []
    post_recs = [_mk(2, i, n_pre + i, w, s) for i, (w, s) in enumerate(seq)]
    return pre, post_recs


def selftest():
    fails = []

    def check(name, cond, detail=""):
        tag = "PASS" if cond else "FAIL"
        print(f"  [{tag}] {name}" + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            fails.append(name)

    print("recovery.py --selftest(Q9 判据函数级自测)")
    # ① 正常回落:切换后 15 步劣化(0.9),随后转好(0.02)且保持;
    #    thr=max(1.1×0.1, 0.1+0.05)=0.15。首个 ≤ 阈值的 10 步滚动窗口起点为
    #    post 下标 14(含 1 个劣化步,(0.9+9×0.02)/10=0.108≤0.15),其后
    #    连续 10 个窗口值(起点 15~24)均为 0.02;回落点 = 该窗口末步下标 23,
    #    step_s=1.0 → recovery_time_s=24.0。
    post = [(0.9, 1.0)] * 15 + [(0.02, 1.0)] * 25
    pre, post_recs = _pre_post(post=post)
    r = analyze_switch(pre, post_recs)
    check("正常回落:status=recovered", r["status"] == "recovered", str(r))
    check("正常回落:基线=0.10、阈值=0.15",
          abs(r["baseline_stall"] - 0.10) < 1e-9 and abs(r["threshold"] - 0.15) < 1e-9)
    check("正常回落:回落点=post 下标 23(首个满足窗口末步)",
          r["recovery_step"] == 23, f"got {r['recovery_step']}")
    check("正常回落:recovery_time_s=24.0(切换后 step_s 累加)",
          r["recovery_time_s"] == 24.0, f"got {r['recovery_time_s']}")

    # ② 右删失:切换后整段 epoch 都 > 阈值 → censored。
    post = [(0.9, 1.0)] * 30
    pre, post_recs = _pre_post(post=post)
    r = analyze_switch(pre, post_recs)
    check("右删失:全程劣化 → censored", r["status"] == "censored" and r["censored"])

    # ②b 短暂回落又抬头(滚动均值随后 10 步未保持)→ 仍判右删失。
    post = [(0.9, 1.0)] * 15 + [(0.02, 1.0)] * 10 + [(1.0, 1.0)] * 15
    pre, post_recs = _pre_post(post=post)
    r = analyze_switch(pre, post_recs)
    check("短暂回落又抬头:未保持 10 步 → censored", r["status"] == "censored")

    # ③ 基线不足:切换前只有 5 步实测 → insufficient_baseline,不进图三。
    pre, post_recs = _pre_post(n_pre=5, post=[(0.9, 1.0)] * 15)
    r = analyze_switch(pre, post_recs)
    check("基线不足(前 5 步):insufficient_baseline",
          r["status"] == "insufficient_baseline" and r["recovery_time_s"] is None)

    # ③b 基线不足事件不进删失率分母。
    s = summarize([{"status": "recovered"}, {"status": "censored"},
                   {"status": "insufficient_baseline"}])
    check("删失率=删失/(回落+删失)=0.5,基线不足不进分母",
          s["censoring_rate"] == 0.5 and s["insufficient_baseline"] == 1)

    # ④ 装载级:临时目录走完整 run 目录契约(jsonl + run.json.switch_events)。
    import tempfile
    with tempfile.TemporaryDirectory(prefix="recovery_selftest_") as td:
        recs = _seq_records([(0.9, 1.0)] * 15 + [(0.02, 1.0)] * 25)
        for i, rec in enumerate(recs):               # 拼上切换前的 20 步基线
            rec["global_step"] = i + BASELINE_STEPS
            rec["epoch"] = 1
        pre_recs = _pre_post()[0]
        for rec in pre_recs:
            rec["epoch"] = 0
        all_recs = pre_recs + recs
        jp = Path(td) / "per_step_1_w1.jsonl"
        jp.write_text("\n".join(json.dumps(r) for r in all_recs) + "\n")
        ev = {"kind": "epoch", "from_epoch": 0, "to_epoch": 1,
              "at": "2026-10-22T00:00:00Z", "at_step": BASELINE_STEPS}
        (Path(td) / "run.json").write_text(json.dumps(
            {"run_id": "selftest", "args": {"shuffle": "per-epoch", "epochs": 2},
             "switch_events": [ev]}))
        out = analyze_run(td)
        e0 = out["events"][0]
        check("装载 run 目录契约并复用判据", e0["status"] == "recovered"
              and e0["recovery_time_s"] == 24.0
              and out["meta"]["shuffle"] == "per-epoch", str(out["events"]))

    # ⑤ 旧格式产物(缺 phase)须报错而非静默:用 __future__ 检查失败即可,
    #    这里直接验 load_records 对无 phase 行抛错。
    with tempfile.TemporaryDirectory(prefix="recovery_selftest_") as td:
        old = [_mk(0, i, i, 0.5, 1.0) for i in range(5)]
        for r_ in old:
            r_.pop("phase")
        (Path(td) / "per_step_x_w1.jsonl").write_text(
            "\n".join(json.dumps(r) for r in old) + "\n")
        try:
            load_records(td)
            check("旧格式(缺 phase)报错", False, "未抛 RecoveryInputError")
        except RecoveryInputError:
            check("旧格式(缺 phase)报错", True)

    if fails:
        print(f"SELFTEST FAIL:{len(fails)} 项:{fails}")
        return 1
    print("SELFTEST PASS:正常回落 / 右删失(含未保持) / 基线不足 / "
          "删失率口径 / run 目录装载 / 旧格式报错,全部通过")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Q9 阶段切换回落稳态判据(生产者)")
    ap.add_argument("run_dirs", nargs="*", type=Path,
                    help="D 轨 run 目录(含 per_step_*.jsonl 与 run.json/epochs_meta.json)")
    ap.add_argument("--out", type=Path, default=None,
                    help="聚合输出文件(默认每个 run 目录写 recovery_q9.json)")
    ap.add_argument("--selftest", action="store_true",
                    help="判据函数级自测(不读写任何 run 目录)")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(selftest())
    if not args.run_dirs:
        ap.error("需要至少一个 run 目录,或使用 --selftest")

    results = []
    for rd in args.run_dirs:
        out = analyze_run(rd)
        results.append(out)
        s = out["summary"]
        rate = "—" if s["censoring_rate"] is None else f"{s['censoring_rate']:.1%}"
        print(f"[{rd}] 事件 {s['n_events']}:回落 {s['recovered']} / "
              f"删失 {s['censored']} / 基线不足 {s['insufficient_baseline']}"
              f"(不进图三);删失率 {rate}")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
        print(f"已写 {args.out}")
    else:
        for rd, res in zip(args.run_dirs, results):
            p = rd / "recovery_q9.json"
            p.write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n")
            print(f"已写 {p}")


if __name__ == "__main__":
    main()
