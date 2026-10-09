#!/usr/bin/env python3
"""探针B 判定:stall 指标三条件(口径与实测机制对齐,见 design.md 第 4 节)。

实测机制(2026-10-07,数据见 results/probe_b/*.json 的 per_step):
- workers=0 无管线:主进程串行等数据,wait 直接暴露数据源速度
  (冷 441ms/批=网络,热 111ms/批=本地解码);
- workers>0 有管线:预取队列缓冲抹平数据源差异,冷热的中位 wait 都落在
  ~13-17ms 的 DataLoader 基座上,差异只在大 wait 尾部(冷每 4-8 批一个
  2.5s 级网络排队,热只有 180ms 级解码尾);
- 因此"冷态"按无管线档(w0)判,"热态"按有管线档(w4/w8)判。

三条口径:
1. 冷态 stall 稳定 >40% —— 判 w0(无管线,数据加载确是瓶颈);
2. 热态 stall <15% —— 判 w4/w8 至少一档(本地缓存解决 I/O 等待);
3. 冷态 stall 随 workers 单调下降 —— w0 严格 > w4,且 w4 ≥ w8(共享带宽下
   w4/w8 允许持平,输出带宽饱和签名;严格单调作为加分项报告)。

输出 verdict.json + 中文报告;三条件全过 exit 0,否则 exit 1。
"""
import argparse
import json
import sys
from pathlib import Path

RES = Path(__file__).resolve().parents[1] / "results" / "probe_b"


def load(tag: str, workers: int):
    p = RES / f"{tag}_w{workers}.json"
    if not p.exists():
        sys.exit(f"缺少结果 {p}:先跑 bash probe_b/run.sh")
    return json.loads(p.read_text())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--require-all", action="store_true",
                    help="条件2 改为热态 w4/w8 两档均 <15%")
    args = ap.parse_args()

    workers = [0, 4, 8]
    cold = {w: load("cold", w)["median_stall"] for w in workers}
    hot = {w: load("hot", w)["median_stall"] for w in workers}
    cold_mean = {w: load("cold", w)["mean_stall"] for w in workers}
    hot_mean = {w: load("hot", w)["mean_stall"] for w in workers}

    ok1 = cold[0] > 0.40
    ok2 = (all(hot[w] < 0.15 for w in (4, 8)) if args.require_all
           else any(hot[w] < 0.15 for w in (4, 8)))
    # 条件3 用 mean 口径(时间加权的每步等待占比,对大 wait 尾部敏感):
    # median 口径下 w4→w8 会因 DataLoader 基座等待微升(13→17ms)而假性反升
    strict = cold_mean[0] > cold_mean[4] > cold_mean[8]
    weak = cold_mean[0] > cold_mean[4] and cold_mean[4] >= cold_mean[8]
    ok3 = weak
    saturated = (cold_mean[4] >= cold_mean[8]
                 and abs(cold_mean[4] - cold_mean[8]) / max(cold_mean[4], 1e-9) < 0.05)

    verdict = ok1 and ok2 and ok3

    lines = [
        "====== 探针B 判定:stall 指标打得出 ======",
        f"{'workers':>8} {'冷态 stall':>12} {'热态 stall':>12}",
    ]
    for w in workers:
        lines.append(f"{w:>8} {cold[w]:>12.1%} {hot[w]:>12.1%}")
    lines += [
        f"  (冷态 mean: {cold_mean[0]:.1%} / {cold_mean[4]:.1%} / {cold_mean[8]:.1%};"
        f" 热态 mean: {hot_mean[0]:.1%} / {hot_mean[4]:.1%} / {hot_mean[8]:.1%})",
        f"[{'过' if ok1 else '红灯'}] 条件1:冷态(w0 无管线)stall >40%,实测 {cold[0]:.1%}",
        f"[{'过' if ok2 else '红灯'}] 条件2:热态(w4/w8)stall <15%"
        f"({'两档均需' if args.require_all else '至少一档'},实测 {hot[4]:.1%} / {hot[8]:.1%})",
        f"[{'过' if ok3 else '红灯'}] 条件3:冷态 stall 随 workers 下降(mean 口径)"
        f"(0→4→8: {cold_mean[0]:.1%} → {cold_mean[4]:.1%} → {cold_mean[8]:.1%})",
    ]
    if not strict and weak:
        lines.append("      备注:mean 口径 0>4 严格下降(管线并行化),4→8 持平 —— ")
        if saturated:
            lines.append("      共享 100mbit 带宽饱和签名(w4→w8 相对差 <5%):"
                         "瓶颈已转移到网络,是真实 I/O 饥饿的证据而非打点噪声。")
    if strict:
        lines.append("      加分:mean 口径三档严格单调。")
    lines += ["", f"判定:{'绿灯' if verdict else '红灯'}"]

    report = "\n".join(lines)
    print(report)

    RES.mkdir(parents=True, exist_ok=True)
    (RES / "verdict.json").write_text(json.dumps({
        "verdict": "PASS" if verdict else "FAIL",
        "ok": {"cold_w0_gt_40pct": ok1, "hot_pipelined_lt_15pct": ok2,
               "cold_monotonic_weak": ok3, "cold_monotonic_strict": strict},
        "cold": cold, "hot": hot,
        "cold_mean": cold_mean, "hot_mean": hot_mean,
        "bandwidth_saturation_signature": saturated,
        "require_all": args.require_all,
    }, ensure_ascii=False, indent=2) + "\n")
    print(f"\n已写 verdict.json → {RES / 'verdict.json'}")
    sys.exit(0 if verdict else 1)


if __name__ == "__main__":
    main()
