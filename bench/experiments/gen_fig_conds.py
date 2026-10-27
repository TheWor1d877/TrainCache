#!/usr/bin/env python3
"""生成五张图与 Pareto 的 COND 清单。

图一/图三/Pareto 一与图二共用对照层 Pod + MinIO 装置（协议 §1.8/§C3），
所以每个 COND 就是一组 (figure, 自变量档位, 组态)。执行器
`run_figures.py` 按 COND 起一条 run_bench 链路。

命名:
  fig1-<rate>-ratio<R>-fan<F>-<gated|free>
  fig2-<rate>-size<S>-ep<E>-<traincache-cold|fluid-cold>
  fig3-<rate>-<shuffle>-ep<E>-<traincache-cold|fluid-cold>
  pareto1-fan<F>-<gated|free>

轨迹按 (n_tars, shuffle, epochs) 现场生成，sha256 写进 COND；run_bench.py
用同一组参数复算，两边必须一致。

用法:
  python3 bench/experiments/gen_fig_conds.py            # 缺什么补什么
  python3 bench/experiments/gen_fig_conds.py --force    # 全部重写
"""
import argparse
import json
import sys
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent
BENCH_ROOT = EXPERIMENTS_DIR.parent
sys.path.insert(0, str(BENCH_ROOT / "compare"))
import trace as trace_mod  # noqa: E402

CONDS_DIR = EXPERIMENTS_DIR / "conds"
TRACES_DIR = EXPERIMENTS_DIR / "traces"
SEED = 20261015
BATCH = 32
STEPS_PER_EPOCH = 400
IMAGES_PER_TAR = 1600
RATES = (100, 300, 1000)
RATIOS = (1, 2, 4)
FANOUTS = (1, 2, 3)


def _template() -> dict:
    """用同目录现有 COND 复制 environment/dataset 端点,避免各文件漂移。"""
    for name in ("compare-b-m-traincache-cold-1000.json",
                 "pareto2-lookahead8-heat.json"):
        p = CONDS_DIR / name
        if p.is_file():
            return json.loads(p.read_text())
    raise SystemExit("找不到可复制的 COND 模板")


def _trace(n_tars: int, shuffle: str, epochs: int) -> tuple[str, str]:
    """生成(或复用)轨迹文件,返回 (相对路径, sha256)。"""
    TRACES_DIR.mkdir(parents=True, exist_ok=True)
    name = f"track-s{n_tars}-{shuffle}-ep{epochs}.json"
    path = TRACES_DIR / name
    trace = trace_mod.build_trace(
        n_tars=n_tars, seed=SEED, shuffle=shuffle, epochs=epochs,
        batch=BATCH, steps_per_epoch=STEPS_PER_EPOCH,
        images_per_tar=IMAGES_PER_TAR)
    want = trace_mod.trace_sha256(trace)
    if not path.is_file() or trace_mod.trace_sha256(
            json.loads(path.read_text())) != want:
        path.write_text(json.dumps(trace, ensure_ascii=False, indent=2) + "\n")
    return str(path.relative_to(BENCH_ROOT)), want


def _cond(tpl: dict, *, name, figure, group, n_tars, ratio, rate, epochs,
          shuffle, fanout, gate_mode, evict="heat", reps=3) -> dict:
    dataset_bytes = n_tars * 256 * (1 << 20)
    cache_bytes = dataset_bytes // ratio
    trace_file, trace_sha = _trace(n_tars, shuffle, epochs)
    c = json.loads(json.dumps(tpl))
    c["schema_version"] = "1"
    c["name"] = name
    c["figure"] = figure
    c["group"] = group
    c["track"] = "B"
    c["dataset"].update({
        "name": "compare-data-8m",
        "source": "minio",
        "bytes": dataset_bytes,
        "shards": n_tars,
        "shard_size_bytes": 256 * (1 << 20),
        "chunk_size_bytes": 8 * (1 << 20),
        "seed": SEED,
    })
    c["trace"] = {"file": trace_file, "sha256": trace_sha, "seed": SEED}
    c["workload"].update({
        "epochs": epochs, "batch": BATCH, "workers": 4,
        "steps_per_epoch": STEPS_PER_EPOCH, "shuffle": shuffle,
    })
    c["network"].update({
        "rate_mbit": rate, "delay_ms": 20,
        # 与 config.burst_for_rate 同口径:0.1 s 的传输量,单位 kbit
        "burst_k": int(round(rate * 125 * 0.1)),
    })
    c["cache"].pop("capacity_ratio", None)
    c["cache"].update({
        "enabled": True,
        "capacity_bytes": cache_bytes,
        "ratio": ratio,
        "evict_policy": evict,
    })
    if shuffle == "curriculum":
        c["workload"]["curriculum"] = {
            "stages": 3,
            "shard_fractions": [1.0, 2.0 / 3.0, 1.0 / 3.0],
            "boundary_rule": "equal-thirds",
        }
    else:
        c["workload"].pop("curriculum", None)
    c["prefetch"].update({
        "lookahead": 8, "window_k": 8,
        "fanout": fanout, "fanout_min": 1,
    })
    c["prefetch"]["gate"]["mode"] = gate_mode
    c["sampling"].update({"reps": reps, "coarse_reps": 3,
                          "bootstrap_iters": 10000})
    c["environment"]["lock_file"] = f"locks/{name}.lock.json"
    c["tags"] = [figure, f"rate={rate}", f"ratio={ratio}",
                 f"fanout={fanout}", f"gate={gate_mode}"]
    c["notes"] = (f"{figure}: {n_tars} 个 256 MiB 分片（{dataset_bytes >> 30} GiB），"
                  f"缓存 {cache_bytes} B（ratio={ratio}），{shuffle} 顺序。")
    return c


def _fig1(tpl):
    out = []
    for rate in RATES:
        for ratio in RATIOS:
            for fan in FANOUTS:
                for gate in ("gated", "free"):
                    name = f"fig1-{rate}-ratio{ratio}-fan{fan}-{gate}"
                    out.append(_cond(
                        tpl, name=name, figure="fig1",
                        group="traincache-warm", n_tars=8, ratio=ratio,
                        rate=rate, epochs=1, shuffle="fixed",
                        fanout=fan, gate_mode=gate))
    return out


def _fig2(tpl):
    out = []
    for rate in RATES:
        for n_tars, ratio in ((8, 2), (32, 8)):
            for ep in (1, 3, 10):
                for group in ("traincache-cold", "fluid-cold"):
                    short = "tc" if group.startswith("traincache") else "fl"
                    name = f"fig2-{rate}-size{n_tars * 256 >> 10}-ep{ep}-{short}"
                    out.append(_cond(
                        tpl, name=name, figure="fig2", group=group,
                        n_tars=n_tars, ratio=ratio, rate=rate, epochs=ep,
                        shuffle="per-epoch", fanout=2, gate_mode="gated"))
    return out


def _fig3(tpl):
    out = []
    for shuffle in ("fixed", "per-epoch", "curriculum"):
        for ep in (1, 3, 10):
            for group in ("traincache-cold", "fluid-cold"):
                short = "tc" if group.startswith("traincache") else "fl"
                name = f"fig3-{shuffle}-ep{ep}-{short}"
                out.append(_cond(
                    tpl, name=name, figure="fig3", group=group,
                    n_tars=8, ratio=2, rate=100, epochs=ep,
                    shuffle=shuffle, fanout=2, gate_mode="gated"))
    return out


def _pareto1(tpl):
    out = []
    for fan in FANOUTS:
        for gate in ("gated", "free"):
            name = f"pareto1-fan{fan}-{gate}"
            out.append(_cond(
                tpl, name=name, figure="pareto1", group="traincache-warm",
                n_tars=8, ratio=2, rate=100, epochs=1, shuffle="fixed",
                fanout=fan, gate_mode=gate, reps=7))
    return out


def main():
    ap = argparse.ArgumentParser(description="生成图系列 COND")
    ap.add_argument("--force", action="store_true", help="覆盖已存在的 COND")
    args = ap.parse_args()

    tpl = _template()
    conds = _fig1(tpl) + _fig2(tpl) + _fig3(tpl) + _pareto1(tpl)
    CONDS_DIR.mkdir(parents=True, exist_ok=True)
    written = skipped = 0
    for c in conds:
        p = CONDS_DIR / f"{c['name']}.json"
        if p.exists() and not args.force:
            skipped += 1
            continue
        p.write_text(json.dumps(c, ensure_ascii=False, indent=2) + "\n")
        written += 1
    print(f"COND: 写入 {written} 份,跳过 {skipped} 份,共 {len(conds)} 份")


if __name__ == "__main__":
    main()
