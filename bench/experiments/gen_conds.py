#!/usr/bin/env python3
"""生成十个实验族的 COND（schema v4）。

族、轴与取值取自 docs/4.2-bench-families.md 的族总表与各族一节。一个族只动该族的轴，其余参数取默认值：

  · 数据集固定 16 × 256 MiB = 4 GiB，epoch 全部 3；
  · 基线五组（direct、juicefs-cold/warm、fluid-cold/warm）只在 rho=4 生成一次；
  · 被测组 traincache-cold/warm 跑满五个 rho（对照族按带宽铺格子）；
  · 采样默认 3 次，端到端族按场景 2 次；
  · image_set 指向 experiments/images/ 下真实存在的文件，取文件内容哈希。

用法：
  python3 bench/experiments/gen_conds.py --family compare          # 只打印计划
  python3 bench/experiments/gen_conds.py --family compare --write  # 校验并落盘
  python3 bench/experiments/gen_conds.py --family all --write      # 九族一次生成

--write 会先校验全部 COND，任一条不过就整体不落盘；同时清掉 conds/ 里遗留的
v3 文件与本族的过期文件，最终目录只留九族生成的 v4 COND。
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent
BENCH_ROOT = EXPERIMENTS_DIR.parent
sys.path.insert(0, str(EXPERIMENTS_DIR))
sys.path.insert(0, str(BENCH_ROOT / "compare"))
import cond as cond_mod  # noqa: E402
import config  # noqa: E402
import trace as trace_mod  # noqa: E402

CONDS_DIR = EXPERIMENTS_DIR / "conds"
IMAGES_DIR = EXPERIMENTS_DIR / "images"
TRACES_DIR = EXPERIMENTS_DIR / "traces"

DATASET_NAME = "dataset-4gib"
DATASET = {
    "name": DATASET_NAME,
    "shards": config.DATASET_SHARDS,
    "shard_size_bytes": config.DATASET_SHARD_BYTES,
    "seed": config.SEED,
}

# 网关镜像取现有 gw COND 里记录的真实 digest；镜像换了用 --gateway-digest 覆盖。
GATEWAY_IMAGE_DIGEST = (
    "docker.io/library/import-2026-11-02@sha256:"
    "0d5521a0c18d09fb98a8e4cce90eba4239ff829385f26a19c32bc3f24747c2b5")

TILT_TRACE_NAME = "track-16-per-epoch-ep3.json"

BASELINES = ("direct", "juicefs-cold", "juicefs-warm",
             "fluid-cold", "fluid-warm")
TRAINCACHE = ("traincache-cold", "traincache-warm")
DEFAULT_GROUP = "traincache-cold"

# ---- 族定义表：轴、取值、COND 前缀、格子数 ----
# values 是各族自变量的完整取值；group 轴里基线只取 rho=4，由 builders 落实。
FAMILIES = {
    "compare": {
        "prefix": "compare",
        "axis": "rho × rate_mbit × group",
        "values": {
            "rho": list(config.RHO_VALUES),
            "rate_mbit": list(config.TC_RATE_MBITS),
            "group": list(TRAINCACHE) + list(BASELINES),
        },
        "cells": 36,
    },
    "gateway": {
        "prefix": "gateway",
        "axis": "gateway_arm × readers",
        "values": {
            "arm": ["g0", "g1", "g2", "g3"],
            "readers": [cond_mod.MAX_READERS],
        },
        "cells": 4,
    },
    "evict": {
        "prefix": "evict",
        "axis": "evict_policy × rho",
        "values": {
            "evict_policy": ["heat", "lru", "lfu"],
            "rho": [1.0, 8.0],
        },
        "cells": 6,
    },
    "prefetch": {
        "prefix": "prefetch",
        "axis": "lookahead × fanout × rho",
        "values": {
            "lookahead": sorted(cond_mod.VALID_LOOKAHEAD),
            "fanout": [1, 2, 3],
            "rho": [1.0, 8.0],
        },
        "cells": 18,
    },
    "warmup": {
        "prefix": "warmup",
        "axis": "gate × warmup_seconds × rho",
        "values": {
            "gate": sorted(cond_mod.GATE_MODES),
            "warmup_seconds": [10, 60, 300],
            "rho": [1.0, 8.0],
        },
        "cells": 12,
    },
    "switch": {
        "prefix": "switch",
        "axis": "scenario × rho",
        "values": {
            "scenario": ["fixed", "per-epoch", "curriculum",
                         "curriculum-narrow"],
            "rho": [1.0, 8.0],
        },
        "cells": 8,
    },
    "chaos": {
        "prefix": "chaos",
        "axis": "fault × rho",
        "values": {
            "fault": ["kill_agent", "tamper_block", "minio_down"],
            "rho": [1.0, 8.0],
        },
        "cells": 6,
    },
    "multiconsumer": {
        "prefix": "multi",
        "axis": "hot_ratio × placement",
        "values": {
            "hot_ratio": [0.10, 0.50],
            "placement": ["spread", "colocated"],
        },
        "cells": 4,
    },
    "e2e": {
        "prefix": "e2e",
        "axis": "recovery_scenario",
        "values": {
            "scenario": ["checkpoint-resume", "checkpoint-version",
                         "dual-restart"],
        },
        "cells": 3,
    },
}


def rho_label(rho: float) -> str:
    """0.5 → 05，其余取整数；COND 名不许有点。"""
    return f"{float(rho):g}".replace(".", "")


def image_set_ref() -> str:
    """images/ 下真实文件的内容哈希，写成 sha256:<hex>。"""
    files = sorted(IMAGES_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"images/ 下没有 image_set 文件：{IMAGES_DIR}")
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if path.stem == digest:
            return f"sha256:{digest}"
    path = files[0]
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def tilt_trace() -> tuple[str, str, dict]:
    """倾斜负载的 16 分片轨迹；返回 (相对路径, sha256, trace)。"""
    trace = trace_mod.build_trace(
        n_tars=config.DATASET_SHARDS, seed=config.SEED, shuffle="per-epoch",
        epochs=config.EPOCHS, batch=32, steps_per_epoch=config.EPOCH_STEPS,
        images_per_tar=config.DATASET_IMGS_PER_TAR)
    return f"traces/{TILT_TRACE_NAME}", trace_mod.trace_sha256(trace), trace


def make_cond(
    *,
    name: str,
    family: str,
    group: str = DEFAULT_GROUP,
    load: str = "dataset",
    rho: float = 4.0,
    rate: int = 1000,
    reps: int | None = None,
    shuffle: str = "per-epoch",
    fractions: tuple[float, ...] | None = None,
    readers: int = 1,
    pattern: str = "single",
    evict: str = "heat",
    lookahead: int = 16,
    fanout: int = 2,
    gate: str | None = None,
    warmup: int = 0,
    cache_enabled: bool = True,
    gateway: bool = False,
    gateway_cache: bool = False,
    gateway_digest: str | None = None,
    trace_ref: tuple[str, str] | None = None,
    tags: tuple[str, ...] = (),
    notes: str = "",
) -> dict:
    cache: dict = {"enabled": bool(cache_enabled)}
    if cache_enabled:
        cache.update({
            "rho": float(rho),
            "evict_policy": evict,
            "warmup_seconds": int(warmup),
        })
    cond = {
        "schema_version": cond_mod.SCHEMA_VERSION,
        "name": name,
        "family": family,
        "group": group,
        "load": load,
        "dataset": dict(DATASET),
        "workload": {
            "epochs": config.EPOCHS,
            "shuffle": shuffle,
            "readers": readers,
            "access_pattern": pattern,
        },
        "network": {"rate_mbit": rate},
        "cache": cache,
        "sampling": {"reps": reps if reps is not None else
                     (config.E2E_REPS if family == "e2e" else config.REPS)},
        "environment": {
            "gateway": bool(gateway),
            "gateway_image": (gateway_digest or GATEWAY_IMAGE_DIGEST)
            if gateway else None,
            "gateway_cache": bool(gateway_cache),
            "image_set": image_set_ref(),
        },
        "tags": list(tags),
        "notes": notes,
    }
    if fractions is not None:
        cond["workload"]["curriculum"] = {
            "stages": 3,
            "shard_fractions": [float(x) for x in fractions],
            "boundary_rule": "equal-thirds",
        }
    if group in cond_mod.TRAINCACHE_GROUPS:
        cond["prefetch"] = {"lookahead": lookahead, "fanout": fanout}
        if gate:
            cond["prefetch"]["gate"] = {"mode": gate}
    if load == "tilt":
        if trace_ref is None:
            raise SystemExit("load=tilt 的格子必须给 trace_ref")
        file, sha = trace_ref
        cond["trace"] = {"file": file, "sha256": sha, "seed": config.SEED}
    return cond


def _note(family: str, **params) -> str:
    body = "，".join(f"{k}={v}" for k, v in params.items())
    return (f"{family} 族格子；{body}；数据集 16×256 MiB（4 GiB），"
            f"{config.EPOCHS} epoch，rho 只改缓存配额。")


def build_compare() -> list[dict]:
    out = []
    # 1000 / 300 mbit：被测两组 × 五个 rho；基线五组只取 rho=4。
    for rate in (1000, 300):
        for group in TRAINCACHE:
            for rho in config.RHO_VALUES:
                name = f"compare-{group}-rho{rho_label(rho)}-{rate}"
                out.append(make_cond(
                    name=name, family="compare", group=group, rho=rho,
                    rate=rate, lookahead=16, fanout=2,
                    tags=("compare", f"group={group}", f"rate={rate}",
                          f"rho={rho}"),
                    notes=_note("compare", group=group, rho=rho, rate=rate)))
        for group in BASELINES:
            rho = 4.0
            name = f"compare-{group}-rho{rho_label(rho)}-{rate}"
            out.append(make_cond(
                name=name, family="compare", group=group, rho=rho, rate=rate,
                cache_enabled=(group != "direct"),
                tags=("compare", f"group={group}", f"rate={rate}",
                      f"rho={rho}", "baseline"),
                notes=_note("compare", group=group, rho=rho, rate=rate,
                            baseline="rho=4 代表点")))
    # 100 mbit：只留 traincache-cold 五个 rho 与 direct 基线。
    for rho in config.RHO_VALUES:
        name = f"compare-traincache-cold-rho{rho_label(rho)}-100"
        out.append(make_cond(
            name=name, family="compare", group="traincache-cold", rho=rho,
            rate=100, lookahead=16, fanout=2,
            tags=("compare", "group=traincache-cold", "rate=100", f"rho={rho}"),
            notes=_note("compare", group="traincache-cold", rho=rho,
                        rate=100)))
    rho = 4.0
    out.append(make_cond(
        name=f"compare-direct-rho{rho_label(rho)}-100", family="compare",
        group="direct", rho=rho, rate=100, cache_enabled=False,
        tags=("compare", "group=direct", "rate=100", f"rho={rho}",
              "baseline"),
        notes=_note("compare", group="direct", rho=rho, rate=100,
                    baseline="rho=4 代表点")))
    return out


def build_gateway() -> list[dict]:
    # G0 无网关无块缓存；G1 只有块缓存；G2 合并+两级缓存；G3 只有合并。
    arms = {
        "g0": dict(cache_enabled=False, gateway=False, gateway_cache=False),
        "g1": dict(cache_enabled=True, gateway=False, gateway_cache=False),
        "g2": dict(cache_enabled=True, gateway=True, gateway_cache=True),
        "g3": dict(cache_enabled=True, gateway=True, gateway_cache=False),
    }
    out = []
    for arm, flags in arms.items():
        name = f"gateway-{arm}-r4"
        out.append(make_cond(
            name=name, family="gateway", group=DEFAULT_GROUP, rho=4.0,
            rate=300, readers=cond_mod.MAX_READERS, pattern="replicated",
            lookahead=16, fanout=2,
            tags=("gateway", f"arm={arm}", "readers=4", "pattern=replicated"),
            notes=_note("gateway", arm=arm, rate=300, rho=4,
                        readers=cond_mod.MAX_READERS, pattern="replicated"),
            **flags))
    return out


def build_evict(trace_ref) -> list[dict]:
    out = []
    for policy in ("heat", "lru", "lfu"):
        for rho in (1.0, 8.0):
            name = f"evict-{policy}-rho{rho_label(rho)}"
            out.append(make_cond(
                name=name, family="evict", group=DEFAULT_GROUP, load="tilt",
                rho=rho, rate=1000, evict=policy, lookahead=16, fanout=2,
                trace_ref=trace_ref,
                tags=("evict", f"evict_policy={policy}", f"rho={rho}",
                      "load=tilt"),
                notes=_note("evict", evict_policy=policy, rho=rho, rate=1000,
                            load="tilt 热 10%")))
    return out


def build_prefetch() -> list[dict]:
    out = []
    for lookahead in sorted(cond_mod.VALID_LOOKAHEAD):
        for fanout in (1, 2, 3):
            for rho in (1.0, 8.0):
                name = (f"prefetch-la{lookahead}-fan{fanout}"
                        f"-rho{rho_label(rho)}")
                out.append(make_cond(
                    name=name, family="prefetch", group=DEFAULT_GROUP,
                    rho=rho, rate=1000, lookahead=lookahead, fanout=fanout,
                    tags=("prefetch", f"lookahead={lookahead}",
                          f"fanout={fanout}", f"rho={rho}"),
                    notes=_note("prefetch", lookahead=lookahead,
                                fanout=fanout, rho=rho, rate=1000)))
    return out


def build_warmup() -> list[dict]:
    out = []
    for gate in ("gated", "free"):
        for window in (10, 60, 300):
            for rho in (1.0, 8.0):
                # free 时窗口不生效，三格参数保持相同，只有名字与标签记录档位。
                warmup = window if gate == "gated" else 0
                name = f"warmup-{gate}-w{window}-rho{rho_label(rho)}"
                out.append(make_cond(
                    name=name, family="warmup", group="traincache-warm",
                    rho=rho, rate=1000, lookahead=16, fanout=2, gate=gate,
                    warmup=warmup,
                    tags=("warmup", f"gate={gate}", f"window={window}",
                          f"rho={rho}"),
                    notes=_note("warmup", gate=gate, window_s=window, rho=rho,
                                rate=1000,
                                effect=("gated 窗口生效" if gate == "gated"
                                        else "free 窗口不生效"))))
    return out


SCENARIOS = {
    "fixed": ("fixed", None),
    "per-epoch": ("per-epoch", None),
    "curriculum": ("curriculum", (1.0, 2.0 / 3.0, 1.0 / 3.0)),
    "curriculum-narrow": ("curriculum", (1.0, 0.5, 0.2)),
}


def build_switch() -> list[dict]:
    out = []
    for scenario, (shuffle, fractions) in SCENARIOS.items():
        for rho in (1.0, 8.0):
            name = f"switch-{scenario}-rho{rho_label(rho)}"
            out.append(make_cond(
                name=name, family="switch", group=DEFAULT_GROUP, rho=rho,
                rate=1000, shuffle=shuffle, fractions=fractions,
                lookahead=16, fanout=2,
                tags=("switch", f"scenario={scenario}", f"rho={rho}"),
                notes=_note("switch", scenario=scenario, rho=rho, rate=1000,
                            shuffle=shuffle)))
    return out


def build_chaos() -> list[dict]:
    out = []
    for fault in ("kill_agent", "tamper_block", "minio_down"):
        for rho in (1.0, 8.0):
            name = f"chaos-{fault.replace('_', '-')}-rho{rho_label(rho)}"
            out.append(make_cond(
                name=name, family="chaos", group=DEFAULT_GROUP, rho=rho,
                rate=1000, lookahead=16, fanout=2,
                tags=("chaos", f"fault={fault}", f"rho={rho}"),
                notes=_note("chaos", fault=fault, rho=rho, rate=1000)))
    return out


def build_multiconsumer(trace_ref) -> list[dict]:
    out = []
    placement = {
        "spread": (1, "跨节点 2 读端（TRAINCACHE_ANTI_AFFINITY=1）"),
        "colocated": (0, "同节点 2 读端（TRAINCACHE_ANTI_AFFINITY=0）"),
    }
    for hot in (0.10, 0.50):
        for mode in ("spread", "colocated"):
            anti_affinity, text = placement[mode]
            name = f"multi-hot{round(hot * 100)}-{mode}"
            out.append(make_cond(
                name=name, family="multiconsumer", group=DEFAULT_GROUP,
                load="tilt", rho=4.0, rate=1000, readers=2,
                pattern="replicated", lookahead=16, fanout=2,
                trace_ref=trace_ref,
                tags=("multiconsumer", f"hot={hot:.2f}",
                      f"placement={mode}",
                      f"anti-affinity={anti_affinity}"),
                notes=_note("multiconsumer", hot_ratio=f"{hot:.2f}",
                            placement=text, rho=4, rate=1000,
                            readers=2)))
    return out


def build_e2e() -> list[dict]:
    out = []
    for scenario in ("checkpoint-resume", "checkpoint-version",
                     "dual-restart"):
        out.append(make_cond(
            name=f"e2e-{scenario}", family="e2e", group=DEFAULT_GROUP,
            load="train", rho=4.0, rate=1000, reps=config.E2E_REPS,
            lookahead=16, fanout=2,
            tags=("e2e", f"scenario={scenario}"),
            notes=_note("e2e", scenario=scenario, rho=4, rate=1000,
                        reps=config.E2E_REPS, run_min=70)))
    return out


def build_family(family: str, trace_ref) -> list[dict]:
    if family == "compare":
        return build_compare()
    if family == "gateway":
        return build_gateway()
    if family == "evict":
        return build_evict(trace_ref)
    if family == "prefetch":
        return build_prefetch()
    if family == "warmup":
        return build_warmup()
    if family == "switch":
        return build_switch()
    if family == "chaos":
        return build_chaos()
    if family == "multiconsumer":
        return build_multiconsumer(trace_ref)
    if family == "e2e":
        return build_e2e()
    raise SystemExit(f"未知族：{family}")


def plan_line(cond: dict) -> str:
    cache = cond["cache"]
    cache_bytes = cond_mod.cache_bytes(cond) if cache.get("enabled") else 0
    return (f"  {cond['name']:<44} group={cond['group']:<16} "
            f"rho={str(cache.get('rho', '-')):<4} "
            f"rate={cond['network']['rate_mbit']:<5} "
            f"data={cond_mod.dataset_bytes(cond)} cache={cache_bytes} "
            f"reps={cond['sampling']['reps']}")


def print_plan(families: list[str], conds_by_family: dict[str, list[dict]]) -> int:
    total = 0
    for family in families:
        meta = FAMILIES[family]
        conds = conds_by_family[family]
        print(f"[{family}] {len(conds)} 个 COND（轴：{meta['axis']}，"
              f"前缀 {meta['prefix']}-）")
        for cond in conds:
            print(plan_line(cond))
        expected = meta["cells"]
        if len(conds) != expected:
            print(f"  ! 格子数 {len(conds)} != 族定义表 {expected}")
        total += len(conds)
    print(f"合计 {total} 个 COND")
    return total


def prune(families: list[str], keep: set[str]) -> list[Path]:
    """删掉 v3 遗留文件与本次生成族的过期 v4 文件。"""
    removed = []
    if not CONDS_DIR.is_dir():
        return removed
    for path in sorted(CONDS_DIR.glob("*.json")):
        if path.stem in keep:
            continue
        try:
            cond = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            cond = None
        stale_v3 = not isinstance(cond, dict) or \
            cond.get("schema_version") != cond_mod.SCHEMA_VERSION
        stale_family = isinstance(cond, dict) and \
            cond.get("family") in families
        if stale_v3 or stale_family:
            path.unlink()
            removed.append(path)
    return removed


def write_all(families: list[str], conds_by_family: dict[str, list[dict]],
              trace_ref, tilt_trace_obj: dict | None,
              prune_stale: bool = True) -> None:
    errors = []
    for family in families:
        for cond in conds_by_family[family]:
            errs = cond_mod.validate_cond(cond)
            if errs:
                errors.append((cond["name"], errs))
    if errors:
        for name, errs in errors:
            print(f"[FAIL] {name}")
            for err in errs:
                print(f"  - {err}")
        raise SystemExit(f"{len(errors)} 个 COND 校验失败，未落盘")

    CONDS_DIR.mkdir(parents=True, exist_ok=True)
    removed = []
    if prune_stale:
        keep = {cond["name"] for family in families
                for cond in conds_by_family[family]}
        removed = prune(families, keep)

    written = 0
    for family in families:
        for cond in conds_by_family[family]:
            path = CONDS_DIR / f"{cond['name']}.json"
            path.write_text(
                json.dumps(cond, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8")
            written += 1

    if tilt_trace_obj is not None and trace_ref is not None:
        # 首次用到倾斜轨迹时落盘；同一参数生成的文件逐字一致。
        trace_mod.write_trace(tilt_trace_obj, TRACES_DIR / TILT_TRACE_NAME)
        print(f"[trace] 写入 {trace_ref[0]} (sha256={trace_ref[1]})")

    print(f"[write] 写入 {written} 个 COND 到 {CONDS_DIR}")
    if removed:
        print(f"[prune] 删除 {len(removed)} 个旧文件")


def main() -> int:
    ap = argparse.ArgumentParser(description="生成九族 COND（schema v4）")
    ap.add_argument("--family", required=True,
                    choices=sorted(FAMILIES) + ["all"],
                    help="族 ID；all 表示九族一次生成")
    ap.add_argument("--write", action="store_true",
                    help="校验并写入 experiments/conds/；缺省只打印计划")
    ap.add_argument("--gateway-digest", default=None,
                    help="覆盖网关镜像 digest（repo@sha256:...）")
    ap.add_argument("--no-prune", action="store_true",
                    help="只写本族文件，不清理 conds/ 里的旧文件")
    args = ap.parse_args()

    if args.gateway_digest:
        if not cond_mod._IMAGE_DIGEST_RE.match(args.gateway_digest):
            raise SystemExit(f"gateway digest 形态不合法：{args.gateway_digest!r}")
        global GATEWAY_IMAGE_DIGEST
        GATEWAY_IMAGE_DIGEST = args.gateway_digest

    families = list(FAMILIES) if args.family == "all" else [args.family]
    need_trace = any(load_family in ("evict", "multiconsumer")
                     for load_family in families)
    trace_path, trace_sha, trace_obj = tilt_trace() if need_trace else (None, None, None)
    trace_ref = (trace_path, trace_sha) if need_trace else None
    tilt_trace_obj = trace_obj

    conds_by_family = {
        family: build_family(family, trace_ref) for family in families
    }
    print_plan(families, conds_by_family)
    if not args.write:
        print("（未写入；加 --write 落盘）")
        return 0
    write_all(families, conds_by_family, trace_ref, tilt_trace_obj,
              prune_stale=not args.no_prune)
    return 0


if __name__ == "__main__":
    sys.exit(main())
