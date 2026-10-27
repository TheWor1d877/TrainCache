#!/usr/bin/env python3
"""COND 契约 v1 解析器与校验器。

COND 文件位于 bench/experiments/conds/<name>.json,描述一个实验点。
字段全集与校验规则见 docs/9.2 实验协议.md §C2/§C5;runner 遇到未知字段直接拒绝。
"""
import json
import re
import sys
from pathlib import Path
from typing import Any

# 复用 bench/config.py 作为数据规模唯一参数源(三档数据量按 N_TARS × 256MiB 推导)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

# ---- schema 常量(与 docs/9.2 §C 冻结值逐字一致)----
SCHEMA_VERSION = "1"

VALID_FIGURES = {
    "fig1", "fig2", "fig3",
    "pareto1", "pareto2",
    "compare-d", "compare-b",
    "policy",
}
# 组态名(D30):与 bench/compare/run_bench.py 的 GROUPS 一一对应。
# 冷热由组态名表达,不再用 "traincache"/"fluid" 这种不分冷热的写法。
VALID_GROUPS = {
    "direct",
    "juicefs-cold", "juicefs-warm",
    "fluid-cold", "fluid-warm",
    "traincache-cold", "traincache-warm",
}
# TrainCache 组:这两组共用 prefetch 契约(冷热只影响启动窗口的预热块数)
TRAINCACHE_GROUPS = {"traincache-cold", "traincache-warm"}
VALID_TRACKS = {"B", "C", "D"}

VALID_SHUFFLES = {"fixed", "per-epoch", "curriculum"}
# 合法淘汰策略(D9:W-TinyLFU 已撤档,只剩 heat/lru/lfu 三档,出现即拒绝;
# external 表示由 JuiceFS/Fluid 自己管缓存,TrainCache 不应用该策略)
VALID_EVICT_POLICIES = {"heat", "lru", "lfu", "external"}
VALID_UPGRADE_RULES = {"sign-change", "none"}

# 持续预取提前量(D28:由 2/4/8/16 收敛为 8/16/32;对照实验 S/M/L 三档固定 16)
VALID_LOOKAHEAD = {8, 16, 32}

# 正式实验块大小(D20:固定 8 MiB,写在 dataset.chunk_size_bytes)
CHUNK_SIZE_BYTES = 8388608

# 顶层允许字段(docs/9.2 §C2.1)
ALLOWED_TOP = {
    "schema_version", "name", "figure", "group", "track",
    "dataset", "trace", "workload", "network", "cache",
    "prefetch", "sampling", "environment", "tags", "notes",
}
ALLOWED_DATASET = {
    "name", "source", "endpoint", "bucket", "bytes", "shards",
    "shard_size_bytes", "chunk_size_bytes", "seed",
}
ALLOWED_TRACE = {"file", "sha256", "seed"}
ALLOWED_WORKLOAD = {
    "epochs", "batch", "workers", "steps_per_epoch", "prefetch_factor",
    "compute_ms", "shuffle", "curriculum",
}
ALLOWED_CURRICULUM = {"stages", "shard_fractions", "boundary_rule"}
ALLOWED_NETWORK = {"rate_mbit", "delay_ms", "burst_k"}
ALLOWED_CACHE = {"enabled", "capacity_bytes", "ratio", "capacity_ratio",
                 "evict_policy", "warmup_seconds", "root"}
ALLOWED_PREFETCH = {
    "lookahead", "window_k", "fanout", "fanout_min", "gate", "score",
}
ALLOWED_GATE = {"threshold", "gap_min", "slope_min", "t_max_s", "min_interval_s",
                "mode"}
GATE_MODES = {"gated", "free"}
ALLOWED_SCORE = {"half_life", "cutoff"}
ALLOWED_SAMPLING = {"reps", "coarse_reps", "upgrade_rule", "bootstrap_iters"}
ALLOWED_ENVIRONMENT = {"kind_workers", "k8s_version", "images", "lock_file"}

DATASET_SOURCES = {"minio", "local-nginx"}
# tc 限速档取值集合:单点定义在 bench/config.py(D31:带宽是实验自变量)
VALID_RATES = set(config.TC_RATE_MBITS)
CURRICULUM_FRACTIONS_FROZEN = [1.0, 2.0 / 3.0, 1.0 / 3.0]

# ---- 容量档位 S/M/L(docs/9.1 任务 0.6/6.13,D19;与 cache.ratio/capacity_bytes 同一口径)----
# 对照实验缓存固定 1GiB;数据量 = N_TARS × SHARD_SIZE_MB(256MiB,即 config.TRACK_B_TAR_BYTES)。
# M 档 N_TARS 即 config.TRACK_B_N_TARS(32);S/L 为其 1/4 与 4 倍。
COMPARE_CACHE_BYTES = 1 << 30  # 1GiB,三档共用
COMPARE_SHARD_SIZE_BYTES = config.TRACK_B_TAR_BYTES  # 256MiB
VALID_CAPACITY_RATIOS = {"S", "M", "L"}
# 数据集规模枚举(D19 三档):S=2GiB / M=8GiB / L=32GiB
VALID_DATASET_BYTES = {2 << 30, 8 << 30, 32 << 30}
# epoch 档位枚举(9.1 M3):1 / 3 / 10
VALID_EPOCHS = {1, 3, 10}
CAPACITY_TIER_RATIO = {"S": 2.0, "M": 8.0, "L": 32.0}   # 数据/缓存容量比(D19)
CAPACITY_TIER_N_TARS = {
    "S": config.TRACK_B_N_TARS // 4,   # 8  × 256MiB = 2GiB
    "M": config.TRACK_B_N_TARS,        # 32 × 256MiB = 8GiB
    "L": config.TRACK_B_N_TARS * 4,    # 128 × 256MiB = 32GiB(分片需新增生成)
}
CAPACITY_TIER_REPS = {"S": 3, "M": 3, "L": 1}             # D32:每档名义次数(runner 默认)
# 每档正式实验的采样次数(D32):S=3/M=3/L=1,不做符号翻转补测。
CAPACITY_TIER_REPS_ALLOWED = {"S": {3}, "M": {3}, "L": {1}}
# D30:各档参跑组。组态名自带冷热,traincache-cold / traincache-warm 是两个独立组态。
CAPACITY_TIER_GROUPS = {
    "S": {"direct", "juicefs-cold", "juicefs-warm",
          "fluid-cold", "fluid-warm", "traincache-cold", "traincache-warm"},
    "M": {"direct", "fluid-cold", "traincache-cold"},
    "L": {"traincache-cold", "fluid-cold"},
}
EXPLORATORY_TIERS = {"L"}  # L 档 1 次,低于 3 次硬纪律:标 exploratory,不进正式表
# 9.1 D32/§12:对照层 runner 的 epochs 缺省随档位联动 —— S 档跑 D 轨口径
# (3 个连续 epoch),M/L 档跑 B 轨符号探点(1 个 epoch)。显式 EPOCHS 环境变量
# 优先;COND 的 workload.epochs 仍按 figure 校验,两者不一致由 COND 校验器拒收。
CAPACITY_TIER_EPOCHS = {"S": 3, "M": 1, "L": 1}


def tier_dataset_bytes(tier: str) -> int:
    """档位 → 数据量字节(N_TARS × SHARD_SIZE_MB)。"""
    return CAPACITY_TIER_N_TARS[tier] * COMPARE_SHARD_SIZE_BYTES


def tier_capacity_bytes(tier: str) -> int:
    """档位 → 缓存容量字节(三档固定 1GiB)。"""
    return COMPARE_CACHE_BYTES


def burst_k_for_rate(rate_mbit: int) -> int:
    """rate → tbf burst(KB):≈0.1 秒的传输量(算式在 bench/config.py,这里只做单位换算)。"""
    return int(config.burst_for_rate(rate_mbit).rstrip("k"))


_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_DIGEST_RE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")

CONDS_DIR_DEFAULT = Path(__file__).resolve().parent / "conds"


def load_cond(name: str, conds_dir: Path | None = None) -> dict:
    """读 bench/experiments/conds/<name>.json,返回 dict。

    conds_dir 默认 bench/experiments/conds/。
    """
    base = Path(conds_dir) if conds_dir else CONDS_DIR_DEFAULT
    path = base / f"{name}.json"
    if not path.is_file():
        raise FileNotFoundError(f"COND 文件不存在: {path}")
    with path.open("r", encoding="utf-8") as f:
        cond = json.load(f)
    if not isinstance(cond, dict):
        raise ValueError(f"COND 顶层不是对象: {path}")
    return cond


def _check_unknown(cond: dict, allowed: set, path: str, errors: list) -> None:
    unknown = set(cond.keys()) - allowed
    if unknown:
        errors.append(f"{path} 含未知字段: {sorted(unknown)}")


def _require(obj: dict, key: str, path: str, errors: list) -> Any:
    if key not in obj or obj[key] is None or obj[key] == "":
        errors.append(f"{path}.{key} 缺失或为空")
        return None
    return obj[key]


def validate_cond(cond: dict) -> list[str]:
    """按 docs/9.2 §C5 校验,返回错误信息列表(空列表=通过)。"""
    errors: list[str] = []

    # 1. 顶层未知字段 + schema_version
    _check_unknown(cond, ALLOWED_TOP, "$", errors)
    if cond.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version 必须为 \"{SCHEMA_VERSION}\"(收到 {cond.get('schema_version')!r})")

    name = _require(cond, "name", "$", errors)
    if name is not None:
        if not isinstance(name, str):
            errors.append("$.name 必须是字符串")
        elif len(name) > 80:
            errors.append(f"$.name 长度 >80({len(name)})")
        elif not _NAME_RE.match(name):
            errors.append(f"$.name 不符合小写 kebab 规范: {name!r}")

    figure = _require(cond, "figure", "$", errors)
    if figure is not None and figure not in VALID_FIGURES:
        errors.append(f"$.figure 非法枚举: {figure!r}(允许 {sorted(VALID_FIGURES)})")

    group = _require(cond, "group", "$", errors)
    if group is not None and group not in VALID_GROUPS:
        errors.append(f"$.group 非法枚举: {group!r}(允许 {sorted(VALID_GROUPS)})")

    track = _require(cond, "track", "$", errors)
    if track is not None and track not in VALID_TRACKS:
        errors.append(f"$.track 非法枚举: {track!r}(允许 {sorted(VALID_TRACKS)})")

    # ---- dataset ----
    ds = _require(cond, "dataset", "$", errors)
    if isinstance(ds, dict):
        _check_unknown(ds, ALLOWED_DATASET, "$.dataset", errors)
        ds_name = _require(ds, "name", "$.dataset", errors)
        if ds_name is not None and not isinstance(ds_name, str):
            errors.append("$.dataset.name 必须是字符串")
        source = _require(ds, "source", "$.dataset", errors)
        if source is not None and source not in DATASET_SOURCES:
            errors.append(f"$.dataset.source 非法: {source!r}")
        if source == "minio":
            bucket = _require(ds, "bucket", "$.dataset", errors)
            if bucket is not None and not isinstance(bucket, str):
                errors.append("$.dataset.bucket 必须是字符串")
        _require(ds, "endpoint", "$.dataset", errors)
        bytes_ = ds.get("bytes")
        shards = ds.get("shards")
        shard_size = ds.get("shard_size_bytes")
        seed = ds.get("seed")
        if bytes_ is None or not isinstance(bytes_, int) or bytes_ <= 0:
            errors.append("$.dataset.bytes 必须是 >0 的整数")
        elif bytes_ not in VALID_DATASET_BYTES:
            # 数据集规模枚举(D19 三档):S=2GiB / M=8GiB / L=32GiB
            errors.append(
                f"$.dataset.bytes={bytes_} 不在枚举内"
                f"(允许 2/8/32 GiB)")
        if shards is None or not isinstance(shards, int) or shards <= 0:
            errors.append("$.dataset.shards 必须是 >0 的整数")
        if shard_size is None or not isinstance(shard_size, int) or shard_size <= 0:
            errors.append("$.dataset.shard_size_bytes 必须是 >0 的整数")
        if seed is None or not isinstance(seed, int) or seed <= 0:
            errors.append("$.dataset.seed 必须是 >0 的整数")
        # 块大小(D20):正式实验固定 8 MiB;不填表示沿用发布快照口径
        chunk_bytes = ds.get("chunk_size_bytes")
        if chunk_bytes is not None:
            if not isinstance(chunk_bytes, int) or chunk_bytes <= 0:
                errors.append("$.dataset.chunk_size_bytes 必须是 >0 的整数")
            elif chunk_bytes != CHUNK_SIZE_BYTES:
                errors.append(
                    f"$.dataset.chunk_size_bytes 正式实验必须为 "
                    f"{CHUNK_SIZE_BYTES}(收到 {chunk_bytes})")
        # 规则 3: bytes == shards * shard_size_bytes ±1%
        if (isinstance(bytes_, int) and isinstance(shards, int)
                and isinstance(shard_size, int) and bytes_ > 0 and shards > 0
                and shard_size > 0):
            expected = shards * shard_size
            rel = abs(bytes_ - expected) / expected
            if rel > 0.01:
                errors.append(
                    f"$.dataset.bytes({bytes_}) 与 shards×shard_size_bytes"
                    f"({expected}) 误差 {rel:.2%} > 1%")

    # ---- trace ----
    trace = _require(cond, "trace", "$", errors)
    if isinstance(trace, dict):
        _check_unknown(trace, ALLOWED_TRACE, "$.trace", errors)
        _require(trace, "file", "$.trace", errors)
        sha = _require(trace, "sha256", "$.trace", errors)
        if sha is not None:
            if not isinstance(sha, str) or not _SHA256_RE.match(sha):
                errors.append("$.trace.sha256 必须是 64 位小写 hex")
            elif sha == "0" * 64:
                errors.append("$.trace.sha256 是全零占位符,需填真实值")
            elif len(set(sha)) == 1:
                # aaaa… / ffff… 这类单字符重复串也是占位符(docs/9.2 §C5 规则 9)
                errors.append("$.trace.sha256 是单字符重复占位符(如 aaaa…),需填真实值")
            elif sha.startswith("deadbeef"):
                errors.append("$.trace.sha256 是占位符(deadbeef…),需填真实值")
        tseed = _require(trace, "seed", "$.trace", errors)
        if tseed is not None and (not isinstance(tseed, int) or tseed <= 0):
            errors.append("$.trace.seed 必须是 >0 的整数")

    # ---- workload ----
    wl = _require(cond, "workload", "$", errors)
    if isinstance(wl, dict):
        _check_unknown(wl, ALLOWED_WORKLOAD, "$.workload", errors)
        for k in ("epochs", "batch", "workers", "steps_per_epoch",
                  "prefetch_factor", "compute_ms"):
            v = _require(wl, k, "$.workload", errors)
            if v is not None and (not isinstance(v, int) or v <= 0):
                errors.append(f"$.workload.{k} 必须是 >0 的整数")
        # epoch 档位枚举(9.1 M3):1 / 3 / 10,不在枚举内的实验点直接拒收
        ep = wl.get("epochs")
        if isinstance(ep, int) and ep not in VALID_EPOCHS:
            errors.append(
                f"$.workload.epochs={ep} 不在枚举内(允许 1/3/10)")
        shuffle = _require(wl, "shuffle", "$.workload", errors)
        if shuffle is not None and shuffle not in VALID_SHUFFLES:
            errors.append(f"$.workload.shuffle 非法: {shuffle!r}")
        # 规则 7: curriculum
        if shuffle == "curriculum":
            cur = _require(wl, "curriculum", "$.workload", errors)
            if isinstance(cur, dict):
                _check_unknown(cur, ALLOWED_CURRICULUM, "$.workload.curriculum", errors)
                stages = _require(cur, "stages", "$.workload.curriculum", errors)
                if stages is not None and stages != 3:
                    errors.append(f"$.workload.curriculum.stages 必须为 3(收到 {stages})")
                fracs = _require(cur, "shard_fractions", "$.workload.curriculum", errors)
                if fracs is not None:
                    if not isinstance(fracs, list) or len(fracs) != 3:
                        errors.append("$.workload.curriculum.shard_fractions 必须是长度 3 的数组")
                    else:
                        for i, (a, b) in enumerate(zip(fracs, CURRICULUM_FRACTIONS_FROZEN)):
                            if abs(float(a) - b) > 1e-4:
                                errors.append(
                                    f"$.workload.curriculum.shard_fractions[{i}]={a}"
                                    f" 与冻结值 {b} 不一致")
                boundary = _require(cur, "boundary_rule", "$.workload.curriculum", errors)
                if boundary is not None and boundary != "equal-thirds":
                    errors.append(
                        f"$.workload.curriculum.boundary_rule 必须为 'equal-thirds'"
                        f"(收到 {boundary!r})")

    # ---- network ----
    net = _require(cond, "network", "$", errors)
    if isinstance(net, dict):
        _check_unknown(net, ALLOWED_NETWORK, "$.network", errors)
        rate = _require(net, "rate_mbit", "$.network", errors)
        if rate is not None and rate not in VALID_RATES:
            errors.append(f"$.network.rate_mbit 非法: {rate!r}(允许 {sorted(VALID_RATES)})")
        delay = _require(net, "delay_ms", "$.network", errors)
        if delay is not None and delay != 20:
            errors.append(f"$.network.delay_ms 必须为 20(收到 {delay})")
        burst = _require(net, "burst_k", "$.network", errors)
        if burst is not None and rate in VALID_RATES:
            # D31:burst 由 rate 推导(≈0.1 秒的传输量),不再固定 1600
            expect_burst = burst_k_for_rate(rate)
            if burst != expect_burst:
                errors.append(
                    f"$.network.burst_k 必须为 {expect_burst}"
                    f"(rate_mbit={rate} 的推导值,收到 {burst})")
        # netem 形态不在 COND 里:固定 delay 由 tc_apply.sh 从 TC_DELAY 取,
        # COND 只带 rate/delay/burst,避免出现互相矛盾的两个真相。

    # ---- cache ----
    cache = _require(cond, "cache", "$", errors)
    if isinstance(cache, dict):
        _check_unknown(cache, ALLOWED_CACHE, "$.cache", errors)
        enabled = cache.get("enabled")
        if enabled is None:
            errors.append("$.cache.enabled 缺失")
        elif not isinstance(enabled, bool):
            errors.append("$.cache.enabled 必须是 bool")
        else:
            # 规则 5: enabled=false 仅 group=direct
            if not enabled and group != "direct":
                errors.append(
                    f"$.cache.enabled=false 仅允许 group=direct(当前 group={group!r})")
            # 规则 4: enabled=true → capacity_bytes & ratio 必填且交叉校验
            cap = ratio = None
            if enabled:
                cap = _require(cache, "capacity_bytes", "$.cache", errors)
                ratio = _require(cache, "ratio", "$.cache", errors)
                if cap is not None and (not isinstance(cap, int) or cap <= 0):
                    errors.append("$.cache.capacity_bytes 必须是 >0 的整数")
                if ratio is not None and (not isinstance(ratio, (int, float)) or ratio <= 0):
                    errors.append("$.cache.ratio 必须是 >0 的数")
                if (isinstance(cap, int) and cap > 0
                        and isinstance(ratio, (int, float)) and ratio > 0
                        and isinstance(ds, dict) and isinstance(ds.get("bytes"), int)
                        and ds["bytes"] > 0):
                    expected = ds["bytes"] / ratio
                    rel = abs(cap - expected) / expected
                    if rel > 0.01:
                        errors.append(
                            f"$.cache.capacity_bytes({cap}) 与 dataset.bytes/ratio"
                            f"({expected:.0f}) 误差 {rel:.2%} > 1%")
            else:
                # enabled=false 时 capacity_bytes/ratio 不填
                if "capacity_bytes" in cache and cache["capacity_bytes"] is not None:
                    errors.append("$.cache.enabled=false 时不应填 capacity_bytes")
                if "ratio" in cache and cache["ratio"] is not None:
                    errors.append("$.cache.enabled=false 时不应填 ratio")
        ep = _require(cache, "evict_policy", "$.cache", errors)
        if ep is not None and ep not in VALID_EVICT_POLICIES:
            errors.append(f"$.cache.evict_policy 非法: {ep!r}")
        # warmup_seconds(D11/D23):本点预热预算秒数(冷组 0 或不填)。
        # 注意它是"预算",实测预热耗时由 runner 写进产物 summary.csv 的同名列。
        ws = cache.get("warmup_seconds")
        if ws is not None:
            if not isinstance(ws, int) or isinstance(ws, bool) or ws < 0:
                errors.append("$.cache.warmup_seconds 必须是 ≥0 的整数")
        # capacity_ratio(D19/任务 0.6):容量档位 S/M/L,与 ratio/capacity_bytes/dataset.bytes 同一口径
        cr = cache.get("capacity_ratio")
        if cr is not None:
            if cr not in VALID_CAPACITY_RATIOS:
                errors.append(
                    f"$.cache.capacity_ratio 非法: {cr!r}(允许 S/M/L)")
            else:
                # 容量档位只属于对照实验(schema 规则 11)
                if figure not in {"compare-d", "compare-b"}:
                    errors.append(
                        f"$.cache.capacity_ratio 只允许出现在 "
                        f"figure=compare-d/compare-b(当前 figure={figure!r})")
                # D30:group 必须落在该档位的参跑组里,越组报错
                allowed_groups = CAPACITY_TIER_GROUPS[cr]
                if group is not None and group not in allowed_groups:
                    errors.append(
                        f"$.group={group!r} 不在 capacity_ratio={cr} 的参跑组内"
                        f"(允许 {sorted(allowed_groups)})")
                if isinstance(ratio, (int, float)) and ratio > 0:
                    if abs(ratio - CAPACITY_TIER_RATIO[cr]) / CAPACITY_TIER_RATIO[cr] > 0.01:
                        errors.append(
                            f"$.cache.capacity_ratio={cr} 要求 $.cache.ratio="
                            f"{CAPACITY_TIER_RATIO[cr]}(收到 {ratio})")
                if isinstance(cap, int) and cap > 0:
                    if abs(cap - COMPARE_CACHE_BYTES) / COMPARE_CACHE_BYTES > 0.01:
                        errors.append(
                            f"$.cache.capacity_ratio 三档缓存固定 1GiB"
                            f"({COMPARE_CACHE_BYTES}),收到 {cap}")
                if isinstance(ds, dict) and isinstance(ds.get("bytes"), int) and ds["bytes"] > 0:
                    expect_bytes = tier_dataset_bytes(cr)
                    rel = abs(ds["bytes"] - expect_bytes) / expect_bytes
                    if rel > 0.01:
                        errors.append(
                            f"$.cache.capacity_ratio={cr} 要求 $.dataset.bytes="
                            f"{expect_bytes}(N_TARS={CAPACITY_TIER_N_TARS[cr]}×"
                            f"{COMPARE_SHARD_SIZE_BYTES}B),误差 {rel:.2%} > 1%")
        if "root" in cache and cache["root"] is not None:
            if not isinstance(cache["root"], str) or not cache["root"].startswith("/"):
                errors.append("$.cache.root 必须是绝对路径")

    # ---- prefetch ----
    prefetch = cond.get("prefetch")
    if group in TRAINCACHE_GROUPS:
        prefetch = _require(cond, "prefetch", "$", errors)
    if isinstance(prefetch, dict):
        _check_unknown(prefetch, ALLOWED_PREFETCH, "$.prefetch", errors)
        lookahead = _require(prefetch, "lookahead", "$.prefetch", errors)
        if lookahead is not None and lookahead not in VALID_LOOKAHEAD:
            errors.append(
                f"$.prefetch.lookahead 非法: {lookahead!r}"
                f"(允许 {'/'.join(str(v) for v in sorted(VALID_LOOKAHEAD))})")
        wk = _require(prefetch, "window_k", "$.prefetch", errors)
        if wk is not None and wk != 8:
            errors.append(f"$.prefetch.window_k 必须为 8(收到 {wk})")
        fanout = _require(prefetch, "fanout", "$.prefetch", errors)
        if fanout is not None and fanout not in {1, 2, 3}:
            errors.append(f"$.prefetch.fanout 非法: {fanout!r}(允许 1/2/3)")
        fanout_min = _require(prefetch, "fanout_min", "$.prefetch", errors)
        if fanout_min is not None and fanout_min != 1:
            errors.append(f"$.prefetch.fanout_min 必须为 1(收到 {fanout_min})")
        gate = _require(prefetch, "gate", "$.prefetch", errors)
        if isinstance(gate, dict):
            _check_unknown(gate, ALLOWED_GATE, "$.prefetch.gate", errors)
            for k, frozen in (("threshold", 0.9), ("gap_min", 0.1),
                              ("slope_min", 0.001), ("t_max_s", 600),
                              ("min_interval_s", 1)):
                v = gate.get(k)
                if v is not None and abs(float(v) - frozen) > 1e-9:
                    errors.append(f"$.prefetch.gate.{k} 必须为 {frozen}(收到 {v})")
            mode = gate.get("mode")
            if mode is not None and mode not in GATE_MODES:
                errors.append(f"$.prefetch.gate.mode 必须为 gated/free(收到 {mode})")
        score = _require(prefetch, "score", "$.prefetch", errors)
        if isinstance(score, dict):
            _check_unknown(score, ALLOWED_SCORE, "$.prefetch.score", errors)
            if score.get("half_life") is not None and score["half_life"] != 4:
                errors.append(f"$.prefetch.score.half_life 必须为 4(收到 {score.get('half_life')})")
            if score.get("cutoff") is not None and score["cutoff"] != 8:
                errors.append(f"$.prefetch.score.cutoff 必须为 8(收到 {score.get('cutoff')})")

    # ---- sampling ----
    samp = _require(cond, "sampling", "$", errors)
    # 容量档位(D19/D32)接管 compare-* 的采样次数:S=3/M=3/L=1
    tier = cache.get("capacity_ratio") if isinstance(cache, dict) else None
    tier_has_reps = tier in CAPACITY_TIER_REPS
    if isinstance(samp, dict):
        _check_unknown(samp, ALLOWED_SAMPLING, "$.sampling", errors)
        reps = _require(samp, "reps", "$.sampling", errors)
        # 规则 8:pareto1/pareto2 固定 7;policy 与 fig1/fig2/fig3 固定 3;
        # compare-* 带 capacity_ratio 时按档位(D32),不带时固定 3
        if figure in {"pareto1", "pareto2"}:
            allowed_reps = {7}
        elif figure in {"compare-d", "compare-b"}:
            allowed_reps = CAPACITY_TIER_REPS_ALLOWED[tier] if tier_has_reps else {3}
        else:
            allowed_reps = {3}
        if reps is not None and reps not in allowed_reps:
            tier_hint = f",capacity_ratio={tier!r}" if tier_has_reps else ""
            errors.append(
                f"$.sampling.reps 必须为 {sorted(allowed_reps)}"
                f"(figure={figure}{tier_hint},收到 {reps})")
        coarse = _require(samp, "coarse_reps", "$.sampling", errors)
        if coarse is not None and coarse != 3:
            errors.append(f"$.sampling.coarse_reps 必须为 3(收到 {coarse})")
        upg = _require(samp, "upgrade_rule", "$.sampling", errors)
        if upg is not None and upg not in VALID_UPGRADE_RULES:
            errors.append(f"$.sampling.upgrade_rule 非法: {upg!r}")
        bi = _require(samp, "bootstrap_iters", "$.sampling", errors)
        if bi is not None and bi != 10000:
            errors.append(f"$.sampling.bootstrap_iters 必须为 10000(收到 {bi})")

    # ---- environment ----
    env = _require(cond, "environment", "$", errors)
    if isinstance(env, dict):
        _check_unknown(env, ALLOWED_ENVIRONMENT, "$.environment", errors)
        kw = _require(env, "kind_workers", "$.environment", errors)
        if kw is not None and kw != 4:
            errors.append(f"$.environment.kind_workers 必须为 4(收到 {kw})")
        _require(env, "k8s_version", "$.environment", errors)
        _require(env, "lock_file", "$.environment", errors)
        images = _require(env, "images", "$.environment", errors)
        if isinstance(images, dict):
            for key, val in images.items():
                if not isinstance(val, str) or not _IMAGE_DIGEST_RE.match(val):
                    errors.append(
                        f"$.environment.images.{key} 必须是 repo@sha256:... 形式"
                        f"(收到 {val!r})")
                elif val.endswith("0" * 64):
                    errors.append(f"$.environment.images.{key} 是全零占位符,需填真实 digest")
                else:
                    # aaaa… / deadbeef… 也是占位符(docs/9.2 §C5 规则 9),digest 必须是真实镜像摘要
                    hexpart = val.split("sha256:", 1)[-1]
                    if len(set(hexpart)) == 1 or hexpart.startswith("deadbeef"):
                        errors.append(
                            f"$.environment.images.{key} 是占位符 digest,需填真实摘要")

    # ---- tags / notes ----
    tags = cond.get("tags")
    if tags is not None:
        if not isinstance(tags, list):
            errors.append("$.tags 必须是数组")
        else:
            for t in tags:
                if not isinstance(t, str) or len(t) > 32:
                    errors.append(f"$.tags 元素须为 ≤32 字符的字符串: {t!r}")
    notes = cond.get("notes")
    if notes is not None and (not isinstance(notes, str) or len(notes) > 500):
        errors.append("$.notes 必须是 ≤500 字符的字符串")

    return errors


def cond_to_env(cond: dict) -> dict[str, str]:
    """把 COND 字段映射成环境变量 dict。

    TRAINCACHE_JOB_CREATED_AT 不在此设置,由 run.py 在创建 Job 前注入。
    """
    env: dict[str, str] = {}
    env["TRAINCACHE_COND"] = cond["name"]
    env["TRAINCACHE_GROUP"] = cond["group"]
    env["TRAINCACHE_DATASET"] = cond["dataset"]["name"]
    env["TRAINCACHE_EVICT_POLICY"] = cond["cache"]["evict_policy"]
    if cond["cache"].get("enabled"):
        env["TRAINCACHE_CACHE_CAPACITY_BYTES"] = str(cond["cache"]["capacity_bytes"])
    cr = cond["cache"].get("capacity_ratio")
    if cr:
        # 对照实验容量档位(D19/任务 0.6):run_bench.py 按它推导数据量/次数默认值
        env["CAPACITY_RATIO"] = str(cr)
        env["COMPARE_TIER"] = str(cr)  # 别名,兼容旧接线
    if cond["group"] in TRAINCACHE_GROUPS:
        pf = cond.get("prefetch") or {}
        if "lookahead" in pf:
            env["TRAINCACHE_PREFETCH_LOOKAHEAD"] = str(pf["lookahead"])
        mode = (pf.get("gate") or {}).get("mode")
        if mode:
            env["TRAINCACHE_GATE_MODE"] = str(mode)
    return env


def cond_to_tc_args(cond: dict) -> list[str]:
    """返回 tc_apply.sh 的位置参数列表:[rate, delay]。

    rate 形如 '100mbit',delay 形如 '20ms'。
    """
    net = cond["network"]
    return [f"{net['rate_mbit']}mbit", f"{net['delay_ms']}ms"]


def cond_to_track_d_args(cond: dict) -> list[str]:
    """返回 bench_track_d.py 的 CLI 参数列表(不含可执行文件,不含 --out-dir)。

    映射:--epochs, --seed, --workers, --batch, --steps-per-epoch,
    --shuffle, --window-k。
    --url / --images-per-tar / --chunk-bytes / --shard-bytes 用 config.py 默认值,
    不在此传递。--out-dir 由调用方(run.py)按本轮产物目录追加。
    """
    wl = cond["workload"]
    args = [
        "--epochs", str(wl["epochs"]),
        "--seed", str(cond["dataset"]["seed"]),
        "--workers", str(wl["workers"]),
        "--batch", str(wl["batch"]),
        "--steps-per-epoch", str(wl["steps_per_epoch"]),
        "--shuffle", wl["shuffle"],
    ]
    pf = cond.get("prefetch") or {}
    if "window_k" in pf:
        args += ["--window-k", str(pf["window_k"])]
    return args


# ---------------------------------------------------------------------------
# selftest:用 fixtures/cond-example-invalid.json 这份示例 COND 做校验
# ---------------------------------------------------------------------------
EXAMPLE_COND_PATH = Path(__file__).resolve().parent / "fixtures" / "cond-example-invalid.json"


def _load_example_cond() -> dict:
    """读示例 COND。它故意带占位符 digest/sha,必须被校验器拒收。"""
    return json.loads(EXAMPLE_COND_PATH.read_text(encoding="utf-8"))


def selftest() -> None:
    cond = _load_example_cond()
    errs = validate_cond(cond)
    # 示例里的 sha256 与镜像 digest 是全零占位符,应被规则 9 拒绝
    assert errs, "示例 COND 含占位符,预期校验失败"
    has_sha = any("sha256" in e for e in errs)
    has_img = any("images" in e and "占位符" in e for e in errs)
    assert has_sha, f"应检出 sha256 占位符,实际错误: {errs}"
    assert has_img, f"应检出镜像 digest 占位符,实际错误: {errs}"

    # 修正占位符后应通过。用一段含 16 个不同字符的 hex,
    # 既是真 sha256 的形状,又不会被占位符规则(单字符重复 / deadbeef)误判。
    real_hex = "0123456789abcdef" * 4
    good = json.loads(json.dumps(cond))
    good["trace"]["sha256"] = real_hex
    for k in good["environment"]["images"]:
        good["environment"]["images"][k] = good["environment"]["images"][k].replace(
            "0" * 64, real_hex)
    errs2 = validate_cond(good)
    assert not errs2, f"修正占位符后仍有错误: {errs2}"

    # aaaa… 这类单字符重复串同样必须被拒(docs/9.2 §C5 规则 9)
    ph = json.loads(json.dumps(good))
    ph["trace"]["sha256"] = "a" * 64
    errs_ph = validate_cond(ph)
    assert any("占位符" in e for e in errs_ph), errs_ph

    # 校验数值约束:dataset.bytes 与 shards*shard_size_bytes
    bad = json.loads(json.dumps(good))
    bad["dataset"]["bytes"] = 999999999999
    errs3 = validate_cond(bad)
    assert any("bytes" in e and "误差" in e for e in errs3), errs3

    # cache.enabled=false 仅 group=direct
    bad2 = json.loads(json.dumps(good))
    bad2["group"] = "traincache-cold"
    bad2["cache"]["enabled"] = False
    bad2["cache"].pop("capacity_bytes", None)
    bad2["cache"].pop("ratio", None)
    errs4 = validate_cond(bad2)
    assert any("enabled=false" in e for e in errs4), errs4

    # 组态词表(D30):7 个组态名全部合法;旧的 "traincache"/"fluid" 不再合法
    assert VALID_GROUPS == {
        "direct",
        "juicefs-cold", "juicefs-warm",
        "fluid-cold", "fluid-warm",
        "traincache-cold", "traincache-warm",
    }, VALID_GROUPS
    for g in sorted(VALID_GROUPS):
        ok_g = json.loads(json.dumps(good))
        ok_g["group"] = g
        if g == "direct":
            ok_g["cache"]["enabled"] = False
            ok_g["cache"].pop("capacity_bytes", None)
            ok_g["cache"].pop("ratio", None)
        errs_g = validate_cond(ok_g)
        assert not errs_g, f"group={g} 应通过: {errs_g}"
    for g in ("traincache", "fluid"):
        bad_g = json.loads(json.dumps(good))
        bad_g["group"] = g
        errs_g = validate_cond(bad_g)
        assert any("group" in e and "非法枚举" in e for e in errs_g), (g, errs_g)

    # 容量档位 capacity_ratio(任务 0.6/6.13,D19/D32):与 cache.ratio/capacity_bytes/
    # dataset.bytes/sampling.reps 交叉对账
    tier_ok = json.loads(json.dumps(good))
    tier_ok["figure"] = "compare-d"
    tier_ok["cache"]["capacity_ratio"] = "S"
    tier_ok["dataset"].update({"bytes": 2 << 30, "shards": 8,
                               "shard_size_bytes": 256 << 20})
    tier_ok["cache"]["ratio"] = 2.0
    tier_ok["cache"]["capacity_bytes"] = 1 << 30
    tier_ok["sampling"]["reps"] = 3
    errs5 = validate_cond(tier_ok)
    assert not errs5, f"S 档 COND 应通过: {errs5}"
    tenv = cond_to_env(tier_ok)
    assert tenv["CAPACITY_RATIO"] == "S" and tenv["COMPARE_TIER"] == "S", tenv

    # tier 与 ratio 不一致 → 报 capacity_ratio 错
    tier_bad = json.loads(json.dumps(tier_ok))
    tier_bad["cache"]["ratio"] = 8.0
    errs6 = validate_cond(tier_bad)
    assert any("capacity_ratio" in e for e in errs6), errs6

    # tier 与 reps 不一致(D32:S 档下限 3)→ 报 reps 错
    tier_bad2 = json.loads(json.dumps(tier_ok))
    tier_bad2["sampling"]["reps"] = 2
    errs7 = validate_cond(tier_bad2)
    assert any("reps" in e for e in errs7), errs7
    # S 档允许集合只含 3,5 应被拒
    tier_bad5 = json.loads(json.dumps(tier_ok))
    tier_bad5["sampling"]["reps"] = 5
    errs_bad5 = validate_cond(tier_bad5)
    assert any("reps" in e for e in errs_bad5), errs_bad5

    # 非法 tier 值 → 报错
    tier_bad3 = json.loads(json.dumps(tier_ok))
    tier_bad3["cache"]["capacity_ratio"] = "XL"
    errs8 = validate_cond(tier_bad3)
    assert any("capacity_ratio" in e for e in errs8), errs8

    # 越组(D30):group 必须落在 CAPACITY_TIER_GROUPS[capacity_ratio] 里
    tier_cross = json.loads(json.dumps(tier_ok))
    tier_cross["group"] = "fluid-warm"
    errs_cross = validate_cond(tier_cross)
    assert not errs_cross, f"S 档 + fluid-warm 应通过: {errs_cross}"
    tier_m = json.loads(json.dumps(tier_ok))
    tier_m["cache"]["capacity_ratio"] = "M"
    tier_m["dataset"].update({"bytes": 8 << 30, "shards": 32})
    tier_m["cache"]["ratio"] = 8.0
    tier_m["group"] = "juicefs-warm"
    errs_m = validate_cond(tier_m)
    assert any("参跑组" in e for e in errs_m), errs_m
    tier_l = json.loads(json.dumps(tier_ok))
    tier_l["cache"]["capacity_ratio"] = "L"
    tier_l["dataset"].update({"bytes": 32 << 30, "shards": 128})
    tier_l["cache"]["ratio"] = 32.0
    tier_l["sampling"]["reps"] = 1
    tier_l["group"] = "juicefs-cold"
    errs_l = validate_cond(tier_l)
    assert any("参跑组" in e for e in errs_l), errs_l

    # 带宽(D31):rate_mbit 只能是 100/300/1000;burst 由 rate 推导
    rate_bad = json.loads(json.dumps(good))
    rate_bad["network"]["rate_mbit"] = 500
    errs_rate = validate_cond(rate_bad)
    assert any("rate_mbit" in e for e in errs_rate), errs_rate
    for rate, burst in ((100, 1250), (300, 3750), (1000, 12500)):
        ok_rate = json.loads(json.dumps(good))
        ok_rate["network"]["rate_mbit"] = rate
        ok_rate["network"]["burst_k"] = burst
        errs_r = validate_cond(ok_rate)
        assert not errs_r, f"rate={rate}/burst={burst} 应通过: {errs_r}"
    burst_bad = json.loads(json.dumps(good))
    burst_bad["network"]["rate_mbit"] = 1000
    burst_bad["network"]["burst_k"] = 1600
    errs_burst = validate_cond(burst_bad)
    assert any("burst_k" in e for e in errs_burst), errs_burst

    # 淘汰策略(D9):W-TinyLFU 撤档,合法集合只剩 heat/lru/lfu/external
    assert VALID_EVICT_POLICIES == {"heat", "lru", "lfu", "external"}, \
        VALID_EVICT_POLICIES
    pol_bad = json.loads(json.dumps(good))
    pol_bad["cache"]["evict_policy"] = "wtinylfu"
    errs9 = validate_cond(pol_bad)
    assert any("evict_policy" in e for e in errs9), errs9

    # lookahead(D28):合法集合 8/16/32,旧的 2/4 已废
    assert VALID_LOOKAHEAD == {8, 16, 32}, VALID_LOOKAHEAD
    for la in sorted(VALID_LOOKAHEAD):
        ok_la = json.loads(json.dumps(good))
        ok_la["prefetch"]["lookahead"] = la
        errs_la = validate_cond(ok_la)
        assert not errs_la, f"lookahead={la} 应通过: {errs_la}"
    for la in (2, 4):
        bad_la = json.loads(json.dumps(good))
        bad_la["prefetch"]["lookahead"] = la
        errs_la = validate_cond(bad_la)
        assert any("lookahead" in e for e in errs_la), (la, errs_la)

    # cache.warmup_seconds 是"预算"(≥0 整数);负数拒绝
    ws_bad = json.loads(json.dumps(good))
    ws_bad["cache"]["warmup_seconds"] = -1
    errs_ws = validate_cond(ws_bad)
    assert any("warmup_seconds" in e for e in errs_ws), errs_ws

    # cond_to_env / cond_to_tc_args / cond_to_track_d_args 冒烟
    env = cond_to_env(good)
    assert env["TRAINCACHE_COND"] == good["name"]
    assert env["TRAINCACHE_PREFETCH_LOOKAHEAD"] == str(good["prefetch"]["lookahead"])
    assert "CAPACITY_RATIO" not in env, "未声明 capacity_ratio 时不应注入档位"
    tc = cond_to_tc_args(good)
    assert tc == ["100mbit", "20ms"], tc
    td = cond_to_track_d_args(good)
    assert "--epochs" in td and "--shuffle" in td and "--window-k" in td, td

    print("cond selftest: PASS")


if __name__ == "__main__":
    if "--selftest" in sys.argv or len(sys.argv) == 1:
        selftest()
