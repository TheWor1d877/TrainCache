#!/usr/bin/env python3
"""COND 契约 v3：解析、校验与翻译。

一个 COND = 一个实验点。v3 只保留会变的量：数据规模、轨迹、epoch、读端形态、
带宽、缓存比例与策略、采样次数、网关开关。其余一律取本文件的默认值，实际生效
值由每次 run 的 `lock.json` 记录。

字段全集与校验规则见 `docs/4.4 实验协议.md` 附录 C；runner 遇到未知字段直接拒绝。
"""
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

SCHEMA_VERSION = "3"

VALID_FIGURES = {
    "fig1", "fig2", "fig3", "pareto1", "pareto2",
    "compare-d", "compare-b", "policy", "gateway-merge",
}
VALID_GROUPS = {
    "direct", "juicefs-cold", "juicefs-warm", "fluid-cold", "fluid-warm",
    "traincache-cold", "traincache-warm",
}
TRAINCACHE_GROUPS = {"traincache-cold", "traincache-warm"}
VALID_TRACKS = {"B", "C", "D"}
VALID_SHUFFLES = {"fixed", "per-epoch", "curriculum"}
VALID_EVICT_POLICIES = {"heat", "lru", "lfu", "external"}
VALID_LOOKAHEAD = {8, 16, 32}
VALID_ACCESS_PATTERNS = {"single", "replicated", "sharded"}
MAX_READERS = 4
VALID_CAPACITY_RATIOS = {"S", "M", "L"}
VALID_EPOCHS = {1, 3, 6}
VALID_RATES = set(config.TC_RATE_MBITS)
GATE_MODES = {"gated", "free"}

# 不再写进 COND 的固定量。改这里之前先想清楚：所有历史点会一起变。
CHUNK_SIZE_BYTES = 8388608
DEFAULTS = {
    "batch": 32,
    "workers": 4,
    "steps_per_epoch": 400,
    "prefetch_factor": 2,
    "compute_ms": 200,
    "delay_ms": 20,
    "window_k": 8,
    "fanout_min": 1,
    "coarse_reps": 3,
    "bootstrap_iters": 10000,
    "kind_workers": 4,
    "k8s_version": "v1.37.0",
}

COMPARE_CACHE_BYTES = 1 << 30
COMPARE_SHARD_SIZE_BYTES = config.TRACK_B_TAR_BYTES
CAPACITY_TIER_RATIO = {"S": 2.0, "M": 8.0, "L": 32.0}
CAPACITY_TIER_N_TARS = {
    "S": config.TRACK_B_N_TARS // 4,
    "M": config.TRACK_B_N_TARS,
    "L": config.TRACK_B_N_TARS * 4,
}
CAPACITY_TIER_REPS = {"S": 3, "M": 3, "L": 1}
CAPACITY_TIER_REPS_ALLOWED = {"S": {3}, "M": {3}, "L": {1}}
CAPACITY_TIER_GROUPS = {
    "S": {"direct", "juicefs-cold", "juicefs-warm",
          "fluid-cold", "fluid-warm", "traincache-cold", "traincache-warm"},
    "M": {"direct", "fluid-cold", "traincache-cold"},
    "L": {"traincache-cold", "fluid-cold"},
}
CAPACITY_TIER_EPOCHS = {"S": 3, "M": 1, "L": 1}
EXPLORATORY_TIERS = {"L"}
VALID_DATASET_BYTES = {2 << 30, 8 << 30, 32 << 30}

ALLOWED_TOP = {
    "schema_version", "name", "figure", "group", "track",
    "dataset", "trace", "workload", "network", "cache",
    "prefetch", "sampling", "environment", "tags", "notes",
}
ALLOWED_DATASET = {"name", "shards", "shard_size_bytes", "seed"}
ALLOWED_TRACE = {"file", "sha256", "seed"}
ALLOWED_WORKLOAD = {"epochs", "shuffle", "curriculum", "readers", "access_pattern"}
ALLOWED_CURRICULUM = {"stages", "shard_fractions", "boundary_rule"}
ALLOWED_NETWORK = {"rate_mbit"}
ALLOWED_CACHE = {"enabled", "capacity_ratio", "ratio", "evict_policy",
                 "warmup_seconds"}
ALLOWED_PREFETCH = {"lookahead", "fanout", "gate"}
ALLOWED_GATE = {"mode"}
ALLOWED_SAMPLING = {"reps"}
ALLOWED_ENVIRONMENT = {"gateway", "gateway_image", "image_set"}

_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_DIGEST_RE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")

CONDS_DIR_DEFAULT = Path(__file__).resolve().parent / "conds"
IMAGES_DIR = Path(__file__).resolve().parent / "images"


def tier_dataset_bytes(tier: str) -> int:
    return CAPACITY_TIER_N_TARS[tier] * COMPARE_SHARD_SIZE_BYTES


def tier_capacity_bytes(tier: str) -> int:
    return COMPARE_CACHE_BYTES


def burst_k_for_rate(rate_mbit: int) -> int:
    return int(config.burst_for_rate(rate_mbit).rstrip("k"))


def dataset_bytes(cond: dict) -> int:
    ds = cond["dataset"]
    return int(ds["shards"]) * int(ds["shard_size_bytes"])


def cache_bytes(cond: dict) -> int:
    cache = cond["cache"]
    if not cache.get("enabled"):
        return 0
    tier = cache.get("capacity_ratio")
    if tier:
        return COMPARE_CACHE_BYTES
    ratio = float(cache.get("ratio") or 0)
    return int(dataset_bytes(cond) / ratio) if ratio > 0 else 0


def image_set_path(image_set: str) -> Path:
    """image_set 形如 `sha256:<hex>`，对应 images/<hex>.json。"""
    hexpart = str(image_set).split("sha256:", 1)[-1]
    return IMAGES_DIR / f"{hexpart}.json"


def resolve_images(cond: dict) -> dict[str, str | None]:
    """展开 image_set 文件，并把 gateway 镜像位放回去。"""
    env = cond["environment"]
    images: dict[str, str | None] = json.loads(
        image_set_path(env["image_set"]).read_text(encoding="utf-8"))
    images["gateway"] = env.get("gateway_image")
    return images


def load_cond(name: str, conds_dir: Path | None = None) -> dict:
    base = Path(conds_dir) if conds_dir else CONDS_DIR_DEFAULT
    path = base / f"{name}.json"
    if not path.is_file():
        raise FileNotFoundError(f"COND 文件不存在: {path}")
    cond = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(cond, dict):
        raise ValueError(f"COND 顶层不是对象: {path}")
    return cond


def _check_unknown(obj: dict, allowed: set, path: str, errors: list) -> None:
    unknown = set(obj.keys()) - allowed
    if unknown:
        errors.append(f"{path} 含未知字段: {sorted(unknown)}")


def _require(obj: dict, key: str, path: str, errors: list) -> Any:
    if key not in obj or obj[key] is None or obj[key] == "":
        errors.append(f"{path}.{key} 缺失或为空")
        return None
    return obj[key]


def _digest_ok(value: Any) -> bool:
    if not isinstance(value, str) or not _IMAGE_DIGEST_RE.match(value):
        return False
    hexpart = value.split("sha256:", 1)[1]
    return not (len(set(hexpart)) == 1 or hexpart.startswith("deadbeef")
                or hexpart == "0" * 64)


def validate_cond(cond: dict) -> list[str]:
    errors: list[str] = []
    _check_unknown(cond, ALLOWED_TOP, "$", errors)
    if cond.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version 必须为 {SCHEMA_VERSION!r}"
                      f"(收到 {cond.get('schema_version')!r})")

    name = _require(cond, "name", "$", errors)
    if isinstance(name, str) and (len(name) > 80 or not _NAME_RE.match(name)):
        errors.append(f"$.name 不符合小写 kebab 规范: {name!r}")
    figure = _require(cond, "figure", "$", errors)
    if figure is not None and figure not in VALID_FIGURES:
        errors.append(f"$.figure 非法: {figure!r}")
    group = _require(cond, "group", "$", errors)
    if group is not None and group not in VALID_GROUPS:
        errors.append(f"$.group 非法: {group!r}")
    track = _require(cond, "track", "$", errors)
    if track is not None and track not in VALID_TRACKS:
        errors.append(f"$.track 非法: {track!r}")

    # ---- dataset ----
    ds = _require(cond, "dataset", "$", errors)
    if isinstance(ds, dict):
        _check_unknown(ds, ALLOWED_DATASET, "$.dataset", errors)
        if not isinstance(_require(ds, "name", "$.dataset", errors), str):
            errors.append("$.dataset.name 必须是字符串")
        for k in ("shards", "shard_size_bytes", "seed"):
            v = _require(ds, k, "$.dataset", errors)
            if v is not None and (not isinstance(v, int) or v <= 0):
                errors.append(f"$.dataset.{k} 必须是 >0 的整数")
        if all(isinstance(ds.get(k), int) and ds.get(k) > 0
               for k in ("shards", "shard_size_bytes")):
            total = ds["shards"] * ds["shard_size_bytes"]
            if total not in VALID_DATASET_BYTES:
                errors.append(f"shards×shard_size_bytes={total} 不在 2/8/32 GiB 枚举内")

    # ---- trace ----
    trace = _require(cond, "trace", "$", errors)
    if isinstance(trace, dict):
        _check_unknown(trace, ALLOWED_TRACE, "$.trace", errors)
        _require(trace, "file", "$.trace", errors)
        sha = _require(trace, "sha256", "$.trace", errors)
        if isinstance(sha, str) and (not _SHA256_RE.match(sha) or sha == "0" * 64):
            errors.append("$.trace.sha256 必须是 64 位小写 hex，且不能全零")
        seed = _require(trace, "seed", "$.trace", errors)
        if seed is not None and (not isinstance(seed, int) or seed <= 0):
            errors.append("$.trace.seed 必须是 >0 的整数")

    # ---- workload ----
    wl = _require(cond, "workload", "$", errors)
    if isinstance(wl, dict):
        _check_unknown(wl, ALLOWED_WORKLOAD, "$.workload", errors)
        ep = _require(wl, "epochs", "$.workload", errors)
        if ep is not None and ep not in VALID_EPOCHS:
            errors.append(f"$.workload.epochs={ep} 不在 1/3/6 内")
        shuffle = _require(wl, "shuffle", "$.workload", errors)
        if shuffle is not None and shuffle not in VALID_SHUFFLES:
            errors.append(f"$.workload.shuffle 非法: {shuffle!r}")
        if shuffle == "curriculum":
            cur = _require(wl, "curriculum", "$.workload", errors)
            if isinstance(cur, dict):
                _check_unknown(cur, ALLOWED_CURRICULUM, "$.workload.curriculum", errors)
                if _require(cur, "stages", "$.workload.curriculum", errors) != 3:
                    errors.append("$.workload.curriculum.stages 必须为 3")
                if _require(cur, "boundary_rule", "$.workload.curriculum",
                            errors) != "equal-thirds":
                    errors.append("$.workload.curriculum.boundary_rule 必须为 equal-thirds")
        readers = _require(wl, "readers", "$.workload", errors)
        pattern = _require(wl, "access_pattern", "$.workload", errors)
        if readers is not None and (not isinstance(readers, int)
                                    or not 1 <= readers <= MAX_READERS):
            errors.append(f"$.workload.readers 必须是 1..{MAX_READERS}")
        if pattern is not None and pattern not in VALID_ACCESS_PATTERNS:
            errors.append(f"$.workload.access_pattern 非法: {pattern!r}")
        if isinstance(readers, int) and pattern in VALID_ACCESS_PATTERNS:
            if readers == 1 and pattern != "single":
                errors.append("readers=1 时 access_pattern 必须是 single")
            if readers > 1 and pattern == "single":
                errors.append("readers>1 时 access_pattern 不能是 single")
            if (pattern == "sharded" and isinstance(ds, dict)
                    and isinstance(ds.get("shards"), int)
                    and ds["shards"] % readers != 0):
                errors.append("sharded 要求 shards 能被 readers 整除")

    # ---- network ----
    net = _require(cond, "network", "$", errors)
    if isinstance(net, dict):
        _check_unknown(net, ALLOWED_NETWORK, "$.network", errors)
        rate = _require(net, "rate_mbit", "$.network", errors)
        if rate is not None and rate not in VALID_RATES:
            errors.append(f"$.network.rate_mbit 非法: {rate!r}")

    # ---- cache ----
    cache = _require(cond, "cache", "$", errors)
    if isinstance(cache, dict):
        _check_unknown(cache, ALLOWED_CACHE, "$.cache", errors)
        enabled = cache.get("enabled")
        if not isinstance(enabled, bool):
            errors.append("$.cache.enabled 必须是 bool")
        if enabled:
            ratio = _require(cache, "ratio", "$.cache", errors)
            if ratio is not None and (not isinstance(ratio, (int, float)) or ratio <= 0):
                errors.append("$.cache.ratio 必须是正数")
            ev = _require(cache, "evict_policy", "$.cache", errors)
            if ev is not None and ev not in VALID_EVICT_POLICIES:
                errors.append(f"$.cache.evict_policy 非法: {ev!r}")
            tier = cache.get("capacity_ratio")
            if tier is not None:
                if tier not in VALID_CAPACITY_RATIOS:
                    errors.append(f"$.cache.capacity_ratio 非法: {tier!r}")
                else:
                    if figure not in {"compare-d", "compare-b", "gateway-merge"}:
                        errors.append("capacity_ratio 只允许 compare-*/gateway-merge")
                    if group is not None and group not in CAPACITY_TIER_GROUPS[tier]:
                        errors.append(f"group={group!r} 不在 {tier} 档参跑组内")
                    if isinstance(ratio, (int, float)) and abs(
                            ratio - CAPACITY_TIER_RATIO[tier]) > 0.01:
                        errors.append(f"capacity_ratio={tier} 要求 ratio="
                                      f"{CAPACITY_TIER_RATIO[tier]}")
                    if isinstance(ds, dict) and all(
                            isinstance(ds.get(k), int) and ds.get(k) > 0
                            for k in ("shards", "shard_size_bytes")):
                        if dataset_bytes(cond) != tier_dataset_bytes(tier):
                            errors.append(f"capacity_ratio={tier} 要求数据量为 "
                                          f"{tier_dataset_bytes(tier)}")
        ws = cache.get("warmup_seconds")
        if ws is not None and (not isinstance(ws, int) or ws < 0):
            errors.append("$.cache.warmup_seconds 必须是 ≥0 的整数")

    # ---- prefetch ----
    prefetch = cond.get("prefetch")
    if group in TRAINCACHE_GROUPS:
        prefetch = _require(cond, "prefetch", "$", errors)
    if isinstance(prefetch, dict):
        _check_unknown(prefetch, ALLOWED_PREFETCH, "$.prefetch", errors)
        la = _require(prefetch, "lookahead", "$.prefetch", errors)
        if la is not None and la not in VALID_LOOKAHEAD:
            errors.append(f"$.prefetch.lookahead 非法: {la!r}")
        fan = _require(prefetch, "fanout", "$.prefetch", errors)
        if fan is not None and (not isinstance(fan, int) or fan < 1):
            errors.append("$.prefetch.fanout 必须是 ≥1 的整数")
        gate = prefetch.get("gate")
        if gate is not None:
            _check_unknown(gate, ALLOWED_GATE, "$.prefetch.gate", errors)
            if gate.get("mode") not in GATE_MODES:
                errors.append(f"$.prefetch.gate.mode 非法: {gate.get('mode')!r}")
        if figure == "fig1" and not (isinstance(gate, dict) and gate.get("mode")):
            errors.append("fig1 必须写 prefetch.gate.mode")

    # ---- sampling ----
    samp = _require(cond, "sampling", "$", errors)
    if isinstance(samp, dict):
        _check_unknown(samp, ALLOWED_SAMPLING, "$.sampling", errors)
        reps = _require(samp, "reps", "$.sampling", errors)
        tier = (cache or {}).get("capacity_ratio")
        if figure == "pareto1":
            allowed = {7}
        elif figure == "pareto2":
            allowed = {2}
        elif figure in {"compare-d", "compare-b", "gateway-merge"}:
            allowed = CAPACITY_TIER_REPS_ALLOWED[tier] if tier else {3}
        else:
            allowed = {1}
        if reps is not None and reps not in allowed:
            errors.append(f"$.sampling.reps 必须为 {sorted(allowed)}"
                          f"(figure={figure},收到 {reps})")

    # ---- environment ----
    env = _require(cond, "environment", "$", errors)
    if isinstance(env, dict):
        _check_unknown(env, ALLOWED_ENVIRONMENT, "$.environment", errors)
        gw = _require(env, "gateway", "$.environment", errors)
        if gw is not None and not isinstance(gw, bool):
            errors.append("$.environment.gateway 必须是 bool")
        gw_img = env.get("gateway_image")
        if gw is True and not _digest_ok(gw_img):
            errors.append("gateway=true 时 gateway_image 必须是真实 digest")
        if gw is False and gw_img is not None:
            errors.append("gateway=false 时 gateway_image 必须是 null")
        image_set = _require(env, "image_set", "$.environment", errors)
        if isinstance(image_set, str):
            hexpart = image_set.split("sha256:", 1)[-1]
            path = image_set_path(image_set)
            if not image_set.startswith("sha256:") or not _SHA256_RE.match(hexpart):
                errors.append("$.environment.image_set 必须是 sha256:<64 hex>")
            elif not path.is_file():
                errors.append(f"image_set 对应的文件不存在: {path.name}")
            elif hashlib.sha256(path.read_bytes()).hexdigest() != hexpart:
                errors.append(f"image_set 与 {path.name} 的内容哈希不一致")

    # ---- tags / notes ----
    tags = cond.get("tags")
    if tags is not None and (not isinstance(tags, list)
                             or any(not isinstance(t, str) or len(t) > 32
                                    for t in tags)):
        errors.append("$.tags 必须是 ≤32 字符的字符串数组")
    notes = cond.get("notes")
    if notes is not None and (not isinstance(notes, str) or len(notes) > 500):
        errors.append("$.notes 必须是 ≤500 字符的字符串")

    return errors


def cond_to_env(cond: dict) -> dict[str, str]:
    """COND → 环境变量。TRAINCACHE_JOB_CREATED_AT 由 runner 另外注入。"""
    env: dict[str, str] = {
        "TRAINCACHE_COND": cond["name"],
        "TRAINCACHE_GROUP": cond["group"],
        "TRAINCACHE_DATASET": cond["dataset"]["name"],
        "TRAINCACHE_READERS": str(cond["workload"]["readers"]),
        "TRAINCACHE_ACCESS_PATTERN": cond["workload"]["access_pattern"],
        "TRAINCACHE_GATEWAY": "1" if cond["environment"].get("gateway") else "0",
    }
    cache = cond["cache"]
    if cache.get("enabled"):
        env["TRAINCACHE_EVICT_POLICY"] = str(cache["evict_policy"])
        env["TRAINCACHE_CACHE_CAPACITY_BYTES"] = str(cache_bytes(cond))
    tier = cache.get("capacity_ratio")
    if tier:
        env["CAPACITY_RATIO"] = str(tier)
        env["COMPARE_TIER"] = str(tier)
    pf = cond.get("prefetch") or {}
    if cond["group"] in TRAINCACHE_GROUPS:
        if "lookahead" in pf:
            env["TRAINCACHE_PREFETCH_LOOKAHEAD"] = str(pf["lookahead"])
        mode = (pf.get("gate") or {}).get("mode")
        if mode:
            env["TRAINCACHE_GATE_MODE"] = str(mode)
    return env


def cond_to_tc_args(cond: dict) -> list[str]:
    return [f"{cond['network']['rate_mbit']}mbit", f"{DEFAULTS['delay_ms']}ms"]


def cond_to_track_d_args(cond: dict) -> list[str]:
    wl = cond["workload"]
    return [
        "--epochs", str(wl["epochs"]),
        "--seed", str(cond["dataset"]["seed"]),
        "--workers", str(DEFAULTS["workers"]),
        "--batch", str(DEFAULTS["batch"]),
        "--steps-per-epoch", str(DEFAULTS["steps_per_epoch"]),
        "--shuffle", wl["shuffle"],
        "--window-k", str(DEFAULTS["window_k"]),
    ]


EXAMPLE_COND_PATH = Path(__file__).resolve().parent / "fixtures" / "cond-example-invalid.json"


def _sample_cond() -> dict:
    return {
        "schema_version": "3",
        "name": "sample-cond",
        "figure": "compare-d",
        "group": "traincache-cold",
        "track": "D",
        "dataset": {"name": "compare-data-8m", "shards": 8,
                    "shard_size_bytes": 268435456, "seed": 20261006},
        "trace": {"file": "traces/x.json", "sha256": "0123456789abcdef" * 4,
                  "seed": 20261015},
        "workload": {"epochs": 3, "shuffle": "per-epoch",
                     "readers": 1, "access_pattern": "single"},
        "network": {"rate_mbit": 100},
        "cache": {"enabled": True, "capacity_ratio": "S", "ratio": 2.0,
                  "evict_policy": "heat"},
        "prefetch": {"lookahead": 16, "fanout": 2},
        "sampling": {"reps": 3},
        "environment": {"gateway": False, "gateway_image": None,
                        "image_set": "sha256:" + "0" * 64},
        "tags": ["compare-d"],
    }


def selftest() -> None:
    good = _sample_cond()
    errs = validate_cond(good)
    assert any("image_set" in e for e in errs), errs

    real = next(IMAGES_DIR.glob("*.json"), None)
    assert real is not None, "images/ 下没有 image_set 文件"
    good["environment"]["image_set"] = "sha256:" + hashlib.sha256(
        real.read_bytes()).hexdigest()
    assert not validate_cond(good), validate_cond(good)

    c = json.loads(json.dumps(good))
    c["workload"]["access_pattern"] = "replicated"
    assert any("readers=1" in e for e in validate_cond(c)), validate_cond(c)
    c = json.loads(json.dumps(good))
    c["workload"].update({"readers": 4, "access_pattern": "sharded"})
    assert not validate_cond(c), validate_cond(c)

    c["dataset"]["shards"] = 6
    assert any("整除" in e for e in validate_cond(c)), validate_cond(c)
    assert any("枚举" in e for e in validate_cond(c)), validate_cond(c)

    c = json.loads(json.dumps(good))
    c["environment"]["gateway"] = True
    assert any("gateway_image" in e for e in validate_cond(c)), validate_cond(c)

    assert dataset_bytes(good) == 2 << 30, dataset_bytes(good)
    assert cache_bytes(good) == 1 << 30, cache_bytes(good)
    env = cond_to_env(good)
    assert env["TRAINCACHE_CACHE_CAPACITY_BYTES"] == str(1 << 30), env
    assert env["TRAINCACHE_READERS"] == "1" and env["TRAINCACHE_GATEWAY"] == "0", env

    files = sorted(CONDS_DIR_DEFAULT.glob("*.json"))
    assert files, "conds 目录为空"
    for path in files:
        cond = json.loads(path.read_text(encoding="utf-8"))
        errs = validate_cond(cond)
        assert not errs, (path.name, errs[:3])

    print(f"cond selftest: PASS（{len(files)} 个 COND）")


if __name__ == "__main__":
    selftest()
