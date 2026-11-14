#!/usr/bin/env python3
"""COND 契约 v4：解析、校验与翻译。

一个 COND = 一个实验点。v4 只保留会变的量：族、负载形态、组态、数据集规模、
epoch、读端形态、带宽、缓存容量比 rho 与淘汰策略、采样次数、网关开关
（`gateway` 与 `gateway_cache`）。其余一律取本文件的默认值，实际生效值由每次
run 的 `lock.json` 记录。

字段全集与校验规则见 `docs/4.2-bench-families.md` 的 COND 契约一节；runner 遇到未知字段直接拒绝。
"""
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

SCHEMA_VERSION = "4"

# 族 ID 与产物目录同名；multiconsumer 族的 COND 名前缀用 multi-，与族 ID 不同。
VALID_FAMILIES = {
    "compare", "gateway", "evict", "prefetch", "warmup",
    "switch", "chaos", "multiconsumer", "e2e", "rangepar",
}
VALID_GROUPS = {
    "direct", "juicefs-cold", "juicefs-warm", "fluid-cold", "fluid-warm",
    "traincache", "traincache-cold", "traincache-warm",
}
TRAINCACHE_GROUPS = {"traincache", "traincache-cold", "traincache-warm"}
# 负载形态取代 v3 的 track：dataset 主数据集、tilt 倾斜访问、train 真实训练循环。
VALID_LOADS = {"dataset", "tilt", "train"}
VALID_SHUFFLES = {"fixed", "per-epoch", "curriculum"}
VALID_EVICT_POLICIES = {"heat", "lru", "lfu", "external"}
VALID_LOOKAHEAD = {8, 16, 32}
VALID_ACCESS_PATTERNS = {"single", "replicated", "sharded"}
MAX_READERS = config.MAX_READERS
VALID_RHO = set(config.RHO_VALUES)
VALID_RATES = set(config.TC_RATE_MBITS)
GATE_MODES = {"gated", "free"}

# v3 的字段名与它在 v4 里的替代者。旧 COND 落进来时要直接指出改哪个字段，
# 不能只报一句"未知字段"。
V3_FIELDS = {
    "figure": "family",
    "track": "load",
    "capacity_ratio": "cache.rho",
    "ratio": "cache.rho",
}

# 不再写进 COND 的固定量。改这里之前先想清楚：所有历史点会一起变。
CHUNK_SIZE_BYTES = 8388608             # bench 实验块粒度 8MiB(生产缺省 64MiB)
DEFAULTS = {
    "batch": 32,
    "workers": 4,
    "steps_per_epoch": config.EPOCH_STEPS,
    "prefetch_factor": 2,
    "compute_ms": config.COMPUTE_MS,
    "delay_ms": 20,
    "window_k": 8,
    "fanout_min": 1,
    "coarse_reps": 3,
    "bootstrap_iters": 10000,
    "kind_workers": 4,
    "k8s_version": "v1.37.0",
}

ALLOWED_TOP = {
    "schema_version", "name", "family", "group", "load",
    "dataset", "trace", "workload", "network", "cache",
    "prefetch", "sampling", "environment", "tags", "notes",
}
ALLOWED_DATASET = {"name", "shards", "shard_size_bytes", "seed"}
ALLOWED_TRACE = {"file", "sha256", "seed"}
ALLOWED_WORKLOAD = {"epochs", "shuffle", "curriculum", "readers", "access_pattern"}
ALLOWED_CURRICULUM = {"stages", "shard_fractions", "boundary_rule"}
ALLOWED_NETWORK = {"rate_mbit", "per_conn_mbit", "delay_ms", "loss_percent"}
ALLOWED_CACHE = {"enabled", "rho", "evict_policy", "warmup_seconds"}
ALLOWED_PREFETCH = {"lookahead", "fanout", "gate"}
ALLOWED_GATE = {"mode"}
ALLOWED_SAMPLING = {"reps"}
ALLOWED_ENVIRONMENT = {"gateway", "gateway_image", "image_set", "gateway_cache",
                       "fetch_range_parallel"}

_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_DIGEST_RE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")

CONDS_DIR_DEFAULT = Path(__file__).resolve().parent / "conds"
IMAGES_DIR = Path(__file__).resolve().parent / "images"


def burst_k_for_rate(rate_mbit: int) -> int:
    return int(config.burst_for_rate(rate_mbit).rstrip("k"))


def dataset_bytes(cond: dict) -> int:
    ds = cond["dataset"]
    return int(ds["shards"]) * int(ds["shard_size_bytes"])


def cache_bytes(cond: dict) -> int:
    """缓存容量字节。组态不带缓存时给 0，缓存容量只能由 rho 与数据集推导。"""
    cache = cond["cache"]
    if not cache.get("enabled"):
        return 0
    return int(dataset_bytes(cond) / float(cache["rho"]))


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


def _reject_v3(obj: dict, path: str, errors: list) -> None:
    for old, new in V3_FIELDS.items():
        if old in obj:
            errors.append(f"{path}.{old} 是 schema v3 字段，改用 {new}")


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


def _rho_ok(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return float(value) in VALID_RHO


def validate_cond(cond: dict) -> list[str]:
    errors: list[str] = []
    _check_unknown(cond, ALLOWED_TOP, "$", errors)
    _reject_v3(cond, "$", errors)
    if cond.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version 必须为 {SCHEMA_VERSION!r}"
                      f"(收到 {cond.get('schema_version')!r})")

    name = _require(cond, "name", "$", errors)
    if isinstance(name, str) and (len(name) > 80 or not _NAME_RE.match(name)):
        errors.append(f"$.name 不符合小写 kebab 规范: {name!r}")
    family = _require(cond, "family", "$", errors)
    if family is not None and family not in VALID_FAMILIES:
        errors.append(f"$.family 非法: {family!r}")
    group = _require(cond, "group", "$", errors)
    if group is not None and group not in VALID_GROUPS:
        errors.append(f"$.group 非法: {group!r}")
    load = _require(cond, "load", "$", errors)
    if load is not None and load not in VALID_LOADS:
        errors.append(f"$.load 非法: {load!r}")

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
            # train 负载读训练数据(16×128 MiB=2 GiB),其余读主数据集(4 GiB)。
            if load == "train":
                want = config.TRAIN_N_TARS * config.TRAIN_SHARD_BYTES
                label = f"{want}(2 GiB,训练数据 16×128 MiB)"
            else:
                want = config.DATASET_BYTES
                label = f"{want}(4 GiB)"
            if total != want:
                errors.append(f"shards×shard_size_bytes={total} 必须等于 {label}")

    # ---- trace ----
    # 倾斜负载按清单文件消费,清单本身要可复现,所以只有 load=tilt 需要记录。
    trace = cond.get("trace")
    if load == "tilt":
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
        if ep is not None and ep != config.EPOCHS:
            errors.append(f"$.workload.epochs={ep} 必须为 {config.EPOCHS}")
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
        # 单流上限(逐连接限速):0 表示不限单流,qdisc 形态是 根 tbf → netem;
        # 正值时形态多一层 fq(maxrate)。见 4.2 的并行下载族一节。
        per_conn = net.get("per_conn_mbit", 0)
        if not isinstance(per_conn, int) or isinstance(per_conn, bool) or per_conn < 0:
            errors.append("$.network.per_conn_mbit 必须是 ≥0 的整数(0 表示不限单流)")
        delay_ms = net.get("delay_ms", 20)
        if not isinstance(delay_ms, int) or isinstance(delay_ms, bool) or delay_ms < 0:
            errors.append("$.network.delay_ms 必须是 ≥0 的整数")
        loss = net.get("loss_percent", 0)
        if (not isinstance(loss, (int, float)) or isinstance(loss, bool)
                or not 0 <= loss <= 100):
            errors.append("$.network.loss_percent 必须在 0 到 100 之间")

    # ---- cache ----
    cache = _require(cond, "cache", "$", errors)
    if isinstance(cache, dict):
        _check_unknown(cache, ALLOWED_CACHE, "$.cache", errors)
        _reject_v3(cache, "$.cache", errors)
        enabled = cache.get("enabled")
        if not isinstance(enabled, bool):
            errors.append("$.cache.enabled 必须是 bool")
        if enabled:
            rho = _require(cache, "rho", "$.cache", errors)
            if rho is not None and not _rho_ok(rho):
                errors.append(f"$.cache.rho 非法: {rho!r}，只能取 {sorted(VALID_RHO)}")
            ev = _require(cache, "evict_policy", "$.cache", errors)
            if ev is not None and ev not in VALID_EVICT_POLICIES:
                errors.append(f"$.cache.evict_policy 非法: {ev!r}")
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
        # 预热族的轴就是门控开关，缺 gate.mode 的格子没有可比的实验条件。
        if family == "warmup" and not (isinstance(gate, dict) and gate.get("mode")):
            errors.append("warmup 族必须写 prefetch.gate.mode")

    # ---- sampling ----
    samp = _require(cond, "sampling", "$", errors)
    if isinstance(samp, dict):
        _check_unknown(samp, ALLOWED_SAMPLING, "$.sampling", errors)
        reps = _require(samp, "reps", "$.sampling", errors)
        want = config.E2E_REPS if family == "e2e" else config.REPS
        if reps is not None and reps != want:
            errors.append(f"$.sampling.reps={reps} 必须为 {want}"
                          f"（family={family}）")

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
        # D23：网关磁盘缓存开关。没写这个键按 false（只做在途合并），
        # 既有 COND 的口径不动；gateway=false 时它必须是 false。
        gw_cache = env.get("gateway_cache", False)
        if not isinstance(gw_cache, bool):
            errors.append("$.environment.gateway_cache 必须是 bool")
        elif gw is False and gw_cache:
            errors.append("gateway=false 时 gateway_cache 必须是 false")
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
        # Range 并行开关:只在 traincache 组出现,其他组态写它没有落点。
        if "fetch_range_parallel" in env:
            frp = env["fetch_range_parallel"]
            if not isinstance(frp, bool):
                errors.append("$.environment.fetch_range_parallel 必须是 bool")
            elif group not in TRAINCACHE_GROUPS:
                errors.append("$.environment.fetch_range_parallel 只在 traincache 组出现")

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
        # D23：磁盘缓存开关。没写这个键按 0（只做在途合并）。
        "TRAINCACHE_GATEWAY_CACHE":
            "1" if cond["environment"].get("gateway_cache") else "0",
    }
    cache = cond["cache"]
    if cache.get("enabled"):
        env["TRAINCACHE_EVICT_POLICY"] = str(cache["evict_policy"])
        # 缓存容量的唯一注入点：ρ 只在这里变成字节数，被测侧不认识 ρ。
        env["TRAINCACHE_CACHE_CAPACITY_BYTES"] = str(cache_bytes(cond))
    else:
        # 缓存关掉时也要显式下发 0：不下发的话装置会按缺省 ρ=4 给 1GiB，
        # 「agent 块缓存关」这一条就名不副实（网关族 G0 臂踩过这个坑）。
        env["TRAINCACHE_CACHE_CAPACITY_BYTES"] = "0"
    pf = cond.get("prefetch") or {}
    if cond["group"] in TRAINCACHE_GROUPS:
        if "lookahead" in pf:
            env["TRAINCACHE_PREFETCH_LOOKAHEAD"] = str(pf["lookahead"])
        mode = (pf.get("gate") or {}).get("mode")
        if mode:
            env["TRAINCACHE_GATE_MODE"] = str(mode)
        # Range 并行开关:并行下载族按格切开关,其余族不注入,停在清单默认。
        frp = cond["environment"].get("fetch_range_parallel")
        if frp is not None:
            env["TRAINCACHE_FETCH_RANGE_PARALLEL"] = "true" if frp else "false"
        # 预热族的窗口轴：门控允许等待的最长时间，写进 WarmupTask 的 gate.tMax。
        # 0 是「没设窗口」，不注入，让 up.sh 用缺省值。
        window = cache.get("warmup_seconds")
        if window:
            env["TRAINCACHE_GATE_TMAX"] = f"{int(window)}s"
    # 并行下载族的链路条件:时延、每连接上限、丢包率,经 up.sh 传给 tc_apply.sh。
    if cond["family"] == "rangepar":
        net = cond["network"]
        env["TC_DELAY"] = f"{int(net.get('delay_ms', 20))}ms"
        env["TC_PER_CONN_MBIT"] = str(int(net.get("per_conn_mbit", 0)))
        env["TC_LOSS_PERCENT"] = str(net.get("loss_percent", 0))
    return env


def cond_to_tc_args(cond: dict) -> list[str]:
    return [f"{cond['network']['rate_mbit']}mbit", f"{DEFAULTS['delay_ms']}ms"]


def cond_to_train_args(cond: dict) -> list[str]:
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
        "schema_version": "4",
        "name": "compare-rho2-r300-traincache-cold",
        "family": "compare",
        "group": "traincache-cold",
        "load": "dataset",
        "dataset": {"name": "dataset-4gib", "shards": 16,
                    "shard_size_bytes": 268435456, "seed": 20261006},
        "trace": {"file": "traces/x.json", "sha256": "0123456789abcdef" * 4,
                  "seed": 20261015},
        "workload": {"epochs": 3, "shuffle": "per-epoch",
                     "readers": 1, "access_pattern": "single"},
        "network": {"rate_mbit": 300},
        "cache": {"enabled": True, "rho": 2.0, "evict_policy": "heat"},
        "prefetch": {"lookahead": 16, "fanout": 2},
        "sampling": {"reps": 3},
        "environment": {"gateway": False, "gateway_image": None,
                        "gateway_cache": False,
                        "image_set": "sha256:" + "0" * 64},
        "tags": ["compare"],
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

    # 数据集固定 4 GiB：shards 与 shard_size_bytes 乘出来不是 4 GiB 就拒绝
    c = json.loads(json.dumps(good))
    c["dataset"]["shards"] = 15
    assert any("4 GiB" in e for e in validate_cond(c)), validate_cond(c)

    # train 负载读训练数据(16×128 MiB=2 GiB),不是主数据集的 4 GiB
    c = json.loads(json.dumps(good))
    c["load"] = "train"
    c["dataset"] = {"name": "train-e2e", "shards": 16,
                    "shard_size_bytes": 134217728, "seed": 20261015}
    assert not validate_cond(c), validate_cond(c)
    c["dataset"]["shard_size_bytes"] = 268435456  # 误用 4 GiB 主数据集口径
    assert any("2 GiB" in e for e in validate_cond(c)), validate_cond(c)

    # rho 只能取五个值
    c = json.loads(json.dumps(good))
    c["cache"]["rho"] = 3
    assert any("rho" in e for e in validate_cond(c)), validate_cond(c)
    for rho in config.RHO_VALUES:
        c["cache"]["rho"] = rho
        assert not validate_cond(c), (rho, validate_cond(c))
        assert cache_bytes(c) == config.cache_bytes(rho), (rho, cache_bytes(c))

    # epoch 与 reps 固定 3
    c = json.loads(json.dumps(good))
    c["workload"]["epochs"] = 1
    assert any("epochs" in e for e in validate_cond(c)), validate_cond(c)
    c["workload"]["epochs"] = 3
    c["sampling"]["reps"] = 1
    assert any("reps" in e for e in validate_cond(c)), validate_cond(c)

    # v3 字段要报出替代字段
    c = json.loads(json.dumps(good))
    c["figure"] = "compare-d"
    assert any("family" in e for e in validate_cond(c)), validate_cond(c)
    c = json.loads(json.dumps(good))
    c["cache"]["ratio"] = 2.0
    assert any("cache.rho" in e for e in validate_cond(c)), validate_cond(c)

    c = json.loads(json.dumps(good))
    c["environment"]["gateway"] = True
    assert any("gateway_image" in e for e in validate_cond(c)), validate_cond(c)

    # D23：gateway=false 且 gateway_cache=true 是非法 COND
    c = json.loads(json.dumps(good))
    c["environment"]["gateway"] = False
    c["environment"]["gateway_cache"] = True
    assert any("gateway_cache" in e for e in validate_cond(c)), validate_cond(c)
    c["environment"]["gateway_cache"] = False
    assert not validate_cond(c), validate_cond(c)
    # gateway=true 时 gateway_cache 可开可关，两个值都合法
    c = json.loads(json.dumps(good))
    c["environment"]["gateway"] = True
    c["environment"]["gateway_image"] = (
        "docker.io/library/x@sha256:" + "0123456789abcdef" * 4)
    c["environment"]["gateway_cache"] = True
    assert not validate_cond(c), validate_cond(c)
    c["environment"]["gateway_cache"] = False
    assert not validate_cond(c), validate_cond(c)
    # 非 bool 值非法
    c["environment"]["gateway_cache"] = "1"
    assert any("bool" in e for e in validate_cond(c)), validate_cond(c)
    # 没写这个键按 false，既有 COND 的口径不变
    c = json.loads(json.dumps(good))
    c["environment"].pop("gateway_cache", None)
    assert not validate_cond(c), validate_cond(c)

    # 预热族的轴是门控，缺 gate.mode 的 COND 无效
    c = json.loads(json.dumps(good))
    c["family"] = "warmup"
    assert any("gate" in e for e in validate_cond(c)), validate_cond(c)
    c["prefetch"]["gate"] = {"mode": "gated"}
    assert not validate_cond(c), validate_cond(c)

    assert dataset_bytes(good) == 4 << 30, dataset_bytes(good)
    assert cache_bytes(good) == 2 << 30, cache_bytes(good)
    env = cond_to_env(good)
    assert env["TRAINCACHE_CACHE_CAPACITY_BYTES"] == str(2 << 30), env
    assert env["TRAINCACHE_READERS"] == "1" and env["TRAINCACHE_GATEWAY"] == "0", env
    assert env["TRAINCACHE_GATEWAY_CACHE"] == "0", env
    cached = json.loads(json.dumps(good))
    cached["environment"]["gateway_cache"] = True
    assert cond_to_env(cached)["TRAINCACHE_GATEWAY_CACHE"] == "1"

    # conds/ 下每个文件都必须是 v4 且通过校验
    files = sorted(CONDS_DIR_DEFAULT.glob("*.json"))
    for path in files:
        cond = json.loads(path.read_text(encoding="utf-8"))
        assert cond.get("schema_version") == SCHEMA_VERSION, path.name
        errs = validate_cond(cond)
        assert not errs, (path.name, errs[:3])

    print(f"cond selftest: PASS（{len(files)} 个 COND 全部通过）")


if __name__ == "__main__":
    selftest()
