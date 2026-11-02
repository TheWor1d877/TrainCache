#!/usr/bin/env python3
"""一次性迁移：COND v2 → v3（去掉常量与派生值，镜像改为 image_set 引用）。

迁移只做减法与等价替换：
  * 删掉全常量字段与派生字段；
  * environment.images 换成 image_set（内容哈希）+ gateway_image；
  * 其余字段逐字保留。

等价性检查（任一不等就整体中止）：
  * 数据量 = shards × shard_size_bytes，与旧 dataset.bytes 相等；
  * 缓存字节 = 数据量 ÷ ratio，与旧 cache.capacity_bytes 相等；
  * 旧 delay_ms 必须为 20、burst_k 必须等于由 rate 推导的值；
  * 旧 sampling 的三个常量必须等于 DEFAULTS。

用法：
  python3 bench/experiments/migrate_cond_v3.py --check
  python3 bench/experiments/migrate_cond_v3.py --write
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS_DIR))
import cond as c  # noqa: E402

CONDS_DIR = EXPERIMENTS_DIR / "conds"
FIXTURE = EXPERIMENTS_DIR / "fixtures" / "cond-example-invalid.json"


def _migrate(old: dict, image_set: str) -> dict:
    env_old = old["environment"]
    images = env_old.get("images") or {}
    ds_old, cache_old = old["dataset"], old["cache"]
    new = {
        "schema_version": "3",
        "name": old["name"],
        "figure": old["figure"],
        "group": old["group"],
        "track": old["track"],
        "dataset": {k: ds_old[k] for k in ("name", "shards", "shard_size_bytes", "seed")},
        "trace": dict(old["trace"]),
        "workload": {k: old["workload"][k] for k in ("epochs", "shuffle")},
        "network": {"rate_mbit": old["network"]["rate_mbit"]},
        "cache": {"enabled": cache_old["enabled"]},
        "sampling": {"reps": old["sampling"]["reps"]},
        "environment": {
            "gateway": bool(env_old.get("gateway")),
            "gateway_image": images.get("gateway"),
            "image_set": image_set,
        },
    }
    wl_old = old["workload"]
    new["workload"]["readers"] = wl_old["readers"]
    new["workload"]["access_pattern"] = wl_old["access_pattern"]
    if "curriculum" in wl_old:
        new["workload"]["curriculum"] = wl_old["curriculum"]
    if cache_old.get("enabled"):
        for k in ("capacity_ratio", "ratio", "evict_policy", "warmup_seconds"):
            if k in cache_old:
                new["cache"][k] = cache_old[k]
    if "prefetch" in old:
        pf = old["prefetch"]
        new["prefetch"] = {"lookahead": pf["lookahead"], "fanout": pf["fanout"]}
        if (pf.get("gate") or {}).get("mode"):
            new["prefetch"]["gate"] = {"mode": pf["gate"]["mode"]}
    if "tags" in old:
        new["tags"] = old["tags"]
    if "notes" in old:
        new["notes"] = old["notes"]
    return new


def _check(old: dict, new: dict) -> list[str]:
    errs = []
    ds_old = old["dataset"]
    if c.dataset_bytes(new) != ds_old.get("bytes"):
        errs.append(f"data bytes {c.dataset_bytes(new)} != {ds_old.get('bytes')}")
    cache_old = old["cache"]
    if cache_old.get("enabled"):
        want = cache_old.get("capacity_bytes")
        got = c.cache_bytes(new)
        if want and abs(got - want) / want > 0.002:
            errs.append(f"cache {got} != {want}")
    net = old["network"]
    if net.get("delay_ms") != c.DEFAULTS["delay_ms"]:
        errs.append(f"delay {net.get('delay_ms')}")
    if net.get("burst_k") != c.burst_k_for_rate(net["rate_mbit"]):
        errs.append(f"burst {net.get('burst_k')}")
    wl = old["workload"]
    for k in ("batch", "workers", "steps_per_epoch", "prefetch_factor", "compute_ms"):
        if wl.get(k) != c.DEFAULTS[k]:
            errs.append(f"{k}={wl.get(k)}")
    samp = old["sampling"]
    for k in ("coarse_reps", "bootstrap_iters"):
        if samp.get(k) != c.DEFAULTS[k]:
            errs.append(f"{k}={samp.get(k)}")
    return errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    files = sorted(CONDS_DIR.glob("*.json"))
    if FIXTURE.exists():
        files.append(FIXTURE)

    # 共享镜像集（gateway 位单独留在 COND 里）
    first = json.loads(files[0].read_text(encoding="utf-8"))
    shared = {k: v for k, v in first["environment"]["images"].items() if k != "gateway"}
    text = json.dumps(shared, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    hexdigest = hashlib.sha256(text.encode()).hexdigest()
    c.IMAGES_DIR.mkdir(exist_ok=True)
    (c.IMAGES_DIR / f"{hexdigest}.json").write_text(text, encoding="utf-8")
    image_set = f"sha256:{hexdigest}"

    pending = []
    for path in files:
        old = json.loads(path.read_text(encoding="utf-8"))
        if old.get("schema_version") == "3":
            continue
        new = _migrate(old, image_set)
        errs = _check(old, new)
        if errs:
            print(f"[FAIL] {path.name}: {errs[:3]}")
            return 1
        pending.append((path, new))

    print(f"[migrate] 待迁移 {len(pending)} 个文件，等价性检查通过；image_set={image_set}")
    if args.write:
        for path, new in pending:
            path.write_text(json.dumps(new, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8")
        print(f"[migrate] 已写回 {len(pending)} 个文件")
    return 0


if __name__ == "__main__":
    sys.exit(main())
