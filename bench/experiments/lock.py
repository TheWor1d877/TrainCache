#!/usr/bin/env python3
"""M2 环境锁定:采集 kubectl/docker/git/tc/seed 等环境信息,写 lock.json。

命令失败时对应字段写 null,不抛异常。
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

EXPERIMENTS_DIR = Path(__file__).resolve().parent
CONDS_DIR = EXPERIMENTS_DIR / "conds"
sys.path.insert(0, str(EXPERIMENTS_DIR))
import cond as cond_mod  # noqa: E402


def _run(cmd: list[str], timeout: int = 30) -> str | None:
    """执行命令,返回 stdout;失败返回 None。shell=False 列表形式。"""
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False)
        if proc.returncode != 0:
            return None
        return proc.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return None


def _kubectl_version() -> dict | None:
    """kubectl version -o json → {client, server}。"""
    out = _run(["kubectl", "version", "-o", "json"])
    if not out:
        return None
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return None
    result: dict[str, Any] = {}
    client = data.get("clientVersion")
    if client:
        result["client"] = {
            "major": client.get("major"),
            "minor": client.get("minor"),
            "gitVersion": client.get("gitVersion"),
            "gitCommit": client.get("gitCommit"),
        }
    server = data.get("serverVersion")
    if server:
        result["server"] = {
            "major": server.get("major"),
            "minor": server.get("minor"),
            "gitVersion": server.get("gitVersion"),
            "gitCommit": server.get("gitCommit"),
        }
    return result or None


def _docker_image_digest(image_ref: str) -> str | None:
    """docker inspect <image> → RepoDigests 第一个(sha256:...)。"""
    out = _run(["docker", "inspect", image_ref], timeout=20)
    if not out:
        return None
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, list) or not data:
        return None
    digests = data[0].get("RepoDigests") or []
    if digests:
        return digests[0]
    # 本地 build 的镜像没有 RepoDigests。此处绝不能回退去取 Id:Id 是 docker 的
    # config digest,与 `repo:tag@sha256:` 语义要求的 manifest digest 是两个值
    # (4.2 的 COND 契约一节 核出)。返回 None 让调用方保留 COND 里记录的真值。
    return None


def _docker_images(cond: dict | None) -> dict[str, str | None]:
    """各镜像的在役标识:主值取 COND `environment.images`(现取自集群
    containerd 的 manifest digest,见 4.2 的 COND 契约一节),另附 docker 侧的 RepoDigests
    供交叉核对——两者对不上是预期的(本地 build 的镜像没有 RepoDigests;
    拉下来的镜像 containerd 存的是 kind load 后本地重算的摘要)。"""
    images_cfg = {}
    if cond and isinstance(cond.get("environment"), dict):
        try:
            images_cfg = cond_mod.resolve_images(cond)
        except (OSError, ValueError, KeyError):
            images_cfg = {}
    result: dict[str, str | None] = {}
    for key, ref in images_cfg.items():
        if not isinstance(ref, str):
            result[key] = None
            continue
        result[key] = ref
    return result


def _docker_repo_digests(cond: dict | None) -> dict[str, str | None]:
    """docker inspect <repo:tag> 的 RepoDigests[0];拿不到写 None。"""
    images_cfg = {}
    if cond and isinstance(cond.get("environment"), dict):
        try:
            images_cfg = cond_mod.resolve_images(cond)
        except (OSError, ValueError, KeyError):
            images_cfg = {}
    result: dict[str, str | None] = {}
    for key, ref in images_cfg.items():
        if not isinstance(ref, str):
            result[key] = None
            continue
        result[key] = _docker_image_digest(ref.split("@")[0] if "@" in ref else ref)
    return result


def _tc_tier(cond: dict | None) -> dict | None:
    """当前 tc 档位:从 COND network 记录(实际值由 tc_apply.sh 保证)。"""
    if not cond or not isinstance(cond.get("network"), dict):
        return None
    net = cond["network"]
    return {
        "rate_mbit": net.get("rate_mbit"),
        "delay_ms": net.get("delay_ms"),
        "burst_k": net.get("burst_k"),
    }


def _seed(cond: dict | None) -> int | None:
    """随机种子:从 COND dataset.seed 记录。"""
    if not cond or not isinstance(cond.get("dataset"), dict):
        return None
    return cond["dataset"].get("seed")


def _chunk_size_bytes(cond: dict | None) -> int | None:
    """实验块大小:从 COND dataset.chunk_size_bytes 记录(正式实验 8388608,
    即 8 MiB,D20;生产默认 67108864 不进 COND)。"""
    if not cond or not isinstance(cond.get("dataset"), dict):
        return None
    return cond["dataset"].get("chunk_size_bytes")


def _dataset_name() -> str:
    """TrainCache 快照/数据集名:环境变量 DATASET_NAME,默认 compare-data-8m,
    与 run_bench.py 同源;块大小脱离所属快照无意义,两者一起进清单。"""
    return os.environ.get("DATASET_NAME") or "compare-data-8m"


def _git_commit() -> str | None:
    """git rev-parse HEAD。"""
    return _run(["git", "rev-parse", "HEAD"])


def _load_cond_from_env() -> dict | None:
    """从环境变量 TRAINCACHE_COND 读取 COND 名并加载。"""
    name = os.environ.get("TRAINCACHE_COND", "").strip()
    if not name:
        return None
    path = CONDS_DIR / f"{name}.json"
    if not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def snapshot() -> dict:
    """采集环境信息并返回 dict。

    字段:
    - kubectl: kubectl version(client + server)
    - docker_images: 各镜像在役标识(COND 冻结值,containerd manifest digest)
    - docker_repo_digests: docker 侧 RepoDigests,仅交叉核对用
    - tc: 当前 tc 档位(从 COND 记录)
    - seed: 随机种子(从 COND 记录)
    - chunk_size_bytes: 实验块大小(从 COND 记录)
    - dataset_name: TrainCache 快照名(环境变量 DATASET_NAME)
    - git_commit: git rev-parse HEAD
    - collected_at: 采集时刻(RFC3339)
    命令失败时对应字段写 null。
    """
    cond = _load_cond_from_env()
    return {
        "schema_version": "1",
        "collected_at": datetime.now(timezone.utc).isoformat(
            timespec="microseconds").replace("+00:00", "Z"),
        "kubectl": _kubectl_version(),
        "docker_images": _docker_images(cond),
        "docker_repo_digests": _docker_repo_digests(cond),
        "tc": _tc_tier(cond),
        "seed": _seed(cond),
        "chunk_size_bytes": _chunk_size_bytes(cond),
        "dataset_name": _dataset_name(),
        "git_commit": _git_commit(),
        "cond": os.environ.get("TRAINCACHE_COND") or None,
        # 固定值与派生值的生效快照：COND 里删掉它们之后，靠这里回溯。
        "effective": _effective(cond),
    }


def _effective(cond: dict | None) -> dict | None:
    """COND 未写出、但本次 run 实际生效的固定值与派生值。"""
    if not cond:
        return None
    wl = cond.get("workload") or {}
    net = cond.get("network") or {}
    return {
        "readers": wl.get("readers"),
        "access_pattern": wl.get("access_pattern"),
        "gateway": (cond.get("environment") or {}).get("gateway"),
        "rate_mbit": net.get("rate_mbit"),
        "delay_ms": cond_mod.DEFAULTS["delay_ms"],
        "burst_k": (cond_mod.burst_k_for_rate(net["rate_mbit"])
                    if net.get("rate_mbit") in cond_mod.VALID_RATES else None),
        "dataset_bytes": cond_mod.dataset_bytes(cond),
        "cache_bytes": cond_mod.cache_bytes(cond),
        "chunk_size_bytes": cond_mod.CHUNK_SIZE_BYTES,
        "batch": cond_mod.DEFAULTS["batch"],
        "steps_per_epoch": cond_mod.DEFAULTS["steps_per_epoch"],
        "compute_ms": cond_mod.DEFAULTS["compute_ms"],
        "window_k": cond_mod.DEFAULTS["window_k"],
    }


def write_lock(path: Path) -> None:
    """snapshot() 的结果写到 path(JSON)。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = snapshot()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def selftest() -> None:
    """自检:snapshot() 不抛异常,字段齐全。"""
    data = snapshot()
    assert "collected_at" in data
    assert "kubectl" in data
    assert "docker_images" in data
    assert "docker_repo_digests" in data
    assert "tc" in data
    assert "seed" in data
    assert "chunk_size_bytes" in data
    assert "dataset_name" in data
    assert isinstance(data["dataset_name"], str) and data["dataset_name"]
    assert data["chunk_size_bytes"] is None or isinstance(data["chunk_size_bytes"], int)
    assert "git_commit" in data
    # docker_images 是 dict
    assert isinstance(data["docker_images"], dict)
    print("lock selftest: PASS (kubectl/docker 可能不可用,对应字段为 null)")


if __name__ == "__main__":
    if "--selftest" in sys.argv or len(sys.argv) == 1:
        selftest()
