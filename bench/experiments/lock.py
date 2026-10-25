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
    # 回退:取 Id(去 sha256: 前缀)
    img_id = data[0].get("Id")
    if img_id:
        return img_id
    return None


def _docker_images(cond: dict | None) -> dict[str, str | None]:
    """采集 COND environment.images 中各镜像的实际 digest。"""
    images_cfg = {}
    if cond and isinstance(cond.get("environment"), dict):
        images_cfg = cond["environment"].get("images") or {}
    result: dict[str, str | None] = {}
    for key, ref in images_cfg.items():
        if not isinstance(ref, str):
            result[key] = None
            continue
        # ref 形如 repo@sha256:...;docker inspect 可能需要 repo 名
        repo = ref.split("@")[0] if "@" in ref else ref
        digest = _docker_image_digest(repo)
        result[key] = digest if digest else ref
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
    - docker_images: 各镜像实际 digest
    - tc: 当前 tc 档位(从 COND 记录)
    - seed: 随机种子(从 COND 记录)
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
        "tc": _tc_tier(cond),
        "seed": _seed(cond),
        "git_commit": _git_commit(),
        "cond": os.environ.get("TRAINCACHE_COND") or None,
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
    assert "tc" in data
    assert "seed" in data
    assert "git_commit" in data
    # docker_images 是 dict
    assert isinstance(data["docker_images"], dict)
    print("lock selftest: PASS (kubectl/docker 可能不可用,对应字段为 null)")


if __name__ == "__main__":
    if "--selftest" in sys.argv or len(sys.argv) == 1:
        selftest()
