#!/usr/bin/env python3
"""模拟缓存 Agent 的抓取脚本(探针A 核心装置)。

download 模式:模拟 Agent 从源站(MinIO,经 tc 限速)按块并发下载到本地缓存目录。
read 模式:模拟训练 Pod 从本地缓存目录直读(热读,零网络,不经 Agent)。
单轮结果以 JSON 打到 stdout,由 run_*.sh 归档。
"""
import argparse
import json
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

SHARDS = [f"shard-{i:05d}.tar" for i in range(config.N_TARS)]
BLOCK = 8 << 20  # 8MB 读块


def download(shard: str) -> int:
    """按块下载单个分片到 Agent 缓存目录,返回字节数。"""
    url = f"http://127.0.0.1:{config.MINIO_PORT}/{config.BUCKET}/{shard}"
    dst = Path(config.AGENT_CACHE) / shard
    dst.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as r, open(dst, "wb") as f:
        total = 0
        while True:
            chunk = r.read(BLOCK)
            if not chunk:
                break
            f.write(chunk)
            total += len(chunk)
    return total


def read_local(shard: str) -> int:
    """顺序直读缓存目录文件(热读,模拟训练 Pod 的 hostPath 读)。"""
    total = 0
    with open(Path(config.AGENT_CACHE) / shard, "rb") as f:
        while True:
            chunk = f.read(BLOCK)
            if not chunk:
                break
            total += len(chunk)
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["download", "read"], required=True)
    args = ap.parse_args()

    t0 = time.perf_counter()
    if args.mode == "download":
        # 有界并发(模拟 Agent 的并发控制语义)
        with ThreadPoolExecutor(max_workers=config.PROBE_A_CONCURRENCY) as pool:
            sizes = list(pool.map(download, SHARDS))
    else:
        sizes = [read_local(s) for s in SHARDS]
    seconds = time.perf_counter() - t0

    nbytes = sum(sizes)
    print(json.dumps({
        "mode": args.mode,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "n_files": len(sizes),
        "bytes": nbytes,
        "seconds": round(seconds, 3),
        "mbps": round(nbytes / 2**20 / seconds, 2),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
