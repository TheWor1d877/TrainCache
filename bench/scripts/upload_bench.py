#!/usr/bin/env python3
"""把 B 轨 tar 分片上传到 kind 内 MinIO 的 bench-b 桶(经 nodePort 30090)。

只追加不覆盖(Q12):桶里已有的 shard-*.tar 一律跳过,重跑本脚本是安全的补洞动作;
确要重写源数据,显式设 UPLOAD_FORCE=1。
"""
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


def _object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=config.MINIO_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def node_ip() -> str:
    """kind 节点容器没有把 nodePort 映射到宿主机,经节点 InternalIP 访问。"""
    out = subprocess.run(
        ["kubectl", "get", "nodes", "-o",
         "jsonpath={.items[0].status.addresses[?(@.type=='InternalIP')].address}"],
        capture_output=True, text=True, check=True)
    return out.stdout.strip()


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{node_ip()}:{config.MINIO_NODEPORT}",
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=config.MINIO_BUCKET)
        print(f"bucket {config.MINIO_BUCKET} 已存在")
    except Exception:
        s3.create_bucket(Bucket=config.MINIO_BUCKET)
        print(f"已建 bucket {config.MINIO_BUCKET}")
    # 公开读:集群内冷读 Pod(busybox wget)匿名读
    import json as _json
    s3.put_bucket_policy(Bucket=config.MINIO_BUCKET, Policy=_json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"AWS": ["*"]},
                       "Action": ["s3:GetObject"],
                       "Resource": ["arn:aws:s3:::*/*"]}]
    }))
    print("bucket 公开读 policy 已设")

    tars = sorted(config.TRACK_B_DIR.glob("shard-*.tar"))
    if not tars:
        sys.exit("B 轨 tar 不存在,先跑 synth_track_b.py")
    t0 = time.time()
    # Q12:bench-b/shard-*.tar 是五组共同的只读源数据,只追加不覆盖。
    # 已存在的对象一律跳过;确要重写源数据得显式 UPLOAD_FORCE=1。
    force = os.environ.get("UPLOAD_FORCE") == "1"
    added = skipped = 0
    for i, p in enumerate(tars, 1):
        if not force and _object_exists(s3, p.name):
            skipped += 1
            continue
        s3.upload_file(str(p), config.MINIO_BUCKET, p.name)
        added += 1
        print(f"[{i}/{len(tars)}] {p.name} 已上传,累计 {time.time() - t0:.0f}s", flush=True)
    resp = s3.list_objects_v2(Bucket=config.MINIO_BUCKET)
    objs = resp.get("Contents", [])
    print(f"完成:本次追加 {added} 个,跳过已存在 {skipped} 个;"
          f"桶内 {len(objs)} 个对象,共 {sum(o['Size'] for o in objs) / 2**30:.2f} GiB")


if __name__ == "__main__":
    main()
