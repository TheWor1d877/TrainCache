#!/usr/bin/env python3
"""把 B 轨 tar 分片上传到 kind 内 MinIO 的 bench-b 桶(经 nodePort 30090)。"""
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


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
    for i, p in enumerate(tars, 1):
        s3.upload_file(str(p), config.MINIO_BUCKET, p.name)
        print(f"[{i}/{len(tars)}] {p.name} 完成,累计 {time.time() - t0:.0f}s", flush=True)
    resp = s3.list_objects_v2(Bucket=config.MINIO_BUCKET)
    objs = resp.get("Contents", [])
    print(f"完成:{len(objs)} 个对象,共 {sum(o['Size'] for o in objs) / 2**30:.2f} GiB")


if __name__ == "__main__":
    main()
