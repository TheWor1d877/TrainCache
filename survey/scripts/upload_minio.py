#!/usr/bin/env python3
"""把探针合成的 tar 分片上传到 kind 内 MinIO(经 nodePort 30090)。

依赖:boto3(清华源安装,pip3 install boto3 -i https://pypi.tuna.tsinghua.edu.cn/simple)
"""
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ENDPOINT = "http://127.0.0.1:30090"
ACCESS_KEY = "traincache"
SECRET_KEY = "traincache-probe"
BUCKET = "train-data"
TARS = Path("/home/howardhe/Project/TrainCache/probes/data/tars")


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"bucket {BUCKET} 已存在")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"已建 bucket {BUCKET}")

    tars = sorted(TARS.glob("*.tar"))
    if not tars:
        sys.exit(f"未找到 tar 分片:{TARS}")
    t0 = time.time()
    for i, p in enumerate(tars, 1):
        s3.upload_file(str(p), BUCKET, p.name)
        print(f"[{i}/{len(tars)}] {p.name} 完成,累计 {time.time() - t0:.0f}s")

    resp = s3.list_objects_v2(Bucket=BUCKET)
    objs = resp.get("Contents", [])
    total = sum(o["Size"] for o in objs)
    print(f"完成:{len(objs)} 个对象,共 {total / 2**30:.2f} GiB")


if __name__ == "__main__":
    main()
