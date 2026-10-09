#!/usr/bin/env bash
# 启动源站替身(nginx 静态服务),tar 目录只读挂到 URL 前缀目录下。
# 原设计为容器 MinIO,因镜像网络受限降级为 nginx(见 config.py 注释与
# design.md「装置降级说明」):HTTP 字节流直取语义等价,URL 结构不变。
# 普通用户 + docker 权限即可,无需 root。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

TARS_DIR="$(CFG TARS_DIR)"
NAME="$(CFG SOURCE_CONTAINER)"
IMAGE="$(CFG SOURCE_IMAGE)"
PORT="$(CFG MINIO_PORT)"
BUCKET="$(CFG BUCKET)"

[ -n "$(ls -A "$TARS_DIR" 2>/dev/null)" ] || {
  echo "未找到 tar 分片,请先跑: python3 setup/01_synth_data.py"
  exit 1
}

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" \
  -p "$PORT:80" \
  -v "$TARS_DIR:/usr/share/nginx/html/$BUCKET:ro" \
  "$IMAGE"

# 等源站就绪并抽查公开读
for _ in $(seq 1 30); do
  curl -sf "http://127.0.0.1:$PORT/$BUCKET/shard-00000.tar" -o /dev/null && break
  sleep 1
done
curl -sf "http://127.0.0.1:$PORT/$BUCKET/shard-00000.tar" -o /dev/null \
  && echo "OK: 源站就绪(nginx),URL 前缀 /$BUCKET/ 公开读验证通过"
