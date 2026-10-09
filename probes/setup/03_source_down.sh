#!/usr/bin/env bash
# 停止并删除源站容器(nginx 替身)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

docker rm -f "$(CFG SOURCE_CONTAINER)" >/dev/null 2>&1 || true
echo "源站容器已停止"
