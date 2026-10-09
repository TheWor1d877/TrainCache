#!/usr/bin/env bash
# [sudo] 摘除源站容器 netns 内 eth0 上的 tc 限速。
set -euo pipefail
[ "$EUID" -eq 0 ] || { echo "需要 root: sudo bash $0"; exit 1; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

NAME="$(CFG SOURCE_CONTAINER)"
PID=$(docker inspect -f '{{.State.Pid}}' "$NAME") 2>/dev/null \
  || { echo "容器 $NAME 不在跑,无需摘除"; exit 0; }
nsenter -t "$PID" -n tc qdisc del dev eth0 root 2>/dev/null \
  && echo "已摘除容器 $NAME eth0 上的 tc 限速" \
  || echo "无 qdisc 可摘(可能未挂载)"
