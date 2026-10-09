#!/usr/bin/env bash
# 热读零网络验证(红灯排查清单第 1/2 条):
# 1) 采样窗口内到源站端口的 established 连接峰值(期望 0);
# 2) 打印容器内 tc 计数,确认限速挂载且计数在涨(冷读期间)。
# 用法:bash verify_isolated.sh [采样秒数,默认 8]
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }
PORT="$(CFG MINIO_PORT)"
NAME="$(CFG SOURCE_CONTAINER)"
WATCH="${1:-8}"

max=0
for _ in $(seq 1 "$WATCH"); do
  n=$(ss -tn state established 2>/dev/null \
    | awk -v p=":$PORT" 'index($4, p) || index($5, p)' | wc -l)
  [ "$n" -gt "$max" ] && max=$n
  sleep 1
done
echo "采样 ${WATCH}s 内到 :$PORT 的 established 连接峰值: $max (热读期望 0)"

echo "--- 容器内 tc 计数(确认限速挂载且计数在涨)---"
PID=$(docker inspect -f '{{.State.Pid}}' "$NAME" 2>/dev/null) \
  && sudo -n nsenter -t "$PID" -n tc -s qdisc show dev eth0 2>/dev/null \
  || echo "(需要 root 才能看计数:sudo nsenter -t \$(docker inspect -f '{{.State.Pid}}' $NAME) -n tc -s qdisc show dev eth0)"
