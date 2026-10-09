#!/usr/bin/env bash
# [sudo] 在源站容器 netns 内的 eth0 egress 挂 tbf 100mbit + netem 20ms。
#
# 两次踩坑记录(实证结论,勿回退):
# 1. 挂 docker0:Linux bridge 二层转发不经过桥主设备的 qdisc → 无效;
# 2. 挂 veth host 侧 root(egress):下载数据方向是 容器→host,在 host 侧
#    veth 上是 ingress,egress qdisc 管不到 → 无效(tcpdump 实证数据包过
#    veth 但 tc 计数不动)。
# 正确挂点:容器 netns 内 eth0 的 egress(nginx 响应发出方向)。
# qdisc 随容器生命周期消失,容器重建后需重新挂载。
set -euo pipefail
[ "$EUID" -eq 0 ] || { echo "需要 root: sudo bash $0"; exit 1; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

NAME="$(CFG SOURCE_CONTAINER)"
PID=$(docker inspect -f '{{.State.Pid}}' "$NAME") || { echo "容器 $NAME 不在跑"; exit 1; }
TC() { nsenter -t "$PID" -n tc "$@"; }

TC qdisc del dev eth0 root 2>/dev/null || true

TC qdisc add dev eth0 root handle 1: \
  tbf rate "$(CFG TC_RATE)" burst "$(CFG TC_BURST)" latency "$(CFG TC_LATENCY)"
TC qdisc add dev eth0 parent 1:1 handle 10: netem delay "$(CFG TC_NETEM_DELAY)"

echo "已挂载 tc 于容器 $NAME 的 eth0 egress(源站出站方向):"
TC qdisc show dev eth0
echo "提示:实验结束后用 sudo bash probe_a/tc_teardown.sh 摘除"
