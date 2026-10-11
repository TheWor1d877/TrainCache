#!/usr/bin/env bash
# 参数化限速:对 kind 内 MinIO Pod 的 eth0 egress 挂 tbf+netem(源站出站方向)。
# 档位即实验自变量:tc_apply.sh 100mbit 20ms / tc_apply.sh 300mbit 10ms
# 挂点依据(现勘实证):必须挂"出站方"的 egress;Pod 重建后需重新执行本脚本。
# 需要 root(经 bench/.sudo-pass 自动 sudo)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }
RATE="${1:-$(CFG TC_RATE)}"
DELAY="${2:-$(CFG TC_DELAY)}"
BURST="${3:-$(CFG TC_BURST)}"

POD=$(kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
NODE=$(kubectl -n minio get pod "$POD" -o jsonpath='{.spec.nodeName}')
echo "MinIO Pod=$POD 节点=$NODE 档位:rate=$RATE delay=$DELAY"

# 节点容器内:定位 Pod sandbox 的 netns,给其 eth0 挂 qdisc
docker exec -i "$NODE" bash -s -- "$RATE" "$DELAY" "$BURST" "$POD" <<'EOF'
set -euo pipefail
RATE="$1"; DELAY="$2"; BURST="$3"; POD="$4"
CID=$(crictl pods --name "$POD" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || { echo "找不到 Pod sandbox netns(CID=$CID)"; exit 1; }
echo "netns=$NETNS"
nsenter --net="$NETNS" tc qdisc del dev eth0 root 2>/dev/null || true
nsenter --net="$NETNS" tc qdisc add dev eth0 root handle 1: \
  tbf rate "$RATE" burst "$BURST" latency 50ms
nsenter --net="$NETNS" tc qdisc add dev eth0 parent 1:1 handle 10: netem delay "$DELAY"
echo "--- 已挂载 ---"
nsenter --net="$NETNS" tc qdisc show dev eth0
EOF
echo "提示:换档位直接重跑本脚本;拆除用 tc_teardown.sh"
