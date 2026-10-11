#!/usr/bin/env bash
# 拆除 MinIO Pod eth0 上的 tc 限速。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POD=$(kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
NODE=$(kubectl -n minio get pod "$POD" -o jsonpath='{.spec.nodeName}')
docker exec -i "$NODE" bash -s -- "$POD" <<'EOF'
set -euo pipefail
POD="$1"
CID=$(crictl pods --name "$POD" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || { echo "无 netns,跳过"; exit 0; }
nsenter --net="$NETNS" tc qdisc del dev eth0 root 2>/dev/null \
  && echo "已拆除 $POD eth0 限速" || echo "无 qdisc 可拆"
EOF
