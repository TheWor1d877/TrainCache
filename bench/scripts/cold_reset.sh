#!/usr/bin/env bash
# 冷态规程:每组实验开始前执行,保证每次冷读一样冷。
# 1) sync + drop_caches 清宿主机内核 page cache(kind 节点容器与宿主机共享内核,
#    节点内所有进程的页缓存一并清掉);
# 2) 重启 MinIO(清其进程内热态)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

echo "== 1/2 drop_caches(宿主机内核,覆盖全部 kind 节点)=="
echo "$(cat "$ROOT/.sudo-pass")" | sudo -S sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null
echo "    page cache 已清"

echo "== 2/2 重启 MinIO =="
kubectl -n minio rollout restart deployment/minio
kubectl -n minio rollout status deployment/minio --timeout=180s
sleep 5
echo "== 3/3 重新挂 tc(MinIO 重启后 Pod 换新 netns,qdisc 随之消失)=="
bash "$ROOT/scripts/tc_apply.sh" 2>&1 | tail -3
echo "冷态就绪(MinIO 已重启、内核缓存已清、tc 已重挂)"
