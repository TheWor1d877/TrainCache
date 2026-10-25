#!/usr/bin/env bash
# [sudo] 冷读 ×7:每轮清空 Agent 缓存 + drop_caches,再经 tc 从 MinIO 下载。
# 需要 root(整个脚本用 sudo 跑):sudo bash probe_a/run_cold.sh
set -euo pipefail
[ "$EUID" -eq 0 ] || { echo "需要 root: sudo bash $0"; exit 1; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

RUNS="$(CFG PROBE_A_RUNS)"
AGENT_CACHE="$(CFG AGENT_CACHE)"
OUT_DIR="$(CFG RESULTS_DIR)/probe_a"
mkdir -p "$OUT_DIR"
chmod 777 "$OUT_DIR"  # root 写的目录,judge 用普通用户读

for i in $(seq 1 "$RUNS"); do
  echo "=== 冷读 $i/$RUNS ==="
  rm -rf "$AGENT_CACHE"
  sync
  echo 3 > /proc/sys/vm/drop_caches
  python3 "$ROOT/probe_a/fetch.py" --mode download \
    | tee "$OUT_DIR/cold_$(printf %02d "$i").json"
  sleep 2
done
echo "冷读完成:$RUNS 轮 → results/probe_a/cold_*.json"
