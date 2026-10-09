#!/usr/bin/env bash
# 热读 ×7:从 Agent 缓存目录直读(零网络),每轮附带零网络验证。
# 普通用户即可;tc 不必摘除(热读不建连,不受影响)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

RUNS="$(CFG PROBE_A_RUNS)"
AGENT_CACHE="$(CFG AGENT_CACHE)"
OUT_DIR="$ROOT/results/probe_a"
mkdir -p "$OUT_DIR"

[ -n "$(ls -A "$AGENT_CACHE" 2>/dev/null)" ] || {
  echo "缓存目录为空,请先跑冷读(下载一次): sudo bash probe_a/run_cold.sh"
  exit 1
}

for i in $(seq 1 "$RUNS"); do
  echo "=== 热读 $i/$RUNS ==="
  # 后台采样 9000 端口连接数,覆盖本轮热读窗口
  bash "$ROOT/probe_a/verify_isolated.sh" 3 &
  VPID=$!
  python3 "$ROOT/probe_a/fetch.py" --mode read \
    | tee "$OUT_DIR/hot_$(printf %02d "$i").json"
  wait "$VPID"
  sleep 2
done
echo "热读完成:$RUNS 轮 → results/probe_a/hot_*.json"
