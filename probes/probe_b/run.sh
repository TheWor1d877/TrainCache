#!/usr/bin/env bash
# 探针B 六组:冷(http)/热(local) × workers 0/4/8。
# 前提:冷态需要 tc 已挂载(probe_a/tc_setup.sh)且未摘除;MinIO 在跑。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CFG() { python3 "$ROOT/config.py" --sh "$1"; }

OUT_DIR="$ROOT/results/probe_b"
mkdir -p "$OUT_DIR" 2>/dev/null || {
  echo "results 目录权限不足(之前用 root 跑过冷读会留下 root 属主):"
  echo "  sudo chown -R $(id -un) \"$ROOT/results\""
  exit 1
}

WORKERS="$(CFG PROBE_B_WORKERS)"
for src in http local; do
  for w in $WORKERS; do
    tag="cold"
    [ "$src" = "local" ] && tag="hot"
    echo "=== $tag workers=$w ==="
    python3 "$ROOT/probe_b/train.py" --source "$src" --workers "$w" \
      --out "$OUT_DIR/${tag}_w${w}.json"
  done
done
echo "完成 → results/probe_b/*.json,判定:python3 probe_b/judge.py"
