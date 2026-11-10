#!/usr/bin/env bash
# 作废哨兵：盯跑分结果目录，一出现 void 行就退出唤醒（不等整组跑完）。
# 用法: void_watch.sh <结果目录> [空闲秒数，默认 43200]
# 退出码: 0=发现 void（stdout 给格名与原因）；3=空闲超时（这阵子没新数据）
set -euo pipefail
ROOT="${1:?用法: void_watch.sh <结果目录> [空闲秒数]}"
IDLE="${2:-43200}"
START=$(date +%s)
while :; do
  OUT=$(ROOT="$ROOT" START="$START" python3 - <<'PY'
import csv, glob, os
root = os.environ['ROOT']
start = float(os.environ['START'])
for p in glob.glob(os.path.join(root, '**', 'summary.csv'), recursive=True):
    if os.path.getmtime(p) < start:
        continue
    try:
        rows = list(csv.DictReader(open(p, newline='', encoding='utf-8')))
    except OSError:
        continue
    for r in rows:
        if str(r.get('void', '')).strip().lower() in ('true', '1'):
            print(f"{p}  reason={r.get('reason', '?')}  rep={r.get('rep', '?')}")
PY
)
  if [ -n "$OUT" ]; then
    echo "发现作废行:"
    printf '%s\n' "$OUT"
    exit 0
  fi
  NEWEST=$(find "$ROOT" -name summary.csv -newermt "@$START" -printf '%T@\n' 2>/dev/null | sort -n | tail -1 | cut -d. -f1 || true)
  NOW=$(date +%s)
  if [ -n "$NEWEST" ]; then LAST="$NEWEST"; else LAST="$START"; fi
  if [ $(( NOW - LAST )) -ge "$IDLE" ]; then
    echo "空闲 ${IDLE}s 无新数据,哨兵退出"
    exit 3
  fi
  sleep 30
done
