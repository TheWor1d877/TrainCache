#!/usr/bin/env bash
# 作废哨兵（轮询版）：每 60s 扫一遍结果目录与驱动日志，发现错误即退出唤醒。
# 用法: void_watch.sh <结果目录> <驱动进程名> <驱动日志>
# 退出码: 0=发现错误（stdout 给现场）；2=驱动正常跑完；4=驱动进程消失且未正常收尾
set -euo pipefail
ROOT="${1:?用法: void_watch.sh <结果目录> <驱动进程名> <驱动日志>}"
DRIVER="${2:?缺驱动进程名}"
LOG="${3:?缺驱动日志}"
START=$(date +%s)
LOG_OFF=0
while :; do
  # 1) summary.csv 的 void 行（只看哨兵启动后动过的文件，避免翻旧账）
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
  # 2) 失败现场文件：Pod 失败/超时与测量体 ERROR 在格中即落盘，不等 summary
  SCENES=$(find "$ROOT" \( -name pod_failed.log -o -name measure_errors.log \) -newermt "@$START" -size +0c 2>/dev/null || true)
  if [ -n "$SCENES" ]; then
    echo "发现失败现场:"
    printf '%s\n' "$SCENES"
    exit 0
  fi
  # 3) 驱动日志的失败标记：只扫上次以来新追加的完整行，避免重报旧失败
  if [ -f "$LOG" ]; then
    SIZE=$(stat -c %s "$LOG")
    if [ "$SIZE" -gt "$LOG_OFF" ]; then
      CHUNK=$(tail -c +"$((LOG_OFF + 1))" "$LOG")
      if [[ "$CHUNK" == *$'\n'* ]]; then
        WHOLE=${CHUNK%$'\n'*}$'\n'
        LOG_OFF=$((LOG_OFF + $(printf '%s' "$WHOLE" | wc -c)))
        MARKS=$(printf '%s' "$WHOLE" | grep -n '本轮作废\|创建 Pod 失败\|失败格:' || true)
        if [ -n "$MARKS" ]; then
          echo "驱动日志报失败:"
          printf '%s\n' "$MARKS"
          exit 0
        fi
      fi
    fi
    if grep -q '^\[done\] 跳过' "$LOG"; then
      if grep -q '失败 0' "$LOG"; then
        echo "驱动正常跑完:"
        grep '^\[done\]' "$LOG"
        exit 2
      fi
      echo "驱动收尾但有失败格:"
      grep '^\[done\]\|失败格:' "$LOG"
      exit 0
    fi
  fi
  # 4) 驱动本体存活：锚定完整命令行，排除命令行里带启动串的包装 shell
  if ! pgrep -f "^python3.*${DRIVER}\.py$" >/dev/null; then
    echo "驱动进程 $DRIVER 已消失且日志无正常收尾"
    exit 4
  fi
  sleep 60
done