#!/usr/bin/env bash
# 跑分进度哨兵：每 60 秒记一次进度与事件；驱动退出后记完成标记并退出。
# 用法: watch_progress.sh <结果目录> <驱动pid文件> [驱动日志] [关注族]
#   结果目录: 正式产物根，形如 ~/tcd/bench/results
#   驱动pid文件: 跑批驱动进程号的 pid 文件
#   驱动日志: 驱动进程的输出日志，用于收尾时取最后一行
#   关注族: 只在进度行里展开这个族的逐格明细，默认全部族只给汇总
# 进度口径: 各格 summary.csv 的「有效行/总行」，有效 = void 非 true。
# 事件口径: 新出现的作废行、失败现场文件（pod_failed.log / measure_errors.log）。
# 只读监控，不改任何状态，不碰驱动进程。
set -u

ROOT="${1:?用法: watch_progress.sh <结果目录> <驱动pid文件> [驱动日志] [关注族]}"
PIDFILE="${2:?缺驱动pid文件}"
RUN_LOG="${3:-}"
FOCUS="${4:-}"

STAMP=$(date +%Y%m%d-%H%M%S)
# 日志与状态标记落 ~/tcd/bench/logs/，不散在 bench 根目录。
LOG_DIR="${LOG_DIR:-$HOME/tcd/bench/logs}"
mkdir -p "$LOG_DIR"
WATCH_LOG=${WATCH_LOG:-$LOG_DIR/watch-progress-$STAMP.log}
VOID_MARK=${VOID_MARK:-$LOG_DIR/watch-progress-void.seen}
STATE_MARK=${STATE_MARK:-$LOG_DIR/watch-progress-cells.seen}
START=$(date +%s)
: > "$VOID_MARK"
: > "$STATE_MARK"

DRIVER_PID=$(cat "$PIDFILE" 2>/dev/null || true)

echo "$(date '+%F %T') WATCH_START driver=$DRIVER_PID log=$RUN_LOG focus=${FOCUS:-全部}" >> "$WATCH_LOG"

while true; do
    ts=$(date '+%F %T')
    if [ -z "$DRIVER_PID" ] || ! kill -0 "$DRIVER_PID" 2>/dev/null; then
        echo "$ts DONE 驱动已退出，末行: $(tail -1 "$RUN_LOG" 2>/dev/null)" >> "$WATCH_LOG"
        break
    fi
    el=$(ps -o etime= -p "$DRIVER_PID" 2>/dev/null | tr -d ' ')

    # 族汇总 + 本轮有变化的格
    IFS='|' read -r totals changed cells_detail < <(python3 - "$ROOT" "$FOCUS" "$STATE_MARK" <<'PY' 2>/dev/null
import csv, glob, os, sys
root, focus, mark = sys.argv[1], sys.argv[2], sys.argv[3]
fam = {}
detail = []
for p in sorted(glob.glob(os.path.join(root, '*', '*', 'summary.csv'))):
    try:
        rows = list(csv.DictReader(open(p, newline='', encoding='utf-8')))
    except (OSError, csv.Error):
        continue
    if not rows:
        continue
    valid = sum(1 for r in rows
                 if str(r.get('void', '')).strip().lower() not in ('true', '1'))
    cell = os.path.basename(os.path.dirname(p))
    f = os.path.basename(os.path.dirname(os.path.dirname(p)))
    d = fam.setdefault(f, [0, 0, 0])
    d[0] += 1
    d[1] += valid
    d[2] += len(rows)
    if focus and f == focus:
        detail.append('%s %d/%d' % (cell, valid, len(rows)))
seen = set(open(mark).read().split()) if os.path.exists(mark) else set()
changed = []
for f in sorted(fam):
    d = fam[f]
    key = '%s=%d/%d' % (f, d[1], d[2])
    if key not in seen:
        seen.add(key)
        changed.append(key)
if changed:
    open(mark, 'w').write('\n'.join(sorted(seen)) + '\n')
totals = ' '.join('%s %d/%d' % (f, fam[f][1], fam[f][2]) for f in sorted(fam))
print(totals, '|', '; '.join(changed) if changed else '无变化', '|',
      '; '.join(detail) if detail else '')
PY
)

    # 事件：新出现的作废行
    newvoid=$(python3 - "$ROOT" "$VOID_MARK" <<'PY' 2>/dev/null
import csv, glob, os, sys
root, mark = sys.argv[1], sys.argv[2]
seen = set(open(mark).read().split()) if os.path.exists(mark) else set()
new = []
for p in sorted(glob.glob(os.path.join(root, '*', '*', 'summary.csv'))):
    try:
        rows = list(csv.DictReader(open(p, newline='', encoding='utf-8')))
    except (OSError, csv.Error):
        continue
    for r in rows:
        if str(r.get('void', '')).strip().lower() in ('true', '1'):
            key = '%s#%s' % (p, r.get('rep'))
            if key not in seen:
                seen.add(key)
                new.append('%s rep=%s reason=%s'
                           % (os.path.basename(os.path.dirname(p)),
                              r.get('rep'), r.get('reason') or '-'))
if new:
    open(mark, 'w').write('\n'.join(sorted(seen)) + '\n')
print('\n'.join(new))
PY
)

    # 事件：失败现场文件
    scenes=$(find "$ROOT" \( -name pod_failed.log -o -name measure_errors.log \) \
        -newermt "@$START" -size +0c 2>/dev/null | head -3)

    pods=$(kubectl -n traincache get pods 2>/dev/null \
        | awk '/train-job/ {c[$3]++} /traincache-agentd/ {a[$3]++} END {printf "train-job"; for (k in c) printf "=%s×%d", k, c[k]; printf " agentd"; for (k in a) printf "=%s×%d", k, a[k]}')

    echo "$ts | etime=$el | $totals | 变化: ${changed:-无} | ${pods:-无Pod}" >> "$WATCH_LOG"
    [ -n "$FOCUS" ] && [ -n "${cells_detail:-}" ] && echo "$ts | $FOCUS 逐格: $cells_detail" >> "$WATCH_LOG"
    [ -n "$newvoid" ] && echo "$ts | EVENT 作废行新增: $newvoid" >> "$WATCH_LOG"
    [ -n "$scenes" ] && echo "$ts | EVENT 失败现场: $scenes" >> "$WATCH_LOG"
    sleep 60
done
