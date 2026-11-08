#!/bin/bash
# TrainCache 跑分进度速查。用法：bash bench/scripts/progress.sh
# 只读，不改任何状态：查 Pod、训练步进度、已出 RESULT 的 Pod、MinIO 限速档位、
# 各格 summary.csv 的有效轮数、跑分日志尾部。
set -uo pipefail
RESULTS_DIR="${TRAINCACHE_RESULTS_DIR:-$HOME/tcd/bench/results}"

echo "===== $(date '+%F %T') ====="

echo "-- 测量 Pod --"
kubectl -n traincache get pods -l app=train-job --no-headers 2>/dev/null | awk '{print "  "$1" "$2" "$3}'

echo "-- 训练进度（各 Pod 最后一条 STEP）--"
for p in $(kubectl -n traincache get pods -l app=train-job -o name 2>/dev/null | cut -d/ -f2); do
  s=$(kubectl -n traincache logs "$p" --tail=80 2>/dev/null | grep '^\[STEP\]' | tail -1)
  echo "  $p: ${s:-（尚未进入读步）}"
done

echo "-- 已完成的 Pod（RESULT 行）--"
for p in $(kubectl -n traincache get pods -l app=train-job -o name 2>/dev/null | cut -d/ -f2); do
  r=$(kubectl -n traincache logs "$p" --tail=300 2>/dev/null | grep '^RESULT' | tail -1)
  [ -n "$r" ] && echo "  $p: $r"
done

echo "-- MinIO eth0 限速档位 --"
POD=$(kubectl -n minio get pod -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
NODE=$(kubectl -n minio get pod -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null)
if [ -n "$POD" ] && [ -n "$NODE" ]; then
  docker exec -i "$NODE" bash -s -- "$POD" <<'INNER' 2>/dev/null
CID=$(crictl pods --state Ready --name "$1" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
nsenter --net="$NETNS" tc qdisc show dev eth0
INNER
fi

echo "-- 产物（summary.csv）--"
for f in "$RESULTS_DIR"/*/*/summary.csv; do
  [ -e "$f" ] || continue
  echo "  $f"
  python3 - "$f" <<'PY' 2>/dev/null
import csv, sys
for r in csv.DictReader(open(sys.argv[1])):
    print("    rep=%s status=%s void=%s reason=%s makespan=%s hit_rate=%s" % (
        r.get("rep"), r.get("status"), r.get("void"), r.get("reason") or "-",
        r.get("total_makespan") or "-", r.get("hit_rate") or "-"))
PY
done

echo "-- 跑分日志尾部 --"
tail -5 "$HOME/tcd/bench/compare/logs/traincache.log" 2>/dev/null
