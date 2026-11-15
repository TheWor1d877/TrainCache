#!/usr/bin/env bash
# evict-mix 族冒烟：9 格（heat/lru/lfu × ρ2/ρ4/ρ8），每格 1 rep。
#
# 覆盖口径：这一族只有两个轴——淘汰策略与 ρ。正式轮 9 格 × 5 rep = 45 跑，
# 冒烟把采样数缩到 1，格数不缩（策略与 ρ 两个轴全走一遍），路径与正式轮同源：
# 同一份 COND、同一套 run.py → run_policy → run_bench 链路、同一份断言代码。
#
# 验收对象是数据，不是流程：每格跑完立即执行硬断言，不符当场终止，不带病跑完。
# 断言写在脚本里（不是清单里），冒烟与正式轮共用同一份判定口径。
#
# 产物落正式产物根 ~/tcd/bench/results/evict-mix/，冒烟行是同格 rep0，
# 正式轮重跑该格时被同 key 新行顶替，不进最终口径。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
cd "$REPO"

DATA_ROOT="${TRAINCACHE_DATA_ROOT:-$(cd "$REPO/.." && pwd)/traincachedata}"
BENCH_DIR="$DATA_ROOT/bench"
RESULTS_DIR="$BENCH_DIR/results"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG="$BENCH_DIR/run-smoke-evictmix-$STAMP.log"
LOGDIR="$BENCH_DIR/smoke-evictmix-$STAMP"
PIDFILE="$BENCH_DIR/smoke-evictmix.pid"
FAILFILE="$BENCH_DIR/smoke-evictmix-failures-$STAMP.txt"

mkdir -p "$LOGDIR"
echo $$ > "$PIDFILE"

: > "$FAILFILE"

# ---- 硬断言：读该格 summary.csv 的一行，逐条判 ----
assert_cell() {
    local cell="$1" pol="$2" rho="$3"
    python3 - "$cell" "$pol" "$rho" "$RESULTS_DIR" <<'PY'
import csv, os, sys

cell, pol, rho, results = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
path = os.path.join(results, "evict-mix", cell, "summary.csv")
if not os.path.isfile(path):
    print(f"[assert] {cell}: 缺 summary.csv")
    sys.exit(2)
rows = list(csv.DictReader(open(path, newline="", encoding="utf-8")))
if len(rows) != 1:
    print(f"[assert] {cell}: summary.csv 行数 {len(rows)}，应为 1")
    sys.exit(2)
r = rows[0]


def num(key):
    v = str(r.get(key, "")).strip()
    if v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


fails = []
# 1) 这轮不能作废
if str(r.get("void", "")).strip().lower() in ("true", "1"):
    fails.append(f"void=true（reason={r.get('reason') or '-'}）")
# 2) 淘汰轮次必须大于 0：轮次为 0 是族判据「装置失败」
normal, danger = num("evict_rounds_normal"), num("evict_rounds_danger")
if normal is None or danger is None:
    fails.append("淘汰轮次列缺失")
elif normal + danger <= 0:
    fails.append(f"淘汰轮次为 0（normal={normal:.0f} danger={danger:.0f}）")
# 3) 两个按块型存活率必须有值
for key in ("lowfreq_fresh_survive", "highfreq_stale_survive"):
    if num(key) is None:
        fails.append(f"{key} 缺值")
# 4) 读侧不丢数据
for key in ("read_errors", "queue_timeouts"):
    v = num(key)
    if v is None or v > 0:
        fails.append(f"{key}={r.get(key)}")
# 5) 字节账自洽：三笔字节差不超过 max(1 B, 1%)
sust = num("sustained_prefetch_bytes") or 0.0
sw = num("switch_reprefetch_bytes") or 0.0
pf = num("prefetch_bytes_run") or 0.0
diff = abs((sust + sw) - pf)
if diff > max(1.0, 0.01 * pf):
    fails.append(f"字节账差 {diff:.0f} B 超限")
# 6) 已知答案探针：数据集与缓存容量按 COND 的 ρ 折算
if (num("dataset_bytes") or 0) != 4294967296:
    fails.append(f"dataset_bytes={r.get('dataset_bytes')}，应为 4294967296")
want_cache = 4294967296 / float(rho)
if abs((num("cache_bytes") or 0) - want_cache) > 1:
    fails.append(f"cache_bytes={r.get('cache_bytes')}，应为 {want_cache:.0f}")
# 7) 这一行必须属于本格：族、策略、ρ 三项对上
if r.get("family") != "evict-mix":
    fails.append(f"family={r.get('family')}，应为 evict-mix")
if r.get("evict_policy") != pol:
    fails.append(f"evict_policy={r.get('evict_policy')}，应为 {pol}")
if abs((num("rho") or 0) - float(rho)) > 1e-6:
    fails.append(f"rho={r.get('rho')}，应为 {rho}")

if fails:
    print(f"[assert] {cell} 不通过：" + "；".join(fails))
    print(f"[assert] {cell} 实测：rounds={r.get('evict_rounds_normal')}/"
          f"{r.get('evict_rounds_danger')} blk={r.get('evict_blocks_run')} "
          f"cap_final={r.get('capacity_bytes_final')} "
          f"cache_bytes={r.get('cache_bytes')} "
          f"peak={r.get('cache_peak_ratio')} makespan={r.get('total_makespan')}")
    sys.exit(1)
print(f"[assert] {cell} 通过 rounds={normal:.0f}/{danger:.0f} "
      f"survive={r.get('lowfreq_fresh_survive')}/{r.get('highfreq_stale_survive')} "
      f"blk={r.get('evict_blocks_run')} peak={r.get('cache_peak_ratio')} "
      f"hit={r.get('hit_rate')} makespan={r.get('total_makespan')}")
PY
}

# ---- 留档：这一格的 agentd 日志（agentd 在下一格会被重启，日志随即冲掉）----
save_agentd_logs() {
    local cell="$1"
    local out="$LOGDIR/$cell-agentd.log"
    : > "$out"
    local p
    for p in $(kubectl -n traincache get pods -l app=traincache-agentd \
               -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
        echo "===== $p =====" >> "$out"
        kubectl -n traincache logs "$p" --tail=600 2>/dev/null >> "$out"
    done
    local n
    n=$(grep -c '淘汰轮结束' "$out" 2>/dev/null || true)
    echo "[留档] $cell agentd 日志 $(wc -l < "$out") 行，淘汰轮结束行 $n 条"
    grep '淘汰轮结束' "$out" 2>/dev/null | head -3
}

# ---- 清理本轮的测量 Pod（Pod 名前缀 train-job，标签随版本变，按名字取）----
cleanup_jobs() {
    local p
    for p in $(kubectl -n traincache get pods --no-headers 2>/dev/null \
               | awk '$1 ~ /^train-job/ {print $1}'); do
        kubectl -n traincache delete pod "$p" --wait=false 2>/dev/null
    done
}

# ---- 环境预检：数据集、装置、产物根 ----
precheck() {
    local rc=0
    for f in "$DATA_ROOT/bench/data/dataset/tars"; do
        if [ ! -d "$f" ]; then
            echo "[预检] 缺数据层目录 $f"
            rc=1
        fi
    done
    if ! kubectl -n traincache get pods -l app=traincache-agentd \
         -o name 2>/dev/null | grep -q agentd; then
        echo "[预检] agentd Pod 不在"
        rc=1
    fi
    if [ "$(kubectl -n traincache get pods --no-headers 2>/dev/null \
            | grep -c 'train-job')" != "0" ]; then
        echo "[预检] 上轮残留训练 Pod，先清理"
        cleanup_jobs
    fi
    return $rc
}

# ---- 只跑断言，不占机时：对既有产物验一遍判定逻辑（已知答案探针）----
if [ "${1:-}" = "--assert-only" ]; then
    rc_all=0
    for pol in heat lru lfu; do
        for rho in 2 4 8; do
            cell="evict-mix-$pol-rho$rho"
            assert_cell "$cell" "$pol" "$rho" || rc_all=1
        done
    done
    echo "[assert] 既有产物断言结束 rc=$rc_all"
    exit "$rc_all"
fi

exec >> "$LOG" 2>&1
echo "[smoke] evict-mix 冒烟开始 $(date '+%F %T') pid=$$ log=$LOG 留档目录=$LOGDIR"

if ! precheck; then
    echo "[smoke] 预检不过，不进冒烟"
    exit 2
fi
echo "[smoke] 预检通过"

echo "[smoke] 冷态重置（族前一次）"
bash "$ROOT/scripts/cold_reset.sh" || echo "[smoke] cold_reset 失败，继续"

failed=0
# 不给参数就跑全部 9 格；给了参数只跑指定的格（形如 evict-mix-lfu-rho8）。
CELLS=("$@")
if [ ${#CELLS[@]} -eq 0 ]; then
    CELLS=(evict-mix-heat-rho8 evict-mix-heat-rho4 evict-mix-heat-rho2
           evict-mix-lru-rho8 evict-mix-lru-rho4 evict-mix-lru-rho2
           evict-mix-lfu-rho8 evict-mix-lfu-rho4 evict-mix-lfu-rho2)
fi
for cell in "${CELLS[@]}"; do
    pol=$(echo "$cell" | sed -E 's/^evict-mix-(heat|lru|lfu)-rho([0-9]+)$/\1/')
    rho=$(echo "$cell" | sed -E 's/^evict-mix-(heat|lru|lfu)-rho([0-9]+)$/\2/')
    if [ "$pol" = "$cell" ] || [ "$rho" = "$cell" ]; then
        echo "[smoke] 格名不合法: $cell"
        exit 2
    fi
    echo ""
    echo "[smoke] ===== $cell 开始 $(date '+%F %T') ====="
        kubectl -n traincache get pods -l app=traincache-agentd \
            -o custom-columns=NAME:.metadata.name,UID:.metadata.uid --no-headers 2>/dev/null \
            | sed 's/^/[生命周期] 跑前 agentd /'
        python3 "$ROOT/run.py" evict-mix --evict-policy "$pol" --rho "$rho" \
                --reps 1 --force
        rc=$?
        echo "[smoke] $cell rc=$rc"
        save_agentd_logs "$cell"
        if [ "$rc" -ne 0 ]; then
            echo "$cell run.py rc=$rc" >> "$FAILFILE"
            failed=1
            echo "[smoke] $cell 跑批失败，终止冒烟"
            break
        fi
        if ! assert_cell "$cell" "$pol" "$rho"; then
            echo "$cell 断言不通过" >> "$FAILFILE"
            failed=1
            echo "[smoke] $cell 断言不通过，终止冒烟"
            break
        fi
        kubectl -n traincache get pods -l app=traincache-agentd \
            -o custom-columns=NAME:.metadata.name,UID:.metadata.uid --no-headers 2>/dev/null \
            | sed 's/^/[生命周期] 跑后 agentd /'
done

echo ""
echo "[smoke] 冒烟结束 $(date '+%F %T') 失败格数 $(wc -l < "$FAILFILE")"
[ -s "$FAILFILE" ] && sed 's/^/[smoke] 失败: /' "$FAILFILE"

# ---- 跑后残留清理：训练 Pod 与本轮的临时锁文件 ----
cleanup_jobs
echo "[清理] 本轮训练 Pod 已删"
echo "[清理] agentd 日志留档目录 $LOGDIR"
echo "[smoke] 失败清单文件 $FAILFILE"
exit "$failed"
