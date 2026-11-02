#!/usr/bin/env bash
# 五格对照串行入口：S@100 / S@300 / S@1000 / M@1000 / L@1000。
#
# 用法：
#   bash bench/compare/run_compare.sh              # 串行五格，约 6.3 h
#   bash bench/compare/run_compare.sh --force      # 忽略已完成格，全量重跑
#   bash bench/compare/run_compare.sh --dry-run    # 只打印五格计划，不跑分
#   make bench-compare [FORCE=1]
#
# 同一时刻只允许一个实例：flock -n /tmp/traincache-compare.lock。
# 每格独立产物 $RESULTS_DIR/m7/<格名>/summary.csv + lock.json；某格数据行数
# 达到「组数 × 次数」时跳过，失败立即停止并保留已完成格产物。
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
PY="${PY:-$REPO/.venv/bin/python}"
RUNNER="$ROOT/compare/run_bench.py"
LOCK_FILE="/tmp/traincache-compare.lock"

FORCE=0
DRY_RUN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
        *) echo "未知参数: $1" >&2; exit 2 ;;
    esac
done

RESULTS_DIR="$("$PY" "$ROOT/config.py" --sh RESULTS_DIR)"
if [ -z "$RESULTS_DIR" ]; then
    echo "错误: config.py 取不到 RESULTS_DIR" >&2
    exit 1
fi

if [ "$DRY_RUN" -eq 0 ]; then
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
        echo "已有对照跑分实例在跑（锁 $LOCK_FILE），本次退出。" >&2
        exit 1
    fi
fi

CURRENT=""
trap 'rc=$?; if [ "$rc" -ne 0 ]; then echo "[compare] 失败：${CURRENT:-入口}（rc=$rc），已完成格产物保留。" >&2; fi' ERR

run_cell() {
    local name="$1" tier="$2" rate="$3" reps="$4" groups="$5" minutes="$6"
    local out="$RESULTS_DIR/m7/$name/summary.csv"
    local n_groups expected rows
    n_groups=$(wc -w <<<"$groups")
    expected=$((n_groups * reps))

    if [ "$DRY_RUN" -eq 1 ]; then
        printf '[compare] %-7s tier=%s rate=%-9s 组数=%d 每点=%d 次 预计约 %s min\n' \
            "$name" "$tier" "$rate" "$n_groups" "$reps" "$minutes"
        printf '      %s --tier %s --rate %s --reps %d --groups %s --out %s\n' \
            "$RUNNER" "$tier" "$rate" "$reps" "$groups" "$out"
        return 0
    fi

    echo "[compare] $name：tier=$tier rate=$rate 组数=$n_groups 每点=$reps 次，预计约 $minutes min"
    if [ "$FORCE" -eq 0 ] && [ -s "$out" ]; then
        rows=$(awk -F, 'NR>1 && $6 != "True"' "$out" | wc -l)
        if [ "$rows" -ge "$expected" ]; then
            echo "[compare] 跳过 $name：$out 已完整（$rows/$expected 行）"
            return 0
        fi
    fi

    env CAPACITY_RATIO="$tier" \
        "$PY" "$RUNNER" --tier "$tier" --rate "$rate" --groups $groups \
        --reps "$reps" --out "$out"
    echo "[compare] 完成 $name：$out"
}

CURRENT="S-100"
run_cell S-100 S 100mbit 3 \
    "direct juicefs-cold juicefs-warm fluid-cold fluid-warm traincache-cold traincache-warm" 192

CURRENT="S-300"
run_cell S-300 S 300mbit 3 "direct fluid-cold traincache-cold" 42

CURRENT="S-1000"
run_cell S-1000 S 1000mbit 3 \
    "direct juicefs-cold juicefs-warm fluid-cold fluid-warm traincache-cold traincache-warm" 96

CURRENT="M-1000"
run_cell M-1000 M 1000mbit 3 "direct fluid-cold traincache-cold" 42

CURRENT="L-1000"
run_cell L-1000 L 1000mbit 1 "traincache-cold fluid-cold" 12

CURRENT=""
if [ "$DRY_RUN" -eq 1 ]; then
    echo "[compare] dry-run：五格共 62 run，约 6.3 h；未连集群、未写产物。"
else
    echo "[compare] 五格完成，产物在 $RESULTS_DIR/m7/"
fi
