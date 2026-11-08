#!/usr/bin/env bash
# 快速跑分:只跑 traincache-cold 与 traincache-warm 同一格,改完代码先看两阶段有没有塌。
#
# 用法:
#   bash bench/scripts/bench_quick.sh [--rho 4] [--rate 1000] [--reps 3]
#
# 产物就是对照族的正常格子:results/compare/compare-<组态>-rho<ρ>-<rate>/,
# 与整族跑分同路径、同字段。
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
PY="${PY:-$REPO/.venv/bin/python}"

RHO="4"
RATE="1000"
REPS="3"
while [ $# -gt 0 ]; do
    case "$1" in
        --rho)  RHO="${2:?--rho 需要取值}"; shift 2 ;;
        --rate) RATE="${2:?--rate 需要取值}"; shift 2 ;;
        --reps) REPS="${2:?--reps 需要取值}"; shift 2 ;;
        -h|--help) sed -n '2,8p' "$0"; exit 0 ;;
        *) echo "未知参数: $1" >&2; exit 2 ;;
    esac
done

# run.py 的 --group 一次只收一个组态，两个组态分两趟跑
for g in traincache-cold traincache-warm; do
    "$PY" "$ROOT/run.py" compare --rho "$RHO" --rate "$RATE" --reps "$REPS" --group "$g"
done
