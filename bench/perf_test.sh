#!/usr/bin/env bash
# TrainCache 一键性能测试脚本
#
# 用法:
#   bash bench/perf_test.sh [--quick|--full] [--out DIR]
#
#   --quick   冒烟模式: 2GiB + 6步 + 1次 + 1epoch (~25min)
#   --full    完整模式: 8GiB + 24步 + 5次 + 3epoch (~8h)
#   (默认)    标准模式: 2GiB + 6步 + 3次 + 3epoch (~90min)
#   --out DIR 输出目录(默认运行时数据层 <repo 同级>/traincachedata/bench/perf_results,
#             可用 TRAINCACHE_DATA_ROOT 覆盖)
#
# 产物:
#   <out>/summary.csv   跑分原始数据
#   <out>/figures/      对比图表
#   <out>/report.txt    文字摘要

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INVOKE_CWD="$(pwd)"
cd "$PROJECT_ROOT"

MODE="standard"
# 产物落运行时数据层,路径口径由 bench/config.py 的 DATA_ROOT 统一定义。
DATA_ROOT="$(python3 "$PROJECT_ROOT/bench/config.py" --sh DATA_ROOT)"
OUT_BASE="$DATA_ROOT/bench/perf_results"
prev=""
for arg in "$@"; do
  case "$arg" in
    --quick)  MODE="quick" ;;
    --full)   MODE="full" ;;
    --out)    prev="out" ;;
    *)
      if [ "$prev" = "out" ]; then
        OUT_BASE="$arg"
        prev=""
      fi
      ;;
  esac
done

# --out 传入的相对路径按调用方原始 cwd 解释(脚本随后会 cd 到 repo 根)
case "$OUT_BASE" in
  /*) ;;
  *)  OUT_BASE="$INVOKE_CWD/$OUT_BASE" ;;
esac

case "$MODE" in
  quick)
    N_TARS=8
    STEPS=6
    EPOCHS=1
    REPS=1
    ;;
  standard)
    N_TARS=8
    STEPS=6
    EPOCHS=3
    REPS=3
    ;;
  full)
    N_TARS=32
    STEPS=8
    EPOCHS=3
    REPS=5
    ;;
esac

OUT_DIR="$OUT_BASE"
FIG_DIR="$OUT_BASE/figures"
SUMMARY="$OUT_DIR/summary.csv"
LOG="/tmp/perf_test_$(date +%Y%m%d_%H%M%S).log"

echo "============================================================"
echo "  TrainCache 性能测试"
echo "  模式: $MODE"
echo "  数据: ${N_TARS} shard × 256MiB = $(( N_TARS * 256 / 1024 ))GiB"
echo "  训练: ${EPOCHS} epoch × ${STEPS} step = $(( EPOCHS * STEPS )) 步"
echo "  重复: ${REPS} 次/组"
echo "  日志: $LOG"
echo "============================================================"

mkdir -p "$OUT_DIR" "$FIG_DIR"

# --- Step 1: 跑分 ---
echo ""
echo "[1/3] 运行五组对照跑分..."
N_TARS=$N_TARS STEPS_PER_EPOCH=$STEPS EPOCHS=$EPOCHS \
  python3 bench/compare/run_bench.py \
    --reps "$REPS" \
    --out "$SUMMARY" \
    2>&1 | tee "$LOG"

VALID=$(grep -c "void=False" "$LOG" 2>/dev/null || echo "0")
TOTAL=$(( 5 * REPS ))
echo "  完成: $VALID / $TOTAL 条有效记录"

# --- Step 2: 出图 ---
echo ""
echo "[2/3] 生成对比图表..."
python3 bench/experiments/plot.py \
  --summary "$SUMMARY" \
  --out-dir "$FIG_DIR" 2>&1 | grep -E "saved|done" || true

# --- Step 3: 摘要 ---
echo ""
echo "[3/3] 生成文字摘要..."
REPORT="$OUT_DIR/report.txt"
{
  echo "TrainCache 性能测试报告"
  echo "生成时间: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "模式: $MODE | 数据: $(( N_TARS * 256 / 1024 ))GiB | 步数: $(( EPOCHS * STEPS )) | 重复: $REPS"
  echo "============================================================"
  echo ""
  echo "各组平均数 (startup / total / stall / hit_rate):"
  echo "------------------------------------------------------------"
  python3 - <<PY
import csv
from pathlib import Path
from collections import defaultdict
import numpy as np

rows = list(csv.DictReader(open("$SUMMARY")))
groups = ["direct","juicefs-cold","juicefs-warm","fluid","traincache"]
labels = {"direct":"Direct","juicefs-cold":"JuiceFS(cold)","juicefs-warm":"JuiceFS(warm)","fluid":"Fluid","traincache":"TrainCache"}
for g in groups:
    vals = defaultdict(list)
    for r in rows:
        if r.get("group")!=g or r.get("void","").lower()=="true": continue
        for m in ["startup_makespan","total_makespan","stall_ratio","hit_rate"]:
            try: vals[m].append(float(r[m]))
            except: pass
    line = f"{labels[g]:18s}"
    for m in ["startup_makespan","total_makespan","stall_ratio","hit_rate"]:
        v = np.mean(vals[m]) if vals[m] else 0
        line += f"  {v:10.2f}"
    print(line)
print()
print("注: startup/total 单位秒; stall/hit_rate 为比例 (0~1); 取平均数")
PY
} > "$REPORT"

cat "$REPORT"

echo ""
echo "============================================================"
echo "  完成!"
echo "  数据: $SUMMARY"
echo "  图表: $FIG_DIR/"
echo "  报告: $REPORT"
echo "============================================================"
