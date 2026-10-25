#!/usr/bin/env bash
# 快速跑分:只跑 traincache-cold 与 traincache-warm,不跑 direct/juicefs/fluid 对照。
#
# 用法:
#   bash bench/scripts/bench_quick.sh --tier S --rate 1000 --reps 3 [--tag TAG]
#   make bench-quick TIER=S RATE=1000 REPS=3 [TAG=...]
#
# 产物落 $(config.py RESULTS_DIR)/quick/<run_id>/:
#   summary.csv   run_bench.py 原始逐轮产物(复用对照层测量体与统计列)
#   run.json      quick 元数据(run_id、git、档位、带宽、有效轮次)
#   lock.json     experiments/lock.py 的环境清单
#   vs_last.md    存在上一次 quick 产物时生成的两阶段差值表(不画图)
#
# run_id = UTC 时间戳(yyyymmddThhmmssZ) + git short sha,可选 -<TAG>。
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
PY="${PY:-$REPO/.venv/bin/python}"

usage() {
    sed -n '2,16p' "$0"
}

TIER="S"
RATE="1000"
REPS="3"
TAG=""
while [ $# -gt 0 ]; do
    case "$1" in
        --tier) TIER="${2:?--tier 需要取值}"; shift 2 ;;
        --rate) RATE="${2:?--rate 需要取值}"; shift 2 ;;
        --reps) REPS="${2:?--reps 需要取值}"; shift 2 ;;
        --tag)  TAG="${2:?--tag 需要取值}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "未知参数: $1" >&2; usage >&2; exit 2 ;;
    esac
done

case "$TIER" in
    S) ;;
    M|L)
        echo "错误: bench-quick 只支持 TIER=S。" >&2
        echo "      M/L 档按 9.1 D30 不含 traincache-warm,凑不齐 cold+warm 两阶段。" >&2
        exit 2
        ;;
    *)
        echo "错误: TIER 必须是 S/M/L(收到 $TIER)" >&2
        exit 2
        ;;
esac
case "$REPS" in
    ''|*[!0-9]*|0)
        echo "错误: REPS 必须是正整数(收到 $REPS)" >&2
        exit 2
        ;;
esac

RATE_STR="$(printf '%s' "$RATE" | tr -d '[:space:]')"
if ! printf '%s' "$RATE_STR" | grep -Eq '^[0-9]+(mbit)?$'; then
    echo "错误: RATE 只接受 <数值> 或 <数值>mbit(如 1000 或 1000mbit),收到 $RATE" >&2
    exit 2
fi
case "$RATE_STR" in
    *mbit) ;;                    # 已带单位
    *) RATE_STR="${RATE_STR}mbit" ;;
esac

STEP="初始化"
trap 'echo "FAILED STEP: ${STEP}" >&2' ERR

STEP="读取 config.py"
RESULTS_DIR="$("$PY" "$ROOT/config.py" --sh RESULTS_DIR)"
TC_DELAY="$("$PY" "$ROOT/config.py" --sh TC_DELAY)"
if [ -z "$RESULTS_DIR" ]; then
    echo "config.py 的 RESULTS_DIR 为空" >&2
    exit 1
fi

STEP="生成 run_id"
GIT_FULL="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_SHORT="$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo nogit)"
if [ -n "$(git -C "$REPO" status --porcelain 2>/dev/null || true)" ]; then
    GIT_DIRTY=true
else
    GIT_DIRTY=false
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${STAMP}-${GIT_SHORT}"
if [ -n "$TAG" ]; then
    SAFE_TAG="$(printf '%s' "$TAG" | tr -c 'A-Za-z0-9._-' '-')"
    SAFE_TAG="${SAFE_TAG#-}"
    SAFE_TAG="${SAFE_TAG%-}"
    if [ -n "$SAFE_TAG" ]; then
        RUN_ID="${RUN_ID}-${SAFE_TAG}"
    fi
fi
QUICK_ROOT="$RESULTS_DIR/quick"
RUN_DIR="$QUICK_ROOT/$RUN_ID"
if [ -e "$RUN_DIR" ]; then
    echo "产物目录已存在: $RUN_DIR" >&2
    exit 1
fi
mkdir -p "$RUN_DIR"

# 上一次 quick 产物:按 run_id 字典序(时间戳前缀)取最近一个含 summary.csv 的目录。
PREV_DIR=""
if [ -d "$QUICK_ROOT" ]; then
    while IFS= read -r d; do
        [ -n "$d" ] || continue
        [ "$(basename "$d")" = "$RUN_ID" ] && continue
        [ -f "$d/summary.csv" ] && PREV_DIR="$d"
    done < <(find "$QUICK_ROOT" -mindepth 1 -maxdepth 1 -type d | sort)
fi

STEP="检查并行跑分"
if command -v kubectl >/dev/null 2>&1; then
    running="$(kubectl get pods -A -l app=train-job \
        --field-selector=status.phase=Running --no-headers 2>/dev/null \
        | grep -c . || true)"
    if [ "${running:-0}" -gt 0 ]; then
        echo "检测到 $running 个运行中的 train-job:正式跑分不可并行,先停掉再跑 quick" >&2
        exit 1
    fi
fi

STEP="run_bench.py(traincache-cold + traincache-warm)"
echo "== quick: tier=$TIER rate=$RATE_STR reps=$REPS =="
echo "== 产物: $RUN_DIR =="
export CAPACITY_RATIO="$TIER"
export TC_DELAY="$TC_DELAY"
QUICK_CMD="$PY $ROOT/compare/run_bench.py --tier $TIER --rate $RATE_STR --reps $REPS --groups traincache-cold traincache-warm --out $RUN_DIR/summary.csv"
"$PY" "$ROOT/compare/run_bench.py" \
    --tier "$TIER" \
    --rate "$RATE_STR" \
    --reps "$REPS" \
    --groups traincache-cold traincache-warm \
    --out "$RUN_DIR/summary.csv"

STEP="校验 summary 并写 run.json"
if [ ! -s "$RUN_DIR/summary.csv" ]; then
    echo "summary.csv 不存在或为空: $RUN_DIR/summary.csv" >&2
    exit 1
fi
RUN_ID="$RUN_ID" TAG="$TAG" TIER="$TIER" RATE_STR="$RATE_STR" REPS="$REPS" \
GIT_FULL="$GIT_FULL" GIT_SHORT="$GIT_SHORT" GIT_DIRTY="$GIT_DIRTY" \
CREATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)" QUICK_CMD="$QUICK_CMD" \
"$PY" - "$RUN_DIR" <<'PY'
import csv
import json
import os
import sys
from pathlib import Path

run_dir = Path(sys.argv[1])
groups = ["traincache-cold", "traincache-warm"]
rows = []
with (run_dir / "summary.csv").open(newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if (row.get("void") or "").strip().lower() == "true":
            continue
        if (row.get("status") or "") == "void":
            continue
        rows.append(row)
valid = {g: sum(1 for r in rows if r.get("group") == g) for g in groups}
missing = {g: n for g, n in valid.items() if n == 0}
if missing:
    print(f"FAILED STEP: summary 无有效轮次 {missing}", file=sys.stderr)
    sys.exit(1)

payload = {
    "schema_version": "1",
    "kind": "quick",
    "run_id": os.environ["RUN_ID"],
    "tag": os.environ.get("TAG") or None,
    "created_at": os.environ["CREATED_AT"],
    "git": {
        "commit": os.environ["GIT_FULL"],
        "short": os.environ["GIT_SHORT"],
        "dirty": os.environ["GIT_DIRTY"] == "true",
    },
    "tier": os.environ["TIER"],
    "rate": os.environ["RATE_STR"],
    "reps": int(os.environ["REPS"]),
    "groups": groups,
    "valid_rows": valid,
    "artifacts": {
        "summary_csv": "summary.csv",
        "lock_json": "lock.json",
    },
    "command": os.environ.get("QUICK_CMD", ""),
}
(run_dir / "run.json").write_text(
    json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"有效轮次: {valid}")
PY

STEP="写 lock.json"
# 在 repo 根执行:lock.py 的 git rev-parse 取当前工作目录的仓库。
(
    cd "$REPO"
    "$PY" - "$ROOT" "$RUN_DIR/lock.json" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
sys.path.insert(0, str(root / "experiments"))
import lock  # noqa: E402

lock.write_lock(Path(sys.argv[2]))
PY
)
if [ ! -s "$RUN_DIR/lock.json" ]; then
    echo "lock.json 未写出: $RUN_DIR/lock.json" >&2
    exit 1
fi

STEP="生成 vs_last.md"
if [ -n "$PREV_DIR" ]; then
    "$PY" - "$PREV_DIR" "$RUN_DIR" "$ROOT" <<'PY'
import csv
import json
import sys
from pathlib import Path

prev_dir = Path(sys.argv[1])
cur_dir = Path(sys.argv[2])
root = Path(sys.argv[3])
sys.path.insert(0, str(root / "experiments"))
try:
    import stats  # noqa: E402
    median = stats.median
except Exception:  # noqa: BLE001  numpy 缺失时退回标准库,numpy 由 doctor 另行体检
    import statistics
    median = statistics.median


def read_rows(path):
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (row.get("void") or "").strip().lower() == "true":
                continue
            if (row.get("status") or "") == "void":
                continue
            rows.append(row)
    return rows


def num(row, key):
    value = (row.get(key) or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


BYTE_COLS = ("startup_warmup_bytes", "sustained_prefetch_bytes",
             "switch_reprefetch_bytes")


def aggregate(rows, group):
    picked = [r for r in rows if r.get("group") == group]
    out = {}
    for metric in ("startup_makespan", "total_makespan", "stall_ratio"):
        vals = [num(r, metric) for r in picked]
        vals = [v for v in vals if v is not None]
        out[metric] = float(median(vals)) if vals else None
    costs = []
    for row in picked:
        parts = [num(row, key) for key in BYTE_COLS]
        if any(v is not None for v in parts):
            costs.append(sum(v or 0.0 for v in parts))
    out["byte_cost"] = float(median(costs)) if costs else None
    return out


def fmt(value, spec):
    return "—" if value is None else spec.format(value)


def fmt_diff(cur, prev, spec):
    if cur is None or prev is None:
        return "—", "—"
    diff = cur - prev
    pct = (diff / prev * 100.0) if prev else None
    return spec.format(diff), ("—" if pct is None else f"{pct:+.1f}%")


def meta(directory):
    path = directory / "run.json"
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


cur_rows = read_rows(cur_dir / "summary.csv")
prev_rows = read_rows(prev_dir / "summary.csv")
meta_cur, meta_prev = meta(cur_dir), meta(prev_dir)

lines = [
    "# quick 最近两次对比",
    "",
    f"- 本次: `{cur_dir.name}`",
    f"- 上次: `{prev_dir.name}`",
]
rate_cur = meta_cur.get("rate") or ""
rate_prev = meta_prev.get("rate") or ""
tier_cur = meta_cur.get("tier") or ""
tier_prev = meta_prev.get("tier") or ""
if rate_cur and rate_prev and (rate_cur, tier_cur) != (rate_prev, tier_prev):
    lines.append(f"- 注意: 档位/带宽不同(上次 {tier_prev}@{rate_prev}, "
                 f"本次 {tier_cur}@{rate_cur}),差值仅作参考")
lines.extend([
    "",
    "指标取各 rep 的中位数,void 轮剔除。",
    "字节成本 = startup_warmup_bytes + sustained_prefetch_bytes + switch_reprefetch_bytes。",
    "",
    "| 组 | 指标 | 上次 | 本次 | 差值 | 变化 |",
    "|---|---|---:|---:|---:|---:|",
])

metrics = [
    ("startup_makespan", "启动 makespan (s)", "{:.3f}"),
    ("total_makespan", "全程 makespan (s)", "{:.3f}"),
    ("stall_ratio", "stall 占比", "{:.4f}"),
    ("byte_cost", "字节成本 (B)", "{:.0f}"),
]
for group in ("traincache-cold", "traincache-warm"):
    prev_agg = aggregate(prev_rows, group)
    cur_agg = aggregate(cur_rows, group)
    for key, label, spec in metrics:
        prev_v, cur_v = prev_agg[key], cur_agg[key]
        diff_v, pct_v = fmt_diff(cur_v, prev_v, spec)
        lines.append(
            f"| {group} | {label} | {fmt(prev_v, spec)} | "
            f"{fmt(cur_v, spec)} | {diff_v} | {pct_v} |")

(cur_dir / "vs_last.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"已写 {cur_dir / 'vs_last.md'}(对比 {prev_dir.name})")
PY
else
    echo "没有上一次 quick 产物,跳过 vs_last.md"
fi

STEP="收尾"
echo "== 完成 =="
echo "  summary.csv: $RUN_DIR/summary.csv"
echo "  run.json:    $RUN_DIR/run.json"
echo "  lock.json:   $RUN_DIR/lock.json"
if [ -f "$RUN_DIR/vs_last.md" ]; then
    echo "  vs_last.md:  $RUN_DIR/vs_last.md"
fi
