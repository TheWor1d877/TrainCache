#!/usr/bin/env bash
# ============================================================================
# D 轨装置编排(down):拆 tc → 停容器 → 删容器(幂等,可重复执行)
# ----------------------------------------------------------------------------
# 零参数;参数自读 bench/config.py 的 TRACK_D_*。
# 步骤(与任务约定逐字一致):
#   ① tc_teardown.sh --target source-d
#   ② docker stop traincache-source-d
#   ③ docker rm traincache-source-d
# 数据(TRACK_D_DIR)与结果(TRACK_D_RESULTS_DIR)目录保留,不删除。
# 开关:--dry-run(只打印将执行的命令)/ -h|--help。
# ============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PY:-/usr/bin/python3.12}"
CFG() { "$PY" "$ROOT/config.py" --sh "$1"; }

NAME="$(CFG TRACK_D_SOURCE_CONTAINER)"
DATA_DIR="$(CFG TRACK_D_SOURCE_MOUNT)"
OUT_DIR="$(CFG TRACK_D_RESULTS_DIR)"
TARGET="source-d"                                # tc_apply/tc_teardown 的 --target 别名

usage() {
  cat <<EOF
用法: $(basename "$0") [--dry-run] [-h|--help]

D 轨装置拆除(零参数即全流程):tc_teardown.sh --target $TARGET →
docker stop $NAME → docker rm $NAME(幂等)。
数据目录 $DATA_DIR 与结果目录 $OUT_DIR 保留,不删除。
  --dry-run   只打印将执行的命令,不触碰 docker/tc
EOF
}

DRY=0
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY=1 ;;
    *) echo "未知参数: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

say() { echo "[dry-run] $*"; }

echo "== D 轨 down:tc_teardown → docker stop → docker rm =="
if [ "$DRY" = 1 ]; then
  say "bash $ROOT/scripts/tc_teardown.sh --target $TARGET(容器不存在时跳过)"
  say "docker stop $NAME(容器不存在时跳过)"
  say "docker rm $NAME(容器不存在时跳过)"
  say "数据($DATA_DIR)与结果($OUT_DIR)目录保留"
  exit 0
fi

if ! docker inspect "$NAME" >/dev/null 2>&1; then
  echo "  容器 $NAME 不存在 → 已处于 down 状态(幂等,无操作)"
  exit 0
fi

echo "-- ①) 拆 tc(--target $TARGET)--"
if grep -q -- '--target' "$ROOT/scripts/tc_teardown.sh"; then
  bash "$ROOT/scripts/tc_teardown.sh" --target "$TARGET" \
    || echo "  警告:tc 拆除失败(容器随后删除,netns 随容器一并消失;继续)"
else
  echo "  警告:tc_teardown.sh 尚不支持 --target;跳过(容器删除后 qdisc 随 netns 消失)"
fi

echo "-- ②) docker stop $NAME --"
docker stop "$NAME" >/dev/null && echo "  已停止 $NAME"

echo "-- ③) docker rm $NAME --"
docker rm "$NAME" >/dev/null && echo "  已删除 $NAME"

if docker inspect "$NAME" >/dev/null 2>&1; then
  echo "拆除校验失败:容器 $NAME 仍存在" >&2
  exit 1
fi
echo "D 轨装置已拆除(数据与结果目录保留)"
