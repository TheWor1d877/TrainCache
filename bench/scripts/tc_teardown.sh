#!/usr/bin/env bash
# 拆除 tc 限速(与 tc_apply.sh 对称;幂等,可重复执行)。
#
# 用法:tc_teardown.sh [--target minio] [--dry-run] [--help]
#   minio(默认,唯一挂点):拆 kind 内 MinIO Pod 的 eth0 egress 限速 —— 既有行为,与改造前一致
#                (无参调用即此路径,make bench-down / cold_reset.sh 流程不变)。
#   --dry-run  :只打印将执行的命令,不实际执行。
set -euo pipefail

usage() {
  cat <<'EOF'
用法: tc_teardown.sh [--target minio] [--dry-run] [--help]
  (无参/--target minio)  拆 kind 内 MinIO Pod 的 eth0 egress 限速(既有行为,不变)
  --dry-run              只打印将执行的命令,不实际执行
  --help                 本用法
EOF
}

# ---- 参数解析:无位置参数;--target 缺省 minio(与既有调用完全兼容)----
TARGET="minio"; DRY_RUN=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h)  usage; exit 0 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    --target)
      [ "$#" -ge 2 ] || { echo "错误:--target 缺值(应为 minio)" >&2; usage >&2; exit 2; }
      TARGET="$2"; shift 2 ;;
    --target=*) TARGET="${1#--target=}"; shift ;;
    -*) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
    *)  echo "错误:本脚本不接受位置参数('$1')" >&2; usage >&2; exit 2 ;;
  esac
done
case "$TARGET" in
  minio) ;;
  *) echo "错误:--target 只支持 minio(收到 '$TARGET')" >&2; usage >&2; exit 2 ;;
esac

case "$TARGET" in
# ==================== minio(既有路径,命令形态与改造前逐字一致)====================
minio)
  if [ "$DRY_RUN" = 1 ]; then
    cat <<'EOF'
[dry-run] target=minio 将执行(本次不实际执行):
[dry-run]   kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}'   # → POD
[dry-run]   kubectl -n minio get pod "$POD" -o jsonpath='{.spec.nodeName}'                  # → NODE
[dry-run]   docker exec -i "$NODE" bash -s -- "$POD" <<'EOS'
[dry-run]     CID=$(crictl pods --state Ready --name "$POD" -q | head -1)
[dry-run]     NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[dry-run]     nsenter --net="$NETNS" tc qdisc del dev eth0 root 2>/dev/null && echo 已拆除 || echo 无 qdisc 可拆
[dry-run]   EOS
EOF
    exit 0
  fi
  POD=$(kubectl -n minio get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
  NODE=$(kubectl -n minio get pod "$POD" -o jsonpath='{.spec.nodeName}')
  docker exec -i "$NODE" bash -s -- "$POD" <<'EOF'
set -euo pipefail
POD="$1"
CID=$(crictl pods --state Ready --name "$POD" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || { echo "无 netns,跳过"; exit 0; }
nsenter --net="$NETNS" tc qdisc del dev eth0 root 2>/dev/null \
  && echo "已拆除 $POD eth0 限速" || echo "无 qdisc 可拆"
EOF
  ;;
esac
