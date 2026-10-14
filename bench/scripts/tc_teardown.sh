#!/usr/bin/env bash
# 拆除 tc 限速(与 tc_apply.sh 对称;幂等,可重复执行)。
#
# 用法:tc_teardown.sh [--target minio|source-d] [--dry-run] [--help]
#   minio(默认):拆 kind 内 MinIO Pod 的 eth0 egress 限速 —— 既有行为,与改造前一致
#                (无参调用即此路径,make bench-down / cold_reset.sh 流程不变)。
#   source-d   :拆 v4 D 轨本地源站容器(名取自 config.py 的 TRACK_D_SOURCE_CONTAINER,
#                可被同名环境变量覆盖)的 eth0 egress 限速;容器不在跑时 qdisc 已随
#                netns 消失,视为已拆,直接跳过。
#   --dry-run  :只打印将执行的命令,不实际执行。
# root 通道与回退同 tc_apply.sh(root 直跑 → sudo -n → sudo -S(.sudo-pass)→ 特权 docker)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PY:-/usr/bin/python3.12}"   # 与 Makefile/探针先例统一(config.py 仅标准库)
CFG() { "$PY" "$ROOT/config.py" --sh "$1"; }

usage() {
  cat <<'EOF'
用法: tc_teardown.sh [--target minio|source-d] [--dry-run] [--help]
  (无参/--target minio)  拆 kind 内 MinIO Pod 的 eth0 egress 限速(既有行为,不变)
  --target source-d      拆本地源站容器 TRACK_D_SOURCE_CONTAINER 的 eth0 egress 限速
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
      [ "$#" -ge 2 ] || { echo "错误:--target 缺值(应为 minio 或 source-d)" >&2; usage >&2; exit 2; }
      TARGET="$2"; shift 2 ;;
    --target=*) TARGET="${1#--target=}"; shift ;;
    -*) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
    *)  echo "错误:本脚本不接受位置参数('$1')" >&2; usage >&2; exit 2 ;;
  esac
done
case "$TARGET" in
  minio|source-d) ;;
  *) echo "错误:--target 只支持 minio|source-d(收到 '$TARGET')" >&2; usage >&2; exit 2 ;;
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
# ================= source-d(v4 D 轨:宿主机本地源站容器拆除)=================
source-d)
  NAME="${TRACK_D_SOURCE_CONTAINER:-$(CFG TRACK_D_SOURCE_CONTAINER)}"
  TC_IMAGE="${TC_IMAGE:-kindest/node:v1.28.15}"   # 同 tc_apply.sh,本机已有
  if [ "$DRY_RUN" = 1 ]; then
    echo "[dry-run] target=source-d container=$NAME 将执行(本次不实际执行):"
    cat <<EOF
[dry-run]   docker inspect -f '{{.State.Pid}}' $NAME   # → PID(容器不在跑则视为已拆)
[dry-run]   # 首选通道 root 直跑 / sudo -n / sudo -S(.sudo-pass):
[dry-run]   nsenter -t \$PID -n tc qdisc del dev eth0 root
[dry-run]   # 回退(无 sudo 权限):特权容器共享目标 netns,镜像 $TC_IMAGE
[dry-run]   docker run --rm --net=container:$NAME --cap-add=NET_ADMIN --entrypoint /bin/sh $TC_IMAGE -c 'tc qdisc del dev eth0 root'
EOF
    exit 0
  fi
  PID=$(docker inspect -f '{{.State.Pid}}' "$NAME" 2>/dev/null) || PID=""
  if [ -z "$PID" ] || [ "$PID" = 0 ]; then
    echo "容器 $NAME 不存在/不在运行:qdisc 随 netns 消失,无需拆除"
    exit 0
  fi

  # ---- root 通道探测同 tc_apply.sh ----
  ROOT_VIA=""
  if [ "$(id -u)" -eq 0 ]; then ROOT_VIA="root"
  elif sudo -n true 2>/dev/null; then ROOT_VIA="sudo -n"
  elif [ -r "$ROOT/.sudo-pass" ] && printf '%s\n' "$(cat "$ROOT/.sudo-pass")" | sudo -S -p '' true 2>/dev/null; then
    ROOT_VIA="sudo -S(.sudo-pass)"
  fi
  run_root() {
    case "$ROOT_VIA" in
      root)      "$@" ;;
      "sudo -n") sudo -n "$@" ;;
      *)         printf '%s\n' "$(cat "$ROOT/.sudo-pass")" | sudo -S -p '' "$@" ;;
    esac
  }
  if [ -n "$ROOT_VIA" ]; then
    run_root nsenter -t "$PID" -n tc qdisc del dev eth0 root 2>/dev/null \
      && echo "已拆除 $NAME eth0 限速(root 通道:$ROOT_VIA)" || echo "无 qdisc 可拆"
  else
    docker image inspect "$TC_IMAGE" >/dev/null 2>&1 \
      || { echo "错误:本机无回退镜像 $TC_IMAGE(用 TC_IMAGE= 指定本地已有镜像)" >&2; exit 1; }
    docker run --rm --net="container:$NAME" --cap-add=NET_ADMIN --entrypoint /bin/sh "$TC_IMAGE" -c "
      tc qdisc del dev eth0 root 2>/dev/null && echo '已拆除 $NAME eth0 限速(特权 docker 回退)' || echo '无 qdisc 可拆'" \
      || { echo "错误:docker 回退拆除失败" >&2; exit 1; }
  fi
  ;;
esac
