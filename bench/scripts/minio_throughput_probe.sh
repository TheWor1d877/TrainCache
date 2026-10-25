#!/usr/bin/env bash
# MinIO 吞吐上限探针:在"没有 tc 限速"的状态下量源站的出站能力上限(MB/s)。
#
# 测的是什么:kind 内 MinIO 被读的时候能跑多快 —— 源站出站能力(含集群内部网络)。
# 不是训练 Pod 的读能力:后者受训练 Pod 的网卡/协议栈/CPU 影响,属于另一个量,另测。
# 所以读方放在集群内、用 busybox 直接 wget,量的是"MinIO 往外吐字节"的速率。
#
# 为什么放在跑分第一格:带宽是 D31 的实验自变量,取值 {100,300,1000} mbit,
#   而 1000mbit ≈ 125MB/s。如果源站本身只能跑出 100MB/s,那么 @1000mbit 这一格
#   测到的是源站天花板,不是链路档位 —— 自变量被污染,这一格作废。
#   判据:上限 < 125MB/s → @1000mbit 档失效,带宽轴缩成 {100, 上限} 两档,并回写 D31。
#
# 前置条件:MinIO Pod 的 eth0 egress 上不能挂着 tbf(挂着就量不到上限)。
#   本脚本只检测、提示、退出,不自己删 qdisc —— 删了会和跑分流程打架。
#   挂点与 tc_apply.sh 一致(节点容器 crictl 找 Pod sandbox netns → nsenter)。
#
# 用法:bench/scripts/minio_throughput_probe.sh [--objects N] [--threshold-mb 125]
#         [--image IMG] [--timeout SEC] [--skip-tc-check] [--dry-run] [--help]
#   --objects N        读几个对象(默认 2;每个必须 ≥ 200MiB)
#   --threshold-mb N   判据阈值 MB/s(默认 125,对应 1000mbit)
#   --image IMG        读方镜像(默认取 config.COLD_READ_IMAGE = busybox:latest)
#   --timeout SEC      等 Pod 跑完的上限秒数(默认 900)
#   --skip-tc-check    跳过"tc 已摘除"检查(默认检查;跳过自负其责)
#   --dry-run          只打印将执行的 kubectl 命令,不实际执行
#
# 退出码:0 = 上限 ≥ 阈值(@1000mbit 档可用)
#         2 = 上限 < 阈值(@1000mbit 档失效:带宽轴缩成 {100, 上限} 两档,回写 D31)
#         1 = 其它错误(探针自己跑不起来:找不到 MinIO / tc 状态不明 / 对象读不动)
#
# 输出:最后两行固定为 VERDICT=pass|fail 与 THROUGHPUT_MBPS=<数值>(THROUGHPUT 在最后一行)。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PY:-/usr/bin/python3}"   # 与 tc_apply.sh 统一
CFG() { "$PY" "$ROOT/config.py" --sh "$1"; }

NS_MINIO="$(CFG K8S_NS_MINIO)"
BUCKET="$(CFG MINIO_BUCKET)"
ENDPOINT="$(CFG MINIO_ENDPOINT)"            # http://minio.minio.svc.cluster.local:9000
IMAGE="$(CFG COLD_READ_IMAGE)"              # busybox:latest(集群内冷读同款镜像)
MIN_OBJ_BYTES=$((200 * 1024 * 1024))        # 单对象下限 200MiB:太小则测量窗口太短
THRESHOLD_MB=125
N_OBJECTS=2
TIMEOUT_S=900
DRY_RUN=0
SKIP_TC_CHECK=0
POD="minio-tput-probe"

usage() {
  cat <<'EOF'
用法: minio_throughput_probe.sh [--objects N] [--threshold-mb 125] [--image IMG]
                               [--timeout SEC] [--skip-tc-check] [--dry-run] [--help]
  量的是源站(MinIO)出站能力上限,不是训练 Pod 的读能力。
  判据:上限 < 125MB/s → @1000mbit 档失效,带宽轴缩成 {100, 上限} 两档,回写 D31。
  退出码:0=达到阈值  2=达不到阈值(@1000 档失效)  1=其它错误
示例:
  minio_throughput_probe.sh --dry-run    # 只看将执行哪些 kubectl 命令
  minio_throughput_probe.sh              # 正常测(需集群在线)
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --skip-tc-check) SKIP_TC_CHECK=1; shift ;;
    --objects) [ "$#" -ge 2 ] || { echo "错误:--objects 缺值" >&2; exit 2; }; N_OBJECTS="$2"; shift 2 ;;
    --objects=*) N_OBJECTS="${1#--objects=}"; shift ;;
    --threshold-mb) [ "$#" -ge 2 ] || { echo "错误:--threshold-mb 缺值" >&2; exit 2; }; THRESHOLD_MB="$2"; shift 2 ;;
    --threshold-mb=*) THRESHOLD_MB="${1#--threshold-mb=}"; shift ;;
    --image) [ "$#" -ge 2 ] || { echo "错误:--image 缺值" >&2; exit 2; }; IMAGE="$2"; shift 2 ;;
    --image=*) IMAGE="${1#--image=}"; shift ;;
    --timeout) [ "$#" -ge 2 ] || { echo "错误:--timeout 缺值" >&2; exit 2; }; TIMEOUT_S="$2"; shift 2 ;;
    --timeout=*) TIMEOUT_S="${1#--timeout=}"; shift ;;
    *) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
  esac
done
case "$N_OBJECTS" in
  ''|*[!0-9]*) echo "错误:--objects 必须是正整数(收到 '$N_OBJECTS')" >&2; exit 2 ;;
esac
[ "$N_OBJECTS" -ge 2 ] || { echo "错误:--objects 至少 2(收到 $N_OBJECTS)" >&2; exit 2; }

OBJECTS="$(seq -f 'shard-%05g.tar' 0 $((N_OBJECTS - 1)) | tr '\n' ' ')"
URL="$ENDPOINT/$BUCKET"

# ---- Pod 内脚本:逐对象计时读流(不落盘,直接管道数字节)----
# 计时优先用 /proc/uptime(10ms 分辨率;容器里读到的是宿主机单调时钟,差值可用),
# 退而求其次用 date +%s%N,最后退到 date +%s。
POD_SCRIPT='URL="$1"; OBJS="$2"
now_ms() {
  v=""
  if [ -r /proc/uptime ]; then
    read u _rest < /proc/uptime
    v=$(awk -v t="$u" "BEGIN{printf \"%d\", t*1000}" 2>/dev/null || true)
  fi
  if [ -z "$v" ]; then
    v=$(date +%s%N 2>/dev/null || true)
    case "$v" in
      *N*) v="" ;;
    esac
    [ -n "$v" ] || v=$(date +%s)
  fi
  echo "$v"
}
for f in $OBJS; do
  t0=$(now_ms)
  b=$(wget -q -O - "$URL/$f" | wc -c)
  t1=$(now_ms)
  echo "OBJ $f $b $((t1 - t0))"
done
echo "PROBE_DONE"'

# ---- MinIO Pod eth0 上的 qdisc(查 tbf 是否还挂着)----
minio_pod() { kubectl -n "$NS_MINIO" get pod -l app=minio -o jsonpath='{.items[0].metadata.name}'; }
minio_node() { kubectl -n "$NS_MINIO" get pod "$1" -o jsonpath='{.spec.nodeName}'; }
minio_qdisc() {   # $1=pod $2=node;打印 MinIO Pod eth0 的 qdisc 列表
  local pod="$1" node="$2"
  docker exec -i "$node" bash -s -- "$pod" <<'EOS'
POD="$1"
CID=$(crictl pods --state Ready --name "$POD" -q | head -1)
[ -n "$CID" ] || exit 3
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || exit 3
nsenter --net="$NETNS" tc qdisc show dev eth0
EOS
}

check_tc_absent() {
  local pod node qdisc
  pod="$(minio_pod 2>/dev/null || true)"
  [ -n "$pod" ] || { echo "错误:ns=$NS_MINIO 里找不到 MinIO Pod(label app=minio)" >&2; return 1; }
  node="$(minio_node "$pod" 2>/dev/null || true)"
  [ -n "$node" ] || { echo "错误:取不到 MinIO Pod 所在节点" >&2; return 1; }
  qdisc="$(minio_qdisc "$pod" "$node" 2>/dev/null || true)"
  if [ -z "$qdisc" ]; then
    echo "错误:读不到 MinIO Pod 的 qdisc,无法确认 tc 已摘除(docker/crictl 通道不可用?)" >&2
    echo "  确认确实没有 tc 之后,可加 --skip-tc-check 跳过该检查(自负其责)" >&2
    return 1
  fi
  if printf '%s\n' "$qdisc" | grep -q 'tbf'; then
    echo "错误:MinIO Pod($pod,节点 $node)的 eth0 上还挂着 tbf,量到的是限速值不是源站上限" >&2
    printf '%s\n' "$qdisc" | sed 's/^/    /' >&2
    echo "  请先拆除:bash $ROOT/scripts/tc_teardown.sh --target minio" >&2
    echo "  (本脚本不自行删 qdisc,避免与跑分流程打架)" >&2
    return 1
  fi
  echo "tc 检查通过:MinIO Pod($pod,节点 $node)eth0 上没有 tbf"
  return 0
}

cleanup() { kubectl -n "$NS_MINIO" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true; }

# ============================ dry-run ============================
if [ "$DRY_RUN" = 1 ]; then
  echo "[dry-run] 目标:量 MinIO 出站上限;对象 $N_OBJECTS 个($OBJECTS),阈值 ${THRESHOLD_MB}MB/s"
  cat <<EOF
[dry-run] 将执行(本次不实际执行):
[dry-run]   kubectl -n $NS_MINIO get pod -l app=minio -o jsonpath='{.items[0].metadata.name}'   # → POD
[dry-run]   kubectl -n $NS_MINIO get pod "\$POD" -o jsonpath='{.spec.nodeName}'                  # → NODE
[dry-run]   docker exec -i "\$NODE" bash -s -- "\$POD" <<'EOS'   # 读 MinIO Pod eth0 的 qdisc(查 tbf)
[dry-run]     nsenter --net="\$NETNS" tc qdisc show dev eth0
[dry-run]   EOS
[dry-run]   kubectl -n $NS_MINIO run $POD --image=$IMAGE --restart=Never \\
[dry-run]     --image-pull-policy=IfNotPresent --command -- sh -c '<逐对象计时读流脚本>' probe "$URL" "$OBJECTS"
[dry-run]   kubectl -n $NS_MINIO get pod $POD -o jsonpath='{.status.phase}'   # 轮询到 Succeeded
[dry-run]   kubectl -n $NS_MINIO logs $POD
[dry-run]   kubectl -n $NS_MINIO delete pod $POD --ignore-not-found
EOF
  exit 0
fi

# ============================ 真跑 ============================
echo "== MinIO 吞吐上限探针 =="
echo "目标:源站(MinIO)出站能力上限,不是训练 Pod 的读能力"
echo "对象:$OBJECTS 阈值:${THRESHOLD_MB}MB/s 镜像:$IMAGE"

if [ "$SKIP_TC_CHECK" = 1 ]; then
  echo "警告:已跳过 tc 检查(--skip-tc-check);若链路上还挂着 tbf,量到的值偏低,结论不可用" >&2
else
  check_tc_absent || exit 1
fi

cleanup
trap cleanup EXIT

kubectl -n "$NS_MINIO" run "$POD" --image="$IMAGE" --restart=Never \
  --image-pull-policy=IfNotPresent \
  --command -- sh -c "$POD_SCRIPT" probe "$URL" "$OBJECTS" >/dev/null

echo "读方 Pod 已起:$POD(最多等 ${TIMEOUT_S}s)"
phase=""
waited=0
while [ "$waited" -lt "$TIMEOUT_S" ]; do
  phase="$(kubectl -n "$NS_MINIO" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo Gone)"
  case "$phase" in
    Succeeded|Failed) break ;;
  esac
  sleep 5
  waited=$((waited + 5))
done

LOGS="$(kubectl -n "$NS_MINIO" logs "$POD" 2>&1 || true)"
if [ "$phase" != "Succeeded" ]; then
  echo "错误:读方 Pod 未成功(phase=$phase)" >&2
  printf '%s\n' "$LOGS" | tail -10 >&2
  exit 1
fi
printf '%s\n' "$LOGS" | grep '^OBJ ' || true

# ---- 汇总:总字节 ÷ 总耗时 ----
TOTAL_BYTES=0
TOTAL_MS=0
NREAD=0
while read -r _tag _name _bytes _ms; do
  [ -n "$_bytes" ] || continue
  NREAD=$((NREAD + 1))
  TOTAL_BYTES=$((TOTAL_BYTES + _bytes))
  TOTAL_MS=$((TOTAL_MS + _ms))
done <<EOF
$(printf '%s\n' "$LOGS" | grep '^OBJ ')
EOF

if [ "$NREAD" -lt 2 ]; then
  echo "错误:只读到 $NREAD 个对象(至少 2 个)" >&2
  exit 1
fi
while read -r _tag _name _bytes _ms; do
  [ -n "$_bytes" ] || continue
  if [ "$_bytes" -lt "$MIN_OBJ_BYTES" ]; then
    echo "错误:对象 $_name 只有 $_bytes 字节(< $MIN_OBJ_BYTES),测量窗口太短" >&2
    exit 1
  fi
done <<EOF
$(printf '%s\n' "$LOGS" | grep '^OBJ ')
EOF

if [ "$TOTAL_MS" -le 0 ]; then
  echo "错误:测得耗时 ≤ 0ms,计时不可用" >&2
  exit 1
fi

MBPS="$(awk -v b="$TOTAL_BYTES" -v ms="$TOTAL_MS" 'BEGIN{printf "%.1f", b/1000000/(ms/1000)}')"
echo "--- 汇总 ---"
echo "对象数:$NREAD  总字节:$TOTAL_BYTES  总耗时:${TOTAL_MS}ms"
echo "平均吞吐:${MBPS} MB/s"

if awk -v v="$MBPS" -v t="$THRESHOLD_MB" 'BEGIN{exit !(v>=t)}'; then
  VERDICT="pass"
else
  VERDICT="fail"
fi
echo "VERDICT=$VERDICT"
if [ "$VERDICT" = "fail" ]; then
  echo "结论:源站上限 ${MBPS}MB/s < ${THRESHOLD_MB}MB/s → @1000mbit 档失效" >&2
  echo "      带宽轴缩成 {100, ${MBPS}} 两档,并把这条结论回写 D31" >&2
else
  echo "结论:源站上限 ${MBPS}MB/s ≥ ${THRESHOLD_MB}MB/s → @1000mbit 档可用" >&2
fi
echo "THROUGHPUT_MBPS=$MBPS"
if [ "$VERDICT" = "pass" ]; then exit 0; fi
exit 2
