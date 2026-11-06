#!/usr/bin/env bash
# netem_degrade.sh — 故障注入:源站(MinIO Pod)eth0 egress 挂 netem 降级。
#
# 挂点(照抄 bench/scripts/tc_apply.sh 的 minio 分支机制,不改它、不 import 它):
#   kubectl -n <ns> get pod -l app=minio → Pod
#   kubectl -n <ns> get pod <pod> -o jsonpath={.spec.nodeName} → 节点容器名
#   docker exec -i <节点容器> bash -s:
#     crictl pods --state Ready --name <pod> -q          → 容器 ID
#     crictl inspectp <cid> | grep -oE '"/var/run/netns/[^"]+"' → sandbox netns
#     nsenter --net=<netns> tc qdisc ... dev eth0
# 必须在"出站方"容器 netns 内挂 eth0 egress:docker0 桥主设备与宿主机 veth 的
# host 侧 egress 都已被实证否决(bench/scripts/tc_apply.sh:4-6、docs/4.1 §1)。
#
# 与 tc_apply.sh 的分工(两个脚本各管各的,别混用):
#   tc_apply.sh     吞吐实验的测量口径:只有固定时延、无抖动、tbf 限速+netem;
#                   其自检断言 `--jitter` 必须被拒绝(bench/scripts/tc_apply.sh:114-118)。
#   netem_degrade.sh 韧性套件的降级注入:时延+抖动+丢包,不参与吞吐测量口径。
#                   所以本脚本接受 --jitter/--loss,与 tc_apply.sh 的限制互不干涉。
#
# 计量口径警告(读结果的人必须知道):
#   netem 挂在 egress 上时,回程的 TCP ACK 也要从源站发出,同样被排队/延迟/丢弃。
#   所以实测延迟 = 网络单向延迟 + TCP 重传/恢复时间。丢包 1% 在千兆链路上
#   会让有效吞吐明显塌(重传占掉带宽),这一档的数字不能当作"纯网络时延"读。
#
# 不踩别人的限速(默认行为,非破坏性):
#   eth0 上已有 root qdisc(例如 tc_apply.sh 挂的 tbf)时,本脚本拒绝覆盖并报错,
#   提示先跑 bench/scripts/tc_teardown.sh;确需覆盖加 --force(会删掉原 root qdisc)。
#   自己挂的 netem 用固定 handle 标记,重复执行是幂等替换。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "$SCRIPT_DIR/../lib/common.sh"

NS="$CHAOS_MINIO_NS_DEFAULT"
LABEL="$CHAOS_MINIO_LABEL_DEFAULT"
POD=""
DELAY="50ms"
JITTER="20ms"
LOSS="1%"
DISTRIBUTION="normal"
HANDLE="1:"
DEV="eth0"
RATE=""            # 空 = 只挂 netem(档 2A);给了速率 = tbf 限速 + netem(档 2B)
BURST=""           # 空 = 按 0.1 秒传输量推导(与 bench/scripts/tc_apply.sh 同一口径)
NETEM_HANDLE="10:"
FORCE=0
RESTORE=0
OBSERVE=30
TRAIN_NS="$CHAOS_NS_DEFAULT"
TRAIN_POD="$CHAOS_TRAIN_POD_DEFAULT"
DRY_RUN=0
KUBECONFIG_ARG=""
OUT_DIR=""

usage() {
  cat <<'EOF'
用法: netem_degrade.sh [--ns NAMESPACE] [--pod MINIO_POD] [--label app=minio]
                       [--delay 50ms] [--jitter 20ms] [--loss 1%]
                       [--distribution normal|uniform|pareto]
                       [--rate 300mbit] [--burst 3750k]
                       [--handle HANDLE] [--dev eth0] [--restore]
                       [--observe SECONDS] [--train-ns NAMESPACE] [--train-pod POD]
                       [--force] [--out DIR] [--kubeconfig PATH]
                       [--dry-run] [--help]
  --ns           源站所在 namespace(默认 minio)
  --pod          直接指定源站 Pod 名(不给则按 label 取第一个)
  --label        Pod 选择器(默认 app=minio,bench/k8s/minio.yaml:30)
  --delay        基础时延(默认 50ms)
  --jitter       抖动幅度(默认 20ms;随 delay 一起给,netem 第二参数)
  --loss         丢包率(默认 1%)
  --distribution 抖动分布(默认 normal)
  --rate         限速档:给了速率就挂 tbf root + netem 子队列(档 2B);不给只挂 netem(档 2A)
  --burst        tbf 突发桶;不给按 0.1 秒传输量推导(与 bench/scripts/tc_apply.sh 同一口径)
  --handle       root qdisc 的 handle(默认 1:)
  --dev          网卡(默认 eth0)
  --restore      拆掉本脚本挂的 qdisc(按 handle 精确删,不碰别的 qdisc)
  --observe      挂上之后的采样窗口秒数(默认 30),期间采在途字节/读等待
  --train-ns     训练 Pod 所在 namespace(默认 traincache)
  --train-pod    训练 Pod 名(默认 traincache-train)
  --force        eth0 上已有 root qdisc 时也覆盖(会删掉原 root qdisc)
  --out          证据目录(写 qdisc.txt / params.txt / assertions.txt)
  --kubeconfig   本轮 kubeconfig 副本;不给则要求环境变量 KUBECONFIG
  --dry-run      只打印将执行的命令,不碰集群与容器
  --help         本用法
注意: netem 挂 egress 会连带丢 ACK,实测延迟含 TCP 恢复时间,不是纯网络时延。
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --ns) [ "$#" -ge 2 ] || { echo "错误:--ns 缺值" >&2; exit 2; }; NS="$2"; shift 2 ;;
    --ns=*) NS="${1#--ns=}"; shift ;;
    --pod) [ "$#" -ge 2 ] || { echo "错误:--pod 缺值" >&2; exit 2; }; POD="$2"; shift 2 ;;
    --pod=*) POD="${1#--pod=}"; shift ;;
    --label) [ "$#" -ge 2 ] || { echo "错误:--label 缺值" >&2; exit 2; }; LABEL="$2"; shift 2 ;;
    --label=*) LABEL="${1#--label=}"; shift ;;
    --delay) [ "$#" -ge 2 ] || { echo "错误:--delay 缺值" >&2; exit 2; }; DELAY="$2"; shift 2 ;;
    --delay=*) DELAY="${1#--delay=}"; shift ;;
    --jitter) [ "$#" -ge 2 ] || { echo "错误:--jitter 缺值" >&2; exit 2; }; JITTER="$2"; shift 2 ;;
    --jitter=*) JITTER="${1#--jitter=}"; shift ;;
    --loss) [ "$#" -ge 2 ] || { echo "错误:--loss 缺值" >&2; exit 2; }; LOSS="$2"; shift 2 ;;
    --loss=*) LOSS="${1#--loss=}"; shift ;;
    --rate) [ "$#" -ge 2 ] || { echo "错误:--rate 缺值" >&2; exit 2; }; RATE="$2"; shift 2 ;;
    --rate=*) RATE="${1#--rate=}"; shift ;;
    --burst) [ "$#" -ge 2 ] || { echo "错误:--burst 缺值" >&2; exit 2; }; BURST="$2"; shift 2 ;;
    --burst=*) BURST="${1#--burst=}"; shift ;;
    --distribution) [ "$#" -ge 2 ] || { echo "错误:--distribution 缺值" >&2; exit 2; }; DISTRIBUTION="$2"; shift 2 ;;
    --distribution=*) DISTRIBUTION="${1#--distribution=}"; shift ;;
    --handle) [ "$#" -ge 2 ] || { echo "错误:--handle 缺值" >&2; exit 2; }; HANDLE="$2"; shift 2 ;;
    --handle=*) HANDLE="${1#--handle=}"; shift ;;
    --dev) [ "$#" -ge 2 ] || { echo "错误:--dev 缺值" >&2; exit 2; }; DEV="$2"; shift 2 ;;
    --dev=*) DEV="${1#--dev=}"; shift ;;
    --restore) RESTORE=1; shift ;;
    --observe) [ "$#" -ge 2 ] || { echo "错误:--observe 缺值" >&2; exit 2; }; OBSERVE="$2"; shift 2 ;;
    --observe=*) OBSERVE="${1#--observe=}"; shift ;;
    --train-ns) [ "$#" -ge 2 ] || { echo "错误:--train-ns 缺值" >&2; exit 2; }; TRAIN_NS="$2"; shift 2 ;;
    --train-ns=*) TRAIN_NS="${1#--train-ns=}"; shift ;;
    --train-pod) [ "$#" -ge 2 ] || { echo "错误:--train-pod 缺值" >&2; exit 2; }; TRAIN_POD="$2"; shift 2 ;;
    --train-pod=*) TRAIN_POD="${1#--train-pod=}"; shift ;;
    --force) FORCE=1; shift ;;
    --out) [ "$#" -ge 2 ] || { echo "错误:--out 缺值" >&2; exit 2; }; OUT_DIR="$2"; shift 2 ;;
    --out=*) OUT_DIR="${1#--out=}"; shift ;;
    --kubeconfig) [ "$#" -ge 2 ] || { echo "错误:--kubeconfig 缺值" >&2; exit 2; }; KUBECONFIG_ARG="$2"; shift 2 ;;
    --kubeconfig=*) KUBECONFIG_ARG="${1#--kubeconfig=}"; shift ;;
    *) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
  esac
done

chaos_resolve_kubeconfig "$KUBECONFIG_ARG"

# ---- burst 推导:与 bench/scripts/tc_apply.sh 同一口径(约 0.1 秒的传输量)----
# bench/config.py 由 bench 拥有,这里只调用它的 burst_for_rate,不自写公式。
BENCH_DIR="$(cd "$CHAOS_ROOT/../" && pwd)"
PY="${PY:-/usr/bin/python3}"

rate_to_mbit() {   # 只认带单位的写法(mbit/kbit/gbit),与 tc 自身语义对齐
  local r="$1" n u m
  r="${r//[[:space:]]/}"
  n="${r%%[!0-9.]*}"
  [ -n "$n" ] || return 1
  u="$(printf '%s' "${r#"$n"}" | tr 'A-Z' 'a-z')"
  case "$u" in
    mbit|mbps) m="$n" ;;
    kbit|kbps) m="$(awk -v x="$n" 'BEGIN{printf "%.6f", x/1000}')" ;;
    gbit|gbps) m="$(awk -v x="$n" 'BEGIN{printf "%.6f", x*1000}')" ;;
    *) return 1 ;;
  esac
  printf '%s' "$m"
}

derive_burst() {   # $1 = rate(带单位) → "1250k"/"3750k" 这类 tbf 突发桶
  local rate="$1" mbit
  mbit="$(rate_to_mbit "$rate")" || return 1
  "$PY" -c 'import sys; sys.path.insert(0, sys.argv[1]); import config; print(config.burst_for_rate(float(sys.argv[2])))' \
    "$BENCH_DIR" "$mbit" 2>/dev/null
}

if [ -n "$RATE" ]; then
  if [ -z "$BURST" ]; then
    BURST="$(derive_burst "$RATE" || true)"
    if [ -z "$BURST" ]; then
      echo "错误:解析不出 rate=$RATE 的 burst,请显式给 --burst(例:--burst 3750k)" >&2
      exit 2
    fi
  fi
fi

# ---- netem 参数拼装 ----
# delay 带抖动:netem 的 delay 参数形态是 "delay <基础> <抖动>",抖动跟着基础时延给。
NETEM_ARGS="delay $DELAY"
if [ -n "$JITTER" ] && [ "$JITTER" != "0" ] && [ "$JITTER" != "0ms" ]; then
  NETEM_ARGS="$NETEM_ARGS $JITTER distribution $DISTRIBUTION"
fi
if [ -n "$LOSS" ] && [ "$LOSS" != "0" ] && [ "$LOSS" != "0%" ]; then
  NETEM_ARGS="$NETEM_ARGS loss $LOSS"
fi

if [ "$DRY_RUN" = 1 ]; then
  chaos_note "[dry-run] 故障:netem_degrade(源站 eth0 egress 挂 netem: $NETEM_ARGS)"
  chaos_show_cmd -n "$NS" get pod -l "$LABEL" -o jsonpath='{.items[0].metadata.name}'
  chaos_note "[dry-run]   ↑ 源站 Pod(标签 $LABEL)"
  chaos_show_cmd -n "$NS" get pod "\$POD" -o jsonpath='{.spec.nodeName}'
  chaos_note "[dry-run]   ↑ 节点容器名(kind 里节点名=容器名)"
  chaos_show_cmd exec -i "\$NODE" bash -s -- "\$POD" "\$NETEM_ARGS" "\$HANDLE" "\$DEV" "\$RESTORE" "\$FORCE"
  chaos_note "[dry-run]   ↑ 在节点容器内执行下列脚本(经 stdin):"
  chaos_show_raw 'CID=$(crictl pods --state Ready --name "$POD" -q | head -1)'
  chaos_show_raw 'NETNS=$(crictl inspectp "$CID" | grep -oE '\''"/var/run/netns/[^"]+"'\'' | head -1 | tr -d '\''"'\'')'
  chaos_show_raw '[ -n "$NETNS" ] || { echo "找不到 Pod sandbox netns"; exit 1; }'
  chaos_show_raw 'nsenter --net="$NETNS" tc qdisc show dev eth0'
  chaos_show_raw '# 已有 root qdisc 且非 --force → 拒绝覆盖(tc_apply.sh 的 tbf 会被保留)'
  if [ -n "$RATE" ]; then
    chaos_show_raw "nsenter --net=\"\$NETNS\" tc qdisc del dev eth0 root 2>/dev/null || true"
    chaos_show_raw "nsenter --net=\"\$NETNS\" tc qdisc add dev eth0 root handle $HANDLE tbf rate $RATE burst $BURST latency 50ms"
    chaos_show_raw "nsenter --net=\"\$NETNS\" tc qdisc add dev eth0 parent ${HANDLE}1 handle $NETEM_HANDLE netem $NETEM_ARGS"
  else
    chaos_show_raw "nsenter --net=\"\$NETNS\" tc qdisc replace dev eth0 root handle $HANDLE netem $NETEM_ARGS"
  fi
  chaos_show_raw 'nsenter --net="$NETNS" tc qdisc show dev eth0'
  chaos_show_raw '# --restore:只删本脚本的 handle'
  chaos_show_raw 'nsenter --net="$NETNS" tc qdisc del dev eth0 root handle 1:'
  chaos_show_cmd -n "$TRAIN_NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics"
  chaos_note "[dry-run]   ↑ 采样窗口 ${OBSERVE}s 内取 traincache_fetch_inflight_bytes / traincache_read_stall_seconds_*(每 3s 一次)"
  chaos_show_cmd -n "$TRAIN_NS" get pod "$TRAIN_POD" -o jsonpath='{.status.phase}'
  chaos_note "[dry-run]   ↑ 训练 Pod 存活判据(期望 Running)"
  chaos_note "[dry-run] 生效 netem 参数: $NETEM_ARGS"
  [ -n "$RATE" ] && chaos_note "[dry-run] 生效限速档: tbf rate=$RATE burst=$BURST(档 2B)"
  exit 0
fi

chaos_require_kubeconfig
chaos_prepare_out_dir
ASSERT_FILE="$OUT_DIR/assertions.txt"
: >"$ASSERT_FILE"
pass() { printf 'PASS %s\n' "$*" | tee -a "$ASSERT_FILE"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$ASSERT_FILE" >&2; }

ctx="$(chaos_record_context)"
chaos_note "kubeconfig=$CHAOS_KUBECONFIG context=$ctx ns=$NS"

# ---- 定位源站 Pod 与节点容器 ----
if [ -z "$POD" ]; then
  POD="$(chaos_kubectl -n "$NS" get pod -l "$LABEL" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
fi
[ -n "$POD" ] || chaos_die "命名空间 $NS 里取不到源站 Pod(标签 $LABEL);先跑 bench 的 k8s-up"
NODE="$(chaos_kubectl -n "$NS" get pod "$POD" -o jsonpath='{.spec.nodeName}')"
chaos_note "源站 Pod=$POD 节点容器=$NODE netem 参数=$NETEM_ARGS"

QDISCS="$(docker exec -i "$NODE" bash -s -- "$POD" "$NETEM_ARGS" "$HANDLE" "$DEV" "$RESTORE" "$FORCE" "$RATE" "$BURST" "$NETEM_HANDLE" <<'EOS'
set -euo pipefail
POD="$1"; NETEM="$2"; HANDLE="$3"; DEV="$4"; RESTORE="$5"; FORCE="$6"; RATE="$7"; BURST="$8"; NETHANDLE="$9"
CID=$(crictl pods --state Ready --name "$POD" -q | head -1)
NETNS=$(crictl inspectp "$CID" | grep -oE '"/var/run/netns/[^"]+"' | head -1 | tr -d '"')
[ -n "$NETNS" ] || { echo "找不到 Pod sandbox netns(CID=$CID)"; exit 1; }
echo "netns=$NETNS"
echo "--- 挂载前 qdisc ---"
nsenter --net="$NETNS" tc qdisc show dev "$DEV" || true
if [ "$RESTORE" = 1 ]; then
  nsenter --net="$NETNS" tc qdisc del dev "$DEV" root handle "$HANDLE" 2>/dev/null \
    && echo "已拆 handle $HANDLE" || echo "无 handle $HANDLE 可拆"
  echo "--- 拆除后 qdisc ---"
  nsenter --net="$NETNS" tc qdisc show dev "$DEV" || true
  exit 0
fi
# 已有 root qdisc 且不是本脚本的 handle → 默认拒绝覆盖
EXIST=$(nsenter --net="$NETNS" tc qdisc show dev "$DEV" | grep -E '^qdisc [a-z_]+ ' | head -1 || true)
if [ -n "$EXIST" ]; then
  if ! printf '%s' "$EXIST" | grep -q "handle $HANDLE"; then
    if [ "$FORCE" != 1 ]; then
      echo "错误: $DEV 上已有 root qdisc($EXIST),拒绝覆盖。" >&2
      echo "      先跑 bench/scripts/tc_teardown.sh 拆掉既有限速,或加 --force。" >&2
      exit 3
    fi
    nsenter --net="$NETNS" tc qdisc del dev "$DEV" root 2>/dev/null || true
  fi
fi
if [ -n "$RATE" ]; then
  # 档 2B:tbf 挂 root 限速,netem 挂子队列做时延与抖动
  nsenter --net="$NETNS" tc qdisc del dev "$DEV" root 2>/dev/null || true
  nsenter --net="$NETNS" tc qdisc add dev "$DEV" root handle "$HANDLE" \
    tbf rate "$RATE" burst "$BURST" latency 50ms
  nsenter --net="$NETNS" tc qdisc add dev "$DEV" parent "${HANDLE}1" handle "$NETHANDLE" netem $NETEM
else
  # 档 2A:只挂 netem
  nsenter --net="$NETNS" tc qdisc replace dev "$DEV" root handle "$HANDLE" netem $NETEM
fi
echo "--- 已挂载 ---"
nsenter --net="$NETNS" tc qdisc show dev "$DEV"
EOS
)"
printf '%s\n' "$QDISCS" | tee "$OUT_DIR/qdisc.txt"

# ---- 断言 ----
if [ "$RESTORE" = 1 ]; then
  if printf '%s\n' "$QDISCS" | grep -q "已拆 handle $HANDLE"; then
    pass "已按 handle $HANDLE 拆掉 netem"
  else
    pass "本脚本的 handle $HANDLE 本就不存在(幂等)"
  fi
  exit 0
fi

if printf '%s\n' "$QDISCS" | grep -q "netem"; then
  pass "netem 已挂到 $POD 的 $DEV egress:$NETEM_ARGS"
else
  fail "qdisc show 里没有 netem(见 $OUT_DIR/qdisc.txt)"
fi
if [ -n "$RATE" ]; then
  if printf '%s\n' "$QDISCS" | grep -q "tbf"; then
    pass "tbf 限速已挂在 root 子队列:$RATE burst=$BURST"
  else
    fail "qdisc show 里没有 tbf(限速档没挂上限,见 $OUT_DIR/qdisc.txt)"
  fi
fi
if printf '%s\n' "$QDISCS" | grep -q "netns="; then
  pass "sandbox netns 定位成功:$(printf '%s\n' "$QDISCS" | grep -m1 'netns=')"
else
  fail "没有取到 sandbox netns"
fi

# ---- 采样:在途字节峰值 / 读等待分布 / 训练 Pod 存活 ----
AGENTD_POD="$(chaos_agentd_pod_rows "$TRAIN_NS" | awk '$3=="True"{print $1; exit}')"
MAX_INFLIGHT=0
STALL_COUNT=0
STALL_BUCKET60=0
if [ -n "$AGENTD_POD" ]; then
  END_TS=$(( $(date +%s) + OBSERVE ))
  while [ "$(date +%s)" -lt "$END_TS" ]; do
    INF="$(chaos_metric_sum "$TRAIN_NS" "$AGENTD_POD" traincache_fetch_inflight_bytes)"
    STALL_COUNT="$(chaos_metric_sum "$TRAIN_NS" "$AGENTD_POD" traincache_read_stall_seconds_count)"
    STALL_BUCKET60="$(chaos_metric_label_sum "$TRAIN_NS" "$AGENTD_POD" traincache_read_stall_seconds_bucket le 60)"
    MAX_INFLIGHT="$(awk -v a="${INF:-0}" -v m="${MAX_INFLIGHT:-0}" 'BEGIN{print (a+0>m+0)?a:m}')"
    sleep 3
  done
else
  chaos_note "警告:命名空间 $TRAIN_NS 里没有 Ready 的 agentd Pod,在途字节/读等待判据跳过"
fi
TRAIN_PHASE="$(chaos_kubectl -n "$TRAIN_NS" get pod "$TRAIN_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
if [ "$TRAIN_PHASE" = "Running" ]; then
  pass "训练 Pod $TRAIN_NS/$TRAIN_POD 存活(phase=Running)"
elif [ -z "$TRAIN_PHASE" ]; then
  fail "训练 Pod $TRAIN_NS/$TRAIN_POD 不存在:先 kubectl apply -f agent/deploy/train-pod.yaml(网络降级的读等待判据要有训练 Pod 才能成立)"
else
  fail "训练 Pod $TRAIN_NS/$TRAIN_POD phase=$TRAIN_PHASE(期望 Running)"
fi

# 有界性:全部读等待落在 60s 桶内(bucket le=60 的计数等于 _count)
if [ -n "${STALL_COUNT:-}" ] && [ -n "${STALL_BUCKET60:-}" ]; then
  if [ "$STALL_BUCKET60" = "$STALL_COUNT" ]; then
    pass "读等待全部在 60s 内(count=$STALL_COUNT,le60=$STALL_BUCKET60)"
  else
    fail "有读等待超过 60s(count=$STALL_COUNT,le60=$STALL_BUCKET60)"
  fi
else
  fail "取不到读等待指标,无法判有界性"
fi

# 限速档:带宽上限指标生效(core/fetch 的字节信号量上限)
BWLIMIT="$(chaos_metric "$TRAIN_NS" "${AGENTD_POD:-}" traincache_fetch_bandwidth_limit_bytes)"
if [ -n "$RATE" ]; then
  if [ -n "$BWLIMIT" ] && [ "$BWLIMIT" != "0" ]; then
    pass "带宽上限指标生效:traincache_fetch_bandwidth_limit_bytes=$BWLIMIT"
  else
    fail "取不到 traincache_fetch_bandwidth_limit_bytes(限速档判据缺一项)"
  fi
fi

printf 'delay=%s\njitter=%s\nloss=%s\ndistribution=%s\nhandle=%s\ndev=%s\npod=%s\nnode=%s\n' \
  "$DELAY" "$JITTER" "$LOSS" "$DISTRIBUTION" "$HANDLE" "$DEV" "$POD" "$NODE" >"$OUT_DIR/params.txt"
[ -n "$RATE" ] && printf 'rate=%s\nburst=%s\ntier=2B\n' "$RATE" "$BURST" >>"$OUT_DIR/params.txt"
[ -z "$RATE" ] && printf 'tier=2A\n' >>"$OUT_DIR/params.txt"
printf 'max_fetch_inflight_calls=%s\nread_stall_count=%s\nread_stall_bucket_le60=%s\ntrain_pod=%s\ntrain_pod_phase=%s\nagentd_pod=%s\nobserve_s=%s\nbandwidth_limit_bytes=%s\n' \
  "$MAX_INFLIGHT" "${STALL_COUNT:-0}" "${STALL_BUCKET60:-0}" "$TRAIN_POD" "${TRAIN_PHASE:-}" \
  "${AGENTD_POD:-}" "$OBSERVE" "${BWLIMIT:-}" >>"$OUT_DIR/params.txt"
chaos_note "采样:${OBSERVE}s 内 max_fetch_inflight_calls=$MAX_INFLIGHT read_stall_count=${STALL_COUNT:-0} bucket_le60=${STALL_BUCKET60:-0}"
chaos_note "qdisc 已回读并落证据:$OUT_DIR/qdisc.txt"
