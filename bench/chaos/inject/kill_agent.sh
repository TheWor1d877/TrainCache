#!/usr/bin/env bash
# kill_agent.sh — 故障注入:SIGKILL agentd Pod,等 DaemonSet 拉起替代 Pod。
#
# 注入手法(与 agent/e2e/run-k8s.sh 验收 2 同义,但那里用 --wait=true 的删除):
#   kubectl -n <ns> delete pod <agentd> --grace-period=0 --wait=false
# --grace-period=0 = 不给 SIGTERM 宽限期,等价于 SIGKILL 语义(容器被杀);
# --wait=false = 不阻塞等删除完成,让 DaemonSet 的替换 Pod 立刻开始拉起,
# 否则脚本自身会被"等待旧 Pod 消失"卡住,测不出重建耗时。
#
# 选哪个 agentd Pod:
#   agentd 是 DaemonSet,每个节点一个 Pod(默认 kind 单 worker 集群 = 1 个)。
#   优先级:--pod 显式指定 > --node 指定节点上的 agentd > 与训练 Pod 同节点的
#   agentd > 第一个 Ready 的 agentd Pod。训练 Pod 同节点的口径取自
#   agent/e2e/run-k8s.sh:76-84(同节点共享 hostPath,缓存才谈得上"重建后仍在")。
#
# 断言(写进证据目录):
#   · 替代 Pod 名与旧 Pod 名不同(Pod 被真删了,不是重启了同一 Pod)
#   · 替代 Pod 在 --timeout 内变 Ready
#   · 重建耗时秒数(replacement_ready_s)
#   · traincache_agent_up 在替代 Pod 上 =1(进程活着)
#
# 安全:只删自己命名空间里的 agentd Pod;不删其它对象;不切 kubectl context。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "$SCRIPT_DIR/../lib/common.sh"

NS="$CHAOS_NS_DEFAULT"
KUBECONFIG_ARG=""
POD=""
NODE=""
TIMEOUT=180
DRY_RUN=0
OUT_DIR=""

usage() {
  cat <<'EOF'
用法: kill_agent.sh [--ns NAMESPACE] [--pod AGENTD_POD] [--node NODE_NAME]
                     [--timeout SECONDS] [--out DIR] [--kubeconfig PATH]
                     [--dry-run] [--help]
  --ns           agentd 所在 namespace(默认 traincache)
  --pod          直接指定要杀的 agentd Pod 名
  --node         指定节点,取该节点上的 agentd Pod
  --timeout      等替代 Pod Ready 的上限秒数(默认 180)
  --out          证据目录(写 replacement_pod.txt / timings.txt / assertions.txt)
  --kubeconfig   本轮 kubeconfig 副本;不给则要求环境变量 KUBECONFIG
  --dry-run      只打印将执行的命令,不碰集群
  --help         本用法
行为: 删除 agentd Pod(--grace-period=0 --wait=false),等 DaemonSet 拉起替代
      Pod 并变 Ready,打印新 Pod 名与耗时。删的是 DaemonSet 管理的 Pod,
      控制器会自动重建 —— 这是本脚本的预期效果,不是意外。
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
    --node) [ "$#" -ge 2 ] || { echo "错误:--node 缺值" >&2; exit 2; }; NODE="$2"; shift 2 ;;
    --node=*) NODE="${1#--node=}"; shift ;;
    --timeout) [ "$#" -ge 2 ] || { echo "错误:--timeout 缺值" >&2; exit 2; }; TIMEOUT="$2"; shift 2 ;;
    --timeout=*) TIMEOUT="${1#--timeout=}"; shift ;;
    --out) [ "$#" -ge 2 ] || { echo "错误:--out 缺值" >&2; exit 2; }; OUT_DIR="$2"; shift 2 ;;
    --out=*) OUT_DIR="${1#--out=}"; shift ;;
    --kubeconfig) [ "$#" -ge 2 ] || { echo "错误:--kubeconfig 缺值" >&2; exit 2; }; KUBECONFIG_ARG="$2"; shift 2 ;;
    --kubeconfig=*) KUBECONFIG_ARG="${1#--kubeconfig=}"; shift ;;
    *) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
  esac
done

chaos_resolve_kubeconfig "$KUBECONFIG_ARG"

if [ "$DRY_RUN" = 1 ]; then
  chaos_note "[dry-run] 故障:kill_agent(SIGKILL agentd Pod,等 DaemonSet 重建)"
  chaos_show_cmd -n "$NS" get pods -l "$CHAOS_AGENTD_LABEL_DEFAULT" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}'
  chaos_note "[dry-run]   ↑ 选目标 Pod(优先 --pod / --node,否则与训练 Pod 同节点的 agentd,否则第一个 Ready)"
  chaos_show_cmd -n "$NS" get pod "$CHAOS_TRAIN_POD_DEFAULT" -o jsonpath='{.spec.nodeName}'
  chaos_note "[dry-run]   ↑ 训练 Pod 所在节点(用于对齐 hostPath 语义)"
  chaos_show_cmd -n "$NS" delete pod "\$AGENTD_POD" --grace-period=0 --wait=false
  chaos_show_cmd -n "$NS" get pod "\$AGENTD_POD" -o jsonpath='{.metadata.uid}'
  chaos_note "[dry-run]   ↑ 旧 Pod UID(确认对象确实消失)"
  chaos_show_cmd -n "$NS" wait --for=condition=Ready "pod/\$NEW_POD" --timeout="${TIMEOUT}s"
  chaos_show_cmd -n "$NS" get pod "\$NEW_POD" -o jsonpath='{.metadata.name}{" "}{.spec.nodeName}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}'
  chaos_note "[dry-run]   ↑ 新 Pod 名 / 节点 / Ready 状态(轮询直到出现不同于旧名的 Ready Pod)"
  chaos_show_cmd -n "$NS" exec "\$NEW_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics"
  chaos_note "[dry-run]   ↑ agentd /metrics,取 traincache_agent_up(应 =1)、fetch_bytes_total、quarantined_blocks"
  chaos_show_cmd -n "$NS" exec "\$NEW_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    traincache node status --node "\$NODE" --item rescan
  chaos_note "[dry-run]   ↑ rescan 阶段(输出 RESCAN_PHASE,判据:离开 SAMPLING)"
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

# ---- 选目标 agentd Pod ----
ROWS="$(chaos_agentd_pod_rows "$NS")"
[ -n "$ROWS" ] || chaos_die "命名空间 $NS 里没有 agentd Pod(label=$CHAOS_AGENTD_LABEL_DEFAULT)"
TARGET="$POD"
if [ -z "$TARGET" ] && [ -n "$NODE" ]; then
  TARGET="$(printf '%s\n' "$ROWS" | awk -v n="$NODE" '$2==n {print $1; exit}')"
  [ -n "$TARGET" ] || chaos_die "节点 $NODE 上没有 agentd Pod"
fi
if [ -z "$TARGET" ]; then
  TRAIN_NODE="$(chaos_kubectl -n "$NS" get pod "$CHAOS_TRAIN_POD_DEFAULT" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  if [ -n "$TRAIN_NODE" ]; then
    TARGET="$(printf '%s\n' "$ROWS" | awk -v n="$TRAIN_NODE" '$2==n {print $1; exit}')"
  fi
fi
if [ -z "$TARGET" ]; then
  TARGET="$(printf '%s\n' "$ROWS" | awk '$3=="True" {print $1; exit}')"
fi
[ -n "$TARGET" ] || chaos_die "选不出要杀的 agentd Pod(全部不 Ready?):$ROWS"
OLD_UID="$(chaos_kubectl -n "$NS" get pod "$TARGET" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
chaos_note "目标 agentd Pod=$TARGET uid=$OLD_UID"

# ---- 注入 ----
T0="$(date +%s)"
chaos_metrics_text "$NS" "$TARGET" >"$OUT_DIR/metrics_before.txt" 2>/dev/null || true
chaos_kubectl -n "$NS" delete pod "$TARGET" --grace-period=0 --wait=false
chaos_note "已发出 SIGKILL 式删除(--grace-period=0 --wait=false),等替代 Pod…"

# ---- 等替代 Pod ----
NEW_POD=""
NEW_READY=""
while :; do
  NOW="$(date +%s)"
  ELAPSED=$((NOW - T0))
  if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
    break
  fi
  ROWS2="$(chaos_agentd_pod_rows "$NS")"
  CAND="$(printf '%s\n' "$ROWS2" | awk -v old="$TARGET" '$1!=old && $3=="True" {print $1; exit}')"
  if [ -n "$CAND" ]; then
    NEW_POD="$CAND"
    NEW_READY="True"
    break
  fi
  sleep 2
done
T1="$(date +%s)"
REPLACEMENT_READY_S=$((T1 - T0))

[ -n "$OUT_DIR" ] && {
  printf '%s\n' "$NEW_POD" >"$OUT_DIR/replacement_pod.txt"
  printf 'old_pod=%s\nold_uid=%s\nreplacement_pod=%s\nreplacement_ready_s=%s\n' \
    "$TARGET" "$OLD_UID" "$NEW_POD" "$REPLACEMENT_READY_S" >"$OUT_DIR/timings.txt"
}

if [ -n "$NEW_POD" ]; then
  pass "替代 Pod 已 Ready: $NEW_POD(耗时 ${REPLACEMENT_READY_S}s)"
  pass "替代 Pod 名不同于旧 Pod($TARGET != $NEW_POD)"
else
  fail "${TIMEOUT}s 内没有出现 Ready 的替代 agentd Pod(旧 Pod=$TARGET)"
fi

# ---- agent_up 断言 ----
AGENT_UP=""
if [ -n "$NEW_POD" ]; then
  for _ in 1 2 3 4 5 6; do
    AGENT_UP="$(chaos_metric "$NS" "$NEW_POD" traincache_agent_up)"
    [ "$AGENT_UP" = "1" ] && break
    sleep 2
  done
  if [ "$AGENT_UP" = "1" ]; then
    pass "替代 Pod 上 traincache_agent_up=1"
  else
    fail "替代 Pod 上 traincache_agent_up=${AGENT_UP:-<取不到>}(期望 1)"
  fi
fi

# ---- 观测:rescan 阶段 + 回源字节 + 隔离块(判据口径见 docs/4.7-chaos-suite.md 第 1 档)----
RESCAN_PHASE=""
NEW_NODE="$(chaos_agentd_pod_rows "$NS" | awk -v p="$NEW_POD" '$1==p {print $2; exit}')"
if [ -n "$NEW_POD" ]; then
  chaos_metrics_text "$NS" "$NEW_POD" >"$OUT_DIR/metrics_after.txt" 2>/dev/null || true
  # rescan 阶段:命令输出是 "RESCAN_PHASE<TAB>SAMPLING" 形式(agent/cmd/traincache/main.go:415-424)
  for _ in $(seq 1 24); do
    RESCAN_PHASE="$(chaos_kubectl -n "$NS" exec "$NEW_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
      traincache node status --node "${NEW_NODE:-}" --item rescan 2>/dev/null \
      | awk -F'\t' '/^RESCAN_PHASE/{print $2; exit}' || true)"
    case "$RESCAN_PHASE" in
      ""|SAMPLING) sleep 5 ;;
      *) break ;;
    esac
  done
  if [ -n "$RESCAN_PHASE" ] && [ "$RESCAN_PHASE" != "SAMPLING" ]; then
    pass "rescan 阶段已离开 SAMPLING:$RESCAN_PHASE"
  else
    fail "rescan 阶段仍是 ${RESCAN_PHASE:-<取不到>}(期望离开 SAMPLING)"
  fi
fi
FETCH_BYTES="$(chaos_metric_sum "$NS" "$NEW_POD" traincache_fetch_bytes_total)"
QUARANTINED="$(chaos_metric_sum "$NS" "$NEW_POD" traincache_quarantined_blocks)"

[ -n "$OUT_DIR" ] && {
  printf 'agent_up=%s\nreplacement_ready_s=%s\nrescan_phase=%s\nnode=%s\nfetch_bytes_total=%s\nquarantined_blocks=%s\n' \
    "${AGENT_UP:-}" "$REPLACEMENT_READY_S" "${RESCAN_PHASE:-}" "${NEW_NODE:-}" \
    "${FETCH_BYTES:-}" "${QUARANTINED:-}" >"$OUT_DIR/metrics.txt"
  printf 'agent_up=%s\nreplacement_ready_s=%s\n' "${AGENT_UP:-}" "$REPLACEMENT_READY_S" >>"$OUT_DIR/timings.txt"
}
chaos_note "替代 Pod=$NEW_POD 重建耗时=${REPLACEMENT_READY_S}s agent_up=${AGENT_UP:-<取不到>} rescan_phase=${RESCAN_PHASE:-<取不到>}"
chaos_note "观测:fetch_bytes_total=${FETCH_BYTES:-<取不到>} quarantined_blocks=${QUARANTINED:-<取不到>}"
[ -n "$NEW_POD" ]
