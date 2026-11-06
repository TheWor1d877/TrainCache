#!/usr/bin/env bash
# node_loss.sh — 故障注入:停掉一个 kind 节点容器,让节点 NotReady。
#
# 注入动作:
#   docker stop <节点容器名>。kind 的节点容器名与 k8s 节点名相同。
#
# 判据(与 docs/4.3-bench-metrics-and-decisions.md §3.2 的 node_loss 行一致):
#   · DataSet 的 condition NoAccessibleNodes 变成 True
#   · WarmupTask 的 status.phase 落到 Degraded
#   · 容器启动回来后节点重新 Ready,上面两条回到 False 与非 Degraded
#
# 与其它故障的区别:本脚本直接操作宿主机上的 docker,不删集群对象。
# 停容器期间该节点上的 Pod 全部失联,脚本不并发跑其它族。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "$SCRIPT_DIR/../lib/common.sh"

NS="$CHAOS_NS_DEFAULT"
CONTAINER=""
NODE=""
TIMEOUT=180
RECOVER_TIMEOUT=300
NO_RECOVER=0
DRY_RUN=0
OUT_DIR=""
KUBECONFIG_ARG=""

usage() {
  cat <<'EOF'
用法: node_loss.sh [--node NODE] [--container NAME] [--ns NAMESPACE]
                     [--timeout SECONDS] [--recover-timeout SECONDS]
                     [--no-recover] [--out DIR] [--kubeconfig PATH]
                     [--dry-run] [--help]
  --node           要停的节点;不给则取训练 Pod 所在节点,否则取第一个 worker
  --container      kind 容器名;不给则用节点名(两者相同)
  --ns             DataSet 与 WarmupTask 所在 namespace(默认 traincache)
  --timeout        等两条状态线降级的上限秒数(默认 180)
  --recover-timeout 等节点与状态线恢复的上限秒数(默认 300)
  --no-recover     停掉节点后不自动启动(默认自动恢复)
  --out            证据目录(写 node.txt / metrics.txt / timings.txt / assertions.txt)
  --kubeconfig     本轮 kubeconfig 副本;不给则要求环境变量 KUBECONFIG
  --dry-run        只打印将执行的命令,不碰 docker 与集群
  --help           本用法
行为: docker stop 目标节点容器,等节点 NotReady、DataSet condition
      NoAccessibleNodes=True、WarmupTask phase=Degraded;然后 docker start,
      等节点 Ready 与两条状态线恢复。停容器期间该节点上的 Pod 全部失联。
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --node) [ "$#" -ge 2 ] || { echo "错误:--node 缺值" >&2; exit 2; }; NODE="$2"; shift 2 ;;
    --node=*) NODE="${1#--node=}"; shift ;;
    --container) [ "$#" -ge 2 ] || { echo "错误:--container 缺值" >&2; exit 2; }; CONTAINER="$2"; shift 2 ;;
    --container=*) CONTAINER="${1#--container=}"; shift ;;
    --ns) [ "$#" -ge 2 ] || { echo "错误:--ns 缺值" >&2; exit 2; }; NS="$2"; shift 2 ;;
    --ns=*) NS="${1#--ns=}"; shift ;;
    --timeout) [ "$#" -ge 2 ] || { echo "错误:--timeout 缺值" >&2; exit 2; }; TIMEOUT="$2"; shift 2 ;;
    --timeout=*) TIMEOUT="${1#--timeout=}"; shift ;;
    --recover-timeout) [ "$#" -ge 2 ] || { echo "错误:--recover-timeout 缺值" >&2; exit 2; }; RECOVER_TIMEOUT="$2"; shift 2 ;;
    --recover-timeout=*) RECOVER_TIMEOUT="${1#--recover-timeout=}"; shift ;;
    --no-recover) NO_RECOVER=1; shift ;;
    --out) [ "$#" -ge 2 ] || { echo "错误:--out 缺值" >&2; exit 2; }; OUT_DIR="$2"; shift 2 ;;
    --out=*) OUT_DIR="${1#--out=}"; shift ;;
    --kubeconfig) [ "$#" -ge 2 ] || { echo "错误:--kubeconfig 缺值" >&2; exit 2; }; KUBECONFIG_ARG="$2"; shift 2 ;;
    --kubeconfig=*) KUBECONFIG_ARG="${1#--kubeconfig=}"; shift ;;
    *) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
  esac
done

chaos_resolve_kubeconfig "$KUBECONFIG_ARG"

node_ready() {
  chaos_kubectl get node "$NODE" \
    -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' 2>/dev/null || true
}

dataset_conditions() {
  chaos_kubectl -n "$NS" get datasets \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="NoAccessibleNodes")].status}{"\n"}{end}' \
    2>/dev/null || true
}

warmup_phases() {
  chaos_kubectl -n "$NS" get warmuptasks \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}' \
    2>/dev/null || true
}

if [ "$DRY_RUN" = 1 ]; then
  chaos_note "[dry-run] 故障:node_loss(停 kind 节点容器,等节点与两条状态线降级)"
  chaos_show_cmd get nodes -o wide
  chaos_note "[dry-run]   ↑ 选目标节点:--node > 训练 Pod 所在节点 > 第一个 worker"
  chaos_show_cmd -n "$NS" get pod "$CHAOS_TRAIN_POD_DEFAULT" -o jsonpath='{.spec.nodeName}'
  chaos_show_raw "docker inspect -f '{{.State.Running}}' \$CONTAINER"
  chaos_show_raw "docker stop \$CONTAINER"
  chaos_show_cmd get node "\$NODE" -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}'
  chaos_show_cmd -n "$NS" get datasets -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="NoAccessibleNodes")].status}{"\n"}{end}'
  chaos_show_cmd -n "$NS" get warmuptasks -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}'
  chaos_show_raw "docker start \$CONTAINER"
  chaos_note "[dry-run]   ↑ 恢复后重读节点 Ready、DataSet condition 与 WarmupTask phase"
  exit 0
fi

chaos_require_kubeconfig
chaos_prepare_out_dir
ASSERT_FILE="$OUT_DIR/assertions.txt"
: >"$ASSERT_FILE"
pass() { printf 'PASS %s\n' "$*" | tee -a "$ASSERT_FILE"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$ASSERT_FILE" >&2; }

command -v docker >/dev/null 2>&1 || chaos_die "宿主机没有 docker 命令"

# ---- 选目标节点 ----
if [ -z "$NODE" ]; then
  NODE="$(chaos_kubectl -n "$NS" get pod "$CHAOS_TRAIN_POD_DEFAULT" \
    -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
fi
if [ -z "$NODE" ]; then
  NODE="$(chaos_kubectl get nodes \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.labels.node-role\.kubernetes\.io/control-plane}{"\n"}{end}' \
    2>/dev/null | awk '$2=="" {print $1; exit}')"
fi
[ -n "$NODE" ] || chaos_die "选不出 worker 节点;用 --node 指定"
[ -n "$CONTAINER" ] || CONTAINER="$NODE"

if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  chaos_die "docker 里没有容器 $CONTAINER(节点名与容器名应相同,可用 --container 指定)"
fi
STATE_BEFORE="$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)"
[ "$STATE_BEFORE" = "true" ] || chaos_die "容器 $CONTAINER 当前不在运行(State.Running=$STATE_BEFORE)"

BEFORE_DATASET="$(dataset_conditions)"
BEFORE_WARMUP="$(warmup_phases)"
[ -n "$BEFORE_DATASET" ] || chaos_die "namespace $NS 里没有 DataSet,判据取不到对象"
[ -n "$BEFORE_WARMUP" ] || chaos_die "namespace $NS 里没有 WarmupTask,判据取不到对象"

ctx="$(chaos_record_context)"
chaos_note "kubeconfig=$CHAOS_KUBECONFIG context=$ctx ns=$NS"
chaos_note "目标节点=$NODE 容器=$CONTAINER 停前 Ready=$(node_ready)"

T0="$(date +%s)"
docker stop "$CONTAINER" >/dev/null
chaos_note "已停容器 $CONTAINER,等节点 NotReady 与两条状态线降级…"

# ---- 等降级 ----
READY_AFTER=""
DATASET_HIT=""
WARMUP_HIT=""
while :; do
  ELAPSED=$(( $(date +%s) - T0 ))
  [ "$ELAPSED" -ge "$TIMEOUT" ] && break
  READY_AFTER="$(node_ready)"
  DATASET_HIT="$(printf '%s\n' "$(dataset_conditions)" | awk '$2=="True" {print $1; exit}')"
  WARMUP_HIT="$(printf '%s\n' "$(warmup_phases)" | awk '$2=="Degraded" {print $1; exit}')"
  if [ "$READY_AFTER" != "True" ] && [ -n "$DATASET_HIT" ] && [ -n "$WARMUP_HIT" ]; then
    break
  fi
  sleep 3
done
T1="$(date +%s)"
DOWN_S=$((T1 - T0))

if [ "$READY_AFTER" != "True" ]; then
  pass "节点 $NODE 已 NotReady(耗时 ${DOWN_S}s)"
else
  fail "${TIMEOUT}s 内节点 $NODE 仍 Ready"
fi
if [ -n "$DATASET_HIT" ]; then
  pass "DataSet $DATASET_HIT 的 NoAccessibleNodes=True"
else
  fail "${TIMEOUT}s 内没有 DataSet 的 NoAccessibleNodes=True"
fi
if [ -n "$WARMUP_HIT" ]; then
  pass "WarmupTask $WARMUP_HIT 的 phase=Degraded"
else
  fail "${TIMEOUT}s 内没有 WarmupTask 落到 Degraded"
fi

[ -n "$OUT_DIR" ] && {
  printf 'node=%s\ncontainer=%s\nstate_before=%s\nready_after=%s\ndown_s=%s\n' \
    "$NODE" "$CONTAINER" "$STATE_BEFORE" "${READY_AFTER:-}" "$DOWN_S" >"$OUT_DIR/node.txt"
  printf 'dataset_before:\n%s\nwarmup_before:\n%s\ndataset_after:\n%s\nwarmup_after:\n%s\n' \
    "$BEFORE_DATASET" "$BEFORE_WARMUP" "$(dataset_conditions)" "$(warmup_phases)" \
    >"$OUT_DIR/metrics.txt"
}

if [ "$NO_RECOVER" = 1 ]; then
  chaos_warn "--no-recover:容器 $CONTAINER 保持停止,节点 $NODE 仍未 Ready"
  exit 0
fi

# ---- 恢复 ----
R0="$(date +%s)"
docker start "$CONTAINER" >/dev/null
chaos_note "已启动容器 $CONTAINER,等节点 Ready 与状态线恢复…"
READY_RECOVERED=""
DATASET_CLEARED=0
WARMUP_CLEARED=0
while :; do
  ELAPSED=$(( $(date +%s) - R0 ))
  [ "$ELAPSED" -ge "$RECOVER_TIMEOUT" ] && break
  READY_RECOVERED="$(node_ready)"
  if [ "$READY_RECOVERED" = "True" ]; then
    if [ -z "$(printf '%s\n' "$(dataset_conditions)" | awk '$2=="True"')" ]; then
      DATASET_CLEARED=1
    fi
    if [ -z "$(printf '%s\n' "$(warmup_phases)" | awk '$2=="Degraded"')" ]; then
      WARMUP_CLEARED=1
    fi
  fi
  [ "$DATASET_CLEARED" = 1 ] && [ "$WARMUP_CLEARED" = 1 ] && break
  sleep 3
done
R1="$(date +%s)"
RECOVER_S=$((R1 - R0))

if [ "$READY_RECOVERED" = "True" ]; then
  pass "节点 $NODE 恢复 Ready(耗时 ${RECOVER_S}s)"
else
  fail "${RECOVER_TIMEOUT}s 内节点 $NODE 没恢复 Ready,容器 $CONTAINER 已启动但节点未就绪"
fi
if [ "$DATASET_CLEARED" = 1 ]; then
  pass "DataSet 的 NoAccessibleNodes 回到非 True"
else
  fail "DataSet 的 NoAccessibleNodes 仍为 True"
fi
if [ "$WARMUP_CLEARED" = 1 ]; then
  pass "WarmupTask 的 phase 离开 Degraded"
else
  fail "WarmupTask 的 phase 仍是 Degraded"
fi

[ -n "$OUT_DIR" ] && {
  printf 'recover_s=%s\nnode_ready=%s\ndataset_cleared=%s\nwarmup_cleared=%s\n' \
    "$RECOVER_S" "${READY_RECOVERED:-}" "$DATASET_CLEARED" "$WARMUP_CLEARED" \
    >"$OUT_DIR/timings.txt"
}
chaos_note "完成:停机 ${DOWN_S}s,恢复 ${RECOVER_S}s"
