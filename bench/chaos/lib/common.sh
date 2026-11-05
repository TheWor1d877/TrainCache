#!/usr/bin/env bash
# bench/chaos/lib/common.sh — 韧性(混沌)套件共用件。
#
# 被 bench/chaos/run.sh 与 bench/chaos/inject/*.sh 引用;被引用时不产生副作用。
#
# 两条硬约束(本文件存在的理由):
#   1. 不切全局 kubectl context:装置不执行 `kubectl config use-context`,
#      只用调用方显式给出的 kubeconfig(KUBECONFIG 环境变量或 --kubeconfig
#      指向的每轮副本)。同机并行会话共用 ~/.kube/config,切 context 会把控制面
#      打到别人的集群上(bench/scripts/ctrl_up.sh:61-85 同样只断言不切换)。
#   2. 证据落新目录:每轮写 $CHAOS_DATA_ROOT/<run-id>/,只增不改;共享数据层
#      (~/tcd)不删任何东西。
set -euo pipefail

CHAOS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAOS_ROOT="$(cd "$CHAOS_LIB_DIR/.." && pwd)"
CHAOS_REPO="$(cd "$CHAOS_ROOT/../.." && pwd)"

# ---- 默认对象名(全部来自仓库清单,不是猜的)----
# namespace:controller/deploy/namespace.yaml、bench/scripts/ctrl_up.sh:34
CHAOS_NS_DEFAULT="traincache"
# agentd DaemonSet:controller/deploy/agent/daemonset.yaml:16
CHAOS_AGENTD_DS_DEFAULT="traincache-agentd"
# agentd 容器名:controller/deploy/agent/daemonset.yaml:34
CHAOS_AGENTD_CONTAINER_DEFAULT="agentd"
# agentd Pod 标签:controller/deploy/agent/daemonset.yaml:20
CHAOS_AGENTD_LABEL_DEFAULT="app.kubernetes.io/name=traincache-agentd"
# 指标端口:controller/deploy/agent/daemonset.yaml:105、agent/svc/config.go:25
CHAOS_METRICS_PORT_DEFAULT="9411"
# 训练 Pod 与 sidecar:agent/deploy/train-pod.yaml:19,58
CHAOS_TRAIN_POD_DEFAULT="traincache-train"
CHAOS_ORDERSRC_CONTAINER_DEFAULT="ordersrc"
# MinIO:bench/k8s/minio.yaml(namespace minio / Deployment minio / label app=minio)
CHAOS_MINIO_NS_DEFAULT="minio"
CHAOS_MINIO_DEPLOY_DEFAULT="minio"
CHAOS_MINIO_LABEL_DEFAULT="app=minio"
# agent 本地缓存根:agent/svc/config.go:29、controller/deploy/agent/daemonset.yaml:146
CHAOS_CACHE_ROOT_DEFAULT="/var/cache/traincache"

CHAOS_KUBECONFIG=""
CHAOS_KC_MODE="none"     # flag | env | none

chaos_die() { printf '错误: %s\n' "$*" >&2; exit 1; }
chaos_warn() { printf '警告: %s\n' "$*" >&2; }
chaos_note() { printf '%s\n' "$*"; }

# ---- kubeconfig 解析:优先 --kubeconfig,其次环境变量 KUBECONFIG ----
chaos_resolve_kubeconfig() {
  local flag="${1:-}"
  if [ -n "$flag" ]; then
    [ -r "$flag" ] || chaos_die "--kubeconfig 指向的文件不可读: $flag"
    CHAOS_KUBECONFIG="$flag"
    CHAOS_KC_MODE="flag"
  elif [ -n "${KUBECONFIG:-}" ]; then
    CHAOS_KUBECONFIG="$KUBECONFIG"
    CHAOS_KC_MODE="env"
  else
    CHAOS_KUBECONFIG=""
    CHAOS_KC_MODE="none"
  fi
}

# 打印 kubectl 前缀(dry-run 用,让打印出来的命令可以照抄执行)
chaos_kubectl_prefix() {
  case "$CHAOS_KC_MODE" in
    flag) printf 'kubectl --kubeconfig %s' "$CHAOS_KUBECONFIG" ;;
    env)  printf 'KUBECONFIG=%s kubectl' "$CHAOS_KUBECONFIG" ;;
    *)    printf 'kubectl' ;;
  esac
}

# 真正执行 kubectl:没有任何 kubeconfig 就拒绝(装置不替调用方选集群)
chaos_kubectl() {
  case "$CHAOS_KC_MODE" in
    flag) kubectl --kubeconfig "$CHAOS_KUBECONFIG" "$@" ;;
    env)  kubectl "$@" ;;
    *)    chaos_die "未提供 kubeconfig:请显式设置环境变量 KUBECONFIG,或用 --kubeconfig 指向本轮副本(装置不切全局 kubectl context)" ;;
  esac
}

# 真跑前的硬门槛:必须显式给出 kubeconfig,否则拒绝(不替调用方选集群)
chaos_require_kubeconfig() {
  [ "$CHAOS_KC_MODE" != "none" ] || chaos_die "未提供 kubeconfig:请显式设置环境变量 KUBECONFIG,或用 --kubeconfig 指向本轮副本(装置不切全局 kubectl context)"
}

# 证据目录:不给 --out 时在共享数据层下新建一个带 run-id 的目录(只增不删)
chaos_prepare_out_dir() {
  if [ -z "$OUT_DIR" ]; then
    OUT_DIR="$(chaos_data_root)/$(chaos_run_id)-$(basename "${0%.sh}")"
    chaos_note "未指定 --out,证据目录取 $OUT_DIR"
  fi
  mkdir -p "$OUT_DIR" || chaos_die "证据目录建不出来: $OUT_DIR"
}

# 只读地记录当前 context(证据用),绝不写 context
chaos_record_context() {
  local out
  out="$(chaos_kubectl config current-context 2>/dev/null || true)"
  printf '%s\n' "${out:-<none>}"
}

# dry-run 打印:一条命令一行
chaos_show_cmd() {
  local pre a
  pre="$(chaos_kubectl_prefix)"
  printf '[dry-run]   %s' "$pre"
  for a in "$@"; do printf ' %s' "$(printf '%q' "$a")"; done
  printf '\n'
}

chaos_show_raw() { printf '[dry-run]   %s\n' "$*"; }

# ---- 集群对象查询 ----
# agentd Pod 列表:输出 "name node ready" 每行一行
chaos_agentd_pod_rows() {
  chaos_kubectl -n "${1:-$CHAOS_NS_DEFAULT}" get pods \
    -l "$CHAOS_AGENTD_LABEL_DEFAULT" \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    2>/dev/null || true
}

# 抓 agentd /metrics 的某个 family 值(无标签族按空格分隔,带标签族取首条样本)
chaos_metric() {
  local ns="$1" pod="$2" family="$3" v
  v="$(chaos_kubectl -n "$ns" exec "$pod" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
        wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics" 2>/dev/null \
      | awk -v f="$family" '$0 ~ "^"f"(\\{| )" {print $2; exit}' 2>/dev/null || true)"
  printf '%s' "${v}"
}

# agentd /metrics 全量文本(证据快照:metrics_before.txt / metrics_after.txt)
chaos_metrics_text() {
  local ns="$1" pod="$2"
  [ -n "$pod" ] || return 0
  chaos_kubectl -n "$ns" exec "$pod" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics" 2>/dev/null || true
}

# 某个 family 的样本值求和(跨 label 求和;counter 差分用)
chaos_metric_sum() {
  local ns="$1" pod="$2" family="$3" v
  v="$(chaos_metrics_text "$ns" "$pod" \
      | awk -v f="$family" '$0 ~ "^"f"(\\{| )" {s+=$2} END{printf "%s", (s==""?"":s)}' 2>/dev/null || true)"
  printf '%s' "${v}"
}

# 某个 family 里标签 k=v 的样本值求和(如 traincache_fetch_requests_total{reason="repair"})
chaos_metric_label_sum() {
  local ns="$1" pod="$2" family="$3" k="$4" v="$5" out
  out="$(chaos_metrics_text "$ns" "$pod" \
      | awk -v f="$family" -v k="$k" -v v="$v" \
          'index($0, f"{")==1 && index($0, k "=\"" v "\"")>0 {s+=$2} END{printf "%s", (s==""?"":s)}' \
      2>/dev/null || true)"
  printf '%s' "${out}"
}

# ---- 证据目录 ----
# run-id:时间戳 + 四位随机后缀(同秒并发跑也不会撞目录)
chaos_run_id() {
  local suffix
  suffix="$(od -An -tx1 -N2 /dev/urandom 2>/dev/null | tr -d ' \n' || true)"
  [ -n "$suffix" ] || suffix="$$"
  printf '%s-%s\n' "$(date -u +%Y%m%dT%H%M%SZ)" "${suffix:0:4}"
}

chaos_data_root() { printf '%s' "${CHAOS_DATA_ROOT:-$HOME/tcd/chaos}"; }

# 写结果键值文件(供 run.sh 读取)
chaos_write_kv() {
  local file="$1"; shift
  [ -n "$file" ] || return 0
  local kv
  for kv in "$@"; do printf '%s\n' "$kv" >>"$file"; done
}
