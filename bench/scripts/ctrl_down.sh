#!/usr/bin/env bash
# ============================================================================
# 拆除 TrainCache 控制面(bench-down-full 调用)。
#
# 删除 controller/deploy 渲染出的控制面资源(不含 Namespace),外加两个
# Secret。保留:
#   * namespace traincache 及其中的工作负载 ConfigMap(trace/ordersrc)
#   * kind 集群本体与 MinIO PVC
#   * ~/tcd 下的全部数据与产物
#
# 删 CRD 会连带回收其 CR 实例(datasets.traincache.io、warmuptasks.traincache.io)。
#
# 用法:
#   bash bench/scripts/ctrl_down.sh            执行删除
#   bash bench/scripts/ctrl_down.sh --dry-run  只打印将执行的 kubectl 命令
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NS="${NS:-traincache}"

DRY_RUN=0
case "${1:-}" in
  --dry-run) DRY_RUN=1 ;;
  -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
  "") ;;
  *) echo "未知参数: $1(可用 --dry-run)" >&2; exit 2 ;;
esac

FAILED=0
run() {
  if [ "$DRY_RUN" = 1 ]; then
    printf '  [dry-run]'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@" || FAILED=1
}

echo "== 拆除 TrainCache 控制面(ns=$NS,保留集群与 ~/tcd 数据) =="

# 工作负载
run kubectl -n "$NS" delete deployment traincache-controller-manager traincache-webhook \
  --ignore-not-found --wait=true --timeout=120s
run kubectl -n kube-system delete deployment traincache-scheduler \
  --ignore-not-found --wait=true --timeout=120s
run kubectl -n "$NS" delete daemonset traincache-agentd \
  --ignore-not-found --wait=true --timeout=120s

# webhook 入口与 scheduler 配置
run kubectl delete mutatingwebhookconfiguration traincache-pod-warmup --ignore-not-found
run kubectl -n "$NS" delete service traincache-webhook --ignore-not-found
run kubectl -n kube-system delete configmap traincache-scheduler-config --ignore-not-found

# RBAC
run kubectl -n "$NS" delete serviceaccount traincache-controller-manager traincache-webhook --ignore-not-found
run kubectl -n kube-system delete serviceaccount traincache-scheduler --ignore-not-found
run kubectl -n "$NS" delete role traincache-dataset-secrets --ignore-not-found
run kubectl -n kube-system delete role traincache-scheduler-leader-election --ignore-not-found
run kubectl -n "$NS" delete rolebinding traincache-controller-manager-secrets --ignore-not-found
run kubectl -n kube-system delete rolebinding traincache-scheduler-auth-reader traincache-scheduler-leader-election --ignore-not-found
run kubectl delete clusterrole traincache-controller-manager traincache-webhook --ignore-not-found
run kubectl delete clusterrolebinding traincache-controller-manager traincache-scheduler \
  traincache-scheduler-volume traincache-webhook --ignore-not-found

# CRD(连带回收 CR 实例)
run kubectl delete crd datasets.traincache.io warmuptasks.traincache.io \
  --ignore-not-found --wait=true --timeout=120s

# 控制面 Secret
run kubectl -n "$NS" delete secret traincache-webhook-tls traincache-objstore --ignore-not-found

# 让下次 ctrl-up 重新 apply(注解不存在时静默跳过)
if [ "$DRY_RUN" = 1 ]; then
  run kubectl annotate namespace "$NS" traincache.io/deploy-hash- --overwrite
else
  kubectl annotate namespace "$NS" traincache.io/deploy-hash- --overwrite >/dev/null 2>&1 || true
fi

if [ "$DRY_RUN" = 1 ]; then
  echo "== dry-run 结束:以上命令未执行 =="
  exit 0
fi
if [ "$FAILED" -ne 0 ]; then
  echo "== ctrl_down 有删除失败项,请检查输出 ==" >&2
  exit 1
fi
echo "== 控制面已拆除;namespace/$NS、kind 集群与 ~/tcd 数据保留 =="
