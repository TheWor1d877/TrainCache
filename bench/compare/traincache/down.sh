#!/usr/bin/env bash
# ============================================================================
# traincache 组 down:清理 traincache 命名空间下的工作负载,保留控制面与 MinIO 数据。
#
# 清理范围:训练 Job、WarmupTask、DataSet、ConfigMap、Secret(组内自建)。
# 保留:控制面(controller-manager/webhook/scheduler/agent DaemonSet)、
#       traincache-webhook-tls Secret、MinIO 桶数据。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="traincache"
NS="traincache"

group_header "$GROUP"
log_group "$GROUP" "开始清理 namespace/$NS 工作负载(保留控制面)"

# 训练 Job(先删,触发 WarmupTask 结束)
kubectl -n "$NS" delete job compare-job --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "job/compare-job 已删除"

# WarmupTask
kubectl -n "$NS" delete warmuptask compare-task --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "warmuptask/compare-task 已删除"

# DataSet(用 datasets.traincache.io 避免与 Fluid CRD 冲突)
DATASET="${DATASET:-compare-data-8m}"
kubectl -n "$NS" delete datasets.traincache.io "$DATASET" --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "dataset/$DATASET 已删除"

# ConfigMap / 组内 Secret(保留 traincache-objstore 与 webhook-tls)
kubectl -n "$NS" delete configmap trace --ignore-not-found 2>/dev/null || true

log_group "$GROUP" "namespace/$NS 工作负载已清理"
log_group "$GROUP" "down 完成:控制面与 MinIO 桶 $MINIO_BUCKET 数据保留"
