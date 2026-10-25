#!/usr/bin/env bash
# ============================================================================
# direct 组 down:清理 direct 命名空间下的训练资源,不动 MinIO 数据。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="direct"
NS="direct"

group_header "$GROUP"
log_group "$GROUP" "开始清理 namespace/$NS"

# 删 Pod(等待终止)
kubectl -n "$NS" delete pod train-direct --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "pod/train-direct 已删除"

# 删 ConfigMap / Secret(组内自建)
kubectl -n "$NS" delete configmap trace --ignore-not-found 2>/dev/null || true
kubectl -n "$NS" delete secret minio-cred --ignore-not-found 2>/dev/null || true
log_group "$GROUP" "configmap/trace 与 secret/minio-cred 已删除"

# 保留命名空间(便于复用);不删 MinIO 数据
log_group "$GROUP" "down 完成:MinIO 桶 $MINIO_BUCKET 数据保留"
