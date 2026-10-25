#!/usr/bin/env bash
# ============================================================================
# juicefs 组 down:清理 juicefs 命名空间下的所有资源,不动 MinIO 数据。
#
# 清理范围:训练 Pod、jfs-mount Pod、Redis Deployment/Service、
#           format Job、ConfigMap、Secret、PVC(若有)。
# 保留:MinIO 桶数据(由 JuiceFS 写入的对象不删,避免影响其他组)。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="juicefs"
NS="juicefs"

group_header "$GROUP"
log_group "$GROUP" "开始清理 namespace/$NS"

# 训练 Pod(若有)
kubectl -n "$NS" delete pod train-juicefs --ignore-not-found --wait=true 2>/dev/null || true
# mount Pod(强制删,privileged Pod 可能卡)
kubectl -n "$NS" delete pod jfs-mount --ignore-not-found --wait=true --force 2>/dev/null || true
# format Job
kubectl -n "$NS" delete job jfs-format --ignore-not-found 2>/dev/null || true
# Redis 保留(元数据引擎,跨运行复用,避免 format 报 storage not empty)
# ConfigMap / Secret 保留(format 与 mount 需要)
# PVC(若有)
kubectl -n "$NS" delete pvc --all --ignore-not-found 2>/dev/null || true

log_group "$GROUP" "namespace/$NS 工作负载已清理(Redis/Secret/ConfigMap 保留)"
log_group "$GROUP" "down 完成:MinIO 桶数据保留,Redis 元数据保留"
