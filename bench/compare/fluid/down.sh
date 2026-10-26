#!/usr/bin/env bash
# ============================================================================
# fluid 组 down:清理 fluid 命名空间下的 Dataset/Runtime/DataLoad 与工作负载。
#
# 清理顺序(Fluid 官方建议):先删 DataLoad,再删 Dataset(级联删 Runtime),
# 最后删训练 Pod、Redis、Secret、ConfigMap。
# 不动 fluid-system 控制面,不动 MinIO 数据。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="fluid"
NS="fluid"

group_header "$GROUP"
log_group "$GROUP" "开始清理 namespace/$NS"

# DataLoad(先删)
kubectl -n "$NS" delete dataload jfs-load --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "dataload/jfs-load 已删除"

# Dataset(级联删 JuiceFSRuntime 与 FUSE Pod)
kubectl -n "$NS" delete dataset jfs-demo --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "dataset/jfs-demo 已删除"
# --wait 只等 Dataset 本身,JuiceFSRuntime 是 Fluid 事后异步级联删的。不在这儿等齐,
# 下一次 up 就会把"还在删除中的 Runtime"当成现成资源跳过 → Dataset 无人认领,
# 永远 NotBound(见 docs/4 §7 第 22 条)。
if resource_exists juicefsruntime jfs-demo -n "$NS"; then
  wait_absent "$GROUP" juicefsruntime jfs-demo -n "$NS" --timeout 180 || \
    log_group "$GROUP" "警告:juicefsruntime/jfs-demo 未在 180s 内删净,下次 up 会自己再等一次" >&2
fi

# 训练 Pod(若有)
kubectl -n "$NS" delete pod train-fluid --ignore-not-found --wait=true 2>/dev/null || true

# Redis
kubectl -n "$NS" delete deployment redis --ignore-not-found 2>/dev/null || true
kubectl -n "$NS" delete service redis --ignore-not-found 2>/dev/null || true

# Secret / ConfigMap
kubectl -n "$NS" delete secret jfs-secret --ignore-not-found 2>/dev/null || true
kubectl -n "$NS" delete configmap trace --ignore-not-found 2>/dev/null || true

# 残留 PVC(Fluid Runtime 可能创建)
kubectl -n "$NS" delete pvc --all --ignore-not-found 2>/dev/null || true

# 清理 MinIO 中的 fluid-demo/ 前缀(JuiceFS 数据与元数据残留),
# 否则下次 format 会报 storage not empty。
# 同时清空 Redis 中的 JuiceFS 元数据,否则下次 format/mount 会冲突。
log_group "$GROUP" "清理 MinIO 与 Redis 残留(确保下次 format 干净)"
MINIO_POD=$(kubectl -n "$MINIO_NS" get pods -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$MINIO_POD" ]; then
  kubectl -n "$MINIO_NS" exec "$MINIO_POD" -- sh -c \
    "mc alias set myminio http://localhost:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY >/dev/null 2>&1; \
     mc rm --recursive --force myminio/$MINIO_BUCKET/fluid-demo/ 2>/dev/null; \
     mc rm --recursive --force myminio/$MINIO_BUCKET/bench-b/fluid-demo/ 2>/dev/null; \
     true" >/dev/null 2>&1 || true
fi
# 清空 Redis(若 redis pod 还在):JuiceFS 把 volume 元数据存在 Redis,
# 不清空会导致下次 format 报 "already formatted" 或 mount 报 "database is not formatted"。
REDIS_POD=$(kubectl -n "$NS" get pods -l app=redis -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$REDIS_POD" ]; then
  kubectl -n "$NS" exec "$REDIS_POD" -- redis-cli FLUSHALL >/dev/null 2>&1 || true
fi

log_group "$GROUP" "namespace/$NS 清理完成"
log_group "$GROUP" "down 完成:fluid-system 控制面与 MinIO 桶 $MINIO_BUCKET 数据保留"
