#!/usr/bin/env bash
# ============================================================================
# traincache 组 down:清理 traincache 命名空间下的工作负载,保留控制面与 MinIO 数据。
#
# 清理范围:训练 Job(单读端)、测量 Pod(多读端)、WarmupTask、DataSet、ConfigMap、
#           Secret(组内自建)。
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

# 多读端路径(网关族)建的是 Pod 不是 Job:train-job-0 到 train-job-3,标签 app=train-job。
# 单读端路径建的是上面那个 Job,两组互不影响,这里只按标签删 Pod。
kubectl -n "$NS" delete pods -l app=train-job --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "pod/app=train-job 已删除"

# WarmupTask
kubectl -n "$NS" delete warmuptask compare-task --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "warmuptask/compare-task 已删除"

# DataSet(用 datasets.traincache.io 避免与 Fluid CRD 冲突)
DATASET="${DATASET:-dataset-4gib}"
kubectl -n "$NS" delete datasets.traincache.io "$DATASET" --ignore-not-found --wait=true 2>/dev/null || true
log_group "$GROUP" "dataset/$DATASET 已删除"

# ConfigMap / 组内 Secret(保留 traincache-objstore 与 webhook-tls)
kubectl -n "$NS" delete configmap trace --ignore-not-found 2>/dev/null || true
# 测量脚本 ConfigMap:单读端叫 measure-script-<run_id>,多读端叫 measure-script-<rep>,
# 没有标签可按,只能按名字前缀逐个删;留着会随跑数一直堆。
for cm in $(kubectl -n "$NS" get configmap -o name 2>/dev/null | grep 'measure-script-' || true); do
  kubectl -n "$NS" delete "$cm" --ignore-not-found >/dev/null 2>&1 || true
done
log_group "$GROUP" "configmap/measure-script-* 已删除"

log_group "$GROUP" "namespace/$NS 工作负载已清理"

# 拆 MinIO egress 限速,与 tc_apply.sh 对称(幂等)。qdisc 挂在 MinIO Pod 的 netns 上,
# cold_reset 不重启 MinIO,down 不拆会把本格档位残留到组间读 MinIO 的路径(快照发布、
# 探针、下一组起组前的检查)。残留档位曾把 MinIO 限到近零导致快照读不动。
bash "$BENCH_ROOT/scripts/tc_teardown.sh" --target minio 2>/dev/null || true
log_group "$GROUP" "MinIO eth0 限速已拆(tc_teardown)"

log_group "$GROUP" "down 完成:控制面与 MinIO 桶 $MINIO_BUCKET 数据保留"
