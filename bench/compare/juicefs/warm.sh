#!/usr/bin/env bash
# ============================================================================
# juicefs 组 warm:预热 JuiceFS 缓存(等价 Fluid DataLoad,M6 任务 6.3)。
#
# 预热语义:
#   * 在 jfs-mount Pod 内执行 juicefs warmup /mnt/jfs/,把全部 shard-*.tar
#     拉入本地缓存(emptyDir)。
#   * 预热时长单独计时,不计入 makespan(D11 决策)。
#   * 预热完成后再启动训练 Pod(读 /mnt/jfs 全命中缓存)。
#
# 依赖:juicefs/up.sh 已运行,jfs-mount Pod 已就绪。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="juicefs"
NS="juicefs"

group_header "$GROUP"
log_group "$GROUP" "开始预热 JuiceFS 缓存(等价 DataLoad)"

# 检查 jfs-mount 就绪
if ! resource_exists pod jfs-mount -n "$NS"; then
  log_group "$GROUP" "错误:pod/jfs-mount 不存在,请先运行 juicefs/up.sh" >&2
  exit 1
fi

# 等待 mount Pod Running
kubectl -n "$NS" wait --for=condition=ready pod/jfs-mount --timeout=60s
log_group "$GROUP" "pod/jfs-mount 就绪"

# 执行 warmup 并计时
T0=$(date +%s.%N)
log_group "$GROUP" "T0=$(date +%T.%3N) 执行 juicefs warmup /mnt/jfs/"
kubectl -n "$NS" exec jfs-mount -- juicefs warmup /mnt/jfs/
T1=$(date +%s.%N)
ELAPSED=$(echo "$T1 - $T0" | bc)
log_group "$GROUP" "T1=$(date +%T.%3N) 预热耗时: ${ELAPSED} 秒"

# 验证缓存目录非空
CACHE_USAGE=$(kubectl -n "$NS" exec jfs-mount -- du -sh /cache 2>/dev/null | awk '{print $1}' || echo "未知")
log_group "$GROUP" "缓存目录占用: $CACHE_USAGE"

log_group "$GROUP" "warm 完成:JuiceFS 缓存已预热,预热时长 ${ELAPSED}s(不计入 makespan)"
echo "预热耗时已记录;可启动训练 Pod 读 /mnt/jfs(全命中缓存)"
