#!/usr/bin/env bash
set -euo pipefail

DATALOAD_NAME="jfs-load"
NAMESPACE="default"
DATALOAD_FILE="dataload.yaml"

echo "=== 1. 清理旧的 DataLoad ==="
kubectl delete dataload "${DATALOAD_NAME}" -n "${NAMESPACE}" --ignore-not-found

echo "=== 2. 记录 T0 并触发 ==="
T0=$(date +%s.%N)
echo "T0 = $(date +%T.%3N)"
kubectl apply -f "${DATALOAD_FILE}"

echo "=== 3. 等待 PHASE=Complete ==="
while true; do
  PHASE=$(kubectl get dataload "${DATALOAD_NAME}" -n "${NAMESPACE}" \
    -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  echo "[$(date +%T)] PHASE=${PHASE}"
  if [ "${PHASE}" = "Complete" ]; then
    break
  fi
  sleep 0.5
done

echo "=== 4. 记录 T1 并计算耗时 ==="
T1=$(date +%s.%N)
echo "T1 = $(date +%T.%3N)"
ELAPSED=$(echo "${T1} - ${T0}" | bc)
echo "总耗时: ${ELAPSED} 秒"

echo "=== 5. 抓取 loader Pod 日志 ==="
LOADER_POD=$(kubectl get pods -n "${NAMESPACE}" --no-headers 2>/dev/null \
  | grep loader | awk '{print $1}' | head -1 || true)

if [ -n "${LOADER_POD}" ]; then
  echo "loader Pod: ${LOADER_POD}"
  kubectl logs "${LOADER_POD}" -n "${NAMESPACE}" > "loader-${DATALOAD_NAME}.log" 2>&1 || true
  echo "日志已保存到: loader-${DATALOAD_NAME}.log"
else
  echo "未找到 loader Pod（可能已完成并退出，或日志随 Pod 删除）"
fi

echo "=== 6. 最终状态 ==="
kubectl get dataload "${DATALOAD_NAME}" -n "${NAMESPACE}"
