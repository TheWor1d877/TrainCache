#!/usr/bin/env bash
# ============================================================================
# direct 组 up:训练 Pod 直读 MinIO,无缓存层(M6 任务 6.1)。
#
# 特征:
#   * 命名空间:direct
#   * 数据通路:训练容器经 HTTP 直接 GET MinIO bucket 内的 shard-*.tar
#   * 无缓存层:不部署 JuiceFS/Fluid/TrainCache,纯源站带宽基线
#   * tc 档位:挂到 MinIO Pod eth0 egress(与其他组共用同一档位)
#   * 轨迹:从 ConfigMap 读 trace.json,五组消费顺序一致
#
# 幂等:Pod/ConfigMap 已存在则跳过;tc 应用脚本本身幂等。
# 凭证:不硬编码,从环境变量 MINIO_ACCESS_KEY/MINIO_SECRET_KEY 或 config.py 取。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="direct"
NS="direct"
TRACE_FILE="${TRACE_FILE:-$BENCH_ROOT/experiments/traces/track-compare-d-s8-per-epoch.json}"

group_header "$GROUP"
check_minio "$GROUP"

# ---- 1. 命名空间 ----
ensure_ns "$NS" "$GROUP"

# ---- 2. MinIO 凭证 Secret(从环境变量取,不硬编码)----
if resource_exists secret minio-cred -n "$NS"; then
  log_group "$GROUP" "secret/minio-cred 已存在,跳过"
else
  kubectl -n "$NS" create secret generic minio-cred \
    --from-literal=access-key="$MINIO_ACCESS_KEY" \
    --from-literal=secret-key="$MINIO_SECRET_KEY"
  log_group "$GROUP" "secret/minio-cred 已创建(来源:环境变量)"
fi

# ---- 3. trace ConfigMap(七组态共用一份)----
if [ ! -f "$TRACE_FILE" ]; then
  log_group "$GROUP" "生成默认 trace.json → $TRACE_FILE"
  "$PY" "$COMPARE_ROOT/trace.py" --out "$TRACE_FILE"
fi
if resource_exists configmap trace -n "$NS"; then
  log_group "$GROUP" "configmap/trace 已存在,跳过"
else
  kubectl -n "$NS" create configmap trace --from-file=trace.json="$TRACE_FILE"
  log_group "$GROUP" "configmap/trace 已创建($TRACE_FILE)"
fi

# ---- 4. tc 档位(挂到 MinIO Pod egress,与其他组共用)----
log_group "$GROUP" "应用 tc 档位: rate=$TC_RATE delay=$TC_DELAY burst=$TC_BURST 抖动=$TC_JITTER(档位 $CAP_TIER)"
bash "$BENCH_ROOT/scripts/tc_apply.sh" "$TC_RATE" "$TC_DELAY" --tier "$CAP_TIER"

# ---- 5. 训练 Pod:直读 MinIO ----
# 训练容器用 python:3.12-slim,内嵌脚本:
#   * 读 /trace/trace.json 拿分片顺序
#   * 经 MinIO endpoint + bucket 直读 shard-*.tar
#   * 每步:下载一个 shard → 解码样本 → matmul compute → 打点
#   * 环境变量注入 TRAINCACHE_JOB_CREATED_AT(runner 负责)
TRAIN_POD_YAML=$(cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: train-direct
  namespace: $NS
  labels:
    app: train-direct
    traincache.io/group: direct
spec:
  restartPolicy: Never
  containers:
    - name: trainer
      image: $TRAIN_IMAGE
      imagePullPolicy: IfNotPresent
      env:
        - name: MINIO_ENDPOINT
          value: "$MINIO_ENDPOINT"
        - name: MINIO_BUCKET
          value: "$MINIO_BUCKET"
        - name: MINIO_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: minio-cred
              key: access-key
        - name: MINIO_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: minio-cred
              key: secret-key
        - name: TRAINCACHE_GROUP
          value: direct
        - name: TRAINCACHE_JOB_CREATED_AT
          value: "\${TRAINCACHE_JOB_CREATED_AT:-}"
      command: ["/bin/sh", "-c"]
      args:
        - |
          set -e
          echo "=== direct 组训练:直读 MinIO(冒烟,仅验证下载流程) ==="
          echo "endpoint=\$MINIO_ENDPOINT bucket=\$MINIO_BUCKET"
          python3 - <<'PY'
          import json, os, time, urllib.request
          from pathlib import Path
          trace = json.loads(Path("/trace/trace.json").read_text())
          endpoint = os.environ["MINIO_ENDPOINT"].rstrip("/")
          bucket = os.environ["MINIO_BUCKET"]
          for ep in trace["per_epoch"]:
              perm = ep["shard_perm"]
              for s in perm:
                  url = f"{endpoint}/{bucket}/shard-{s:05d}.tar"
                  t0 = time.perf_counter()
                  with urllib.request.urlopen(url, timeout=120) as r:
                      data = r.read()
                  t1 = time.perf_counter()
                  print(f"epoch={ep['epoch']} shard={s} bytes={len(data)} "
                        f"fetch_s={t1-t0:.3f}", flush=True)
          print("=== direct 组训练完成 ===")
          PY
      volumeMounts:
        - name: trace
          mountPath: /trace
          readOnly: true
      resources:
        requests:
          cpu: 500m
          memory: 256Mi
        limits:
          cpu: "2"
          memory: 1Gi
  volumes:
    - name: trace
      configMap:
        name: trace
EOF
)

if [ "${RUN_BENCH:-0}" = "1" ]; then
  log_group "$GROUP" "RUN_BENCH=1:跳过 train-direct Pod(由 run_bench.py 的 measure Pod 负责测量)"
elif resource_exists pod train-direct -n "$NS"; then
  log_group "$GROUP" "pod/train-direct 已存在,跳过创建"
else
  echo "$TRAIN_POD_YAML" | kubectl apply -f -
  log_group "$GROUP" "pod/train-direct 已创建(直读 MinIO,无缓存层)"
fi

log_group "$GROUP" "up 完成:Pod 直读 MinIO,tc 档位=$TC_RATE/$TC_DELAY"
echo "查看日志: kubectl -n $NS logs -f pod/train-direct"
echo "拆除:     make -C $COMPARE_ROOT compare-down GROUP=direct"
