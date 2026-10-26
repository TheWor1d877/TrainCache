#!/usr/bin/env bash
# ============================================================================
# fluid 组 up:Fluid Dataset + JuiceFSRuntime + DataLoad + 亲和调度(M6 任务 6.4)。
#
# 部署形态(参考 survey/manifests/{dataset,juicefsruntime,dataload}.yaml,
# 重写为幂等脚本):
#   * 命名空间:fluid(工作负载;Fluid 控制面在 fluid-system,需预先安装)
#   * Secret jfs-secret:metaurl(redis)+ access-key/secret-key
#   * Dataset jfs-demo:mountPoint juicefs:///,bucket 指向 MinIO
#   * JuiceFSRuntime jfs-demo:tieredstore HDD 缓存,落节点 hostPath /tmp/juicefs-cache,
#     配额对齐其他组(1GiB;数据 ~2.1GiB > 缓存,模拟真实场景)
#   * DataLoad jfs-load:预热整棵数据树(等价 JuiceFS warmup)
#   * 训练 Pod:nodeAffinity 绑定到缓存 worker 节点(FUSE 就近读)
#
# 前置:fluid-system 控制面已安装(helm install fluid);本脚本不安装 Fluid。
# 幂等:Dataset/Runtime/DataLoad/Secret 已存在则跳过。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="fluid"
NS="fluid"
TRACE_FILE="${TRACE_FILE:-$BENCH_ROOT/experiments/traces/track-compare-d-s8-per-epoch.json}"
# 本档位消费的分片数(run_bench 按 S/M/L 档位下发;缺省取 M 档全量 32 片)。
# 同步按这个数收窄:桶里共有 32 片,不收窄的话 S 档(只消费 8 片 = 2GiB)会把 8GiB
# 全同步进来,数据集口径与容量比都失真,DataLoad 预热也要多搬 4 倍字节。
N_TARS="${N_TARS:-$(cfg TRACK_B_N_TARS)}"
N_TARS="${N_TARS:-32}"
SHARD_INCLUDES="$(shard_include_args "$N_TARS")"

# Fluid 缓存配额(统一 1GiB,数据 ~2.1GiB > 缓存,模拟真实场景)
FLUID_CACHE_GI="${FLUID_CACHE_GI:-1}"
# 是否预热(1=DataLoad 预热 1GB,0=不预热冷启动)
FLUID_PREHEAT="${FLUID_PREHEAT:-1}"

group_header "$GROUP"
check_minio "$GROUP"

# 前置检查:fluid-system 控制面
if ! kubectl get namespace fluid-system >/dev/null 2>&1; then
  log_group "$GROUP" "错误:fluid-system 命名空间不存在,请先安装 Fluid(helm install fluid)" >&2
  exit 1
fi

# ---- 1. 命名空间 ----
ensure_ns "$NS" "$GROUP"

# ---- 2. jfs-secret(metaurl + MinIO 凭证)----
# metaurl 复用 juicefs 组的 Redis?Fluid 独立部署,故在 fluid ns 内也起一个 Redis,
# 或复用 juicefs ns 的 Redis。为隔离,这里在 fluid ns 内起 Redis。
REDIS_YAML=$(cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: $NS
spec:
  replicas: 1
  selector:
    matchLabels: { app: redis }
  template:
    metadata:
      labels: { app: redis }
    spec:
      containers:
        - name: redis
          image: $REDIS_IMAGE
          imagePullPolicy: IfNotPresent
          ports: [{ containerPort: 6379 }]
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits: { cpu: "1", memory: 512Mi }
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: $NS
spec:
  selector: { app: redis }
  ports: [{ port: 6379 }]
EOF
)
if resource_exists deployment redis -n "$NS"; then
  log_group "$GROUP" "deployment/redis 已存在,跳过"
else
  echo "$REDIS_YAML" | kubectl apply -f -
  log_group "$GROUP" "deployment/redis + service/redis 已创建"
fi
kubectl -n "$NS" rollout status deployment/redis --timeout=120s
log_group "$GROUP" "redis 就绪"

METAURL="redis://redis.$NS.svc.cluster.local:6379/0"
if resource_exists secret jfs-secret -n "$NS"; then
  log_group "$GROUP" "secret/jfs-secret 已存在,跳过"
else
  kubectl -n "$NS" create secret generic jfs-secret \
    --from-literal=metaurl="$METAURL" \
    --from-literal=access-key="$MINIO_ACCESS_KEY" \
    --from-literal=secret-key="$MINIO_SECRET_KEY"
  log_group "$GROUP" "secret/jfs-secret 已创建(metaurl=$METAURL)"
fi

# ---- 3. trace ConfigMap ----
if [ ! -f "$TRACE_FILE" ]; then
  log_group "$GROUP" "生成默认 trace.json → $TRACE_FILE"
  "$PY" "$COMPARE_ROOT/trace.py" --out "$TRACE_FILE"
fi
if resource_exists configmap trace -n "$NS"; then
  log_group "$GROUP" "configmap/trace 已存在,跳过"
else
  kubectl -n "$NS" create configmap trace --from-file=trace.json="$TRACE_FILE"
  log_group "$GROUP" "configmap/trace 已创建"
fi

# ---- 4. Dataset ----
DATASET_YAML=$(cat <<EOF
apiVersion: data.fluid.io/v1alpha1
kind: Dataset
metadata:
  name: jfs-demo
  namespace: $NS
spec:
  mounts:
    - name: fluid-demo
      mountPoint: juicefs:///
      options:
        bucket: $MINIO_ENDPOINT/$MINIO_BUCKET
        storage: minio
      encryptOptions:
        - name: metaurl
          valueFrom:
            secretKeyRef: { name: jfs-secret, key: metaurl }
        - name: access-key
          valueFrom:
            secretKeyRef: { name: jfs-secret, key: access-key }
        - name: secret-key
          valueFrom:
            secretKeyRef: { name: jfs-secret, key: secret-key }
EOF
)
if resource_exists dataset jfs-demo -n "$NS"; then
  log_group "$GROUP" "dataset/jfs-demo 已存在,跳过"
else
  echo "$DATASET_YAML" | kubectl apply -f -
  log_group "$GROUP" "dataset/jfs-demo 已创建"
fi

# ---- 5. JuiceFSRuntime(与 Dataset 同名自动绑定)----
# fuse 镜像:Fluid 会自动拼接 imageTag,故 image 不带 tag
RUNTIME_YAML=$(cat <<EOF
apiVersion: data.fluid.io/v1alpha1
kind: JuiceFSRuntime
metadata:
  name: jfs-demo
  namespace: $NS
spec:
  replicas: 1
  fuse:
    image: juicedata/mount
  tieredstore:
    levels:
      - mediumtype: HDD
        path: /tmp/juicefs-cache
        quota: ${FLUID_CACHE_GI}Gi
        low: "0.1"
EOF
)
if resource_exists juicefsruntime jfs-demo -n "$NS"; then
  log_group "$GROUP" "juicefsruntime/jfs-demo 已存在,跳过"
else
  echo "$RUNTIME_YAML" | kubectl apply -f -
  log_group "$GROUP" "juicefsruntime/jfs-demo 已创建(cache=${FLUID_CACHE_GI}Gi)"
fi

# ---- 6. 等待 Dataset Bound ----
log_group "$GROUP" "等待 dataset/jfs-demo 进入 Bound 状态"
for i in $(seq 1 60); do
  PHASE=$(kubectl get dataset jfs-demo -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [ "$PHASE" = "Bound" ]; then
    log_group "$GROUP" "dataset/jfs-demo 已 Bound"
    break
  fi
  if [ "$i" = "60" ]; then
    log_group "$GROUP" "等待 Dataset Bound 超时(当前 phase=$PHASE)" >&2
    exit 1
  fi
  sleep 2
done

# ---- 6.5. 数据导入:从 MinIO bench-b/shard-*.tar 同步到 Fluid JuiceFS(幂等)----
# Dataset Bound 后 JuiceFS 文件系统已格式化,但为空;需把 shard 数据导入。
FLUID_IMPORT_YAML=$(cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: jfs-fluid-import
  namespace: $NS
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: import
          image: $JUICEFS_IMAGE
          imagePullPolicy: IfNotPresent
          env:
            - name: MINIO_ACCESS_KEY
              valueFrom:
                secretKeyRef: { name: jfs-secret, key: access-key }
            - name: MINIO_SECRET_KEY
              valueFrom:
                secretKeyRef: { name: jfs-secret, key: secret-key }
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              METAURL="$METAURL"
              JFS_META="\$METAURL" juicefs sync \
                "s3://\${MINIO_ACCESS_KEY}:\${MINIO_SECRET_KEY}@$MINIO_BUCKET.minio.minio.svc.cluster.local:9000/" \
                "jfs://JFS_META/" \
                $SHARD_INCLUDES --exclude "*" \
                --no-https
              echo "Fluid JuiceFS 数据导入完成($N_TARS 片)"
EOF
)
kubectl -n "$NS" delete job jfs-fluid-import --ignore-not-found 2>/dev/null || true
echo "$FLUID_IMPORT_YAML" | kubectl apply -f -
log_group "$GROUP" "job/jfs-fluid-import 已创建(从 MinIO 同步 shard 到 Fluid JuiceFS)"
# Job 失败时 condition=complete 永远不成立,wait 会空等满 1800s;轮询 succeeded/failed,失败立即退出。
for i in $(seq 1 360); do
  S=$(kubectl -n "$NS" get job jfs-fluid-import -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
  F=$(kubectl -n "$NS" get job jfs-fluid-import -o jsonpath='{.status.failed}' 2>/dev/null || true)
  if [ "$S" = "1" ]; then
    break
  fi
  if [ -n "$F" ] && [ "$F" != "0" ]; then
    kubectl -n "$NS" logs job/jfs-fluid-import --tail=10 >&2 || true
    log_group "$GROUP" "job/jfs-fluid-import 失败(failed=$F)" >&2
    exit 1
  fi
  if [ "$i" = "360" ]; then
    log_group "$GROUP" "job/jfs-fluid-import 超时(1800s)" >&2
    exit 1
  fi
  sleep 5
done
log_group "$GROUP" "Fluid JuiceFS 数据导入完成"

# ---- 7. tc 档位(先挂限速,再预热)----
# DataLoad 与测量都从 MinIO egress 取数,顺序反了会把预热时长记成满速搬运的成本。
log_group "$GROUP" "应用 tc 档位: rate=$TC_RATE delay=$TC_DELAY burst=$TC_BURST 抖动=$TC_JITTER(档位 $CAP_TIER)"
bash "$BENCH_ROOT/scripts/tc_apply.sh" "$TC_RATE" "$TC_DELAY" --tier "$CAP_TIER"

# ---- 8. DataLoad(预热,仅 FLUID_PREHEAT=1 时执行)----
FLUID_WARMUP_TIMEOUT=900  # 硬编码 15 分钟,不可配置
if [ "$FLUID_PREHEAT" = "1" ]; then
DATALOAD_YAML=$(cat <<EOF
apiVersion: data.fluid.io/v1alpha1
kind: DataLoad
metadata:
  name: jfs-load
  namespace: $NS
spec:
  dataset:
    name: jfs-demo
    namespace: $NS
  target:
    - path: /
EOF
)
# DataLoad 每次需重新创建(同名不能重复 apply),先删旧的
kubectl -n "$NS" delete dataload jfs-load --ignore-not-found 2>/dev/null || true
echo "$DATALOAD_YAML" | kubectl apply -f -
log_group "$GROUP" "dataload/jfs-load 已创建(预热整棵数据树到 ${FLUID_CACHE_GI}GiB 缓存)"

# 等待 DataLoad 完成(硬编码 15 分钟超时)
log_group "$GROUP" "等待 dataload/jfs-load 完成(预热数据到缓存,超时 ${FLUID_WARMUP_TIMEOUT}s)"
T0=$(date +%s)
PHASE=""
while true; do
  PHASE=$(kubectl get dataload jfs-load -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [ "$PHASE" = "Complete" ]; then
    T1=$(date +%s)
    WARM_SECONDS=$(( T1 - T0 ))
    log_group "$GROUP" "dataload/jfs-load 完成,预热时长=${WARM_SECONDS}s"
    echo "[WARMUP_SECONDS] $WARM_SECONDS"  # run_bench.py 解析此行
    break
  fi
  if [ "$PHASE" = "Failed" ]; then
    log_group "$GROUP" "dataload/jfs-load 失败(phase=Failed),整轮作废" >&2
    exit 1
  fi
  T_NOW=$(date +%s)
  if [ $(( T_NOW - T0 )) -ge "$FLUID_WARMUP_TIMEOUT" ]; then
    log_group "$GROUP" "等待 DataLoad 超时(>${FLUID_WARMUP_TIMEOUT}s,当前 phase=$PHASE),整轮作废" >&2
    exit 1
  fi
  sleep 10
done
else
  log_group "$GROUP" "FLUID_PREHEAT=0,跳过 DataLoad 预热(冷启动)"
fi

log_group "$GROUP" "up 完成:Fluid Dataset+Runtime+DataLoad 已部署,cache=${FLUID_CACHE_GI}Gi"
echo "等待 DataLoad 完成: kubectl -n $NS get dataload jfs-load -w"
echo "查看缓存 worker: kubectl -n $NS get pods -l role=juicefs-worker"
echo "拆除: make -C $COMPARE_ROOT compare-down GROUP=fluid"
