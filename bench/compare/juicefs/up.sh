#!/usr/bin/env bash
# ============================================================================
# juicefs 组 up:JuiceFS CE 单机部署 + 冷启动(M6 任务 6.2)。
#
# 部署形态(参考 survey/manifests/jfs-standalone.yaml,重写为幂等脚本):
#   * 命名空间:juicefs(cold 与 warm 共用)
#   * Redis:元数据引擎(单机 Deployment + Service)
#   * JuiceFS mount Pod:juicefs mount redis://... /mnt/jfs,
#     --cache-dir /cache --cache-size <容量>,/cache 挂自节点 hostPath /var/cache/jfs
#   * 训练 Pod:读 /mnt/jfs 下的 shard-*.tar(经 FUSE)
#
# 冷启动语义:mount Pod 的缓存是节点 hostPath /var/cache/jfs(D24),冷态由 cold_reset 清空后空缓存启动,
# 首次读取全回源 MinIO。缓存不随 Pod 销毁而消失,预热成果能活到 train-job 消费(D24)。
# 热启动:先跑 warm.sh 做 juicefs warmup,再跑训练。
#
# 幂等:Redis/mount/训练 Pod 已存在则跳过。
# 凭证:MinIO 凭证从环境变量取,Redis 无密码(集群内单租户)。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="juicefs"
NS="juicefs"
TRACE_FILE="${TRACE_FILE:-$BENCH_ROOT/experiments/traces/track-compare-d-s8-per-epoch.json}"
# 本档位消费的��片数(run_bench 按 S/M/L 档位下发;缺省取 M 档全量 32 片)。
# 同步与预热都按这个数收窄:桶里共有 32 片,不收窄的话 S 档(只消费 8 片 = 2GiB)
# 会把 8GiB 全同步进来,数据集口径与容量比都失真。
N_TARS="${N_TARS:-$(cfg TRACK_B_N_TARS)}"
N_TARS="${N_TARS:-32}"
SHARD_INCLUDES="$(shard_include_args "$N_TARS")"

# JuiceFS 缓存容量(与其他组对齐,默认 1GiB = 数据集 2GiB 的 50%)
JFS_CACHE_MB="${JFS_CACHE_MB:-1024}"
JFS_CACHE_SIZE="$JFS_CACHE_MB"  # juicefs --cache-size 单位是 MiB

group_header "$GROUP"
check_minio "$GROUP"

# ---- 1. 命名空间 ----
ensure_ns "$NS" "$GROUP"

# ---- 2. MinIO 凭证 Secret ----
if resource_exists secret minio-cred -n "$NS"; then
  log_group "$GROUP" "secret/minio-cred 已存在,跳过"
else
  kubectl -n "$NS" create secret generic minio-cred \
    --from-literal=access-key="$MINIO_ACCESS_KEY" \
    --from-literal=secret-key="$MINIO_SECRET_KEY" \
    --from-literal=bucket="$MINIO_BUCKET"
  log_group "$GROUP" "secret/minio-cred 已创建"
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

# ---- 4. Redis(元数据引擎)----
REDIS_YAML=$(cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: $NS
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: $REDIS_IMAGE
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 6379
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: "1"
              memory: 512Mi
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: $NS
spec:
  selector:
    app: redis
  ports:
    - port: 6379
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

# ---- 5. JuiceFS 文件系统 format(仅首次;已 format 则跳过)----
# format 命令需要能访问 MinIO 与 Redis;用一次性 Job 执行。
FORMAT_JOB_YAML=$(cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: jfs-format
  namespace: $NS
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: format
          image: $JUICEFS_IMAGE
          imagePullPolicy: IfNotPresent
          env:
            - name: MINIO_ACCESS_KEY
              valueFrom:
                secretKeyRef: { name: minio-cred, key: access-key }
            - name: MINIO_SECRET_KEY
              valueFrom:
                secretKeyRef: { name: minio-cred, key: secret-key }
            - name: MINIO_BUCKET
              valueFrom:
                secretKeyRef: { name: minio-cred, key: bucket }
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              # 已 format 则跳过(redis 里有 juicefs 元数据 key)
              METAURL="redis://redis.$NS.svc.cluster.local:6379/0"
              if juicefs status "\$METAURL" >/dev/null 2>&1; then
                echo "JuiceFS 已 format,跳过"
                exit 0
              fi
              juicefs format \
                --storage minio \
                --bucket "\${MINIO_ENDPOINT:-$MINIO_ENDPOINT}/\$MINIO_BUCKET" \
                --access-key "\$MINIO_ACCESS_KEY" \
                --secret-key "\$MINIO_SECRET_KEY" \
                "\$METAURL" traincache-demo
              echo "JuiceFS format 完成"
EOF
)
# format Job 用一次性方式:先删旧的(若失败残留),再创建
kubectl -n "$NS" delete job jfs-format --ignore-not-found 2>/dev/null || true
echo "$FORMAT_JOB_YAML" | kubectl apply -f -
log_group "$GROUP" "job/jfs-format 已创建(幂等:已 format 则立即退出)"
kubectl -n "$NS" wait --for=condition=complete job/jfs-format --timeout=120s
log_group "$GROUP" "JuiceFS format 完成"

# ---- 5.5. 数据导入:从 MinIO bench-b/shard-*.tar 同步到 JuiceFS(幂等,已有则跳过)----
# 用 juicefs sync 直接从 S3 源写入 JuiceFS 元数据/对象存储,无需挂载 FUSE。
IMPORT_JOB_YAML=$(cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: jfs-import
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
                secretKeyRef: { name: minio-cred, key: access-key }
            - name: MINIO_SECRET_KEY
              valueFrom:
                secretKeyRef: { name: minio-cred, key: secret-key }
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              METAURL="redis://redis.$NS.svc.cluster.local:6379/0"
              # 用 juicefs sync 从 MinIO 同步 shard 文件到 JuiceFS 根目录(幂等:已存在则跳过)
              # jfs:// 目标通过环境变量 JFS_META 指向 JuiceFS 元数据 URL
              # --include 按本档位分片数收窄(SHL)
              JFS_META="\$METAURL" juicefs sync \
                "s3://\${MINIO_ACCESS_KEY}:\${MINIO_SECRET_KEY}@$MINIO_BUCKET.minio.minio.svc.cluster.local:9000/" \
                "jfs://JFS_META/" \
                $SHARD_INCLUDES --exclude "*" \
                --no-https
              echo "JuiceFS 数据导入完成($N_TARS 片)"
EOF
)
kubectl -n "$NS" delete job jfs-import --ignore-not-found 2>/dev/null || true
echo "$IMPORT_JOB_YAML" | kubectl apply -f -
log_group "$GROUP" "job/jfs-import 已创建(从 MinIO 同步 shard 到 JuiceFS)"
kubectl -n "$NS" wait --for=condition=complete job/jfs-import --timeout=1800s
log_group "$GROUP" "JuiceFS 数据导入完成"

# ---- 6. JuiceFS mount Pod(hostPath 持久缓存,预热先于计时的前提)----
METAURL="redis://redis.$NS.svc.cluster.local:6379/0"
# 预热目标(默认 16 shard * 256 MiB ≈ 4 GiB,数据集全量)
JFS_WARMUP_TARGET="${JFS_WARMUP_TARGET:-/mnt/jfs/}"
JFS_WARMUP_TIMEOUT=900  # 硬编码 15 分钟,不可配置

MOUNT_POD_YAML=$(cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: jfs-mount
  namespace: $NS
  labels:
    app: jfs-mount
spec:
  restartPolicy: Always
  containers:
    - name: mount
      image: $JUICEFS_IMAGE
      imagePullPolicy: IfNotPresent
      securityContext:
        privileged: true
      command: ["sh", "-c"]
      args:
        - |
          set -e
          juicefs version
          juicefs mount $METAURL /mnt/jfs \
            --cache-dir /cache --cache-size $JFS_CACHE_SIZE -d
          echo "MOUNTED cache_size=${JFS_CACHE_SIZE}MiB"
          # 列出数据目录确认可读
          ls -la /mnt/jfs/ || true
          sleep 3600
      volumeMounts:
        - name: cache
          mountPath: /cache
  volumes:
    - name: cache
      # 跨 Pod 持久缓存:emptyDir 会随 Pod 销毁丢失,预热后必须存活到 train-job 消费
      hostPath:
        path: /var/cache/jfs
        type: DirectoryOrCreate
EOF
)
if resource_exists pod jfs-mount -n "$NS"; then
  log_group "$GROUP" "pod/jfs-mount 已存在,跳过"
else
  echo "$MOUNT_POD_YAML" | kubectl apply -f -
  log_group "$GROUP" "pod/jfs-mount 已创建(hostPath=/var/cache/jfs,cache-size=${JFS_CACHE_MB}MiB)"
fi

# 等待 mount Pod Ready
kubectl -n "$NS" wait --for=condition=ready pod/jfs-mount --timeout=120s
# Pod 没有 readinessProbe,Ready 只表示容器已启动;FUSE 挂载完成以 /proc/mounts 出现 /mnt/jfs 为准。
for i in $(seq 1 120); do
  if kubectl -n "$NS" exec jfs-mount -- sh -c \
       'while read -r _dev mnt _rest; do [ "$mnt" = "/mnt/jfs" ] && exit 0; done < /proc/mounts; exit 1' \
       >/dev/null 2>&1; then
    break
  fi
  if [ "$i" = "120" ]; then
    log_group "$GROUP" "等待 /mnt/jfs 挂载超时(120s)" >&2
    exit 1
  fi
  sleep 1
done
log_group "$GROUP" "pod/jfs-mount 就绪且 /mnt/jfs 已挂载"

# ---- 6.5. 预热(warm 组:juicefs warmup; 冷组:跳过)----
# warm 判定:RUN_BENCH=1 且 env JFS_WARMUP=1;或非 RUN_BENCH 手动调用时始终预热
if [ "${JFS_WARMUP:-0}" = "1" ]; then
  log_group "$GROUP" "执行 juicefs warmup $JFS_WARMUP_TARGET (超时 ${JFS_WARMUP_TIMEOUT}s)"
  T0=$(date +%s)
  # 带硬编码超时保护,超时则整轮作废(exit 1 让 run_bench.py 把该轮标记 up_failed)
  if ! kubectl -n "$NS" exec jfs-mount -- \
       timeout ${JFS_WARMUP_TIMEOUT} juicefs warmup "$JFS_WARMUP_TARGET"; then
    log_group "$GROUP" "预热失败或超时(>${JFS_WARMUP_TIMEOUT}s),整轮作废" >&2
    exit 1
  fi
  T1=$(date +%s)
  WARM_SECONDS=$(( T1 - T0 ))
  log_group "$GROUP" "juicefs warmup 完成,预热时长=${WARM_SECONDS}s"
  echo "[WARMUP_SECONDS] $WARM_SECONDS"  # run_bench.py 解析此行
  CACHE_USAGE=$(kubectl -n "$NS" exec jfs-mount -- du -sh /cache 2>/dev/null | awk '{print $1}' || echo "?")
  log_group "$GROUP" "缓存目录占用: $CACHE_USAGE"
else
  log_group "$GROUP" "JFS_WARMUP!=1,跳过预热(冷启动)"
fi

# ---- 7. tc 档位(挂到 MinIO Pod egress)----
log_group "$GROUP" "应用 tc 档位: rate=$TC_RATE delay=$TC_DELAY burst=$TC_BURST 抖动=$TC_JITTER(档位 $CAP_TIER)"
bash "$BENCH_ROOT/scripts/tc_apply.sh" "$TC_RATE" "$TC_DELAY" --tier "$CAP_TIER"

log_group "$GROUP" "up 完成:JuiceFS CE 单机已挂载(hostPath 持久),cache=${JFS_CACHE_MB}MiB"
echo "warmup: 已由 up.sh 自动完成(硬编码 15min 超时)"
echo "拆除:   make -C $COMPARE_ROOT compare-down GROUP=juicefs"
