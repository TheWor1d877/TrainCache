#!/usr/bin/env bash
# ============================================================================
# traincache 组 up:复用 TrainCache 装置(DataSet + WarmupTask + 训练 Job,M6 任务 6.5)。
#
# 复用现有装置:
#   * 控制面(controller-manager/webhook/scheduler/agent DaemonSet)由
#     scripts/ctrl_up.sh 部署,bench-up 与 traincache/up.sh 共用同一实现;
#     重复执行不滚动重启。
#   * 工作负载(DataSet/WarmupTask/Job)创建在 traincache 命名空间,
#     复用 controller/e2e/manifests 的 CR 结构,数据指向共享 MinIO bucket。
#
# 数据对齐:
#   * DataSet.spec.shards 与 trace.json 的 n_tars 一致(默认 8 = S 档 2GiB;
#     正式跑分由 run_bench.py 按容量档位 S/M/L 下发 N_TARS,此处只是独立调用时的兜底)
#   * chunkSize 实验口径 8MiB(D20/6.13:1GiB 缓存切 128 块,预取窗口占 1/8;
#     生产默认 64MiB 不动;可用 CHUNK_BYTES 环境变量覆盖)
#   * bucket 取 config.py MINIO_BUCKET(bench-b),与其他组共享
#
# 幂等:控制面已就绪则跳过;DataSet/WarmupTask/Job 已存在则先删后建
#       (WarmupTask/Job 不可原地更新,需重建)。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../_common.sh"

GROUP="traincache"
NS="traincache"
TRACE_FILE="${TRACE_FILE:-$BENCH_ROOT/experiments/traces/track-compare-d-s8-per-epoch.json}"
# DataSet/快照名(冒烟或并行实验可指向独立前缀,如 compare-data-smoke;
# 默认 compare-data-8m——8MiB 块快照,D20/6.13/Q12;旧 64MiB 快照 compare-data
# 只留作历史产物复现,正式实验不再引用。快照发布本身是后续动作)
DATASET="${DATASET:-compare-data-8m}"

# TrainCache 控制面镜像(允许环境变量覆盖)
TAG="${TAG:-dev}"
IMG_MANAGER="${IMG_MANAGER:-ghcr.io/thewor1d877/traincache/controller-manager:$TAG}"
IMG_WEBHOOK="${IMG_WEBHOOK:-ghcr.io/thewor1d877/traincache/webhook:$TAG}"
IMG_SCHEDULER="${IMG_SCHEDULER:-ghcr.io/thewor1d877/traincache/kube-scheduler:$TAG}"
IMG_AGENT="${IMG_AGENT:-ghcr.io/thewor1d877/traincache/traincache-agentd:$TAG}"

# DataSet 参数
# 实验块大小 8MiB(D20/6.13;生产默认 64MiB 不动;环境变量 CHUNK_BYTES 可覆盖)
CHUNK_BYTES="${CHUNK_BYTES:-8388608}"
N_TARS="${N_TARS:-$(cfg TRACK_B_N_TARS)}"   # 分片取自 track_b(256MiB/份),不是 D 轨 128MiB 份
N_TARS="${N_TARS:-32}"                        # M 档全量(8GiB);run_bench 按档位覆盖
# WarmupTask 预热块数:启动窗口口径=预热 1GiB 字节(=1GiB 缓存容量整档;
# 对 M 档 8GiB 数据即"预热至多覆盖 1/8"的 9.0/D19 口径,与旧 64MiB×16 块同字节量)。
# 块大小 8MiB 后 k = 1GiB / 8MiB = 128;块大小改档时按 字节量/CHUNK_BYTES 重算。
# 0 = bench 冷启动哨兵:跳过 WarmupTask 创建与 gate,不把 0 写进 CRD 的 podDetails[].k。
# 可用 TRAINCACHE_WARMUP_K 覆盖。
WARMUP_K="${TRAINCACHE_WARMUP_K:-128}"

# 本地 tar 数据源(trainpublish 读本地文件切块上传)
TRACK_B_DIR="${TRACK_B_DIR:-$(cfg TRACK_B_DIR)}"
TRACK_B_DIR="${TRACK_B_DIR:-$BENCH_ROOT/data/track_b/tars}"

# trainpublish 二进制(缺失时从源码构建)
TRAINPUBLISH_BIN="${TRAINPUBLISH_BIN:-$BENCH_ROOT/scripts/trainpublish}"
TRAINCACHE_REPO="${TRAINCACHE_REPO:-$(cd "$COMPARE_ROOT/../.." && pwd)}"

group_header "$GROUP"
check_minio "$GROUP"

# ---- 0. 发布数据快照(五档共用一份;B 轨全部分片进 manifest,档位只收窄轨迹)----
publish_snapshot() {
  local minio_pod
  minio_pod="$(kubectl -n "$MINIO_NS" get pod -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -z "$minio_pod" ]; then
    log_group "$GROUP" "错误: MinIO Pod 未就绪,无法发布数据" >&2
    return 1
  fi
  kubectl -n "$MINIO_NS" exec "$minio_pod" -- mc alias set local \
    http://localhost:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null 2>&1 || true

  # 快照覆盖的 tar 数：manifest 的 shards[] 是 idx 文件（trainpublish 会把
  # 多个 tar 打进同一个 idx），必须数 idx 里的 distinct path，不能数 manifest 条目。
  snapshot_shard_count() {
    local cur man keys key idx_tmp n
    cur="$(kubectl -n "$MINIO_NS" exec "$minio_pod" -- \
      mc cat "local/$MINIO_BUCKET/dataset/$DATASET/CURRENT" 2>/dev/null | awk '{print $1}' || true)"
    [ -n "$cur" ] || { echo 0; return 0; }
    man="$(kubectl -n "$MINIO_NS" exec "$minio_pod" -- \
      mc cat "local/$MINIO_BUCKET/dataset/$DATASET/snapshots/$cur/manifest.json" 2>/dev/null || true)"
    keys="$(printf '%s' "$man" | "$PY" -c 'import json,sys
try:
    print("\n".join(s["key"] for s in json.load(sys.stdin).get("shards", [])))
except Exception:
    pass' 2>/dev/null || true)"
    [ -n "$keys" ] || { echo 0; return 0; }
    idx_tmp="$(mktemp)"
    while IFS= read -r key; do
      [ -n "$key" ] || continue
      kubectl -n "$MINIO_NS" exec "$minio_pod" -- \
        mc cat "local/$MINIO_BUCKET/$key" >>"$idx_tmp" 2>/dev/null || true
      printf '\n' >>"$idx_tmp"
    done <<< "$keys"
    n="$("$PY" -c 'import json,sys
data=open(sys.argv[1], encoding="utf-8", errors="replace").read()
dec=json.JSONDecoder(); i=0; paths=set()
while i < len(data):
    while i < len(data) and data[i].isspace():
        i += 1
    if i >= len(data):
        break
    try:
        obj, i = dec.raw_decode(data, i)
    except ValueError:
        break
    for e in obj:
        if isinstance(e, dict) and e.get("path"):
            paths.add(e["path"])
print(len(paths))' "$idx_tmp" 2>/dev/null || echo 0)"
    rm -f "$idx_tmp"
    echo "${n:-0}"
  }

  # 全部分片进一份快照:S/M/L 的轨迹只决定消费前几个,不另发布小快照
  local tars=() f
  for f in "$TRACK_B_DIR"/shard-*.tar; do
    [ -f "$f" ] && tars+=("$f")
  done
  if [ "${#tars[@]}" -eq 0 ]; then
    log_group "$GROUP" "错误: $TRACK_B_DIR 下没有 shard-*.tar" >&2
    return 1
  fi
  local want="${#tars[@]}" have
  have="$(snapshot_shard_count | tr -d '[:space:]')"
  have="${have:-0}"
  if [ "$have" -ge "$want" ]; then
    log_group "$GROUP" "数据快照已覆盖 $have 个分片(need $want),跳过发布"
    return 0
  fi
  if [ "$have" -gt 0 ]; then
    log_group "$GROUP" "数据快照只覆盖 $have 个分片,补发布到 $want"
  fi

  # 构建 trainpublish(缺失时)
  if [ ! -x "$TRAINPUBLISH_BIN" ]; then
    if [ ! -d "$TRAINCACHE_REPO/core/cmd/trainpublish" ]; then
      log_group "$GROUP" "错误: 找不到 trainpublish 源码($TRAINCACHE_REPO)" >&2
      return 1
    fi
    log_group "$GROUP" "构建 trainpublish..."
    export PATH="$PATH:/usr/local/go/bin"
    (cd "$TRAINCACHE_REPO" && go build -o "$TRAINPUBLISH_BIN" ./core/cmd/trainpublish) \
      || { log_group "$GROUP" "错误: trainpublish 构建失败" >&2; return 1; }
  fi
  # 经 nodePort 访问 MinIO(kind 不映射到宿主机,用节点 InternalIP)
  local node_ip
  node_ip="$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null)"
  local node_port="${MINIO_NODEPORT:-30090}"
  # work-dir 必须落在数据层:默认 /tmp 是 16 GiB tmpfs,128 分片全量快照会写爆
  local work_dir="${TRAINPUBLISH_WORK_DIR:-$TRACK_B_DIR/../../_trainpublish}"
  mkdir -p "$work_dir"
  log_group "$GROUP" "发布 ${#tars[@]} 个 shard 到 MinIO (dataset=$DATASET, work=$work_dir)..."
  "$TRAINPUBLISH_BIN" \
    --dataset "$DATASET" \
    --endpoint "${node_ip}:${node_port}" \
    --access-key "$MINIO_ACCESS_KEY" \
    --secret-key "$MINIO_SECRET_KEY" \
    --bucket "$MINIO_BUCKET" \
    --chunk-size "$CHUNK_BYTES" \
    --concurrency 8 \
    --work-dir "$work_dir" \
    "${tars[@]}" \
    || { log_group "$GROUP" "错误: trainpublish 发布失败" >&2; return 1; }

  have="$(snapshot_shard_count | tr -d '[:space:]')"
  if [ "${have:-0}" -lt "$want" ]; then
    log_group "$GROUP" "错误: 快照只发布 ${have:-0}/$want 个分片" >&2
    return 1
  fi
  log_group "$GROUP" "数据快照发布完成($have 个分片)"
}
publish_snapshot

# ---- 1. 控制面与 agentd(与 bench-up 共用 ctrl_up.sh)----
# 命名空间、节点标签、两个 Secret、webhook TLS+caBundle、apply -k 与 agentd
# 配置都在 ctrl_up.sh 内完成;清单未变时不 apply。
export DATASET MINIO_BUCKET
bash "$BENCH_ROOT/scripts/ctrl_up.sh"

# ---- 2. trace ConfigMap ----
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

# ---- 3. ordersrc.py ConfigMap(训练 Pod 用于写消费顺序 ring)----
ORDERSRC_SRC="$BENCH_ROOT/../agent/ordersrc/ordersrc.py"
if resource_exists configmap traincache-ordersrc -n "$NS"; then
  log_group "$GROUP" "configmap/traincache-ordersrc 已存在,跳过"
elif [ -f "$ORDERSRC_SRC" ]; then
  kubectl -n "$NS" create configmap traincache-ordersrc \
    --from-file=ordersrc.py="$ORDERSRC_SRC"
  log_group "$GROUP" "configmap/traincache-ordersrc 已创建"
else
  log_group "$GROUP" "WARNING: $ORDERSRC_SRC 不存在,ordersrc ConfigMap 未创建" >&2
fi

# ---- 4. DataSet CR ----
# 生成 shards 列表:按实际文件大小计算 chunk 数(与 trainpublish 发布的快照一致)
SHARDS_YAML=""
for i in $(seq 0 $(( N_TARS - 1 ))); do
  f="$(printf '%s/shard-%05d.tar' "$TRACK_B_DIR" "$i")"
  shard_size="$(stat -c '%s' "$f" 2>/dev/null || echo 0)"
  count=$(( (shard_size + CHUNK_BYTES - 1) / CHUNK_BYTES ))
  last=$(( shard_size % CHUNK_BYTES ))
  [ "$last" = 0 ] && last=$CHUNK_BYTES
  SHARDS_YAML="${SHARDS_YAML}    - name: shard-$(printf '%05d' $i).tar
      count: $count
      lastChunkSize: $last
"
done

DATASET_YAML=$(cat <<EOF
apiVersion: traincache.io/v1alpha1
kind: DataSet
metadata:
  name: $DATASET
  namespace: $NS
spec:
  endpoint: $MINIO_NS.minio.svc.cluster.local:9000
  bucket: $MINIO_BUCKET
  datasetName: $DATASET
  secretRef:
    name: traincache-objstore
  chunkSize: $CHUNK_BYTES
  shards:
$SHARDS_YAML
  shuffle: false
EOF
)
# DataSet 可原地更新,直接 apply
echo "$DATASET_YAML" | kubectl apply -f -
log_group "$GROUP" "dataset/$DATASET 已 apply($N_TARS shards,chunk=${CHUNK_BYTES}B)"

# 等待 DataSet Ready(注意用 datasets.traincache.io 避免与 Fluid 的 datasets.data.fluid.io 冲突)
for i in $(seq 1 90); do
  READY=$(kubectl -n "$NS" get datasets.traincache.io "$DATASET" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
  if [ "$READY" = "True" ]; then
    log_group "$GROUP" "dataset/$DATASET Ready"
    break
  fi
  if [ "$i" = "90" ]; then
    log_group "$GROUP" "等待 DataSet Ready 超时" >&2
    exit 1
  fi
  sleep 2
done

# ---- 5. tc 档位(先挂限速,再预热)----
# 预热与测量都从 MinIO egress 取数,与 juicefs/fluid 两组同口径:预热在档位带宽之内
# 进行。之前这里先拆 tc 让预热跑满速,会让 traincache-warm 的启动收益虚高。
log_group "$GROUP" "应用 tc 档位: rate=$TC_RATE delay=$TC_DELAY burst=$TC_BURST(netem 固定 delay)"
bash "$BENCH_ROOT/scripts/tc_apply.sh" "$TC_RATE" "$TC_DELAY"

# ---- 6. WarmupTask CR(先删后建,不可原地更新)----
TRAINCACHE_WARMUP_TIMEOUT=900  # 硬编码 15 分钟,不可配置
# gate 绝对超时。全档统一 5m(单一 MinIO 出站 100mbit × fanout 2,
# 2GiB 预热约需 172s,5m 留足余量;9.2 §7 撤销了旧的 7m 特例)。
GATE_TMAX="${TRAINCACHE_GATE_TMAX:-5m}"
WARM_SECONDS=0
kubectl -n "$NS" delete warmuptask compare-task --ignore-not-found --wait=true 2>/dev/null || true

if [ "$WARMUP_K" = "0" ]; then
  # traincache-cold:WARMUP_K=0 是 bench 冷启动哨兵,不创建 WarmupTask,不走 gate
  log_group "$GROUP" "WARMUP_K=0(cold):跳过 WarmupTask 创建与等待"
  echo "[WARMUP_SECONDS] 0"  # run_bench.py 解析此行
  echo "[WARMUP_BASIS] cold"
else
  # fanout 固定 2(D26):先给两个节点预热,scheduler 按就绪度选落点;
  # 节点数只作上限校验,不够 2 个直接中止
  S3_NODE_CNT=$(kubectl get nodes -l traincache.io/s3-access=true --no-headers 2>/dev/null | wc -l)
  PREHEAT_FANOUT="${TRAINCACHE_PREHEAT_FANOUT:-2}"
  PREHEAT_FANOUT_MIN="${TRAINCACHE_PREHEAT_FANOUT_MIN:-1}"
  if [ "$S3_NODE_CNT" -lt "$PREHEAT_FANOUT" ]; then
    log_group "$GROUP" "错误: s3-access 节点数 $S3_NODE_CNT 少于 fanout $PREHEAT_FANOUT" >&2
    exit 1
  fi
  if [ "$PREHEAT_FANOUT_MIN" -gt "$PREHEAT_FANOUT" ]; then
    PREHEAT_FANOUT_MIN="$PREHEAT_FANOUT"
  fi
  log_group "$GROUP" "WarmupTask fanout: preheatFanoutMin=$PREHEAT_FANOUT_MIN preheatFanout=$PREHEAT_FANOUT ($S3_NODE_CNT 个 s3-access 节点)"

  WARMUP_YAML=$(cat <<EOF
apiVersion: traincache.io/v1alpha1
kind: WarmupTask
metadata:
  name: compare-task
  namespace: $NS
spec:
  dataSetRef:
    name: $DATASET
  targetRef:
    apiVersion: v1
    kind: Pod
    name: train-job-0
  podCount: 1
  preheatFanoutMin: $PREHEAT_FANOUT_MIN
  preheatFanout: $PREHEAT_FANOUT
  affinity: Any
  trainingType: static
  podDetails:
    - index: 0
      k: $WARMUP_K
      resources:
        requests:
          cpu: 500m
          memory: 256Mi
  gate:
    threshold: 0.9
    gapMin: 0.1
    slopeMin: 0.001
    tMax: $GATE_TMAX
    minInterval: 1s
  score:
    halfLife: 4
    cutoff: 8
  candidate:
    requireReady: true
    ignoreNoSchedule: false
EOF
  )
  echo "$WARMUP_YAML" | kubectl apply -f -
  log_group "$GROUP" "warmuptask/compare-task 已创建(targetRef=Pod/train-job-0,k=$WARMUP_K)"

  # 等待 WarmupTask 进入 Released(预热完成,gate 放行)
  log_group "$GROUP" "等待 warmuptask/compare-task 完成(预热,超时 ${TRAINCACHE_WARMUP_TIMEOUT}s)"
  WARM_T0=$(date +%s)
  WARM_PHASE=""
  while true; do
    WARM_PHASE=$(kubectl -n "$NS" get warmuptask compare-task -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    GATE_STATE=$(kubectl -n "$NS" get warmuptask compare-task -o jsonpath='{.status.gateDecision.state}' 2>/dev/null || echo "")
    if [ "$WARM_PHASE" = "Released" ] || [ "$GATE_STATE" = "Released" ]; then
      WARM_T1=$(date +%s)
      WALL_SECONDS=$(( WARM_T1 - WARM_T0 ))
      # 预热时长优先取控制器事件时间：startedAt → gateDecision.decidedAt，
      # 不受轮询量化影响。字段缺失时回退墙钟。
      STARTED_AT=$(kubectl -n "$NS" get warmuptask compare-task -o jsonpath='{.status.startedAt}' 2>/dev/null || true)
      DECIDED_AT=$(kubectl -n "$NS" get warmuptask compare-task -o jsonpath='{.status.gateDecision.decidedAt}' 2>/dev/null || true)
      WARM_SECONDS=$WALL_SECONDS
      WARM_BASIS=wall
      if [ -n "$STARTED_AT" ] && [ -n "$DECIDED_AT" ]; then
        S_EPOCH=$(date -d "$STARTED_AT" +%s 2>/dev/null || true)
        D_EPOCH=$(date -d "$DECIDED_AT" +%s 2>/dev/null || true)
        if [ -n "$S_EPOCH" ] && [ -n "$D_EPOCH" ] && [ "$D_EPOCH" -ge "$S_EPOCH" ]; then
          WARM_SECONDS=$(( D_EPOCH - S_EPOCH ))
          WARM_BASIS=event
        fi
      fi
      log_group "$GROUP" "warmuptask/compare-task 已 Released,预热时长=${WARM_SECONDS}s(basis=$WARM_BASIS,wall=${WALL_SECONDS}s,phase=$WARM_PHASE gate=$GATE_STATE)"
      echo "[WARMUP_SECONDS] $WARM_SECONDS"  # run_bench.py 解析此行
      echo "[WARMUP_BASIS] $WARM_BASIS"
      break
    fi
    if [ "$WARM_PHASE" = "Failed" ] || [ "$WARM_PHASE" = "Degraded" ]; then
      log_group "$GROUP" "warmuptask/compare-task 异常(phase=$WARM_PHASE gate=$GATE_STATE),整轮作废" >&2
      exit 1
    fi
    WARM_NOW=$(date +%s)
    if [ $(( WARM_NOW - WARM_T0 )) -ge "$TRAINCACHE_WARMUP_TIMEOUT" ]; then
      log_group "$GROUP" "等待 WarmupTask 超时(>${TRAINCACHE_WARMUP_TIMEOUT}s,phase=$WARM_PHASE gate=$GATE_STATE),整轮作废" >&2
      exit 1
    fi
    sleep 2
  done
fi

log_group "$GROUP" "up 完成:DataSet Ready,WarmupTask 已 Released,预热时长=${WARM_SECONDS}s"
echo "等待 train-job-0 被 webhook gate 放行: kubectl -n $NS get pod train-job-0 -w"
echo "查看 WarmupTask: kubectl -n $NS get warmuptask compare-task"
echo "拆除: make -C $COMPARE_ROOT compare-down GROUP=traincache"
