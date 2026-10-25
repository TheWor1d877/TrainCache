#!/usr/bin/env bash
# ============================================================================
# 对照组部署脚本共享工具(由各组 up.sh/down.sh source)。
#
# 设计目标:
#   * 幂等:所有资源操作先查再建;已存在则跳过并在日志标注。
#   * 零硬编码密钥:MinIO/JuiceFS 凭证从环境变量或既有 Secret 读取。
#   * tc 档位一致:从 COND 或环境变量取,七个组态共用同一档位(速率随 TC_RATE 走)。
#   * 日志统一:所有输出追加到 bench/compare/logs/<group>.log。
#   * 不删数据:down.sh 只删 Pod/PVC/CRD 实例,不动 MinIO 桶数据。
#
# 被 source 时不做任何副作用(只定义函数与变量);调用方按需调用。
# ============================================================================
set -euo pipefail

# ---- 路径锚点(以本文件所在目录为根)----
COMPARE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "$COMPARE_ROOT/.." && pwd)"
REPO="$(cd "$BENCH_ROOT/.." && pwd)"
PY="${PY:-$REPO/.venv/bin/python}"

# ---- config.py 取值(与 bench/scripts 同口径)----
cfg() { "$PY" "$BENCH_ROOT/config.py" --sh "$1" 2>/dev/null || true; }

# 日志落运行时数据层(路径口径由 bench/config.py 定义,repo 内不建日志目录)
LOG_DIR="$(cfg COMPARE_LOGS_DIR)"
LOG_DIR="${LOG_DIR:-$BENCH_ROOT/../../traincachedata/bench/compare/logs}"
mkdir -p "$LOG_DIR"

# ---- 共享基础设施参数(只读 config.py,不硬编码)----
MINIO_NS="$(cfg K8S_NS_MINIO)"; MINIO_NS="${MINIO_NS:-minio}"
MINIO_BUCKET="$(cfg MINIO_BUCKET)"; MINIO_BUCKET="${MINIO_BUCKET:-bench-b}"
MINIO_ENDPOINT="$(cfg MINIO_ENDPOINT)"; MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio.minio.svc.cluster.local:9000}"
# 凭证优先环境变量(允许 CI/本地覆盖),回退到 config.py 的非敏感占位
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-$(cfg MINIO_ACCESS_KEY)}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-$(cfg MINIO_SECRET_KEY)}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-traincache}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-traincache-probe}"
MINIO_NODEPORT="${MINIO_NODEPORT:-$(cfg MINIO_NODEPORT)}"
MINIO_NODEPORT="${MINIO_NODEPORT:-30090}"

# ---- tc 档位(从 COND 或环境变量取,七个组态共用同一档位)----
# D31:rate 是实验自变量 {100,300,1000}mbit;burst 跟着 rate 走(≈0.1 秒传输量,
# config.burst_for_rate),抖动只在临界格开(config.jitter_enabled(档位, rate))。
# 真正挂链路的是 tc_apply.sh(它自己推 burst、自己判抖动),这里算出来是为了
# 日志与产物里的数值等于实际挂上的数值——旧写法固定打印 1600k 是失真值。
TC_RATE="${TC_RATE:-$(cfg TC_RATE)}"; TC_RATE="${TC_RATE:-100mbit}"
TC_DELAY="${TC_DELAY:-$(cfg TC_DELAY)}"; TC_DELAY="${TC_DELAY:-20ms}"
CAP_TIER="${CAP_TIER:-${CAPACITY_RATIO:-S}}"; CAP_TIER="$(printf '%s' "$CAP_TIER" | tr '[:lower:]' '[:upper:]')"
RATE_NUM="$(printf '%s' "$TC_RATE" | sed -n 's/^\([0-9][0-9]*\).*/\1/p')"

burst_for_rate() {   # 与 tc_apply.sh 同一口径:config.burst_for_rate(rate_mbit)
  [ -n "$RATE_NUM" ] || return 0
  "$PY" -c 'import sys; sys.path.insert(0, sys.argv[1]); import config; print(config.burst_for_rate(float(sys.argv[2])))' \
    "$BENCH_ROOT" "$RATE_NUM" 2>/dev/null || true
}
jitter_for_tier() {  # 与 tc_apply.sh 同一口径:config.jitter_enabled(tier, rate_mbit)
  [ -n "$RATE_NUM" ] || return 0
  "$PY" -c 'import sys; sys.path.insert(0, sys.argv[1]); import config; print("on" if config.jitter_enabled(sys.argv[2], float(sys.argv[3])) else "off")' \
    "$BENCH_ROOT" "$CAP_TIER" "$RATE_NUM" 2>/dev/null || true
}
TC_BURST="${TC_BURST:-$(burst_for_rate)}"; TC_BURST="${TC_BURST:-$(cfg TC_BURST)}"; TC_BURST="${TC_BURST:-1600k}"
TC_JITTER="$(jitter_for_tier)"; TC_JITTER="${TC_JITTER:-off}"

# ---- 镜像(允许环境变量覆盖,默认取 survey 实测版本)----
JUICEFS_IMAGE="${JUICEFS_IMAGE:-juicedata/mount:ce-v1.3.0}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7-alpine}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
BUSYBOX_IMAGE="${BUSYBOX_IMAGE:-busybox:latest}"
TRAIN_IMAGE="${TRAIN_IMAGE:-python:3.12-alpine}"

# ---- 分片范围:把 0..N-1 号分片编成 juicefs sync 的 --include 模式 ----
# 分片命名 shard-%05d.tar。juicefs sync 的 --include 可重复传入,且必须配合 --exclude
# (include 的语义是「不排除匹配该模式的 key」)。这里把「0..N-1」拆成若干条
# 「前缀 + 数字区间」模式,精确覆盖本档位的分片,不误伤桶里更大的分片。
# 不加这段的话,juicefs/fluid 会把桶里全部 32 片(8GiB)同步进自己的文件系统,
# S 档(本档只消费 8 片 = 2GiB)的数据量口径就被撑大了 4 倍。
# 用法: shard_include_args <n>  →  打印若干行 `--include "shard-xxxxx.tar"`
shard_include_args() {
  "$PY" - "$1" <<'PYEOF'
import sys

n = int(sys.argv[1])
if n <= 0:
    sys.exit(0)

digits = [int(c) for c in f"{n - 1:05d}"]
pats = []


def emit_free(prefix, pos):
    """pos 位之后全部自由取 0..9。"""
    if pos == 5:
        pats.append(prefix)
        return
    emit_free(prefix + "[0-9]", pos + 1)


def walk(i, prefix):
    d = digits[i]
    if d > 0:
        # 这一位取比上界小的数字 → 后面的位全自由
        head = prefix + (f"[0-{d - 1}]" if d - 1 > 0 else "0")
        if i == 4:
            pats.append(head)
        else:
            emit_free(head, i + 1)
    # 这一位取等于上界的数字 → 继续下一位
    if i == 4:
        pats.append(prefix + str(d))
    else:
        walk(i + 1, prefix + str(d))


walk(0, "")
# 输出一行(中间不能断行:调用方把它拼进 juicefs sync 的续行里)
print(" ".join(f'--include "shard-{p}.tar"' for p in sorted(set(pats))))
PYEOF
}

# ---- 日志:同时写 stdout 与 logs/<group>.log ----
# 用法: log_group <group> <msg...>
log_group() {
  local grp="$1"; shift
  local ts
  ts="$(date '+%Y-%m-%d %H:%M:%S')"
  local line="[$ts] [$grp] $*"
  echo "$line"
  echo "$line" >> "$LOG_DIR/$grp.log"
}

# ---- 幂等命名空间:已存在则跳过 ----
ensure_ns() {
  local ns="$1" grp="$2"
  if kubectl get namespace "$ns" >/dev/null 2>&1; then
    log_group "$grp" "namespace/$ns 已存在,跳过"
  else
    kubectl create namespace "$ns"
    log_group "$grp" "namespace/$ns 已创建"
  fi
}

# ---- 幂等 apply:已存在同名资源则跳过(用 --dry-run 探测)----
# 用法: apply_if_absent <group> <kind> <name> [-n <ns>] -- <yaml 或 -f file>
# 简化:直接 kubectl apply(apply 本身幂等;日志里记已存在)。
apply_yaml() {
  local grp="$1"; shift
  local desc="$1"; shift
  log_group "$grp" "apply: $desc"
  kubectl apply "$@"
}

# ---- 资源是否存在 ----
resource_exists() {
  # resource_exists <kind> <name> [-n <ns>]
  kubectl get "$@" >/dev/null 2>&1
}

# ---- 等待资源就绪(轮询 jsonpath)----
# wait_ready <group> <kind> <name> [-n <ns>] <jsonpath> <expected> <timeout_s>
wait_ready() {
  local grp="$1" kind="$2" name="$3"; shift 3
  local ns="" path="" expected="" timeout=180
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -n) ns="$2"; shift 2 ;;
      --path) path="$2"; shift 2 ;;
      --expect) expected="$2"; shift 2 ;;
      --timeout) timeout="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  local args=("$kind" "$name")
  [ -n "$ns" ] && args+=(-n "$ns")
  local start; start="$(date +%s)"
  while true; do
    local cur
    cur="$(kubectl get "${args[@]}" -o "jsonpath={$path}" 2>/dev/null || true)"
    if [ "$cur" = "$expected" ]; then
      log_group "$grp" "$kind/$name 就绪($path=$expected)"
      return 0
    fi
    if [ "$(($(date +%s) - start))" -ge "$timeout" ]; then
      log_group "$grp" "等待 $kind/$name 超时(${timeout}s,当前 $path=$cur)" >&2
      return 1
    fi
    sleep 2
  done
}

# ---- 冷态规程(训练组跑前):本函数只打印提示。down+up 重建 Pod 只能清
# emptyDir;/var/cache/jfs、/tmp/juicefs-cache、/var/cache/traincache/* 是
# 节点 hostPath,不随 Pod 删除消失,由 run_bench.py 的 cold_reset() 在节点上
# 显式清理并判空。对 direct,无缓存层无需冷态。
cold_state_note() {
  local grp="$1"
  log_group "$grp" "冷态:hostPath 缓存由 run_bench.py cold_reset() 显式清理并判空;down+up 只清 emptyDir"
}

# ---- 校验 MinIO 桶可访问(所有组共享)----
check_minio() {
  local grp="$1"
  if ! kubectl get namespace "$MINIO_NS" >/dev/null 2>&1; then
    log_group "$grp" "错误:MinIO 命名空间 $MINIO_NS 不存在,请先 make bench-up" >&2
    return 1
  fi
  local pod
  pod="$(kubectl -n "$MINIO_NS" get pod -l app=minio -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -z "$pod" ]; then
    log_group "$grp" "错误:MinIO Pod 未就绪" >&2
    return 1
  fi
  log_group "$grp" "MinIO 就绪:ns=$MINIO_NS bucket=$MINIO_BUCKET pod=$pod"
  return 0
}

# ---- 清理某命名空间下的所有工作负载与 CRD 实例(不动 MinIO 数据)----
# 用法: cleanup_ns <group> <ns> [extra kinds...]
cleanup_ns() {
  local grp="$1" ns="$2"; shift 2
  if ! kubectl get namespace "$ns" >/dev/null 2>&1; then
    log_group "$grp" "namespace/$ns 不存在,无需清理"
    return 0
  fi
  log_group "$grp" "清理 namespace/$ns 下的工作负载与 CRD 实例(保留 MinIO 数据)"
  # 先删 Pod/Job/Deployment/StatefulSet/PVC/ConfigMap/Secret(组内自建)
  for kind in pods jobs deployments statefulsets \
              persistentvolumeclaims configmaps secrets; do
    kubectl -n "$ns" delete "$kind" --all --ignore-not-found --wait=false \
      2>/dev/null || true
  done
  # Fluid 与 TrainCache 的 CRD 实例(按 kind 逐个删,不存在则跳过)
  for kind in "$@" datasets dataloads juicefsruntimes warmuptasks; do
    kubectl -n "$ns" delete "$kind" --all --ignore-not-found --wait=false \
      2>/dev/null || true
  done
  log_group "$grp" "namespace/$ns 清理完成(命名空间本身保留,便于复用)"
}

# ---- 打印分组头部 ----
group_header() {
  local grp="$1"
  echo ""
  echo "============================================================"
  log_group "$grp" "===== 对照组: $grp ====="
  log_group "$grp" "MinIO: ns=$MINIO_NS bucket=$MINIO_BUCKET endpoint=$MINIO_ENDPOINT"
  log_group "$grp" "tc 档位: rate=$TC_RATE delay=$TC_DELAY burst=$TC_BURST 抖动=$TC_JITTER(档位 $CAP_TIER)"
  echo "============================================================"
}
