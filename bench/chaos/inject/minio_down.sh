#!/usr/bin/env bash
# minio_down.sh — 故障注入:把 MinIO Deployment 缩到 0 副本(源站整体不可用)。
#
# 注入手法:
#   kubectl -n <ns> scale deploy/<minio-deploy> --replicas=0
#   等 Deployment 的 readyReplicas 归零 + Pod 全部消失(有界重试)
#
# --restore 模式:
#   按记录下来的原始副本数 scale 回去(原始值在缩容前先记录到状态文件,
#   跨进程可读,--restore 可以是另一次调用)。
#
# 数据安全性:
#   MinIO 的数据在 PVC 上(bench/k8s/minio.yaml:9-20,minio-data,30Gi),
#   缩容只停 Pod,不动 PVC,桶里的数据不丢;恢复后原样在。
#
# 与 tc_apply.sh 的关系:两者都会碰 MinIO Pod 的网络(前者限速、这个下线),
#   跑本脚本前若 eth0 上还挂着 tc_apply.sh 的限速,建议先 tc_teardown.sh。
#
# 默认行为是"缩容"(故障注入本身),不缩容时 --restore 显式恢复;
# 两者都不是删除对象,Pod/Deployment/PVC 都留着。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "$SCRIPT_DIR/../lib/common.sh"

NS="$CHAOS_MINIO_NS_DEFAULT"
DEPLOY="$CHAOS_MINIO_DEPLOY_DEFAULT"
TIMEOUT=120
OBSERVE=30
TRAIN_POD="$CHAOS_TRAIN_POD_DEFAULT"
AGENT_NS="$CHAOS_NS_DEFAULT"
RESTORE=0
REPLICAS_OVERRIDE=""
RECOVER=1
RECOVER_OBSERVE=60
TRAIN_MANIFEST=""
DRY_RUN=0
KUBECONFIG_ARG=""
OUT_DIR=""

usage() {
  cat <<'EOF'
用法: minio_down.sh [--ns NAMESPACE] [--deploy DEPLOY] [--timeout SECONDS]
                    [--observe SECONDS] [--agent-ns NAMESPACE]
                    [--restore] [--replicas N] [--no-recover]
                    [--recover-observe SECONDS] [--train-manifest PATH]
                    [--out DIR] [--kubeconfig PATH]
                    [--dry-run] [--help]
  --ns           MinIO 所在 namespace(默认 minio)
  --deploy       MinIO Deployment 名(默认 minio;bench/k8s/minio.yaml:25)
  --timeout      等副本归零/Pod 消失的上限秒数(默认 120)
  --observe      下线后的观察窗口秒数(默认 30),期间轮询 agentd 侧指标
  --agent-ns     agentd 所在 namespace(默认 traincache),用于取 source_mode 等指标
  --restore      恢复模式:按记录的原始副本数 scale 回去
  --replicas     恢复时指定副本数(不给则用状态文件里记录的原始值)
  --no-recover   下线后不自动恢复源站与训练 Pod
  --recover-observe 恢复源站后的观察窗口秒数(默认 60),期间取回源错误计数增量
  --train-manifest  训练 Pod 清单路径(默认取仓库内 agent/deploy/train-pod.yaml)
  --out          证据目录(写 replicas.txt / assertions.txt)
  --kubeconfig   本轮 kubeconfig 副本;不给则要求环境变量 KUBECONFIG
  --dry-run      只打印将执行的命令,不碰集群
  --help         本用法
状态文件: $CHAOS_DATA_ROOT/minio_replicas.txt(默认 ~/tcd/chaos/minio_replicas.txt),
          每行 "ns deploy replicas timestamp",--restore 取最后一条匹配记录。
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --ns) [ "$#" -ge 2 ] || { echo "错误:--ns 缺值" >&2; exit 2; }; NS="$2"; shift 2 ;;
    --ns=*) NS="${1#--ns=}"; shift ;;
    --deploy) [ "$#" -ge 2 ] || { echo "错误:--deploy 缺值" >&2; exit 2; }; DEPLOY="$2"; shift 2 ;;
    --deploy=*) DEPLOY="${1#--deploy=}"; shift ;;
    --timeout) [ "$#" -ge 2 ] || { echo "错误:--timeout 缺值" >&2; exit 2; }; TIMEOUT="$2"; shift 2 ;;
    --timeout=*) TIMEOUT="${1#--timeout=}"; shift ;;
    --observe) [ "$#" -ge 2 ] || { echo "错误:--observe 缺值" >&2; exit 2; }; OBSERVE="$2"; shift 2 ;;
    --observe=*) OBSERVE="${1#--observe=}"; shift ;;
    --agent-ns) [ "$#" -ge 2 ] || { echo "错误:--agent-ns 缺值" >&2; exit 2; }; AGENT_NS="$2"; shift 2 ;;
    --agent-ns=*) AGENT_NS="${1#--agent-ns=}"; shift ;;
    --restore) RESTORE=1; shift ;;
    --replicas) [ "$#" -ge 2 ] || { echo "错误:--replicas 缺值" >&2; exit 2; }; REPLICAS_OVERRIDE="$2"; shift 2 ;;
    --replicas=*) REPLICAS_OVERRIDE="${1#--replicas=}"; shift ;;
    --no-recover) RECOVER=0; shift ;;
    --recover-observe) [ "$#" -ge 2 ] || { echo "错误:--recover-observe 缺值" >&2; exit 2; }; RECOVER_OBSERVE="$2"; shift 2 ;;
    --recover-observe=*) RECOVER_OBSERVE="${1#--recover-observe=}"; shift ;;
    --train-manifest) [ "$#" -ge 2 ] || { echo "错误:--train-manifest 缺值" >&2; exit 2; }; TRAIN_MANIFEST="$2"; shift 2 ;;
    --train-manifest=*) TRAIN_MANIFEST="${1#--train-manifest=}"; shift ;;
    --out) [ "$#" -ge 2 ] || { echo "错误:--out 缺值" >&2; exit 2; }; OUT_DIR="$2"; shift 2 ;;
    --out=*) OUT_DIR="${1#--out=}"; shift ;;
    --kubeconfig) [ "$#" -ge 2 ] || { echo "错误:--kubeconfig 缺值" >&2; exit 2; }; KUBECONFIG_ARG="$2"; shift 2 ;;
    --kubeconfig=*) KUBECONFIG_ARG="${1#--kubeconfig=}"; shift ;;
    *) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
  esac
done

chaos_resolve_kubeconfig "$KUBECONFIG_ARG"

STATE_FILE="$(chaos_data_root)/minio_replicas.txt"

if [ "$DRY_RUN" = 1 ]; then
  chaos_note "[dry-run] 故障:minio_down(缩 MinIO 到 0 副本)$([ "$RESTORE" = 1 ] && echo ' [恢复模式]')"
  if [ "$RESTORE" = 1 ]; then
    chaos_show_cmd -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.spec.replicas}'
    chaos_note "[dry-run]   ↑ 当前副本数"
    chaos_note "[dry-run]   原始副本数来源: --replicas 显式值,否则读状态文件 $STATE_FILE 的最后一条匹配记录"
    chaos_show_cmd -n "$NS" scale "deploy/$DEPLOY" --replicas="\$ORIGINAL"
    chaos_note "[dry-run]   ↑ 按原始副本数 scale 回去"
    chaos_show_cmd -n "$NS" rollout status "deploy/$DEPLOY" --timeout="${TIMEOUT}s"
  else
    chaos_show_cmd -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.spec.replicas}'
    chaos_note "[dry-run]   ↑ 缩容前先取原始副本数(记到 $STATE_FILE,供 --restore 使用)"
    chaos_show_cmd -n "$NS" scale "deploy/$DEPLOY" --replicas=0
    chaos_show_cmd -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.status.readyReplicas}{" "}{.status.replicas}'
    chaos_note "[dry-run]   ↑ 轮询等 readyReplicas 归零"
    chaos_show_cmd -n "$NS" get pods -l app=minio -o wide
    chaos_note "[dry-run]   ↑ 确认 MinIO Pod 全部消失(有界重试,超时即失败)"
    chaos_show_cmd -n "$AGENT_NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
      wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics"
    chaos_note "[dry-run]   ↑ 观察窗口 ${OBSERVE}s 内取 traincache_source_mode / traincache_gateway_fallback_total(源站不可用时不应变化)"
    chaos_show_cmd -n "$AGENT_NS" get warmuptasks.eta.traincache.io -o jsonpath='{.items[*].status.phase}'
    chaos_note "[dry-run]   ↑ 取 WarmupTask status.phase(CRD 直读,门控降级判据)"
    if [ "$RECOVER" = 1 ]; then
      chaos_show_cmd -n "$NS" scale "deploy/$DEPLOY" --replicas="\$ORIGINAL"
      chaos_note "[dry-run]   ↑ 恢复源站副本数"
      chaos_show_cmd -n "$AGENT_NS" delete pod "\$TRAIN_POD" --grace-period=0 --wait=false
      chaos_show_cmd -n "$AGENT_NS" apply -f "\$TRAIN_MANIFEST"
      chaos_note "[dry-run]   ↑ 重新起训:删掉训练 Pod 再按清单重建"
      chaos_show_cmd -n "$AGENT_NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
        wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics"
      chaos_note "[dry-run]   ↑ 恢复窗口 ${RECOVER_OBSERVE}s 内取 traincache_fetch_requests_total{result=\"error\"} 增量"
    fi
  fi
  exit 0
fi

chaos_require_kubeconfig
chaos_prepare_out_dir
ASSERT_FILE="$OUT_DIR/assertions.txt"
: >"$ASSERT_FILE"
pass() { printf 'PASS %s\n' "$*" | tee -a "$ASSERT_FILE"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$ASSERT_FILE" >&2; }

ctx="$(chaos_record_context)"
chaos_note "kubeconfig=$CHAOS_KUBECONFIG context=$ctx ns=$NS deploy=$DEPLOY"

chaos_kubectl -n "$NS" get deploy "$DEPLOY" >/dev/null 2>&1 \
  || chaos_die "部署 $NS/$DEPLOY 不存在;MinIO 由 bench 的 k8s-up 目标装(bench/Makefile:47-49)"

CURRENT="$(chaos_kubectl -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.spec.replicas}')"
chaos_note "当前副本数=$CURRENT"

if [ "$RESTORE" = 1 ]; then
  TARGET="$REPLICAS_OVERRIDE"
  if [ -z "$TARGET" ]; then
    if [ -r "$STATE_FILE" ]; then
      TARGET="$(grep -E "^${NS} ${DEPLOY} " "$STATE_FILE" | tail -1 | awk '{print $3}')"
    fi
    [ -n "$TARGET" ] || chaos_die "没有原始副本数可恢复:状态文件 $STATE_FILE 里没有 $NS/$DEPLOY 的记录,且未给 --replicas"
  fi
  chaos_note "恢复到副本数=$TARGET"
  chaos_kubectl -n "$NS" scale "deploy/$DEPLOY" --replicas="$TARGET" >/dev/null
  OK=0
  for _ in $(seq 1 $((TIMEOUT / 5 + 1))); do
    READY="$(chaos_kubectl -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)"
    [ "${READY:-0}" = "$TARGET" ] && { OK=1; break; }
    sleep 5
  done
  if [ "$OK" = 1 ]; then
    pass "MinIO 已恢复到 $TARGET 副本且 Ready"
  else
    fail "MinIO 恢复超时(${TIMEOUT}s 内 readyReplicas 未到 $TARGET)"
  fi

  # ---- 恢复后补块:训练侧读到取不到的块,hook 记 read_miss,Agent 回源补块 ----
  # read_miss 由训练侧经 hook socket 上报,集群里没有挂 hook 的训练 Pod 时该判据不适用。
  AGENTD_POD="$(chaos_agentd_pod_rows "$AGENT_NS" | awk '$3=="True"{print $1; exit}')"
  HOOK_POD="$(chaos_hook_client_pod "$AGENT_NS")"
  MISS_BEFORE=""
  MISS_AFTER=""
  FETCH_BEFORE=""
  FETCH_AFTER=""
  ERR_BEFORE=""
  ERR_AFTER=""
  TRAIN_PHASE_AFTER=""
  if [ -n "$AGENTD_POD" ]; then
    MISS_BEFORE="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_hook_read_miss_total reason absent)"
    FETCH_BEFORE="$(chaos_metric_sum "$AGENT_NS" "$AGENTD_POD" traincache_fetch_bytes_total)"
    ERR_BEFORE="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_fetch_requests_total result error)"
    END_TS=$(( $(date +%s) + OBSERVE ))
    while [ "$(date +%s)" -lt "$END_TS" ]; do
      MISS_AFTER="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_hook_read_miss_total reason absent)"
      FETCH_AFTER="$(chaos_metric_sum "$AGENT_NS" "$AGENTD_POD" traincache_fetch_bytes_total)"
      ERR_AFTER="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_fetch_requests_total result error)"
      sleep 3
    done
    if [ -n "$HOOK_POD" ]; then
      MISS_DELTA="$(awk -v a="${MISS_AFTER:-0}" -v b="${MISS_BEFORE:-0}" 'BEGIN{printf "%d",(a+0)-(b+0)}')"
      if [ "$MISS_DELTA" -gt 0 ]; then
        pass "源站恢复后 hook read_miss(reason=absent)上升 ${MISS_DELTA}(${MISS_BEFORE:-0} → ${MISS_AFTER:-0}),训练侧取不到的块被补回"
      else
        fail "源站恢复后 hook read_miss(reason=absent)没涨(${MISS_BEFORE:-0} → ${MISS_AFTER:-0}),补块没发生"
      fi
    else
      chaos_note "命名空间 $AGENT_NS 里没有挂 hook 的训练 Pod,read_miss 判据不适用;回源字节 ${FETCH_BEFORE:-0} → ${FETCH_AFTER:-0} 只作证据"
    fi
    ERR_DELTA="$(awk -v a="${ERR_AFTER:-0}" -v b="${ERR_BEFORE:-0}" 'BEGIN{printf "%d",(a+0)-(b+0)}')"
    if [ "$ERR_DELTA" -eq 0 ]; then
      pass "源站恢复窗口内回源错误计数没涨(${ERR_BEFORE:-0} → ${ERR_AFTER:-0}),回源路径恢复"
    else
      fail "源站恢复窗口内回源错误计数涨了 ${ERR_DELTA}(${ERR_BEFORE:-0} → ${ERR_AFTER:-0})"
    fi
    TRAIN_PHASE_AFTER="$(chaos_kubectl -n "$AGENT_NS" get pod "$TRAIN_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [ "$TRAIN_PHASE_AFTER" = "Running" ]; then
      pass "训练 Pod $TRAIN_POD 在源站恢复后是 Running"
    else
      fail "训练 Pod $TRAIN_POD 在源站恢复后的 phase=${TRAIN_PHASE_AFTER:-<取不到>}(期望 Running)"
    fi
  else
    chaos_note "命名空间 $AGENT_NS 里没有 Ready 的 agentd Pod,补块判据跳过"
  fi
  printf 'restored_replicas=%s\ncurrent_replicas=%s\nhook_client_pod=%s\nread_miss_absent_before=%s\nread_miss_absent_after=%s\nfetch_bytes_before=%s\nfetch_bytes_after=%s\nfetch_errors_before=%s\nfetch_errors_after=%s\ntrain_pod_phase_after=%s\n' \
    "$TARGET" "${READY:-0}" "${HOOK_POD:-}" "${MISS_BEFORE:-}" "${MISS_AFTER:-}" \
    "${FETCH_BEFORE:-}" "${FETCH_AFTER:-}" "${ERR_BEFORE:-}" "${ERR_AFTER:-}" "${TRAIN_PHASE_AFTER:-}" \
    >"$OUT_DIR/replicas.txt"
  [ "$OK" = 1 ]
  exit $?
fi

# ---- 缩容:先记原始副本数,再 scale 到 0 ----
ORIGINAL="$CURRENT"
mkdir -p "$(dirname "$STATE_FILE")"
printf '%s %s %s %s\n' "$NS" "$DEPLOY" "$ORIGINAL" "$(date -u +%Y%m%dT%H%M%SZ)" >>"$STATE_FILE"
chaos_note "原始副本数=$ORIGINAL 已记入 $STATE_FILE"

# ---- 下线前基线:agentd 侧 source_mode / gateway_fallback_total(源站不可用时它们不应动)----
AGENTD_POD="$(chaos_agentd_pod_rows "$AGENT_NS" | awk '$3=="True"{print $1; exit}')"
SRC_MODE_BEFORE=""
FB_BEFORE=""
if [ -n "$AGENTD_POD" ]; then
  SRC_MODE_BEFORE="$(chaos_metric_sum "$AGENT_NS" "$AGENTD_POD" traincache_source_mode)"
  FB_BEFORE="$(chaos_metric_sum "$AGENT_NS" "$AGENTD_POD" traincache_gateway_fallback_total)"
  chaos_note "基线(agentd=$AGENTD_POD):source_mode=${SRC_MODE_BEFORE:-<取不到>} gateway_fallback_total=${FB_BEFORE:-0}"
else
  chaos_note "警告:命名空间 $AGENT_NS 里没有 Ready 的 agentd Pod,source_mode/gateway_fallback 判据跳过"
fi

chaos_kubectl -n "$NS" scale "deploy/$DEPLOY" --replicas=0 >/dev/null
OK=0
for _ in $(seq 1 $((TIMEOUT / 5 + 1))); do
  READY="$(chaos_kubectl -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)"
  REPLICAS="$(chaos_kubectl -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.status.replicas}' 2>/dev/null || echo 0)"
  if [ "${READY:-0}" = "0" ] && [ "${REPLICAS:-0}" = "0" ]; then
    OK=1
    break
  fi
  sleep 5
done
if [ "$OK" = 1 ]; then
  pass "MinIO 已下线(replicas=0 readyReplicas=0)"
else
  fail "MinIO 未在 ${TIMEOUT}s 内下线(readyReplicas=${READY:-?} replicas=${REPLICAS:-?})"
fi
printf 'original_replicas=%s\nready_replicas=%s\nreplicas=%s\nstate_file=%s\n' \
  "$ORIGINAL" "${READY:-}" "${REPLICAS:-}" "$STATE_FILE" >"$OUT_DIR/replicas.txt"
chaos_note "原始副本数=$ORIGINAL(恢复用:--restore)"

# ---- 观察窗口:源站不可用时 source_mode / gateway_fallback_total 不应变化 ----
SRC_MODE_AFTER="$SRC_MODE_BEFORE"
FB_AFTER="$FB_BEFORE"
if [ -n "$AGENTD_POD" ]; then
  END_TS=$(( $(date +%s) + OBSERVE ))
  while [ "$(date +%s)" -lt "$END_TS" ]; do
    SRC_MODE_AFTER="$(chaos_metric_sum "$AGENT_NS" "$AGENTD_POD" traincache_source_mode)"
    FB_AFTER="$(chaos_metric_sum "$AGENT_NS" "$AGENTD_POD" traincache_gateway_fallback_total)"
    sleep 3
  done
  FB_DELTA="$(awk -v a="${FB_AFTER:-0}" -v b="${FB_BEFORE:-0}" 'BEGIN{print (a+0)-(b+0)}')"
  if [ "$(awk -v a="$FB_DELTA" 'BEGIN{print (a+0==0)?1:0}')" = "1" ]; then
    pass "traincache_gateway_fallback_total 未增长(${FB_BEFORE:-0} → ${FB_AFTER:-0}),没有错误切到备用源"
  else
    fail "traincache_gateway_fallback_total 增长了 ${FB_DELTA}(源站不可用时不应切源)"
  fi
  if [ "${SRC_MODE_AFTER:-}" = "${SRC_MODE_BEFORE:-}" ]; then
    pass "traincache_source_mode 未变化(${SRC_MODE_BEFORE:-<取不到>})"
  else
    fail "traincache_source_mode 变了:${SRC_MODE_BEFORE:-<取不到>} → ${SRC_MODE_AFTER:-<取不到>}"
  fi
  printf 'source_mode_before=%s\nsource_mode_after=%s\ngateway_fallback_before=%s\ngateway_fallback_after=%s\nagentd_pod=%s\nobserve_s=%s\n' \
    "${SRC_MODE_BEFORE:-}" "${SRC_MODE_AFTER:-}" "${FB_BEFORE:-}" "${FB_AFTER:-}" "$AGENTD_POD" "$OBSERVE" \
    >>"$OUT_DIR/replicas.txt"
fi

# ---- 门控与训练终态:源站不可用时 WarmupTask 落 Degraded,训练终止是预期终态 ----
# 门控与训练终态由 bench/compare 的装置产生(WarmupTask + 带 hook 的训练 Job)。
# 集群里没有 WarmupTask 时这两项判据不适用,只记录不判定。
WARMUP_NAMES="$(chaos_cr_jsonpath warmuptask '{range .items[*]}{.metadata.name}{"\n"}{end}' "$AGENT_NS")"
WARMUP_PHASES="$(chaos_cr_jsonpath warmuptask '{range .items[*].status.phase}{.}{" "}{end}' "$AGENT_NS")"
TRAIN_PHASE="$(chaos_kubectl -n "$AGENT_NS" get pod "$TRAIN_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
JOB_FAILED="$(chaos_cr_jsonpath job '{range .items[*]}{.status.failed}{" "}{end}' "$AGENT_NS")"
if [ -z "$(printf '%s' "$WARMUP_NAMES" | tr -d '[:space:]')" ]; then
  chaos_note "集群里没有 WarmupTask,门控判据与训练终态判据不适用(门控装置在 bench/compare 的 WarmupTask 里)"
else
  if printf '%s' "$WARMUP_PHASES" | grep -q "Degraded"; then
    pass "WarmupTask status.phase 出现 Degraded:${WARMUP_PHASES}"
  else
    fail "WarmupTask status.phase 里没有 Degraded(当前:${WARMUP_PHASES:-<取不到>})"
  fi
  TERMINAL=0
  case "$TRAIN_PHASE" in
    Failed|Succeeded) TERMINAL=1 ;;
  esac
  if [ "$(awk -v a="${JOB_FAILED:-0}" 'BEGIN{print (a+0>0)?1:0}')" = "1" ]; then
    TERMINAL=1
  fi
  if [ "$TERMINAL" = "1" ]; then
    pass "训练终态到达(训练 Pod $TRAIN_POD phase=${TRAIN_PHASE:-<无>},Job failed=${JOB_FAILED:-0}),源站不可用下训练终止是预期终态"
  else
    fail "训练终态没到(训练 Pod $TRAIN_POD phase=${TRAIN_PHASE:-<取不到>},Job failed=${JOB_FAILED:-0}),源站不可用时训练应终止"
  fi
fi
printf 'warmup_names=%s\nwarmup_phases=%s\ntrain_pod=%s\ntrain_pod_phase=%s\njob_failed=%s\n' \
  "$(printf '%s' "${WARMUP_NAMES:-}" | tr '\n' ' ')" "${WARMUP_PHASES:-}" "$TRAIN_POD" "${TRAIN_PHASE:-}" "${JOB_FAILED:-}" \
  >>"$OUT_DIR/replicas.txt"

# ---- 恢复:源站回到原副本数,训练 Pod 重新起训,回源错误不再增长 ----
if [ "$RECOVER" = 1 ]; then
  if [ -z "$TRAIN_MANIFEST" ]; then
    TRAIN_MANIFEST="$CHAOS_REPO/agent/deploy/train-pod.yaml"
  fi
  chaos_note "恢复源站:scale deploy/$DEPLOY --replicas=$ORIGINAL"
  chaos_kubectl -n "$NS" scale "deploy/$DEPLOY" --replicas="$ORIGINAL" >/dev/null
  ROK=0
  for _ in $(seq 1 $((TIMEOUT / 5 + 1))); do
    RREADY="$(chaos_kubectl -n "$NS" get deploy "$DEPLOY" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)"
    [ "${RREADY:-0}" = "$ORIGINAL" ] && { ROK=1; break; }
    sleep 5
  done
  if [ "$ROK" = 1 ]; then
    pass "源站已恢复到 $ORIGINAL 副本且 Ready"
  else
    fail "源站恢复超时(${TIMEOUT}s 内 readyReplicas 未到 $ORIGINAL)"
  fi

  ERR_BEFORE=0
  if [ -n "$AGENTD_POD" ]; then
    ERR_BEFORE="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_fetch_requests_total result error)"
    ERR_BEFORE="${ERR_BEFORE:-0}"
  fi

  chaos_note "重新起训:删除训练 Pod $TRAIN_POD 后按清单重建"
  HOOK_POD="$(chaos_hook_client_pod "$AGENT_NS")"
  if [ "$HOOK_POD" = "$TRAIN_POD" ]; then
    chaos_note "训练 Pod $TRAIN_POD 挂了 hook socket,不删不重建,避免丢掉 hook 挂载"
  else
  chaos_kubectl -n "$AGENT_NS" delete pod "$TRAIN_POD" --grace-period=0 --wait=false >/dev/null 2>&1 || true
  sleep 3
  if [ -r "$TRAIN_MANIFEST" ]; then
    chaos_kubectl -n "$AGENT_NS" apply -f "$TRAIN_MANIFEST" >/dev/null 2>&1 \
      || fail "训练 Pod 重建失败:apply -f $TRAIN_MANIFEST"
  else
    chaos_note "警告:训练 Pod 清单不可读($TRAIN_MANIFEST),跳过重建"
  fi
  TOK=0
  TPHASE=""
  for _ in $(seq 1 $((TIMEOUT / 5 + 1))); do
    TPHASE="$(chaos_kubectl -n "$AGENT_NS" get pod "$TRAIN_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$TPHASE" = "Running" ] && { TOK=1; break; }
    sleep 5
  done
  if [ "$TOK" = 1 ]; then
    pass "训练 Pod $TRAIN_POD 重建后 Running"
  else
    fail "训练 Pod $TRAIN_POD 重建后未 Running(phase=${TPHASE:-<取不到>})"
  fi
  fi

  ERR_AFTER="$ERR_BEFORE"
  if [ -n "$AGENTD_POD" ]; then
    END_TS=$(( $(date +%s) + RECOVER_OBSERVE ))
    while [ "$(date +%s)" -lt "$END_TS" ]; do
      ERR_AFTER="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_fetch_requests_total result error)"
      ERR_AFTER="${ERR_AFTER:-0}"
      sleep 5
    done
    ERR_DELTA="$(awk -v a="${ERR_AFTER:-0}" -v b="${ERR_BEFORE:-0}" 'BEGIN{print (a+0)-(b+0)}')"
    if [ "$(awk -v a="$ERR_DELTA" 'BEGIN{print (a+0==0)?1:0}')" = "1" ]; then
      pass "恢复窗口 ${RECOVER_OBSERVE}s 内 traincache_fetch_requests_total{result=\"error\"} 增量为 0(${ERR_BEFORE} → ${ERR_AFTER}),回源路径不再报错"
    else
      fail "恢复窗口 ${RECOVER_OBSERVE}s 内回源错误仍增长 ${ERR_DELTA}(源站恢复后不应再有错误回源)"
    fi
    printf 'recover_error_before=%s\nrecover_error_after=%s\nrecover_observe_s=%s\ntrain_manifest=%s\n' \
      "$ERR_BEFORE" "$ERR_AFTER" "$RECOVER_OBSERVE" "$TRAIN_MANIFEST" >>"$OUT_DIR/replicas.txt"
  fi

  # 补块续上:挂 hook 的训练 Pod 在位时,读不到块会报 read_miss,Agent 走 demand 补块
  if [ -z "$HOOK_POD" ]; then
    chaos_note "命名空间 $AGENT_NS 里没有挂 hook socket 的训练 Pod,read_miss 补块判据不适用"
  elif [ -z "$AGENTD_POD" ]; then
    chaos_note "没有 Ready 的 agentd Pod,read_miss 补块判据跳过"
  else
    MISS_BEFORE="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_hook_read_miss_total reason absent)"
    MISS_BEFORE="${MISS_BEFORE:-0}"
    MISS_AFTER="$MISS_BEFORE"
    END_TS=$(( $(date +%s) + RECOVER_OBSERVE ))
    while [ "$(date +%s)" -lt "$END_TS" ]; do
      MISS_AFTER="$(chaos_metric_label_sum "$AGENT_NS" "$AGENTD_POD" traincache_hook_read_miss_total reason absent)"
      MISS_AFTER="${MISS_AFTER:-0}"
      sleep 5
    done
    MISS_DELTA="$(awk -v a="${MISS_AFTER:-0}" -v b="${MISS_BEFORE:-0}" 'BEGIN{print (a+0)-(b+0)}')"
    if [ "$(awk -v a="$MISS_DELTA" 'BEGIN{print (a+0>0)?1:0}')" = "1" ]; then
      pass "恢复后 traincache_hook_read_miss_total{reason=\"absent\"} 上升 ${MISS_DELTA}(hook 训练 Pod=$HOOK_POD),读不到块的走 demand 补块"
    else
      fail "恢复后 read_miss 没有上升(${MISS_BEFORE} → ${MISS_AFTER}),补块路径没被触发"
    fi
    printf 'hook_pod=%s\nread_miss_absent_before=%s\nread_miss_absent_after=%s\n' \
      "$HOOK_POD" "$MISS_BEFORE" "$MISS_AFTER" >>"$OUT_DIR/replicas.txt"
  fi
fi

[ "$OK" = 1 ]
