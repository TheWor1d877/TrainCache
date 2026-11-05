#!/usr/bin/env bash
# stop_ordersrc.sh — 故障注入:冻结训练 Pod 里 ordersrc sidecar 的写端,让订单 ring 停摆。
#
# 为什么不能 kill -STOP 1:
#   容器里的 pid 1 忽略 SIGSTOP(内核不对 pid 1 施加默认动作),对 pid 1 发 SIGSTOP
#   是空操作。ordersrc sidecar 的入口就是 pid 1(sh -c 循环,agent/deploy/train-pod.yaml:62-69),
#   所以要冻的是它下面那些 PPid==1 的子进程(python3 / sleep):sh 阻塞在 wait,
#   写端停摆 → ring 不再推进 → Agent 侧按停滞判据进入降级。
#   同一手法已在 agent/e2e/run-k8s.sh:148-196 实现并验证过(那里叫 sidecar_pause/
#   sidecar_resume),本脚本复用其做法,不重写机制。
#
# 实现要点:
#   · 在训练 Pod 的 ordersrc 容器内遍历 /proc,选 PPid==1 且不是自己 exec 进来的
#     shell 的进程发 SIGSTOP;排除自己用 $$ + 一个唯一 marker(cmdline 里带 marker
#     的一律跳过),免得把执行本命令的 shell 自己也冻住。
#   · --resume 对同一批 pid 发 SIGCONT。
#
# 断言(证据目录内):
#   · 报告被冻结的 pid 列表与数量
#   · ring 文件(/run/traincache/orders/<pod-uid>.ring)在冻结窗口内不再推进
#     (size/mtime 不变);ordersrc 每 10s 写一次(agent/deploy/train-pod.yaml:66-69),
#     故观察窗口默认 35s 足以看出"仍在推进"的对照。
#   · agentd 侧降级判据:traincache_agent_ordersources{state="stalled"} ≥1、
#     traincache_agent_degraded 由 0 走到 1(写端停摆满 30s 后才判降级,
#     agent/metrics/families.go:68、agent/metrics/families.go:99)。
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "$SCRIPT_DIR/../lib/common.sh"

NS="$CHAOS_NS_DEFAULT"
POD="$CHAOS_TRAIN_POD_DEFAULT"
CONTAINER="$CHAOS_ORDERSRC_CONTAINER_DEFAULT"
OBSERVE=45
RESUME=0
DRY_RUN=0
KUBECONFIG_ARG=""
OUT_DIR=""

usage() {
  cat <<'EOF'
用法: stop_ordersrc.sh [--ns NAMESPACE] [--pod TRAIN_POD] [--container ordersrc]
                       [--observe SECONDS] [--resume] [--out DIR]
                       [--kubeconfig PATH] [--dry-run] [--help]
  --ns           训练 Pod 所在 namespace(默认 traincache)
  --pod          训练 Pod 名(默认 traincache-train;agent/deploy/train-pod.yaml:19)
  --container    sidecar 容器名(默认 ordersrc)
  --observe      冻结后的观察窗口秒数(默认 45;写端停摆满 30s 才判降级)
  --resume       恢复模式:对被冻结的进程发 SIGCONT
  --out          证据目录(写 metrics.txt / assertions.txt / ring_stat.txt)
  --kubeconfig   本轮 kubeconfig 副本;不给则要求环境变量 KUBECONFIG
  --dry-run      只打印将执行的命令,不碰集群
  --help         本用法
注意: 容器 pid 1 忽略 SIGSTOP,脚本冻的是 PPid==1 的子进程(见文件头说明)。
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --ns) [ "$#" -ge 2 ] || { echo "错误:--ns 缺值" >&2; exit 2; }; NS="$2"; shift 2 ;;
    --ns=*) NS="${1#--ns=}"; shift ;;
    --pod) [ "$#" -ge 2 ] || { echo "错误:--pod 缺值" >&2; exit 2; }; POD="$2"; shift 2 ;;
    --pod=*) POD="${1#--pod=}"; shift ;;
    --container) [ "$#" -ge 2 ] || { echo "错误:--container 缺值" >&2; exit 2; }; CONTAINER="$2"; shift 2 ;;
    --container=*) CONTAINER="${1#--container=}"; shift ;;
    --observe) [ "$#" -ge 2 ] || { echo "错误:--observe 缺值" >&2; exit 2; }; OBSERVE="$2"; shift 2 ;;
    --observe=*) OBSERVE="${1#--observe=}"; shift ;;
    --resume) RESUME=1; shift ;;
    --out) [ "$#" -ge 2 ] || { echo "错误:--out 缺值" >&2; exit 2; }; OUT_DIR="$2"; shift 2 ;;
    --out=*) OUT_DIR="${1#--out=}"; shift ;;
    --kubeconfig) [ "$#" -ge 2 ] || { echo "错误:--kubeconfig 缺值" >&2; exit 2; }; KUBECONFIG_ARG="$2"; shift 2 ;;
    --kubeconfig=*) KUBECONFIG_ARG="${1#--kubeconfig=}"; shift ;;
    *) echo "错误:未知选项 $1" >&2; usage >&2; exit 2 ;;
  esac
done

chaos_resolve_kubeconfig "$KUBECONFIG_ARG"

MARKER="chaos-stop-ordersrc-$$"

if [ "$DRY_RUN" = 1 ]; then
  chaos_note "[dry-run] 故障:stop_ordersrc(冻结 ordersrc 写端,ring 停摆)$([ "$RESUME" = 1 ] && echo ' [恢复模式]')"
  chaos_show_cmd -n "$NS" get pod "$POD" -o jsonpath='{.metadata.uid}{" "}{.status.phase}{"\n"}'
  chaos_note "[dry-run]   ↑ 训练 Pod 与 Pod UID(ring 文件名是 <pod-uid>.ring)"
  chaos_show_cmd -n "$NS" exec "\$POD" -c "$CONTAINER" -- sh -c \
    'ls -l /run/traincache/orders/*.ring 2>/dev/null; stat -c "%s %Y" /run/traincache/orders/*.ring 2>/dev/null'
  chaos_note "[dry-run]   ↑ 取 ring 文件当前 size/mtime(冻结前基线)"
  chaos_show_cmd -n "$NS" exec "\$POD" -c "$CONTAINER" -- sh -c \
    "for p in /proc/[0-9]*; do pid=\${p#/proc/}; [ \"\$pid\" = \"\$\$\" ] && continue; cmd=\$(tr '\\0' ' ' < /proc/\$pid/cmdline 2>/dev/null); case \"\$cmd\" in *chaos-stop-ordersrc*) continue;; esac; ppid=\$(awk '/^PPid/{print \$2}' /proc/\$pid/status 2>/dev/null); [ \"\$ppid\" = 1 ] && kill -STOP \"\$pid\" 2>/dev/null; done"
  chaos_note "[dry-run]   ↑ 冻结 PPid==1 的子进程(pid 1 本身忽略 SIGSTOP,不动它)"
  chaos_show_raw "sleep $OBSERVE   # 观察窗口:ordersrc 每 10s 写一次 ring"
  chaos_show_cmd -n "$NS" exec "\$POD" -c "$CONTAINER" -- sh -c \
    'stat -c "%s %Y" /run/traincache/orders/*.ring 2>/dev/null'
  chaos_note "[dry-run]   ↑ 再取一次 size/mtime:与基线相同 = ring 已停摆"
  chaos_show_cmd -n "$NS" exec "\$POD" -c "$CONTAINER" -- sh -c \
    "for p in /proc/[0-9]*; do pid=\${p#/proc/}; cmd=\$(tr '\\0' ' ' < /proc/\$pid/cmdline 2>/dev/null); case \"\$cmd\" in *chaos-stop-ordersrc*) continue;; esac; ppid=\$(awk '/^PPid/{print \$2}' /proc/\$pid/status 2>/dev/null); [ \"\$ppid\" = 1 ] && kill -CONT \"\$pid\" 2>/dev/null; done"
  chaos_note "[dry-run]   ↑ --resume:对同样一批进程发 SIGCONT"
  chaos_show_cmd -n "$NS" exec "\$AGENTD_POD" -c "$CHAOS_AGENTD_CONTAINER_DEFAULT" -- \
    wget -qO- "http://127.0.0.1:${CHAOS_METRICS_PORT_DEFAULT}/metrics"
  chaos_note "[dry-run]   ↑ 观察窗口内每 3s 取一次:traincache_agent_degraded / traincache_agent_ordersources{state=\"stalled\"}"
  exit 0
fi

chaos_require_kubeconfig
chaos_prepare_out_dir
ASSERT_FILE="$OUT_DIR/assertions.txt"
: >"$ASSERT_FILE"
pass() { printf 'PASS %s\n' "$*" | tee -a "$ASSERT_FILE"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$ASSERT_FILE" >&2; }

ctx="$(chaos_record_context)"
chaos_note "kubeconfig=$CHAOS_KUBECONFIG context=$ctx ns=$NS pod=$POD container=$CONTAINER"

chaos_kubectl -n "$NS" get pod "$POD" >/dev/null 2>&1 \
  || chaos_die "训练 Pod $NS/$POD 不存在(agent/deploy/train-pod.yaml)"

# ---- agentd 侧观测对象(降级判据在 agentd 的 /metrics 上)----
AGENTD_POD="$(chaos_agentd_pod_rows "$NS" | awk '$3=="True"{print $1; exit}')"
[ -n "$AGENTD_POD" ] || chaos_die "命名空间 $NS 里没有 Ready 的 agentd Pod(降级判据读不到)"
chaos_note "agentd 观测 Pod=$AGENTD_POD"
DEGRADED_BEFORE="$(chaos_metric "$NS" "$AGENTD_POD" traincache_agent_degraded)"
STALLED_BEFORE="$(chaos_metric_label_sum "$NS" "$AGENTD_POD" traincache_agent_ordersources state stalled)"
chaos_note "基线:degraded=${DEGRADED_BEFORE:-<取不到>} ordersources{state=stalled}=${STALLED_BEFORE:-0}"

# ring 文件路径:ordersrc 写 /run/traincache/orders/<pod-uid>.ring
# (agent/deploy/train-pod.yaml:12、agent/ordersrc/ordersrc.py:173)
ring_stat() {
  chaos_kubectl -n "$NS" exec "$POD" -c "$CONTAINER" -- sh -c \
    'for f in /run/traincache/orders/*.ring; do [ -e "$f" ] || continue; printf "%s %s %s\n" "$f" "$(stat -c %s "$f" 2>/dev/null)" "$(stat -c %Y "$f" 2>/dev/null)"; done'
}

# ---- 冻结/恢复子进程 ----
# 脚本正文在容器内跑,marker 出现在自己 cmdline 里 → 靠 marker 排除自己。
FREEZE_SCRIPT="for p in /proc/[0-9]*; do
  pid=\${p#/proc/}
  [ \"\$pid\" = \"\$\$\" ] && continue
  cmd=\$(tr '\\0' ' ' < /proc/\$pid/cmdline 2>/dev/null) || cmd=''
  case \"\$cmd\" in *${MARKER}*) continue ;; esac
  ppid=\$(awk '/^PPid/{print \$2}' /proc/\$pid/status 2>/dev/null)
  [ \"\$ppid\" = 1 ] || continue
  if kill -${SIGNAL} \"\$pid\" 2>/dev/null; then echo \"\$pid\";
  fi
done"

if [ "$RESUME" = 1 ]; then
  SIGNAL="CONT"
  OUT="$(chaos_kubectl -n "$NS" exec "$POD" -c "$CONTAINER" -- sh -c "$FREEZE_SCRIPT" 2>/dev/null || true)"
  PIDS="$(printf '%s\n' "$OUT" | grep -E '^[0-9]+$' | tr '\n' ' ')"
  if [ -n "$PIDS" ]; then
    pass "已对下列进程发 SIGCONT:$PIDS"
  else
    pass "没有需要恢复的进程(可能已被容器重启带走)"
  fi
  # 恢复后降级应回落:采样 traincache_agent_degraded(最多 40s)
  DEG_AFTER=""
  for _ in $(seq 1 14); do
    DEG_AFTER="$(chaos_metric "$NS" "$AGENTD_POD" traincache_agent_degraded)"
    [ "$DEG_AFTER" = "0" ] && break
    sleep 3
  done
  printf 'resumed_pids=%s\ndegraded_after_resume=%s\n' "${PIDS:-none}" "${DEG_AFTER:-}" >"$OUT_DIR/metrics.txt"
  if [ "$DEG_AFTER" = "0" ]; then
    pass "恢复后 traincache_agent_degraded 回落为 0"
  else
    fail "恢复后 traincache_agent_degraded=${DEG_AFTER:-<取不到>}(期望 0)"
  fi
  exit 0
fi

SIGNAL="STOP"
BEFORE="$(ring_stat || true)"
chaos_note "冻结前 ring 状态:${BEFORE:-<取不到>}"

OUT="$(chaos_kubectl -n "$NS" exec "$POD" -c "$CONTAINER" -- sh -c "$FREEZE_SCRIPT" 2>/dev/null || true)"
PIDS="$(printf '%s\n' "$OUT" | grep -E '^[0-9]+$' | tr '\n' ' ')"
CNT="$(printf '%s\n' "$PIDS" | grep -c '[0-9]' || true)"

if [ "${CNT:-0}" -gt 0 ]; then
  pass "已冻结 $CNT 个 PPid==1 的写端进程(pid: $PIDS)"
else
  fail "没有冻结到任何进程(ordersrc 侧可能已退出或形态变化)"
fi

# ---- 观察窗口:ring 是否还在推进 + agentd 降级判据 ----
MAX_DEGRADED=0
MAX_STALLED=0
DEGRADED_SAMPLES=""
STALLED_SAMPLES=""
END_TS=$(( $(date +%s) + OBSERVE ))
while [ "$(date +%s)" -lt "$END_TS" ]; do
  D="$(chaos_metric "$NS" "$AGENTD_POD" traincache_agent_degraded)"
  S="$(chaos_metric_label_sum "$NS" "$AGENTD_POD" traincache_agent_ordersources state stalled)"
  D="${D:-0}"; S="${S:-0}"
  DEGRADED_SAMPLES="$DEGRADED_SAMPLES $D"
  STALLED_SAMPLES="$STALLED_SAMPLES $S"
  MAX_DEGRADED="$(awk -v a="${D:-0}" -v m="${MAX_DEGRADED:-0}" 'BEGIN{print (a+0>m+0)?a:m}')"
  MAX_STALLED="$(awk -v a="${S:-0}" -v m="${MAX_STALLED:-0}" 'BEGIN{print (a+0>m+0)?a:m}')"
  sleep 3
done
AFTER="$(ring_stat || true)"
printf 'before=%s\nafter=%s\nobserve_s=%s\n' "${BEFORE:-}" "${AFTER:-}" "$OBSERVE" >"$OUT_DIR/ring_stat.txt"

if [ -z "$BEFORE" ] || [ -z "$AFTER" ]; then
  fail "取不到 ring 文件状态(容器里没有 /run/traincache/orders/*.ring)"
elif [ "$BEFORE" = "$AFTER" ]; then
  pass "观察窗口 ${OBSERVE}s 内 ring 未推进(size/mtime 不变):$AFTER"
else
  fail "ring 仍在推进(冻结没生效):前=$BEFORE 后=$AFTER"
fi

# ---- agentd 降级判据 ----
if [ "${MAX_STALLED:-0}" -ge 1 ] 2>/dev/null; then
  pass "agentd 侧 traincache_agent_ordersources{state=\"stalled\"} 观测到 ${MAX_STALLED}(≥1)"
else
  fail "agentd 侧没有 stalled 的订单源(观测 max=${MAX_STALLED:-0};写端停摆满 30s 才判)"
fi
if [ "${MAX_DEGRADED:-0}" -ge 1 ] 2>/dev/null; then
  pass "traincache_agent_degraded 由 ${DEGRADED_BEFORE:-?} 走到 1(观测 max=${MAX_DEGRADED})"
else
  fail "traincache_agent_degraded 未置 1(基线=${DEGRADED_BEFORE:-?} 观测 max=${MAX_DEGRADED:-0})"
fi

printf 'frozen_pids=%s\nfrozen_count=%s\nobserve_s=%s\nring_before=%s\nring_after=%s\n' \
  "${PIDS:-none}" "${CNT:-0}" "$OBSERVE" "${BEFORE:-}" "${AFTER:-}" >"$OUT_DIR/metrics.txt"
printf 'degraded_before=%s\ndegraded_max=%s\ndegraded_samples=%s\nstalled_before=%s\nstalled_max=%s\nstalled_samples=%s\nagentd_pod=%s\n' \
  "${DEGRADED_BEFORE:-}" "$MAX_DEGRADED" "${DEGRADED_SAMPLES# }" "${STALLED_BEFORE:-}" "$MAX_STALLED" \
  "${STALLED_SAMPLES# }" "$AGENTD_POD" >>"$OUT_DIR/metrics.txt"
chaos_note "冻结 pid:${PIDS:-none} 观察 ${OBSERVE}s 后 ring:${AFTER:-<取不到>}"
chaos_note "降级观测:degraded max=${MAX_DEGRADED} stalled max=${MAX_STALLED}(agentd=$AGENTD_POD)"
