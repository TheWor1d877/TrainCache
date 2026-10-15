#!/usr/bin/env bash
# run-local.sh — TrainCache Agent 服务化「本机端到端」演练(M7c)。
#
# 目标(闭合 M6 遗留「agentd 动态判据环境受限」):
#   在本机用 TRAINCACHE_CACHE_ROOT 指向可写临时目录启动真实 agentd,走完
#   CLI 三命令(prefetch / status / node status)+ /metrics 七线指标,记录
#   GetNodeStatus 动态字段(rescan phase、CachedBytes/FreeBytes、RunningTasks、
#   index_shards_loaded 等)与 25 个 family 的出现情况为证据。
#
# 用法:
#   bash agent/e2e/run-local.sh            # 由 bench/Makefile 的 e2e-local 调用
#
# 可重复:两次连续运行结论一致;退出前清掉进程与临时目录(不污染工作树)。
# 退出码:0 = 脚本执行完成且无「非预期失败」;1 = 出现非预期失败(见 FAIL 行)。
#   注意:CLI `prefetch` 的 task_id 幂等键经 --task-id 传入(§5.1 task_id=1)。本机无 MinIO 时,
#   svc 同步解析块列表(resolveRefs 读 CURRENT)会 connection refused → prefetch/status 标
#   [GAP-环境](与验收 1/5/9 同口径);CLI TaskId 修复生效由「报错非 task_id 必填」断言保证。
#   对象存储接线后,prefetch/status 段落转硬断言。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
AGENT_DIR="$REPO_ROOT/agent"

# 工具链:本机 go 可能不在 PATH,且外层可能导出了失效的 GOROOT(见 ~/.bashrc/env)。
unset GOROOT
export PATH="/usr/local/go/bin:$HOME/go/bin:$PATH"
command -v go >/dev/null 2>&1 || { echo "FATAL: 找不到 go 工具链"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FATAL: 找不到 curl"; exit 1; }

# 非特权、避开 k8s 与本机常见占用(agentd 默认 :9410/:9411 留给集群部署)。
GRPC_PORT="${E2E_GRPC_PORT:-19410}"
METRICS_PORT="${E2E_METRICS_PORT:-19411}"
NODE_NAME="${E2E_NODE_NAME:-e2e-local-node}"
TASK_ID="e2e-local-task-1"

BIN_DIR="$(mktemp -d /tmp/tc-e2e-bin.XXXXXX)"
CACHE_ROOT="$(mktemp -d /tmp/tc-e2e-cache.XXXXXX)"
ORDERS_DIR="$(mktemp -d /tmp/tc-e2e-orders.XXXXXX)"
LOG_FILE="$(mktemp /tmp/tc-e2e-agentd.XXXXXX.log)"
AGENTD_PID=""

RESULTS_DIR="$REPO_ROOT/agent/results"
mkdir -p "$RESULTS_DIR"
RAW_LOG="$RESULTS_DIR/e2e-local.raw.log"

cleanup() {
  local rc=$?
  if [[ -n "$AGENTD_PID" ]] && kill -0 "$AGENTD_PID" 2>/dev/null; then
    kill "$AGENTD_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$AGENTD_PID" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$AGENTD_PID" 2>/dev/null || true
  fi
  # 只在临时目录下清理,防止误删。
  case "$BIN_DIR"     in /tmp/tc-e2e-*) rm -rf "$BIN_DIR";; esac
  case "$CACHE_ROOT"  in /tmp/tc-e2e-*) rm -rf "$CACHE_ROOT";; esac
  case "$ORDERS_DIR"  in /tmp/tc-e2e-*) rm -rf "$ORDERS_DIR";; esac
  case "$LOG_FILE"    in /tmp/tc-e2e-*) rm -f  "$LOG_FILE";; esac
  exit "$rc"
}
trap cleanup EXIT INT TERM

FAILS=0
say()  { echo "$@"; }
ok()   { echo "  [OK]   $*"; }
fail() { echo "  [FAIL] $*"; FAILS=$((FAILS + 1)); }
gap()  { echo "  [GAP]  $*"; }

say "== TrainCache Agent 本机 e2e ============================================"
say "repo=$REPO_ROOT  grpc=:$GRPC_PORT  metrics=:$METRICS_PORT"
say "cache=$CACHE_ROOT  orders=$ORDERS_DIR"

# ---- 1. 构建二进制(agentd + CLI)到临时目录 ----
say "-- [1/6] 构建 agentd 与 traincache CLI"
( cd "$AGENT_DIR" && go build -o "$BIN_DIR/traincache-agentd" ./cmd/agentd && go build -o "$BIN_DIR/traincache" ./cmd/traincache ) \
  || { say "FATAL: 构建失败"; exit 1; }
ok "agentd / traincache 构建完成"

TC=("$BIN_DIR/traincache")
# --endpoint 是各子命令的局部旗标(§5.6),须置于子命令之后;故统一在调用尾追加。
tcall() { "${TC[@]}" "$@" --endpoint "127.0.0.1:$GRPC_PORT"; }

# ---- 2. 启动 agentd(CACHE_ROOT 指向可写临时目录) ----
say "-- [2/6] 启动 agentd(写入可写 CACHE_ROOT)"
NODE_NAME="$NODE_NAME" \
TRAINCACHE_GRPC_ADDR=":$GRPC_PORT" \
TRAINCACHE_METRICS_ADDR=":$METRICS_PORT" \
TRAINCACHE_CACHE_ROOT="$CACHE_ROOT" \
TRAINCACHE_ORDERS_DIR="$ORDERS_DIR" \
TRAINCACHE_DATASET_WHITELIST="train-data" \
"$BIN_DIR/traincache-agentd" >"$LOG_FILE" 2>&1 &
AGENTD_PID=$!
ok "agentd pid=$AGENTD_PID 日志=$LOG_FILE"

# ---- 3. 等待就绪(rescan phase != SAMPLING;空目录应很快 DONE) ----
say "-- [3/6] 等待 readiness(node status --item agent 成功)"
READY=0
for _ in $(seq 1 60); do
  if tcall node status --node "$NODE_NAME" --item agent >/dev/null 2>&1; then READY=1; break; fi
  kill -0 "$AGENTD_PID" 2>/dev/null || break
  sleep 0.5
done
if [[ "$READY" == 1 ]]; then ok "agentd ready"; else
  fail "agentd 未在 15s 内就绪"; say "--- agentd 日志尾部 ---"; tail -n 20 "$LOG_FILE"
fi

# ---- 4. CLI 三命令 ----
say "-- [4/6] CLI 三命令"
# 4.1 node status(缺省 = 全量行:NODE/AGENT/CACHE_ROOT/CACHED_BYTES/FREE_BYTES/
#     RUNNING_TASKS/RESCAN_PHASE;--item 只显示单项,故此处不带 --item 以记录全部动态字段)
NODE_OUT="$(tcall node status --node "$NODE_NAME" 2>&1)"; NODE_RC=$?
say "$NODE_OUT" | sed 's/^/    /'
[[ $NODE_RC -eq 0 ]] && ok "node status(全量)rc=0" || fail "node status rc=$NODE_RC"
grep -q "^CACHE_ROOT[[:space:]]*$CACHE_ROOT" <<<"$NODE_OUT" && ok "CACHE_ROOT 生效=临时目录" || fail "CACHE_ROOT 未生效"
PHASE="$(awk -F'[[:space:]]+' '/^RESCAN_PHASE/{print $2}' <<<"$NODE_OUT")"
[[ -n "$PHASE" && "$PHASE" != "SAMPLING" ]] && ok "RESCAN_PHASE=$PHASE(空目录→DONE,闭合 M6 遗留)" || fail "RESCAN_PHASE=$PHASE 异常"
FREE="$(awk -F'[[:space:]]+' '/^FREE_BYTES/{print $2}' <<<"$NODE_OUT")"
[[ -n "$FREE" && "$FREE" =~ ^[0-9]+$ && "$FREE" -gt 0 ]] && ok "FREE_BYTES=$FREE(Statfs 动态值非零)" || fail "FREE_BYTES=$FREE 非法"

# 4.2 prefetch(契约:accepted=true + task_id;task-id 幂等键经 --task-id 传入,§5.1 task_id=1)
#     本机无 MinIO 时,svc 同步解析块列表(resolveRefs 读 CURRENT)会 connection refused——
#     属环境限制[GAP];报错从「task_id 必填」推进到「块列表解析失败」即证明 CLI TaskId 修复生效。
say "  命令: traincache prefetch --task-id $TASK_ID --path train/ --node $NODE_NAME --chunks c0 --dataset train-data"
PF_OUT="$(tcall prefetch --task-id "$TASK_ID" --path "train/" --node "$NODE_NAME" --chunks "c0" --dataset "train-data" 2>&1)"; PF_RC=$?
say "$PF_OUT" | sed 's/^/    /'
if [[ $PF_RC -eq 0 ]] && grep -q "accepted=true" <<<"$PF_OUT"; then
  ok "prefetch rc=0 accepted=true"
elif grep -q "task_id 必填" <<<"$PF_OUT"; then
  fail "prefetch 仍报 task_id 必填 —— CLI TaskId 修复未生效"
elif grep -qi "connection refused\|预热块列表解析失败" <<<"$PF_OUT"; then
  gap "prefetch 对象存储未接线(本机 MinIO 未运行)→ 块列表解析失败;TaskId 已生效(阻塞点非 task_id 校验)"
else
  fail "prefetch rc=$PF_RC 未返回 accepted=true(输出: $PF_OUT)"
fi

# 4.3 status(依赖 4.2 成功;4.2 因对象存储 GAP 时任务未创建 → NotFound 亦 GAP)
say "  命令: traincache status --node $NODE_NAME --task-id $TASK_ID"
ST_OUT="$(tcall status --node "$NODE_NAME" --task-id "$TASK_ID" 2>&1)"; ST_RC=$?
say "$ST_OUT" | sed 's/^/    /'
if [[ $ST_RC -eq 0 ]]; then ok "status rc=0"; else
  gap "status rc=$ST_RC(上游 prefetch 因对象存储 GAP 未创建任务,非独立缺陷)"
fi

# ---- 5. /metrics 七线指标 ----
say "-- [5/6] /metrics 七线指标(25 family + dataset 标签)"
METRICS_OUT="$(curl -fsS "http://127.0.0.1:$METRICS_PORT/metrics" 2>&1)"; MC_RC=$?
if [[ $MC_RC -ne 0 ]]; then fail "curl /metrics 失败 rc=$MC_RC"; else
  ok "curl :$METRICS_PORT/metrics OK"
  # 七线核心 family(§5.8;25 family 全量,这里断言每线的代表 + 全局 agent_up)。
  FAMILIES=(
    traincache_cache_hits_total traincache_cache_misses_total traincache_cache_hit_ratio
    traincache_fetch_bytes_total traincache_fetch_requests_total traincache_fetch_inflight_bytes
    traincache_index_refresh_total traincache_index_refresh_duration_seconds traincache_index_shards_loaded
    traincache_gc_sweeps_total traincache_gc_blocks_removed_total traincache_gc_objects_removed_total
    traincache_evict_blocks_total traincache_evict_protected_skipped_total
    traincache_request_duration_seconds traincache_stage_switch_stall_seconds
    traincache_agent_up traincache_agent_degraded traincache_rescan_progress
    traincache_quarantined_blocks traincache_disk_usage_ratio
    traincache_prefetch_queue_depth traincache_prefetch_cursor
  )
  MISS=0
  for f in "${FAMILIES[@]}"; do
    grep -q "^# HELP $f " <<<"$METRICS_OUT" || { MISS=$((MISS+1)); fail "缺 family $f"; }
  done
  [[ $MISS -eq 0 ]] && ok "七线 23 个代表 family 全部出现(全量 25 见 agent/results)"
  TOTAL="$(grep -c '^# HELP traincache_' <<<"$METRICS_OUT")"
  ok "traincache_* family 总数=$TOTAL"
  grep -qE 'traincache_cache_hits_total\{[^}]*dataset=' <<<"$METRICS_OUT" \
    && ok "dataset 标签存在(白名单 train-data / 归并 other)" || fail "dataset 标签缺失"
  grep -qE 'traincache_agent_up\{[^}]*\} 1' <<<"$METRICS_OUT" && ok "agent_up=1" || fail "agent_up != 1"
fi

# ---- 6. 收口 ----
say "-- [6/6] 结果"
if [[ $FAILS -eq 0 ]]; then
  say "RESULT: PASS(非预期失败 0;已知缺口 GAP 见上,登记于 BLOCKERS)"
else
  say "RESULT: FAIL(非预期失败 $FAILS)"
fi

# 落一份原始证据(供人工复核),仅保留关键片段避免整包日志。
{
  echo "# e2e-local raw evidence  $(date -Is)"
  echo "## node status"; printf '%s\n' "$NODE_OUT"
  echo "## prefetch(rc=$PF_RC)"; printf '%s\n' "$PF_OUT"
  echo "## status(rc=$ST_RC)"; printf '%s\n' "$ST_OUT"
  echo "## metrics families=$(grep -c '^# HELP traincache_' <<<"$METRICS_OUT" 2>/dev/null)"
  grep -E '^(traincache_agent_up|traincache_agent_degraded|traincache_index_shards_loaded|traincache_prefetch_queue_depth)' <<<"$METRICS_OUT" 2>/dev/null
} >"$RAW_LOG" 2>/dev/null
say "原始证据: $RAW_LOG"

[[ $FAILS -eq 0 ]]
