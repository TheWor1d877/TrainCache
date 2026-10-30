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
# 前置:复用 kind 集群里已有的 MinIO(kubectl -n minio svc/minio)。脚本会临时
#   port-forward 到本机、建桶并发布一份 dataset=train-data 的种子快照;集群或 MinIO
#   不可用时直接 FAIL(先跑 controller/e2e/run.sh setup 或 make e2e),不退化为 GAP。
# 可重复:两次连续运行结论一致;退出前清掉 agentd、port-forward 与临时目录(不污染工作树)。
# 退出码:0 = 全部硬断言通过;1 = 任一失败或前置缺失(见 FAIL 行)。
#   4.2 prefetch 与 4.3 status 已转硬断言(对象存储接线后不再有 GAP 分支):
#   prefetch 必须 rc=0 且 accepted=true,status 必须 rc=0 且能查到任务。
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

# 对象存储复用 kind 集群里的 MinIO:凭证与 controller/e2e/run.sh 保持一致。
# 本地端口避开 controller/e2e 的 19000,避免两边 port-forward 抢端口。
S3_PORT="${E2E_S3_PORT:-19080}"
S3_ACCESS_KEY="traincache"
S3_SECRET_KEY="traincache-probe"
S3_BUCKET="traincache"
# trainpublish 的索引路径取入参文件的 basename,故用 train- 前缀让 --path 能命中。
SEED_NAME="train-shard-000.tar"
SEED_PREFIX="train"

BIN_DIR="$(mktemp -d /tmp/tc-e2e-bin.XXXXXX)"
CACHE_ROOT="$(mktemp -d /tmp/tc-e2e-cache.XXXXXX)"
ORDERS_DIR="$(mktemp -d /tmp/tc-e2e-orders.XXXXXX)"
SEED_DIR="$(mktemp -d /tmp/tc-e2e-seed.XXXXXX)"
PF_LOG="$(mktemp /tmp/tc-e2e-portforward.XXXXXX.log)"
LOG_FILE="$(mktemp /tmp/tc-e2e-agentd.XXXXXX.log)"
AGENTD_PID=""
PF_PID=""

# 产物落运行时数据层(repo 同级 traincachedata,与 bench/config.py 同一口径)。
DATA_ROOT="${TRAINCACHE_DATA_ROOT:-$(cd "$REPO_ROOT/.." && pwd)/traincachedata}"
RESULTS_DIR="$DATA_ROOT/agent/results"
mkdir -p "$RESULTS_DIR"
RAW_LOG="$RESULTS_DIR/e2e-local.raw.log"

cleanup() {
  local rc=$?
  if [[ -n "$AGENTD_PID" ]] && kill -0 "$AGENTD_PID" 2>/dev/null; then
    kill "$AGENTD_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$AGENTD_PID" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$AGENTD_PID" 2>/dev/null || true
  fi
  # 回收 port-forward,避免 kubectl 进程残留(先 TERM 再 KILL)。
  if [[ -n "$PF_PID" ]] && kill -0 "$PF_PID" 2>/dev/null; then
    kill "$PF_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$PF_PID" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$PF_PID" 2>/dev/null || true
  fi
  wait "$PF_PID" 2>/dev/null || true
  # 只在临时目录下清理,防止误删。
  case "$BIN_DIR"     in /tmp/tc-e2e-*) rm -rf "$BIN_DIR";; esac
  case "$CACHE_ROOT"  in /tmp/tc-e2e-*) rm -rf "$CACHE_ROOT";; esac
  case "$ORDERS_DIR"  in /tmp/tc-e2e-*) rm -rf "$ORDERS_DIR";; esac
  case "$SEED_DIR"    in /tmp/tc-e2e-*) rm -rf "$SEED_DIR";; esac
  case "$PF_LOG"      in /tmp/tc-e2e-*) rm -f  "$PF_LOG";; esac
  case "$LOG_FILE"    in /tmp/tc-e2e-*) rm -f  "$LOG_FILE";; esac
  exit "$rc"
}
trap cleanup EXIT INT TERM

FAILS=0
say()  { echo "$@"; }
ok()   { echo "  [OK]   $*"; }
fail() { echo "  [FAIL] $*"; FAILS=$((FAILS + 1)); }

say "== TrainCache Agent 本机 e2e ============================================"
say "repo=$REPO_ROOT  grpc=:$GRPC_PORT  metrics=:$METRICS_PORT"
say "cache=$CACHE_ROOT  orders=$ORDERS_DIR"

# ---- 1. 构建二进制(agentd + CLI)到临时目录 ----
say "-- [1/7] 构建 agentd 与 traincache CLI"
( cd "$AGENT_DIR" && go build -o "$BIN_DIR/traincache-agentd" ./cmd/agentd && go build -o "$BIN_DIR/traincache" ./cmd/traincache ) \
  || { say "FATAL: 构建失败"; exit 1; }
ok "agentd / traincache 构建完成"

TC=("$BIN_DIR/traincache")
# --endpoint 是各子命令的局部旗标(§5.6),须置于子命令之后;故统一在调用尾追加。
tcall() { "${TC[@]}" "$@" --endpoint "127.0.0.1:$GRPC_PORT"; }

# ---- 2. 准备对象存储(复用 kind 集群里已有的 MinIO) ----
# 4.2/4.3 要真正走通"读 CURRENT + 解析块列表",必须有可用的对象存储。集群/服务
# 缺失属于前置不满足,直接 FAIL 并给出可执行提示,不走 GAP 软断言。
say "-- [2/7] 准备对象存储(kind 集群 MinIO)"
command -v kubectl >/dev/null 2>&1 \
  || { fail "找不到 kubectl:对象存储复用 kind 集群的 MinIO"; say "  先准备集群: (cd \"$REPO_ROOT/controller\" && ./e2e/run.sh setup) 或 make e2e"; exit 1; }
if ! kubectl -n minio get svc/minio >/dev/null 2>&1; then
  fail "minio 命名空间没有可用的 svc/minio"
  say "  先准备集群与 MinIO: (cd \"$REPO_ROOT/controller\" && ./e2e/run.sh setup) 或 make e2e"
  exit 1
fi
ok "svc/minio 存在"

# 打通到本机回环端口;cleanup 负责回收,失败路径也不会留下 kubectl 进程。
kubectl -n minio port-forward svc/minio "$S3_PORT:9000" >"$PF_LOG" 2>&1 &
PF_PID=$!
S3_READY=0
for _ in $(seq 1 40); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$S3_PORT") 2>/dev/null; then exec 3>&- 3<&-; S3_READY=1; break; fi
  kill -0 "$PF_PID" 2>/dev/null || break
  sleep 0.5
done
if [[ "$S3_READY" != "1" ]] || ! kill -0 "$PF_PID" 2>/dev/null; then
  fail "MinIO port-forward 未就绪(127.0.0.1:$S3_PORT)"
  tail -n 10 "$PF_LOG" 2>/dev/null | sed 's/^/    /'
  exit 1
fi
ok "port-forward 127.0.0.1:$S3_PORT -> svc/minio:9000"

# 建桶:复用 controller/e2e/mkbucket,凭证与 controller/e2e/run.sh 一致;已有桶幂等。
if ! (cd "$REPO_ROOT/controller" && go run ./e2e/mkbucket \
    --endpoint "127.0.0.1:$S3_PORT" --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" --bucket "$S3_BUCKET") >"$BIN_DIR/mkbucket.log" 2>&1; then
  fail "建桶 $S3_BUCKET 失败"
  tail -n 10 "$BIN_DIR/mkbucket.log" | sed 's/^/    /'
  exit 1
fi
ok "bucket $S3_BUCKET 就绪"

# 播种快照:trainpublish 把每个入参文件当作一个"分片",索引路径 = filepath.Base(文件名),
# 块号 = 按桶声明的 chunkSize 从 0 顺序切分(故首块恒为 0,与 tar 内部条目无关——它只
# 按字节切块,不解析 tar 目录)。因此把随机数据命名为 train-shard-000.tar:
#   --path train                       前缀命中 "train-shard-000.tar";
#   --chunks train-shard-000.tar:0     精确命中首块(与前缀求交,非空)。
# 不传 --chunk-size:沿用桶里已有的块粒度(空桶则写默认值),重复执行不会因大小不一致失败。
# 内容用全零而非随机:同一 (dataset,path,chunk) 键重复发布时覆盖,不随执行次数增长存储。
# 旧稿的 "--chunks c0" 不符合 CLI 的 path:chunk 语法,且 basename 不含 "train/",无法让
# "train/" 前缀命中;这里按实现把路径前缀改成 "train"。
dd if=/dev/zero of="$SEED_DIR/$SEED_NAME" bs=1M count=2 status=none
PUB_OUT="$(cd "$REPO_ROOT" && go run ./core/cmd/trainpublish \
  --dataset train-data --endpoint "127.0.0.1:$S3_PORT" \
  --access-key "$S3_ACCESS_KEY" --secret-key "$S3_SECRET_KEY" --bucket "$S3_BUCKET" \
  "$SEED_DIR/$SEED_NAME" 2>&1)"; PUB_RC=$?
say "$PUB_OUT" | sed 's/^/    /'
if [[ $PUB_RC -eq 0 ]]; then ok "种子快照已发布(dataset=train-data)"; else
  fail "trainpublish rc=$PUB_RC"; exit 1
fi

# ---- 3. 启动 agentd(CACHE_ROOT 指向可写临时目录,并接上对象存储) ----
say "-- [3/7] 启动 agentd(写入可写 CACHE_ROOT)"
NODE_NAME="$NODE_NAME" \
TRAINCACHE_GRPC_ADDR=":$GRPC_PORT" \
TRAINCACHE_METRICS_ADDR=":$METRICS_PORT" \
TRAINCACHE_CACHE_ROOT="$CACHE_ROOT" \
TRAINCACHE_ORDERS_DIR="$ORDERS_DIR" \
TRAINCACHE_OBJSTORE_ENDPOINT="127.0.0.1:$S3_PORT" \
TRAINCACHE_OBJSTORE_ACCESS_KEY="$S3_ACCESS_KEY" \
TRAINCACHE_OBJSTORE_SECRET_KEY="$S3_SECRET_KEY" \
TRAINCACHE_OBJSTORE_BUCKET="$S3_BUCKET" \
TRAINCACHE_DATASET_WHITELIST="train-data" \
"$BIN_DIR/traincache-agentd" >"$LOG_FILE" 2>&1 &
AGENTD_PID=$!
ok "agentd pid=$AGENTD_PID 日志=$LOG_FILE"

# ---- 4. 等待就绪(rescan phase != SAMPLING;空目录应很快 DONE) ----
say "-- [4/7] 等待 readiness(node status --item agent 成功)"
READY=0
for _ in $(seq 1 60); do
  if tcall node status --node "$NODE_NAME" --item agent >/dev/null 2>&1; then READY=1; break; fi
  kill -0 "$AGENTD_PID" 2>/dev/null || break
  sleep 0.5
done
if [[ "$READY" == 1 ]]; then ok "agentd ready"; else
  fail "agentd 未在 15s 内就绪"; say "--- agentd 日志尾部 ---"; tail -n 20 "$LOG_FILE"
fi

# ---- 5. CLI 三命令 ----
say "-- [5/7] CLI 三命令"
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

# 4.2 prefetch(硬断言:rc=0 且 accepted=true;task_id 幂等键经 --task-id 传入,§5.1)
#     svc 会同步读 CURRENT 并按 --path 前缀与 --chunks 精确块求交;种子快照已在步骤 2
#     发布,故这里的块必须真实命中(而不是空任务也返回 accepted)。
say "  命令: traincache prefetch --task-id $TASK_ID --path $SEED_PREFIX --node $NODE_NAME --chunks $SEED_NAME:0 --dataset train-data"
PF_OUT="$(tcall prefetch --task-id "$TASK_ID" --path "$SEED_PREFIX" --node "$NODE_NAME" --chunks "$SEED_NAME:0" --dataset "train-data" 2>&1)"; PF_RC=$?
say "$PF_OUT" | sed 's/^/    /'
if [[ $PF_RC -eq 0 ]] && grep -q "accepted=true" <<<"$PF_OUT"; then
  ok "prefetch rc=0 accepted=true(块列表解析成功)"
else
  fail "prefetch rc=$PF_RC 未返回 accepted=true(输出: $PF_OUT)"
fi

# 4.3 status(硬断言:rc=0 且能查到本任务;状态允许 QUEUED/RUNNING/DONE,不在本步约束)
say "  命令: traincache status --node $NODE_NAME --task-id $TASK_ID"
ST_OUT="$(tcall status --node "$NODE_NAME" --task-id "$TASK_ID" 2>&1)"; ST_RC=$?
say "$ST_OUT" | sed 's/^/    /'
if [[ $ST_RC -eq 0 ]] && grep -qE "^TASK_ID[[:space:]]+$TASK_ID$" <<<"$ST_OUT"; then
  ok "status rc=0 且查到任务 $TASK_ID"
else
  fail "status rc=$ST_RC 未查到任务 $TASK_ID(输出: $ST_OUT)"
fi

# ---- 6. /metrics 七线指标 ----
say "-- [6/7] /metrics 七线指标(25 family + dataset 标签)"
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
    traincache_request_duration_seconds traincache_stage_switch_eta_seconds
    traincache_agent_up traincache_agent_degraded traincache_rescan_progress
    traincache_quarantined_blocks traincache_disk_usage_ratio
    traincache_prefetch_queue_depth traincache_prefetch_cursor
  )
  MISS=0
  for f in "${FAMILIES[@]}"; do
    grep -q "^# HELP $f " <<<"$METRICS_OUT" || { MISS=$((MISS+1)); fail "缺 family $f"; }
  done
  [[ $MISS -eq 0 ]] && ok "七线 23 个代表 family 全部出现(全量 25 见 $RESULTS_DIR)"
  TOTAL="$(grep -c '^# HELP traincache_' <<<"$METRICS_OUT")"
  ok "traincache_* family 总数=$TOTAL"
  grep -qE 'traincache_cache_hits_total\{[^}]*dataset=' <<<"$METRICS_OUT" \
    && ok "dataset 标签存在(白名单 train-data / 归并 other)" || fail "dataset 标签缺失"
  grep -qE 'traincache_agent_up\{[^}]*\} 1' <<<"$METRICS_OUT" && ok "agent_up=1" || fail "agent_up != 1"
fi

# ---- 7. 收口 ----
say "-- [7/7] 结果"
if [[ $FAILS -eq 0 ]]; then
  say "RESULT: PASS(全部硬断言通过,失败 0)"
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
