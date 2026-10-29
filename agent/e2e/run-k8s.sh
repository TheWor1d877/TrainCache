#!/usr/bin/env bash
# run-k8s.sh — TrainCache Agent 服务化「kind 集群端到端」演练(M7c)。
#
# 前置:复用 M7b 已部署的 kind 集群(traincache),镜像 traincache-agentd:dev /
#       traincache-ordersrc:dev / busybox:latest 已 kind load(见 agent/deploy/*.yaml 头注释)。
#       脚本先 kubectl apply 两个清单(幂等:已存在则不改),再逐条演练验收。
# 覆盖验收:2 / 3 / 8 / 10(1/5/9 需 MinIO 对象存储 + 测试数据,见 DECISIONS 与回传)。
# 用法:
#   bash agent/e2e/run-k8s.sh        # 由 bench/Makefile 的 e2e-k8s 调用
# 退出码:0 = 全部可观察现象成立;1 = 出现 FAIL(逐条打印)。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NS=traincache
DS=traincache-agentd
TRAIN_POD=traincache-train
AGENTD_LABEL="app.kubernetes.io/name=traincache-agentd"
STALL_WAIT=50      # 降级阈值 30s + 裕量
RECOVER_WAIT=40

command -v kubectl >/dev/null 2>&1 || { echo "FATAL: 找不到 kubectl"; exit 1; }

FAILS=0
say()  { echo "$@"; }
ok()   { echo "  [OK]   $*"; }
fail() { echo "  [FAIL] $*"; FAILS=$((FAILS + 1)); }
note() { echo "  [NOTE] $*"; }

# 产物落运行时数据层(repo 同级 traincachedata,与 bench/config.py 同一口径)。
DATA_ROOT="${TRAINCACHE_DATA_ROOT:-$(cd "$REPO_ROOT/.." && pwd)/traincachedata}"
RESULTS_DIR="$DATA_ROOT/agent/results"
mkdir -p "$RESULTS_DIR"
RAW_LOG="$RESULTS_DIR/e2e-k8s.raw.log"
: >"$RAW_LOG"
rec() { echo "$@" >>"$RAW_LOG"; }

say "== TrainCache Agent k8s e2e(kind / ns=$NS)=="

# ---- 0. 前置:集群可达 ----
say "-- [0/7] 前置检查"
kubectl cluster-info >/dev/null 2>&1 || { say "FATAL: 集群不可达(kubectl cluster-info 失败)"; exit 1; }
kubectl get nodes >/dev/null 2>&1 || { say "FATAL: kubectl get nodes 失败"; exit 1; }
ok "集群可达"

# ---- 1. 幂等部署 ----
say "-- [1/7] 幂等 apply 部署清单"
kubectl apply -f "$REPO_ROOT/controller/deploy/namespace.yaml" >>"$RAW_LOG" 2>&1 || fail "apply namespace.yaml 失败"
kubectl apply -f "$REPO_ROOT/controller/deploy/agent/daemonset.yaml" >>"$RAW_LOG" 2>&1 || fail "apply agent daemonset 失败"
# 演练用本地构建的镜像：清单里写的是推送后的 ghcr 引用，这里替换成 kind load 的名字。
kubectl -n "$NS" set image ds/"$DS" agentd=traincache-agentd:dev >>"$RAW_LOG" 2>&1 || fail "set image 失败"
kubectl apply -f "$REPO_ROOT/agent/deploy/train-pod.yaml" >>"$RAW_LOG" 2>&1 || fail "apply train-pod.yaml 失败"
kubectl -n "$NS" rollout status ds/"$DS" --timeout=120s >>"$RAW_LOG" 2>&1 && ok "DaemonSet $DS ready" || fail "DaemonSet 未在 120s 内 ready"
kubectl -n "$NS" wait --for=condition=Ready pod/"$TRAIN_POD" --timeout=120s >>"$RAW_LOG" 2>&1 && ok "Pod $TRAIN_POD ready" || fail "train pod 未 ready"

# 训练 Pod 所在节点 = 待演练 Agent 节点(同节点共享 hostPath)。
TRAIN_NODE="$(kubectl -n "$NS" get pod "$TRAIN_POD" -o jsonpath='{.spec.nodeName}')"
[[ -n "$TRAIN_NODE" ]] && ok "train pod 节点=$TRAIN_NODE" || { fail "无法取得 train 节点"; exit 1; }

# 重新解析同节点的 agentd Pod(删除重建后 Pod 名会变,故每次现取)。
agentd_pod() {
  kubectl -n "$NS" get pods -l "$AGENTD_LABEL" \
    -o jsonpath="{range .items[?(@.spec.nodeName=='$TRAIN_NODE')]}{.metadata.name}{'\n'}{end}" | head -n1
}
AGENTD_POD="$(agentd_pod)"
[[ -n "$AGENTD_POD" ]] && ok "同节点 agentd pod=$AGENTD_POD" || { fail "找不到同节点 agentd pod"; exit 1; }

# 在 agentd 容器内取指标(alpine 自带 wget),避免宿主机 port-forward 生命周期问题。
metrics() { kubectl -n "$NS" exec "$AGENTD_POD" -- wget -qO- http://127.0.0.1:9411/metrics 2>/dev/null; }
# metric_val <family> → 取无标签或首条样本的值。
metric_val() { metrics | awk -v f="$1" '$0 ~ "^"f"\\{" {print $2; exit}'; }
agentd_exec() { kubectl -n "$NS" exec "$AGENTD_POD" -- sh -c "$1"; }
trainer_exec() { kubectl -n "$NS" exec "$TRAIN_POD" -c trainer -- sh -c "$1"; }

# ---- 2. 验收 2:删除 Agent Pod → 重建 → 索引/缓存自恢复 ----
say "-- [2/7] 验收 2:Agent Pod 删除重建,缓存持久 + 索引恢复"
MARK="/var/cache/traincache/.e2e-marker"
STAMP="e2e-$(date +%s)"
agentd_exec "echo '$STAMP' > $MARK && cat $MARK" >/dev/null 2>&1 && ok "写入 hostPath 标记($STAMP)" || fail "写入标记失败"
CB_BEFORE="$(kubectl -n "$NS" exec "$AGENTD_POD" -- traincache node status --node "$TRAIN_NODE" --item space 2>/dev/null | awk '/^CACHED_BYTES/{print $2}')"
note "删除前 CACHED_BYTES=${CB_BEFORE:-?}"

kubectl -n "$NS" delete pod "$AGENTD_POD" --wait=true >>"$RAW_LOG" 2>&1 && ok "已删除 agentd pod $AGENTD_POD" || fail "删除 agentd pod 失败"
NEW_POD=""
for _ in $(seq 1 60); do
  NEW_POD="$(agentd_pod)"
  if [[ -n "$NEW_POD" && "$NEW_POD" != "$AGENTD_POD" ]]; then
    if kubectl -n "$NS" wait --for=condition=Ready pod/"$NEW_POD" --timeout=5s >/dev/null 2>&1; then break; fi
  fi
  sleep 1
done
if [[ -n "$NEW_POD" && "$NEW_POD" != "$AGENTD_POD" ]]; then
  AGENTD_POD="$NEW_POD"; ok "重建 agentd pod=$AGENTD_POD 且 ready"
else
  fail "60s 内未见重建的 ready agentd pod"; AGENTD_POD="$(agentd_pod)"
fi
# 缓存持久:标记文件仍在(hostPath 跨 Pod 生命周期)。
if agentd_exec "cat $MARK" 2>/dev/null | grep -q "$STAMP"; then ok "hostPath 缓存持久:标记仍在"; else fail "标记丢失(缓存未持久)"; fi
# 索引恢复:新 Pod rescan != SAMPLING(node status 成功)+ agent_up=1。
NEW_RESCAN="$(kubectl -n "$NS" exec "$AGENTD_POD" -- traincache node status --node "$TRAIN_NODE" --item rescan 2>/dev/null | awk '/^RESCAN_PHASE/{print $2}')"
[[ -n "$NEW_RESCAN" && "$NEW_RESCAN" != "SAMPLING" ]] && ok "索引恢复:RESCAN_PHASE=$NEW_RESCAN" || fail "RESCAN_PHASE=$NEW_RESCAN 异常"
[[ "$(metric_val traincache_agent_up)" == "1" ]] && ok "重建后 agent_up=1" || fail "重建后 agent_up != 1"
agentd_exec "rm -f $MARK" >/dev/null 2>&1

# ---- 3. 验收 3:trainer 直读 hostPath blocks —— 读通/写拒 ----
say "-- [3/7] 验收 3:训练容器直读 blocks(只读 hostPath)"
PROBE="/var/cache/traincache/blocks/.e2e-probe"
agentd_exec "mkdir -p /var/cache/traincache/blocks && echo e2e-block-data > $PROBE" >/dev/null 2>&1 \
  && ok "agentd 侧写入探针块 $PROBE" || fail "写入探针块失败"
READ_OUT="$(trainer_exec "cat $PROBE" 2>&1)"
grep -q "e2e-block-data" <<<"$READ_OUT" && ok "trainer 直读成功:$READ_OUT" || fail "trainer 直读失败:$READ_OUT"
WRITE_OUT="$(trainer_exec "touch /var/cache/traincache/blocks/.e2e-write" 2>&1)"; WRITE_RC=$?
if [[ $WRITE_RC -ne 0 ]] || grep -qi "read-only\|只读" <<<"$WRITE_OUT"; then
  ok "trainer 写入被拒(只读):${WRITE_OUT:-rc=$WRITE_RC}"
else
  fail "trainer 写入未被拒(预期只读):$WRITE_OUT"
fi
agentd_exec "rm -f $PROBE /var/cache/traincache/blocks/.e2e-write" >/dev/null 2>&1

# ---- 4. 验收 8:ordersrc 写 ring → agentd 收到 ConsumeOrder ----
say "-- [4/7] 验收 8:ordersrc sidecar → agentd 收到 ConsumeOrder"
GOT_ORDER=0
for _ in $(seq 1 30); do
  if kubectl -n "$NS" logs "$AGENTD_POD" --since=5m 2>/dev/null | grep -q "收到 ConsumeOrder"; then GOT_ORDER=1; break; fi
  sleep 2
done
if [[ $GOT_ORDER -eq 1 ]]; then
  ok "agentd 日志出现「收到 ConsumeOrder」"
  kubectl -n "$NS" logs "$AGENTD_POD" --since=5m 2>/dev/null | grep "收到 ConsumeOrder" | tail -n 1 | sed 's/^/    /' | tee -a "$RAW_LOG" >/dev/null
else
  fail "60s 内未见 ConsumeOrder(检查 sidecar 与 ring 目录)"
fi
# 说明:无对象存储时,紧随其后的「orders: 预取接线失败」为预期(下载不透传,非本验收判据)。
kubectl -n "$NS" logs "$AGENTD_POD" --since=5m 2>/dev/null | grep -q "预取接线失败" \
  && note "另见「预取接线失败」= 对象存储未接(预期;验收 8 只判接收链路)"

# ---- 5. 验收 10:sidecar 停写 → 30s 内 degraded=1 → 恢复 ----
say "-- [5/7] 验收 10:sidecar 停写 30s 内 agent_degraded=1,恢复后回 0"
# 注意:PID 命名空间内的 pid1 会忽略 SIGSTOP(内核不对 pid1 施加默认动作),故
# `kill -STOP 1` 是无操作;改为冻结循环的当前子进程(PPid==1 的 python3/sleep),
# 令 sh 阻塞在 wait,写端停摆 → ring 停滞 → 聚合降级。排除自身 shell($$)。
sidecar_pause() {
  local out=""
  for _ in 1 2 3; do
    out="$(kubectl -n "$NS" exec "$TRAIN_POD" -c ordersrc -- sh -c '
      n=0
      for p in /proc/[0-9]*; do
        pid=${p#/proc/}; [ "$pid" = "$$" ] && continue
        ppid=$(awk "/^PPid/{print \$2}" /proc/$pid/status 2>/dev/null)
        [ "$ppid" = 1 ] && { kill -STOP "$pid" 2>/dev/null && n=$((n+1)); }
      done
      echo "stopped=$n"' 2>/dev/null)"
    case "$out" in *"stopped="[1-9]*) ok "已冻结 sidecar 写端($out)"; return 0;; esac
    sleep 2
  done
  fail "未能冻结 sidecar 写端($out)"; return 1
}
sidecar_resume() {
  kubectl -n "$NS" exec "$TRAIN_POD" -c ordersrc -- sh -c '
    for p in /proc/[0-9]*; do
      pid=${p#/proc/}; [ "$pid" = "$$" ] && continue
      ppid=$(awk "/^PPid/{print \$2}" /proc/$pid/status 2>/dev/null)
      [ "$ppid" = 1 ] && kill -CONT "$pid" 2>/dev/null
    done
    exit 0' >/dev/null 2>&1
}

DEG0="$(metric_val traincache_agent_degraded)"; note "停写前 agent_degraded=${DEG0:-?}"
sidecar_pause
DEG1=""
for i in $(seq 1 "$STALL_WAIT"); do
  DEG1="$(metric_val traincache_agent_degraded)"
  [[ "$DEG1" == "1" ]] && { ok "停写后 ${i}s 内 agent_degraded=1"; break; }
  sleep 1
done
[[ "$DEG1" == "1" ]] || fail "sidecar 停写后 ${STALL_WAIT}s 内未进入降级(agent_degraded=$DEG1)"

sidecar_resume && ok "已恢复 ordersrc 写端(CONT)"
DEG2=""
for i in $(seq 1 "$RECOVER_WAIT"); do
  DEG2="$(metric_val traincache_agent_degraded)"
  [[ "$DEG2" == "0" ]] && { ok "恢复后 ${i}s 内 agent_degraded=0"; break; }
  sleep 1
done
[[ "$DEG2" == "0" ]] || fail "恢复后 ${RECOVER_WAIT}s 内未退出降级(agent_degraded=$DEG2)"

# ---- 6. 七线指标抽样(佐证) ----
say "-- [6/7] 七线指标抽样(同集群 agentd /metrics)"
M="$(metrics)"
TOTAL="$(grep -c '^# HELP traincache_' <<<"$M")"
[[ "$TOTAL" -ge 25 ]] && ok "traincache_* family 总数=${TOTAL}(>=25)" || fail "family 总数=${TOTAL}(<25)"
for f in traincache_agent_up traincache_agent_degraded traincache_index_shards_loaded \
         traincache_cache_hits_total traincache_fetch_bytes_total \
         traincache_gc_sweeps_total traincache_evict_blocks_total \
         traincache_request_duration_seconds traincache_rescan_progress; do
  grep -q "^# HELP $f " <<<"$M" && ok "$f" || fail "缺 family $f"
done

# ---- 7. 收口 ----
say "-- [7/7] 结果"
if [[ $FAILS -eq 0 ]]; then say "RESULT: PASS(验收 2/3/8/10 可观察现象全部成立)"; else say "RESULT: FAIL($FAILS 项)"; fi
say "原始证据: $RAW_LOG"
say "注:验收 1/5/9 需对象存储(MinIO)+ 测试数据,未在本脚本覆盖,见回传 DECISIONS。"

[[ $FAILS -eq 0 ]]
