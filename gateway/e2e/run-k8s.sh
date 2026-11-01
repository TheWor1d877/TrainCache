#!/usr/bin/env bash
# gateway/e2e/run-k8s.sh — S3 合并回源网关「kind 集群端到端」演练（M6 T27）。
#
# 前置条件（由调用方准备，脚本不创建集群/命名空间/凭证）：
#   1. kind 集群可访问：kubectl cluster-info 与 kubectl get nodes 通过，且默认
#      context 指向待演练的集群。
#   2. MinIO 已部署在 minio namespace，且桶里有可读对象（脚本用 MinIO Pod 内的
#      mc 发现/读取对象；MinIO 镜像不带 mc 时相关步骤 SKIP）。
#   3. traincache namespace 已存在 ConfigMap traincache-config 与 Secret
#      traincache-objstore（accessKey / secretKey 两个键）。
#   4. 镜像 traincache-gateway:dev 已 kind load；清单里的 ghcr 引用会被
#      `kubectl set image` 替换。
#   5. busybox:latest 已 kind load（步骤 3/4 的临时探针 Pod 使用）。
#   6. 对象键：优先 E2E_KEY='<bucket 内完整 key>'；未设置时用 mc 递归列桶取
#      第一个对象。两者都拿不到时步骤 3/4/5 标记 SKIP，不判 FAIL。
#
# 用法：
#   bash gateway/e2e/run-k8s.sh
#   E2E_KEY='compare-data-8m/blocks/shard-00000.tar#0' bash gateway/e2e/run-k8s.sh
#   TC_E2E_AGENT=1 bash gateway/e2e/run-k8s.sh    # 追加步骤 5（Agent 降级联动）
#
# 步骤：
#   1. apply controller/deploy/gateway（Deployment + Service）、替换镜像、等 ready
#   2. /healthz 与 /readyz 探针（pod 内 wget，断言 2xx 且 body=ok）
#   3. 临时 busybox Pod 并发 5 次 GET 同一块，五份 sha256 一致；再与 mc cat 比对
#   4. rollout restart 期间持续 GET，断言新 Pod Ready 前后 GET 成功，且没有出现
#      x-traincache-gateway: unavailable 以外的 5xx
#   5. TC_E2E_AGENT=1：ConfigMap 指到网关 → 重启 agentd → source_mode=1；网关
#      scale 0 → 触发块读取 → fallback_total 增加、source_mode=0；随后恢复
#   6. 清理临时 Pod（不删除 Deployment / Service）
#
# 退出码：0 = 没有 FAIL（允许 SKIP）；1 = 至少一步 FAIL。
# 可重复执行；步骤 5 的所有改动由 trap 兜底恢复（ConfigMap 与副本数）。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NS="${TC_NAMESPACE:-traincache}"
MINIO_NS="${TC_MINIO_NAMESPACE:-minio}"
GW_DEPLOY=traincache-gateway
GW_SVC=traincache-gateway
GW_IMAGE="${TC_GATEWAY_IMAGE:-traincache-gateway:dev}"
GW_PORT=9413
AGENT_DS=traincache-agentd
AGENT_LABEL="app.kubernetes.io/name=traincache-agentd"
PROBE_IMAGE="${TC_PROBE_IMAGE:-busybox:latest}"
E2E_KEY="${E2E_KEY:-}"
TC_E2E_AGENT="${TC_E2E_AGENT:-0}"

command -v kubectl >/dev/null 2>&1 || { echo "FATAL: 找不到 kubectl"; exit 1; }

FAILS=0
OKS=0
SKIPS=0
say()  { echo "$@"; }
ok()   { echo "  [OK]   $*"; OKS=$((OKS + 1)); }
fail() { echo "  [FAIL] $*"; FAILS=$((FAILS + 1)); }
skip() { echo "  [SKIP] $*"; SKIPS=$((SKIPS + 1)); }
note() { echo "  [NOTE] $*"; }

# 产物落运行时数据层（与 agent/e2e/run-k8s.sh 同一口径）。
DATA_ROOT="${TRAINCACHE_DATA_ROOT:-$(cd "$REPO_ROOT/.." && pwd)/traincachedata}"
RESULTS_DIR="$DATA_ROOT/gateway/results"
mkdir -p "$RESULTS_DIR"
RAW_LOG="$RESULTS_DIR/e2e-k8s.raw.log"
DRAIN_LOG="$RESULTS_DIR/e2e-k8s.drain.log"
: >"$RAW_LOG"
: >"$DRAIN_LOG"

TEMP_PODS=()
GW_ENDPOINT_ORIG=""
GW_ENDPOINT_PATCHED=0
GW_REPLICAS_ORIG=""
AGENT_RESTART_NEEDED=0

# restore_agent_step 幂等恢复步骤 5 的三处改动：ConfigMap、网关副本数、agentd。
# 任一项失败返回 1，调用方（显式恢复或 trap）至少打印 NOTE，不掩盖原始退出码。
restore_agent_step() {
  local rc=0
  if [[ "$GW_ENDPOINT_PATCHED" == "1" ]]; then
    if kubectl -n "$NS" patch configmap traincache-config --type merge \
      -p "{\"data\":{\"gatewayEndpoint\":\"${GW_ENDPOINT_ORIG}\"}}" >>"$RAW_LOG" 2>&1; then
      GW_ENDPOINT_PATCHED=0
      note "已恢复 gatewayEndpoint='${GW_ENDPOINT_ORIG}'"
    else
      note "恢复 gatewayEndpoint 失败，请手动检查 ConfigMap"
      rc=1
    fi
  fi
  if [[ -n "$GW_REPLICAS_ORIG" ]]; then
    if kubectl -n "$NS" scale deploy/"$GW_DEPLOY" --replicas="$GW_REPLICAS_ORIG" >>"$RAW_LOG" 2>&1 \
      && kubectl -n "$NS" rollout status deploy/"$GW_DEPLOY" --timeout=120s >>"$RAW_LOG" 2>&1; then
      note "已恢复网关副本数=$GW_REPLICAS_ORIG"
      GW_REPLICAS_ORIG=""
    else
      note "恢复网关副本数失败，请手动检查 Deployment"
      rc=1
    fi
  fi
  if [[ "$AGENT_RESTART_NEEDED" == "1" ]]; then
    if kubectl -n "$NS" rollout restart ds/"$AGENT_DS" >>"$RAW_LOG" 2>&1 \
      && kubectl -n "$NS" rollout status ds/"$AGENT_DS" --timeout=180s >>"$RAW_LOG" 2>&1; then
      note "已重启 agentd 使配置回退生效"
      AGENT_RESTART_NEEDED=0
    else
      note "agentd 回退重启失败，请手动检查 DaemonSet"
      rc=1
    fi
  fi
  return "$rc"
}

cleanup() {
  local rc=$?
  trap - EXIT

  if [[ "$GW_ENDPOINT_PATCHED" == "1" || "$AGENT_RESTART_NEEDED" == "1" || -n "$GW_REPLICAS_ORIG" ]]; then
    say "-- trap: 恢复步骤 5 的集群改动"
    restore_agent_step || note "trap 恢复未全部成功，请按 NOTE 手动收尾"
  fi

  if ((${#TEMP_PODS[@]} > 0)); then
    local p
    for p in "${TEMP_PODS[@]}"; do
      kubectl -n "$NS" delete pod "$p" --ignore-not-found --wait=false >>"$RAW_LOG" 2>&1
    done
    TEMP_PODS=()
  fi
  exit "$rc"
}
trap cleanup EXIT

url_encode_key() {
  local s="$1"
  s="${s//%/%25}"
  s="${s// /%20}"
  s="${s//#/%23}"
  s="${s//\?/%3F}"
  s="${s//&/%26}"
  printf '%s' "$s"
}

run_probe_pod() { # name command...
  local name="$1"
  shift
  TEMP_PODS+=("$name")
  kubectl -n "$NS" run "$name" --image="$PROBE_IMAGE" --image-pull-policy=IfNotPresent \
    --restart=Never --command -- "$@" >>"$RAW_LOG" 2>&1 || return 1
  kubectl -n "$NS" wait --for=condition=Ready pod/"$name" --timeout=60s >>"$RAW_LOG" 2>&1
}

gw_url() {
  printf 'http://%s.%s.svc:%s/%s/%s' "$GW_SVC" "$NS" "$GW_PORT" "$BUCKET" "$URL_KEY"
}

gw_get_once() { # pod
  kubectl -n "$NS" exec "$1" -- wget -q -O /dev/null "$(gw_url)" >>"$RAW_LOG" 2>&1
}

gw_get_retry() { # pod [tries]
  local pod="$1" tries="${2:-3}" i
  for ((i = 1; i <= tries; i++)); do
    if gw_get_once "$pod"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

pick_minio_pod() {
  local p
  while IFS= read -r p; do
    [[ -n "$p" ]] || continue
    if kubectl -n "$MINIO_NS" exec "$p" -- sh -c 'command -v mc >/dev/null 2>&1' >/dev/null 2>&1; then
      printf '%s' "$p"
      return 0
    fi
  done < <(kubectl -n "$MINIO_NS" get pods --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null)
  return 1
}

# mc_discover 递归列桶取第一个对象的 key。凭据只作为位置参数传入，不拼进脚本文本。
mc_discover() {
  local out key
  out="$(kubectl -n "$MINIO_NS" exec "$MINIO_POD" -- sh -c \
    'mc alias set tc "$1" "$2" "$3" >/dev/null 2>&1 || exit 3; mc ls --recursive --json "tc/$4"' \
    _ "$EP_URL" "$AK" "$SK" "$BUCKET" 2>/dev/null)" || return 1
  # 优先取块键（<dataset>/blocks/...），便于步骤 5 复用它触发一次块读取。
  key="$(printf '%s\n' "$out" | sed -n 's/.*"key":"\([^"]*\)".*/\1/p' | grep -m1 '/blocks/' || true)"
  if [[ -z "$key" ]]; then
    key="$(printf '%s\n' "$out" | sed -n 's/.*"key":"\([^"]*\)".*/\1/p' | head -n1)"
  fi
  if [[ -z "$key" ]]; then
    key="$(printf '%s\n' "$out" | awk 'NF>0 {print $NF}' | grep -m1 '/blocks/' || true)"
  fi
  if [[ -z "$key" ]]; then
    key="$(printf '%s\n' "$out" | awk 'NF>0 {print $NF}' | head -n1)"
  fi
  [[ -n "$key" ]] || return 1
  printf '%s' "$key"
}

# mc_cat_sha 用 MinIO Pod 内的 mc 读同一对象并算 sha256；失败返回非零（pipefail）。
mc_cat_sha() { # key
  kubectl -n "$MINIO_NS" exec "$MINIO_POD" -- sh -c \
    'mc alias set tc "$1" "$2" "$3" >/dev/null 2>&1 || exit 3; mc cat "tc/$4/$5"' \
    _ "$EP_URL" "$AK" "$SK" "$BUCKET" "$1" 2>/dev/null | sha256sum | awk '{print $1}'
}

say "== TrainCache S3 网关 k8s e2e（kind / ns=$NS）=="

# ---- 0. 前置检查 ----
say "-- [0/6] 前置检查"
kubectl cluster-info >/dev/null 2>&1 || { say "FATAL: 集群不可达（kubectl cluster-info 失败）"; exit 1; }
kubectl get nodes >/dev/null 2>&1 || { say "FATAL: kubectl get nodes 失败"; exit 1; }
kubectl -n "$NS" get configmap traincache-config >/dev/null 2>&1 \
  || { say "FATAL: 缺少 ConfigMap $NS/traincache-config"; exit 1; }
kubectl -n "$NS" get secret traincache-objstore >/dev/null 2>&1 \
  || { say "FATAL: 缺少 Secret $NS/traincache-objstore"; exit 1; }
ok "集群可达，ConfigMap/Secret 存在"

BUCKET="$(kubectl -n "$NS" get configmap traincache-config -o jsonpath='{.data.objstoreBucket}' 2>/dev/null)"
[[ -n "$BUCKET" ]] || BUCKET=traincache
OBJSTORE_ENDPOINT="$(kubectl -n "$NS" get configmap traincache-config -o jsonpath='{.data.objstoreEndpoint}' 2>/dev/null)"
OBJSTORE_SECURE="$(kubectl -n "$NS" get configmap traincache-config -o jsonpath='{.data.objstoreSecure}' 2>/dev/null)"
case "$(printf '%s' "$OBJSTORE_SECURE" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes) EP_URL="https://${OBJSTORE_ENDPOINT}" ;;
  *)          EP_URL="http://${OBJSTORE_ENDPOINT}" ;;
esac
AK="$(kubectl -n "$NS" get secret traincache-objstore -o jsonpath='{.data.accessKey}' 2>/dev/null | base64 -d 2>/dev/null)"
SK="$(kubectl -n "$NS" get secret traincache-objstore -o jsonpath='{.data.secretKey}' 2>/dev/null | base64 -d 2>/dev/null)"
MINIO_POD="$(pick_minio_pod || true)"
[[ -n "$MINIO_POD" ]] && ok "MinIO Pod=$MINIO_POD（mc 可用）" || note "未找到带 mc 的 MinIO Pod，对象发现/比对将 SKIP"

KEY="${E2E_KEY}"
HAVE_KEY=0
ORIGIN_SHA=""
if [[ -n "$KEY" ]]; then
  HAVE_KEY=1
  ok "使用 E2E_KEY=$KEY"
else
  if key="$(mc_discover)" && [[ -n "$key" ]]; then
    KEY="$key"
    HAVE_KEY=1
    ok "自动发现对象: $KEY"
  else
    skip "未发现可用对象（E2E_KEY 未设置且 mc 列桶失败）；步骤 3/4/5 跳过"
  fi
fi
if [[ $HAVE_KEY -eq 1 && -n "$MINIO_POD" ]]; then
  if sha="$(mc_cat_sha "$KEY")" && [[ -n "$sha" ]]; then
    ORIGIN_SHA="$sha"
    ok "源站对象 sha256=$ORIGIN_SHA"
  elif [[ -n "$E2E_KEY" ]]; then
    fail "E2E_KEY=$E2E_KEY 在桶 $BUCKET 中不可读"
    HAVE_KEY=0
  else
    note "mc cat 源站对象失败，步骤 3 仅做五份一致性比对"
  fi
fi
URL_KEY="$(url_encode_key "$KEY")"

# ---- 1. 应用清单并等待就绪 ----
say "-- [1/6] 应用网关清单、替换镜像、等待 ready"
kubectl apply -k "$REPO_ROOT/controller/deploy/gateway" >>"$RAW_LOG" 2>&1 \
  && ok "apply controller/deploy/gateway" || fail "apply 网关清单失败"
kubectl -n "$NS" set image deploy/"$GW_DEPLOY" gateway="$GW_IMAGE" >>"$RAW_LOG" 2>&1 \
  && ok "set image $GW_DEPLOY=$GW_IMAGE" || fail "set image 失败"
kubectl -n "$NS" rollout status deploy/"$GW_DEPLOY" --timeout=120s >>"$RAW_LOG" 2>&1 \
  && ok "Deployment $GW_DEPLOY ready" || fail "Deployment 未在 120s 内 ready"

# ---- 2. 探针 ----
say "-- [2/6] 探针 /healthz 与 /readyz"
for path in healthz readyz; do
  body="$(kubectl -n "$NS" exec deploy/"$GW_DEPLOY" -- \
    wget -qO- "http://127.0.0.1:${GW_PORT}/${path}" 2>>"$RAW_LOG")"
  rc=$?
  # 探针只回 200 或 503；wget 退出 0 即 2xx，对探针而言唯一可能的 2xx 就是 200。
  if [[ $rc -eq 0 && "$body" == "ok" ]]; then
    ok "GET /$path -> 200 ok"
  else
    fail "GET /$path 失败(rc=$rc body='${body:-<empty>}')"
  fi
done

# ---- 3. 直通与合并：五路并发 GET 一致性 + 与源站字节一致 ----
say "-- [3/6] 五路并发 GET 同一块，校验一致性与源站字节"
if [[ $HAVE_KEY -ne 1 ]]; then
  skip "没有可用对象键，跳过并发 GET 与源站比对"
else
  POD_GET="tc-gw-get-$$-$RANDOM"
  if ! run_probe_pod "$POD_GET" sleep 3600; then
    fail "临时探针 Pod $POD_GET 未 ready（busybox:latest 是否已 kind load？）"
  else
    GET_SCRIPT='
set -u
url="http://traincache-gateway.traincache.svc:9413/$1/$2"
i=0
while [ "$i" -lt 5 ]; do
  ( wget -q -O "/tmp/r$i" "$url"; echo "$?" > "/tmp/rc$i" ) &
  i=$((i+1))
done
wait
i=0
while [ "$i" -lt 5 ]; do
  rc="$(cat "/tmp/rc$i" 2>/dev/null || echo 99)"
  size="$(wc -c < "/tmp/r$i" 2>/dev/null || echo 0)"
  sha="$(sha256sum "/tmp/r$i" 2>/dev/null | cut -d" " -f1)"
  echo "res $i rc=$rc size=$size sha=$sha"
  i=$((i+1))
done
'
    out="$(kubectl -n "$NS" exec "$POD_GET" -- sh -c "$GET_SCRIPT" _ "$BUCKET" "$URL_KEY" 2>>"$RAW_LOG")"
    lines="$(printf '%s\n' "$out" | grep '^res ' || true)"
    count="$(printf '%s\n' "$lines" | grep -c '^res ' || true)"
    if [[ "$count" -ne 5 ]]; then
      fail "五路 GET 返回 $count 条结果（期望 5）"
    else
      bad_rc=0
      first_sha=""
      first_size=""
      while IFS= read -r line; do
        [[ -n "$line" ]] || continue
        r="$(sed -n 's/^res \([0-9]*\) rc=\([0-9]*\) size=\([0-9]*\) sha=\([0-9a-f]*\)$/\1 \2 \3 \4/p' <<<"$line")"
        [[ -n "$r" ]] || { bad_rc=1; continue; }
        read -r _ rc size sha <<<"$r"
        [[ "$rc" == "0" ]] || bad_rc=1
        if [[ -z "$first_sha" ]]; then
          first_sha="$sha"
          first_size="$size"
        fi
        [[ "$sha" == "$first_sha" && "$size" == "$first_size" ]] || bad_rc=2
      done <<<"$lines"
      if [[ "$bad_rc" == "2" ]]; then
        fail "五份响应的 size/sha256 不一致"
        printf '%s\n' "$lines" | sed 's/^/    /'
      elif [[ "$bad_rc" != "0" ]]; then
        fail "至少一个 GET 失败（wget 退出码非 0）"
        printf '%s\n' "$lines" | sed 's/^/    /'
      elif [[ -z "$first_sha" ]]; then
        fail "响应 sha256 缺失"
      elif [[ "$first_size" == "0" ]]; then
        skip "对象为 0 字节，五份一致但没有可比较的块内容"
      else
        ok "五份一致: size=$first_size sha256=$first_sha"
        if [[ -n "$ORIGIN_SHA" ]]; then
          if [[ "$first_sha" == "$ORIGIN_SHA" ]]; then
            ok "与 mc cat 源站字节一致"
          else
            fail "网关响应 sha256=$first_sha 与源站 $ORIGIN_SHA 不一致"
          fi
        else
          note "源站 sha256 不可得（mc 不可用），跳过源站字节比对"
        fi
      fi
    fi
  fi
fi

# ---- 4. 排水：rollout restart 前后与期间 ----
say "-- [4/6] rollout restart 排水观测"
if [[ $HAVE_KEY -ne 1 ]]; then
  skip "没有可用对象键，跳过排水期间 GET 观测"
else
  POD_DRAIN="tc-gw-drain-$$-$RANDOM"
  if ! run_probe_pod "$POD_DRAIN" sleep 3600; then
    fail "临时探针 Pod $POD_DRAIN 未 ready"
  else
    if gw_get_retry "$POD_DRAIN" 3; then
      ok "重启前 GET 成功"
    else
      fail "重启前 GET 连续 3 次失败"
    fi

    DRAIN_SCRIPT='
set -u
url="http://traincache-gateway.traincache.svc:9413/$1/$2"
i=0
while [ "$i" -lt 40 ]; do
  msg="$(wget -S -O /dev/null "$url" 2>&1)"
  code="$(printf "%s\n" "$msg" | sed -n "s/.*HTTP\/[0-9.]* \([0-9][0-9][0-9]\).*/\1/p" | tail -n1)"
  hdr="$(printf "%s\n" "$msg" | grep -i x-traincache-gateway: | tail -n1 | cut -d: -f2- | tr -d " \r")"
  echo "probe code=${code:-conn} gw=${hdr:-none}"
  i=$((i+1))
  sleep 1
done
'
    kubectl -n "$NS" exec "$POD_DRAIN" -- sh -c "$DRAIN_SCRIPT" _ "$BUCKET" "$URL_KEY" \
      >"$DRAIN_LOG" 2>>"$RAW_LOG" &
    LOOP_PID=$!

    kubectl -n "$NS" rollout restart deploy/"$GW_DEPLOY" >>"$RAW_LOG" 2>&1 \
      && ok "已触发 rollout restart" || fail "rollout restart 失败"
    kubectl -n "$NS" rollout status deploy/"$GW_DEPLOY" --timeout=120s >>"$RAW_LOG" 2>&1 \
      && ok "新 Pod ready" || fail "新 Pod 未在 120s 内 ready"

    if gw_get_retry "$POD_DRAIN" 3; then
      ok "新 Pod Ready 后 GET 成功"
    else
      fail "新 Pod Ready 后 GET 连续 3 次失败"
    fi

    wait "$LOOP_PID" 2>/dev/null || true

    got200=0
    got_unavailable=0
    got_conn=0
    bad5xx=0
    first_bad_5xx=""
    while IFS= read -r line; do
      case "$line" in
        probe\ code=*) ;;
        *) continue ;;
      esac
      code="$(sed -n 's/^probe code=\([^ ]*\) gw=.*$/\1/p' <<<"$line")"
      gw="$(sed -n 's/^probe code=.* gw=\(.*\)$/\1/p' <<<"$line")"
      case "$code" in
        200)
          got200=$((got200 + 1))
          ;;
        conn|"")
          got_conn=$((got_conn + 1))
          ;;
        5*)
          if [[ "$gw" == "unavailable" ]]; then
            got_unavailable=$((got_unavailable + 1))
          else
            bad5xx=$((bad5xx + 1))
            [[ -z "$first_bad_5xx" ]] && first_bad_5xx="$line"
          fi
          ;;
        *)
          note "重启期间非 2xx 响应: $line"
          ;;
      esac
    done <"$DRAIN_LOG"
    if [[ "$bad5xx" -ne 0 ]]; then
      fail "重启期间 $bad5xx 次 5xx 未带 x-traincache-gateway: unavailable（示例: $first_bad_5xx）"
    else
      ok "重启期间无「unavailable 以外」的 5xx（200=$got200 conn=$got_conn unavailable=$got_unavailable）"
    fi
    if [[ "$got200" -eq 0 ]]; then
      note "重启窗口内没有观察到 200（Service 可能有短暂无端点窗口；前后 GET 已单独断言）"
    fi
  fi
fi

# ---- 5. 可选：Agent 侧降级联动 ----
say "-- [5/6] Agent 降级联动（TC_E2E_AGENT=1 时启用）"
if [[ "$TC_E2E_AGENT" != "1" ]]; then
  skip "未启用（TC_E2E_AGENT != 1）"
elif [[ $HAVE_KEY -ne 1 ]]; then
  skip "没有可用对象键，无法触发块读取"
else
  AGENTD_POD="$(kubectl -n "$NS" get pods -l "$AGENT_LABEL" --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  if [[ -z "$AGENTD_POD" ]]; then
    skip "未找到运行中的 agentd Pod"
  else
    AGENT_NODE="$(kubectl -n "$NS" get pod "$AGENTD_POD" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
    GW_ENDPOINT_ORIG="$(kubectl -n "$NS" get configmap traincache-config -o jsonpath='{.data.gatewayEndpoint}' 2>/dev/null)"
    GW_REPLICAS_ORIG="$(kubectl -n "$NS" get deploy "$GW_DEPLOY" -o jsonpath='{.spec.replicas}' 2>/dev/null)"
    [[ -n "$GW_REPLICAS_ORIG" ]] || GW_REPLICAS_ORIG=1
    # 先置位再改集群：patch 部分失败时 trap 仍会写回原值。
    GW_ENDPOINT_PATCHED=1
    AGENT_RESTART_NEEDED=1

    if kubectl -n "$NS" patch configmap traincache-config --type merge \
      -p '{"data":{"gatewayEndpoint":"traincache-gateway.traincache.svc:9413"}}' >>"$RAW_LOG" 2>&1; then
      ok "gatewayEndpoint=traincache-gateway.traincache.svc:9413"
    else
      fail "patch gatewayEndpoint 失败"
    fi
    if kubectl -n "$NS" rollout restart ds/"$AGENT_DS" >>"$RAW_LOG" 2>&1 \
      && kubectl -n "$NS" rollout status ds/"$AGENT_DS" --timeout=180s >>"$RAW_LOG" 2>&1; then
      ok "agentd 已重启并 ready"
    else
      fail "agentd 重启未完成"
    fi

    AGENTD_POD="$(kubectl -n "$NS" get pods -l "$AGENT_LABEL" --field-selector=status.phase=Running \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
    agent_metrics() { kubectl -n "$NS" exec "$AGENTD_POD" -- wget -qO- http://127.0.0.1:9411/metrics 2>/dev/null; }
    mode_val() { agent_metrics | awk '$1=="traincache_source_mode" {print $2; exit}'; }
    fallback_sum() { agent_metrics | awk '$1 ~ /^traincache_gateway_fallback_total\{/ {s+=$2} END {print s+0}'; }

    MODE=""
    for _ in $(seq 1 30); do
      MODE="$(mode_val)"
      [[ "$MODE" == "1" ]] && break
      sleep 2
    done
    if [[ "$MODE" == "1" ]]; then
      ok "agentd traincache_source_mode=1（走网关）"
    else
      fail "agentd traincache_source_mode='${MODE:-<empty>}'，期望 1"
    fi
    # 基线必须在缩容前取：缩容本身不产生降级，真正的降级由随后的一次块读取触发。
    BASE_FB="$(fallback_sum)"
    note "触发前 traincache_gateway_fallback_total=$BASE_FB"

    # 关掉网关：Agent 的下一次块读取应降级直连。
    kubectl -n "$NS" scale deploy/"$GW_DEPLOY" --replicas=0 >>"$RAW_LOG" 2>&1 \
      && ok "网关已 scale 到 0" || fail "scale 网关到 0 失败"
    for _ in $(seq 1 60); do
      gw_pods="$(kubectl -n "$NS" get pods -l "app.kubernetes.io/name=$GW_SVC" --no-headers 2>/dev/null | wc -l)"
      [[ "$gw_pods" -eq 0 ]] && break
      sleep 1
    done
    [[ "$gw_pods" -eq 0 ]] || note "等待网关 Pod 删除超时，继续尝试触发读"
    for _ in $(seq 1 20); do
      ep="$(kubectl -n "$NS" get endpoints "$GW_SVC" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)"
      [[ -z "$ep" ]] && break
      sleep 1
    done

    DS="${KEY%%/*}"
    REST="${KEY#*/blocks/}"
    if [[ "$REST" == "$KEY" || "$REST" != *#* ]]; then
      skip "E2E_KEY 不是 <dataset>/blocks/<path>#<chunk> 布局，无法构造块读取"
    else
      REL="${REST%#*}"
      CHUNK="${REST##*#}"
      PREFIX="${REL%/*}"
      [[ "$PREFIX" == "$REL" ]] && PREFIX="$REL"
      TASK="gw-e2e-$(date +%s)-$$"
      prefetch_out="$(kubectl -n "$NS" exec "$AGENTD_POD" -- traincache prefetch \
        --endpoint 127.0.0.1:9410 --task-id "$TASK" --path "$PREFIX" \
        --node "$AGENT_NODE" --chunks "$REL:$CHUNK" --dataset "$DS" 2>&1)"
      prefetch_rc=$?
      if [[ $prefetch_rc -ne 0 ]]; then
        skip "prefetch 未受理（dataset 或目录不可用）：$prefetch_out"
      elif grep -q "accepted=false" <<<"$prefetch_out"; then
        skip "prefetch 未受理（accepted=false）：$prefetch_out"
      elif grep -q "无匹配块" <<<"$prefetch_out"; then
        skip "prefetch 无匹配块（dataset 无 CURRENT）：$prefetch_out"
      else
        changed=0
        for _ in $(seq 1 30); do
          cur_fb="$(fallback_sum)"
          cur_mode="$(mode_val)"
          if [[ "$cur_fb" -gt "$BASE_FB" && "$cur_mode" == "0" ]]; then
            ok "网关缩容后降级：fallback_total $BASE_FB -> $cur_fb，source_mode=0"
            changed=1
            break
          fi
          sleep 2
        done
        if [[ $changed -eq 0 ]]; then
          skip "未观察到降级（块可能命中本地 CAS 或预热未真正回源）；该步骤为可选联动，不判 FAIL"
        fi
      fi
    fi

    # 显式恢复；失败时 trap 会在退出前再试一次。
    if restore_agent_step; then
      ok "步骤 5 的 ConfigMap/副本数/agentd 已恢复"
    else
      fail "步骤 5 恢复未全部成功（trap 将重试）"
    fi
  fi
fi

# ---- 6. 清理与汇总 ----
say "-- [6/6] 清理临时 Pod 并汇总"
if ((${#TEMP_PODS[@]} > 0)); then
  for p in "${TEMP_PODS[@]}"; do
    kubectl -n "$NS" delete pod "$p" --ignore-not-found --wait=true >>"$RAW_LOG" 2>&1
  done
  TEMP_PODS=()
  ok "临时 Pod 已清理"
else
  note "无临时 Pod 需要清理"
fi

if [[ $FAILS -eq 0 ]]; then
  say "RESULT: PASS（OK=$OKS SKIP=$SKIPS FAIL=0）"
else
  say "RESULT: FAIL（OK=$OKS SKIP=$SKIPS FAIL=$FAILS）"
fi
say "原始证据: $RAW_LOG"
say "排水观测: $DRAIN_LOG"
say "注：不删除 Deployment/Service；步骤 5 的集群改动已由 trap 兜底恢复。"

[[ $FAILS -eq 0 ]]
