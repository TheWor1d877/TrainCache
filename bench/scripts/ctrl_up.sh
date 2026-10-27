#!/usr/bin/env bash
# ============================================================================
# TrainCache 控制面拉起/校验(可重复执行)。
#
# 由 bench/Makefile 的 ctrl-up 目标与 bench/compare/traincache/up.sh 共用。
# 步骤:
#   1. namespace traincache
#   2. worker 节点 traincache.io/s3-access=true 标签
#   3. 控制面镜像预检(缺失直接失败并打印拉取/导入命令)
#   4. Secret traincache-objstore(MinIO 凭证)
#   5. webhook TLS Secret(含 ca.crt)+ MutatingWebhookConfiguration caBundle
#   6. kubectl apply -k controller/deploy(仅清单哈希变化或工作负载缺失时)
#   7. traincache-config 的 WHITELIST/CACHE_CAPACITY_BYTES/STARTUP_WINDOW_BYTES/MAX_FILL_FRACTION 与 agentd 的 BUCKET
#   8. 四个工作负载 rollout 就绪,打印 Ready 数与 Pod UID
#
# 幂等:清单哈希记在 namespace 注解 traincache.io/deploy-hash。哈希未变且
# controller-manager/webhook/scheduler/agentd 都在时不执行 apply,连续两次
# 调用不会滚动重启 agentd。
# ============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$BENCH_ROOT/compare/_common.sh"

GROUP="${GROUP:-traincache}"
NS="${NS:-traincache}"
DATASET="${DATASET:-compare-data-8m}"
DEPLOY_DIR="$REPO/controller/deploy"
CRD_DIR="$REPO/controller/config/crd/bases"
HASH_ANNOTATION="traincache.io/deploy-hash"
CAPACITY_BYTES="${TRAINCACHE_CACHE_CAPACITY_BYTES:-1073741824}"
STARTUP_WINDOW_BYTES="536870912"
MAX_FILL_FRACTION="0.125"
CLUSTER_NAME="${CLUSTER_NAME:-$(cfg CLUSTER_NAME)}"
CLUSTER_NAME="${CLUSTER_NAME:-traincache}"
WEBHOOK_CA_B64=""

# ---- 1. 节点 s3-access 标签(DataSet Ready 前置条件)----
label_s3_nodes() {
  local workers n cur cnt
  workers="$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o name 2>/dev/null || true)"
  if [ -z "$workers" ]; then
    log_group "$GROUP" "WARNING: 无 worker 节点,无法打 s3-access 标签" >&2
    return 0
  fi
  for n in $workers; do
    cur="$(kubectl get "$n" -o jsonpath='{.metadata.labels.traincache\.io/s3-access}' 2>/dev/null || true)"
    if [ "$cur" = "true" ]; then
      continue
    fi
    kubectl label "$n" traincache.io/s3-access=true --overwrite >/dev/null
  done
  cnt="$(kubectl get nodes -l traincache.io/s3-access=true --no-headers 2>/dev/null | wc -l)"
  log_group "$GROUP" "s3-access 标签: $cnt 个 worker 节点"
}

# ---- 2. 镜像预检:只查,不拉取 ----
# 四个工作负载的 image 引用直接从 kustomize 渲染结果取,与清单同源。
# kind load 刚结束时 containerd 元数据可能还没同步,单次 crictl 会误报缺失
# (随即手工 crictl images -q 又正常),故每个镜像最多探 3 次、间隔 2s。
image_present() {
  local node="$1" img="$2" i
  for i in 1 2 3; do
    if docker exec "$node" crictl images -q "$img" 2>/dev/null | grep -q .; then
      return 0
    fi
    if [ "$i" -lt 3 ]; then
      sleep 2
    fi
  done
  return 1
}

check_images() {
  local images nodes node img missing=0
  if ! command -v docker >/dev/null 2>&1 || ! docker ps >/dev/null 2>&1; then
    log_group "$GROUP" "WARNING: docker 不可用,跳过控制面镜像预检" >&2
    return 0
  fi
  images="$(kubectl kustomize "$DEPLOY_DIR" 2>/dev/null \
    | awk '/^[[:space:]]*image:[[:space:]]*/{print $2}' | sort -u)"
  if [ -z "$images" ]; then
    log_group "$GROUP" "WARNING: 未能从 controller/deploy 解析镜像引用,跳过预检" >&2
    return 0
  fi
  nodes="$(kubectl get nodes -o name 2>/dev/null | sed 's#^node/##' \
    | grep -E 'worker[0-9]*$' || true)"
  if [ -z "$nodes" ]; then
    log_group "$GROUP" "WARNING: 无 worker 节点,跳过控制面镜像预检" >&2
    return 0
  fi
  for node in $nodes; do
    if ! docker exec "$node" true >/dev/null 2>&1; then
      log_group "$GROUP" "WARNING: docker exec $node 不可用,跳过该节点镜像预检" >&2
      continue
    fi
    for img in $images; do
      if ! image_present "$node" "$img"; then
        printf '缺失镜像: %s -> %s\n' "$node" "$img" >&2
        missing=1
      fi
    done
  done
  if [ "$missing" -ne 0 ]; then
    cat >&2 <<EOF

控制面镜像未导入 kind 节点,离线时会 ImagePullBackOff。下一步(在 repo 根执行):
  # 镜像域名取 /etc/docker/daemon.json 的 registry-mirrors,去掉 https://
  docker pull <image>
  kind load docker-image <image> --name ${CLUSTER_NAME:-traincache}
  make -C bench ctrl-up
已带加速域名的完整引用见上方缺失列表;不要在本脚本内等待拉取。
EOF
    return 1
  fi
  log_group "$GROUP" "控制面镜像齐全: $(printf '%s' "$images" | tr '\n' ' ')"
}

# ---- 3. 两个 Secret ----
ensure_objstore_secret() {
  if resource_exists secret traincache-objstore -n "$NS"; then
    log_group "$GROUP" "secret/traincache-objstore 已存在,跳过"
  else
    kubectl -n "$NS" create secret generic traincache-objstore \
      --from-literal=accessKey="$MINIO_ACCESS_KEY" \
      --from-literal=secretKey="$MINIO_SECRET_KEY" >/dev/null
    log_group "$GROUP" "secret/traincache-objstore 已创建"
  fi
}

# 生成自签服务端证书,同时把签发 CA 存进 Secret 的 ca.crt,后续可复算 caBundle。
ensure_webhook_tls() {
  local tmp cur_ca
  if kubectl -n "$NS" get secret traincache-webhook-tls >/dev/null 2>&1; then
    WEBHOOK_CA_B64="$(kubectl -n "$NS" get secret traincache-webhook-tls \
      -o jsonpath='{.data.ca\.crt}' 2>/dev/null || true)"
    if [ -n "$WEBHOOK_CA_B64" ]; then
      log_group "$GROUP" "secret/traincache-webhook-tls 已存在,跳过"
      return 0
    fi
    cur_ca="$(kubectl get mutatingwebhookconfiguration traincache-pod-warmup \
      -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)"
    if [ -n "$cur_ca" ]; then
      WEBHOOK_CA_B64="$cur_ca"
      log_group "$GROUP" "secret/traincache-webhook-tls 已存在(无 ca.crt),沿用现有 caBundle"
      return 0
    fi
    log_group "$GROUP" "secret/traincache-webhook-tls 缺 ca.crt 且 caBundle 为空,重建自签证书"
    kubectl -n "$NS" delete secret traincache-webhook-tls --ignore-not-found >/dev/null
  fi
  log_group "$GROUP" "生成 webhook TLS 自签证书"
  tmp="$(mktemp -d)"
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout "$tmp/ca.key" -out "$tmp/ca.crt" \
    -subj "/CN=traincache-ca" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "$tmp/tls.key" -out "$tmp/tls.csr" \
    -subj "/CN=traincache-webhook.$NS.svc" >/dev/null 2>&1
  printf 'subjectAltName=DNS:traincache-webhook.%s.svc,DNS:traincache-webhook.%s.svc.cluster.local\n' \
    "$NS" "$NS" > "$tmp/san.ext"
  openssl x509 -req -in "$tmp/tls.csr" -CA "$tmp/ca.crt" -CAkey "$tmp/ca.key" \
    -CAcreateserial -out "$tmp/tls.crt" -days 365 -extfile "$tmp/san.ext" >/dev/null 2>&1
  kubectl -n "$NS" create secret tls traincache-webhook-tls \
    --cert="$tmp/tls.crt" --key="$tmp/tls.key" --dry-run=client -o yaml \
    | kubectl apply -f - >/dev/null
  WEBHOOK_CA_B64="$(base64 -w0 "$tmp/ca.crt")"
  kubectl -n "$NS" patch secret traincache-webhook-tls --type=merge \
    -p "{\"data\":{\"ca.crt\":\"$WEBHOOK_CA_B64\"}}" >/dev/null
  rm -rf "$tmp"
}

ensure_ca_bundle() {
  local cur
  if [ -z "$WEBHOOK_CA_B64" ]; then
    log_group "$GROUP" "WARNING: 未取得 webhook CA,跳过 caBundle 注入" >&2
    return 0
  fi
  cur="$(kubectl get mutatingwebhookconfiguration traincache-pod-warmup \
    -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)"
  if [ "$cur" = "$WEBHOOK_CA_B64" ]; then
    log_group "$GROUP" "webhook caBundle 已是最新,跳过"
    return 0
  fi
  kubectl patch mutatingwebhookconfiguration traincache-pod-warmup --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/webhooks/0/clientConfig/caBundle\",\"value\":\"$WEBHOOK_CA_B64\"}]" \
    >/dev/null
  log_group "$GROUP" "webhook caBundle 已注入"
}

# ---- 4. 清单内容哈希 + 条件 apply ----
deploy_hash() {
  { find "$DEPLOY_DIR" "$CRD_DIR" -type f \( -name '*.yaml' -o -name '*.yml' \) -print0 \
      | sort -z | xargs -0 sha256sum; } | sha256sum | awk '{print $1}'
}

workloads_present() {
  kubectl -n "$NS" get deployment traincache-controller-manager >/dev/null 2>&1 \
    && kubectl -n "$NS" get deployment traincache-webhook >/dev/null 2>&1 \
    && kubectl -n kube-system get deployment traincache-scheduler >/dev/null 2>&1 \
    && kubectl -n "$NS" get daemonset traincache-agentd >/dev/null 2>&1
}

ensure_control_plane() {
  local want cur
  want="$(deploy_hash)"
  cur="$(kubectl -n "$NS" get namespace "$NS" \
    -o jsonpath='{.metadata.annotations.traincache\.io/deploy-hash}' 2>/dev/null || true)"
  if [ "$want" = "$cur" ] && workloads_present; then
    log_group "$GROUP" "控制面清单未变化且四个工作负载都在,跳过 apply(hash=${want:0:12})"
    return 0
  fi
  log_group "$GROUP" "应用控制面清单 kubectl apply -k controller/deploy"
  kubectl apply -k "$DEPLOY_DIR" >/dev/null
  kubectl annotate namespace "$NS" "$HASH_ANNOTATION=$want" --overwrite >/dev/null
  log_group "$GROUP" "控制面已 apply(hash=${want:0:12})"
}

# ---- 5. agentd 配置(白名单/桶/容量/自动 K 参数)----
# 白名单、容量与自动 K 的两个输入在 traincache-config ConfigMap;桶仍是 DaemonSet 显式 env。
ensure_agent_config() {
  local cur_wl cur_bucket cur_cap cur_sw cur_mf changed=0
  cur_wl="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.datasetMetricsWhitelist}' 2>/dev/null || true)"
  cur_cap="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.cacheCapacityBytes}' 2>/dev/null || true)"
  cur_sw="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.startupWindowBytes}' 2>/dev/null || true)"
  cur_mf="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.maxFillFraction}' 2>/dev/null || true)"
  cur_bucket="$(kubectl -n "$NS" get ds traincache-agentd -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="TRAINCACHE_OBJSTORE_BUCKET")].value}' 2>/dev/null || true)"
  if [ "$cur_wl" != "$DATASET" ] || [ "$cur_cap" != "$CAPACITY_BYTES" ] \
     || [ "$cur_sw" != "$STARTUP_WINDOW_BYTES" ] || [ "$cur_mf" != "$MAX_FILL_FRACTION" ]; then
    log_group "$GROUP" "修正 agentd 配置: WHITELIST=$DATASET CACHE_CAPACITY_BYTES=$CAPACITY_BYTES(1GiB) STARTUP_WINDOW_BYTES=$STARTUP_WINDOW_BYTES MAX_FILL_FRACTION=$MAX_FILL_FRACTION"
    kubectl -n "$NS" patch configmap traincache-config --type merge \
      -p "{\"data\":{\"datasetMetricsWhitelist\":\"$DATASET\",\"cacheCapacityBytes\":\"$CAPACITY_BYTES\",\"startupWindowBytes\":\"$STARTUP_WINDOW_BYTES\",\"maxFillFraction\":\"$MAX_FILL_FRACTION\"}}" >/dev/null
    changed=1
  fi
  if [ "$cur_bucket" != "$MINIO_BUCKET" ]; then
    log_group "$GROUP" "修正 agentd 桶: BUCKET=$MINIO_BUCKET"
    kubectl -n "$NS" set env daemonset/traincache-agentd TRAINCACHE_OBJSTORE_BUCKET="$MINIO_BUCKET" >/dev/null
    changed=1
  fi
  if [ "$changed" -eq 1 ]; then
    kubectl -n "$NS" rollout restart daemonset/traincache-agentd >/dev/null
    kubectl -n "$NS" rollout status daemonset/traincache-agentd --timeout=240s >/dev/null
    log_group "$GROUP" "agentd 配置已修正并滚动更新完成"
  else
    log_group "$GROUP" "agentd 配置正确(WHITELIST=$DATASET BUCKET=$MINIO_BUCKET CACHE_CAPACITY_BYTES=$CAPACITY_BYTES)"
  fi
}

wait_ready() {
  kubectl -n "$NS" rollout status deployment/traincache-controller-manager --timeout=180s
  kubectl -n "$NS" rollout status deployment/traincache-webhook --timeout=180s
  kubectl -n kube-system rollout status deployment/traincache-scheduler --timeout=180s
  kubectl -n "$NS" rollout status daemonset/traincache-agentd --timeout=240s
}

# ---- 6. 结束打印 Ready 数与 Pod UID,供校验无滚动重启 ----
print_status() {
  local kind name ns desired ready uids
  echo "== 控制面状态 =="
  for entry in \
    "deployment traincache-controller-manager $NS" \
    "deployment traincache-webhook $NS" \
    "deployment traincache-scheduler kube-system" \
    "daemonset traincache-agentd $NS"; do
    read -r kind name ns <<<"$entry"
    if [ "$kind" = "daemonset" ]; then
      desired="$(kubectl -n "$ns" get ds "$name" -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || true)"
      ready="$(kubectl -n "$ns" get ds "$name" -o jsonpath='{.status.numberReady}' 2>/dev/null || true)"
    else
      desired="$(kubectl -n "$ns" get deploy "$name" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
      ready="$(kubectl -n "$ns" get deploy "$name" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    fi
    uids="$(kubectl -n "$ns" get pods -l "app.kubernetes.io/name=$name" \
      -o jsonpath='{range .items[*]}{.metadata.uid}{" "}{end}' 2>/dev/null || true)"
    printf '  %-30s Ready %s/%s  UID: %s\n' "$name" "${ready:-0}" "${desired:-?}" "${uids:-<none>}"
  done
}

ensure_ns "$NS" "$GROUP"
label_s3_nodes
check_images
ensure_objstore_secret
ensure_webhook_tls
ensure_control_plane
ensure_ca_bundle
ensure_agent_config
wait_ready
print_status
log_group "$GROUP" "TrainCache 控制面就绪"
