#!/usr/bin/env bash
# ============================================================================
# TrainCache 控制面拉起/校验(可重复执行)。
#
# 由 bench/Makefile 的 ctrl-up 目标与 bench/compare/traincache/up.sh 共用。
# 步骤:
#   0. kubectl context 断言(kind-$CLUSTER_NAME;别的会话切走 context 时直接失败)
#   1. namespace traincache
#   2. worker 节点 traincache.io/s3-access=true 标签
#   3. 控制面镜像预检(缺失直接失败并打印拉取/导入命令)
#   3b. 镜像新鲜度(镜像源码指纹标签 != 当前工作树则失败;旧二进制会把修掉的
#       缺陷再量一遍)
#   4. Secret traincache-objstore(MinIO 凭证)
#   5. webhook TLS Secret(含 ca.crt)+ MutatingWebhookConfiguration caBundle
#   6. kubectl apply -k controller/deploy(仅清单哈希变化或工作负载缺失时)
#   7. 网关开关(TRAINCACHE_GATEWAY=1):traincache-gateway 副本 1 并等 rollout;
#      不带或为 0:副本 0(对象保留,不删 Deployment)。见 ensure_gateway
#   7b. 网关磁盘缓存开关(TRAINCACHE_GATEWAY_CACHE=1/0):patch traincache-config
#      的 gatewayCache(1→true、0→false);configMapKeyRef 不热生效,改完必须
#      重启 deployment/traincache-gateway。未设置则保持清单默认。见 ensure_gateway_cache
#   8. traincache-config 的 WHITELIST/CACHE_CAPACITY_BYTES/STARTUP_WINDOW_BYTES/MAX_FILL_FRACTION/BUCKET、
#      gatewayEndpoint(开网关指向 traincache-gateway.<NS>.svc:9413,关网关置空)
#      与本轮的 PREFETCH_LOOKAHEAD/EVICT_POLICY(apply 会盖回清单默认,所以在 apply 之后对齐);
#      只有取值真的变化才 patch 并重启一次 agentd
#   8b. gate 网络带宽上限(TRAINCACHE_GATE_NETWORK_BANDWIDTH):patch
#      traincache-config 的 gateNetworkBandwidthBytes,再重启
#      deployment/traincache-controller-manager(configMapKeyRef 不热生效)。
#      缺省 0 = 不启用带宽判断。见 ensure_gate_bandwidth
#   9. 五个工作负载 rollout 就绪(开网关时含网关),打印 Ready 数与 Pod UID
#
# 幂等:清单哈希记在 namespace 注解 traincache.io/deploy-hash。哈希未变且
# controller-manager/webhook/scheduler/agentd/gateway 都在时不执行 apply,
# 连续两次调用不会滚动重启 agentd;网关只改副本数,同一分支连跑两次不重建 Pod。
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
# 与 controller/deploy/agent/configmap.yaml 的默认值一致:独立调用(不带 COND)时
# 这两项就停在清单默认,不额外触发滚动重启。
PREFETCH_LOOKAHEAD="${TRAINCACHE_PREFETCH_LOOKAHEAD:-16}"
EVICT_POLICY="${TRAINCACHE_EVICT_POLICY:-heat}"
# gate 网络带宽上限(bytes/sec):门控把 Agent 上报吞吐与该值的 0.8 倍比较,接近
# 上限即认为带宽够快、跳过门控。取值来自环境变量,bench 侧不写死集群专有数值;
# 缺省 0 = 不启用带宽判断,门控行为与加这一项之前一致。
GATE_NETWORK_BANDWIDTH="${TRAINCACHE_GATE_NETWORK_BANDWIDTH:-0}"
case "$GATE_NETWORK_BANDWIDTH" in
  ''|*[!0-9]*)
    log_group "$GROUP" "错误: TRAINCACHE_GATE_NETWORK_BANDWIDTH='$GATE_NETWORK_BANDWIDTH' 非法(要非负整数字节/秒)" >&2
    exit 1
    ;;
esac
CLUSTER_NAME="${CLUSTER_NAME:-$(cfg CLUSTER_NAME)}"
CLUSTER_NAME="${CLUSTER_NAME:-traincache}"
WEBHOOK_CA_B64=""

# ---- 网关开关:非 1 一律视为 0(无网关,agentd 直连源站)----
GATEWAY_ON="${TRAINCACHE_GATEWAY:-0}"
[ "$GATEWAY_ON" = "1" ] || GATEWAY_ON=0
GATEWAY_PORT="${GATEWAY_PORT:-9413}"
GATEWAY_ENDPOINT="traincache-gateway.$NS.svc:$GATEWAY_PORT"
if [ "$GATEWAY_ON" = "1" ]; then
  WANT_GATEWAY_ENDPOINT="$GATEWAY_ENDPOINT"
else
  WANT_GATEWAY_ENDPOINT=""
fi

# ---- 网关磁盘缓存开关(D23):TRAINCACHE_GATEWAY_CACHE=1 → gatewayCache=true,
#      =0 → gatewayCache=false。未设置则不动 ConfigMap(保持清单默认 true)。
#      configMapKeyRef 改值不热生效,改完必须重启网关 Deployment。
GATEWAY_CACHE_RAW="${TRAINCACHE_GATEWAY_CACHE:-}"
WANT_GATEWAY_CACHE=""
case "$GATEWAY_CACHE_RAW" in
  "") ;;
  1) WANT_GATEWAY_CACHE=true ;;
  0) WANT_GATEWAY_CACHE=false ;;
  *)
    WANT_GATEWAY_CACHE=false
    log_group "$GROUP" "WARNING: TRAINCACHE_GATEWAY_CACHE 取值 '$GATEWAY_CACHE_RAW' 非法(只认 0/1),按 0(false)处理" >&2
    ;;
esac

# ---- 0. context 断言:装置只跑在自己的 kind 集群上 ----
# 同一台机器上并行的会话会共用 ~/.kube/config,而 kind create cluster 会把
# current-context 切到新集群。上个会话建 e2e 集群后,这里若不拦,up.sh 会把
# 控制面 apply 到别人的单节点集群、量出来的数与 bench 无关。
assert_context() {
  local want="kind-$CLUSTER_NAME" cur have
  have="$(kubectl config get-contexts -o name 2>/dev/null || true)"
  [ -n "$have" ] || return 0
  if ! printf '%s\n' "$have" | grep -qx "$want"; then
    log_group "$GROUP" "WARNING: kubeconfig 里没有 context $want,按单集群环境处理(真实集群/CI)" >&2
    return 0
  fi
  cur="$(kubectl config current-context 2>/dev/null || true)"
  if [ "$cur" = "$want" ]; then
    return 0
  fi
  if [ "${TRAINCACHE_ALLOW_CONTEXT_MISMATCH:-0}" = "1" ]; then
    log_group "$GROUP" "WARNING: context 是 ${cur:-<无>} 而非 $want,已按 TRAINCACHE_ALLOW_CONTEXT_MISMATCH=1 放行" >&2
    return 0
  fi
  log_group "$GROUP" "错误: kubectl current-context=${cur:-<无>},装置要求 $want。别的会话切走了 context;用独立 kubeconfig 跑,不要改共享的 ~/.kube/config:" >&2
  printf '  kubectl --context %s config view --minify --flatten >/tmp/%s.kubeconfig && export KUBECONFIG=/tmp/%s.kubeconfig\n' \
    "$want" "$CLUSTER_NAME" "$CLUSTER_NAME" >&2
  return 1
}

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
# 五个工作负载的 image 引用直接从 kustomize 渲染结果取,与清单同源。
rendered_images() {
  kubectl kustomize "$DEPLOY_DIR" 2>/dev/null \
    | awk '/^[[:space:]]*image:[[:space:]]*/{print $2}' | sort -u
}

# 镜像新鲜度:热集群里 :dev 标签会被反复复用,只查"在不在"会让上一次构建的
# 二进制继续参与测量——#17 探针就是这么把已在 HEAD 修掉的字节维度又量了一遍
# (容量 gauge 报 0、目录涨到全量数据集、淘汰增量 0)。判据用 hack/build-images.sh
# 打在镜像上的源码指纹标签:与当前工作树不一致或没有标签,就是旧镜像。
check_image_freshness() {
  local images img label fp stale=0
  if ! command -v docker >/dev/null 2>&1 || ! docker ps >/dev/null 2>&1; then
    log_group "$GROUP" "WARNING: docker 不可用,跳过镜像新鲜度检查" >&2
    return 0
  fi
  fp="$(bash "$REPO/hack/build-images.sh" --print-fp 2>/dev/null || true)"
  if [ -z "$fp" ]; then
    log_group "$GROUP" "WARNING: 源码指纹取不到,跳过镜像新鲜度检查" >&2
    return 0
  fi
  images="$(rendered_images)"
  for img in $images; do
    case "$img" in
      ghcr.io/thewor1d877/traincache/*) ;;
      *) continue ;;
    esac
    docker image inspect "$img" >/dev/null 2>&1 || continue  # 本机没有=从仓库拉取,无从判定
    label="$(docker image inspect "$img" --format '{{ index .Config.Labels "io.traincache.src-fp" }}' 2>/dev/null || true)"
    if [ "$label" = "$fp" ]; then
      continue
    fi
    printf '镜像源码指纹不匹配: %s\n  镜像标签 %s != 当前工作树 %s\n' \
      "$img" "${label:-<无标签>}" "$fp" >&2
    stale=1
  done
  if [ "$stale" -ne 0 ]; then
    if [ "${TRAINCACHE_ALLOW_STALE_IMAGE:-0}" = "1" ]; then
      log_group "$GROUP" "WARNING: 存在旧镜像,已按 TRAINCACHE_ALLOW_STALE_IMAGE=1 放行(只用于复现历史产物)" >&2
      return 0
    fi
    cat >&2 <<EOF

控制面镜像比当前源码旧,直接测量会读到一个已经修掉的缺陷。下一步(repo 根):
  ./hack/build-images.sh                 # 重建五个 dev 镜像
  for i in controller-manager webhook kube-scheduler traincache-agentd traincache-gateway; do
    kind load docker-image ghcr.io/thewor1d877/traincache/\$i:dev --name $CLUSTER_NAME
  done
  kubectl -n $NS rollout restart daemonset/traincache-agentd deployment/traincache-controller-manager
复现历史产物要故意用旧镜像时设 TRAINCACHE_ALLOW_STALE_IMAGE=1。
EOF
    return 1
  fi
  log_group "$GROUP" "镜像新鲜度通过(源码指纹 ${fp:0:12})"
}

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
  images="$(rendered_images)"
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
      -o jsonpath='{range .webhooks[*]}{.clientConfig.caBundle}{"\n"}{end}' 2>/dev/null \
      | grep -v '^$' | sort -u || true)"
    # 两条条目的 CA 相同才可直接沿用，不同说明是半旧状态，重建证书。
    if [ "$(printf '%s\n' "$cur_ca" | grep -c .)" = "1" ]; then
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
  # 两条 webhook 条目都要是同一个 CA：checkpoint 条目的 failurePolicy 是 Fail，
  # caBundle 为空时带 traincache.io/checkpoint 标签的 Pod 会被准入拒绝。
  cur="$(kubectl get mutatingwebhookconfiguration traincache-pod-warmup \
    -o jsonpath='{range .webhooks[*]}{.clientConfig.caBundle}{"\n"}{end}' 2>/dev/null | sort -u || true)"
  if [ "$cur" = "$WEBHOOK_CA_B64" ]; then
    log_group "$GROUP" "webhook caBundle 已是最新,跳过"
    return 0
  fi
  kubectl patch mutatingwebhookconfiguration traincache-pod-warmup --type=strategic \
    -p "{\"webhooks\":[{\"name\":\"pods.traincache.io\",\"clientConfig\":{\"caBundle\":\"$WEBHOOK_CA_B64\"}},{\"name\":\"pods-checkpoint.traincache.io\",\"clientConfig\":{\"caBundle\":\"$WEBHOOK_CA_B64\"}}]}" \
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
    && kubectl -n "$NS" get daemonset traincache-agentd >/dev/null 2>&1 \
    && kubectl -n "$NS" get deployment traincache-gateway >/dev/null 2>&1
}

ensure_control_plane() {
  local want cur
  want="$(deploy_hash)"
  cur="$(kubectl -n "$NS" get namespace "$NS" \
    -o jsonpath='{.metadata.annotations.traincache\.io/deploy-hash}' 2>/dev/null || true)"
  if [ "$want" = "$cur" ] && workloads_present; then
    log_group "$GROUP" "控制面清单未变化且五个工作负载都在,跳过 apply(hash=${want:0:12})"
    return 0
  fi
  log_group "$GROUP" "应用控制面清单 kubectl apply -k controller/deploy"
  kubectl apply -k "$DEPLOY_DIR" >/dev/null
  kubectl annotate namespace "$NS" "$HASH_ANNOTATION=$want" --overwrite >/dev/null
  log_group "$GROUP" "控制面已 apply(hash=${want:0:12})"
}

# ---- 网关开关:只切副本数,不动 Deployment 对象 ----
# off 时保留对象(scale 0),否则下次 `kubectl apply -k` 会把它重新拉到 1 副本。
ensure_gateway() {
  local replicas=0
  if ! kubectl -n "$NS" get deployment traincache-gateway >/dev/null 2>&1; then
    if [ "$GATEWAY_ON" = "1" ]; then
      log_group "$GROUP" "错误: deployment/traincache-gateway 不存在,先执行 kubectl apply -k controller/deploy" >&2
      return 1
    fi
    log_group "$GROUP" "WARNING: deployment/traincache-gateway 不存在(旧集群),跳过网关开关" >&2
    return 0
  fi
  if [ "$GATEWAY_ON" = "1" ]; then
    replicas=1
  fi
  kubectl -n "$NS" scale deployment/traincache-gateway --replicas="$replicas" >/dev/null
  log_group "$GROUP" "网关副本数=$replicas (gatewayEndpoint='$WANT_GATEWAY_ENDPOINT')"
  if [ "$GATEWAY_ON" = "1" ]; then
    kubectl -n "$NS" rollout status deployment/traincache-gateway --timeout=120s
  fi
}

# ---- 网关磁盘缓存开关:patch traincache-config 的 gatewayCache ----
# configMapKeyRef 不热生效:改完必须重启网关 Deployment 才吃到新值。
# 值没变就不 patch 也不重启(与 agentd 配置同一口径,重复调用不滚 Pod)。
ensure_gateway_cache() {
  local cur replicas
  if [ -z "$WANT_GATEWAY_CACHE" ]; then
    log_group "$GROUP" "TRAINCACHE_GATEWAY_CACHE 未设置:保持 traincache-config 的 gatewayCache 现状"
    return 0
  fi
  if ! kubectl -n "$NS" get deployment traincache-gateway >/dev/null 2>&1; then
    log_group "$GROUP" "WARNING: deployment/traincache-gateway 不存在,跳过磁盘缓存开关" >&2
    return 0
  fi
  cur="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.gatewayCache}' 2>/dev/null || true)"
  if [ "$cur" = "$WANT_GATEWAY_CACHE" ]; then
    log_group "$GROUP" "网关磁盘缓存开关已是 gatewayCache=$WANT_GATEWAY_CACHE,不改值不重启"
    return 0
  fi
  kubectl -n "$NS" patch configmap traincache-config --type merge \
    -p "{\"data\":{\"gatewayCache\":\"$WANT_GATEWAY_CACHE\"}}" >/dev/null
  log_group "$GROUP" "网关磁盘缓存开关已 patch: gatewayCache=$WANT_GATEWAY_CACHE"
  replicas="$(kubectl -n "$NS" get deployment traincache-gateway -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
  kubectl -n "$NS" rollout restart deployment/traincache-gateway >/dev/null
  if [ "${replicas:-0}" -gt 0 ] && [ "$GATEWAY_ON" = "1" ]; then
    if ! kubectl -n "$NS" rollout status deployment/traincache-gateway --timeout=240s >/dev/null 2>&1; then
      log_group "$GROUP" "错误: 网关滚动更新未在 240s 内就绪(gatewayCache=$WANT_GATEWAY_CACHE)" >&2
      kubectl -n "$NS" get pods -l app.kubernetes.io/name=traincache-gateway -o wide >&2 || true
      return 1
    fi
    log_group "$GROUP" "网关已重启,磁盘缓存开关生效(gatewayCache=$WANT_GATEWAY_CACHE)"
  else
    log_group "$GROUP" "网关本轮无在跑副本:新 Pod 起来时直接读 gatewayCache=$WANT_GATEWAY_CACHE"
  fi
}

# ---- gate 网络带宽上限:patch traincache-config 的 gateNetworkBandwidthBytes ----
# configMapKeyRef 不热生效:改完必须重启 controller-manager Deployment 才吃到新值。
# 值没变就不 patch 也不重启(与 agentd 配置同一口径,重复调用不滚 Pod)。
ensure_gate_bandwidth() {
  local cur
  cur="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.gateNetworkBandwidthBytes}' 2>/dev/null || true)"
  if [ "$cur" = "$GATE_NETWORK_BANDWIDTH" ]; then
    log_group "$GROUP" "gate 网络带宽上限已是 gateNetworkBandwidthBytes=$GATE_NETWORK_BANDWIDTH,不改值不重启"
    return 0
  fi
  kubectl -n "$NS" patch configmap traincache-config --type merge \
    -p "{\"data\":{\"gateNetworkBandwidthBytes\":\"$GATE_NETWORK_BANDWIDTH\"}}" >/dev/null
  log_group "$GROUP" "gate 网络带宽上限已 patch: gateNetworkBandwidthBytes=$GATE_NETWORK_BANDWIDTH"
  kubectl -n "$NS" rollout restart deployment/traincache-controller-manager >/dev/null
  if ! kubectl -n "$NS" rollout status deployment/traincache-controller-manager --timeout=240s >/dev/null 2>&1; then
    log_group "$GROUP" "错误: controller-manager 滚动更新未在 240s 内就绪(gateNetworkBandwidthBytes=$GATE_NETWORK_BANDWIDTH)" >&2
    kubectl -n "$NS" get pods -l app.kubernetes.io/name=traincache-controller-manager -o wide >&2 || true
    return 1
  fi
  log_group "$GROUP" "controller-manager 已重启,带宽上限生效(gateNetworkBandwidthBytes=$GATE_NETWORK_BANDWIDTH)"
}

# ---- 5. agentd 配置(白名单/桶/容量/自动 K 参数/网关端点/预取提前量/淘汰策略)----
# 这六处取值全部在 traincache-config ConfigMap:DaemonSet 只写 configMapKeyRef
# 引用,改值不再动工作负载清单。桶以前由 `kubectl set env` 直接覆盖 DaemonSet 的
# 显式 env,那份覆盖会在下次 `kubectl apply -k` 时被仓库清单盖回去。
# 同一件事也发生在 ConfigMap 上:apply -k 会把整份 data 回到清单里的默认值,所以
# run_bench.py 起跑前写的 prefetchLookahead/evictPolicy 会被 up.sh 盖掉。这里在
# apply 之后按环境变量再对齐一次,COND 的自变量才留得住(4.2 §4 的淘汰矩阵、
# §5.2 的 lookahead 扫点都靠它)。
ensure_agent_config() {
  local cur_wl cur_bucket cur_cap cur_sw cur_mf cur_gw cur_la cur_ep changed=0
  cur_wl="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.datasetMetricsWhitelist}' 2>/dev/null || true)"
  cur_cap="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.cacheCapacityBytes}' 2>/dev/null || true)"
  cur_sw="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.startupWindowBytes}' 2>/dev/null || true)"
  cur_mf="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.maxFillFraction}' 2>/dev/null || true)"
  cur_bucket="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.objstoreBucket}' 2>/dev/null || true)"
  cur_gw="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.gatewayEndpoint}' 2>/dev/null || true)"
  cur_la="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.prefetchLookahead}' 2>/dev/null || true)"
  cur_ep="$(kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data.evictPolicy}' 2>/dev/null || true)"
  if [ "$cur_wl" != "$DATASET" ] || [ "$cur_cap" != "$CAPACITY_BYTES" ] \
     || [ "$cur_sw" != "$STARTUP_WINDOW_BYTES" ] || [ "$cur_mf" != "$MAX_FILL_FRACTION" ] \
     || [ "$cur_bucket" != "$MINIO_BUCKET" ] || [ "$cur_gw" != "$WANT_GATEWAY_ENDPOINT" ] \
     || [ "$cur_la" != "$PREFETCH_LOOKAHEAD" ] || [ "$cur_ep" != "$EVICT_POLICY" ]; then
    log_group "$GROUP" "修正 agentd 配置: WHITELIST=$DATASET BUCKET=$MINIO_BUCKET CACHE_CAPACITY_BYTES=$CAPACITY_BYTES(1GiB) STARTUP_WINDOW_BYTES=$STARTUP_WINDOW_BYTES MAX_FILL_FRACTION=$MAX_FILL_FRACTION GATEWAY_ENDPOINT='$WANT_GATEWAY_ENDPOINT' PREFETCH_LOOKAHEAD=$PREFETCH_LOOKAHEAD EVICT_POLICY=$EVICT_POLICY"
    kubectl -n "$NS" patch configmap traincache-config --type merge \
      -p "{\"data\":{\"datasetMetricsWhitelist\":\"$DATASET\",\"objstoreBucket\":\"$MINIO_BUCKET\",\"cacheCapacityBytes\":\"$CAPACITY_BYTES\",\"startupWindowBytes\":\"$STARTUP_WINDOW_BYTES\",\"maxFillFraction\":\"$MAX_FILL_FRACTION\",\"gatewayEndpoint\":\"$WANT_GATEWAY_ENDPOINT\",\"prefetchLookahead\":\"$PREFETCH_LOOKAHEAD\",\"evictPolicy\":\"$EVICT_POLICY\"}}" >/dev/null
    changed=1
  fi
  if [ "$changed" -eq 1 ]; then
    kubectl -n "$NS" rollout restart daemonset/traincache-agentd >/dev/null
    if ! kubectl -n "$NS" rollout status daemonset/traincache-agentd --timeout=240s >/dev/null 2>&1; then
      log_group "$GROUP" "错误: agentd 滚动更新未在 240s 内就绪(配置 WHITELIST=$DATASET BUCKET=$MINIO_BUCKET CACHE_CAPACITY_BYTES=$CAPACITY_BYTES GATEWAY_ENDPOINT='$WANT_GATEWAY_ENDPOINT' PREFETCH_LOOKAHEAD=$PREFETCH_LOOKAHEAD EVICT_POLICY=$EVICT_POLICY)" >&2
      kubectl -n "$NS" get pods -l app.kubernetes.io/name=traincache-agentd -o wide >&2 || true
      kubectl -n "$NS" get cm traincache-config -o jsonpath='{.data}' >&2 || true
      echo >&2
      return 1
    fi
    log_group "$GROUP" "agentd 配置已修正并滚动更新完成"
  else
    log_group "$GROUP" "agentd 配置正确(WHITELIST=$DATASET BUCKET=$MINIO_BUCKET CACHE_CAPACITY_BYTES=$CAPACITY_BYTES GATEWAY_ENDPOINT='$WANT_GATEWAY_ENDPOINT' PREFETCH_LOOKAHEAD=$PREFETCH_LOOKAHEAD EVICT_POLICY=$EVICT_POLICY)"
  fi
}

wait_ready() {
  local entry kind name ns label
  local targets=(
    "deployment traincache-controller-manager $NS"
    "deployment traincache-webhook $NS"
    "deployment traincache-scheduler kube-system"
    "daemonset traincache-agentd $NS"
  )
  if [ "$GATEWAY_ON" = "1" ]; then
    targets+=("deployment traincache-gateway $NS")
  fi
  for entry in "${targets[@]}"; do
    read -r kind name ns <<<"$entry"
    label="$kind/$name($ns)"
    if ! kubectl -n "$ns" rollout status "$kind/$name" --timeout=240s >/dev/null 2>&1; then
      log_group "$GROUP" "错误: $label 未就绪" >&2
      kubectl -n "$ns" get "$kind" "$name" -o jsonpath='{.status.conditions}' >&2 || true
      echo >&2
      kubectl -n "$ns" get pods -l "app.kubernetes.io/name=$name" -o wide >&2 || true
      return 1
    fi
    log_group "$GROUP" "$label 已就绪"
  done
}

# ---- 6. 结束打印 Ready 数与 Pod UID,供校验无滚动重启 ----
print_status() {
  local kind name ns desired ready uids entries
  echo "== 控制面状态 =="
  entries=(
    "deployment traincache-controller-manager $NS"
    "deployment traincache-webhook $NS"
    "deployment traincache-scheduler kube-system"
    "daemonset traincache-agentd $NS"
  )
  if [ "$GATEWAY_ON" = "1" ]; then
    entries+=("deployment traincache-gateway $NS")
  fi
  for entry in "${entries[@]}"; do
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

assert_context
ensure_ns "$NS" "$GROUP"
label_s3_nodes
check_images
check_image_freshness
ensure_objstore_secret
ensure_webhook_tls
ensure_control_plane
ensure_gateway_cache
ensure_gateway
ensure_ca_bundle
ensure_gate_bandwidth
ensure_agent_config
wait_ready
print_status
log_group "$GROUP" "TrainCache 控制面就绪(gateway=$GATEWAY_ON gatewayCache=${WANT_GATEWAY_CACHE:-<未设置,保持现状>} gateNetworkBandwidthBytes=$GATE_NETWORK_BANDWIDTH)"
