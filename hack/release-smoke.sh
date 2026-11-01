#!/usr/bin/env bash
# release-smoke.sh — 在临时 kind 集群里安装已经发布的 install.yaml。
#
# 验证的是发布物，不是源码：镜像从 GHCR 拉，清单用 digest 引用。
#
# 用法:
#   ./hack/release-smoke.sh <install.yaml> [vX.Y.Z]
#
# 环境变量:
#   CLUSTER=traincache-smoke   kind 集群名
#   NODE_IMAGE                 kind 节点镜像，默认 kindest/node:v1.37.0
#   KEEP=1                     保留集群，排障用
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL="${1:-}"
TAG="${2:-unknown}"
CLUSTER="${CLUSTER:-traincache-smoke}"
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.37.0}"
DATA_ROOT="${TRAINCACHE_DATA_ROOT:-$(cd "$ROOT/.." && pwd)/traincachedata}"
OUT="$DATA_ROOT/hack/release-smoke"
TLS="$OUT/tls"
NS=traincache
SVC="traincache-webhook.$NS.svc"

FAILS=0
ok()   { printf '  [OK]   %s\n' "$*"; }
info() { printf '  [INFO] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILS=$((FAILS + 1)); }

cleanup() {
  if [[ "${KEEP:-0}" == "1" ]]; then
    info "KEEP=1，保留集群 $CLUSTER"
    return
  fi
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

[[ -n "$INSTALL" && -f "$INSTALL" ]] || { echo "usage: $0 <install.yaml> [tag]" >&2; exit 2; }
for t in docker kind kubectl openssl base64; do
  command -v "$t" >/dev/null 2>&1 || { echo "missing tool: $t" >&2; exit 2; }
done

mkdir -p "$OUT" "$TLS"

printf '\n== kind cluster\n'
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --wait 180s >/dev/null \
  && ok "cluster $CLUSTER" || { fail "kind create"; exit 1; }

printf '\n== install %s\n' "$TAG"
kubectl apply -f "$INSTALL" >/dev/null && ok "install.yaml applied" || fail "kubectl apply"

printf '\n== objstore credential\n'
# agentd 的 DaemonSet 要求同命名空间存在凭证 Secret；smoke 不读写块数据，
# 值非空即可让容器启动。
kubectl -n "$NS" create secret generic traincache-objstore \
  --from-literal=accessKey=smoke --from-literal=secretKey=smoke \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null \
  && ok "objstore secret" || fail "objstore secret"

printf '\n== webhook TLS\n'
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -keyout "$TLS/ca.key" -out "$TLS/ca.crt" -subj "/CN=traincache-smoke-ca" >/dev/null 2>&1 \
  || fail "generate CA"
openssl req -newkey rsa:2048 -nodes -keyout "$TLS/tls.key" -out "$TLS/tls.csr" \
  -subj "/CN=$SVC" >/dev/null 2>&1 || fail "generate CSR"
printf 'subjectAltName=DNS:%s,DNS:%s.%s.svc.cluster.local\n' "$SVC" traincache-webhook "$NS" >"$TLS/san.ext"
openssl x509 -req -in "$TLS/tls.csr" -CA "$TLS/ca.crt" -CAkey "$TLS/ca.key" \
  -CAcreateserial -out "$TLS/tls.crt" -days 2 -extfile "$TLS/san.ext" >/dev/null 2>&1 \
  || fail "sign serving cert"
kubectl -n "$NS" create secret tls traincache-webhook-tls \
  --cert="$TLS/tls.crt" --key="$TLS/tls.key" --dry-run=client -o yaml | kubectl apply -f - >/dev/null \
  && ok "TLS secret" || fail "TLS secret"
ca_b64="$(base64 -w0 "$TLS/ca.crt")"
kubectl patch mutatingwebhookconfiguration traincache-pod-warmup --type=json \
  -p "[{\"op\":\"replace\",\"path\":\"/webhooks/0/clientConfig/caBundle\",\"value\":\"$ca_b64\"}]" >/dev/null \
  && ok "caBundle patched" || fail "patch caBundle"

printf '\n== CRDs\n'
kubectl wait --for=condition=Established --timeout=120s \
  crd/datasets.traincache.io crd/warmuptasks.traincache.io >/dev/null \
  && ok "CRDs established" || fail "CRDs not established"

printf '\n== workloads\n'
kubectl -n "$NS" rollout status deploy/traincache-controller-manager --timeout=300s >/dev/null \
  && ok "controller-manager" || fail "controller-manager rollout"
kubectl -n "$NS" rollout status deploy/traincache-webhook --timeout=300s >/dev/null \
  && ok "webhook" || fail "webhook rollout"
kubectl -n kube-system rollout status deploy/traincache-scheduler --timeout=300s >/dev/null \
  && ok "scheduler" || fail "scheduler rollout"
kubectl -n "$NS" rollout status ds/traincache-agentd --timeout=300s >/dev/null \
  && ok "agentd" || fail "agentd rollout"
kubectl -n "$NS" rollout status deploy/traincache-gateway --timeout=300s >/dev/null \
  && ok "gateway" || fail "gateway rollout"

printf '\n== image and endpoint\n'
imgs="$(kubectl -n "$NS" get pods -o jsonpath='{range .items[*]}{.spec.containers[*].image}{"\n"}{end}' 2>/dev/null)"
if grep -q "^ghcr.io/thewor1d877/traincache/" <<<"$imgs"; then
  ok "pods run ghcr.io images"
else
  fail "pods are not running ghcr.io images"
fi
kubectl -n "$NS" get pods >"$OUT/pods.txt" 2>&1
if [[ -n "$(kubectl -n "$NS" get endpoints traincache-webhook -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null)" ]]; then
  ok "webhook endpoints registered"
else
  fail "webhook service has no endpoints"
fi

if [[ "$FAILS" -ne 0 ]]; then
  printf '\n== diagnostics\n'
  kubectl -n "$NS" get pods -o wide 2>&1
  kubectl -n "$NS" get events --sort-by=.lastTimestamp 2>&1 | tail -20
  kubectl -n kube-system get pods -o wide 2>&1 | grep traincache || true
fi

printf '\nrelease-smoke %s: %d failure(s)\n' "$TAG" "$FAILS"
exit $((FAILS > 0))
