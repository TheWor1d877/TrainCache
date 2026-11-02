#!/usr/bin/env bash
# render-install.sh — 把发布清单里的五个镜像后缀换成 digest 后渲染成单文件。
#
# 用法:
#   ./hack/render-install.sh <vX.Y.Z> <d_manager> <d_webhook> <d_scheduler> <d_agent> <d_gateway> [output]
#
# digest 形如 sha256:<64 hex>。不改组件 YAML，也不写死 tag。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REG="ghcr.io/thewor1d877/traincache"

if [[ $# -lt 6 ]]; then
  echo "usage: $0 <vX.Y.Z> <d_manager> <d_webhook> <d_scheduler> <d_agent> <d_gateway> [output]" >&2
  exit 2
fi

VER="$1" D_MANAGER="$2" D_WEBHOOK="$3" D_SCHEDULER="$4" D_AGENT="$5" D_GATEWAY="$6"
OUT="${7:-install.yaml}"

[[ "$VER" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "版本不是 vX.Y.Z: $VER" >&2; exit 2; }
for pair in "manager:$D_MANAGER" "webhook:$D_WEBHOOK" "scheduler:$D_SCHEDULER" "agent:$D_AGENT" "gateway:$D_GATEWAY"; do
  name="${pair%%:*}" dg="${pair#*:}"
  [[ "$dg" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "$name 的 digest 非法: $dg" >&2; exit 2; }
done

# kustomize 不接受绝对路径的 resources，overlay 必须建在仓库内。
tmp="$(mktemp -d "$ROOT/.render-install-XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

cat >"$tmp/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../controller/deploy
images:
  - name: $REG/controller-manager
    newName: $REG/controller-manager
    digest: $D_MANAGER
  - name: $REG/webhook
    newName: $REG/webhook
    digest: $D_WEBHOOK
  - name: $REG/kube-scheduler
    newName: $REG/kube-scheduler
    digest: $D_SCHEDULER
  - name: $REG/traincache-agentd
    newName: $REG/traincache-agentd
    digest: $D_AGENT
  - name: $REG/traincache-gateway
    newName: $REG/traincache-gateway
    digest: $D_GATEWAY
EOF

kubectl kustomize "$tmp" >"$OUT"

bad=0
if grep -q ':dev' "$OUT"; then
  echo "渲染结果仍含 :dev 标签" >&2
  bad=1
fi
for pair in "controller-manager:$D_MANAGER" "webhook:$D_WEBHOOK" "kube-scheduler:$D_SCHEDULER" "traincache-agentd:$D_AGENT" "traincache-gateway:$D_GATEWAY"; do
  comp="${pair%%:*}" dg="${pair#*:}"
  grep -q "$REG/$comp@$dg" "$OUT" || { echo "缺少 $comp@$dg" >&2; bad=1; }
done
[[ "$bad" -eq 0 ]] || exit 1

echo "rendered $OUT ($VER)"
