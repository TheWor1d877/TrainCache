#!/usr/bin/env bash
# verify-release-metadata.sh — 仓库内的版本引用必须与"tag 是唯一来源"一致。
# 检查项见 docs/RELEASE-CONSTRAINTS.md。只在 PR dev→main 与发版流水线里跑。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 2

bad=0
fail() { printf '  [FAIL] %s\n' "$*"; bad=1; }

# 1. 部署清单里的镜像只能带 dev 标签或 digest。observability 是 dev-only，不参与。
images="$(grep -rh --exclude-dir=observability -E '^[[:space:]]*image:' controller/deploy \
  | sed -E 's/^[[:space:]]*image:[[:space:]]*//' | sort -u)"
[[ "$(printf '%s\n' "$images" | grep -c .)" -ge 5 ]] || fail "controller/deploy 中镜像少于 5 个"
while IFS= read -r img; do
  [[ -n "$img" ]] || continue
  case "$img" in
    *@sha256:*) ;;
    *:dev) ;;
    *) fail "镜像未使用 dev 标签或 digest: $img" ;;
  esac
done <<<"$images"

# 2. kustomization 的 images 必须覆盖全部部署组件，且发布前都指向 dev。
#    checkpointd 由准入按 Pod 注入，不进 kustomization，因此这五个组件的
#    循环保持五行；它的取值放在 traincache-checkpoint-image ConfigMap 里，
#    单独在下面校验，它的 Dockerfile/main 在第 3、4 节校验。
for comp in controller-manager webhook kube-scheduler traincache-agentd traincache-gateway; do
  if ! grep -A1 -E "name: .*/traincache/${comp}$" controller/deploy/kustomization.yaml \
      | grep -q 'newTag: dev'; then
    fail "kustomization images 缺少 $comp 或未指向 dev"
  fi
done

# 2b. checkpointd 的镜像取值必须由 ConfigMap 承载，且只能是 dev 标签或 digest。
ckpt_image="$(awk '/^[[:space:]]*checkpointImage:/ {print $2; exit}' \
  controller/deploy/webhook/checkpoint-image.yaml)"
[[ -n "$ckpt_image" ]] || fail "checkpoint-image.yaml 缺少 checkpointImage 键"
case "$ckpt_image" in
  *@sha256:*) ;;
  *:dev) ;;
  *) fail "checkpointd 镜像未使用 dev 标签或 digest: $ckpt_image" ;;
esac
grep -q 'name: CHECKPOINT_IMAGE' controller/deploy/webhook/deployment.yaml \
  || fail "webhook Deployment 没有读 CHECKPOINT_IMAGE"
grep -q 'name: traincache-checkpoint-image' controller/deploy/webhook/deployment.yaml \
  || fail "webhook Deployment 的 CHECKPOINT_IMAGE 没引用 traincache-checkpoint-image"

# 3. 组件清单从五变六：五个部署组件加 checkpointd（由准入注入的 sidecar）。
#    Dockerfile 不设镜像站参数,VERSION 默认 dev 并由 ldflags 注入。
dockerfiles=(
  agent/Dockerfile
  checkpoint/Dockerfile
  controller/deploy/manager/Dockerfile
  controller/deploy/webhook/Dockerfile
  controller/deploy/scheduler/Dockerfile
  gateway/Dockerfile
)
mains=(
  agent/cmd/agentd/main.go
  checkpoint/cmd/checkpointd/main.go
  controller/cmd/controller-manager/main.go
  controller/cmd/webhook/main.go
  controller/cmd/scheduler/main.go
  gateway/cmd/gatewayd/main.go
)
# 组件数门槛：checkpointd 不进 kustomization，但仍是随版本构建的第六个组件，
# 两条清单都必须覆盖它，避免新增组件只改了 build-images.sh。
[[ ${#dockerfiles[@]} -ge 6 && ${#mains[@]} -ge 6 ]] \
  || fail "组件少于 6 个（Dockerfile ${#dockerfiles[@]}，main ${#mains[@]}）"

for df in "${dockerfiles[@]}"; do
  ! grep -q 'ARG REGISTRY' "$df" || fail "$df: 不应有镜像站参数,基础镜像写官方短名"
  grep -qx 'ARG VERSION=dev' "$df" || fail "$df: ARG VERSION 默认值不是 dev"
  grep -q -- '-X main.version=\${VERSION}' "$df" || fail "$df: 未用 ldflags 注入 main.version"
done

# 4. 六个 main 包必须暴露可被 -X 覆盖的 version 变量。
for f in "${mains[@]}"; do
  grep -qx 'var version = "dev"' "$f" || fail "$f: 缺少 var version = \"dev\""
done

# 5. CHANGELOG 的版本标题：唯一、严格降序、日期为 YYYY-MM-DD。代码块里的示例不参与。
#    未发布小节允许存在（`## 未发布` 或 `## [未发布]`），但至多一个且必须排在最前——发版时由人把它改成 vX.Y.Z。
[[ -f CHANGELOG.md ]] || fail "缺少 CHANGELOG.md"
vers="$(awk '
  /^[[:space:]]*```/ { fence = !fence; next }
  fence { next }
  /^## (\[未发布\]|未发布)$/ { next }
  /^## \[/ { print }
' CHANGELOG.md \
  | sed -nE 's/^## \[(v[0-9]+\.[0-9]+\.[0-9]+)\] - ([0-9]{4}-[0-9]{2}-[0-9]{2})$/\1 \2/p')"

headings="$(awk '
  /^[[:space:]]*```/ { fence = !fence; next }
  fence { next }
  /^## (\[未发布\]|未发布)$/ { next }
  /^## \[/ { print }
' CHANGELOG.md | wc -l)"
[[ "$headings" -eq "$(printf '%s\n' "$vers" | grep -c .)" ]] \
  || fail "CHANGELOG 存在不符合 '## [vX.Y.Z] - YYYY-MM-DD' 的版本标题"

unreleased="$(awk '
  /^[[:space:]]*```/ { fence = !fence; next }
  fence { next }
  /^## (\[未发布\]|未发布)$/ { print }
' CHANGELOG.md | wc -l)"
if [[ "$unreleased" -gt 1 ]]; then
  fail "CHANGELOG 有 $unreleased 个未发布小节"
fi
if [[ "$unreleased" -eq 1 ]]; then
  # 只看未发布与版本标题这两类,说明区自己的 ## 标题不参与排序判定。
  first_heading="$(awk '
    /^[[:space:]]*```/ { fence = !fence; next }
    fence { next }
    /^## (\[未发布\]|未发布)$/ { print; exit }
    /^## \[/ { print; exit }
  ' CHANGELOG.md)"
  case "$first_heading" in
    '## [未发布]' | '## 未发布') ;;
    *) fail "未发布小节必须排在所有版本标题之前" ;;
  esac
fi

dups="$(printf '%s\n' "$vers" | awk '{print $1}' | sort | uniq -d)"
[[ -z "$dups" ]] || fail "CHANGELOG 版本标题重复: $dups"

ordered="$(printf '%s\n' "$vers" | awk '{print $1}' | sort -rV)"
current="$(printf '%s\n' "$vers" | awk 'NF{print $1}')"
[[ "$ordered" == "$current" ]] || fail "CHANGELOG 版本标题未按版本号降序排列"

# 6. 清单里只允许 ghcr.io 或官方公开源,禁止任何私有 registry 主机名。
while IFS= read -r img; do
  [[ -n "$img" ]] || continue
  host="${img%%/*}"
  [[ "$host" == "$img" ]] && continue
  [[ "$host" == *.* ]] || continue
  [[ "$host" == ghcr.io ]] && continue
  fail "镜像带私有 registry: $img"
done < <(grep -rh --exclude-dir=observability -E '^[[:space:]]*image:' \
  controller/deploy controller/e2e/manifests \
  | sed -E 's/^[[:space:]]*image:[[:space:]]*//' | sort -u)

# 7. 版本与 tag 对齐：HEAD 上的 v* tag 必须在 CHANGELOG 有条目；
#    发版流水线传 TAG 时，该版本还必须是首条（最新）。
tag_at_head="$(git tag --points-at HEAD --list 'v[0-9]*' 2>/dev/null | sort -V | tail -1 || true)"
if [[ -n "$tag_at_head" ]]; then
  printf '%s\n' "$vers" | grep -q "^$tag_at_head " || fail "CHANGELOG 缺少 HEAD tag $tag_at_head 的条目"
fi
if [[ -n "${TAG:-}" ]]; then
  [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "TAG 不是 vX.Y.Z: $TAG"
  printf '%s\n' "$vers" | grep -q "^$TAG " || fail "CHANGELOG 缺少 $TAG 条目"
  first="$(printf '%s\n' "$vers" | awk 'NF{print $1; exit}')"
  [[ "$first" == "$TAG" ]] || fail "CHANGELOG 首条是 ${first:-空}，不是 $TAG"
fi

exit "$bad"
