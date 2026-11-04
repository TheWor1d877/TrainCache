#!/usr/bin/env bash
# build-images.sh — 构建全部开发镜像（tag 默认 dev）。
#
# 用法:
#   ./hack/build-images.sh                  # manager webhook scheduler agent gateway checkpoint
#   ./hack/build-images.sh manager agent    # 只构建指定组件
#   TAG=v0.1.0 ./hack/build-images.sh
#   ./hack/build-images.sh --prepare-only   # 只准备基础镜像，不构建（供 e2e 预检）
#   ./hack/build-images.sh --print-fp       # 只打印当前源码指纹（不构建，供镜像新鲜度门禁）
#
# 每个镜像都打上 io.traincache.src-fp 标签，值是构建时源码指纹。热集群里
# :dev 标签会被反复复用，容器只比"镜像在不在"就会拿旧二进制参与测量——
# bench/scripts/ctrl_up.sh 用这个标签判定新鲜度。
#
# 构建器按 Dockerfile 里的官方短名解析基础镜像。本机镜像只有带镜像站前缀的
# 名字时，构建器会改走远端拉取，慢网络下长时间停在拉取上。脚本先用
# docker tag 把本机副本补成官方短名，不产生网络访问；两种名字都没有时立即
# 报错并打印处理命令，不发起拉取。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REG="ghcr.io/thewor1d877/traincache"
TAG="${TAG:-dev}"

declare -A DF=(
  [manager]="$ROOT/controller/deploy/manager/Dockerfile"
  [webhook]="$ROOT/controller/deploy/webhook/Dockerfile"
  [scheduler]="$ROOT/controller/deploy/scheduler/Dockerfile"
  [agent]="$ROOT/agent/Dockerfile"
  [gateway]="$ROOT/gateway/Dockerfile"
  [checkpoint]="$ROOT/checkpoint/Dockerfile"
)
declare -A IMG=(
  [manager]="$REG/controller-manager"
  [webhook]="$REG/webhook"
  [scheduler]="$REG/kube-scheduler"
  [agent]="$REG/traincache-agentd"
  [gateway]="$REG/traincache-gateway"
  [checkpoint]="$REG/traincache-checkpointd"
)

prepare_only=0
print_fp=0
components=()
for arg in "$@"; do
  case "$arg" in
    --prepare-only) prepare_only=1 ;;
    --print-fp) print_fp=1 ;;
    -*)
      echo "未知参数: $arg" >&2
      exit 2
      ;;
    *)
      [[ -n "${DF[$arg]:-}" ]] || { echo "未知组件: $arg（可选 manager webhook scheduler agent gateway checkpoint）" >&2; exit 2; }
      components+=("$arg")
      ;;
  esac
done
[[ ${#components[@]} -gt 0 ]] || components=(manager webhook scheduler agent gateway checkpoint)

# 源码指纹：进镜像的那批文件的内容哈希。全部组件共用同一个值——构建上下文
# （go.work + agent/core/controller/gateway/checkpoint）本来就整份送进每个
# Dockerfile，分组件算指纹只会让门禁漏判（改了 core 却只标 agent 过期）。
src_fingerprint() {
  find "$ROOT/go.work" "$ROOT/go.work.sum" \
       "$ROOT/agent" "$ROOT/core" "$ROOT/controller" "$ROOT/gateway" "$ROOT/checkpoint" \
       -type f \( -name '*.go' -o -name 'go.mod' -o -name 'go.sum' \
                  -o -name 'Dockerfile*' -o -name '*.Dockerfile' \) \
       ! -name '*_test.go' -print0 2>/dev/null \
    | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | cut -d' ' -f1
}

if [[ "$print_fp" == 1 ]]; then
  printf '%s\n' "$(src_fingerprint)"
  exit 0
fi

# 取 FROM 行引用的基础镜像，跳过阶段别名（如 FROM build）。
base_refs() {
  awk '
    /^FROM[ \t]+/ {
      if ($2 in alias) next
      print $2
      if ($3 == "AS" || $3 == "as") alias[$4] = 1
    }' "$@"
}

# 本机镜像里找同一基础镜像的副本：官方镜像在镜像站下多一层 library/。
find_local_alias() {
  local ref="$1" name="${1%:*}" tag="${1##*:}" line
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    if [[ "$name" == */* ]]; then
      [[ "$line" == *"/$name:$tag" ]] && { printf '%s\n' "$line"; return 0; }
    else
      [[ "$line" == *"/library/$name:$tag" ]] && { printf '%s\n' "$line"; return 0; }
    fi
  done < <(docker image ls --format '{{.Repository}}:{{.Tag}}')
  return 1
}

declare -A seen=()
missing=()
local_ref=""
for comp in "${components[@]}"; do
  while IFS= read -r ref; do
    [[ -n "$ref" ]] || continue
    if [[ -n "${seen[$ref]:-}" ]]; then
      continue
    fi
    seen[$ref]=1
    if docker image inspect "$ref" >/dev/null 2>&1; then
      continue
    fi
    if [[ "$ref" == *@sha256:* ]]; then
      missing+=("$ref")
      continue
    fi
    if local_ref="$(find_local_alias "$ref")"; then
      docker tag "$local_ref" "$ref"
      printf '基础镜像 %s 复用本机副本 %s（docker tag，无网络访问）\n' "$ref" "$local_ref"
    else
      missing+=("$ref")
    fi
  done < <(base_refs "${DF[$comp]}")
done

if [[ ${#missing[@]} -gt 0 ]]; then
  if [[ "$prepare_only" == 1 ]]; then
    printf '基础镜像不在本机: %s（交给构建器从远端拉取）\n' "${missing[*]}" >&2
    exit 0
  fi
  echo "以下基础镜像不在本机，也没有可改名的本地副本:" >&2
  for ref in "${missing[@]}"; do
    printf '  %s\n    docker pull %s\n' "$ref" "$ref" >&2
    if [[ "$ref" != *@sha256:* ]]; then
      name="${ref%:*}" tag="${ref##*:}"
      if [[ "$name" == */* ]]; then
        printf '    docker tag <镜像站>/%s:%s %s\n' "$name" "$tag" "$ref" >&2
      else
        printf '    docker tag <镜像站>/library/%s:%s %s\n' "$name" "$tag" "$ref" >&2
      fi
    fi
  done
  exit 1
fi

if [[ "$prepare_only" == 1 ]]; then
  exit 0
fi

# 与 e2e 一致：只把 go.work 与全部模块送进构建上下文，不带 .git/docs/bench。
stage="$(mktemp -d "${TMPDIR:-/tmp}/traincache-build.XXXXXX")"
trap 'rm -rf "$stage"' EXIT
cp -a "$ROOT/go.work" "$ROOT/go.work.sum" "$ROOT/agent" "$ROOT/core" "$ROOT/controller" "$ROOT/gateway" "$ROOT/checkpoint" "$stage/"

for comp in "${components[@]}"; do
  printf '==> 构建 %s: %s:%s\n' "$comp" "${IMG[$comp]}" "$TAG"
  docker build -f "${DF[$comp]}" \
    --build-arg VERSION="$TAG" \
    --label "io.traincache.src-fp=$(src_fingerprint)" \
    -t "${IMG[$comp]}:$TAG" "$stage"
done
