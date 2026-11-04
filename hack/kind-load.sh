#!/usr/bin/env bash
# kind-load.sh — 把本机已有的镜像载入 kind 集群的所有节点。
#
# 用法:
#   ./hack/kind-load.sh <cluster> <image> [image...]
#   ./hack/kind-load.sh traincache-e2e pgsty/minio:RELEASE.2026-08-04T00-00-00Z busybox:1.36.1
#
# 为什么不直接用 kind load docker-image：Docker 29 的全新安装默认启用 containerd
# 镜像库，升级安装才继续留在 overlay2，所以开发机上看不出差别，装了 Docker 29 的
# CI runner 上才会撞。该库里 docker pull 只把本机平台的 blob 落盘，tag 仍指向完整
# 的多平台 index；kind load docker-image 用 docker save 照 index 导出，归档里留着
# 本机没有的那些平台的引用，节点侧 ctr images import --all-platforms 随即报
# "content digest sha256:...: not found"。docker save --platform 只导本机平台，
# index 里没有悬空引用，两种存储后端下都能导入。
#
# 归档写在临时目录，退出时删除；节点里的镜像名与本机 tag 一致。kind 的
# image-archive 与 docker-image 两条路径在节点侧执行的是同一条 ctr images import。
#
# 退出码: 0 全部镜像已载入；1 参数不对、工具缺失或某个镜像载入失败。
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <cluster> <image> [image...]" >&2
  exit 1
fi
CLUSTER="$1"
shift

for tool in docker kind; do
  command -v "$tool" >/dev/null 2>&1 || { echo "ERROR: 缺 $tool" >&2; exit 1; }
done

# 旧版 docker 的 docker save 没有 --platform，退回不过滤平台的导出。
supports_platform=0
if docker save --help 2>&1 | grep -q -- '--platform'; then
  supports_platform=1
fi

workdir="$(mktemp -d "${TMPDIR:-/tmp}/traincache-kindload.XXXXXX")"
trap 'rm -rf "$workdir"' EXIT

rc=0
for image in "$@"; do
  short="$(printf '%s' "$image" | tr '/:@' '___')"
  tar="$workdir/$short.tar"

  if (( supports_platform )); then
    platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")" \
      || { echo "FAIL inspect $image" >&2; rc=1; continue; }
    docker save --platform "$platform" -o "$tar" "$image" \
      || { echo "FAIL docker save --platform $platform $image" >&2; rc=1; continue; }
  else
    docker save -o "$tar" "$image" \
      || { echo "FAIL docker save $image" >&2; rc=1; continue; }
  fi

  kind load image-archive --name "$CLUSTER" "$tar" \
    || { echo "FAIL kind load image-archive $image -> $CLUSTER" >&2; rc=1; continue; }
  echo "loaded $image -> $CLUSTER"
done

exit $rc
