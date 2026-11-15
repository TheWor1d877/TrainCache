#!/usr/bin/env bash
# 构建 traincache-trainer:dev 并载入 kind(traincache)。可重跑:上下文每次
# 重新组装到 /tmp 临时目录,退出即清理;镜像与 kind 载入都是覆盖式。
# 离线优先:直接拷宿主机已装好的 site-packages(torch 在 ~/.local,依赖散在
# 用户 site 与系统 dist-packages),不走 pip 网络。
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
VENV_SP="$ROOT/.venv/lib/python3.14/site-packages"
USER_SP="$HOME/.local/lib/python3.14/site-packages"
SYS_SP="/usr/lib/python3/dist-packages"
IMAGE=traincache-trainer:dev

[ -d "$VENV_SP" ] || { echo "缺 $VENV_SP(先在宿主机建 .venv)" >&2; exit 1; }
[ -d "$USER_SP/torch" ] || { echo "缺 $USER_SP/torch" >&2; exit 1; }

CTX=$(mktemp -d /tmp/tc-trainer-ctx.XXXXXX)
trap 'rm -rf "$CTX"' EXIT
mkdir -p "$CTX/app" "$CTX/site-packages"

cp "$ROOT/bench/scripts/load_train.py" "$HERE/blockread.py" \
   "$ROOT/agent/ordersrc/ordersrc.py" "$ROOT/bench/config.py" "$CTX/app/"

cp -a "$VENV_SP/." "$CTX/site-packages/"
# torch 及其 import 依赖(用户 site);dist-info 一并拷入保留版本元数据。
for p in torch torch-*.dist-info torchgen functorch \
         filelock filelock-*.dist-info fsspec fsspec-*.dist-info \
         sympy sympy-*.dist-info mpmath mpmath-*.dist-info \
         networkx networkx-*.dist-info isympy.py; do
    for src in "$USER_SP/"$p; do
        [ -e "$src" ] && cp -a "$src" "$CTX/site-packages/"
    done
done
# typing_extensions/jinja2/markupsafe/setuptools 在系统 dist-packages
# (markupsafe 的 _speedups 是 cp314 manylinux 产物,slim 同为 glibc 可用)。
for p in typing_extensions.py typing_extensions-*.dist-info \
         jinja2 jinja2-*.dist-info markupsafe markupsafe-*.dist-info \
         setuptools setuptools-*.egg-info pkg_resources; do
    for src in "$SYS_SP/"$p; do
        [ -e "$src" ] && cp -a "$src" "$CTX/site-packages/"
    done
done

docker build -t "$IMAGE" -f "$HERE/Dockerfile" "$CTX"
docker run --rm "$IMAGE" python3 -c \
    "import torch, webdataset, numpy, PIL; print('torch', torch.__version__, '| webdataset', webdataset.__version__, '| numpy', numpy.__version__, '| Pillow', PIL.__version__)"
kind load docker-image "$IMAGE" --name traincache
echo "OK: $IMAGE 已构建并载入 kind(traincache)"