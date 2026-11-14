# traincache-trainer:dev — e2e 块读训练镜像(数据面)。
# 构建上下文由 build-trainer.sh 组装(不在仓库内落大产物):
#   app/           load_train.py blockread.py ordersrc.py config.py
#   site-packages/ 宿主机 .venv site-packages + ~/.local 的 torch 及依赖
#                  + 系统 dist-packages 的 typing_extensions/jinja2/markupsafe/
#                    setuptools(torch 的 import 依赖,离线拷入免 pip 网络)
# torch 2.14.1+cpu 是 manylinux_2_28 wheel,glibc 的 python:3.14-slim 可直接用,
# 且与宿主机 .venv 的 Python 3.14  ABI 一致。
FROM python:3.14-slim

COPY site-packages/ /usr/local/lib/python3.14/site-packages/
COPY app/ /app/
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    CACHE_ROOT=/var/cache/traincache/blocks \
    ORDERS_DIR=/run/traincache/orders \
    TRAINCACHE_HOOK_SOCK=/run/tc-hook/agent.sock

# 离线拷贝失效时的兜底(需网络,已验证镜像源可达):
# RUN pip install -i https://mirrors.aliyun.com/pypi/simple/ \
#     webdataset==1.0.2 numpy pillow && \
#     pip install torch --index-url https://mirrors.aliyun.com/pypi/simple/

CMD ["python3", "/app/load_train.py"]