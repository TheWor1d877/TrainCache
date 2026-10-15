# traincache-ordersrc:ConsumeOrder 最小写端 sidecar 镜像(design 7.1 §8 训练 Pod 侧;
# 脚本 agent/ordersrc/ordersrc.py,D22 冻结 ring 布局,仅标准库依赖)。
#
# 构建(仓库根为构建上下文):
#   docker build -f agent/deploy/ordersrc.Dockerfile -t traincache-ordersrc:dev .
#
# 单入口 `python3 /opt/traincache/ordersrc.py`(--help 可见全参数表);写入节奏
# (单次/循环)与参数(pod-uid/dataset/chunk 清单)由 Pod manifest 决定,见
# agent/deploy/train-pod.yaml 的 sidecar 容器。
FROM python:3.12-alpine

COPY agent/ordersrc/ordersrc.py /opt/traincache/ordersrc.py

# 只写宿主经 hostPath 挂载的 /run/traincache/orders(§6.2、§8),镜像内不写路径,
# 兼容 readOnlyRootFilesystem=true。
ENTRYPOINT ["python3", "/opt/traincache/ordersrc.py"]
