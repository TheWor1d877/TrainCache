# syntax=docker/dockerfile:1
#
# traincache-agentd 节点镜像(DaemonSet 入口;design 7.1 §8:镜像入口 traincache-agentd、单容器)。
#
# 构建(仓库根为构建上下文:go.work 同时 use ./agent、./core、./controller,
# 三个模块目录都需入上下文):
#   docker build -f agent/Dockerfile -t <registry>/traincache/traincache-agentd:0.8.3 .
#
# 镜像内容:agentd 服务二进制(ENTRYPOINT)+ traincache CLI(探针与运维共用;本机无 grpcurl,
# liveness/readiness 走 exec 调 CLI,判据见 agent/deploy/agentd.yaml 注释与 DECISIONS)。
# 构建口径:CGO_ENABLED=0 静态构建(§8 部署要点);运行面仅依赖 hostPath 两个目录
# (/var/cache/traincache、/run/traincache),镜像内不写任何路径——兼容
# readOnlyRootFilesystem=true 安全上下文;以 root 运行是 hostPath 目录
# (kubelet 以 root:root 创建)写权限的授予方式,不经 USER 切换。
#
# 基础镜像统一走专属镜像域名(k8s/containerd 不读 daemon.json;tag 固定,禁用 latest)。
ARG REGISTRY=dgl3i4ucowsgvvdwty.xuanyuan.run

FROM ${REGISTRY}/library/golang:1.26-alpine AS build

# GOPROXY 默认 goproxy.cn,受限网络可用 --build-arg GOPROXY=https://proxy.golang.org,direct 覆盖。
ARG GOPROXY=https://goproxy.cn,direct
ENV CGO_ENABLED=0 \
    GOPROXY=${GOPROXY}

WORKDIR /src
COPY go.work go.work.sum ./
COPY core/ core/
COPY agent/ agent/
COPY controller/ controller/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd agent \
 && go build -o /out/traincache-agentd ./cmd/agentd \
 && go build -o /out/traincache ./cmd/traincache

FROM ${REGISTRY}/library/alpine:3.24

COPY --from=build /out/traincache-agentd /usr/local/bin/traincache-agentd
COPY --from=build /out/traincache /usr/local/bin/traincache

# 配置全部来自 env(§8 env 表);崩溃即退出,由 DaemonSet 拉起。
ENTRYPOINT ["/usr/local/bin/traincache-agentd"]
