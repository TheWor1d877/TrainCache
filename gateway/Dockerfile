# syntax=docker/dockerfile:1
#
# traincache-gatewayd 网关镜像(design 10.1 §3;单副本 Deployment 入口,M5 落清单)。
#
# 构建(仓库根为构建上下文:go.work 同时 use ./agent、./core、./controller、./gateway,
# 四个模块目录都需入上下文):
#   docker build -f gateway/Dockerfile -t ghcr.io/thewor1d877/traincache/traincache-gateway:dev .
# 本机构建用 hack/build-images.sh:构建前先把基础镜像补成官方短名。
#
# 镜像内容只有网关二进制;配置全部来自 env(design 10.1 §5.3),
# 探针与 S3 共用 9413,/debug/loglevel 绑回环。CGO_ENABLED=0 静态构建,
# 运行面不写任何路径。
FROM golang:1.26-alpine AS build

# GOPROXY 默认 goproxy.cn,受限网络可用 --build-arg GOPROXY=https://proxy.golang.org,direct 覆盖。
ARG GOPROXY=https://goproxy.cn,direct
ARG VERSION=dev
ENV CGO_ENABLED=0 \
    GOPROXY=${GOPROXY}

WORKDIR /src
COPY go.work go.work.sum ./
COPY core/ core/
COPY agent/ agent/
COPY controller/ controller/
COPY gateway/ gateway/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd gateway \
 && go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/traincache-gateway ./cmd/gatewayd

FROM alpine:3.24

COPY --from=build /out/traincache-gateway /usr/local/bin/traincache-gateway

# 配置全部来自 env;崩溃即退出,由 Deployment 拉起。
ENTRYPOINT ["/usr/local/bin/traincache-gateway"]
