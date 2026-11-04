# syntax=docker/dockerfile:1
#
# traincache-checkpointd 及 S3/local 插件镜像（design 10.1 §4.1、§6.1）。
#
# 构建（仓库根为构建上下文；go.work 同时 use ./agent、./core、./controller、
# ./gateway、./checkpoint，五个模块目录都需入上下文）：
#   docker build -f checkpoint/Dockerfile -t ghcr.io/thewor1d877/traincache/traincache-checkpointd:dev .
# 本机构建用 hack/build-images.sh checkpoint：构建前先把基础镜像补成官方短名。
#
# 运行镜像里只有 checkpointd 与两个插件。插件按默认路径
# /opt/traincache/plugins/<plugin>/<plugin> 安装，由准入注入的
# checkpoint-plugins initContainer 复制到 Pod 内容器可读的卷上。
# 进程以非 root 运行；工作卷与 /run/checkpoint 由 Pod 侧提供。
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
COPY checkpoint/ checkpoint/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd checkpoint \
 && go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/checkpointd ./cmd/checkpointd \
 && go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/plugin-s3 ./cmd/plugin-s3 \
 && go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/plugin-local ./cmd/plugin-local

FROM alpine:3.24

# S3 走 TLS 时需要根证书；插件也可能访问 HTTPS 端点。
RUN apk add --no-cache ca-certificates

COPY --from=build /out/checkpointd /usr/local/bin/checkpointd
COPY --from=build /out/plugin-s3 /opt/traincache/plugins/s3/s3
COPY --from=build /out/plugin-local /opt/traincache/plugins/local/local

USER 65532:65532

# 配置全部来自 env；崩溃即退出，由 Pod 的 restartPolicy 拉起。
ENTRYPOINT ["/usr/local/bin/checkpointd"]
