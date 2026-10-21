# syntax=docker/dockerfile:1
#
# Pod mutating webhook image. The controller module imports traincache/agent
# and traincache/core through go.work, so build from the repo root (or a staged
# copy of it):
#   docker build -f controller/deploy/webhook/Dockerfile \
#     -t <registry>/traincache/webhook:0.8.0 .
ARG REGISTRY=dgl3i4ucowsgvvdwty.xuanyuan.run

FROM ${REGISTRY}/library/golang:1.26-bookworm AS build
ARG GOPROXY=https://goproxy.cn,direct
ARG VERSION=0.8.0
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOPROXY=${GOPROXY}
WORKDIR /src
COPY go.work go.work.sum ./
COPY core/ core/
COPY agent/ agent/
COPY controller/ controller/
# No "go mod download" pre-layer: in workspace mode it walks the whole module
# graph (including k8s.io/kubernetes test-only staging modules).
WORKDIR /src/controller
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/webhook ./cmd/webhook

FROM ${REGISTRY}/library/debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/webhook /usr/local/bin/webhook
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/webhook"]
