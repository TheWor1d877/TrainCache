# syntax=docker/dockerfile:1
#
# Custom kube-scheduler image carrying the TrainCacheScore plugin (Q2(a)).
# The controller module imports traincache/agent and traincache/core through
# go.work, so build from the repo root (or a staged copy of it):
#
#   docker build -f controller/deploy/scheduler/Dockerfile \
#     -t dgl3i4ucowsgvvdwty.xuanyuan.run/traincache/kube-scheduler:0.8.0 .
#
# Every image is pinned to the registry mirror from /etc/docker/daemon.json;
# k8s/containerd does not read that file, so the full domain is spelled out.
ARG REGISTRY=dgl3i4ucowsgvvdwty.xuanyuan.run

FROM ${REGISTRY}/library/golang:1.26-bookworm AS build

ARG GOPROXY=https://goproxy.cn,direct
ARG VERSION=0.8.0

ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local \
    GOPROXY=${GOPROXY}

WORKDIR /src
COPY go.work go.work.sum ./
COPY core/ core/
COPY agent/ agent/
COPY controller/ controller/
# No "go mod download" pre-layer: in workspace mode it walks the whole module
# graph (including k8s.io/kubernetes test-only staging modules).
WORKDIR /src/controller
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/kube-scheduler ./cmd/scheduler

FROM ${REGISTRY}/library/debian:bookworm-slim

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/kube-scheduler /usr/local/bin/kube-scheduler

USER 65534:65534
ENTRYPOINT ["/usr/local/bin/kube-scheduler"]
