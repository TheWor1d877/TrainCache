# syntax=docker/dockerfile:1
#
# controller-manager image. The controller module imports traincache/agent and
# traincache/core through go.work, so the build context must be the repo root
# (or a staged copy of it) containing all three modules:
#   docker build -f controller/deploy/manager/Dockerfile \
#     -t ghcr.io/thewor1d877/traincache/controller-manager:dev .
FROM golang:1.26-bookworm AS build
ARG GOPROXY=https://goproxy.cn,direct
ARG VERSION=dev
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOPROXY=${GOPROXY}
WORKDIR /src
COPY go.work go.work.sum ./
COPY core/ core/
COPY agent/ agent/
COPY controller/ controller/
# No "go mod download" pre-layer: in workspace mode it walks the whole module
# graph (including k8s.io/kubernetes test-only staging modules) instead of just
# the build's transitive imports.
WORKDIR /src/controller
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/controller-manager ./cmd/controller-manager

FROM debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/controller-manager /usr/local/bin/controller-manager
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/controller-manager"]
