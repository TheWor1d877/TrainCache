# TrainCache 验证入口。CI 只跑 verify；e2e 需要 docker + kind，按需本地执行。

.PHONY: verify verify-controller verify-core verify-agent e2e

verify: verify-controller verify-core verify-agent

verify-controller:
	cd controller && go build ./... && go vet ./... && go test ./... -race -count=1
	cd controller && test -z "$$(gofmt -l .)"
	@! grep -Rn "GetPreheatProgress\|ScorePolicy\.Window\|consumeOrderRef" \
		controller agent --include="*.go" --include="*.proto"

verify-core:
	cd core && go build ./... && go test ./... -race -count=1

verify-agent:
	cd agent && go build ./... && go test ./... -race -count=1

# 端到端：脚本自行创建/清理 kind 集群，源码比镜像新会自动重建。
e2e:
	cd controller && ./e2e/run.sh

# ---- bench 装置入口转发 ----
# 编排全部在 bench/Makefile；这里只转发,便于在仓库根直接执行 make bench-*。
REPO_ROOT := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
BENCH_MAKE := $(MAKE) --no-print-directory -C $(REPO_ROOT)/bench
# 仅在变量有值时转发:空的 REPS 若显式传入会改变 bench-quick 对 origin 的判断。
BENCH_VARS = $(foreach v,TIER RATE REPS TAG COND QUICK DELAY GROUP FORCE,$(if $(strip $($(v))),$(v)=$($(v)),))

.PHONY: bench-up bench-down bench-down-full ctrl-up ctrl-down bench-status bench-doctor bench-quick bench-m7 bench-list \
	bench-cold bench-verify bench bench-selftest tc-up tc-down \
	track-d-up track-d-verify track-d-down \
	kind-up k8s-up data-up upload bench-data bench-tc track-d-data \
	e2e-local e2e-k8s

bench-up bench-down bench-down-full ctrl-up ctrl-down bench-status bench-doctor bench-quick bench-m7 bench-list \
bench-cold bench-verify bench tc-up tc-down \
track-d-up track-d-verify track-d-down \
kind-up k8s-up data-up upload bench-data bench-tc track-d-data \
e2e-local e2e-k8s:
	+@$(BENCH_MAKE) $@ $(BENCH_VARS)

# bench/Makefile 里的目标名是 selftest;根入口统一叫 bench-selftest。
bench-selftest:
	+@$(BENCH_MAKE) selftest $(BENCH_VARS)
