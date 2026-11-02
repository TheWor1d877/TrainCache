# TrainCache 验证入口。CI 跑 verify；PR 到 main 另跑 verify-metadata 与 kind e2e；
# e2e 需要 docker + kind，也可手动触发。

.PHONY: verify verify-controller verify-core verify-agent verify-gateway verify-hooklib verify-metadata e2e \
	release-install release-smoke

verify: verify-controller verify-core verify-agent verify-gateway verify-hooklib

verify-controller:
	cd controller && go build ./... && go vet ./... && go test ./... -race -count=1
	cd controller && test -z "$$(gofmt -l .)"
	@! grep -Rn "GetPreheatProgress\|ScorePolicy\.Window\|consumeOrderRef" \
		controller agent --include="*.go" --include="*.proto"

verify-core:
	cd core && go build ./... && go vet ./... && go test ./... -race -count=1
	cd core && test -z "$$(gofmt -l .)"

verify-agent:
	cd agent && go build ./... && go vet ./... && go test ./... -race -count=1
	cd agent && test -z "$$(gofmt -l .)"

verify-gateway:
	cd gateway && go build ./... && go vet ./... && go test ./... -race -count=1
	cd gateway && test -z "$$(gofmt -l .)"

# DataLoader 适配器只有 Python 标准库依赖，不需要 Go 工具链。
verify-hooklib:
	cd agent/hooklib && python3 -m unittest discover -s . -p 'test_*.py'

# 版本引用与文档引用的静态检查，不需要 Go 工具链。
# verify-config-placement.sh 检查配置落位：清单零字面量 env、ConfigMap 与引用对齐。
verify-metadata:
	./hack/verify-release-metadata.sh
	./hack/verify-doc-refs.sh
	./hack/verify-config-placement.sh

# 端到端：脚本自行创建/清理 kind 集群，源码比镜像新会自动重建。
e2e:
	cd controller && ./e2e/run.sh

# 发布清单：把五个镜像换成 digest 后渲染 install.yaml。
# 用法: make release-install VERSION=v0.1.0 D_MANAGER=sha256:... (五个 digest 都要给)
release-install:
	./hack/render-install.sh $(VERSION) $(D_MANAGER) $(D_WEBHOOK) $(D_SCHEDULER) $(D_AGENT) $(D_GATEWAY)

# 发布后冒烟：在临时 kind 里装已经发布的 install.yaml。
# 用法: make release-smoke TAG=v0.1.0 INSTALL=install.yaml
release-smoke:
	./hack/release-smoke.sh "$(INSTALL)" "$(TAG)"

# ---- bench 装置入口转发 ----
# 编排全部在 bench/Makefile；这里只转发,便于在仓库根直接执行 make bench-*。
REPO_ROOT := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
BENCH_MAKE := $(MAKE) --no-print-directory -C $(REPO_ROOT)/bench
# 仅在变量有值时转发:空的 REPS 若显式传入会改变 bench-quick 对 origin 的判断。
BENCH_VARS = $(foreach v,TIER RATE REPS TAG COND QUICK DELAY GROUP FORCE,$(if $(strip $($(v))),$(v)=$($(v)),))

.PHONY: bench-up bench-down bench-down-full ctrl-up ctrl-down bench-status bench-doctor bench-quick bench-compare bench-list \
	bench-cold bench-verify bench bench-selftest tc-up tc-down \
	track-d-up track-d-verify track-d-down \
	kind-up k8s-up data-up upload bench-data bench-tc track-d-data \
	e2e-local e2e-k8s

bench-up bench-down bench-down-full ctrl-up ctrl-down bench-status bench-doctor bench-quick bench-compare bench-list \
bench-cold bench-verify bench tc-up tc-down \
track-d-up track-d-verify track-d-down \
kind-up k8s-up data-up upload bench-data bench-tc track-d-data \
e2e-local e2e-k8s:
	+@$(BENCH_MAKE) $@ $(BENCH_VARS)

# bench/Makefile 里的目标名是 selftest;根入口统一叫 bench-selftest。
bench-selftest:
	+@$(BENCH_MAKE) selftest $(BENCH_VARS)
