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
