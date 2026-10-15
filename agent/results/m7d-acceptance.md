# M7 集成总闸 — 独立验收报告(只读验收,未改任何代码/脚本/docs)

现场:2026-10-15。工具链 env `export PATH=/usr/local/go/bin:$PATH GOROOT=/usr/local/go`。
验收代理独立执行;原始输出见 `m7d-agent.raw.log`、`m7d-core.raw.log`、`m7d-e2e-local.run1.log`、`m7d-e2e-local.run2.log`。

## VERDICT: FAIL

## 命令 → 结论

| 命令 | 结论 | 证据 |
|------|------|------|
| `agent: go build ./...` | PASS | 退出 0 |
| `agent: go vet ./...` | PASS | 退出 0 |
| `agent: gofmt -l .` | PASS | 输出为空 |
| `agent: go test ./... -race -count=1` | PASS | 11 包全 ok |
| `core: go build/vet/test -race` | PASS | 5 包全 ok(回归) |
| `bash agent/e2e/run-local.sh` | **FAIL** | 退出码 1;prefetch/status 两条硬断言失败 |

## 失败根因(≤5 行)

`svc/preheat.go` Submit 第 4 步 `resolveRefs` 对 `prefixes` **同步**调用 `Catalog.List(prefix)`,
该调用先读对象存储 `dataset/<ds>/CURRENT`(默认 127.0.0.1:9000)。本机 MinIO 未运行 →
`connect: connection refused` → RPC 在返回 `accepted=true` **之前**即失败(Internal)。

即:任务要求的前提「prefetch 提交 accepted=true 不受影响(状态机异步入队,立即返回)」与当前
`svc/preheat.go` 实现**不符**——块列表解析是同步且强依赖对象存储的。复现路径:
`bash agent/e2e/run-local.sh`(两次运行结论一致)。

## CONTRACT:CLI prefetch 修复点抽查

- `runPrefetch` 设置 `Request.TaskId = *taskID`:`main.go:127` ✅ 已配置
- 新增 `--task-id` flag 且为必填:`main.go:90`(定义)、`main.go:103`(空值拦截) ✅
- `status` 子命令 `--task-id` 必填校验:命名/口径与 prefetch 一致(`main.go:210`、`main.go:217`) ✅
- 修复真实生效:错误从修复前 `InvalidArgument: task_id 必填` 推进到修复后 `Internal: 预热块列表解析失败` —— 证明 TaskId 已生效,阻塞点转移到对象存储
- 旁注:`--chunks` 被校验但**从未写入 `Request.Chunks`**(`main.go:133` 注释「chunks 留空」),CLI 实际永远只发 `Prefixes`,故必然走对象存储解析路径

## 十一条验收复验

1 预热任务提交+进度可查 — [GAP-环境]:机制由 `svc` 单测覆盖;本机无对象存储,submit 即被块列表解析拦截
2 DaemonSet 重启缓存不丢、索引自恢复 — PASS:依据 `e2e-k8s.md`(删 Pod 重建、标记留存、RESCAN_PHASE=DONE);本次未独立复跑 kind
3 Pod hostPath 只读直读 blocks — PASS:依据 `e2e-k8s.md`(trainer 读回、touch 被拒 Read-only);未独立复跑
4 CLI 三命令 — **FAIL**:`node status` 通过;`prefetch`/`status` 硬断言失败(见上)
5 路径预热只加载所需分片 — [GAP-环境]:`Catalog.ShardsLoaded`/`ListPrefixes` 有实现+单测;需对象存储+测试数据
6 坏块修复不重复下载 — PASS:代码路径 `agent/rescan/repair.go` + `core/cas` Quarantine(含重复下载抑制),相关单测绿
7 指标七线+标签基数 — PASS:e2e-local 实测 25 family、dataset 标签、agent_up=1
8 sidecar→ConsumeOrder→预取启动 — PASS:依据 `e2e-k8s.raw.log`(收到 ConsumeOrder,chunks=4);预取接线因缺对象存储失败=环境(验收 8 只判接收)
9 阶段切换 K 块高优先级 — [GAP-环境]:需真实订单流+对象存储
10 30s 降级+指标 — PASS:依据 `e2e-k8s.md`(degraded=1 @23s ≤30s,恢复回 0)
11 全流程可重复测试 — PASS:两模块单测全绿;e2e-local 两次运行结论一致(均为失败,可重复)

## BLOCKERS(需主对话裁决)

1. **前提与实现对不上**:验收要求「4 必须在 e2e-local 硬通过」,但 `svc/preheat.go` 对前缀预热同步解析块列表、强依赖对象存储,本机无 MinIO 时 `accepted=true` 无法返回。需裁决三选一:(a) 将 e2e-local 的 prefetch/status 断言降级为 [GAP-环境](与 1/5/9 同口径);(b) 改 `svc` 让不可达对象存储视作空列表/延后解析;(c) e2e-local 需先起 MinIO。本次未改代码。
2. **陈旧证据**:`agent/results/e2e-local.md`(作者 20:28 撰写)仍记录修复前 GAP(`InvalidArgument task_id 必填`)并声明「退出码 0 / RESULT: PASS」,与修复后现状不符,应更新。
3. `agent/e2e/run-local.sh` 头部注释称「prefetch/status 已硬断言(不再有 GAP)」,与当前本机可满足性不符。
