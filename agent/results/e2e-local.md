# e2e-local — 本机端到端证据(M7c)

脚本:`agent/e2e/run-local.sh`(可重复,两次运行结论一致;退出码 0)
现场:2026-10-15 20:28 / 20:29(两次)。原始:`e2e-local.raw.log`
口径:临时目录 CACHE_ROOT + 临时端口 :19410/:19411,退出即清理。

## GetNodeStatus 动态字段(闭合 M6 遗留「agentd 动态判据环境受限」)

- CACHE_ROOT   = /tmp/tc-e2e-cache.Z9TXB4(env 生效 ✓)
- CACHED_BYTES = 0(空缓存,合理)
- FREE_BYTES   = 16018378752(≈16GB,Statfs 动态值非零 ✓)
- RESCAN_PHASE = DONE(空目录,非 SAMPLING ✓)
- RUNNING_TASKS= 0;AGENT=true

## /metrics 七线指标

- family 总数 = 25(=`grep -c '^# HELP traincache_'`)
- 23 个代表 family 全部出现(覆盖七线:命中率/取数/索引/GC/淘汰/请求时延/降级)
- dataset 标签存在(白名单 train-data / 归并 other);agent_up=1;agent_degraded=0

## 已知缺口(GAP,非本脚本失败)

- `prefetch` rc=1:`Internal desc = 预热块列表解析失败: index: get dataset/train-data/CURRENT: dial tcp 127.0.0.1:9000: connection refused`
  —— CLI TaskId 已生效(报错从修复前 `InvalidArgument task_id 必填` 推进到块列表解析失败);阻塞点为本机 MinIO 未运行,svc 同步 resolveRefs 读对象存储 CURRENT 失败(环境限制,见 M7d 裁决 BLOCKER 1)
- `status` rc=1:`NotFound desc = task_id "e2e-local-task-1" 不存在`(上游 prefetch 因对象存储 GAP 未创建任务,非独立缺陷)

## 结论

RESULT: PASS(CACHE_ROOT/动态字段/七线指标全部成立);prefetch/status 因本机无对象存储标 [GAP-环境],与验收 1/5/9 同口径。CLI TaskId 修复由「报错非 task_id 必填」断言验证(见 run-local.sh 4.2 段)。
