# e2e-k8s — kind 集群端到端证据(M7c)

脚本:`agent/e2e/run-k8s.sh`(幂等;连续两次 PASS,第二次见 `e2e-k8s.run2.log`)
现场:kind 集群 traincache / ns traincache;节点 traincache-worker。原始:`e2e-k8s.raw.log`

## 验收 2:删 Agent Pod → 重建 → 索引/缓存自恢复

- hostPath 标记写入后删除 pod traincache-agentd-fwgdb,重建为 -l2qkq 且 ready
- 缓存持久:标记文件仍在(hostPath 跨 Pod 生命周期)✓
- 索引恢复:RESCAN_PHASE=DONE(≠SAMPLING);重建后 agent_up=1 ✓

## 验收 3:训练容器直读 hostPath blocks(只读)

- agentd 侧写探针块成功;trainer `cat` 读回 `e2e-block-data` ✓
- trainer `touch` 被拒:`Read-only file system`(只读挂载生效)✓

## 验收 8:ordersrc sidecar → agentd 收到 ConsumeOrder

- agentd 日志:`收到 ConsumeOrder ... dataset=train-data chunks=4` ✓
- 同日志伴随「预取接线失败」= 对象存储未接(预期,验收 8 只判接收链路)

## 验收 10:sidecar 停写 → 30s 内 degraded=1 → 恢复回 0

- 冻结 sidecar 写端(ppid==1 的 python3/sleep);`agent_degraded=1` @23s ✓(阈值 30s 内)
- 恢复(CONT)后 `agent_degraded=0` @2s ✓

## 七线指标抽样

- family 总数=25;代理族 agent_up/degraded/index_shards_loaded/cache_hits/fetch_bytes/
  gc_sweeps/evict_blocks/request_duration/rescan_progress 全在 ✓

## 结论

RESULT: PASS(验收 2/3/8/10 可观察现象全部成立)。验收 1/5/9 需对象存储+测试数据,另行覆盖。
