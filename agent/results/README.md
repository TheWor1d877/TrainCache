# agent/results — M7c e2e 演练证据

本目录存放 Agent 服务化端到端演练(M7c)的可复核证据。脚本在 `agent/e2e/`,
经 `make -C bench e2e-local` / `make -C bench e2e-k8s` 调用。

| 文件 | 内容 |
|------|------|
| `e2e-local.md` | 本机 e2e 摘要:GetNodeStatus 动态字段 + 七线指标 + 已知 GAP |
| `e2e-k8s.md` | kind 集群 e2e 摘要:验收 2/3/8/10 |
| `e2e-local.raw.log` | 脚本自动落盘的本机原始片段 |
| `e2e-k8s.raw.log` | 脚本自动落盘的集群原始片段 |
| `e2e-k8s.run2.log` | 第二次连续运行整段输出(证明可重复/幂等) |

覆盖:验收 2/3/8/10(集群)+ 本机动态判据与七线指标。
未覆盖:验收 1/5/9(全量预取/路径加载/阶段切换)需对象存储(MinIO)+ 测试数据,见回传 DECISIONS。
