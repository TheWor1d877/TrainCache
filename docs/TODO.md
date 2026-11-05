# TODO

按处理方式分组；组内按 [A]/[B]/[C]/[D] 分级排。每条写清落点与证据。

## 需要改代码（逻辑、清单、流程）

(thewor1d877)： [C] 5.1 §11.5：数据集删除后 Agent 本地副本的清理入口归 Agent 层

## 需要你裁决

(thewor1d877)： [C] docs/0 §1：dataset 到 bucket 的一对一映射（独立桶）未设计，`Store` 接口签名保持不变，落点在 Controller 层

(thewor1d877)： [B] 发布流水线未把 checkpointd 镜像的 digest 纳入 `install.yaml` 与 `verify-release-assets` 的校验范围：现在候选里是 6 个 digest，清单只引用 5 个，校验也只查 5 个

(thewor1d877)： [B] 训练程序与 checkpointd 共享工作卷时的权限契约：现契约要求训练程序用组可写方式建 step 目录（`umask 0002`），否则 checkpointd 删不掉副本；要不要改成 checkpointd 用 root 跑以免训练程序配合
