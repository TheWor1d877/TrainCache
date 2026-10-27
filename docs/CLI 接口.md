# CLI 接口

`traincache` 是纯 gRPC 客户端：缓存索引、块下载、落盘全在 Agent 侧，CLI 只发请求、打印响应。源码 `agent/cmd/traincache/main.go`。

## 1. 定位与连接模型

- 三条子命令：`prefetch`、`node status`、`status`，没有本地操作模式。
- `--endpoint` 决定连哪台 Agent，默认 `127.0.0.1:9410`，明文 gRPC 无 TLS，惰性建连：首次 RPC 才拨号；每条 RPC 的 context 超时 10 秒。
- `--node` 只是请求里的校验字段，告诉服务端"预期操作哪台机器"，不做地址解析；填错节点名由 Agent 拒绝。
- `--endpoint` 由各子命令自己注册，必须写在子命令之后。`traincache --endpoint X prefetch ...` 被当成未知子命令，退出码 2。
- 进度靠反复调用 `status` 轮询，不用长连接。

## 2. traincache prefetch

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `--endpoint` | string | 否 | `127.0.0.1:9410` | Agent gRPC 地址 |
| `--task-id` | string | 是 | 空 | 幂等键，原样填入 `task_id`；同 id 内容不同由 Agent 返回 `AlreadyExists` |
| `--path` | string | 是 | 空 | 路径前缀，作为 `prefixes` 列表的唯一元素 |
| `--node` | string | 是 | 空 | 原样填入 `node`；CLI 只校验非空，语义校验在 Agent |
| `--chunks` | string | 是 | 空 | 逗号分隔块名。只做非空校验（切分、去空格、丢空项），**不进入请求**，`chunks` 字段留空 |
| `--dataset` | string | 否 | 空 | 原样填入 `dataset` |
| `--snapshot-id` | string | 否 | 空 | 空 = 当前 CURRENT |
| `--priority` | string | 否 | `p0` | `p0|p1|p2|p3`，大小写与首尾空格不敏感，映射枚举 `P0..P3` |

输出固定一行 `accepted=<bool> task_id=<id> <message>`，字段取自响应。`accepted=false` 仍退出 0，拒绝原因看 `message`；`message` 为空时行尾留一个空格。

## 3. traincache node status

两列表格（键、值），左对齐、列间两空格。`NODE` 行不受 `--item` 影响，永远先输出；其余行按 `--item` 取子集。

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `--endpoint` | string | 否 | `127.0.0.1:9410` | Agent gRPC 地址 |
| `--node` | string | 是 | 空 | 原样填入 `node`；非空校验，语义校验在 Agent |
| `--item` | string | 否 | 空 | `agent|catalog|space|tasks|rescan`；空 = 全量。大小写与首尾空格不敏感，不支持缩写，非法值退出 2 |

| 键 | 出现在 `--item` | 值 |
|---|---|---|
| `NODE` | 总是 | 响应里的节点名 |
| `AGENT` | 全量、`agent` | `true` / `false` |
| `CACHE_ROOT` | 全量、`space` | 缓存根目录路径 |
| `CACHED_BYTES` | 全量、`space` | 已缓存字节数，十进制 |
| `FREE_BYTES` | 全量、`space` | 剩余空间字节数，十进制 |
| `RUNNING_TASKS` | 全量、`tasks` | 运行中预热任务数，十进制 |
| `RESCAN_PHASE` | 全量、`rescan` | `SAMPLING` / `FULL_SCAN` / `DONE`；响应未带 rescan 时为 `-` |
| `CATALOG` | `catalog` | 每个已登记 dataset/snapshot 一行，形如 `<dataset>/<snapshot> shards=<n> entries=<n>`；catalog 为空则无此组行 |

## 4. traincache status

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `--endpoint` | string | 否 | `127.0.0.1:9410` | Agent gRPC 地址 |
| `--node` | string | 是 | 空 | 只做非空校验，**不进入请求**；请求里只有 `task_id` |
| `--task-id` | string | 是 | 空 | 原样填入 `task_id` |

两列表格，行序固定：

| 键 | 值 |
|---|---|
| `TASK_ID` | 任务 id |
| `NODE` | 响应里的 `node`，不是命令行传的 `--node` |
| `PATHS` | 响应 `paths` 用逗号拼接，无空格 |
| `STATE` | `QUEUED` / `RUNNING` / `DONE` / `ERROR` |
| `TOTAL_BLOCKS` / `COMPLETED_BLOCKS` / `FAILED_BLOCKS` | 十进制整数 |
| `STARTED_AT` / `UPDATED_AT` | RFC3339；时间戳缺失或非法显示 `-` |

## 5. 退出码

| 码 | 判定条件 |
|---|---|
| 0 | 成功；`accepted=false` 也算成功；`help`、`-h`、`--help`（含 `node` 层） |
| 1 | 建连失败；RPC 失败（超时、`Unavailable`、`InvalidArgument`、`AlreadyExists`、`Unimplemented` 一律归此码） |
| 2 | 无参数；未知子命令；`node` 缺子命令或未知 `node` 子命令；flag 解析失败；出现位置参数；必填项为空；`--priority` / `--item` 非法 |


## 6. 示例

```bash
# 提交预热，退出码 0：accepted=true task_id=t-1026-01 ...
traincache prefetch --endpoint 127.0.0.1:9410 --node worker-01 \
  --task-id t-1026-01 --path /train-data --dataset coco \
  --chunks chunk-001,chunk-002 --priority p1

# 全量节点状态，退出码 0
traincache node status --node worker-01
# NODE          worker-01
# AGENT         true
# CACHED_BYTES  1073741824

# 只查 catalog，退出码 0：CATALOG  coco/snap-20261020 shards=8 entries=1000000
traincache node status --node worker-01 --item catalog

# 查任务进度，退出码 0
traincache status --node worker-01 --task-id t-1026-01

# 缺 --chunks：stderr 提示 prefetch: --chunks 必填(逗号分隔块名) 并附用法，退出码 2
traincache prefetch --node worker-01 --path /train-data --task-id t-1026-01

# 优先级非法：prefetch: --priority="p9" 非法(仅支持 p0|p1|p2|p3)，退出码 2
traincache prefetch --node worker-01 --path /train-data --task-id t-1026-01 \
  --chunks chunk-001 --priority p9

# Agent 不可达：traincache: GetNodeStatus 失败: rpc error: code = Unavailable ...，退出码 1
traincache node status --node worker-01 --endpoint 127.0.0.1:1
```

## 7. 与 proto 的对应

| 子命令 | RPC（`agent/api/agent.proto`） | 请求字段映射 |
|---|---|---|
| `prefetch` | `PreheatService.SubmitPreheat` | `task_id`←`--task-id`；`node`←`--node`；`dataset`←`--dataset`；`snapshot_id`←`--snapshot-id`；`prefixes`←`[--path]`；`chunks` 留空；`priority`←`--priority`；`generation` 不设置 |
| `status` | `PreheatService.GetPreheatStatus` | `task_id`←`--task-id`；`--node` 不参与 |
| `node status` | `NodeService.GetNodeStatus` | `node`←`--node`；`item`←`--item`（空 → `UNSPECIFIED`） |

`UpdatePreheat`、`CancelPreheat`、`ListSnapshot`、`ReportBlocks` 没有对应子命令。

## 8. 版本与兼容

CLI 与 `traincache-agentd` 同源码、同 git tag 构建，随 Agent 镜像分发，版本以镜像 tag 为准；CLI 没有 `--version`。flag 名、输出字段、退出码属于实现细节，不承诺跨大版本兼容；跨版本混用时以 Agent 镜像内的同一份二进制为准。

## 9. trainpublish

把本地 tar 切块上传并提交一次索引快照的独立命令，源码 `core/cmd/trainpublish`。当前可执行文件名是 `trainpublish`；改名为 `traincachepublish` 并保留别名登记在 `docs/TODO.md` D 类，尚未实施。

```
trainpublish --dataset NAME [options] <tar> [<tar>...]
```

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `--dataset` | string | 是 | 空 | 目标 dataset，写入 `dataset/<name>/` 前缀；须与 DataSet 的 `spec.datasetName` 一致 |
| `--endpoint` | string | 否 | `TRAINCACHE_OBJSTORE_ENDPOINT` 或 `127.0.0.1:9000` | S3 兼容端点，不带 scheme |
| `--access-key` | string | 否 | `TRAINCACHE_OBJSTORE_ACCESS_KEY` | 静态 AK |
| `--secret-key` | string | 否 | `TRAINCACHE_OBJSTORE_SECRET_KEY` | 静态 SK |
| `--bucket` | string | 否 | `TRAINCACHE_OBJSTORE_BUCKET` 或 `traincache` | 单桶 |
| `--secure` | bool | 否 | `TRAINCACHE_OBJSTORE_SECURE` | 用 TLS 连对象存储 |
| `--chunk-size` | int64 | 否 | `67108864`（64 MiB） | 切块字节数，必须 >0；须与 DataSet 的 `spec.chunkSize` 一致 |
| `--concurrency` | int | 否 | `4` | 并发上传数，必须 >0 |
| `--work-dir` | string | 否 | 系统临时目录下的 `trainpublish-*` | 切块临时目录 |
| `--keep-work-dir` | bool | 否 | `false` | 保留临时目录，排障用 |
| `--probe-conditional-write` | bool | 否 | `TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE` | 发布前探测后端条件写，失败即退出 |

flag 显式值优先于环境变量；环境变量名与 Agent 的五个 `TRAINCACHE_OBJSTORE_*` 相同，口径一致。

每个 tar 的 `Path` 取文件名（`filepath.Base`），重复文件名直接报 `ErrDuplicatePath`。全部块上传成功后提交索引并推进 CURRENT。stdout 只有一行：

```
snapID=<id> entries=<n> bytes=<n> unique_blocks=<n>
```

| 退出码 | 条件 |
|---|---|
| 0 | 发布成功 |
| 1 | 文件打开、对象存储、探测或发布失败 |
| 2 | 缺 `--dataset`、没有 tar 参数、`--chunk-size<=0`、`--concurrency<=0`、flag 解析失败 |

```bash
export TRAINCACHE_OBJSTORE_ENDPOINT=s3.example.com:9000
export TRAINCACHE_OBJSTORE_ACCESS_KEY=...
export TRAINCACHE_OBJSTORE_SECRET_KEY=...

trainpublish --dataset coco --bucket traincache \
  --chunk-size 67108864 /data/coco-000.tar /data/coco-001.tar
# snapID=20261026T120000Z-a1b2c3 entries=1000000 bytes=68719476736 unique_blocks=980000
```
