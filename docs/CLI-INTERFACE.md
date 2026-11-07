# CLI 接口

TrainCache 仓库里有三个命令行程序：`traincache` 与 Agent 通信，`trainpublish` 发布数据集，`traincacheds` 做数据集管理与回收。三个程序都只调用 core 库对外暴露的接口，不重复实现缓存索引、块下载与落盘逻辑。

## 定位与连接模型

- `traincache` 有五条子命令：`prefetch`、`node status`、`status`、`ls`、`resolve`。前四条通过 gRPC 访问 Agent，明文传输不带 TLS；`resolve` 只访问控制面的 HTTP 端点，不发 gRPC 请求。
- `--endpoint` 决定连哪台 Agent，留空表示按节点名自动解析。连接惰性建立，第一次 RPC 才拨号，每条 RPC 的 context 超时 10 秒。
- `--node` 是请求里的校验字段，告诉服务端预期操作哪台机器，填错节点名由 Agent 拒绝。留空 `--endpoint` 时，`--node` 还用来解析 Agent 地址。
- `--controller` 是控制面地址，只在按节点名解析地址时用到。
- `--endpoint` 与 `--controller` 由各子命令自己注册，必须写在子命令之后。`traincache --endpoint X prefetch ...` 被当成未知子命令，退出码 2。
- 任务进度靠反复调用 `status` 获取。

### 地址解析顺序

`--endpoint` 非空时直接用它，不再解析。留空时按下面顺序取第一个可用的结果：

1. 环境变量 `NODE_NAME` 等于 `--node`：用 `127.0.0.1:9410`。Agent DaemonSet 用 Downward API 把所在节点名注入 `NODE_NAME`，目标节点就是本机时走这条。
2. `--node` 非空：向控制面发 `GET /resolve?node=<节点名>`，拿回候选地址数组，按数组顺序逐个试连，第一个连上的就用。每个候选的试连超时 2 秒；一个都连不上时报错退出 1。
3. `--node` 为空：用 `127.0.0.1:9410`。

试连判据是 gRPC 连接进入 Ready，即目标端口上有 Agent 在应答；端口能连上但没有 Agent 应答不算连上。

## traincache prefetch

用法：

```
traincache prefetch --path PATH --node NODE (--chunks PATH:CHUNK,... | --files TAR,...) [--dataset NAME] [--snapshot-id ID] [--priority p0|p1|p2|p3]
```

参数：

- `--endpoint`：string，默认空，可选。Agent 的 gRPC 地址；留空表示按节点名自动解析，顺序见「地址解析顺序」。
- `--controller`：string，默认取 `TRAINCACHE_CONTROLLER_ENDPOINT`，未设置时为 `traincache-controller-manager.traincache.svc:9412`，可选。控制面地址，用来按节点名解析 Agent 地址；不带 scheme 时按 `http://` 处理。
- `--task-id`：string，必填。预热任务 ID，同时是幂等键，原样填入请求的 `task_id`；同一个 id 提交不同内容时 Agent 返回 `AlreadyExists`。
- `--path`：string，必填。数据路径前缀，作为请求 `prefixes` 列表的唯一元素。
- `--node`：string，必填。目标节点，原样填入请求的 `node`；CLI 只校验非空，语义校验在 Agent 侧。
- `--chunks`：string，可选。逗号分隔的精确块清单，每项形如 `path:chunk`，每项构造一个 `ChunkRef` 填入请求的 `chunks`。与 `--files` 互斥，两者至少给一个。
- `--files`：string，可选。逗号分隔的 tar 分片名，必须同时给 `--dataset`。CLI 先调 `ListSnapshot`（不带 prefix）把分片名展开成块，再填入请求的 `chunks`；分片名在快照里找不到时退出 1。
- `--dataset`：string，可选。dataset 名，原样填入请求的 `dataset`。
- `--snapshot-id`：string，可选。快照 ID，留空表示当前 CURRENT。
- `--priority`：string，默认 `p0`。取值 `p0|p1|p2|p3`，大小写与首尾空格不敏感，映射到 `PreheatPriority` 枚举的 `P0` 到 `P3`。

输出固定一行 `accepted=<bool> task_id=<id> <message>`，字段取自响应。`accepted=false` 仍退出 0，拒绝原因写在 `message` 里；`message` 为空时行尾留一个空格。

`--files` 的分片名先用 `traincache ls --dataset NAME` 查。

## traincache node status

用法：

```
traincache node status --node NODE [--item agent|catalog|space|tasks|rescan]
```

输出是两列表格（键、值），左对齐、列间两空格。`NODE` 行不受 `--item` 影响，永远先输出，其余行按 `--item` 取子集。

参数：

- `--endpoint`：string，默认空，可选。Agent 的 gRPC 地址；留空表示按节点名自动解析，顺序见「地址解析顺序」。
- `--controller`：string，默认取 `TRAINCACHE_CONTROLLER_ENDPOINT`，未设置时为 `traincache-controller-manager.traincache.svc:9412`，可选。控制面地址，用来按节点名解析 Agent 地址；不带 scheme 时按 `http://` 处理。
- `--node`：string，必填。目标节点，原样填入请求的 `node`；CLI 只校验非空，语义校验在 Agent 侧。
- `--item`：string，默认空，可选。取值 `agent|catalog|space|tasks|rescan`，留空表示全量；大小写与首尾空格不敏感，不支持缩写，非法取值退出 2。

输出键：

| 键 | 出现在 `--item` | 值 |
|---|---|---|
| `NODE` | 总是 | 响应里的节点名 |
| `AGENT` | 全量、`agent` | `true` 或 `false` |
| `CACHE_ROOT` | 全量、`space` | 缓存根目录路径 |
| `CACHED_BYTES` | 全量、`space` | 已缓存字节数，十进制 |
| `FREE_BYTES` | 全量、`space` | 剩余空间字节数，十进制 |
| `RUNNING_TASKS` | 全量、`tasks` | 运行中预热任务数，十进制 |
| `RESCAN_PHASE` | 全量、`rescan` | `SAMPLING` / `FULL_SCAN` / `DONE`，响应未带 rescan 时为 `-` |
| `CATALOG` | `catalog` | 每个已登记 dataset/snapshot 一行，形如 `<dataset>/<snapshot> shards=<n> entries=<n>`，catalog 为空时没有这一组行 |

## traincache status

用法：

```
traincache status --node NODE --task-id ID
```

参数：

- `--endpoint`：string，默认空，可选。Agent 的 gRPC 地址；留空表示按节点名自动解析，顺序见「地址解析顺序」。
- `--controller`：string，默认取 `TRAINCACHE_CONTROLLER_ENDPOINT`，未设置时为 `traincache-controller-manager.traincache.svc:9412`，可选。控制面地址，用来按节点名解析 Agent 地址；不带 scheme 时按 `http://` 处理。
- `--node`：string，必填。目标节点，只做非空校验，请求里只带 `task_id`。
- `--task-id`：string，必填。预热任务 ID，原样填入请求的 `task_id`。

输出是两列表格，行序固定：

| 键 | 值 |
|---|---|
| `TASK_ID` | 任务 id |
| `NODE` | 响应里的 `node` 字段值 |
| `PATHS` | 响应 `paths` 用逗号拼接，无空格 |
| `STATE` | `QUEUED` / `RUNNING` / `DONE` / `ERROR` |
| `TOTAL_BLOCKS` / `COMPLETED_BLOCKS` / `FAILED_BLOCKS` | 十进制整数 |
| `STARTED_AT` / `UPDATED_AT` | RFC3339，时间戳缺失或非法显示 `-` |

## traincache ls

用法：

```
traincache ls --dataset NAME [--snapshot-id ID] [--prefix P] [--chunks]
```

列出某个 dataset 快照的条目，只发一次 `ListSnapshot` 请求，响应里的条目就是全部输出。默认按文件聚合，用来看有哪些 tar 分片、各多少块多少字节；`--chunks` 展开到块级明细。

参数：

- `--endpoint`：string，默认空，可选。Agent 的 gRPC 地址；留空表示按节点名自动解析，顺序见「地址解析顺序」。
- `--controller`：string，默认取 `TRAINCACHE_CONTROLLER_ENDPOINT`，未设置时为 `traincache-controller-manager.traincache.svc:9412`，可选。控制面地址，用来按节点名解析 Agent 地址；不带 scheme 时按 `http://` 处理。`ls` 没有 `--node`，用不到这个地址。
- `--dataset`：string，必填。dataset 名，原样填入请求的 `dataset`；留空时 CLI 本地报 usage 错误并退出 2，不发 RPC。
- `--snapshot-id`：string，可选。快照 ID，留空表示当前 CURRENT。
- `--prefix`：string，可选。路径前缀过滤，原样填入请求的 `prefix`；留空表示不过滤。
- `--chunks`：bool，默认 `false`，可选。展开到块级明细，默认按文件聚合。

输出带表头，左对齐、列间两空格，数字十进制，按路径字节序升序（ASCII 分片名即字典序）：

| 模式 | 表头 | 每行 |
|---|---|---|
| 默认（文件聚合） | `FILE  CHUNKS  BYTES` | 一个 tar 分片一行：文件名、块数、字节合计 |
| `--chunks` | `FILE  CHUNK  BYTES` | 一个块一行，按 `(FILE, CHUNK)` 升序，`CHUNK` 从 0 起 |

```bash
# 文件级：看有哪些分片、各多少块
traincache ls --dataset coco
# FILE           CHUNKS  BYTES
# coco-000.tar   1024    8589934592
# coco-001.tar   1024    8589934592

# 块级明细，按前缀过滤
traincache ls --dataset coco --prefix coco-001 --chunks
# FILE           CHUNK  BYTES
# coco-001.tar   0      8388608
# coco-001.tar   1      8388608
```

`FILE` 是条目路径，当前发布物就是 tar 文件名。无匹配条目时只输出表头，退出 0。

## traincache resolve

用法：

```
traincache resolve --node NODE [--controller ADDR]
```

按节点名解析 Agent 地址，每个候选地址一行，按优先顺序打印。解析规则与「地址解析顺序」一致：`--node` 为空时报参数错误退出 2；环境变量 `NODE_NAME` 等于 `--node` 时打印 `127.0.0.1:9410`；其余情况向控制面发 `GET /resolve?node=<节点名>`，把返回的候选数组逐行打印。

`resolve` 不发 gRPC 请求，只查控制面，也不试连候选地址；打印的顺序就是 CLI 拨号时的试连顺序。

参数：

- `--node`：string，必填。目标节点名，原样填进控制面查询参数。
- `--controller`：string，默认取 `TRAINCACHE_CONTROLLER_ENDPOINT`，未设置时为 `traincache-controller-manager.traincache.svc:9412`，可选。控制面地址，不带 scheme 时按 `http://` 处理。

控制面返回 404 表示该节点没有 Agent 地址，返回 500 表示查询本身失败；两种情况都打印错误并退出 1。控制面连不上时同样退出 1。

```bash
# 按节点名解析 Agent 地址，退出码 0
traincache resolve --node worker-01
# 10.0.0.5:9410
# 192.168.1.10:9410
```

## 退出码

- `0`：成功。`accepted=false` 也算成功；`help`、`-h`、`--help`（含 `node` 层）返回 0。
- `1`：建连失败；按节点名解析地址时控制面不可达、该节点没有 Agent 地址、候选地址一个都连不上；RPC 失败（超时、`Unavailable`、`InvalidArgument`、`NotFound`、`AlreadyExists`、`Unimplemented` 一律归此码）；`prefetch --files` 在快照里找不到分片。
- `2`：无参数；未知子命令；`node` 缺子命令或未知 `node` 子命令；flag 解析失败；出现位置参数；必填项为空；`prefetch` 的 `--chunks` 与 `--files` 同时给或都没给、`--files` 缺 `--dataset`、`--chunks` 的项不是 `path:chunk`；`--priority` 或 `--item` 取值非法。

## 示例

```bash
# 提交预热，退出码 0：accepted=true task_id=t-1026-01 ...
traincache prefetch --endpoint 127.0.0.1:9410 --node worker-01 \
  --task-id t-1026-01 --path /train-data --dataset coco \
  --files coco-000.tar,coco-001.tar --priority p1

# 全量节点状态，退出码 0
traincache node status --node worker-01
# NODE          worker-01
# AGENT         true
# CACHED_BYTES  1073741824

# 只查 catalog，退出码 0：CATALOG  coco/snap-20261020 shards=8 entries=1000000
traincache node status --node worker-01 --item catalog

# 列快照里的 tar 分片，退出码 0
traincache ls --dataset coco --snapshot-id snap-20261020
# FILE          CHUNKS  BYTES
# coco-000.tar  8192    68719476736

# 查任务进度，退出码 0
traincache status --node worker-01 --task-id t-1026-01

# 按节点名解析 Agent 地址，退出码 0
traincache resolve --node worker-01
# 10.0.0.5:9410
# 192.168.1.10:9410

# --chunks 与 --files 都没给：stderr 提示 prefetch: --chunks 或 --files 至少给一个 并附用法，退出码 2
traincache prefetch --node worker-01 --path /train-data --task-id t-1026-01

# 优先级非法：prefetch: --priority="p9" 非法(仅支持 p0|p1|p2|p3)，退出码 2
traincache prefetch --node worker-01 --path /train-data --task-id t-1026-01 \
  --dataset coco --files coco-000.tar --priority p9

# Agent 不可达：traincache: GetNodeStatus 失败: rpc error: code = Unavailable ...，退出码 1
traincache node status --node worker-01 --endpoint 127.0.0.1:1
```

## gRPC 接口对应

前四条子命令各调一个 RPC，请求字段与来源：

| 子命令 | RPC | 请求字段 | 来源 |
|---|---|---|---|
| `prefetch` | `PreheatService.SubmitPreheat` | `task_id` | `--task-id` |
| `prefetch` | `PreheatService.SubmitPreheat` | `node` | `--node` |
| `prefetch` | `PreheatService.SubmitPreheat` | `dataset` | `--dataset` |
| `prefetch` | `PreheatService.SubmitPreheat` | `snapshot_id` | `--snapshot-id` |
| `prefetch` | `PreheatService.SubmitPreheat` | `prefixes` | 只含 `--path` 的单元素列表 |
| `prefetch` | `PreheatService.SubmitPreheat` | `chunks` | `--chunks` 解析出的 `ChunkRef`，或 `--files` 经 `ListSnapshot` 展开出的块 |
| `prefetch` | `PreheatService.SubmitPreheat` | `priority` | `--priority` |
| `prefetch` | `PreheatService.SubmitPreheat` | `generation` | 不设置 |
| `status` | `PreheatService.GetPreheatStatus` | `task_id` | `--task-id`，`--node` 不参与请求 |
| `node status` | `NodeService.GetNodeStatus` | `node` | `--node` |
| `node status` | `NodeService.GetNodeStatus` | `item` | `--item`，留空映射为 `UNSPECIFIED` |
| `ls` | `NodeService.ListSnapshot` | `dataset` | `--dataset` |
| `ls` | `NodeService.ListSnapshot` | `snapshot_id` | `--snapshot-id` |
| `ls` | `NodeService.ListSnapshot` | `prefix` | `--prefix`，`--chunks` 只决定输出形态 |

协议里的 `UpdatePreheat`、`CancelPreheat`、`ReportBlocks` 三个 RPC 没有对应的 CLI 子命令。

`resolve` 不调 gRPC，它发的是控制面的 HTTP 请求 `GET /resolve?node=<节点名>`，响应体是候选地址的 JSON 数组。

## 版本与兼容

CLI 与 `traincache-agentd` 同源码、同 git tag 构建，随 Agent 镜像分发，版本号以镜像 tag 为准，`traincache` 自身不提供版本查询。跨版本混用时以 Agent 镜像内的同一份二进制为准。flag 名、输出字段与退出码随大版本可能变动，版本历史记在 `CHANGELOG.md`。

## trainpublish

把本地 tar 切块上传并提交一次索引快照的独立命令，源码在 `core/cmd/trainpublish`，可执行文件名 `trainpublish`。

```
trainpublish --dataset NAME [options] <tar> [<tar>...]
trainpublish --dataset NAME --check-dataset PATH
```

参数：

- `--dataset`：string，必填。目标 dataset，写入 `dataset/<name>/` 前缀；须与 DataSet 的 `spec.datasetName` 一致。
- `--endpoint`：string，默认取 `TRAINCACHE_OBJSTORE_ENDPOINT`，未设置时为 `127.0.0.1:9000`，可选。S3 兼容端点，不带 scheme。
- `--access-key`：string，默认取 `TRAINCACHE_OBJSTORE_ACCESS_KEY`，可选。静态 AK。
- `--secret-key`：string，默认取 `TRAINCACHE_OBJSTORE_SECRET_KEY`，可选。静态 SK。
- `--bucket`：string，默认取 `TRAINCACHE_OBJSTORE_BUCKET`，未设置时为 `traincache`，可选。单桶。
- `--secure`：bool，默认取 `TRAINCACHE_OBJSTORE_SECURE`，可选。用 TLS 连对象存储。
- `--chunk-size`：int64，默认 `67108864`（64 MiB），可选，必须大于 0。切块字节数，只在空桶首次发布生效，桶里已有格式声明时沿用声明值，显式传不同值直接报错；须与 DataSet 的 `spec.chunkSize` 一致。
- `--concurrency`：int，默认 `4`，可选，必须大于 0。并发上传数。
- `--work-dir`：string，默认系统临时目录下的 `trainpublish-` 前缀目录，可选。切块临时目录。
- `--keep-work-dir`：bool，默认 `false`，可选。保留临时目录，供排障用。
- `--probe-conditional-write`：bool，默认取 `TRAINCACHE_OBJSTORE_PROBE_CONDITIONAL_WRITE`，可选。发布前探测后端条件写，探测失败即退出。
- `--check-dataset`：string，默认空，可选。只校验模式：只读桶里的格式声明并校验该 DataSet 文件，不上传、不推进 CURRENT；设置时 `--dataset` 必填且禁止 tar 位置参数。

flag 显式值优先于环境变量；环境变量名与 Agent 的 `TRAINCACHE_OBJSTORE_*` 同名，口径一致。

桶格式声明。块大小按桶固定，同一桶内所有 dataset 共用，块名与索引偏移都由它推出。

首次发布在创建 store 之后、上传之前，用条件写（`If-None-Match: *`）写入对象键 `_traincache/format.json`，内容为 `{"version":1,"chunkSize":<int64>}`。并发首次发布只有一个进程写成功，其余进程回读已有声明。后端不支持条件写时直接报错退出。

之后的发布只读这份声明：`--chunk-size` 沿用声明值，显式传入与声明不同的值则退出 1。桶里已有数据但还没有声明时，第一次发布要显式传历史块大小，否则会按默认 64 MiB 写入声明。块大小要改动时，清空桶重新发布，或者换一个新桶：原地改值会让旧块名和偏移与新索引对不上。

每个 tar 的 `Path` 取文件名（`filepath.Base`），重复文件名直接报 `ErrDuplicatePath`。全部块上传成功后提交索引并推进 CURRENT。发布成功时 stdout 只有一行：

```
snapID=<id> entries=<n> bytes=<n> unique_blocks=<n>
```

退出码：

- `0`：发布成功，或 `--check-dataset` 校验通过。
- `1`：文件打开、对象存储、格式声明、探测或发布失败；`--check-dataset` 校验不一致，或桶里还没有格式声明。
- `2`：缺 `--dataset`、没有 tar 参数、`--chunk-size` 不大于 0、`--concurrency` 不大于 0、`--check-dataset` 与 tar 位置参数同时出现、flag 解析失败。

```bash
export TRAINCACHE_OBJSTORE_ENDPOINT=s3.example.com:9000
export TRAINCACHE_OBJSTORE_ACCESS_KEY=...
export TRAINCACHE_OBJSTORE_SECRET_KEY=...

trainpublish --dataset coco --bucket traincache \
  --chunk-size 67108864 /data/coco-000.tar /data/coco-001.tar
# snapID=20261026T120000Z-a1b2c3 entries=1000000 bytes=68719476736 unique_blocks=980000

trainpublish --dataset coco --bucket traincache --check-dataset /data/coco-dataset.yaml
# ok dataset=coco chunk_size=67108864 dataset_file=/data/coco-dataset.yaml
```

## traincacheds

数据集管理命令，源码在 `core/cmd/traincacheds`。查询面只读索引侧对象，不发任何写请求；写面两个命令：`rm` 提交一个新快照完成删除，`gc` 触发一次回收。两者默认只预览，`--yes` 才落盘。

用法：

```
traincacheds ls datasets
traincacheds ls snapshots <ds>
traincacheds ls entries   <ds> [--prefix p/] [--limit n]
traincacheds stat         <ds> [--path p.tar]
traincacheds rm           <ds> (--path p.tar[,p.tar] | --prefix p/) [--dry-run] [--yes]
traincacheds gc           <ds> [--orphan-blocks] [--orphan-grace 24h] [--dry-run] [--yes]
```

对象存储参数：

- `--endpoint`：string，默认取 `TRAINCACHE_OBJSTORE_ENDPOINT`，未设置时为 `127.0.0.1:9000`，可选。S3 兼容端点，不带 scheme。
- `--access-key`：string，默认取 `TRAINCACHE_OBJSTORE_ACCESS_KEY`，可选。静态 AK。
- `--secret-key`：string，默认取 `TRAINCACHE_OBJSTORE_SECRET_KEY`，可选。静态 SK。
- `--bucket`：string，默认取 `TRAINCACHE_OBJSTORE_BUCKET`，未设置时为 `traincache`，可选。单桶。
- `--secure`：bool，默认取 `TRAINCACHE_OBJSTORE_SECURE`，可选。用 TLS 连对象存储。

按命令划分的参数：

- `--prefix`：string，默认空，可选。`ls entries` 按前缀过滤条目，`rm` 按前缀批量删除。
- `--limit`：int，默认 `0`，可选。`ls entries` 只保留前 n 条，`0` 表示不限，取值不得小于 0。
- `--path`：string，默认空，可选。`stat` 只统计该路径下的条目，`rm` 是逗号分隔的待删路径。
- `--dry-run`：bool，默认 `false`，可选。`rm` 与 `gc` 只报告不落盘，覆盖 `--yes`。
- `--yes`：bool，默认 `false`，可选。`rm` 与 `gc` 真正落盘。
- `--orphan-blocks`：bool，默认 `false`，可选。`gc` 附带删除远端孤儿块对象。
- `--orphan-grace`：duration，默认 `24h`，可选。`gc --orphan-blocks` 的最小块龄，必须与 `--orphan-blocks` 同时给出，取值不得小于 0。

每个动词有自己的 flag 白名单，白名单外的 flag 打印用法并退出 2；`--` 之后的参数全部按位置参数处理。

各命令的输出与语义：

- `ls datasets`：每行一个数据集名，判据是存在 `dataset/<数据集名>/CURRENT` 这个键；无匹配时不输出任何行。
- `ls snapshots <ds>`：每行一个快照 ID，CURRENT 指向的那一行行尾标 ` current`；CURRENT 不存在时所有行都不带标记。
- `ls entries <ds>`：每行 `<path> <chunk> <size> <fp>`。
- `stat <ds>`：打印 `dataset=… snapshot=… shards=… entries=…`，只读 CURRENT 与 manifest 两次 GET 就出结果。`stat <ds> --path p` 会下载相关分片，并追加条目级的 `paths=… bytes=…`；条目级总量不在 manifest 里，所以不带 `--path` 时不报这两个数。
- `rm`：删指定 tar 或前缀下的条目，只走提交新快照这一条路径。读 CURRENT 展开目标条目，把「这次不带它」做成新快照；core 的删除保持精确的 `(path, chunk)` 粒度，`--prefix` 的展开在 CLI 侧完成。没有命中时打印 `nothing to remove`；预览时打印 `would remove entries=… paths=… bytes=…`，落盘时打印 `removed entries=… paths=… bytes=… snapshot=…`，多出的 `snapshot=` 是提交的新快照 ID。
- `gc`：手动跑一次对象存储侧回收，回收不再被引用的快照对象，`--orphan-blocks` 再清远端孤儿块对象。孤儿候选在删快照对象之前就用删除前的快照集合算好，预览与真正执行看到同一批候选，结果可复现。输出没有 `local_blocks=` 字段：各节点的本地副本由节点侧 Agent 的周期清扫删除（默认 10 分钟，`TRAINCACHE_LOCAL_GC_INTERVAL`），CLI 不提供这个入口。

退出码：

- `0`：成功，或预览正常结束。
- `1`：运行期错误，包括对象存储故障、数据集没有 CURRENT、CURRENT 段数不符、CURRENT 指向的 manifest 缺失或损坏。
- `2`：参数校验失败，包括未知动词、`ls` 的子命令不认识、位置参数个数不对、flag 不属于该动词、`--path` 与 `--prefix` 同时给、`rm` 两个都不给、`--limit` 小于 0、`--orphan-grace` 不带 `--orphan-blocks`、`--orphan-grace` 小于 0。

`gc` 的输出口径：

```
removed objects=2                                          # 不带 --orphan-blocks
removed objects=2 orphan_blocks=1 orphan_bytes=67108864    # 带 --orphan-blocks
```

预览时行首换成 `would remove`，其余字段与真正执行时相同。
