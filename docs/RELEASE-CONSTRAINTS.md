# 发布约束

## 版本

- git tag `vX.Y.Z` 是版本号唯一来源。
- 镜像 tag、二进制版本（`-X main.version`）、`CHANGELOG.md` 版本标题三处逐字一致。
- `CHANGELOG.md` 发版前补条目，`dev` 的日常提交不写。
- 查旧版本接口用 `git show <tag>:<path>`。

## 镜像

### 组件与名字

| 组件               | Dockerfile                               | 镜像名                                              |
|--------------------|------------------------------------------|-----------------------------------------------------|
| controller-manager | `controller/deploy/manager/Dockerfile`   | `ghcr.io/thewor1d877/traincache/controller-manager` |
| webhook            | `controller/deploy/webhook/Dockerfile`   | `ghcr.io/thewor1d877/traincache/webhook`            |
| kube-scheduler     | `controller/deploy/scheduler/Dockerfile` | `ghcr.io/thewor1d877/traincache/kube-scheduler`     |
| traincache-agentd  | `agent/Dockerfile`                       | `ghcr.io/thewor1d877/traincache/traincache-agentd`  |
| traincache-gateway | `gateway/Dockerfile`                     | `ghcr.io/thewor1d877/traincache/traincache-gateway` |
| checkpointd        | `checkpoint/Dockerfile`                  | `ghcr.io/thewor1d877/traincache/traincache-checkpointd` |

镜像名固定，不带镜像站前缀。仓库内任何文件不写私有 registry 域名。

checkpointd 没有自己的 Deployment：它由准入注入到训练 Pod 当 sidecar。它的 digest 写进清单里的 `traincache-checkpoint-image` ConfigMap（键 `checkpointImage`），webhook 读它作为注入镜像的发布默认值。

### 构建

从仓库根构建。`VERSION` 默认 `dev`，候选构建时传目标版本号，也就是随后要打的 tag：

```bash
docker build -f agent/Dockerfile \
  --build-arg VERSION=v0.1.0 \
  -t ghcr.io/thewor1d877/traincache/traincache-agentd:v0.1.0 .
```

基础镜像从 docker.io 拉取，走宿主 docker daemon 的 `registry-mirrors`，构建命令不带 registry 参数。

本机构建用 `hack/build-images.sh`：`TAG` 默认 `dev`，参数给组件名只构建子集。脚本先把基础镜像准备成官方短名，本机只有带镜像站前缀的同名副本时执行 `docker tag` 复用，不产生网络访问；两种名字都不在本机时立即报错，并给出 `docker pull` 与 `docker tag` 两条处理命令。

### 清单

- 仓库内清单写 `:dev`，用 `kustomize build controller/deploy` 渲染。
- 发布清单由 kustomize `images:` 把五个 Deployment 的镜像改成 `@sha256:<digest>`，checkpointd 的 digest 由 overlay 的 patch 写进 `traincache-checkpoint-image` ConfigMap，不手改组件 YAML。
- 候选镜像另打 `sha-<7 位短 sha>` 标签，供人工复现候选；清单与发布物只用 `@sha256` 引用，不出现 `sha-<短 sha>`。
- kind 环境用 `./hack/kind-load.sh <cluster> <image>` 导入同名镜像（单平台导出，不用 `kind load docker-image`），清单的 `imagePullPolicy` 是 `IfNotPresent`。

## 发布

### GHCR

- 首次 push 自动建包，不需要预建仓库；路径全小写，namespace 是 GitHub 账号。
- 推送方需要 `packages: write` 权限。
- package 可见性改 Public 后匿名可拉。仓库 public 不等于 package public。

### 产物

- 全部镜像 `docker save` 进同一个 tar，共享基础层只存一份。
- tar 保留 GHCR 全名，导入后与清单引用逐字相同，不需要 retag。
- 只发 linux/amd64。
- 清单用 digest（`@sha256:`）引用。tag 配 `IfNotPresent` 时节点沿用同名旧镜像，换 digest 才会重拉。
- 发布包不含 `controller/deploy/observability/`，也不含 minio、busybox、kindest/node 和对象存储。
- `install.yaml` 与两个 CLI 由候选构建产出，tag 晋升后连同单 tar、`SHA256SUMS` 挂到 Release，不入库。

## CI

五个 workflow 都在 `.github/workflows/`，依次负责合入门禁、候选构建、候选验证、tag 晋升与发布。

base 是 main 的 PR 自动触发 `e2e.yml`。门禁是代码在 kind 集群里能跑通，跑绿才合入 main。

main 上手动触发 `release-candidate.yml`，输入 `version=vX.Y.Z`。它构建全部镜像并推 `:sha-<7 位短 sha>` 标签（VERSION 烤进二进制），渲染 `install.yaml`，编译两个 CLI，上传 artifact `release-candidate-<短 sha>`，artifact 里含 `candidate.json`。

候选构建完成后 `release-smoke.yml` 触发，`workflow_run` 监听 `release-candidate` 成功。验证内容是候选文件齐全、六个 digest 可解析、装机链路通。通过就进入下一步；失败就在没改代码的前提下重跑同一 sha 的候选与 smoke。

main 上打 tag `vX.Y.Z` 并推送会触发 `release.yml`。它找同一 sha 的候选，断言 `candidate.json` 的 version 与 tag 逐字相等，再断言同一 sha 有成功的 release-smoke run；缺任一条就停在这一步。断言全过就把候选 digest 晋升成 `:vX.Y.Z`，以 draft 建 Release，五个发布物上传成功后翻成正式版。

人工的闸门在打 tag 这一步：推送 `vX.Y.Z` 就等于放行发布。

| 文件 | 触发 |
|---|---|
| `verify.yml` | push 到 `dev`/`main`，任意 base 的 PR |
| `e2e.yml` | PR 到 `main` 自动，或手动 |
| `release-candidate.yml` | 手动输入 `version` 与可选 `sha` |
| `release.yml` | push tag `v*` |
| `release-smoke.yml` | `release-candidate` 成功后自动，或手动输入 `sha`（可选 `run_id`） |

### 触发矩阵

| 触发 | 内容 | 是否阻塞 |
|---|---|---|
| push `dev` / 任意 PR | 各模块 build + vet + test `-race`，gofmt，禁用符号检查，`kubectl kustomize` 可渲染 | 必须过 |
| PR `dev` 到 `main` | 上一行的内容加版本一致性检查（清单不写手填版本、Dockerfile 不写死 tag、CHANGELOG 与 tag 一致）加文档引用完整性 | 必须过 |
| PR `dev` 到 `main` | kind e2e | 必须过，合并门禁 |
| 手动 `release-candidate` | 全部镜像推 `:sha-<7 位短 sha>`，渲染 `install.yaml`，编译 CLI，上传候选 artifact | 发版前置 |
| 候选构建成功 | release-smoke：候选文件齐全且哈希对得上、六个 digest 可解析、临时 kind 装机链路通 | 由 `release.yml` 断言 |
| push tag `v*` | 断言候选 version 与 tag 一致，断言同 sha 有成功 smoke，候选 digest 晋升 `:vX.Y.Z`，打单 tar，建 Release 并转正 | 发布本身 |

### 要求

- `main` 只收 PR，`verify` 与 `kind e2e` 设 required；`dev` 允许直推。
- CI 不跑 bench：bench 依赖本机数据层和 32G 内存，属于实验装置。
- 候选或 smoke 失败先判性质：没改代码就重跑同一 sha 的候选与 smoke，`version` 不变；要改代码就回 PR 重新走，用新提交重建候选，`version` 仍不变。
- 版本号在 Release 转正那一刻才算用掉；候选、晋升与上传阶段的失败都不消耗版本号，重跑 tag job 仍用同一个版本号。
- 候选 artifact 保留 14 天，tag 要在候选构建后 14 天内打；超期就按同一 sha 重跑候选，版本号不变。
- 版本号与 tag 逐字一致靠构建期注入：候选构建用 `--build-arg VERSION=<version>` 把版本写入镜像与二进制，tag job 断言 `candidate.json` 的 version 与 tag 相等，不等就失败并提示重建候选。

### 实现

- `make verify-<module>` 依次跑 build、vet、`go test -race`、gofmt；controller 的 job 另查禁用符号（`GetPreheatProgress`、`ScorePolicy.Window`、`consumeOrderRef`）。
- `make verify-metadata` 跑 `hack/verify-release-metadata.sh`（版本、镜像名、CHANGELOG 与 tag）、`hack/verify-doc-refs.sh`（文档相对链接）与 `hack/verify-config-placement.sh`（配置落位：清单零字面量 env、ConfigMap 与引用对齐）。
- `release-candidate.yml` 推六个 `:sha-<7 位短 sha>` 镜像，产出 `install.yaml`、两个 CLI、`candidate.json` 与 `SHA256SUMS`，作为 artifact `release-candidate-<短 sha>` 保留 14 天。`candidate.json` 记 `sha`、`short_sha`、`version`、`images` 下每个组件的 digest（值带 `sha256:` 前缀）与 `created_at`。
- `release.yml` 只在 tag 上跑。它按 commit 找成功的候选 run，校验候选 version 与 tag 逐字相等、候选 sha 与 tag 指向的提交相同，再断言同一 sha 有成功的 release-smoke 运行；缺任一条就停在这一步，不晋升、不建 Release。
- 判定通过后用 `docker buildx imagetools create` 把候选 digest 晋升成 `:vX.Y.Z`，再回查 `:vX.Y.Z` 指向的清单中含候选 digest。晋升会把单平台的 image manifest 包进一层 manifest list，tag 摘要与候选摘要不相等，两者指向同一个镜像内容。
- 随后 `docker pull` 全部镜像、`docker save` 成单 tar，连同候选的 `install.yaml`、两个 CLI 与 `SHA256SUMS` 以 draft 建 Release；`gh release create` 成功后，同 job 的最后一步跑 `gh release edit <tag> --draft=false` 转正。
- 转正放在建 Release 之后一步，是为了让上传中断停在 draft：五个发布物齐了才对外可见。留下的 draft 要先 `gh release delete <tag>` 再重跑 tag job，否则 `gh release create` 会因同名 Release 已存在而失败。
- `release-smoke.yml` 由 `workflow_run` 在 `release-candidate` 成功后触发，也可以手动输入 sha；该文件要先合入 `main`，`workflow_run` 才会注册。
- 它先跑 `hack/verify-release-assets.sh <候选目录>`，判据三条：`install.yaml` 恰好六个 `@sha256:` 引用（五个 Deployment 镜像加 `traincache-checkpoint-image` 里的 checkpointd）、`SHA256SUMS` 覆盖目录内每个文件且哈希对得上、六个 digest 在 GHCR 可解析（`docker manifest inspect`，只看 manifest，不下载层）。
- 层数据由紧随其后的装机段实际拉取：kind 节点按清单里的 digest 从 GHCR 匿名拉取，package 不是 Public 时这一步拉取失败。装机段检查五个有 Deployment 的组件 Ready、CRD Established、Pod 用的是 GHCR 镜像。业务闭环由 e2e 覆盖。
- e2e 与 smoke 的节点镜像由 `NODE_IMAGE` 指定，默认 `docker.io/kindest/node:v1.37.0`；产物分别写到 `TRAINCACHE_DATA_ROOT/controller/e2e/results/` 与 `TRAINCACHE_DATA_ROOT/hack/release-smoke/`。

## 用户自备

发布包只有控制面、Agent 与 CRD。webhook 的 TLS Secret `traincache-webhook-tls` 与 caBundle 不在清单里，安装方自备；`release-smoke` 用自签证书演示这一步。

### 对象存储

后端是 minio-go v7，支持 S3 API 的存储都能接，例如 COS、OSS、S3、MinIO。

| 操作 | 用途 |
|---|---|
| `GetObject` | Agent 按块拉对象，并行开关打开时每个块发 4 条带 Range 头的 GetObject |
| `StatObject`（HEAD） | 读前确认对象存在 |
| `ListObjects`（递归） | 枚举快照与索引对象 |
| `PutObject` + `If-None-Match: *` / `If-Match` | 发布索引，冲突必须回 412 |
| SigV4 + 静态 accessKey/secretKey | 鉴权 |

键布局：索引在 `dataset/<dataset>/snapshots/<id>/`，含 `manifest.json` 与 `shard-<fp8>.idx`；数据块在 `<dataset>/blocks/<path>#<chunk>`；桶根 `_traincache/format.json` 记录块大小。单桶，一个桶只有一个块大小；改块大小要清空桶重新发布，或者换一个桶。

端点和凭据分两处，必须指向同一个桶：

- 控制面：`DataSet.spec.endpoint`（不带 scheme）、`spec.bucket`、`spec.secretRef`。Secret 字段 `accessKey`、`secretKey`，与 DataSet 同 namespace。
- Agent：`TRAINCACHE_OBJSTORE_ENDPOINT`、`TRAINCACHE_OBJSTORE_ACCESS_KEY`、`TRAINCACHE_OBJSTORE_SECRET_KEY`、`TRAINCACHE_OBJSTORE_BUCKET`、`TRAINCACHE_OBJSTORE_SECURE`。

控制面读凭证按 DataSet 所在 namespace 逐个授权，集群默认只授了 `traincache` 一个 namespace（`controller/deploy/manager/rbac.yaml`）。DataSet 放到别的 namespace，安装方要在该 namespace 补一份 Role + RoleBinding，给 controller-manager 的 ServiceAccount `secrets: get`；写法在 `controller/e2e/manifests/tenant.yaml`。漏授时 DataSet 停在 `Ready=False`，condition 报 `StorageUnreachable: ... secrets "traincache-objstore" is forbidden`。不给集群级 Secret 读权限。

条件写支持用 `trainpublish --probe-conditional-write` 探测。

### 训练负载

- 训练镜像由用户自备，仓库不构建、不发布。e2e 用 busybox 加 `sleep` 作为占位负载。
- OrderSource sidecar `agent/ordersrc/ordersrc.py` 只 import `argparse`、`json`、`os`、`struct`、`sys`，用到 f-string，python3.6 以上即可。
- 消费顺序由 sidecar 按 Job 配置算出；读 miss 与阶段切换事件由训练代码的钩子上报。不插桩时 Agent 退到 inotify 兜底。
- `WarmupTask.spec.preheatFanout` 不填时默认 2（`preheatFanoutMin` 默认 1）。单节点候选时实际只分配 1 个预热节点，不触发 `MinUnmet`；介意预热流量（多节点上预热 IO 与缓存占用最多翻倍）可显式写 `preheatFanout: 1`。

### 运行时参数

运行时参数在两个 ConfigMap：`traincache` namespace 的 `traincache-config`（`controller/deploy/agent/configmap.yaml`，现 21 个配置项）与 kube-system 的 `traincache-scheduler-config`（`controller/deploy/scheduler/kube-scheduler-config.yaml`，scheduler 的 2 个配置项，同一对象还挂 `scheduler.yaml`）。scheduler 单独一份，是因为 `configMapKeyRef` 只在本 Pod 的 namespace 找对象，kube-system 里的 scheduler 引用不到 `traincache-config`。数据集与训练任务两层的参数落在 DataSet 与 WarmupTask 两个 CRD 上，不进 ConfigMap。

改值只需编辑 ConfigMap 并重启对应工作负载。`cacheCapacityBytes` 同时被 Agent 与 Controller 读取，改它要重启两者；`objstoreEndpoint`、`objstoreBucket`、`objstoreSecure`、`objstoreCompression` 同时被 Agent 与网关读取，改这四个键要重启两者；`fetchRangeParallel` 只被 Agent 读取，`gateNetworkBandwidthBytes` 只被 Controller 读取。其余键各有一个读取方：`gatewayBucketConcurrency`、`gatewayBlockBufferBytes`、`gatewayCache`、`gatewayCacheDatasetBytes`、`gatewayCacheTotalBytes` 只被网关读取，改这五个键重启网关 Deployment；scheduler 的两个键只被 scheduler 读取，改完重启 scheduler Deployment。

`cacheCapacityBytes` 是本地缓存字节上限，`0` 关闭字节维度，淘汰只看磁盘水位，设正整数时淘汰循环额外按字节阈值触发，1GiB 写 `1073741824`。

五份工作负载清单里没有任何 `value:` 字面量 env。落位规则是：凭证走 Secret，节点名走 Downward API，等于代码默认的参数不写清单，其余参数写进工作负载自己 namespace 里的 ConfigMap 并用 `configMapKeyRef` 引用。

| key | 默认值 | 归属 | 说明 |
|---|---|---|---|
| `cacheCapacityBytes` | `"0"` | Agent、Controller | 本地缓存字节上限，Controller 只用它算 `K_cache`。 |
| `prefetchLookahead` | `"16"` | Agent | 游标之后持续预取的邻域块数。 |
| `evictPolicy` | `"heat"` | Agent | 淘汰顺序，取 `heat`/`lru`/`lfu`，非法值使 Agent 拒绝启动。 |
| `datasetMetricsWhitelist` | 空（`""`） | Agent | 逗号分隔的 DataSet 名，为空时所有 DataSet 的指标 `dataset` 标签合并成 `other`。 |
| `startupWindowBytes` | `"536870912"`（512MiB） | Controller | 自动算 K 的启动窗口，必须 `>0`，算式是 `K_window = ceil(startupWindowBytes / chunkSize)`，1GiB 写 `1073741824`。 |
| `maxFillFraction` | `"0.125"`（八分之一） | Controller | 全部 Pod 的预热总量占缓存容量的比例，取值 `(0,0.25]`，算式是 `K_cache = floor(cacheCapacityBytes × maxFillFraction / (podCount × chunkSize))`，`cacheCapacityBytes=0` 时不参与计算。 |
| `objstoreEndpoint` | `"minio.minio.svc.cluster.local:9000"` | Agent、网关 | 源站端点，不带 scheme，接 COS/OSS/S3 时改这一处。 |
| `objstoreBucket` | `"traincache"` | Agent、网关 | 单桶名，必须等于 `DataSet.spec.bucket`，`SubmitPreheat` 不带桶名，不一致时每个块读取都失败。 |
| `objstoreSecure` | `"false"` | Agent、网关 | `true`/`1`/`yes` 时用 HTTPS 连源站。 |
| `objstoreCompression` | `"false"` | Agent、网关 | 传输压缩开关，`true`/`1`/`yes` 时向源站声明 gzip，其余取值关。 |
| `fetchRangeParallel` | `"true"` | Agent | Range 并行下载开关，默认打开，打开时每个块拆成 4 条 Range GET 并行下载，关闭时每个块走一条整对象 GET，块大小不参与判定。 |
| `gatewayEndpoint` | 空（`""`） | Agent | 合并回源网关地址，不带 scheme，空表示全部直连，非空时只有块 GET 走网关。 |
| `gatewaySecure` | `"false"` | Agent | Agent 到网关是否 TLS，与 `objstoreSecure` 无关。 |
| `gatewayBucketConcurrency` | `"64"` | 网关 | 到源站的并发下载上限，正整数，非法值使网关拒绝启动。 |
| `gatewayBlockBufferBytes` | `"10485760"`（10MiB） | 网关 | 每个在途块环形缓冲的字节数，正整数，非法值使网关拒绝启动。 |
| `gatewayCache` | `"true"` | 网关 | 磁盘缓存开关，默认打开，`1`/`true`/`yes` 开，其余取值关，关掉后网关只做在途合并不落盘。 |
| `gatewayCacheDatasetBytes` | `"34359738368"`（32GiB） | 网关 | 单个 dataset 在网关盘上的驻写上限，正整数，非法值使网关拒绝启动。 |
| `gatewayCacheTotalBytes` | `"68719476736"`（64GiB） | 网关 | 全局驻写上限，正整数且不小于 `gatewayCacheDatasetBytes`，否则网关拒绝启动。 |
| `controllerPollInterval` | `"1s"` | Controller | 轮询 Agent 进度的间隔，代码默认 2s。 |
| `controllerScoreTTL` | `"10m"` | Controller | 节点分数有效期，代码默认 5m。 |
| `gateNetworkBandwidthBytes` | `"0"` | Controller | 网络带宽上限，单位 bytes/sec，`0` 关闭门控的带宽判断，非零时门控拿 0.8 倍与 Agent 上报吞吐比较。 |
| `schedulerScoreTTL` | `"10m"` | Scheduler，写在 `traincache-scheduler-config` | 分数过期判定，必须与 `controllerScoreTTL` 同值。 |
| `schedulerLogLevel` | `"debug"` | Scheduler，写在 `traincache-scheduler-config` | 每个 (pod,node) 决策打一行 `scheduler score`，manager 与 webhook 用代码默认 `info`。 |

`k` 的确定：`k` 为 0 或省略时 Controller 自动计算并写回 `spec`，发 Normal 事件 `KDefaulted`；自动值 `K_i = clamp(min(W_i, K_window, K_cache), 1, W_i)`，`W_i` 是该 Pod 实际分到的块数。显式 `k` 的有效范围是 1..`W_i`；`k > W_i` 置 `Condition InvalidSpec=False` 并拒绝，不钳制、不截断。实际预热块数 = `min(max(K_i, score.cutoff), W_i)`。全部 Pod 的实际请求块数合计超过 `cacheCapacityBytes × maxFillFraction` 时发 Warning 事件 `KExceedsFillLimit`，任务继续执行。

指标口径：`dataset` 标签只对白名单内的 DataSet 取原名，其余一律合并成 `other`，目的是限制该标签的基数。白名单为空时所有带 `dataset` 标签的指标都合并到 `other`，按 dataset 拆分的看板看不出数据集差异，排查单数据集时再把名字加回白名单。

ConfigMap 的配置项用 camelCase。Agent 的 11 项由 DaemonSet 用 `configMapKeyRef` 注入，环境变量名不变：`TRAINCACHE_CACHE_CAPACITY_BYTES`、`TRAINCACHE_PREFETCH_LOOKAHEAD`、`TRAINCACHE_EVICT_POLICY`、`TRAINCACHE_DATASET_WHITELIST`、`TRAINCACHE_OBJSTORE_ENDPOINT`、`TRAINCACHE_OBJSTORE_BUCKET`、`TRAINCACHE_OBJSTORE_SECURE`、`TRAINCACHE_OBJSTORE_COMPRESSION`、`TRAINCACHE_FETCH_RANGE_PARALLEL`、`TRAINCACHE_GATEWAY_ENDPOINT`、`TRAINCACHE_GATEWAY_SECURE`。Controller 的 6 项由 `controller/deploy/manager/deployment.yaml` 注入：`CONTROLLER_POLL_INTERVAL`、`CONTROLLER_SCORE_TTL`、`TRAINCACHE_CACHE_CAPACITY_BYTES`、`TRAINCACHE_STARTUP_WINDOW_BYTES`、`TRAINCACHE_MAX_FILL_FRACTION`、`TRAINCACHE_GATE_NETWORK_BANDWIDTH`。scheduler 的 2 项由 `controller/deploy/scheduler/deployment.yaml` 注入：`SCHEDULER_SCORE_TTL`、`CONTROLLER_LOG_LEVEL`。网关的 9 项由 `controller/deploy/gateway/deployment.yaml` 注入：`TRAINCACHE_OBJSTORE_ENDPOINT`、`TRAINCACHE_OBJSTORE_BUCKET`、`TRAINCACHE_OBJSTORE_SECURE`、`TRAINCACHE_OBJSTORE_COMPRESSION`、`TRAINCACHE_GATEWAY_BUCKET_CONCURRENCY`、`TRAINCACHE_GATEWAY_BLOCK_BUFFER_BYTES`、`TRAINCACHE_GATEWAY_CACHE`、`TRAINCACHE_GATEWAY_CACHE_DATASET_BYTES`、`TRAINCACHE_GATEWAY_CACHE_TOTAL_BYTES`；AK/SK 另走 Secret `traincache-objstore`。webhook 另有一条 `CHECKPOINT_IMAGE`，值取自 `traincache-checkpoint-image` ConfigMap，是注入训练 Pod 的镜像引用。ConfigMap 或任一被引用的配置项缺失时读取方 Pod 停在 `CreateContainerConfigError`，不静默回退到内置默认值。

## CLI 下发

发布两个二进制：gRPC 客户端 `traincache`（`agent/cmd/traincache`）与快照发布器 `trainpublish`（`core/cmd/trainpublish`）。候选构建时由 `release-candidate.yml` 编译 `traincache-linux-amd64`、`trainpublish-linux-amd64`（linux/amd64、CGO 关闭、静态链接），这两个文件随候选 artifact 进入 Release 资产，与 `install.yaml`、镜像 tar、`SHA256SUMS` 一起挂在 Release 上。

安装：

```bash
sha256sum --ignore-missing -c SHA256SUMS
install -m 0755 traincache-linux-amd64 /usr/local/bin/traincache
install -m 0755 trainpublish-linux-amd64 /usr/local/bin/trainpublish
traincache help
trainpublish -h
```

`traincache` 也可从 Agent 镜像取；`trainpublish` 不在镜像里，只能下载发布物或自行编译：

```bash
docker create --name tc ghcr.io/thewor1d877/traincache/traincache-agentd@sha256:<digest>
docker cp tc:/usr/local/bin/traincache ./traincache
docker rm tc
```

CLI 默认连 `127.0.0.1:9410`。访问集群内的 Agent 先做端口转发：

```bash
kubectl -n traincache port-forward pod/<agentd-pod> 9410:9410
```
