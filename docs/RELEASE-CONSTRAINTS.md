# 发布约束

## 1. 版本

- git tag `vX.Y.Z` 是版本号唯一来源。
- 镜像 tag、二进制版本（`-X main.version`）、`CHANGELOG.md` 版本标题三处逐字一致。
- `CHANGELOG.md` 发版前补条目，`dev` 的日常提交不写。
- 查旧版本接口用 `git show <tag>:<path>`。

## 2. 镜像

### 2.1 组件与名字

| 组件               | Dockerfile                               | 镜像名                                              |
|--------------------|------------------------------------------|-----------------------------------------------------|
| controller-manager | `controller/deploy/manager/Dockerfile`   | `ghcr.io/thewor1d877/traincache/controller-manager` |
| webhook            | `controller/deploy/webhook/Dockerfile`   | `ghcr.io/thewor1d877/traincache/webhook`            |
| kube-scheduler     | `controller/deploy/scheduler/Dockerfile` | `ghcr.io/thewor1d877/traincache/kube-scheduler`     |
| traincache-agentd  | `agent/Dockerfile`                       | `ghcr.io/thewor1d877/traincache/traincache-agentd`  |

镜像名固定，不带镜像站前缀。仓库内任何文件不写私有 registry 域名。

### 2.2 构建

从仓库根构建。`VERSION` 默认 `dev`，发版时传 tag：

```bash
docker build -f agent/Dockerfile \
  --build-arg VERSION=v0.1.0 \
  -t ghcr.io/thewor1d877/traincache/traincache-agentd:v0.1.0 .
```

基础镜像从 docker.io 拉取，走宿主 docker daemon 的 `registry-mirrors`，构建命令不带 registry 参数。

### 2.3 清单

- 仓库内清单写 `:dev`，用 `kustomize build controller/deploy` 渲染。
- 发布清单由 kustomize `images:` 把四个镜像改成 `@sha256:<digest>`，不手改组件 YAML。
- kind 环境用 `kind load docker-image` 导入同名镜像，清单的 `imagePullPolicy` 是 `IfNotPresent`。

## 3. 发布

### 3.1 GHCR

- 首次 push 自动建包，不需要预建仓库；路径全小写，namespace 是 GitHub 账号。
- 推送方需要 `packages: write` 权限。
- package 可见性改 Public 后匿名可拉。仓库 public 不等于 package public。

### 3.2 产物

- 四个镜像 `docker save` 进同一个 tar，共享基础层只存一份。
- tar 保留 GHCR 全名，导入后与清单引用逐字相同，不需要 retag。
- 只发 linux/amd64。
- 清单用 digest（`@sha256:`）引用。tag 配 `IfNotPresent` 时节点不会重拉同名旧镜像，digest 可以。
- 发布包不含 `controller/deploy/observability/`，也不含 minio、busybox、kindest/node 和对象存储。
- `install.yaml`、单 tar、CLI 二进制、`SHA256SUMS` 由 `release.yml` 在 tag 上生成并挂到 draft Release，不入库。

## 4. CI

四个 workflow，都在 `.github/workflows/`。合入门禁、产物构建、发布验证、draft 转正各归一段，前后关系见下图。

```
PR（dev→main）
  └─ e2e.yml 自动跑（门禁：代码在 kind 集群里能跑通）
       └─ 绿 → 合入 main

main 上打 tag v0.1.0 并推送
  └─ release.yml 触发
       ├─ 构建四个镜像 → 推 GHCR
       ├─ 渲染 digest
       └─ 生成 draft Release（草稿，不正式发布）

release.yml 完成后
  └─ release-smoke.yml 触发（workflow_run 监听 release.yml 成功）
       └─ 验证：镜像能拉、digest 正确、Release 资产完整、装机链路通
            ├─ 通过 → 人工确认，把 draft Release 发布为正式版
            └─ 失败 → 修复后重打 tag 或重跑
```

| 文件 | 触发 |
|---|---|
| `verify.yml` | push 到 `dev`/`main`；任意 base 的 PR |
| `e2e.yml` | PR 到 `main` 自动，或手动 |
| `release.yml` | push tag `v*` |
| `release-smoke.yml` | `release.yml` 成功后自动，或手动输入 tag |

### 4.1 触发矩阵

| 触发 | 内容 | 时长 | 是否阻塞 |
|---|---|---|---|
| push `dev` / 任意 PR | 三模块 build + vet + test `-race`；gofmt；禁用符号检查；`kubectl kustomize` 可渲染 | ~5 min | 必须过 |
| PR `dev`→`main` | 同上 + 版本一致性检查（清单不写手填版本、Dockerfile 不写死 tag、CHANGELOG 与 tag 对齐）+ 文档引用完整性 | ~5 min | 必须过 |
| PR `dev`→`main` | kind e2e | ~45 min | 必须过，合并门禁 |
| push tag `v*` | 构建推镜像 → 渲染 `install.yaml` → 打 tar → 建 draft Release | ~15 min | 发布本身 |
| `release.yml` 成功 | release-smoke：五项资产齐全且哈希对得上、四个 digest 可解析、临时 kind 装机链路通 | ~15 min | 通过后人工把 draft 转正 |

### 4.2 纪律

- `main` 只收 PR，`verify` 与 `kind e2e` 设 required；`dev` 允许直推。
- CI 不跑 bench：bench 依赖本机数据层和 32G 内存，属于实验装置。

### 4.3 实现

- `make verify-<module>` 依次跑 build、vet、`go test -race`、gofmt；controller 的 job 另查禁用符号（`GetPreheatProgress`、`ScorePolicy.Window`、`consumeOrderRef`）。
- `make verify-metadata` 跑 `hack/verify-release-metadata.sh`（版本、镜像名、CHANGELOG 与 tag）和 `hack/verify-doc-refs.sh`（文档相对链接）。
- `release.yml` 只在 tag 上跑；它推四个镜像、编译 CLI、用 `hack/render-install.sh` 把 digest 写进 `install.yaml`、`docker save` 单 tar，并把清单、tar、CLI 与 `SHA256SUMS` 挂到 draft Release。
- draft 转正由人工在 Release 页面点 Publish，CI 不自动发布。要改成自动，在 `release-smoke` 成功后追加 `gh release edit <tag> --draft=false`，该步需要 `contents: write` 权限。
- `release-smoke.yml` 由 `workflow_run` 在 `release.yml` 成功后触发，也可以手动输入 tag；该文件要先合入 `main`，`workflow_run` 才会注册。它先跑 `hack/verify-release-assets.sh`：五项资产（清单、镜像 tar、两个 CLI、`SHA256SUMS`）逐项比对齐全，`SHA256SUMS` 覆盖的四个文件哈希对得上，`install.yaml` 里的四个 `@sha256:` 引用在 GHCR 可解析（`docker manifest inspect`，只看 manifest，不下载层）。层数据的真拉由紧随其后的装机段完成：kind 节点按清单里的 digest 从 GHCR 匿名拉取，包不是 Public 就在这一步起不来。装机段检查四个组件 Ready、CRD Established、Pod 用的是 GHCR 镜像。它不跑业务闭环，闭环由 e2e 覆盖。
- e2e 与 smoke 的节点镜像由 `NODE_IMAGE` 指定，默认 `docker.io/kindest/node:v1.37.0`；产物分别写到 `TRAINCACHE_DATA_ROOT/controller/e2e/results/` 与 `TRAINCACHE_DATA_ROOT/hack/release-smoke/`。

## 5. 用户自备

发布包只有控制面、Agent 与 CRD。webhook 的 TLS Secret `traincache-webhook-tls` 与 caBundle 不在清单里，安装方自备；`release-smoke` 用自签证书演示这一步。

### 5.1 对象存储

后端是 minio-go v7，支持 S3 API 的存储都能接，例如 COS、OSS、S3、MinIO。

| 操作 | 用途 |
|---|---|
| `GetObject` | Agent 按块拉整个对象，不发 Range 请求 |
| `StatObject`（HEAD） | 读前确认对象存在 |
| `ListObjects`（递归） | 枚举快照与索引对象 |
| `PutObject` + `If-None-Match: *` / `If-Match` | 发布索引，冲突必须回 412 |
| SigV4 + 静态 accessKey/secretKey | 鉴权 |

键布局：索引在 `dataset/<dataset>/snapshots/<id>/`，含 `manifest.json` 与 `shard-<fp8>.idx`；数据块在 `<dataset>/blocks/<path>#<chunk>`；桶根 `_traincache/format.json` 记录块大小。单桶，一个桶只有一个块大小；改大小必须清空桶重灌或换桶。

端点和凭据分两处，必须指向同一个桶：

- 控制面：`DataSet.spec.endpoint`（不带 scheme）、`spec.bucket`、`spec.secretRef`。Secret 字段 `accessKey`、`secretKey`，与 DataSet 同 namespace。
- Agent：`TRAINCACHE_OBJSTORE_ENDPOINT`、`TRAINCACHE_OBJSTORE_ACCESS_KEY`、`TRAINCACHE_OBJSTORE_SECRET_KEY`、`TRAINCACHE_OBJSTORE_BUCKET`、`TRAINCACHE_OBJSTORE_SECURE`。

条件写支持用 `trainpublish --probe-conditional-write` 探测。

### 5.2 训练负载

- 训练镜像由用户自备，本项目不构建、不代发。e2e 用 busybox 加 `sleep` 顶替。
- OrderSource sidecar `agent/ordersrc/ordersrc.py` 只 import `argparse`、`json`、`os`、`struct`、`sys`，用到 f-string，python3.6 以上即可。
- 消费顺序由 sidecar 按 Job 配置算出；读 miss 与阶段切换事件由训练代码的钩子上报。不插桩时 Agent 退到 inotify 兜底。
- `WarmupTask.spec.preheatFanout` 不填时默认 2（`preheatFanoutMin` 默认 1）。单节点候选时实际只分配 1 个预热节点，不触发 `MinUnmet`；介意预热流量（多节点上预热 IO 与缓存占用最多翻倍）可显式写 `preheatFanout: 1`。

### 5.3 运行时参数

运行时参数集中在 `traincache` namespace 的 ConfigMap `traincache-config`（`controller/deploy/agent/configmap.yaml`），共六个 key。Agent 读 `cacheCapacityBytes`、`prefetchLookahead`、`evictPolicy`、`datasetMetricsWhitelist`；Controller 读 `cacheCapacityBytes`、`startupWindowBytes`、`maxFillFraction` 三个算 K。改值只需编辑 ConfigMap 并重启对应工作负载，不必改清单；`cacheCapacityBytes` 同时被 Agent 与 Controller 读取，改它要重启两者。

| key | 默认值 | 归属 | 说明 |
|---|---|---|---|
| `cacheCapacityBytes` | `0` | Agent、Controller | 本地缓存字节上限（Controller 只用它算 `K_cache`）。`0` 关闭字节维度，淘汰只看磁盘水位；设正整数时淘汰循环额外按字节阈值触发（1GiB 写 `1073741824`） |
| `prefetchLookahead` | `16` | Agent | 游标之后持续预取的邻域块数 |
| `evictPolicy` | `heat` | Agent | 淘汰顺序，取 `heat`/`lru`/`lfu`；非法值使 Agent 拒绝启动 |
| `datasetMetricsWhitelist` | 空（`""`） | Agent | 逗号分隔的 DataSet 名。为空时所有 DataSet 的指标 `dataset` 标签全部归并为 `other`；只有白名单内的名字保留原名 |
| `startupWindowBytes` | `536870912`（512MiB） | Controller | 自动算 K 的启动窗口，必须 `>0`：`K_window = ceil(startupWindowBytes / chunkSize)`；1GiB 写 `1073741824` |
| `maxFillFraction` | `0.125`（八分之一） | Controller | 全部 Pod 的预热总量占缓存容量的比例，取值 `(0,0.25]`：`K_cache = floor(cacheCapacityBytes × maxFillFraction / (podCount × chunkSize))`；`cacheCapacityBytes=0` 时不参与 |

`k` 的确定：`k` 为 0 或省略时 Controller 自动计算并写回 `spec`，发 Normal 事件 `KDefaulted`；自动值 `K_i = clamp(min(W_i, K_window, K_cache), 1, W_i)`，`W_i` 是该 Pod 实际分到的块数。显式 `k` 的有效范围是 1..`W_i`；`k > W_i` 置 `Condition InvalidSpec=False` 并拒绝，不钳制、不截断。实际预热块数 = `min(max(K_i, score.cutoff), W_i)`。全部 Pod 的实际请求块数合计超过 `cacheCapacityBytes × maxFillFraction` 时发 Warning 事件 `KExceedsFillLimit`，任务继续执行。

指标口径：`dataset` 标签只对白名单内的 DataSet 取原名，其余一律归并 `other`，用来把标签基数钉死（D35）。**清空白名单后所有带 `dataset` 标签的指标都归并到 `other`**，此时按 dataset 拆分的看板看不出数据集差异，排查单数据集时再把名字加回白名单。

ConfigMap 的 key 用 camelCase。Agent 四个 key 由 DaemonSet 用 `configMapKeyRef` 注入，环境变量名不变：`TRAINCACHE_CACHE_CAPACITY_BYTES`、`TRAINCACHE_PREFETCH_LOOKAHEAD`、`TRAINCACHE_EVICT_POLICY`、`TRAINCACHE_DATASET_WHITELIST`。Controller 的三个 K 参数由 manager Deployment 通过 `configMapKeyRef` 注入。ConfigMap 或任一 key 缺失时读取方报错，不静默回退到内置默认值。完整参数表（集群、数据集、训练任务）见 [参数配置](PARAMETER-CONFIGURATION.md)。

## 6. CLI 下发

发布两个二进制：gRPC 客户端 `traincache`（`agent/cmd/traincache`）与快照发布器 `trainpublish`（`core/cmd/trainpublish`）。发版时由 `release.yml` 编译 `traincache-linux-amd64`、`trainpublish-linux-amd64`（linux/amd64、CGO 关闭、静态链接），与 `install.yaml`、镜像 tar、`SHA256SUMS` 一起挂在 Release 上。

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

只发布 linux/amd64，不提供包管理器与 Windows 制品；其他平台自行 `go build ./agent/cmd/traincache` 或 `go build ./core/cmd/trainpublish`。两个命令的参数见 [CLI 接口](CLI-INTERFACE.md)。
