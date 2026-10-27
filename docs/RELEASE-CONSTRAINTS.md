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
- `install.yaml`、单 tar、CLI 二进制、`SHA256SUMS` 由 `release.yml` 在 tag 上生成并挂 Release，不入库。

## 4. CI

四个 workflow，都在 `.github/workflows/`：

| 文件 | 触发 |
|---|---|
| `verify.yml` | push、PR 到 `dev`/`main` |
| `e2e.yml` | PR 打 `ci/e2e` 标签，或手动 |
| `release.yml` | push tag `v*` |
| `release-smoke.yml` | 手动，输入已发布的 tag |

### 4.1 触发矩阵

| 触发 | 内容 | 时长 | 是否阻塞 |
|---|---|---|---|
| push `dev` / PR→`dev` | 三模块 build + vet + test `-race`；gofmt；禁用符号检查；`kubectl kustomize` 可渲染 | ~5 min | 必须过 |
| PR `dev`→`main` | 同上 + 版本一致性检查（清单不写手填版本、Dockerfile 不写死 tag、CHANGELOG 与 tag 对齐）+ 文档引用完整性 | ~5 min | 必须过 |
| PR 打 `ci/e2e` 标签 / 手动 | kind e2e | ~45 min | 不阻塞合并，发版前必过 |
| push tag `v*` | 构建推镜像 → 渲染 `install.yaml` → 打 tar → 建 Release | ~15 min | 发布本身 |
| 发布后手动 | release-smoke | ~10 min | 发版后必过 |

### 4.2 纪律

- `main` 只收 PR，校验设 required；`dev` 允许直推。
- e2e 不进每个 PR：要起 kind、拉镜像、会抖，进每个 PR 只会导致 CI 被关掉。
- CI 不跑 bench：bench 依赖本机数据层和 32G 内存，属于实验装置。

### 4.3 实现

- `make verify-<module>` 依次跑 build、vet、`go test -race`、gofmt；controller 的 job 另查禁用符号（`GetPreheatProgress`、`ScorePolicy.Window`、`consumeOrderRef`）。
- `make verify-metadata` 跑 `hack/verify-release-metadata.sh`（版本、镜像名、CHANGELOG 与 tag）和 `hack/verify-doc-refs.sh`（文档相对链接）。
- `release.yml` 只在 tag 上跑；它推四个镜像、编译 CLI、用 `hack/render-install.sh` 把 digest 写进 `install.yaml`、`docker save` 单 tar，并把清单、tar、CLI 与 `SHA256SUMS` 挂到 Release。
- `release-smoke.yml` 下载 Release 的 `install.yaml`，在临时 kind 里装机，检查四个组件 Ready、CRD Established、Pod 用的是 GHCR 镜像。它不跑业务闭环，闭环由 e2e 覆盖。
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

键布局：索引在 `dataset/<dataset>/snapshots/<id>/`，含 `manifest.json` 与 `shard-<fp8>.idx`；数据块在 `<dataset>/blocks/<path>#<chunk>`。单桶。

端点和凭据分两处，必须指向同一个桶：

- 控制面：`DataSet.spec.endpoint`（不带 scheme）、`spec.bucket`、`spec.secretRef`。Secret 字段 `accessKey`、`secretKey`，与 DataSet 同 namespace。
- Agent：`TRAINCACHE_OBJSTORE_ENDPOINT`、`TRAINCACHE_OBJSTORE_ACCESS_KEY`、`TRAINCACHE_OBJSTORE_SECRET_KEY`、`TRAINCACHE_OBJSTORE_BUCKET`、`TRAINCACHE_OBJSTORE_SECURE`。

条件写支持用 `trainpublish --probe-conditional-write` 探测。

### 5.2 训练负载

- 训练镜像由用户自备，本项目不构建、不代发。e2e 用 busybox 加 `sleep` 顶替。
- OrderSource sidecar `agent/ordersrc/ordersrc.py` 只 import `argparse`、`json`、`os`、`struct`、`sys`，用到 f-string，python3.6 以上即可。
- 消费顺序由 sidecar 按 Job 配置算出；读 miss 与阶段切换事件由训练代码的钩子上报。不插桩时 Agent 退到 inotify 兜底。

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

只发布 linux/amd64，不提供包管理器与 Windows 制品；其他平台自行 `go build ./agent/cmd/traincache` 或 `go build ./core/cmd/trainpublish`。两个命令的参数见 [CLI 接口](CLI%20接口.md)。
