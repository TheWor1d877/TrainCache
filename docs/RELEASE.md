# TrainCache 发布方案

> 状态：方案已定稿，按 §10 执行。
> 本文是发布方案的唯一落点；docs 之外不再保留发布讨论稿。

## 0. 结论摘要

| # | 结论 |
|---|---|
| 1 | 发布分三个对象：软件版本、实验锚点、复现包。三者生命周期不同，tag 分开。 |
| 2 | 软件版本统一到 `v0.1.0`：git tag、镜像 tag、二进制版本逐字一致。 |
| 3 | 轩辕域名只用于开发；发布物里一个字符都不出现。 |
| 4 | 镜像主路径 GHCR，兜底 Release 挂镜像 tar；两条路用同一个镜像名。 |
| 5 | v0.1.0 不引入 Helm；发布包 = install.yaml + install.sh + examples + 镜像 tar + 版本清单。 |
| 6 | 不是单 yaml 能装：webhook TLS 和对象存储参数必须由 install.sh 处理。 |
| 7 | CI 分四层：校验、端到端、发布、发布后冒烟。e2e 不进每个 PR。 |
| 8 | e2e 验证源码；release-smoke 验证发布物。后者目前缺失。 |
| 9 | 文档正文不标“v几新增”；版本历史进 CHANGELOG，只有对外契约表标起始版本。 |
| 10 | 版本统一会改变四个镜像的 digest；先打 `v0.1.0` 发布镜像，再回填 34 个 COND，最后在回填 commit 上打 `prereg-c1`。 |

## 1. 三个发布对象

| 对象 | 冻结什么 | 面向谁 | 当前缺口 |
|---|---|---|---|
| 软件版本 `v0.1.x` | 镜像、安装清单、CRD 接口、二进制 | 想部署的人 | 无安装脚本、无公共镜像、版本四处手填 |
| 实验锚点 `prereg-c1` | 9.2 协议、34 个 COND、4 条轨迹 | 评审者、自己复跑 | tag 未创建；COND 里的 digest 还是 0.8.x，待发布镜像回填 |
| 复现包 | 上面两者 + bench harness + 原始数据 + 报告模板 | 第三方复现 | 未打包；数据在 `~/tcd` |

判断标准：`v0.1.0` 只是复现包的前置产物，不等于复现包本身。

## 2. 镜像与仓库

### 2.1 轩辕域名的边界

`dgl3i4ucowsgvvdwty.xuanyuan.run` 是私有、按量计费的拉取加速域名，只能出现在开发构建参数里。发布物中出现它有两个后果：别人拉不到；别人从源码重建会把流量记到本项目账上。

镜像引用 `host/namespace/name:tag` 中，`host` 只回答“去哪拉”，不回答“谁做的”。现在清单里的 `xuanyuan/.../traincache/controller-manager:0.8.0` 是本地构建时拼出的名字，没有任何 push 发生，外界不存在这个地址。

### 2.2 第三方镜像分三类

| 类别 | 现位置 | 发布时 | 谁在用 |
|---|---|---|---|
| Dockerfile 基础镜像 | 4 个 Dockerfile，`ARG REGISTRY` 默认轩辕 | 默认改 `docker.io`；开发脚本显式覆盖回轩辕 | 构建 |
| 观测栈 4 镜像 | `controller/deploy/observability/`（prometheus、loki、grafana、fluent-bit） | 不进发布包 | 仅开发 |
| dev/bench 装置 | pgsty/minio、busybox、kindest/node | 不进发布包 | 只有 e2e 和 bench |

边界说明：

- observability 不在根 `controller/deploy/kustomization.yaml` 的 resources 里，发布包不包含它。
- 发布包不带对象存储。用户接自己的 COS/OSS/S3；`pgsty/minio` 只服务 e2e/bench。

### 2.3 GHCR + tar 双通道

| 项 | 做法 |
|---|---|
| 主路径 | `install.yaml` 引用 `ghcr.io/thewor1d877/traincache/<组件>@sha256:<digest>`，tag `v0.1.0` 同时推送 |
| 兜底 | Release 附件放 `traincache-images-v0.1.0.tar.zst` + `SHA256SUMS`；`kind load image-archive` 或每节点 `ctr -n k8s.io images import` |
| 名字一致性 | tar 用 `docker save` 生成，保留 GHCR 全名；导入后与清单引用逐字相同，不需要 retag |
| 多架构 | v0.1.0 只发 linux/amd64，写进 Release |
| 可见性 | 仓库与 GHCR 包均为 public，免认证拉取 |

两个操作细节：

- 四个镜像 `docker save` 到同一个 tar，共享基础层只存一份；分成四个 tar 会重复几百 MB。
- tag 引用配 `IfNotPresent` 有坑：节点上已有同名 tag 的旧镜像不会重拉。用 digest 引用可避开。

### 2.4 引用方式

`install.yaml` 用 digest（`@sha256:`）钉死。tar 是同一份字节，digest 一致，兜底路径照样成立。用户若经第三方镜像站 retag 再拉，digest 对不上；本项目不承诺这条路径。

## 3. 版本统一到 v0.1.0

### 3.1 目标链路

| 层 | 值 |
|---|---|
| git tag | `v0.1.0` |
| 镜像 tag | `v0.1.0` |
| 二进制版本 | `v0.1.0`，四个组件都注入 |
| 仓库内清单 | `dev` |
| 发布清单 | CI 用 kustomize `images:` 渲染成 `ghcr.io/thewor1d877/traincache/<组件>@sha256:<digest>`，tag `v0.1.0` |

镜像 tag 保留 `v`：Docker 惯例去 v，但 k8s 官方镜像本身带 v；这里让 tag、镜像、二进制逐字一致，省掉心算映射。

### 3.2 现状是四条互不相干的版本线

| 位置 | 当前值 |
|---|---|
| git tag | 无；`08e4ba9` 的提交信息是 v0.1.0 |
| 清单镜像 tag | manager/webhook/scheduler `0.8.0`，agentd `0.8.3` |
| 二进制版本（ldflags） | manager/webhook/scheduler `0.8.0`；agentd 未注入，日志回退 `dev` |
| 脚本 `TAG` 默认 | `0.8.0`（`controller/e2e/run.sh`、`bench/compare/traincache/up.sh`） |

`agentd 0.8.3` 与其余 `0.8.0` 并存，说明手工值已经开始漂。9.2 §8.3 的 digest 正是按这些版本冻结的。

### 3.3 要补的代码

- agentd 版本注入：`agent/Dockerfile` 没有 `ARG VERSION`，也没有 `-ldflags`；`agent/cmd/agentd/main.go` 的 `buildVersion` 在 workspace 构建下回退 `dev`。补齐后四个组件用同一套注入方式。
- e2e 的 TAG 陷阱：`build_image` 用 `$TAG` 构建，`deploy_control_plane` 却直接 `kubectl apply -k` 使用清单写死的 0.8.0。传 `TAG=0.9.0` 会构建新镜像、部署旧镜像：本机有旧镜像就静默测旧代码，没有就 ImagePullBackOff。统一后让 e2e 也走 kustomize 渲染，或固定 `dev`。
- 清单渲染：`controller/deploy/kustomization.yaml` 增加 `images:` 段，发布时替换 registry 与 tag，不手改 YAML。

## 4. 发布全流程

### 4.1 发版步骤

| 步 | 动作 | 判据 |
|---|---|---|
| 1 | `dev` → PR → `main`，校验 CI 通过 | 见 §5 |
| 2 | 在 `main` 打 annotated tag `v0.1.0` | tag 只从 main 打 |
| 3 | release workflow 构建 4 个镜像（linux/amd64），推 GHCR，取 digest | 镜像可拉、digest 已记录 |
| 4 | kustomize 渲染 `install.yaml`，镜像固定到 digest | 渲染结果可直接 apply |
| 5 | `docker save` 4 镜像 → tar.zst，算 sha256 | tar 可导入 |
| 6 | 建 GitHub Release，挂 §4.2 的产物 | Release 页面自洽 |
| 7 | 发布后冒烟：干净 kind 从发布物安装 | 见 §6 |
| 8 | 回填 34 个 COND 的镜像 digest，在回填 commit 上打 `prereg-c1` | 见 §8.2 |

### 4.2 用户拿到的产物

| 产物 | 作用 |
|---|---|
| `install.yaml` | 渲染好的完整清单：CRD、RBAC、manager、webhook、scheduler、agentd、webhook 配置 |
| `install.sh` | 生成 webhook CA/证书与 Secret、patch caBundle、创建对象存储 Secret、按 env 文件填参数、apply、等就绪 |
| `examples/` | DataSet + WarmupTask + 训练 Pod（含 `schedulerName` 与 hostPath 挂载） |
| `traincache-images-v0.1.0.tar.zst` | 拉不到 GHCR 时的兜底 |
| `versions.txt` + `SHA256SUMS` | tag、commit、4 个 digest、Go 版本、校验和 |

`install.sh` 不是可选项，原因有两条：

- `mutatingwebhook.yaml` 的 `caBundle` 是空的，`traincache-webhook-tls` Secret 由 e2e 脚本现场生成再 patch。用户只 apply yaml，webhook 起不来，门控注入失效。
- agent DaemonSet 写死了 benchmark 默认值：数据集白名单 `compare-data`、MinIO 地址、桶 `bench-b`、缓存 1GiB。不改就静默不生效。

### 4.3 发布前必须参数化的项

| 类别 | 项 |
|---|---|
| 对象存储 | endpoint、access key、secret key、bucket、secure |
| 缓存 | 容量字节数（现在写死 1GiB） |
| 数据集 | 白名单（现在是 `compare-data`） |
| 镜像 | registry、tag |
| 资源 | agent/controller/webhook/scheduler 的 request/limit |

### 4.4 Helm 时机

v0.1.0 不上 Helm，用 `install.sh + env 文件`。触发上 Helm 的条件：

| 触发条件 | 说明 |
|---|---|
| 安装参数超过十几个且按环境区分 | 现在已有迹象 |
| 需要升级/回滚管理 | `helm history` / `rollback` |
| 出现第二类使用者 | 外部用户需要稳定安装方式 |
| 想免掉 TLS 手工活 | Helm 模板可直接 `genCA`/`genSignedCert` 并回填 caBundle |

Helm 是打包格式，不是发布本身；它不解决版本统一和镜像仓库问题。

## 5. CI

### 5.1 触发矩阵

| 触发 | 内容 | 时长 | 是否阻塞 |
|---|---|---|---|
| push `dev` / PR→`dev` | 三模块 build + vet + test -race；gofmt 全模块；禁用符号检查；`kubectl kustomize` 可渲染 | ~5 min | 必须过 |
| PR `dev`→`main` | 同上 + 版本一致性检查（清单不写手填版本、Dockerfile 不写死 tag、CHANGELOG 与 git tag 对齐）+ 文档引用完整性 | ~5 min | 必须过 |
| PR 打 `ci/e2e` 标签 / 手动 | kind e2e | ~45 min | 不阻塞合并，发版前必过 |
| push tag `v*` | 构建推镜像 → 渲染 install.yaml → 打 tar → 建 Release | ~15 min | 发布本身 |
| 发布后手动 | release-smoke | ~10 min | 发版后必过 |

### 5.2 纪律

- `main` 只收 PR，校验设 required；`dev` 允许直推。否则 dev/main 只是两个名字。
- e2e 不进每个 PR：要起 kind、拉镜像、会抖，进每个 PR 只会导致 CI 被关掉。
- CI 不跑 bench：bench 依赖本机数据层和 32G 内存，属于实验装置。
- 版本一致性检查防“四处手填”复发。
- CRD 生成物漂移检查需要 pin controller-gen；仓库现在没有 controller/Makefile，可后补，不阻塞 v0.1.0。

## 6. e2e 与 release-smoke

三层职责：

| 层 | 跑在哪 | 验证什么 | 谁跑 | 时长 |
|---|---|---|---|---|
| 单测 | 本地/CI | 函数与模块 | 每次 push | 秒~分 |
| e2e | kind | 系统连通与功能正确 | 你 / CI 手动 | ~45 min |
| bench | 本机 + 数据层 | 性能对照 | 你 | 小时~天 |

e2e 是功能验证，不是给用户用的，也不是性能实验。用户侧只给 examples + install.sh + 五分钟冒烟步骤。

缺口：e2e 验证的是“源码构建出来的镜像”，不是“发布出来的产物”。需新增 release-smoke：

1. 干净 kind 集群；
2. 从 GHCR 拉取（失败则导入 tar）；
3. 跑 `install.sh` 与 `examples/`；
4. 断言预热、门控放行、打分发生。

它复用 e2e 的断言，但入口必须是发布物。没有这一步，“发布物能用”只是推测。

## 7. 文档与版本

### 7.1 三层分工

| 层 | 载体 | 规则 |
|---|---|---|
| 设计文档（1/2/5/6/7/8） | docs 现有章节 | 只写“现在是什么”，不标引入版本；历史靠 git tag 快照 |
| 版本历史 | `CHANGELOG.md` | 唯一版本记录，Keep a Changelog 格式，最新版本在最上面 |
| 对外契约 | CRD 字段表、env 表、CLI flag 表 | 加“起始版本/稳定性”列，只有这些地方标 v几 |

判断标准：读者要据此写代码、写 manifest、写脚本的地方标版本；只是理解设计的地方不标。内部实现章节标版本会变成双份维护，必然漂。

### 7.2 CHANGELOG 的写法

`CHANGELOG.md` 记录**所有版本的变更**，不是全量接口手册。结构：

```markdown
## [Unreleased]
## [v0.2.0] - 2026-xx-xx
### Added / Changed / Deprecated / Removed / Fixed / Security
## [v0.1.0] - 2026-xx-xx
```

每条写“用户可感知的变化”，带指向文档或 issue 的链接。完整接口不抄进 CHANGELOG。

### 7.3 旧版本的 CRD 接口怎么查

CHANGELOG 只说明“变过什么”，不提供旧接口全文。旧接口有三个权威来源，优先级从高到低：

| 来源 | 查法 | 说明 |
|---|---|---|
| 该版本的 CRD YAML | `git show v0.1.0:controller/config/crd/bases/traincache.io_datasets.yaml` | 机器可读的完整 schema，最权威 |
| 该版本的 API 类型 | `git show v0.1.0:controller/api/v1alpha1/dataset_types.go` | 字段注释与默认值最全 |
| 该版本的文档 | GitHub 切到 tag `v0.1.0` 浏览 docs | 叙述性说明 |

规则：

- `main` 上的文档只描述当前版本，不维护旧版副本。需要旧版就切 tag。
- API 版本保持 `v1alpha1`：不承诺向后兼容。破坏性变更要么升 API 版本（`v1beta1`），要么在 CHANGELOG 的 Removed/Changed 里写迁移说明。
- 如果将来同时支持两个版本，再考虑文档站版本化；现在不做。

### 7.4 实验协议独立

`docs/9.2 实验协议.md` 不跟软件版本走。它的锚点是 `prereg-c1`，软件从 v0.1 升到 v0.2 也不改它；协议本身要改，只追加 §7 偏离记录。软件 tag 和协议 tag 不能互相替代。

## 8. 标签计划

### 8.1 两个 tag

| tag | 指向 | 含义 | 规则 |
|---|---|---|---|
| `v0.1.0` | 发布 commit（annotated） | 软件版本 | 只在 main 打；随 bug 修复往后走 |
| `prereg-c1` | 回填 digest 后的 commit（轻量） | 实验协议 | TrainCache 修复与 digest 回填完成后在最新 commit 打；协议不改就不动 |

两个 tag 目前都不存在。`08e4ba9` 只是提交信息叫 v0.1.0，没有对应 tag。`v0.1.0` 必须先打：c1 里要写发布镜像的 digest，tag 无法包含尚未产生的值。

### 8.2 版本统一与 digest 回填的顺序

rerun 没有进入报告的行，正在跑的是非 TrainCache 组，不受版本统一影响。顺序固定：

1. 版本统一合并 main，打 `v0.1.0`；CI 构建并推送四个镜像，取 GHCR manifest digest。
2. 用 digest 更新 34 个 COND 的 `environment.images` 四个 Traincache 键；training、minio、juicefs、fluid、fluid-csi 五个键不动。
3. 同步更新 9.2 §8.3 九键表与 §8 冻结记录，重算 COND 集哈希。
4. 在该 commit 打 `prereg-c1`。不建 c2，不写偏离记录。

顺序不能反：镜像重建后 digest 必变，先打 tag 再回填会让锚点与 COND 对不上。

## 9. 不做的事（v0.1.0 范围）

- 不做 Helm。
- 不做文档站版本化。
- 不做多架构镜像（只发 linux/amd64）。
- 不把 bench 放进 CI。
- 不做镜像站同步；GHCR 拉不到就走 tar。
- 发布包不含 observability。

## 10. 执行顺序

1. 定稿本文与 CLAUDE.md 的版本规则。
2. 版本统一：四个 Dockerfile、清单、脚本；补 agentd 版本注入；kustomization 加 `images:`；修 e2e 的 TAG 陷阱。
3. 参数化：对象存储、缓存、白名单从清单挪到安装参数；写 `install.sh`（TLS + Secret + apply）。
4. 加 CI：校验层的版本一致性检查；tag 发布 job；release-smoke。
5. 建 `CHANGELOG.md`。
6. 本地 e2e 通过 → 合并 main → 打 `v0.1.0` → CI 发布。
7. 从发布物在干净 kind 上跑 release-smoke。
8. 取发布镜像 digest，更新 34 个 COND 与 9.2 冻结记录，提交并在该 commit 打 `prereg-c1`。
9. 开跑 TrainCache 实验，产物目录标注软件版本与协议锚点。
