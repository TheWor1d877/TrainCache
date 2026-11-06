# Changelog说明
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。版本号与 git tag 逐字一致，带 `v`；最新版本在最上面。
每完成一项用户可见的改动就补一条，写在 `## 未发布` 下，先不写版本号；发版时把该节标题改成 `## [vX.Y.Z] - 日期`。

## 写法示例
下面的代码块只演示格式，不是发布记录。分组按改动性质从 Added / Changed / Deprecated / Removed / Fixed / Security 中选，没有就不写该组。
```markdown
## [v9.9.9] - 2026-12-01

### Added
- `WarmupTask` 新增按块预热开关 `spec.blockWarmup`，只预热训练首个窗口。字段契约见 `docs/4.4-experiment-protocol.md` §C。

### Changed
- Agent 默认缓存上限从固定 1GiB 改为节点内存的 10%。

### Fixed
- 节点重启后同一块被重复预取，导致命中率统计偏高。
```

# Changelog正文

## 未发布

### Added

- Agent 下载不小于 64MiB 的块时拆成 4 条 Range GET 并行取，拼成完整块后再算指纹落盘；网关收到带 Range 的块 GET 直接透传源站，不进在途合并表、不查磁盘缓存。契约见 `docs/5.1-design.md` §5.3、`docs/9.0-s3-merge-gateway.md` 的 singleflight 合并一节。

### Changed

- Agent 每个进程的下载任务默认上限从 8 提到 16，预铺仍占一半。大块走 4 路并行时单进程到源站或网关的连接峰值为 64，与 `MaxConnsPerHost` 默认值齐平。
- 网关到桶并发上限的默认值从 32 提到 64，覆盖预热阶段多个 Agent 同时取数；网关 Deployment 的内存上限同步从 1Gi 提到 1.5Gi。

## [v0.6.0] - 2026-11-06

### Added

- 新增混沌套件 `bench/chaos/`：`run.sh` 逐档注入六种单点故障并断言降级行为，注入脚本在 `bench/chaos/inject/`（Agent 崩溃、网络劣化两档、块数据损坏、源站宕机、订单通道停写、节点丢失）。网络劣化分抖动丢包与加限速两档。产物写到数据层 `~/tcd/chaos/<run-id>/`，规格见 `docs/4.7-chaos-suite.md`。
- 网关新增磁盘缓存层，默认开启。网关回源取到的块留在盘上，同一个块的后续请求从盘上读出，不再回源。缓存只覆盖 `<dataset>/blocks/` 前缀的块。契约见 `docs/9.1-design.md` §5.5。
- 网关新增三个配置项，取值来自 ConfigMap `traincache-config`：`gatewayCache`（默认 `"true"`，磁盘缓存开关）、`gatewayCacheDatasetBytes`（默认 `34359738368`，单个 dataset 的盘上驻写上限，单位字节）、`gatewayCacheTotalBytes`（默认 `68719476736`，全部 dataset 合计的盘上驻写上限，单位字节）；对应环境变量 `TRAINCACHE_GATEWAY_CACHE`、`TRAINCACHE_GATEWAY_CACHE_DATASET_BYTES`、`TRAINCACHE_GATEWAY_CACHE_TOTAL_BYTES`。后两项取值须为正整数，非法值让网关拒绝启动。契约见 `docs/9.1-design.md` §5.3。
- 网关 Deployment 新增一个 `emptyDir` 卷，挂在 `/var/lib/traincache-gateway/cache`；网关容器保持只读根文件系统。网关 Pod 删除后盘上内容随之清空，Pod 重建后第一次取数回源并回填。契约见 `docs/9.1-design.md` §5.5。
- 网关 60s 汇总日志新增 `cache_hit` 字段，记本窗口字节从盘上取走的请求数；磁盘缓存关闭时该值恒为 0。契约见 `docs/9.1-design.md` §7.3。
- 网关条目结束日志行新增 `from` 字段，取值 `origin` 或 `cache`，标明这次扇出的字节来自源站还是盘上缓存。契约见 `docs/9.1-design.md` §5.4。
- bench 侧：`summary.csv` 新增 `gateway_cache` 列，取值 1 表示网关磁盘缓存开启，0 表示只做在途合并。实验 COND 的 `environment` 新增 `gateway_cache` 键；`gateway` 为 false 时该键必须为 false。
- bench 侧：新增 6 条带网关磁盘缓存的实验 COND（`gw-100-r1-on-cache`、`gw-100-r4repl-on-cache`、`gw-100-r4shard-on-cache`、`gw-1000-r1-on-cache`、`gw-1000-r4repl-on-cache`、`gw-1000-r4shard-on-cache`），与同档不带缓存的臂对照。COND 由 `bench/experiments/gen_gw_conds.py` 生成。

### Fixed

- 恢复放弃的记账口径写错：v0.5.2 的条目与 `docs/10.1`、`docs/10.2` 都写成「计入 Job 的 `status.failed`」。实际是 `restartPolicy: OnFailure` 下 init 容器在同一个 Pod 里重启，每次重启算一次失败、计入 `backoffLimit`，用完后 Job 以 `BackoffLimitExceeded` 判 Failed 并删除 Pod；`status.failed` 记的是失败的 Pod 数，这种形态下是 1。`checkpoint/e2e` 的 `restore-giveup` 模式断言随之改为核对 `BackoffLimitExceeded` 与 `status.failed=1`（D14）。

## [v0.5.2] - 2026-11-05

### Added

- Agent 每 10 分钟清扫一次本节点的本地孤儿块副本，环境变量 `TRAINCACHE_LOCAL_GC_INTERVAL` 调周期，循环启动即跑首轮。数据集删除或条目移除后，节点上那份不再被任何快照引用的副本由它回收，代价是一次回源重拉。机制见 `docs/5.1-design.md` §11.5、`docs/6.1-design.md` §7.6。
- 发布产物新增第 6 个 digest：`traincache-checkpointd`。它的镜像引用写进新的 ConfigMap `traincache-checkpoint-image`（键 `checkpointImage`，namespace `traincache`），webhook 以 `CHECKPOINT_IMAGE` 读作注入 sidecar 的默认值，发布清单里渲染成本次发布的 digest。Pod 注解 `traincache.io/checkpoint-image` 与接入方自己的 ConfigMap 键仍优先。
- checkpoint 恢复新增放弃出口：`TRAINCACHE_CHECKPOINT_RESTORE_GIVE_UP_AFTER`（默认 60m）到点后写一条 `reason=restore_give_up` 的 ERROR，checkpointd 以退出码 3 结束，`restartPolicy: OnFailure` 下计入 Job 的 `status.failed` 与 `backoffLimit`，不再无限占着 Pod。决策见 `docs/10.2-decisions.md` D14。
- checkpointd 每条日志带 `job` 字段，值来自准入从 owner Job 注入的 `JOB` 环境变量；裸 Pod 为空串。它是 checkpoint 链路与 Job 的对账键，决策见 `docs/10.2-decisions.md` D26。
- 插件 `resolve` 返回新增可选字段 `max_extra_rank`，带回已提交的最大 rank。第三方插件不返回该字段照常可用。
- `checkpoint/e2e` 新增 `restore-giveup` 模式：本地后端留一份数据与 manifest 的 sha256 不符的已提交 step，每次恢复都判 CORRUPT，断言 checkpointd 到点以退出码 3 结束、`restartPolicy: OnFailure` 的 Job 把每次放弃计进 `status.failed`、超过 `backoffLimit: 2` 后转 Failed。整体预算约 40 分钟。

### Changed

- checkpoint 的启用标记合一：label `traincache.io/checkpoint` 的值从固定 `enabled` 改成该 Pod 用的 ConfigMap 名，`objectSelector` 按键是否存在匹配。Pod 注解 `traincache.io/checkpoint` 删除。接入方需要把标签值改成 ConfigMap 名；写错值的 Pod 会被准入拒绝并给出原因。契约见 `docs/10.1-design.md` §4.2、决策见 D24。
- 一个 step 判完整的条件改成「已提交的 rank 集合恰好等于 `{0 … world_size-1}`」：缺份和多出 `rank ≥ world_size` 都判不完整，多出的那份另记一条 `reason=rank_beyond_world_size` 的 WARN。弹性训练下副本数与 rank 数不等时不再误判可恢复，决策见 D25。
- `traincacheds gc` 只回收对象存储里的快照对象与孤儿块对象，不再删除任何节点的本地副本，输出去掉 `local_blocks=`。本地副本归各节点 Agent。
- `TRAINCACHE_CHECKPOINT_RESTORE_GIVE_UP_AFTER` 必须大于 `TRAINCACHE_CHECKPOINT_RESTORE_DEADLINE`（默认 10m），否则 `checkpointd` 启动即判配置非法、以退出码 2 结束。
- `traincache_gc_blocks_removed_total` 改由 Agent 的节点级清扫打点，`dataset` 标签为 `other`。

### Fixed

- 保存终态 ERROR 的 `retry` 把没真正打到插件上的那轮也计了进去，比实际尝试次数多 1。

## [v0.5.1] - 2026-11-05

### Added

- `checkpoint/e2e` 增加两个集群模式：`enospc` 用 1 MiB tmpfs 状态卷触发真实容量写满，断言最终保存失败、工作目录保留、该 step 未上传；`loop` 连跑 100 步，断言上传 100 次、工作目录清空、NVMe 缓存按保留份数收敛、sidecar 打开的文件数不随步数增长。

### Changed

- checkpoint 的 sidecar 镜像改由 ConfigMap 键 `checkpointImage` 或 Pod 注解 `traincache.io/checkpoint-image` 指定，两处都为空时准入拒绝该 Pod；原先写死的镜像 tag 从未发布，拉不到。契约见 `docs/10.1-design.md` §4.2。
- 日志组件枚举补 `checkpointd`，采集侧按该标签查询 checkpoint 日志。决策见 `docs/8.2-decisions.md` ADR-016。

### Fixed

- 安装期给 `MutatingWebhookConfiguration` 注入 `caBundle` 时只写了第一条 webhook，checkpoint 那条（`failurePolicy: Fail`）一直是空 `caBundle`，带 `traincache.io/checkpoint=enabled` 标签的 Pod 会被准入拒绝。README、`hack/release-smoke.sh`、`controller/e2e/run.sh`、`bench/scripts/ctrl_up.sh` 五处注入点改为按 webhook 名逐条注入。
- checkpoint 准入读 ConfigMap 与 Job 走缓存 client，而 webhook 的 RBAC 只有 `get`，informer 起不来，准入请求必然超时；改走直连 API 的 reader。决策见 `docs/10.2-decisions.md` D22。
- checkpointd 删不掉工作卷上的副本：`<task_id>` 目录按 0755 建出，训练程序建的 step 目录默认也是 0755，非 root 的 checkpointd 没有目录写权限，删除总是失败，工作卷会一直涨。目录改为按 0775 建出，训练程序要用组可写方式建 step 目录（`umask 0002`）。契约见 `docs/10.1-design.md` §4.3，决策见 `docs/10.2-decisions.md` D23。
- checkpoint 的 `s3`、`local` 插件把版本号声明成常量，`-ldflags -X` 对常量静默无效，镜像里握手始终报 `dev`；改为变量后与 checkpointd 一样携带 tag 版本。

## [v0.5.0] - 2026-11-05

### Added

- checkpoint 异步 sidecar（`checkpoint/` 模块）：训练 Pod 内按 Pod 搬整份 checkpoint，保存到 S3 兼容后端并在本地留缓存；恢复在训练容器启动前完成。设计与决策见 `docs/10.1-design.md`、`docs/10.2-decisions.md`。
- 发布新增镜像 `traincache-checkpointd`，内含 checkpointd 与 s3/local 两个插件；该镜像不进 `install.yaml`，由准入按 Pod 注入。

## [v0.4.2] - 2026-11-04

### Added

- README 拆成英文默认版 `README.md` 与中文版 `README.zh-CN.md`，顶部放语言切换；两版分别引用 `.en` 与 `.zh` 图，并注明 `docs/` 内容为中文。
- `bench/experiments/plot.py` 一次运行输出中英两套图：8 种图 × 2 种语言，`_save` 追加 `.zh`/`.en` 后缀，图面文案集中到 `EN` 表；CSV 仍只写一次，不带语言后缀。
- 架构图拆成 `docs/assets/figures/architecture_v0.4.1.en.svg` 与 `architecture_v0.4.1.zh.svg`。
- `docs/10.0-checkpoint-asynchronous-sidecar.md`：checkpoint 异步 sidecar 设计（checkpointd 按 Pod 搬整份目录，不进读缓存路径、不经合并回源网关），标注设计态、尚未落地。

### Changed

- `docs/` 下 35 个文档改为「数字前缀 + 英文短名」，正文保持中文；README、CHANGELOG、CLAUDE、docs 内部互链与 `bench/exp.py` 的版本记录路径同步更新。首轮同步只覆盖到 Markdown 与 `bench/exp.py`，源码注释与 `controller/deploy/observability/prometheus/rules-configmap.yaml` 的 `runbook_url` 仍写旧中文文件名，`hack/verify-doc-refs.sh` 只看 `](target)` 形式的链接、查不到这批；本轮补齐 41 个文件 53 处，其中 `runbook_url` 5 条（按旧名 `docs/8.1%20design.md` 找不到文件，告警里的排障链接是死链）。旧名只保留在 CHANGELOG 的历史条目和 `docs/4.5-metric-decisions.md` 导言那条「原 `docs/9.0 核心能力验收与测试.md` 已并入它」里。
- `docs/assets/logo/` 换成不带版本后缀的图标、字标、单色与深色标记，README 顶部改用 `traincache-lockup.svg`。
- `hack/verify-doc-refs.sh` 的检查列表加入 `README.zh-CN.md`。
- README.md 与 README.zh-CN.md 的 Figure 6 架构图更新为中英双版本（`docs/assets/figures/architecture_v0.4.1.zh.svg` 与 `.en.svg`），`architecture_v0.4.1.svg` 保留为中文版。图内文字表述与 README 正文未变，图片链接不变。

### Fixed

- `docs/4.3-startup-and-experiments.md` 与 `docs/4.4-experiment-protocol.md` 的实验族数写成「七族」，`bench/exp.py run all` 实跑八族（`gateway-merge` 是第八族，12 点 × 3 reps）：族数改成八族，4.3 第 5 节补上 `gateway-merge` 一行（3 读法 × 2 带宽 × 开关、产物 `gateway/gw-*/`），4.4 §7 机时账补该族 36 run 的账；dev 与 release 两侧都写七族，不是合并造成的。
- `hack/verify-release-metadata.sh` 判「未发布小节必须排在所有版本标题之前」时取的是文件里第一个 `## ` 标题，说明区的 `## 写法示例` 抢在前面，只要存在未发布节就必判 FAIL。改为只在未发布标题与版本标题两类之间比序，说明区的 `## ` 不参与；未发布节排在任一版本标题之后仍然 FAIL。dev 合并 release 后 `make verify-metadata` 的红灯来自这条，release 自己同样红。
- `fig1_startup_surface` 三面各标一次横轴标题，英文文案互相压住；改为只在中间面标一条共享标题，中文图同步生效。

## [v0.4.1] - 2026-11-02

### Added

- bench 新增网关合并实验族 `gateway-merge`：12 个点（单读端 / 4 节点复制读 / 4 节点分片读 × 100/1000 mbit × 网关开关），每个点 1 epoch、3 reps。多读端执行体 `compare/gateway_multi.py` 负责反亲和建 Pod、绝对时刻起跑栅栏、分片轨迹与结果聚合；`make -C bench gw-conds` 生成 COND，`gw-plan` 打印计划。
- `bench/experiments/images/<sha256>.json` 共享镜像集：COND 只存内容哈希（`environment.image_set`）与网关镜像位，不再逐文件重复十行 digest。

### Changed

- COND 契约升到 v3：只保留会变的量（数据规模、轨迹、epoch、读端形态、带宽、缓存比例与策略、采样次数、网关开关）。固定值集中在 `bench/experiments/cond.py` 的 `DEFAULTS`，派生值（数据量、缓存字节、burst、时延）由代码计算；`lock.json` 新增 `effective` 快照记录实际生效值。118 个 COND 从 11,496 行降到 6,578 行。
- `docs/4.4-experiment-protocol.md` 删掉与 4.1/4.2/4.3 重复的七组态表、只作诊断的 `t_origin_first_byte` 行与 C4 消费方大表，746 行压到 672 行；§5.4 新增网关合并一族的口径与判据；§7 保留 2026-11-02 取证后的机时账（25 ms 轮询、倾斜两族单 run ≈ 37 min、9 run ≈ 5.5 h），并在 §7.4 补 `gateway-merge` 的 12 点 × 3 reps = 36 run 分项。
- `bench/Makefile` 的 python 解释器加回退：项目根 `.venv` 不存在时计划类目标改用系统 `python3`，不再在解析阶段报错。

### Removed

- COND 删掉无消费者的字段（`dataset.endpoint`/`bucket`、`prefetch.gate` 的五个数值、`prefetch.score` 两项、`prefetch.window_k`）与全常量字段（`batch`、`workers`、`steps_per_epoch`、`compute_ms`、`delay_ms`、`chunk_size_bytes`、`coarse_reps`、`bootstrap_iters`、`kind_workers`、`k8s_version`、`lock_file` 等）。

### Fixed

- `docs/4.3-startup-and-experiments.md` 排障表两行被 `||` 连成一行，渲染错位。

## [v0.4.0] - 2026-11-02

### Added

- 新增 `traincache-gateway`：在 Agent 与对象存储之间合并在途的块 GET，同一个块只回源一次；单副本 Deployment + ClusterIP Service（`:9413`），`replicas` 固定 1。契约见 `docs/9.1-design.md`。
- Agent 的块 GET 可经网关：新增 `gatewayEndpoint`（空表示直连）与 `gatewaySecure` 两个配置；`Put`/`List`/`GetETag`/`PutIfMatch`/`Delete` 仍直连源站。网关不可用时按 10s→120s 退避降级直连，探测成功再切回，判据与状态机见 `docs/9.2-decisions.md` D1/D2/D5。
- Agent 新增两个指标族：`traincache_source_mode`（0 直连 / 1 网关）与 `traincache_gateway_fallback_total{reason}`，启动注册的族从 40 增至 42。
- 发布链增加到五个镜像：`hack/build-images.sh`（manager/webhook/scheduler/agent/gateway）、`controller/deploy/kustomization.yaml`、`hack/render-install.sh`、发版与 e2e 工作流均含 `traincache-gateway`；ConfigMap `traincache-config` 的 `gatewayEndpoint`、`gatewaySecure`、`gatewayBucketConcurrency`、`gatewayBlockBufferBytes` 四项计入下条 Changed 的 15 项。
- bench 增加网关开关 `TRAINCACHE_GATEWAY=1`，`summary.csv` 增 `gateway` 列（1/0），汇总把开关不同的行折进 `<格>+gw` 格子；新增 `gateway/e2e/run-k8s.sh` 与 CI 的 `gateway kind e2e` 作业。
- 新增 `bench/experiments/row_gate.py`：读侧三道过滤。`plot.py`、`summarize.py` 收 `summary.csv` 时逐行按顺序核 ① 点目录的 `version` 戳等于 `docs/4.6-experiment-version-log.md` 最后一行的当前版本；② `void` 为假，倾斜两族的行还要 §1.7 三列齐且非空；③ 倾斜两族的行满足 `total_makespan ≥ fetch_bytes_run ÷ 该档实测有效带宽`（表在 `bench/config.py` 的 `TILT_EFF_BW_MB_S`，@100 取 11.95 MB/s）。只筛 `traincache-*` 行，第三条只对倾斜两族成立：其余族的预热与计算重叠，`fetch_bytes_run ÷ total_makespan` 普遍高于整形线速，当上界会误杀。拒因只取第一条命中的，顺序是先版本戳、再 void 与证据列、最后字节地板；出图的把排除明细写 `results/summary/rejected.csv`。2026-11-01 的 9 条倾斜行缺 §1.7 三列也没有版本戳，从此进不了图和表。自检 `bench/experiments/row_gate.py --selftest`（已串进 `make -C bench selftest`）。
- 新增 `bench/experiments/policy_table.py`：淘汰矩阵（族 `evict-policy`）的出表入口，`docs/4.4-experiment-protocol.md` §5.3 的三档七列此前没有消费者。读 `results/policy/policy-<策略>/<run ID>/summary.csv`、套 `row_gate.py` 的三道过滤，产出 `results/summary/policy.csv` 与 `policy.md`；列顺序按 §5.3 固定，每档带一条 §1.7 判定（淘汰是否触发、热分片有没有被逐、目录峰值对声明容量的比值有没有越过按 `cache_dir_peak_period_s` 回代的上界），判据不过的那档标「只作诊断」；三栏判定只用判据全过的档，不足两档写「样本不足」。§5.3 增「出表」一条；自检 `policy_table.py --selftest`，含 2026-11-02 取证实跑那条 run 的读数，同样串进 `make -C bench selftest`。

### Changed

- 集群配置统一落位：五份工作负载清单（Agent DaemonSet、manager、webhook、scheduler、gateway）不再写任何字面量 env。等于代码默认的参数（监听端口、`TRAINCACHE_VERIFY`、`TRAINCACHE_CACHE_ROOT`、11 个 `TRAINCACHE_HEAT_*`、`CONTROLLER_*` 默认档）从清单删除，默认值只留在 `agent/svc/config.go` 与 `controller/internal/config/config.go`。
- 随集群或随实验轮次变化的取值改由 ConfigMap 注入：`traincache-config` 新增 `objstoreEndpoint`、`objstoreBucket`、`objstoreSecure`、`controllerPollInterval`、`controllerScoreTTL` 五项，从 6 项扩到 11 项；合并网关分支后再加 `gatewayEndpoint`、`gatewaySecure`、`gatewayBucketConcurrency`、`gatewayBlockBufferBytes` 四项，现共 15 项；scheduler 的 `schedulerScoreTTL`、`schedulerLogLevel` 写在 kube-system 的 `traincache-scheduler-config`，因为 `configMapKeyRef` 取不到别的 namespace 的对象。bench 的桶名不再用 `kubectl set env` 覆盖 DaemonSet，与白名单、容量一起 merge patch 进 ConfigMap。
- `make verify-metadata` 增加 `hack/verify-config-placement.sh`：清单出现字面量 env、ConfigMap 与清单引用脱钩、引用落在别的 namespace、Secret 引用非 `accessKey`/`secretKey` 都会失败。落位判据与逐项位置见 `docs/PARAMETER-CONFIGURATION.md`。
- `hack/build-images.sh` 给每个镜像打 `io.traincache.src-fp` 标签，值是进镜像那批源码（`*.go`、`go.mod`/`go.sum`/`go.work`、Dockerfile）的内容指纹；`bench/scripts/ctrl_up.sh` 起控制面前比对标签与当前工作树，不一致就退出并打印重建命令。`:dev` 标签被反复复用、节点上已有同名镜像时 `imagePullPolicy: IfNotPresent` 不重新拉取，没有这道门禁就会拿旧二进制测量。同脚本另断言 kubectl context 是 `kind-traincache`，防止并行会话把 current-context 切到自己的集群后装置在别的集群上跑。
- `bench/compare/traincache/up.sh` 的 `traincache-ordersrc` ConfigMap 改为每次 up 都从 `agent/ordersrc/ordersrc.py` 重新 apply。原来「已存在就跳过」，改完的写端代码进不了集群，与静默复用旧镜像是同一类陈旧缺陷。
- `bench/exp.py` 的产物目录改落数据层既有的五个分区：`compare-groups`→`results/scores/<格名>/`、`startup-surface`/`total-makespan`/`switch-recovery`/`pareto-startup` 四族→`results/figures/<COND 名>/`、`pareto-sustained`/`evict-policy` 两族→`results/policy/<COND 名>/<run ID>/`，对应表在 `results_root()`。此前它写 `results/<族 ID>/<点名>/`，与 `run_figures.py`/`run_policy.py` 写的不是同一棵树：`summarize.py` 只读 `scores/`+`baselines/`、`plot.py` 只认 `figures/fig*-*`，走统一入口跑完的点是「跑完了但出不了图」。点名寻址不变，`--point` 仍写短名，由 `FAMILY_COND_PREFIX` 还原成目录名；`exp.py selftest` 增加分区落位断言，`status` 打印每族的产物分区。
- `bench/experiments/plot.py` 自动扫描时按一级分区挑七组对照样本（只收 `scores/`、`baselines/`、旧 `compare/`），不再按点目录名前缀排除。前缀黑名单挡不住淘汰矩阵的 run 目录名（`policy-heat-r01-*`），会把 C 轨倾斜样本混进七组柱状图。
- 实验文档并入 4.x 系列，网关文档降为 9.x：`docs/9.2 实验协议.md`→`docs/4.4-experiment-protocol.md`、`docs/9.1 decisions.md`→`docs/4.5-metric-decisions.md`、`docs/4.4 实验版本记录.md`→`docs/4.6-experiment-version-log.md`，`docs/10.0`~`docs/10.3`→`docs/9.0`~`docs/9.3`。`bench/exp.py` 按路径读版本记录，引用这批路径的脚本注释、部署清单与 README 索引同步改完，`hack/verify-doc-refs.sh` 返回 0。
- `docs/4.5-metric-decisions.md` 的 D13、D18、D28 三行按 `docs/4.4-experiment-protocol.md` 的现行采样口径改写：D18 的每点次数改为 `pareto-startup` 7 次、`pareto-sustained` 2 次、三个曲面族与淘汰矩阵 1 次；D28 的 lookahead 扫点从「3 档 × 3 策略 = 9 点」改为 3 + 3 点；D13 的理由列改按预注册纪律表述。
- `docs/4.4-experiment-protocol.md` §1.7 的淘汰证据从两条判据增到三条。新增一条管目录峰值：`cache_dir_peak_bytes ÷ cache_bytes ≤ 1.15`，分母是 COND 声明容量，1.15 按淘汰节拍 30 s（`agent/evictor/evictor.go` 的 `defaultInterval`）乘 @100 mbit 实测有效带宽 12 MB/s、再对 2 576 980 377 算出。「拿 `capacity_bytes_final` 当分母、阈值 1.05」的旧写法作废——那是 Agent 训练结束时的在用字节末值，不是声明上限，1.05 等于要求淘汰节拍 ≤10 s。§7.4 的倾斜两族时长账按 2026-11-02 实测重写：@100 mbit 有效带宽 11.95 MB/s、单 run ≈ 37 min、9 run ≈ 5.5 h；旧的 30–33 min 与 4.6–6.0 h 作废。
- `bench/compare/run_bench.py` 测量体等补块的轮询从 0.5 s 降到 25 ms。块落盘是临时文件 + `os.Rename`（`core/cas/store.go`），`exists` 即完整可读，所以可以加密；0.5 s 的粒度让下一块的 `read_miss` 必须等检测与读完才发出，倾斜 run 单块墙钟从抓取的 0.82 s 被抬到 1.79 s，抓取槽只占用 35%。实测单块降到 0.70 s、槽占用 94%，单 run 从 92 min 到 37 min。命中率与分阶段字节账口径不变。
- `bench/compare/run_bench.py` 把目录峰值的采样节拍写进产物：`summary.csv` 增 `cache_dir_peak_samples`（窗口内采了几次）与 `cache_dir_peak_period_s`（相邻两次采样的时刻差均值，`docker exec` 串四个节点会把它从标称 5 s 拉长）。`docs/4.4-experiment-protocol.md` §1.7 判据三的上界随之从 1.15 改到 1.16——盲区是淘汰节拍 30 s 加采样周期 5 s，(30 s + 5 s) × 11.95 MB/s ÷ 2 576 980 377 ≈ 16%，只看淘汰节拍会漏掉「采样格点之间涨上去没被读到」；@300 档的对应上界 1.42 作废，按同一式子改 1.48。§7.4 的 @300 那句同步改。判据一的 5% 余量说明改齐代码：预取在途的 `tmp-*` 不在目录峰值口径里（只统计写完的 `.blk`），余量只给采样间隔内的增长。目录峰值列的取值描述也改成实际命令（节点内 `find -name '*.blk'` 求和取单节点最大值），旧的「`du -sb`」写法作废。
- tc 的 burst 口径按实挂值改齐，注释与文档里的算法不再用十进制：tc 把 `1250k` 解析成 1024 倍，即 1 280 000 字节，对 @100 的 tbf 速率 12.5 MB/s 是 102 ms 突发窗口（`bench/config.py` 的 `burst_for_rate` 注释、`bench/scripts/tc_apply.sh` 头注释、`docs/4.4-experiment-protocol.md` §1.9 三处原来写「≈0.1 秒」按 1000 算，读者会算错）；旧固定值 `1600k` 的突发窗口 128 ms 改 131 ms（`docs/4.5-metric-decisions.md` D31 同一处）。一次性容器内实测（iproute2-6.1.0）：命令行同时给 `latency 50ms` 不覆盖这个桶，三个 latency 取值回读的 burst 不变，latency 只定队列长度。`make bench-doctor` 加第五条断言，从 MinIO Pod netns 回读的 tbf burst 折算成字节后必须等于 `config.burst_for_rate(该档 rate)`，容 ±1%（iproute2 按 32 位字取整，3750k 回读 3839962b）；`docs/4.4` §1.7 的「四条断言」改「五条」。

### Removed

- 删除 `docs/9.1 plan.md`：里程碑表（M0–M8）与任务表不再维护，验收改按实验族寻址；机时口径、带宽与容量档参数、对照层 62 run 分项与各族点数表并入 `docs/4.4-experiment-protocol.md` §7。
- 删除 `docs/10.4 plan.md`（按其自述：里程碑全部验收后删除）与 `docs/9.0 核心能力验收与测试.md`（内容已散在 4.x，口径合并进 `docs/4.4-experiment-protocol.md`）。

### Fixed

- scheduler 的 `configMapKeyRef` 指到了 `traincache` 的 `traincache-config`，而它在 kube-system，取不到对象，Pod 停在 `CreateContainerConfigError`；两个 Scheduler 配置项改到 kube-system 的 `traincache-scheduler-config`。热集群一直复用旧 ReplicaSet、清单哈希不变就不 `apply -k`，这条直到 M5 前置探针第一次全量 apply 才露头；`hack/verify-config-placement.sh` 现按 namespace 校验每条引用，改回跨 namespace 引用即失败。
- 字节维度缓存上限（`TRAINCACHE_CACHE_CAPACITY_BYTES`）的记账错在两处，导致设了上限也不会按字节淘汰。其一，口径不一致：`cas.Put` 只加数据长度，启动预扫与淘汰扣减按块文件 size（数据 + 128B 块头），删空目录后累计仍停在 −128B/块；现三个来源统一到文件 size。其二，进程重启后启动预扫只播种累计、没播种去重表，目录里的块被本进程首次打开时再加一次（8GiB 缓存读成 16GiB），淘汰每块只减一次，累计永远回不到真值并停在危险线上反复空转；预扫现同时把已有块指纹写入去重表。契约见 `docs/6.2-decisions.md` D58，判据用例 `agent/svc/capacity_wiring_test.go`。
- 字节累计的漏记还有一处：`Open` 前置观察者对**缺失块**也触发，而字节累计拿它当「这个块记过了」用——预取窗口先用 `cas.Open` 探就绪（ETA、回填、rescan 同形态），未命中就把块指纹写进 `seenFPs`，随后本进程 `Put` 同一块被当成已累计跳过。冷缓存下几乎每个块都先被探测一遍，写入字节全程漏记，`traincache_cache_capacity_bytes_total{source="bytes"}` 停在启动预扫值、淘汰增量为 0（M5 前置探针现场）。现在缺失块探测不占用去重槽，累计只在块文件确实存在或 `Put` 落地时计入；判据用例 `TestCapacitySurvivesMissOpenProbe`。
- ring 写端的 10s 保活心跳每跳一次就把订单重铺一遍。代际的语义是「写端会话换了」，可 `ordersrc.py` 每次调用都按新会话算（`writer_generation` +1），而读端遇到代际变化会按 design §7.4 把同载荷订单也重放一次，于是每 10s 一次虚假的 sidecar 重启 → 一次 `OnStageSwitch` → 预取队列整段重铺（M5 前置探针现场：1024 块/次，字节上限生效后演变成无界回源）。新增 `--same-generation`：同一会话内的续写只推进 `write_seq`、代际不变；`agent/deploy/train-pod.yaml` 的 sidecar 循环与 bench reader 的心跳线程改用它，epoch 首写仍按新代际。契约见 `docs/6.2-decisions.md` D22/D41/D57，判据用例 `agent/ordersrc/ordersrc_test.py`。

## [v0.3.2] - 2026-10-31

### Changed

- 下载准入档位改名：`fetch.Priority` → `fetch.Admission`、`FetchWithPriority` → `FetchWithAdmission`、`PriorityForeground`/`PriorityBackground` → `AdmissionReading`（在读：需求读补块与坏块修复）/`AdmissionSpeculative`（预铺：预取含显式预热）。只影响 `traincache/core/fetch` 的导入方；半额额度、让路门、同 key 合并与回源指标口径不变。契约见 `docs/5.1-design.md` §5.3 与 `docs/6.3 decisions.md` D61。

## [v0.3.1] - 2026-10-30

### Added

- 控制面到 Agent 的四个 RPC 新增 gRPC metadata 三键 `x-traincache-request-id`/`x-traincache-task`/`x-traincache-generation`：Controller 客户端注入，Agent 拦截器把三者写进 context logger，两端日志可按同一次请求串联。契约见 `docs/8.1-design.md` §5。
- 钩子帧可携带任务身份 `warmup_task`/`pod`/`pod_index`/`dataset`/`generation`，缺省时行为与旧帧一致；`read_miss` 的补块日志带上这组字段。webhook 相应补注入 `TRAINCACHE_POD`。契约见 `docs/8.0-logging-and-monitoring.md` 与 `docs/6.1-design.md` §5.4。
- 日志采样与限流：`TRAINCACHE_LOG_SAMPLE_RATE`（Info，默认 1.0）与 `TRAINCACHE_LOG_DEBUG_SAMPLE_RATE`（Debug，默认 0.01）控制级别采样，配进程内令牌桶（控制面 100 行/秒、Agent 200 行/秒）与 60s 去重窗口；Warn/Error 不采样、不被丢弃。丢弃条数由新增的 `traincache_log_dropped_total{component,level}` 暴露。契约见 `docs/8.1-design.md` §3.3。
- 日志固定带 `sampled`/`sample_rate` 两个字段；带 `err` 的 Warn/Error 固定成对带 `err_class`（`invalid_spec`/`not_found`/`unavailable`/`permanent`/`corrupt`/`io`/`timeout`）。契约见 `docs/8.1-design.md` §3.1。
- Agent 高频路径改为 60s 汇总：逐块下载/校验每窗口一条 Info（块数、字节、失败数、P50/P99），缓存命中/未命中/read_miss 每窗口一条 Info（按 dataset 与 reason）。逐块明细降为 Debug 1/100 采样。契约见 `docs/8.1-design.md` §3.3 与 §4.2。
- observability 装置补齐告警与看板：五条 Prometheus 规则（门控 Hold 超时、Agent 不可达、read stall 超阈值、磁盘危险线、日志断流），每条带 `runbook_url` 指向 `docs/8.1-design.md` 附录 A；Grafana 增加控制平面、Agent、DataLoader 三层看板并从 ConfigMap 自动加载。
- 训练侧 DataLoader 适配器 `agent/hooklib/`：包装 torch 的 `Dataset`/`IterableDataset`，发 advance/stage_switch/open_report/read_miss/read_stall 五帧，并按 `docs/8.1-design.md` §4.3 打 JSON 日志；webhook 注入 `TRAINCACHE_TASK`/`TRAINCACHE_POD`/`TRAINCACHE_POD_INDEX`/`TRAINCACHE_DATASET`/`TRAINCACHE_HOOK_SOCK`/`TRAINCACHE_GENERATION_AT_START`，参考清单把 `/run/traincache` 挂进训练容器。
- `traincacheds gc --orphan-blocks` 清理远端孤儿块：删除不被任何快照引用、且最后修改时间早于 `--orphan-grace`（默认 24h）的块；manifest 或分片读不通时整轮放弃删除。
- `traincacheds` 交付 `rm` 与 `gc`：`rm --path/--prefix` 经 `Writer.CommitChanges` 提交新快照完成删除，`gc` 触发一次 `Collector.Sweep` 回收旧快照；两者默认只预览，带 `--yes` 才执行。

### Changed

- 节点分数注解从 Node 迁到训练 Pod，键名由 `traincache.io/scores` 改为 `traincache.io/node-scores`；controller 与 scheduler 的 `CONTROLLER_SCORE_ANNOTATION` 必须同值。放行在同一次 Pod patch 内写分数并摘 gate，Scheduler 不再有读不到分数的窗口。契约见 `docs/7.1-design.md` 与 `docs/PARAMETER-CONFIGURATION.md`（D49 取代 D27）。
- `read_stall` 归因关联窗口从固定 ±5s 改为可配：`TRAINCACHE_READ_STALL_WINDOW_MS`，默认 `5000`，表示以帧 `ts` 为中心前后各该毫秒数。契约见 `docs/PARAMETER-CONFIGURATION.md`。
- Agent 的部署清单只剩 `controller/deploy/agent/daemonset.yaml` 一份，kind e2e 改为 apply 该清单后替换镜像引用；`agent/deploy/agentd.yaml` 删除，同名 DaemonSet 两套 request/limit 并存的问题消失。Agent 内存 limit 为 4Gi。

## [v0.3.0] - 2026-10-29

### Added

- Agent 新增订单通道观测指标：`traincache_agent_ordersources{node,state}` 给出每路订单源处于 discovered / healthy / stalled，`traincache_agent_ordersource_ended_total{node,reason}` 记录订单源被收回（`missing_ttl` / `stale_file`），`traincache_orders_dropped_total{reason}` 记录跳读、损坏槽与空订单（`resync` / `corrupt` / `invalid`）。契约见 `docs/6.3 decisions.md` D63。

### Changed

- 订单通道降级改为观测语义：`traincache_agent_degraded{node}` 只在全部订单源都 stalled 时为 1，不再表示「停止推送订单」，降级期间订单照常下发；sidecar 退出或 ring 停止推进 30 秒后该路标 stalled，任一路恢复推进自动解除。inotify 只做统计，不计入恢复。

### Fixed

- 宽预热在训练 Pod 尚未创建时把 `PodSetMismatch` 置为 True，日志与告警被刷屏；现在 Pod 集为空只记 Debug 并清除该条件，只有部分不匹配才 Warn。
- 窗口是否变化改按 `window_blocks` 判定，不再比较 bitmap 字节数，字节数相同的不同窗口不再被当成同一窗口。

## [v0.2.0] - 2026-10-29

### Added

- Agent 启动日志级别不再固定为 `info`，改由 `TRAINCACHE_LOG_LEVEL` 决定（默认 `info`）。
- 四个进程新增 localhost-only 的 `/debug/loglevel`：`GET` 读当前级别，`PUT` 改级别即时生效，进程重启回落到 env 值。监听地址由 `CONTROLLER_DEBUG_ADDR`（controller-manager / scheduler / webhook）与 `TRAINCACHE_DEBUG_ADDR`（agentd）给出，默认 `127.0.0.1:6060`，空串关闭。契约见 `docs/8.1-design.md` §7。
- Agent 快照跟随循环在切换成功后补一条 Info（`catalog: 快照切换成功`），与失败侧的 Warn 成对；此前只有失败出声，「没有新快照」和「读不到」在日志里分不开。

### Changed

- 控制面日志的 `assign` 字段由 `"p0=n0"` 字符串改为结构化对象（Pod → 节点数组），可按节点聚合；Agent RPC 的进程内重试循环去掉，失败日志不再带恒为 1 的 `attempt`，退避交给 controller-runtime。
- `CancelPreheat` 撤回该任务还没下完的块：受理预热任务时结一个下载作用域，取消即取消它，块在下一个 ctx 边界（准入排队、源站读、写盘）退出并按失败回调，任务落 `ERROR`、`completed` 保留取消前已落盘的块数。作用域只在任务出表时释放，`DONE` 不释放——DONE 后仍可能收到 `UpdatePreheat` 续窗。再次提交同一 `task_id` 仍视为新任务。契约见 `docs/6.1-design.md` 的 `CancelPreheat` 条目，决策记 `docs/6.3 decisions.md` D62。
- bench 与现勘清单的 MinIO 镜像从 `latest` 钉到固定 release tag（`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`，manifest digest 已 `docker pull` 验证），滚动升级不再带来行为漂移。

### Fixed

- 预热任务在同一窗口上反复取消重提、Agent 每次从块 0 重下，请求 1 GiB 实搬 3–4 GiB、warm 反慢于 cold。四层叠出来的，逐条见 issue #4–#8：
  - 「已经提交过什么」不再从 `status.podNodeReadiness` 反推，改成每轮 `GetPreheatStatus` 问 Agent；`PreheatStatus` 加 `canceled` 字段，被取消的任务不算持有，节点重新选中时会被重新派活而不是静默跳过。Agent 上没有该任务不再算永久拒绝，`Cancel` 收到它按幂等空操作成功。
  - GWAA 目标集合加在册保留（`Input.Held`）：在册且仍在保留带（`2*c_ij ≥ best`，低信号自动满足）的节点不因一块差距被换下，只换不加、停止节点不参与。
  - `Submit`/`Update` 成功后清除该配对在本进程的 `seq` 高水位，重投任务从 1 起的新上报不再被当成陈旧丢弃。
  - 宽预热落选节点的下载随 `CancelPreheat` 撤回（见 Changed）。

## [v0.1.0] - 2026-10-27

首个版本，包含控制面、节点面、缓存库与配套工具。

### Added

- 控制面：`DataSet`/`WarmupTask` CRD、预热编排 Controller、调度器评分插件、Pod 注入 webhook。
- 节点面：`traincache-agentd` DaemonSet、块级缓存、预热执行、gRPC 服务与 inotify 兜底。
- 缓存库：内容寻址存储、索引分片、S3 兼容对象存储读写与条件写。
- 工具：`traincache` CLI（prefetch / node status / status）、快照发布工具、kind e2e 与 bench 装置。
