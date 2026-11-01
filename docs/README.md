# 文档索引

`docs/` 只放长期维护的文档：需求基线、实现契约、决策记录、测试说明书与跨模块约定。开发计划不落在这里。

## 文件命名

| 编号 | 角色 | 维护方式 |
|---|---|---|
| `X.0` | 需求基线 | 用户手写，禁止改动；下游文档承载其全部语义，只补不减 |
| `X.1` | 实现契约（`design`） | 接口签名、数据格式、包结构一经冻结不得私改；要改先回传 `BLOCKED` |
| `X.2` | 决策记录（`decisions`） | `D` 是已定案决策，编号只增不改；`Q` 是未决问题，结案后并入 `D` |
| `X.3` | 测试说明书（`test`） | 测试怎么跑、每个文件测哪条契约 |
| `X.4` | 使用与运维手册 | 面向使用者的操作说明 |

模块 9 是验收与实验，没有实现契约：`9.0` 是需求基线，`9.1 decisions.md` 记录验收与实验口径的裁决，`9.2` 是实验协议（预注册、COND 契约与报告模板）。模块 9 的编号不按本表顺排。

开发计划是临时文件，用完即弃：写进 issue 或 `.qoder/drafts/*/todo.md`，完成后删除，不进 `docs/`。

## 总览

| 文档 | 内容 |
|---|---|
| [0. 云对象存储部署指南](0.云对象存储部署指南.md) | MinIO/COS/OSS/S3 的桶与凭据接入 |
| [1 TrainCache介绍与背景](1 TrainCache介绍与背景.md) | 问题定义、外部事实、与 Fluid/JuiceFS 的定位差异 |
| [2 TrainCache三层架构](2 TrainCache三层架构.md) | 控制面、节点面、数据源的分工与数据面约定 |
| [3 现勘报告](3 现勘报告.md) | 现场勘查数据与缺口证据 |
| [4 bench装置说明](4 bench装置说明.md) | 实验装置、条件清单、操作规程与产物格式 |
| [5.0 缓存核心库设计方法](5.0 缓存核心库设计方法.md) | 核心库需求基线 |
| [5.1 design](5.1 design.md) | 核心库实现契约 |
| [5.2 decisions](5.2 decisions.md) | 核心库决策 |
| [5.3 test](5.3 test.md) | 核心库测试说明书 |
| [6.0 Agent服务化](6.0 Agent服务化.md) | 节点面需求基线 |
| [6.1 design](6.1 design.md) | Agent 实现契约 |
| [6.2 decisions](6.2 decisions.md) | Agent 决策 |
| [6.3 test](6.3 test.md) | Agent 测试说明书 |
| [7.0 Controller & Scheduler](7.0 Controller & Scheduler.md) | 控制面需求基线 |
| [7.1 design](7.1 design.md) | Controller 与 Scheduler 实现契约 |
| [7.2 decisions](7.2 decisions.md) | 控制面决策 |
| [7.3 test](7.3 test.md) | 控制面测试说明书 |
| [8.0 日志与监控](8.0 日志与监控.md) | 日志与监控需求基线 |
| [8.1 design](8.1 design.md) | 日志与指标契约 |
| [8.2 decisions](8.2 decisions.md) | 日志与监控决策 |
| [8.3 test](8.3 test.md) | 日志与监控测试说明书 |
| [8.4 日志与监控使用说明](8.4 日志与监控使用说明.md) | 查询、告警与排障手册 |
| [9.0 核心能力验收与测试](9.0 核心能力验收与测试.md) | 验收与实验需求基线 |
| [9.1 decisions](9.1 decisions.md) | 验收与实验口径的裁决 |
| [9.2 实验协议](9.2 实验协议.md) | 预注册假设、COND 契约、机时预算与报告模板 |
| [10.0 S3合并回源网关](10.0%20S3合并回源网关.md) | 合并回源网关需求基线 |
| [10.0 开工交接](10.0%20开工交接.md) | 网关开工前置项与交付清单 |
| [CLI-INTERFACE](CLI-INTERFACE.md) | 两个二进制 CLI 的参数与输出 |
| [PARAMETER-CONFIGURATION](PARAMETER-CONFIGURATION.md) | 运行时参数（CRD 字段、env、flag） |
| [RELEASE-CONSTRAINTS](RELEASE-CONSTRAINTS.md) | 版本、镜像、发布物与 CI 口径 |
| [TODO](TODO.md) | 本地临时清单，只记未完成项 |
