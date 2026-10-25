# 旗舰实验报告模板（10.1 §11 · 9.3）

> 用法：复制到 `~/tcd/results/<批次>/report.md` 后逐节填写；无数据的格子一律写「待填」，不删行删列、不臆造数字。
> 判据：① 无需口头补充；② 环境清单定位到镜像 digest；③ 每张图含「预注册假设 vs 实测」对照栏。
> 归档：原始 jsonl 保留，中间 csv 可由 jsonl 重建；每个点可回溯到 COND 与 run ID。

## 1. 环境清单（每批次一份，从各 run 的 `lock.json` 汇总）

| 项 | 值 | 来源 |
|---|---|---|
| git commit | 待填 | `lock.json` 的 `git_commit` |
| 镜像 digest | 待填 | `lock.json` 的 `docker_images`（逐镜像 sha256） |
| tc 档位（rate/delay/burst） | 待填 | `lock.json` 的 `tc` |
| 节点数 | 待填 | COND `environment.kind_workers` |
| 种子 | 待填 | `lock.json` 的 `seed` |
| chunk-size 与所属快照 | 待填 | `lock.json` 的 `chunk_size_bytes` + `dataset_name`（正式 8388608 / `compare-data-8m`） |

## 2. 五组对照总表（S/M/L 三档容量 + exploratory 栏）

正式格子填「中位数（95% CI）」；样本次数与预注册一致才写胜负，3 次以下的点只写「样本不足」。

| 组态 | S 档 | M 档 | L 档 | exploratory |
|---|---|---|---|---|
| direct | 待填 | 待填 | 不参与 | 待填 |
| juicefs-cold | 待填 | 不参与 | 不参与 | 待填 |
| juicefs-warm | 待填 | 不参与 | 不参与 | 待填 |
| fluid-cold | 待填 | 待填 | 待填 | 待填 |
| fluid-warm | 待填 | 不参与 | 不参与 | 待填 |
| traincache-cold | 待填 | 待填 | 待填 | 待填 |
| traincache-warm | 待填 | 不参与 | 不参与 | 待填 |

档位参跑组按 D30；L 档 `reps=1`（D32），是探点、不进正式胜负，结果同时抄进 exploratory 栏并标 `exploratory`。exploratory 栏收纳其余所有不足 3 次采样的点。

### 2.1 预注册假设 vs 实测（五组对照）

| 预注册假设（preregister §1.9 原文照抄） | 实测（中位数，95% CI） | 判定（一致/相悖/样本不足） |
|---|---|---|
| 待填 | 待填 | 待填 |

## 3. 逐图小节

每张图（图一/图二/图三/Pareto 一/Pareto 二）复制下面骨架各写一份，标题与预注册章节对齐。

### 图 X：<图名>

- 假设：从预注册对应小节原文照抄，不改写。待填
- 结果：一句话结论 + 图 `figX.png` + 底层数据 `figX_summary.csv`，每个点标 COND 与 run ID。待填
- 预注册假设 vs 实测：

| 预注册假设 | 实测（中位数，95% CI） | 判定（一致/相悖/样本不足） |
|---|---|---|
| 待填 | 待填 | 待填 |

- 偏离说明：与冻结计划的偏差（超时作废、补跑、缺档、受节点数约束的 fanout 点等）；冻结后只允许追加，不回改假设。待填
- 阴性结论：哪些区域/点明确为负收益，如实报出、不隐藏；负收益区域在图上标出。待填

## 4. 复现入口

- 单点复现：`make -C bench bench COND=<name> REPS=<n>`，从空集群开始；产物目录结构与本批次一致（`run.json` / `metrics.jsonl` / `summary.csv` / `lock.json`）。
- 回归自检：仓库根 `make verify`（controller/core/agent）+ 五组冒烟 + 装置自检（`lock.py --selftest`、`cond.py --selftest`、`run_bench.py --selftest`、`tc_apply.sh --self-test`、`make -C bench/compare check`）。
