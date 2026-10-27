# 交接：M7/M8/M5/M9 跑分阶段

截止 2026-10-27 的状态。下一个会话按本文件接着做，先读 `CLAUDE.md` 与 `docs/RELEASE-CONSTRAINTS.md`。

## 1. 当前状态

- 装置代码已改完，离线自检全部通过；真机冒烟还没跑（需要 kind + MinIO）。
- M7 五格合并表已整理：`~/tcd/bench/results/m7/{S-100,S-300,S-1000,M-1000,L-1000}/summary.all.csv`，合计 65 行数据。
- 用户马上要打 git tag。打标签后他会切出 RELEASE 分支，**之后所有实验与修改都只在 RELEASE 分支做，main 加保护**。
- 不要在 main 上继续改代码或跑正式实验。

## 2. 用户定过的口径

- 图二缓存固定 1 GiB：8 分片 `ratio=2`，32 分片 `ratio=8`。
- 图一/图三/Pareto 一的数据源走对照层 `bench/compare/run_bench.py` 的 Pod + MinIO 链路（方案 A）。
- M5 C 轨执行形态：单 Pod 双 Job 线程（`TRAINCACHE_TILT=1`）。
- Pareto 二基线：改用自身基线 `lookahead=8 + heat`，纵轴是相对该点的改善。
- M7 只跑 TrainCache 相关组，其余组用已有数据。

## 3. 本轮改了什么（都已落盘）

- `bench/compare/run_bench.py`
  - `ensure_agent_config()`：起跑前把 `cacheCapacityBytes`/`prefetchLookahead`/`evictPolicy` 对齐 `traincache-config`；容量变化才重启 controller-manager，agentd 由逐轮复位重启。
  - `AGENT_PREFETCH_K`（读 `TRAINCACHE_PREFETCH_LOOKAHEAD`）与训练侧 `PREFETCH_K`（`TRAINCACHE_PREFETCH_K`，固定 16）解耦。
  - `TRAINCACHE_TILT=1` 分支：测量 Pod 内两条线程跑 A/B 倾斜负载，`RESULT` 带 `a_hit_ratio`、`a_wait_ratio`、`b_evictions_of_hot`、`a_steps`、`b_steps`、`tilt_hot_shards`，CSV 同名六列。
  - `TILT` 只接受 `traincache*` 组，其它组直接退出。
- `bench/experiments/run_policy.py`：真实路径改为调 `run_bench.py`（`TILT=1`）；删除旧冷态/tc/采集死代码；selftest 仍走 mock。
- `bench/experiments/plot.py`：`plot_pareto2` 改自身基线，x 轴优先 `prefetch_bytes_run`，回退 `sustained_prefetch_bytes`。
- `controller/deploy/agent/configmap.yaml` + `daemonset.yaml`：新增 `evictPolicy`（默认 `heat`），DaemonSet 用 `configMapKeyRef` 注入 `TRAINCACHE_EVICT_POLICY`。
- 文档同步：`docs/4 bench装置说明.md`、`docs/9.2 实验协议.md` §5.2/§C3、`docs/RELEASE-CONSTRAINTS.md` §5.3、`docs/PARAMETER-CONFIGURATION.md`（ConfigMap 六键）。
- `bench/Makefile`：`py_compile` 补 `compare/summarize_m7.py`。

## 4. 验证过什么

```
python3 -m py_compile 全通过
python3 bench/compare/run_bench.py --selftest        PASS
python3 bench/experiments/run_policy.py --selftest   PASS
python3 bench/experiments/run_figures.py --dry-run --cond fig1-100-ratio2-fan2-gated  正确翻译命令
合成 9 点数据跑 plot_pareto2                          出图成功，基线点纵轴为 0
```

**没验证**：TILT 分支的真机执行、逐 COND 容量对齐的真实 patch、五张图在真实产物上的出图。

## 5. 接下来怎么跑

顺序：先真机冒烟 → M7 补齐 → M8 图系列 → M5 → M9。

### 5.1 起装置

```bash
make -C bench bench-up          # kind + k8s + MinIO + 控制面
make -C bench bench-status
make -C bench bench-doctor
```

镜像拉不到时先在宿主机 `docker pull`，再 `kind load`；不要改清单里的镜像名前缀。

### 5.2 每类图一个冒烟点

```bash
make -C bench bench COND=fig1-100-ratio2-fan2-gated
make -C bench bench COND=fig2-300-size8-ep10-tc
make -C bench bench COND=fig3-curriculum-ep3-fl
make -C bench bench COND=pareto1-fan2-free
make -C bench bench COND=pareto2-lookahead8-heat
```

冒烟后立刻 `python3 bench/experiments/plot.py` 确认五张图都能从产物读出来，再开全量。冒烟产物落在 `~/tcd/`，只追加不覆盖。

### 5.3 M7 核对

正式出图前先数一遍五格覆盖：

```bash
python3 bench/compare/summarize_m7.py --help   # 看当前用法
ls ~/tcd/bench/results/m7/*/summary.all.csv
```

S-100 应有七组态 × 3 次，S-300 三组 × 3，S-1000 七组 × 3，M-1000 与 L-1000 按 D30/D32。缺的格用 `run_bench.py --groups <组> --tier <档> --rate <档>` 补，产物直接落到对应 m7 目录；统计时要递归扫 `_topup*`/`_v010*`/`_rerun*` 子目录。

### 5.4 全量

```bash
# 图一/图二/图三/Pareto 一：run_figures 只覆盖这四类，pareto2 不在其中
python3 bench/experiments/run_figures.py --figure all

# Pareto 二：9 个 COND 逐个跑（Makefile 分流到 run_policy）
for c in pareto2-lookahead8-heat pareto2-lookahead8-lru pareto2-lookahead8-lfu \
         pareto2-lookahead16-heat pareto2-lookahead16-lru pareto2-lookahead16-lfu \
         pareto2-lookahead32-heat pareto2-lookahead32-lru pareto2-lookahead32-lfu; do
  make -C bench bench COND=$c
done

# 淘汰矩阵
make -C bench bench COND=policy-heat REPS=3
make -C bench bench COND=policy-lru  REPS=3
make -C bench bench COND=policy-lfu  REPS=3

# 出图
python3 bench/experiments/plot.py
```

## 6. 容易踩的坑

- **两个预取旋钮**：pareto2 的 8/16/32 是 Agent 侧 `TRAINCACHE_PREFETCH_LOOKAHEAD`；训练侧 `TRAINCACHE_PREFETCH_K` 必须保持 16。改 ConfigMap 后 agentd 必须重启才生效，`reset_traincache_agent()` 每 rep 会做。
- **pareto2 基线点必须跑**：`pareto2-lookahead8-heat` 缺了，`plot_pareto2` 直接跳过整张图。
- **TILT 组限制**：只有 `traincache-*` 组能进 `TRAINCACHE_TILT=1`，对照实验不要带这个变量。
- **ConfigMap 六键缺一不可**：缺 `evictPolicy` 会让 agentd 停在 `CreateContainerConfigError`。
- **数据层三条禁令**：不删 `~/tcd`、产物只追加、任何清理命令不得指向 `~/tcd`。
- **版本口径**：tag `vX.Y.Z`、镜像 tag、CHANGELOG 标题逐字一致；镜像名固定 `ghcr.io/thewor1d877/traincache/<组件>`，repo 内不出现私有镜像站域名。`CHANGELOG.md` 只在发版时写。
- **文档同步**：改代码前先改对应文档（文档是声明式）。文档按 `CLAUDE.md` 的写作规则改，不追加旧句、不写日期和过程。

## 7. 交接检查清单

- [ ] 确认当前分支是用户的 RELEASE 分支，不是 main。
- [ ] `make -C bench selftest` 通过。
- [ ] 集群与 MinIO 就绪，`bench-doctor` 全绿。
- [ ] 每类图各跑通 1 个点并能出图。
- [ ] 再开全量与 M9 复现。
