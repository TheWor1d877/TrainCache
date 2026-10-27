# 交接：M7 traincache 行重跑（暂停于 18:10）

截止 2026-10-27 18:10。下一个会话按本文件接着做，先读 `CLAUDE.md` 与 `docs/RELEASE-CONSTRAINTS.md`。

## 1. 当前状态

- 分支 `release`（`traincache` 的 git worktree），HEAD `fd6f302`，未 push、无 tag。
- 用户关机暂停实验；集群与 tc 随关机消失，恢复后重新 `bench-up` 即可。
- M7 重跑口径已冻结在 `docs/9.2 实验协议.md` §7 偏离记录：traincache 全行作废重跑，非 traincache 行沿用。
- gate `t_max_s` 已由 300 抬到 600（`up.sh` 默认 `10m`、`cond.py` 冻结值、148 个 COND 同步，已提交）。

## 2. 已完成的重跑产物

| 格子 | 组态 | 位置 | 行数 |
|---|---|---|---|
| S-100 | traincache-cold | `m7/S-100/_rerun2/traincache-cold/summary.csv` | 3 |

（另有 `m7/S-1000/_rerun20261027/traincache-cold/summary.csv`，是更早一轮，列结构旧，不作正式源。）

S-300 跑到 rep 2 时被中断，该组无产物，需整组重跑。

## 3. 未完成的重跑

按此顺序，串行，产物都落 `m7/<格>/_rerun2/<组>/summary.csv`：

```bash
cd ~/Project/TrainCache/traincache-release
PY=.venv/bin/python; R=~/tcd/bench/results/m7
# cold 剩四组
$PY bench/compare/run_bench.py --tier S --rate 300mbit  --groups traincache-cold --reps 3 --out $R/S-300/_rerun2/traincache-cold/summary.csv
$PY bench/compare/run_bench.py --tier S --rate 1000mbit --groups traincache-cold --reps 3 --out $R/S-1000/_rerun2/traincache-cold/summary.csv
$PY bench/compare/run_bench.py --tier M --rate 1000mbit --groups traincache-cold --reps 3 --out $R/M-1000/_rerun2/traincache-cold/summary.csv
$PY bench/compare/run_bench.py --tier L --rate 1000mbit --groups traincache-cold --reps 1 --out $R/L-1000/_rerun2/traincache-cold/summary.csv
# warm 两组
$PY bench/compare/run_bench.py --tier S --rate 100mbit  --groups traincache-warm --reps 3 --out $R/S-100/_rerun2/traincache-warm/summary.csv
$PY bench/compare/run_bench.py --tier S --rate 1000mbit --groups traincache-warm --reps 3 --out $R/S-1000/_rerun2/traincache-warm/summary.csv
```

之后再跑 M7 的 7.6（Pareto 一输入，42 run，约 4 h）：

```bash
for c in pareto1-fan1-free pareto1-fan1-gated pareto1-fan2-free pareto1-fan2-gated \
         pareto1-fan3-free pareto1-fan3-gated; do
  make -C bench bench COND=$c
done
```

## 4. 出汇总

```bash
$PY bench/compare/summarize_m7.py --selftest     # 先自检
$PY bench/compare/summarize_m7.py                # 出 m7_baseline.md / .csv
```

脚本已覆盖 7.4 与 7.5：表 A（makespan/stall）、表 B（命中率、三笔字节、预热时长）、组间 bootstrap CI 判定。

## 5. 环境准备（关机重启后必做）

1. `make -C bench bench-up`（kind + MinIO + 控制面 + tc + 冷读验证）。
2. release worktree 缺两个 gitignore 的本地件，缺则补：`.venv`（软链到主 worktree）、`bench/.sudo-pass`。
3. `make -C bench bench-status`、`make -C bench bench-doctor`，全 PASS 再开跑。

## 6. 容易踩的坑

- 两轮实验共用 MinIO 出口 tc，严禁并行；`run_bench` 每轮自己设 tc。
- 预热在 @100 档实测 332–343 s，`5m` 会 `tmax_exceeded`；现默认 10m。
- 中断组态会丢整组行（summary 在组结束时写盘），重跑从该组开头起。
- 数据层三条禁令：不删 `~/tcd`、产物只追加、清理命令不得指向 `~/tcd`。
- 版本口径：tag `vX.Y.Z`、镜像 tag、CHANGELOG 标题逐字一致；`CHANGELOG.md` 只在发版时写。
