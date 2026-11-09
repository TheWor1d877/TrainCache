## 探针开销不变量核对（待做）

- 动作类型：补实验数据。`bench/experiments/probe_overhead.py` 的不变量两列 `total_fetch_bytes` 与 `hit_ratio_steady` 只在 dataset 形态的集群里能取到，train 形态的 `collect.py` 写空值，工具输出「不可核对」。
- 做法：另起一个 kind 集群（主集群 `traincache` 正在跑实验，不动它），跑一个 COND 两遍，一遍 `TRAINTRACE_PROBE=1`、一遍不挂探针，用 `collect.py` 取到两列真值后重跑 `probe_overhead.py --off/--on`。
- 判定：两列相对差在容差内（`--fetch-tol-pct` 与 `--hit-tol`），开销数据成立；超出容差则两遍不可比，差值作废。
- 代价：一个 kind 集群的起停时间加两遍 COND 的机时。
