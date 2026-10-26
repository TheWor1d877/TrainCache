#!/usr/bin/env python3
"""M2 统计:平均数 + bootstrap 95% CI。

只依赖 numpy;数值列自动识别(尝试 float 转换)。
"""
import sys
from pathlib import Path

import numpy as np

# collect.py metrics.csv 中的数值指标列(用于 summarize_runs 的列识别兜底;
# 实际以 float 可解析为准,这里只作参考,不硬性限定)
NUMERIC_METRIC_HINTS = (
    "startup_makespan_s", "full_makespan_s", "post_pod_s", "gate_hold_s",
    "startup_fetch_bytes", "sustained_fetch_bytes", "switch_fetch_bytes",
    "total_fetch_bytes", "fetch_recon_diff_pct", "fetch_boundary_error_s",
    "fanout_actual", "fanout_max", "startup_bytes_status_est",
    "hit_hits_startup", "hit_misses_startup", "hit_ratio_startup",
    "hit_hits_steady", "hit_misses_steady", "hit_ratio_steady",
    "hit_hits_switch", "hit_misses_switch", "hit_ratio_switch",
    "hit_ratio_gauge_startup", "hit_ratio_gauge_steady",
    "hit_ratio_gauge_switch", "switch_count", "switch_censored",
)


def mean(values: list[float]) -> float:
    """返回平均数。空列表返回 0.0。"""
    if not values:
        return 0.0
    return float(np.mean(np.asarray(values, dtype=float)))


def bootstrap_ci(values: list[float], iters: int = 10000, alpha: float = 0.05,
                 seed: int | None = None) -> tuple[float, float]:
    """返回 (low, high) 置信区间。

    对 values 有放回采样 iters 次,每次算平均数,取 alpha/2 和 1-alpha/2 分位数。
    values 长度 < 2 时返回 (values[0], values[0])。
    """
    arr = np.asarray(values, dtype=float)
    if arr.size < 2:
        v = float(arr[0]) if arr.size == 1 else 0.0
        return (v, v)
    rng = np.random.default_rng(seed)
    n = arr.size
    # 有放回采样:iters × n 的索引矩阵
    idx = rng.integers(0, n, size=(iters, n))
    samples = arr[idx]
    means = np.mean(samples, axis=1)
    low = float(np.percentile(means, 100 * alpha / 2))
    high = float(np.percentile(means, 100 * (1 - alpha / 2)))
    return (low, high)


def _is_float(value) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def summarize_runs(runs: list[dict]) -> dict:
    """聚合多个 run 的指标。

    runs 是 collect.py 输出的 metrics.csv 行(dict)。对每个数值指标列算
    mean + bootstrap CI。void=True 的 run 不参与统计。
    返回 {metric: {'mean': x, 'ci_low': y, 'ci_high': z}}。
    """
    # 过滤 void run
    valid = []
    for r in runs:
        void = r.get("void")
        if isinstance(void, str):
            void = void.strip().lower() == "true"
        if void:
            continue
        valid.append(r)

    if not valid:
        return {}

    # 收集所有列
    all_cols: list[str] = []
    seen = set()
    for r in valid:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                all_cols.append(k)

    summary: dict = {}
    for col in all_cols:
        vals = []
        for r in valid:
            v = r.get(col)
            if _is_float(v):
                vals.append(float(v))
        if not vals:
            continue
        # 至少要有 2 个非空值才算有意义的统计(否则 CI 退化为单点)
        avg = mean(vals)
        ci_low, ci_high = bootstrap_ci(vals)
        summary[col] = {
            "mean": avg,
            "ci_low": ci_low,
            "ci_high": ci_high,
            "n": len(vals),
        }
    return summary


# ---------------------------------------------------------------------------
# selftest:构造已知分布验证 mean 与 bootstrap CI
# ---------------------------------------------------------------------------
def selftest() -> None:
    # mean
    assert mean([]) == 0.0
    assert mean([5.0]) == 5.0
    assert mean([1.0, 2.0, 3.0]) == 2.0
    assert mean([1.0, 2.0, 3.0, 4.0]) == 2.5

    # bootstrap_ci 单点退化
    lo, hi = bootstrap_ci([42.0])
    assert lo == 42.0 and hi == 42.0, (lo, hi)

    # 大样本对称分布:CI 应包含真实平均数
    rng = np.random.default_rng(42)
    data = list(rng.normal(loc=100.0, scale=10.0, size=500))
    avg = mean(data)
    lo, hi = bootstrap_ci(data, iters=10000, seed=123)
    assert lo <= avg <= hi, (lo, avg, hi)
    # CI 宽度应合理(窄于数据极差)
    assert hi - lo < (max(data) - min(data)), (lo, hi, max(data) - min(data))

    # summarize_runs:构造 collect.py 风格的 metrics 行
    runs = [
        {"run_id": "r1", "cond": "c1", "void": "false",
         "startup_makespan_s": 10.0, "full_makespan_s": 100.0,
         "hit_ratio_steady": 0.5, "notes": "ok"},
        {"run_id": "r2", "cond": "c1", "void": "false",
         "startup_makespan_s": 12.0, "full_makespan_s": 110.0,
         "hit_ratio_steady": 0.6, "notes": "ok"},
        {"run_id": "r3", "cond": "c1", "void": "true",  # void,应被排除
         "startup_makespan_s": 999.0, "full_makespan_s": 999.0,
         "hit_ratio_steady": 0.0, "notes": "void"},
        {"run_id": "r4", "cond": "c1", "void": "false",
         "startup_makespan_s": 11.0, "full_makespan_s": 105.0,
         "hit_ratio_steady": 0.55, "notes": "ok"},
    ]
    s = summarize_runs(runs)
    assert "startup_makespan_s" in s
    assert s["startup_makespan_s"]["n"] == 3  # void 被排除
    assert s["startup_makespan_s"]["mean"] == 11.0
    assert s["full_makespan_s"]["n"] == 3
    # 字符串列不应进入统计
    assert "run_id" not in s
    assert "notes" not in s
    assert "void" not in s

    # 空 runs
    assert summarize_runs([]) == {}
    assert summarize_runs([{"void": "true", "x": 1.0}]) == {}

    print("stats selftest: PASS")


if __name__ == "__main__":
    if "--selftest" in sys.argv or len(sys.argv) == 1:
        selftest()
