#!/usr/bin/env python3
"""百分位 bootstrap 置信区间与变异系数(零新依赖,仅 numpy)。"""
import numpy as np


def pct_bootstrap_ci(samples, stat=np.median, n_resamples=2000, alpha=0.05, seed=42):
    """有放回重采样,取 stat 的 bootstrap 分布,返回 (lo, hi, point)。"""
    samples = np.asarray(samples, dtype=float)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(samples), size=(n_resamples, len(samples)))
    dist = np.array([stat(samples[i]) for i in idx])
    return (
        float(np.percentile(dist, 100 * alpha / 2)),
        float(np.percentile(dist, 100 * (1 - alpha / 2))),
        float(stat(samples)),
    )


def cv(samples):
    """变异系数 std/mean(组内稳定性判据)。"""
    samples = np.asarray(samples, dtype=float)
    return float(samples.std() / samples.mean())
