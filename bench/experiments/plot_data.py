#!/usr/bin/env python3
"""跑分结果的取数层：读 summary.csv 与 recovery.json，按族聚合成采样行。

只负责取数与聚合，不做渲染。plot_echarts.py 与 report.py 共用这里的口径，
避免出图与报告各自解析一份数据。
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parent.parent
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH))

import config  # noqa: E402
import stats  # noqa: E402

GROUP_ORDER = ["direct", "juicefs-cold", "juicefs-warm",
               "fluid-cold", "fluid-warm", "traincache-cold", "traincache-warm"]
GROUP_COLORS = {
    "direct": "#6f6f6f",
    "juicefs-cold": "#6fa9e0", "juicefs-warm": "#1f5f9e",
    "fluid-cold": "#f0a85a", "fluid-warm": "#c96a1c",
    "traincache-cold": "#a973d1", "traincache-warm": "#5b2a82",
}
SERIES_COLORS = ["#1f5f9e", "#a973d1", "#c96a1c", "#5b2a82", "#2f8f6b"]

E2E_SCENARIO_CN = {
    "checkpoint-resume": "断点续跑",
    "checkpoint-version": "换数据版本后恢复",
    "dual-restart": "训练与 agentd 同时重启",
}
CHAOS_FAULT_CN = {
    "kill_agent": "agentd 被杀",
    "minio_down": "源站下线",
    "tamper_block": "块被改坏",
    "netem_degrade": "源站降速",
    "netem_degrade_rate": "源站降速加限速",
    "stop_ordersrc": "订单写端停摆",
    "node_loss": "节点掉线",
}


def _num(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _cell(row: dict) -> str:
    return str(row.get("cond") or row.get("_cell") or "")


def _rho(row: dict) -> float | None:
    return _num(row.get("rho"))


def _rate(row: dict) -> str:
    return str(row.get("rate") or "")


def family_rows(family: str) -> list[dict]:
    """该族全部有效采样行。status=ok、void=false、无基线顶替。"""
    root = config.family_results_dir(family)
    rows: list[dict] = []
    if not root.is_dir():
        return rows
    for path in sorted(root.rglob("summary.csv")):
        rel = path.relative_to(root).parts
        if any(part.startswith("_") for part in rel):
            continue
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if str(row.get("family") or "") != family:
                    continue
                # status 列缺省按 ok 认：训练负载那支写手不落这一列，
                # 作废轮由 void 列单独拦。
                status = str(row.get("status") or "").strip().lower()
                if status and status != "ok":
                    continue
                if str(row.get("void") or "").strip().lower() in ("true", "1"):
                    continue
                if str(row.get("baseline_substituted") or "").strip():
                    continue
                row["_cell"] = rel[0]
                rows.append(row)
    return rows


def _collect(rows, metric, facet, x, series):
    """(分面, 横轴, 分组) → (均值, CI 下界, CI 上界, 样本数)。"""
    buckets: dict[tuple, list[float]] = {}
    for row in rows:
        value = _num(row.get(metric))
        if value is None:
            continue
        buckets.setdefault((facet(row), x(row), series(row)), []).append(value)
    out = {}
    for key, values in buckets.items():
        mean = sum(values) / len(values)
        if len(values) > 1:
            low, high = stats.bootstrap_ci(values, seed=7)
        else:
            low = high = mean
        out[key] = (mean, low, high, len(values))
    return out


def _gate(row):
    return str(row.get("gate_mode") or "")


def _policy(row):
    return str(row.get("evict_policy") or "")


def _window(row):
    match = re.search(r"-w(\d+)-rho", _cell(row))
    return int(match.group(1)) if match else None


def _arm(row):
    match = re.match(r"gateway-(g\d+)-", _cell(row))
    return match.group(1) if match else "?"


def _scenario(row):
    match = re.match(r"switch-(.+)-rho", _cell(row))
    return match.group(1) if match else "?"


def _hot(row):
    match = re.match(r"multi-hot(\d+)-", _cell(row))
    return f"{int(match.group(1))}%" if match else "?"


def _placement(row):
    cell = _cell(row)
    if cell.endswith("colocated"):
        return "colocated"
    if cell.endswith("spread"):
        return "spread"
    return "?"


def _rangepar_parts(row):
    """COND 名 rangepar-<档>-<臂> → (档, 臂)。"""
    match = re.match(r"^rangepar-(l\d+)-(par-on|par-off)$", _cell(row))
    if not match:
        return "?", "?"
    return match.group(1), match.group(2)


def _e2e_scenario(row):
    """COND 名 e2e-<场景> → 场景名。"""
    match = re.match(r"^e2e-(.+)$", _cell(row))
    return match.group(1) if match else "?"


def _chaos_rho(cond_name):
    """COND 名 chaos-<故障>-rho<ρ> → ρ 标签。"""
    match = re.search(r"-rho(\d+)$", cond_name)
    return f"ρ={match.group(1)}" if match else "?"


def _rate_key(row):
    return _rate(row)


def _rho_key(row):
    value = _rho(row)
    return f"{value:g}" if value is not None else "?"


def _rho_sorted(values):
    return sorted((v for v in values if v != "?"), key=float)


def _rate_fmt(value):
    mbit = config.rate_mbit(value)
    return f"{mbit} mbit" if mbit else str(value)


def _gateway_source_bytes(row):
    """判据用的源站字节：G2 取网关记的源站字节（数据面经网关从源站拉到的
    字节），G0 与 G1 取 MinIO 出站。两边都只算 Get 路径的流量，
    Agent 元数据路径直连源站的部分不在口径内。"""
    if _arm(row) == "g2":
        return _num(row.get("gateway_bytes"))
    return _num(row.get("origin_egress_bytes"))


def switch_recovery_rows() -> list[dict]:
    """切换族的回落时长：summary.csv 不含这一列，只能从每轮 recovery.json 取。

    每轮把成功回落的事件时长取平均，算该轮一个采样；删失与基线不足的事件
    没有 recovery_time_s，天然不进统计。
    """
    root = config.family_results_dir("switch")
    rows: list[dict] = []
    if not root.is_dir():
        return rows
    for path in sorted(root.glob("*/runs/*/recovery.json")):
        cell = path.relative_to(root).parts[0]
        try:
            events = json.loads(path.read_text(encoding="utf-8")).get("events") or []
        except (OSError, json.JSONDecodeError):
            continue
        times = [_num(event.get("recovery_time_s")) for event in events]
        times = [value for value in times if value is not None]
        if not times:
            continue
        match = re.search(r"-rho(\d+(?:\.\d+)?)$", cell)
        rows.append({"cond": cell, "_cell": cell,
                     "rho": match.group(1) if match else "",
                     "recovery_time_s": sum(times) / len(times)})
    return rows


def chaos_rows(family: str = "chaos") -> list[dict]:
    """混沌族的逐轮结果：results/chaos/<格子>/<轮次>/summary.txt 里的 PASS/FAIL。"""
    root = config.family_results_dir(family)
    rows: list[dict] = []
    if not root.is_dir():
        return rows
    for path in sorted(root.glob("*/*/summary.txt")):
        cond = path.parent.parent.name
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].startswith(("PASS", "FAIL")):
                rows.append({"cond": cond, "_cell": cond, "run": path.parent.name,
                             "fault": parts[0],
                             "status": "PASS" if parts[1].startswith("PASS") else "FAIL"})
    return rows


FAMILY_ROWS = {"chaos": chaos_rows}