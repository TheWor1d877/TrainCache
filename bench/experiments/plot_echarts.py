#!/usr/bin/env python3
"""ECharts 出图：一套 option 同时产出交互页与静态 PNG。

数据口径直接复用 plot_data 的取行与聚合函数，渲染改用 ECharts：
Python 生成 option → Node 服务端渲染出 SVG → cairosvg 出 PNG，
同一批 option 另写进 index.html，浏览器里可交互。
产物落 results/figures-echarts/。混沌族不出图，判定看 summary.txt。
"""
from __future__ import annotations

from html import escape
import json
import shutil
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH = HERE.parent
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
import plot_data as pf  # noqa: E402
import stats  # noqa: E402

OUT = config.RESULTS_DIR / "figures-echarts"
ASSETS = HERE / "assets"
RENDER_JS = ASSETS / "render_echarts.js"
ECHARTS_JS = ASSETS / "echarts.min.js"
ERRORBAR_JS = ASSETS / "errorbar.js"

FONT = "Noto Sans CJK SC"
INK = "#15181d"
MUTED = "#5f6773"
SUB = "#3c434e"
GRID = "#eef1f5"
AXIS = "#d7dbe1"

TITLE_SIZE = 15
SUB_SIZE = 11
NAME_SIZE = 11
TICK_SIZE = 10

PAD_LEFT = 96
PAD_RIGHT = 40
PAD_TOP = 122
PAD_BOTTOM = 62
GAP_X = 58
GAP_Y = 82
CELL_W = 420
CELL_H = 470
SOLO_W = 820
SOLO_H = 430

PAGE_TITLE = {"zh": "TrainCache v0.9.3 实测数据",
              "en": "TrainCache v0.9.3 benchmark results"}

# 面向不熟悉这套系统的人：图上不出现 direct、g0、heat 这类内部代号。
GROUP_LABELS = {
    "zh": {"direct": "直读源站\n（无缓存）",
           "juicefs-cold": "JuiceFS\n未预热",
           "juicefs-warm": "JuiceFS\n已预热",
           "fluid-cold": "Fluid\n未预热",
           "fluid-warm": "Fluid\n已预热",
           "traincache-cold": "TrainCache\n未预热",
           "traincache-warm": "TrainCache\n已预热"},
    "en": {"direct": "origin only\n(no cache)",
           "juicefs-cold": "JuiceFS\ncold start",
           "juicefs-warm": "JuiceFS\nprewarmed",
           "fluid-cold": "Fluid\ncold start",
           "fluid-warm": "Fluid\nprewarmed",
           "traincache-cold": "TrainCache\ncold start",
           "traincache-warm": "TrainCache\nprewarmed"},
}
POLICY_LABELS = {
    "zh": {"heat": "热度优先", "lru": "LRU 最近最少用", "lfu": "LFU 最少使用"},
    "en": {"heat": "heat priority", "lru": "LRU (least recently used)",
           "lfu": "LFU (least frequently used)"},
}
ARM_LABELS = {
    "zh": {"g0": "直读源站", "g1": "仅本地缓存",
           "g2": "本地缓存＋网关缓存"},
    "en": {"g0": "origin only", "g1": "local cache only",
           "g2": "local cache + gateway cache"},
}


@lru_cache(maxsize=None)
def family_rows(family: str) -> tuple:
    return tuple(pf.family_rows(family))


def _base(width: int, height: int, title: str, sub: str = "",
          legend: list[str] | None = None) -> dict:
    option = {
        "__width": width,
        "__height": height,
        "animation": False,
        "backgroundColor": "#ffffff",
        "textStyle": {"fontFamily": FONT, "fontSize": 11, "fontWeight": 500,
                      "color": INK},
        "title": {
            "text": title,
            "subtext": sub,
            "left": 34,
            "top": 22,
            "textStyle": {"fontSize": TITLE_SIZE, "fontWeight": 700, "color": INK},
            "subtextStyle": {"fontSize": SUB_SIZE, "fontWeight": 500, "color": MUTED},
            "itemGap": 8,
        },
        "tooltip": {
            "trigger": "axis",
            "backgroundColor": "rgba(21,24,29,0.92)",
            "borderWidth": 0,
            "textStyle": {"fontSize": 11, "color": "#f4f6f8"},
            "axisPointer": {"type": "shadow",
                            "shadowStyle": {"color": "rgba(47,109,181,0.08)"}},
        },
    }
    if legend:
        option["legend"] = {
            "data": legend,
            "top": 78 if sub else 58,
            "left": 34,
            "icon": "roundRect",
            "itemWidth": 10,
            "itemHeight": 10,
            "itemGap": 16,
            "textStyle": {"fontSize": 11, "fontWeight": 600, "color": MUTED},
        }
    return option


def _wrap(label: str) -> str:
    """组态名按连字符断成两行，避免刻度标签倾斜。"""
    if "-" in label:
        head, tail = label.split("-", 1)
        return f"{head}\n{tail}"
    return label


def _x_axis(name: str, labels: list[str], index: int) -> dict:
    return {
        "type": "category",
        "name": name,
        "nameLocation": "middle",
        "nameGap": 40,
        "nameTextStyle": {"fontSize": NAME_SIZE, "fontWeight": 600,
                          "color": MUTED, "padding": [0, 0, 0, 0]},
        "data": labels,
        "gridIndex": index,
        "axisLine": {"lineStyle": {"color": AXIS}},
        "axisTick": {"show": False},
        "axisLabel": {"fontSize": TICK_SIZE, "fontWeight": 500, "color": MUTED,
                      "interval": 0, "rotate": 0, "lineHeight": 15},
    }


def _y_axis(name: str, index: int, top: float | None = None) -> dict:
    axis = {
        "type": "value",
        "name": name,
        "nameLocation": "middle",
        "nameRotate": 90,
        "nameGap": 54,
        "nameTextStyle": {"fontSize": NAME_SIZE, "fontWeight": 600, "color": MUTED},
        "gridIndex": index,
        "min": 0,
        "axisLine": {"show": False},
        "axisTick": {"show": False},
        "axisLabel": {"fontSize": TICK_SIZE, "fontWeight": 500, "color": "#8a919c"},
        "splitLine": {"lineStyle": {"color": GRID, "width": 1}},
    }
    if top is not None and top > 0:
        axis["max"] = top
    return axis


def _pct(value: float, total: float) -> str:
    """grid 用百分比定位，页面容器与画布不等宽时不会把右侧裁掉。"""
    return f"{value / total * 100:.2f}%"


def _layout(count: int, ncols: int, width: int, height: int) -> list[dict]:
    bottom = height - PAD_BOTTOM
    nrows = -(-count // ncols)
    w = (width - PAD_LEFT - PAD_RIGHT - GAP_X * (ncols - 1)) / ncols
    h = (bottom - PAD_TOP - GAP_Y * (nrows - 1)) / nrows
    grids = []
    for i in range(count):
        row, col = divmod(i, ncols)
        grids.append({"left": _pct(PAD_LEFT + col * (w + GAP_X), width),
                      "top": PAD_TOP + row * (h + GAP_Y),
                      "width": _pct(w, width), "height": h,
                      "containLabel": False})
    return grids


def _facet_titles(option: dict, grids: list[dict], labels: list[str]) -> dict:
    titles = [option["title"]]
    for grid, label in zip(grids, labels):
        titles.append({
            "text": label,
            "left": grid["left"],
            "top": grid["top"] - 30,
            "textStyle": {"fontSize": 12, "fontWeight": 600, "color": SUB},
        })
    option["title"] = titles
    return option


def build(name: str, lang: str, spec: dict, data: dict) -> dict:
    """把 (分面, 横轴, 系列) 的聚合值铺成 ECharts option。"""
    facets = spec.get("facet_order") or sorted({k[0] for k in data}, key=str)
    if not facets:
        facets = [""]
    xs = spec.get("x_order") or sorted({k[1] for k in data}, key=str)
    series_order = spec.get("series_order") or sorted({k[2] for k in data}, key=str)
    solo = len(facets) == 1 and facets[0] == ""
    kind = spec.get("kind", "bar")
    colors = spec.get("colors") or {}
    labels = spec.get("series_labels") or {}
    x_labels = spec.get("x_labels") or {}
    facet_labels = spec.get("facet_labels") or {}
    labeled = spec.get("value_labels", kind == "bar")
    stack = spec.get("stack", False)
    text = spec["text"][lang]
    legend_names = [labels.get(s, s) for s in series_order] if spec.get("legend") else None

    if solo:
        width, height, ncols = SOLO_W, SOLO_H, 1
    else:
        ncols = min(spec.get("ncols", 3), len(facets))
        nrows = -(-len(facets) // ncols)
        width = CELL_W * ncols + 110
        height = PAD_TOP + PAD_BOTTOM + nrows * 286 + (nrows - 1) * GAP_Y

    grids = _layout(len(facets), ncols, width, height)
    xaxes, yaxes = [], []
    for i in range(len(facets)):
        xaxes.append(_x_axis(text["x"], [x_labels.get(x, _wrap(str(x))) for x in xs], i))
        yaxes.append(_y_axis(text["y"] if (solo or i % ncols == 0) else "", i))

    series = []
    errorbars = []
    for fi, facet in enumerate(facets):
        facet_values = [data[(facet, x, s)][2]
                        for x in xs for s in series_order
                        if (facet, x, s) in data]
        top = None
        if kind == "bar" and labeled and facet_values:
            peak = max(v + max(0.0, v - 0.0) for v in facet_values)
            top = round(max(facet_values) * 1.16, 6) or None
        if top:
            yaxes[fi]["max"] = top

        if stack:
            for s in series_order:
                values = [round(data[(facet, x, s)][0], 2)
                          if (facet, x, s) in data else 0 for x in xs]
                series.append({
                    "name": labels.get(s, s), "type": "bar", "stack": "total",
                    "xAxisIndex": fi, "yAxisIndex": fi,
                    "itemStyle": {"color": colors.get(s, "#4b5563")},
                    "data": values,
                })
            continue

        for s in series_order:
            color = colors.get(s, "#4b5563")
            points = [data.get((facet, x, s)) for x in xs]
            if kind == "bar":
                if spec.get("color_by") == "x":
                    bars = []
                    for x, point in zip(xs, points):
                        bars.append({
                            "value": round(point[0], 2) if point else None,
                            "itemStyle": {"color": colors.get(x, "#4b5563"),
                                          "borderRadius": [3, 3, 0, 0]},
                        })
                else:
                    bars = [{"value": round(p[0], 2) if p else None,
                             "itemStyle": {"color": color,
                                           "borderRadius": [3, 3, 0, 0]}}
                            for p in points]
                series.append({
                    "name": labels.get(s, s), "type": "bar", "barMaxWidth": 24,
                    "barGap": "18%", "xAxisIndex": fi, "yAxisIndex": fi,
                    "itemStyle": {"color": color},
                    "data": bars,
                    "label": {"show": labeled, "position": "top", "fontSize": 9,
                              "fontWeight": 500, "color": MUTED},
                })
            else:
                line = {
                    "name": labels.get(s, s), "type": "line", "smooth": False,
                    "symbol": "circle", "symbolSize": 6,
                    "lineStyle": {"width": 2, "color": color},
                    "itemStyle": {"color": color, "borderWidth": 2,
                                  "borderColor": "#ffffff"},
                    "emphasis": {"scale": 1.6},
                    "xAxisIndex": fi, "yAxisIndex": fi,
                    "data": [round(p[0], 2) if p else None for p in points],
                }
                if spec.get("area"):
                    line["areaStyle"] = {"color": color, "opacity": 0.07}
                series.append(line)
                if spec.get("errorbars", True):
                    errorbars.append({
                        "xAxisIndex": fi, "yAxisIndex": fi, "color": color,
                        "points": [[x, round(p[1], 2), round(p[2], 2)]
                                   for x, p in zip(xs, points) if p],
                    })

    option = _base(width, height, text["title"], text.get("note", ""),
                   legend=legend_names)
    option.update({"grid": grids, "xAxis": xaxes, "yAxis": yaxes, "series": series})
    if errorbars:
        option["__errorbars"] = errorbars
    if not solo:
        option = _facet_titles(
            option, grids, [facet_labels.get(f, f) for f in facets])
    option["__name"] = name
    return option


def _h_value_axis(name: str, index: int) -> dict:
    return {
        "type": "value",
        "name": name,
        "nameLocation": "middle",
        "nameGap": 40,
        "nameTextStyle": {"fontSize": NAME_SIZE, "fontWeight": 600, "color": MUTED},
        "gridIndex": index,
        "min": 0,
        "axisLine": {"show": False},
        "axisTick": {"show": False},
        "axisLabel": {"fontSize": TICK_SIZE, "fontWeight": 500, "color": "#8a919c"},
        "splitLine": {"lineStyle": {"color": GRID, "width": 1}},
    }


def _h_category_axis(name: str, labels: list[str], index: int) -> dict:
    return {
        "type": "category",
        "name": "",
        "gridIndex": index,
        "data": labels,
        "inverse": True,
        "axisLine": {"lineStyle": {"color": AXIS}},
        "axisTick": {"show": False},
        "axisLabel": {"fontSize": TICK_SIZE, "fontWeight": 500, "color": MUTED,
                      "margin": 16, "lineHeight": 14},
    }


def build_hbar(name: str, lang: str, spec: dict, data: dict) -> dict:
    """横向条形：类别少、标签长，竖柱会显得空旷，横条把类别铺在纵轴。"""
    facets = spec.get("facet_order") or [""]
    xs = spec["x_order"]
    xs_by_facet = spec.get("x_order_by_facet") or {}
    series_order = spec.get("series_order") or ["all"]
    text = spec["text"][lang]
    colors = spec.get("colors") or {}
    labels = spec.get("series_labels") or {}
    x_labels = spec.get("x_labels") or {}
    facet_labels = spec.get("facet_labels") or {}
    baseline = spec.get("baseline")
    solo = len(facets) == 1 and facets[0] == ""
    rows = max((len(xs_by_facet.get(f, xs)) for f in facets), default=len(xs))
    if solo:
        width = 880
        height = max(360, 132 + rows * 62)
        left = 214
        grids = [{"left": _pct(left, width), "top": 122,
                  "width": _pct(width - left - 116, width),
                  "height": height - 122 - 56}]
    else:
        ncols = min(3, len(facets))
        cell_w, label_w = 420, 190
        width = 40 + ncols * cell_w + (ncols - 1) * 26 + 24
        height = 132 + 56 + rows * 50
        grids = [{"left": _pct(40 + (i % ncols) * (cell_w + 26) + label_w, width),
                  "top": 132,
                  "width": _pct(cell_w - label_w - 14, width),
                  "height": height - 132 - 56} for i in range(len(facets))]

    def point(facet: str, x, s):
        return data.get((facet, x, s)) or data.get((x, s))

    series = []
    for fi, facet in enumerate(facets):
        facet_xs = xs_by_facet.get(facet, xs)
        for s in series_order:
            bars = []
            for x in facet_xs:
                found = point(facet, x, s)
                color = (colors.get(x, "#4b5563") if spec.get("color_by") == "x"
                         else colors.get(s, "#4b5563"))
                bars.append({"value": round(found[0], 2) if found else None,
                             "itemStyle": {"color": color,
                                           "borderRadius": [0, 3, 3, 0]}})
            entry = {
                "name": labels.get(s, s), "type": "bar", "barMaxWidth": 20,
                "barGap": "28%", "data": bars,
                "itemStyle": {"color": color},
                "xAxisIndex": fi, "yAxisIndex": fi,
                "label": {"show": True, "position": "right", "fontSize": 9,
                          "fontWeight": 500, "color": MUTED},
            }
            if baseline is not None:
                entry["markLine"] = {
                    "silent": True, "symbol": "none",
                    "lineStyle": {"color": "#9aa0a6", "type": "dashed", "width": 1},
                    "label": {"show": True, "position": "end", "fontSize": 9,
                              "color": MUTED},
                    "data": [{"xAxis": baseline,
                              "label": {"formatter": f"{baseline}"}}],
                }
            series.append(entry)

    option = _base(width, height, text["title"], text.get("note", ""),
                   legend=[labels.get(s, s) for s in series_order]
                   if spec.get("legend") else None)
    option.update({
        "grid": grids,
        "xAxis": [_h_value_axis(text["y"] if i == 0 else "", i)
                  for i in range(len(facets))],
        "yAxis": [_h_category_axis(
            "", [x_labels.get(x, str(x)) for x in xs_by_facet.get(f, xs)], i)
            for i, f in enumerate(facets)],
        "series": series,
    })
    if solo and text.get("x"):
        # 类别轴名放到网格上方左端，避免与左侧类别标签、左上角图例抢位置。
        option["title"] = [option["title"], {
            "text": text["x"], "left": grids[0]["left"], "top": grids[0]["top"] - 28,
            "textStyle": {"fontSize": NAME_SIZE, "fontWeight": 600, "color": MUTED},
        }]
    elif not solo:
        option = _facet_titles(
            option, grids, [facet_labels.get(f, f) for f in facets])
    option["__name"] = name
    return option


def build_dumbbell(name: str, lang: str, spec: dict, data: dict) -> dict:
    """哑铃图：一条线连两个端点，用来看同一对象在两端的差。"""
    xs = spec["x_order"]
    series_order = spec["series_order"]
    text = spec["text"][lang]
    colors = spec.get("colors") or {}
    labels = spec.get("series_labels") or {}
    x_labels = spec.get("x_labels") or {}
    width = 880
    height = max(340, 132 + len(xs) * 74)
    left, right, top, bottom = 214, 130, 122, 56
    grid = {"left": left, "top": top, "width": width - left - right,
            "height": height - top - bottom}

    series = []
    for index, s in enumerate(series_order):
        points = []
        for i, x in enumerate(xs):
            point = data.get((x, s))
            if point:
                points.append([round(point[0], 2), i])
        series.append({
            "name": labels.get(s, s), "type": "scatter", "symbolSize": 10,
            "itemStyle": {"color": colors.get(s, "#4b5563"),
                          "borderWidth": 2, "borderColor": "#ffffff"},
            "label": {"show": index == len(series_order) - 1,
                      "position": "right", "fontSize": 9, "fontWeight": 500,
                      "color": MUTED},
            "data": points,
        })

    lines = []
    for i, x in enumerate(xs):
        ends = [data.get((x, s)) for s in series_order]
        if len(ends) == 2 and all(ends):
            lines.append([round(ends[0][0], 2), round(ends[1][0], 2), i])

    option = _base(width, height, text["title"],
                   legend=[labels.get(s, s) for s in series_order])
    option.update({
        "grid": [grid],
        "xAxis": [_h_value_axis(text["y"], 0)],
        "yAxis": [_h_category_axis(text["x"],
                                   [x_labels.get(x, str(x)) for x in xs], 0)],
        "series": series,
        "__dumbbells": [{"xAxisIndex": 0, "yAxisIndex": 0, "color": "#9aa0a6",
                         "points": lines}],
    })
    if text.get("x"):
        option["title"] = [option["title"], {
            "text": text["x"], "left": left, "top": top - 28,
            "textStyle": {"fontSize": NAME_SIZE, "fontWeight": 600, "color": MUTED},
        }]
    option["__name"] = name
    return option


# ---- 各族的取数 ----------------------------------------------------------

def _rho_text(v: str, lang: str) -> str:
    return f"ρ={v}" if lang == "zh" else f"rho={v}"


def fig_compare_groups(lang: str) -> dict:
    rows = family_rows("compare")
    at = [r for r in rows if pf._rho_key(r) in ("1", "4")]
    groups = [g for g in pf.GROUP_ORDER
              if any(str(r.get("group") or "") == g for r in at)]
    facets = sorted({str(r.get("rate") or "") for r in at},
                    key=config.rate_mbit)
    xs_by_facet = {
        f: [g for g in groups
            if any(str(r.get("rate") or "") == f
                   and str(r.get("group") or "") == g for r in at)]
        for f in facets
    }
    data = pf._collect(at, "total_makespan", pf._rate_key,
                       lambda r: str(r.get("group") or ""), pf._rho_key)
    partial = {f: len(xs_by_facet[f]) < len(groups) for f in facets}
    return build_hbar("compare-rho1-rho4", lang, {
        "facet_order": facets, "x_order": groups,
        "x_order_by_facet": xs_by_facet,
        "series_order": ["1", "4"], "legend": True,
        "colors": {"1": pf.SERIES_COLORS[0], "4": pf.SERIES_COLORS[1]},
        "series_labels": {
            "1": "ρ=1（缓存装得下）" if lang == "zh" else "rho=1 (cache fits)",
            "4": "ρ=4（缓存不足）" if lang == "zh" else "rho=4 (cache too small)"},
        "x_labels": {g: GROUP_LABELS[lang][g].replace("\n", " ") for g in groups},
        "facet_labels": {
            f: f"链路带宽 {config.rate_mbit(f)} Mbit/s"
               + ("（部分方案无读数）" if partial[f] else "")
            if lang == "zh"
            else f"{config.rate_mbit(f)} Mbit/s"
                 + (" (some setups not measured)" if partial[f] else "")
            for f in facets},
        "text": {"zh": {"title": "缓存足够与缓存不足时的端到端耗时（ρ=1 与 ρ=4）",
                        "y": "端到端耗时（s）", "x": "存储方案"},
                 "en": {"title": "End-to-end run time with cache fitting and cache short (rho=1 and rho=4)",
                        "y": "end-to-end run time (s)", "x": "storage setup"}},
    }, data)


def fig_compare_ratio(lang: str) -> dict:
    rows = family_rows("compare")
    groups = [g for g in pf.GROUP_ORDER if g != "direct"]
    rhos = ["0.5", "1", "2", "4", "8"]
    facets = sorted({str(r.get("rate") or "") for r in rows
                     if str(r.get("group") or "") == "direct"},
                    key=config.rate_mbit)
    base = {}
    for f in facets:
        values = [pf._num(r.get("total_makespan")) for r in rows
                  if str(r.get("group") or "") == "direct"
                  and str(r.get("rate") or "") == f]
        values = [v for v in values if v is not None]
        if values:
            base[f] = sum(values) / len(values)
    facets = [f for f in facets if f in base]
    data = {}
    for f in facets:
        for rho in rhos:
            for g in groups:
                values = [pf._num(r.get("total_makespan")) for r in rows
                          if str(r.get("rate") or "") == f
                          and str(r.get("group") or "") == g
                          and pf._rho_key(r) == rho]
                values = [v for v in values if v is not None]
                if not values:
                    continue
                ratio = sum(values) / len(values) / base[f]
                data[(f, rho, g)] = (ratio, ratio, ratio, len(values))
    present = {f: {g for (ff, _, g) in data if ff == f} for f in facets}
    option = build("compare-ratio", lang, {
        "kind": "line", "facet_order": facets, "x_order": rhos,
        "series_order": groups, "legend": True, "errorbars": False,
        "colors": pf.GROUP_COLORS,
        "series_labels": {g: GROUP_LABELS[lang][g].replace("\n", " ")
                          for g in groups},
        "facet_labels": {
            f: f"链路带宽 {config.rate_mbit(f)} Mbit/s"
               + ("（仅 TrainCache 有读数）" if len(present[f]) == 1 else "")
            if lang == "zh"
            else f"{config.rate_mbit(f)} Mbit/s"
                 + (" (TrainCache only)" if len(present[f]) == 1 else "")
            for f in facets},
        "text": {"zh": {"title": "相对直读源站的耗时比（低于 1 表示更快）",
                        "y": "相对直读的耗时倍数",
                        "x": "缓存缺口 ρ（数据集字节 ÷ 缓存容量）",
                        "note": "直读源站只测 ρ=4，基准按不随 ρ 变化处理"},
                 "en": {"title": "Run time relative to reading the origin (below 1 is faster)",
                        "y": "run-time ratio against origin",
                        "x": "cache shortfall rho (dataset bytes / cache size)",
                        "note": "origin only measured at rho=4; reference held constant across rho"}},
    }, data)
    for index, f in enumerate(facets):
        values = [data[(f, rho, g)][0] for rho in rhos for g in groups
                  if (f, rho, g) in data]
        if not values:
            continue
        low, high = min(values), max(values)
        pad = max(0.02, (high - low) * 0.15)
        option["yAxis"][index]["min"] = round(max(0.0, low - pad), 3)
        option["yAxis"][index]["max"] = round(high + pad, 3)
    seen = set()
    for entry in option["series"]:
        index = entry["xAxisIndex"]
        if index in seen:
            continue
        seen.add(index)
        entry["markLine"] = {
            "silent": True, "symbol": "none",
            "lineStyle": {"color": "#8a919c", "type": "dashed", "width": 1},
            "label": {"show": True, "position": "insideEndTop", "fontSize": 9,
                      "color": MUTED,
                      "formatter": "直读源站 1.0" if lang == "zh" else "origin only 1.0"},
            "data": [{"yAxis": 1.0}],
        }
    return option


def fig_compare_rho(lang: str) -> dict:
    rows = family_rows("compare")
    tc = [r for r in rows if str(r.get("group") or "").startswith("traincache")]
    data = pf._collect(tc, "total_makespan", pf._rate_key, pf._rho_key,
                       lambda r: str(r.get("group") or ""))
    facets = sorted({k[0] for k in data}, key=config.rate_mbit)
    rhos = pf._rho_sorted({k[1] for k in data})
    only_cold = {f: not any(str(r.get("group") or "") == "traincache-warm"
                            and str(r.get("rate") or "") == f for r in tc)
                 for f in facets}
    option = build("compare-rho", lang, {
        "kind": "line", "facet_order": facets, "x_order": rhos,
        "series_order": ["traincache-cold", "traincache-warm"],
        "colors": pf.GROUP_COLORS, "legend": True, "area": True,
        "series_labels": {
            "traincache-cold": "未预热" if lang == "zh" else "cold start",
            "traincache-warm": "已预热" if lang == "zh" else "prewarmed"},
        "facet_labels": {f: f"链路带宽 {config.rate_mbit(f)} Mbit/s"
                            + ("（仅未预热组）" if only_cold[f] else "")
                         if lang == "zh"
                         else f"{config.rate_mbit(f)} Mbit/s"
                              + (" (cold start only)" if only_cold[f] else "")
                         for f in facets},
        "text": {"zh": {"title": "缓存容量收紧时端到端耗时的变化（TrainCache，按链路带宽分面）",
                        "y": "端到端耗时（s）",
                        "x": "缓存缺口 ρ（数据集字节 ÷ 缓存容量）"},
                 "en": {"title": "End-to-end run time as cache shrinks (TrainCache, by bandwidth)",
                        "y": "end-to-end run time (s)",
                        "x": "cache shortfall rho (dataset bytes / cache size)"}},
    }, data)
    base = {}
    for f in facets:
        values = [pf._num(r.get("total_makespan")) for r in rows
                  if str(r.get("group") or "") == "direct"
                  and str(r.get("rate") or "") == f]
        values = [v for v in values if v is not None]
        if values:
            base[f] = sum(values) / len(values)
    seen = set()
    for entry in option["series"]:
        index = entry["xAxisIndex"]
        if index in seen or facets[index] not in base:
            continue
        seen.add(index)
        entry["markLine"] = {
            "silent": True, "symbol": "none",
            "lineStyle": {"color": "#8a919c", "type": "dashed", "width": 1},
            "label": {"show": True, "position": "insideEndBottom",
                      "fontSize": 9, "color": MUTED,
                      "formatter": ("直读源站 %.0f s" if lang == "zh"
                                    else "origin only %.0f s") % base[facets[index]]},
            "data": [{"yAxis": round(base[facets[index]], 1)}],
        }
    for index, f in enumerate(facets):
        values = []
        for rho in rhos:
            for series_name in ("traincache-cold", "traincache-warm"):
                point = data.get((f, rho, series_name))
                if point:
                    values.extend((point[1], point[2]))
        if f in base:
            values.append(base[f])
        if not values:
            continue
        low, high = min(values), max(values)
        pad = max(8.0, (high - low) * 0.08)
        option["yAxis"][index]["min"] = round(max(0.0, low - pad), 1)
        option["yAxis"][index]["max"] = round(high + pad, 1)
    return option


def _gateway_rows() -> list[dict]:
    rows = [dict(r, source_bytes=pf._gateway_source_bytes(r))
            for r in family_rows("gateway")]
    return rows


def _drop_facet(data: dict) -> dict:
    """去掉分面维度，横向图与哑铃图的键是 (横轴, 系列)。"""
    return {(k[1], k[2]): v for k, v in data.items()}


def fig_gateway_makespan(lang: str) -> dict:
    rows = _gateway_rows()
    arms = sorted({pf._arm(r) for r in rows})
    data = pf._collect(rows, "total_makespan", lambda r: "", pf._arm, lambda r: "all")
    return build_hbar("gateway-makespan", lang, {
        "x_order": arms, "series_order": ["all"], "color_by": "x",
        "colors": {a: "#2f6db5" for a in arms},
        "x_labels": ARM_LABELS[lang],
        "text": {"zh": {"title": "网关缓存对端到端耗时的影响",
                        "y": "端到端耗时（s）", "x": "读取路径"},
                 "en": {"title": "Impact of the gateway cache on end-to-end run time",
                        "y": "end-to-end run time (s)", "x": "read path"}},
    }, _drop_facet(data))


def fig_gateway_bytes(lang: str) -> dict:
    rows = _gateway_rows()
    for row in rows:
        value = pf._num(row.get("source_bytes"))
        row["source_bytes"] = None if value is None else value / 1e9
    arms = sorted({pf._arm(r) for r in rows})
    data = pf._collect(rows, "source_bytes", lambda r: "", pf._arm, lambda r: "all")
    return build_hbar("gateway-source-bytes", lang, {
        "x_order": arms, "series_order": ["all"], "color_by": "x",
        "colors": {a: "#2f6db5" for a in arms},
        "x_labels": ARM_LABELS[lang],
        "text": {"zh": {"title": "网关缓存对源站出流量的影响",
                        "y": "源站出流量（GB）", "x": "读取路径"},
                 "en": {"title": "Impact of the gateway cache on origin egress",
                        "y": "origin egress (GB)", "x": "read path"}},
    }, _drop_facet(data))


def _rho_label(v: str, lang: str) -> str:
    """图例里的 ρ：标出缓存是否装得下数据集。"""
    tight = float(v) > 1.0
    if lang == "zh":
        return f"ρ={v}（缓存不足）" if tight else f"ρ={v}（缓存足够）"
    return f"rho={v} (cache too small)" if tight else f"rho={v} (cache fits)"


def _evict_spec(lang: str, metric: str, title_zh: str, title_en: str,
                y_zh: str, y_en: str) -> tuple[dict, dict]:
    rows = family_rows("evict")
    xs = [p for p in ["heat", "lru", "lfu"] if any(pf._policy(r) == p for r in rows)]
    rhos = pf._rho_sorted({pf._rho_key(r) for r in rows})
    data = pf._collect(rows, metric, lambda r: "", pf._policy, pf._rho_key)
    spec = {
        "x_order": xs, "series_order": rhos, "legend": True,
        "colors": dict(zip(rhos, pf.SERIES_COLORS)),
        "series_labels": {r: _rho_label(r, lang) for r in rhos},
        "x_labels": POLICY_LABELS[lang],
        "text": {"zh": {"title": title_zh, "y": y_zh, "x": "缓存淘汰策略"},
                 "en": {"title": title_en, "y": y_en, "x": "eviction policy"}},
    }
    return spec, data


def fig_evict_makespan(lang: str) -> dict:
    spec, data = _evict_spec(lang, "total_makespan",
                             "三种缓存淘汰策略的端到端耗时（ρ=1 与 ρ=8）",
                             "End-to-end run time of three eviction policies (rho=1 vs rho=8)",
                             "端到端耗时（s）", "end-to-end run time (s)")
    return build_hbar("evict-makespan", lang, spec, _drop_facet(data))


def fig_evict_hot(lang: str) -> dict:
    spec, data = _evict_spec(lang, "b_evictions_of_hot",
                             "三种淘汰策略下的热数据驱逐次数",
                             "Hot-data evictions under three eviction policies",
                             "热数据驱逐次数", "hot-data evictions")
    spec["text"][lang]["note"] = ("ρ=1 时三种策略均未驱逐热数据"
                                  if lang == "zh"
                                  else "no hot data evicted at rho=1")
    return build_hbar("evict-hot-evictions", lang, spec, _drop_facet(data))


def _evict_mix_cols(lang: str, metric: str, title_zh: str, title_en: str,
                    y_zh: str, y_en: str) -> dict:
    rows = family_rows("evict-mix")
    policies = [p for p in ["heat", "lru", "lfu"]
                if any(pf._policy(r) == p for r in rows)]
    rhos = pf._rho_sorted({pf._rho_key(r) for r in rows})
    data = pf._collect(rows, metric, lambda r: "", pf._rho_key, pf._policy)
    return build(metric.replace("_", "-"), lang, {
        "x_order": rhos, "series_order": policies, "legend": True,
        "colors": dict(zip(policies, pf.SERIES_COLORS)),
        "series_labels": POLICY_LABELS[lang],
        "x_labels": {r: _rho_text(r, lang) for r in rhos},
        "text": {"zh": {"title": title_zh, "y": y_zh,
                        "x": "缓存缺口 ρ（数据集字节 ÷ 缓存容量）"},
                 "en": {"title": title_en, "y": y_en,
                        "x": "cache shortfall rho (dataset bytes / cache size)"}},
    }, data)


def fig_evict_mix_makespan(lang: str) -> dict:
    option = _evict_mix_cols(lang, "total_makespan",
                             "混合访问模式下三种淘汰策略的端到端耗时",
                             "End-to-end run time of three eviction policies under mixed access",
                             "端到端耗时（s）", "end-to-end run time (s)")
    option["__name"] = "evict-mix-makespan"
    return option


def fig_evict_mix_hot(lang: str) -> dict:
    option = _evict_mix_cols(lang, "b_evictions_of_hot",
                             "混合访问模式下的热数据驱逐次数",
                             "Hot-data evictions under mixed access",
                             "热数据驱逐次数", "hot-data evictions")
    option["__name"] = "evict-mix-hot-evictions"
    return option



def fig_evict_mix_survive(lang: str) -> dict:
    rows = family_rows("evict-mix")
    policies = [p for p in ["heat", "lru", "lfu"]
                if any(pf._policy(r) == p for r in rows)]
    rhos = pf._rho_sorted({pf._rho_key(r) for r in rows})
    low = pf._collect(rows, "lowfreq_fresh_survive", pf._policy, pf._rho_key,
                      lambda r: "lowfreq-fresh")
    high = pf._collect(rows, "highfreq_stale_survive", pf._policy, pf._rho_key,
                       lambda r: "highfreq-stale")
    return build("evict-mix-survive", lang, {
        "kind": "line", "facet_order": policies, "x_order": rhos,
        "series_order": ["lowfreq-fresh", "highfreq-stale"], "legend": True,
        "area": True,
        "colors": {"lowfreq-fresh": "#2f8f6b", "highfreq-stale": "#c95f81"},
        "series_labels": {
            "lowfreq-fresh": "低频新数据" if lang == "zh" else "rarely read data",
            "highfreq-stale": "高频旧数据" if lang == "zh" else "repeatedly read old data"},
        "facet_labels": POLICY_LABELS[lang],
        "text": {"zh": {"title": "混合访问模式下两类数据的缓存存活率",
                        "y": "缓存存活率",
                        "x": "缓存缺口 ρ（数据集字节 ÷ 缓存容量）"},
                 "en": {"title": "Cache survival ratio of two data types under mixed access",
                        "y": "survival ratio",
                        "x": "cache shortfall rho (dataset bytes / cache size)"}},
    }, {**low, **high})


def fig_evict_mix_rounds(lang: str) -> dict:
    rows = family_rows("evict-mix")
    policies = [p for p in ["heat", "lru", "lfu"]
                if any(pf._policy(r) == p for r in rows)]
    rhos = pf._rho_sorted({pf._rho_key(r) for r in rows})
    danger = pf._collect(rows, "evict_rounds_danger", pf._policy, pf._rho_key,
                         lambda r: "danger")
    return build("evict-mix-rounds", lang, {
        "facet_order": policies, "x_order": rhos,
        "series_order": ["danger"],
        "colors": {"danger": "#c0392b"},
        "series_labels": {"danger": "强制清理" if lang == "zh" else "forced cleanup"},
        "facet_labels": POLICY_LABELS[lang],
        "x_labels": {r: _rho_text(r, lang) for r in rhos},
        "text": {"zh": {"title": "混合访问模式下的强制清理轮次",
                        "y": "强制清理轮次", "x": "ρ",
                        "note": "三种策略在正常路径下均未触发强制清理"},
                 "en": {"title": "Forced cleanup rounds under mixed access",
                        "y": "forced cleanup rounds", "x": "rho",
                        "note": "no forced cleanup triggered on the normal path"}},
    }, danger)


def fig_prefetch(lang: str) -> dict:
    rows = family_rows("prefetch")
    rhos = pf._rho_sorted({pf._rho_key(r) for r in rows})
    look = sorted({int(pf._num(r.get("prefetch_lookahead")) or 0) for r in rows})
    fans = sorted({int(pf._num(r.get("prefetch_fanout")) or 0) for r in rows})
    data = pf._collect(rows, "total_makespan", pf._rho_key,
                       lambda r: int(pf._num(r.get("prefetch_lookahead")) or 0),
                       lambda r: int(pf._num(r.get("prefetch_fanout")) or 0))
    option = build("prefetch-lookahead", lang, {
        "kind": "line", "facet_order": rhos, "x_order": look,
        "series_order": fans, "legend": True, "area": True,
        "colors": dict(zip(fans, pf.SERIES_COLORS)),
        "series_labels": {f: (f"副本数 {f}" if lang == "zh" else f"{f} replica{'s' if f > 1 else ''}")
                          for f in fans},
        "facet_labels": {r: _rho_label(r, lang) for r in rhos},
        "text": {"zh": {"title": "预取窗口与副本数对端到端耗时的影响",
                        "y": "端到端耗时（s）", "x": "预取前瞻块数"},
                 "en": {"title": "Effect of prefetch lookahead and replica count on run time",
                        "y": "end-to-end run time (s)", "x": "prefetch lookahead (chunks)"}},
    }, data)
    for index, rho in enumerate(rhos):
        values = []
        for count in look:
            for fan in fans:
                point = data.get((rho, count, fan))
                if point:
                    values.extend((point[1], point[2]))
        if not values:
            continue
        low, high = min(values), max(values)
        pad = max(5.0, (high - low) * 0.08)
        option["yAxis"][index]["min"] = round(max(0.0, low - pad), 1)
        option["yAxis"][index]["max"] = round(high + pad, 1)
    return option


def fig_warmup(lang: str) -> dict:
    rows = family_rows("warmup")
    rhos = pf._rho_sorted({pf._rho_key(r) for r in rows})
    windows = sorted({pf._window(r) for r in rows if pf._window(r) is not None})
    gates = [g for g in ["gated", "free"] if any(pf._gate(r) == g for r in rows)]
    data = pf._collect(rows, "startup_makespan", pf._rho_key, pf._window, pf._gate)
    return build("warmup-startup", lang, {
        "facet_order": rhos, "x_order": windows, "series_order": gates,
        "legend": True, "colors": dict(zip(gates, pf.SERIES_COLORS)),
        "series_labels": {"gated": "门控放行（gated）" if lang == "zh" else "gated release",
                          "free": "立即放行（free）" if lang == "zh" else "immediate release"},
        "facet_labels": {r: _rho_label(r, lang) for r in rhos},
        "text": {"zh": {"title": "预热门控与等待上限对启动耗时的影响",
                        "y": "启动耗时（s）", "x": "门控等待上限（s）"},
                 "en": {"title": "Effect of warmup gating and wait limit on startup time",
                        "y": "startup time (s)", "x": "gating wait limit (s)"}},
    }, data)


SWITCH_LABELS = {
    "zh": {"fixed": "固定顺序", "per-epoch": "每 epoch 重排",
           "curriculum": "课程式收窄", "curriculum-narrow": "课程式快速收窄"},
    "en": {"fixed": "fixed order", "per-epoch": "reshuffled each epoch",
           "curriculum": "curriculum, gradual",
           "curriculum-narrow": "curriculum, steep"},
}


def fig_switch(lang: str) -> dict:
    rows = pf.switch_recovery_rows()
    xs = [s for s in ["fixed", "per-epoch", "curriculum", "curriculum-narrow"]
          if any(pf._scenario(r) == s for r in rows)]
    rhos = pf._rho_sorted({pf._rho_key(r) for r in rows})
    data = pf._collect(rows, "recovery_time_s", lambda r: "", pf._scenario,
                       pf._rho_key)
    return build("switch-recovery", lang, {
        "x_order": xs, "series_order": rhos, "legend": True,
        "colors": dict(zip(rhos, pf.SERIES_COLORS)),
        "series_labels": {r: _rho_label(r, lang) for r in rhos},
        "x_labels": SWITCH_LABELS[lang],
        "text": {"zh": {"title": "数据消费顺序变化后的回落时间",
                        "y": "回落时间（s）", "x": "数据顺序变化方式"},
                 "en": {"title": "Recovery time after the data consumption order changes",
                        "y": "recovery time (s)", "x": "order change pattern"}},
    }, data)


def _multi(lang: str, metric: str, title_zh: str, title_en: str,
           y_zh: str, y_en: str, name: str) -> dict:
    rows = family_rows("multiconsumer")
    xs = [h for h in ["10%", "50%"] if any(pf._hot(r) == h for r in rows)]
    places = [p for p in ["spread", "colocated"]
              if any(pf._placement(r) == p for r in rows)]
    data = pf._collect(rows, metric, lambda r: "", pf._hot, pf._placement)
    return build(name, lang, {
        "x_order": xs, "series_order": places, "legend": True,
        "colors": dict(zip(places, pf.SERIES_COLORS)),
        "series_labels": {
            "spread": "跨节点部署" if lang == "zh" else "readers on different nodes",
            "colocated": "同节点部署" if lang == "zh" else "readers on the same node"},
        "x_labels": {"10%": "热数据\n10%" if lang == "zh" else "hot data\n10%",
                     "50%": "热数据\n50%" if lang == "zh" else "hot data\n50%"},
        "text": {"zh": {"title": title_zh, "y": y_zh, "x": "热数据占比"},
                 "en": {"title": title_en, "y": y_en, "x": "hot-data share"}},
    }, data)


def fig_multiconsumer_hit(lang: str) -> dict:
    return _multi(lang, "hit_rate",
                  "两个训练任务共享缓存时的命中率",
                  "Cache hit rate when two training jobs share a cache",
                  "本地缓存命中率", "local cache hit rate", "multiconsumer-hit")


def fig_multiconsumer_makespan(lang: str) -> dict:
    return _multi(lang, "total_makespan",
                  "两个训练任务共享缓存时的端到端耗时",
                  "End-to-end run time when two training jobs share a cache",
                  "端到端耗时（s）", "end-to-end run time (s)",
                  "multiconsumer-makespan")


E2E_LABELS = {
    "zh": {"checkpoint-resume": "检查点续跑",
           "checkpoint-version": "换数据集后续跑",
           "dual-restart": "训练与 agentd 同时重启"},
    "en": {"checkpoint-resume": "resume from checkpoint",
           "checkpoint-version": "resume after dataset switch",
           "dual-restart": "training and agentd restart together"},
}


def _e2e(lang: str, metric: str, title_zh: str, title_en: str,
         y_zh: str, y_en: str, name: str) -> dict:
    rows = family_rows("e2e")
    scenarios = []
    for row in rows:
        s = pf._e2e_scenario(row)
        if s != "?" and s not in scenarios:
            scenarios.append(s)
    data = pf._collect(rows, metric, lambda r: "", pf._e2e_scenario, lambda r: "all")
    return build_hbar(name, lang, {
        "x_order": scenarios, "series_order": ["all"],
        "colors": {"all": "#2f6db5"},
        "x_labels": {s: E2E_LABELS[lang].get(s, s) for s in scenarios},
        "text": {"zh": {"title": title_zh, "y": y_zh, "x": "恢复场景"},
                 "en": {"title": title_en, "y": y_en, "x": "recovery scenario"}},
    }, _drop_facet(data))


def fig_e2e_makespan(lang: str) -> dict:
    return _e2e(lang, "total_makespan",
                "三个故障恢复场景的端到端耗时",
                "End-to-end run time of three recovery scenarios",
                "端到端耗时（s）", "end-to-end run time (s)", "e2e-makespan")


def fig_e2e_recovery(lang: str) -> dict:
    return _e2e(lang, "e2e_recovery_time_s",
                "故障中断到恢复训练的时长",
                "Time from interruption to resumed training",
                "恢复时长（s）", "time to resume (s)", "e2e-recovery")


RANGEPAR_TIER = {
    "zh": {"l0": "不限速", "l1": "单连接限速 32 Mbit/s",
           "l2": "单连接限速 16 Mbit/s", "l3": "RTT 200 ms、丢包 1%"},
    "en": {"l0": "no per-connection cap", "l1": "32 Mbit/s per connection",
           "l2": "16 Mbit/s per connection", "l3": "200 ms RTT, 1% loss"},
}
RANGEPAR_ARM = {"zh": {"par-off": "单连接", "par-on": "4 路并行"},
                "en": {"par-off": "single connection", "par-on": "4 parallel connections"}}


def _rangepar_buckets() -> dict:
    rows = family_rows("rangepar")
    buckets: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        tier, arm = pf._rangepar_parts(row)
        value = pf._num(row.get("total_makespan"))
        if tier == "?" or value is None:
            continue
        buckets.setdefault((tier, arm), []).append(value)
    return buckets


def fig_rangepar_makespan(lang: str) -> dict:
    buckets = _rangepar_buckets()
    tiers = sorted({tier for tier, _ in buckets})
    data = {}
    for tier in tiers:
        for arm in ("par-off", "par-on"):
            values = buckets.get((tier, arm))
            if not values:
                continue
            mean = sum(values) / len(values)
            low, high = (stats.bootstrap_ci(values, seed=7)
                         if len(values) > 1 else (mean, mean))
            data[("", tier, arm)] = (mean, low, high, len(values))
    return build("rangepar-makespan", lang, {
        "x_order": tiers, "series_order": ["par-off", "par-on"], "legend": True,
        "colors": {"par-off": "#9aa0a6", "par-on": "#2f6db5"},
        "series_labels": RANGEPAR_ARM[lang],
        "x_labels": RANGEPAR_TIER[lang],
        "text": {"zh": {"title": "不同链路条件下单连接与并行下载的端到端耗时",
                        "y": "端到端耗时（s）", "x": "链路条件"},
                 "en": {"title": "End-to-end run time: single vs parallel downloads by network condition",
                        "y": "end-to-end run time (s)", "x": "network condition"}},
    }, data)


def fig_rangepar_speedup(lang: str) -> dict:
    buckets = _rangepar_buckets()
    tiers = sorted({tier for tier, _ in buckets})
    data = {}
    for tier in tiers:
        on, off = buckets.get((tier, "par-on")), buckets.get((tier, "par-off"))
        if not on or not off:
            continue
        on_med, off_med = stats.median(on), stats.median(off)
        if not on_med:
            continue
        ratio = off_med / on_med
        data[(tier, "all")] = (ratio, ratio, ratio, min(len(on), len(off)))
    present = [t for t in tiers if (t, "all") in data]
    return build_hbar("rangepar-speedup", lang, {
        "x_order": present, "series_order": ["all"],
        "colors": {"all": "#2f6db5"}, "baseline": 1.0,
        "x_labels": RANGEPAR_TIER[lang],
        "text": {"zh": {"title": "并行下载的耗时比（单连接 ÷ 4 路并行，大于 1 表示并行更快）",
                        "y": "耗时比", "x": "链路条件"},
                 "en": {"title": "Run-time ratio of single versus parallel download (above 1 means faster)",
                        "y": "run-time ratio", "x": "network condition"}},
    }, data)


FIG_NOTES = {
    "compare-ratio": {
        "zh": "横轴是数据集字节与单节点缓存容量之比 ρ。ρ≤1 时缓存装得下工作集，"
              "TrainCache 在 100 Mbit/s 下把直读的 1083 s 压到 547 s；"
              "300 Mbit/s 已预热组比 Fluid 已预热少 55 s。ρ>1 后命中率降到 0.32 以下，"
              "300 Mbit/s、ρ=8 的未预热组比直读多 35%。"
              "直读只测 ρ=4，虚线按不随 ρ 变化处理。",
        "en": "The x-axis is the ratio of dataset bytes to per-node cache size. "
              "At rho<=1 the working set fits: TrainCache cuts 1083 s of origin reads "
              "to 547 s at 100 Mbit/s, and the prewarmed 300 Mbit/s case is 55 s faster than prewarmed Fluid. "
              "Above rho=1 the hit rate falls below 0.32, and the cold 300 Mbit/s, rho=8 case "
              "is 35% slower than reading the origin. Origin was measured only at rho=4; "
              "the dashed reference is held constant across rho."},
    "compare-rho1-rho4": {
        "zh": "同一带宽下比较缓存足够（ρ=1）与缓存不足（ρ=4）。ρ=1 是优势区："
              "100 Mbit/s 比直读快 49%，300 Mbit/s 已预热比 Fluid 已预热快 18%。"
              "ρ=4 是退化边界：300 Mbit/s 未预热比 Fluid 未预热慢 9%，100 Mbit/s 比直读慢 5%。"
              "100 Mbit/s 分面没有 Fluid 与 JuiceFS 读数。",
        "en": "Cache fitting (rho=1) and cache short (rho=4) at the same bandwidth. "
              "rho=1 is the advantage zone: 49% faster than origin at 100 Mbit/s, "
              "and 18% faster than prewarmed Fluid at 300 Mbit/s. "
              "rho=4 is the degradation boundary: 9% slower than cold Fluid at 300 Mbit/s, "
              "and 5% slower than origin at 100 Mbit/s. "
              "The 100 Mbit/s facet has no Fluid or JuiceFS runs."},
    "compare-rho": {
        "zh": "TrainCache 自身随缓存容量的变化。100 Mbit/s 在 ρ≤1 时约 547 s，"
              "ρ=2 起升到 873–1138 s（ρ=8 回落到 1062 s）；300 Mbit/s 在 ρ≤1 时为 244–330 s，"
              "ρ≥2 升到 325–503 s；1000 Mbit/s 全程 263–273 s，stall 接近 0，"
              "瓶颈在计算不在数据路径。虚线是该带宽下直读源站的耗时。",
        "en": "TrainCache alone as cache size changes. At 100 Mbit/s, rho<=1 takes about 547 s, "
              "while rho>=2 rises to 873-1138 s (1062 s at rho=8). "
              "At 300 Mbit/s rho<=1 spans 244-330 s and rho>=2 rises to 325-503 s. "
              "At 1000 Mbit/s the whole sweep stays at 263-273 s with stall near zero: "
              "the bottleneck is compute, not the data path. "
              "The dashed line is the origin-only run time at that bandwidth."},
    "gateway-makespan": {
        "zh": "网关把重复的块请求合并成一次回源。直读 1449 s，仅本地缓存 1321 s，"
              "本地缓存加网关缓存 330 s。",
        "en": "The gateway merges duplicate block requests into one origin fetch. "
              "Origin only takes 1449 s, local cache only 1321 s, "
              "and local cache plus gateway cache 330 s."},
    "gateway-source-bytes": {
        "zh": "源站出流量从 50.9 GB 降到 4.3 GB，降幅 92%。",
        "en": "Origin egress falls from 50.9 GB to 4.3 GB, a 92% reduction."},
    "evict-makespan": {
        "zh": "ρ=1 三种策略 76–79 s，差异在噪声内；ρ=8 热度优先 132 s、LRU 143 s、LFU 148 s。"
              "缓存不足时策略差异才显现。",
        "en": "At rho=1 the three policies take 76-79 s, within noise. "
              "At rho=8 heat priority takes 132 s, LRU 143 s and LFU 148 s. "
              "Policy differences only appear when the cache is short."},
    "evict-hot-evictions": {
        "zh": "ρ=1 三种策略都不驱逐热数据；ρ=8 热度优先 107 次、LFU 72 次、LRU 95 次。"
              "驱逐次数与端到端耗时不是单调关系，热度优先驱逐最多但耗时最低，"
              "该指标不能单独作为策略优劣判据。",
        "en": "At rho=1 no policy evicts hot data. At rho=8 heat priority evicts 107 times, "
              "LFU 72 and LRU 95. Eviction count is not monotonic with run time: "
              "heat priority evicts the most yet finishes fastest, "
              "so this metric cannot stand alone as a policy criterion."},
    "evict-mix-makespan": {
        "zh": "混合访问下 ρ=2 三种策略 69–79 s，ρ=8 109–116 s，"
              "策略间差距小于单一访问模式。",
        "en": "Under mixed access the three policies take 69-79 s at rho=2 and 109-116 s at rho=8. "
              "The spread is smaller than under a single access pattern."},
    "evict-mix-hot-evictions": {
        "zh": "ρ=8 时热度优先驱逐热数据 107 次、LFU 72 次、LRU 95 次；"
              "ρ≤4 三种策略基本不驱逐。",
        "en": "At rho=8 heat priority evicts hot data 107 times, LFU 72 and LRU 95. "
              "At rho<=4 none of the policies evicts hot data to speak of."},
    "evict-mix-survive": {
        "zh": "纵轴是窗口结束时仍在缓存中的比例。两类数据都低于 0.12，ρ≥4 接近 0。"
              "三种策略在这条指标上分不出差异，判断策略应回到端到端耗时与热数据驱逐次数。",
        "en": "The y-axis is the share still resident when the window closes. "
              "Both data types stay below 0.12, and from rho=4 they approach zero. "
              "The three policies are indistinguishable here; "
              "policy choice should rest on run time and hot-data evictions."},
    "evict-mix-rounds": {
        "zh": "正常路径下三种策略都没有触发强制清理，图中各点均为 0。"
              "该图用于确认淘汰过程没有走到兜底路径。",
        "en": "No policy triggers forced cleanup on the normal path; every point is zero. "
              "The figure confirms eviction never falls back to the cleanup path."},
    "prefetch-lookahead": {
        "zh": "lookahead 8 最优，ρ=8 时 273 s；lookahead 32 最差，337–359 s。"
              "副本数从 1 增到 3 没有收益，ρ=8 时副本数 3 反而更慢（323 s 对 294 s）。",
        "en": "Lookahead 8 is best: 273 s at rho=8. Lookahead 32 is worst: 337-359 s. "
              "Raising the replica count from 1 to 3 brings no gain; "
              "at rho=8, three replicas are slower (323 s versus 294 s)."},
    "warmup-startup": {
        "zh": "ρ=1、等待上限 60 s 时门控把启动耗时从 32.8 s 降到 3.4 s；"
              "上限 10 s 只降到 29.3 s，窗口太短。"
              "ρ=8 时门控比立即放行慢 0.3–2.0 s，缓存不足时门控是净开销。",
        "en": "At rho=1 with a 60 s wait limit, gating cuts startup from 32.8 s to 3.4 s. "
              "A 10 s limit only reaches 29.3 s because the window is too short. "
              "At rho=8 gating is 0.3-2.0 s slower than immediate release: "
              "with a short cache, gating is pure overhead."},
    "switch-recovery": {
        "zh": "ρ=1 四种顺序变化都在 2.3 s；ρ=8 升到 6.5–11.4 s，课程式快速收窄最慢（11.4 s）。"
              "缓存不足时切换代价被放大。",
        "en": "At rho=1 all four order changes settle in 2.3 s. "
              "At rho=8 they rise to 6.5-11.4 s, with the steep curriculum slowest at 11.4 s. "
              "A short cache amplifies the cost of switching."},
    "multiconsumer-hit": {
        "zh": "两个训练任务共享缓存时命中率 0.39–0.42。同节点与跨节点部署相差 0.01–0.03，"
              "共享缓存没有带来明显命中优势。",
        "en": "When two jobs share a cache the hit rate is 0.39-0.42. "
              "Same-node and cross-node placement differ by 0.01-0.03, "
              "so sharing a cache brings no clear hit-rate advantage."},
    "multiconsumer-makespan": {
        "zh": "同节点部署 408–412 s，跨节点 413–418 s，差 1–3%。",
        "en": "Same-node placement takes 408-412 s, cross-node 413-418 s, a 1-3% difference."},
    "e2e-makespan": {
        "zh": "三个故障恢复场景的全程耗时均值 271 s，每格 2 轮。",
        "en": "The three recovery scenarios average 271 s end to end, with two rounds per cell."},
    "e2e-recovery": {
        "zh": "中断到恢复训练平均 17.5 s；检查点续跑 13.1–13.2 s，"
              "训练与 agentd 同时重启 22.0–24.9 s。恢复窗口内读错误为 0。",
        "en": "Interruption to resumed training averages 17.5 s: 13.1-13.2 s for checkpoint resume "
              "and 22.0-24.9 s when training and agentd restart together. "
              "Read errors in the recovery window are zero."},
    "rangepar-makespan": {
        "zh": "不限速 92.3 对 92.4 s，并行没有差异；单连接限速 32 Mbit/s 从 286 s 降到 107 s；"
              "16 Mbit/s 从 564 s 降到 131 s；RTT 200 ms、丢包 1% 从 1135 s 降到 289 s。",
        "en": "Without a per-connection cap, 92.3 s versus 92.4 s: no difference. "
              "Under a 32 Mbit/s per-connection cap, 286 s falls to 107 s. "
              "At 16 Mbit/s, 564 s falls to 131 s. "
              "With 200 ms RTT and 1% loss, 1135 s falls to 289 s."},
    "rangepar-speedup": {
        "zh": "耗时比 2.7×、4.3×、3.9×，不限速为 1.0。"
              "收益来自多连接分摊单连接限速与长 RTT。",
        "en": "Run-time ratios are 2.7x, 4.3x and 3.9x; uncapped is 1.0. "
              "The gain comes from spreading a per-connection cap and long RTT over four connections."},
}


FIGS = [
    fig_compare_ratio, fig_compare_groups, fig_compare_rho,
    fig_gateway_makespan, fig_gateway_bytes,
    fig_evict_makespan, fig_evict_hot,
    fig_evict_mix_makespan, fig_evict_mix_hot,
    fig_evict_mix_survive, fig_evict_mix_rounds,
    fig_prefetch, fig_warmup, fig_switch,
    fig_multiconsumer_hit, fig_multiconsumer_makespan,
    fig_e2e_makespan, fig_e2e_recovery,
    fig_rangepar_makespan, fig_rangepar_speedup,
]


# ---- 渲染与页面 ----------------------------------------------------------

def render(option: dict, fname: str, lang: str) -> Path | None:
    OUT.mkdir(parents=True, exist_ok=True)
    opt_path = OUT / f".{fname}.{lang}.json"
    svg_path = OUT / f"{fname}.{lang}.svg"
    png_path = OUT / f"{fname}.{lang}.png"
    opt_path.write_text(json.dumps(option, ensure_ascii=False), encoding="utf-8")
    result = subprocess.run(["node", str(RENDER_JS), str(opt_path), str(svg_path)],
                            capture_output=True, text=True)
    opt_path.unlink(missing_ok=True)
    if result.returncode != 0:
        print(f"[fail] {fname}.{lang}: {result.stderr.strip()[:300]}", file=sys.stderr)
        return None
    import cairosvg
    cairosvg.svg2png(url=str(svg_path), write_to=str(png_path), scale=2)
    return png_path


def write_page(items: list[tuple[str, dict]], lang: str) -> Path:
    """交互页：只放图，标题已在图内，页面不再重复。"""
    OUT.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ECHARTS_JS, OUT / "echarts.min.js")
    shutil.copyfile(ERRORBAR_JS, OUT / "errorbar.js")
    clean = [{k: v for k, v in option.items() if not k.startswith("__")}
             for _, option in items]
    blocks = "".join(
        f'\n<section>\n  <div class="chart" id="chart{i}" '
        f'style="height:{option.get("__height", 470)}px"></div>\n'
        f'  <p class="note">'
        f'{escape((FIG_NOTES.get(name) or dict()).get(lang, ""))}</p>\n</section>'
        for i, (name, option) in enumerate(items))
    title = PAGE_TITLE[lang]
    html_lang = "zh-CN" if lang == "zh" else "en"
    other = "index-zh.html" if lang == "en" else "index.html"
    other_lang = "zh-CN" if lang == "en" else "en"
    switch_label = "简体中文" if lang == "en" else "English"
    page_html = f"""<!doctype html>
<html lang="{html_lang}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<script src="echarts.min.js"></script>
<script src="errorbar.js"></script>
<style>
  *{{box-sizing:border-box}}
  body{{margin:0;padding:40px 48px 72px;background:#f6f7f9;color:{INK};
       font-family:"{FONT}","Inter",system-ui,sans-serif}}
  header{{max-width:1480px;margin:0 auto 28px;display:flex;
          justify-content:space-between;align-items:baseline;gap:24px}}
  h1{{font-size:22px;font-weight:700;margin:0;letter-spacing:-0.2px}}
  .lang{{font-size:13px;font-weight:600;color:#2f6db5;text-decoration:none}}
  .lang:hover{{text-decoration:underline}}
  section{{max-width:1480px;margin:0 auto 34px;background:#fff;
           border:1px solid #e6e9ee;border-radius:10px;padding:22px 24px 16px}}
  .chart{{width:100%;height:460px}}
  .note{{margin:16px 2px 2px;font-size:13px;line-height:1.75;color:{MUTED};
         max-width:1100px}}
</style>
</head>
<body>
<header><h1>{title}</h1><a class="lang" href="{other}" hreflang="{other_lang}">{switch_label}</a></header>
{blocks}
<script>
const OPTIONS = {json.dumps(clean, ensure_ascii=False)};
const charts = OPTIONS.map((option, index) => {{
  addErrorBars(option);
  addDumbbells(option);
  const chart = echarts.init(document.getElementById("chart" + index), null,
                             {{renderer: "canvas"}});
  chart.setOption(option);
  return chart;
}});
window.addEventListener("resize", () => charts.forEach(c => c.resize()));
</script>
</body>
</html>"""
    page = OUT / ("index.html" if lang == "en" else "index-zh.html")
    page.write_text(page_html, encoding="utf-8")
    return page


def main() -> int:
    langs = [a for a in sys.argv[1:] if a in ("zh", "en")] or ["zh", "en"]
    items: dict[str, list[tuple[str, dict]]] = {lang: [] for lang in langs}
    for build_figure in FIGS:
        for lang in langs:
            option = build_figure(lang)
            name = option["__name"]
            path = render(option, name, lang)
            print(f"{name}.{lang}: {path}")
            items[lang].append((name, option))
    for lang in langs:
        print(f"page[{lang}]: {write_page(items[lang], lang)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())