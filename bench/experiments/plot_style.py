#!/usr/bin/env python3
"""图样式共享模块：配色、字号、字体栈、边框与网格。

所有图脚本从这里取样式，不再各自抄一份 rcParams。用法：

    from plot_style import INK, MUTED, GRID, SAVE_DPI, setup
    plt = setup("zh")          # 或 "en"

中文字体缺 Noto Sans CJK SC 时 setup() 直接报错退出，不静默回退到无中文字形
的字体：缺字形会把汉字画成方框，图看着正常、字全错。
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402  (后端必须在 pyplot 之前设)

INK = "#1a1a1a"
MUTED = "#6b7280"
GRID = "#ececec"
SAVE_DPI = 200

# 标题与轴标签用真实 Bold 字面：Medium 家族里没有 bold，设 weight=bold
# 会被回退成更细的 Regular，200 dpi 下笔画发虚。
BOLD_FAMILY = "Noto Sans CJK SC"
ZH_SANS = ["Noto Sans CJK SC Medium", "Noto Sans CJK SC",
           "Noto Sans CJK JP", "DejaVu Sans"]
EN_SANS = ["Inter", "DejaVu Sans"]

_BASE = {
    "axes.unicode_minus": False,
    "font.size": 10.5,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "xtick.labelsize": 9.5,
    "ytick.labelsize": 9.5,
    "legend.fontsize": 9.5,
    "axes.edgecolor": "#c7c9cc",
    "axes.linewidth": 0.7,
    "axes.labelcolor": "#374151",
    "axes.titlelocation": "left",
    "axes.titleweight": "normal",
    "axes.titlepad": 7,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.axisbelow": True,
    "xtick.color": "#9aa0a6",
    "ytick.color": "#9aa0a6",
    "xtick.labelcolor": "#4b5563",
    "ytick.labelcolor": "#4b5563",
    "xtick.major.size": 3,
    "ytick.major.size": 3,
    "xtick.major.width": 0.7,
    "ytick.major.width": 0.7,
    "grid.color": GRID,
    "grid.linewidth": 0.6,
    "figure.facecolor": "white",
    "savefig.facecolor": "white",
}


def setup(lang: str = "zh"):
    """按语言设字体栈并写入全局 rcParams，返回 pyplot。"""
    if lang == "en":
        family, bold = EN_SANS, "DejaVu Sans"
    else:
        family, bold = ZH_SANS, "Noto Sans CJK SC"
    plt.rcParams.update({"font.sans-serif": family, **(_BASE | {"font.family": "sans-serif"})})
    globals()["BOLD_FAMILY"] = bold
    return plt


def bolden(fig) -> None:
    """标题、轴标签、图例改用真实 Bold 字面。"""
    for ax in fig.axes:
        for text in (ax.title, ax._left_title, ax._right_title,
                     ax.xaxis.label, ax.yaxis.label):
            text.set_fontfamily(BOLD_FAMILY)
            text.set_fontweight("bold")
        leg = ax.get_legend()
        if leg is not None:
            for text in leg.get_texts():
                text.set_fontfamily(BOLD_FAMILY)
                text.set_fontweight("bold")
    for leg in fig.legends:
        for text in leg.get_texts():
            text.set_fontfamily(BOLD_FAMILY)
            text.set_fontweight("bold")
    if fig._suptitle is not None:
        fig._suptitle.set_fontfamily(BOLD_FAMILY)
        fig._suptitle.set_fontweight("bold")
