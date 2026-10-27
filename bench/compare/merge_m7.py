#!/usr/bin/env python3
"""把 m7 各档的主 summary.csv 与补跑子目录合并成 summary.all.csv。

run_m7.sh 每格只写主 summary.csv；补跑的组落在 <格>/_topup*/、_v010r2/、
_rerun*/ 下。补跑行的 rep 号与主表重叠(都是 0..n-1)，直接拼接会造成
"同组同 rep 两条"，所以这里按 run 重新编号：主表行在前，子目录行按路径
排序追加，同组内 rep 从 0 连续编号。

去重按整行哈希：_v010r2 里的 traincache 行与主表是同一批产物(数值全同)，
只保留一次。故障批 _device-defect-* 与冒烟 _smoke-* 不进合并。

输出不覆盖任何现有文件：目标已存在时直接报错，需显式 --force。

用法:
  python3 bench/compare/merge_m7.py            # 五档全部
  python3 bench/compare/merge_m7.py --cells S-100 M-1000
"""
import argparse
import csv
import hashlib
import sys
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BENCH_ROOT))
import config as bench_config  # noqa: E402

M7_ROOT = bench_config.RESULTS_DIR / "m7"
CELLS = ["S-100", "S-300", "S-1000", "M-1000", "L-1000"]
# 补跑目录;其余子目录(故障批、冒烟)不参与合并
TOUP_DIR_GLOBS = ["_topup", "_topup2", "_topup3", "_v010", "_v010r2",
                  "_rerun*"]
EXCLUDE_DIR_MARKERS = ("_device-defect", "_smoke")


def _row_key(row: dict) -> str:
    """整行指纹,用于去掉主表与补跑目录的重复产物。"""
    cols = ["group", "rate", "startup_makespan", "total_makespan",
            "trace_sha256", "warmup_fanout", "startup_warmup_bytes",
            "sustained_prefetch_bytes"]
    return hashlib.sha256(
        "|".join(str(row.get(c, "")) for c in cols).encode()).hexdigest()


def _read_rows(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _extra_summaries(cell_dir: Path) -> list[Path]:
    out = []
    for pattern in TOUP_DIR_GLOBS:
        for p in sorted(cell_dir.glob(pattern)):
            if not p.is_dir():
                continue
            if any(m in p.name for m in EXCLUDE_DIR_MARKERS):
                continue
            out.extend(sorted(p.rglob("summary.csv")))
    return out


def merge_cell(cell_dir: Path) -> tuple[int, int, Path]:
    """返回 (主表行数, 合并后行数, 输出路径)。"""
    main_csv = cell_dir / "summary.csv"
    if not main_csv.is_file():
        raise SystemExit(f"缺少主表: {main_csv}")

    rows, seen = [], set()
    for r in _read_rows(main_csv):
        if str(r.get("void", "")).lower() == "true":
            continue
        key = _row_key(r)
        if key in seen:
            continue
        seen.add(key)
        rows.append(r)
    n_main = len(rows)

    for p in _extra_summaries(cell_dir):
        is_rerun = any("_rerun" in part for part in p.parent.parts)
        for r in _read_rows(p):
            if str(r.get("void", "")).lower() == "true":
                continue
            key = _row_key(r)
            if key in seen:
                continue
            seen.add(key)
            r["_origin"] = str(p.parent.relative_to(cell_dir))
            if is_rerun:
                # _rerun* 是替代:同组同 rep 的旧行作废,换成重跑结果
                old = next((x for x in rows
                            if x.get("group") == r.get("group")
                            and x.get("rep") == r.get("rep")), None)
                if old is not None:
                    rows[rows.index(old)] = r
                    continue
            rows.append(r)

    # 同组内按实际 run 数重新编号 rep
    counter: dict[str, int] = {}
    for r in rows:
        g = r.get("group", "")
        r["rep"] = str(counter.get(g, 0))
        counter[g] = counter.get(g, 0) + 1
        r["run_id"] = f"{g}-r{int(r['rep']):02d}"

    fields = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    out = cell_dir / "summary.all.csv"
    return n_main, len(rows), out, fields, rows


def main():
    ap = argparse.ArgumentParser(description="合并 m7 主表与补跑产物")
    ap.add_argument("--cells", nargs="+", default=CELLS, choices=CELLS)
    ap.add_argument("--force", action="store_true", help="允许覆盖已有 summary.all.csv")
    args = ap.parse_args()

    for name in args.cells:
        cell_dir = M7_ROOT / name
        n_main, n_all, out, fields, rows = merge_cell(cell_dir)
        if out.exists() and not args.force:
            print(f"[skip] {out} 已存在(加 --force 覆盖)")
            continue
        with out.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for r in rows:
                writer.writerow({k: r.get(k, "") for k in fields})
        print(f"[{name}] 主表 {n_main} 行 → 合并 {n_all} 行: {out}")


if __name__ == "__main__":
    main()
