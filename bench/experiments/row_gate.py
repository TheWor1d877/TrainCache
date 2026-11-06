#!/usr/bin/env python3
"""读侧收数的三道过滤(4.3 §3.1、4.2 §12、4.2 §11)。

汇总与出表脚本从数据层取 `summary.csv` 时都过这三道,按顺序取
第一条命中的拒因:

  1. 点目录(装 `summary.csv` 的那个目录)的 `version` 戳等于 `docs/4.5`
     最后一行的当前版本;
  2. `void` 为假;倾斜两族的行还要三列淘汰证据(`evict_blocks_run`、
     `b_evictions_of_hot`、`cache_dir_peak_bytes`)齐且非空(4.3 §2.6);
  3. 倾斜两族的行满足字节地板 `total_makespan ≥ fetch_bytes_run ÷ 该档
     实测有效带宽`(带宽表在 `bench/config.py`,与 4.2 §12 同源)。

只筛 TrainCache 自己的行:`baselines/` 里的 Fluid/JuiceFS/direct 行不带版本戳,
也不产淘汰证据,属既有对照数据。地板同理只对倾斜两族成立——其余族的预热与
计算重叠,`fetch_bytes_run ÷ total_makespan` 普遍高于整形线速(4.2 §12 的
12.5/37.5/125 MiB/s 就是这种口径),拿它当上界会误杀。

被排除的行既打印也给一份 `results/summary/rejected.csv`,让「谁被排除」入账。

用法:
  from row_gate import current_version, read_gated
  rows, rejected = read_gated(summary_path, current_version())
"""
import argparse
import csv
import sys
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BENCH_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as bench_config  # noqa: E402

EVIDENCE_COLUMNS = ("evict_blocks_run", "b_evictions_of_hot",
                    "cache_dir_peak_bytes")
REJECT_FIELDS = ("source", "cond", "group", "rate", "total_makespan",
                 "fetch_bytes_run", "reason")


VERSION_LOG = bench_config.REPO_ROOT / "docs" / "4.5-experiment-version-log.md"


def current_version() -> str | None:
    """`docs/4.5` 最后一行的版本号;表里还没有数据行时给 None。"""
    if not VERSION_LOG.is_file():
        return None
    rows = [ln for ln in VERSION_LOG.read_text(encoding="utf-8").splitlines()
            if ln.strip().startswith("|") and "---" not in ln]
    if len(rows) < 2:
        return None
    cells = [c.strip() for c in rows[-1].strip().strip("|").split("|")]
    return cells[0] or None


def stamp_of(point_dir: Path) -> str | None:
    path = point_dir / "version"
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8").strip() or None


def _is_tilt(row: dict) -> bool:
    """按 load 字段判倾斜负载；没有该列的行不按倾斜处理。"""
    return str(row.get("load", "")).strip() == "tilt"


def _first_bad(values) -> str:
    return next((v for v in values if v not in ("", None)), "")


def reject_reason(row: dict, stamp: str | None, version: str | None) -> str:
    """命中的第一条拒因;通过给空串。"""
    if not str(row.get("group", "")).startswith("traincache"):
        return ""
    if str(row.get("void", "")).strip().lower() in ("true", "1"):
        return "void"
    if version is None:
        return "no_registry_version"
    if stamp is None:
        return "no_version_stamp"
    if stamp != version:
        return f"version_stale({stamp})"
    if _is_tilt(row):
        if any(c not in row for c in EVIDENCE_COLUMNS):
            return "missing_eviction_evidence"
        if _first_bad([row.get(c) for c in EVIDENCE_COLUMNS]) == "":
            return "empty_eviction_evidence"
        floor = bench_config.tilt_floor_seconds(row)
        try:
            makespan = float(row.get("total_makespan") or "")
        except ValueError:
            makespan = None
        if floor is not None and makespan is not None and makespan < floor:
            return f"below_byte_floor(need>={floor:.0f}s,got={makespan:.0f}s)"
    return ""


def gate_rows(rows: list[dict], point_dir: Path,
              version: str | None) -> tuple[list[dict], list[dict]]:
    """(通过的行, 被排除的行);每条被排除的行带一个 `reason` 字段。"""
    stamp = stamp_of(point_dir)
    kept, rejected = [], []
    for row in rows:
        reason = reject_reason(row, stamp, version)
        if reason:
            rejected.append({
                "source": str(point_dir),
                "cond": row.get("cond", ""),
                "group": row.get("group", ""),
                "rate": row.get("rate", ""),
                "total_makespan": row.get("total_makespan", ""),
                "fetch_bytes_run": row.get("fetch_bytes_run", ""),
                "reason": reason,
            })
        else:
            kept.append(row)
    return kept, rejected


def read_gated(summary_path: Path, version: str | None,
               rows: list[dict] | None = None
               ) -> tuple[list[dict], list[dict]]:
    """读一份 summary.csv 并过滤;`rows` 给了就不碰磁盘(自检用)。"""
    if rows is None:
        with Path(summary_path).open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    return gate_rows(rows, Path(summary_path).parent, version)


def print_rejects(rejected: list[dict], root: Path | None = None) -> None:
    """按来源目录打被拒行数与拒因分布。"""
    if not rejected:
        return
    by_source: dict[str, dict[str, int]] = {}
    for r in rejected:
        counts = by_source.setdefault(r["source"], {})
        reason = r["reason"].split("(")[0]
        counts[reason] = counts.get(reason, 0) + 1
    for src, counts in sorted(by_source.items()):
        path = Path(src)
        rel = f"{path.relative_to(root)}" if root else str(path)
        detail = ", ".join(f"{k}×{v}" for k, v in sorted(counts.items()))
        print(f"[reject] {rel}: 排除 {sum(counts.values())} 条（{detail}）")


def write_reject_log(out_csv: Path, rejected: list[dict]) -> None:
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REJECT_FIELDS)
        writer.writeheader()
        writer.writerows(rejected)


def selftest() -> None:
    """混装 2026-11-01 缺陷期行与一条合格行的定点断言。"""
    import tempfile

    version = "v-selftest"

    def gib(x):
        return x * (1 << 30)

    def tilt_row(cond, makespan, fetch_bytes, **kw):
        row = {"cond": cond, "group": "traincache-cold", "rate": "100mbit",
               "load": "tilt",
               "void": "False", "total_makespan": f"{makespan:.2f}",
               "fetch_bytes_run": f"{fetch_bytes:.1f}",
               "evict_blocks_run": "128", "b_evictions_of_hot": "0",
               "cache_dir_peak_bytes": "2731459712"}
        row.update(kw)
        return row

    # 交接 §4A 表里那 9 条:makespan 全部低于自己的字节地板。
    legacy_spec = [("policy-heat", 818.7, 13.39), ("policy-lru", 824.4, 13.15),
                   ("policy-lfu", 825.1, 12.97),
                   ("pareto2-lookahead8-heat", 809.8, 12.80),
                   ("pareto2-lookahead8-heat", 831.5, 12.63),
                   ("pareto2-lookahead16-heat", 740.0, 53.96),
                   ("pareto2-lookahead16-heat", 745.7, 64.47),
                   ("pareto2-lookahead32-heat", 752.8, 216.93),
                   ("pareto2-lookahead32-heat", 745.9, 176.05)]

    with tempfile.TemporaryDirectory() as td:
        point = Path(td) / "policy-heat" / "policy-heat-r01-x"
        point.mkdir(parents=True)
        (point / "version").write_text(version + "\n", encoding="utf-8")

        # 11-01 的产物没有三列淘汰证据,第一道拒因就是缺证据列
        legacy = [{"cond": c, "group": "traincache-cold", "rate": "100mbit",
                   "load": "tilt",
                   "void": "False", "total_makespan": f"{m:.2f}",
                   "fetch_bytes_run": f"{gib(b):.1f}"}
                  for c, m, b in legacy_spec]
        good = tilt_row("policy-heat", gib(25.2) / (11.95 * 1e6) * 1.02,
                        gib(25.2))
        kept, rejected = gate_rows(legacy + [good], point, version)
        assert len(rejected) == 9, f"缺陷期 9 条要全被排除,实际 {len(rejected)}"
        assert kept == [good], "合格行被误杀"
        assert {r["reason"] for r in rejected} == {"missing_eviction_evidence"}, \
            [r["reason"] for r in rejected]

        # 补上三列后地板接管:同样 9 条改由字节地板拒掉
        kept, rejected = gate_rows(
            [{**row, "evict_blocks_run": "0", "b_evictions_of_hot": "70000",
              "cache_dir_peak_bytes": "2935277824"} for row in legacy],
            point, version)
        assert not kept and len(rejected) == 9
        assert {r["reason"].split("(")[0] for r in rejected} == {"below_byte_floor"}, \
            [r["reason"] for r in rejected]

        # 版本戳:旧戳与无戳各自的拒因
        (point / "version").write_text("v-old\n", encoding="utf-8")
        _, rejected = gate_rows([good], point, version)
        assert rejected[0]["reason"] == "version_stale(v-old)", rejected
        (point / "version").write_text(version + "\n", encoding="utf-8")
        _, rejected = gate_rows([good], Path(td), version)
        assert rejected[0]["reason"] == "no_version_stamp", rejected

        # void 先于版本判定;对照行(baselines)一律不筛
        _, rejected = gate_rows([tilt_row("policy-heat", 9e9, gib(25.2),
                                          void="True")],
                                Path(td), version)
        assert rejected[0]["reason"] == "void"
        _, rejected = gate_rows([{"cond": "fig2-100-size8-ep1",
                                  "group": "juicefs-cold", "rate": "100mbit"}],
                                Path(td), version)
        assert rejected == []

        # 带宽表里没有的档位给不出地板,行放行而不是误杀
        _, rejected = gate_rows([tilt_row("policy-heat", 108.9, gib(8),
                                          rate="1000mbit")], point, version)
        assert rejected == []

        out = Path(td) / "summary" / "rejected.csv"
        write_reject_log(out, [{"source": str(point), "cond": "policy-heat",
                                "group": "traincache-cold", "rate": "100mbit",
                                "total_makespan": "818",
                                "fetch_bytes_run": "13390000000",
                                "reason": "missing_eviction_evidence"}])
        with out.open(newline="", encoding="utf-8") as f:
            back = list(csv.DictReader(f))
        assert len(back) == 1 and back[0]["reason"] == "missing_eviction_evidence"
        print_rejects([{"source": str(point), "reason": "void"}])

    assert bench_config.rate_mbit("100mbit") == 100
    assert bench_config.rate_mbit(None) is None
    assert bench_config.tilt_floor_seconds(
        {"cond": "fig1-100-ratio1-fan1-free", "rate": "100mbit",
         "fetch_bytes_run": "201326592"}) is None
    print("row_gate selftest PASS")


def main() -> int:
    ap = argparse.ArgumentParser(description="读侧三道过滤")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
