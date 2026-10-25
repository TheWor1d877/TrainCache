#!/usr/bin/env python3
"""对照组统一请求轨迹生成器(M6 任务 6.6)。

同一 track+档位的七个组态(direct / juicefs-cold / juicefs-warm / fluid-cold /
fluid-warm / traincache-cold / traincache-warm)共用一份 trace.json,保证消费顺序
(分片顺序、seed、每步样本数)逐位一致,否则 makespan 与 stall 不可比。

轨迹语义与 bench/scripts/bench_track_d.py 的 epoch_perm()/stage_of() 逐字对齐
(预注册 §4.1,cond.schema.md §2.4):
  * fixed:     所有 epoch 用同一个 seed 生成的分片置换;
  * per-epoch: 每个 epoch 用 seed+epoch 生成置换(默认);
  * curriculum:全局置换 P=Random(seed).permutation(n),stage 取嵌套前缀
               P[:ceil(n*fraction)],stage 内顺序固定;stage 归属按 equal-thirds。

口径(9.1 §8 任务 6.12/6.13,与 run_bench.py 和 D 轨统一,2026-10-24 定稿):
  * 数据量: 2GiB 级、8MiB 块(D20)、steps_per_epoch=400、batch=32;
  * B 轨 MinIO 分片 ~256MiB/份、images_per_tar=1600 → 每 epoch 恰 400×32/1600=8
    次整片读取,即 S 档(n_tars=8)每 epoch 恰好一轮全量消费;
  * 容量档位(D19)只改 n_tars:S=8(2GiB)、M=32(8GiB)、L=128(32GiB),
    缓存侧固定 1GiB;其余参数(seed/batch/steps/images_per_tar)三档一致。

输出 trace.json 结构:
{
  "schema_version": "1",
  "n_tars": <int>,
  "seed": <int>,
  "shuffle": "fixed"|"per-epoch"|"curriculum",
  "epochs": <int>,
  "batch": <int>,
  "steps_per_epoch": <int>,
  "images_per_tar": <int>,
  "curriculum_fractions": [1.0, 0.6667, 0.3333],
  "per_epoch": [
    {"epoch": 0, "stage": 0, "shard_perm": [...], "repeats": <int>},
    ...
  ]
}

用法:
  python3 bench/compare/trace.py --n-tars 8 --seed 20261015 \
      --shuffle per-epoch --epochs 3 --batch 32 --steps-per-epoch 400 \
      --images-per-tar 1600 \
      --out bench/experiments/traces/track-compare-d-s8-per-epoch.json
  # M 档:--n-tars 32;L 档:--n-tars 128(其余参数不变)
  python3 bench/compare/trace.py --selftest

  # 批量生成参考轨迹套件(COND 的 trace.file 指向这些文件):
  python3 bench/compare/trace.py --emit-suite --out-dir bench/experiments/traces
  # --out-dir 缺省即 bench/experiments/traces;每份轨迹的 sha256 由
  # trace_sha256 现场算并逐行打印,COND 里的 trace.sha256 照抄即可。
"""
import argparse
import hashlib
import json
import math
import random
import sys
from pathlib import Path

CURRICULUM_FRACTIONS = (1.0, 2.0 / 3.0, 1.0 / 3.0)
SCHEMA_VERSION = "1"

# ---- 参考轨迹套件(T1)----
# COND 的 trace.file 指向这里生成的文件;每份的 sha256 由 trace_sha256 现场算,
# 不在仓库里硬编码。参数口径:
#   * n_tars 按容量档位(D19):S=8 / M=32 / L=128,分片 256MiB;
#   * seed 20261015(D/B 轨对照,run_bench.py TRACE_SEED)、20261006(C 轨,bench/config.py SEED);
#   * shuffle=per-epoch、batch=32、steps_per_epoch=400、images_per_tar=1600 三档一致;
#   * epochs:对照 S 档 3(B 轨对照 M/L 档 1)、C 轨 3。
TRACE_SUITE = (
    ("track-compare-d-s8-per-epoch.json",
     dict(n_tars=8, seed=20261015, shuffle="per-epoch", epochs=3,
          batch=32, steps_per_epoch=400, images_per_tar=1600)),
    ("track-compare-b-m32-per-epoch.json",
     dict(n_tars=32, seed=20261015, shuffle="per-epoch", epochs=1,
          batch=32, steps_per_epoch=400, images_per_tar=1600)),
    ("track-compare-b-l128-per-epoch.json",
     dict(n_tars=128, seed=20261015, shuffle="per-epoch", epochs=1,
          batch=32, steps_per_epoch=400, images_per_tar=1600)),
    ("track-c-32shard-per-epoch.json",
     dict(n_tars=32, seed=20261006, shuffle="per-epoch", epochs=3,
          batch=32, steps_per_epoch=400, images_per_tar=1600)),
)

# --emit-suite 的默认输出目录:bench/experiments/traces(相对本文件定位)
DEFAULT_SUITE_DIR = Path(__file__).resolve().parents[1] / "experiments" / "traces"


def shard_perm(seed: int, epoch: int, n_tars: int) -> list[int]:
    """分片级置换:父进程 RNG,种子 = seed + epoch(与 bench_track_d.py 同口径)。"""
    perm = list(range(n_tars))
    random.Random(seed + epoch).shuffle(perm)
    return perm


def stage_of(epoch: int, epochs: int) -> int:
    """curriculum stage 归属(E<3 全 stage 0,否则 equal-thirds)。"""
    if epochs < 3:
        return 0
    if epoch < epochs // 3:
        return 0
    if epoch < (2 * epochs) // 3:
        return 1
    return 2


def stage_shards(n_tars: int, stage: int) -> int:
    """stage 分片数:ceil(n_tars * fraction),至少 1。"""
    return max(1, math.ceil(n_tars * CURRICULUM_FRACTIONS[stage]))


def epoch_perm(seed: int, epoch: int, epochs: int, n_tars: int,
               shuffle: str) -> tuple[list[int], int]:
    """该 epoch 的 (分片置换, stage),与 bench_track_d.py 逐字对齐。"""
    if shuffle == "fixed":
        return shard_perm(seed, 0, n_tars), 0
    if shuffle == "per-epoch":
        return shard_perm(seed, epoch, n_tars), 0
    # curriculum
    stage = stage_of(epoch, epochs)
    order = shard_perm(seed, 0, n_tars)
    return order[:stage_shards(n_tars, stage)], stage


def repeat_count(stage_count: int, images_per_tar: int, batch: int,
                 steps_per_epoch: int) -> int:
    """curriculum 窄 stage 循环几遍分片表才能填满固定 epoch 长度。"""
    if stage_count <= 0 or images_per_tar <= 0:
        return 1
    return max(1, math.ceil(steps_per_epoch * batch / (stage_count * images_per_tar)))


def build_trace(n_tars: int, seed: int, shuffle: str, epochs: int,
                batch: int, steps_per_epoch: int,
                images_per_tar: int) -> dict:
    """生成完整 trace dict。"""
    per_epoch = []
    for epoch in range(epochs):
        perm, stage = epoch_perm(seed, epoch, epochs, n_tars, shuffle)
        repeats = repeat_count(len(perm), images_per_tar, batch, steps_per_epoch)
        per_epoch.append({
            "epoch": epoch,
            "stage": stage,
            "shard_perm": perm,
            "repeats": repeats,
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "n_tars": n_tars,
        "seed": seed,
        "shuffle": shuffle,
        "epochs": epochs,
        "batch": batch,
        "steps_per_epoch": steps_per_epoch,
        "images_per_tar": images_per_tar,
        "curriculum_fractions": list(CURRICULUM_FRACTIONS),
        "per_epoch": per_epoch,
    }


def trace_sha256(trace: dict) -> str:
    """对 per_epoch 序列做稳定哈希(供 COND 的 trace.sha256 字段)。"""
    payload = json.dumps(trace["per_epoch"], separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_trace(trace: dict, out_path: Path) -> str:
    """写 trace.json,返回 sha256。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(trace, ensure_ascii=False, indent=2) + "\n")
    return trace_sha256(trace)


def emit_suite(out_dir: Path | None = None) -> list[tuple[str, str]]:
    """批量生成整套参考轨迹,返回 [(文件名, sha256)]。

    轨迹清单见 TRACE_SUITE:对照 S/M/L 三档 + C 轨各一份。
    同一参数生成的文件逐字一致,故 COND 的 trace.sha256 可由本函数复算。
    """
    target_dir = Path(out_dir) if out_dir is not None else DEFAULT_SUITE_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    results: list[tuple[str, str]] = []
    for filename, params in TRACE_SUITE:
        trace = build_trace(**params)
        sha = write_trace(trace, target_dir / filename)
        results.append((filename, sha))
    return results


# ---------------------------------------------------------------------------
# selftest
# ---------------------------------------------------------------------------
def selftest() -> None:
    """自检:三种 shuffle 形态均能生成合法 trace,且关键不变量成立。"""
    # 1. per-epoch:不同 epoch 的置换不同
    t = build_trace(n_tars=16, seed=20261015, shuffle="per-epoch",
                    epochs=3, batch=32, steps_per_epoch=400, images_per_tar=800)
    perms = [e["shard_perm"] for e in t["per_epoch"]]
    assert len(perms) == 3
    assert perms[0] != perms[1] != perms[2], "per-epoch 应每 epoch 不同"
    assert all(sorted(p) == list(range(16)) for p in perms), "置换应覆盖全部分片"
    assert t["batch"] == 32 and t["steps_per_epoch"] == 400

    # 2. fixed:所有 epoch 置换相同
    t2 = build_trace(n_tars=16, seed=20261015, shuffle="fixed",
                     epochs=3, batch=32, steps_per_epoch=400, images_per_tar=800)
    perms2 = [e["shard_perm"] for e in t2["per_epoch"]]
    assert perms2[0] == perms2[1] == perms2[2], "fixed 应所有 epoch 相同"
    assert all(e["stage"] == 0 for e in t2["per_epoch"]), "fixed 的 stage 恒为 0"

    # 3. curriculum:stage 嵌套前缀(stage 1 是 stage 0 的前缀,stage 2 是 stage 1 的前缀)
    t3 = build_trace(n_tars=16, seed=20261015, shuffle="curriculum",
                     epochs=3, batch=32, steps_per_epoch=400, images_per_tar=800)
    stages = [e["stage"] for e in t3["per_epoch"]]
    assert stages == [0, 1, 2], f"E=3 时 stage 应为 [0,1,2],实际 {stages}"
    p0 = t3["per_epoch"][0]["shard_perm"]
    p1 = t3["per_epoch"][1]["shard_perm"]
    p2 = t3["per_epoch"][2]["shard_perm"]
    assert p1 == p0[:len(p1)], "curriculum stage 1 应是 stage 0 的前缀"
    assert p2 == p1[:len(p2)], "curriculum stage 2 应是 stage 1 的前缀"
    assert len(p0) == 16
    assert len(p1) == math.ceil(16 * 2 / 3)
    assert len(p2) == math.ceil(16 * 1 / 3)

    # 4. sha256 稳定:同参数两次生成 hash 一致
    sha_a = trace_sha256(t)
    sha_b = trace_sha256(build_trace(n_tars=16, seed=20261015, shuffle="per-epoch",
                                     epochs=3, batch=32, steps_per_epoch=400,
                                     images_per_tar=800))
    assert sha_a == sha_b, "同参数 hash 应一致"
    assert len(sha_a) == 64 and all(c in "0123456789abcdef" for c in sha_a)

    # 5. curriculum 窄 stage 的 repeats > 1
    assert t3["per_epoch"][2]["repeats"] > 1, "窄 stage repeats 应 > 1"

    # 6. E<3 时 curriculum 全 stage 0
    t4 = build_trace(n_tars=16, seed=1, shuffle="curriculum",
                     epochs=1, batch=32, steps_per_epoch=400, images_per_tar=800)
    assert t4["per_epoch"][0]["stage"] == 0
    assert len(t4["per_epoch"][0]["shard_perm"]) == 16

    # 7. 套件清单自洽:每项参数可生成、hash 稳定且为 64 位小写 hex
    assert len(TRACE_SUITE) == 4, TRACE_SUITE
    for filename, params in TRACE_SUITE:
        t5 = build_trace(**params)
        sha = trace_sha256(t5)
        assert len(sha) == 64 and all(c in "0123456789abcdef" for c in sha), filename
        assert sha == trace_sha256(build_trace(**params)), filename
        assert len(t5["per_epoch"]) == params["epochs"], filename

    print("trace selftest: PASS")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="对照组统一请求轨迹生成器(同一 track+档位的七组态共用一份 trace.json)")
    ap.add_argument("--n-tars", type=int, default=8,
                    help="分片总数(默认 8=S 档 2GiB;M 档 32、L 档 128,6.13/D19)")
    ap.add_argument("--seed", type=int, default=20261015, help="轨迹种子(默认 20261015)")
    ap.add_argument("--shuffle", choices=("fixed", "per-epoch", "curriculum"),
                    default="per-epoch", help="消费顺序形态(默认 per-epoch)")
    ap.add_argument("--epochs", type=int, default=3, help="epoch 数(默认 3)")
    ap.add_argument("--batch", type=int, default=32, help="每步样本数(默认 32)")
    ap.add_argument("--steps-per-epoch", type=int, default=400,
                    help="每 epoch 步数(默认 400,D 轨统一口径 6.12)")
    ap.add_argument("--images-per-tar", type=int, default=1600,
                    help="每 tar 样本数(默认 1600:B 轨分片 256MiB/1600 图,"
                         "400×32/1600=8 恰一轮全量;亦用于 curriculum repeats)")
    ap.add_argument("--out", type=Path, default=None,
                    help="输出 trace.json 路径;缺省不写文件,只打印 sha256")
    ap.add_argument("--emit-suite", action="store_true",
                    help="批量生成整套参考轨迹(见 TRACE_SUITE),打印每份的 sha256")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="--emit-suite 的输出目录(缺省 bench/experiments/traces)")
    ap.add_argument("--selftest", action="store_true", help="运行自检并退出")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return

    if args.emit_suite:
        out_dir = args.out_dir if args.out_dir is not None else DEFAULT_SUITE_DIR
        results = emit_suite(out_dir)
        print(f"参考轨迹套件已写入: {out_dir}")
        for filename, sha in results:
            print(f"  {filename}  sha256={sha}")
        return

    if args.n_tars < 1:
        ap.error("--n-tars 必须 ≥ 1")
    if args.seed < 1:
        ap.error("--seed 必须 ≥ 1")
    if args.epochs < 1:
        ap.error("--epochs 必须 ≥ 1")
    if args.batch < 1:
        ap.error("--batch 必须 ≥ 1")
    if args.steps_per_epoch < 1:
        ap.error("--steps-per-epoch 必须 ≥ 1")
    if args.images_per_tar < 1:
        ap.error("--images-per-tar 必须 ≥ 1")

    trace = build_trace(
        n_tars=args.n_tars, seed=args.seed, shuffle=args.shuffle,
        epochs=args.epochs, batch=args.batch,
        steps_per_epoch=args.steps_per_epoch,
        images_per_tar=args.images_per_tar)
    sha = trace_sha256(trace)

    if args.out is not None:
        actual = write_trace(trace, args.out)
        print(f"trace.json 已写入: {args.out}")
        print(f"sha256: {actual}")
    else:
        print(json.dumps(trace, ensure_ascii=False, indent=2))
        print(f"\nsha256: {sha}", file=sys.stderr)


if __name__ == "__main__":
    main()
