#!/usr/bin/env python3
"""生成网关合并实验的 18 个 COND（3 读法 × 2 带宽 × 开关 + 每档一条带缓存臂）。

实验族 `gateway-merge`，COND 前缀 `gw`。读法：
  r1       单读端，量网关自身开销
  r4repl   4 节点读同一份数据，同 key 并发最密（收益臂）
  r4shard  4 节点各读不相交分片（阴性对照）

每档除 `off`/`on` 两条外，还产出一条带网关磁盘缓存的 COND：`<on 名>-cache`，
即 `gw-<rate>-<short>-on-cache`，environment.gateway=true 且 gateway_cache=true，
其余字段（dataset / trace / reps / digest）与同档 `-on` 臂一致。

网关镜像位必须写真实 digest：本脚本从 kind 节点 containerd 读，读不到就退出，
不写占位符。镜像变了要重新生成，`make -C bench gw-conds` 做这件事。

用法：
  python3 bench/experiments/gen_gw_conds.py --digest <repo@sha256:...>
  python3 bench/experiments/gen_gw_conds.py --from-cluster
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent
BENCH_ROOT = EXPERIMENTS_DIR.parent
REPO_ROOT = BENCH_ROOT.parent
sys.path.insert(0, str(EXPERIMENTS_DIR))
sys.path.insert(0, str(BENCH_ROOT / "compare"))
import cond as cond_mod  # noqa: E402
import trace as trace_mod  # noqa: E402

CONDS_DIR = EXPERIMENTS_DIR / "conds"
TRACES_DIR = EXPERIMENTS_DIR / "traces"
TEMPLATE = CONDS_DIR / "compare-d-traincache-cold-100.json"
GATEWAY_IMAGE = "traincache-gateway:dev"
TRACE_NAME = "track-gw-s8-e1-per-epoch.json"

READERS = {"r1": (1, "single"), "r4repl": (4, "replicated"),
           "r4shard": (4, "sharded")}
RATES = (100, 1000)


def _cluster_digest(image: str) -> str:
    """从 kind worker 的 containerd 读 manifest digest。"""
    nodes = subprocess.run(["kind", "get", "nodes", "--name", "traincache"],
                           capture_output=True, text=True)
    if nodes.returncode != 0:
        raise SystemExit(f"读不到 kind 节点：{nodes.stderr.strip()}")
    last = ""
    for node in nodes.stdout.split():
        p = subprocess.run(["docker", "exec", node, "crictl", "inspecti", image],
                           capture_output=True, text=True)
        if p.returncode != 0:
            continue
        try:
            d = json.loads(p.stdout)
        except ValueError:
            continue
        digs = (d.get("status") or {}).get("repoDigests") or []
        if digs:
            last = digs[0]
            break
    if not last or "@sha256:" not in last:
        raise SystemExit(
            f"节点上没有 {image} 的 manifest digest；先构建并 kind load，"
            f"或用 --digest 显式给出")
    hexpart = last.split("sha256:", 1)[1]
    if len(set(hexpart)) == 1 or hexpart.startswith("deadbeef"):
        raise SystemExit(f"{last} 看起来是占位符摘要")
    return last


def _trace() -> tuple[str, str]:
    """生成 1 epoch 的 S 档轨迹，返回 (相对路径, sha256)。"""
    t = trace_mod.build_trace(n_tars=8, seed=20261015, shuffle="per-epoch",
                              epochs=1, batch=32, steps_per_epoch=400,
                              images_per_tar=1600)
    TRACES_DIR.mkdir(parents=True, exist_ok=True)
    sha = trace_mod.write_trace(t, TRACES_DIR / TRACE_NAME)
    return f"traces/{TRACE_NAME}", sha


def build(template: dict, digest: str, rate: int, short: str, on: bool,
          cache: bool = False) -> dict:
    """构造一条 COND。cache=True 时产出带网关磁盘缓存的臂（必须 on=True）。"""
    readers, pattern = READERS[short]
    c = json.loads(json.dumps(template))
    name = f"gw-{rate}-{short}-{'on' if on else 'off'}"
    if cache:
        if not on:
            raise SystemExit("带缓存的臂必须同时开网关")
        name += "-cache"
    c["name"] = name
    c["figure"] = "gateway-merge"
    c["group"] = "traincache-cold"
    c["track"] = "D"
    c["workload"]["epochs"] = 1
    c["workload"]["readers"] = readers
    c["workload"]["access_pattern"] = pattern
    c["network"] = {"rate_mbit": rate}
    c["sampling"] = {"reps": 3}
    # environment 的键序固定：gateway / gateway_image / gateway_cache / image_set
    image_set = c["environment"]["image_set"]
    c["environment"] = {
        "gateway": on,
        "gateway_image": digest if on else None,
    }
    if cache:
        c["environment"]["gateway_cache"] = True
    c["environment"]["image_set"] = image_set
    path, sha = _trace()
    c["trace"]["file"] = path
    c["trace"]["sha256"] = sha
    c["tags"] = ["gateway-merge"]
    c["notes"] = ("网关合并实验：r1=单读端开销，r4repl=同 key 并发，"
                  "r4shard=不相交分片阴性对照")
    if cache:
        c["tags"].append("gateway-cache")
        c["notes"] += ("；本臂开网关磁盘缓存(gateway_cache=true)，"
                       "与同档不带缓存的 -on 臂对照")
    return c


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--digest", help="gateway 镜像 digest（repo@sha256:...）")
    ap.add_argument("--from-cluster", action="store_true",
                    help="从 kind 节点 containerd 读 digest")
    ap.add_argument("--check", action="store_true", help="只校验不写")
    args = ap.parse_args()

    if bool(args.digest) == bool(args.from_cluster):
        ap.error("--digest 与 --from-cluster 二选一")
    digest = args.digest or _cluster_digest(GATEWAY_IMAGE)
    if not cond_mod._IMAGE_DIGEST_RE.match(digest):
        raise SystemExit(f"digest 形态不合法：{digest!r}")

    template = json.loads(TEMPLATE.read_text(encoding="utf-8"))
    conds = []
    for rate in RATES:
        for short in READERS:
            conds.append(build(template, digest, rate, short, False))
            conds.append(build(template, digest, rate, short, True))
            # 每档一条带网关磁盘缓存的臂，与同档 -on 臂同参，
            # 只有 environment.gateway_cache 不同。
            conds.append(build(template, digest, rate, short, True, cache=True))
    errs = []
    for c in conds:
        e = cond_mod.validate_cond(c)
        if e:
            errs.append((c["name"], e))
    if errs:
        for n, e in errs:
            print(n, e)
        return 1
    print(f"[gw-conds] {len(conds)} 个点校验通过，gateway digest={digest}")
    if args.check:
        return 0
    for c in conds:
        (CONDS_DIR / f"{c['name']}.json").write_text(
            json.dumps(c, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[gw-conds] 已写入 {CONDS_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
