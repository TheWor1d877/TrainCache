#!/usr/bin/env python3.12
"""probe_d 汇总裁判:消费顺序可得性探针。

按序运行 verify_wds.py -> verify_torch.py -> reconstruct.py(子进程、同一解释器),
读取三份结果 JSON,汇总七个验证点并写 results/probe_d/verdict.json。
全部通过 exit 0,否则 exit 1。

验证点与判据:
  1 分片顺序(shardshuffle=False,列表序跟随)          [断言]
  2 分片置换(同 (seed,epoch) 相同、异 seed/epoch 不同) [断言]
  3 样本洗牌(.shuffle(100, seed=S) 400 步序列确定)     [断言]
  4 shardshuffle=True 的 seed 通道取证                  [记录:证据齐+观察成立]
  5 RandomSampler(generator 同 seed 确定)               [断言]
  6 DistributedSampler((seed,epoch,ws,rank) 确定)       [断言]
  7 端到端 (列表, seed, epoch) 重建                     [断言]
"""
import json
import subprocess
import sys
import time
from pathlib import Path

PROBE_DIR = Path(__file__).resolve().parent
OUT_DIR = PROBE_DIR.parent / "results" / "probe_d"
SCRIPTS = ["verify_wds.py", "verify_torch.py", "reconstruct.py"]
POINT_ORDER = ["1_shard_order", "2_shard_permutation", "3_sample_shuffle",
               "4_shardshuffle_seed_channel", "5_random_sampler",
               "6_distributed_sampler", "7_reconstruct_end2end"]


def run_script(name):
    t0 = time.time()
    proc = subprocess.run([sys.executable, str(PROBE_DIR / name)],
                          capture_output=True, text=True, timeout=900, cwd=str(PROBE_DIR))
    return {
        "script": name,
        "returncode": proc.returncode,
        "elapsed_s": round(time.time() - t0, 2),
        "stdout_tail": proc.stdout.strip().splitlines()[-4:],
        "stderr_tail": proc.stderr.strip().splitlines()[-4:],
    }


def load(name):
    return json.loads((OUT_DIR / name).read_text())


def main():
    t_start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    runs = [run_script(s) for s in SCRIPTS]
    runs_ok = all(r["returncode"] == 0 for r in runs)

    wds = load("wds_results.json")
    torch_res = load("torch_results.json")
    recon = load("reconstruct_results.json")

    points = {}
    # 1-4:wds 侧
    for k, p in wds["points"].items():
        entry = {
            "status": p["status"], "assertion": p["assertion"], "desc": p["desc"],
            "evidence": {kk: vv for kk, vv in p.items()
                         if kk not in ("status", "assertion", "desc", "source_evidence", "conclusion")},
        }
        if k == "4_shardshuffle_seed_channel":
            entry["assertion_kind"] = "record"   # 非行为断言:通过=证据齐+观察成立
            entry["source_evidence_lines"] = {
                kk: [f"{h['file'].split('/')[-1]}:{h['line']}" for h in vv]
                for kk, vv in p["source_evidence"].items() if isinstance(vv, list) and vv
            }
            entry["conclusion"] = p["conclusion"]
        else:
            entry["assertion_kind"] = "assert"
            if "source_evidence" in p:
                entry["source_evidence_lines"] = {
                    kk: [f"{h['file'].split('/')[-1]}:{h['line']}" for h in vv]
                    for kk, vv in p["source_evidence"].items() if isinstance(vv, list) and vv
                }
        points[k] = entry

    # 2 的源码证据行从源码取证点借(置换实现位置)
    points["2_shard_permutation"]["source_evidence_lines"] = {
        "detshuffle_epoch_seed": [f"filters.py:{h['line']}"
                                  for h in wds["points"]["4_shardshuffle_seed_channel"]["source_evidence"]["detshuffle_epoch_seed"]],
        "simple_shardlist_seed_shuffle": [f"shardlists.py:{h['line']}"
                                          for h in wds["points"]["4_shardshuffle_seed_channel"]["source_evidence"]["simple_shardlist_seed_shuffle"]],
        "multishardsample_set_epoch": [f"shardlists.py:{h['line']}"
                                       for h in wds["points"]["4_shardshuffle_seed_channel"]["source_evidence"]["multishardsample_set_epoch"]],
        "multishardsample_get_shards": [f"shardlists.py:{h['line']}"
                                        for h in wds["points"]["4_shardshuffle_seed_channel"]["source_evidence"]["multishardsample_get_shards"]],
        "pytorchshardlist_hits": wds["points"]["4_shardshuffle_seed_channel"]["source_evidence"]["pytorchshardlist_scan_hits"],
    }

    # 5-6:torch 侧(源码证据统一为 file:line 列表)
    for k in ("5_random_sampler", "6_distributed_sampler"):
        p = dict(torch_res["points"][k])
        se = p.pop("source_evidence", {})
        p["assertion_kind"] = "assert"
        if se:
            p["source_evidence_lines"] = {
                kk: [f"{h['file'].split('/')[-1]}:{h['line']}" for h in vv]
                for kk, vv in se.items() if isinstance(vv, list) and vv
            }
        points[k] = p

    # 7:重建
    points["7_reconstruct_end2end"] = {
        "status": recon["status"], "assertion": True, "assertion_kind": "assert",
        "desc": recon["desc"], "params": recon["params"],
        "checks": [{"name": c["name"], "ok": c["ok"]} for c in recon["checks"]],
    }

    # 关键结论与语义记录
    conclusions = {
        "seed_semantics_measured": {
            ".shuffle(100, seed=S) 有效种子": "实测 = S 本身(每轮迭代以 random.Random(S) 重建 rng;"
                                              "该样本级洗牌无 epoch 项,同 S 的所有轮次序列完全相同)",
            "detshuffle(seed=S) 有效种子": "实测 = S + epoch(同一 stage 第 k 次 run 的 epoch = k-1,"
                                           "故首迭有效种子 = S + 0;epoch 是 stage 内部计数器,"
                                           "独立于外部 epoch;新构造的对象从 epoch=0 重新计数)",
            "task_brief_差异": "任务书'Shuffler 首迭有效种子=S+1'在本安装(1.0.2)未复现:"
                               "实测首迭=S+0;另任务书所述 pipeline.py 的 .shuffle 实为 compat.py "
                               "FluidInterface.shuffle -> filters.shuffle/_shuffle;"
                               "PytorchShardList 类不存在,带 set_epoch/get_shards_for_epoch 的类是 MultiShardSample",
        },
        "shardshuffle_seed_channel": "1.0.2 的 shardshuffle 路径有 seed 注入点:"
                                     "WebDataset(shardshuffle=True) 改写为 100 并 filters.shuffle(100, seed=self.seed);"
                                     "self.seed 缺省 = WDS_SEED 环境变量或 random.randint(0,1e6) -> 默认非确定;"
                                     "需要 (seed,epoch) 语义时用 detshuffle=True(更贴近 OrderSource 需求)",
        "num_workers": "全部采集在单进程完成(wds 主进程直迭;佐证 DataLoader num_workers=0)。"
                       "num_workers>0 时 split_by_worker 以 islice(src, worker, None, num_workers) 交错切分"
                       "URL/样本流,主进程看到的顺序 = 各 worker 流的确定性交织(取决于 prefetch/worker 时序),"
                       "重建需另加 worker 切分模型 —— 本探针不覆盖,作为 worker 模式的顺序语义差异记录。",
        "order_source_implication": "(shard列表, seed, epoch) 三元组足以确定:分片置换(detshuffle/SimpleShardList/"
                                    "MultiShardSample 三选一)+ 样本级洗牌(.shuffle(buffer, seed)),"
                                    "reconstruct.py 仅凭该三元组 + tar 文件名(不经框架)即复现 400 步消费序列",
    }

    all_pass = runs_ok and all(points[k]["status"] == "pass" for k in POINT_ORDER)
    verdict = {
        "probe": "probe_d:消费顺序可得性(PyTorch DataLoader / webdataset)",
        "env": {**wds["env"], "wds_results": wds["env"], "torch_results": torch_res["env"]},
        "all_pass": all_pass,
        "n_points": len(POINT_ORDER),
        "n_pass": sum(1 for k in POINT_ORDER if points[k]["status"] == "pass"),
        "point_order": POINT_ORDER,
        "points": points,
        "script_runs": runs,
        "conclusions": conclusions,
        "total_elapsed_s": round(time.time() - t_start, 2),
    }
    (OUT_DIR / "verdict.json").write_text(json.dumps(verdict, ensure_ascii=False, indent=2) + "\n")

    print(f"probe_d verdict: {verdict['n_pass']}/{verdict['n_points']} points pass"
          f" (scripts ok={runs_ok})")
    for k in POINT_ORDER:
        p = points[k]
        tag = "assert" if p.get("assertion_kind") == "assert" else "record"
        print(f"  {p['status']:4s} [{tag}] {k}")
    print(f"verdict.json -> {OUT_DIR / 'verdict.json'}  ({verdict['total_elapsed_s']}s)")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
