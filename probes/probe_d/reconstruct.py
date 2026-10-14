#!/usr/bin/env python3.12
"""probe_d 验证点 7:端到端重建 —— (分片列表, seed, epoch) -> 消费顺序。

思路(不依赖 webdataset 运行,只依赖其源码语义的独立复现):
  1. 分片置换:用本文件内的 sim_shuffle(webdataset filters._shuffle 的逐行等价副本)
     在 Random(shard_seed + epoch) 上对流式分片列表做缓冲洗牌
     —— 对应 wds 端 filters.detshuffle(bufsize, seed) 的 epoch 语义
     (detshuffle.py 源码:filters.py `self.epoch += 1; rng.seed(self.seed + self.epoch)`)。
  2. 样本级洗牌:每个分片的 __key__ 清单由 tarfile 直接解析(mirror tariterators.base_plus_ext
     的 key 提取规则,不经 wds),按第 1 步的分片置换拼接成输入流,
     再用 sim_shuffle(Random(sample_seed)) 复现 .shuffle(bufsize, seed=S) 的输出。
  3. 与 verify_wds.py 采集的真实消费序列逐位比对:
     - tarfile 清单 vs wds 消费清单(输入流本身的校验)
     - 每 epoch 的分片置换与 400 步 __key__ 序列
     - 固定分片序(shardshuffle=False)下的样本洗牌序列

输入:results/probe_d/captures_wds.json(verify_wds.py 产出)
输出:results/probe_d/reconstruct_results.json
"""
import json
import random
import re
import sys
import tarfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

OUT_DIR = config.RESULTS_DIR / "probe_d"
KEY_RE = re.compile(r"^((?:.*/|)[^.]+)[.]([^/]*)$")  # tariterators.base_plus_ext 的正则


def sim_shuffle(data, bufsize, rng, initial=100):
    """webdataset filters._shuffle 的逐行等价副本(种子由调用方给定)。"""
    initial = min(initial, bufsize)
    buf = []
    for sample in data:
        buf.append(sample)
        if len(buf) < bufsize:
            try:
                buf.append(next(data))
            except StopIteration:
                pass
        if len(buf) >= initial:
            k = rng.randint(0, len(buf) - 1)
            out = buf[k]
            buf[k] = buf[-1]
            buf.pop()
            yield out
    while len(buf) > 0:
        k = rng.randint(0, len(buf) - 1)
        out = buf[k]
        buf[k] = buf[-1]
        buf.pop()
        yield out


def keys_from_tar(path: str):
    """tarfile 直读分片的样本 key 清单(base_plus_ext 语义,去重保持首现顺序)。"""
    out, seen = [], set()
    with tarfile.open(path, "r") as t:
        for name in t.getnames():
            m = KEY_RE.match(name)
            if not m:
                continue
            key = m.group(1)
            if key not in seen:
                seen.add(key)
                out.append(key)
    return out


def main():
    t_start = time.time()
    cap = json.loads((OUT_DIR / "captures_wds.json").read_text())
    urls = cap["urls"]
    bufsize = cap["bufsize"]
    per_shard = cap["per_shard"]
    checks = []

    def check(name, ok, detail=None):
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    # 1. 独立解析 tar 得到每分片 key 清单,并与 wds 采集的 manifest 对齐
    tar_keys = {str(i): keys_from_tar(urls[i]) for i in range(len(urls))}
    manifest_wds = cap["manifest"]
    counts_ok = all(len(tar_keys[str(i)]) == per_shard for i in range(len(urls)))
    manifest_ok = counts_ok and all(tar_keys[k] == manifest_wds[k] for k in map(str, range(len(urls))))
    check("tarfile_key_manifest_matches_wds_consumption", manifest_ok,
          {"shards": len(urls), "per_shard": per_shard,
           "first_key_shard0": tar_keys["0"][0] if counts_ok else None})

    # 2. 端到端:每 epoch 重建分片置换 + 样本级 400 步序列 + 全量 pass 序列
    e2e = cap["end2end"]
    shard_seed, sample_seed = e2e["shard_seed"], e2e["sample_seed"]
    idx = list(range(len(urls)))
    for rec in e2e["epochs"]:
        ep = rec["epoch"]
        # 分片置换:detshuffle 有效种子 = shard_seed + epoch(首迭 epoch=0)
        perm_idx = list(sim_shuffle(iter(idx), bufsize, random.Random(shard_seed + ep)))
        perm_ok = perm_idx == rec["shard_first_appearance"]
        # 输入流 = 置换后分片的 key 清单拼接;样本级洗牌 seed = sample_seed
        flat = [k for i in perm_idx for k in tar_keys[str(i)]]
        pred_stream = list(sim_shuffle(iter(flat), bufsize, random.Random(sample_seed)))
        pred = pred_stream[:len(rec["keys400"])]
        keys_ok = pred == rec["keys400"]
        # 全量 pass:重建的分片 run 序列与观测逐位比对(12800 样本)
        pred_runs = []
        for k in pred_stream:
            sh = int(k.split("_")[0].split("-")[1])
            if not pred_runs or pred_runs[-1][0] != sh:
                pred_runs.append([sh, 1])
            else:
                pred_runs[-1][1] += 1
        full_ok = pred_runs == rec["shard_runs"]
        check(f"end2end_epoch{ep}_shard_perm", perm_ok, {"perm": perm_idx})
        check(f"end2end_epoch{ep}_keys400", keys_ok,
              {"n": len(rec["keys400"]), "first3_pred": pred[:3]})
        check(f"end2end_epoch{ep}_full_pass_runs", full_ok,
              {"n_runs_pred": len(pred_runs), "n_runs_obs": len(rec["shard_runs"]),
               "n_samples": sum(r[1] for r in pred_runs)})

    # 3. 固定分片序(shardshuffle=False)下的样本洗牌序列重建
    ss = cap["sample_shuffle"]
    flat_list_order = [k for i in range(len(urls)) for k in tar_keys[str(i)]]
    pred_ss = list(sim_shuffle(iter(flat_list_order), bufsize, random.Random(ss["seed"])))[:len(ss["keys_iter1"])]
    check("sample_shuffle_keys400_list_order", pred_ss == ss["keys_iter1"],
          {"n": len(ss["keys_iter1"]), "first3_pred": pred_ss[:3]})

    # 4. 重建序列本身的可复算性:同 (list, seed, epoch) 两次重建逐位一致
    perm_a = list(sim_shuffle(iter(idx), bufsize, random.Random(shard_seed)))
    perm_b = list(sim_shuffle(iter(list(range(len(urls)))), bufsize, random.Random(shard_seed)))
    check("reconstruction_self_reproducible", perm_a == perm_b, {"perm": perm_a})

    # 5. 异 epoch 的重建结果确实不同(证明 epoch 参与了顺序生成)
    perm_ep0 = list(sim_shuffle(iter(idx), bufsize, random.Random(shard_seed + 0)))
    perm_ep1 = list(sim_shuffle(iter(idx), bufsize, random.Random(shard_seed + 1)))
    check("different_epoch_yields_different_order", perm_ep0 != perm_ep1,
          {"epoch0_perm": perm_ep0, "epoch1_perm": perm_ep1})

    status = "pass" if all(c["ok"] for c in checks) else "fail"
    out = {
        "script": "reconstruct.py",
        "status": status,
        "desc": "(分片列表, seed, epoch) -> 重建分片置换 + 样本级洗牌序列,与 verify_wds 采集逐位比对",
        "params": {"n_tars": len(urls), "bufsize": bufsize, "shard_seed": shard_seed,
                   "sample_seed": sample_seed, "epochs": [r["epoch"] for r in e2e["epochs"]]},
        "checks": checks,
        "total_elapsed_s": round(time.time() - t_start, 2),
    }
    (OUT_DIR / "reconstruct_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    for c in checks:
        print(f"  [reconstruct] {c['name']}: {'ok' if c['ok'] else 'FAIL'}")
    print(f"  [reconstruct] {status} -> {OUT_DIR / 'reconstruct_results.json'}  ({out['total_elapsed_s']}s)")


if __name__ == "__main__":
    main()
