#!/usr/bin/env python3.12
"""probe_d 验证点 1-4:webdataset 1.0.2 消费顺序的确定性与采集。

验证点与本文件对应关系:
  1. 分片顺序:shardshuffle=False + 显式 URL 列表 -> 一 pass 内消费分片序 = 列表序;
     列表置换后消费序跟随(从样本 __key__ 第一字段提分片号)。
  2. 分片置换:同 seed 逐位相同、异 seed 不同。三条实现路径都测:
     SimpleShardList(urls, seed=S) / MultiShardSample.set_epoch(S)+get_shards_for_epoch()
     / filters.detshuffle(bufsize, seed=S)((seed, epoch) 形式)。
  3. 样本洗牌:.shuffle(100, seed=S) 同 S 复跑 400 步 __key__ 逐位相同、异 S 不同;
     并用 _shuffle 算法副本在候选种子集上复现输出,实测"有效种子"。
  4. shardshuffle=True 的 seed 通道:源码证据(file:line)+ 运行确认默认路径非确定性、
     显式 seed 时确定性。此点只做记录,不作为行为断言(见 judge.py 的 kind 字段)。

运行语义:全部单进程 —— wds 侧直接在主进程迭代 WebDataset(等价 num_workers=0),
不引入任何 worker 进程;仅点 3 附带一个 DataLoader(num_workers=0) 的佐证采集。

输出:
  results/probe_d/wds_results.json   验证点 1-4 的断言与证据
  results/probe_d/captures_wds.json  供 reconstruct.py 比对的序列采集(manifest/样本洗牌/端到端)
"""
import json
import platform
import random
import re
import sys
import time
import warnings
from pathlib import Path

import webdataset as wds
from webdataset import filters, shardlists

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

OUT_DIR = config.RESULTS_DIR / "probe_d"
BASE_URLS = [str(config.TARS_DIR / f"shard-{i:05d}.tar") for i in range(config.N_TARS)]
N_PER_SHARD = config.N_IMGS_PER_TAR

# 采集常量(reconstruct.py 从 captures_wds.json 读取同一组值)
SHARD_SEED = 20261006          # 分片置换种子(端到端,走 detshuffle)
SAMPLE_SEED = 1001             # 样本洗牌种子
BUFSIZE = 100
CAPTURE_KEYS = 400             # 样本级序列比对步数
WDS_DIR = Path(wds.__file__).resolve().parent


def key_to_shard(key: str) -> int:
    """从样本 __key__(形如 shard-00007_000123)提分片号。"""
    return int(key.split("_")[0].split("-")[1])


def url_to_shard(url: str) -> int:
    """从分片 URL(形如 .../shard-00007.tar)提分片号。"""
    return int(re.search(r"shard-(\d+)\.tar$", url).group(1))


def iter_keys(ds, n, close=True):
    """从 pipeline 迭代器取前 n 个样本的 __key__。"""
    it = iter(ds)
    keys = []
    try:
        for _ in range(n):
            try:
                keys.append(next(it)["__key__"])
            except StopIteration:
                break
    finally:
        if close and hasattr(it, "close"):
            it.close()
    return keys


def collect_pass(ds, n_keys=CAPTURE_KEYS):
    """跑完一个完整 pass:返回 (分片 run 序列 [[shard,count],...], 前 n_keys 个 __key__, 总数, 每分片 key 清单)。"""
    runs, keys, manifest = [], [], {}
    total = 0
    for s in ds:
        key = s["__key__"]
        sh = key_to_shard(key)
        if not runs or runs[-1][0] != sh:
            runs.append([sh, 1])
        else:
            runs[-1][1] += 1
        if len(keys) < n_keys:
            keys.append(key)
        manifest.setdefault(sh, []).append(key)
        total += 1
    return runs, keys, total, manifest


def sim_shuffle(data, bufsize, rng, initial=100):
    """webdataset.filters._shuffle 的算法副本(用于实测有效种子/离线重建,严格逐行等价)。

    源代码:filters.py `_shuffle`(bufsize/initial 截断、buf[k]=buf[-1] 的 pick、末尾 drain)。
    注意:data 必须是迭代器(next(data) 语义)。
    """
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


def find_lines(path: Path, needle: str):
    """在源码文件里按子串定位行号(源码证据)。"""
    hits = []
    for i, line in enumerate(path.read_text().splitlines(), 1):
        if needle in line:
            hits.append({"file": str(path), "line": i, "text": line.strip()})
    return hits


def measure_effective_seed(observed, input_seq, candidates, bufsize, n_out):
    """用 sim_shuffle 在候选种子上复现 observed;返回能逐位复现的种子列表。"""
    matching = []
    for c in candidates:
        got = list(sim_shuffle(iter(input_seq), bufsize, random.Random(c)))[:n_out]
        if got == observed:
            matching.append(c)
    return matching


def main():
    t_start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    points = {}
    captures = {
        "urls": BASE_URLS,
        "n_tars": config.N_TARS,
        "per_shard": N_PER_SHARD,
        "bufsize": BUFSIZE,
        "sample_seed": SAMPLE_SEED,
        "shard_seed": SHARD_SEED,
    }

    # 安装 shuffle 调用记录器:记录 FluidInterface/WebDataset 实际为洗牌传了什么 seed
    declared_calls = []
    orig_shuffle_stage = filters.shuffle

    def spy_shuffle(*a, **kw):
        declared_calls.append({"args": list(a), "kwargs": dict(kw)})
        return orig_shuffle_stage(*a, **kw)

    filters.shuffle = spy_shuffle

    # ---------------- 验证点 1:分片顺序 ----------------
    t0 = time.time()
    ds = wds.WebDataset(BASE_URLS, shardshuffle=False)
    runs, keys400, total, manifest = collect_pass(ds)
    list_order = list(range(config.N_TARS))
    p1_full = {
        "runs": runs,
        "total_samples": total,
        "order_equals_list": [r[0] for r in runs] == list_order,
        "counts_ok": all(r[1] == N_PER_SHARD for r in runs),
        "first_keys": keys400[:3],
    }
    # 置换列表:取 4 个分片的置换,完整 pass 核对消费序跟随
    perm = [7, 2, 0, 15]
    ds_perm = wds.WebDataset([BASE_URLS[i] for i in perm], shardshuffle=False)
    runs_p, _, total_p, _ = collect_pass(ds_perm, n_keys=4)
    p1_perm = {
        "list": perm,
        "runs": runs_p,
        "total_samples": total_p,
        "order_follows_permutation": [r[0] for r in runs_p] == perm,
        "counts_ok": all(r[1] == N_PER_SHARD for r in runs_p),
    }
    ok1 = p1_full["order_equals_list"] and p1_full["counts_ok"] \
        and p1_perm["order_follows_permutation"] and p1_perm["counts_ok"] \
        and p1_full["total_samples"] == config.N_TARS * N_PER_SHARD
    points["1_shard_order"] = {
        "status": "pass" if ok1 else "fail",
        "assertion": True,
        "desc": "shardshuffle=False + 显式 URL 列表:一 pass 内消费分片序=列表序;置换列表后跟随",
        "full_pass": p1_full,
        "permuted": p1_perm,
        "elapsed_s": round(time.time() - t0, 2),
    }
    captures["manifest"] = {str(k): v for k, v in sorted(manifest.items())}

    # ---------------- 验证点 2:分片置换(seed -> 置换) ----------------
    t0 = time.time()
    S2A, S2B = 424242, 424243
    # 路径 a:SimpleShardList(urls, seed=S)(__iter__ 内 random.Random(S).shuffle)
    a1 = [d["url"] for d in shardlists.SimpleShardList(list(BASE_URLS), seed=S2A)]
    a2 = [d["url"] for d in shardlists.SimpleShardList(list(BASE_URLS), seed=S2A)]
    a3 = [d["url"] for d in shardlists.SimpleShardList(list(BASE_URLS), seed=S2B)]
    # 路径 b:MultiShardSample(1.0.2 中真正带 set_epoch/get_shards_for_epoch API 的类)
    # 该类被 @obsolete 门禁(Reason: 将迁移到 WIDS JSON),需显式放行;与顺序语义无关
    import os
    os.environ["ALLOW_OBSOLETE"] = "1"
    spec = {"datasets": [{"name": "probe_d", "shards": list(BASE_URLS)}]}
    m = shardlists.MultiShardSample(spec)
    m.set_epoch(S2A)
    b1 = m.get_shards_for_epoch()
    m.set_epoch(S2A)
    b2 = m.get_shards_for_epoch()
    m.set_epoch(S2B)
    b3 = m.get_shards_for_epoch()
    # 路径 c:filters.detshuffle((seed + epoch) 形式,epoch 计数器自 -1 起,run 时先 +1)
    url_dicts = [{"url": u} for u in BASE_URLS]
    d = filters.detshuffle(BUFSIZE, seed=S2A)
    c_e0 = [x["url"] for x in d.run(iter(url_dicts))]
    c_e1 = [x["url"] for x in d.run(iter(url_dicts))]
    d2 = filters.detshuffle(BUFSIZE, seed=S2A)
    c_e0b = [x["url"] for x in d2.run(iter(url_dicts))]
    d3 = filters.detshuffle(BUFSIZE, seed=S2B)
    c_e0c = [x["url"] for x in d3.run(iter(url_dicts))]
    # 有效种子实测:找到能复现置换的 random.Random 种子
    # (sim 输入用纯 URL 字符串流,与观测到的 URL 字符串逐位比对;dict/str 不可混比)
    eff_e0 = measure_effective_seed(c_e0, list(BASE_URLS), [S2A - 1, S2A, S2A + 1, S2A + 2], BUFSIZE, len(BASE_URLS))
    eff_e1 = measure_effective_seed(c_e1, list(BASE_URLS), [S2A, S2A + 1, S2A + 2], BUFSIZE, len(BASE_URLS))
    ok2 = (a1 == a2) and (a1 != a3) and (b1 == b2) and (b1 != b3) \
        and (c_e0 != c_e1) and (c_e0 == c_e0b) and (c_e0 != c_e0c) \
        and eff_e0 == [S2A] and eff_e1 == [S2A + 1]
    points["2_shard_permutation"] = {
        "status": "pass" if ok2 else "fail",
        "assertion": True,
        "desc": "同 (seed,epoch) 分片置换逐位相同;异 seed/异 epoch 不同(三条路径)",
        "simple_shardlist": {
            "same_seed_equal": a1 == a2, "diff_seed_differ": a1 != a3,
            "seed_a_perm": [url_to_shard(u) for u in a1],
        },
        "multishardsample_set_epoch": {
            "same_seed_equal": b1 == b2, "diff_seed_differ": b1 != b3,
        },
        "detshuffle": {
            "seed_a_epoch0_perm": [url_to_shard(u) for u in c_e0],
            "epoch0_reproducible": c_e0 == c_e0b,
            "epoch1_differs": c_e0 != c_e1,
            "epoch1_perm": [url_to_shard(u) for u in c_e1],
            "diff_seed_differs": c_e0 != c_e0c,
            "effective_seed_epoch0": eff_e0,
            "effective_seed_epoch1": eff_e1,
            "seed_map_note": "实测:第 k 次迭代 run 的有效种子 = S2A + (k-1),即首迭 = S2A + 0"
                             "(epoch 计数器 -1 起、run 时先 +1);与任务书'首迭 = S+1'不同",
        },
        "elapsed_s": round(time.time() - t0, 2),
    }

    # ---------------- 验证点 3:样本洗牌 ----------------
    t0 = time.time()
    declared_calls.clear()
    dsA = wds.WebDataset(BASE_URLS, shardshuffle=False).shuffle(BUFSIZE, seed=SAMPLE_SEED)
    k1 = iter_keys(dsA, CAPTURE_KEYS)          # 同一对象第 1 次迭代
    k2 = iter_keys(dsA, CAPTURE_KEYS)          # 同一对象第 2 次迭代(epoch 语义重跑)
    dsB = wds.WebDataset(BASE_URLS, shardshuffle=False).shuffle(BUFSIZE, seed=SAMPLE_SEED)
    k3 = iter_keys(dsB, CAPTURE_KEYS)          # 独立构造、同 seed
    dsC = wds.WebDataset(BASE_URLS, shardshuffle=False).shuffle(BUFSIZE, seed=SAMPLE_SEED + 1)
    k4 = iter_keys(dsC, CAPTURE_KEYS)          # 异 seed
    declared_seeded = list(declared_calls)
    # 无 seed 路径(FluidInterface.shuffle 不传 seed)-> _shuffle 内部 pid+time 随机
    declared_calls.clear()
    dsN = wds.WebDataset(BASE_URLS, shardshuffle=False).shuffle(BUFSIZE)
    n1 = iter_keys(dsN, 60)
    n2 = iter_keys(dsN, 60)
    declared_noseed = list(declared_calls)
    # DataLoader(num_workers=0) 佐证:经 DataLoader 消费是否保序
    import torch
    def dl_batches(seed, nbatch=3, bs=16):
        dsx = wds.WebDataset(BASE_URLS, shardshuffle=False).shuffle(BUFSIZE, seed=seed)
        dl = torch.utils.data.DataLoader(dsx, batch_size=bs, num_workers=0, collate_fn=list)
        out = []
        for i, b in enumerate(dl):
            out.append([s["__key__"] for s in b])
            if i + 1 >= nbatch:
                break
        return out
    dl_a, dl_b = dl_batches(SAMPLE_SEED), dl_batches(SAMPLE_SEED)
    # 有效种子实测(输入 = manifest 列表序的 key 流)
    flat_manifest = [k for sh in sorted(manifest) for k in manifest[sh]]
    eff_sample = measure_effective_seed(k1, flat_manifest, [SAMPLE_SEED - 1, SAMPLE_SEED, SAMPLE_SEED + 1, SAMPLE_SEED + 2], BUFSIZE, CAPTURE_KEYS)
    same_epoch_rerun = (k1 == k2) and (k1 == k3)
    ok3 = same_epoch_rerun and (k4 != k1) and (dl_a == dl_b) and (n1 != n2) and eff_sample == [SAMPLE_SEED]
    points["3_sample_shuffle"] = {
        "status": "pass" if ok3 else "fail",
        "assertion": True,
        "desc": ".shuffle(100, seed=S) 同 (S, epoch) 复跑 400 步 __key__ 逐位相同;异 S 不同",
        "n_keys": CAPTURE_KEYS,
        "same_object_rerun_equal": k1 == k2,
        "fresh_construction_same_seed_equal": k1 == k3,
        "other_seed_differs": k4 != k1,
        "dataloader_w0_batches_equal": dl_a == dl_b,
        "dataloader_w0_first_batch": dl_a[0][:4],
        "no_seed_stage_non_deterministic_across_iter": n1 != n2,
        "declared_calls_seeded": declared_seeded,
        "declared_calls_no_seed": declared_noseed,
        "effective_seed_matching": eff_sample,
        "effective_seed_note": "实测:输出与 random.Random(SAMPLE_SEED) 逐位一致(即有效种子=S 本身);"
                               "该洗牌阶段每次迭代都以 Random(S) 重建 rng,故 epoch 不影响其序列",
        "elapsed_s": round(time.time() - t0, 2),
    }
    captures["sample_shuffle"] = {
        "seed": SAMPLE_SEED,
        "other_seed": SAMPLE_SEED + 1,
        "bufsize": BUFSIZE,
        "keys_iter1": k1,
        "keys_iter2_same_object": k2,
        "keys_fresh_same_seed": k3,
        "keys_other_seed": k4,
    }

    # ---------------- 验证点 4:shardshuffle=True 的 seed 通道(记录,非断言) ----------------
    t0 = time.time()
    src = {
        "webdataset_dir": str(WDS_DIR),
        "default_seed_random_randint": find_lines(WDS_DIR / "compat.py", "random.randint(0, 1000000)"),
        "shardshuffle_compose_filters_shuffle": find_lines(WDS_DIR / "compat.py", "filters.shuffle(args.shardshuffle, seed=self.seed)"),
        "detshuffle_compose": find_lines(WDS_DIR / "compat.py", "filters.detshuffle(args.shardshuffle, seed=self.seed)"),
        "shardshuffle_true_rewritten_to_100": find_lines(WDS_DIR / "compat.py", "shardshuffle = 100"),
        "fluidinterface_shuffle_def": find_lines(WDS_DIR / "compat.py", "def shuffle(self, size, **kw):"),
        "shuffle_no_seed_rng": find_lines(WDS_DIR / "filters.py", "random.Random(int((os.getpid() + time.time()) * 1e9))"),
        "detshuffle_epoch_seed": find_lines(WDS_DIR / "filters.py", "rng.seed(self.seed + self.epoch)"),
        "detshuffle_epoch_default_minus1": find_lines(WDS_DIR / "filters.py", "epoch=-1"),
        "detshuffle_epoch_incr": find_lines(WDS_DIR / "filters.py", "self.epoch += 1"),
        "simple_shardlist_seed_shuffle": find_lines(WDS_DIR / "shardlists.py", "random.Random(self.seed).shuffle(urls)"),
        "multishardsample_set_epoch": find_lines(WDS_DIR / "shardlists.py", "def set_epoch"),
        "multishardsample_get_shards": find_lines(WDS_DIR / "shardlists.py", "def get_shards_for_epoch"),
        "multishardsample_rng_shuffle": find_lines(WDS_DIR / "shardlists.py", "self.rng.shuffle(result)"),
    }
    has_pytorchshardlist = []
    for f in sorted(WDS_DIR.glob("*.py")):
        for i, line in enumerate(f.read_text().splitlines(), 1):
            if "PytorchShardList" in line:
                has_pytorchshardlist.append({"file": str(f), "line": i})
    src["pytorchshardlist_scan_hits"] = has_pytorchshardlist  # 1.0.2 中不存在该类

    def shard_head(ds, transitions=2, max_samples=3000):
        """取前若干次分片切换为止的 run 序列(warning 由调用方抑制)。"""
        runs, got = [], 0
        for s in ds:
            sh = key_to_shard(s["__key__"])
            if not runs or runs[-1][0] != sh:
                runs.append([sh, 1])
                got += 1
                if got >= transitions:
                    break
            else:
                runs[-1][1] += 1
        return runs

    url_brace = str(config.TARS_DIR / "shard-{00000..00015}.tar")
    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        nondet = []
        for _ in range(3):
            dsn = wds.WebDataset(url_brace, shardshuffle=True)
            nondet.append({"assigned_seed": dsn.seed, "head_runs": shard_head(dsn)})
        seeded = []
        for _ in range(2):
            dss = wds.WebDataset(url_brace, shardshuffle=True, seed=S2A)
            seeded.append({"assigned_seed": dss.seed, "head_runs": shard_head(dss)})
        warnings_n = len(wlist)
    nondet_distinct = len({json.dumps(x["head_runs"]) for x in nondet}) >= 2
    seeded_equal = seeded[0]["head_runs"] == seeded[1]["head_runs"]
    ok4 = all(src[k] for k in src if k not in ("pytorchshardlist_scan_hits",)) \
        and nondet_distinct and seeded_equal and not has_pytorchshardlist
    points["4_shardshuffle_seed_channel"] = {
        "status": "pass" if ok4 else "fail",
        "assertion": False,   # 记录点:通过 = 源码证据齐 + 运行观察成立;不代表"无 seed 注入点"这一论断成立
        "desc": "shardshuffle=True 的 seed 通道取证:1.0.2 中 seed 注入点是存在的"
                "(seed= 参数 / WDS_SEED 环境变量);缺省 seed 时走 random.randint(0,1e6) -> 非确定性",
        "source_evidence": src,
        "runtime": {
            "default_no_seed_3_constructions": nondet,
            "default_no_seed_distinct_orders": nondet_distinct,
            "explicit_seed_2_constructions": seeded,
            "explicit_seed_equal": seeded_equal,
            "warnings_count": warnings_n,
        },
        "conclusion": "与任务书'默认 shardlist 路径 shardshuffle=True 无 seed 注入点'不符:"
                      "WebDataset(shardshuffle=True) 会改写为 100 并经 filters.shuffle(100, seed=self.seed) 注入种子,"
                      "self.seed 缺省 = os.environ['WDS_SEED'] 或 random.randint(0,1e6)(compat.py 见来源行号)。"
                      "真正的'无 seed'路径是 FluidInterface.shuffle(size) 不传 seed(点 3 的 noseed 观察:filters.py 用 pid+time 造 rng)。"
                      "另 PytorchShardList 类在 1.0.2 中不存在(全包 0 处命中);带 set_epoch/get_shards_for_epoch 的是 MultiShardSample(yaml/dict 路径)。",
        "elapsed_s": round(time.time() - t0, 2),
    }

    # ---------------- 端到端采集(供 reconstruct.py 验证点 7) ----------------
    t0 = time.time()
    end2end = {"shard_seed": SHARD_SEED, "sample_seed": SAMPLE_SEED, "bufsize": BUFSIZE, "epochs": []}
    declared_calls.clear()
    ds_e2e = wds.WebDataset(url_brace, shardshuffle=BUFSIZE, detshuffle=True, seed=SHARD_SEED) \
        .shuffle(BUFSIZE, seed=SAMPLE_SEED)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for ep in range(3):
            runs_e, keys_e, total_e, _ = collect_pass(ds_e2e)
            first_seen = []
            for r in runs_e:
                if r[0] not in first_seen:
                    first_seen.append(r[0])
            end2end["epochs"].append({
                "epoch": ep,
                "shard_runs": runs_e,                 # 样本流的分片 run 压缩序列(完整 pass)
                "shard_run_ids": [r[0] for r in runs_e],
                "shard_first_appearance": first_seen,  # 各分片在样本流中首次出现的顺序 = URL 层置换
                "total_samples": total_e,
                "keys400": keys_e,
            })
    end2end["declared_calls"] = list(declared_calls)
    captures["end2end"] = end2end

    filters.shuffle = orig_shuffle_stage  # 还原

    # ---------------- 落盘 ----------------
    env = {
        "python": platform.python_version(),
        "webdataset": getattr(wds, "__version__", "?"),
        "torch": __import__("torch").__version__,
        "num_workers": 0,
        "run_mode": "单进程:主进程直接迭代 WebDataset;佐证 DataLoader(num_workers=0)",
    }
    results = {
        "script": "verify_wds.py",
        "env": env,
        "points": points,
        "captures_file": "captures_wds.json",
        "total_elapsed_s": round(time.time() - t_start, 2),
    }
    (OUT_DIR / "wds_results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    (OUT_DIR / "captures_wds.json").write_text(json.dumps(captures, ensure_ascii=False, indent=2) + "\n")
    for name, p in points.items():
        print(f"  [wds] {name}: {p['status']}"
              + ("(assert)" if p["assertion"] else "(record)"))
    print(f"  [wds] captures -> {OUT_DIR / 'captures_wds.json'}  ({results['total_elapsed_s']}s)")


if __name__ == "__main__":
    main()
