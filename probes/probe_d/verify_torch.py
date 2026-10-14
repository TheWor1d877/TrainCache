#!/usr/bin/env python3.12
"""probe_d 验证点 5-6:PyTorch Sampler 消费顺序的确定性。

验证点:
  5. RandomSampler(ds, generator=torch.Generator().manual_seed(S)):
     同一 seed -> 索引序列逐位相同;异 seed -> 不同。
     附带 DataLoader(num_workers=0) 级别的复核(消费顺序经 collate 后仍确定)。
  6. DistributedSampler(seed=base) + set_epoch(e):
     (seed, epoch, world_size, rank) 固定则索引序列确定(world_size=1 本地验证)。

全部单进程(num_workers=0)。输出:results/probe_d/torch_results.json
"""
import json
import platform
import sys
import time
from pathlib import Path

import torch
from torch.utils.data import DataLoader, Dataset, DistributedSampler, RandomSampler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

OUT_DIR = config.RESULTS_DIR / "probe_d"
N = 2000                    # 虚拟索引空间(RandomSampler/DataLoader)
N_DIST = 4096               # DistributedSampler 数据集长度
SEED_A = 777
SEED_B = 778
BASE = 12345
EPOCH = 3
TORCH_DIR = Path(torch.__file__).resolve().parent


class IdxDataset(Dataset):
    """返回自身索引的数据集,便于从 batch 内容直接读出消费顺序。"""

    def __init__(self, n):
        self.n = n

    def __len__(self):
        return self.n

    def __getitem__(self, i):
        return i


def find_lines(path: Path, needle: str):
    hits = []
    for i, line in enumerate(path.read_text().splitlines(), 1):
        if needle in line:
            hits.append({"file": str(path), "line": i, "text": line.strip()})
    return hits


def main():
    t_start = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results = {"script": "verify_torch.py", "env": {
        "python": platform.python_version(),
        "torch": torch.__version__,
        "num_workers": 0,
    }, "points": {}}

    ds = IdxDataset(N)

    # ---------------- 验证点 5:RandomSampler ----------------
    t0 = time.time()
    def random_idx(seed):
        g = torch.Generator().manual_seed(seed)
        return list(RandomSampler(ds, generator=g))
    r_a1, r_a2 = random_idx(SEED_A), random_idx(SEED_A)
    r_b = random_idx(SEED_B)
    # 同一 generator 对象复跑的状态推进说明(操作提示)
    g_shared = torch.Generator().manual_seed(SEED_A)
    r_shared_1 = list(RandomSampler(ds, generator=g_shared))
    r_shared_2 = list(RandomSampler(ds, generator=g_shared))
    # DataLoader(num_workers=0) 级别复核:两次独立构造、同 seed,batch 内容逐位相同
    def dl_indices(seed, nb=2, bs=32):
        g = torch.Generator().manual_seed(seed)
        dl = DataLoader(ds, batch_size=bs, sampler=RandomSampler(ds, generator=g), num_workers=0)
        out = []
        for i, b in enumerate(dl):
            out.append(b.tolist())
            if i + 1 >= nb:
                break
        return out
    d_a, d_b = dl_indices(SEED_A), dl_indices(SEED_A)
    ok5 = (r_a1 == r_a2) and (r_a1 != r_b) and (d_a == d_b) \
        and len(r_a1) == N and sorted(r_a1) == list(range(N))
    results["points"]["5_random_sampler"] = {
        "status": "pass" if ok5 else "fail",
        "assertion": True,
        "desc": "RandomSampler + Generator().manual_seed(S):同 seed 索引序列逐位相同、异 seed 不同",
        "same_seed_equal": r_a1 == r_a2,
        "diff_seed_differs": r_a1 != r_b,
        "full_permutation_of_dataset": len(r_a1) == N and sorted(r_a1) == list(range(N)),
        "dataloader_w0_two_constructions_equal": d_a == d_b,
        "dataloader_w0_first_batch": d_a[0][:8],
        "same_generator_object_second_pass_differs": r_shared_1 != r_shared_2,
        "shared_generator_note": "显式 generator 对象会被消费;同一 generator 不重建直接复跑,"
                                 "第二次序列不同 —— OrderSource 若复用 sampler 必须每 epoch 重建/重播种 generator",
        "first8_same_seed": r_a1[:8],
        "first8_other_seed": r_b[:8],
        "source_evidence": {
            "random_sampler_iter": find_lines(TORCH_DIR / "utils/data/sampler.py", "class RandomSampler"),
            "randperm_with_generator": find_lines(TORCH_DIR / "utils/data/sampler.py", "torch.randperm(n, generator=generator)"),
        },
        "elapsed_s": round(time.time() - t0, 2),
    }

    # ---------------- 验证点 6:DistributedSampler ----------------
    t0 = time.time()
    dds = IdxDataset(N_DIST)

    def dist_idx(seed, epoch, ws=1, rank=0):
        s = DistributedSampler(dds, num_replicas=ws, rank=rank, shuffle=True, seed=seed)
        s.set_epoch(epoch)
        return list(s)
    s_e1, s_e2 = dist_idx(BASE, EPOCH), dist_idx(BASE, EPOCH)   # 同 (seed, epoch, ws, rank)
    s_e_other = dist_idx(BASE, EPOCH + 1)                        # 异 epoch
    s_seed_other = dist_idx(BASE + 1, EPOCH)                     # 异 seed
    # 同一对象固定 epoch 复跑不漂移(set_epoch 前)
    s_obj = DistributedSampler(dds, num_replicas=1, rank=0, shuffle=True, seed=BASE)
    s_obj.set_epoch(EPOCH)
    o1, o2 = list(s_obj), list(s_obj)
    ok6 = (s_e1 == s_e2) and (s_e1 != s_e_other) and (s_e1 != s_seed_other) \
        and (o1 == o2) and len(s_e1) == N_DIST and sorted(s_e1) == list(range(N_DIST))
    results["points"]["6_distributed_sampler"] = {
        "status": "pass" if ok6 else "fail",
        "assertion": True,
        "desc": "DistributedSampler(seed=base)+set_epoch(e):(seed,epoch,world_size,rank) 固定则序列确定(world_size=1)",
        "params": {"seed": BASE, "epoch": EPOCH, "world_size": 1, "rank": 0, "n": N_DIST},
        "same_triple_equal": s_e1 == s_e2,
        "other_epoch_differs": s_e1 != s_e_other,
        "other_seed_differs": s_e1 != s_seed_other,
        "same_object_same_epoch_rerun_equal": o1 == o2,
        "world_size1_covers_full_dataset": len(s_e1) == N_DIST and sorted(s_e1) == list(range(N_DIST)),
        "first8": s_e1[:8],
        "source_evidence": {
            "epoch_attr_init": find_lines(TORCH_DIR / "utils/data/distributed.py", "self.epoch = 0"),
            "manual_seed_seed_plus_epoch": find_lines(TORCH_DIR / "utils/data/distributed.py", "g.manual_seed(self.seed + self.epoch)"),
            "randperm": find_lines(TORCH_DIR / "utils/data/distributed.py", "torch.randperm"),
            "set_epoch_def": find_lines(TORCH_DIR / "utils/data/distributed.py", "def set_epoch"),
        },
        "effective_seed_note": "源码语义:迭代用 generator.manual_seed(seed + epoch) 作 randperm 种子;"
                               "world_size=1/rank=0 时 num_samples=len(ds),即整个数据集的一个置换",
        "elapsed_s": round(time.time() - t0, 2),
    }

    results["total_elapsed_s"] = round(time.time() - t_start, 2)
    (OUT_DIR / "torch_results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    for name, p in results["points"].items():
        print(f"  [torch] {name}: {p['status']}")
    print(f"  [torch] results -> {OUT_DIR / 'torch_results.json'}  ({results['total_elapsed_s']}s)")


if __name__ == "__main__":
    main()
