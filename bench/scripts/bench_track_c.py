#!/usr/bin/env python3
"""C 轨多 Job 负载器:倾斜场景下的缓存挤占测量。

场景(M3.1):
  * Job A:反复读 10% 热分片(模拟多轮微调和重复 epoch);
  * Job B:顺序扫全量数据(流寇式扫描);
  * 缓存只给数据集的 30%(capacity_bytes 由 COND/环境变量
    TRAINCACHE_CACHE_CAPACITY_BYTES 控制,不在本脚本硬编码);
  * 两 Job 并发运行,Job B 可交错启动(--stagger-s);
  * 统计:A 命中率、A 等待占比、B 把热数据挤出去的次数。

数据复用 B 轨(32×256MiB≈8GB),源站 MinIO 或本地 nginx;热/全量分片清单
由 synth_track_c.py 生成(hot-shards.txt / full-shards.txt)。

输出(--out-dir,缺省 bench/results/track_c):
  * run.json          run 级元数据(t_submit/t_first_step/t_last_step/
                      switch_events/phase_policy/args);
  * per_step_a.jsonl  Job A 每步:step, wait_s, step_s, shard, phase, hit;
  * per_step_b.jsonl  Job B 每步:step, wait_s, step_s, shard, phase;
  * eviction_stats.json  挤占统计(a_hits/a_misses/a_hit_ratio/a_wait_ratio/
                      b_evictions_of_hot/hot_shard_count/full_shard_count)。

挤占统计口径:
  * Job A 每次访问某热块前,检查该块在本地缓存目录是否存在(os.path.exists);
  * 首次访问该热块:不存在 = 冷 miss(不计挤占);存在 = 命中;
  * 后续访问该热块:不存在 = 挤占(B 把它挤出去了,或被容量淘汰);
    存在 = 命中。

并发模型:
  * 主线程用 ThreadPoolExecutor 跑两个 Job 各一个工作线程;
  * Job B 延迟 --stagger-s 秒启动(让 A 先建立热集);
  * 每个 Job 内部再用 workers-a / workers-b 个线程并发下载分片;
  * 主线程等待两 Job 完成后汇总统计。

缓存目录:由 --cache-dir 指定,缺省取环境变量 TRAINCACHE_CACHE_ROOT,
再缺省 /var/cache/traincache。缓存文件以分片名(如 shard-000003.tar)
直接落在该目录下,便于 os.path.exists 判定。

selftest(--selftest):用合成的热/全量分片列表 + mock 下载跑一轮,
不访问网络,验证输出文件格式与挤占统计逻辑。
"""
import argparse
import json
import os
import random
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

# ---- 常量(口径对齐 config.py)----
TRACK_C_RESULTS_DIR = config.RESULTS_DIR / "track_c"
# B 轨分片字节(用于 selftest 的容量换算);真实容量由环境变量控制
SHARD_BYTES_DEFAULT = config.TRACK_B_TAR_BYTES  # 256MiB
N_TARS_DEFAULT = config.TRACK_B_N_TARS           # 32

# 每步 compute 替身开销(秒):C 轨以 IO 为主,compute 可忽略,但 step_s 需
# 略大于 wait_s 以保持口径一致;取 1ms 量级。
COMPUTE_OVERHEAD_S = 0.001


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
def utc_now() -> str:
    """当前 UTC 时刻,RFC3339 微秒精度。"""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z")


def parse_rfc3339(text: str) -> datetime:
    """解析 RFC3339;失败抛 ValueError。"""
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def shard_index(name: str) -> int:
    """从分片名提取整数索引:shard-000003.tar → 3。"""
    head = name.split("-")[-1].split(".")[0]
    return int(head)


def cache_path(cache_dir: Path, shard_name: str) -> Path:
    """分片在本地缓存目录中的路径。"""
    return Path(cache_dir) / shard_name


def load_shard_list(path: Path) -> list[str]:
    """读分片清单文件(每行一个分片名),返回非空行列表。"""
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return [ln.strip() for ln in lines if ln.strip()]


def args_dict(args) -> dict:
    """argparse Namespace → JSON 可写字典(Path 转字符串)。"""
    return {k: (str(v) if isinstance(v, Path) else v)
            for k, v in vars(args).items()}


# ---------------------------------------------------------------------------
# 下载器:真实模式走 urllib,selftest 模式模拟缓存+LRU 淘汰
# ---------------------------------------------------------------------------
class Downloader:
    """分片下载器,封装真实/模拟两种模式与缓存命中判定。

    download(shard_name) 返回该分片在下载前是否已存在于缓存(True=命中)。
    """

    def __init__(self, mode: str, url_tmpl: str, cache_dir: Path,
                 capacity_bytes: int, shard_bytes: int,
                 mock_io_s: float = 0.0):
        assert mode in ("real", "mock")
        self.mode = mode
        self.url_tmpl = url_tmpl
        self.cache_dir = Path(cache_dir)
        self.capacity_bytes = capacity_bytes
        self.shard_bytes = shard_bytes
        self.mock_io_s = mock_io_s  # mock 模式下模拟单次 IO 耗时(秒)
        # mock 模式下的 LRU 顺序(线程安全)
        self._lru: list[str] = []
        self._lock = threading.Lock()

    def exists(self, shard_name: str) -> bool:
        return cache_path(self.cache_dir, shard_name).exists()

    def download(self, shard_name: str) -> bool:
        """下载分片,返回下载前是否已缓存。"""
        existed = self.exists(shard_name)
        if self.mode == "real":
            self._download_real(shard_name)
        else:
            self._download_mock(shard_name)
        return existed

    def _download_real(self, shard_name: str) -> None:
        idx = shard_index(shard_name)
        url = self.url_tmpl % idx
        with urllib.request.urlopen(url, timeout=300) as resp:
            # 消费全部内容,触发缓存层写入
            while resp.read(1 << 20):
                pass

    def _download_mock(self, shard_name: str) -> None:
        """模拟下载:创建缓存文件,超容量时按 LRU 淘汰。"""
        if self.mock_io_s > 0:
            time.sleep(self.mock_io_s)
        path = cache_path(self.cache_dir, shard_name)
        with self._lock:
            path.touch()
            if shard_name in self._lru:
                self._lru.remove(shard_name)
            self._lru.append(shard_name)
            # 超容量则从队头(LRU)淘汰,直到容量内
            while (len(self._lru) * self.shard_bytes > self.capacity_bytes
                   and len(self._lru) > 1):
                evicted = self._lru.pop(0)
                ep = cache_path(self.cache_dir, evicted)
                if ep.exists():
                    ep.unlink()


# ---------------------------------------------------------------------------
# Job 执行
# ---------------------------------------------------------------------------
def _make_epoch_lists(shards: list[str], epochs: int, seed: int,
                      shuffle: bool) -> list[list[str]]:
    """生成各 epoch 的分片访问列表。

    Job A:每 epoch 对热分片做 seed+epoch 的置换(模拟多轮微调换序);
    Job B:顺序扫全量,不打乱(流寇式)。
    """
    out = []
    for e in range(epochs):
        if shuffle:
            perm = list(shards)
            random.Random(seed + e).shuffle(perm)
            out.append(perm)
        else:
            out.append(list(shards))
    return out


def run_job_a(name: str, hot_shards: list[str], epochs: int, workers: int,
              downloader: Downloader, out_path: Path, seed: int,
              stop_event: threading.Event) -> dict:
    """Job A:反复读热分片,记录每步命中/挤占。

    返回统计字典:{n_steps, hits, misses, evictions, wait_total, step_total,
    first_step_at, last_step_at}。
    """
    epoch_lists = _make_epoch_lists(hot_shards, epochs, seed, shuffle=True)
    first_access: set[str] = set()
    fa_lock = threading.Lock()
    stats = {"n_steps": 0, "hits": 0, "misses": 0, "evictions": 0,
             "wait_total": 0.0, "step_total": 0.0,
             "first_step_at": None, "last_step_at": None}
    stats_lock = threading.Lock()
    switch_events: list[dict] = []
    step_counter = 0

    with out_path.open("w", encoding="utf-8") as fout, \
            ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for epoch, shards in enumerate(epoch_lists):
            if stop_event.is_set():
                break
            epoch_start_wall = utc_now()
            # epoch 边界记一次 switch 事件
            switch_events.append({
                "job": name, "epoch": epoch, "at": epoch_start_wall,
                "n_shards": len(shards),
            })
            futs = {}
            for shard in shards:
                if stop_event.is_set():
                    break
                t_submit = time.perf_counter()
                fut = pool.submit(downloader.download, shard)
                futs[fut] = (shard, t_submit, epoch)
            for fut in as_completed(futs):
                shard, t_submit, ep = futs[fut]
                try:
                    existed = fut.result()
                except Exception as exc:
                    print(f"[{name}] shard {shard} 下载失败: {exc}",
                          file=sys.stderr)
                    continue
                t_done = time.perf_counter()
                wait_s = t_done - t_submit
                step_s = wait_s + COMPUTE_OVERHEAD_S
                wall = utc_now()

                # 挤占判定
                with fa_lock:
                    seen_before = shard in first_access
                    if not seen_before:
                        first_access.add(shard)
                with stats_lock:
                    if existed:
                        stats["hits"] += 1
                    else:
                        stats["misses"] += 1
                        if seen_before:
                            # 之前访问过、现在不在缓存 → 被 B/容量挤出去
                            stats["evictions"] += 1
                    stats["wait_total"] += wait_s
                    stats["step_total"] += step_s
                    stats["n_steps"] += 1
                    if stats["first_step_at"] is None:
                        stats["first_step_at"] = wall
                    stats["last_step_at"] = wall
                    step_counter += 1
                    rec_step = step_counter

                phase = "switch" if ep == 0 and rec_step <= 1 else "steady"
                rec = {
                    "step": rec_step,
                    "wait_s": round(wait_s, 6),
                    "step_s": round(step_s, 6),
                    "shard": shard,
                    "phase": phase,
                    "hit": bool(existed),
                }
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fout.flush()

    stats["switch_events"] = switch_events
    return stats


def run_job_b(name: str, full_shards: list[str], epochs: int, workers: int,
              downloader: Downloader, out_path: Path,
              stop_event: threading.Event) -> dict:
    """Job B:顺序扫全量分片。返回 {n_steps, wait_total, step_total,
    first_step_at, last_step_at}。"""
    epoch_lists = _make_epoch_lists(full_shards, epochs, seed=0, shuffle=False)
    stats = {"n_steps": 0, "wait_total": 0.0, "step_total": 0.0,
             "first_step_at": None, "last_step_at": None}
    stats_lock = threading.Lock()
    switch_events: list[dict] = []
    step_counter = 0

    with out_path.open("w", encoding="utf-8") as fout, \
            ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for epoch, shards in enumerate(epoch_lists):
            if stop_event.is_set():
                break
            switch_events.append({
                "job": name, "epoch": epoch, "at": utc_now(),
                "n_shards": len(shards),
            })
            futs = {}
            for shard in shards:
                if stop_event.is_set():
                    break
                t_submit = time.perf_counter()
                fut = pool.submit(downloader.download, shard)
                futs[fut] = (shard, t_submit, epoch)
            for fut in as_completed(futs):
                shard, t_submit, ep = futs[fut]
                try:
                    fut.result()
                except Exception as exc:
                    print(f"[{name}] shard {shard} 下载失败: {exc}",
                          file=sys.stderr)
                    continue
                t_done = time.perf_counter()
                wait_s = t_done - t_submit
                step_s = wait_s + COMPUTE_OVERHEAD_S
                wall = utc_now()
                with stats_lock:
                    stats["wait_total"] += wait_s
                    stats["step_total"] += step_s
                    stats["n_steps"] += 1
                    if stats["first_step_at"] is None:
                        stats["first_step_at"] = wall
                    stats["last_step_at"] = wall
                    step_counter += 1
                    rec_step = step_counter
                phase = "scan"
                rec = {
                    "step": rec_step,
                    "wait_s": round(wait_s, 6),
                    "step_s": round(step_s, 6),
                    "shard": shard,
                    "phase": phase,
                }
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fout.flush()

    stats["switch_events"] = switch_events
    return stats


# ---------------------------------------------------------------------------
# selftest
# ---------------------------------------------------------------------------
def selftest() -> int:
    """合成数据跑一轮 mock,验证输出格式与挤占逻辑。返回 0=通过。"""
    import tempfile

    tmp = Path(tempfile.mkdtemp(prefix="track-c-selftest-"))
    cache_dir = tmp / "cache"
    cache_dir.mkdir()
    out_dir = tmp / "results"
    out_dir.mkdir()

    # 合成清单:8 个分片,其中 2 个热分片(25%,比真实 10% 更易观测挤占)
    all_shards = [f"shard-{i:06d}.tar" for i in range(8)]
    hot_shards = all_shards[:2]
    full_shards = all_shards
    hot_file = tmp / "hot-shards.txt"
    full_file = tmp / "full-shards.txt"
    hot_file.write_text("\n".join(hot_shards) + "\n")
    full_file.write_text("\n".join(full_shards) + "\n")

    # 容量:只够放 3 个分片(37.5%),保证 B 扫描时会挤出 A 的热分片
    shard_bytes = 1 << 20  # 1MiB(selftest 用小体积,省时间)
    capacity_bytes = 3 * shard_bytes

    dl = Downloader("mock", url_tmpl="http://x/shard-%06d.tar",
                    cache_dir=cache_dir, capacity_bytes=capacity_bytes,
                    shard_bytes=shard_bytes, mock_io_s=0.005)

    stop_event = threading.Event()
    a_path = out_dir / "per_step_a.jsonl"
    b_path = out_dir / "per_step_b.jsonl"

    # A 跑 15 轮热分片(15×2×5ms≈150ms);B 跑 2 轮全量(2×8×5ms≈80ms);
    # B 延迟 0.01s 启动,保证两 Job 并发交错、B 能挤出 A 的热分片
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_a = pool.submit(
            run_job_a, "A", hot_shards, 15, 1, dl, a_path, 1234, stop_event)
        time.sleep(0.01)
        fut_b = pool.submit(
            run_job_b, "B", full_shards, 2, 1, dl, b_path, stop_event)
        sa = fut_a.result()
        sb = fut_b.result()

    # ---- 校验输出文件存在 ----
    for p in (a_path, b_path):
        assert p.is_file(), f"selftest 失败:缺少 {p.name}"

    # ---- 校验 per_step_a.jsonl 格式 ----
    a_recs = [json.loads(ln) for ln in a_path.read_text().splitlines() if ln.strip()]
    assert a_recs, "per_step_a.jsonl 为空"
    expected_keys = {"step", "wait_s", "step_s", "shard", "phase", "hit"}
    for r in a_recs:
        assert expected_keys.issubset(r.keys()), \
            f"per_step_a 字段缺失: {r.keys()}"
        assert isinstance(r["hit"], bool)
    # A 总步数 = 15 epoch × 2 热分片
    assert sa["n_steps"] == 30, f"A 步数应为 30,实际 {sa['n_steps']}"

    # ---- 校验 per_step_b.jsonl 格式 ----
    b_recs = [json.loads(ln) for ln in b_path.read_text().splitlines() if ln.strip()]
    assert b_recs, "per_step_b.jsonl 为空"
    b_keys = {"step", "wait_s", "step_s", "shard", "phase"}
    for r in b_recs:
        assert b_keys.issubset(r.keys()), f"per_step_b 字段缺失: {r.keys()}"
    # B 总步数 = 2 epoch × 8 分片
    assert sb["n_steps"] == 16, f"B 步数应为 16,实际 {sb['n_steps']}"

    # ---- 校验挤占逻辑 ----
    # 容量仅 3 片,B 扫 8 片必然把 A 的热分片挤出去,故 evictions > 0
    assert sa["evictions"] > 0, \
        "selftest 失败:容量仅 3 片、B 扫 8 片,应观测到挤占,实际 evictions=0"
    # hits + misses == n_steps
    assert sa["hits"] + sa["misses"] == sa["n_steps"], \
        "hits+misses 应等于总步数"
    # 首次访问不计挤占;evictions 不应超过 misses
    assert sa["evictions"] <= sa["misses"], \
        "evictions 不应超过 misses"
    # 每个热分片至少被访问一次,首次访问要么 hit 要么 miss(非挤占)
    # 挤占数 = 非首次访问中的 miss 数
    first_access_count = len(hot_shards)  # 每个热分片首次访问一次
    non_first = sa["n_steps"] - first_access_count
    assert sa["evictions"] <= non_first, \
        "evictions 不应超过非首次访问次数"

    # ---- 校验汇总统计函数 ----
    ev = compute_eviction_stats(sa, len(hot_shards), len(full_shards))
    assert ev["a_hits"] == sa["hits"]
    assert ev["a_misses"] == sa["misses"]
    assert ev["a_hit_ratio"] == round(sa["hits"] / sa["n_steps"], 6)
    assert ev["b_evictions_of_hot"] == sa["evictions"]
    assert ev["hot_shard_count"] == len(hot_shards)
    assert ev["full_shard_count"] == len(full_shards)
    # a_wait_ratio 在 [0,1]
    assert 0.0 <= ev["a_wait_ratio"] <= 1.0

    # ---- 校验 eviction_stats.json 落盘格式 ----
    ev_path = out_dir / "eviction_stats.json"
    ev_path.write_text(json.dumps(ev, ensure_ascii=False, indent=2) + "\n")
    ev_on_disk = json.loads(ev_path.read_text())
    for k in ("a_hits", "a_misses", "a_hit_ratio", "a_wait_ratio",
              "b_evictions_of_hot", "hot_shard_count", "full_shard_count"):
        assert k in ev_on_disk, f"eviction_stats.json 缺字段 {k}"

    # ---- 校验 run.json 落盘格式(对齐 main 的 run 结构)----
    run_path = out_dir / "run.json"
    first_steps = [t for t in (sa.get("first_step_at"), sb.get("first_step_at")) if t]
    last_steps = [t for t in (sa.get("last_step_at"), sb.get("last_step_at")) if t]
    run_obj = {
        "schema_version": "1", "track": "C",
        "t_submit": utc_now(), "t_first_step": min(first_steps) if first_steps else None,
        "t_last_step": max(last_steps) if last_steps else None,
        "switch_events": sa.get("switch_events", []) + sb.get("switch_events", []),
        "phase_policy": {"phases": ["startup", "steady", "switch", "scan"]},
        "args": {"epochs_a": 15, "epochs_b": 2},
        "capacity_bytes": capacity_bytes,
    }
    run_path.write_text(json.dumps(run_obj, ensure_ascii=False, indent=2) + "\n")
    run_on_disk = json.loads(run_path.read_text())
    for k in ("t_submit", "t_first_step", "t_last_step", "switch_events",
              "phase_policy"):
        assert k in run_on_disk, f"run.json 缺字段 {k}"
    assert run_on_disk["track"] == "C"
    assert isinstance(run_on_disk["switch_events"], list)
    assert run_on_disk["phase_policy"]["phases"]

    print(f"selftest PASS: A steps={sa['n_steps']} hits={sa['hits']} "
          f"misses={sa['misses']} evictions={sa['evictions']} "
          f"hit_ratio={ev['a_hit_ratio']:.2%}; "
          f"B steps={sb['n_steps']}; 输出 4 文件齐全")
    return 0


# ---------------------------------------------------------------------------
# 汇总统计
# ---------------------------------------------------------------------------
def compute_eviction_stats(a_stats: dict, hot_count: int,
                           full_count: int) -> dict:
    n = a_stats["n_steps"]
    hits = a_stats["hits"]
    misses = a_stats["misses"]
    hit_ratio = (hits / n) if n > 0 else 0.0
    wait_ratio = (a_stats["wait_total"] / a_stats["step_total"]
                  if a_stats["step_total"] > 0 else 0.0)
    return {
        "a_hits": hits,
        "a_misses": misses,
        "a_hit_ratio": round(hit_ratio, 6),
        "a_wait_ratio": round(wait_ratio, 6),
        "b_evictions_of_hot": a_stats["evictions"],
        "hot_shard_count": hot_count,
        "full_shard_count": full_count,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="C 轨多 Job 负载器:Job A 反复读热分片 + Job B 全量扫描,"
                    "测量缓存欠配下的挤占与命中率")
    ap.add_argument("--url", default=getattr(
        config, "TRACK_B_URL",
        f"{config.MINIO_ENDPOINT}/{config.MINIO_BUCKET}/shard-%06d.tar"),
        help="tar URL 模板(含一个整数占位),缺省取 B 轨 MinIO URL")
    ap.add_argument("--hot-shards", type=Path,
                    default=config.TRACK_C_DIR / "hot-shards.txt",
                    help="热分片清单文件(缺省 data/track_c/hot-shards.txt)")
    ap.add_argument("--full-shards", type=Path,
                    default=config.TRACK_C_DIR / "full-shards.txt",
                    help="全量分片清单文件(缺省 data/track_c/full-shards.txt)")
    ap.add_argument("--epochs-a", type=int, default=10,
                    help="Job A 循环轮数(缺省 10)")
    ap.add_argument("--epochs-b", type=int, default=3,
                    help="Job B 扫全量轮数(缺省 3)")
    ap.add_argument("--workers-a", type=int, default=4,
                    help="Job A worker 数(缺省 4)")
    ap.add_argument("--workers-b", type=int, default=4,
                    help="Job B worker 数(缺省 4)")
    ap.add_argument("--batch", type=int, default=32,
                    help="batch 大小(缺省 32;C 轨每步读 1 分片,batch 记录在 run.json)")
    ap.add_argument("--stagger-s", type=float, default=5.0,
                    help="Job B 延迟启动秒数(缺省 5.0,让 A 先建立热集)")
    ap.add_argument("--out-dir", type=Path, default=TRACK_C_RESULTS_DIR,
                    help="输出目录(缺省 bench/results/track_c)")
    ap.add_argument("--seed", type=int, default=config.SEED,
                    help=f"随机种子(缺省 {config.SEED})")
    ap.add_argument("--cache-dir", type=Path,
                    default=Path(os.environ.get(
                        "TRAINCACHE_CACHE_ROOT", "/var/cache/traincache")),
                    help="本地缓存目录(缺省 $TRAINCACHE_CACHE_ROOT 或 "
                         "/var/cache/traincache)")
    ap.add_argument("--cache-capacity-bytes", type=int, default=None,
                    help="缓存容量字节(selftest/模拟用;真实运行由环境变量 "
                         "TRAINCACHE_CACHE_CAPACITY_BYTES 控制,不在此硬编码)")
    ap.add_argument("--shard-bytes", type=int, default=SHARD_BYTES_DEFAULT,
                    help=f"单分片字节(用于容量换算,缺省 {SHARD_BYTES_DEFAULT})")
    ap.add_argument("--selftest", action="store_true",
                    help="自检测试:mock 下载跑一轮,不访问网络")
    return ap


def main() -> int:
    ap = build_parser()
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    # ---- 参数校验 ----
    if args.epochs_a < 1 or args.epochs_b < 1:
        ap.error("epochs-a / epochs-b 需 ≥ 1")
    if args.workers_a < 1 or args.workers_b < 1:
        ap.error("workers-a / workers-b 需 ≥ 1")
    if args.batch < 1:
        ap.error("batch 需 ≥ 1")
    if args.stagger_s < 0:
        ap.error("stagger-s 需 ≥ 0")
    # 校验 URL 模板含整数占位
    try:
        probe = args.url % 0
    except (TypeError, ValueError) as e:
        ap.error(f"--url 需含一个整数占位(如 %06d): {e}")
    if probe == args.url:
        ap.error("--url 模板未使用整数占位")

    if not args.hot_shards.is_file():
        ap.error(f"热分片清单不存在: {args.hot_shards}")
    if not args.full_shards.is_file():
        ap.error(f"全量分片清单不存在: {args.full_shards}")

    hot_shards = load_shard_list(args.hot_shards)
    full_shards = load_shard_list(args.full_shards)
    if not hot_shards:
        ap.error("热分片清单为空")
    if not full_shards:
        ap.error("全量分片清单为空")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.cache_dir.mkdir(parents=True, exist_ok=True)

    run_path = args.out_dir / "run.json"
    a_path = args.out_dir / "per_step_a.jsonl"
    b_path = args.out_dir / "per_step_b.jsonl"
    ev_path = args.out_dir / "eviction_stats.json"

    # 缓存容量:优先环境变量,其次命令行
    capacity_bytes = args.cache_capacity_bytes
    env_cap = os.environ.get("TRAINCACHE_CACHE_CAPACITY_BYTES", "").strip()
    if env_cap:
        try:
            capacity_bytes = int(env_cap)
        except ValueError:
            print(f"警告:TRAINCACHE_CACHE_CAPACITY_BYTES={env_cap!r} 无法解析,"
                  f"回退命令行/缺省", file=sys.stderr)

    # ---- run 边界 ----
    started_at = utc_now()
    submit_env = os.environ.get("TRAINCACHE_JOB_CREATED_AT", "").strip()
    if submit_env:
        try:
            parse_rfc3339(submit_env)
            t_submit, t_submit_source = submit_env, "env"
        except ValueError:
            t_submit, t_submit_source = started_at, "invalid-env-fallback"
    else:
        t_submit, t_submit_source = started_at, "script-start-fallback"

    cond = os.environ.get("TRAINCACHE_COND", "").strip() or None
    run_id = os.environ.get("TRAINCACHE_RUN_ID", "").strip() or (
        f"{cond or 'track-c'}-s{args.seed}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{os.getpid()}")

    downloader = Downloader(
        mode="real", url_tmpl=args.url, cache_dir=args.cache_dir,
        capacity_bytes=capacity_bytes or 0, shard_bytes=args.shard_bytes)

    run = {
        "schema_version": "1",
        "run_id": run_id,
        "cond": cond,
        "track": "C",
        "started_at": started_at,
        "finished_at": None,
        "t_submit": t_submit,
        "t_submit_source": t_submit_source,
        "t_first_step": None,
        "t_last_step": None,
        "args": args_dict(args),
        "capacity_bytes": capacity_bytes,
        "hot_shard_count": len(hot_shards),
        "full_shard_count": len(full_shards),
        "phase_policy": {
            "phases": ["startup", "steady", "switch", "scan"],
            "startup": "t_submit → t_first_step(run 级;步记录从 steady/switch 起)",
            "switch": "Job A 每个 epoch 首个步(热集重建边界)",
            "steady": "Job A 其余步",
            "scan": "Job B 全量扫描步",
        },
        "switch_events": [],
        "per_step_a_path": str(a_path),
        "per_step_b_path": str(b_path),
        "eviction_stats_path": str(ev_path),
    }
    run_path.write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")

    print(f"C 轨负载:url={args.url} hot={len(hot_shards)} full={len(full_shards)} "
          f"epochs_a={args.epochs_a} epochs_b={args.epochs_b} "
          f"workers_a={args.workers_a} workers_b={args.workers_b} "
          f"stagger={args.stagger_s}s seed={args.seed}")
    print(f"输出:{run_path} / {a_path} / {b_path} / {ev_path}", flush=True)

    stop_event = threading.Event()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_a = pool.submit(
                run_job_a, "A", hot_shards, args.epochs_a, args.workers_a,
                downloader, a_path, args.seed, stop_event)
            if args.stagger_s > 0:
                time.sleep(args.stagger_s)
            fut_b = pool.submit(
                run_job_b, "B", full_shards, args.epochs_b, args.workers_b,
                downloader, b_path, stop_event)
            a_stats = fut_a.result()
            b_stats = fut_b.result()
    except KeyboardInterrupt:
        stop_event.set()
        print("收到中断,停止 Job...", file=sys.stderr)
        raise

    # ---- 汇总 run.json ----
    first_steps = [t for t in (a_stats.get("first_step_at"),
                               b_stats.get("first_step_at")) if t]
    last_steps = [t for t in (a_stats.get("last_step_at"),
                              b_stats.get("last_step_at")) if t]
    run["t_first_step"] = min(first_steps) if first_steps else None
    run["t_last_step"] = max(last_steps) if last_steps else None
    run["finished_at"] = utc_now()
    run["switch_events"] = (a_stats.get("switch_events", [])
                            + b_stats.get("switch_events", []))
    run["n_steps_a"] = a_stats["n_steps"]
    run["n_steps_b"] = b_stats["n_steps"]
    run_path.write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")

    # ---- 挤占统计 ----
    eviction_stats = compute_eviction_stats(
        a_stats, len(hot_shards), len(full_shards))
    ev_path.write_text(
        json.dumps(eviction_stats, ensure_ascii=False, indent=2) + "\n")

    print(f"C 轨完成:A hits={eviction_stats['a_hits']} "
          f"misses={eviction_stats['a_misses']} "
          f"hit_ratio={eviction_stats['a_hit_ratio']:.2%} "
          f"wait_ratio={eviction_stats['a_wait_ratio']:.2%} "
          f"evictions={eviction_stats['b_evictions_of_hot']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
