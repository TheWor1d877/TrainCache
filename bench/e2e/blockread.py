#!/usr/bin/env python3
"""块→tar 适配器:训练进程从节点本地块缓存拼回分片 tar 字节流喂 webdataset。

生产消费路径的数据面读端(对照 bench/compare/run_bench.py 的 traincache 臂,
但那边只数字节,这里拼回完整 tar):

  * 快照经 MinIO HTTP 拉取(CURRENT → manifest.json → 逐片 idx),按 path 聚合
    (chunk, fp, size) 并排序,布局同 core/index/format.go;
  * 分片名 → 按 chunk 序读 <CACHE_ROOT>/<fp[:2]>/<fp>.blk,跳 128B CAS 头
    (core/cas/header.go),concat payload 成 tar 字节流(io.BytesIO,整片
    ~132MiB 进内存可接受);
  * 缺块:发 read_miss hook 帧 → 25ms stat 轮询等 agentd 补块(上限
    TRAINCACHE_MISS_WAIT_S,默认 180s)→ 超时即本次读失败(生产上同样是
    训练读报错);
  * 每读一块发 advance hook 帧;orders 经 ordersrc.py 写 ring,每 epoch 一次
    (stage_id=epoch),心跳线程每 10s 同代际续写(D57)。

orders/hook 全部 best-effort:sock/目录不存在时静默跳过,不影响读字节。

webdataset 接入:install() 把 opener 注册进 gopen 的 scheme 表,分片 URL 形如
tcblk://shard-000000.tar;DataLoader worker 经 fork 继承注册,无需重复 install。

env:TRAINCACHE_DATASET / TRAINCACHE_ENDPOINT / TRAINCACHE_BUCKET /
TRAINCACHE_CHUNK_SIZE(默认 8388608)/ TRAINCACHE_PREFETCH_K(默认 16)/
TRAINCACHE_HOOK_SOCK(默认 /run/tc-hook/agent.sock)/ POD_UID /
CACHE_ROOT(默认 /var/cache/traincache/blocks)/ ORDERS_DIR(默认
/run/traincache/orders)/ TRAINCACHE_MISS_WAIT_S(默认 180)/
TRAINCACHE_ORDERSRC(ordersrc.py 路径,缺省按镜像/仓库布局探测)。
"""
import io
import json
import os
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

DATASET = os.environ.get("TRAINCACHE_DATASET", "")
ENDPOINT = os.environ.get("TRAINCACHE_ENDPOINT", "").rstrip("/")
BUCKET = os.environ.get("TRAINCACHE_BUCKET", "")
CHUNK_SIZE = int(os.environ.get("TRAINCACHE_CHUNK_SIZE", "8388608"))
PREFETCH_K = int(os.environ.get("TRAINCACHE_PREFETCH_K", "16"))
HOOK_SOCK = os.environ.get("TRAINCACHE_HOOK_SOCK", "/run/tc-hook/agent.sock")
POD_UID = os.environ.get("POD_UID", "unknown-pod")
CACHE_ROOT = os.environ.get("CACHE_ROOT", "/var/cache/traincache/blocks")
ORDERS_DIR = os.environ.get("ORDERS_DIR", "/run/traincache/orders")
MISS_WAIT_S = float(os.environ.get("TRAINCACHE_MISS_WAIT_S", "180"))

SCHEME = "tcblk"
HEADER_SIZE = 128          # CAS 块头(core/cas/header.go,v1 即最终格式)
_MAGIC = b"TCBK"

# 块级诊断计数(进程内;worker 各自一份,仅供日志/自测,不参与测量口径)
STATS = {"local": 0, "miss": 0, "miss_filled": 0}

_chunk_map = None


def _http_get(url, range_start=None, range_end=None):
    req = urllib.request.Request(url)
    if range_start is not None:
        req.add_header("Range", f"bytes={range_start}-{range_end}")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def load_snapshot():
    """拉 CURRENT 快照建 chunk map:{path: [(chunk, fp, size), ...]}(按 chunk 排序)。"""
    global _chunk_map
    if _chunk_map is not None:
        return _chunk_map
    if not (ENDPOINT and BUCKET and DATASET):
        raise RuntimeError(
            "块读需要 TRAINCACHE_ENDPOINT/TRAINCACHE_BUCKET/TRAINCACHE_DATASET")
    base = f"{ENDPOINT}/{BUCKET}/dataset/{DATASET}"
    cur = _http_get(base + "/CURRENT").decode().strip().split()[0]
    manifest = json.loads(_http_get(base + "/snapshots/" + cur + "/manifest.json"))
    m = {}
    for sh in manifest["shards"]:
        idx = json.loads(_http_get(f"{ENDPOINT}/{BUCKET}/" + sh["key"]))
        for e in idx:
            m.setdefault(e["path"], []).append((e["chunk"], e["fp"], e["size"]))
    for k in m:
        m[k].sort()
    _chunk_map = m
    return m


# ---- hook 帧(Unix datagram,行分隔 JSON;发送失败静默忽略)-----------------------

def _hook_send(frame):
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            s.sendto((json.dumps(frame, separators=(",", ":")) + "\n").encode(),
                     HOOK_SOCK)
        finally:
            s.close()
    except OSError:
        pass


def _hook_advance(name, chunk_idx):
    _hook_send({"type": "advance", "path": name, "chunk": int(chunk_idx)})


def _hook_read_miss(name, chunk_idx):
    _hook_send({"type": "read_miss", "path": name, "chunk": int(chunk_idx),
                "reason": "absent"})


# ---- 块读取 -------------------------------------------------------------------

def _read_local_block(blk, size):
    """读本地 .blk 的 payload;不在/头不符/读不全一律按缺块返回 None。

    块可能在 open 前后被 agentd 淘汰(FileNotFoundError),或落盘半途被看到——
    块落盘是临时文件 + rename,exists 即完整,头校验兜住异常形态。
    """
    try:
        with open(blk, "rb") as f:
            head = f.read(HEADER_SIZE)
            if len(head) < HEADER_SIZE:
                return None
            magic, version, _flags, block_size = struct.unpack_from("<4sHHI", head)
            if magic != _MAGIC or version != 1 or block_size != size:
                return None
            payload = f.read(size)
            return payload if len(payload) == size else None
    except FileNotFoundError:
        return None


def read_shard_bytes(name):
    """按 chunk 序拼回分片 tar 字节:本地命中直读,缺块等 agentd,超时即读失败。"""
    chunks = load_snapshot().get(name)
    if not chunks:
        raise FileNotFoundError(f"快照里没有分片 {name}(dataset={DATASET})")
    parts = []
    for chunk_idx, fp, size in chunks:
        blk = os.path.join(CACHE_ROOT, fp[:2], fp + ".blk")
        payload = _read_local_block(blk, size)
        if payload is not None:
            STATS["local"] += 1
        else:
            STATS["miss"] += 1
            # 先报 read_miss 触发 P0 demand 补块,再轮询等持续预取。25ms 纯 stat:
            # 块落盘是 rename 原子可见,等待量级降到秒(同 run_bench.py 的口径)。
            _hook_read_miss(name, chunk_idx)
            deadline = time.perf_counter() + MISS_WAIT_S
            while payload is None and time.perf_counter() < deadline:
                time.sleep(0.025)
                payload = _read_local_block(blk, size)
            if payload is not None:
                STATS["miss_filled"] += 1
            else:
                # 超时:块没等到,本次读失败。生产上同样是训练读报错。
                raise IOError(
                    f"块未在 {MISS_WAIT_S}s 内落盘: {name} chunk={chunk_idx}")
        _hook_advance(name, chunk_idx)
        parts.append(payload)
    return b"".join(parts)


# ---- webdataset 接入 ----------------------------------------------------------

def shard_url(name):
    return f"{SCHEME}://{name}"


def shard_name(url):
    return url.split("://", 1)[1] if "://" in url else url


def opener(url, mode="rb", bufsize=8192, **kw):
    """gopen 兼容 opener:分片 URL → 拼好的 tar 字节流。"""
    if mode != "rb":
        raise ValueError(f"块读只支持 rb,got {mode!r}")
    return io.BytesIO(read_shard_bytes(shard_name(url)))


def install():
    """把 opener 注册进 webdataset 的 scheme 分发表(幂等)。

    必须经 importlib 取模块:webdataset/__init__ 把包属性 gopen 重绑成了
    同名函数,from webdataset import gopen 拿到的是函数不是模块。
    """
    import importlib
    gopen = importlib.import_module("webdataset.gopen")
    gopen.gopen_schemes[SCHEME] = opener


# ---- orders ring(经 ordersrc.py;每 epoch 一次 + 10s 心跳续写)------------------

_hb_lock = threading.RLock()
_hb_stop = threading.Event()
_hb_stage = "0"
_hb_names = []
_hb_thread = None


def _ordersrc_path():
    p = os.environ.get("TRAINCACHE_ORDERSRC")
    if p:
        return p if os.path.isfile(p) else None
    if os.path.isfile("/app/ordersrc.py"):
        return "/app/ordersrc.py"
    # 宿主机布局:bench/e2e/blockread.py → 仓库根/agent/ordersrc/ordersrc.py;
    # 镜像里文件平铺在 /app,parents 不足三层,跳过仓库探测。
    parents = Path(__file__).resolve().parents
    if len(parents) > 2:
        cand = str(parents[2] / "agent" / "ordersrc" / "ordersrc.py")
        if os.path.isfile(cand):
            return cand
    return None


def _write_order(stage_id, names, heartbeat=False):
    # 宿主机冒烟等无 orders 挂载的场景:目录不在就静默跳过,不影响读字节。
    if not os.path.isdir(ORDERS_DIR):
        return
    script = _ordersrc_path()
    if script is None:
        return
    snap = load_snapshot()
    chunks = []
    seen = set()
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        for chunk_idx, _fp, _size in snap.get(name, []):
            chunks.append(f"{name}:{chunk_idx}")
    if not chunks:
        return
    cmd = [
        sys.executable, script,
        "--pod-uid", POD_UID,
        "--dataset", DATASET,
        "--chunks", ",".join(chunks),
        "--prefetch-chunks", str(PREFETCH_K),
        "--stage-id", str(stage_id),
        "--orders-dir", ORDERS_DIR,
    ]
    if heartbeat:
        # 心跳必须同代际:载荷相同本不会推送,代际一变读端就重放订单(D57)。
        cmd.append("--same-generation")
    try:
        # 心跳线程与 epoch 主循环串行写同一 ring:并发会各自算出同一个 seq,
        # 头 commit 谁后写谁生效,可能把订单盖成心跳的旧载荷。
        with _hb_lock:
            subprocess.run(cmd, check=True, capture_output=True, timeout=30)
        print(f"[ordersrc] stage={stage_id} {'hb' if heartbeat else 'new-gen'} "
              f"wrote {len(chunks)} chunks, K={PREFETCH_K}", flush=True)
    except Exception as e:
        print(f"[ordersrc] WARNING: write order failed: {e}", flush=True)


def set_order(stage_id, shard_names):
    """epoch 开始时调用:记录消费序并写一份新代际订单(stage_id=epoch)。"""
    global _hb_stage, _hb_names
    with _hb_lock:
        _hb_stage = str(stage_id)
        _hb_names = list(shard_names)
        stage, names = _hb_stage, list(_hb_names)
    _write_order(stage, names, heartbeat=False)


def _heartbeat_loop():
    while not _hb_stop.wait(10):
        with _hb_lock:
            stage, names = _hb_stage, list(_hb_names)
        if names:
            _write_order(stage, names, heartbeat=True)


def start_heartbeat():
    global _hb_thread
    if _hb_thread is None:
        _hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
        _hb_thread.start()


def stop_heartbeat():
    _hb_stop.set()