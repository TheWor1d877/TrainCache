#!/usr/bin/env python3
"""blockread 本地自测(不依赖集群/MinIO):造假缓存 + 假 MinIO,验块→tar 拼回。

A(缺省模式):取真实 train tar(~/tcd/bench/data/train/shard-000000.tar,不在
则用 synth_train 合成一小份),按 chunk 切块、按 CAS 头格式写 .blk 到临时
CACHE_ROOT、构造 CURRENT/manifest/idx 并经本地 http.server 供快照拉取,然后:
  A1 全本地命中拼回,sha256 与原 tar 逐字节一致;
  A2 缺块后 agentd 延迟补块(0.3s 后写回),轮询等待路径拼回一致;
  A3 缺块且不补,超时走 HTTP 回源兜底,拼回一致;
  A4 orders ring 写入:新代际 gen=1,心跳同代际只推进 seq。

C(--smoke):用同一假缓存跑 load_train.py 块读模式(interrupt + resume 两段),
证明真实 torch 循环能从块拼回的 tar 解码样本并推进步数。

用法:.venv/bin/python bench/e2e/test_blockread.py [--smoke]
临时产物全部落在 tempfile 目录,退出即清理。
"""
import argparse
import hashlib
import http.server
import json
import os
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DATASET = "selftest-ds"
BUCKET = "bkt"
SHARD = "shard-000000.tar"


def _source_tar(tmp: Path) -> tuple[Path, bool]:
    """返回 (tar 路径, 是否合成兜底)。真实 train tar 优先,缺则合成一小份。"""
    for d in (Path.home() / "tcd/bench/data/train",
              Path.home() / "tcd/bench/data/train/tars"):
        real = d / SHARD
        if real.is_file():
            return real, False
    # 兜底:合成一份小 tar,走与正式数据同一合成管线。图数要够 C 冒烟每个
    # epoch 跑满 steps_per_epoch×batch(3×8=24)步,否则数据流提前耗尽、
    # 中断点写不出、resume 段失败;取 64 张(8 个 batch)留余量。
    sys.path.insert(0, str(REPO / "bench" / "scripts"))
    import io
    import tarfile
    import synth_train
    out = tmp / SHARD
    with tarfile.open(out, "w") as tf:
        for i in range(64):
            img = synth_train.synth(0, i, seed=7, size=128, sigma=20)
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=85)
            data = buf.getvalue()
            ti = tarfile.TarInfo(f"shard-000000_{i:06d}.jpg")
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
    return out, True


def build_fixture(tar_path: Path, root: Path, chunk_size: int, names=(SHARD,)):
    """写假缓存(root/cache)与假对象存储布局(root/origin/<bucket>/...)。

    多个分片名复用同一份 tar 字节(冒烟只验读通路,不验内容差异)。
    fp 用 sha256 前 32 hex 代替 xxh3_128(宿主机无 xxhash;blockread 只把 fp
    当文件名,不重算指纹),头部其余字段严格按 core/cas/header.go 布局。
    """
    cache = root / "cache"
    origin = root / "origin"
    data = tar_path.read_bytes()
    entries = []
    for name in names:
        for chunk in range((len(data) + chunk_size - 1) // chunk_size):
            payload = data[chunk * chunk_size:(chunk + 1) * chunk_size]
            fp = hashlib.sha256(payload).hexdigest()[:32]
            sha = hashlib.sha256(payload).digest()
            head = struct.pack("<4sHHI", b"TCBK", 1, 1, len(payload))
            head += bytes.fromhex(fp) + sha + b"\x00" * 68
            blk = cache / fp[:2] / (fp + ".blk")
            blk.parent.mkdir(parents=True, exist_ok=True)
            blk.write_bytes(head + payload)
            # 块对象 = 纯 payload(core/publish uploadChunk 直传切块字节,无 CAS 头)
            obj = origin / BUCKET / DATASET / "blocks" / f"{name}#{chunk}"
            obj.parent.mkdir(parents=True, exist_ok=True)
            obj.write_bytes(payload)
            entries.append({"path": name, "chunk": chunk, "fp": fp,
                            "size": len(payload)})
    snap = origin / BUCKET / "dataset" / DATASET / "snapshots" / "snap-1"
    snap.mkdir(parents=True, exist_ok=True)
    idx_key = f"dataset/{DATASET}/snapshots/snap-1/shard-0.idx"
    (origin / BUCKET / idx_key).write_text(json.dumps(entries))
    manifest = {"snapshot_id": "snap-1", "shards": [{"key": idx_key}]}
    (snap / "manifest.json").write_text(json.dumps(manifest))
    (origin / BUCKET / "dataset" / DATASET / "CURRENT").write_text(
        "snap-1 " + "0" * 16)
    return cache, origin, entries


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


def serve(origin: Path):
    srv = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), lambda *a, **kw: _QuietHandler(*a, directory=str(origin), **kw))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def _env(cache: Path, endpoint: str, orders: Path, chunk_size: int,
         miss_wait="5"):
    os.environ.update({
        "TRAINCACHE_DATASET": DATASET,
        "TRAINCACHE_ENDPOINT": endpoint,
        "TRAINCACHE_BUCKET": BUCKET,
        "TRAINCACHE_CHUNK_SIZE": str(chunk_size),
        "TRAINCACHE_HOOK_SOCK": "/nonexistent/agent.sock",
        "TRAINCACHE_MISS_WAIT_S": miss_wait,
        "POD_UID": "selftest-pod",
        "CACHE_ROOT": str(cache),
        "ORDERS_DIR": str(orders),
    })


def run_selftest():
    with tempfile.TemporaryDirectory(prefix="tc-blockread-test-") as td:
        root = Path(td)
        tar, synth = _source_tar(root)
        # 真实 tar(~132MiB)用正式块粒度 8MiB;合成兜底小 tar 用 16KiB,
        # 保证切出 ≥6 块供缺块/回源分支取不同块号。
        chunk_size = 16384 if synth else 8388608
        want = hashlib.sha256(tar.read_bytes()).hexdigest()
        cache, origin, entries = build_fixture(tar, root, chunk_size)
        srv, endpoint = serve(origin)
        orders = root / "orders"
        orders.mkdir()
        _env(cache, endpoint, orders, chunk_size)
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import blockread

        def check(tag):
            got = blockread.opener(blockread.shard_url(SHARD)).read()
            assert hashlib.sha256(got).hexdigest() == want, f"{tag}: sha256 不一致"
            assert len(got) == tar.stat().st_size, f"{tag}: 长度不一致"
            print(f"{tag} PASS: {len(got)} bytes, sha256={want[:16]}... stats={blockread.STATS}")

        try:
            check("A1 全本地命中")

            # A2:缺块 → 0.3s 后"agentd"补块,轮询等待路径应拼回一致。
            e = entries[3]
            blk = cache / e["fp"][:2] / (e["fp"] + ".blk")
            saved = blk.read_bytes()
            blk.unlink()
            threading.Timer(0.3, lambda: blk.write_bytes(saved)).start()
            check("A2 缺块轮询补块")
            assert blockread.STATS["miss_filled"] >= 1

            # A3:缺块不补 → 超时读失败(不直连对象存储,与生产一致)。
            e = entries[5]
            blk = cache / e["fp"][:2] / (e["fp"] + ".blk")
            saved = blk.read_bytes()
            blk.unlink()
            blockread.MISS_WAIT_S = 0.3
            try:
                blockread.opener(blockread.shard_url(SHARD)).read()
            except IOError as exc:
                assert "未在" in str(exc), str(exc)
                print(f"A3 超时读失败(不回源) PASS: {exc}")
            else:
                raise AssertionError("A3 超时未抛错:回源兜底仍在")
            # 块补回来后读路径照常拼回一致。
            blk.write_bytes(saved)
            check("A3b 补块后读回一致")

            # A4:orders ring 新代际 + 心跳同代际。
            blockread.set_order("0", [SHARD])
            ring = orders / "selftest-pod.ring"
            head = ring.read_bytes()[:64]
            magic, ver, _, gen, seq = struct.unpack_from("<4sHHQQ", head)
            assert magic == b"TCO1" and gen == 1 and seq == 1, (magic, gen, seq)
            blockread._write_order("0", [SHARD], heartbeat=True)
            magic, ver, _, gen2, seq2 = struct.unpack_from("<4sHHQQ", ring.read_bytes()[:64])
            assert gen2 == 1 and seq2 == 2, (gen2, seq2)
            print(f"A4 orders PASS: gen={gen2} seq={seq2} ring={ring.name}")
            print("SELFTEST PASS")
        finally:
            srv.shutdown()


def run_smoke(workers=0):
    """C:真实 torch 循环从假缓存块读 tar,跑 interrupt + resume 两段。"""
    with tempfile.TemporaryDirectory(prefix="tc-blockread-smoke-") as td:
        root = Path(td)
        tar, synth = _source_tar(root)
        chunk_size = 16384 if synth else 8388608
        # wds 按 worker 切分片:多 worker 时给够分片数,避免空 worker 报 empty。
        names = (SHARD, "shard-000001.tar") if workers > 1 else (SHARD,)
        cache, origin, _ = build_fixture(tar, root, chunk_size, names)
        srv, endpoint = serve(origin)
        orders = root / "orders"
        orders.mkdir()
        _env(cache, endpoint, orders, chunk_size)
        out = root / "out"
        ckpt = root / "ckpt"
        base = [sys.executable, str(REPO / "bench/scripts/load_train.py"),
                "--n-tars", str(len(names)), "--epochs", "2",
                "--steps-per-epoch", "3",
                "--workers", str(workers), "--batch", "8", "--warmup", "1",
                "--seed", "7",
                "--window-k", "0", "--out-dir", str(out),
                "--checkpoint-dir", str(ckpt)]
        try:
            p1 = subprocess.run(base + ["--interrupt-after-epoch", "1"],
                                capture_output=True, text=True, timeout=300)
            print(p1.stdout, end="")
            assert p1.returncode == 0, p1.stderr[-2000:]
            p2 = subprocess.run(base + ["--resume"],
                                capture_output=True, text=True, timeout=300)
            print(p2.stdout, end="")
            assert p2.returncode == 0, p2.stderr[-2000:]
            recs = [json.loads(l) for l in
                    (out / f"per_step_7_w{workers}.jsonl").read_text().splitlines() if l.strip()]
            assert len(recs) == 6, f"期望 6 步,got {len(recs)}"
            run = json.loads((out / "run.json").read_text())
            assert run["complete"] and run["resumed"], run
            print("per_step 证据:")
            for r in recs:
                print(f"  epoch={r['epoch']} step={r['step_in_epoch']} "
                      f"wait_s={r['wait_s']} step_s={r['step_s']} shard={r['shard']}")
            print("SMOKE PASS: 6 步(2 epoch × 3),interrupt+resume 语义保持")
        finally:
            srv.shutdown()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true", help="跑 C(torch 循环冒烟)")
    ap.add_argument("--workers", type=int, default=0, help="smoke 的 loader worker 数")
    args = ap.parse_args()
    if args.smoke:
        run_smoke(args.workers)
    else:
        run_selftest()