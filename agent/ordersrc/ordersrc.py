#!/usr/bin/env python3
"""ordersrc.py — TrainCache ConsumeOrder sidecar 最小写端(design §6.1/§6.2;decisions D22)。

向 <orders-dir>/<pod-uid>.ring 写一份 ConsumeOrder(共享内存 ring,单写端):

- 头 64B 固定布局(偏移 0..64),其后为槽数组,每槽 `seq int64 + len int32 + payload`(JSON,UTF-8),
  全部小端;槽索引 =(seq-1) mod slot_count;布局冻结,与 Go 读端 agent/ordersource 严格一致;
- commit 协议:先整槽一次 pwrite 覆盖,头最后写(write_seq 推进 = 对外可见),读者只会看到完整槽;
- 文件只增不缩(ftruncate 至 max(现长, 64+slot_size)):读端 mmap 映射被截断的页会 SIGBUS;
- writer_generation 每次启动 +1、write_seq 跨代际续增:读端据此检测 writer 重启并重读最新。

只依赖标准库(os/struct/json/argparse/sys),用 os.pread/os.pwrite,不用 mmap。
退出码:0 成功;1 运行时 I/O 错误(open/mkdir/ftruncate/pwrite/fsync);2 参数/校验错误。
"""

import argparse
import json
import os
import struct
import sys

# ---- ring 布局常量(design §6.2,冻结)------------------------------------------------
MAGIC = b"TCO1"
VERSION = 1
HEADER_SIZE = 64          # 头固定 64B(偏移 32..64 保留)
SLOT_HEADER_SIZE = 12     # 每槽头:seq int64 + len int32,其后为 payload
_HEADER = struct.Struct("<4sHHQQII32s")   # magic, version, 保留, generation, write_seq, slot_size, slot_count, 保留 32B
_SLOT_HEADER = struct.Struct("<qi")
_ZERO32 = b"\x00" * 32

# ---- 默认值(与 design §6.2 / 读端几何 sanity 上限一致)------------------------------
DEFAULT_ORDERS_DIR = "/run/traincache/orders"   # 两侧 Pod 同路径挂载 hostPath /run/traincache
DEFAULT_SLOT_SIZE = 1048576                     # 1 MiB
DEFAULT_SLOT_COUNT = 64

# ---- 退出码 -------------------------------------------------------------------------
EXIT_OK = 0
EXIT_IO = 1
EXIT_USAGE = 2

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


def build_parser():
    """构造 CLI(参数表见 design §6.2 配套约定;argparse 错误自然退出码 2)。"""
    p = argparse.ArgumentParser(
        prog="ordersrc.py",
        description="TrainCache ConsumeOrder sidecar 最小写端:写 <pod-uid>.ring(design §6.1/§6.2;D22)",
    )
    p.add_argument("--pod-uid", required=True,
                   help="ring 文件名 <pod-uid>.ring;校验非空、不含路径分隔符、不等于 . / ..")
    p.add_argument("--dataset", required=True, help="ConsumeOrder.dataset")
    p.add_argument("--chunks", required=True,
                   help='消费顺序,"path:chunk,path:chunk" 逗号分隔;列表顺序 = 消费顺序')
    p.add_argument("--snapshot-id", default="", help="ConsumeOrder.snapshot_id(默认空)")
    p.add_argument("--stage-id", default="", help="ConsumeOrder.stage_id(默认空)")
    p.add_argument("--shuffle-seed", type=int, default=0,
                   help="ConsumeOrder.shuffle_seed(int64,默认 0,允许负数)")
    p.add_argument("--batch-size", type=int, default=1, help="ConsumeOrder.batch_size(int64,默认 1)")
    p.add_argument("--prefetch-chunks", type=int, default=1, help="ConsumeOrder.prefetch_chunks(int,默认 1)")
    p.add_argument("--orders-dir", default=DEFAULT_ORDERS_DIR,
                   help=f"ring 发现目录(默认 {DEFAULT_ORDERS_DIR})")
    p.add_argument("--slot-size", type=int, default=DEFAULT_SLOT_SIZE,
                   help=f"槽大小字节(≥16,默认 {DEFAULT_SLOT_SIZE} = 1 MiB)")
    p.add_argument("--slot-count", type=int, default=DEFAULT_SLOT_COUNT,
                   help=f"槽数(≥1,默认 {DEFAULT_SLOT_COUNT})")
    return p


def parse_chunks(p, raw):
    """解析 "path:chunk,path:chunk" → [{"path": p, "chunk": c}, ...];列表顺序 = 消费顺序。

    chunk 必须是非负整数(int64);任何格式问题 → 参数错误(退出码 2)。
    """
    chunks = []
    for item in raw.split(","):
        if item == "":
            p.error('--chunks 含空项(需为 "path:chunk,path:chunk")')
        path, sep, chunk_s = item.rpartition(":")
        if sep == "" or path == "":
            p.error(f"--chunks 项 {item!r} 非法(需为 path:chunk)")
        try:
            chunk = int(chunk_s, 10)
        except ValueError:
            p.error(f"--chunks 项 {item!r} 的 chunk 非整数")
        if chunk < 0 or chunk > INT64_MAX:
            p.error(f"--chunks 项 {item!r} 的 chunk={chunk} 越界(需为非负 int64)")
        chunks.append({"path": path, "chunk": chunk})
    if not chunks:
        p.error("--chunks 至少需要一个 path:chunk 项")
    return chunks


def _check_int64(p, name, v):
    """int64 范围校验(shuffle_seed / batch_size;读端按 int64 反序列化,越界槽会被判损坏)。"""
    if not (INT64_MIN <= v <= INT64_MAX):
        p.error(f"{name}={v} 超出 int64 范围")


def parse_and_validate(p, argv):
    """解析并校验参数;一切参数/校验错误 exit 2(argparse 错误天然退出码 2)。"""
    args = p.parse_args(argv)

    if args.pod_uid == "" or args.pod_uid in (".", "..") or "/" in args.pod_uid or os.sep in args.pod_uid:
        p.error(f"--pod-uid={args.pod_uid!r} 非法:需非空且不含路径分隔符")
    if args.slot_size < 16:
        p.error(f"--slot-size={args.slot_size} 非法:需 ≥16")
    if args.slot_count < 1:
        p.error(f"--slot-count={args.slot_count} 非法:需 ≥1")
    _check_int64(p, "--shuffle-seed", args.shuffle_seed)
    _check_int64(p, "--batch-size", args.batch_size)

    args.chunks = parse_chunks(p, args.chunks)
    return args


def next_generation_and_seq(fd, ring_path):
    """定本次写入的 (generation, write_seq)。

    fstat size==0 → 新文件 (1, 1);否则 pread 旧头 64B:magic/version 合法 → (旧gen+1, 旧write_seq+1)
    (D22:write_seq 跨代际全局递增续增);旧头非法或不足 64B → stderr warn 后按新文件 (1, 1)。
    """
    if os.fstat(fd).st_size == 0:
        return 1, 1
    old = os.pread(fd, HEADER_SIZE, 0)
    if len(old) == HEADER_SIZE:
        magic, version, _reserved, gen, seq, _slot_size, _slot_count, _rest = _HEADER.unpack(old)
        if magic == MAGIC and version == VERSION:
            return gen + 1, seq + 1
    print(
        f"ordersrc: {ring_path} 旧头非法(不足 {HEADER_SIZE}B 或 magic/version 不符),"
        "按新文件 gen=1 seq=1",
        file=sys.stderr,
    )
    return 1, 1


def build_payload(args):
    """构造 ConsumeOrder JSON(UTF-8 字节;字段与读端 json tag 一一对应,§6.1)。"""
    return json.dumps(
        {
            "dataset": args.dataset,
            "snapshot_id": args.snapshot_id,
            "stage_id": args.stage_id,
            "shuffle_seed": args.shuffle_seed,
            "batch_size": args.batch_size,
            "prefetch_chunks": args.prefetch_chunks,
            "chunks": args.chunks,
        },
        ensure_ascii=False,
    ).encode("utf-8")


def write_ring(args, payload):
    """按 D22 写序落盘:打开/创建 → 定 gen/seq → ftruncate 只增不缩 → 整槽覆盖 → 头最后写(commit)→ fsync。"""
    ring_path = os.path.join(args.orders_dir, args.pod_uid + ".ring")

    try:
        os.makedirs(args.orders_dir, exist_ok=True)
        fd = os.open(ring_path, os.O_RDWR | os.O_CREAT, 0o644)
    except OSError as e:
        print(f"ordersrc: 打开 ring 失败 {ring_path}: {e}", file=sys.stderr)
        return EXIT_IO

    try:
        gen, seq = next_generation_and_seq(fd, ring_path)

        # 只增不缩:读端按文件当前长度 mmap,收缩会让旧映射访问出 SIGBUS。
        size = os.fstat(fd).st_size
        if size < HEADER_SIZE + args.slot_size:
            os.ftruncate(fd, HEADER_SIZE + args.slot_size)

        # 整槽一次 pwrite 覆盖(顺带把旧 payload 尾部清零);槽索引 =(seq-1) mod slot_count。
        slot_off = HEADER_SIZE + ((seq - 1) % args.slot_count) * args.slot_size
        slot = (
            _SLOT_HEADER.pack(seq, len(payload))
            + payload
            + b"\x00" * (args.slot_size - SLOT_HEADER_SIZE - len(payload))
        )
        os.pwrite(fd, slot, slot_off)

        # commit:头最后写(write_seq 推进 = 本订单对外可见)。
        os.pwrite(
            fd,
            _HEADER.pack(MAGIC, VERSION, 0, gen, seq, args.slot_size, args.slot_count, _ZERO32),
            0,
        )
        os.fsync(fd)
    except OSError as e:
        print(f"ordersrc: 写 ring 失败 {ring_path}: {e}", file=sys.stderr)
        return EXIT_IO
    finally:
        os.close(fd)

    print(f"wrote ConsumeOrder gen={gen} seq={seq} ring={ring_path} chunks={len(args.chunks)}")
    return EXIT_OK


def main(argv=None):
    parser = build_parser()
    args = parse_and_validate(parser, argv)
    payload = build_payload(args)

    # payload 超槽容量属参数错误(退出码 2),在建文件之前拒绝。
    room = args.slot_size - SLOT_HEADER_SIZE
    if len(payload) > room:
        print(
            f"ordersrc: payload {len(payload)}B 超出槽可用空间 {room}B(--slot-size 太小)",
            file=sys.stderr,
        )
        return EXIT_USAGE

    return write_ring(args, payload)


if __name__ == "__main__":
    sys.exit(main())
