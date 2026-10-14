#!/usr/bin/env python3
"""ordersrc_test.py — ordersrc.py 的字节级单测(unittest;只读测试自建的临时 ring)。

覆盖:头/槽字节布局(design §6.2 冻结)、gen 递增与 write_seq 续增、槽索引取模、
只增不缩、坏旧头降级、坏参数退出码 2、I/O 错误退出码 1、--help。

运行:python3 agent/ordersrc/ordersrc_test.py
"""

import json
import os
import struct
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "ordersrc.py")

# 与 ordersrc.py / Go 读端一致的冻结布局(测试独立字面量,防实现与测试同源漂移)。
HEADER = struct.Struct("<4sHHQQII32s")
SLOT_HEADER = struct.Struct("<qi")
MAGIC = b"TCO1"
VERSION = 1
HEADER_SIZE = 64
SLOT_HEADER_SIZE = 12
ZERO32 = b"\x00" * 32


def run_writer(*args):
    """跑 ordersrc.py(不用 shell),返回 CompletedProcess。"""
    return subprocess.run([sys.executable, SCRIPT, *args], capture_output=True, text=True)


def read_header(path):
    with open(path, "rb") as f:
        raw = f.read(HEADER_SIZE)
    if len(raw) != HEADER_SIZE:
        raise AssertionError(f"ring 头不足 {HEADER_SIZE}B")
    return HEADER.unpack(raw)


def read_slot(path, seq, slot_size, slot_count):
    """按槽索引 =(seq-1) mod slot_count 读回 (槽 seq, len, payload 字节)。"""
    off = HEADER_SIZE + ((seq - 1) % slot_count) * slot_size
    with open(path, "rb") as f:
        f.seek(off)
        raw = f.read(SLOT_HEADER_SIZE)
        if len(raw) != SLOT_HEADER_SIZE:
            raise AssertionError("槽头不足 12B")
        got_seq, n = SLOT_HEADER.unpack(raw)
        payload = f.read(n)
    if len(payload) != n:
        raise AssertionError("payload 字节数不足")
    return got_seq, n, payload


class OrdersrcTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = self._tmp.name

    def ring_path(self, pod_uid):
        return os.path.join(self.dir, pod_uid + ".ring")

    # ---- 布局与成功路径 ------------------------------------------------------------

    def test_header_and_slot_layout(self):
        """64B 头逐字段、槽头 12B、payload 内容与槽尾零填充(§6.2 冻结布局)。"""
        slot_size, slot_count = 512, 4
        r = run_writer(
            "--pod-uid", "pod-a", "--dataset", "b-track",
            "--chunks", "shard-000.tar:0,shard-007.tar:3",
            "--snapshot-id", "snap-20261014", "--stage-id", "epoch-2",
            "--shuffle-seed", "-7", "--batch-size", "32", "--prefetch-chunks", "256",
            "--orders-dir", self.dir, "--slot-size", str(slot_size), "--slot-count", str(slot_count),
        )
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("gen=1 seq=1", r.stdout)
        self.assertIn("chunks=2", r.stdout)

        path = self.ring_path("pod-a")
        # 首次写入 ftruncate 到 64 + slot_size(一个槽)。
        self.assertEqual(HEADER_SIZE + slot_size, os.path.getsize(path))

        magic, version, reserved, gen, seq, ss, sc, rest = read_header(path)
        self.assertEqual(MAGIC, magic)
        self.assertEqual(VERSION, version)
        self.assertEqual(0, reserved)
        self.assertEqual(1, gen)
        self.assertEqual(1, seq)
        self.assertEqual(slot_size, ss)
        self.assertEqual(slot_count, sc)
        self.assertEqual(ZERO32, rest)  # 偏移 32..64 保留 32 个 0

        got_seq, n, payload = read_slot(path, 1, slot_size, slot_count)
        self.assertEqual(1, got_seq)  # 槽头 seq
        self.assertEqual(
            {
                "dataset": "b-track",
                "snapshot_id": "snap-20261014",
                "stage_id": "epoch-2",
                "shuffle_seed": -7,
                "batch_size": 32,
                "prefetch_chunks": 256,
                "chunks": [
                    {"path": "shard-000.tar", "chunk": 0},
                    {"path": "shard-007.tar", "chunk": 3},
                ],
            },
            json.loads(payload.decode("utf-8")),
        )
        # 槽尾零填充到 slot_size:payload 之后不得有旧数据残留。
        with open(path, "rb") as f:
            f.seek(HEADER_SIZE + SLOT_HEADER_SIZE + n)
            self.assertEqual(b"\x00" * (slot_size - SLOT_HEADER_SIZE - n),
                             f.read(slot_size - SLOT_HEADER_SIZE - n))

    def test_second_run_increments_generation_and_seq(self):
        """再次运行 = writer 重启:generation +1、write_seq 续增(D22),旧槽不被覆盖。"""
        common = ["--pod-uid", "pod-a", "--orders-dir", self.dir,
                  "--slot-size", "256", "--slot-count", "2"]

        r1 = run_writer(*common, "--dataset", "a-track", "--chunks", "s:0")
        self.assertEqual(0, r1.returncode, r1.stderr)
        self.assertIn("gen=1 seq=1", r1.stdout)

        r2 = run_writer(*common, "--dataset", "b-track", "--chunks", "s:1")
        self.assertEqual(0, r2.returncode, r2.stderr)
        self.assertIn("gen=2 seq=2", r2.stdout)

        path = self.ring_path("pod-a")
        magic, version, _reserved, gen, seq, ss, sc, _rest = read_header(path)
        self.assertEqual((MAGIC, VERSION, 2, 2), (magic, version, gen, seq))
        self.assertEqual((256, 2), (ss, sc))

        # 两次写入各占一槽(seq=1 → 槽 0;seq=2 → 槽 1),内容都在。
        seq1, _, p1 = read_slot(path, 1, 256, 2)
        seq2, _, p2 = read_slot(path, 2, 256, 2)
        self.assertEqual(1, seq1)
        self.assertEqual(2, seq2)
        self.assertEqual("a-track", json.loads(p1)["dataset"])
        self.assertEqual("b-track", json.loads(p2)["dataset"])

    def test_slot_index_wraps_modulo_slot_count(self):
        """槽索引 =(seq-1) mod slot_count;文件几何保持 64 + slot_count*slot_size。"""
        slot_size = 256
        common = ["--pod-uid", "pod-w", "--dataset", "a-track", "--chunks", "s:0",
                  "--orders-dir", self.dir, "--slot-size", str(slot_size), "--slot-count", "2"]
        for i in (1, 2, 3):
            r = run_writer(*common)
            self.assertEqual(0, r.returncode, r.stderr)
            self.assertIn(f"gen={i} seq={i}", r.stdout)  # 每次运行续增(gen 与 seq 同步 +1)

        path = self.ring_path("pod-w")
        # seq=3 → 槽索引 (3-1) mod 2 = 0,覆盖 seq=1 的槽位。
        seq_, _, _ = read_slot(path, 3, slot_size, 2)
        self.assertEqual(3, seq_)
        self.assertEqual(HEADER_SIZE + 2 * slot_size, os.path.getsize(path))

    def test_payload_is_utf8_json(self):
        """payload 为 UTF-8 JSON(非 ASCII 字段按原字节落盘,可解码回原值)。"""
        r = run_writer("--pod-uid", "pod-u", "--dataset", "b-track-中文",
                       "--chunks", "分片-0.tar:2", "--orders-dir", self.dir,
                       "--slot-size", "256", "--slot-count", "2")
        self.assertEqual(0, r.returncode, r.stderr)

        _, n, payload = read_slot(self.ring_path("pod-u"), 1, 256, 2)
        self.assertEqual(n, len(payload))
        order = json.loads(payload.decode("utf-8"))
        self.assertEqual("b-track-中文", order["dataset"])
        self.assertEqual({"path": "分片-0.tar", "chunk": 2}, order["chunks"][0])

    # ---- 旧头与只增不缩 ------------------------------------------------------------

    def test_ftruncate_never_shrinks_and_valid_old_header_continues(self):
        """旧头合法 → gen/seq 续增;大文件只增不缩(绝不 ftruncate 收缩)。"""
        path = self.ring_path("pod-big")
        big = 2 * 1024 * 1024
        with open(path, "wb") as f:
            f.write(HEADER.pack(MAGIC, VERSION, 0, 5, 7, 256, 2, ZERO32))
            f.truncate(big)
        self.assertEqual(big, os.path.getsize(path))

        r = run_writer("--pod-uid", "pod-big", "--dataset", "a-track", "--chunks", "s:0",
                       "--orders-dir", self.dir, "--slot-size", "256", "--slot-count", "2")
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("gen=6 seq=8", r.stdout)          # 旧 gen=5/write_seq=7 续增
        self.assertEqual(big, os.path.getsize(path))    # 只增不缩

        seq_, _, payload = read_slot(path, 8, 256, 2)
        self.assertEqual(8, seq_)
        self.assertEqual("a-track", json.loads(payload)["dataset"])

    def test_bad_old_header_warns_and_restarts(self):
        """旧头非法(不足 64B / magic 不符)→ stderr warn,按新文件 gen=1 seq=1。"""
        path = self.ring_path("pod-bad")
        with open(path, "wb") as f:
            f.write(b"\x00" * 128)  # 头 magic 非法

        r = run_writer("--pod-uid", "pod-bad", "--dataset", "a-track", "--chunks", "s:0",
                       "--orders-dir", self.dir, "--slot-size", "256", "--slot-count", "2")
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("gen=1 seq=1", r.stdout)
        self.assertIn("旧头非法", r.stderr)

        magic, version, _reserved, gen, seq, _ss, _sc, _rest = read_header(path)
        self.assertEqual((MAGIC, VERSION, 1, 1), (magic, version, gen, seq))

    # ---- 退出码 --------------------------------------------------------------------

    def test_help_exit_code_0(self):
        r = run_writer("--help")
        self.assertEqual(0, r.returncode, r.stderr)
        for opt in ("--pod-uid", "--dataset", "--chunks", "--orders-dir", "--slot-size", "--slot-count"):
            self.assertIn(opt, r.stdout)

    def test_bad_args_exit_code_2(self):
        """参数/校验错误(含 payload 超限、pod-uid 非法)一律退出码 2,且不落 ring 文件。"""
        ok = ["--pod-uid", "pod-a", "--dataset", "a", "--chunks", "s:0", "--orders-dir", self.dir]
        cases = [
            ("缺 --pod-uid", ["--dataset", "a", "--chunks", "s:0", "--orders-dir", self.dir]),
            ("缺 --dataset", ["--pod-uid", "pod-a", "--chunks", "s:0", "--orders-dir", self.dir]),
            ("缺 --chunks", ["--pod-uid", "pod-a", "--dataset", "a", "--orders-dir", self.dir]),
            ("pod-uid 为空", ["--pod-uid", "", "--dataset", "a", "--chunks", "s:0", "--orders-dir", self.dir]),
            ("pod-uid 含 /", ["--pod-uid", "a/b", "--dataset", "a", "--chunks", "s:0", "--orders-dir", self.dir]),
            ("pod-uid 为 .", ["--pod-uid", ".", "--dataset", "a", "--chunks", "s:0", "--orders-dir", self.dir]),
            ("pod-uid 为 ..", ["--pod-uid", "..", "--dataset", "a", "--chunks", "s:0", "--orders-dir", self.dir]),
            ("slot-size 过小", [*ok, "--slot-size", "15"]),
            ("slot-size 非整数", [*ok, "--slot-size", "x"]),
            ("slot-count 为 0", [*ok, "--slot-count", "0"]),
            ("shuffle-seed 超 int64", [*ok, "--shuffle-seed", str(2 ** 63)]),
            ("chunk 非整数", [*ok[:4], "--chunks", "s:x", *ok[5:]]),
            ("chunk 为负", [*ok[:4], "--chunks", "s:-1", *ok[5:]]),
            ("chunks 缺冒号", [*ok[:4], "--chunks", "s", *ok[5:]]),
            ("chunks 空项", [*ok[:4], "--chunks", "s:0,", *ok[5:]]),
            ("payload 超限", [*ok, "--slot-size", "16"]),
        ]
        for name, argv in cases:
            with self.subTest(name):
                r = run_writer(*argv)
                self.assertEqual(2, r.returncode, f"{name}: stdout={r.stdout!r} stderr={r.stderr!r}")
                self.assertFalse(os.path.exists(self.ring_path("pod-a")),
                                 f"{name}: 校验失败不得落 ring 文件")

    def test_io_error_exit_code_1(self):
        """I/O 错误(目录无法创建)→ 退出码 1。"""
        blocker = os.path.join(self.dir, "blocker")
        with open(blocker, "w", encoding="utf-8") as f:
            f.write("x")
        r = run_writer("--pod-uid", "pod-a", "--dataset", "a", "--chunks", "s:0",
                       "--orders-dir", os.path.join(blocker, "orders"))
        self.assertEqual(1, r.returncode, r.stderr)
        self.assertIn("ordersrc:", r.stderr)


if __name__ == "__main__":
    unittest.main()
