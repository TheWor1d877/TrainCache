"""hooklib 单测：真 Unix datagram socket + 纯标准库，不依赖 torch。"""

import io
import json
import os
import socket
import tempfile
import time
import unittest

from traincache_hook import HookContext, HookSender, JsonLogger, wrap_dataset, wrap_iterable_dataset
from traincache_hook import frames


class _SockCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "agent.sock")
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.server.bind(self.path)
        self.server.settimeout(0.5)

    def tearDown(self):
        self.server.close()
        self.tmp.cleanup()

    def drain(self):
        frames_out = []
        while True:
            try:
                data = self.server.recv(8192)
            except socket.timeout:
                return frames_out
            frames_out.append(json.loads(data.decode("utf-8")))


class FrameTest(unittest.TestCase):
    def test_frame_fields_match_agent_contract(self):
        self.assertEqual(frames.advance("a.tar", 3), {"type": "advance", "path": "a.tar", "chunk": 3})
        self.assertEqual(frames.open_report("a.tar", 3), {"type": "open_report", "path": "a.tar", "chunk": 3})
        self.assertEqual(frames.stage_switch("epoch-2", 1), {"type": "stage_switch", "stage_id": "epoch-2", "shuffle_seed": 1})
        miss = frames.read_miss("a.tar", 3)
        self.assertEqual(miss["type"], "read_miss")
        self.assertEqual(miss["reason"], "absent")
        self.assertTrue(miss["ts"].endswith("Z"))
        stall = frames.read_stall("a.tar", 3, 250)
        self.assertEqual(stall["wait_ms"], 250)

    def test_encode_rejects_oversized_frame(self):
        with self.assertRaises(ValueError):
            frames.encode(frames.advance("x" * 5000, 0))


class SenderTest(_SockCase):
    def test_sends_datagram(self):
        sender = HookSender(self.path)
        self.assertTrue(sender.send(frames.advance("a.tar", 1)))
        got = self.drain()
        self.assertEqual(got, [{"type": "advance", "path": "a.tar", "chunk": 1}])
        self.assertEqual(sender.stats.sent, 1)

    def test_missing_socket_is_counted_not_raised(self):
        sender = HookSender(os.path.join(self.tmp.name, "missing.sock"))
        self.assertFalse(sender.send(frames.advance("a.tar", 1)))
        self.assertEqual(sender.stats.dropped, 1)
        self.assertEqual(sender.unreported, 1)

    def test_oversized_is_counted(self):
        sender = HookSender(self.path)
        self.assertFalse(sender.send(frames.advance("x" * 5000, 0)))
        self.assertEqual(sender.stats.oversized, 1)


class ContextTest(unittest.TestCase):
    def test_reads_webhook_env(self):
        ctx = HookContext.from_env({
            "TRAINCACHE_TASK": "train/warm",
            "TRAINCACHE_POD_INDEX": "3",
            "TRAINCACHE_DATASET": "train/coco",
            "TRAINCACHE_HOOK_SOCK": "/run/traincache/agent.sock",
            "TRAINCACHE_GENERATION_AT_START": "7",
            "HOSTNAME": "train-pod-0",
        })
        self.assertFalse(ctx.context_missing)
        self.assertEqual(ctx.warmup_task, "train/warm")
        self.assertEqual(ctx.pod_index, 3)
        self.assertEqual(ctx.dataset, "train/coco")
        self.assertEqual(ctx.generation_at_start, "7")
        self.assertEqual(ctx.pod, "train-pod-0")

    def test_missing_env_degrades(self):
        ctx = HookContext.from_env({})
        self.assertTrue(ctx.context_missing)
        self.assertEqual(ctx.hook_sock, "/run/traincache/agent.sock")
        self.assertTrue(ctx.log_fields()["context_missing"])


class _Sample:
    def __init__(self, path, chunk, delay=0.0, err=None):
        self.path = path
        self.chunk = chunk
        self.delay = delay
        self.err = err


class _Dataset:
    def __init__(self, samples):
        self.samples = samples

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, i):
        s = self.samples[i]
        if s.delay:
            time.sleep(s.delay)
        if s.err:
            raise s.err
        return s


class _Iterable:
    def __init__(self, samples):
        self.samples = samples

    def __iter__(self):
        return iter(self.samples)


def _ref(sample):
    return (sample.path, sample.chunk)


class WrapTest(_SockCase):
    def _context(self):
        return HookContext(warmup_task="train/warm", dataset="train/coco", pod="p0", pod_index=0, hook_sock=self.path)

    def _logger(self):
        self.stream = io.StringIO()
        return JsonLogger(self._context().log_fields(), stream=self.stream)

    def test_dataset_emits_open_and_advance(self):
        ds = wrap_dataset(_Dataset([_Sample("a.tar", 0)]), _ref, context=self._context(), logger=self._logger())
        ds[0]
        types = [f["type"] for f in self.drain()]
        self.assertEqual(types, ["open_report", "advance"])

    def test_iterable_emits_advance(self):
        ds = wrap_iterable_dataset(_Iterable([_Sample("a.tar", 0), _Sample("a.tar", 1)]), _ref,
                                   context=self._context(), logger=self._logger())
        list(ds)
        types = [f["type"] for f in self.drain()]
        self.assertEqual(types, ["open_report", "advance", "open_report", "advance"])

    def test_slow_read_emits_stall(self):
        ds = wrap_dataset(_Dataset([_Sample("a.tar", 0, delay=0.12)]), _ref, context=self._context(), logger=self._logger())
        ds[0]
        stall = [f for f in self.drain() if f["type"] == "read_stall"]
        self.assertEqual(len(stall), 1)
        self.assertGreaterEqual(stall[0]["wait_ms"], 100)

    def test_miss_reports_and_reraises(self):
        ds = wrap_dataset(_Dataset([_Sample("a.tar", 0, err=FileNotFoundError("gone"))]), _ref,
                          ref_index=lambda i: ("a.tar", 0),
                          context=self._context(), logger=self._logger())
        with self.assertRaises(FileNotFoundError):
            ds[0]
        got = self.drain()
        self.assertEqual([f["type"] for f in got], ["read_miss"])
        self.assertEqual(got[0]["reason"], "absent")
        records = [json.loads(line) for line in self.stream.getvalue().splitlines()]
        failed = [r for r in records if r["msg"] == "sample read failed"][0]
        self.assertEqual(failed["level"], "WARN")
        self.assertEqual(failed["err_class"], "not_found")

    def test_startup_and_exit_records(self):
        ds = wrap_dataset(_Dataset([_Sample("a.tar", 0)]), _ref, context=self._context(), logger=self._logger())
        ds[0]
        ds._runtime.close()
        records = [json.loads(line) for line in self.stream.getvalue().splitlines()]
        self.assertEqual(records[0]["component"], "dataloader")
        self.assertEqual(records[0]["log_schema_version"], 1)
        self.assertEqual(records[0]["msg"], "dataloader started")
        stopped = [r for r in records if r["msg"] == "dataloader stopped"][0]
        self.assertEqual(stopped["batches"], 1)
        self.assertIn("total_wait_ms", stopped)
        self.assertIn("unreported_frames", stopped)


if __name__ == "__main__":
    unittest.main()
