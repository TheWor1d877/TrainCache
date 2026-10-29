"""torch Dataset / IterableDataset 包装：计时、发帧、打日志。

包装层不改数据，只旁路观测：读取异常原样抛出，发帧失败只计数。ref(sample) 返回
(path, chunk) 时才发帧——没有块身份的样本（图片、token 之类）只计时。
"""

from __future__ import annotations

import atexit
import threading
import time
from typing import Callable, Iterator, Optional

from . import frames
from .context import HookContext
from .logs import JsonLogger
from .sender import HookSender

DEFAULT_STALL_MS = 100
DEFAULT_SUMMARY_INTERVAL = 60.0


class _Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.batches = 0
        self.wait_ms = 0
        self.latencies = []
        self.last_summary = time.perf_counter()

    def record(self, latency_ms: int) -> None:
        with self.lock:
            self.batches += 1
            self.wait_ms += latency_ms
            self.latencies.append(latency_ms)

    def snapshot(self):
        with self.lock:
            lat = sorted(self.latencies)
            self.latencies = []
            return self.batches, self.wait_ms, lat


def _percentile(sorted_values, pct: int) -> int:
    if not sorted_values:
        return 0
    idx = max(0, min(len(sorted_values) - 1, int(round(pct / 100.0 * (len(sorted_values) - 1)))))
    return int(sorted_values[idx])


class _Runtime:
    """一次包装共享的发送端、日志与统计。"""

    def __init__(
        self,
        context: Optional[HookContext],
        logger: Optional[JsonLogger],
        sender: Optional[HookSender],
        phase: str,
        stall_ms: int,
    ) -> None:
        self.context = context or HookContext.from_env()
        self.logger = logger or JsonLogger(self.context.log_fields())
        self.sender = sender or HookSender(self.context.hook_sock)
        self.phase = phase
        self.stall_ms = stall_ms
        self.stats = _Stats()
        self._closed = False
        atexit.register(self.close)
        self.logger.info("dataloader started", dataset=self.context.dataset, hook_sock=self.context.hook_sock)

    def emit(self, frame: dict) -> None:
        self.sender.send(frame)

    def observe(self, ref: Optional[tuple], latency_ms: int, stage_id: Optional[str] = None) -> None:
        self.stats.record(latency_ms)
        self.logger.debug(
            "sample read",
            chunk=ref[1] if ref else None,
            wait_ms=latency_ms,
            phase=stage_id or self.phase,
        )
        if ref is not None:
            path, chunk = ref
            self.emit(frames.open_report(path, chunk))
            self.emit(frames.advance(path, chunk))
            if latency_ms >= self.stall_ms:
                self.emit(frames.read_stall(path, chunk, latency_ms))
        self._maybe_summary()

    def _maybe_summary(self) -> None:
        now = time.perf_counter()
        if now - self.stats.last_summary < DEFAULT_SUMMARY_INTERVAL:
            return
        self.stats.last_summary = now
        batches, wait_ms, lat = self.stats.snapshot()
        if not lat:
            return
        self.logger.info(
            "dataloader summary",
            dataset=self.context.dataset,
            phase=self.phase,
            batches=batches,
            p50_ms=_percentile(lat, 50),
            p95_ms=_percentile(lat, 95),
            p99_ms=_percentile(lat, 99),
            stall_ms=wait_ms,
        )

    def miss(self, ref: Optional[tuple], err: BaseException) -> None:
        reason = "corrupt" if isinstance(err, ValueError) else "absent"
        err_class = "not_found" if isinstance(err, FileNotFoundError) else "io"
        self.logger.warn(
            "sample read failed",
            dataset=self.context.dataset,
            reason=reason,
            err_class=err_class,
            fallback="raise",
        )
        if ref is not None:
            self.emit(frames.read_miss(ref[0], ref[1], reason=reason))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        batches, wait_ms, lat = self.stats.snapshot()
        if lat:
            self.logger.info(
                "dataloader summary",
                dataset=self.context.dataset,
                phase=self.phase,
                batches=batches,
                p50_ms=_percentile(lat, 50),
                p95_ms=_percentile(lat, 95),
                p99_ms=_percentile(lat, 99),
                stall_ms=wait_ms,
            )
        self.logger.info(
            "dataloader stopped",
            dataset=self.context.dataset,
            batches=self.stats.batches,
            total_wait_ms=self.stats.wait_ms,
            unreported_frames=self.sender.unreported,
        )
        self.sender.close()


class _WrappedDataset:
    def __init__(self, dataset, runtime: _Runtime, ref: Optional[Callable], ref_index: Optional[Callable], stage_id: Optional[str]) -> None:
        self._dataset = dataset
        self._runtime = runtime
        self._ref = ref
        self._ref_index = ref_index
        self._stage_id = stage_id

    def __len__(self):
        return len(self._dataset)

    def __getattr__(self, name):
        return getattr(self.__dict__["_dataset"], name)

    def __getitem__(self, index):
        started = time.perf_counter()
        try:
            sample = self._dataset[index]
            ref = self._ref(sample) if self._ref else None
        except Exception as err:  # 适配器只旁路观测，异常原样抛出
            # 样本本身没读到，块身份只能由 ref_index(index) 提供；没给就只记日志。
            ref = self._ref_index(index) if self._ref_index else None
            self._runtime.miss(ref, err)
            raise
        latency_ms = int((time.perf_counter() - started) * 1000)
        self._runtime.observe(ref, latency_ms, self._stage_id)
        return sample

    def set_epoch(self, epoch: int, shuffle_seed: int = 0) -> None:
        """每个 epoch 开头发 stage_switch；底层有 set_epoch 时一并转发。"""
        self._stage_id = "epoch-%d" % epoch
        self._runtime.emit(frames.stage_switch(self._stage_id, shuffle_seed))
        inner = getattr(self._dataset, "set_epoch", None)
        if callable(inner):
            try:
                inner(epoch)
            except TypeError:
                inner()


class _WrappedIterable:
    def __init__(self, dataset, runtime: _Runtime, ref: Optional[Callable], stage_id: Optional[str]) -> None:
        self._dataset = dataset
        self._runtime = runtime
        self._ref = ref
        self._stage_id = stage_id

    def __len__(self):
        return len(self._dataset)

    def __getattr__(self, name):
        return getattr(self.__dict__["_dataset"], name)

    def __iter__(self) -> Iterator:
        for sample in self._dataset:
            started = time.perf_counter()
            try:
                ref = self._ref(sample) if self._ref else None
            except Exception as err:
                self._runtime.miss(None, err)
                raise
            latency_ms = int((time.perf_counter() - started) * 1000)
            self._runtime.observe(ref, latency_ms, self._stage_id)
            yield sample

    def set_epoch(self, epoch: int, shuffle_seed: int = 0) -> None:
        self._stage_id = "epoch-%d" % epoch
        self._runtime.emit(frames.stage_switch(self._stage_id, shuffle_seed))
        inner = getattr(self._dataset, "set_epoch", None)
        if callable(inner):
            try:
                inner(epoch)
            except TypeError:
                inner()


def _runtime(context, logger, sender, phase: str, stall_ms: int) -> _Runtime:
    return _Runtime(context, logger, sender, phase, stall_ms)


def wrap_dataset(
    dataset,
    ref: Optional[Callable] = None,
    ref_index: Optional[Callable] = None,
    *,
    context: Optional[HookContext] = None,
    logger: Optional[JsonLogger] = None,
    sender: Optional[HookSender] = None,
    phase: str = "startup",
    stage_id: Optional[str] = None,
    stall_ms: int = DEFAULT_STALL_MS,
):
    """包装 map-style Dataset：每次 __getitem__ 计时，并按 ref 给出的块身份发帧。

    ref_index(index) 可选：只在读取抛异常时用来补 read_miss 的块身份，正常路径不调用。
    """
    return _WrappedDataset(dataset, _runtime(context, logger, sender, phase, stall_ms), ref, ref_index, stage_id)


def wrap_iterable_dataset(
    dataset,
    ref: Optional[Callable] = None,
    *,
    context: Optional[HookContext] = None,
    logger: Optional[JsonLogger] = None,
    sender: Optional[HookSender] = None,
    phase: str = "startup",
    stage_id: Optional[str] = None,
    stall_ms: int = DEFAULT_STALL_MS,
):
    """包装 IterableDataset：每产出一个样本计时并发帧。"""
    return _WrappedIterable(dataset, _runtime(context, logger, sender, phase, stall_ms), ref, stage_id)
