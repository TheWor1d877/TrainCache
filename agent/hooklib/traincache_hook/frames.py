"""五帧构造：字段与 agent/svc/hooksock.go 的 hookFrame 逐字一致（docs/6.1 design.md §5.4）。

单帧上限 4 KiB，超限由发送端丢弃并计数，不截断——截断的 JSON 在 Agent 侧同样算解析失败。
"""

from __future__ import annotations

import json
import time
from typing import Optional

MAX_FRAME_BYTES = 4096

FRAME_ADVANCE = "advance"
FRAME_STAGE_SWITCH = "stage_switch"
FRAME_OPEN_REPORT = "open_report"
FRAME_READ_MISS = "read_miss"
FRAME_READ_STALL = "read_stall"


def _timestamp() -> str:
    """RFC3339Nano：纳秒精度，Agent 按 ts 做 ±5s 关联。"""
    now = time.time()
    secs = int(now)
    nanos = int(round((now - secs) * 1e9))
    if nanos >= 1_000_000_000:
        secs += 1
        nanos -= 1_000_000_000
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(secs))
    frac = ("%09d" % nanos).rstrip("0") or "0"
    return base + "." + frac + "Z"


def advance(path: str, chunk: int) -> dict:
    return {"type": FRAME_ADVANCE, "path": path, "chunk": int(chunk)}


def open_report(path: str, chunk: int) -> dict:
    return {"type": FRAME_OPEN_REPORT, "path": path, "chunk": int(chunk)}


def read_miss(path: str, chunk: int, reason: str = "absent", ts: Optional[str] = None) -> dict:
    if reason not in ("absent", "corrupt"):
        reason = "absent"
    return {"type": FRAME_READ_MISS, "path": path, "chunk": int(chunk), "reason": reason, "ts": ts or _timestamp()}


def read_stall(path: str, chunk: int, wait_ms: int, ts: Optional[str] = None) -> dict:
    return {
        "type": FRAME_READ_STALL,
        "path": path,
        "chunk": int(chunk),
        "wait_ms": int(wait_ms),
        "ts": ts or _timestamp(),
    }


def stage_switch(stage_id: str, shuffle_seed: int = 0) -> dict:
    return {"type": FRAME_STAGE_SWITCH, "stage_id": stage_id, "shuffle_seed": int(shuffle_seed)}


def encode(frame: dict) -> bytes:
    """编码为一行 JSON（含换行）；超过 4 KiB 抛 ValueError，由发送端计数丢弃。"""
    payload = (json.dumps(frame, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("hook frame exceeds %d bytes" % MAX_FRAME_BYTES)
    return payload
