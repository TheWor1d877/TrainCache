"""钩子发送端：单向 Unix datagram，漏发只计数，绝不打断训练。

socket 不存在、权限不足、发送缓冲满都按"丢一帧"处理——Agent 侧的 inotify 兜底
不依赖这些帧的完整性（docs/6.1 design.md §5.4）。
"""

from __future__ import annotations

import socket
from dataclasses import dataclass

from . import frames


@dataclass
class SendStats:
    sent: int = 0
    dropped: int = 0
    oversized: int = 0


class HookSender:
    def __init__(self, sock_path: str) -> None:
        self.sock_path = sock_path
        self.stats = SendStats()
        self._sock = None

    def _socket(self):
        if self._sock is None:
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        return self._sock

    def send(self, frame: dict) -> bool:
        """发一帧；成功返回 True，任何失败返回 False 并计数，不抛异常。"""
        try:
            payload = frames.encode(frame)
        except ValueError:
            self.stats.oversized += 1
            return False
        try:
            self._socket().sendto(payload, self.sock_path)
        except OSError:
            self.stats.dropped += 1
            return False
        self.stats.sent += 1
        return True

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    @property
    def unreported(self) -> int:
        return self.stats.dropped + self.stats.oversized
