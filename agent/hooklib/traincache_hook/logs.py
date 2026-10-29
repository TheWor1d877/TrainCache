"""JSON 行日志：字段按 docs/8.1 design.md §3.1 公共字段 + §4.3 DataLoader 契约。

只写 stdout，不改 handler、不加依赖；component 固定 dataloader，log_schema_version
从 1 起只增不改。
"""

from __future__ import annotations

import json
import sys
import time
from typing import Optional, TextIO

LOG_SCHEMA_VERSION = 1
COMPONENT = "dataloader"


class JsonLogger:
    def __init__(self, context_fields: dict, version: str = "", stream: Optional[TextIO] = None) -> None:
        self._context_missing = bool(context_fields.get("context_missing"))
        self._fields = {k: v for k, v in context_fields.items() if k != "context_missing"}
        self.version = version
        self.stream = stream if stream is not None else sys.stdout

    def log(self, level: str, msg: str, **fields) -> dict:
        record = {
            "time": _timestamp(),
            "level": level,
            "msg": msg,
            "log_schema_version": LOG_SCHEMA_VERSION,
            "component": COMPONENT,
        }
        if self.version:
            record["version"] = self.version
        record.update(self._fields)
        if self._context_missing:
            record["context_missing"] = True
        for key, value in fields.items():
            if value is not None:
                record[key] = value
        self.stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.stream.flush()
        return record

    def debug(self, msg: str, **fields) -> dict:
        return self.log("DEBUG", msg, **fields)

    def info(self, msg: str, **fields) -> dict:
        return self.log("INFO", msg, **fields)

    def warn(self, msg: str, **fields) -> dict:
        return self.log("WARN", msg, **fields)

    def error(self, msg: str, **fields) -> dict:
        return self.log("ERROR", msg, **fields)


def _timestamp() -> str:
    now = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + ("%.3f" % (now % 1))[1:] + "Z"
