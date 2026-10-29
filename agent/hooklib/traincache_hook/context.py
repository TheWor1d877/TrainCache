"""环境上下文：DataLoader 只读 webhook 注入的 TRAINCACHE_* 变量（docs/8.1 design.md §4.3）。

环境变量缺失时不报错、不猜值：记 context_missing=True，日志照常打，钩子照常发——
训练正确性不依赖这些字段。
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from typing import Mapping, Optional

ENV_TASK = "TRAINCACHE_TASK"
ENV_POD_INDEX = "TRAINCACHE_POD_INDEX"
ENV_DATASET = "TRAINCACHE_DATASET"
ENV_HOOK_SOCK = "TRAINCACHE_HOOK_SOCK"
ENV_GENERATION_AT_START = "TRAINCACHE_GENERATION_AT_START"
DEFAULT_HOOK_SOCK = "/run/traincache/agent.sock"


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


@dataclass
class HookContext:
    """一次训练进程的 DataLoader 上下文，字段名与 8.1 §4.3 的日志字段一一对应。"""

    warmup_task: str = ""
    pod: str = ""
    pod_index: int = 0
    dataset: str = ""
    generation_at_start: str = ""
    hook_sock: str = DEFAULT_HOOK_SOCK
    context_missing: bool = False

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "HookContext":
        env = os.environ if env is None else env
        task = _clean(env.get(ENV_TASK))
        dataset = _clean(env.get(ENV_DATASET))
        socket_path = _clean(env.get(ENV_HOOK_SOCK)) or DEFAULT_HOOK_SOCK
        raw_index = _clean(env.get(ENV_POD_INDEX))
        try:
            pod_index = int(raw_index)
        except ValueError:
            pod_index = 0
        missing = not (task and dataset and raw_index)
        return cls(
            warmup_task=task,
            pod=_pod_name(env),
            pod_index=pod_index,
            dataset=dataset,
            generation_at_start=_clean(env.get(ENV_GENERATION_AT_START)),
            hook_sock=socket_path,
            context_missing=missing,
        )

    def log_fields(self) -> dict:
        """启动与汇总日志共用的上下文字段。"""
        fields = {
            "warmup_task": self.warmup_task,
            "pod": self.pod,
            "pod_index": self.pod_index,
            "dataset": self.dataset,
            "generation_at_start": self.generation_at_start,
            "hook_sock": self.hook_sock,
        }
        if self.context_missing:
            fields["context_missing"] = True
        return fields


def _pod_name(env: Mapping[str, str]) -> str:
    """Pod 名：kubelet 把 HOSTNAME 设为 Pod 名；取不到就用主机名，不回退到 IP。"""
    return _clean(env.get("HOSTNAME")) or socket.gethostname()
