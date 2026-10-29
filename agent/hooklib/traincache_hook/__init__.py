"""TrainCache DataLoader 适配器：发五帧钩子，按 8.1 §4.3 打日志。

用法（torch Dataset）：

    from traincache_hook import wrap_dataset

    loader = DataLoader(wrap_dataset(ds, ref=lambda s: (s.path, s.chunk)), batch_size=8)

ref 把样本映射到 (path, chunk)；返回 None 的样本只计时、不发帧。适配器不 import
torch：任何提供 __len__/__getitem__（或 __iter__）的对象都能包。
"""

from .context import HookContext
from .logs import JsonLogger
from .sender import HookSender
from .torch import wrap_dataset, wrap_iterable_dataset

__all__ = ["HookContext", "JsonLogger", "HookSender", "wrap_dataset", "wrap_iterable_dataset"]
