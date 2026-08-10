"""Handler 注册表：通过 (source, action) → TaskHandler 注册与查找

未命中时回退到 ``(source, "")``（action 省略视为默认动作）。
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from .model import TaskResult

if TYPE_CHECKING:
    from .model import TaskContext


class TaskHandler:
    """业务处理器：负责执行具体业务，通过 ctx 汇报进度/日志，返回 TaskResult

    ``run`` 内阻塞操作应 ``await asyncio.to_thread(...)``；纯 Python CPU 密集
    可放子进程；Polars/Arrow 原生多线程计算直接执行。
    """

    async def run(self, ctx: "TaskContext") -> TaskResult:
        raise NotImplementedError


class TaskRegistry:
    def __init__(self) -> None:
        self._handlers: dict[tuple[str, str], TaskHandler] = {}

    def register(self, source: str, action: str, handler: TaskHandler) -> None:
        self._handlers[(source, action)] = handler

    def get(self, source: str, action: str) -> TaskHandler | None:
        h = self._handlers.get((source, action))
        if h is None:
            h = self._handlers.get((source, ""))
        return h

    def actions(self, source: str) -> list[str]:
        return sorted(a for (s, a) in self._handlers if s == source)
