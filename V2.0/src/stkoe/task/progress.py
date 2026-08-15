"""任务进度桥：worker 线程 on_progress 回调 → 事件循环 ctx.update

物化/统计等重活跑在 asyncio.to_thread 的 worker 线程里，无法直接 await ctx。
``worker_on_progress`` 构造一个线程内可安全同步调用的回调：每次触发做
取消检查（协作式抛 TaskCancelled）→ 暂停等待 → 桥回事件循环上报进度。
"""
from __future__ import annotations

import asyncio
import time


def worker_on_progress(ctx, loop: asyncio.AbstractEventLoop):
    """构造 worker 线程可用的 ``on_progress(i, total, msg)`` 回调。

    - 每次回调：``ctx.is_cancelled()`` 抛 ``TaskCancelled`` 协作式退出；
    - 暂停中阻塞等待恢复（等价同步版 ``wait_if_paused``）；
    - 正常则把 ``update(progress=i/total, message="<msg>（i/total）")``
      和 ``log(msg)`` 桥回事件循环执行。
    """
    from ..task.model import TaskCancelled

    def _cb(i: int, total: int, msg: str) -> None:
        if ctx.is_cancelled():
            raise TaskCancelled()
        while ctx.is_paused() and not ctx.is_cancelled():
            time.sleep(0.05)
        if ctx.is_cancelled():
            raise TaskCancelled()

        async def _emit() -> None:
            await ctx.update(progress=i / total, message=f"{msg}（{i}/{total}）")
            await ctx.log(msg)

        try:
            fut = asyncio.run_coroutine_threadsafe(_emit(), loop)
            fut.add_done_callback(lambda f: f.exception())  # 吞掉异常避免告警
        except RuntimeError:
            pass  # 事件循环已关闭（服务停止中）

    return _cb


__all__ = ["worker_on_progress"]
