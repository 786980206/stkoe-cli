"""asyncio 调度器：独立事件循环线程，Submit 的 Handler 在此执行

- async I/O  → 直接 await
- 阻塞操作   → ``scheduler.run_blocking``（线程池）
- CPU 密集   → 子进程（Handler 自行决定）
- Polars/Arrow 原生多线程计算 → 直接执行
"""
from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future


class Scheduler:
    """一个跑在专属线程里的 asyncio 事件循环"""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._loop is not None:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name="stkoe-scheduler", daemon=True)
        self._thread.start()

    def submit(self, coro) -> Future:
        """把协程调度到循环线程执行，返回 concurrent.futures.Future"""
        self.start()
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def run_blocking(self, fn, *args, **kwargs):
        """把阻塞函数丢到线程池，返回可 await 的对象"""
        self.start()
        fut = self._loop.run_in_executor(
            None, lambda: fn(*args, **kwargs))
        return asyncio.wrap_future(fut)

    def stop(self) -> None:
        """停止：取消未完成任务，排空后关闭循环（避免 pending task 告警）"""
        if self._loop is None:
            return
        loop, self._loop = self._loop, None

        def _shutdown() -> None:
            pending = [
                t for t in asyncio.all_tasks(loop)
                if t is not asyncio.current_task()
            ]
            for t in pending:
                t.cancel()
            if pending:
                async def _drain() -> None:
                    await asyncio.gather(*pending, return_exceptions=True)
                    loop.stop()
                asyncio.ensure_future(_drain())
            else:
                loop.stop()

        loop.call_soon_threadsafe(_shutdown)
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None
