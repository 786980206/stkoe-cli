"""任务模型：Task / TaskEvent / TaskResult / TaskContext / TaskCancelled

状态机：
    pending → running → succeeded
                       ↘ failed
                       ↘ cancelled
"""
from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone


def _now() -> datetime:
    return datetime.now(timezone.utc)


TERMINAL_STATES = ("succeeded", "failed", "cancelled")


class TaskCancelled(Exception):
    """Handler 主动退出：任务被取消（或收到取消信号后自行抛出）"""


@dataclass
class Task:
    """一个后台任务（进程内 + 持久化到 TaskStore/SQLite）

    只保存元信息与结果引用 result_ref；大结果放 ResultStore，不在此。
    """

    task_id: str
    source: str
    action: str
    args: list[str] = field(default_factory=list)
    state: str = "pending"  # pending/running/succeeded/failed/cancelled
    progress: float = 0.0  # 0 ~ 1
    created_at: datetime = field(default_factory=_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str = ""
    result_ref: str = ""
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def to_dict(self) -> dict:
        """任务概要（``task list`` 输出）：只含元信息，不含大结果"""
        def iso(dt: datetime | None) -> str | None:
            return dt.isoformat() if dt is not None else None

        return {
            "task_id": self.task_id,
            "source": self.source,
            "action": self.action,
            "args": list(self.args),
            "state": self.state,
            "progress": round(self.progress, 4),
            "created_at": iso(self.created_at),
            "started_at": iso(self.started_at),
            "finished_at": iso(self.finished_at),
            "error": self.error,
            "result_ref": self.result_ref,
        }


@dataclass(frozen=True)
class TaskEvent:
    """一条完整事件：progress + message + state 一次 update 产生一条

    存 EventStore/SQLite：task_event(task_id, seq, time, progress, message, data, state)
    """

    task_id: str
    seq: int  # 单调递增（1 起）
    time: datetime
    progress: float
    message: str
    data: str  # 随消息可能附带的数据（JSON 字符串）
    state: str


@dataclass
class TaskResult:
    """Handler 的返回：只带引用，大结果放 ResultStore

    - ``result_ref``：结果引用（相对 data_dir 的路径，如 ``tasks/<id>/<name>``）
    - ``data``：小结果 JSON（终态事件携带，用于 RPC 展示）
    """

    result_ref: str = ""
    data: str = ""


class TaskContext:
    """Handler 运行上下文：进度/消息/状态更新、日志、取消检查、结果落盘

    一次 ``update(progress=, message=, state=)`` 产生一条完整 TaskEvent。
    ``log()`` 只写 task.log（调试/排错），不产生 RPC 事件。
    """

    def __init__(self, manager, task: Task):
        self._manager = manager
        self._task = task

    @property
    def task_id(self) -> str:
        return self._task.task_id

    @property
    def data_dir(self):
        """任务管理器的数据目录（业务模块构造本地存储用）"""
        return self._manager.data_dir

    @property
    def args(self) -> list[str]:
        return self._task.args

    async def update(
        self,
        *,
        progress: float | None = None,
        message: str | None = None,
        state: str | None = None,
    ) -> None:
        """产生一条完整 TaskEvent 并持久化 + 广播给订阅者"""
        self._manager.emit(
            self._task,
            progress=progress,
            message="" if message is None else message,
            state=state,
        )

    async def log(self, message: str) -> None:
        """写详细日志：task/<task_id>/task.log（不产生事件）"""
        self._manager.log(self._task, message)

    def is_cancelled(self) -> bool:
        """任务是否被取消（Handler 应协作式检查并尽早退出）"""
        return self._manager.is_cancelled(self._task)

    def is_paused(self) -> bool:
        """任务是否处于暂停（Handler 在检查点配合暂停）"""
        return self._manager.is_paused(self._task)

    async def wait_if_paused(self) -> None:
        """暂停检查点：若任务已暂停则等待直到恢复或取消"""
        while self.is_paused() and not self.is_cancelled():
            await asyncio.sleep(0.05)

    def put_result(self, name: str, data: bytes) -> str:
        """把结果写入 ResultStore（task/<task_id>/<name>），返回 result_ref"""
        return self._manager.results.put(self._task.task_id, name, data)
