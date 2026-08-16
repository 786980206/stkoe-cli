"""TaskManager：任务框架编排核心

职责：
- 创建/查询/取消 Task，调度执行（Scheduler）
- Task 状态管理（pending → running → 终态）
- TaskEvent 持久化（EventStore/SQLite）+ 实时订阅（queue）
- 任务日志（LogStore/task.log）与结果引用（ResultStore）

不负责具体业务，也不保存大结果。Handler 通过 (source, action) 在 TaskRegistry 注册。

核心流程：
    SubmitTask → TaskManager → Handler → TaskContext.update()
        → TaskEvent → SQLite + Subscribers → TaskResult → ResultStore → succeeded
"""
from __future__ import annotations

import queue
import threading
import uuid
from concurrent.futures import Future
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from google.protobuf import timestamp_pb2

from ..grpc import stkoe_pb2
from ..jsonutil import dumps_str
from . import handlers as _default_handlers
from .model import Task, TaskCancelled, TaskEvent
from .registry import TaskRegistry
from .results import ResultStore
from .scheduler import Scheduler
from .logs import LogStore
from .store import EventStore, TaskStore, _DB

POLL_INTERVAL = 0.05  # Subscribe 轮询间隔（秒）


def default_data_dir() -> Path:
    return Path.home() / ".stkoe"


class TaskManager:
    """任务管理器：TaskStore + EventStore + TaskRegistry + Scheduler + LogStore + ResultStore"""

    def __init__(self, data_dir: Path | str | None = None) -> None:
        self.data_dir = Path(data_dir).expanduser() if data_dir else default_data_dir()
        db = _DB(self.data_dir / "tasks.db")
        self._db = db
        self.tasks: TaskStore = TaskStore(db)
        self.events: EventStore = EventStore(db)
        self.registry: TaskRegistry = TaskRegistry()
        self.scheduler: Scheduler = Scheduler()
        self.logs: LogStore = LogStore(self.data_dir / "task")
        self.results: ResultStore = ResultStore(self.data_dir / "task")

        self._lock = threading.RLock()
        self._live: dict[str, Task] = {}  # 运行中任务（终态后移除，按需从 SQLite 读）
        self._queues: dict[str, list[queue.Queue]] = {}  # task_id → 订阅队列
        self._cancelled: set[str] = set()
        self._paused: set[str] = set()
        self._futures: dict[str, "Future"] = {}  # task_id → 调度 Future

        _default_handlers.register(self.registry)
        from ..table.handlers import register as _register_table_handlers

        _register_table_handlers(self.registry)
        from ..index.handlers import register as _register_index_handlers

        _register_index_handlers(self.registry)
        from ..panel.handlers import register as _register_panel_handlers

        _register_panel_handlers(self.registry)
        from ..stat.handlers import register as _register_stat_handlers

        _register_stat_handlers(self.registry)
        from ..fieldset.handlers import register as _register_fieldset_handlers

        _register_fieldset_handlers(self.registry)
        from ..sample.handlers import register as _register_sample_handlers

        _register_sample_handlers(self.registry)
        from ..feature.handlers import register as _register_feature_handlers

        _register_feature_handlers(self.registry)
        from ..factor.handlers import register as _register_factor_handlers

        _register_factor_handlers(self.registry)
        from ..factor_test.handlers import register as _register_factor_test_handlers

        _register_factor_test_handlers(self.registry)
        from ..mock.handlers import register as _register_mock_handlers

        _register_mock_handlers(self.registry)

    # ---------- 提交 / 查询 / 取消 ----------

    def submit(self, source: str, action: str, args: list[str]) -> Task:
        task = Task(task_id=uuid.uuid4().hex, source=source,
                    action=action, args=list(args))
        with self._lock:
            self._live[task.task_id] = task
        self.emit(task, state="pending", message=f"任务已创建: {source} {action}")
        with self._lock:
            self._futures[task.task_id] = self.scheduler.submit(self._execute(task))
        return task

    def get(self, task_id: str) -> Task | None:
        """优先内存（运行中），否则从 SQLite 读（含历史任务）"""
        with self._lock:
            task = self._live.get(task_id)
        if task is not None:
            return task
        return self.tasks.get(task_id)

    def cancel(self, task_id: str) -> bool:
        """请求取消：置取消标记；未启动的 pending 任务直接终态 cancelled，
        运行中的任务由 Handler 在检查点看到标记后自行抛出 TaskCancelled 结束
        （协作式尽早退出，不会出现标记被终态流程提前清掉导致任务继续跑）"""
        task = self.get(task_id)
        if task is None or task.is_terminal():
            return False
        with self._lock:
            self._cancelled.add(task_id)
        if task.state == "pending":
            self._finalize(task, "cancelled", message="任务已取消")
        return True

    def pause(self, task_id: str) -> bool:
        """请求暂停：置暂停标记 + 状态 paused（Handler 在检查点等待）"""
        task = self.get(task_id)
        if task is None or task.is_terminal():
            return False
        if self.is_paused(task):
            return True  # 幂等
        with self._lock:
            self._paused.add(task_id)
        self.emit(task, state="paused", message="任务已暂停")
        return True

    def resume(self, task_id: str) -> bool:
        """请求恢复：清除暂停标记 + 状态回到 running"""
        task = self.get(task_id)
        if task is None or task.is_terminal():
            return False
        if not self.is_paused(task):
            return False
        with self._lock:
            self._paused.discard(task_id)
        self.emit(task, state="running", message="任务已恢复")
        return True

    def control(self, task_id: str, action: str) -> tuple[bool, str]:
        """TaskControl RPC 入口：cancel / pause / resume；返回 (ok, message)"""
        if action == "cancel":
            return (self.cancel(task_id),
                    "" if self.get(task_id) else f"task not found: {task_id}")
        if action == "pause":
            return (self.pause(task_id),
                    "" if self.get(task_id) else f"task not found: {task_id}")
        if action == "resume":
            ok = self.resume(task_id)
            task = self.get(task_id)
            if task is None:
                return False, f"task not found: {task_id}"
            return ok, "" if ok else f"任务未处于暂停或已终态: {task_id}"
        return False, f"不支持的任务操作: {action}"

    # ---------- 执行 ----------

    def is_cancelled(self, task: Task) -> bool:
        with self._lock:
            return task.task_id in self._cancelled

    def is_paused(self, task: Task) -> bool:
        with self._lock:
            return task.task_id in self._paused

    def emit(self, task: Task, *, progress: float | None = None,
             message: str = "", data: str = "", state: str | None = None) -> TaskEvent:
        """产生一条完整 TaskEvent：写 EventStore + 更新 Task + 广播给订阅者"""
        with task.lock:
            if state is not None:
                task.state = state
            if progress is not None:
                task.progress = progress
            ev = TaskEvent(
                task_id=task.task_id,
                seq=self.events.max_seq(task.task_id) + 1,
                time=datetime.now(timezone.utc),
                progress=task.progress,
                message=message,
                data=data,
                state=task.state,
            )
            self.events.insert(ev)
            self.tasks.save(task)
        with self._lock:
            for q in self._queues.get(task.task_id, ()):
                q.put(ev)
        return ev

    def log(self, task: Task, message: str) -> None:
        self.logs.log(task.task_id, message)

    async def _execute(self, task: Task) -> None:
        if task.is_terminal():
            return  # 启动前已被 cancel/finalize，跳过执行
        task.started_at = datetime.now(timezone.utc)
        self.emit(task, state="running",
                  message=f"任务开始: {task.source} {task.action}")
        try:
            handler = self.registry.get(task.source, task.action)
            if handler is None:
                raise ValueError(f"不支持的命令: {task.source} {task.action}")

            from .model import TaskContext

            result = await handler.run(TaskContext(self, task))

            if task.is_terminal():
                return  # 运行中被 cancel 已终态，跳过
            self._finalize(task, "succeeded", message="任务完成",
                           data=result.data, result_ref=result.result_ref)
        except TaskCancelled:
            if not task.is_terminal():
                self._finalize(task, "cancelled", message="任务已取消")
        except Exception as e:
            if task.is_terminal():
                return
            task.error = str(e)
            self._finalize(task, "failed", message=str(e))

    def _finalize(self, task: Task, state: str, *, message: str,
                  data: str = "", result_ref: str | None = None) -> None:
        with task.lock:
            if task.is_terminal():
                return  # 幂等：stop 与正常完成竞态下只 finalize 一次
            task.state = state
            task.state = state
            task.finished_at = datetime.now(timezone.utc)
            if result_ref is not None:
                task.result_ref = result_ref
            if state == "succeeded":
                task.progress = 1.0
        with self._lock:
            self._cancelled.discard(task.task_id)
            self._paused.discard(task.task_id)
        self.emit(task, state=state, message=message, data=data,
                  progress=1.0 if state == "succeeded" else None)
        with self._lock:
            self._live.pop(task.task_id, None)
            self._futures.pop(task.task_id, None)

    # ---------- 订阅 ----------

    def subscribe(self, task_id: str, replay: bool) -> Iterator[stkoe_pb2.SubscribeTaskResponse]:
        """订阅任务事件流；replay=True 先回放历史，否则只推订阅后事件；终态后 EOF"""
        task = self.get(task_id)
        if task is None:
            yield stkoe_pb2.SubscribeTaskResponse(
                header=stkoe_pb2.DataHeader(code=2, message=f"task not found: {task_id}"))
            return

        q: queue.Queue = queue.Queue(maxsize=1024)
        with self._lock:
            self._queues.setdefault(task_id, []).append(q)
        try:
            seen = 0 if replay else self.events.max_seq(task_id)
            yield stkoe_pb2.SubscribeTaskResponse(
                header=stkoe_pb2.DataHeader(code=0, message="ok"))
            while True:
                for ev in self.events.list_by_task(task_id, after_seq=seen):
                    seen = ev.seq
                    yield self._event_response(ev)
                try:
                    ev = q.get(timeout=POLL_INTERVAL)
                except queue.Empty:
                    cur = self.get(task_id)
                    if cur is None or cur.is_terminal():
                        # 终态事件先于 _live 摘除落库/入队，等一个轮询周期后补读
                        for ev in self.events.list_by_task(task_id, after_seq=seen):
                            seen = ev.seq
                            yield self._event_response(ev)
                        return  # EOF
                    continue
                if ev.seq > seen:
                    seen = ev.seq
                    yield self._event_response(ev)
        finally:
            with self._lock:
                try:
                    self._queues[task_id].remove(q)
                except (KeyError, ValueError):
                    pass

    @staticmethod
    def _event_response(ev: TaskEvent) -> stkoe_pb2.SubscribeTaskResponse:
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(ev.time)
        return stkoe_pb2.SubscribeTaskResponse(event=stkoe_pb2.TaskEvent(
            seq=ev.seq, time=ts, progress=float(ev.progress),
            message=ev.message, data=ev.data, state=ev.state))

    # ---------- 生命周期 ----------

    def start(self) -> None:
        self.scheduler.start()

    def stop(self) -> None:
        """停止：先在跑任务统一收尾 cancelled → 取消未完成任务、停调度线程、关 SQLite"""
        with self._lock:
            live = list(self._live.values())
        for task in live:
            if not task.is_terminal():
                self._finalize(task, "cancelled", message="任务已取消（服务停止）")
        with self._lock:
            for fut in self._futures.values():
                fut.cancel()
            self._futures.clear()
        self.scheduler.stop()
        self._db.close()

    # 兼容旧字段名（如有外部引用）
    @property
    def result_store(self) -> ResultStore:
        return self.results

    def __repr__(self) -> str:
        return f"<TaskManager data_dir={self.data_dir}>"
