"""stkoe 任务框架

TaskManager
│
├── TaskStore       → SQLite（task 表）
├── EventStore      → SQLite（task_event 表）
├── TaskRegistry    → Handler
├── Scheduler       → asyncio
├── LogStore        → task/<task_id>/task.log
└── ResultStore     → task/<task_id>/<name> 大结果
"""
from .manager import TaskManager, default_data_dir
from .model import Task, TaskCancelled, TaskContext, TaskEvent, TaskResult
from .registry import TaskHandler, TaskRegistry

__all__ = [
    "TaskManager", "default_data_dir",
    "Task", "TaskCancelled", "TaskContext", "TaskEvent", "TaskResult",
    "TaskHandler", "TaskRegistry",
]
