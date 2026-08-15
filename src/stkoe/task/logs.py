"""任务日志：task/<task_id>/task.log（详细日志，与 RPC 事件分离）

区别：
- TaskEvent → RPC / 状态（进度/消息/状态）
- task.log  → 调试 / 排错（详细日志）
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


class LogStore:
    def __init__(self, root: Path):
        self.root = root

    def dir(self, task_id: str) -> Path:
        return self.root / task_id

    def log(self, task_id: str, message: str) -> None:
        d = self.dir(task_id)
        d.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat()
        with (d / "task.log").open("a", encoding="utf-8") as f:
            f.write(f"{ts}  {message}\n")
