"""SQLite 存储：TaskStore（task 表）+ EventStore（task_event 表）

单库双表：``<data_dir>/tasks.db``（WAL 模式，跨线程安全）。
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from ..jsonutil import dumps_str, loads
from .model import Task, TaskEvent

_SCHEMA = """
CREATE TABLE IF NOT EXISTS task (
    task_id      TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    action       TEXT NOT NULL,
    args         TEXT NOT NULL,          -- JSON 数组
    state        TEXT NOT NULL,
    progress     REAL NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,          -- ISO8601
    started_at   TEXT,
    finished_at  TEXT,
    error        TEXT,
    result_ref   TEXT
);
CREATE TABLE IF NOT EXISTS task_event (
    task_id      TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    time         TEXT NOT NULL,          -- ISO8601
    progress     REAL NOT NULL,
    message      TEXT,
    data         TEXT,
    state        TEXT NOT NULL,
    PRIMARY KEY (task_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_task_event_task ON task_event (task_id, seq);
"""


class _DB:
    """延迟打开的单连接（跨线程 + 锁保护）"""

    def __init__(self, path: Path):
        self._path = path
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    def _ensure(self) -> None:
        if self._conn is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)

    def execute(self, sql: str, params=()):
        with self._lock:
            self._ensure()
            return self._conn.execute(sql, params)

    def commit(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None


def _iso(dt) -> str:
    return dt.isoformat() if dt is not None else None


def _from_iso(s: str | None):
    from datetime import datetime

    return datetime.fromisoformat(s) if s else None


class TaskStore:
    """task 表的读写"""

    def __init__(self, db: _DB):
        self._db = db

    _COLS = ("task_id", "source", "action", "args", "state", "progress",
             "created_at", "started_at", "finished_at", "error", "result_ref")

    def save(self, task: Task) -> None:
        self._db.execute(
            f"""INSERT INTO task ({", ".join(self._COLS)})
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    source=excluded.source, action=excluded.action, args=excluded.args,
                    state=excluded.state, progress=excluded.progress,
                    started_at=excluded.started_at, finished_at=excluded.finished_at,
                    error=excluded.error, result_ref=excluded.result_ref""",
            (task.task_id, task.source, task.action,
             dumps_str(task.args), task.state, task.progress,
             _iso(task.created_at), _iso(task.started_at), _iso(task.finished_at),
             task.error, task.result_ref),
        )
        self._db.commit()

    def get(self, task_id: str) -> Task | None:
        row = self._db.execute(
            "SELECT * FROM task WHERE task_id=?", (task_id,)).fetchone()
        return self._from_row(row) if row is not None else None

    def list(self, state: str | None = None, limit: int = 200) -> list[Task]:
        """任务列表：按创建时间倒序（最新在前）；``state`` 可选按状态过滤"""
        if state:
            rows = self._db.execute(
                "SELECT * FROM task WHERE state=? ORDER BY created_at DESC LIMIT ?",
                (state, limit)).fetchall()
        else:
            rows = self._db.execute(
                "SELECT * FROM task ORDER BY created_at DESC LIMIT ?",
                (limit,)).fetchall()
        return [self._from_row(r) for r in rows]

    @staticmethod
    def _from_row(row) -> Task:
        return Task(
            task_id=row["task_id"], source=row["source"], action=row["action"],
            args=loads(row["args"]), state=row["state"], progress=row["progress"],
            created_at=_from_iso(row["created_at"]),
            started_at=_from_iso(row["started_at"]),
            finished_at=_from_iso(row["finished_at"]),
            error=row["error"] or "", result_ref=row["result_ref"] or "",
        )


class EventStore:
    """task_event 表的追加写 + 查询"""

    def __init__(self, db: _DB):
        self._db = db

    def insert(self, ev: TaskEvent) -> None:
        self._db.execute(
            """INSERT INTO task_event (task_id, seq, time, progress, message, data, state)
               VALUES (?,?,?,?,?,?,?)""",
            (ev.task_id, ev.seq, _iso(ev.time), ev.progress,
             ev.message, ev.data, ev.state),
        )
        self._db.commit()

    def max_seq(self, task_id: str) -> int:
        row = self._db.execute(
            "SELECT MAX(seq) FROM task_event WHERE task_id=?", (task_id,)).fetchone()
        return row[0] or 0

    def list_by_task(self, task_id: str, after_seq: int = 0) -> list[TaskEvent]:
        rows = self._db.execute(
            "SELECT * FROM task_event WHERE task_id=? AND seq>? ORDER BY seq",
            (task_id, after_seq)).fetchall()
        return [TaskEvent(
            task_id=r["task_id"], seq=r["seq"], time=_from_iso(r["time"]),
            progress=r["progress"], message=r["message"] or "",
            data=r["data"] or "", state=r["state"],
        ) for r in rows]
