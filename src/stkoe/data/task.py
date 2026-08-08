"""任务管理：stkoe_tasks 登记 + 同步/后台执行 + 日志/进度/暂停/取消

状态机：``submitted -> running <-> paused -> succeeded|failed|cancelled``

- 进度：``progress``(0..1) 与 ``stage``(当前活动) 持久化；flush 节流（分区边界批量落盘）
- 日志：批量写 ``stkoe_task_logs``，``task_log()`` 按 ``seq`` 增量拉取（REPL/grpc 轮询）
- pause/stop 协作式：任务在分区边界调用 ``ctl.check()``；纯顺序快任务不支持打断
- 并发安全：后台任务用独立连接（``catalog().new_conn()``），WAL 多连接读写分离
"""
import threading
import time
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field

from .catalog.spec import TaskHandle, TaskLog
from .util import now

MAX_WORKERS = 4
PAUSE_POLL = 0.2  # 暂停时轮询间隔（秒）

_executor: ThreadPoolExecutor | None = None
_controls: dict[str, "TaskControl"] = {}
_reg_lock = threading.Lock()


class TaskCancelled(Exception):
    """协作式取消：``ctl.check()`` 抛出，任务方在边界捕获退出"""


@contextmanager
def conn_txn(conn):
    """任意连接的事务上下文（后台 worker 连接专用）"""
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


@dataclass
class TaskControl:
    """任务执行上下文：日志/进度/阶段 + 协作式暂停取消（跨线程安全）"""

    task_id: str
    type: str
    object_ref: str
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _progress: float = 0.0
    _stage: str = ""
    _paused: bool = False
    _cancelled: bool = False
    _seq: int = 0
    _log: list[tuple[str, str, str]] = field(default_factory=list)  # (ts, level, msg)
    _dirty: bool = True

    # --- 日志 ---
    def log(self, level: str, msg: str) -> None:
        with self._lock:
            self._log.append((now(), level, msg))
            self._dirty = True

    def info(self, msg: str) -> None:
        self.log("INFO", msg)

    def debug(self, msg: str) -> None:
        self.log("DEBUG", msg)

    def warning(self, msg: str) -> None:
        self.log("WARNING", msg)

    def error(self, msg: str) -> None:
        self.log("ERROR", msg)

    # --- 进度 / 阶段 ---
    def progress(self, value: float, msg: str | None = None) -> None:
        with self._lock:
            self._progress = max(0.0, min(1.0, float(value)))
            if msg:
                self._stage = msg
            self._dirty = True

    def stage(self, msg: str) -> None:
        with self._lock:
            self._stage = msg
            self._dirty = True

    # --- 协作控制（分区边界调用）---
    def check(self) -> None:
        """暂停时阻塞等待；取消时抛 TaskCancelled"""
        while True:
            with self._lock:
                cancelled, paused = self._cancelled, self._paused
            if cancelled:
                raise TaskCancelled(self.object_ref)
            if not paused:
                return
            time.sleep(PAUSE_POLL)

    def pause(self) -> None:
        with self._lock:
            self._paused = True

    def resume(self) -> None:
        with self._lock:
            self._paused = False

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True

    # --- 持久化（flush 由任务方在边界调用；worker 结束兜底）---
    def flush(self, conn) -> None:
        with self._lock:
            if not self._dirty and not self._log:
                return
            logs = list(self._log)
            self._log.clear()
            progress, stage = self._progress, self._stage
            self._dirty = False
        with conn_txn(conn):
            for ts, level, msg in logs:
                self._seq += 1
                conn.execute(
                    "INSERT INTO stkoe_task_logs (task_id, seq, ts, level, message) "
                    "VALUES (?,?,?,?,?)",
                    (self.task_id, self._seq, ts, level, msg),
                )
            conn.execute(
                "UPDATE stkoe_tasks SET progress=?, stage=?, updated_at=? WHERE task_id=?",
                (progress, stage, now(), self.task_id),
            )


def _pool() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="stkoe-task")
    return _executor


def _set_status(conn, task_id: str, status: str, *, error: str | None = None,
                finished: bool = False) -> None:
    sets, args = ["status=?", "updated_at=?"], [status, now()]
    if error is not None:
        sets.append("error=?")
        args.append(error)
    if finished:
        sets.append("finished_at=?")
        args.append(now())
    args.append(task_id)
    with conn_txn(conn):
        conn.execute(f"UPDATE stkoe_tasks SET {', '.join(sets)} WHERE task_id=?", args)


def _handle(ctl: TaskControl, status: str, error: str | None = None) -> TaskHandle:
    return TaskHandle(task_id=ctl.task_id, type=ctl.type, object_ref=ctl.object_ref,
                      status=status, progress=ctl._progress, stage=ctl._stage, error=error)


def _worker(ctl: TaskControl, fn) -> TaskHandle:
    """后台执行体：fn(conn, ctl)，fn 自行管理事务；失败/取消记录进 tasks"""
    from . import catalog
    conn = catalog().new_conn()
    status = "succeeded"
    error = None
    try:
        _set_status(conn, ctl.task_id, "running")
        fn(conn, ctl)
        _set_status(conn, ctl.task_id, "succeeded", finished=True)
    except TaskCancelled:
        status = "cancelled"
        _set_status(conn, ctl.task_id, "cancelled", finished=True)
    except Exception as e:
        status = "failed"
        error = f"{type(e).__name__}: {e}"
        ctl.error(error)
        _set_status(conn, ctl.task_id, "failed", error=error, finished=True)
    finally:
        try:
            ctl.flush(conn)
        except Exception:
            pass
        conn.close()
        with _reg_lock:
            _controls.pop(ctl.task_id, None)
    return _handle(ctl, status, error)


def run_task(task_type: str, object_ref: str, fn, *, background: bool = False) -> TaskHandle:
    """登记任务并执行：``fn(conn, ctl)``；``background=True`` 提交线程池立即返回

    - ``fn`` 需自行管理事务（用 ``conn_txn`` / ``ctl.flush(conn)``）
    - 同步调用（默认）复用当前线程，返回最终状态
    """
    from . import catalog
    task_id = uuid.uuid4().hex
    ctl = TaskControl(task_id=task_id, type=task_type, object_ref=object_ref)
    with catalog().txn() as conn:
        conn.execute(
            "INSERT INTO stkoe_tasks (task_id, type, object_ref, status, progress, created_at) "
            "VALUES (?,?,?,?,0,?)",
            (task_id, task_type, object_ref, "submitted", now()),
        )
    with _reg_lock:
        _controls[task_id] = ctl
    if background:
        _pool().submit(_worker, ctl, fn)
        return _handle(ctl, "submitted")
    return _worker(ctl, fn)


# ---------- 任务管理 API ----------

def _to_handle(row) -> TaskHandle:
    return TaskHandle(task_id=row["task_id"], type=row["type"], object_ref=row["object_ref"],
                      status=row["status"], progress=row["progress"] or 0.0,
                      stage=row["stage"] or "", error=row["error"])


def task_list(*, status: str | None = None, type: str | None = None,
              limit: int = 100) -> list[TaskHandle]:
    """任务列表（按创建时间倒序；可按 status/type 过滤）"""
    from . import catalog
    sql = "SELECT * FROM stkoe_tasks WHERE 1=1"
    args: list = []
    if status is not None:
        sql += " AND status=?"
        args.append(status)
    if type is not None:
        sql += " AND type=?"
        args.append(type)
    sql += " ORDER BY rowid DESC LIMIT ?"
    args.append(limit)
    rows = catalog().conn.execute(sql, args).fetchall()
    return [_to_handle(r) for r in rows]


def _running_control(task_id: str) -> TaskControl:
    with _reg_lock:
        ctl = _controls.get(task_id)
    if ctl is None:
        raise KeyError(f"task not running: {task_id}")
    return ctl


def task_pause(task_id: str) -> TaskHandle:
    """暂停运行中任务（协作式，下一个分区边界生效）"""
    from . import catalog
    ctl = _running_control(task_id)
    ctl.pause()
    with catalog().txn() as conn:
        conn.execute("UPDATE stkoe_tasks SET status='paused', updated_at=? WHERE task_id=?",
                     (now(), task_id))
    return _handle(ctl, "paused")


def task_resume(task_id: str) -> TaskHandle:
    """恢复已暂停任务"""
    from . import catalog
    ctl = _running_control(task_id)
    ctl.resume()
    with catalog().txn() as conn:
        conn.execute("UPDATE stkoe_tasks SET status='running', updated_at=? WHERE task_id=?",
                     (now(), task_id))
    return _handle(ctl, "running")


def task_stop(task_id: str) -> TaskHandle:
    """停止任务（协作式：下次 check() 抛 TaskCancelled；worker 收尾置 cancelled）"""
    ctl = _running_control(task_id)
    ctl.cancel()
    return _handle(ctl, _current_status(task_id))


def task_stop_all(timeout: float = 5.0) -> int:
    """停止所有运行/暂停任务（协作式取消）并等待其收尾到完成态；返回已停止数

    ``task_clean()`` 需在任务进入完成态后才能真正删除，故此处先等待 worker 收尾。
    """
    from . import catalog
    with _reg_lock:
        ids = list(_controls.keys())
    for tid in ids:
        ctl = _controls.get(tid)
        if ctl is not None:
            ctl.cancel()
    deadline = time.time() + timeout
    while ids and time.time() < deadline:
        ph = ",".join("?" * len(ids))
        row = catalog().conn.execute(
            f"SELECT COUNT(*) AS n FROM stkoe_tasks "
            f"WHERE task_id IN ({ph}) AND status IN ('submitted','running','paused')",
            ids,
        ).fetchone()
        if row["n"] == 0:
            break
        time.sleep(0.05)
    return len(ids)


def task_clean() -> int:
    """删除全部完成态任务（succeeded/failed/cancelled，日志级联删除）；返回删除条数"""
    from . import catalog
    with catalog().txn() as conn:
        cur = conn.execute(
            "DELETE FROM stkoe_tasks WHERE status IN ('succeeded','failed','cancelled')")
        return cur.rowcount


def _current_status(task_id: str) -> str:
    from . import catalog
    row = catalog().conn.execute(
        "SELECT status FROM stkoe_tasks WHERE task_id=?", (task_id,)
    ).fetchone()
    return row["status"] if row else "unknown"


def task_log(task_id: str, *, after_seq: int = 0, limit: int = 500) -> list[TaskLog]:
    """任务日志（按 seq 增量拉取：after_seq=上次最大 seq）"""
    from . import catalog
    rows = catalog().conn.execute(
        "SELECT id, task_id, seq, ts, level, message FROM stkoe_task_logs "
        "WHERE task_id=? AND seq>? ORDER BY seq LIMIT ?",
        (task_id, after_seq, limit),
    ).fetchall()
    return [TaskLog(id=r["id"], task_id=r["task_id"], seq=r["seq"], ts=r["ts"],
                    level=r["level"], message=r["message"]) for r in rows]
