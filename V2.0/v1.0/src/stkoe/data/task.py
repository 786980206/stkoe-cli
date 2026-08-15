"""任务管理：stkoe_tasks 登记 + 同步/后台执行 + 日志/进度/暂停/取消

状态机：``submitted -> running <-> paused -> succeeded|failed|cancelled``

执行模式（v0.5.0 起统一）：**所有请求默认同步执行**（CLI/REPL/gRPC 一致），
需要后台时显式传 ``background=True``（CLI ``--async``）→ 提交线程池立即返回
TaskHandle（task_id），随后用 ``task get <task_id>`` 查询状态/进度/结果。

各模块统一经 ``defer(kind, ref, fn, background=...)`` 执行，业务函数签名
恒为 ``fn(conn, ctl)``，``ctl`` 是注入的 logger/控制对象（进度/日志/暂停）：
- 同步：注入 ``console`` 模式的 ``TaskControl``（日志直接打印，不落表）
- 异步：worker 注入真 ``TaskControl``（进度/日志写 stkoe_* 表，结果序列化存
  ``result_ref``，``task_get()`` 反序列化返回；dataclass 经 ``to_dict``）

- 日志：同步打印（loguru），异步批量写 ``stkoe_task_logs``，``task_log()`` 增量拉取
- pause/stop 协作式：任务在分区边界调用 ``ctl.check()``；console 模式直接放行
- 并发安全：后台任务用独立连接（``catalog().new_conn()``），WAL 多连接读写分离
"""
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field

from loguru import logger

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
    """任务执行上下文：日志/进度/阶段 + 协作式暂停取消（跨线程安全）

    ``console=True`` 时（同步执行）不写 task 表：日志/进度直接打印（loguru），
    check/flush/pause/resume/cancel 为空操作——业务函数无需区分同步/异步。
    """

    task_id: str
    type: str
    object_ref: str
    console: bool = False
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _progress: float = 0.0
    _stage: str = ""
    _state: str = "submitted"
    _paused: bool = False
    _cancelled: bool = False
    _seq: int = 0
    _log: list[tuple[int, str, str, str]] = field(default_factory=list)  # (seq, ts, level, msg)
    # 订阅事件流（seq 单调递增；日志/进度/状态统一入列，供 gRPC SubscribeTask 拉取）
    _events: list[dict] = field(default_factory=list)
    _dirty: bool = True

    # --- 日志 ---
    def log(self, level: str, msg: str) -> None:
        if self.console:
            logger.log(level, msg)
            return
        with self._lock:
            self._seq += 1
            ts = now()
            self._log.append((self._seq, ts, level, msg))
            self._events.append({
                "seq": self._seq, "ts": ts, "kind": "log", "level": level,
                "message": msg, "progress": self._progress, "state": self._state,
            })
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
        if self.console:
            if msg:
                logger.info(msg)
            return
        with self._lock:
            self._progress = max(0.0, min(1.0, float(value)))
            if msg:
                self._stage = msg
            self._seq += 1
            self._events.append({
                "seq": self._seq, "ts": now(), "kind": "progress", "level": "INFO",
                "message": msg or "", "progress": self._progress, "state": self._state,
            })
            self._dirty = True

    def stage(self, msg: str) -> None:
        if self.console:
            logger.info(msg)
            return
        with self._lock:
            self._stage = msg
            self._seq += 1
            self._events.append({
                "seq": self._seq, "ts": now(), "kind": "progress", "level": "INFO",
                "message": msg, "progress": self._progress, "state": self._state,
            })
            self._dirty = True

    def set_state(self, state: str, message: str = "") -> None:
        """记录状态迁移事件（running/succeeded/failed/cancelled…）"""
        if self.console:
            return
        with self._lock:
            self._state = state
            self._seq += 1
            self._events.append({
                "seq": self._seq, "ts": now(), "kind": "state", "level": "INFO",
                "message": message, "progress": self._progress, "state": state,
            })
            self._dirty = True

    def events_since(self, seq: int) -> tuple[list[dict], int, float, str, str]:
        """返回 seq 大于给定值的未读事件 + 最新 seq/progress/stage/state（订阅拉取）"""
        with self._lock:
            out = [e for e in self._events if e["seq"] > seq]
            return out, self._seq, self._progress, self._stage, self._state

    # --- 协作控制（分区边界调用）---
    def check(self) -> None:
        """暂停时阻塞等待；取消时抛 TaskCancelled（console 模式直接放行）"""
        if self.console:
            return
        while True:
            with self._lock:
                cancelled, paused = self._cancelled, self._paused
            if cancelled:
                raise TaskCancelled(self.object_ref)
            if not paused:
                return
            time.sleep(PAUSE_POLL)

    def pause(self) -> None:
        if not self.console:
            with self._lock:
                self._paused = True

    def resume(self) -> None:
        if not self.console:
            with self._lock:
                self._paused = False

    def cancel(self) -> None:
        if not self.console:
            with self._lock:
                self._cancelled = True

    # --- 持久化（flush 由任务方在边界调用；worker 结束兜底）---
    def flush(self, conn) -> None:
        if self.console:
            return
        with self._lock:
            if not self._dirty and not self._log:
                return
            logs = list(self._log)
            self._log.clear()
            progress, stage = self._progress, self._stage
            self._dirty = False
        with conn_txn(conn):
            for seq, ts, level, msg in logs:
                conn.execute(
                    "INSERT INTO stkoe_task_logs (task_id, seq, ts, level, message) "
                    "VALUES (?,?,?,?,?)",
                    (self.task_id, seq, ts, level, msg),
                )
            conn.execute(
                "UPDATE stkoe_tasks SET progress=?, stage=?, updated_at=? WHERE task_id=?",
                (progress, stage, now(), self.task_id),
            )


# ---------- 执行模式 ----------

def console_ctl(kind: str = "", ref: str = "") -> TaskControl:
    """同步执行的 console 模式 TaskControl：日志/进度直接打印（loguru），不落表"""
    return TaskControl(task_id="", type=kind, object_ref=ref, console=True)


def defer(kind: str, ref: str, fn, *, background: bool = False,
          result_fn=None, **run_kw):
    """统一任务入口：同步执行返回 ``result_fn``（缺省为 fn 返回值），异步返回 TaskHandle。

    - ``background=False``（默认）：当前线程直接执行，注入 console 模式 TaskControl
      （日志/进度直接打印）；返回业务结果（异常直接抛给调用方）
    - ``background=True``：提交线程池立即返回 TaskHandle；完成后结果序列化存
      ``stkoe_tasks.result_ref``，``task_get()`` 可拉取
    - ``fn(conn, ctl)``：conn 为主连接时可为 None（同步）；后台时 worker 注入独立连接
    - ``result_fn(result)``：把 fn 的原始结果转成对外返回值（同步原样返回；
      异步时应用于持久化前的序列化结果）
    """
    if background:
        logger.debug(f"task defer[{kind}:{ref}]: background submit")
        return run_task(kind, ref, fn, background=True, result_fn=result_fn)
    logger.debug(f"task defer[{kind}:{ref}]: sync run")
    result = fn(None, console_ctl(kind, ref))
    return result_fn(result) if result_fn else result


def _pool() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="stkoe-task")
    return _executor


def _set_status(conn, task_id: str, status: str, *, error: str | None = None,
                result_ref: str | None = None, finished: bool = False) -> None:
    sets, args = ["status=?", "updated_at=?"], [status, now()]
    if error is not None:
        sets.append("error=?")
        args.append(error)
    if result_ref is not None:
        sets.append("result_ref=?")
        args.append(result_ref)
    if finished:
        sets.append("finished_at=?")
        args.append(now())
    args.append(task_id)
    with conn_txn(conn):
        conn.execute(f"UPDATE stkoe_tasks SET {', '.join(sets)} WHERE task_id=?", args)


def _handle(ctl: TaskControl, status: str, error: str | None = None) -> TaskHandle:
    return TaskHandle(task_id=ctl.task_id, type=ctl.type, object_ref=ctl.object_ref,
                      status=status, progress=ctl._progress, stage=ctl._stage, error=error)


def _result_json(result) -> str | None:
    """任务结果 → JSON 字符串（result_ref 存 JSON；dataclass 经 to_dict；失败返回 None）"""
    if result is None:
        return None
    try:
        obj = result.to_dict() if hasattr(result, "to_dict") else result
        return json.dumps(obj, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return None


def _worker(ctl: TaskControl, fn, result_fn=None) -> TaskHandle:
    """后台执行体：fn(conn, ctl)，fn 自行管理事务；失败/取消记录进 tasks"""
    from . import catalog
    conn = catalog().new_conn()
    status = "succeeded"
    error = None
    try:
        _set_status(conn, ctl.task_id, "running")
        ctl.set_state("running")
        out = fn(conn, ctl)
        result = result_fn(out) if result_fn else out
        result_ref = _result_json(result)
        ctl.set_state("succeeded")
        _set_status(conn, ctl.task_id, "succeeded", result_ref=result_ref, finished=True)
    except TaskCancelled:
        status = "cancelled"
        ctl.set_state("cancelled")
        _set_status(conn, ctl.task_id, "cancelled", finished=True)
    except Exception as e:
        status = "failed"
        error = f"{type(e).__name__}: {e}"
        ctl.error(error)
        ctl.set_state("failed", message=error)
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


def run_task(task_type: str, object_ref: str, fn, *, background: bool = False,
             result_fn=None) -> TaskHandle:
    """登记任务并执行：``fn(conn, ctl)``；``background=True`` 提交线程池立即返回

    - ``fn`` 需自行管理事务（用 ``conn_txn`` / ``ctl.flush(conn)``）
    - 同步调用（默认）复用当前线程，返回最终状态；``result_fn`` 应用于结果持久化
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
        logger.debug(f"task run[{task_type}:{object_ref}] {task_id}: queued to pool "
                     f"(workers={_executor._max_workers if _executor else MAX_WORKERS})")
        _pool().submit(_worker, ctl, fn, result_fn)
        return _handle(ctl, "submitted")
    logger.debug(f"task run[{task_type}:{object_ref}] {task_id}: sync execute")
    return _worker(ctl, fn, result_fn)


# ---------- 任务管理 API ----------

def _to_handle(row) -> TaskHandle:
    return TaskHandle(task_id=row["task_id"], type=row["type"], object_ref=row["object_ref"],
                      status=row["status"], progress=row["progress"] or 0.0,
                      stage=row["stage"] or "", error=row["error"],
                      result_ref=row["result_ref"])


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


def task_meta(task_id: str) -> dict:
    """任务详情：状态/进度/阶段/错误/时间 + 最近日志摘要（REPL 轮询用）"""
    from . import catalog
    row = catalog().conn.execute(
        "SELECT * FROM stkoe_tasks WHERE task_id=?", (task_id,)
    ).fetchone()
    if row is None:
        raise KeyError(f"task not found: {task_id}")
    logs = task_log(task_id, limit=5)
    return {
        "task_id": row["task_id"],
        "type": row["type"],
        "object_ref": row["object_ref"],
        "status": row["status"],
        "progress": row["progress"],
        "stage": row["stage"],
        "error": row["error"],
        "result": _load_result(row["result_ref"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "finished_at": row["finished_at"],
        "recent_logs": [l.message for l in logs],
    }


def _load_result(result_ref: str | None):
    """result_ref JSON → Python 对象（None/解析失败 → None）"""
    if not result_ref:
        return None
    try:
        return json.loads(result_ref)
    except (TypeError, ValueError):
        return None


def task_get(task_id: str) -> dict:
    """获取任务状态 + 结果（``result_ref`` 反序列化）"""
    return task_meta(task_id)


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


def live_control(task_id: str) -> TaskControl | None:
    """运行中任务的实时控制对象（用于订阅进度/日志；未运行/不存在返回 None）"""
    with _reg_lock:
        return _controls.get(task_id)