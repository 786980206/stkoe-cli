"""任务管理测试：同步/后台执行、日志/进度、暂停/恢复/停止"""
import time

import pytest

import stkoe.data as data
from stkoe.data import catalog
from stkoe.data.task import TaskCancelled, conn_txn, run_task


def _quick(conn, ctl):
    ctl.info("hello task")
    ctl.progress(0.5, msg="halfway")
    with conn_txn(conn):
        conn.execute("INSERT INTO stkoe_objects (type,name,version,signature,meta,created_at,updated_at)"
                     " VALUES ('probe','p1',1,'x','{}','t','t')")
    return "done"


def test_run_task_sync_success(root):
    h = run_task("test_sync", "obj", _quick)
    assert h.status == "succeeded"
    assert h.progress == 0.5 and h.stage == "halfway"
    # catalog 事务已提交
    assert catalog().conn.execute("SELECT id FROM stkoe_objects WHERE name='p1'").fetchone() is not None
    # 日志已落盘
    logs = data.task_log(h.task_id)
    assert [l.message for l in logs] == ["hello task"]


def test_run_task_sync_failure(root):
    def _fail(conn, ctl):
        ctl.info("about to fail")
        raise ValueError("boom")

    h = run_task("test_fail", "obj", _fail)
    assert h.status == "failed"
    assert "boom" in h.error
    assert data.task_log(h.task_id)[-1].level == "ERROR"


def test_run_task_background_progress_logs(root):
    def _slow(conn, ctl):
        for i in range(3):
            ctl.check()
            ctl.info(f"step {i}")
            ctl.progress((i + 1) / 3, msg=f"step {i}/3")
            ctl.flush(conn)
            time.sleep(0.05)

    h = run_task("test_bg", "obj", _slow, background=True)
    assert h.status == "submitted"

    # 轮询直至完成
    deadline = time.time() + 10
    cur = data.task_list(status="succeeded", type="test_bg")
    while not cur and time.time() < deadline:
        time.sleep(0.05)
        cur = data.task_list(status="succeeded", type="test_bg")
    assert cur and cur[0].progress == 1.0

    # 增量拉取日志（after_seq 语义）
    all_logs = data.task_log(h.task_id)
    tail = data.task_log(h.task_id, after_seq=all_logs[0].seq)
    assert [l.message for l in tail] == ["step 1", "step 2"]
    assert catalog().conn.execute(
        "SELECT COUNT(*) AS n FROM stkoe_task_logs WHERE task_id=?", (h.task_id,)
    ).fetchone()["n"] == 3


def test_pause_resume_stop(root):
    def _loop(conn, ctl):
        i = 0
        while True:
            ctl.check()          # 边界：暂停阻塞 / 取消抛出
            i += 1
            ctl.info(f"iter {i}")
            ctl.flush(conn)
            time.sleep(0.02)
            if i >= 10000:       # 防御性上限
                raise RuntimeError("loop runaway")

    h = run_task("test_pause", "obj", _loop, background=True)

    deadline = time.time() + 10
    while not data.task_list(status="running", type="test_pause") and time.time() < deadline:
        time.sleep(0.02)
    running = data.task_list(status="running", type="test_pause")
    assert running, "task did not enter running"

    # 暂停
    ph = data.task_pause(h.task_id)
    assert ph.status == "paused"
    assert data.task_list(status="paused", type="test_pause")
    n_before = data.task_log(h.task_id, limit=10000)

    # 暂停期间日志不再增长（协作式边界阻塞）
    time.sleep(0.15)
    n_after = data.task_log(h.task_id, limit=10000)
    assert len(n_after) == len(n_before)

    # 恢复后继续增长
    rh = data.task_resume(h.task_id)
    assert rh.status == "running"
    time.sleep(0.15)
    n_resumed = data.task_log(h.task_id, limit=10000)
    assert len(n_resumed) > len(n_after)

    # 停止 → cancelled
    data.task_stop(h.task_id)
    deadline = time.time() + 10
    cancelled = data.task_list(status="cancelled", type="test_pause")
    while not cancelled and time.time() < deadline:
        time.sleep(0.05)
        cancelled = data.task_list(status="cancelled", type="test_pause")
    assert cancelled


def test_task_stop_unknown(root):
    with pytest.raises(KeyError):
        data.task_stop("nonexistent")


def test_task_cancelled_exception(root):
    def _mk(conn, ctl):
        ctl.check()
        raise TaskCancelled("obj")

    h = run_task("test_cancel_x", "obj", _mk, background=True)
    deadline = time.time() + 10
    while data.task_list(status="submitted", type="test_cancel_x") and time.time() < deadline:
        time.sleep(0.05)
    assert data.task_list(status="cancelled", type="test_cancel_x")


def test_task_clean(root):
    run_task("test_clean", "obj", _quick)                    # succeeded
    run_task("test_clean", "obj", lambda c, k: (_ for _ in ()).throw(ValueError("x")))  # failed
    assert data.task_list(type="test_clean")

    n = data.task_clean()
    assert n == 2
    assert not data.task_list(type="test_clean")
    # 日志级联删除（stkoe_task_logs.task_id FK ON DELETE CASCADE）
    assert catalog().conn.execute(
        "SELECT COUNT(*) AS n FROM stkoe_task_logs").fetchone()["n"] == 0


def test_task_stop_all_and_clean(root):
    def _loop(conn, ctl):
        while True:
            ctl.check()
            time.sleep(0.02)

    run_task("test_stopall", "obj", _loop, background=True)
    run_task("test_stopall", "obj", _loop, background=True)

    deadline = time.time() + 10
    while not data.task_list(status="running", type="test_stopall") and time.time() < deadline:
        time.sleep(0.02)

    stopped = data.task_stop_all()          # 取消并等待收尾到完成态
    assert stopped == 2
    assert data.task_list(status="cancelled", type="test_stopall")

    n = data.task_clean()
    assert n == 2
    assert not data.task_list(type="test_stopall")
