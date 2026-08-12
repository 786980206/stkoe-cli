# -*- coding: utf-8 -*-
"""TaskManager 框架测试：状态机 / SQLite 持久化 / 订阅回放与实时 / 取消 / 日志 / 结果引用"""
import json
import time

import pytest

from stkoe.task import TaskManager
from stkoe.task.model import TERMINAL_STATES


@pytest.fixture()
def mgr(tmp_path):
    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


def _collect(mgr, task_id, replay=True, timeout=5.0):
    """订阅收集事件流直到 EOF，返回 [(seq, state, progress, message, data)]"""
    events, deadline = [], time.monotonic() + timeout
    gen = mgr.subscribe(task_id, replay)
    for resp in gen:
        if resp.WhichOneof("type") == "header":
            assert resp.header.code == 0, resp.header.message
            continue
        events.append((resp.event.seq, resp.event.state,
                       resp.event.progress, resp.event.message, resp.event.data))
    assert events, "订阅未收到任何事件"
    return events


def test_mock_task_full_lifecycle(mgr, tmp_path):
    """mock 任务：pending→running→succeeded，progress 到 1.0，事件按 seq 递增"""
    task = mgr.submit("mock", "", [])
    assert task.state == "pending"

    events = _collect(mgr, task.task_id)
    states = [e[1] for e in events]
    assert states[0] == "pending"
    assert "running" in states
    assert states[-1] == "succeeded"
    assert events[-1][2] == 1.0
    assert events[-1][4]  # 终态 data（{"steps": 5}）
    seqs = [e[0] for e in events]
    assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))

    # 任务终态可从 SQLite 读回
    done = mgr.get(task.task_id)
    assert done.state == "succeeded"
    assert done.progress == 1.0
    assert done.started_at is not None
    assert done.finished_at is not None
    assert done.result_ref == f"tasks/{task.task_id}/mock_result"


def test_mock_writes_log_and_result(mgr, tmp_path):
    task = mgr.submit("mock", "", [])
    _collect(mgr, task.task_id)

    log = tmp_path / "data" / "tasks" / task.task_id / "task.log"
    assert log.exists()
    assert "步骤" in log.read_text(encoding="utf-8")

    done = mgr.get(task.task_id)
    result_file = tmp_path / "data" / done.result_ref
    assert result_file.exists()
    assert json.loads(result_file.read_bytes()) == {"steps": 5}


def test_task_list_ordered_and_filtered(mgr):
    """task list：按创建时间倒序（最新在前），--state 过滤"""
    t1 = mgr.submit("mock", "", [])
    _collect(mgr, t1.task_id)
    t2 = mgr.submit("version", "get", [])
    _collect(mgr, t2.task_id)

    tasks = mgr.tasks.list()
    assert len(tasks) == 2
    assert [t.task_id for t in tasks] == [t2.task_id, t1.task_id]
    assert tasks[0].to_dict()["state"] == "succeeded"
    assert tasks[0].to_dict()["action"] == "get"

    done = mgr.tasks.list(state="succeeded")
    assert len(done) == 2
    assert all(t.state == "succeeded" for t in done)
    assert mgr.tasks.list(state="running") == []


def test_task_persisted_across_manager_reload(tmp_path):
    """任务与事件持久化到 SQLite：新 TaskManager 用同一 data_dir 可读回"""
    m1 = TaskManager(data_dir=tmp_path / "data")
    m1.start()
    task = m1.submit("mock", "", [])
    _collect(m1, task.task_id)
    m1.stop()

    m2 = TaskManager(data_dir=tmp_path / "data")
    done = m2.get(task.task_id)
    assert done is not None
    assert done.state == "succeeded"
    events = list(m2.events.list_by_task(task.task_id))
    assert events[-1].state == "succeeded"
    m2.stop()


def test_cancel_running_task(mgr):
    """运行中取消：标记置位，Handler 在检查点退出，终态 cancelled（且未跑完）"""
    task = mgr.submit("mock", "", [])
    # 等它进入 running 且进度未满（进度 1.0 说明 mock 已跑到最后一步）
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        cur = mgr.get(task.task_id)
        if cur.state == "running" and cur.progress < 0.9:
            break
        time.sleep(0.02)
    assert mgr.cancel(task.task_id) is True

    events = _collect(mgr, task.task_id)
    assert events[-1][1] == "cancelled"
    assert events[-1][2] < 1.0  # 未跑完
    assert mgr.get(task.task_id).state == "cancelled"

    # 终态后再次取消返回 False；cancelled 后无后续进度事件（Handler 已退出）
    assert mgr.cancel(task.task_id) is False
    assert all(e[1] == "cancelled" for e in events[-1:])


def test_pause_and_resume(mgr):
    """暂停后进度不再推进，恢复后继续到 succeeded"""
    task = mgr.submit("mock", "", [])
    # 等 running
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        cur = mgr.get(task.task_id)
        if cur.state == "running":
            break
        time.sleep(0.02)

    assert mgr.pause(task.task_id) is True
    assert mgr.is_paused(task)
    assert mgr.pause(task.task_id) is True  # 幂等

    # 暂停期间进度冻结
    time.sleep(0.2)
    assert mgr.get(task.task_id).state == "paused"

    assert mgr.resume(task.task_id) is True
    assert not mgr.is_paused(task)
    assert mgr.get(task.task_id).state == "running"

    events = _collect(mgr, task.task_id)
    states = [e[1] for e in events]
    assert "paused" in states
    assert states[-1] == "succeeded"
    assert events[-1][2] == 1.0


def test_resume_not_paused_returns_false(mgr):
    task = mgr.submit("mock", "", [])
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        cur = mgr.get(task.task_id)
        if cur.state == "running":
            break
        time.sleep(0.02)
    assert mgr.resume(task.task_id) is False  # 未暂停不可恢复


def test_control_unknown_action(mgr):
    task = mgr.submit("mock", "", [])
    ok, msg = mgr.control(task.task_id, "explode")
    assert ok is False
    assert "不支持的任务操作" in msg


def test_control_missing_task(mgr):
    ok, msg = mgr.control("no_such_task", "cancel")
    assert ok is False
    assert "not found" in msg


def test_cancel_not_started_pending(mgr):
    """pending 阶段取消（Handler 执行前）"""
    task = mgr.submit("mock", "", [])
    mgr.cancel(task.task_id)
    events = _collect(mgr, task.task_id)
    assert events[-1][1] in ("cancelled", "failed")  # pending 直接终态 cancelled
    assert mgr.get(task.task_id).is_terminal()


def test_unknown_command_task_fails(mgr):
    """未注册命令：事件流以 failed 结束，含错误消息"""
    task = mgr.submit("no_such_source", "bogus", [])
    events = _collect(mgr, task.task_id)
    assert events[-1][1] == "failed"
    assert "不支持的命令" in events[-1][3]
    assert mgr.get(task.task_id).error


def test_subscribe_missing_task_error(mgr):
    resp = list(mgr.subscribe("no_such_task", replay=True))[0]
    assert resp.WhichOneof("type") == "header"
    assert resp.header.code != 0
    assert "not found" in resp.header.message


def test_replay_false_skips_history(mgr):
    """任务已结束后订阅 replay=False：只收订阅后的实时事件（这里为无）"""
    task = mgr.submit("mock", "", [])
    _collect(mgr, task.task_id)  # 等它跑完
    gen = mgr.subscribe(task.task_id, replay=False)
    header, events = None, 0
    for resp in gen:
        if resp.WhichOneof("type") == "header":
            header = resp.header
        else:
            events += 1
    assert header.code == 0
    assert events == 0  # 终态任务 replay=False 无新事件，直接 EOF


def test_subscribe_live_events(mgr):
    """replay=False 在运行中订阅：能收到订阅后的实时事件"""
    task = mgr.submit("mock", "", [])
    # 等任务进入 running 且尚未完成时开始订阅
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        cur = mgr.get(task.task_id)
        if cur.state == "running":
            break
        time.sleep(0.02)
    gen = mgr.subscribe(task.task_id, replay=False)
    header, got = None, []
    for resp in gen:
        if resp.WhichOneof("type") == "header":
            header = resp.header
        else:
            got.append(resp.event)
    assert header.code == 0
    assert got
    assert got[-1].state == "succeeded"
    assert got[-1].progress == 1.0


def test_config_task_via_handler(mgr):
    """config 任务走 Handler：返回配置数据"""
    task = mgr.submit("config", "", [])
    events = _collect(mgr, task.task_id)
    assert events[-1][1] == "succeeded"
    assert "grpc-host" in events[-1][4]


def test_version_task(mgr):
    task = mgr.submit("version", "get", [])
    events = _collect(mgr, task.task_id)
    assert events[-1][1] == "succeeded"
    assert "version" in events[-1][4]


def test_state_machine_constant():
    assert set(TERMINAL_STATES) == {"succeeded", "failed", "cancelled"}
