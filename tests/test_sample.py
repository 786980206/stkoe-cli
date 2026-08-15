# -*- coding: utf-8 -*-
"""sample 任务版链路测试（graph 语义）：add→check→get→set→del 全链路。

V2.0 死代码 SampleController 直测已移入 V2.0/tests/test_sample.py（默认全量不收集）。
"""
import polars as pl
import pytest


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


def _write(root, name, rows):
    d = root / "table" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _write_idx(root, name, rows):
    """index 资产写 index/ 目录（独立于 table/）"""
    d = root / "index" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _gsetup(root):
    """graph 语义造数：idx/mem 表 → index(sym/date) → panel ds → fieldset fs1(x2 校验通过)"""
    _write_idx(root, "idx", pl.DataFrame({
        "sym": ["a", "b", "c"],
        "x": [1.0, 2.0, 3.0],
        "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
    }))
    _write(root, "mem", pl.DataFrame({
        "sym": ["a", "b", "c"], "date": ["2026-01-01", "2026-01-02", "2026-01-03"]}))
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=root)
    svc.table_add("mem")
    svc.index_add("idx")
    svc.panel_add("ds", "idx", ["mem"])  # keys 由 index 推断 [sym, date]
    svc.fieldset_add("fs1", "ds")
    svc.fieldset_add_field("fs1", "x2", "x*2")
    svc.fieldset_check("fs1", "x2")
    svc.close()
    return root


def test_task_framework_sample_handlers(mgr):
    """任务版：sample add → check → get（含 fieldset 衍生列 + result 落盘）→ delete（graph 语义）"""
    _gsetup(mgr.data_dir)

    t_add = mgr.submit("sample", "add", ["s1", "--fieldset", "fs1", "--formula", "x>=2.0"])
    _await(mgr, t_add)
    assert _mgr_result(mgr, t_add)["name"] == "s1"

    t_check = mgr.submit("sample", "check", ["s1"])
    _await(mgr, t_check)
    assert _mgr_result(mgr, t_check)["ok"] is True

    t_get = mgr.submit("sample", "get", ["s1"])
    _await(mgr, t_get)
    get_res = _mgr_result(mgr, t_get)
    assert get_res["columns"] == ["sym", "x", "date", "x2"]
    assert get_res["result_ref"]

    t_set = mgr.submit("sample", "set", ["s1", "--formula", "x==1.0"])
    _await(mgr, t_set)
    assert _mgr_result(mgr, t_set)["formula"] == "x==1.0"

    t_del = mgr.submit("sample", "del", ["s1"])
    _await(mgr, t_del)
    assert _mgr_result(mgr, t_del) == {"deleted": "s1"}


# ---------- 任务框架助手 ----------

def _await(mgr, task, timeout=5.0):
    import time

    from stkoe.task.model import TERMINAL_STATES

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur = mgr.get(task.task_id)
        if cur is not None and cur.state in TERMINAL_STATES:
            return cur
        time.sleep(0.02)
    raise TimeoutError(f"task not terminal: {mgr.get(task.task_id).state}")


def _mgr_result(mgr, task):
    import json
    import time

    from stkoe.task.model import TERMINAL_STATES

    task_id = task.task_id if hasattr(task, "task_id") else task
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        evs = mgr.events.list_by_task(task_id)
        if evs and evs[-1].state in TERMINAL_STATES:
            return json.loads(evs[-1].data) if evs[-1].data else None
        time.sleep(0.01)
    evs = mgr.events.list_by_task(task_id)
    return json.loads(evs[-1].data) if evs and evs[-1].data else None
