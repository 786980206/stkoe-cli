# -*- coding: utf-8 -*-
"""fieldset 任务版链路测试（graph 语义）：add→add_field→check→scan→get→test 全链路。

V2.0 死代码 FieldsetController 直测已移入 V2.0/tests/test_fieldset.py（默认全量不收集）。
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


def _gsetup(root, index_rows=None):
    """graph 语义造数：idx/mem 表 → index(sym/date) → panel ds（panel 已 update 就绪）"""
    index_rows = index_rows if index_rows is not None else pl.DataFrame({
        "sym": ["a", "b", "c"], "x": [1.0, 2.0, 3.0],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "optime": ["2024-01-01 08:00:00"] * 3})
    _write_idx(root, "idx", index_rows)
    _write(root, "mem", pl.DataFrame({
        "sym": ["a", "b", "c"], "date": ["2024-01-01", "2024-01-02", "2024-01-03"]}))
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=root)
    svc.table_add("mem")
    svc.index_add("idx")
    svc.panel_add("ds", "idx", ["mem"])  # keys 由 index 推断 [sym, date]
    svc.panel_update("ds")  # 上游就绪（update 语义）
    svc.close()
    return root


def test_task_framework_fieldset_handlers(mgr):
    """任务版：fieldset add→add_field→check→scan 全链路 + 结果落盘（graph 语义）"""
    _gsetup(mgr.data_dir)

    t_add = mgr.submit("fieldset", "add", ["fs1", "--dataset", "ds"])
    _await(mgr, t_add)
    assert _mgr_result(mgr, t_add)["name"] == "fs1"

    t_field = mgr.submit("fieldset", "add", ["fs1", "x2", "--formula", "x*2"])
    _await(mgr, t_field)
    t_check = mgr.submit("fieldset", "check", ["fs1", "x2"])
    _await(mgr, t_check)
    assert _mgr_result(mgr, t_check)[0]["ok"] is True

    t_scan = mgr.submit("fieldset", "scan", ["fs1"])
    _await(mgr, t_scan)
    assert _mgr_result(mgr, t_scan)["materialized"] is True

    t_get = mgr.submit("fieldset", "get", ["fs1"])
    _await(mgr, t_get)
    get_res = _mgr_result(mgr, t_get)
    assert get_res["columns"] == ["sym", "x", "date", "optime", "x2"]  # panel 视图 + 衍生指标
    assert get_res["result_ref"]

    # --fields-only 仅返回衍生数据（keys + 指标）
    t_get_fs = mgr.submit("fieldset", "get", ["fs1", "--fields-only"])
    _await(mgr, t_get_fs)
    assert _mgr_result(mgr, t_get_fs)["columns"] == ["sym", "date", "x2"]

    # 引擎/测试任务
    t_test = mgr.submit("fieldset", "test", ["fs1", "--formula", "x+1"])
    _await(mgr, t_test)
    assert _mgr_result(mgr, t_test)["ok"] is True


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
