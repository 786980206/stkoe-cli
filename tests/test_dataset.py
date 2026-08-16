# -*- coding: utf-8 -*-
"""dataset（panel）任务版链路测试（graph 语义）：add→meta→list→get→delete 全链路。

V2.0 死代码 DatasetController 直测已移入 V2.0/tests/test_dataset.py（默认全量不收集）。
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


def test_task_framework_dataset_handlers(mgr):
    """dataset handlers 注册进任务框架：add→meta→get 全链路（转发 panel，graph 语义）"""
    from stkoe.graph.service import GraphService

    root = mgr.data_dir
    _write_idx(root, "index", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "price": [1.0, 2.0], "optime": ["2024-01-01 08:00:00"] * 2}))
    _write(root, "m1", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "name": ["AA", "BB"], "industry": ["金融", "科技"]}))
    gsvc = GraphService(data_dir=root)
    gsvc.table_add("m1")
    gsvc.index_add("index")
    gsvc.close()

    t_add = mgr.submit("dataset", "add", ["ds1", "index", "m1"])  # keys 由 index 推断
    _await(mgr, t_add)
    add_res = _mgr_result(mgr, t_add)
    assert add_res["name"] == "ds1"
    assert add_res["keys"] == ["sym", "date"]  # panel 实时 join，无物化概念

    t_meta = mgr.submit("dataset", "meta", ["ds1"])
    _await(mgr, t_meta)
    assert _mgr_result(mgr, t_meta)["index"] == "index:index"

    t_list = mgr.submit("dataset", "list", [])
    _await(mgr, t_list)
    assert [d["name"] for d in _mgr_result(mgr, t_list)] == ["ds1"]

    t_upd = mgr.submit("dataset", "update", ["ds1"])  # get 三态：先物化
    _await(mgr, t_upd)
    assert _mgr_result(mgr, t_upd)["materialized"] is True

    t_get = mgr.submit("dataset", "get", ["ds1"])
    _await(mgr, t_get)
    assert _mgr_result(mgr, t_get)["rows"] == 2

    t_del = mgr.submit("dataset", "delete", ["ds1"])
    _await(mgr, t_del)
    assert _mgr_result(mgr, t_del) == {"deleted": "ds1"}


def test_task_framework_panel_handlers(mgr):
    """panel handlers 注册进任务框架：s:panel add→meta→get 全链路（与 dataset 同实现）"""
    from stkoe.graph.service import GraphService

    root = mgr.data_dir
    _write_idx(root, "index", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "price": [1.0, 2.0], "optime": ["2024-01-01 08:00:00"] * 2}))
    _write(root, "m1", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"],
        "name": ["AA", "BB"], "industry": ["金融", "科技"]}))
    gsvc = GraphService(data_dir=root)
    gsvc.table_add("m1")
    gsvc.index_add("index")
    gsvc.close()

    t_add = mgr.submit("panel", "add", ["ds1", "index", "m1"])
    _await(mgr, t_add)
    assert _mgr_result(mgr, t_add)["name"] == "ds1"

    t_upd = mgr.submit("panel", "update", ["ds1"])
    _await(mgr, t_upd)
    assert _mgr_result(mgr, t_upd)["materialized"] is True

    t_get = mgr.submit("panel", "get", ["ds1"])
    _await(mgr, t_get)
    assert _mgr_result(mgr, t_get)["rows"] == 2

    t_del = mgr.submit("panel", "delete", ["ds1"])
    _await(mgr, t_del)
    assert _mgr_result(mgr, t_del) == {"deleted": "ds1"}


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
