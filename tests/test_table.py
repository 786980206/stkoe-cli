# -*- coding: utf-8 -*-
"""table 任务版链路测试（graph 语义）：add→meta→set→col→scan→get→delete 全链路。

V2.0 死代码 TableController 直测已移入 V2.0/tests/test_table.py（默认全量不收集）。
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


def _write_single(root, name, rows, columns=("sym", "price")):
    d = root / "table" / name
    d.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pl.DataFrame):
        df = rows
    else:
        df = pl.DataFrame({c: rows[c] for c in columns})
    df.write_parquet(d / "data.parquet")
    return df


def test_task_framework_table_handlers(mgr):
    """table handlers 注册进任务框架：add→meta→get 全链路"""
    _write_single(mgr.data_dir, "demo", {"sym": ["a", "b"], "price": [1.0, 2.0]})
    _write_single(mgr.data_dir, "demo2", {"sym": ["a"], "price": [1.0]})

    t_add = mgr.submit("table", "add", ["demo"])
    _await(mgr, t_add)
    assert _mgr_result(mgr, t_add)["name"] == "demo"

    t_meta = mgr.submit("table", "meta", ["demo"])
    _await(mgr, t_meta)
    assert _mgr_result(mgr, t_meta)["layout"] == "single"

    # add 携带元数据
    t_add2 = mgr.submit("table", "add",
                        ["demo2", "--display_name=E表", "--tags=a,b"])
    _await(mgr, t_add2)
    meta2 = _mgr_result(mgr, t_add2)
    assert meta2["name"] == "demo2"
    from stkoe.graph.service import GraphService

    gsvc = GraphService(data_dir=mgr.data_dir)
    assert gsvc.table_meta("demo2")["tags"] == ["a", "b"]
    gsvc.close()

    t_get = mgr.submit("table", "get", ["demo", "--where=price>=2"])
    _await(mgr, t_get)
    res = _mgr_result(mgr, t_get)
    assert res["rows"] == 1

    t_list = mgr.submit("table", "list", [])
    _await(mgr, t_list)
    assert [m["name"] for m in _mgr_result(mgr, t_list)] == ["demo", "demo2"]

    t_set = mgr.submit("table", "set", ["demo", "--display_name=D表", "--source=local"])
    _await(mgr, t_set)
    set_res = _mgr_result(mgr, t_set)
    assert set_res["display_name"] == "D表"
    assert set_res["description"] == ""

    t_col = mgr.submit("table", "col", ["demo", "sym", "--display_name=代码", "--unit=元"])
    _await(mgr, t_col)
    col_res = _mgr_result(mgr, t_col)
    sym = next(c for c in col_res["columns"] if c["name"] == "sym")
    assert sym["display_name"] == "代码"
    assert sym["unit"] == "元"

    from stkoe.graph.service import GraphService

    gsvc = GraphService(data_dir=mgr.data_dir)
    meta_check = gsvc.table_meta("demo")
    gsvc.close()
    assert meta_check["display_name"] == "D表"

    # 追加数据 → s:table update 显式重扫：changed=True，版本递增
    pl.DataFrame({"sym": ["c"], "price": [3.0]}).write_parquet(
        mgr.data_dir / "table" / "demo" / "more.parquet")
    t_update = mgr.submit("table", "update", ["demo"])
    _await(mgr, t_update)
    scan_res = _mgr_result(mgr, t_update)
    assert scan_res["changed"] is True
    assert scan_res["version_after"] > 1  # 经 set/col 已递增，追加数据后再 +1
    gsvc = GraphService(data_dir=mgr.data_dir)
    assert gsvc.table_get("demo").height == 3
    gsvc.close()

    t_del = mgr.submit("table", "delete", ["demo"])
    _await(mgr, t_del)
    assert _mgr_result(mgr, t_del) == {"deleted": "demo"}


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
    """取任务最后一个事件携带的 data（JSON 字符串；轮询等终态事件落库）"""
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
