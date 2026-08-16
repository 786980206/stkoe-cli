# -*- coding: utf-8 -*-
"""factor 任务版链路测试（graph 语义）：add→get→check→scan→del 全链路。

V2.0 死代码 FactorController 直测已移入 V2.0/tests/test_factor.py（默认全量不收集）。
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
    """graph 语义造数：idx/mem 表 → index(sym/date) → panel ds → sample sp1 → feature f1
    （上游链依次 update 就绪）"""
    _write_idx(root, "idx", pl.DataFrame({
        "sym": ["a", "b"], "x": [1.0, 2.0],
        "date": ["2024-01-01", "2024-01-02"]}))
    _write(root, "mem", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-02"]}))
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=root)
    svc.table_add("mem")
    svc.index_add("idx")
    svc.panel_add("ds", "idx", ["mem"])  # keys 由 index 推断 [sym, date]
    svc.fieldset_add("fs1", "ds")
    svc.sample_add("sp1", "fs1", "idx")
    svc.feature_add("f1", "x*2")
    for t, n in [("panel", "ds"), ("fieldset", "fs1"),
                 ("sample", "sp1"), ("feature", "f1")]:
        getattr(svc, f"{t}_update")(n)
    svc.close()
    return root


def test_task_framework_factor_handlers(mgr):
    """任务版：factor add → check → get（result 落盘）→ scan → delete 全链路（graph 语义）"""
    _gsetup(mgr.data_dir)

    t_add = mgr.submit("factor", "add", ["fac1", "--feature", "f1",
                                         "--sample", "sp1"])
    _await(mgr, t_add)
    assert _mgr_result(mgr, t_add)["name"] == "fac1"

    t_scan = mgr.submit("factor", "scan", ["fac1"])  # get 三态：先物化
    _await(mgr, t_scan)
    assert _mgr_result(mgr, t_scan)["changed"] is True

    t_get = mgr.submit("factor", "get", ["fac1"])
    _await(mgr, t_get)
    get_res = _mgr_result(mgr, t_get)
    assert get_res["columns"] == ["sym", "date", "f1"]
    assert get_res["result_ref"]
    assert get_res["rows"] == 2

    t_check = mgr.submit("factor", "check", ["fac1"])
    _await(mgr, t_check)
    assert _mgr_result(mgr, t_check)["ok"] is True

    t_del = mgr.submit("factor", "del", ["fac1"])
    _await(mgr, t_del)
    assert _mgr_result(mgr, t_del) == {"deleted": "fac1"}


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
