# -*- coding: utf-8 -*-
"""test（因子测试数据集）graph 语义测试：stat 测试器集成 + 任务版链路。

V2.0 死代码 FactorTestController 直测已移入 V2.0/tests/test_factor_test.py（默认全量不收集）。
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


def _run(coro):
    import asyncio

    return asyncio.run(coro)


def _gsetup(tmp_path):
    """graph 语义造数：idx/mem 表 → index → panel ds([sym,date]) → fieldset fs1
    → sample sp1 → feature f1 → factor fac1（任务版/stat 测试用）"""
    root = tmp_path / "data"
    rows = [
        {"sym": "a", "date": "2024-01-01", "r": 0.01, "ic": "G1",
         "fv": 1.0, "x": 1.0},
        {"sym": "b", "date": "2024-01-01", "r": 0.02, "ic": "G1",
         "fv": 2.0, "x": 2.0},
        {"sym": "c", "date": "2024-01-01", "r": 0.03, "ic": "G2",
         "fv": 3.0, "x": 3.0},
        {"sym": "a", "date": "2024-01-02", "r": 0.01, "ic": "G1",
         "fv": 1.0, "x": 1.0},
        {"sym": "b", "date": "2024-01-02", "r": 0.02, "ic": "G1",
         "fv": 2.0, "x": 2.0},
        {"sym": "c", "date": "2024-01-02", "r": 0.03, "ic": "G2",
         "fv": 3.0, "x": 3.0},
    ]
    _write_idx(root, "idx", pl.DataFrame(rows))
    _write(root, "mem", pl.DataFrame(
        {"sym": ["a", "b", "c"] * 2, "date": ["2024-01-01"] * 3 + ["2024-01-02"] * 3}))
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=root)
    svc.table_add("mem")
    svc.index_add("idx", symbol_col="sym", datetime_col="date")
    svc.panel_add("ds", "idx", ["mem"])  # keys 由 index 推断
    svc.fieldset_add("fs1", "ds")
    svc.fieldset_add_field("fs1", "x2", "x*2")
    svc.fieldset_check("fs1", "x2")
    svc.sample_add("sp1", "fs1", "idx")
    svc.feature_add("f1", "x*2")
    svc.factor_add("fac1", "f1", "sp1")
    # update 语义：上游链依次就绪（panel → fieldset → sample → feature → factor）
    for t, n in [("panel", "ds"), ("fieldset", "fs1"), ("sample", "sp1"),
                 ("feature", "f1"), ("factor", "fac1")]:
        getattr(svc, f"{t}_update")(n)
    svc.close()
    return root


# ---------- stat testers 集成 ----------

def test_stat_scan_test_target(tmp_path):
    """stat scan test <name> --kind ic：写 stat/test/<name>/ic/*.parquet（graph 语义）"""
    _gsetup(tmp_path)
    from stkoe.graph.service import GraphService
    from stkoe.stat import StatController

    svc = GraphService(data_dir=tmp_path / "data")
    svc.tester_add("t1", "fac1")
    svc.tester_update("t1")  # stat 消费物化（get 三态）
    svc.close()
    st = StatController(data_dir=tmp_path / "data")
    report = _run(st.scan("tester", "t1", kind="ic"))
    assert report.target_type == "tester"
    assert report.target_name == "t1"
    assert set(report.partitions) == {"ic_d1", "ic_d5", "ic_d10"}
    out_dir = tmp_path / "data" / "stat" / "tester" / "t1" / "ic"
    assert (out_dir / "ic_d1.parquet").exists()


def test_stat_get_test_partition(tmp_path):
    _gsetup(tmp_path)
    from stkoe.graph.service import GraphService
    from stkoe.stat import StatController

    svc = GraphService(data_dir=tmp_path / "data")
    svc.tester_add("t1", "fac1")
    svc.tester_update("t1")
    svc.close()
    st = StatController(data_dir=tmp_path / "data")
    _run(st.scan("tester", "t1", kind="ic"))
    df = _run(st.get("tester", "t1", kind="ic", partition_by="ic_d1"))
    assert "IC(d1)" in df.columns
    assert "RankIC(d1)" in df.columns


def test_stat_all_testers(tmp_path):
    _gsetup(tmp_path)
    from stkoe.factor_tester.tester import TESTER_KINDS
    from stkoe.graph.service import GraphService
    from stkoe.stat import StatController

    svc = GraphService(data_dir=tmp_path / "data")
    svc.tester_add("t1", "fac1")
    svc.tester_update("t1")
    svc.close()
    st = StatController(data_dir=tmp_path / "data")
    for kind in TESTER_KINDS:
        report = _run(st.scan("tester", "t1", kind=kind))
        assert report.files, f"{kind} 应产出文件"
        assert all((tmp_path / "data" / "stat" / f.rel_path).exists()
                   for f in report.files)


def test_stat_scan_test_unregistered(tmp_path):
    _gsetup(tmp_path)
    from stkoe.stat import StatController, StatNotFoundError

    st = StatController(data_dir=tmp_path / "data")
    with pytest.raises(StatNotFoundError):
        _run(st.scan("tester", "nope", kind="ic"))


def test_stat_get_unknown_partition(tmp_path):
    _gsetup(tmp_path)
    from stkoe.graph.service import GraphService
    from stkoe.stat import StatController, StatNotFoundError

    svc = GraphService(data_dir=tmp_path / "data")
    svc.tester_add("t1", "fac1")
    svc.tester_update("t1")
    svc.close()
    st = StatController(data_dir=tmp_path / "data")
    _run(st.scan("tester", "t1", kind="ic"))
    with pytest.raises(StatNotFoundError):
        _run(st.get("tester", "t1", kind="ic", partition_by="nope"))


# ---------- 任务框架 ----------

def _task_result(mgr, task):
    return _result(mgr, task.task_id)


def test_task_add(mgr, tmp_path):
    _gsetup(tmp_path)
    t = _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1",
                                               "--returns", "r", "--groupby", "ic",
                                               "--marketcap", "fv"]))
    tm = _task_result(mgr, t)
    assert tm["name"] == "t1"
    assert tm["factor"] == "fac1"


def test_task_update(mgr, tmp_path):
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("tester", "update", ["t1"]))
    rep = _task_result(mgr, t)
    assert rep["materialized"] is True
    assert rep["changed"] is True


def test_task_update_idempotent(mgr, tmp_path):
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    _await(mgr, mgr.submit("tester", "update", ["t1"]))
    t = _await(mgr, mgr.submit("tester", "update", ["t1"]))
    rep = _task_result(mgr, t)
    assert rep["changed"] is False


def test_task_check(mgr, tmp_path):
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("tester", "check", ["t1"]))
    res = _task_result(mgr, t)
    assert res["ok"] is True


def test_task_list(mgr, tmp_path):
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("tester", "list", []))
    out = _task_result(mgr, t)
    assert any(x["name"] == "t1" for x in out)


def test_task_delete(mgr, tmp_path):
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    _await(mgr, mgr.submit("tester", "delete", ["t1"]))
    t = _await(mgr, mgr.submit("tester", "list", []))
    out = _task_result(mgr, t)
    assert out == []


def test_task_meta(mgr, tmp_path):
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("tester", "meta", ["t1"]))
    tm = _task_result(mgr, t)
    assert tm["name"] == "t1"
    assert tm["keys"] == ["sym", "date"]


def test_task_set_spec_shortcut(mgr, tmp_path):
    """任务版 test set --spec <csv>：逗号串 → periods（与 Execute 对齐）"""
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("tester", "set", ["t1", "--spec", "1,2"]))
    tm = _task_result(mgr, t)
    assert tm["spec"]["periods"] == [1, 2]


def test_task_stat_single_positional_test(mgr, tmp_path):
    """任务版 stat 单位置参数简写 → test 目标（scan 需 --kind 测试器，get/meta/delete 无条件）"""
    _gsetup(tmp_path)
    _await(mgr, mgr.submit("tester", "add", ["t1", "--factor", "fac1"]))
    _await(mgr, mgr.submit("tester", "update", ["t1"]))

    t = _await(mgr, mgr.submit("stat", "scan", ["t1", "--kind", "ic"]))
    rep = _task_result(mgr, t)
    assert rep["target_type"] == "tester" and rep["target_name"] == "t1"

    t2 = _await(mgr, mgr.submit("stat", "get", ["t1", "--kind", "ic"]))
    g = _task_result(mgr, t2)
    assert g["target"] == "tester:t1"

    t3 = _await(mgr, mgr.submit("stat", "meta", ["t1", "--kind", "ic"]))
    m = _task_result(mgr, t3)
    assert m["target_type"] == "tester" and m["target_name"] == "t1"


# ---------- 任务框架助手 ----------

def _await(mgr, task, timeout=10.0):
    import time

    from stkoe.task.model import TERMINAL_STATES

    deadline = time.monotonic() + timeout
    key = task.task_id if hasattr(task, "task_id") else task
    while time.monotonic() < deadline:
        cur = mgr.get(key)
        if cur is not None and cur.state in TERMINAL_STATES:
            return cur
        time.sleep(0.02)
    raise TimeoutError(f"task not terminal: {mgr.get(key).state}")


def _result(mgr, task_id):
    import json
    import time

    from stkoe.task.model import TERMINAL_STATES

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        evs = mgr.events.list_by_task(task_id)
        if evs and evs[-1].state in TERMINAL_STATES:
            return json.loads(evs[-1].data) if evs[-1].data else None
        time.sleep(0.01)
    evs = mgr.events.list_by_task(task_id)
    return json.loads(evs[-1].data) if evs and evs[-1].data else None
