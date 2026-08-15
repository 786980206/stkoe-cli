# -*- coding: utf-8 -*-
"""FieldsetController 测试：指标集/指标的 CRUD、check/test/scan 物化、依赖阻断、任务版链路"""
import polars as pl
import pytest

from stkoe.fieldset import FieldsetController, FieldsetNotFoundError
from stkoe.fieldset.engine import engine_names, get_engine


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


@pytest.fixture()
def ctl(tmp_path):
    return FieldsetController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")

def _write_idx(root, name, rows):
    """index 资产写 indexs/ 目录（独立于 tables/）"""
    d = root / "indexs" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _setup_source(tmp_path, index_rows=None, member_rows=None):
    root = tmp_path / "data"
    index_rows = index_rows if index_rows is not None else pl.DataFrame({
        "k": ["a", "b", "c"],
        "x": [1.0, 2.0, 3.0],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
    })
    _write(root, "idx", index_rows)
    if member_rows is not None:
        _write(root, "m1", member_rows)
    from stkoe.table import TableController

    from stkoe.dataset import DatasetController

    tctl = TableController(data_dir=root)
    _run(tctl.add("idx", meta={"type": "index"}))
    dc = DatasetController(data_dir=root)
    if member_rows is not None:
        _run(tctl.add("m1"))
        _run(dc.add("ds", "idx", "m1", keys=["k"]))
    else:
        _run(dc.add("ds", "idx", keys=["k"]))
    return root


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


def test_engine_registry_has_polars(ctl):
    assert "polars" in engine_names()
    assert get_engine("polars").name == "polars"


def test_add_requires_dataset(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(ValueError):
        _add(ctl, "fs1")  # 缺 --dataset


def test_add_unknown_dataset_error(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(FileNotFoundError):
        _add(ctl, "fs1", dataset="nope")


def test_add_field_meta_flow(ctl, tmp_path):
    """指标集：add（含 meta）→ 指标 add → set 公式 → check → scan → get"""
    _setup_source(tmp_path)
    fm = _add(ctl, "fs1", dataset="ds", display_name="指标集1", tags="a,b")
    assert fm.dataset == "ds"
    assert fm.display_name == "指标集1"
    assert fm.tags == ("a", "b")
    assert fm.keys == ("k",)
    assert fm.columns == () or {c.name for c in fm.columns} >= {"k", "x", "date"}

    fm = _add_field(ctl, "fs1", "x2", formula="x*2")
    assert fm.fields[0].validated is False  # 新指标未校验

    fm = _set_field(ctl, "fs1", "x2", display_name="两倍", formula="x*2")
    assert fm.fields[0].display_name == "两倍"

    res = _check(ctl, "fs1", "x2")
    assert res[0].ok is True
    assert fm.fields[0].validated is False or _field_of(_meta(ctl, "fs1"), "x2").validated

    rep = _scan(ctl, "fs1")
    assert rep.materialized and rep.changed
    assert rep.fields_count == 1
    # 默认 get = dataset + fieldset 已校验指标 join 拼接视图
    df = _get(ctl, "fs1")
    assert df.columns == ["k", "x", "date", "x2"]
    assert df["x2"].to_list() == [2.0, 4.0, 6.0]
    # --fields-only 只返回衍生数据（keys + 已校验指标）
    df2 = _get(ctl, "fs1", fields_only=True)
    assert df2.columns == ["k", "x2"]
    assert df2["x2"].to_list() == [2.0, 4.0, 6.0]


def test_check_failure_keeps_unvalidated(ctl, tmp_path):
    """公式引发聚合/错误 → check 失败，指标保持未校验，scan 不含它"""
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    _add_field(ctl, "fs1", "agg", formula="pl.col('x').sum()")
    _add_field(ctl, "fs1", "x2", formula="x*2")
    res = _check(ctl, "fs1", "agg")
    assert res[0].ok is False

    # x2 也走 check → 全通过，只有聚合指标失败
    res_all = _check(ctl, "fs1", all_fields=True)
    by_name = {r.field: r.ok for r in res_all}
    assert by_name["agg"] is False
    assert by_name["x2"] is True

    rep = _scan(ctl, "fs1")
    assert rep.fields_count == 1
    df = _get(ctl, "fs1")
    assert "agg" not in df.columns


def test_field_formula_edit_resets_validated(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    _add_field(ctl, "fs1", "x2", formula="x*2")
    _check(ctl, "fs1", "x2")
    assert _field_of(_meta(ctl, "fs1"), "x2").validated is True
    _set_field(ctl, "fs1", "x2", formula="x*3")  # 改公式 → 复位
    assert _field_of(_meta(ctl, "fs1"), "x2").validated is False


def test_field_meta_and_delete(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    _add_field(ctl, "fs1", "x2", formula="x*2")
    f = _run(ctl.field_meta("fs1", "x2"))
    assert f.name == "x2" and f.formula == "x*2"
    with pytest.raises(FieldsetNotFoundError):
        _run(ctl.field_meta("fs1", "nope"))
    _run(ctl.delete_field("fs1", "x2"))
    assert _meta(ctl, "fs1").fields == ()


def test_set_fieldset_meta(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    fm = _run(ctl.set("fs1", display_name="改名", tags="x,y", custom="v"))
    assert fm.display_name == "改名"
    assert fm.tags == ("x", "y")
    assert fm.extra.get("custom") == "v"


def test_test_formula_success_and_error(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    df = _run(ctl.test("fs1", "x+1"))
    assert df["field"].to_list() == [2.0, 3.0, 4.0]
    with pytest.raises(Exception):
        _run(ctl.test("fs1", "nope+1"))


def test_scan_idempotent(ctl, tmp_path):
    """依赖签名不变 → 再 scan 不变化、不 bump version"""
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    _add_field(ctl, "fs1", "x2", formula="x*2")
    _check(ctl, "fs1", "x2")
    rep1 = _scan(ctl, "fs1")
    rep2 = _scan(ctl, "fs1")
    assert rep1.changed is True
    assert rep2.changed is False
    assert rep2.version_before == rep2.version_after


def test_get_live_before_scan(ctl, tmp_path):
    """未物化时 get 走实时计算视图（不隐式物化）"""
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    _add_field(ctl, "fs1", "x2", formula="x*2")
    _check(ctl, "fs1", "x2")
    fm = _meta(ctl, "fs1")
    assert fm.materialized is False
    df = _get(ctl, "fs1")
    assert df["x2"].to_list() == [2.0, 4.0, 6.0]


def test_delete_fieldset_and_dataset_blocked(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    from stkoe.dataset import DatasetController

    from stkoe.table.controller import DependencyError

    dc = DatasetController(data_dir=tmp_path / "data")
    with pytest.raises(DependencyError):
        _run(dc.delete("ds"))  # fs1 依赖 ds → 阻断
    assert _run(ctl.delete("fs1")) == {"deleted": "fs1"}
    assert _run(dc.delete("ds")) == {"deleted": "ds"}


def test_list(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fs1", dataset="ds")
    _add(ctl, "fs2", dataset="ds")
    assert [m.name for m in _run(ctl.list())] == ["fs1", "fs2"]


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


# ---------- async 助手 ----------

def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def _add(ctl, name, **kw):
    return _run(ctl.add(name, **kw))


def _add_field(ctl, name, field, **kw):
    return _run(ctl.add_field(name, field, **kw))


def _set_field(ctl, name, field, **kw):
    return _run(ctl.set_field(name, field, **kw))


def _check(ctl, name, *field, all_fields=False):
    return _run(ctl.check(name, field[0] if field else None, all_fields=all_fields))


def _scan(ctl, name, **kw):
    return _run(ctl.scan(name, **kw))


def _get(ctl, name, **kw):
    return _run(ctl.get(name, **kw))


def _meta(ctl, name):
    return _run(ctl.meta(name))


def _field_of(fm, name):
    return next(f for f in fm.fields if f.name == name)


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