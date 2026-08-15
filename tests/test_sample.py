# -*- coding: utf-8 -*-
"""SampleController 测试：样本池 CRUD、dataset_with_fieldset 过滤、check 有效性、依赖阻断、任务版链路"""
import polars as pl
import pytest

from stkoe.sample import SampleController, SampleNotFoundError
from stkoe.sample.engine import engine_names, get_engine


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


@pytest.fixture()
def ctl(tmp_path):
    return SampleController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")

def _write_idx(root, name, rows):
    """index 资产写 indexs/ 目录（独立于 tables/）"""
    d = root / "indexs" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _setup_source(tmp_path):
    """基础源：idx 表 + dataset ds（keys=k）；日期为字符串列便于公式过滤"""
    root = tmp_path / "data"
    _write(root, "idx", pl.DataFrame({
        "k": ["a", "b", "c"],
        "x": [1.0, 2.0, 3.0],
        "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
    }))
    from stkoe.table import TableController

    from stkoe.dataset import DatasetController

    tctl = TableController(data_dir=root)
    _run(tctl.add("idx", meta={"type": "index"}))
    dc = DatasetController(data_dir=root)
    _run(dc.add("ds", "idx", keys=["k"]))
    return root


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


def _add_fieldset(root):
    """在 ds 上建 fieldset fs1 并校验指标 x2=x*2（参与 dataset_with_fieldset）"""
    from stkoe.fieldset import FieldsetController

    fs = FieldsetController(data_dir=root)
    _add(fs, "fs1", dataset="ds")
    _run(fs.add_field("fs1", "x2", formula="x*2"))
    _run(fs.check("fs1", "x2"))
    return fs


def test_engine_registry_has_polars(ctl):
    assert "polars" in engine_names()
    assert get_engine("polars").name == "polars"


def test_add_requires_dataset(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(ValueError):
        _add(ctl, "s1")


def test_add_unknown_dataset_error(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(FileNotFoundError):
        _add(ctl, "s1", dataset="nope")


def test_add_meta_flow(ctl, tmp_path):
    """add 携带元数据：formula/display_name/tags 进入 meta；keys 继承 dataset"""
    _setup_source(tmp_path)
    sm = _add(ctl, "s1", dataset="ds", formula="x>=2.0",
              display_name="样本1", tags="a,b")
    assert sm.dataset == "ds"
    assert sm.formula == "x>=2.0"
    assert sm.display_name == "样本1"
    assert sm.tags == ("a", "b")
    assert sm.keys == ("k",)


def test_get_empty_formula_returns_whole(ctl, tmp_path):
    """formula 为空 → 返回整个 dataset_with_fieldset（全部行 + fieldset 衍生列）"""
    root = _setup_source(tmp_path)
    _add_fieldset(root)
    sm = _add(ctl, "s1", dataset="ds")
    assert sm.formula == ""
    df = _get(ctl, "s1")
    assert df.height == 3
    assert {"k", "x", "date", "x2"} <= set(df.columns)
    assert df["x2"].to_list() == [2.0, 4.0, 6.0]


def test_get_with_formula_filter(ctl, tmp_path):
    """公式过滤生效：列作用域表达式按行过滤"""
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds", formula="x>=2.0")
    df = _get(ctl, "s1")
    assert df["k"].to_list() == ["b", "c"]
    assert df.height == 2


def test_get_with_expr_formula(ctl, tmp_path):
    """polars 表达式公式（pl.col 形态）与 is_in 过滤"""
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds",
         formula="pl.col('x').is_in([2.0, 3.0])")
    df = _get(ctl, "s1")
    assert df["k"].to_list() == ["b", "c"]


def test_get_columns_limit_where(ctl, tmp_path):
    """get 的 --columns/--where/--limit/--offset 与公式叠加"""
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds", formula="x>=2.0")
    df, total = _run(ctl.get("s1", columns=["k", "x"], where="x<=3.0",
                             limit=1, count_total=True))
    assert df.height == 1
    assert df.columns == ["k", "x"]
    assert total == 2  # 公式+where 过滤后未分页行数


def test_set_updates_formula(ctl, tmp_path):
    """set 改 formula → get 立即反映（无物化，读时动态过滤）"""
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds", formula="x>=2.0")
    sm = _run(ctl.set("s1", formula="x==1.0", display_name="改名", custom="v"))
    assert sm.formula == "x==1.0"
    assert sm.display_name == "改名"
    assert sm.extra.get("custom") == "v"
    df = _get(ctl, "s1")
    assert df["k"].to_list() == ["a"]


def test_check_valid_and_empty(ctl, tmp_path):
    """check：过滤后含全部索引列且行数>0 → ok；空结果 → 不 ok"""
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds", formula="x>=2.0")
    res = _check(ctl, "s1")
    assert res.ok is True
    assert res.rows == 2

    _run(ctl.set("s1", formula="x>100"))
    res2 = _check(ctl, "s1")
    assert res2.ok is False
    assert res2.rows == 0


def test_check_bad_formula(ctl, tmp_path):
    """公式执行失败 → check 不 ok 且消息含错误"""
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds", formula="nope+1")
    res = _check(ctl, "s1")
    assert res.ok is False
    assert "执行失败" in res.message


def test_meta_columns_include_fieldset(ctl, tmp_path):
    """meta.columns = 源 dataset 列 + fieldset 已校验衍生指标列"""
    root = _setup_source(tmp_path)
    _add_fieldset(root)
    sm = _add(ctl, "s1", dataset="ds")
    names = {c.name for c in sm.columns}
    assert {"k", "x", "date", "x2"} <= names


def test_delete_sample_and_dataset_blocked(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds")
    from stkoe.dataset import DatasetController

    from stkoe.table.controller import DependencyError

    dc = DatasetController(data_dir=tmp_path / "data")
    with pytest.raises(DependencyError):
        _run(dc.delete("ds"))  # s1 依赖 ds → 阻断
    with pytest.raises(SampleNotFoundError):
        _run(ctl.delete("nope"))
    assert _run(ctl.delete("s1")) == {"deleted": "s1"}
    assert _run(dc.delete("ds")) == {"deleted": "ds"}


def test_list(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "s1", dataset="ds")
    _add(ctl, "s2", dataset="ds")
    assert [sm.name for sm in _run(ctl.list())] == ["s1", "s2"]


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


# ---------- async 助手 ----------

def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def _add(ctl, name, **kw):
    return _run(ctl.add(name, **kw))


def _get(ctl, name, **kw):
    return _run(ctl.get(name, **kw))


def _check(ctl, name):
    return _run(ctl.check(name))


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