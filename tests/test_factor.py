# -*- coding: utf-8 -*-
"""FactorController 测试：CRUD、实时计算、check、scan 物化、pipeline 算子链、依赖阻断、任务版"""
import polars as pl
import pytest

from stkoe.factor import FactorController, FactorNotFoundError
from stkoe.factor.engine import engine_names, get_engine, operator_names, parse_pipeline


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


@pytest.fixture()
def ctl(tmp_path):
    return FactorController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _setup_source(tmp_path):
    """源：idx 表 + dataset ds（keys=k）+ sample sp1 + feature f1（x*2 命名公式）"""
    root = tmp_path / "data"
    _write(root, "idx", pl.DataFrame({
        "k": ["a", "b", "c"],
        "x": [1.0, 2.0, 3.0],
    }))
    from stkoe.dataset import DatasetController
    from stkoe.feature import FeatureController
    from stkoe.sample import SampleController
    from stkoe.table import TableController

    tctl = TableController(data_dir=root)
    _run(tctl.add("idx", meta={"type": "index"}))
    dc = DatasetController(data_dir=root)
    _run(dc.add("ds", "idx", keys=["k"]))
    sc = SampleController(data_dir=root)
    _run(sc.add("sp1", dataset="ds"))
    fc = FeatureController(data_dir=root)
    _run(fc.add("f1", formula="x*2"))
    return root


def _gsetup(root):
    """graph 语义造数：idx/mem 表 → index → panel ds(keys=k) → sample sp1 → feature f1"""
    _write(root, "idx", pl.DataFrame({
        "k": ["a", "b"], "x": [1.0, 2.0]}))
    _write(root, "mem", pl.DataFrame({"k": ["a", "b"]}))
    from stkoe.graph.service import GraphService

    svc = GraphService(data_dir=root)
    svc.table_add("idx")
    svc.table_add("mem")
    svc.index_add("idx")
    svc.panel_add("ds", "idx", ["mem"], keys=["k"])
    svc.fieldset_add("fs1", "ds")
    svc.sample_add("sp1", "fs1")
    svc.feature_add("f1", "x*2")
    svc.close()
    return root


def test_engine_and_operator_registry(ctl):
    assert "polars" in engine_names()
    assert get_engine("polars").name == "polars"
    assert "nothing" in operator_names()
    ops = parse_pipeline("nothing()|nothing()")
    assert [op.name for op in ops] == ["nothing", "nothing"]


def test_parse_pipeline_unknown_operator(ctl):
    with pytest.raises(ValueError):
        parse_pipeline("standardlize()")


def test_parse_pipeline_bad_syntax(ctl):
    with pytest.raises(ValueError):
        parse_pipeline("nothing|foo")


def test_add_requires_feature_and_sample(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(ValueError):
        _add(ctl, "fac1")


def test_add_unknown_feature_error(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(FileNotFoundError):
        _add(ctl, "fac1", feature="nope", sample="sp1")


def test_add_unknown_sample_error(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(FileNotFoundError):
        _add(ctl, "fac1", feature="f1", sample="nope")


def test_add_meta_flow(ctl, tmp_path):
    _setup_source(tmp_path)
    fm = _add(ctl, "fac1", feature="f1", sample="sp1",
              display_name="因子1", pipeline="nothing()")
    assert fm.feature == "f1"
    assert fm.sample == "sp1"
    assert fm.factor_col == "f1"
    assert fm.keys == ("k",)
    assert fm.materialized is False
    assert fm.field is None


def test_get_computes_inline(ctl, tmp_path):
    """实时计算：sample 视图 x*2 → 索引列 k + 因子列 f1"""
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    df = _get(ctl, "fac1")
    assert df.columns == ["k", "f1"]
    assert df["k"].to_list() == ["a", "b", "c"]
    assert df["f1"].to_list() == [2.0, 4.0, 6.0]


def test_get_respects_sample_filter(ctl, tmp_path):
    """sample 带过滤公式 → 因子只作用在过滤后的样本上"""
    _setup_source(tmp_path)
    from stkoe.sample import SampleController

    sc = SampleController(data_dir=tmp_path / "data")
    _run(sc.set("sp1", formula="x>=2.0"))
    _add(ctl, "fac1", feature="f1", sample="sp1")
    df = _get(ctl, "fac1")
    assert df["k"].to_list() == ["b", "c"]
    assert df["f1"].to_list() == [4.0, 6.0]


def test_get_with_pipeline_nothing(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1", pipeline="nothing()")
    df = _get(ctl, "fac1")
    assert df["f1"].to_list() == [2.0, 4.0, 6.0]


def test_aggregate_feature_invalid(ctl, tmp_path):
    """feature 为聚合公式（结果行数 != 样本行数）→ 计算报错"""
    _setup_source(tmp_path)
    from stkoe.feature import FeatureController

    fc = FeatureController(data_dir=tmp_path / "data")
    _run(fc.add("fsum", formula="pl.col('x').sum()"))
    _add(ctl, "fac1", feature="fsum", sample="sp1")
    with pytest.raises(ValueError):
        _get(ctl, "fac1")


def test_set_updates_definition(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    fm = _run(ctl.set("fac1", pipeline="nothing()", display_name="改名"))
    assert fm.display_name == "改名"
    # 改 pipeline 后物化态复位（未物化时无感知）
    assert fm.pipeline == "nothing()"


def test_set_engine_is_definition_key(ctl, tmp_path):
    """set --engine：作为定义键（校验 get_engine + 物化失效），与 add 一致"""
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    _run(ctl.scan("fac1"))
    assert _meta(ctl, "fac1").curated is True
    fm = _run(ctl.set("fac1", engine="polars"))
    assert fm.engine == "polars"
    assert fm.materialized is False
    assert _meta(ctl, "fac1").curated is False
    with pytest.raises(ValueError):
        _run(ctl.set("fac1", engine="nope"))


def test_check_valid(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    res = _check(ctl, "fac1")
    assert res.ok is True
    assert res.rows == 3
    assert res.columns == ("k", "f1")


def test_check_invalid_feature(ctl, tmp_path):
    _setup_source(tmp_path)
    from stkoe.feature import FeatureController

    fc = FeatureController(data_dir=tmp_path / "data")
    _run(fc.add("fsum", formula="pl.col('x').sum()"))
    _add(ctl, "fac1", feature="fsum", sample="sp1")
    res = _check(ctl, "fac1")
    assert res.ok is False
    assert "非逐行" in res.message


def test_scan_materialize_and_read(ctl, tmp_path):
    """物化落盘 → meta.materialized=True、curated=True → get 读数致"""
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    rep = _run(ctl.scan("fac1"))
    assert rep.changed is True
    assert rep.partition_by == ()
    assert (tmp_path / "data" / "factors" / "fac1" / "data.parquet").exists()

    fm = _meta(ctl, "fac1")
    assert fm.materialized is True
    assert fm.curated is True

    df = _get(ctl, "fac1")
    assert df["f1"].to_list() == [2.0, 4.0, 6.0]


def test_scan_idempotent(ctl, tmp_path):
    """二次 scan：指纹一致跳过，不 bump 版本"""
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    r1 = _run(ctl.scan("fac1"))
    v = _meta(ctl, "fac1").version
    r2 = _run(ctl.scan("fac1"))
    assert r2.changed is False
    assert r2.version_after == v


def test_scan_resync(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    _run(ctl.scan("fac1"))
    v = _meta(ctl, "fac1").version
    r = _run(ctl.scan("fac1", resync=True))
    assert r.changed is True
    assert r.version_after == v + 1


def test_feature_change_invalidates_curated(ctl, tmp_path):
    """改 feature 公式 → 指纹变化，curated=False、get 回退实时计算"""
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    _run(ctl.scan("fac1"))
    assert _meta(ctl, "fac1").curated is True
    from stkoe.feature import FeatureController

    fc = FeatureController(data_dir=tmp_path / "data")
    _run(fc.set("f1", formula="x*3"))
    assert _meta(ctl, "fac1").curated is False
    df = _get(ctl, "fac1")
    assert df["f1"].to_list() == [3.0, 6.0, 9.0]


def test_meta_and_delete(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    with pytest.raises(FactorNotFoundError):
        _meta(ctl, "nope")
    assert _run(ctl.delete("fac1")) == {"deleted": "fac1"}
    with pytest.raises(FactorNotFoundError):
        _run(ctl.delete("fac1"))


def test_feature_delete_blocked_by_factor(ctl, tmp_path):
    """factor → feature 依赖：删除 feature 需 --force"""
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    from stkoe.feature import FeatureController
    from stkoe.table.controller import DependencyError

    fc = FeatureController(data_dir=tmp_path / "data")
    with pytest.raises(DependencyError):
        _run(fc.delete("f1"))


def test_sample_delete_blocked_by_factor(ctl, tmp_path):
    """factor → sample 依赖：删除 sample 需 --force"""
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    from stkoe.sample import SampleController
    from stkoe.table.controller import DependencyError

    sc = SampleController(data_dir=tmp_path / "data")
    with pytest.raises(DependencyError):
        _run(sc.delete("sp1"))


def test_list(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    _add(ctl, "fac2", feature="f1", sample="sp1")
    assert [fm.name for fm in _run(ctl.list())] == ["fac1", "fac2"]


def test_scan_all(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, "fac1", feature="f1", sample="sp1")
    reports = _run(ctl.scan(all=True))
    assert [r.name for r in reports] == ["fac1"]


def test_task_framework_factor_handlers(mgr):
    """任务版：factor add → check → get（result 落盘）→ scan → delete 全链路（graph 语义）"""
    _gsetup(mgr.data_dir)

    t_add = mgr.submit("factor", "add", ["fac1", "--feature", "f1",
                                         "--sample", "sp1"])
    _await(mgr, t_add)
    assert _mgr_result(mgr, t_add)["name"] == "fac1"

    t_get = mgr.submit("factor", "get", ["fac1"])
    _await(mgr, t_get)
    get_res = _mgr_result(mgr, t_get)
    assert get_res["columns"] == ["k", "f1"]
    assert get_res["result_ref"]
    assert get_res["rows"] == 2

    t_check = mgr.submit("factor", "check", ["fac1"])
    _await(mgr, t_check)
    assert _mgr_result(mgr, t_check)["ok"] is True

    t_scan = mgr.submit("factor", "scan", ["fac1"])
    _await(mgr, t_scan)
    assert _mgr_result(mgr, t_scan)["changed"] is True

    t_del = mgr.submit("factor", "del", ["fac1"])
    _await(mgr, t_del)
    assert _mgr_result(mgr, t_del) == {"deleted": "fac1"}


# ---------- async 助手 ----------

def _run(awaitable):
    import asyncio

    return asyncio.run(awaitable)


def _add(ctl, name, **kw):
    return _run(ctl.add(name, **kw))


def _get(ctl, name, **kw):
    return _run(ctl.get(name, **kw))


def _meta(ctl, name):
    return _run(ctl.meta(name))


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