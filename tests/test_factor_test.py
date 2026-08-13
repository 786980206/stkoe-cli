# -*- coding: utf-8 -*-
"""FactorTestController 测试：CRUD、必需列校验、实时构造、scan 物化幂等、
curated 失效、依赖阻断、任务版、stat testers 集成"""
import polars as pl
import pytest

from stkoe.factor_test import (FactorTestController, FactorTestExistsError,
                               FactorTestNotFoundError)
from stkoe.factor_test.spec import FactorTesterSpec


@pytest.fixture()
def mgr(tmp_path):
    from stkoe.task import TaskManager

    m = TaskManager(data_dir=tmp_path / "data")
    m.start()
    yield m
    m.stop()


@pytest.fixture()
def ctl(tmp_path):
    return FactorTestController(data_dir=tmp_path / "data")


def _write(root, name, rows):
    d = root / "tables" / name
    d.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(d / "data.parquet")


def _run(coro):
    import asyncio

    return asyncio.run(coro)


def _setup_source(tmp_path, panel=True):
    """源：idx 表（sym/date/r/ic/fv/x）+ dataset ds + sample sp1 + feature f1 + factor fac1"""
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
    _write(root, "idx", pl.DataFrame(rows))
    from stkoe.dataset import DatasetController
    from stkoe.factor import FactorController
    from stkoe.feature import FeatureController
    from stkoe.sample import SampleController
    from stkoe.table import TableController

    tctl = TableController(data_dir=root)
    _run(tctl.add("idx"))
    dc = DatasetController(data_dir=root)
    _run(dc.add("ds", "idx", "idx", keys=["sym", "date"]))
    sc = SampleController(data_dir=root)
    _run(sc.add("sp1", dataset="ds"))
    fc = FeatureController(data_dir=root)
    _run(fc.add("f1", formula="x*2"))
    fx = FactorController(data_dir=root)
    _run(fx.add("fac1", feature="f1", sample="sp1"))
    return root


def _add(ctl, name="t1", **kw):
    defaults = {"factor": "fac1", "returns": "r", "groupby": "ic",
                "marketcap": "fv"}
    defaults.update(kw)
    return _run(ctl.add(name, **defaults))


def _get(ctl, name="t1", **kw):
    df, total = _run(ctl.get(name, count_total=True, **kw))
    return df


def _scan(ctl, name="t1", **kw):
    return _run(ctl.scan(name, **kw))


# ---------- add 校验 ----------

def test_add_requires_factor(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(ValueError):
        _add(ctl, factor="")  # type: ignore[arg-type]


def test_add_unknown_factor_error(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(FileNotFoundError):
        _add(ctl, factor="nope")


def test_add_missing_required_cols_rejected(ctl, tmp_path):
    """sample 缺 returns/groupby/marketcap 列 → 不能创建"""
    root = tmp_path / "data"
    _write(root, "idx", pl.DataFrame({
        "sym": ["a", "b"], "date": ["2024-01-01", "2024-01-01"], "x": [1.0, 2.0],
    }))
    from stkoe.dataset import DatasetController
    from stkoe.factor import FactorController
    from stkoe.feature import FeatureController
    from stkoe.sample import SampleController
    from stkoe.table import TableController

    tctl = TableController(data_dir=root)
    _run(tctl.add("idx"))
    dc = DatasetController(data_dir=root)
    _run(dc.add("ds", "idx", "idx", keys=["sym", "date"]))
    sc = SampleController(data_dir=root)
    _run(sc.add("sp1", dataset="ds"))
    fc = FeatureController(data_dir=root)
    _run(fc.add("f1", formula="x*2"))
    fx = FactorController(data_dir=root)
    _run(fx.add("fac1", feature="f1", sample="sp1"))
    with pytest.raises(ValueError) as ei:
        _add(ctl, factor="fac1")
    assert "缺少测试必需列" in str(ei.value)
    assert "r" in str(ei.value)
    assert "ic" in str(ei.value)
    assert "fv" in str(ei.value)


def test_add_meta_flow(ctl, tmp_path):
    _setup_source(tmp_path)
    tm = _add(ctl)
    assert tm.name == "t1"
    assert tm.factor == "fac1"
    assert tm.sample == "sp1"
    assert tm.returns == "r"
    assert tm.groupby == "ic"
    assert tm.marketcap == "fv"
    assert tm.keys == ("sym", "date")
    assert tm.materialized is False
    assert tm.spec.quantiles == 5
    assert tm.spec.periods == (1, 5, 10)


def test_add_custom_spec_naming(ctl, tmp_path):
    _setup_source(tmp_path)
    spec = FactorTesterSpec(by_group=True, quantiles=3, periods=(1, 2),
                            date_range=("2024-01-01", "2024-01-02"))
    tm = _run(ctl.add("t1", factor="fac1", returns="r", groupby="ic",
                      marketcap="fv", spec=spec))
    assert tm.spec.by_group is True
    assert tm.spec.quantiles == 3
    assert tm.spec.periods == (1, 2)


def test_add_duplicate_error(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    with pytest.raises(FactorTestExistsError):
        _add(ctl)


# ---------- get / check ----------

def test_get_real_time_panel(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, spec=FactorTesterSpec(quantiles=3))
    df = _get(ctl)
    assert set(["date", "sym", "sample", "returns", "group", "marketcap",
                "factor", "d1", "d5", "d10", "factor_quantile"]).issubset(df.columns)
    assert df.height == 6
    assert df["factor"].to_list() == [2.0, 4.0, 6.0, 2.0, 4.0, 6.0]
    assert set(df["factor_quantile"].to_list()) == {1, 2, 3}


def test_get_forward_returns_correct(ctl, tmp_path):
    """d1 = 下一日是收益（sym 内升序）；末日为 null"""
    _setup_source(tmp_path)
    _add(ctl)
    df = _get(ctl).sort(["sym", "date"])
    a = df.filter(pl.col("sym") == "a")
    assert a["d1"].to_list()[0] == pytest.approx(0.01)
    assert a["d1"].to_list()[1] is None  # 最后一日无前向收益


def test_check_valid(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    res = _run(ctl.check("t1"))
    assert res.ok is True
    assert res.rows == 6


def test_get_not_found(ctl, tmp_path):
    _setup_source(tmp_path)
    with pytest.raises(FactorTestNotFoundError):
        _get(ctl, name="nope")


# ---------- scan 物化 ----------

def test_scan_materializes(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    rep = _scan(ctl)
    assert rep.materialized is True
    assert rep.changed is True
    assert rep.rows == 6
    tm = _run(ctl.meta("t1"))
    assert tm.materialized is True
    assert tm.curated is True
    assert len(tm.columns) == 11
    assert (tmp_path / "data" / "factor_tests" / "t1" / "data.parquet").exists()


def test_scan_idempotent(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    rep1 = _scan(ctl)
    rep2 = _scan(ctl)
    assert rep2.changed is False
    assert rep2.version_before == rep1.version_after
    assert _run(ctl.meta("t1")).curated is True


def test_scan_resync(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    rep = _scan(ctl, resync=True)
    assert rep.changed is True
    assert rep.version_after == rep.version_before + 1


def test_get_after_scan_reads_materialized(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    df = _get(ctl)
    assert df.height == 6


def test_set_config_invalidates_curated(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    _run(ctl.set("t1", quantiles=3))
    tm = _run(ctl.meta("t1"))
    assert tm.materialized is False
    assert tm.curated is False
    df = _get(ctl)
    assert set(df["factor_quantile"].to_list()) <= {1, 2, 3}


def test_set_returns_col_invalidates(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    _run(ctl.set("t1", returns="r"))
    tm = _run(ctl.meta("t1"))
    assert tm.materialized is False or tm.curated is False


# ---------- 依赖 / 删除 ----------

def test_delete_factor_blocked_by_test(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    from stkoe.factor import FactorController

    fx = FactorController(data_dir=tmp_path / "data")
    with pytest.raises(Exception) as ei:
        _run(fx.delete("fac1"))
    assert "dependencies exist" in str(ei.value)


def test_delete_factor_force(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    from stkoe.factor import FactorController

    fx = FactorController(data_dir=tmp_path / "data")
    _run(fx.delete("fac1", force=True))
    res = _run(ctl.check("t1"))
    assert res.ok is False


def test_delete_test(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    out = _run(ctl.delete("t1"))
    assert out == {"deleted": "t1"}
    with pytest.raises(FactorTestNotFoundError):
        _run(ctl.meta("t1"))
    assert not (tmp_path / "data" / "factor_tests" / "t1").exists()


def test_list(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl, name="t1")
    _add(ctl, name="t2")
    names = [tm.name for tm in _run(ctl.list())]
    assert names == ["t1", "t2"]


# ---------- stat testers 集成 ----------

def test_stat_scan_test_target(ctl, tmp_path):
    """stat scan test <name> --kind ic：写 stats/test/<name>/ic/*.parquet"""
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    from stkoe.stat import StatController

    st = StatController(data_dir=tmp_path / "data")
    report = _run(st.scan("test", "t1", kind="ic"))
    assert report.target_type == "test"
    assert report.target_name == "t1"
    assert set(report.partitions) == {"ic_d1", "ic_d5", "ic_d10"}
    out_dir = tmp_path / "data" / "stats" / "test" / "t1" / "ic"
    assert (out_dir / "ic_d1.parquet").exists()


def test_stat_get_test_partition(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    from stkoe.stat import StatController

    st = StatController(data_dir=tmp_path / "data")
    _run(st.scan("test", "t1", kind="ic"))
    df = _run(st.get("test", "t1", kind="ic", partition_by="ic_d1"))
    assert "IC(d1)" in df.columns
    assert "RankIC(d1)" in df.columns


def test_stat_all_testers(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    from stkoe.factor_test.tester import TESTER_KINDS
    from stkoe.stat import StatController

    st = StatController(data_dir=tmp_path / "data")
    for kind in TESTER_KINDS:
        report = _run(st.scan("test", "t1", kind=kind))
        assert report.files, f"{kind} 应产出文件"
        assert all((tmp_path / "data" / "stats" / f.rel_path).exists()
                   for f in report.files)


def test_stat_scan_test_unregistered(ctl, tmp_path):
    _setup_source(tmp_path)
    from stkoe.stat import StatController, StatNotFoundError

    st = StatController(data_dir=tmp_path / "data")
    with pytest.raises(StatNotFoundError):
        _run(st.scan("test", "nope", kind="ic"))


def test_stat_get_unknown_partition(ctl, tmp_path):
    _setup_source(tmp_path)
    _add(ctl)
    _scan(ctl)
    from stkoe.stat import StatController, StatNotFoundError

    st = StatController(data_dir=tmp_path / "data")
    _run(st.scan("test", "t1", kind="ic"))
    with pytest.raises(StatNotFoundError):
        _run(st.get("test", "t1", kind="ic", partition_by="nope"))


# ---------- 任务框架 ----------

def _task_result(mgr, task):
    return _result(mgr, task.task_id)


def test_task_add(ctl, mgr, tmp_path):
    _setup_source(tmp_path)
    t = _await(mgr, mgr.submit("test", "add", ["t1", "--factor", "fac1",
                                               "--returns", "r", "--groupby", "ic",
                                               "--marketcap", "fv"]))
    tm = _task_result(mgr, t)
    assert tm["name"] == "t1"
    assert tm["factor"] == "fac1"


def test_task_scan(ctl, mgr, tmp_path):
    _setup_source(tmp_path)
    _await(mgr, mgr.submit("test", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("test", "scan", ["t1"]))
    rep = _task_result(mgr, t)
    assert rep["materialized"] is True
    assert rep["changed"] is True


def test_task_scan_idempotent(ctl, mgr, tmp_path):
    _setup_source(tmp_path)
    _await(mgr, mgr.submit("test", "add", ["t1", "--factor", "fac1"]))
    _await(mgr, mgr.submit("test", "scan", ["t1"]))
    t = _await(mgr, mgr.submit("test", "scan", ["t1"]))
    rep = _task_result(mgr, t)
    assert rep["changed"] is False


def test_task_check(ctl, mgr, tmp_path):
    _setup_source(tmp_path)
    _await(mgr, mgr.submit("test", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("test", "check", ["t1"]))
    res = _task_result(mgr, t)
    assert res["ok"] is True


def test_task_list(ctl, mgr, tmp_path):
    _setup_source(tmp_path)
    _await(mgr, mgr.submit("test", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("test", "list", []))
    out = _task_result(mgr, t)
    assert any(x["name"] == "t1" for x in out)


def test_task_delete(ctl, mgr, tmp_path):
    _setup_source(tmp_path)
    _await(mgr, mgr.submit("test", "add", ["t1", "--factor", "fac1"]))
    _await(mgr, mgr.submit("test", "delete", ["t1"]))
    t = _await(mgr, mgr.submit("test", "list", []))
    out = _task_result(mgr, t)
    assert out == []


def test_task_meta(ctl, mgr, tmp_path):
    _setup_source(tmp_path)
    _await(mgr, mgr.submit("test", "add", ["t1", "--factor", "fac1"]))
    t = _await(mgr, mgr.submit("test", "meta", ["t1"]))
    tm = _task_result(mgr, t)
    assert tm["name"] == "t1"
    assert tm["keys"] == ["sym", "date"]


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

    evs = mgr.events.list_by_task(task_id)
    return json.loads(evs[-1].data) if evs and evs[-1].data else None